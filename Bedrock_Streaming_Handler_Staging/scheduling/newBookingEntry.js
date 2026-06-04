'use strict';

/**
 * newBookingEntry.js — INTEGRATOR glue (§B16d): the BSH entry-hook for the in-chat
 * NEW-booking flow. It connects the merged WS-NEWBOOK-FLOW module
 * (`newBookingFlow.runNewBookingTurn`) to the runtime:
 *   - the BCH invoke seams (`deps.invokeProposal` / `deps.invokeBookingCommit`) — supplied
 *     by index.js (a RequestResponse invoke of Booking_Commit_Handler),
 *   - the C9 session state store (`deps.loadState` / `deps.saveState`) — supplied by index.js,
 *   - a resolved `qualifyingContext` (appointment-type / timezone / conference) built HERE
 *     from the tenant `scheduling` config block.
 *
 * Mirrors `bindingContext.injectSchedulingContext`: ONE call per BSH post-stream site, and a
 * NO-OP (returns `{ handled:false }`) for any non-new-booking session — so normal chat AND the
 * recovery loop are untouched (the caller runs this only when the recovery loop did NOT handle
 * the turn). Feature-gating (`schedulingEnabled`) is the caller's, exactly like runSchedulingTurn.
 *
 * ── ENTRY (§B16d) ──
 *   A fresh chat with the widget's `routing_metadata.scheduling_intent === 'new_booking'`
 *   signal BOOTSTRAPS the flow: create the `qualifying` ConversationSchedulingSession row (if
 *   one isn't already in flight), then `runNewBookingTurn` drives qualifying→proposing→
 *   confirming→booked. On later turns (no signal) an in-flight new-booking state row keeps it
 *   driving. There is NO §B10 token binding (that is the recovery loop).
 *
 * ── ATTENDEE (v1 scope) ──
 *   `qualifyingContext.attendee` is NOT populated here yet, so the flow proposes slots and
 *   handles slot selection but HOLDS the commit at `confirming` (the FLOW's attendee-not-yet-
 *   known guard) until identity is sourced. Attendee-sourcing (form-injection structured read
 *   for the post-application case, or in-chat collection for from-scratch) is a tracked
 *   follow-up; the `invokeBookingCommit` dep is wired-ready for it.
 *
 * ── tenant (audit row 9) ──
 *   The caller passes `tenantId = config?.tenant_id` (authenticated S3 config), NEVER a
 *   request-body value — same as the runSchedulingTurn call sites.
 */

const { runNewBookingTurn } = require('./newBookingFlow');
const { isSchedulingEnabled } = require('./bindingContext');

// The in-flight new-booking session states (NOT 'booked' — a booked arc is finished, so a
// stray later turn must not re-engage it). Mirrors newBookingFlow's NEW_BOOKING_STATES minus
// the terminal 'booked'.
const IN_FLIGHT_STATES = Object.freeze(['qualifying', 'proposing', 'confirming']);

/**
 * Build `qualifyingContext` from the tenant `scheduling` config block. Schema-discipline:
 * tolerate missing fields. Returns at least `{ appointmentTypeId, userTimeZone, conference_type }`.
 * `appointment_type` is the config object the §B16c commit forwards; `attendee` is intentionally
 * omitted in v1 (see header).
 * @param {object} params - { config, routingMetadata, attendee? }
 * @returns {object} qualifyingContext
 */
function resolveQualifyingContext({ config, routingMetadata = {}, attendee } = {}) {
  const scheduling = (config && config.scheduling) || {};
  const types = scheduling.appointment_types || {};
  const ids = Object.keys(types);

  // appointmentTypeId: the CTA's explicit choice if it named a real one, else the SOLE
  // configured type (the v1 single-appt-type tenant), else null (qualifying would ask — the
  // flow holds without it).
  const requested = routingMetadata.appointment_type_id || routingMetadata.appointmentTypeId;
  let appointmentTypeId = null;
  if (requested && types[requested]) appointmentTypeId = requested;
  else if (ids.length === 1) appointmentTypeId = ids[0];
  else if (requested) appointmentTypeId = requested; // pass through; propose validates/escalates

  const appointment_type = appointmentTypeId ? types[appointmentTypeId] : undefined;

  const userTimeZone =
    (appointment_type && (appointment_type.timezone || appointment_type.time_zone)) ||
    routingMetadata.user_time_zone ||
    routingMetadata.userTimeZone ||
    'UTC';

  // v1 is Google-only; default to google_meet unless the appt-type opts out.
  const conference_type =
    (appointment_type && (appointment_type.conference_type || appointment_type.conferenceType)) ||
    'google_meet';

  const qctx = { appointmentTypeId, appointment_type, userTimeZone, conference_type };
  if (attendee && attendee.email) qctx.attendee = attendee;
  return qctx;
}

/**
 * Post-stream new-booking entry. Returns `{ handled }` like runSchedulingTurn. Non-fatal:
 * any error degrades to `{ handled:false }` so a scheduling failure never breaks the (already
 * streamed) chat response.
 *
 * @param {object} params - { responseText, conversationHistory, tenantId, sessionId, config,
 *                            bedrock, write, routingMetadata, deps }
 * @returns {Promise<{handled:boolean}>}
 */
async function runNewBookingEntry({
  responseText,
  conversationHistory,
  tenantId,
  sessionId,
  config,
  bedrock,
  write,
  routingMetadata = {},
  deps = {},
} = {}) {
  try {
    if (!tenantId || !sessionId) return { handled: false };
    // Defense-in-depth (symmetric with newBookingFlow.runNewBookingTurn): the call site already
    // gates on schedulingEnabled, but never engage on a config lacking the feature flag even if
    // this is ever called directly. Fail-closed.
    if (!isSchedulingEnabled(config)) return { handled: false };

    const intentNew = routingMetadata.scheduling_intent === 'new_booking';

    // One state read to decide engagement (same cost profile as the recovery loop's binding
    // read; gated by schedulingEnabled at the call site). Skip the whole path — including any
    // qctx/attendee resolution — for a normal chat turn that is neither a fresh new_booking
    // signal nor an in-flight new-booking session.
    const prior = deps.loadState ? await deps.loadState({ tenantId, sessionId }) : null;
    const inFlight = !!(prior && IN_FLIGHT_STATES.includes(prior.state));
    if (!intentNew && !inFlight) return { handled: false };

    // Fresh entry: create the qualifying row when a new_booking signal arrives and nothing is
    // in flight. Idempotent — an in-flight session is NOT reset (so a re-sent signal mid-flow
    // doesn't clobber proposing/confirming).
    if (intentNew && !inFlight && deps.saveState) {
      await deps.saveState({ tenantId, sessionId, state: 'qualifying' });
    }

    const qualifyingContext = resolveQualifyingContext({ config, routingMetadata });

    return await runNewBookingTurn({
      responseText,
      conversationHistory,
      tenantId,
      sessionId,
      config,
      bedrock,
      write,
      deps: { ...deps, qualifyingContext },
    });
  } catch (err) {
    console.error(`[WS-NEWBOOK] entry-hook failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: false, error: true };
  }
}

module.exports = { runNewBookingEntry, resolveQualifyingContext, IN_FLIGHT_STATES };
