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
// §B16d attendee-sourcing (form-injection read): CONSUME the WS-C2 module's already-exported
// same-session form-submission read primitives. We do NOT touch formInjection.js — we assemble
// the attendee from its canonical `contact` here, in integrator-owned glue.
const { fetchSessionSubmissions, pickLatest } = require('./formInjection');

// The in-flight new-booking session states (NOT 'booked' — a booked arc is finished, so a
// stray later turn must not re-engage it). Mirrors newBookingFlow's NEW_BOOKING_STATES minus
// the terminal 'booked'.
const IN_FLIGHT_STATES = Object.freeze(['qualifying', 'proposing', 'confirming']);

// Email shape check. The commit (§B16c) REQUIRES a usable attendee.email; a value that can't be
// an address is worse than none (it would book a dead inbox), so we treat it as "no identity" and
// let the flow hold at `confirming`. Rejects angle-bracket forms (`<a@b.com>`), trailing-dot
// domains (`a@b.com.`), IP-literals (`a@[1.2.3.4]`), and <2-char TLDs; accepts plus-addressing,
// subdomains, and mixed case. (Tightened per PR #230 audit A1 — the prior regex passed all of
// the above.) Length is capped separately (RFC 5321 = 254).
const EMAIL_SHAPE = /^[^\s@<>]+@[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*\.[A-Za-z]{2,}$/;

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
 * §B16d attendee-sourcing (form-injection read) — the POST-APPLICATION case: a visitor who
 * submitted a form THIS session (e.g. a volunteer application) and then books. Resolve their
 * identity from the most recent same-session submission's canonical `contact` so the flow can
 * COMMIT instead of holding at `confirming`. Returns `{ email, first_name?, last_name? }` or
 * `null` when no submission / no usable email exists (→ the flow's identity-required hold).
 *
 * Distinct from formInjection.buildFormContextBlock (which sanitizes for the LLM PROMPT surface):
 * here the email/name flow to the calendar COMMIT, not into a prompt — so values are read
 * canonically (trimmed, email shape-validated), NOT injection-marker-mangled. Read-only;
 * non-fatal (any error → null, so a sourcing failure degrades to the hold, never breaks chat).
 *
 * @param {object} params - { tenantId, sessionId, client? }
 * @returns {Promise<{email:string, first_name?:string, last_name?:string}|null>}
 */
async function resolveSessionAttendee({ tenantId, sessionId, client } = {}) {
  try {
    const items = await fetchSessionSubmissions({ tenantId, sessionId, client });
    if (!items || !items.length) return null;
    const latest = pickLatest(items);
    const contact = (latest && latest.contact && typeof latest.contact === 'object') ? latest.contact : {};
    const email = typeof contact.email === 'string' ? contact.email.trim() : '';
    // Reject empty / over-long (RFC 5321 = 254) / malformed — see EMAIL_SHAPE (audit A1/A2).
    if (!email || email.length > 254 || !EMAIL_SHAPE.test(email)) return null;
    const out = { email };
    // §B16c also accepts optional first_name/last_name/phone; cap each so an over-long stored
    // value can't bloat the calendar invite (audit A2/A3). `name` is composed BCH-side.
    const fn = typeof contact.first_name === 'string' ? contact.first_name.trim().slice(0, 100) : '';
    const ln = typeof contact.last_name === 'string' ? contact.last_name.trim().slice(0, 100) : '';
    const ph = typeof contact.phone === 'string' ? contact.phone.trim().slice(0, 40) : '';
    if (fn) out.first_name = fn;
    if (ln) out.last_name = ln;
    if (ph) out.phone = ph;
    return out;
  } catch (err) {
    // PII-safe: log ONLY the error shape — never tenantId, sessionId, email, or name.
    console.error(`[WS-NEWBOOK] attendee-sourcing skipped (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return null;
  }
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

    // §B16d attendee-sourcing: resolve same-session applicant identity (post-application case)
    // so the commit can complete. Injectable for tests; defaults to the form-injection read.
    // Non-fatal → null → the flow holds at `confirming` (identity_required), never breaks chat.
    const sourceAttendee = deps.getSessionAttendee || resolveSessionAttendee;
    let attendee = await sourceAttendee({ tenantId, sessionId });

    // §B16d amendment (deterministic pipeline): a chat-captured email persisted on the
    // session row (captureAttendeeEmail below) is the fallback identity source when no
    // same-session form submission carries one. Shape-revalidated on read — a stored
    // value must never bypass the commit's email contract.
    if (!attendee && prior && typeof prior.attendee_email === 'string') {
      const storedEmail = prior.attendee_email.trim();
      if (storedEmail && storedEmail.length <= 254 && EMAIL_SHAPE.test(storedEmail)) {
        attendee = { email: storedEmail };
      }
    }

    const qualifyingContext = resolveQualifyingContext({ config, routingMetadata, attendee });

    return await runNewBookingTurn({
      responseText,
      conversationHistory,
      tenantId,
      sessionId,
      config,
      bedrock,
      write,
      // §B16e: surface the widget's deterministic day-picker signal to the flow
      // (rides routing_metadata like scheduling_intent; the flow validates it
      // against the offered strip before acting). §B16b amendment: the slot-chip /
      // confirm-button clicks ride the same seam (scheduling_action + scheduling_slot_id)
      // so the flow can act deterministically instead of via the LLM detector.
      deps: {
        ...deps,
        qualifyingContext,
        schedulingDaySelected: routingMetadata.scheduling_day_selected,
        schedulingAction: routingMetadata.scheduling_action,
        schedulingSlotId: routingMetadata.scheduling_slot_id,
      },
    });
  } catch (err) {
    console.error(`[WS-NEWBOOK] entry-hook failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: false, error: true };
  }
}

/**
 * §B16d amendment (deterministic pipeline): capture an attendee email TYPED in chat for an
 * in-flight new-booking session holding at `confirming`. Deterministic — the caller has
 * already shape-tested the message as a bare email; this re-validates, requires a
 * confirming session WITH a selected slot, persists the email on the session row, and
 * re-arms the widget's confirm affordance via a `scheduling_confirm` SSE.
 *
 * NEVER commits (§B14: free text never commits) — the user must still tap the confirm
 * button, whose click rides scheduling_action:'confirm_book'.
 *
 * @param {object} params - { tenantId, sessionId, email, deps: {loadState, saveState}, write }
 * @returns {Promise<{captured: boolean, email?: string, reason?: string}>}
 */
async function captureAttendeeEmail({ tenantId, sessionId, email, deps = {}, write } = {}) {
  try {
    if (!tenantId || !sessionId || !deps.loadState || !deps.saveState) {
      return { captured: false, reason: 'unwired' };
    }
    const trimmed = typeof email === 'string' ? email.trim() : '';
    if (!trimmed || trimmed.length > 254 || !EMAIL_SHAPE.test(trimmed)) {
      return { captured: false, reason: 'invalid_email' };
    }
    const prior = await deps.loadState({ tenantId, sessionId });
    if (!prior || prior.state !== 'confirming') {
      return { captured: false, reason: 'no_confirming_session' };
    }
    const selected = prior.selected_slot;
    if (!selected || !selected.slotId) {
      return { captured: false, reason: 'no_slot_selected' };
    }
    await deps.saveState({
      tenantId,
      sessionId,
      state: 'confirming',
      candidate_slots: prior.candidate_slots,
      selected_slot: prior.selected_slot,
      proposal: prior.proposal,
      rejected_slot_ids: prior.rejected_slot_ids,
      attendee_email: trimmed,
    });
    if (typeof write === 'function') {
      const fromCandidates = (prior.candidate_slots || []).find((s) => s && s.slotId === selected.slotId);
      write(`data: ${JSON.stringify({
        type: 'scheduling_confirm',
        session_id: sessionId,
        slot: { slotId: selected.slotId, label: (fromCandidates && fromCandidates.label) || selected.label },
        attendee_email: trimmed,
      })}\n\n`);
    }
    return { captured: true, email: trimmed };
  } catch (err) {
    console.error(`[WS-NEWBOOK] captureAttendeeEmail failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { captured: false, reason: 'error' };
  }
}

module.exports = { runNewBookingEntry, resolveQualifyingContext, resolveSessionAttendee, captureAttendeeEmail, EMAIL_SHAPE, IN_FLIGHT_STATES };
