'use strict';

/**
 * WS-CONVO — in-chat reschedule/cancel flow + the §B14 action BOUNDARY (B3 keystone).
 *
 * Canonical: scheduling_design.md §9.2 (state machine) + §9.4 (reschedule/cancel);
 * FROZEN_CONTRACTS.md §B14 (THE BOUNDARY — LOCKED), §B9 (`executeReschedule`/
 * `executeCancel`), §B13 (`buildCalendarFacade`), §B6 (ConferenceProvider), §B15
 * (Zoom `updateMeeting`), §B3 (`generateSlots`), C9 (`stateMachine`).
 *
 * ── THE BOUNDARY (§B14, the load-bearing rule) ──
 *   The C9 stateMachine is AUTHORITATIVE; the streaming LLM is ADVISORY. A state-changing
 *   calendar op (executeReschedule / executeCancel) runs ONLY on a discrete STRUCTURED
 *   action produced by a FOCUSED post-stream call (mirrors the V4.0 Action Selector — BSH
 *   has no native tool-use). Free-text the streaming LLM emits ("I've confirmed it" in prose)
 *   NEVER executes. Every transition is validated through `stateMachine.transition`. The
 *   binding + C9 state row are ground truth; an unparseable / unknown detector output is
 *   treated as 'none' (fail-closed → no execution).
 *
 * ── DI seam / packaging ──
 *   esbuild bundles BSH from index.js. The §B13 facade's real auth modules
 *   (Booking_Commit_Handler/oauth-client.js + calendar-events.js) pull `googleapis` /
 *   `google-auth-library`, which BSH does NOT depend on — so they are NOT required here;
 *   the INTEGRATOR injects `deps.getOAuthClient` + `deps.calendarEvents` (or a pre-built
 *   `deps.calendar` facade) + `deps.conference` + the booking I/O (`deps.loadBooking` /
 *   `deps.saveBooking`) + state I/O (`deps.loadState` / `deps.saveState`) when wiring the
 *   live path. Everything esbuild-safe (execute*, generateSlots, updateMeeting,
 *   resolveBinding, buildCalendarFacade factory, stateMachine) gets a real default so unit
 *   tests run against the shipped logic and prod imports the same code. When the Google /
 *   I/O seam is absent, execution is SKIPPED non-fatally (detection + transitions still run)
 *   — see `_resolveFacade`. With NO binding the whole flow is a no-op (no-regression).
 */

const { resolveBinding: realResolveBinding } = require('../../shared/scheduling/sessionBinding');
const { executeReschedule: realExecuteReschedule } = require('../../shared/scheduling/reschedule');
const { executeCancel: realExecuteCancel } = require('../../shared/scheduling/cancel');
const { generateSlots: realGenerateSlots } = require('../../shared/scheduling/slots');
const { buildCalendarFacade: realBuildCalendarFacade } = require('../../shared/scheduling/calendarFacade');
// NOTE (packaging): the shared/scheduling modules above bundle into BSH cleanly because
// their only npm deps are AWS-SDK packages marked external in esbuild.config.mjs. The
// Booking_Commit_Handler/* modules do NOT — they resolve deps (googleapis, secrets-manager,
// @smithy/*) from the SIBLING Lambda's node_modules, which BSH cannot bundle. So §B15
// `updateMeeting`, the §B6 ConferenceProvider, and the §B13 facade's Google-auth modules
// (getOAuthClient/calendarEvents) are NOT required here — the integrator INJECTS them when
// wiring the live path (deps.updateMeeting / deps.conference / deps.getOAuthClient +
// deps.calendarEvents, or a pre-built deps.calendar). Absent → execution is skipped non-fatally.
const {
  transition,
  IllegalStateTransition,
} = require('../../shared/scheduling/stateMachine');

const { initStateFromIntent } = require('./bindingContext');

// The four §B14 structured actions. Anything else (incl. unparseable output) → 'none'.
const ACTIONS = Object.freeze(['select_slot', 'confirm_reschedule', 'confirm_cancel', 'none']);

// ─── booking field reads (schema discipline — tolerate camel OR snake) ──────────────────

function pick(booking, camel, snake) {
  if (!booking) return undefined;
  return booking[camel] != null ? booking[camel] : booking[snake];
}

// v1: the coordinator's calendar id == coordinator_email == resourceId (§B7).
function calendarIdOf(booking) {
  return (
    pick(booking, 'coordinatorEmail', 'coordinator_email') ||
    pick(booking, 'resourceId', 'resource_id')
  );
}

function eventIdOf(booking) {
  return pick(booking, 'externalEventId', 'external_event_id');
}

// Is this booking a Zoom meeting (needs the §B15 start-time PATCH after a move)?
function zoomMeetingIdOf(booking) {
  const provider = pick(booking, 'conferenceProvider', 'conference_provider');
  const confId = pick(booking, 'conferenceId', 'conference_id');
  if (!confId) return null;
  // Treat an explicit zoom provider, OR a numeric conference id (Zoom meeting ids are
  // numeric) when the provider is unset, as Zoom. Meet/Null ids are not PATCHed.
  if (provider && String(provider).toLowerCase() === 'zoom') return String(confId);
  if (!provider && /^\d+$/.test(String(confId))) return String(confId);
  return null;
}

// ─── §B14 structured action detector (focused post-stream call; mirrors selectActionsV4) ─

/**
 * Decide, from the just-streamed turn, whether the user took a discrete scheduling action.
 * Returns one of the four §B14 actions. Fail-closed: any error / unparseable / unknown
 * output → { action: 'none' } so a transient LLM hiccup can NEVER trigger a calendar op.
 *
 * @param {object} params
 * @param {string} params.responseText       - the assistant's just-streamed response
 * @param {Array}  params.conversationHistory
 * @param {object} params.session            - { state } (advisory context for the model)
 * @param {object} params.binding            - the §B10 binding ({ booking_id, intent })
 * @param {object} params.config             - tenant config (model_id)
 * @param {object} params.bedrock            - BedrockRuntimeClient (injected)
 * @returns {Promise<{action:string, slotId?:string, booking_id?:string}>}
 */
async function detectSchedulingAction({
  responseText,
  conversationHistory,
  session,
  binding,
  config,
  bedrock,
} = {}) {
  const bookingId = binding && binding.booking_id;
  try {
    if (!bedrock || typeof bedrock.send !== 'function') {
      return { action: 'none' };
    }
    const recent = (conversationHistory || []).slice(-6);
    const conversationBlock = recent
      .map((m) => `${m.role === 'user' ? 'User' : 'Assistant'}: ${(m.content || m.text || '').trim()}`)
      .filter(Boolean)
      .join('\n');

    const prompt = `You detect whether the user just took a discrete SCHEDULING action in a reschedule/cancel flow. The session state is "${session && session.state}".

CONVERSATION:
${conversationBlock}
Assistant: ${responseText}

Decide the SINGLE action the user has explicitly taken RIGHT NOW:
- "select_slot": the user picked a specific offered time slot. Include its slotId.
- "confirm_reschedule": the user explicitly confirmed moving the booking to the selected time.
- "confirm_cancel": the user explicitly confirmed canceling the booking.
- "none": anything else — a question, hesitation, general chat, or the assistant merely SAYING it's done. Default to "none" unless the user clearly and explicitly committed.

Return ONLY raw JSON: {"action":"select_slot|confirm_reschedule|confirm_cancel|none","slotId":"<id or omit>"}. No markdown, no prose.`;

    const { InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
    const modelId = (config && (config.model_id || (config.aws && config.aws.model_id))) || process.env.BEDROCK_MODEL_ID;

    const command = new InvokeModelCommand({
      modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: 60,
        temperature: 0,
      }),
    });

    const response = await bedrock.send(command);
    const body = JSON.parse(new TextDecoder().decode(response.body));
    let raw = ((body && body.content && body.content[0] && body.content[0].text) || '').trim();
    if (raw.startsWith('```')) {
      raw = raw.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '').trim();
    }

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return { action: 'none' };
    }
    const action = parsed && ACTIONS.includes(parsed.action) ? parsed.action : 'none';
    const out = { action, booking_id: bookingId };
    if (action === 'select_slot' && parsed.slotId != null) {
      out.slotId = String(parsed.slotId);
    }
    return out;
  } catch (err) {
    console.error(`[WS-CONVO] action detect failed (fail-closed → none): error_name=${(err && err.name) || 'unknown'}`);
    return { action: 'none' };
  }
}

// ─── facade / conference resolution (the integrator Google-auth seam) ────────────────────

/**
 * Resolve the §B13 calendar facade for this booking. Precedence: a pre-built
 * `deps.calendar` (tests / future integrator wiring) → else build it from the injected
 * `deps.getOAuthClient` + `deps.calendarEvents` (the real Google-auth modules the
 * integrator wires; NOT bundled here). Returns null when the seam is unwired — the caller
 * then SKIPS execution non-fatally.
 */
function _resolveFacade({ tenantId, binding, booking, deps }) {
  if (deps.calendar) return deps.calendar;
  const buildFacade = deps.buildCalendarFacade || realBuildCalendarFacade;
  if (deps.getOAuthClient && deps.calendarEvents) {
    const coordinatorId =
      (binding && binding.coordinator_id) ||
      pick(booking, 'resourceId', 'resource_id') ||
      calendarIdOf(booking);
    return buildFacade({
      tenantId,
      coordinatorId,
      deps: { getOAuthClient: deps.getOAuthClient, calendarEvents: deps.calendarEvents },
    });
  }
  return null;
}

// ─── execution (only reached past the §B14 boundary) ─────────────────────────────────────

async function _doReschedule({ tenantId, binding, booking, newSlot, deps, logger }) {
  const facade = _resolveFacade({ tenantId, binding, booking, deps });
  if (!facade || !deps.conference) {
    // Integrator seam (Google auth / ConferenceProvider) not wired — do NOT execute.
    console.warn('[WS-CONVO] calendar facade / conference not wired (integrator seam) — reschedule skipped');
    return { executed: false, reason: 'calendar_seam_unwired' };
  }
  const executeReschedule = deps.executeReschedule || realExecuteReschedule;
  const result = await executeReschedule({
    booking,
    newSlot,
    deps: { calendar: facade, conference: deps.conference, logger },
  });

  // §B15: reschedule.js preserves the Zoom JOIN url (read-before-write) but does NOT PATCH
  // the start time — the in-chat caller does, for a successful/pending move on a Zoom booking.
  if (result.outcome === 'success' || result.outcome === 'pending_calendar_sync') {
    const meetingId = zoomMeetingIdOf(result.booking || booking);
    if (meetingId && deps.updateMeeting) {
      try {
        await deps.updateMeeting({
          tenantId,
          meetingId,
          start: newSlot.start,
          end: newSlot.end,
          timezone: pick(result.booking || booking, 'timeZone', 'timezone'),
        });
      } catch (err) {
        // Non-fatal: the move already happened; a stale Zoom start time is recoverable.
        console.warn(`[WS-CONVO] zoom updateMeeting failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
      }
    }
  }

  if (deps.saveBooking) await deps.saveBooking(result.booking);
  return { executed: true, outcome: result.outcome, booking: result.booking };
}

async function _doCancel({ tenantId, binding, booking, deps, logger }) {
  const facade = _resolveFacade({ tenantId, binding, booking, deps });
  if (!facade) {
    console.warn('[WS-CONVO] calendar facade not wired (integrator seam) — cancel skipped');
    return { executed: false, reason: 'calendar_seam_unwired' };
  }
  // §B9 (cancel.js re-synced to the two-arg shape, lambda#212): executeCancel resolves
  // calendarId (coordinator_email) + eventId (external_event_id) from the booking itself
  // and calls the §B13 facade's deleteEvent(calendarId, eventId) directly — so we pass the
  // facade as-is (the earlier booking-shape adapter is obsolete; no fork, no bridge needed).
  const executeCancel = deps.executeCancel || realExecuteCancel;
  const result = await executeCancel({ booking, deps: { calendar: facade, logger } });
  // The §14.2 cal-lifecycle listener flips Booking.status on the calendar delete — NOT us.
  if (deps.saveBooking) await deps.saveBooking(result.booking);
  return { executed: true, outcome: result.outcome, booking: result.booking };
}

// ─── slot presentation (B-minimal: emit slot DATA; rich chip rendering is C12 / B-remainder) ─

async function _presentSlots({ tenantId, sessionId, binding, booking, config, deps, write, fromState }) {
  const generateSlots = deps.generateSlots || realGenerateSlots;
  let slots = [];
  try {
    slots = generateSlots({
      busyIntervals: deps.busyIntervals || [],
      appointmentType: deps.appointmentType || (config && config.appointment_type) || {},
      userTimeZone: deps.userTimeZone || pick(booking, 'timeZone', 'timezone') || 'UTC',
      alreadyRejected: deps.alreadyRejected || [],
      resourceId: (binding && binding.coordinator_id) || pick(booking, 'resourceId', 'resource_id') || null,
    });
  } catch (err) {
    console.warn(`[WS-CONVO] slot generation failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
  }
  // Validate + advance the session state machine: rescheduling → proposing.
  const next = transition({ state: fromState }, 'proposing'); // throws if illegal
  if (deps.saveState) {
    await deps.saveState({ tenantId, sessionId, state: 'proposing', candidate_slots: slots });
  }
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({ type: 'scheduling_slots', slots, session_id: sessionId })}\n\n`);
  }
  return { handled: true, executed: false, state: next.state, slots };
}

// ─── runSchedulingTurn — the post-stream entry the BSH handler calls ─────────────────────

/**
 * Post-stream scheduling-turn handler. Mirrors how selectActionsV4 is invoked after the
 * response streams. Resolves the §B10 binding; if absent, returns { handled:false } and the
 * caller proceeds with normal CTA logic (no-regression). If present, runs the §B14 boundary:
 * detect a structured action, validate the transition through stateMachine.transition, and
 * execute the matching calendar op — never on free text.
 *
 * @param {object} params - { responseText, conversationHistory, tenantId, sessionId, config,
 *                            bedrock, write, deps }
 * @returns {Promise<{handled:boolean, executed?:boolean, action?:string, state?:string,
 *                    rejected?:boolean, reason?:string}>}
 */
async function runSchedulingTurn({
  responseText,
  conversationHistory,
  tenantId,
  sessionId,
  config,
  bedrock,
  write,
  deps = {},
} = {}) {
  const logger = deps.logger || console;
  try {
    // [B-2] guard before realResolveBinding (which throws on empty keys) — a misconfigured
    // tenant (no tenant_id) / missing session becomes a clean no-op, not a per-turn CloudWatch error.
    if (!tenantId || !sessionId) return { handled: false };
    const resolveFn = deps.resolveBinding || realResolveBinding;
    const binding = await resolveFn({ tenantId, sessionId, deps });
    if (!binding) return { handled: false };

    const initialState = initStateFromIntent(binding.intent);
    if (!initialState) return { handled: false }; // recovery_intent etc. → B-remainder

    const prior = deps.loadState ? await deps.loadState({ tenantId, sessionId }) : null;
    const state = (prior && prior.state) || initialState;

    const detected = await detectSchedulingAction({
      responseText,
      conversationHistory,
      session: { state },
      binding,
      config,
      bedrock,
    });
    const action = detected.action;

    // Load the booking the binding governs (integrator-wired DDB read). Needed for every
    // execute path; cheap to skip when there's nothing to execute.
    const needsBooking = action === 'confirm_reschedule' || action === 'confirm_cancel';
    const booking = needsBooking && deps.loadBooking
      ? await deps.loadBooking({ tenantId, bookingId: binding.booking_id })
      : (needsBooking ? deps.booking : undefined);

    try {
      // ── CANCEL intent ────────────────────────────────────────────────────────────────
      if (initialState === 'canceling') {
        if (action === 'confirm_cancel') {
          // §B14 [S-1]: gate via the state machine (the canceling→booked "cancel the cancel"
          // edge) — IllegalStateTransition (caught below) rejects a confirm_cancel from any
          // other state, exactly like the reschedule path. No manual state check.
          transition({ state }, 'booked');
          if (!booking) {
            console.warn('[WS-CONVO] booking not loaded (integrator seam) — cancel skipped');
            return { handled: true, executed: false, reason: 'booking_unavailable' };
          }
          const res = await _doCancel({ tenantId, binding, booking, deps, logger });
          // [B-1]: advance the session off 'canceling' so a later turn within the binding TTL
          // can't re-fire _doCancel (booked→booked is illegal → rejected). The §14.2 listener
          // owns the async Booking.status='canceled' flip; this only terminates the SESSION.
          if (res.executed && deps.saveState) {
            await deps.saveState({ tenantId, sessionId, state: 'booked' });
          }
          return { handled: true, action, ...res };
        }
        return { handled: true, executed: false, action }; // await explicit confirmation
      }

      // ── RESCHEDULE intent ─────────────────────────────────────────────────────────────
      if (action === 'confirm_reschedule') {
        // §B14: validate confirming → booked (illegal from any other state → rejected, no op).
        transition({ state }, 'booked');
        if (!booking) {
          console.warn('[WS-CONVO] booking not loaded (integrator seam) — reschedule skipped');
          return { handled: true, executed: false, reason: 'booking_unavailable' };
        }
        const selected = (prior && prior.selected_slot) || deps.selectedSlot;
        if (!selected || !selected.start || !selected.end) {
          return { handled: true, executed: false, rejected: true, reason: 'no_slot_selected' };
        }
        const res = await _doReschedule({
          tenantId, binding, booking,
          newSlot: { start: selected.start, end: selected.end },
          deps, logger,
        });
        if (res.executed && deps.saveState) {
          await deps.saveState({ tenantId, sessionId, state: 'booked' });
        }
        return { handled: true, action, ...res };
      }

      if (action === 'select_slot') {
        // §B14: validate proposing → confirming (illegal from any other state → rejected).
        transition({ state }, 'confirming');
        const candidates = (prior && prior.candidate_slots) || deps.candidateSlots || [];
        const chosen = candidates.find((s) => s.slotId === detected.slotId) || deps.selectedSlot;
        if (!chosen) {
          return { handled: true, executed: false, rejected: true, reason: 'unknown_slot' };
        }
        if (deps.saveState) {
          await deps.saveState({
            tenantId, sessionId, state: 'confirming',
            selected_slot: { slotId: chosen.slotId, start: chosen.start, end: chosen.end },
            candidate_slots: candidates,
          });
        }
        return { handled: true, executed: false, action, state: 'confirming' };
      }

      // action === 'none': on first entry to 'rescheduling', present slots; else no-op
      // (free-text / hesitation → NO execution — the §B14 boundary).
      if (state === 'rescheduling') {
        return await _presentSlots({ tenantId, sessionId, binding, booking, config, deps, write, fromState: state });
      }
      return { handled: true, executed: false, action: 'none', state };
    } catch (err) {
      if (err instanceof IllegalStateTransition) {
        // The advisory action asked for a move the §9.2 machine forbids — reject, no op.
        return { handled: true, executed: false, rejected: true, reason: err.message };
      }
      throw err;
    }
  } catch (err) {
    // Non-fatal: a scheduling failure must not break the chat response (already streamed).
    console.error(`[WS-CONVO] scheduling turn failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: false, error: true };
  }
}

module.exports = {
  runSchedulingTurn,
  detectSchedulingAction,
  // exported for unit tests
  ACTIONS,
  calendarIdOf,
  eventIdOf,
  zoomMeetingIdOf,
  _resolveFacade,
  _doReschedule,
  _doCancel,
};
