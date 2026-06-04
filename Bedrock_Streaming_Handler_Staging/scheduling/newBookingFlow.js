'use strict';

/**
 * WS-NEWBOOK-FLOW — in-chat NEW-booking flow (qualifying → proposing → confirming → booked)
 * + the §B14 action BOUNDARY (wave keystone, B-remainder).
 *
 * This is the new-booking TWIN of the shipped `schedulingFlow.js` (reschedule/cancel). Same
 * boundary, same DI/fallback patterns, same SSE-emit shape — only the arc and the action
 * vocabulary differ. Read `schedulingFlow.js` end-to-end before this; the divergences are
 * called out inline.
 *
 * Canonical: scheduling_design.md §9.2 (state machine), §9.3 (slots), §10.4 (coordinator
 * revealed at confirm). FROZEN_CONTRACTS.md §B14 (THE BOUNDARY — LOCKED), §B16a (the
 * `scheduling_propose` route we invoke), §B16b (our action vocab + the boundary), §B16c
 * (the C8 commit route we invoke), §B16d (the integrator-owned bootstrap — NOT ours).
 *
 * ── THE BOUNDARY (§B14 / §B16b, the load-bearing rule) ──
 *   The C9 stateMachine is AUTHORITATIVE; the streaming LLM is ADVISORY. A commit (the
 *   §B16c BCH invoke) runs ONLY on a discrete STRUCTURED action produced by a FOCUSED
 *   post-stream call (mirrors the V4.0 Action Selector — BSH has no native tool-use). Free
 *   text the streaming LLM emits ("you're all booked!" in prose) NEVER commits. Every
 *   transition is validated through `stateMachine.transition`; an unparseable / unknown
 *   detector output is treated as 'none' (fail-closed → no commit).
 *
 * ── DI seam / packaging (§B16a/c architecture) ──
 *   `availability.js` (C4) + `pool.js` (C6) pull `googleapis` / `google-auth-library`, which
 *   BSH cannot bundle — so BOTH the `proposing` slot-gen and the `booked` commit run in
 *   `Booking_Commit_Handler` and are reached from BSH by Lambda invoke. The INTEGRATOR wires
 *   `deps.invokeProposal` (→ BCH `scheduling_propose`) + `deps.invokeBookingCommit` (→ BCH
 *   default commit route) + the state I/O (`deps.loadState` / `deps.saveState`) + the
 *   resolved `deps.qualifyingContext` (appt-type / routing / identity, re-supplied each turn
 *   per §B16d). Only the esbuild-safe pieces (stateMachine, the detector) get real defaults,
 *   so unit tests run the shipped logic and prod imports the same code. When the invoke seam
 *   is absent, execution is SKIPPED non-fatally (detection + transitions still run). With NO
 *   new-booking session (`deps.loadState` → null) the whole flow is a no-op (no-regression).
 *
 * ── what this module does NOT do ──
 *   It writes NO Booking rows, calls NO calendar/Zoom API, imports NO `googleapis` — every
 *   calendar-bound op is delegated to BCH via the invoke seams. It owns the CONVERSATION; BCH
 *   owns everything calendar-bound. It persists NO PII (identity lives only as injected
 *   `deps.qualifyingContext`, forwarded to the commit invoke, never stored here).
 */

const {
  transition,
  IllegalStateTransition,
} = require('../../shared/scheduling/stateMachine');

const { isSchedulingEnabled } = require('./bindingContext');

// The three §B16b structured actions. Anything else (incl. unparseable output) → 'none'.
const ACTIONS = Object.freeze(['select_slot', 'confirm_book', 'none']);

// The session states THIS flow drives (the new-booking arc). A loaded session in any other
// state — `rescheduling` / `canceling` (the recovery loop's, §B9–§B15) / `pending_attendance`
// etc. — is NOT ours: return { handled:false } so the integrator's dispatch (or normal chat)
// is untouched. `booked` is included so the post-commit double-fire guard runs through the
// state machine (booked → booked is illegal → rejected), mirroring the recovery loop's SR-2.
const NEW_BOOKING_STATES = Object.freeze(['qualifying', 'proposing', 'confirming', 'booked']);

// Tech-lead Tier-2 note (mirror schedulingFlow): on commit error / outcome 'failed', surface
// a clear "we'll confirm by email" notice rather than a silent no-op. Emitted via the SSE
// write; the widget rendering is WS-C12 — the event is on the wire now.
function _emitFallbackNotice(write, sessionId) {
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({ type: 'scheduling_notice', notice: 'request_received_email_followup', session_id: sessionId })}\n\n`);
  }
}

// §10.4: the coordinator is GENERIC on the chips and revealed only at confirm — emit the
// assigned resourceId (the commit's returned coordinator) on the booked turn.
function _emitBookingConfirmed(write, sessionId, res) {
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({
      type: 'scheduling_booked',
      session_id: sessionId,
      booking_id: res.bookingId,
      resource_id: res.resourceId,
    })}\n\n`);
  }
}

// ─── §B14 structured action detector (focused post-stream call; mirrors selectActionsV4) ─

/**
 * Decide, from the just-streamed turn, whether the user took a discrete new-booking action.
 * Returns one of the three §B16b actions. Fail-closed: any error / unparseable / unknown
 * output → { action: 'none' } so a transient LLM hiccup can NEVER trigger a commit.
 *
 * @param {object} params
 * @param {string} params.responseText       - the assistant's just-streamed response
 * @param {Array}  params.conversationHistory
 * @param {object} params.session            - { state } (advisory context for the model)
 * @param {object} params.config             - tenant config (model_id)
 * @param {object} params.bedrock            - BedrockRuntimeClient (injected)
 * @returns {Promise<{action:string, slotId?:string}>}
 */
async function detectNewBookingAction({
  responseText,
  conversationHistory,
  session,
  config,
  bedrock,
} = {}) {
  try {
    if (!bedrock || typeof bedrock.send !== 'function') {
      return { action: 'none' };
    }
    const recent = (conversationHistory || []).slice(-6);
    const conversationBlock = recent
      .map((m) => `${m.role === 'user' ? 'User' : 'Assistant'}: ${(m.content || m.text || '').trim()}`)
      .filter(Boolean)
      .join('\n');

    const prompt = `You detect whether the user just took a discrete SCHEDULING action while booking a NEW appointment. The session state is "${session && session.state}".

CONVERSATION:
${conversationBlock}
Assistant: ${responseText}

Decide the SINGLE action the user has explicitly taken RIGHT NOW:
- "select_slot": the user picked a specific offered time slot. Include its slotId.
- "confirm_book": the user explicitly confirmed booking the selected time.
- "none": anything else — a question, hesitation, asking for "more times", general chat, or the assistant merely SAYING it's done. Default to "none" unless the user clearly and explicitly committed.

Return ONLY raw JSON: {"action":"select_slot|confirm_book|none","slotId":"<id or omit>"}. No markdown, no prose.`;

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
    const out = { action };
    if (action === 'select_slot' && parsed.slotId != null) {
      out.slotId = String(parsed.slotId);
    }
    return out;
  } catch (err) {
    console.error(`[WS-NEWBOOK] action detect failed (fail-closed → none): error_name=${(err && err.name) || 'unknown'}`);
    return { action: 'none' };
  }
}

// ─── qualifyingContext field reads (schema discipline — tolerate camel OR snake) ─────────

function ctxAppointmentTypeId(qctx) {
  return qctx.appointmentTypeId != null ? qctx.appointmentTypeId : qctx.appointment_type_id;
}
function ctxUserTimeZone(qctx) {
  return qctx.userTimeZone || qctx.user_time_zone || 'UTC';
}
function ctxAttendeeEmail(qctx) {
  const a = qctx.attendee || {};
  return a.email;
}

// ─── propose (qualifying → proposing entry + the proposing 'none' self-loop) ─────────────

/**
 * Invoke §B16a `scheduling_propose` (delegated to BCH) and, on `outcome:'ok'`, advance to
 * 'proposing' in the SAME saveState that persists the slots + the propose metadata. The
 * STRAND-PREVENTION rule (§B16b): advance ONLY after `outcome:'ok'`; on 'no_availability'
 * STAY in `fromState`. The 'none' self-loop ACCUMULATES presented slotIds → `alreadyRejected`
 * so a re-propose returns FRESH times.
 */
async function _propose({ tenantId, sessionId, fromState, prior, qctx, config, deps, write, logger }) {
  const appointmentTypeId = ctxAppointmentTypeId(qctx);
  // Multi-appt-type / not-yet-resolved (§B16d): qualifying ASKS which type. Without a type we
  // cannot route — STAY in qualifying; the LLM collects it (this turn was free-text chat).
  if (!appointmentTypeId) {
    return { handled: true, executed: false, state: fromState, reason: 'awaiting_appointment_type' };
  }
  // Invoke seam unwired → SKIP non-fatally (detection already ran). Stay in fromState.
  if (!deps.invokeProposal) {
    (logger || console).warn('[WS-NEWBOOK] propose seam (deps.invokeProposal) not wired — proposal skipped');
    return { handled: true, executed: false, state: fromState, reason: 'propose_seam_unwired' };
  }

  // ALREADY-REJECTED ACCUMULATION (§B16b): on the proposing self-loop, exclude both the
  // previously-accumulated rejects AND the slots just presented; on the qualifying entry
  // nothing has been presented yet.
  const priorRejected = (prior && prior.rejected_slot_ids) || [];
  const priorSlotIds = ((prior && prior.candidate_slots) || []).map((s) => s.slotId).filter(Boolean);
  const alreadyRejected = fromState === 'proposing'
    ? Array.from(new Set([...priorRejected, ...priorSlotIds]))
    : priorRejected;

  let res;
  try {
    res = await deps.invokeProposal({
      action: 'scheduling_propose',
      tenantId,
      sessionId,
      appointmentTypeId,
      userTimeZone: ctxUserTimeZone(qctx),
      alreadyRejected,
    });
  } catch (err) {
    (logger || console).error(`[WS-NEWBOOK] propose invoke failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: true, executed: false, state: fromState, reason: 'propose_failed' };
  }

  const outcome = res && res.outcome;
  if (outcome !== 'ok') {
    // no_availability / failed → do NOT advance (strand-prevention). Offer to widen / retype.
    if (typeof write === 'function') {
      write(`data: ${JSON.stringify({ type: 'scheduling_no_availability', reason: outcome || 'failed', session_id: sessionId })}\n\n`);
    }
    return { handled: true, executed: false, state: fromState, reason: outcome || 'no_availability' };
  }

  const slots = res.slots || [];
  // Validate + advance the state machine: qualifying→proposing OR proposing→proposing (both legal).
  const next = transition({ state: fromState }, 'proposing'); // throws if illegal
  if (deps.saveState) {
    await deps.saveState({
      tenantId,
      sessionId,
      state: 'proposing',
      candidate_slots: slots,
      // §B16c: pool_size at commit = this TOP-LEVEL poolSize; persist it (+ the round-robin
      // carry-forwards) so the later confirming→booked turn can forward them.
      proposal: { poolSize: res.poolSize, tieBreaker: res.tieBreaker, roundRobinCursor: res.roundRobinCursor },
      rejected_slot_ids: alreadyRejected,
    });
  }
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({ type: 'scheduling_slots', slots, session_id: sessionId })}\n\n`);
  }
  return { handled: true, executed: false, state: next.state, slots };
}

// ─── commit (confirming → booked; the §B16c BCH invoke — only reached past the §B14 boundary) ─

/**
 * Delegate the booking commit to the EXISTING C8 commit route (BCH default action) via
 * `deps.invokeBookingCommit`. The boundary stays in `runNewBookingTurn` — it validated
 * confirming→booked BEFORE this is reached; BCH is a pure executor (do NOT re-run the state
 * machine there, §B16c). On error / COMMIT_FAILED we DON'T claim success: the caller surfaces
 * the "we'll confirm by email" fallback rather than a silent no-op.
 */
async function _doCommit({ tenantId, sessionId, prior, selected, qctx, deps, logger }) {
  if (!deps.invokeBookingCommit) {
    (logger || console).warn('[WS-NEWBOOK] commit seam (deps.invokeBookingCommit) not wired — commit skipped');
    return { executed: false, reason: 'commit_seam_unwired' };
  }
  const proposal = (prior && prior.proposal) || {};
  // §B16c: pool_size is the propose response's TOP-LEVEL poolSize (the ROUTING pool size),
  // NOT slot.candidateResourceIds.length. The length is only a defensive floor if the propose
  // metadata wasn't persisted (commit requires pool_size ≥ 1).
  const poolSize = proposal.poolSize != null
    ? proposal.poolSize
    : (Array.isArray(selected.candidateResourceIds) ? selected.candidateResourceIds.length : 1);

  const payload = {
    tenant_id: tenantId,
    session_id: sessionId,
    slot: {
      start: selected.start,
      end: selected.end,
      candidateResourceIds: selected.candidateResourceIds || [],
    },
    attendee: qctx.attendee, // identity injected by the integrator (§B5 form-injection or chat)
    conference_type: qctx.conference_type || qctx.conferenceType || 'null',
    pool_size: poolSize,
    appointment_type: qctx.appointment_type || qctx.appointmentType,
  };
  // Optional carry-forwards (forward only when present — commit reads tolerantly).
  if (qctx.coordinator_emails) payload.coordinator_emails = qctx.coordinator_emails;
  if (qctx.coordinator_name) payload.coordinator_name = qctx.coordinator_name;
  if (qctx.org_name) payload.org_name = qctx.org_name;
  if (qctx.deep_link_base) payload.deep_link_base = qctx.deep_link_base;
  const utz = qctx.userTimeZone || qctx.user_time_zone;
  if (utz) payload.user_time_zone = utz;
  if (proposal.tieBreaker != null) payload.tie_breaker = proposal.tieBreaker;
  if (proposal.roundRobinCursor != null) payload.round_robin_cursor = proposal.roundRobinCursor;

  let res;
  try {
    res = await deps.invokeBookingCommit(payload);
  } catch (err) {
    (logger || console).error(`[WS-NEWBOOK] commit invoke failed (fallback→email): error_name=${(err && err.name) || 'unknown'}`);
    return { executed: false, outcome: 'failed', fallback: 'email' };
  }

  const status = res && res.status;
  if (status === 'BOOKED') {
    return { executed: true, outcome: 'booked', bookingId: res.bookingId, resourceId: res.resourceId, booking: res.booking };
  }
  if (status === 'ALREADY_CONFIRMED') {
    // C11 idempotent re-confirm — treat as a success (the booking exists).
    return { executed: true, outcome: 'already_confirmed', bookingId: res.bookingId, booking: res.booking };
  }
  if (status === 'SLOT_UNAVAILABLE') {
    // Lost the race → the FLOW re-proposes (caller returns to 'proposing'). NOT a commit.
    return { executed: false, outcome: 'slot_unavailable', reason: res && res.reason };
  }
  if (status === 'SCHEDULING_DISABLED') {
    // Defense-in-depth (BSH gates first); the feature was disabled between gate and commit.
    // Nothing happened — reject without an email follow-up (the feature is off).
    return { executed: false, outcome: 'disabled', rejected: true, reason: res && res.reason };
  }
  // COMMIT_FAILED / unknown → graceful "confirm by email" fallback (never a silent no-op).
  return { executed: false, outcome: 'failed', fallback: 'email', reason: res && res.reason };
}

// ─── runNewBookingTurn — the post-stream entry the BSH handler calls (mirrors runSchedulingTurn) ─

/**
 * Post-stream new-booking-turn handler. Mirrors how `runSchedulingTurn` / `selectActionsV4`
 * are invoked after the response streams. Gates on the feature flag + an active new-booking
 * session (`deps.loadState`); with neither, returns { handled:false } and the caller proceeds
 * with normal chat (no-regression). With a session, runs the §B14 boundary: detect a
 * structured action, validate the transition through `stateMachine.transition`, and commit
 * ONLY on `confirm_book` from `confirming` — never on free text.
 *
 * @param {object} params - { responseText, conversationHistory, tenantId, sessionId, config,
 *                            bedrock, write, deps }
 * @returns {Promise<{handled:boolean, executed?:boolean, action?:string, state?:string,
 *                    rejected?:boolean, reason?:string}>}
 */
async function runNewBookingTurn({
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
    if (!tenantId || !sessionId) return { handled: false };
    // Feature-gated (§B16d): dormant unless the tenant explicitly enabled scheduling.
    if (!isSchedulingEnabled(config)) return { handled: false };

    const prior = deps.loadState ? await deps.loadState({ tenantId, sessionId }) : null;
    const state = prior && prior.state;
    // No new-booking session (or a session NOT in our arc — e.g. the recovery loop's
    // rescheduling/canceling) → not ours. Normal chat is untouched (no-regression).
    if (!NEW_BOOKING_STATES.includes(state)) return { handled: false };

    const qctx = deps.qualifyingContext || {};

    const detected = await detectNewBookingAction({
      responseText,
      conversationHistory,
      session: { state },
      config,
      bedrock,
    });
    const action = detected.action;

    try {
      // ── confirm_book → commit (legal ONLY from 'confirming'; §B16c invoked HERE only) ──
      if (action === 'confirm_book') {
        // §B14: validate confirming → booked. From qualifying/proposing/booked this throws
        // IllegalStateTransition (caught below) → rejected, NO commit (double-fire guard +
        // the "free text / wrong state never commits" rule).
        transition({ state }, 'booked');

        // Identity gate (§B16d): the §B16c commit REQUIRES attendee.email. If identity isn't
        // resolved yet, do NOT commit and do NOT advance — stay in 'confirming' so the LLM can
        // collect it and the user can retry.
        if (!ctxAttendeeEmail(qctx)) {
          return { handled: true, executed: false, rejected: true, reason: 'identity_required', state };
        }
        const selected = (prior && prior.selected_slot) || deps.selectedSlot;
        if (!selected || !selected.start || !selected.end) {
          return { handled: true, executed: false, rejected: true, reason: 'no_slot_selected', state };
        }

        const res = await _doCommit({ tenantId, sessionId, prior, selected, qctx, deps, logger });

        // SLOT_UNAVAILABLE → return to 'proposing' (re-offer) — do NOT advance to booked.
        if (res.outcome === 'slot_unavailable') {
          if (deps.saveState) {
            await deps.saveState({
              tenantId,
              sessionId,
              state: 'proposing',
              candidate_slots: (prior && prior.candidate_slots) || [],
              proposal: prior && prior.proposal,
              rejected_slot_ids: prior && prior.rejected_slot_ids,
            });
          }
          return { handled: true, executed: false, action, state: 'proposing', reason: 'slot_unavailable' };
        }

        // DOUBLE-FIRE GUARD (§B16b, mirrors recovery loop SR-2): advance to 'booked' on commit
        // SUCCESS *or* the email fallback so a later turn cannot re-fire commit (booked→booked
        // is illegal → rejected). The seam-unwired / disabled / identity paths do NOT advance.
        if ((res.executed || res.fallback === 'email') && deps.saveState) {
          await deps.saveState({ tenantId, sessionId, state: 'booked' });
        }
        if (res.fallback === 'email') _emitFallbackNotice(write, sessionId);
        if (res.executed) _emitBookingConfirmed(write, sessionId, res); // §10.4 reveal coordinator

        const nextState = (res.executed || res.fallback === 'email') ? 'booked' : state;
        return { handled: true, action, ...res, state: nextState };
      }

      // ── select_slot → proposing → confirming (no calendar op) ──
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
            tenantId,
            sessionId,
            state: 'confirming',
            selected_slot: { slotId: chosen.slotId, start: chosen.start, end: chosen.end, candidateResourceIds: chosen.candidateResourceIds },
            candidate_slots: candidates,
            // carry the propose metadata forward so the commit turn can read it.
            proposal: prior && prior.proposal,
            rejected_slot_ids: prior && prior.rejected_slot_ids,
          });
        }
        return { handled: true, executed: false, action, state: 'confirming' };
      }

      // ── action === 'none' ──
      //   qualifying  → present slots (propose). proposing → "more times" self-loop (re-propose
      //   w/ alreadyRejected). confirming / booked → no-op (free text / hesitation → NO commit).
      if (state === 'qualifying' || state === 'proposing') {
        return await _propose({ tenantId, sessionId, fromState: state, prior, qctx, config, deps, write, logger });
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
    logger.error(`[WS-NEWBOOK] new-booking turn failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: false, error: true };
  }
}

module.exports = {
  runNewBookingTurn,
  detectNewBookingAction,
  // exported for unit tests
  ACTIONS,
  NEW_BOOKING_STATES,
  _propose,
  _doCommit,
};
