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

// §B16e: day-picker helpers (WS-T3-DAYPICK-BE).
const {
  MAX_PICKER_CYCLES,
  buildDayStrip,
  dateWindowForDay,
  emitDayPicker,
  emitPickerEscapeNotice,
} = require('./dayPicker');

// ─── picker_days shape validator (fix #7) ────────────────────────────────────────────
// Validates persisted prior.picker_days before re-emitting it. Returns true only when
// the value is an array of {date: YYYY-MM-DD, label: string <= 40 chars} objects.
// Invalid/corrupt persisted strip → caller rebuilds fresh instead of emitting junk.
const DAY_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function isValidDayStrip(days) {
  if (!Array.isArray(days) || days.length === 0) return false;
  return days.every(
    (d) =>
      d &&
      typeof d === 'object' &&
      typeof d.date === 'string' &&
      DAY_DATE_RE.test(d.date) &&
      typeof d.label === 'string' &&
      d.label.length <= 40
  );
}

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
  logger = console,
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
    logger.error(`[WS-NEWBOOK] action detect failed (fail-closed → none): error_name=${(err && err.name) || 'unknown'}`);
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

// ─── §B16e day-selected signal handler ───────────────────────────────────────────────────

/**
 * Handle the `scheduling_day_selected` deterministic widget signal (§B16e).
 *
 * Re-runs invokeProposal constrained to the picked day (via `date_window`). STATE RULE
 * (§B16e): day selection never commits anything and does NOT advance the state machine
 * past 'qualifying' or 'proposing' on its own — only an 'ok' propose advances per §B16b.
 *
 * On 'ok'  → present slots per §B16b ordering (advance qualifying→proposing or
 *            proposing stays in proposing after the slots SSE + state advance in _propose).
 * On 'no_availability' → re-emit the picker (same 7 days persisted in prior.picker_days),
 *            LLM text says that day had no fit. Counts as another picker cycle.
 * >3 total picker cycles → the §9.3 async escape (scheduling_notice fallback).
 */
async function _handleDaySelected({ tenantId, sessionId, state, prior, qctx, daySelected, deps, write, logger }) {
  // Only valid from the new-booking arc states that show the picker (qualifying / proposing).
  // From confirming/booked a day-selected signal is stale — ignore it (no-op, handled:false
  // so the normal flow continues; the state machine gates any commit anyway).
  if (state !== 'qualifying' && state !== 'proposing') {
    return { handled: false, reason: 'day_selected_wrong_state' };
  }

  // Build the dateWindow for the selected day (UTC midnight boundaries).
  let dateWindow;
  try {
    dateWindow = dateWindowForDay(daySelected);
  } catch (err) {
    (logger || console).warn(`[WS-T3-DAYPICK] invalid day_selected='${daySelected}' — ignored`);
    return { handled: false, reason: 'invalid_day_selected' };
  }

  const appointmentTypeId = ctxAppointmentTypeId(qctx);
  if (!appointmentTypeId) {
    return { handled: true, executed: false, state, reason: 'awaiting_appointment_type' };
  }
  if (!deps.invokeProposal) {
    (logger || console).warn('[WS-T3-DAYPICK] propose seam not wired — day_selected skipped');
    return { handled: true, executed: false, state, reason: 'propose_seam_unwired' };
  }

  const priorRejected = (prior && prior.rejected_slot_ids) || [];
  const priorSlotIds = ((prior && prior.candidate_slots) || []).map((s) => s.slotId).filter(Boolean);
  const alreadyRejected = state === 'proposing'
    ? Array.from(new Set([...priorRejected, ...priorSlotIds]))
    : priorRejected;

  const proposePayload = {
    action: 'scheduling_propose',
    tenantId,
    sessionId,
    appointmentTypeId,
    userTimeZone: ctxUserTimeZone(qctx),
    alreadyRejected,
    // §B16e: constrain to the selected day (BCH → pool.select → generateSlots).
    date_window: { start: dateWindow.startISO, end: dateWindow.endISO },
  };
  // Forward the configured availability window if present (same as _propose does).
  const windowStart = qctx.windowStart ?? qctx.window_start;
  const windowEnd = qctx.windowEnd ?? qctx.window_end;
  if (windowStart != null) proposePayload.windowStart = windowStart;
  if (windowEnd != null) proposePayload.windowEnd = windowEnd;

  let res;
  try {
    res = await deps.invokeProposal(proposePayload);
  } catch (err) {
    (logger || console).error(`[WS-T3-DAYPICK] day-constrained propose failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: true, executed: false, state, reason: 'propose_failed' };
  }

  const outcome = res && res.outcome;
  if (outcome === 'ok') {
    // Slots available on the selected day → present them per §B16b ordering.
    // Advance qualifying→proposing (or stay proposing) in the same saveState.
    const slots = res.slots || [];
    const proposal = { poolSize: res.poolSize };
    if (res.tieBreaker != null) proposal.tieBreaker = res.tieBreaker;
    if (res.roundRobinCursor != null) proposal.roundRobinCursor = res.roundRobinCursor;
    const next = transition({ state }, 'proposing'); // throws if illegal (shouldn't — already gated)
    if (deps.saveState) {
      await deps.saveState({
        tenantId,
        sessionId,
        state: 'proposing',
        candidate_slots: slots,
        proposal,
        rejected_slot_ids: alreadyRejected,
        ...(prior && prior.picker_cycles != null ? { picker_cycles: prior.picker_cycles } : {}),
        ...(prior && prior.picker_days != null ? { picker_days: prior.picker_days } : {}),
      });
    }
    if (typeof write === 'function') {
      write(`data: ${JSON.stringify({ type: 'scheduling_slots', slots, session_id: sessionId })}\n\n`);
    }
    (logger || console).info(
      JSON.stringify({ event: 'day_selected_slots_presented', tenant_id: tenantId, session_id: sessionId, day: daySelected, slot_count: slots.length })
    );
    return { handled: true, executed: false, state: next.state, slots };
  }

  if (outcome === 'failed') {
    // Transient BCH/infra error — do NOT touch state, do NOT emit a picker, do NOT
    // increment picker_cycles (same rule as _propose: outcome:'failed' is not a §B16e trigger).
    (logger || console).warn(`[WS-T3-DAYPICK] day-constrained propose returned outcome='failed' (non-fatal, no picker): tenant=${tenantId}`);
    return { handled: true, executed: false, state, reason: 'propose_failed_outcome' };
  }

  // no_availability for the selected day → re-emit the picker (same 7 days when persisted,
  // else rebuild). Counts as another picker cycle (→ escape when > MAX_PICKER_CYCLES).
  (logger || console).info(
    JSON.stringify({ event: 'day_selected_no_availability', tenant_id: tenantId, session_id: sessionId, day: daySelected })
  );
  // Re-use the persisted strip if available (§B16e: "same 7 days") — but only if it
  // shape-validates (array, each item { date: YYYY-MM-DD, label: string <= 40 chars }).
  // Invalid/corrupt persisted strip → rebuild fresh instead of emitting junk.
  const rawPersistedDays = prior && prior.picker_days;
  const persistedDays = isValidDayStrip(rawPersistedDays) ? rawPersistedDays : null;
  const pickerCycles = (prior && Number.isFinite(prior.picker_cycles) ? prior.picker_cycles : 0) + 1;
  if (pickerCycles > MAX_PICKER_CYCLES) {
    emitPickerEscapeNotice(write, sessionId);
    if (deps.saveState) {
      await deps.saveState({
        tenantId, sessionId, state, // STATE RULE: do NOT advance
        ...(prior ? { candidate_slots: prior.candidate_slots, proposal: prior.proposal, rejected_slot_ids: prior.rejected_slot_ids } : {}),
        picker_cycles: pickerCycles,
      });
    }
    (logger || console).warn(`[WS-T3-DAYPICK] picker escape on re-emit: tenant=${tenantId} cycles=${pickerCycles}`);
    return { handled: true, executed: false, state, reason: 'picker_escape', pickerCycles };
  }
  const userTimeZone = ctxUserTimeZone(qctx);
  const days = persistedDays || buildDayStrip({ userTimeZone });
  emitDayPicker(write, sessionId, days, userTimeZone);
  if (deps.saveState) {
    await deps.saveState({
      tenantId, sessionId, state, // STATE RULE: do NOT advance
      ...(prior ? { candidate_slots: prior.candidate_slots, proposal: prior.proposal, rejected_slot_ids: prior.rejected_slot_ids } : {}),
      picker_cycles: pickerCycles,
      picker_days: days,
    });
  }
  return { handled: true, executed: false, state, reason: 'day_picker_reemit', pickerCycles, days };
}

// ─── §B16e day-picker emit (trigger a + b) + cycle escape ────────────────────────────────

/**
 * Emit the §B16e `scheduling_day_picker` SSE message, or, if the cycle count exceeds
 * MAX_PICKER_CYCLES, emit the §9.3 `scheduling_notice` async escape instead.
 *
 * STATE RULE (§B16e strand-prevention): stay in `fromState` — do NOT advance. The caller
 * (trigger a = _propose on no_availability; trigger b = the proposing self-loop >= 2)
 * must NOT advance the state machine after this returns.
 *
 * Cycle count rides in `prior.picker_cycles` (schema-discipline: absent → 0).
 */
async function _emitPickerOrEscape({ tenantId, sessionId, fromState, prior, qctx, write, deps, logger }) {
  const pickerCycles = (prior && Number.isFinite(prior.picker_cycles) ? prior.picker_cycles : 0) + 1;
  if (pickerCycles > MAX_PICKER_CYCLES) {
    // §9.3 async escape: too many picker cycles → stop trying, notify by email.
    emitPickerEscapeNotice(write, sessionId);
    if (deps.saveState) {
      await deps.saveState({
        tenantId,
        sessionId,
        state: fromState, // STATE RULE: do NOT advance
        // carry forward existing fields (schema-discipline); update cycle count
        ...(prior ? { candidate_slots: prior.candidate_slots, proposal: prior.proposal, rejected_slot_ids: prior.rejected_slot_ids } : {}),
        picker_cycles: pickerCycles,
      });
    }
    (logger || console).warn(`[WS-T3-DAYPICK] picker escape fired: tenant=${tenantId} session=${sessionId} cycles=${pickerCycles}`);
    return { handled: true, executed: false, state: fromState, reason: 'picker_escape', pickerCycles };
  }

  const userTimeZone = ctxUserTimeZone(qctx);
  // §8: clamp maxAdvanceDays — negative/zero/fractional/NaN config must not produce an
  // empty strip. Math.max(1, ...) ensures at least one day is always available.
  const maxAdvanceDays = Math.max(
    1,
    Number(
      (qctx.appointmentType && qctx.appointmentType.max_advance_days) ||
      (qctx.appointment_type && qctx.appointment_type.max_advance_days)
    ) || 60
  );
  const days = buildDayStrip({ userTimeZone, maxAdvanceDays });
  emitDayPicker(write, sessionId, days, userTimeZone);

  if (deps.saveState) {
    await deps.saveState({
      tenantId,
      sessionId,
      state: fromState, // STATE RULE: do NOT advance
      // carry forward existing candidate + proposal fields so the session is not
      // stripped on a picker turn (schema-discipline: absent fields → missing, OK)
      ...(prior ? { candidate_slots: prior.candidate_slots, proposal: prior.proposal, rejected_slot_ids: prior.rejected_slot_ids } : {}),
      picker_cycles: pickerCycles,
      // Persist the current 7-day strip so the re-emit on a no_availability day can
      // reproduce the SAME strip (§B16e: "re-emit the picker (same 7 days)").
      picker_days: days,
    });
  }
  (logger || console).info(
    JSON.stringify({ event: 'scheduling_day_picker_emitted', tenant_id: tenantId, session_id: sessionId, picker_cycles: pickerCycles, state: fromState })
  );
  return { handled: true, executed: false, state: fromState, reason: 'day_picker', pickerCycles, days };
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

  const proposePayload = {
    action: 'scheduling_propose',
    tenantId,
    sessionId,
    appointmentTypeId,
    userTimeZone: ctxUserTimeZone(qctx),
    alreadyRejected,
  };
  // §B16a optional availability window — forward the tenant's configured window when present
  // (schema-discipline: tolerate camel OR snake) so propose's pool.select doesn't silently
  // fall back to its default window. Omit when absent (don't send explicit undefined).
  const windowStart = qctx.windowStart ?? qctx.window_start;
  const windowEnd = qctx.windowEnd ?? qctx.window_end;
  if (windowStart != null) proposePayload.windowStart = windowStart;
  if (windowEnd != null) proposePayload.windowEnd = windowEnd;

  let res;
  try {
    res = await deps.invokeProposal(proposePayload);
  } catch (err) {
    (logger || console).error(`[WS-NEWBOOK] propose invoke failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    return { handled: true, executed: false, state: fromState, reason: 'propose_failed' };
  }

  const outcome = res && res.outcome;
  if (outcome === 'failed') {
    // Transient BCH/infra error — do NOT touch state, do NOT emit a picker, do NOT
    // increment picker_cycles. The graceful-error path is a non-fatal no-op: the
    // chat turn already streamed; BSH surfaces its own "we'll follow up" handling.
    // Pre-PR behavior restored: outcome:'failed' is NOT a §B16e trigger.
    (logger || console).warn(`[WS-NEWBOOK] propose returned outcome='failed' (non-fatal, no picker): tenant=${tenantId}`);
    return { handled: true, executed: false, state: fromState, reason: 'propose_failed_outcome' };
  }
  if (outcome !== 'ok') {
    // §B16e TRIGGER (a): invokeProposal returns 'no_availability' → emit the day-picker
    // so the volunteer can try a different day. STATE RULE (strand-prevention): stay in
    // fromState — do NOT advance. The scheduling_no_availability message is REPLACED by
    // the picker emit (picker is the UX surface; the LLM text will say no slots today).
    return await _emitPickerOrEscape({
      tenantId, sessionId, fromState, prior, qctx, write, deps, logger,
    });
  }

  const slots = res.slots || [];
  // §B16c: pool_size at commit = this TOP-LEVEL poolSize; persist it (+ the round-robin
  // carry-forwards) so the later confirming→booked turn can forward them. Omit the optional
  // carry-forwards when absent (don't persist explicit undefined).
  const proposal = { poolSize: res.poolSize };
  if (res.tieBreaker != null) proposal.tieBreaker = res.tieBreaker;
  if (res.roundRobinCursor != null) proposal.roundRobinCursor = res.roundRobinCursor;
  // Validate + advance the state machine: qualifying→proposing OR proposing→proposing (both legal).
  const next = transition({ state: fromState }, 'proposing'); // throws if illegal
  if (deps.saveState) {
    await deps.saveState({
      tenantId,
      sessionId,
      state: 'proposing',
      candidate_slots: slots,
      proposal,
      rejected_slot_ids: alreadyRejected,
      // Carry picker_cycles + proposing_none_count forward (schema-discipline: absent → 0).
      ...(prior && prior.picker_cycles != null ? { picker_cycles: prior.picker_cycles } : {}),
      ...(prior && prior.proposing_none_count != null ? { proposing_none_count: prior.proposing_none_count } : {}),
    });
  }
  if (typeof write === 'function') {
    // §B18b: forward context when the propose result carries it (ADDITIVE; omit when absent —
    // old-shape tolerant per CLAUDE.md schema discipline).
    const sseEvent = { type: 'scheduling_slots', slots, session_id: sessionId };
    if (res.context != null) sseEvent.context = res.context;
    write(`data: ${JSON.stringify(sseEvent)}\n\n`);
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
  // NOT slot.candidateResourceIds.length — BCH's §5.5 solo-vs-pool branch depends on it.
  // If it wasn't persisted, the only honest cause is an integrator mis-wire (a saveState that
  // dropped the `proposal` field). Fail LOUD rather than fall back to candidateResourceIds.length,
  // which would silently mis-flag the booking. (Caller does NOT advance to booked on this.)
  const poolSize = proposal.poolSize;
  if (poolSize == null) {
    (logger || console).error('[WS-NEWBOOK] commit aborted: missing pool_size (propose metadata not persisted — integrator saveState mis-wire)');
    return { executed: false, outcome: 'failed', error: 'missing_pool_size' };
  }

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

    // §B16e: check for the DETERMINISTIC `scheduling_day_selected` widget signal BEFORE
    // the LLM detector. The signal arrives in the request body (same seam as
    // `scheduling_intent`); the integrator surfaces it via `deps.schedulingDaySelected`.
    // This is NOT an LLM action — the §B14 boundary is UNAFFECTED (day selection never
    // commits anything). Schema-discipline: tolerate absent (normal non-picker turns).
    const daySelected = deps.schedulingDaySelected;
    if (daySelected && /^\d{4}-\d{2}-\d{2}$/.test(daySelected)) {
      // §B16e strip validation: the selected day MUST be present in the persisted
      // `prior.picker_days` strip. A date not offered (past, stale, never-emitted,
      // or from a different session) is silently ignored — fall through to normal chat
      // (no propose, no cycle burn, no SSE). Shape-validation of picker_days is in
      // _handleDaySelected (fix #7); absent picker_days → no offered set → ignore.
      const offeredDays = prior && Array.isArray(prior.picker_days) ? prior.picker_days : null;
      if (offeredDays && !offeredDays.some((d) => d && d.date === daySelected)) {
        (logger || console).warn(`[WS-T3-DAYPICK] scheduling_day_selected='${daySelected}' not in offered strip — ignored`);
        // Fall through: return handled:false so the normal turn continues.
        return { handled: false, reason: 'day_not_offered' };
      }
      // When no strip has been persisted yet (first-turn day-selected with no prior
      // picker_days) we have no offered set to validate against — accept the signal
      // (the picker was just emitted this turn and the state store may not yet have it).
      return await _handleDaySelected({
        tenantId, sessionId, state, prior, qctx, daySelected, deps, write, logger,
      });
    }

    // §B16b amendment (deterministic pipeline): the widget's slot-chip / confirm-button
    // clicks carry scheduling_action (+ scheduling_slot_id for select) in routing_metadata —
    // a deterministic source consumed BEFORE the LLM detector, exactly like the §B16e
    // day-selected signal above. The detector remains the fallback for typed text only.
    // The §B14 boundary is UNCHANGED either way: every action below still gates through
    // stateMachine.transition + persisted-state validation (a spoofed signal with a slotId
    // not in persisted candidate_slots is rejected as unknown_slot; confirm_book from a
    // non-confirming state throws IllegalStateTransition).
    let detected;
    if (deps.schedulingAction === 'select_slot' || deps.schedulingAction === 'confirm_book') {
      detected = { action: deps.schedulingAction };
      if (deps.schedulingAction === 'select_slot' && deps.schedulingSlotId != null) {
        detected.slotId = String(deps.schedulingSlotId);
      }
      logger.info && logger.info(`[WS-NEWBOOK] deterministic widget action: ${detected.action}`);
    } else {
      // The detector output is ADVISORY only — the C9 state machine (transition() below), NOT
      // this prompt, is the primary defense against a fabricated/injected action: every action
      // is gated through stateMachine.transition, so a spoofed `confirm_book` from the wrong
      // state is rejected regardless of what the detector returns.
      detected = await detectNewBookingAction({
        responseText,
        conversationHistory,
        session: { state },
        config,
        bedrock,
        logger,
      });
    }
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
        // Security (§B14): the slot MUST come from PERSISTED state — never an injected seam.
        // An LLM-fabricated slot that bypassed the select_slot unknown_slot guard must not
        // reach commit. No `deps.selectedSlot` fallback.
        const selected = prior && prior.selected_slot;
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
        // Security (§B14): the chosen slot MUST be one we PERSISTED as a candidate — never an
        // injected seam. An LLM-fabricated slotId that matches no persisted candidate is
        // rejected (unknown_slot). No `deps.candidateSlots` / `deps.selectedSlot` fallback.
        const candidates = (prior && prior.candidate_slots) || [];
        const chosen = candidates.find((s) => s.slotId === detected.slotId);
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
            // §B16d amendment: a previously chat-captured email survives re-selection.
            attendee_email: prior && prior.attendee_email,
          });
        }
        // Deterministic pipeline: the confirm affordance is SERVER-driven. With identity
        // already resolved, arm it now via `scheduling_confirm`; otherwise the caller asks
        // for the email and captureAttendeeEmail re-arms it on the capture turn.
        const attendeeEmail = ctxAttendeeEmail(qctx);
        if (attendeeEmail && typeof write === 'function') {
          write(`data: ${JSON.stringify({
            type: 'scheduling_confirm',
            session_id: sessionId,
            slot: { slotId: chosen.slotId, label: chosen.label },
            attendee_email: attendeeEmail,
          })}\n\n`);
        }
        return { handled: true, executed: false, action, state: 'confirming', identity: !!attendeeEmail, selected_label: chosen.label };
      }

      // ── action === 'none' ──
      //   qualifying  → present slots (propose). proposing → "more times" self-loop (re-propose
      //   w/ alreadyRejected). confirming / booked → no-op (free text / hesitation → NO commit).
      if (state === 'qualifying' || state === 'proposing') {
        // §B16e TRIGGER (b): the 'proposing' none-self-loop has re-proposed >= 2 times and
        // still no pick → emit the day-picker. The count rides in `prior.proposing_none_count`
        // (schema-discipline: absent → 0). On the qualifying entry this is the first propose,
        // so only 'proposing' self-loops count toward the trigger.
        if (state === 'proposing') {
          const noneCount = (prior && Number.isFinite(prior.proposing_none_count) ? prior.proposing_none_count : 0) + 1;
          if (noneCount >= 2) {
            // Trigger b fires — hand off to the picker (which tracks its own cycle count).
            return await _emitPickerOrEscape({
              tenantId, sessionId, fromState: state, prior, qctx, write, deps, logger,
            });
          }
          // Not yet at the threshold — re-propose but persist the incremented count so the
          // next none-loop iteration can check it.
          const priorForRepropose = { ...prior, proposing_none_count: noneCount };
          const result = await _propose({
            tenantId, sessionId, fromState: state, prior: priorForRepropose, qctx, config, deps, write, logger,
          });
          // _propose's saveState already runs inside; augment the persisted count if it saved OK.
          // The count is only needed BEFORE the picker fires, so this is sufficient.
          return result;
        }
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
  // §B16e exports (WS-T3-DAYPICK-BE)
  _emitPickerOrEscape,
  _handleDaySelected,
};
