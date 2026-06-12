'use strict';

/**
 * agentTools.js — the two §B17c tool executors (WS-AG-CORE; Phase-0 catalog v1).
 *
 * Canonical: FROZEN_CONTRACTS.md §B17c (tool schemas — LOCKED, incl. the 2026-06-12
 * governance amendments: `starts_at_iso` + authority note + the attendee_email
 * verbatim-match anti-hallucination guard). Human-readable catalog:
 * `Bedrock_Streaming_Handler_Staging/scheduling/TOOL_CATALOG.md`.
 *
 * EXACTLY TWO executors (work-order WS-AG-CORE item 2):
 *   executeGetAvailableTimes        — wraps the SHIPPED §B16a `scheduling_propose`
 *                                     BCH seam via `deps.invokeProposal` (NEVER
 *                                     re-implements proposal logic).
 *   executeRequestBookingConfirmation — validates + STAGES via the SAME
 *                                     `deps.saveState` path the shipped deterministic
 *                                     pipeline uses (one staging implementation, two
 *                                     callers). NEVER books: there is no reference to
 *                                     the §B16c commit seam anywhere in this module
 *                                     (jest statically asserts that).
 *
 * ── Executor calling convention (consumed by agentTurn.js + WS-AG-EVAL) ──
 *   Each executor returns the MODEL-FACING result object exactly as pinned in §B17c
 *   (e.g. `{ slots, user_time_zone, note }` / `{ staged: true, label }` /
 *   `{ error: ... }`). Side-effects (saveState + UI SSE emit) happen inside. Because
 *   a later tool call in the SAME turn must see this call's persisted session fields,
 *   executors report the updated row via the optional `setSession(updatedRow)`
 *   callback — the caller (agentTurn) threads it as its live session view.
 *
 * ── Server authority (§B17c) ──
 *   tenantId / sessionId / appointmentTypeId / userTimeZone come from server
 *   context/config — NEVER from model args. The model supplies only: date,
 *   exclude_slot_ids, slot_id, attendee_email, attendee_name. slot_id is validated
 *   against SERVER-persisted candidates; attendee_email against EMAIL_SHAPE + the
 *   verbatim-match guard. Model-supplied timestamps are not an input anywhere.
 *
 * ── PII rule (§B17g) ──
 *   This module never logs emails, names, message text, or tool args. Error logging
 *   is `err.name` only.
 */

const { transition, IllegalStateTransition } = require('../../shared/scheduling/stateMachine');
// §B17c: EMAIL_SHAPE is IMPORTED from the shipped entry-hook — never copied.
const { EMAIL_SHAPE, resolveQualifyingContext } = require('./newBookingEntry');
// §B16e shipped helper: model-supplied `date` → the propose route's date_window.
const { dateWindowForDay } = require('./dayPicker');

// ─── tool definitions (anthropic tool-use schema; descriptions verbatim §B17c) ───────

const AGENT_TOOL_DEFINITIONS = Object.freeze([
  Object.freeze({
    name: 'get_available_times',
    description:
      'Look up real, bookable appointment times. Use whenever the user wants to schedule, ' +
      'see times, or asks about a specific day or time of day. Never invent times — only ' +
      'ones returned here exist.',
    input_schema: {
      type: 'object',
      properties: {
        date: {
          type: 'string',
          description: 'Optional YYYY-MM-DD — constrain the lookup to a specific calendar day.',
        },
        exclude_slot_ids: {
          type: 'array',
          items: { type: 'string' },
          description: 'Optional — slot IDs the user already rejected, so fresh times are proposed.',
        },
      },
      required: [],
    },
  }),
  Object.freeze({
    name: 'request_booking_confirmation',
    description:
      "Stage a booking for the user's chosen time so they can confirm it. Requires their " +
      'email. This does NOT book — the user must press the Confirm button.',
    input_schema: {
      type: 'object',
      properties: {
        slot_id: {
          type: 'string',
          description: "REQUIRED — must be a slot from the current session's candidates.",
        },
        attendee_email: {
          type: 'string',
          description: "REQUIRED — the user's email; ask naturally if not yet known.",
        },
        attendee_name: { type: 'string', description: 'Optional.' },
      },
      required: ['slot_id', 'attendee_email'],
    },
  }),
]);

// ─── shared helpers ───────────────────────────────────────────────────────────────────

// Resolve the server-side qualifying context (appointment type / timezone). Prefers the
// integrator-supplied deps.qualifyingContext (same seam newBookingEntry threads); falls
// back to the shipped resolver over the tenant config. NEVER model args.
function resolveQctx({ tenantConfig, deps }) {
  if (deps && deps.qualifyingContext && typeof deps.qualifyingContext === 'object') {
    return deps.qualifyingContext;
  }
  return resolveQualifyingContext({ config: tenantConfig });
}

function qctxUserTimeZone(qctx) {
  return (qctx && (qctx.userTimeZone || qctx.user_time_zone)) || 'UTC';
}

// Sanitize the model's exclude_slot_ids: strings only, bounded (opaque server-issued ids).
function sanitizeExcludeSlotIds(value) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((v) => typeof v === 'string' && v.length > 0 && v.length <= 200)
    .slice(0, 100);
}

// F6: cap on the persisted candidate_slots union (multi-day same-turn lookups).
const MAX_PERSISTED_CANDIDATES = 10;

// ─── TOOL 1: get_available_times ──────────────────────────────────────────────────────

/**
 * §B17c tool 1 — wraps the SHIPPED §B16a `scheduling_propose` BCH route via
 * `deps.invokeProposal` (the same seam newBookingFlow uses; never re-implemented).
 *
 * On success: persists `candidate_slots` to the session row (state 'proposing', same
 * saveState shape as the deterministic `_propose`), emits the `scheduling_slots` SSE
 * (unchanged widget contract), and returns the §B17c output shape to the model:
 *   { slots: [{ slot_id, label, starts_at_iso }], user_time_zone, note }
 * `starts_at_iso` lets the model REASON about times ("after 3pm", "the later one");
 * it has no write authority over them — staging accepts only slot_id validated
 * against server-persisted candidates.
 *
 * Errors to model: { error: 'no_availability' | 'lookup_failed', note }.
 *
 * @param {object} params
 * @param {object}   params.input        - model args: { date?, exclude_slot_ids? }
 * @param {object}   [params.session]    - the live session row (may be null pre-session)
 * @param {string}   params.tenantId     - server context (NOT model args)
 * @param {string}   params.sessionId    - server context (NOT model args)
 * @param {object}   params.tenantConfig
 * @param {object}   params.deps         - { invokeProposal, saveState, qualifyingContext?, logger? }
 * @param {Function} [params.write]      - BSH SSE stream writer
 * @param {Function} [params.setSession] - callback receiving the updated session row
 * @param {object}   [params.turnCandidates] - turn-scoped accumulator threaded by agentTurn
 *   ({ slots: Array|null }): the candidate_slots THIS TURN's earlier calls already
 *   persisted. See the F6 union note below. Absent (direct/legacy callers) → replace.
 * @returns {Promise<object>} the model-facing §B17c result
 */
async function executeGetAvailableTimes({
  input = {},
  session,
  tenantId,
  sessionId,
  tenantConfig,
  deps = {},
  write,
  setSession,
  turnCandidates,
} = {}) {
  const logger = deps.logger || console;

  const qctx = resolveQctx({ tenantConfig, deps });
  const appointmentTypeId =
    qctx.appointmentTypeId != null ? qctx.appointmentTypeId : qctx.appointment_type_id;
  const userTimeZone = qctxUserTimeZone(qctx);

  if (typeof deps.invokeProposal !== 'function') {
    // Unwired seam — an honest transient failure to the model (§B17c error vocabulary
    // is closed: no_availability | lookup_failed).
    logger.warn('[WS-AG-CORE] get_available_times unavailable (propose seam unwired)');
    return {
      error: 'lookup_failed',
      note: 'The live availability lookup is not available right now. Apologize honestly and offer the email fallback. Never invent times.',
    };
  }
  // appointmentTypeId is server-context sourced (qualifyingContext / tenant config) and
  // included WHEN RESOLVED — it is never a model arg and never a hard precondition here:
  // the SHIPPED §B16a propose route owns resolution/validation behind the seam (a server-
  // side resolution failure surfaces as outcome 'failed' → lookup_failed). Pinned by the
  // §B17 eval suite (agentEvals A1–A3/A12 invoke the seam without a configured appt-type).

  // §B14-family state guard: persisting 'proposing' must be a legal move from the live
  // state. qualifying/proposing/confirming → proposing are all legal; a 'booked' (or
  // recovery-loop) session must not be re-opened by the model → honest lookup_failed.
  // No prior row = session INITIALIZATION in 'proposing' (legal per stateMachine docs).
  const priorState = session && session.state;
  if (priorState) {
    try {
      transition({ state: priorState }, 'proposing');
    } catch (err) {
      if (err instanceof IllegalStateTransition) {
        logger.warn(`[WS-AG-CORE] get_available_times rejected: illegal ${priorState} → proposing`);
        return {
          error: 'lookup_failed',
          note: 'Times cannot be looked up for this conversation right now. Offer the email/human fallback.',
        };
      }
      throw err;
    }
  }

  // §B17c: exclude_slot_ids → alreadyRejected. ACCUMULATE with the session row's
  // persisted rejected_slot_ids (server state — mirrors the §B16b accumulation rule) so
  // a model that forgets an earlier rejection cannot resurface already-rejected times.
  const persistedRejected = (session && Array.isArray(session.rejected_slot_ids))
    ? session.rejected_slot_ids.filter((v) => typeof v === 'string')
    : [];
  const alreadyRejected = Array.from(
    new Set([...persistedRejected, ...sanitizeExcludeSlotIds(input.exclude_slot_ids)])
  );

  const proposePayload = {
    action: 'scheduling_propose',
    tenantId,
    sessionId,
    userTimeZone,
    alreadyRejected,
  };
  if (appointmentTypeId != null) proposePayload.appointmentTypeId = appointmentTypeId;
  // §B16a optional availability window — forward the tenant's configured window when
  // present (mirrors _propose; schema-discipline: tolerate camel OR snake; omit absent).
  const windowStart = qctx.windowStart ?? qctx.window_start;
  const windowEnd = qctx.windowEnd ?? qctx.window_end;
  if (windowStart != null) proposePayload.windowStart = windowStart;
  if (windowEnd != null) proposePayload.windowEnd = windowEnd;

  // Model `date` arg → the §B16e date_window passthrough (shipped). An invalid /
  // non-calendar date from the model is an honest lookup_failed (never a silent
  // unconstrained lookup the model would then mis-narrate as day-specific).
  if (input.date != null) {
    let dateWindow;
    try {
      dateWindow = dateWindowForDay(String(input.date));
    } catch {
      return {
        error: 'lookup_failed',
        note: 'That date could not be used for a lookup. Ask the user for the day again (YYYY-MM-DD resolvable), or offer the email fallback.',
      };
    }
    proposePayload.date_window = { start: dateWindow.startISO, end: dateWindow.endISO };
  }

  let res;
  const startedAt = Date.now();
  try {
    res = await deps.invokeProposal(proposePayload);
  } catch (err) {
    logger.error(`[WS-AG-CORE] propose invoke failed (→ lookup_failed): error_name=${(err && err.name) || 'unknown'}`);
    return {
      error: 'lookup_failed',
      note: 'The live availability lookup failed just now. Say the lookup failed right now — never that you lack scheduling access — and offer the email fallback. Never invent times.',
    };
  }
  void startedAt; // latency is measured by the agentTurn audit wrapper

  const outcome = res && res.outcome;
  if (outcome === 'no_availability') {
    return {
      error: 'no_availability',
      note: 'No real openings matched. Apologize honestly, suggest a different day or the email fallback, and never invent times.',
    };
  }
  if (outcome !== 'ok' || !Array.isArray(res.slots) || res.slots.length === 0) {
    return {
      error: 'lookup_failed',
      note: 'The live availability lookup failed just now. Say the lookup failed right now — never that you lack scheduling access — and offer the email fallback. Never invent times.',
    };
  }

  const slots = res.slots;
  // §B16c carry-forwards (mirrors _propose): pool_size at commit = TOP-LEVEL poolSize.
  const proposal = { poolSize: res.poolSize };
  if (res.tieBreaker != null) proposal.tieBreaker = res.tieBreaker;
  if (res.roundRobinCursor != null) proposal.roundRobinCursor = res.roundRobinCursor;

  // F6 (live defect 2026-06-11): a multi-day ask ("Monday or Tuesday?") makes TWO dated
  // calls in ONE turn; saveState is a PutItem whitelist write, so call 2's
  // candidate_slots REPLACED call 1's — staging a first-day slot then failed with
  // unknown_slot. Fix: UNION with the slots THIS TURN's earlier calls persisted,
  // threaded by agentTurn via the turn-scoped accumulator (never guessed from
  // timestamps; prior-TURN candidates are still replaced — the §B16b re-propose
  // semantics). Dedupe by slotId (first occurrence wins — §B16b ordering is the
  // propose route's presentation order, earlier call first), cap 10. The
  // scheduling_slots SSE below still carries ONLY this call's slots (the widget merges).
  const priorTurnSlots = (turnCandidates && Array.isArray(turnCandidates.slots))
    ? turnCandidates.slots
    : [];
  const seenSlotIds = new Set();
  const persistedSlots = [];
  for (const s of [...priorTurnSlots, ...slots]) {
    if (!s || !s.slotId || seenSlotIds.has(s.slotId)) continue;
    seenSlotIds.add(s.slotId);
    persistedSlots.push(s);
    if (persistedSlots.length >= MAX_PERSISTED_CANDIDATES) break;
  }

  const updatedSession = {
    ...(session || {}),
    state: 'proposing',
    candidate_slots: persistedSlots,
    proposal,
    rejected_slot_ids: alreadyRejected,
  };
  if (typeof deps.saveState === 'function') {
    await deps.saveState({
      tenantId,
      sessionId,
      state: 'proposing',
      candidate_slots: persistedSlots,
      proposal,
      rejected_slot_ids: alreadyRejected,
      // §B16d amendment: a previously captured attendee email survives a re-propose
      // (the agent path must not clobber it — A5/A9 depend on it).
      ...(session && session.attendee_email != null ? { attendee_email: session.attendee_email } : {}),
    });
  }
  if (typeof setSession === 'function') setSession(updatedSession);
  if (turnCandidates) turnCandidates.slots = persistedSlots;

  // Emit the SHIPPED scheduling_slots SSE → existing widget chips (unchanged contract).
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({ type: 'scheduling_slots', slots, session_id: sessionId })}\n\n`);
  }

  // F5 (eval A2/A3): when a re-lookup returns the SAME times the session already had,
  // say so in the note — the model must never narrate "other options" it did not receive.
  const priorIds = (session && Array.isArray(session.candidate_slots))
    ? session.candidate_slots.map((s) => s && s.slotId).filter(Boolean)
    : [];
  const newIds = slots.map((s) => s.slotId);
  const sameAsBefore =
    priorIds.length > 0 &&
    newIds.length === priorIds.length &&
    newIds.every((id) => priorIds.includes(id));

  // §B17c output to model. GENERIC slots only (label + starts_at_iso) — coordinator
  // identity (candidateResourceIds) is server-side data and never reaches the model.
  return {
    slots: slots.map((s) => ({ slot_id: s.slotId, label: s.label, starts_at_iso: s.start })),
    user_time_zone: userTimeZone,
    note: sameAsBefore
      ? 'SAME results as before — no new times opened up. Tell the user plainly that ' +
        'nothing else is open right now (never affirm new availability you did not ' +
        'receive), and offer a different day or the email/human fallback.'
      : 'These are the only real, bookable times. Offer them conversationally; the chips are ' +
        'already on screen. The user can tap a chip, or you can stage one with ' +
        'request_booking_confirmation once you have their email. A time can be taken until confirmed.',
  };
}

// ─── TOOL 2: request_booking_confirmation ─────────────────────────────────────────────

/**
 * §B17c tool 2 — validates + STAGES (never books). Validation order per contract:
 *   1. slot_id MUST be in the session's persisted candidate_slots → { error: 'unknown_slot' }
 *   2. attendee_email MUST match EMAIL_SHAPE (imported)            → { error: 'invalid_email' }
 *   3. ANTI-HALLUCINATION GUARD (§B17c #3): attendee_email is REJECTED unless it appears
 *      verbatim in this session's user-side transcript or equals the session row's
 *      captured attendee_email                                      → { error: 'invalid_email' }
 *   4. saveState({ state:'confirming', selected_slot, attendee_email }) — the SAME
 *      staging path the shipped deterministic pipeline uses (deps.saveState; one
 *      implementation, two callers)
 *   5. emit the SHIPPED `scheduling_confirm` SSE (picasso#538 confirm card)
 *
 * Returns { staged: true, label } on success. The model CANNOT reach the booking
 * commit under any input: this module holds no reference to the §B16c seam.
 *
 * @param {object} params
 * @param {object}   params.input          - model args: { slot_id, attendee_email, attendee_name? }
 * @param {object}   [params.session]      - the live session row
 * @param {string}   params.tenantId       - server context (NOT model args)
 * @param {string}   params.sessionId      - server context (NOT model args)
 * @param {object}   params.tenantConfig
 * @param {object}   params.deps           - { saveState, logger? }
 * @param {Function} [params.write]        - BSH SSE stream writer
 * @param {string[]} [params.userTranscript] - the session's USER-SIDE messages (for guard #3)
 * @param {Function} [params.setSession]   - callback receiving the updated session row
 * @returns {Promise<object>} the model-facing §B17c result
 */
async function executeRequestBookingConfirmation({
  input = {},
  session,
  tenantId,
  sessionId,
  tenantConfig,
  deps = {},
  write,
  userTranscript = [],
  setSession,
} = {}) {
  const logger = deps.logger || console;
  void tenantConfig; // reserved (tenant scope is server-derived; nothing tenant-shaped is read here yet)

  // 1. slot_id ∈ persisted candidates (server state — never a model-supplied list).
  const slotId = typeof input.slot_id === 'string' ? input.slot_id : String(input.slot_id ?? '');
  const candidates = (session && Array.isArray(session.candidate_slots)) ? session.candidate_slots : [];
  const chosen = candidates.find((s) => s && s.slotId === slotId);
  if (!chosen) {
    return { error: 'unknown_slot' };
  }

  // Fail-closed state gate (§B14 family): staging is legal from 'proposing' (the
  // proposing→confirming move) or 'confirming' (a re-stage — same state, not a move;
  // mirrors the shipped captureAttendeeEmail). Any other live state (e.g. 'booked')
  // must not be re-armed by the model — rejected via the closed error vocabulary.
  const priorState = session && session.state;
  if (priorState && priorState !== 'confirming') {
    try {
      transition({ state: priorState }, 'confirming');
    } catch (err) {
      if (err instanceof IllegalStateTransition) {
        logger.warn(`[WS-AG-CORE] request_booking_confirmation rejected: illegal ${priorState} → confirming`);
        return { error: 'unknown_slot' };
      }
      throw err;
    }
  }

  // 2. Email shape (EMAIL_SHAPE imported from newBookingEntry — §B17c: DO NOT copy).
  //    Case-normalized to lowercase: email matching is case-insensitive in practice,
  //    and the model often re-cases what the user typed.
  const email = typeof input.attendee_email === 'string' ? input.attendee_email.trim().toLowerCase() : '';
  if (!email || email.length > 254 || !EMAIL_SHAPE.test(email)) {
    return { error: 'invalid_email' };
  }

  // 3. ANTI-HALLUCINATION GUARD (§B17c #3, governance pass 2026-06-12): the model cannot
  //    stage an address the user never typed. Verbatim containment in the user-side
  //    transcript, or exact equality with the session row's captured attendee_email —
  //    both compared case-insensitively (lowercased) so a model-re-cased copy of the
  //    user's own address is accepted while invented addresses stay rejected.
  const capturedEmail = session && typeof session.attendee_email === 'string'
    ? session.attendee_email.toLowerCase()
    : null;
  const inTranscript = Array.isArray(userTranscript)
    && userTranscript.some((t) => typeof t === 'string' && t.toLowerCase().includes(email));
  if (!inTranscript && email !== capturedEmail) {
    logger.warn('[WS-AG-CORE] request_booking_confirmation rejected: attendee_email failed the verbatim-match guard');
    return { error: 'invalid_email' };
  }

  // 4. Stage via the SAME saveState path the deterministic pipeline uses (one staging
  //    implementation, two callers — mirrors newBookingFlow select_slot + captureAttendeeEmail).
  const selectedSlot = {
    slotId: chosen.slotId,
    start: chosen.start,
    end: chosen.end,
    candidateResourceIds: chosen.candidateResourceIds,
  };
  const updatedSession = {
    ...(session || {}),
    state: 'confirming',
    selected_slot: selectedSlot,
    candidate_slots: candidates,
    attendee_email: email,
  };
  if (typeof deps.saveState === 'function') {
    await deps.saveState({
      tenantId,
      sessionId,
      state: 'confirming',
      selected_slot: selectedSlot,
      candidate_slots: candidates,
      // carry the propose metadata forward so the (deterministic) commit turn can read it.
      proposal: session && session.proposal,
      rejected_slot_ids: session && session.rejected_slot_ids,
      attendee_email: email,
    });
  }
  if (typeof setSession === 'function') setSession(updatedSession);

  // 5. Emit the SHIPPED scheduling_confirm SSE → the server-driven confirm card. The
  //    COMMIT stays the deterministic confirm-click path (§B16c/§B14) — out of model reach.
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({
      type: 'scheduling_confirm',
      session_id: sessionId,
      slot: { slotId: chosen.slotId, label: chosen.label },
      attendee_email: email,
    })}\n\n`);
  }

  return { staged: true, label: chosen.label };
}

module.exports = {
  AGENT_TOOL_DEFINITIONS,
  executeGetAvailableTimes,
  executeRequestBookingConfirmation,
  MAX_PERSISTED_CANDIDATES,
};
