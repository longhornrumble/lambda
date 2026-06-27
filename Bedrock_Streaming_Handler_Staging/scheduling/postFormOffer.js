'use strict';

/**
 * postFormOffer.js — post-form scheduling offer (Track-D fix 3, WS-TRACKD-BE; design-doc
 * Appendix-A row D3: "form submitted → templated offer + propose; email pre-filled; two
 * taps to booked").
 *
 * Given a COMPLETED form submission carrying an attendee email for a scheduling-enabled
 * tenant, this module:
 *   1. runs the §B16a `scheduling_propose` BCH route via the injected `deps.invokeProposal`
 *      (the SHIPPED seam — same payload shape as newBookingFlow._propose), and
 *   2. returns warm templated offer copy for the caller to surface, and
 *   3. on `outcome:'ok'`: emits the `scheduling_slots` SSE (via the injected `deps.emitSse`)
 *      and persists the proposing session WITH the form's attendee_email pre-filled — so
 *      the later confirm step (request_booking_confirmation / the deterministic confirm
 *      card) does NOT need to re-ask for the email.
 *
 * MODULE + TESTS ONLY: the integrator wires the call site in form_handler.js.
 *
 * ── Layered in-flight clobber guard (adversarial-audit fix 3) ──
 *   A mid-booking form submission must not be clobbered by a fresh offer. Two layers:
 *   1. Integrator guard (call site): invoke this only when no scheduling session is
 *      already in flight — the integrator has loadState at the call site.
 *   2. Self-defense (this module): when the OPTIONAL `deps.loadState` seam is wired, the
 *      session row is read BEFORE any propose; a 'proposing'/'confirming' row (staged
 *      slots / a staged pick that a fresh propose + saveState would clobber) → returns
 *      `{ suppressed: true, reason: 'session_in_flight' }` with NO propose, NO saveState,
 *      NO SSE. A failed read also SUPPRESSES (`reason: 'session_state_unverifiable'`) —
 *      a write you can't verify is safe to make is safe to skip. 'qualifying' deliberately
 *      proceeds: nothing staged to clobber, and the offer (slots + pre-filled email) is
 *      the natural next step for that arc — including the retry after this module's own
 *      no_availability path left the row in 'qualifying'.
 *   When `deps.loadState` is absent, behavior is unchanged (layer 1 alone governs).
 *
 * ── Boundary (§B14, LOCKED) ──
 *   This module NEVER calls `invokeBookingCommit` and NEVER advances the session to
 *   'confirming' (or beyond) unilaterally. It stages at most a 'proposing' session — the
 *   user's own slot-pick + confirm clicks (the deterministic §B16b pipeline) do the rest.
 *
 * ── Shared staging path (§B16b) ──
 *   The saveState call uses EXACTLY the deterministic pipeline's whitelist keys
 *   (schedulingStateStore.saveState: state, candidate_slots, selected_slot, proposal,
 *   rejected_slot_ids, attendee_email) and the §B16b ordering rule: advance to 'proposing'
 *   ONLY on `outcome:'ok'`, in the SAME saveState that persists the slots; on
 *   'no_availability' the session stays in 'qualifying' (strand-prevention — a slot-less
 *   'proposing' session is a permanent strand).
 *
 * ── PII ──
 *   The attendee email is shape-validated (EMAIL_SHAPE — imported from newBookingEntry,
 *   per the work-order; never copied) and flows ONLY to the session row (saveState) and
 *   the caller's return value. It is NEVER logged: audit events carry `email_present`
 *   (boolean), error logging is `err.name` only.
 *
 * ── Return contract (work-order item 6) ──
 *   postFormOffer({ tenantConfig, sessionId, attendee, deps })
 *     → { offerText: string|null, slotsResult: object|null }
 *   outcome 'ok'              → offerText = warm offer copy; scheduling_slots SSE emitted.
 *   outcome 'no_availability' → offerText = warm no-times copy; NO SSE.
 *   outcome 'failed' (or invoke throw / unknown outcome) → offerText = null (the caller
 *     suppresses the offer silently); slotsResult carries the outcome.
 *   Guard-rejected (feature off / no usable email / no resolvable appointment type /
 *     seam unwired) → { offerText: null, slotsResult: null } (no propose attempted).
 *   Self-defense guard (deps.loadState wired; audit fix 3) → { offerText: null,
 *     slotsResult: null, suppressed: true, reason: 'session_in_flight' |
 *     'session_state_unverifiable' } (no propose attempted).
 */

const { isSchedulingEnabled } = require('./bindingContext');
// EMAIL_SHAPE: import — do NOT copy (work-order). resolveQualifyingContext: the shipped
// appointment-type / timezone resolution from the tenant `scheduling` config block — reuse,
// never re-implement.
const { EMAIL_SHAPE, resolveQualifyingContext } = require('./newBookingEntry');

// Warm templated copy (§B17e voice rule: warm and honest, never robotic; no guarantee
// language about offered times).
const OFFER_TEXT_OK =
  'Would you like to book a quick call? Here are some times that work:';
const OFFER_TEXT_NO_AVAILABILITY =
  "Would you like to book a quick call? I don't see any open times right now — " +
  'but I\'m happy to check other days if you\'d like.';

// The session states the self-defense guard suppresses on: rows carrying staged slots
// ('proposing') or a staged pick ('confirming') that a fresh propose + saveState would
// clobber. Deliberately NOT 'qualifying' — see the module-header guard note.
const IN_FLIGHT_GUARD_STATES = Object.freeze(['proposing', 'confirming']);

/**
 * PII-safe audit emit: a single JSON line, fields fixed here — NEVER the email itself
 * (`email_present` boolean only), never free text.
 */
function _audit(logger, fields) {
  (logger || console).info(JSON.stringify({ event: 'post_form_offer', ...fields }));
}

/**
 * Offer a booking right after a completed form submission (design-doc Appendix-A D3).
 *
 * @param {object} params
 * @param {object} params.tenantConfig - the authenticated tenant config (S3); tenant_id is
 *   read from HERE, never from a request body (same rule as the runSchedulingTurn sites).
 * @param {string} params.sessionId    - the chat session id
 * @param {object} params.attendee     - { email, first_name?, last_name?, phone? } from the
 *   form submission's canonical contact
 * @param {object} params.deps         - { invokeProposal, emitSse, saveState?, loadState?, logger? }
 *   invokeProposal: the §B16a BCH seam (REQUIRED — without it there is nothing to offer)
 *   emitSse: injected SSE emitter; called with the event OBJECT (the integrator owns the
 *     wire format) — { type:'scheduling_slots', slots, session_id }
 *   saveState: the §B16b shared staging path (schedulingStateStore whitelist)
 *   loadState: OPTIONAL self-defense seam (audit fix 3) — when wired, the session row is
 *     read first and an in-flight 'proposing'/'confirming' row suppresses the offer
 * @returns {Promise<{offerText: string|null, slotsResult: object|null}>}
 */
async function postFormOffer({ tenantConfig, sessionId, attendee, postBookingQuestion, deps = {} } = {}) {
  const logger = deps.logger || console;
  const suppressed = { offerText: null, slotsResult: null };

  try {
    // ── Guards (fail-closed; no propose, no SSE, no state write) ──
    if (!isSchedulingEnabled(tenantConfig)) return suppressed;
    const tenantId = tenantConfig && tenantConfig.tenant_id;
    if (!tenantId || !sessionId) return suppressed;

    const email = attendee && typeof attendee.email === 'string' ? attendee.email.trim() : '';
    if (!email || email.length > 254 || !EMAIL_SHAPE.test(email)) {
      // No usable identity → no offer (the whole point of D3 is the pre-filled email).
      _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'skipped', reason: 'no_usable_email', email_present: false });
      return suppressed;
    }

    if (typeof deps.invokeProposal !== 'function') {
      logger.warn('[WS-TRACKD] postFormOffer: propose seam (deps.invokeProposal) not wired — offer skipped');
      return suppressed;
    }

    // ── Self-defense in-flight clobber guard (audit fix 3 — layer 2; see module header) ──
    // Read the session BEFORE any propose: an in-flight 'proposing'/'confirming' row must
    // not be clobbered by a fresh offer. When deps.loadState is absent the integrator's
    // call-site guard (layer 1) alone governs — behavior unchanged.
    if (typeof deps.loadState === 'function') {
      let existingRow;
      try {
        existingRow = await deps.loadState({ tenantId, sessionId });
      } catch (err) {
        // SUPPRESS on a failed read (NOT fail-open): a write you can't verify is safe to
        // make is safe to skip — the user keeps the in-chat path; nothing gets clobbered.
        // PII-safe: err.name only.
        logger.error(`[WS-TRACKD] postFormOffer state read failed (offer suppressed): error_name=${(err && err.name) || 'unknown'}`);
        _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'skipped', reason: 'session_state_unverifiable', email_present: true });
        return { offerText: null, slotsResult: null, suppressed: true, reason: 'session_state_unverifiable' };
      }
      if (existingRow && IN_FLIGHT_GUARD_STATES.includes(existingRow.state)) {
        _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'skipped', reason: 'session_in_flight', email_present: true });
        return { offerText: null, slotsResult: null, suppressed: true, reason: 'session_in_flight' };
      }
    }

    // Appointment-type / timezone resolution — the shipped §B16d logic, reused.
    const qctx = resolveQualifyingContext({ config: tenantConfig });
    const appointmentTypeId = qctx.appointmentTypeId;
    if (!appointmentTypeId) {
      // Multi-type tenant with no sole default: the offer can't pick for the user — the
      // in-chat qualifying flow (which can ask) owns that case. Suppress silently.
      _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'skipped', reason: 'no_appointment_type', email_present: true });
      return suppressed;
    }

    // ── §B16a propose (same payload shape as newBookingFlow._propose; fresh session →
    //     nothing rejected yet) ──
    // (No windowStart/windowEnd forwarding here: resolveQualifyingContext never returns
    // them — propose falls back to its default window, same as a fresh in-chat entry.)
    const proposePayload = {
      action: 'scheduling_propose',
      tenantId,
      sessionId,
      appointmentTypeId,
      userTimeZone: qctx.userTimeZone || 'UTC',
      alreadyRejected: [],
    };

    let slotsResult;
    try {
      slotsResult = await deps.invokeProposal(proposePayload);
    } catch (err) {
      // PII-safe: err.name only.
      // AUDIT-EVENT FLOW (dual-catch note, audit fix 5): this inner catch RETURNS, so the
      // outer catch below never sees an invoke-throw — exactly ONE 'failed' audit event
      // fires on that path. Do NOT add a second _audit in the outer catch "for symmetry":
      // any future rethrow/refactor here would then double-fire the event.
      logger.error(`[WS-TRACKD] postFormOffer propose invoke failed (offer suppressed): error_name=${(err && err.name) || 'unknown'}`);
      _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'failed', email_present: true });
      return { offerText: null, slotsResult: { outcome: 'failed' } };
    }

    const outcome = slotsResult && slotsResult.outcome;

    // ── outcome:'ok' → stage 'proposing' + slots + pre-filled email (ONE saveState,
    //     §B16b ordering) and emit the scheduling_slots SSE ──
    if (outcome === 'ok') {
      const slots = slotsResult.slots || [];
      const proposal = { poolSize: slotsResult.poolSize };
      if (slotsResult.tieBreaker != null) proposal.tieBreaker = slotsResult.tieBreaker;
      if (slotsResult.roundRobinCursor != null) proposal.roundRobinCursor = slotsResult.roundRobinCursor;

      if (deps.saveState) {
        const proposingState = {
          tenantId,
          sessionId,
          state: 'proposing',
          candidate_slots: slots,
          proposal,
          rejected_slot_ids: [],
          attendee_email: email, // the D3 pre-fill: confirm never re-asks
        };
        // §B post-booking amendment: stash the form-configured "what would you like to talk
        // about?" question on the session so the booked turn can ask it (carried forward
        // through select_slot → confirm_book). Additive — absent → byte-identical.
        if (typeof postBookingQuestion === 'string' && postBookingQuestion.trim()) {
          proposingState.post_booking_question = postBookingQuestion.trim();
        }
        await deps.saveState(proposingState);
      }
      if (typeof deps.emitSse === 'function') {
        // §B18b: forward context when the propose result carries it (ADDITIVE; omit when absent —
        // old-shape tolerant per CLAUDE.md schema discipline).
        const sseEvent = { type: 'scheduling_slots', slots, session_id: sessionId };
        if (slotsResult.context != null) sseEvent.context = slotsResult.context;
        deps.emitSse(sseEvent);
      }
      _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'ok', slot_count: slots.length, email_present: true });
      return { offerText: OFFER_TEXT_OK, slotsResult };
    }

    // ── outcome:'no_availability' → warm copy, NO SSE; stay in 'qualifying' (§B16b
    //     strand-prevention) with the email pre-filled for any later booking turn ──
    if (outcome === 'no_availability') {
      if (deps.saveState) {
        await deps.saveState({
          tenantId,
          sessionId,
          state: 'qualifying',
          attendee_email: email,
        });
      }
      _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'no_availability', email_present: true });
      return { offerText: OFFER_TEXT_NO_AVAILABILITY, slotsResult };
    }

    // ── outcome:'failed' (or anything unrecognized — fail-closed) → suppress silently ──
    _audit(logger, { tenant_id: tenantId, session_id: sessionId, outcome: 'failed', email_present: true });
    return { offerText: null, slotsResult };
  } catch (err) {
    // Never break the form-completion turn over an offer. PII-safe: err.name only.
    // NO _audit here BY DESIGN (audit fix 5): the invoke-throw path already audited and
    // returned in the inner catch above — adding one here would double-fire if that catch
    // ever rethrows. Throws reaching THIS catch (e.g. saveState/emitSse) are log-only.
    logger.error(`[WS-TRACKD] postFormOffer failed (non-fatal, offer suppressed): error_name=${(err && err.name) || 'unknown'}`);
    return suppressed;
  }
}

module.exports = {
  postFormOffer,
  OFFER_TEXT_OK,
  OFFER_TEXT_NO_AVAILABILITY,
  IN_FLIGHT_GUARD_STATES,
};
