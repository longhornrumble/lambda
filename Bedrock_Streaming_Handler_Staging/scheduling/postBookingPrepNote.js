'use strict';

/**
 * postBookingPrepNote.js — capture the attendee's answer to the form-configured
 * "what would you like to talk about?" question on the turn AFTER a booking commits
 * (§B post-booking amendment).
 *
 * Flow: when the originating form configured a question, newBookingFlow streams it as
 * ordinary assistant text right after the booking confirms and marks the booked session
 * row with `awaiting_prep_note: true` + `booking_id`. The user's NEXT plain free-text turn
 * is their answer. This module — invoked from index.js's deterministic-bypass band (after
 * the click router + bare-email capture, before the agent/chat path) — captures that turn
 * WITHOUT a model call (a state-blind LLM would otherwise answer it), attaches it to the
 * Booking row via the BCH executor (`action: 'attach_prep_note'`), clears the one-shot flag,
 * and streams a brief ack.
 *
 * Boundaries / discipline:
 *   - Deterministic only: NEVER calls a model. The answer is the verbatim user turn.
 *   - One-shot: the flag is cleared regardless of attach outcome — the user is never re-asked.
 *   - Fail-soft: any miss (no session / wrong state / not awaiting) → { captured:false } and
 *     the caller falls through to normal chat. The answer is OPTIONAL — never strand the user.
 *   - PII: the answer text is NEVER logged (it flows only to the executor payload). Logs carry
 *     err.name / booking_id only — same discipline as postFormOffer / captureAttendeeEmail.
 */

const PREP_NOTE_ACK = "Thanks — I'll pass that along so they can prepare for your conversation.";

/**
 * @param {object} args
 * @param {string} args.tenantId
 * @param {string} args.sessionId
 * @param {string} args.userInput            the raw user turn (the candidate answer)
 * @param {object} args.deps
 * @param {Function} args.deps.loadState      ({tenantId,sessionId}) → session row | null
 * @param {Function} [args.deps.saveState]    ({...}) → void  (clears the one-shot flag)
 * @param {Function} [args.deps.invokeAttachPrepNote] (payload) → void  (BCH executor seam)
 * @param {Function} [args.write]             SSE writer
 * @returns {Promise<{captured: boolean}>}
 */
async function capturePrepNote({ tenantId, sessionId, userInput, deps = {}, write } = {}) {
  if (!tenantId || !sessionId) return { captured: false };
  if (typeof userInput !== 'string' || !userInput.trim()) return { captured: false };
  if (typeof deps.loadState !== 'function') return { captured: false };

  let row;
  try {
    row = await deps.loadState({ tenantId, sessionId });
  } catch (err) {
    // Fail-soft: a state read we can't trust → fall through to normal chat (PII-safe: err.name).
    console.error(`[WS-PREP] state read failed (capture skipped): error_name=${(err && err.name) || 'unknown'}`);
    return { captured: false };
  }
  if (!row || row.awaiting_prep_note !== true || !row.booking_id) return { captured: false };

  // Attach to the Booking row (best-effort). If the executor seam is unwired or errors we
  // STILL clear the flag + ack below (one-shot; never re-ask). PII: never log the answer.
  try {
    if (typeof deps.invokeAttachPrepNote === 'function') {
      await deps.invokeAttachPrepNote({
        action: 'attach_prep_note',
        tenantId,
        bookingId: row.booking_id,
        prepNote: userInput.trim(),
      });
    }
  } catch (err) {
    console.error(`[WS-PREP] attach_prep_note invoke failed (non-fatal): booking_id=${row.booking_id} error_name=${(err && err.name) || 'unknown'}`);
  }

  // Clear the one-shot flag so the next turn is normal chat. saveState is a Put-overwrite, so
  // re-saving 'booked' WITHOUT the prep fields drops awaiting_prep_note / booking_id / question.
  try {
    if (typeof deps.saveState === 'function') {
      await deps.saveState({ tenantId, sessionId, state: 'booked' });
    }
  } catch (err) {
    // Fail-soft: the flag stays set; worst case the next turn acks once more. PII-safe.
    console.error(`[WS-PREP] clear awaiting flag failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
  }

  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({ type: 'text', content: PREP_NOTE_ACK, session_id: sessionId })}\n\n`);
  }
  return { captured: true };
}

module.exports = { capturePrepNote, PREP_NOTE_ACK };
