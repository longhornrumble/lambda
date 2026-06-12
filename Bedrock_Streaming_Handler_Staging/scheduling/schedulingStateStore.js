'use strict';

/**
 * Tier-1 deps-wiring for the in-chat scheduling recovery loop (B-minimal).
 *
 * Provides the DynamoDB I/O seam that runSchedulingTurn (schedulingFlow.js) consumes:
 *   - loadState / saveState : the C9 conversation-state row on the
 *     conversation-scheduling-session table (PK tenantId · SK = the PLAIN <sessionId>;
 *     distinct from the §B10 binding row at SK `binding#<sessionId>`).
 *   - loadBooking : the Booking row the §B10 binding governs (PK tenantId · SK booking_id).
 *
 * resolveBinding / detectSchedulingAction / generateSlots / stateMachine use
 * schedulingFlow's bundled defaults — they need no injection. The Google-auth calendar
 * EXECUTION seam (getOAuthClient / calendarEvents / conference / updateMeeting) is Tier 2
 * (a Booking_Commit_Handler executor invoke — BSH cannot bundle googleapis); it is NOT
 * wired here, so a confirm_reschedule/confirm_cancel detects + validates the §B14
 * transition but the calendar op is SKIPPED non-fatally until Tier 2 lands.
 */

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');

/**
 * @param {object} opts
 * @param {string} opts.sessionTable - conversation-scheduling-session table name
 * @param {string} opts.bookingTable - Booking table name
 * @param {string} [opts.region]
 * @param {object} [opts.client]     - injectable DynamoDBClient (tests)
 * @returns {{loadState:Function, saveState:Function, loadBooking:Function}}
 */
function buildSchedulingDeps({ sessionTable, bookingTable, region, client } = {}) {
  const ddb = DynamoDBDocumentClient.from(
    client || new DynamoDBClient({ region: region || process.env.AWS_REGION })
  );

  // All three are FAIL-SOFT: this is a post-stream enhancement on the chat hot path,
  // never worth breaking the (already-streamed) response over. A DDB error logs (PII-free
  // error_name) and degrades gracefully — reads → null (flow falls back to initial state /
  // skips execution non-fatally), saveState → no-op (next turn re-derives). This also keeps
  // a saveState throw from propagating to handled:false and leaking the normal CTA chain
  // onto a live scheduling turn (audit S-1).

  // C9 state row read (plain-<sessionId> SK). Returns the raw item ({ state,
  // candidate_slots?, selected_slot? }) or null on a first turn / missing row / error.
  async function loadState({ tenantId, sessionId } = {}) {
    if (!tenantId || !sessionId) return null;
    try {
      const res = await ddb.send(new GetCommand({
        TableName: sessionTable,
        Key: { tenantId, session_id: sessionId },
      }));
      return res.Item || null;
    } catch (err) {
      console.error(`[WS-CONVO] loadState failed (fail-soft → null): error_name=${(err && err.name) || 'unknown'}`);
      return null;
    }
  }

  // C9 state row write. PUTs the plain-<sessionId> row — never touches the
  // `binding#<sessionId>` row, so the §B10 binding is preserved. Guards `state`
  // (DDB rejects a null/undefined attribute, audit S-2).
  async function saveState({ tenantId, sessionId, state, candidate_slots, selected_slot, proposal, rejected_slot_ids, attendee_email } = {}) {
    if (!tenantId || !sessionId || !state) return;
    const item = { tenantId, session_id: sessionId, state, updated_at: new Date().toISOString() };
    if (candidate_slots !== undefined) item.candidate_slots = candidate_slots;
    if (selected_slot !== undefined) item.selected_slot = selected_slot;
    // WS-NEWBOOK (§B16): the new-booking flow persists the propose metadata (poolSize +
    // tie-breaker + round-robin cursor) so _doCommit can supply the §B16c pool_size, and the
    // accumulated rejected slot ids so the "more times" self-loop re-proposes fresh times.
    if (proposal !== undefined) item.proposal = proposal;
    if (rejected_slot_ids !== undefined) item.rejected_slot_ids = rejected_slot_ids;
    // §B16d amendment (deterministic pipeline): chat-captured attendee email rides the
    // session row so the commit's identity gate can pass without a form submission.
    if (attendee_email !== undefined) item.attendee_email = attendee_email;
    try {
      await ddb.send(new PutCommand({ TableName: sessionTable, Item: item }));
    } catch (err) {
      console.error(`[WS-CONVO] saveState failed (fail-soft → no-op): error_name=${(err && err.name) || 'unknown'}`);
    }
  }

  // Booking read (PK tenantId · SK booking_id). schedulingFlow reads it tolerantly
  // (camel OR snake), so the raw item is fine. GetItem only — BSH never writes Booking.
  async function loadBooking({ tenantId, bookingId } = {}) {
    if (!tenantId || !bookingId) return null;
    try {
      const res = await ddb.send(new GetCommand({
        TableName: bookingTable,
        Key: { tenantId, booking_id: bookingId },
      }));
      return res.Item || null;
    } catch (err) {
      console.error(`[WS-CONVO] loadBooking failed (fail-soft → null): error_name=${(err && err.name) || 'unknown'}`);
      return null;
    }
  }

  return { loadState, saveState, loadBooking };
}

module.exports = { buildSchedulingDeps };
