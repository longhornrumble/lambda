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

  // C9 state row read (plain-<sessionId> SK). Returns the raw item ({ state,
  // candidate_slots?, selected_slot? }) or null on a first turn / missing row.
  async function loadState({ tenantId, sessionId } = {}) {
    if (!tenantId || !sessionId) return null;
    const res = await ddb.send(new GetCommand({
      TableName: sessionTable,
      Key: { tenantId, session_id: sessionId },
    }));
    return res.Item || null;
  }

  // C9 state row write. PUTs the plain-<sessionId> row — never touches the
  // `binding#<sessionId>` row, so the §B10 binding is preserved.
  async function saveState({ tenantId, sessionId, state, candidate_slots, selected_slot } = {}) {
    if (!tenantId || !sessionId) return;
    const item = { tenantId, session_id: sessionId, state, updated_at: new Date().toISOString() };
    if (candidate_slots !== undefined) item.candidate_slots = candidate_slots;
    if (selected_slot !== undefined) item.selected_slot = selected_slot;
    await ddb.send(new PutCommand({ TableName: sessionTable, Item: item }));
  }

  // Booking read (PK tenantId · SK booking_id). schedulingFlow reads it tolerantly
  // (camel OR snake), so the raw item is fine. GetItem only — BSH never writes Booking.
  async function loadBooking({ tenantId, bookingId } = {}) {
    if (!tenantId || !bookingId) return null;
    const res = await ddb.send(new GetCommand({
      TableName: bookingTable,
      Key: { tenantId, booking_id: bookingId },
    }));
    return res.Item || null;
  }

  return { loadState, saveState, loadBooking };
}

module.exports = { buildSchedulingDeps };
