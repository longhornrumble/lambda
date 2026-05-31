'use strict';

/**
 * booking-store.js — Booking-table reads/writes for the C8 commit.
 *
 * The Booking table (FROZEN_CONTRACTS §A): PK `tenantId` · SK `booking_id`; GSIs
 * `tenantId-start_at-index` and `tenantId-coordinator_email-index`. The slot-lock
 * primitive (C6 pool.lockSlot) lives in this SAME table as a discriminated
 * `item_type='slot_lock'` item whose SK is the deterministic lock key — C8 owns the
 * lock RELEASE + reconciliation (the C6→C8 deferral).
 *
 * ⚑ Flagged for the integrator (FROZEN §C — not forked): FROZEN §A says C8 sets
 * `extendedProperties.private.booking_id = <Booking PK>`. The Booking PK attribute is
 * `tenantId`; the per-booking identifier is the SK `booking_id`. The ownership tag
 * must carry the SK (the unique booking id) so the B2 listener can resolve ONE
 * booking — pool.js's lock comment ("SK booking_id") confirms this. Built to the SK;
 * suggest tightening the §A wording from "PK" to "the booking_id (SK)".
 *
 * Idempotency model (AC #6 — double-tap / network-retry never double-books):
 *   - `booking_id` is DETERMINISTIC over (tenantId, sessionId, start) → a retry of
 *     the same confirm computes the same SK.
 *   - Step-0 gate: getBookingById() — if a `booked` row already exists, the commit
 *     short-circuits and returns it ("already confirmed", C11). No new event.
 *   - The C6 slot-lock is the concurrency mutex; the Booking write is a conditional
 *     PutItem (`attribute_not_exists(booking_id)`) so a race loser never overwrites.
 */

const crypto = require('crypto');
const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { isBookingStatus } = require('../shared/booking-status');

const ddb = new DynamoDBClient({});

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;

// ─── deterministic booking id (the idempotency key) ──────────────────────────────────

function buildBookingId(tenantId, sessionId, start) {
  if (!tenantId || !sessionId || !start) {
    throw new Error('tenantId, sessionId, and start are required to derive booking_id');
  }
  const digest = crypto
    .createHash('sha256')
    .update(`${tenantId}|${sessionId}|${start}`)
    .digest('hex')
    .slice(0, 32);
  return `booking#${digest}`;
}

const BOOKING_KEY = (tenantId, bookingId) => ({
  tenantId: { S: tenantId },
  booking_id: { S: bookingId },
});

// ─── marshalling ─────────────────────────────────────────────────────────────────────

function s(value) {
  return { S: String(value) };
}

// Build the Booking item. GSI key attributes (start_at, coordinator_email) MUST be
// present so the B9/B11/E9 range/coordinator queries find real bookings. start_at is
// stored as the same RFC3339 dateTime Google returns for a timed event (Phase 2b
// audit row 9: a date-only vs dateTime mismatch would break calendar_moved/OOO).
function buildBookingItem(b) {
  if (!isBookingStatus(b.status)) {
    throw new Error(`refusing to write illegal Booking.status: ${b.status}`);
  }
  const item = {
    tenantId: s(b.tenantId),
    booking_id: s(b.bookingId),
    item_type: s('booking'),
    status: s(b.status),
    start_at: s(b.start), // GSI key (tenantId-start_at-index)
    end_at: s(b.end),
    coordinator_email: s(b.coordinatorEmail), // GSI key (tenantId-coordinator_email-index)
    resource_id: s(b.resourceId),
    session_id: s(b.sessionId),
    appointment_type_id: s(b.appointmentTypeId || ''),
    attendee_email: s(b.attendeeEmail),
    external_event_id: s(b.externalEventId),
    external_provider: s('google'),
    conference_provider: s(b.conferenceProvider || 'null'),
    timezone: s(b.timezone || 'UTC'),
    idempotency_key: s(b.bookingId),
    created_at: s(b.createdAt),
    last_calendar_mutation_at: s(b.lastCalendarMutationAt || b.createdAt),
  };
  // Optional / PII fields — only set when present (schema discipline; forward-compat).
  if (b.attendeeName) item.attendee_name = s(b.attendeeName);
  if (b.attendeePhone) item.attendee_phone = s(b.attendeePhone);
  if (b.conferenceId) item.conference_id = s(b.conferenceId);
  if (b.joinUrl) item.channel_details = s(b.joinUrl);
  if (b.rescheduleOfBookingId) item.reschedule_of_booking_id = s(b.rescheduleOfBookingId);
  return item;
}

// ─── idempotency gate (step 0) ────────────────────────────────────────────────────────

async function getBookingById(tenantId, bookingId) {
  const res = await ddb.send(new GetItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, bookingId),
  }));
  return res.Item || null;
}

// ─── conditional write ────────────────────────────────────────────────────────────────

function isConditionalCheckFailed(err) {
  return Boolean(err) && err.name === 'ConditionalCheckFailedException';
}

async function writeBooking(bookingFields) {
  const Item = buildBookingItem(bookingFields);
  await ddb.send(new PutItemCommand({
    TableName: BOOKING_TABLE,
    Item,
    ConditionExpression: 'attribute_not_exists(booking_id)',
  }));
  return Item;
}

// ─── slot-lock lifecycle (C6→C8 deferral) ─────────────────────────────────────────────

// Recover any conference id a prior partial attempt recorded on the lock (Zoom
// read-before-write), so a retry reuses the meeting rather than creating a duplicate.
async function readLock(tenantId, lockKey) {
  const res = await ddb.send(new GetItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, lockKey),
  }));
  return res.Item || null;
}

// Persist a freshly-minted conference id on the lock item BEFORE events.insert, so a
// later retry can reuse it (the only durable artifact between create-conference and
// the Booking write).
async function recordConferenceOnLock(tenantId, lockKey, { conferenceId, provider }) {
  if (!conferenceId) return;
  await ddb.send(new UpdateItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, lockKey),
    UpdateExpression: 'SET conference_id = :c, conference_provider = :p',
    ExpressionAttributeValues: {
      ':c': s(conferenceId),
      ':p': s(provider || 'unknown'),
    },
  }));
}

// Unconditional release — called on BOTH success and every failure path. Idempotent
// (a missing item is a no-op). This is the C6→C8 lock-release deferral.
async function releaseLock(tenantId, lockKey) {
  await ddb.send(new DeleteItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, lockKey),
  }));
}

// When compensation could NOT fully clean up (e.g. orphan Zoom meeting delete failed),
// leave the lock item with a queryable reconciliation flag instead of releasing it, so
// the ops sweep (runbook: orphan `slot_lock#` items) can find + finish the cleanup.
async function flagLockForReconciliation(tenantId, lockKey, reason) {
  await ddb.send(new UpdateItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, lockKey),
    UpdateExpression: 'SET needs_reconciliation = :t, reconciliation_reason = :r, flagged_at = :a',
    ExpressionAttributeValues: {
      ':t': { BOOL: true },
      ':r': s(String(reason).slice(0, 256)),
      ':a': s(new Date().toISOString()),
    },
  }));
}

// ─── durable degraded-coordinator marker (§5.5 row 4) ─────────────────────────────────

// A discriminated item (item_type='coordinator_degraded') — NOT under the Booking GSI
// key names, so it never surfaces as a booking. The in-memory circuit-breaker in
// pool.js is the per-invocation optimization; THIS is the durable record the work-order
// requires + an admin alert (sent by index.js). A dedicated coordinator-state surface
// is integrator/E-phase territory; this is the minimal durable marker.
async function writeDegradedMarker(tenantId, coordinatorId, reason) {
  await ddb.send(new PutItemCommand({
    TableName: BOOKING_TABLE,
    Item: {
      tenantId: s(tenantId),
      booking_id: s(`coordinator_degraded#${coordinatorId}`),
      item_type: s('coordinator_degraded'),
      degraded_resource_id: s(coordinatorId),
      reason: s(String(reason).slice(0, 256)),
      degraded_at: s(new Date().toISOString()),
    },
  }));
}

module.exports = {
  buildBookingId,
  buildBookingItem,
  getBookingById,
  writeBooking,
  readLock,
  recordConferenceOnLock,
  releaseLock,
  flagLockForReconciliation,
  writeDegradedMarker,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
};
