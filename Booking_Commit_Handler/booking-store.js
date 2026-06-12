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
const { sdkConfig } = require('./aws-client-config');

const ddb = new DynamoDBClient(sdkConfig());

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
// Slot locks self-expire as a backstop to the explicit release, so a crash between
// lock acquisition and release can never strand a slot forever. Far longer than the
// 60s commit SLA, so it never fires on a live commit. Requires DDB TTL enabled on
// `lock_expires_at` (integrator IaC — real Booking rows never carry this attr, so
// they never expire).
const SLOT_LOCK_TTL_SECONDS = Number(process.env.SLOT_LOCK_TTL_SECONDS || 600);

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
  // Track 1 S1.1: persist the display name + appointment-type name so a later in-chat
  // reschedule (which loads this row to re-bind reminders) renders real reminder copy
  // instead of the generic "your appointment with us" / "appointment" fallbacks.
  if (b.organizationName) item.organization_name = s(b.organizationName);
  if (b.appointmentTypeName) item.appointment_type_name = s(b.appointmentTypeName);
  if (b.conferenceId) item.conference_id = s(b.conferenceId);
  if (b.joinUrl) item.channel_details = s(b.joinUrl);
  // E16-descope replacement: the dashboard deep-links "Open in Google Calendar" from
  // this (ADA already projects it; BookingCard already renders it when present).
  if (b.htmlLink) item.html_link = s(b.htmlLink);
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

// Tier-2 reschedule persistence (option A): after the executor moves the calendar
// event, write the booking's NEW fields (start_at — also the GSI key — + the new
// external_event_id + the pending_calendar_sync flag). UpdateItem (not writeBooking,
// which is conditional-create); guarded on the booking already existing. CANCEL is
// NOT persisted here — the §14.2 cal-lifecycle listener owns Booking.status on delete.
async function updateBookingReschedule(tenantId, bookingId, { startAt, externalEventId, pendingCalendarSync } = {}) {
  if (!tenantId || !bookingId) throw new Error('updateBookingReschedule requires tenantId and bookingId');
  const sets = ['last_calendar_mutation_at = :now'];
  const vals = { ':now': s(new Date().toISOString()) };
  if (startAt) { sets.push('start_at = :sa'); vals[':sa'] = s(startAt); }
  if (externalEventId) { sets.push('external_event_id = :eid'); vals[':eid'] = s(externalEventId); }
  if (pendingCalendarSync !== undefined) { sets.push('pending_calendar_sync = :pcs'); vals[':pcs'] = { BOOL: !!pendingCalendarSync }; }
  await ddb.send(new UpdateItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, bookingId),
    UpdateExpression: 'SET ' + sets.join(', '),
    ExpressionAttributeValues: vals,
    ConditionExpression: 'attribute_exists(booking_id)',
  }));
}

// G6 cancel-with-reason — persist the audit-only cancel reason + actor. The Booking.status
// flip is the §14.2 listener's job (the calendar delete drives it); this writes ONLY the
// reason/actor attributes. ConditionExpression guards a vanished row (a concurrent delete).
async function updateBookingCancelReason(tenantId, bookingId, { reason, canceledBy } = {}) {
  if (!tenantId || !bookingId) throw new Error('updateBookingCancelReason requires tenantId and bookingId');
  // Guard a non-string/empty reason → never marshal `String(undefined)`='undefined' into DDB.
  if (typeof reason !== 'string' || !reason) throw new Error('updateBookingCancelReason requires a non-empty reason string');
  const sets = ['cancel_reason = :r'];
  const vals = { ':r': s(reason) };
  if (canceledBy) { sets.push('canceled_by = :by'); vals[':by'] = s(canceledBy); }
  await ddb.send(new UpdateItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, bookingId),
    UpdateExpression: 'SET ' + sets.join(', '),
    ExpressionAttributeValues: vals,
    ConditionExpression: 'attribute_exists(booking_id)',
  }));
}

// G6 reschedule-link rate limit (anti email-bombing): atomically claim a send-slot by writing
// reschedule_link_sent_at ONLY if it is unset or older than cooldownSeconds. Returns true if the
// slot was claimed (caller may proceed to mint+notify); false (ConditionalCheckFailed) if a send
// happened within the cooldown — the caller then refuses without minting a fresh token. The
// conditional write is atomic, so two simultaneous POSTs cannot both claim the slot (no TOCTOU).
async function touchRescheduleLinkSentAt(tenantId, bookingId, cooldownSeconds, nowMs = Date.now()) {
  if (!tenantId || !bookingId) throw new Error('touchRescheduleLinkSentAt requires tenantId and bookingId');
  const now = new Date(nowMs).toISOString();
  const cutoff = new Date(nowMs - cooldownSeconds * 1000).toISOString();
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: BOOKING_KEY(tenantId, bookingId),
      UpdateExpression: 'SET reschedule_link_sent_at = :now',
      // booking must exist AND (no prior send OR the prior send is older than the cooldown).
      ConditionExpression: 'attribute_exists(booking_id) AND (attribute_not_exists(reschedule_link_sent_at) OR reschedule_link_sent_at < :cutoff)',
      ExpressionAttributeValues: { ':now': s(now), ':cutoff': s(cutoff) },
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false; // within cooldown (or vanished row) → refuse
    throw err;
  }
}

// Stamp a TTL on the lock item right after acquisition (the lock is created by C6
// pool.lockSlot, which this module must not modify — so C8 adds the TTL attribute
// via UpdateItem). DynamoDB TTL then garbage-collects any lock orphaned by a crash
// between acquisition and release.
async function setLockTtl(tenantId, lockKey, nowMs = Date.now()) {
  const expiresAt = Math.floor(nowMs / 1000) + SLOT_LOCK_TTL_SECONDS;
  await ddb.send(new UpdateItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, lockKey),
    UpdateExpression: 'SET lock_expires_at = :e',
    ExpressionAttributeValues: { ':e': { N: String(expiresAt) } },
  }));
  return expiresAt;
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
  updateBookingReschedule,
  updateBookingCancelReason,
  touchRescheduleLinkSentAt,
  readLock,
  recordConferenceOnLock,
  setLockTtl,
  releaseLock,
  flagLockForReconciliation,
  writeDegradedMarker,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
};
