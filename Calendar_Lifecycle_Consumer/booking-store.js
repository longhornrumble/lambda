'use strict';

/**
 * booking-store.js — Booking-table conditional write primitives for the §14.2
 * calendar-lifecycle reconciliation consumer.
 *
 * The Booking table (FROZEN_CONTRACTS §A): PK `tenantId` · SK `booking_id`. This module
 * performs ONLY the three coordinator-side reconciliation writes, each as an idempotent
 * conditional `UpdateItem` so SQS at-least-once / duplicate Google pushes never produce a
 * wrong outcome:
 *
 *   - cancelOnCoordinatorDelete (calendar_deleted, §14.2) — transition `status
 *     booked → canceled` + stamp `canceled_at` / `cancel_reason='coordinator_deleted'`.
 *   - cancelOnCoordinatorMove   (calendar_moved,   §14.2 v1 SCOPE) — transition `status
 *     booked → canceled` + `cancel_reason='coordinator_moved'` (which marks the
 *     moved-not-rebooked row). Does NOT auto-create the replacement booking (deferred —
 *     that is C8 + a re-pool) and does NOT write `rescheduleOfBookingId` (F2).
 *   - reassignCoordinator       (calendar_reassigned, §14.2) — point `resource_id` /
 *     `coordinator_email` at the new organizer. NO status change, NO notification
 *     (agent-of-CoR §5.1: Google's attendee-update email already covers it).
 *
 * Every write is guarded by `attribute_exists(booking_id)` so a stray event can NEVER
 * create a ghost Booking row (an UpdateItem against an absent key would otherwise insert
 * one). ConditionalCheckFailed is translated to a `false` return (idempotent no-op); any
 * other DDB error propagates so the SQS record redrives. New attributes are additive
 * non-key (`canceled_at` / `cancel_reason` / `reassigned_at`) — schema discipline: every
 * reader tolerates their absence.
 *
 * Consumes `shared/booking-status` (CI-3c SoT) so the status literals can never drift from
 * the canonical vocabulary — a drift trips at module load, not in production.
 */

const {
  DynamoDBClient,
  UpdateItemCommand,
  GetItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { isBookingStatus } = require('../shared/booking-status');
const { sdkConfig } = require('./aws-client-config');

const ddb = new DynamoDBClient(sdkConfig());

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;

// Tie our literals to the CI-3c single-source-of-truth vocabulary (matches the sibling
// consumer + C8): a vocabulary drift fails fast at module load rather than silently
// writing an illegal status in production.
const STATUS_BOOKED = 'booked';
const STATUS_CANCELED = 'canceled';
for (const v of [STATUS_BOOKED, STATUS_CANCELED]) {
  if (!isBookingStatus(v)) {
    throw new Error(`booking-store: '${v}' is not a canonical Booking.status (shared/booking-status drift)`);
  }
}

const CANCEL_REASON_DELETED = 'coordinator_deleted';
const CANCEL_REASON_MOVED = 'coordinator_moved';

// Basic email shape (F6). `new_resource_id` is the new organizer's email and lands in the
// GSI-indexed `coordinator_email` — reject a non-email so a bad payload can't pollute the
// tenantId-coordinator_email-index. Deliberately permissive (one @, a dotted domain); full
// RFC validation is neither needed nor wanted here.
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function s(value) {
  return { S: String(value) };
}

const BOOKING_KEY = (tenantId, bookingId) => ({
  tenantId: { S: tenantId },
  booking_id: { S: bookingId },
});

function isConditionalCheckFailed(err) {
  return Boolean(err) && err.name === 'ConditionalCheckFailedException';
}

function nowIso() {
  return new Date().toISOString();
}

// ─── calendar_deleted: coordinator deleted the platform event → cancel (idempotent) ──────
//
// Returns true iff THIS call performed the booked→canceled transition; false when the
// booking is absent or not `booked` (already canceled / terminal). The `status == booked`
// guard is itself the dedupe — a re-delivered deletion finds the row already canceled and
// no-ops. Volunteer-initiated cancels (events.delete) flow through this SAME path: one
// transition, one notification, by construction (§14.2).
async function cancelOnCoordinatorDelete({ tenantId, bookingId, now = nowIso() }) {
  if (!tenantId || !bookingId) {
    throw new Error('cancelOnCoordinatorDelete requires tenantId, bookingId');
  }
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: BOOKING_KEY(tenantId, bookingId),
      // if_not_exists preserves an EXISTING cancel_reason — e.g. a G6 admin cancel-with-reason
      // writes the reason (status still booked) just before this listener fires; without the
      // guard this overwrites it with the system 'coordinator_*' reason and the admin's reason
      // is silently lost (the G6 v1-MUST). A direct coordinator delete has no prior reason → the
      // system reason is written as before.
      UpdateExpression: 'SET #st = :canceled, canceled_at = :at, cancel_reason = if_not_exists(cancel_reason, :r)',
      ConditionExpression: 'attribute_exists(booking_id) AND #st = :booked',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':canceled': s(STATUS_CANCELED),
        ':booked': s(STATUS_BOOKED),
        ':at': s(now),
        ':r': s(CANCEL_REASON_DELETED),
      },
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false;
    throw err;
  }
}

// ─── calendar_moved (v1 SCOPE): coordinator changed the time → cancel ────────────────────
//
// Returns true iff THIS call performed the booked→canceled transition; false on a
// re-delivery / already-canceled row. v1 does NOT auto-create the replacement booking;
// `cancel_reason='coordinator_moved'` already marks moved-not-rebooked rows for the
// (stubbed) reschedule path / a future rebook wave. (No `rescheduleOfBookingId` write —
// integrator decision F2: that attribute means NEW→original, and self-anchoring the
// original inverts it; the canonical link is set when the replacement booking is created.)
async function cancelOnCoordinatorMove({ tenantId, bookingId, now = nowIso() }) {
  if (!tenantId || !bookingId) {
    throw new Error('cancelOnCoordinatorMove requires tenantId, bookingId');
  }
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: BOOKING_KEY(tenantId, bookingId),
      // if_not_exists preserves an EXISTING cancel_reason — e.g. a G6 admin cancel-with-reason
      // writes the reason (status still booked) just before this listener fires; without the
      // guard this overwrites it with the system 'coordinator_*' reason and the admin's reason
      // is silently lost (the G6 v1-MUST). A direct coordinator delete has no prior reason → the
      // system reason is written as before.
      UpdateExpression: 'SET #st = :canceled, canceled_at = :at, cancel_reason = if_not_exists(cancel_reason, :r)',
      ConditionExpression: 'attribute_exists(booking_id) AND #st = :booked',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':canceled': s(STATUS_CANCELED),
        ':booked': s(STATUS_BOOKED),
        ':at': s(now),
        ':r': s(CANCEL_REASON_MOVED),
      },
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false;
    throw err;
  }
}

// ─── calendar_reassigned: organizer changed → repoint resource_id/coordinator_email ──────
//
// Returns true iff THIS call repointed the booking; false when the booking is absent OR
// `resource_id` no longer matches `previousResourceId` (a re-delivery — the row already
// points at the new organizer — OR a stale event). The `resource_id == previous` guard is
// the idempotency anchor; no status change, no notification (agent-of-CoR §5.1). Both
// `resource_id` and `coordinator_email` are repointed so the
// `tenantId-coordinator_email-index` GSI reflects the new organizer.
async function reassignCoordinator({ tenantId, bookingId, previousResourceId, newResourceId, now = nowIso() }) {
  if (!tenantId || !bookingId || !previousResourceId || !newResourceId) {
    throw new Error('reassignCoordinator requires tenantId, bookingId, previousResourceId, newResourceId');
  }
  if (!EMAIL_RE.test(newResourceId)) {
    // A non-email new organizer can never validate on a redrive — tag malformed so index.js
    // routes it to the DLQ instead of retry-storming, and never writes it to the GSI field.
    const err = new Error(`reassignCoordinator: newResourceId is not an email: rejected`);
    err.malformed = true;
    throw err;
  }
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: BOOKING_KEY(tenantId, bookingId),
      UpdateExpression: 'SET resource_id = :new, coordinator_email = :new, reassigned_at = :at',
      ConditionExpression: 'attribute_exists(booking_id) AND resource_id = :prev',
      ExpressionAttributeValues: {
        ':new': s(newResourceId),
        ':prev': s(previousResourceId),
        ':at': s(now),
      },
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false;
    throw err;
  }
}

// (Y) gap C — the calendar_deleted envelope carries no attendee contact info; read the
// fields the volunteer cancel/reschedule notice needs. Projects ONLY those fields (schema
// discipline / bounds the PII surface). Returns null if the row is absent.
async function getNoticeContext({ tenantId, bookingId }) {
  if (!tenantId || !bookingId) {
    throw new Error('getNoticeContext requires tenantId, bookingId');
  }
  const res = await ddb.send(new GetItemCommand({
    TableName: BOOKING_TABLE,
    Key: BOOKING_KEY(tenantId, bookingId), // same key helper every other fn here uses
    ProjectionExpression: 'attendee_email, attendee_name, appointment_type_id, start_at',
  }));
  if (!res.Item) return null;
  const it = res.Item;
  return {
    attendeeEmail: it.attendee_email?.S ?? null,
    attendeeName: it.attendee_name?.S ?? null,
    appointmentTypeId: it.appointment_type_id?.S ?? null,
    startAt: it.start_at?.S ?? null,
  };
}

module.exports = {
  cancelOnCoordinatorDelete,
  cancelOnCoordinatorMove,
  reassignCoordinator,
  getNoticeContext,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
  _CANCEL_REASON_DELETED: CANCEL_REASON_DELETED,
  _CANCEL_REASON_MOVED: CANCEL_REASON_MOVED,
};
