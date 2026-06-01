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
 *     booked → canceled` + `cancel_reason='coordinator_moved'` + set
 *     `rescheduleOfBookingId` (see the semantic note below). Does NOT auto-create the
 *     replacement booking (deferred — that is C8 + a re-pool).
 *   - reassignCoordinator       (calendar_reassigned, §14.2) — point `resource_id` /
 *     `coordinator_email` at the new organizer. NO status change, NO notification
 *     (agent-of-CoR §5.1: Google's attendee-update email already covers it).
 *
 * Every write is guarded by `attribute_exists(booking_id)` so a stray event can NEVER
 * create a ghost Booking row (an UpdateItem against an absent key would otherwise insert
 * one). ConditionalCheckFailed is translated to a `false` return (idempotent no-op); any
 * other DDB error propagates so the SQS record redrives. New attributes are additive
 * non-key (`canceled_at` / `cancel_reason` / `rescheduleOfBookingId` / `reassigned_at`) —
 * schema discipline: every reader tolerates their absence.
 *
 * `rescheduleOfBookingId` SEMANTIC (v1, PRODUCED here — see PR report-back for §A
 * codification): on a coordinator-MOVED cancellation, v1 does NOT create the replacement
 * booking, so the only row that exists is the canceled original. It is stamped
 * `rescheduleOfBookingId = <its own booking_id>` as a self-anchor sentinel marking it as a
 * reschedule source awaiting rebook. When the replacement IS later created (a future
 * wave), the canonical chain is the NEW booking carrying `rescheduleOfBookingId =
 * <original booking_id>`. FLAGGED to the integrator for confirmation.
 *
 * Consumes `shared/booking-status` (CI-3c SoT) so the status literals can never drift from
 * the canonical vocabulary — a drift trips at module load, not in production.
 */

const {
  DynamoDBClient,
  UpdateItemCommand,
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
      UpdateExpression: 'SET #st = :canceled, canceled_at = :at, cancel_reason = :r',
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

// ─── calendar_moved (v1 SCOPE): coordinator changed the time → cancel + self-anchor ──────
//
// Returns true iff THIS call performed the booked→canceled transition; false on a
// re-delivery / already-canceled row. v1 does NOT auto-create the replacement booking; it
// records `cancel_reason='coordinator_moved'` + a self-anchor `rescheduleOfBookingId` so
// the (stubbed) reschedule path / a future rebook wave can find moved bookings awaiting
// rebook. See the module-header semantic note.
async function cancelOnCoordinatorMove({ tenantId, bookingId, now = nowIso() }) {
  if (!tenantId || !bookingId) {
    throw new Error('cancelOnCoordinatorMove requires tenantId, bookingId');
  }
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: BOOKING_KEY(tenantId, bookingId),
      UpdateExpression:
        'SET #st = :canceled, canceled_at = :at, cancel_reason = :r, rescheduleOfBookingId = :self',
      ConditionExpression: 'attribute_exists(booking_id) AND #st = :booked',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':canceled': s(STATUS_CANCELED),
        ':booked': s(STATUS_BOOKED),
        ':at': s(now),
        ':r': s(CANCEL_REASON_MOVED),
        ':self': s(bookingId),
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

module.exports = {
  cancelOnCoordinatorDelete,
  cancelOnCoordinatorMove,
  reassignCoordinator,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
  _CANCEL_REASON_DELETED: CANCEL_REASON_DELETED,
  _CANCEL_REASON_MOVED: CANCEL_REASON_MOVED,
};
