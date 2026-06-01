'use strict';

/**
 * booking-updates.js — Booking-table conditional writes for B9 + B10.
 *
 * The Booking table (FROZEN_CONTRACTS §A): PK `tenantId` · SK `booking_id`. This module
 * performs ONLY the two writes the listener-driven consumer needs, both as idempotent
 * conditional `UpdateItem`s so SQS at-least-once / duplicate Google pushes never produce
 * a wrong outcome:
 *
 *   - flagOooConflict (B9, §14.2) — mark a `booked` booking as OOO-conflicted. New,
 *     additive non-key attributes ONLY (`ooo_conflict_status` / `ooo_conflict_at` /
 *     `ooo_conflict_mutation_at` + optional `ooo_conflict_start_at`/`_end_at`). Does NOT
 *     touch the PK/SK/GSI keys — schema discipline: additive fields, every reader
 *     tolerates their absence.
 *   - cancelOnDecline (B10, §14.2) — transition `status booked → canceled` + stamp
 *     `canceled_at` / `cancel_reason`.
 *
 * Every write is guarded by `attribute_exists(booking_id)` so a stray event can NEVER
 * create a ghost Booking row (an UpdateItem against an absent key would otherwise insert
 * one). ConditionalCheckFailed is translated to a `false` return (idempotent no-op);
 * any other DDB error propagates so the SQS record redrives.
 *
 * Consumes `shared/booking-status` (CI-3c SoT) so the status literals can never drift
 * from the canonical vocabulary — a drift trips at module load, not in production.
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

// Tie our literals to the CI-3c single-source-of-truth vocabulary (matches C8's
// discipline): a vocabulary drift fails fast at module load rather than silently writing
// an illegal status in production.
const STATUS_BOOKED = 'booked';
const STATUS_CANCELED = 'canceled';
for (const v of [STATUS_BOOKED, STATUS_CANCELED]) {
  if (!isBookingStatus(v)) {
    throw new Error(`booking-updates: '${v}' is not a canonical Booking.status (shared/booking-status drift)`);
  }
}

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

// ─── B9: flag an OOO conflict on a booked booking (idempotent) ────────────────────────
//
// Returns true iff THIS call newly flagged the booking (→ caller fires the admin alert);
// false when the booking is absent, not `booked`, or already flagged for this exact
// `mutationAt` (the dedupe basis) — so the alert fires at-most-once per
// (booking, calendar-mutation), never on an SQS / push re-delivery.
async function flagOooConflict({ tenantId, bookingId, mutationAt, oooStartAt, oooEndAt, now = nowIso() }) {
  if (!tenantId || !bookingId || !mutationAt) {
    throw new Error('flagOooConflict requires tenantId, bookingId, mutationAt');
  }

  const values = {
    ':booked': s(STATUS_BOOKED),
    ':m': s(mutationAt),
    ':flagged': s('flagged'),
    ':at': s(now),
  };
  const setClauses = [
    'ooo_conflict_status = :flagged',
    'ooo_conflict_at = :at',
    'ooo_conflict_mutation_at = :m',
  ];
  if (oooStartAt) {
    setClauses.push('ooo_conflict_start_at = :os');
    values[':os'] = s(oooStartAt);
  }
  if (oooEndAt) {
    setClauses.push('ooo_conflict_end_at = :oe');
    values[':oe'] = s(oooEndAt);
  }

  try {
    await ddb.send(new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: BOOKING_KEY(tenantId, bookingId),
      UpdateExpression: `SET ${setClauses.join(', ')}`,
      ConditionExpression:
        'attribute_exists(booking_id) AND #st = :booked '
        + 'AND (attribute_not_exists(ooo_conflict_mutation_at) OR ooo_conflict_mutation_at <> :m)',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: values,
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false;
    throw err;
  }
}

// ─── B10: transition booked → canceled on a volunteer decline (idempotent) ────────────
//
// Returns true iff THIS call performed the transition; false when the booking is absent
// or not `booked` (already canceled / terminal). The conditional (`status == booked`) is
// itself the dedupe — a second decline event finds the row already canceled and no-ops.
async function cancelOnDecline({ tenantId, bookingId, now = nowIso() }) {
  if (!tenantId || !bookingId) {
    throw new Error('cancelOnDecline requires tenantId, bookingId');
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
        ':r': s('attendee_declined'),
      },
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false;
    throw err;
  }
}

module.exports = {
  flagOooConflict,
  cancelOnDecline,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
};
