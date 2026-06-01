'use strict';

/**
 * booking-store.js — Booking-table reads/writes for the B11 stranded-booking remediator.
 *
 * The Booking table (FROZEN_CONTRACTS §A): PK `tenantId` · SK `booking_id`; GSIs
 * `tenantId-start_at-index` and `tenantId-coordinator_email-index`. Field names mirror
 * the C8 writer (Booking_Commit_Handler/booking-store.js buildBookingItem) — B11 must
 * read exactly what C8 wrote. All reads are forward-compatible (schema discipline): an
 * optional field absent on an older row yields a default, never a crash.
 *
 * B11 owns two table operations:
 *
 *   findStrandedBookings — query `tenantId-coordinator_email-index` for the departed
 *     coordinator, then keep only rows that are STRANDED per canonical §7.3:
 *       item_type === 'booking'                         (exclude slot_lock / degraded markers)
 *       status    === 'booked'                          (active, not already terminal)
 *       last_calendar_mutation_at < offboarding_time    (the calendar admin did NOT
 *                                                         already address this booking;
 *                                                         a later mutation means it was)
 *     The status + item_type filters run server-side (exact string match — safe in a
 *     FilterExpression); the time comparison runs in code via Date.parse so a differing
 *     RFC3339 offset can't mis-sort under DynamoDB's lexicographic string `<`.
 *
 *   reassignBookingResource — handling (a) commit: repoint a booking at the new
 *     coordinator. Conditional UpdateItem guarded on (still booked) AND (still the
 *     departed resource) so a concurrent change (e.g. the volunteer cancelled, or the
 *     §14.2 listener already moved it) makes this a no-op rather than clobbering newer
 *     state. coordinator_email is a GSI key, so the row relocates in
 *     `tenantId-coordinator_email-index` to the new coordinator — correct.
 */

const {
  DynamoDBClient,
  QueryCommand,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { sdkConfig } = require('./aws-client-config');

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
const COORDINATOR_INDEX = 'tenantId-coordinator_email-index';

const ddb = new DynamoDBClient(sdkConfig());

function s(value) {
  return { S: String(value) };
}

// Parse a Booking row into a plain object. Forward-compatible: optional fields absent
// on an older row default rather than crash (CLAUDE.md Schema Discipline).
function parseBookingRow(item) {
  return {
    tenantId:              item.tenantId?.S              ?? null,
    bookingId:             item.booking_id?.S            ?? null,
    itemType:              item.item_type?.S             ?? 'booking',
    status:                item.status?.S               ?? null,
    startAt:               item.start_at?.S             ?? null,
    endAt:                 item.end_at?.S               ?? null,
    coordinatorEmail:      item.coordinator_email?.S    ?? null,
    resourceId:            item.resource_id?.S          ?? null,
    appointmentTypeId:     item.appointment_type_id?.S  ?? '',
    externalEventId:       item.external_event_id?.S    ?? null,
    timezone:              item.timezone?.S             ?? 'UTC',
    lastCalendarMutationAt: item.last_calendar_mutation_at?.S ?? null,
    attendeeEmail:         item.attendee_email?.S       ?? null,
    sessionId:             item.session_id?.S           ?? null,
  };
}

// A booking is stranded only if its last calendar mutation predates the offboarding
// moment (the admin didn't already reassign/cancel it calendar-side). A row missing
// last_calendar_mutation_at is treated as stranded (can't prove it was addressed) —
// C8 always writes it (defaulting to created_at), so this only guards malformed rows.
function isStranded(booking, offboardingMs) {
  if (booking.itemType !== 'booking') return false;
  if (booking.status !== 'booked') return false;
  if (!booking.lastCalendarMutationAt) return true;
  const mutatedMs = Date.parse(booking.lastCalendarMutationAt);
  if (Number.isNaN(mutatedMs)) return true;
  return mutatedMs < offboardingMs;
}

async function findStrandedBookings({ tenantId, coordinatorEmail, offboardingTime }) {
  const offboardingMs = Date.parse(offboardingTime);
  if (Number.isNaN(offboardingMs)) {
    throw new Error('offboarding_time must be a parseable date/time');
  }

  const stranded = [];
  let lastKey;
  do {
    const resp = await ddb.send(
      new QueryCommand({
        TableName: BOOKING_TABLE,
        IndexName: COORDINATOR_INDEX,
        KeyConditionExpression: 'tenantId = :t AND coordinator_email = :email',
        // Exact-match discriminators server-side; the time comparison is done in code.
        FilterExpression: 'item_type = :bk AND #st = :booked',
        ExpressionAttributeNames: { '#st': 'status' },
        ExpressionAttributeValues: {
          ':t': s(tenantId),
          ':email': s(coordinatorEmail),
          ':bk': s('booking'),
          ':booked': s('booked'),
        },
        ExclusiveStartKey: lastKey,
      })
    );
    for (const item of resp.Items ?? []) {
      const booking = parseBookingRow(item);
      if (isStranded(booking, offboardingMs)) {
        stranded.push(booking);
      }
    }
    lastKey = resp.LastEvaluatedKey;
  } while (lastKey);

  return stranded;
}

async function reassignBookingResource({
  tenantId,
  bookingId,
  fromResourceId,
  newResourceId,
  newCoordinatorEmail,
  mutationAt,
}) {
  await ddb.send(
    new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: { tenantId: s(tenantId), booking_id: s(bookingId) },
      UpdateExpression:
        'SET resource_id = :new, coordinator_email = :email, last_calendar_mutation_at = :at',
      // Optimistic guard: only repoint a row that (still) exists, is still booked, and
      // still belongs to the departed coordinator. attribute_exists(booking_id) gives
      // parity with the B9B10 writes (never resurrect a deleted row via UpdateItem). A
      // ConditionalCheckFailed means newer state won — the caller treats it as
      // "already handled", not an error.
      ConditionExpression: 'attribute_exists(booking_id) AND #st = :booked AND resource_id = :old',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':new': s(newResourceId),
        ':email': s(newCoordinatorEmail),
        ':at': s(mutationAt),
        ':booked': s('booked'),
        ':old': s(fromResourceId),
      },
    })
  );
}

function isConditionalCheckFailed(err) {
  return Boolean(err) && err.name === 'ConditionalCheckFailedException';
}

module.exports = {
  findStrandedBookings,
  reassignBookingResource,
  parseBookingRow,
  isStranded,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
  _COORDINATOR_INDEX: COORDINATOR_INDEX,
};
