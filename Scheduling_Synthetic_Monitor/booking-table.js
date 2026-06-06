'use strict';

/**
 * booking-table.js — the synthetic monitor's reads/writes against the Booking table.
 *
 * The Booking table (FROZEN_CONTRACTS §A): PK `tenantId` · SK `booking_id`; rows carry
 * `item_type='booking'` (locks + degraded-markers use other discriminators). Attribute
 * names are read from the SHIPPED C8 writer (Booking_Commit_Handler/booking-store.js):
 * `status`, `start_at`, `end_at`, `coordinator_email`, `resource_id`, `external_event_id`,
 * `conference_provider`, `timezone`, `created_at` (ISO8601 string).
 *
 * This module NEVER edits C8's writer. It only:
 *   - reads a row back (commit omits coordinator_email per §5.7; cancel needs it),
 *   - stamps `is_synthetic=true` on rows the monitor itself created (§E6 — additive attr,
 *     schema-discipline; the monitor marks its OWN rows, it does not change the contract),
 *   - queries + deletes stale synthetic rows for nightly hygiene (bounded to ONE tenant
 *     partition — never a full-table scan, never cross-tenant).
 */

const {
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');
const { sdkConfig } = require('./aws-clients');

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
const ddb = new DynamoDBClient(sdkConfig());

function key(tenantId, bookingId) {
  return { tenantId: { S: tenantId }, booking_id: { S: bookingId } };
}

function str(attr) {
  return attr && attr.S != null ? attr.S : null;
}

/**
 * Read a Booking row as a plain object. Both `tenantId`/`tenant_id` and snake-case
 * fields are returned so the BCH scheduling_mutate cancel path (which does
 * `booking[camel] ?? booking[snake]`) accepts it as-is. Schema discipline: every field
 * tolerates absence (null).
 */
async function getBooking(tenantId, bookingId, { client = ddb } = {}) {
  const res = await client.send(
    new GetItemCommand({ TableName: BOOKING_TABLE, Key: key(tenantId, bookingId) })
  );
  if (!res || !res.Item) return null;
  const it = res.Item;
  return {
    tenantId: str(it.tenantId) ?? tenantId,
    tenant_id: str(it.tenantId) ?? tenantId,
    booking_id: str(it.booking_id) ?? bookingId,
    status: str(it.status),
    start_at: str(it.start_at),
    end_at: str(it.end_at),
    timezone: str(it.timezone),
    coordinator_email: str(it.coordinator_email),
    resource_id: str(it.resource_id),
    external_event_id: str(it.external_event_id),
    conference_provider: str(it.conference_provider),
    conference_id: str(it.conference_id),
    created_at: str(it.created_at),
    item_type: str(it.item_type),
    is_synthetic: Boolean(it.is_synthetic && it.is_synthetic.BOOL === true),
  };
}

/**
 * Stamp `is_synthetic=true` on a booking the monitor just created (§E6). Guarded on the
 * row existing (a synthetic row the monitor itself wrote). Additive attribute only.
 */
async function stampSynthetic(tenantId, bookingId, { client = ddb } = {}) {
  await client.send(
    new UpdateItemCommand({
      TableName: BOOKING_TABLE,
      Key: key(tenantId, bookingId),
      UpdateExpression: 'SET is_synthetic = :t',
      ExpressionAttributeValues: { ':t': { BOOL: true } },
      ConditionExpression: 'attribute_exists(booking_id)',
    })
  );
}

/**
 * Query the synthetic tenant's bookings created before `cutoffIso`. Bounded to a single
 * partition (KeyConditionExpression on the PK) — never a full-table scan, never
 * cross-tenant. `created_at` is ISO8601, so a lexicographic `<` compare is chronological.
 * Filters to `item_type='booking'` AND `is_synthetic=true` so locks/markers/real rows are
 * never returned. Paginates fully.
 */
async function querySyntheticOlderThan(tenantId, cutoffIso, { client = ddb } = {}) {
  const out = [];
  let ExclusiveStartKey;
  do {
    const res = await client.send(
      new QueryCommand({
        TableName: BOOKING_TABLE,
        KeyConditionExpression: 'tenantId = :t',
        FilterExpression:
          'is_synthetic = :true AND item_type = :booking AND created_at < :cut',
        ExpressionAttributeValues: {
          ':t': { S: tenantId },
          ':true': { BOOL: true },
          ':booking': { S: 'booking' },
          ':cut': { S: cutoffIso },
        },
        ExclusiveStartKey,
      })
    );
    for (const it of res.Items || []) {
      out.push({
        tenantId: str(it.tenantId),
        booking_id: str(it.booking_id),
        created_at: str(it.created_at),
        status: str(it.status), // for straggler visibility (non-canceled = possible orphan event)
      });
    }
    ExclusiveStartKey = res.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return out;
}

// Defense-in-depth: the delete is CONDITIONAL on the row being synthetic, so even if a
// caller ever passed a non-synthetic key (a query bug, a stale row), DynamoDB rejects it
// (ConditionalCheckFailed) rather than deleting a real booking. `is_synthetic` is not a
// reserved word (cf. Stranded_Booking_Remediator's unaliased `item_type` filter).
async function deleteBooking(tenantId, bookingId, { client = ddb } = {}) {
  await client.send(
    new DeleteItemCommand({
      TableName: BOOKING_TABLE,
      Key: key(tenantId, bookingId),
      ConditionExpression: 'is_synthetic = :true',
      ExpressionAttributeValues: { ':true': { BOOL: true } },
    })
  );
}

module.exports = {
  getBooking,
  stampSynthetic,
  querySyntheticOlderThan,
  deleteBooking,
  _BOOKING_TABLE: BOOKING_TABLE,
};
