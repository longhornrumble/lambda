'use strict';

/**
 * scheduled-messages-table.js — the synthetic monitor's READ of the reminder rows the
 * Track 1 scheduler (Reminder_Scheduler/scheduler.js) writes at commit.
 *
 * The reminder cadence cycle proves the FIRING path (EventBridge Scheduler →
 * Scheduled_Message_Sender → row status flip) by polling these rows until a reminder row
 * transitions `pending → sent`. That status flip is the dispatch signal — Scheduled_
 * Message_Sender sets status='sent'/'suppressed'/'failed' after it dispatches (index.mjs).
 * This is an infra-light proof: no SES inbound / Gmail polling (deferred — see README).
 *
 * Read-only. Rows are written/torn-down by the scheduler + the §14.2 lifecycle consumer;
 * the monitor never edits them. Bounded to ONE booking via the `by-appointment` GSI
 * (hash appointment_id, range pk) AND the tenant partition — never a full-table scan.
 *
 * Table (FROZEN_CONTRACTS §E1): pk `TENANT#{tenantId}` · sk `SCHEDULED#{startAtIso}#{messageId}`;
 * GSI `by-appointment` (ALL projection) keyed (appointment_id, pk). Row attrs: `status`,
 * `moment` ('reminder'), `tier` (t24h|t1h|t15m; absent on attendance rows), `fire_at`,
 * `message_id`, `channel`, `attendance_check` (BOOL, attendance rows only).
 */

const { DynamoDBClient, QueryCommand } = require('@aws-sdk/client-dynamodb');
const { sdkConfig } = require('./aws-clients');

const SCHEDULED_MESSAGES_TABLE =
  process.env.SCHEDULED_MESSAGES_TABLE || 'picasso-scheduled-messages';
const BY_APPOINTMENT_INDEX = 'by-appointment';
const ddb = new DynamoDBClient(sdkConfig());

function str(attr) {
  return attr && attr.S != null ? attr.S : null;
}

function messagePk(tenantId) {
  return `TENANT#${tenantId}`;
}

/**
 * All scheduled-message rows for one booking, via the `by-appointment` GSI. Scoped to the
 * tenant partition (pk in the key condition) so the query is single-booking, single-tenant.
 * Paginates fully. Every field tolerates absence (schema discipline).
 *
 * @returns {Promise<Array<{ sk, message_id, status, moment, tier, fire_at, channel,
 *   attendance_check }>>}
 */
async function queryByAppointment(tenantId, bookingId, { client = ddb } = {}) {
  const out = [];
  let ExclusiveStartKey;
  do {
    const res = await client.send(
      new QueryCommand({
        TableName: SCHEDULED_MESSAGES_TABLE,
        IndexName: BY_APPOINTMENT_INDEX,
        KeyConditionExpression: 'appointment_id = :a AND pk = :p',
        ExpressionAttributeValues: {
          ':a': { S: bookingId },
          ':p': { S: messagePk(tenantId) },
        },
        ExclusiveStartKey,
      })
    );
    for (const it of res.Items || []) {
      out.push({
        sk: str(it.sk),
        message_id: str(it.message_id),
        status: str(it.status),
        moment: str(it.moment),
        tier: str(it.tier),
        fire_at: str(it.fire_at),
        channel: str(it.channel),
        attendance_check: Boolean(it.attendance_check && it.attendance_check.BOOL === true),
      });
    }
    ExclusiveStartKey = res.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return out;
}

module.exports = {
  queryByAppointment,
  _SCHEDULED_MESSAGES_TABLE: SCHEDULED_MESSAGES_TABLE,
};
