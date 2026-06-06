'use strict';

/**
 * booking-store.js — AWS glue for the Attendance_Disposition_Handler (WS-E-ATTEND).
 *
 * All the side-effecting code the pure logic modules (shared/scheduling/attendance.js,
 * escalation.js, zoomOutagePaging.js) inject as `deps`. Keeping it here keeps those modules
 * pure + unit-testable; this file is exercised via aws-sdk-client-mock.
 *
 * Booking table (FROZEN §A): PK `tenantId` · SK `booking_id`; GSI `tenantId-start_at-index`.
 */

const {
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const { loadTenantConfig } = require('../shared/scheduling/featureGate');

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
const START_AT_INDEX = process.env.BOOKING_START_AT_INDEX || 'tenantId-start_at-index';
const SEND_EMAIL_FUNCTION = process.env.SEND_EMAIL_FUNCTION || 'send_email';
const SMS_SENDER_FUNCTION = process.env.SMS_SENDER_FUNCTION || 'SMS_Sender';
const ATTENDANCE_STATE_PENDING = 'pending_attendance';
const STATUS_BOOKED = 'booked';
const DAY_MS = 24 * 60 * 60 * 1000;

// Bounded SDK clients (#202 pattern — bundle @smithy/node-http-handler, don't externalize).
const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const MAX_ATTEMPTS = Number(process.env.AWS_MAX_ATTEMPTS || 2);
const ddb = new DynamoDBClient({
  maxAttempts: MAX_ATTEMPTS,
  requestHandler: new NodeHttpHandler({
    connectionTimeout: CONNECTION_TIMEOUT_MS,
    requestTimeout: REQUEST_TIMEOUT_MS,
  }),
});
const lambda = new LambdaClient({ maxAttempts: MAX_ATTEMPTS });

function s(v) {
  return { S: String(v) };
}
function str(item, name) {
  return item && item[name] && item[name].S != null ? item[name].S : null;
}

// Forward-compatible projected read of a Booking row (schema discipline — tolerate absence).
function fromItem(it) {
  if (!it) return null;
  return {
    tenantId: str(it, 'tenantId'),
    booking_id: str(it, 'booking_id'),
    status: str(it, 'status'),
    attendance_state: str(it, 'attendance_state'),
    coordinator_email: str(it, 'coordinator_email'),
    coordinator_name: str(it, 'coordinator_name'),
    coordinator_phone: str(it, 'coordinator_phone'),
    attendee_email: str(it, 'attendee_email'),
    attendee_name: str(it, 'attendee_name'),
    attendee_phone: str(it, 'attendee_phone'),
    appointment_type_name: str(it, 'appointment_type_name'),
    start_at: str(it, 'start_at'),
    end_at: str(it, 'end_at'),
    when_label: str(it, 'when_label'),
    conference_provider: str(it, 'conference_provider'),
    join_url: str(it, 'join_url'),
  };
}

async function getBooking(tenantId, bookingId) {
  const res = await ddb.send(
    new GetItemCommand({
      TableName: BOOKING_TABLE,
      Key: { tenantId: s(tenantId), booking_id: s(bookingId) },
    })
  );
  return fromItem(res && res.Item);
}

// E5 idempotent write: set the non-key attendance_state only when still `booked` AND not yet
// marked. Returns true iff THIS call set it (a re-fire / reconciler-already-marked → false).
async function setAttendanceState({ tenantId, bookingId }) {
  try {
    await ddb.send(
      new UpdateItemCommand({
        TableName: BOOKING_TABLE,
        Key: { tenantId: s(tenantId), booking_id: s(bookingId) },
        UpdateExpression: 'SET attendance_state = :pending, attendance_check_at = :at',
        ConditionExpression:
          'attribute_exists(booking_id) AND #st = :booked AND attribute_not_exists(attendance_state)',
        ExpressionAttributeNames: { '#st': 'status' },
        ExpressionAttributeValues: {
          ':pending': s(ATTENDANCE_STATE_PENDING),
          ':booked': s(STATUS_BOOKED),
          ':at': s(new Date().toISOString()),
        },
      })
    );
    return true;
  } catch (err) {
    if (err && err.name === 'ConditionalCheckFailedException') return false;
    throw err;
  }
}

// Tenant-config notification recipients for escalation cc / urgent / digest. Tolerant of a
// few likely config shapes; fail-closed to [] (escalation degrades to no-cc, never throws).
// ⚑ The exact config key is a tenant-config convention seam — confirm with config-builder.
async function getAdminEmails(tenantId) {
  try {
    const config = await loadTenantConfig(tenantId);
    const sched = (config && config.scheduling) || {};
    const candidates = []
      .concat(sched.notification_emails || [])
      .concat(sched.admin_emails || [])
      .concat(sched.admin_email ? [sched.admin_email] : [])
      .concat(config && config.notification_emails ? config.notification_emails : []);
    return Array.from(new Set(candidates.filter((e) => typeof e === 'string' && e)));
  } catch (_) {
    return [];
  }
}

// t7d digest enumerator: bounded GSI query (NO full-table scan — §E7 pattern). Returns the
// still-pending_attendance, still-booked rows whose start_at is between [window start] and
// [the >7d cutoff]. start_at is ISO8601, so lexicographic BETWEEN == chronological order.
// Hard-capped at MAX_PENDING rows so a huge backlog can't exhaust memory (S5; the digest
// itself further caps the rendered rows). ⚑ Overlaps the WS-E-REMIND E9 reconciler's bounded
// scan — integrator may consolidate.
const MAX_PENDING = 500;
async function queryPendingAttendance({ tenantId, olderThanDays = 7, now = Date.now() }) {
  const cutoffEnd = new Date(now - olderThanDays * DAY_MS).toISOString(); // newest included (>7d old)
  const windowStart = new Date(now - 90 * DAY_MS).toISOString(); // oldest included (90d bound)
  const out = [];
  let ExclusiveStartKey;
  do {
    const res = await ddb.send(
      new QueryCommand({
        TableName: BOOKING_TABLE,
        IndexName: START_AT_INDEX,
        KeyConditionExpression: 'tenantId = :t AND start_at BETWEEN :windowStart AND :cutoffEnd',
        FilterExpression: 'attendance_state = :pending AND #st = :booked',
        ExpressionAttributeNames: { '#st': 'status' },
        ExpressionAttributeValues: {
          ':t': s(tenantId),
          ':windowStart': s(windowStart),
          ':cutoffEnd': s(cutoffEnd),
          ':pending': s(ATTENDANCE_STATE_PENDING),
          ':booked': s(STATUS_BOOKED),
        },
        ExclusiveStartKey,
      })
    );
    for (const it of res.Items || []) out.push(fromItem(it));
    ExclusiveStartKey = res.LastEvaluatedKey;
  } while (ExclusiveStartKey && out.length < MAX_PENDING);
  return out;
}

// Email via the shared send_email Lambda (async/Event, best-effort). `to`/`cc` may be a
// string or array. send_email reads event.body as a JSON string (API-Gateway shaped).
async function sendEmail({ tenantId, to, cc, subject, html_body, text_body }) {
  const toArr = Array.isArray(to) ? to : to ? [to] : [];
  const inner = {
    to: toArr,
    subject,
    html_body,
    text_body,
    tags: { tenant_id: String(tenantId || 'unknown').slice(0, 256), email_type: 'scheduling_attendance' },
  };
  if (cc && (Array.isArray(cc) ? cc.length : true)) inner.cc = Array.isArray(cc) ? cc : [cc];
  await lambda.send(
    new InvokeCommand({
      FunctionName: SEND_EMAIL_FUNCTION,
      InvocationType: 'Event',
      Payload: Buffer.from(JSON.stringify({ body: JSON.stringify(inner) })),
    })
  );
}

// SMS via the shared SMS_Sender Lambda (async/Event). sendType:'internal' (staff, bypasses
// the contact consent gate) or 'contact' (consent-gated inside SMS_Sender).
async function sendSms({ tenantId, to, body, sendType }) {
  await lambda.send(
    new InvokeCommand({
      FunctionName: SMS_SENDER_FUNCTION,
      InvocationType: 'Event',
      Payload: Buffer.from(
        JSON.stringify({
          to,
          body,
          tenantId,
          type: 'reminder',
          sendType: sendType || 'contact',
          sessionId: '',
        })
      ),
    })
  );
}

// Customer-Portal inbox alert surface — PRODUCED by E10, CONSUMED by WS-E-PORTAL.
// ⚑ STUB: WS-E-PORTAL owns the durable inbox surface (table/shape). Until it lands this logs
// the alert (so it is observable) and returns a stub marker; escalation treats a throw as a
// non-fatal best-effort failure regardless.
async function writePortalInboxAlert({ tenantId, bookingId, kind, createdAt }) {
  console.log(
    JSON.stringify({
      event: 'portal_inbox_alert_stub',
      note: 'TODO(WS-E-PORTAL): persist to the Customer-Portal inbox surface',
      tenant_id: tenantId,
      booking_id: bookingId,
      kind,
      created_at: createdAt,
    })
  );
  return { stub: true };
}

// C13 reachability probe seam. The T-15 trigger (WS-E-REMIND/integrator) determines Zoom
// reachability — either by passing `zoom_unreachable` on the event, or (future) wiring the
// Booking_Commit_Handler/zoom-client.getMeeting probe here. We honor an explicit event signal
// and otherwise assume reachable (no page) — fail-SAFE for the unwired default.
// ⚑ FLAGGED: real probe wiring is integrator glue.
function makeZoomReachableProbe(event) {
  return async function checkZoomReachable() {
    if (event && event.zoom_unreachable === true) return false;
    return true;
  };
}

module.exports = {
  getBooking,
  setAttendanceState,
  getAdminEmails,
  queryPendingAttendance,
  sendEmail,
  sendSms,
  writePortalInboxAlert,
  makeZoomReachableProbe,
  fromItem,
  _BOOKING_TABLE: BOOKING_TABLE,
  _START_AT_INDEX: START_AT_INDEX,
};
