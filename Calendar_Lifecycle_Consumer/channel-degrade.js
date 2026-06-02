'use strict';

/**
 * channel-degrade.js — `booking.event_made_private` handling (§14.2 / §11).
 *
 * When a coordinator makes a platform event private, the listener may lose read access to
 * the event body, so the platform can no longer programmatically verify attendance (§11)
 * for that booking. This is NOT a Booking write — the booking stays valid. Instead it is a
 * watch-channel degradation: set `status = 'event_body_private'` on the
 * `picasso-calendar-watch-channels-{env}` row (the row is keyed by `channel_id`; §14.2
 * documents the three known status values: `active`, `unwatched_renewal_failed`,
 * `event_body_private`) and fire a best-effort admin alert so a human can ask the
 * coordinator to un-private or use the manual email-based attendance prompt.
 *
 * CONTRACT GAP — RESOLVED (lambda#199 / I2-A cutover). The listener now includes
 * `channel_id` in the `event_made_private` envelope body ONLY (the one-line integrator
 * change this module anticipated). A keyed UpdateItem on the channels table needs
 * `channel_id`, and the channels table has no GSI on `tenant_id`/`coordinator_id`/`booking_id`
 * to resolve it — so the listener passes it through (it is the validated X-Goog-Channel-ID;
 * the degrade below is `tenant_id`-guarded so a mismatched channel_id cannot cross-tenant
 * degrade). The `if (channelId)` branch below is now the LIVE happy path (the channel is
 * degraded), NOT dead code — do not remove it. The version-skew fallback (old Listener,
 * new Consumer → `channel_id` absent) STILL fires the admin alert from `tenant_id`/`booking_id`
 * so degradation is never silently dropped; the record is NOT sent to the DLQ (a missing
 * `channel_id` will never appear on a redrive — DLQ would retry-storm). The
 * `event_made_private_channel_id_absent` log line surfaces any such skew in CloudWatch.
 *
 * Idempotency: the channel UpdateItem is conditional — it degrades only an `active` (or
 * status-absent) channel, so a re-delivery (already `event_body_private`) is a no-op and a
 * `unwatched_renewal_failed` channel (a more severe, unwatched state) is NOT clobbered. The
 * admin alert fires at-most-once per channel degradation (only when the conditional write
 * newly succeeds).
 */

const {
  DynamoDBClient,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const { sdkConfig } = require('./aws-client-config');

const ENV = process.env.ENVIRONMENT || 'staging';
const CHANNELS_TABLE = process.env.CALENDAR_WATCH_CHANNELS_TABLE || `picasso-calendar-watch-channels-${ENV}`;
const OPS_ALERTS_TOPIC_ARN = process.env.OPS_ALERTS_TOPIC_ARN || '';

const STATUS_EVENT_BODY_PRIVATE = 'event_body_private';
const STATUS_ACTIVE = 'active';

const ddb = new DynamoDBClient(sdkConfig());
const sns = new SNSClient(sdkConfig());

function s(value) {
  return { S: String(value) };
}
function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}
function isConditionalCheckFailed(err) {
  return Boolean(err) && err.name === 'ConditionalCheckFailedException';
}
function nowIso() {
  return new Date().toISOString();
}

// Require non-empty string fields (mirrors booking-reconcile.requireStrings). A miss throws
// a `malformed`-tagged error so index.js routes the record to the DLQ rather than silently
// processing an event with undefined identity fields.
function requireStrings(env, fields) {
  const missing = fields.filter((f) => typeof env[f] !== 'string' || env[f].length === 0);
  if (missing.length) {
    const err = new Error(`envelope missing required field(s): ${missing.join(', ')}`);
    err.malformed = true;
    throw err;
  }
}

// Conditional UpdateItem keyed on channel_id. Returns true iff THIS call newly degraded an
// active channel (→ caller fires the admin alert); false when the channel is absent, owned
// by a different tenant, or not `active` (already private — dedupe — or renewal-failed —
// not clobbered). The `tenant_id = :tid` guard is forward-safe: once channel_id enters the
// envelope, a cross-tenant channel_id cannot degrade another tenant's row (no extra GetItem).
async function degradeChannel({ channelId, tenantId, now = nowIso() }) {
  if (!channelId || !tenantId) {
    throw new Error('degradeChannel requires channelId, tenantId');
  }
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: s(channelId) },
      UpdateExpression: 'SET #st = :private, event_body_private_at = :at',
      ConditionExpression:
        'attribute_exists(channel_id) AND tenant_id = :tid AND (attribute_not_exists(#st) OR #st = :active)',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':private': s(STATUS_EVENT_BODY_PRIVATE),
        ':active': s(STATUS_ACTIVE),
        ':tid': s(tenantId),
        ':at': s(now),
      },
    }));
    return true;
  } catch (err) {
    if (isConditionalCheckFailed(err)) return false;
    throw err;
  }
}

// Best-effort admin alert (mirrors the sibling consumer's alertAdmin — never fails the
// record). The durable channel status is the source of truth; the SNS alert is a
// notification on top of it. Swallowing the SNS error keeps idempotency clean.
async function alertAdmin(detail) {
  if (!OPS_ALERTS_TOPIC_ARN) {
    warn('admin_alert_skipped_no_topic', { kind: 'booking.event_made_private' });
    return;
  }
  try {
    await sns.send(new PublishCommand({
      TopicArn: OPS_ALERTS_TOPIC_ARN,
      Subject: 'Scheduling: calendar event made private (attendance unverifiable)'.slice(0, 100),
      Message: JSON.stringify(detail),
    }));
  } catch (err) {
    warn('admin_alert_failed', { kind: 'booking.event_made_private', error: err.message });
  }
}

// Entry point for the `booking.event_made_private` event. `tenant_id` + `booking_id` are
// required (the admin alert always fires from them); `channel_id` is the forward-compatible
// field the channel UpdateItem needs (see the CONTRACT GAP note above).
async function degradeOnEventPrivate(env) {
  requireStrings(env, ['tenant_id', 'booking_id']);
  const tenantId = env.tenant_id;
  const bookingId = env.booking_id;
  const channelId = env.channel_id; // forward-compatible — see CONTRACT GAP note.

  let channelDegraded = false;
  if (channelId) {
    channelDegraded = await degradeChannel({ channelId, tenantId });
  } else {
    // The channel cannot be degraded without channel_id, and it will never appear on a
    // redrive — escalate loudly + still alert; do NOT DLQ.
    warn('event_made_private_channel_id_absent', { tenant_id: tenantId, booking_id: bookingId });
  }

  // Alert when the channel was newly degraded OR when channel_id was absent (so the gap is
  // human-visible). A re-delivery on an already-private channel (channelDegraded=false,
  // channel_id present) is a silent no-op.
  if (channelDegraded || !channelId) {
    await alertAdmin({
      kind: 'booking.event_made_private',
      tenant_id: tenantId,
      booking_id: bookingId,
      channel_degraded: channelDegraded,
      channel_id_present: Boolean(channelId),
    });
  }

  log('event_made_private_processed', {
    tenant_id: tenantId,
    booking_id: bookingId,
    channel_degraded: channelDegraded,
    channel_id_present: Boolean(channelId),
  });
}

module.exports = {
  degradeOnEventPrivate,
  degradeChannel,
  isConditionalCheckFailed,
  _CHANNELS_TABLE: CHANNELS_TABLE,
};
