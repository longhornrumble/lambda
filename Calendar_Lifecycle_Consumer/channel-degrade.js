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
 * ⚠️ CONTRACT GAP (ESCALATED to the integrator — see PR report-back). The
 * `listener_dispatch_interface.md` common envelope does NOT carry `channel_id`, and the
 * listener deliberately excludes `channel_id` from the SQS message body (it is used only
 * in the FIFO MessageDeduplicationId hash). A keyed UpdateItem on the channels table needs
 * `channel_id`, and the channels table has no GSI on `tenant_id`/`coordinator_id`/`booking_id`
 * to resolve it. This module is therefore built FORWARD-COMPATIBLY: it reads `channel_id`
 * from the envelope and degrades the channel the moment the listener includes it (a
 * one-line integrator change to the `event_made_private` envelope, or a `coordinator_id` +
 * channels GSI). Until then, `event_made_private` STILL fires the admin alert (which needs
 * only `tenant_id`/`booking_id`) so the degradation is never silently dropped; the record
 * is NOT sent to the DLQ (a missing `channel_id` will never appear on a redrive — DLQ would
 * retry-storm). The escalation log line surfaces the gap in CloudWatch.
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

// Conditional UpdateItem keyed on channel_id. Returns true iff THIS call newly degraded an
// active channel (→ caller fires the admin alert); false when the channel is absent, not
// `active` (already private — dedupe — or renewal-failed — not clobbered).
async function degradeChannel({ channelId, now = nowIso() }) {
  if (!channelId) {
    throw new Error('degradeChannel requires channelId');
  }
  try {
    await ddb.send(new UpdateItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: s(channelId) },
      UpdateExpression: 'SET #st = :private, event_body_private_at = :at',
      ConditionExpression:
        'attribute_exists(channel_id) AND (attribute_not_exists(#st) OR #st = :active)',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':private': s(STATUS_EVENT_BODY_PRIVATE),
        ':active': s(STATUS_ACTIVE),
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
  const tenantId = env.tenant_id;
  const bookingId = env.booking_id;
  const channelId = env.channel_id; // forward-compatible — see CONTRACT GAP note.

  let channelDegraded = false;
  if (channelId) {
    channelDegraded = await degradeChannel({ channelId });
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
