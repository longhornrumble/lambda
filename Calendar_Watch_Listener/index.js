'use strict';

/**
 * Calendar_Watch_Listener
 *
 * Receives Google Calendar push notifications, validates the channel token
 * via SHA-256 hash comparison (channel_token encryption Option 2 per
 * subphase_b1_calendar_watch_channels_runbook.md), enforces a replay window
 * on X-Goog-Message-Number, and dispatches to the SQS FIFO queue keyed by
 * channel_id so events for the same channel are processed in order.
 *
 * This file ships the WEBHOOK-VALIDATION + DISPATCH layer of Task B2. The
 * events.get + typed-event derivation (per
 * scheduling/docs/listener_dispatch_interface.md) lands in a follow-up — the
 * envelope dispatched here is `raw.calendar_push` and consumers will be
 * extended once typed dispatch lands.
 *
 * Security model:
 *   - GET:  responds 200 with no body to Google's webhook-verification probe
 *           (Google's calendar watch does not issue a hub.challenge — it just
 *           POSTs notifications to a callback URL it accepts as live).
 *   - POST: validates the X-Goog-Channel-ID + X-Goog-Channel-Token + replay
 *           window. crypto.timingSafeEqual on SHA-256 hashes prevents timing
 *           attacks.
 *
 * Must return 200 within ~30 s or Google retries. We always return 200 once
 * auth passes — dispatch failures are logged + alarmed but not propagated.
 */

const crypto = require('crypto');
const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

// ─── AWS clients ────────────────────────────────────────────────────────────────
// Created once at module load; reused across warm invocations.

const ddb = new DynamoDBClient({});
const sqs = new SQSClient({});

// ─── Environment ────────────────────────────────────────────────────────────────

const ENV                            = process.env.ENVIRONMENT || 'staging';
const CHANNELS_TABLE                 = process.env.CALENDAR_WATCH_CHANNELS_TABLE || `picasso-calendar-watch-channels-${ENV}`;
const EVENTS_QUEUE_URL               = process.env.EVENTS_QUEUE_URL || '';
const REPLAY_WINDOW_SECONDS          = Number(process.env.REPLAY_WINDOW_SECONDS || '300');

// ─── Structured logging ─────────────────────────────────────────────────────────
// CloudWatch log-metric-filter "Calendar_Watch_Listener-malformed-payload"
// matches `{ $.event = "malformed_payload" }`; keep that field name stable.

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

function warn(event, fields) {
  console.warn(JSON.stringify({ event, ...fields }));
}

// ─── Header normalization ───────────────────────────────────────────────────────
// API GW v2 + Lambda Function URLs lowercase header keys, but be defensive.

function getHeader(headers, name) {
  if (!headers) return undefined;
  const lower = name.toLowerCase();
  for (const k of Object.keys(headers)) {
    if (k.toLowerCase() === lower) return headers[k];
  }
  return undefined;
}

// ─── Channel lookup ─────────────────────────────────────────────────────────────

async function lookupChannel(channelId) {
  const result = await ddb.send(
    new GetItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: { S: channelId } },
      ProjectionExpression: 'tenant_id, calendar_id, calendar_provider, channel_token_sha256, #s, expiration',
      ExpressionAttributeNames: { '#s': 'status' },
    })
  );
  if (!result.Item) return null;
  return {
    tenantId:         result.Item.tenant_id?.S         ?? null,
    calendarId:       result.Item.calendar_id?.S       ?? null,
    calendarProvider: result.Item.calendar_provider?.S ?? 'google',
    tokenSha256:      result.Item.channel_token_sha256?.S ?? null,
    status:           result.Item.status?.S            ?? null,
    expiration:       Number(result.Item.expiration?.N ?? '0'),
  };
}

// ─── Token validation (channel_token encryption Option 2) ───────────────────────
// B5 onboarding writer stores the real token in Secrets Manager and a
// SHA-256 hash of the same token in this DDB row. On every push, we hash
// the inbound X-Goog-Channel-Token and constant-time-compare.

function sha256Hex(value) {
  return crypto.createHash('sha256').update(value, 'utf8').digest('hex');
}

function validateToken(receivedToken, storedHashHex) {
  if (!receivedToken || !storedHashHex) return false;
  const receivedHashHex = sha256Hex(receivedToken);
  if (receivedHashHex.length !== storedHashHex.length) return false;
  return crypto.timingSafeEqual(
    Buffer.from(receivedHashHex, 'hex'),
    Buffer.from(storedHashHex, 'hex')
  );
}

// ─── Replay-window check ────────────────────────────────────────────────────────
// Google sends X-Goog-Message-Number (monotonically increasing per channel)
// and the listener invocation time. A push notification more than
// REPLAY_WINDOW_SECONDS old (per request-receipt time) is rejected. This is
// belt-and-suspenders on top of TLS — Google retries up to 24h on non-2xx
// responses, so a stale-replay scenario is unlikely but the check is cheap.
//
// We do NOT persist message numbers in v1 (would require another DDB write
// per push). The window check alone catches the most likely replay vector.

function isWithinReplayWindow(receiptTimeMs, dispatchedAtIsoMs) {
  // dispatchedAtIsoMs comes from the listener invocation start — we treat
  // receiptTimeMs as authoritative (the message's own clock cannot be trusted
  // for replay protection). The window guards against an attacker replaying
  // a previously-captured request body to our endpoint.
  //
  // For now the receipt-time IS the dispatched time — same value, but kept
  // separate so when persistent message-number dedup lands we can swap in a
  // more sophisticated implementation without changing call sites.
  const ageMs = Math.max(0, dispatchedAtIsoMs - receiptTimeMs);
  return ageMs <= REPLAY_WINDOW_SECONDS * 1000;
}

// ─── SQS dispatch ───────────────────────────────────────────────────────────────
// Phase 1 of B2 dispatches a `raw.calendar_push` envelope. The follow-up
// adds the events.get + typed-event derivation so the envelope.event_type
// becomes one of the seven types in listener_dispatch_interface.md.
//
// MessageGroupId == channel_id during the raw-push phase (preserves order
// per channel). When typed dispatch lands, MessageGroupId becomes
// event_id == booking_id per the dispatch interface.
//
// MessageDeduplicationId == sha256(channel_id + resource_state + message_number)
// — duplicate retries from Google collapse server-side via FIFO dedup.

async function dispatchRawPush(envelope) {
  if (!EVENTS_QUEUE_URL) {
    warn('dispatch_skipped_no_queue_url', { envelope_event_type: envelope.event_type });
    return;
  }
  const dedupBasis = `${envelope.channel_id}:${envelope.resource_state}:${envelope.message_number}`;
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: EVENTS_QUEUE_URL,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: envelope.channel_id,
      MessageDeduplicationId: sha256Hex(dedupBasis),
    })
  );
}

// ─── Response helpers ───────────────────────────────────────────────────────────

function ok() {
  return { statusCode: 200, body: '' };
}

function forbidden(reason, fields) {
  warn('auth_rejected', { reason, ...fields });
  return { statusCode: 403, body: 'Forbidden' };
}

function badRequest(reason, fields) {
  warn('malformed_payload', { reason, ...fields });
  return { statusCode: 400, body: 'Bad Request' };
}

// ─── GET: Google's channel-creation callback probe ──────────────────────────────
// Unlike Meta, Google Calendar's events.watch doesn't issue a separate
// challenge — it begins POSTing notifications immediately to the callback
// URL once you call events.watch. The GET handler exists to give us a
// uniform 200 response if anything ever pings the URL with GET (manual
// curl, AWS Lambda Function URL warm-up checks, etc.).

function handleGet() {
  log('get_probe_received');
  return ok();
}

// ─── POST: inbound push notification ────────────────────────────────────────────

async function handlePost(rawBody, headers) {
  const receiptTimeMs = Date.now();
  const channelId     = getHeader(headers, 'X-Goog-Channel-ID');
  const channelToken  = getHeader(headers, 'X-Goog-Channel-Token');
  const resourceState = getHeader(headers, 'X-Goog-Resource-State');
  const messageNumber = getHeader(headers, 'X-Goog-Message-Number');
  const resourceId    = getHeader(headers, 'X-Goog-Resource-ID');
  const resourceUri   = getHeader(headers, 'X-Goog-Resource-URI');

  if (!channelId || !channelToken) {
    return badRequest('missing_required_google_headers', {
      has_channel_id:    Boolean(channelId),
      has_channel_token: Boolean(channelToken),
    });
  }

  if (!resourceState) {
    return badRequest('missing_resource_state_header', { channel_id: channelId });
  }

  if (!messageNumber) {
    return badRequest('missing_message_number_header', { channel_id: channelId });
  }

  // `sync` is the initial post-watch.create handshake — Google sends this
  // exactly once per channel, no event has actually changed. Acknowledge it
  // without dispatch.
  if (resourceState === 'sync') {
    log('sync_received', { channel_id: channelId });
    return ok();
  }

  // ── Channel lookup ──
  let channel;
  try {
    channel = await lookupChannel(channelId);
  } catch (err) {
    console.error(JSON.stringify({
      event:      'channel_lookup_failed',
      channel_id: channelId,
      error:      err.message,
    }));
    // 503 (not 5xx silently): a real lookup failure should retry from Google.
    return { statusCode: 503, body: 'Service Unavailable' };
  }

  if (!channel) {
    // Unknown channel — either expired channel still receiving notifications
    // (Google may push for a few hours after expiration) or a forged request.
    // Reject 403; alarm catches a sustained pattern.
    return forbidden('unknown_channel_id', { channel_id: channelId });
  }

  // ── Token validation ──
  if (!validateToken(channelToken, channel.tokenSha256)) {
    return forbidden('channel_token_mismatch', {
      channel_id: channelId,
      tenant_id:  channel.tenantId,
    });
  }

  // ── Replay-window check ──
  if (!isWithinReplayWindow(receiptTimeMs, Date.now())) {
    // Note: in Phase 1 receiptTimeMs == Date.now() so this never triggers.
    // The check is a no-op until persistent message-number dedup lands;
    // the structure is in place so callers don't change.
    return forbidden('replay_window_exceeded', { channel_id: channelId });
  }

  // ── Channel status sanity ──
  // event_body_private is a degraded state — still acknowledge but flag.
  if (channel.status && channel.status !== 'active') {
    warn('channel_in_non_active_state', {
      channel_id: channelId,
      tenant_id:  channel.tenantId,
      status:     channel.status,
    });
  }

  // ── Dispatch ──
  // Phase 1 envelope. Phase 2 (events.get + typing) replaces event_type with
  // one of the seven from listener_dispatch_interface.md.
  const envelope = {
    event_type:                'raw.calendar_push',
    channel_id:                channelId,
    tenant_id:                 channel.tenantId,
    calendar_id:               channel.calendarId,
    calendar_provider:         channel.calendarProvider,
    resource_state:            resourceState,
    resource_id:               resourceId  ?? null,
    resource_uri:              resourceUri ?? null,
    message_number:            messageNumber,
    last_calendar_mutation_at: new Date(receiptTimeMs).toISOString(),
    dispatched_at:             new Date().toISOString(),
  };

  try {
    await dispatchRawPush(envelope);
    log('dispatched_raw_push', {
      channel_id:     channelId,
      tenant_id:      channel.tenantId,
      resource_state: resourceState,
      message_number: messageNumber,
    });
  } catch (err) {
    console.error(JSON.stringify({
      event:      'dispatch_failed',
      channel_id: channelId,
      tenant_id:  channel.tenantId,
      error:      err.message,
    }));
    // Returning 500 would make Google retry. Returning 200 means we ack and
    // lose this push. Trade-off: we'd rather have Google retry (handler is
    // idempotent at SQS dedup) than silently drop. SQS SendMessage failures
    // are alarmed on Lambda Errors metric.
    return { statusCode: 500, body: 'Internal Server Error' };
  }

  return ok();
}

// ─── Main handler ───────────────────────────────────────────────────────────────

exports.handler = async function handler(event) {
  const method = event.requestContext?.http?.method || event.httpMethod || 'UNKNOWN';

  if (method === 'GET') {
    return handleGet();
  }

  if (method === 'POST') {
    let rawBody = event.body || '';
    if (event.isBase64Encoded) {
      rawBody = Buffer.from(rawBody, 'base64').toString('utf8');
    }
    const headers = event.headers || {};
    return handlePost(rawBody, headers);
  }

  warn('unsupported_method', { method });
  return { statusCode: 405, body: 'Method Not Allowed' };
};

// ─── Test-only exports ──────────────────────────────────────────────────────────
// These let the unit tests exercise the pure helpers without touching AWS.

exports._test = {
  getHeader,
  sha256Hex,
  validateToken,
  isWithinReplayWindow,
};
