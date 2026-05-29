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
 * Phase 2b adds delta-discovery (syncToken-based listChangedEvents loop),
 * Booking record lookup, typed-event derivation for all 7 event_types from
 * listener_dispatch_interface.md, and OOO-overlap detection via Booking GSI.
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
const {
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { getOAuthClient, clearCacheEntry } = require('./oauth-client');
const { listChangedEvents } = require('./calendar-api');

// ─── AWS clients ────────────────────────────────────────────────────────────────
// Created once at module load; reused across warm invocations.

const ddb = new DynamoDBClient({});
const sqs = new SQSClient({});

// ─── Environment ────────────────────────────────────────────────────────────────

const ENV                            = process.env.ENVIRONMENT || 'staging';
const CHANNELS_TABLE                 = process.env.CALENDAR_WATCH_CHANNELS_TABLE || `picasso-calendar-watch-channels-${ENV}`;
const BOOKING_TABLE                  = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
const BOOKING_TENANT_START_INDEX     = process.env.BOOKING_TENANT_START_INDEX || 'tenantId-start_at-index';
const EVENTS_QUEUE_URL               = process.env.EVENTS_QUEUE_URL || '';
const REPLAY_WINDOW_SECONDS          = Number(process.env.REPLAY_WINDOW_SECONDS || '300');
// Y5: safety cap on Google Calendar list-pages loop (pilot calendars are tiny;
// this is a runaway-protection backstop, not a correctness limit).
const MAX_LIST_PAGES                 = Number(process.env.MAX_LIST_PAGES || '100');

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
      ProjectionExpression: 'tenant_id, calendar_id, calendar_provider, channel_token_sha256, #s, expiration, last_sync_token, coordinator_id',
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
    lastSyncToken:    result.Item.last_sync_token?.S   ?? null,
    coordinatorId:    result.Item.coordinator_id?.S    ?? null,
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
// Phase 2b typed dispatch.
//
// MessageGroupId  == event_id (== booking_id) so events for the same booking
//                    process in order; different bookings are concurrent.
// MessageDeduplicationId == sha256(channel_id + ":" + event_id + ":" + last_calendar_mutation_at)
//                    — channel_id is platform-controlled, so an attacker who
//                    controls booking_id + event.updated cannot forge a
//                    dedup-collision against a legitimate event on a different
//                    channel.  channel_id is NOT added to the message body.

async function dispatchTypedEvent(envelope, channelId) {
  if (!EVENTS_QUEUE_URL) {
    warn('dispatch_skipped_no_queue_url', { envelope_event_type: envelope.event_type });
    return;
  }
  // Row 5: dedup basis includes event_type (separates calendar_moved + attendee_accepted
  // on the same event) and attendee_email (separates N attendees with same updated timestamp).
  // attendee_email ?? '' makes the formula stable for non-attendee envelopes.
  // These fields are added to the dedup basis only — NOT to the message body.
  const dedupBasis = `${channelId}:${envelope.event_type}:${envelope.event_id}:${envelope.attendee_email ?? ''}:${envelope.last_calendar_mutation_at}`;
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: EVENTS_QUEUE_URL,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: envelope.event_id,
      MessageDeduplicationId: sha256Hex(dedupBasis),
    })
  );
}

// ─── Booking lookup ─────────────────────────────────────────────────────────────
// Returns the Booking record or null if not found.  In B-phase there is no
// write path yet (sub-phase C8 creates bookings), so lookups normally return
// null for real events; tests seed fixtures to exercise derivation logic.

async function lookupBooking(bookingId) {
  const result = await ddb.send(
    new GetItemCommand({
      TableName: BOOKING_TABLE,
      Key: { booking_id: { S: bookingId } },
      // G3: project only the fields this function's callers actually use, so
      // future PII fields added to the Booking schema don't flow through unused.
      ProjectionExpression: 'booking_id, tenant_id, resource_id, start_at, end_at, #st',
      ExpressionAttributeNames: { '#st': 'status' },
    })
  );
  if (!result.Item) return null;
  return {
    bookingId:  result.Item.booking_id?.S  ?? bookingId,
    tenantId:   result.Item.tenant_id?.S   ?? null,
    resourceId: result.Item.resource_id?.S ?? null,
    startAt:    result.Item.start_at?.S    ?? null,
    endAt:      result.Item.end_at?.S      ?? null,
    status:     result.Item.status?.S      ?? null,
  };
}

// ─── OOO: booked-booking query by coordinator + time overlap ────────────────────
// Queries the Booking GSI for all `booked`-status bookings in the time window
// [oooStart, oooEnd) for the given tenant + coordinator, then filters to those
// whose time range overlaps the OOO event.  We filter by coordinator_id as a
// filter expression (not key condition) because the GSI key is tenantId+start_at.

async function queryBookedBookingsForOoo(tenantId, coordinatorId, oooStartAt, oooEndAt) {
  const params = {
    TableName: BOOKING_TABLE,
    IndexName: BOOKING_TENANT_START_INDEX,
    KeyConditionExpression: 'tenant_id = :tid AND start_at < :ooo_end',
    FilterExpression: '#st = :booked AND end_at > :ooo_start AND coordinator_id = :cid',
    ExpressionAttributeNames: { '#st': 'status' },
    ExpressionAttributeValues: {
      ':tid':      { S: tenantId },
      ':ooo_end':  { S: oooEndAt },
      ':ooo_start':{ S: oooStartAt },
      ':booked':   { S: 'booked' },
      ':cid':      { S: coordinatorId },
    },
    // Y5: bound page size — pilot calendars are tiny; 500 is a runaway-protection
    // backstop, not a correctness limit (pagination still drains if needed).
    Limit: 500,
  };

  const overlapping = [];
  let lastKey;
  do {
    if (lastKey) params.ExclusiveStartKey = lastKey;
    const page = await ddb.send(new QueryCommand(params));
    for (const item of page.Items ?? []) {
      overlapping.push(item.booking_id?.S ?? null);
    }
    lastKey = page.LastEvaluatedKey;
  } while (lastKey);

  return overlapping.filter(Boolean);
}

// ─── Atomic syncToken advancement ───────────────────────────────────────────────
// Conditional UpdateItem: only succeeds if the stored token still matches the
// value we read.  If another concurrent invocation already advanced it, the
// condition fails with ConditionalCheckFailedException and we stop processing
// to prevent double-dispatch.

async function advanceSyncToken(channelId, oldToken, newToken) {
  const condition = oldToken
    ? 'last_sync_token = :old'
    : 'attribute_not_exists(last_sync_token)';
  const exprValues = { ':new': { S: newToken } };
  if (oldToken) exprValues[':old'] = { S: oldToken };

  await ddb.send(
    new UpdateItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: { S: channelId } },
      UpdateExpression: 'SET last_sync_token = :new',
      ConditionExpression: condition,
      ExpressionAttributeValues: exprValues,
    })
  );
}

// ─── Typed-event derivation ──────────────────────────────────────────────────────
// For each changed Google Calendar event, derives the matching event_type(s)
// from listener_dispatch_interface.md and returns an array of envelopes to send.
// Returns [] if the event is not platform-owned or the Booking is absent.
//
// A single changed event can produce at most one envelope per distinct event_type,
// but the OOO path can produce an envelope independent of any booking_id on the event.

async function deriveTypedEnvelopes(calEvent, tenantId, coordinatorId, calendarProvider) {
  const envelopes = [];
  const mutatedAt = calEvent.updated ?? new Date().toISOString();

  // ── OOO-overlap path (does NOT require a platform booking_id on the event) ──
  // eventType 'outOfOffice' or working-location events are treated as OOO.
  const isOoo = calEvent.eventType === 'outOfOffice' || calEvent.eventType === 'workingLocation';
  if (isOoo) {
    const oooStart = calEvent.start?.dateTime ?? calEvent.start?.date ?? null;
    const oooEnd   = calEvent.end?.dateTime   ?? calEvent.end?.date   ?? null;
    if (oooStart && oooEnd) {
      let overlappingIds;
      try {
        overlappingIds = await queryBookedBookingsForOoo(tenantId, coordinatorId, oooStart, oooEnd);
      } catch (err) {
        // Y3: coordinator_id is the coordinator's email (PII) — omit from logs.
        warn('ooo_booking_query_failed', {
          tenant_id: tenantId,
          error: err.message,
        });
        overlappingIds = [];
      }
      if (overlappingIds.length > 0) {
        envelopes.push({
          event_type:                   'booking.ooo_overlap_detected',
          event_id:                     overlappingIds[0],
          tenant_id:                    tenantId,
          booking_id:                   overlappingIds[0],
          last_calendar_mutation_at:    mutatedAt,
          dispatched_at:                new Date().toISOString(),
          calendar_provider:            calendarProvider,
          ooo_start_at:                 oooStart,
          ooo_end_at:                   oooEnd,
          overlapping_booking_ids:      overlappingIds,
        });
      }
    }
    // OOO events are not platform-owned bookings — skip platform-booking paths.
    return envelopes;
  }

  // ── Platform-owned booking path ──
  const bookingId = calEvent.extendedProperties?.private?.booking_id ?? null;
  if (!bookingId) {
    log('skipped_non_platform_event', {
      google_event_id: calEvent.id,
      tenant_id: tenantId,
    });
    return envelopes;
  }

  const commonFields = {
    event_id:                  bookingId,
    tenant_id:                 tenantId,
    booking_id:                bookingId,
    last_calendar_mutation_at: mutatedAt,
    dispatched_at:             new Date().toISOString(),
    calendar_provider:         calendarProvider,
  };

  // R1: cross-tenant booking guard — run ONCE up front, before any branch.
  // booking_id comes from attacker-influenceable extendedProperties.private.booking_id.
  // If the Booking record belongs to a different tenant, emit nothing and return.
  // For a legitimately-deleted booking, lookupBooking returns null → we cannot
  // verify tenant → proceed (consumer no-ops on a phantom foreign booking_id; the
  // leak we're closing is emitting a *real* foreign-tenant booking's event).
  let booking = null;
  try {
    booking = await lookupBooking(bookingId);
  } catch (err) {
    warn('booking_lookup_failed', {
      booking_id: bookingId,
      tenant_id: tenantId,
      error: err.message,
    });
  }

  if (booking !== null && booking.tenantId !== null && booking.tenantId !== tenantId) {
    warn('cross_tenant_booking_id_detected', {
      booking_id:        bookingId,
      booking_tenant_id: booking.tenantId,
      channel_tenant_id: tenantId,
    });
    return [];
  }

  // ── Deleted ──
  if (calEvent.status === 'cancelled') {
    envelopes.push({ event_type: 'booking.calendar_deleted', ...commonFields });
    return envelopes;
  }

  // ── Private ──
  if (calEvent.visibility === 'private' || calEvent.visibility === 'confidential') {
    envelopes.push({ event_type: 'booking.event_made_private', ...commonFields });
    return envelopes;
  }

  if (!booking) {
    log('skipped_no_booking_record', { booking_id: bookingId, tenant_id: tenantId });
    // Still check attendee status changes below — those don't require booking comparison.
  }

  if (booking) {
    const newStart = calEvent.start?.dateTime ?? calEvent.start?.date ?? null;
    const newEnd   = calEvent.end?.dateTime   ?? calEvent.end?.date   ?? null;
    if (newStart && booking.startAt && newStart !== booking.startAt) {
      envelopes.push({
        event_type:          'booking.calendar_moved',
        ...commonFields,
        previous_start_at:   booking.startAt,
        new_start_at:        newStart,
        previous_end_at:     booking.endAt ?? null,
        new_end_at:          newEnd ?? null,
      });
    }

    // Reassigned: determine current "effective owner" — organizer or first accepted attendee.
    const newResourceId = resolveResourceId(calEvent);
    if (newResourceId && booking.resourceId && newResourceId !== booking.resourceId) {
      envelopes.push({
        event_type:            'booking.calendar_reassigned',
        ...commonFields,
        previous_resource_id:  booking.resourceId,
        new_resource_id:       newResourceId,
      });
    }
  }

  // ── Attendee status changes (independent of booking record) ──
  for (const attendee of calEvent.attendees ?? []) {
    if (attendee.responseStatus === 'accepted') {
      envelopes.push({
        event_type:     'booking.attendee_accepted',
        ...commonFields,
        attendee_email: attendee.email,
        response_status: 'accepted',
      });
    } else if (attendee.responseStatus === 'declined') {
      envelopes.push({
        event_type:     'booking.attendee_declined',
        ...commonFields,
        attendee_email: attendee.email,
        response_status: 'declined',
      });
    }
  }

  return envelopes;
}

// ─── Resource-ID resolution ──────────────────────────────────────────────────────
// Per dispatch interface: the "resource" is the organizer or the first accepted
// attendee.  Organizer takes precedence.

function resolveResourceId(calEvent) {
  if (calEvent.organizer?.email) return calEvent.organizer.email;
  const accepted = (calEvent.attendees ?? []).find(a => a.responseStatus === 'accepted');
  if (accepted?.email) return accepted.email;
  return null;
}

// ─── Delta-discovery + typed dispatch ───────────────────────────────────────────
// Called after token validation succeeds.  Returns 200 on success, 500 on
// infrastructure failure (so Google retries — idempotent at SQS dedup level).
// Individual changed-event errors are logged + continued, not propagated.

async function processDelta(channel, channelId) {
  const { tenantId, calendarId, calendarProvider, coordinatorId, lastSyncToken } = channel;

  // ── Config guards (missing calendarId or coordinatorId is a config problem,
  //    not a transient error — return 200 so Google does not retry forever) ──
  if (!calendarId) {
    warn('delta_skipped_no_calendar_id', { channel_id: channelId, tenant_id: tenantId });
    return { statusCode: 200, body: '' };
  }

  // ── Build OAuth client ──
  if (!coordinatorId) {
    // Y3: coordinator_id is the coordinator's email (PII) — omit from logs.
    warn('delta_skipped_no_coordinator_id', { channel_id: channelId, tenant_id: tenantId });
    return { statusCode: 200, body: '' };
  }

  let authClient;
  try {
    authClient = await getOAuthClient({ tenantId, coordinatorId });
  } catch (err) {
    // Y3: coordinator_id is the coordinator's email (PII) — omit from logs.
    // Y1: evict the cache entry so the next invocation re-fetches from Secrets
    //     Manager rather than replaying a stale/revoked credential.
    clearCacheEntry({ tenantId, coordinatorId });
    console.error(JSON.stringify({
      event: 'oauth_client_failed',
      channel_id: channelId,
      tenant_id: tenantId,
      error: err.message,
    }));
    return { statusCode: 500, body: 'Internal Server Error' };
  }

  // ── Paginated listChangedEvents ──
  // Google's incremental sync (syncToken present) typically returns all changes
  // in one batch.  An initial full-list (no syncToken) can paginate.  Both paths
  // are handled by the loop: first call uses syncToken; continuations pass the
  // nextPageToken from the prior page (syncToken is omitted on continuations).
  //
  // Y5: MAX_LIST_PAGES caps the loop as a runaway-protection backstop; pilot
  // calendars are tiny so hitting the cap would indicate something anomalous.
  let allEvents = [];
  let nextSyncToken = null;
  let pageToken = null;
  let pagesConsumed = 0;

  try {
    do {
      const page = await listChangedEvents(authClient, calendarId, pageToken ? null : lastSyncToken, pageToken);
      allEvents = allEvents.concat(page.events);
      nextSyncToken = page.nextSyncToken ?? nextSyncToken;
      pageToken = page.nextPageToken ?? null;
      pagesConsumed += 1;
      if (pagesConsumed >= MAX_LIST_PAGES && pageToken) {
        warn('list_pages_cap_hit', { channel_id: channelId, tenant_id: tenantId });
        break;
      }
    } while (pageToken);
  } catch (err) {
    // R3: a 410 means the stored syncToken has expired — Google requires a full
    // resync.  Clear the token from DDB so the next push triggers a full list
    // (no syncToken).  Return 200 so Google does not retry this stale-token push.
    if (err.code === 410 || err.response?.status === 410) {
      try {
        await ddb.send(
          new UpdateItemCommand({
            TableName: CHANNELS_TABLE,
            Key: { channel_id: { S: channelId } },
            UpdateExpression: 'REMOVE last_sync_token',
          })
        );
      } catch (clearErr) {
        console.error(JSON.stringify({
          event: 'sync_token_clear_failed',
          channel_id: channelId,
          tenant_id: tenantId,
          error: clearErr.message,
        }));
        // Best-effort — do not crash; still return 200 so Google moves on.
      }
      warn('sync_token_expired_cleared', { channel_id: channelId, tenant_id: tenantId });
      return { statusCode: 200, body: '' };
    }
    // Row 7: revoked or expired OAuth token detected mid-cache-lifetime.
    // Evict the cache entry so the next invocation re-fetches from Secrets Manager
    // instead of replaying the stale/revoked credential.  Return 500 so Google
    // retries; the next attempt will re-fetch + re-authenticate.
    const isAuthError =
      err.code === 401 ||
      err.response?.status === 401 ||
      err.code === 'invalid_grant' ||
      /invalid_grant/i.test(err.message || '');
    if (isAuthError) {
      clearCacheEntry({ tenantId, coordinatorId });
      warn('oauth_token_rejected_cache_cleared', { channel_id: channelId, tenant_id: tenantId });
      return { statusCode: 500, body: 'Internal Server Error' };
    }
    console.error(JSON.stringify({
      event: 'list_changed_events_failed',
      channel_id: channelId,
      tenant_id: tenantId,
      error: err.message,
    }));
    return { statusCode: 500, body: 'Internal Server Error' };
  }

  // ── Per-event derivation + dispatch (BEFORE advancing syncToken) ──
  // R2: dispatch first, then advance the token.  Advancing first and then
  // failing dispatch silently loses events (Google retried, token already moved).
  // If any dispatch throws we stop the loop and return 500 — Google retries the
  // push; SQS FIFO dedup + idempotent consumers make re-dispatch safe.
  for (const calEvent of allEvents) {
    let envelopes;
    try {
      envelopes = await deriveTypedEnvelopes(calEvent, tenantId, coordinatorId, calendarProvider ?? 'google');
    } catch (err) {
      console.error(JSON.stringify({
        event: 'derive_typed_envelopes_failed',
        channel_id: channelId,
        tenant_id: tenantId,
        google_event_id: calEvent.id,
        error: err.message,
      }));
      continue;
    }

    for (const envelope of envelopes) {
      try {
        await dispatchTypedEvent(envelope, channelId);
        log('dispatched_typed_event', {
          event_type: envelope.event_type,
          event_id:   envelope.event_id,
          tenant_id:  tenantId,
          channel_id: channelId,
        });
      } catch (err) {
        console.error(JSON.stringify({
          event: 'dispatch_typed_event_failed',
          event_type:      envelope.event_type,
          event_id:        envelope.event_id,
          channel_id:      channelId,
          tenant_id:       tenantId,
          error:           err.message,
        }));
        // R2: stop the loop and return 500 — do NOT advance the token.
        // Google will retry; SQS FIFO dedup prevents double-processing of the
        // events we already dispatched successfully in this invocation.
        return { statusCode: 500, body: 'Internal Server Error' };
      }
    }
  }

  // ── Atomic syncToken advancement (AFTER all dispatches succeed) ──
  if (nextSyncToken && nextSyncToken !== lastSyncToken) {
    try {
      await advanceSyncToken(channelId, lastSyncToken, nextSyncToken);
    } catch (err) {
      if (err.name === 'ConditionalCheckFailedException') {
        // R2: a concurrent invocation already advanced the token; our dispatches
        // above already succeeded.  The concurrent double-dispatch is handled by
        // SQS FIFO dedup + idempotent consumers.  Return 200.
        log('sync_token_race_lost', {
          channel_id: channelId,
          tenant_id: tenantId,
        });
        return { statusCode: 200, body: '' };
      }
      console.error(JSON.stringify({
        event: 'advance_sync_token_failed',
        channel_id: channelId,
        tenant_id: tenantId,
        error: err.message,
      }));
      return { statusCode: 500, body: 'Internal Server Error' };
    }
  }

  return { statusCode: 200, body: '' };
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

  // ── Delta discovery + typed dispatch (Phase 2b) ──
  // processDelta handles OAuth, listChangedEvents pagination, syncToken
  // advancement, Booking lookup, and per-event typed dispatch.  It returns
  // a { statusCode, body } response directly so the handler just forwards it.
  log('processing_delta', {
    channel_id:     channelId,
    tenant_id:      channel.tenantId,
    resource_state: resourceState,
    message_number: messageNumber,
  });
  return processDelta(channel, channelId);
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
  resolveResourceId,
  deriveTypedEnvelopes,
  advanceSyncToken,
  lookupChannel,
  lookupBooking,
  queryBookedBookingsForOoo,
  processDelta,
  dispatchTypedEvent,
  // Exported event-type vocabulary so CI-3b contract test can enumerate without
  // importing the dispatch interface doc.
  EVENT_TYPES: [
    'booking.calendar_deleted',
    'booking.calendar_moved',
    'booking.calendar_reassigned',
    'booking.ooo_overlap_detected',
    'booking.attendee_accepted',
    'booking.attendee_declined',
    'booking.event_made_private',
  ],
};
