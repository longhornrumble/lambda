'use strict';

/**
 * Calendar_Watch_Onboarder — scheduling sub-phase B Task B5.
 *
 * Registers an initial Google Calendar push-notification watch channel for a
 * (tenant_id, coordinator_id, calendar_id) tuple. v1 pilot-scale invocation
 * is direct (aws lambda invoke); a future DDB-stream trigger on
 * picasso-employee-registry-v2-{env} can subscribe this handler once the E13
 * UI / F2 onboarding flow populates `scheduling_tags`.
 *
 * Per-invocation flow:
 *   1. Validate input (strict allowlist on tenant_id / coordinator_id)
 *   2. Fetch OAuth client (per tenant, per coordinator) from Secrets Manager
 *   3. events.list(calendarId) — seeds `last_sync_token` (Phase 2b precondition)
 *   4. Generate channel_id (UUID) + channel_token (256-bit random)
 *   5. events.watch — register the push channel
 *   6. PutItem on picasso-calendar-watch-channels-{env} with the SHA-256 hash
 *      of the token — and on ANY failure after the watch is live, revoke the
 *      channel (events.stop) so we never strand an unrevokable Google channel.
 *
 * Return: { channel_id, expiration, last_sync_token_seeded }
 *
 * Security model:
 *   - channel_token entropy: crypto.randomBytes(32) → 64 hex chars (per B8).
 *   - Only the SHA-256 HASH of the token is persisted (in DDB). The raw token
 *     is handed to Google in events.watch and never stored at rest — the
 *     Listener authenticates inbound pushes by hashing X-Goog-Channel-Token
 *     and constant-time-comparing against this stored hash. SHA-256 (no salt)
 *     is sufficient here because the token is 256 bits of uniform randomness,
 *     not a low-entropy password. This is a one-way commitment, not encryption.
 */

const crypto = require('crypto');
const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { getOAuthClient } = require('./oauth-client');
const { registerWatch, seedInitialSyncToken, stopWatch } = require('./calendar-watch');

// ─── AWS clients ────────────────────────────────────────────────────────────────

const ddb = new DynamoDBClient({});

// ─── Environment ────────────────────────────────────────────────────────────────
// CHANNELS_TABLE + LISTENER_URL are REQUIRED (no silent defaults): a missing
// table name must not silently write to the staging table from another env,
// and a missing listener URL must not register a watch pointing nowhere. Both
// are validated at handler entry.

const CHANNELS_TABLE = process.env.CALENDAR_WATCH_CHANNELS_TABLE || '';
const LISTENER_URL   = process.env.LISTENER_URL || '';

// ─── Input format allowlists ────────────────────────────────────────────────────
// tenant_id / coordinator_id flow into the OAuth Secrets Manager path
// (picasso/scheduling/oauth/{tenantId}/{coordinatorId}) and into DDB keys.
// Reject anything that could path-traverse or break the canonical schema. A
// `/` in either value would silently produce a different secret path.

const TENANT_ID_RE      = /^[A-Za-z0-9_-]{1,64}$/;
const COORDINATOR_ID_RE = /^[A-Za-z0-9._@+-]{1,128}$/;

// ─── Structured logging ─────────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── Input validation ───────────────────────────────────────────────────────────

function validateInput(input) {
  if (!input || typeof input !== 'object') {
    throw new Error('Input must be a JSON object');
  }
  const tenantId = input.tenant_id;
  const coordinatorId = input.coordinator_id;
  if (!tenantId || typeof tenantId !== 'string' || !TENANT_ID_RE.test(tenantId)) {
    throw new Error('tenant_id is required and must match /^[A-Za-z0-9_-]{1,64}$/');
  }
  if (!coordinatorId || typeof coordinatorId !== 'string' || !COORDINATOR_ID_RE.test(coordinatorId)) {
    throw new Error('coordinator_id is required and must match /^[A-Za-z0-9._@+-]{1,128}$/');
  }
  // calendarId defaults to 'primary' — Google interprets this as the
  // authenticated user's primary calendar.
  const calendarId = (input.calendar_id && typeof input.calendar_id === 'string')
    ? input.calendar_id
    : 'primary';
  return { tenantId, coordinatorId, calendarId };
}

// ─── Token + channel id generation ──────────────────────────────────────────────

function generateChannelId() {
  return crypto.randomUUID();
}

function generateChannelToken() {
  return crypto.randomBytes(32).toString('hex'); // 64 hex chars per B8
}

function sha256Hex(value) {
  return crypto.createHash('sha256').update(value, 'utf8').digest('hex');
}

// ─── DDB row write ──────────────────────────────────────────────────────────────
// Conditional on attribute_not_exists(channel_id) — channel_ids are UUIDs so a
// collision is astronomically unlikely, but the guard prevents an accidental
// overwrite if the caller retries without realizing the prior call succeeded.

async function writeChannelRow(row) {
  const item = {
    channel_id:           { S: row.channelId },
    tenant_id:            { S: row.tenantId },
    coordinator_id:       { S: row.coordinatorId },
    calendar_id:          { S: row.calendarId },
    calendar_provider:    { S: 'google' },
    channel_token_sha256: { S: row.channelTokenSha256 },
    status:               { S: 'active' },
    expiration:           { N: String(row.expiration) },
    created_at:           { S: row.createdAt },
  };
  if (row.lastSyncToken) {
    item.last_sync_token = { S: row.lastSyncToken };
  }
  if (row.resourceId) {
    item.resource_id = { S: row.resourceId };
  }
  if (row.resourceUri) {
    item.resource_uri = { S: row.resourceUri };
  }
  await ddb.send(
    new PutItemCommand({
      TableName: CHANNELS_TABLE,
      Item: item,
      ConditionExpression: 'attribute_not_exists(channel_id)',
    })
  );
}

// ─── Main handler ───────────────────────────────────────────────────────────────

exports.handler = async function handler(event) {
  if (!CHANNELS_TABLE) {
    throw new Error('CALENDAR_WATCH_CHANNELS_TABLE env var is required');
  }
  if (!LISTENER_URL || !LISTENER_URL.startsWith('https://')) {
    throw new Error('LISTENER_URL env var is required and must be https://');
  }

  const { tenantId, coordinatorId, calendarId } = validateInput(event);
  log('onboarder_invoked', { tenant_id: tenantId, coordinator_id: coordinatorId, calendar_id: calendarId });

  // 1. OAuth client (per-tenant secret)
  const authClient = await getOAuthClient({ tenantId, coordinatorId });

  // 2. Seed last_sync_token via events.list (no syncToken)
  const seed = await seedInitialSyncToken(authClient, calendarId);
  if (!seed.syncToken) {
    // A channel with no sync baseline is nearly useless for Phase 2b
    // delta-discovery. Surface loudly; the row is still written so the
    // operator can re-seed, but this must not pass silently.
    warn('sync_token_absent', {
      tenant_id: tenantId,
      coordinator_id: coordinatorId,
      seed_pages: seed.pages,
      seed_events_seen: seed.totalSeen,
    });
  } else {
    log('sync_token_seeded', {
      tenant_id: tenantId,
      coordinator_id: coordinatorId,
      seed_pages: seed.pages,
      seed_events_seen: seed.totalSeen,
    });
  }

  // 3. Generate channel identifiers
  const channelId = generateChannelId();
  const channelToken = generateChannelToken();
  const channelTokenSha256 = sha256Hex(channelToken);

  // 4. Register the watch with Google
  const watch = await registerWatch(authClient, calendarId, channelId, channelToken, LISTENER_URL);
  log('events_watch_registered', {
    channel_id: channelId,
    resource_id: watch.resourceId,
    expiration: watch.expiration,
  });

  // 5. Validate + persist. The watch is now LIVE; any failure here strands a
  // Google channel pushing to a Listener that has no row to authenticate it.
  // Compensate by revoking (events.stop) before re-throwing.
  try {
    if (watch.expiration === null || watch.expiration === undefined || !/^\d+$/.test(String(watch.expiration))) {
      throw new Error(`events.watch returned a non-numeric expiration: ${watch.expiration}`);
    }
    await writeChannelRow({
      channelId,
      tenantId,
      coordinatorId,
      calendarId,
      channelTokenSha256,
      lastSyncToken: seed.syncToken,
      expiration: watch.expiration,
      resourceId: watch.resourceId,
      resourceUri: watch.resourceUri,
      createdAt: new Date().toISOString(),
    });
  } catch (err) {
    warn('onboarder_compensating', {
      channel_id: channelId,
      resource_id: watch.resourceId,
      reason: err.message,
    });
    try {
      await stopWatch(authClient, channelId, watch.resourceId);
      warn('onboarder_compensation_succeeded', { channel_id: channelId });
    } catch (stopErr) {
      // Compensation itself failed — the channel is orphaned. Log enough for
      // an operator to manually events.stop with these identifiers.
      warn('onboarder_compensation_failed', {
        channel_id: channelId,
        resource_id: watch.resourceId,
        stop_error: stopErr.message,
      });
    }
    throw err;
  }
  log('channel_row_written', { channel_id: channelId, tenant_id: tenantId });

  return {
    channel_id: channelId,
    expiration: watch.expiration,
    last_sync_token_seeded: Boolean(seed.syncToken),
  };
};

// ─── Test-only exports ──────────────────────────────────────────────────────────

exports._test = {
  validateInput,
  generateChannelId,
  generateChannelToken,
  sha256Hex,
  writeChannelRow,
};
