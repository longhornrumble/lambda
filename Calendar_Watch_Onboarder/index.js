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
 *   1. Validate input
 *   2. Fetch OAuth client (per tenant, per coordinator) from Secrets Manager
 *   3. events.list(calendarId) — seeds `last_sync_token` (Phase 2b precondition)
 *   4. Generate channel_id (UUID) + channel_token (256-bit random)
 *   5. CreateSecret picasso/scheduling/channel-token/{channel_id} (raw token)
 *   6. events.watch — register the push channel
 *   7. PutItem on picasso-calendar-watch-channels-{env} with the SHA-256 hash
 *      of the token (channel_token encryption Option 2)
 *
 * Return: { channel_id, expiration, last_sync_token_seeded }
 *
 * Security model:
 *   - channel_token entropy: crypto.randomBytes(32) → 64 hex chars (per B8).
 *   - Raw token stored in Secrets Manager; only the SHA-256 hash is in DDB.
 *   - Listener compares hashes via crypto.timingSafeEqual.
 *   - Onboarder writes to a NEW secret per channel; no overwrite semantics.
 */

const crypto = require('crypto');
const {
  SecretsManagerClient,
  CreateSecretCommand,
} = require('@aws-sdk/client-secrets-manager');
const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { getOAuthClient } = require('./oauth-client');
const { registerWatch, seedInitialSyncToken } = require('./calendar-watch');

// ─── AWS clients ────────────────────────────────────────────────────────────────

const ddb = new DynamoDBClient({});
const secrets = new SecretsManagerClient({});

// ─── Environment ────────────────────────────────────────────────────────────────

const ENV                       = process.env.ENVIRONMENT || 'staging';
const CHANNELS_TABLE            = process.env.CALENDAR_WATCH_CHANNELS_TABLE || `picasso-calendar-watch-channels-${ENV}`;
const LISTENER_URL              = process.env.LISTENER_URL || '';
const CHANNEL_TOKEN_SECRET_PREFIX = process.env.CHANNEL_TOKEN_SECRET_PREFIX || 'picasso/scheduling/channel-token';

// ─── Structured logging ─────────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

// ─── Input validation ───────────────────────────────────────────────────────────

function validateInput(input) {
  if (!input || typeof input !== 'object') {
    throw new Error('Input must be a JSON object');
  }
  const tenantId = input.tenant_id;
  const coordinatorId = input.coordinator_id;
  if (!tenantId || typeof tenantId !== 'string') {
    throw new Error('tenant_id is required (string)');
  }
  if (!coordinatorId || typeof coordinatorId !== 'string') {
    throw new Error('coordinator_id is required (string)');
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

// ─── Secrets Manager write ──────────────────────────────────────────────────────
// One new secret per channel. Path: picasso/scheduling/channel-token/{channel_id}.
// Tagged so housekeeping (B6 offboarder) can delete by channel_id without lookup.

async function storeChannelTokenSecret(channelId, channelToken, tenantId, coordinatorId) {
  const secretId = `${CHANNEL_TOKEN_SECRET_PREFIX}/${channelId}`;
  await secrets.send(
    new CreateSecretCommand({
      Name: secretId,
      Description: `Google Calendar watch-channel token for channel ${channelId} (tenant ${tenantId}, coordinator ${coordinatorId}).`,
      SecretString: JSON.stringify({
        channel_token: channelToken,
        channel_id: channelId,
        tenant_id: tenantId,
        coordinator_id: coordinatorId,
        created_at: new Date().toISOString(),
      }),
      Tags: [
        { Key: 'Subphase', Value: 'B5' },
        { Key: 'tenant_id', Value: tenantId },
        { Key: 'channel_id', Value: channelId },
      ],
    })
  );
  return secretId;
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
  if (!LISTENER_URL) {
    throw new Error('LISTENER_URL env var is required');
  }

  const { tenantId, coordinatorId, calendarId } = validateInput(event);
  log('onboarder_invoked', { tenant_id: tenantId, coordinator_id: coordinatorId, calendar_id: calendarId });

  // 1. OAuth client (per-tenant secret)
  const authClient = await getOAuthClient({ tenantId, coordinatorId });

  // 2. Seed last_sync_token via events.list (no syncToken)
  const seed = await seedInitialSyncToken(authClient, calendarId);
  log('sync_token_seeded', {
    tenant_id: tenantId,
    coordinator_id: coordinatorId,
    seed_pages: seed.pages,
    seed_events_seen: seed.totalSeen,
    seed_token_present: Boolean(seed.syncToken),
  });

  // 3. Generate channel identifiers
  const channelId = generateChannelId();
  const channelToken = generateChannelToken();
  const channelTokenSha256 = sha256Hex(channelToken);

  // 4. Store raw token in Secrets Manager (before events.watch so a failed
  // watch leaves a recoverable artifact rather than a stranded channel_id).
  const secretId = await storeChannelTokenSecret(channelId, channelToken, tenantId, coordinatorId);
  log('channel_token_secret_created', { channel_id: channelId, secret_id: secretId });

  // 5. Register the watch with Google
  const watch = await registerWatch(authClient, calendarId, channelId, channelToken, LISTENER_URL);
  log('events_watch_registered', {
    channel_id: channelId,
    resource_id: watch.resourceId,
    expiration: watch.expiration,
  });

  // 6. Write DDB row
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
  log('channel_row_written', { channel_id: channelId, tenant_id: tenantId });

  return {
    channel_id: channelId,
    expiration: watch.expiration,
    last_sync_token_seeded: Boolean(seed.syncToken),
    secret_id: secretId,
  };
};

// ─── Test-only exports ──────────────────────────────────────────────────────────

exports._test = {
  validateInput,
  generateChannelId,
  generateChannelToken,
  sha256Hex,
  storeChannelTokenSecret,
  writeChannelRow,
};
