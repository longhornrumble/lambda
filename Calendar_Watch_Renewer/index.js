'use strict';

/**
 * Calendar_Watch_Renewer — scheduling sub-phase B Task B3 (+ B4 schedule, B7 alarms).
 *
 * Google Calendar watch channels expire (Google caps them ~7 days). This
 * Lambda runs on an EventBridge Scheduler cron (every ~6h, B4) and re-watches
 * any channel approaching expiry so coordinator-side calendar changes keep
 * flowing into the platform.
 *
 * Trigger: EventBridge Scheduler direct-invoke. The handler also accepts an
 * optional manual/test payload:
 *   { tenant_ids?: string[], renewal_buffer_ms?: number }
 * Absent those, it reads SCHEDULING_TENANT_IDS + RENEWAL_BUFFER_MS from env.
 *
 * Per-run flow, for each configured tenant:
 *   1. Query `tenant-expiration-index` GSI for rows with expiration <= now+buffer.
 *   2. For each expiring channel, renewChannel():
 *      a. fetch OAuth client (per tenant+coordinator)
 *      b. events.watch a FRESH channel (new UUID id + new 256-bit token)
 *      c. write the new channel row (carrying last_sync_token forward) with the
 *         SHA-256 hash of the new token — raw token never stored at rest (G6)
 *      d. events.stop the OLD channel + delete the old row (best-effort)
 *   3. On any per-channel failure: flip the OLD row to `unwatched_renewal_failed`
 *      and emit the `CalendarWatchRenewalFailed` metric. The OLD row is left
 *      intact so the NEXT run re-queries and retries it (self-healing).
 *   4. Emit a `CalendarWatchRenewerRunCompleted` heartbeat (dead-man's-switch).
 *
 * Zero-gap renewal: the new channel is live BEFORE the old one is stopped, and
 * the renewal buffer (7d) is far larger than the ~6h cadence, so a channel is
 * never left unwatched mid-renewal.
 *
 * Partial-failure safety mirrors the Onboarder: if the new-row write fails
 * after events.watch succeeded, the just-created channel is revoked
 * (events.stop) so no live Google channel is stranded without an
 * authenticating DDB row; the old row stays put for the next run.
 *
 * Security model: per-channel token = crypto.randomBytes(32) (B8); only its
 * SHA-256 hash is persisted; OAuth secrets read per-tenant (G2); no
 * process-level OAuth cache (G5); no channel-token store (G6).
 */

const crypto = require('crypto');
const {
  DynamoDBClient,
  QueryCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { CloudWatchClient, PutMetricDataCommand } = require('@aws-sdk/client-cloudwatch');

const { getOAuthClient } = require('./oauth-client');
const { registerWatch, stopWatch } = require('./calendar-watch');

// ─── AWS clients ────────────────────────────────────────────────────────────────

const ddb = new DynamoDBClient({});
const cw = new CloudWatchClient({});

// ─── Environment / constants ──────────────────────────────────────────────────────
// CHANNELS_TABLE + LISTENER_URL are REQUIRED (validated at handler entry): a
// missing table must not silently target another env's table, and a missing
// listener URL must not register a watch pointing nowhere.

const CHANNELS_TABLE   = process.env.CALENDAR_WATCH_CHANNELS_TABLE || '';
const LISTENER_URL     = process.env.LISTENER_URL || '';
const METRIC_NAMESPACE = process.env.METRIC_NAMESPACE || 'Picasso/Scheduling';
const EXPIRATION_INDEX = 'tenant-expiration-index';

const TENANT_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const DEFAULT_RENEWAL_BUFFER_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

// ─── Structured logging ─────────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── Input resolution ─────────────────────────────────────────────────────────────
// Tenant ids + renewal buffer come from the (optional) invocation payload, else
// from env. Tenant ids flow into the OAuth Secrets Manager path and DDB keys, so
// they are allowlisted exactly as the Onboarder validates its input.

function resolveTenantIds(event) {
  let raw;
  if (event && Array.isArray(event.tenant_ids) && event.tenant_ids.length > 0) {
    raw = event.tenant_ids;
  } else {
    const fromEnv = (process.env.SCHEDULING_TENANT_IDS || '').trim();
    if (!fromEnv) {
      throw new Error('No tenant ids: provide event.tenant_ids or set SCHEDULING_TENANT_IDS');
    }
    if (fromEnv.startsWith('[')) {
      try {
        raw = JSON.parse(fromEnv);
      } catch (err) {
        throw new Error('SCHEDULING_TENANT_IDS is not valid JSON');
      }
    } else {
      raw = fromEnv.split(',');
    }
  }
  const ids = raw.map((s) => String(s).trim()).filter(Boolean);
  if (ids.length === 0) {
    throw new Error('No valid tenant ids resolved');
  }
  for (const id of ids) {
    if (!TENANT_ID_RE.test(id)) {
      throw new Error(`Invalid tenant_id "${id}" — must match /^[A-Za-z0-9_-]{1,64}$/`);
    }
  }
  return ids;
}

function resolveBufferMs(event) {
  const candidate = (event && event.renewal_buffer_ms !== undefined)
    ? event.renewal_buffer_ms
    : process.env.RENEWAL_BUFFER_MS;
  if (candidate === undefined || candidate === '') {
    return DEFAULT_RENEWAL_BUFFER_MS;
  }
  const n = Number(candidate);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error('renewal_buffer_ms must be a positive integer (milliseconds)');
  }
  return n;
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

// ─── Row parsing ────────────────────────────────────────────────────────────────

function parseChannelRow(item) {
  return {
    channelId:        item.channel_id?.S         ?? null,
    tenantId:         item.tenant_id?.S          ?? null,
    coordinatorId:    item.coordinator_id?.S     ?? null,
    calendarId:       item.calendar_id?.S        ?? null,
    calendarProvider: item.calendar_provider?.S  ?? 'google',
    lastSyncToken:    item.last_sync_token?.S    ?? null,
    resourceId:       item.resource_id?.S        ?? null,
    status:           item.status?.S            ?? null,
    expiration:       Number(item.expiration?.N ?? '0'),
  };
}

// ─── DDB access ─────────────────────────────────────────────────────────────────

async function queryExpiringChannels(tenantId, thresholdMs) {
  const rows = [];
  let lastKey;
  do {
    const resp = await ddb.send(
      new QueryCommand({
        TableName: CHANNELS_TABLE,
        IndexName: EXPIRATION_INDEX,
        KeyConditionExpression: 'tenant_id = :t AND expiration <= :th',
        ExpressionAttributeValues: {
          ':t':  { S: tenantId },
          ':th': { N: String(thresholdMs) },
        },
        ExclusiveStartKey: lastKey,
      })
    );
    for (const item of resp.Items ?? []) {
      rows.push(parseChannelRow(item));
    }
    lastKey = resp.LastEvaluatedKey;
  } while (lastKey);
  return rows;
}

async function writeNewRow(row) {
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
    last_renewed_at:      { S: row.createdAt },
    renewed_from:         { S: row.renewedFrom },
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

// Flip the OLD row to unwatched_renewal_failed so ops can see a lapsing watch.
// attribute_exists guard prevents resurrecting a row that was already deleted.
async function markRenewalFailed(channelId) {
  await ddb.send(
    new UpdateItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: { S: channelId } },
      UpdateExpression: 'SET #s = :failed, last_renewal_attempt_at = :now',
      ConditionExpression: 'attribute_exists(channel_id)',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: {
        ':failed': { S: 'unwatched_renewal_failed' },
        ':now':    { S: new Date().toISOString() },
      },
    })
  );
}

async function deleteOldRow(channelId) {
  await ddb.send(
    new DeleteItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: { S: channelId } },
    })
  );
}

// ─── CloudWatch metric ──────────────────────────────────────────────────────────

async function emitMetric(metricName, dimsObj) {
  const Dimensions = Object.entries(dimsObj || {}).map(([Name, Value]) => ({
    Name,
    Value: String(Value),
  }));
  await cw.send(
    new PutMetricDataCommand({
      Namespace: METRIC_NAMESPACE,
      MetricData: [{ MetricName: metricName, Dimensions, Value: 1, Unit: 'Count', Timestamp: new Date() }],
    })
  );
}

// ─── Single-channel renewal ───────────────────────────────────────────────────────

async function renewChannel(oldRow) {
  const authClient = await getOAuthClient({ tenantId: oldRow.tenantId, coordinatorId: oldRow.coordinatorId });

  const newChannelId = generateChannelId();
  const newToken = generateChannelToken();
  const newHash = sha256Hex(newToken);

  // Register the replacement channel FIRST (zero-gap renewal).
  const watch = await registerWatch(authClient, oldRow.calendarId, newChannelId, newToken, LISTENER_URL);
  log('renewer_watch_registered', {
    old_channel_id: oldRow.channelId,
    new_channel_id: newChannelId,
    resource_id: watch.resourceId,
    expiration: watch.expiration,
  });

  // The new channel is LIVE. Validate expiration + persist; any failure here
  // would strand a Google channel pushing to a Listener with no authenticating
  // row, so revoke the new channel before re-throwing. The OLD row + channel
  // remain intact for the next run to retry.
  try {
    if (watch.expiration === null || watch.expiration === undefined || !/^\d+$/.test(String(watch.expiration))) {
      throw new Error(`events.watch returned a non-numeric expiration: ${watch.expiration}`);
    }
    await writeNewRow({
      channelId: newChannelId,
      tenantId: oldRow.tenantId,
      coordinatorId: oldRow.coordinatorId,
      calendarId: oldRow.calendarId,
      channelTokenSha256: newHash,
      lastSyncToken: oldRow.lastSyncToken,
      expiration: watch.expiration,
      resourceId: watch.resourceId,
      resourceUri: watch.resourceUri,
      renewedFrom: oldRow.channelId,
      createdAt: new Date().toISOString(),
    });
  } catch (err) {
    warn('renewer_compensating', {
      old_channel_id: oldRow.channelId,
      new_channel_id: newChannelId,
      resource_id: watch.resourceId,
      reason: err.message,
    });
    try {
      await stopWatch(authClient, newChannelId, watch.resourceId);
      warn('renewer_compensation_succeeded', { new_channel_id: newChannelId });
    } catch (stopErr) {
      warn('renewer_compensation_failed', {
        new_channel_id: newChannelId,
        resource_id: watch.resourceId,
        stop_error: stopErr.message,
      });
    }
    throw err;
  }

  // New row is live. Revoke the old channel + delete the old row. Both are
  // best-effort and MUST NOT fail an otherwise-successful renewal: the old
  // channel auto-expires within ~7d regardless, and a leftover stale row is
  // harmless (its channel is stopped, so no pushes arrive for it).
  let oldChannelStopped = true;
  try {
    await stopWatch(authClient, oldRow.channelId, oldRow.resourceId);
  } catch (stopErr) {
    oldChannelStopped = false;
    warn('renewer_old_channel_stop_failed', {
      old_channel_id: oldRow.channelId,
      resource_id: oldRow.resourceId,
      stop_error: stopErr.message,
    });
  }
  try {
    await deleteOldRow(oldRow.channelId);
  } catch (delErr) {
    warn('renewer_old_row_delete_failed', {
      old_channel_id: oldRow.channelId,
      delete_error: delErr.message,
    });
  }

  return { newChannelId, expiration: watch.expiration, oldChannelStopped };
}

// ─── Main handler ───────────────────────────────────────────────────────────────

exports.handler = async function handler(event) {
  if (!CHANNELS_TABLE) {
    throw new Error('CALENDAR_WATCH_CHANNELS_TABLE env var is required');
  }
  if (!LISTENER_URL || !LISTENER_URL.startsWith('https://')) {
    throw new Error('LISTENER_URL env var is required and must be https://');
  }

  const tenantIds = resolveTenantIds(event || {});
  const bufferMs = resolveBufferMs(event || {});
  const thresholdMs = Date.now() + bufferMs;

  log('renewer_invoked', { tenant_ids: tenantIds, buffer_ms: bufferMs, threshold_ms: thresholdMs });

  const summary = { scanned: 0, renewed: [], failed: [] };

  for (const tenantId of tenantIds) {
    let channels;
    try {
      channels = await queryExpiringChannels(tenantId, thresholdMs);
    } catch (err) {
      // A query failure for one tenant must not abort the others. Record it +
      // emit the renewal-failed metric so the alarm fires; continue.
      warn('renewer_query_failed', { tenant_id: tenantId, error: err.message });
      summary.failed.push({ tenant_id: tenantId, channel_id: null, error: `query_failed: ${err.message}` });
      await emitMetric('CalendarWatchRenewalFailed', { Provider: 'google' })
        .catch((mErr) => warn('renewer_metric_failed', { tenant_id: tenantId, error: mErr.message }));
      continue;
    }

    summary.scanned += channels.length;

    for (const ch of channels) {
      try {
        const res = await renewChannel(ch);
        summary.renewed.push({ old_channel_id: ch.channelId, new_channel_id: res.newChannelId, expiration: res.expiration });
        log('channel_renewed', {
          old_channel_id: ch.channelId,
          new_channel_id: res.newChannelId,
          tenant_id: ch.tenantId,
          expiration: res.expiration,
          old_channel_stopped: res.oldChannelStopped,
        });
      } catch (err) {
        warn('channel_renewal_failed', { channel_id: ch.channelId, tenant_id: ch.tenantId, error: err.message });
        await markRenewalFailed(ch.channelId)
          .catch((mErr) => warn('renewer_mark_failed_error', { channel_id: ch.channelId, error: mErr.message }));
        await emitMetric('CalendarWatchRenewalFailed', { Provider: ch.calendarProvider })
          .catch((mErr) => warn('renewer_metric_failed', { channel_id: ch.channelId, error: mErr.message }));
        summary.failed.push({ channel_id: ch.channelId, error: err.message });
      }
    }
  }

  // Dead-man's-switch heartbeat: emitted on every completed run regardless of
  // per-channel outcomes. The B7 alarm fires if no data point lands in 7h
  // (scheduler stopped / Lambda never invoked). Per-renewal failures are caught
  // by the separate CalendarWatchRenewalFailed alarm, not this one.
  await emitMetric('CalendarWatchRenewerRunCompleted', {})
    .catch((mErr) => warn('renewer_heartbeat_metric_failed', { error: mErr.message }));

  log('renewer_run_complete', {
    scanned: summary.scanned,
    renewed_count: summary.renewed.length,
    failed_count: summary.failed.length,
  });
  return summary;
};

// ─── Test-only exports ──────────────────────────────────────────────────────────

exports._test = {
  resolveTenantIds,
  resolveBufferMs,
  generateChannelId,
  generateChannelToken,
  sha256Hex,
  parseChannelRow,
  renewChannel,
  queryExpiringChannels,
};
