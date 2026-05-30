'use strict';

/**
 * Calendar_Watch_Offboarder — scheduling sub-phase B Task B6.
 *
 * Tears down Google Calendar push-notification watch channels when a coordinator
 * is no longer bookable. The two upstream business triggers — AdminEmployee
 * `scheduling_tags` cleared, or the Workspace account suspended (canonical §4.5
 * row 4) — both resolve to the same action: stop the coordinator's live watch
 * channel(s) and delete their DDB row(s). v1 pilot-scale invocation is direct
 * (`aws lambda invoke`); a future DDB-stream trigger on
 * picasso-employee-registry-v2-{env} can subscribe this handler once the E13 UI
 * / F2 onboarding flow populates `scheduling_tags` (same deferral as the B5
 * Onboarder). Until then this Lambda is also the programmatic `channels.stop`
 * teardown bridge the B3/B5 smoke runbooks refer to.
 *
 * Input (direct-invoke) — exactly one target selector, both requiring tenant_id:
 *   { tenant_id, coordinator_id }   → offboard ALL active channels for that
 *                                     coordinator (the offboarding trigger)
 *   { tenant_id, channel_id }       → offboard one specific channel (operator
 *                                     teardown of a known leftover channel)
 *
 * Per-channel flow:
 *   1. Resolve the target row(s) (GetItem by channel_id, or Query the
 *      tenant-expiration-index by tenant_id then filter by coordinator_id).
 *   2. Validate the identifiers read off the row (G3 allowlist) before they flow
 *      into the OAuth secret path / DDB keys.
 *   3. Fetch the per-tenant OAuth client and channels.stop the channel.
 *      - 204 success, or a 404/410 "already gone" → proceed to delete the row.
 *      - any other (transient) stop failure → leave the row in place so a
 *        re-invoke retries, record the channel as failed, continue.
 *   4. Delete the DDB row, guarded by tenant ownership (G6).
 *
 * Idempotent / at-least-once safe: offboarding a coordinator/channel with no
 * rows is a no-op (empty summary, not an error); re-invoking after success finds
 * nothing to do.
 *
 * Return: { requested, stopped, deleted, failed: [{ channel_id, error }] }
 *
 * Security model: per-tenant OAuth scope (G2 — IAM grants only
 * picasso/scheduling/oauth/{tenant}/*); no OAuth process cache (G5); the raw
 * channel token is never read or stored here (G6 — the Offboarder doesn't touch
 * the token; teardown needs only channel_id + resourceId). Identifiers read off
 * a DDB row are allowlisted before use so a corrupted/crafted row can't
 * path-traverse to another coordinator's secret or delete another tenant's row.
 */

const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { getOAuthClient } = require('./oauth-client');
const { stopWatch } = require('./calendar-watch');

// ─── AWS clients ────────────────────────────────────────────────────────────────

const ddb = new DynamoDBClient({});

// ─── Environment ────────────────────────────────────────────────────────────────
// CHANNELS_TABLE is REQUIRED (no silent default): a missing table name must not
// silently delete rows from another env's table. Validated at handler entry.

const CHANNELS_TABLE  = process.env.CALENDAR_WATCH_CHANNELS_TABLE || '';
const EXPIRATION_INDEX = 'tenant-expiration-index';

// ─── Input format allowlists ────────────────────────────────────────────────────
// tenant_id / coordinator_id flow into the OAuth Secrets Manager path
// (picasso/scheduling/oauth/{tenantId}/{coordinatorId}) and into DDB keys; the
// same charset the Onboarder/Renewer enforce. channel_id is a UUID we minted.

const TENANT_ID_RE      = /^[A-Za-z0-9_-]{1,64}$/;
const COORDINATOR_ID_RE = /^[A-Za-z0-9._@+-]{1,128}$/;
const CHANNEL_ID_RE     = /^[A-Za-z0-9-]{1,128}$/;

// ─── Structured logging ─────────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── Input validation ───────────────────────────────────────────────────────────
// Exactly one of coordinator_id / channel_id selects the teardown target; both
// require tenant_id so every stop + delete is tenant-ownership-guarded.

function validateInput(input) {
  if (!input || typeof input !== 'object') {
    throw new Error('Input must be a JSON object');
  }
  const tenantId = input.tenant_id;
  if (!tenantId || typeof tenantId !== 'string' || !TENANT_ID_RE.test(tenantId)) {
    throw new Error('tenant_id is required and must match /^[A-Za-z0-9_-]{1,64}$/');
  }
  const hasCoordinator = input.coordinator_id !== undefined && input.coordinator_id !== null;
  const hasChannel = input.channel_id !== undefined && input.channel_id !== null;
  if (hasCoordinator === hasChannel) {
    throw new Error('exactly one of coordinator_id or channel_id is required');
  }
  if (hasCoordinator) {
    if (typeof input.coordinator_id !== 'string' || !COORDINATOR_ID_RE.test(input.coordinator_id)) {
      throw new Error('coordinator_id must match /^[A-Za-z0-9._@+-]{1,128}$/');
    }
    return { tenantId, coordinatorId: input.coordinator_id, channelId: null };
  }
  if (typeof input.channel_id !== 'string' || !CHANNEL_ID_RE.test(input.channel_id)) {
    throw new Error('channel_id must match /^[A-Za-z0-9-]{1,128}$/');
  }
  return { tenantId, coordinatorId: null, channelId: input.channel_id };
}

// ─── Row parsing ────────────────────────────────────────────────────────────────

function parseChannelRow(item) {
  return {
    channelId:        item.channel_id?.S        ?? null,
    tenantId:         item.tenant_id?.S         ?? null,
    coordinatorId:    item.coordinator_id?.S    ?? null,
    calendarProvider: item.calendar_provider?.S ?? 'google',
    resourceId:       item.resource_id?.S       ?? null,
  };
}

// ─── "Already gone" detection ─────────────────────────────────────────────────────
// A 404/410 from channels.stop means the channel already expired or was already
// stopped — for offboarding that is success-equivalent (the end state we want is
// "not watched"), so we proceed to delete the row. Any other error is treated as
// transient: leave the row for a retry.

function isAlreadyGone(err) {
  const status = err?.response?.status ?? err?.code;
  return status === 404 || status === 410 || status === '404' || status === '410';
}

// A revoked OAuth grant — the defining signal of the "account suspended" trigger
// path (§4.5 row 4): when a Workspace account is suspended (or the coordinator
// revokes the app's access), the stored refresh_token stops working, surfacing
// as a 401 from channels.stop or an `invalid_grant`/`unauthorized_client` from
// the SDK's token auto-refresh. We can no longer stop the channel (no valid
// token) — but Google also stops delivering pushes for a revoked grant, so the
// channel is effectively dead. Treat it like already-gone and DELETE the row;
// leaving it would make the Renewer renew a departed coordinator's channel
// forever and trip a false CalendarWatchRenewalFailed alarm. 403 is deliberately
// NOT included — Google overloads it for rate/quota limits, which are transient.
function isAuthRevoked(err) {
  const status = err?.response?.status ?? err?.code;
  if (status === 401 || status === '401') {
    return true;
  }
  const oauthError = err?.response?.data?.error;
  if (oauthError === 'invalid_grant' || oauthError === 'unauthorized_client') {
    return true;
  }
  return typeof err?.message === 'string' && err.message.includes('invalid_grant');
}

// ─── Target resolution ──────────────────────────────────────────────────────────

async function getRowByChannelId(channelId, tenantId) {
  const resp = await ddb.send(
    new GetItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: { S: channelId } },
      ProjectionExpression:
        'channel_id, tenant_id, coordinator_id, calendar_provider, resource_id',
    })
  );
  if (!resp.Item) {
    return [];
  }
  const row = parseChannelRow(resp.Item);
  // G6: a channel_id only offboards if it belongs to the asserted tenant.
  if (row.tenantId !== tenantId) {
    warn('offboarder_channel_tenant_mismatch', {
      channel_id: channelId,
      requested_tenant: tenantId,
    });
    return [];
  }
  return [row];
}

async function getRowsByCoordinator(coordinatorId, tenantId) {
  // No coordinator GSI exists on the channels table (canonical §14.1 GSIs are
  // channel_id PK + tenant-expiration-index). At v1 pilot scale a tenant has a
  // handful of channels, so query the tenant partition and filter by
  // coordinator_id client-side. Query without an expiration bound returns every
  // channel for the tenant.
  const rows = [];
  let lastKey;
  do {
    const resp = await ddb.send(
      new QueryCommand({
        TableName: CHANNELS_TABLE,
        IndexName: EXPIRATION_INDEX,
        KeyConditionExpression: 'tenant_id = :t',
        ExpressionAttributeValues: { ':t': { S: tenantId } },
        ExclusiveStartKey: lastKey,
      })
    );
    for (const item of resp.Items ?? []) {
      const row = parseChannelRow(item);
      if (row.coordinatorId === coordinatorId) {
        rows.push(row);
      }
    }
    lastKey = resp.LastEvaluatedKey;
  } while (lastKey);
  return rows;
}

// ─── DDB delete ─────────────────────────────────────────────────────────────────

async function deleteRow(channelId, tenantId) {
  // G6: guard the delete by tenant ownership so a corrupted row's channel_id can
  // never delete a different tenant's row.
  await ddb.send(
    new DeleteItemCommand({
      TableName: CHANNELS_TABLE,
      Key: { channel_id: { S: channelId } },
      ConditionExpression: 'tenant_id = :t',
      ExpressionAttributeValues: { ':t': { S: tenantId } },
    })
  );
}

// ─── Single-channel offboard ──────────────────────────────────────────────────────

async function offboardChannel(row) {
  // G3: validate identifiers read off the DDB row before they flow into the
  // OAuth secret path / DDB keys.
  if (typeof row.tenantId !== 'string' || !TENANT_ID_RE.test(row.tenantId)) {
    throw new Error(`channel row has an invalid tenant_id for channel ${row.channelId}`);
  }
  if (typeof row.coordinatorId !== 'string' || !COORDINATOR_ID_RE.test(row.coordinatorId)) {
    throw new Error(`channel row has an invalid coordinator_id for channel ${row.channelId}`);
  }

  // Stop the Google channel. Best-effort with a meaningful distinction:
  //   - success / already-gone (404|410) → delete the row
  //   - transient failure                → leave the row for the next invoke
  if (!row.resourceId) {
    // No resourceId means channels.stop can't target the channel (Google needs
    // both id + resourceId). The channel self-limits (auto-expires ~7d); delete
    // the row so we stop tracking a channel we can't revoke.
    warn('offboarder_no_resource_id', {
      channel_id: row.channelId,
      note: 'cannot channels.stop without resourceId; channel auto-expires ~7d',
    });
  } else {
    const authClient = await getOAuthClient({ tenantId: row.tenantId, coordinatorId: row.coordinatorId });
    try {
      await stopWatch(authClient, row.channelId, row.resourceId);
      log('offboarder_watch_stopped', { channel_id: row.channelId, tenant_id: row.tenantId });
    } catch (err) {
      if (isAlreadyGone(err)) {
        warn('offboarder_channel_already_gone', {
          channel_id: row.channelId,
          note: 'channels.stop returned 404/410; treating as already stopped',
        });
      } else if (isAuthRevoked(err)) {
        // The "account suspended" trigger path (§4.5 row 4): grant revoked, so
        // the channel can't be stopped but is dead Google-side. Delete the row.
        warn('offboarder_channel_auth_revoked', {
          channel_id: row.channelId,
          note: 'OAuth grant revoked (account suspended / access removed); deleting row',
        });
      } else {
        // Transient — keep the row so a re-invoke retries the stop.
        throw new Error(`channels.stop failed (transient): ${err.message}`);
      }
    }
  }

  await deleteRow(row.channelId, row.tenantId);
  log('offboarder_row_deleted', { channel_id: row.channelId, tenant_id: row.tenantId });
}

// ─── Main handler ───────────────────────────────────────────────────────────────

exports.handler = async function handler(event) {
  if (!CHANNELS_TABLE) {
    throw new Error('CALENDAR_WATCH_CHANNELS_TABLE env var is required');
  }

  const { tenantId, coordinatorId, channelId } = validateInput(event);
  log('offboarder_invoked', {
    tenant_id: tenantId,
    coordinator_id: coordinatorId,
    channel_id: channelId,
  });

  const rows = channelId
    ? await getRowByChannelId(channelId, tenantId)
    : await getRowsByCoordinator(coordinatorId, tenantId);

  const summary = { requested: rows.length, stopped: [], deleted: [], failed: [] };

  for (const row of rows) {
    try {
      await offboardChannel(row);
      summary.stopped.push(row.channelId);
      summary.deleted.push(row.channelId);
    } catch (err) {
      warn('offboarder_channel_failed', {
        channel_id: row.channelId,
        tenant_id: row.tenantId,
        error: err.message,
      });
      summary.failed.push({ channel_id: row.channelId, error: err.message });
    }
  }

  log('offboarder_run_complete', {
    tenant_id: tenantId,
    requested: summary.requested,
    deleted_count: summary.deleted.length,
    failed_count: summary.failed.length,
  });
  return summary;
};

// ─── Test-only exports ──────────────────────────────────────────────────────────

exports._test = {
  validateInput,
  parseChannelRow,
  isAlreadyGone,
  isAuthRevoked,
  getRowByChannelId,
  getRowsByCoordinator,
  deleteRow,
  offboardChannel,
};
