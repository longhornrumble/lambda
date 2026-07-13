'use strict';

/**
 * M-Hb — Abuse & cost controls (docs/messenger/CONTRACTS.md C4; plan §6 M-Hb).
 *
 * Threat: an unauthenticated public path (any DM to a connected Page/IG
 * account) into Bedrock spend — real once Meta Advanced Access ships and
 * traffic is no longer limited to the developer/tester allowlist. This
 * module implements two config-driven counters riding the C4
 * picasso-conversation-state table (additive stateTypes, no contract
 * amendment needed):
 *
 *   - per-PSID hourly turn count   (row: sessionId = `meta:{pageId}:{psid}`,
 *                                    stateType = `rl_user:{yyyymmddHH}`, UTC)
 *   - per-tenant daily turn count  (row: sessionId = `tenant:{tenantId}`,
 *                                    stateType = `rl_day:{yyyymmdd}`, UTC)
 *
 * Both counters are bumped together on every check (one Bedrock-spending
 * invocation ⇒ one increment of each) via a plain ADD UpdateItem — no
 * conditional logic, no read-then-write race window. TTL (`expires_at`) is
 * set only on first write (`if_not_exists`) to the bucket's end + 1h slack,
 * so the row self-cleans shortly after its window can no longer be bumped.
 *
 * Fail-open: any DDB error resolves to `{limited: false}` (logged WARN) —
 * this limiter is a cost control, never an availability gate. A flaky
 * conversation-state table must never be the reason a legitimate user gets
 * no reply.
 *
 * v1 scope note: `messenger_behavior.rate_limits` is NOT read through
 * `channel_overrides` (C2's per-channel override layer) — a single pair of
 * limits applies to both Messenger and Instagram. Revisit if a tenant needs
 * per-channel throttle tuning; keeping v1 simple per the plan.
 */

const { UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const DEFAULT_PER_USER_HOURLY = 30;
const DEFAULT_TENANT_DAILY = 1000;

// First N breaches past the limit still get the polite reply; sustained
// flood beyond that goes silent (bounded reply spam — mirrors C7's own
// drain-cap rate_limited behavior, just at a coarser per-user/per-tenant
// grain).
const POLITE_BREACH_MARGIN = 3;

const SECONDS_PER_HOUR = 3600;
const SECONDS_PER_DAY = 86400;

function pad2(n) {
  return String(n).padStart(2, '0');
}

/** UTC hour bucket key, e.g. '2026071304'. */
function utcHourBucket(date) {
  return (
    String(date.getUTCFullYear()) +
    pad2(date.getUTCMonth() + 1) +
    pad2(date.getUTCDate()) +
    pad2(date.getUTCHours())
  );
}

/** UTC day bucket key, e.g. '20260713'. */
function utcDayBucket(date) {
  return String(date.getUTCFullYear()) + pad2(date.getUTCMonth() + 1) + pad2(date.getUTCDate());
}

/** epoch seconds at the START of this UTC hour bucket. */
function hourBucketStartSec(date) {
  return Math.floor(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours()) / 1000
  );
}

/** epoch seconds at the START of this UTC day bucket. */
function dayBucketStartSec(date) {
  return Math.floor(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()) / 1000);
}

/**
 * Read `messenger_behavior.rate_limits` (C2) with code-owned defaults.
 * @param {object} config — tenant config
 * @returns {{per_user_hourly: number, tenant_daily: number}}
 */
function resolveLimits(config) {
  const cfg = config?.messenger_behavior?.rate_limits || {};
  return {
    per_user_hourly:
      typeof cfg.per_user_hourly === 'number' ? cfg.per_user_hourly : DEFAULT_PER_USER_HOURLY,
    tenant_daily: typeof cfg.tenant_daily === 'number' ? cfg.tenant_daily : DEFAULT_TENANT_DAILY,
  };
}

/**
 * ADD turn_count :one on a single counter row, setting the common C4
 * attributes on first write only (if_not_exists). Returns the post-ADD count.
 */
async function bumpCounter({ client, tableName, sessionId, stateType, expiresAt }) {
  const nowMs = Date.now();
  const result = await client.send(
    new UpdateCommand({
      TableName: tableName,
      Key: { sessionId, stateType },
      UpdateExpression:
        'ADD turn_count :one SET updated_at = :now, schema_version = if_not_exists(schema_version, :one), expires_at = if_not_exists(expires_at, :expiresAt)',
      ExpressionAttributeValues: { ':one': 1, ':now': nowMs, ':expiresAt': expiresAt },
      ReturnValues: 'UPDATED_NEW',
    })
  );
  return result?.Attributes?.turn_count ?? 1;
}

/**
 * Bump both the per-user-hourly and per-tenant-daily counters for one
 * Bedrock-spending invocation and report whether either limit is breached.
 *
 * Call ONCE per winning lock-hold (not once per drained message) — a C7
 * drain cycle already combines a coalesced burst into a single Bedrock
 * call, so one increment here is the correct unit of spend accounting.
 *
 * @param {{client: object, tableName: string, sessionId: string, tenantId: string, config: object, now?: Date}} params
 * @returns {Promise<{
 *   limited: boolean, userLimited: boolean, tenantLimited: boolean,
 *   userCount: number, tenantCount: number, shouldReplyPolitely: boolean,
 *   failedOpen?: boolean
 * }>}
 */
async function incrementAndCheck({ client, tableName, sessionId, tenantId, config, now }) {
  const limits = resolveLimits(config);
  const nowDate = now instanceof Date ? now : new Date(now ?? Date.now());

  const userStateType = `rl_user:${utcHourBucket(nowDate)}`;
  const tenantStateType = `rl_day:${utcDayBucket(nowDate)}`;
  const userExpiresAt = hourBucketStartSec(nowDate) + SECONDS_PER_HOUR + SECONDS_PER_HOUR; // bucket end + 1h slack
  const tenantExpiresAt = dayBucketStartSec(nowDate) + SECONDS_PER_DAY + SECONDS_PER_HOUR; // bucket end + 1h slack

  try {
    const [userCount, tenantCount] = await Promise.all([
      bumpCounter({
        client,
        tableName,
        sessionId,
        stateType: userStateType,
        expiresAt: userExpiresAt,
      }),
      bumpCounter({
        client,
        tableName,
        sessionId: `tenant:${tenantId}`,
        stateType: tenantStateType,
        expiresAt: tenantExpiresAt,
      }),
    ]);

    const userLimited = userCount > limits.per_user_hourly;
    const tenantLimited = tenantCount > limits.tenant_daily;
    const shouldReplyPolitely =
      (userLimited && userCount <= limits.per_user_hourly + POLITE_BREACH_MARGIN) ||
      (tenantLimited && tenantCount <= limits.tenant_daily + POLITE_BREACH_MARGIN);

    return {
      limited: userLimited || tenantLimited,
      userLimited,
      tenantLimited,
      userCount,
      tenantCount,
      shouldReplyPolitely,
    };
  } catch (err) {
    // Fail OPEN — a limiter-infra blip must never block a reply.
    console.warn(
      JSON.stringify({
        level: 'WARN',
        message: 'Rate-limit counter update failed — failing open',
        service: 'MetaResponseProcessor',
        sessionId,
        tenantId,
        error: err.message,
      })
    );
    return {
      limited: false,
      userLimited: false,
      tenantLimited: false,
      userCount: 0,
      tenantCount: 0,
      shouldReplyPolitely: false,
      failedOpen: true,
    };
  }
}

module.exports = {
  incrementAndCheck,
  resolveLimits,
  DEFAULT_PER_USER_HOURLY,
  DEFAULT_TENANT_DAILY,
};
