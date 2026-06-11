'use strict';

/**
 * jti.js — Init-token single-use enforcement via the picasso-token-jti-blacklist DDB table.
 *
 * The `picasso-token-jti-blacklist` table key schema:
 *   PK (HASH): tenantId (S)
 *   SK (RANGE): jti      (S)
 * TTL attribute: ttl (N, Unix epoch seconds) — DDB TTL removes burned items after ~48h window.
 *
 * Only /connect burns the jti (the single-use gate). /connection/status is a repeated read-only
 * probe and MUST NOT burn; /oauth/callback's state token has a 10-min TTL + code-exchange
 * self-limiting so replay is already structurally infeasible there (out of scope, see PR body).
 *
 * FAIL-OPEN DESIGN: if DDB is unavailable, we log `jti_burn_unavailable` and allow the connect
 * to proceed. Rationale: an outage that bricks onboarding for ALL coordinators is higher-harm
 * than a very narrow replay window (already short-TTL + no-referrer). This is documented here
 * because it is an explicit, reasoned security trade-off — NOT an oversight.
 *
 * Env:
 *   JTI_BLACKLIST_TABLE — when unset/empty, burn is OFF (logs a once-per-cold-start warning).
 *                         Safe deploy: the integrator wires the env + dynamodb:PutItem grant
 *                         after this code ships.
 *
 * DI seam: pass `{ putItem }` in deps to swap in the real DDB client command, or a mock.
 */

const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const JTI_BLACKLIST_TABLE = process.env.JTI_BLACKLIST_TABLE || '';

// Bounded client (mirror index.js pattern).
const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const MAX_ATTEMPTS = Number(process.env.AWS_MAX_ATTEMPTS || 2);

// Lazy-init: only create if the env is configured (avoids any SDK overhead when off).
let _ddb = null;
function getDdbClient() {
  if (!_ddb) {
    _ddb = new DynamoDBClient({
      maxAttempts: MAX_ATTEMPTS,
      requestHandler: new NodeHttpHandler({
        connectionTimeout: CONNECTION_TIMEOUT_MS,
        requestTimeout: REQUEST_TIMEOUT_MS,
      }),
    });
  }
  return _ddb;
}

// Warn once per cold start if the env is absent so the operator notices.
let _warnedUnconfigured = false;

/**
 * Attempt to burn a jti by doing a conditional PutItem (attribute_not_exists on both keys).
 *
 * Returns:
 *   { burned: true }  — first use; caller should proceed with the connect flow.
 *   { burned: false, reason: 'already_used' } — second use; caller should reject.
 *   { burned: true, warn: 'unavailable' }     — DDB error; fail-open (proceed, log loud).
 *   { burned: true, warn: 'unconfigured' }    — env unset; burn is OFF.
 *
 * @param {object} args  - { tenantId, jti, expSeconds }
 *   expSeconds: the token's `exp` claim (Unix seconds). TTL = exp + 600s buffer so the
 *               burned record outlives the token window by 10 minutes.
 * @param {object} [deps] - { putItem } injectable for tests
 */
async function burnJti({ tenantId, jti, expSeconds }, deps = {}) {
  if (!JTI_BLACKLIST_TABLE) {
    if (!_warnedUnconfigured) {
      _warnedUnconfigured = true;
      console.warn(JSON.stringify({
        event: 'jti_burn_unconfigured',
        level: 'WARN',
        message: 'JTI_BLACKLIST_TABLE env is unset — init-token single-use enforcement is OFF',
      }));
    }
    return { burned: true, warn: 'unconfigured' };
  }

  const ttl = expSeconds + 600; // 10-min buffer past token expiry

  try {
    const putItem = deps.putItem || ((cmd) => getDdbClient().send(cmd));
    await putItem(new PutItemCommand({
      TableName: JTI_BLACKLIST_TABLE,
      Item: {
        tenantId: { S: String(tenantId) },
        jti:      { S: String(jti) },
        ttl:      { N: String(ttl) },
      },
      // Conditional write: only succeeds if neither key already exists.
      // First use → no existing item → ConditionExpression passes → item written.
      // Replay  → item exists from prior burn → ConditionExpression fails → throws ConditionalCheckFailedException.
      ConditionExpression: 'attribute_not_exists(tenantId) AND attribute_not_exists(jti)',
    }));
    return { burned: true };
  } catch (err) {
    if (err && err.name === 'ConditionalCheckFailedException') {
      // This jti has already been used — reject the replay.
      return { burned: false, reason: 'already_used' };
    }
    // Any other DDB error (throttle, outage, permission issue): fail-open.
    // Connecting twice during an outage is lower-harm than blocking ALL onboarding.
    console.warn(JSON.stringify({
      event: 'jti_burn_unavailable',
      level: 'WARN',
      tenant_id: tenantId,
      name: err && err.name,
      message: 'DDB jti burn unavailable — failing open to preserve onboarding availability',
    }));
    return { burned: true, warn: 'unavailable' };
  }
}

module.exports = {
  burnJti,
  JTI_BLACKLIST_TABLE,
  // Test-only: allow resetting the cold-start warn flag and the DDB client.
  _resetForTest: () => {
    _warnedUnconfigured = false;
    _ddb = null;
  },
};
