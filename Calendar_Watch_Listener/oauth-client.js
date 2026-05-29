'use strict';

/**
 * oauth-client.js — Per-tenant Google OAuth2 client factory.
 *
 * Phase 2a (B2 plumbing only — handler does not call this yet; Phase 2b wires it in).
 *
 * Canonical secret path per `subphase_b_oauth_provisioning_runbook_2026-05-25.md`:
 *   picasso/scheduling/oauth/{tenantId}/{coordinatorId}
 *
 * Expected SecretString JSON shape (canonical schema per runbook line 244 & 198):
 *   { client_id, client_secret, refresh_token, scopes, coordinator_email }
 *
 * IAM grant is already in place on Calendar_Watch_Listener-exec-staging (verified
 * 2026-05-26): secretsmanager:GetSecretValue + DescribeSecret on
 *   arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/*
 *
 * Process-level cache: OAuth2Client instances are reused across warm Lambda
 * invocations keyed by secret-path. Stale tokens self-refresh via
 * google-auth-library; no manual refresh logic required here.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { OAuth2Client } = require('google-auth-library');

const OAUTH_SECRET_PATH_PREFIX = process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';

// Y1: cache entries expire after 50 min so a rotated/revoked refresh_token
// does not wedge the coordinator until the next cold start.  50 min is well
// under Google's 60-min access-token lifetime so a valid token is never evicted
// while it's still usable.
const CACHE_TTL_MS = Number(process.env.OAUTH_CACHE_TTL_MS || String(50 * 60 * 1000));

const secrets = new SecretsManagerClient({});

// Cache stores { client, cachedAt } so TTL can be checked on each access.
const _clientCache = new Map();

function buildSecretPath(tenantId, coordinatorId) {
  if (!tenantId || !coordinatorId) {
    throw new Error('tenantId and coordinatorId are required');
  }
  return `${OAUTH_SECRET_PATH_PREFIX}/${tenantId}/${coordinatorId}`;
}

async function fetchOAuthSecret(secretPath) {
  const result = await secrets.send(new GetSecretValueCommand({ SecretId: secretPath }));
  if (!result.SecretString) {
    throw new Error(`OAuth secret has no SecretString: ${secretPath}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(result.SecretString);
  } catch (err) {
    throw new Error(`OAuth secret is not valid JSON: ${secretPath}`);
  }
  for (const required of ['client_id', 'client_secret', 'refresh_token']) {
    if (!parsed[required]) {
      throw new Error(`OAuth secret missing required field "${required}": ${secretPath}`);
    }
  }
  return parsed;
}

async function getOAuthClient({ tenantId, coordinatorId }) {
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  const cached = _clientCache.get(secretPath);
  // Y1: treat an entry older than CACHE_TTL_MS as a miss so stale/rotated
  // credentials are re-fetched automatically without waiting for a cold start.
  if (cached && (Date.now() - cached.cachedAt) < CACHE_TTL_MS) {
    return cached.client;
  }
  const secret = await fetchOAuthSecret(secretPath);
  const client = new OAuth2Client({
    clientId: secret.client_id,
    clientSecret: secret.client_secret,
  });
  client.setCredentials({ refresh_token: secret.refresh_token });
  _clientCache.set(secretPath, { client, cachedAt: Date.now() });
  return client;
}

/**
 * Y1: evict a specific entry from the OAuth client cache.
 * Called by index.js when getOAuthClient throws so the next invocation
 * re-fetches the secret from Secrets Manager rather than replaying a
 * stale/revoked credential.
 *
 * @param {{ tenantId: string, coordinatorId: string }} param
 */
function clearCacheEntry({ tenantId, coordinatorId }) {
  try {
    const secretPath = buildSecretPath(tenantId, coordinatorId);
    _clientCache.delete(secretPath);
  } catch (_) {
    // buildSecretPath may throw if args are missing; safe to swallow here
    // because we're already on the error path.
  }
}

function _resetCacheForTests() {
  _clientCache.clear();
}

module.exports = {
  getOAuthClient,
  clearCacheEntry,
  buildSecretPath,
  fetchOAuthSecret,
  _resetCacheForTests,
  // Exported for tests that inject a fake Date.now
  _CACHE_TTL_MS: CACHE_TTL_MS,
};
