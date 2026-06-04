'use strict';

/**
 * oauth-client.js — Per-(tenant, coordinator) Google OAuth2 client factory.
 *
 * C8 owns its own copy of this module (the Calendar_Watch_* convention — never a
 * shared exec role / shared client cache across Lambdas, CLAUDE.md "never-share-IAM").
 * Identical secret shape + path to the Calendar_Watch_* copies:
 *   picasso/scheduling/oauth/{tenantId}/{coordinatorId}
 *   SecretString JSON: { client_id, client_secret, refresh_token, coordinator_email, scopes? }
 *
 * The commit path (events.insert) needs a writable Calendar client for the WINNING
 * coordinator. google-auth-library transparently refreshes an expired access token
 * from the refresh_token; a 401 that still surfaces means the refresh itself failed
 * (revoked grant / invalid_grant) — index.js threads that into the §5.5-row-4
 * transient-vs-permanent decision and calls clearCacheEntry() so the next attempt
 * re-fetches the secret rather than replaying a stale/revoked credential.
 *
 * Cache TTL (50 min) is < Google's 60-min access-token lifetime so a still-valid
 * token is never evicted mid-use, but a rotated/revoked refresh_token is picked up
 * within the window without a cold start (matches the Listener Y1 hardening).
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { OAuth2Client } = require('google-auth-library');
const { sdkConfig } = require('./aws-client-config');

const OAUTH_SECRET_PATH_PREFIX = process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';

const CACHE_TTL_MS = Number(process.env.OAUTH_CACHE_TTL_MS || String(50 * 60 * 1000));

const secrets = new SecretsManagerClient(sdkConfig());

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
  // Error messages omit the secret path: it encodes tenantId + coordinatorId (an
  // email), so logging it on a fetch failure turns CloudWatch into a cross-tenant
  // existence oracle. Matches the hardened Calendar_Watch_* copies.
  if (!result.SecretString) {
    throw new Error('OAuth secret has no SecretString for the requested coordinator');
  }
  let parsed;
  try {
    parsed = JSON.parse(result.SecretString);
  } catch (err) {
    throw new Error('OAuth secret is not valid JSON for the requested coordinator');
  }
  for (const required of ['client_id', 'client_secret', 'refresh_token']) {
    if (typeof parsed[required] !== 'string' || parsed[required].length === 0) {
      throw new Error(`OAuth secret missing/empty required field "${required}" for the requested coordinator`);
    }
  }
  return parsed;
}

// Fetch-or-cache the per-(tenant, coordinator) entry: the authed client AND the
// coordinator's real Google calendarId. The OAuth secret's coordinator_email is the
// authoritative writable calendar; coordinatorId (the secret-path key / routing
// resourceId) is only an identifier and is NOT guaranteed to be a real calendar id —
// it can be an opaque label. Fall back to coordinatorId when coordinator_email is
// absent (v1 convention: the two are equal, and older secrets predate the field).
// Mirrors shared/scheduling/availability.js getAuthClient's { client, calendarId }.
async function _getCacheEntry(secretPath, coordinatorId) {
  const cached = _clientCache.get(secretPath);
  if (cached && (Date.now() - cached.cachedAt) < CACHE_TTL_MS) {
    return cached;
  }
  const secret = await fetchOAuthSecret(secretPath);
  const client = new OAuth2Client({
    clientId: secret.client_id,
    clientSecret: secret.client_secret,
  });
  client.setCredentials({ refresh_token: secret.refresh_token });
  const calendarId =
    typeof secret.coordinator_email === 'string' && secret.coordinator_email
      ? secret.coordinator_email
      : coordinatorId;
  const entry = { client, calendarId, cachedAt: Date.now() };
  _clientCache.set(secretPath, entry);
  return entry;
}

async function getOAuthClient({ tenantId, coordinatorId }) {
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  return (await _getCacheEntry(secretPath, coordinatorId)).client;
}

/**
 * Resolve the coordinator's real Google calendarId from the OAuth secret's
 * coordinator_email (falling back to coordinatorId for pre-coordinator_email secrets
 * / the v1 convention). Shares getOAuthClient's TTL cache, so a preceding
 * getOAuthClient call makes this a cache hit with no extra Secrets Manager fetch.
 * The commit path uses this so events.insert / events.delete and the persisted
 * Booking.coordinatorEmail target the coordinator's real calendar, NOT the
 * secret-path key (which can be an opaque label).
 */
async function getCoordinatorCalendarId({ tenantId, coordinatorId }) {
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  return (await _getCacheEntry(secretPath, coordinatorId)).calendarId;
}

/**
 * Evict a specific entry from the OAuth client cache. Called by index.js on a
 * 401 from events.insert so the retry re-fetches the secret from Secrets Manager
 * rather than replaying a stale/revoked credential.
 */
function clearCacheEntry({ tenantId, coordinatorId }) {
  try {
    const secretPath = buildSecretPath(tenantId, coordinatorId);
    _clientCache.delete(secretPath);
  } catch (_) {
    // buildSecretPath throws on missing args; safe to swallow on the error path.
  }
}

function _resetCacheForTests() {
  _clientCache.clear();
}

module.exports = {
  getOAuthClient,
  getCoordinatorCalendarId,
  clearCacheEntry,
  buildSecretPath,
  fetchOAuthSecret,
  _resetCacheForTests,
  _CACHE_TTL_MS: CACHE_TTL_MS,
};
