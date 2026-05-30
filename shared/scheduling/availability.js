'use strict';

/**
 * availability.js — Concrete Google freeBusy availability source.
 *
 * WS-C4 / FROZEN_CONTRACTS §B1 / canonical §10.2. v1 is Google-`freeBusy.query`-only:
 * NO provider abstraction (canonical §4.3 "concrete-first"; a second provider gets a
 * second concrete module + an integrator decision, not a speculative interface here).
 *
 * Consumed by C6 (pool evaluation builds `freeBusyByResource` from this).
 *
 * OAuth: this mirrors the per-`(tenantId, coordinatorId)` secret pattern of the
 * `Calendar_Watch_*` oauth-client.js modules — secret at
 *   picasso/scheduling/oauth/{tenantId}/{coordinatorId}
 * with JSON `{ client_id, client_secret, refresh_token, coordinator_email }`. There is
 * deliberately NO process-level OAuth-client cache here: the only cache is the 60s
 * freeBusy-result cache below, so a cache-miss always re-fetches the secret fresh
 * (a rotated/revoked refresh_token is picked up on the next miss; no wildcard secret).
 *
 * Cache key is `${tenantId}:${coordinatorId}:${windowBucket}` (Security-Reviewer P2,
 * 2026-05-02) — tenant-prefixed so the SAME coordinator email under two tenants can
 * never share an entry (cross-tenant credential/availability leak). See the
 * cross-tenant-isolation test.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { OAuth2Client } = require('google-auth-library');
const calendarApi = require('@googleapis/calendar');

const calendar = calendarApi.calendar('v3');

const SOURCE = 'google_freebusy';

// 60-second TTL (canonical §10.2). Overridable for tests/ops via env; defaults to 60s.
const CACHE_TTL_MS = Number(process.env.FREEBUSY_CACHE_TTL_MS || String(60 * 1000));

const OAUTH_SECRET_PATH_PREFIX = process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';

const secrets = new SecretsManagerClient({});

// freeBusy-result cache. Key = `${tenantId}:${coordinatorId}:${windowBucket}`.
// Value = { expiresAt: <ms epoch>, result: <frozen §B1 return object> }.
const _cache = new Map();

// The cache key joins three parts with ':'. tenantId is an opaque platform id and
// coordinatorId is an email/stable id — neither contains ':' — so the prefix
// `${tenantId}:${coordinatorId}:` used by invalidate() cannot straddle a boundary.
function buildCacheKey(tenantId, coordinatorId, windowStart, windowEnd) {
  // windowBucket is the EXACT query window (no rounding): two different windows must
  // never collide, or a caller would get busy intervals computed for a different range.
  return `${tenantId}:${coordinatorId}:${windowStart}|${windowEnd}`;
}

function buildSecretPath(tenantId, coordinatorId) {
  return `${OAUTH_SECRET_PATH_PREFIX}/${tenantId}/${coordinatorId}`;
}

/**
 * Fetch the per-(tenant, coordinator) OAuth secret and build an authed client.
 * Error messages deliberately OMIT the secret path (it encodes a tenantId + a
 * coordinator email; leaking it to CloudWatch turns logs into a cross-tenant
 * existence oracle — matches the hardened Calendar_Watch_* copies).
 */
async function getAuthClient(tenantId, coordinatorId) {
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  const res = await secrets.send(new GetSecretValueCommand({ SecretId: secretPath }));
  if (!res.SecretString) {
    throw new Error('OAuth secret has no SecretString for the requested coordinator');
  }
  let parsed;
  try {
    parsed = JSON.parse(res.SecretString);
  } catch (err) {
    throw new Error('OAuth secret is not valid JSON for the requested coordinator');
  }
  for (const required of ['client_id', 'client_secret', 'refresh_token']) {
    if (typeof parsed[required] !== 'string' || parsed[required].length === 0) {
      throw new Error(`OAuth secret missing/empty required field "${required}" for the requested coordinator`);
    }
  }
  const client = new OAuth2Client({
    clientId: parsed.client_id,
    clientSecret: parsed.client_secret,
  });
  client.setCredentials({ refresh_token: parsed.refresh_token });
  // Forward-compatible read: prefer the explicit calendar address; fall back to the
  // coordinatorId when an older secret predates the coordinator_email field.
  const calendarId =
    typeof parsed.coordinator_email === 'string' && parsed.coordinator_email
      ? parsed.coordinator_email
      : coordinatorId;
  return { client, calendarId };
}

/**
 * §B1: getBusyIntervals({ tenantId, resourceId, coordinatorId, windowStart, windowEnd })
 *   → { busy: [{ start, end }], cachedAt, source: 'google_freebusy' }
 *
 * resourceId is part of the frozen signature (the caller's routing resource) but in
 * v1 availability is computed against the coordinator's own calendar, so the query
 * and the cache key are keyed on coordinatorId, not resourceId (per §B1's locked key).
 */
async function getBusyIntervals({ tenantId, resourceId, coordinatorId, windowStart, windowEnd }) {
  if (!tenantId || !coordinatorId || !windowStart || !windowEnd) {
    throw new Error('tenantId, coordinatorId, windowStart, and windowEnd are required');
  }

  const cacheKey = buildCacheKey(tenantId, coordinatorId, windowStart, windowEnd);
  const now = Date.now();
  const cached = _cache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return cached.result;
  }

  const { client, calendarId } = await getAuthClient(tenantId, coordinatorId);

  const response = await calendar.freebusy.query({
    auth: client,
    requestBody: {
      timeMin: windowStart,
      timeMax: windowEnd,
      items: [{ id: calendarId }],
    },
  });

  const calData = response.data && response.data.calendars && response.data.calendars[calendarId];
  if (calData && Array.isArray(calData.errors) && calData.errors.length > 0) {
    // Propagate so the caller can DLQ + alarm (matches calendar-api.js: only the three
    // discriminated statuses are swallowed; everything else surfaces).
    const reasons = calData.errors.map((e) => e.reason || 'unknown').join(',');
    throw new Error(`freeBusy.query returned calendar errors: ${reasons}`);
  }

  const busy = ((calData && calData.busy) || []).map((b) =>
    Object.freeze({ start: b.start, end: b.end })
  );

  // Freeze the cached object: the same reference is handed to every consumer of a
  // cache-hit, so an in-place mutation by one caller must not corrupt another's read.
  const result = Object.freeze({
    busy: Object.freeze(busy),
    cachedAt: new Date(now).toISOString(),
    source: SOURCE,
  });

  _cache.set(cacheKey, { expiresAt: now + CACHE_TTL_MS, result });
  return result;
}

/**
 * invalidate(tenantId, coordinatorId) — drop every cached window for one coordinator
 * under one tenant. Called by the B2 listener on a calendar push (the listener just
 * imports and calls this; it is not wired here).
 */
function invalidate(tenantId, coordinatorId) {
  if (!tenantId || !coordinatorId) {
    return;
  }
  const prefix = `${tenantId}:${coordinatorId}:`;
  for (const key of _cache.keys()) {
    if (key.startsWith(prefix)) {
      _cache.delete(key);
    }
  }
}

function _resetCacheForTests() {
  _cache.clear();
}

module.exports = {
  getBusyIntervals,
  invalidate,
  buildSecretPath,
  buildCacheKey,
  _resetCacheForTests,
  _CACHE_TTL_MS: CACHE_TTL_MS,
  _SOURCE: SOURCE,
};
