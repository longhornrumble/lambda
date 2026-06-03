'use strict';

/**
 * zoom-client.js — Zoom Server-to-Server OAuth + meeting create/delete.
 *
 * Canonical §3.1 / §6.2 (Zoom conferencing) + the ZOOM_OAUTH_PROVISIONING runbook.
 * Per-tenant credential at `picasso/scheduling/zoom/{tenantId}` (NEVER wildcard;
 * one secret per tenant). The runbook Model says C8 acquires the token "by SECRET
 * SHAPE": today an S2S `account_credentials` grant (account_id/client_id/
 * client_secret), later a published-OAuth `refresh_token` — the `POST /users/{id}/
 * meetings` call is identical, so this module branches ONLY on which fields the
 * secret carries and the meeting calls don't change.
 *
 * Built on Node 20's global `fetch`; EVERY call is bounded by AbortSignal.timeout
 * (an unbounded Zoom hang on the 60s commit path would strand a slot lock).
 *
 * ── Read-before-write idempotency (§3.1 / C8) ──
 *   Zoom has NO client-supplied idempotency key. createMeeting() accepts
 *   `existingMeetingId`: a prior partial attempt's meeting is RE-READ and reused
 *   (no duplicate). If that meeting is GONE (404 — compensation already deleted it),
 *   we fall through and create a fresh one rather than throwing.
 *
 * ── Token cache ──  per-container, keyed by tenantId, raw expiry stored once;
 *   the early-refresh margin (refresh ~60s before expiry, §6.2) is applied ONLY at
 *   the read check (no double-subtract). A 401 evicts the cached token and retries
 *   once. For the OAuth refresh-token shape, a rotated refresh_token returned by
 *   Zoom is written back to Secrets Manager so the next cold start isn't wedged.
 */

const {
  SecretsManagerClient,
  GetSecretValueCommand,
  PutSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');
const { sdkConfig } = require('./aws-client-config');

const ZOOM_SECRET_PATH_PREFIX = process.env.ZOOM_SECRET_PATH_PREFIX || 'picasso/scheduling/zoom';
const ZOOM_OAUTH_URL = process.env.ZOOM_OAUTH_URL || 'https://zoom.us/oauth/token';
const ZOOM_API_BASE = process.env.ZOOM_API_BASE || 'https://api.zoom.us/v2';
const TOKEN_EARLY_REFRESH_MS = 60 * 1000;
const ZOOM_FETCH_TIMEOUT_MS = Number(process.env.ZOOM_FETCH_TIMEOUT_MS || 5000);

const secrets = new SecretsManagerClient(sdkConfig());

// tenantId → { accessToken, expiresAtMs }  (expiresAtMs = RAW now+expires_in)
const _tokenCache = new Map();

function buildSecretPath(tenantId) {
  if (!tenantId) throw new Error('tenantId is required');
  return `${ZOOM_SECRET_PATH_PREFIX}/${tenantId}`;
}

// AbortSignal.timeout bounds every Zoom HTTP call (Fix 1).
function timeoutSignal() {
  return AbortSignal.timeout(ZOOM_FETCH_TIMEOUT_MS);
}

async function fetchZoomSecret(tenantId) {
  const secretPath = buildSecretPath(tenantId);
  const res = await secrets.send(new GetSecretValueCommand({ SecretId: secretPath }));
  // Omit the secret path from error text (it encodes the tenantId).
  if (!res.SecretString) {
    throw new Error('Zoom secret has no SecretString for the requested tenant');
  }
  let parsed;
  try {
    parsed = JSON.parse(res.SecretString);
  } catch (err) {
    throw new Error('Zoom secret is not valid JSON for the requested tenant');
  }
  if (typeof parsed.client_id !== 'string' || parsed.client_id.length === 0 ||
      typeof parsed.client_secret !== 'string' || parsed.client_secret.length === 0) {
    throw new Error('Zoom secret missing client_id/client_secret for the requested tenant');
  }
  const isS2S = typeof parsed.account_id === 'string' && parsed.account_id.length > 0;
  const isRefresh = typeof parsed.refresh_token === 'string' && parsed.refresh_token.length > 0;
  if (!isS2S && !isRefresh) {
    throw new Error('Zoom secret must carry account_id (S2S) or refresh_token (OAuth)');
  }
  return { parsed, secretPath };
}

function buildTokenForm(secret) {
  if (typeof secret.account_id === 'string' && secret.account_id.length > 0) {
    return new URLSearchParams({ grant_type: 'account_credentials', account_id: secret.account_id });
  }
  return new URLSearchParams({ grant_type: 'refresh_token', refresh_token: secret.refresh_token });
}

async function fetchAccessToken(secret) {
  const basic = Buffer.from(`${secret.client_id}:${secret.client_secret}`).toString('base64');
  const res = await fetch(ZOOM_OAUTH_URL, {
    method: 'POST',
    headers: { Authorization: `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: buildTokenForm(secret).toString(),
    signal: timeoutSignal(),
  });
  if (!res.ok) {
    throw new Error(`Zoom OAuth token request failed: ${res.status}`);
  }
  const json = await res.json();
  if (!json.access_token) {
    throw new Error('Zoom OAuth response missing access_token');
  }
  const expiresInMs = (Number(json.expires_in) || 3600) * 1000;
  return {
    accessToken: json.access_token,
    expiresAtMs: Date.now() + expiresInMs, // RAW expiry; early-refresh applied at read
    refreshToken: json.refresh_token || null,
  };
}

// (5a) Persist a rotated refresh_token back to Secrets Manager so a later cold
// start doesn't replay a stale token. Only for the OAuth refresh shape, and only
// when Zoom actually returned a NEW value. Best-effort: a writeback failure must
// not fail the booking (the in-memory token is already valid).
async function maybePersistRotatedToken(secretPath, secret, newRefreshToken) {
  if (!newRefreshToken || !secret.refresh_token || newRefreshToken === secret.refresh_token) {
    return;
  }
  try {
    await secrets.send(new PutSecretValueCommand({
      SecretId: secretPath,
      SecretString: JSON.stringify({ ...secret, refresh_token: newRefreshToken }),
    }));
  } catch (err) {
    console.warn(JSON.stringify({ event: 'zoom_refresh_token_writeback_failed', level: 'WARN', error: err.message }));
  }
}

async function getAccessToken(tenantId) {
  const cached = _tokenCache.get(tenantId);
  // (5b) early-refresh margin applied ONCE, here at the read (no double-subtract).
  if (cached && cached.expiresAtMs - TOKEN_EARLY_REFRESH_MS > Date.now()) {
    return cached.accessToken;
  }
  const { parsed: secret, secretPath } = await fetchZoomSecret(tenantId);
  const token = await fetchAccessToken(secret);
  await maybePersistRotatedToken(secretPath, secret, token.refreshToken);
  _tokenCache.set(tenantId, { accessToken: token.accessToken, expiresAtMs: token.expiresAtMs });
  return token.accessToken;
}

function evictToken(tenantId) {
  _tokenCache.delete(tenantId);
}

// (5d) Authed Zoom fetch with a 401 classifier: a 401 evicts the cached token,
// re-fetches, and retries ONCE. Returns the Response (callers handle 404/!ok).
async function zoomFetch(tenantId, url, opts = {}) {
  const token = await getAccessToken(tenantId);
  const doFetch = (t) => fetch(url, {
    ...opts,
    headers: { ...(opts.headers || {}), Authorization: `Bearer ${t}` },
    signal: timeoutSignal(),
  });
  let res = await doFetch(token);
  if (res.status === 401) {
    evictToken(tenantId);
    const fresh = await getAccessToken(tenantId);
    res = await doFetch(fresh);
  }
  return res;
}

/**
 * createMeeting({ tenantId, coordinatorId, topic, start, end, timezone, existingMeetingId? })
 *   → { meetingId, joinUrl }
 */
async function createMeeting({ tenantId, coordinatorId, topic, start, end, timezone, existingMeetingId }) {
  if (!tenantId || !coordinatorId || !start || !end) {
    throw new Error('tenantId, coordinatorId, start, and end are required');
  }
  if (existingMeetingId) {
    // read-before-write: reuse the prior meeting unless it's GONE (404) — in which
    // case (5c) fall through and create a fresh one rather than throwing.
    const existing = await getMeeting(tenantId, coordinatorId, existingMeetingId);
    if (existing) return existing;
  }
  const durationMin = Math.max(1, Math.round((Date.parse(end) - Date.parse(start)) / 60000));
  const res = await zoomFetch(tenantId, `${ZOOM_API_BASE}/users/${encodeURIComponent(coordinatorId)}/meetings`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      topic: (topic || 'Appointment').slice(0, 200),
      type: 2,
      start_time: start,
      duration: durationMin,
      timezone: timezone || 'UTC',
    }),
  });
  if (!res.ok) {
    throw new Error(`Zoom create-meeting failed: ${res.status}`);
  }
  const json = await res.json();
  if (!json.id || !json.join_url) {
    throw new Error('Zoom create-meeting response missing id/join_url');
  }
  return { meetingId: String(json.id), joinUrl: json.join_url };
}

// (5c) Returns null on 404 (meeting gone) so createMeeting can re-create instead of
// throwing on a retry whose prior meeting was already compensated away.
async function getMeeting(tenantId, coordinatorId, meetingId) {
  const res = await zoomFetch(tenantId, `${ZOOM_API_BASE}/meetings/${encodeURIComponent(meetingId)}`);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(`Zoom get-meeting failed: ${res.status}`);
  }
  const json = await res.json();
  return { meetingId: String(json.id || meetingId), joinUrl: json.join_url };
}

/**
 * updateMeeting({ tenantId, meetingId, start, end, timezone }) → void  (§B15 / §9.4 seam-3)
 *   Reschedule reuses the meeting (createMeeting({existingMeetingId}) preserves the JOIN URL),
 *   but its START TIME stays stale until this PATCH. Per-tenant token via getAccessToken (in zoomFetch).
 *   Idempotent: re-PATCH to the same time → 204, no-op-equivalent.
 */
async function updateMeeting({ tenantId, meetingId, start, end, timezone }) {
  if (!tenantId || !meetingId || !start || !end) {
    throw new Error('tenantId, meetingId, start, and end are required');
  }
  const durationMin = Math.max(1, Math.round((Date.parse(end) - Date.parse(start)) / 60000));
  const res = await zoomFetch(tenantId, `${ZOOM_API_BASE}/meetings/${encodeURIComponent(meetingId)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      start_time: start,
      duration: durationMin,
      timezone: timezone || 'UTC',
    }),
  });
  if (!res.ok) {
    throw new Error(`Zoom update-meeting failed: ${res.status}`);
  }
}

// Compensating delete (§4.5 / §6.2). 404 ⇒ already gone ⇒ success (idempotent).
async function deleteMeeting(tenantId, meetingId) {
  if (!tenantId || !meetingId) {
    throw new Error('tenantId and meetingId are required');
  }
  const res = await zoomFetch(tenantId, `${ZOOM_API_BASE}/meetings/${encodeURIComponent(meetingId)}`, { method: 'DELETE' });
  if (!res.ok && res.status !== 404) {
    throw new Error(`Zoom delete-meeting failed: ${res.status}`);
  }
}

function _resetForTests() {
  _tokenCache.clear();
}

module.exports = {
  createMeeting,
  getMeeting,
  updateMeeting,
  deleteMeeting,
  getAccessToken,
  fetchZoomSecret,
  buildSecretPath,
  evictToken,
  _resetForTests,
};
