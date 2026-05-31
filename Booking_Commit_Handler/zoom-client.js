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
 * Built on Node 20's global `fetch` (no new HTTP dependency — "concrete-first").
 *
 * ── Read-before-write idempotency (§3.1 / C8) ──
 *   Zoom has NO client-supplied idempotency key. createMeeting() therefore accepts
 *   `existingMeetingId`: when the caller (index.js) has already recorded a Zoom
 *   meeting id for this booking (slot-lock item, prior partial attempt), it is
 *   passed in and we RETURN it WITHOUT calling Zoom — no duplicate meeting on retry.
 *
 * Token cache: per-container, keyed by tenantId, TTL = expires_in - 60s (refresh a
 * minute early to avoid edge-of-window 401s, per §6.2). Acceptable at v1 scale.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const ZOOM_SECRET_PATH_PREFIX = process.env.ZOOM_SECRET_PATH_PREFIX || 'picasso/scheduling/zoom';
const ZOOM_OAUTH_URL = process.env.ZOOM_OAUTH_URL || 'https://zoom.us/oauth/token';
const ZOOM_API_BASE = process.env.ZOOM_API_BASE || 'https://api.zoom.us/v2';
const TOKEN_EARLY_REFRESH_MS = 60 * 1000;

const secrets = new SecretsManagerClient({});

// tenantId → { accessToken, expiresAtMs }
const _tokenCache = new Map();

function buildSecretPath(tenantId) {
  if (!tenantId) throw new Error('tenantId is required');
  return `${ZOOM_SECRET_PATH_PREFIX}/${tenantId}`;
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
  // S2S needs account_id; published-OAuth needs refresh_token. Exactly one shape.
  const isS2S = typeof parsed.account_id === 'string' && parsed.account_id.length > 0;
  const isRefresh = typeof parsed.refresh_token === 'string' && parsed.refresh_token.length > 0;
  if (!isS2S && !isRefresh) {
    throw new Error('Zoom secret must carry account_id (S2S) or refresh_token (OAuth)');
  }
  return parsed;
}

// Build the OAuth token request body per secret shape (runbook Model).
function buildTokenForm(secret) {
  if (typeof secret.account_id === 'string' && secret.account_id.length > 0) {
    return new URLSearchParams({
      grant_type: 'account_credentials',
      account_id: secret.account_id,
    });
  }
  return new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: secret.refresh_token,
  });
}

async function fetchAccessToken(secret) {
  const basic = Buffer.from(`${secret.client_id}:${secret.client_secret}`).toString('base64');
  const res = await fetch(ZOOM_OAUTH_URL, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: buildTokenForm(secret).toString(),
  });
  if (!res.ok) {
    // Do not echo the body (may include error detail tied to credentials).
    throw new Error(`Zoom OAuth token request failed: ${res.status}`);
  }
  const json = await res.json();
  if (!json.access_token) {
    throw new Error('Zoom OAuth response missing access_token');
  }
  const expiresInMs = (Number(json.expires_in) || 3600) * 1000;
  return { accessToken: json.access_token, expiresAtMs: Date.now() + expiresInMs };
}

async function getAccessToken(tenantId) {
  const cached = _tokenCache.get(tenantId);
  if (cached && cached.expiresAtMs - TOKEN_EARLY_REFRESH_MS > Date.now()) {
    return cached.accessToken;
  }
  const secret = await fetchZoomSecret(tenantId);
  const token = await fetchAccessToken(secret);
  _tokenCache.set(tenantId, {
    accessToken: token.accessToken,
    expiresAtMs: token.expiresAtMs - TOKEN_EARLY_REFRESH_MS,
  });
  return token.accessToken;
}

/**
 * createMeeting({ tenantId, coordinatorId, topic, start, end, timezone, existingMeetingId? })
 *   → { meetingId, joinUrl }
 *
 * read-before-write: if existingMeetingId is supplied (prior partial attempt
 * recorded one), reuse it and make NO Zoom API call.
 */
async function createMeeting({ tenantId, coordinatorId, topic, start, end, timezone, existingMeetingId }) {
  if (!tenantId || !coordinatorId || !start || !end) {
    throw new Error('tenantId, coordinatorId, start, and end are required');
  }
  if (existingMeetingId) {
    // Idempotent retry — recover the meeting without creating a duplicate.
    return getMeeting(tenantId, coordinatorId, existingMeetingId);
  }
  const token = await getAccessToken(tenantId);
  const durationMin = Math.max(1, Math.round((Date.parse(end) - Date.parse(start)) / 60000));
  const res = await fetch(`${ZOOM_API_BASE}/users/${encodeURIComponent(coordinatorId)}/meetings`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      topic: (topic || 'Appointment').slice(0, 200),
      type: 2, // scheduled meeting
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

async function getMeeting(tenantId, coordinatorId, meetingId) {
  const token = await getAccessToken(tenantId);
  const res = await fetch(`${ZOOM_API_BASE}/meetings/${encodeURIComponent(meetingId)}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    throw new Error(`Zoom get-meeting failed: ${res.status}`);
  }
  const json = await res.json();
  return { meetingId: String(json.id || meetingId), joinUrl: json.join_url };
}

// Compensating delete (§4.5 / §6.2): orphan-Zoom cleanup when a later commit step
// fails. A 404 is treated as success (already gone) so compensation is idempotent.
async function deleteMeeting(tenantId, meetingId) {
  if (!tenantId || !meetingId) {
    throw new Error('tenantId and meetingId are required');
  }
  const token = await getAccessToken(tenantId);
  const res = await fetch(`${ZOOM_API_BASE}/meetings/${encodeURIComponent(meetingId)}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${token}` },
  });
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
  deleteMeeting,
  getAccessToken,
  fetchZoomSecret,
  buildSecretPath,
  _resetForTests,
};
