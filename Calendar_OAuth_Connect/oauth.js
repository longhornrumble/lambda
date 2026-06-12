'use strict';

/**
 * oauth.js — thin wrapper over google-auth-library's OAuth2Client for the consent flow.
 *
 * Reuses the SAME library the shipped Calendar_Watch_Onboarder/oauth-client.js uses (which is
 * a token READER); this is the complementary token WRITER side. Four operations:
 *   • buildAuthUrl  — the Google consent redirect (access_type=offline + prompt=consent so a
 *                     refresh_token is always returned; scope-minimized per D2).
 *   • exchangeCode  — authorization_code → tokens (must include a refresh_token).
 *   • probeRefresh  — refresh_token → access_token, used by /connection/status to detect
 *                     revocation (an invalid_grant throw classifies as permanently disconnected).
 *   • revokeToken   — POST to Google's revocation endpoint (§E11b). Best-effort: caller MUST
 *                     treat a network/4xx failure as non-fatal; it logs and continues.
 *
 * SCOPE MINIMIZATION (D2): calendar.events (insert/list/watch/get/delete — C8 + B5 + listener)
 * + calendar.freebusy (availability.js freeBusy.query). NOT the full auth/calendar scope. This
 * intentionally diverges from the legacy hand-provisioned test-coordinator token (full calendar);
 * the two narrow scopes cover every downstream scheduling call.
 */

const { OAuth2Client } = require('google-auth-library');

const SCOPES = [
  'https://www.googleapis.com/auth/calendar.events',
  'https://www.googleapis.com/auth/calendar.freebusy',
];

// google-auth-library / gaxios has no easily-injectable global timeout across versions, so we bound
// each network call with a race (the index.js timeout intent, library-agnostic). On timeout the
// rejection classifies as transient (httpStatus null) → stale_connected / 502, never a false revoke.
const HTTP_TIMEOUT_MS = Number(process.env.OAUTH_HTTP_TIMEOUT_MS || 8000);

function withTimeout(promise, label, ms = HTTP_TIMEOUT_MS) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  // Promise.race attaches a handler to `promise`, so a later settle is not an unhandled rejection.
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function makeClient({ clientId, clientSecret, redirectUri }) {
  return new OAuth2Client({ clientId, clientSecret, redirectUri });
}

/**
 * Build the Google consent URL.
 * @param {object} args - { clientId, clientSecret, redirectUri, state }
 * @returns {string}
 */
function buildAuthUrl({ clientId, clientSecret, redirectUri, state }) {
  const client = makeClient({ clientId, clientSecret, redirectUri });
  return client.generateAuthUrl({
    access_type: 'offline', // request a refresh_token
    prompt: 'consent', // force the consent screen so a refresh_token is ALWAYS returned
    scope: SCOPES,
    state,
    include_granted_scopes: false, // do not silently widen scopes (minimization)
  });
}

/**
 * Exchange an authorization code for tokens.
 * @param {object} args - { clientId, clientSecret, redirectUri, code }
 * @returns {Promise<{ refresh_token: string|null, scope: string|null, token_type: string|null }>}
 */
async function exchangeCode({ clientId, clientSecret, redirectUri, code }) {
  const client = makeClient({ clientId, clientSecret, redirectUri });
  const { tokens } = await withTimeout(client.getToken(code), 'getToken');
  return {
    refresh_token: (tokens && tokens.refresh_token) || null,
    scope: (tokens && tokens.scope) || null,
    token_type: (tokens && tokens.token_type) || null,
  };
}

/**
 * Probe a stored refresh_token by minting a fresh access token. Resolves on success; THROWS the
 * underlying GaxiosError on failure (caller classifies via revocation.classifyTokenError).
 * @param {object} args - { clientId, clientSecret, refreshToken }
 */
async function probeRefresh({ clientId, clientSecret, refreshToken }) {
  const client = makeClient({ clientId, clientSecret });
  client.setCredentials({ refresh_token: refreshToken });
  // getAccessToken() refreshes when needed; an invalid_grant (revoked/expired) throws here.
  await withTimeout(client.getAccessToken(), 'getAccessToken');
}

// Google's token-revocation endpoint (RFC 7009 / Google identity docs).
// Accepts the refresh_token as a form-encoded `token` parameter.
const GOOGLE_REVOKE_URL = 'https://oauth2.googleapis.com/revoke';

/**
 * Revoke a Google OAuth refresh_token at Google's revocation endpoint (§E11b).
 *
 * Best-effort contract: the caller MUST treat a rejected promise as non-fatal.
 * Any network error, 4xx, or 5xx is surfaced as a thrown Error so the caller can
 * log it and continue — it MUST NOT block the disconnect.
 *
 * PII: the token itself is never logged; only the http_status on failure.
 *
 * @param {object} args - { refreshToken }
 * @returns {Promise<void>} resolves on HTTP 200; throws otherwise.
 */
async function revokeToken({ refreshToken }) {
  // Use the google-auth-library OAuth2Client to call the revocation endpoint.
  // The library posts `token=<value>` as application/x-www-form-urlencoded.
  const client = new OAuth2Client();
  await withTimeout(client.revokeToken(refreshToken), 'revokeToken');
}

module.exports = {
  SCOPES,
  buildAuthUrl,
  exchangeCode,
  probeRefresh,
  revokeToken,
  GOOGLE_REVOKE_URL,
};
