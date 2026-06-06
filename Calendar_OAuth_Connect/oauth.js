'use strict';

/**
 * oauth.js — thin wrapper over google-auth-library's OAuth2Client for the consent flow.
 *
 * Reuses the SAME library the shipped Calendar_Watch_Onboarder/oauth-client.js uses (which is
 * a token READER); this is the complementary token WRITER side. Three operations:
 *   • buildAuthUrl  — the Google consent redirect (access_type=offline + prompt=consent so a
 *                     refresh_token is always returned; scope-minimized per D2).
 *   • exchangeCode  — authorization_code → tokens (must include a refresh_token).
 *   • probeRefresh  — refresh_token → access_token, used by /connection/status to detect
 *                     revocation (an invalid_grant throw classifies as permanently disconnected).
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
  const { tokens } = await client.getToken(code);
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
  await client.getAccessToken();
}

module.exports = {
  SCOPES,
  buildAuthUrl,
  exchangeCode,
  probeRefresh,
};
