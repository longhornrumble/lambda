'use strict';

/**
 * secrets.js — Secrets Manager I/O for the consent flow.
 *
 *   • readPlatformApp   — the ONE platform-owned Google OAuth app (D2): { client_id,
 *                         client_secret, redirect_uri } at OAUTH_PLATFORM_SECRET_NAME.
 *   • writeCoordinator  — write the per-coordinator secret after a successful code exchange.
 *   • markDisconnected  — stamp the per-coordinator secret revoked on confirmed revocation.
 *
 * SECRET SHAPE (integrator-ratified 2026-06-05 — back-compat, NOT D2's consolidated shape):
 * the per-coordinator secret is written in the EXISTING shipped shape that oauth-client.js
 * (×6) + shared/scheduling/availability.js already READ — i.e. it INCLUDES client_id +
 * client_secret (copied from the platform app) — PLUS the D2 additive fields (calendar_id,
 * connected_at, status). This keeps every shipped reader working with zero change. D2's
 * "strip client creds out of the per-coordinator secret" is a deferred migration that would
 * touch all 6 readers — out of WS-E scope.
 *
 * PATH + SLOT-POISONING GUARD: the path mirrors oauth-client.js buildSecretPath
 * (picasso/scheduling/oauth/{tenantId}/{coordinatorId}). tenantId/coordinatorId are validated
 * with the SAME allowlist regexes the Calendar_Watch_Onboarder uses — a `/` would silently
 * retarget the secret path. (Identity itself comes from the signed init/state token, never the
 * query string; this is defense-in-depth.)
 */

const {
  SecretsManagerClient,
  GetSecretValueCommand,
  CreateSecretCommand,
  PutSecretValueCommand,
  DescribeSecretCommand,
} = require('@aws-sdk/client-secrets-manager');

const OAUTH_SECRET_PATH_PREFIX = process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';
const OAUTH_PLATFORM_SECRET_NAME =
  process.env.OAUTH_PLATFORM_SECRET_NAME || 'picasso/scheduling/oauth/_platform/google-app';
const GOOGLE_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';

// Mirror Calendar_Watch_Onboarder/index.js allowlists (path-traversal / schema guard).
const TENANT_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const COORDINATOR_ID_RE = /^[A-Za-z0-9._@+-]{1,128}$/;

const sm = new SecretsManagerClient({});

function buildSecretPath(tenantId, coordinatorId) {
  if (!TENANT_ID_RE.test(tenantId || '')) throw new Error('invalid tenantId');
  if (!COORDINATOR_ID_RE.test(coordinatorId || '')) throw new Error('invalid coordinatorId');
  // Reserved-namespace guard: the platform app secret + state-signing key live at
  // picasso/scheduling/oauth/_platform/... and .../_state-signing-key. A leading underscore
  // is reserved so a per-coordinator write can NEVER clobber them, even if a token somehow
  // carried tenant='_platform'. (TENANT_ID_RE permits '_', so this is the explicit guard.)
  if (tenantId.startsWith('_')) throw new Error('reserved tenantId namespace');
  return `${OAUTH_SECRET_PATH_PREFIX}/${tenantId}/${coordinatorId}`;
}

/**
 * Read the platform OAuth app credentials. Throws (caller 500s) if absent/incomplete.
 * @param {object} [deps] - { client }
 * @returns {Promise<{ client_id, client_secret, redirect_uri: string|null }>}
 */
async function readPlatformApp(deps = {}) {
  const client = deps.client || sm;
  const res = await client.send(new GetSecretValueCommand({ SecretId: OAUTH_PLATFORM_SECRET_NAME }));
  if (!res || typeof res.SecretString !== 'string') {
    throw new Error('platform OAuth app secret has no SecretString');
  }
  const parsed = JSON.parse(res.SecretString);
  for (const required of ['client_id', 'client_secret']) {
    if (typeof parsed[required] !== 'string' || parsed[required].length === 0) {
      throw new Error(`platform OAuth app secret missing "${required}"`);
    }
  }
  return {
    client_id: parsed.client_id,
    client_secret: parsed.client_secret,
    redirect_uri: typeof parsed.redirect_uri === 'string' ? parsed.redirect_uri : null,
  };
}

/**
 * Write (create or update) the per-coordinator secret. Idempotent: DescribeSecret → exists?
 * PutSecretValue : CreateSecret. Returns the secret path (never the contents).
 * @param {object} args - { tenantId, coordinatorId, coordinatorEmail, refreshToken, clientId,
 *                          clientSecret, scopes, calendarId, nowIso, deps }
 */
async function writeCoordinator(args) {
  const {
    tenantId,
    coordinatorId,
    coordinatorEmail,
    refreshToken,
    clientId,
    clientSecret,
    scopes,
    calendarId = 'primary',
    nowIso,
    deps = {},
  } = args;
  const client = deps.client || sm;
  const secretPath = buildSecretPath(tenantId, coordinatorId);

  const payload = JSON.stringify({
    provider: 'google',
    // back-compat shipped shape (oauth-client.js + availability.js read these)
    client_id: clientId,
    client_secret: clientSecret,
    refresh_token: refreshToken,
    coordinator_email: coordinatorEmail,
    scopes,
    token_endpoint: GOOGLE_TOKEN_ENDPOINT,
    // D2 additive fields
    calendar_id: calendarId,
    connected_at: nowIso,
    status: 'connected',
  });

  let exists = false;
  try {
    await client.send(new DescribeSecretCommand({ SecretId: secretPath }));
    exists = true;
  } catch (err) {
    if (err && err.name !== 'ResourceNotFoundException') throw err;
  }

  if (exists) {
    await client.send(new PutSecretValueCommand({ SecretId: secretPath, SecretString: payload }));
  } else {
    try {
      await client.send(
        new CreateSecretCommand({
          Name: secretPath,
          Description:
            'Per-coordinator Google Calendar OAuth refresh token (WS-E-OAUTH consent flow). Written by Calendar_OAuth_Connect.',
          SecretString: payload,
        })
      );
    } catch (err) {
      // TOCTOU: a concurrent reconnect (double-click) can create the secret between our
      // DescribeSecret and CreateSecret. Treat the lost race as an update, not a 500.
      if (err && err.name === 'ResourceExistsException') {
        await client.send(new PutSecretValueCommand({ SecretId: secretPath, SecretString: payload }));
      } else {
        throw err;
      }
    }
  }
  return secretPath;
}

/**
 * Read + parse the per-coordinator secret. Returns null if absent (the natural "never
 * connected" signal). Throws only on unexpected errors / malformed JSON.
 * @param {object} args - { tenantId, coordinatorId, deps }
 * @returns {Promise<object|null>}
 */
async function readCoordinator({ tenantId, coordinatorId, deps = {} }) {
  const client = deps.client || sm;
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  try {
    const res = await client.send(new GetSecretValueCommand({ SecretId: secretPath }));
    if (!res || typeof res.SecretString !== 'string') return null;
    return JSON.parse(res.SecretString);
  } catch (err) {
    if (err && err.name === 'ResourceNotFoundException') return null;
    throw err;
  }
}

/**
 * Stamp the per-coordinator secret as revoked (confirmed-revocation path). Preserves the other
 * fields for audit; flips status + adds disconnected_at. No-op-safe if the secret is absent
 * (returns { found:false }). Does NOT delete (reversible + auditable; the §9.4 recovery window).
 * @param {object} args - { tenantId, coordinatorId, nowIso, deps }
 */
async function markDisconnected({ tenantId, coordinatorId, nowIso, deps = {} }) {
  const client = deps.client || sm;
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  let current;
  try {
    const res = await client.send(new GetSecretValueCommand({ SecretId: secretPath }));
    // Mirror readCoordinator's guard: a present-but-non-string SecretString is treated as absent
    // (nothing to stamp) rather than crashing JSON.parse(undefined).
    if (!res || typeof res.SecretString !== 'string') return { found: false, secretPath };
    current = JSON.parse(res.SecretString);
  } catch (err) {
    if (err && err.name === 'ResourceNotFoundException') return { found: false, secretPath };
    throw err;
  }
  const next = JSON.stringify({ ...current, status: 'revoked', disconnected_at: nowIso });
  await client.send(new PutSecretValueCommand({ SecretId: secretPath, SecretString: next }));
  return { found: true, secretPath };
}

module.exports = {
  readPlatformApp,
  writeCoordinator,
  readCoordinator,
  markDisconnected,
  buildSecretPath,
  TENANT_ID_RE,
  COORDINATOR_ID_RE,
  OAUTH_PLATFORM_SECRET_NAME,
  OAUTH_SECRET_PATH_PREFIX,
  GOOGLE_TOKEN_ENDPOINT,
};
