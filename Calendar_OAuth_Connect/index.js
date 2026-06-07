'use strict';

/**
 * Calendar_OAuth_Connect — scheduling sub-phase E Task E11 (WS-E-OAUTH backend).
 *
 * The per-coordinator Google Calendar 3-legged OAuth consent flow, served behind the WS-D3
 * `staging.schedule.myrecruiter.ai` CloudFront distribution (Lambda Function URL origin).
 * D2 (SCHEDULING_UX_DECISIONS) is the design of record; integrator-ratified 2026-06-05.
 *
 * ROUTES (Function URL, payload format 2.0):
 *   GET /connect?init=<initToken>
 *     Verify an HMAC init-token minted by the Clerk-authed dashboard backend (INTEGRATOR
 *     GLUE — see "init-token contract" below) → Flag-A feature gate → build the Google consent
 *     URL (access_type=offline + prompt=consent so a refresh_token is always returned; scope-
 *     minimized to calendar.events + calendar.freebusy) with a signed `state` → 302 to Google.
 *
 *   GET /oauth/callback?code=&state=  (or ?error=access_denied)
 *     Verify `state` (OAuth CSRF) → exchange `code` for a refresh_token via the platform OAuth
 *     app → write the per-coordinator secret in the shipped oauth-client.js shape + D2 additive
 *     fields → fire the B5 Calendar_Watch_Onboarder (best-effort) → 302 back to the dashboard.
 *
 *   GET /connection/status?init=<initToken>
 *     Verify init-token → probe the stored refresh_token: success → connected; invalid_grant →
 *     stamp the secret revoked + report disconnected/bookable:false; 5xx/transient → stale_connected.
 *
 * SLOT-POISONING DEFENSE: tenant_id/coordinator_id/coordinator_email are read ONLY from the
 * signed init/state token, NEVER from the query string — so a caller cannot *forge* a consent
 * flow into another coordinator's secret slot. RESIDUAL (flagged §E0 / Beta-blocker): the init
 * token is not yet single-use, so an attacker who *intercepts* a victim's valid init token
 * within its short TTL could replay it. Mitigations in place: short TTL + no-referrer on the
 * redirect (so the token isn't leaked in Referer). MUST add single-use before Beta — recommended
 * via a conditional PutItem of the token nonce to the existing `picasso-token-jti-blacklist`
 * table (the §B4 one-time-use pattern). Tracked in DEPLOY_NOTES §5.
 *
 * ── init-token contract (the integrator mints; flagged for FROZEN_CONTRACTS §E0) ──
 *   The dashboard's existing Clerk-authed backend (Analytics_Dashboard_API, which already
 *   verifies Clerk JWTs) mints, AFTER authenticating the staff member:
 *     state.sign({ typ:'init', claims:{ tenant_id, coordinator_id, coordinator_email },
 *                  ttlSeconds: ~300 })   // coordinator_* derived from the verified Clerk identity
 *   using the SHARED dedicated key at OAUTH_STATE_SIGNING_SECRET_NAME. The dashboard then
 *   navigates the browser to `${THIS_FUNCTION_URL}/connect?init=<token>`. This Lambda only
 *   VERIFIES the token; it never authenticates Clerk itself (a top-level redirect can't carry a
 *   bearer header, and D3 ≠ the dashboard origin).
 *
 * ── revocation → routing (ratified; flagged) ──
 *   On confirmed revocation this stamps the secret status:'revoked'. Excluding a revoked
 *   coordinator from the candidate pool is a SEPARATE integrator change to the frozen §B7
 *   candidate-resolver (it does not check connection today) — out of this file-disjoint slice.
 *
 * PII hygiene (§5.7): the init/state token, authorization code, refresh_token, and
 * coordinator_email are NEVER logged. We log path, outcome, tenant_id, and a coordinator_id
 * hash prefix (coordinator_id may be an email — mirrors the Onboarder's Y3 discipline).
 */

const crypto = require('crypto');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const state = require('./state');
const oauth = require('./oauth');
const secrets = require('./secrets');
const { classifyTokenError } = require('./revocation');
// Backend scheduling feature gate (Flag A — fail-closed, like Forms). Flag B
// (calendar_integration_enabled, tenant-admin) is integrator glue, enforced upstream.
const { isSchedulingEnabledForTenant } = require('../shared/scheduling/featureGate');

// ─── config ───────────────────────────────────────────────────────────────────────
const ENV = process.env.ENVIRONMENT || 'staging';
const OAUTH_REDIRECT_URI = process.env.OAUTH_REDIRECT_URI || '';
const DASHBOARD_RETURN_URL = process.env.DASHBOARD_RETURN_URL || '';
const ONBOARDER_FUNCTION_NAME = process.env.ONBOARDER_FUNCTION_NAME || `Calendar_Watch_Onboarder-${ENV}`;
const STATE_TTL_SECONDS = (() => {
  const v = Number(process.env.STATE_TTL_SECONDS || 600);
  if (!Number.isFinite(v) || v <= 0) throw new Error(`Invalid STATE_TTL_SECONDS: ${process.env.STATE_TTL_SECONDS}`);
  return v;
})();

// Known Google OAuth error-vocabulary (callback ?error=) — anything else logs as 'unknown'.
const KNOWN_OAUTH_ERRORS = new Set([
  'access_denied',
  'interaction_required',
  'login_required',
  'consent_required',
  'invalid_scope',
  'server_error',
  'temporarily_unavailable',
]);

const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const MAX_ATTEMPTS = Number(process.env.AWS_MAX_ATTEMPTS || 2);
const lambda = new LambdaClient({
  maxAttempts: MAX_ATTEMPTS,
  requestHandler: new NodeHttpHandler({
    connectionTimeout: CONNECTION_TIMEOUT_MS,
    requestTimeout: REQUEST_TIMEOUT_MS,
  }),
});

// ─── logging (no PII) ───────────────────────────────────────────────────────────────
function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}
function coordHash(coordinatorId) {
  return crypto.createHash('sha256').update(String(coordinatorId), 'utf8').digest('hex').slice(0, 12);
}

// ─── request parsing (Function URL payload 2.0) ──────────────────────────────────────
function getPath(event) {
  const raw =
    (event && event.rawPath) ||
    (event && event.requestContext && event.requestContext.http && event.requestContext.http.path) ||
    '/';
  if (raw.length > 1 && raw.endsWith('/')) return raw.slice(0, -1);
  return raw;
}
function getQuery(event) {
  return (event && event.queryStringParameters) || {};
}

// ─── responses ───────────────────────────────────────────────────────────────────────
function htmlEscape(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
function page(statusCode, title, body) {
  return {
    statusCode,
    headers: {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'",
      'referrer-policy': 'no-referrer',
    },
    body:
      '<!doctype html><html lang="en"><head><meta charset="utf-8">' +
      '<meta name="viewport" content="width=device-width,initial-scale=1">' +
      '<meta name="robots" content="noindex">' +
      `<title>${htmlEscape(title)}</title>` +
      '<style>body{font:16px/1.5 system-ui,sans-serif;max-width:32rem;margin:4rem auto;' +
      'padding:0 1.25rem;color:#1f2937}h1{font-size:1.25rem}p{color:#4b5563}</style>' +
      `</head><body><h1>${htmlEscape(title)}</h1>${body}</body></html>`,
  };
}
function json(statusCode, obj) {
  return {
    statusCode,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
    body: JSON.stringify(obj),
  };
}
function redirect(location) {
  // no-referrer so a query-string token on this URL is not leaked to the redirect target.
  return { statusCode: 302, headers: { location, 'cache-control': 'no-store', 'referrer-policy': 'no-referrer' }, body: '' };
}
// Generic, low-information failure page — never leaks why (tampered vs expired look identical).
function genericFailure(status) {
  return page(
    status,
    "We couldn't complete that",
    '<p>This link is no longer valid, or something went wrong. Please return to the dashboard and try connecting again.</p>'
  );
}

// ─── fire B5 (best-effort) ────────────────────────────────────────────────────────────
// The watch channel is re-creatable; a B5 failure must NOT roll back a successful connection
// (the secret is already written = connected). Surface the watch state, don't fail the connect.
async function fireOnboarder({ tenantId, coordinatorId, calendarId }) {
  try {
    const res = await lambda.send(
      new InvokeCommand({
        FunctionName: ONBOARDER_FUNCTION_NAME,
        InvocationType: 'RequestResponse',
        Payload: Buffer.from(
          JSON.stringify({ tenant_id: tenantId, coordinator_id: coordinatorId, calendar_id: calendarId })
        ),
      })
    );
    if (res.FunctionError) {
      warn('onboarder_function_error', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), function_error: res.FunctionError });
      return { ok: false };
    }
    return { ok: true };
  } catch (err) {
    warn('onboarder_invoke_failed', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), name: err && err.name });
    return { ok: false };
  }
}

// ─── route: GET /connect ──────────────────────────────────────────────────────────────
async function handleConnect(event) {
  const initToken = getQuery(event).init;
  let claims;
  try {
    claims = await state.verify(initToken, { expectedType: 'init' });
  } catch (err) {
    log('connect_init_rejected', { code: err && err.code });
    return genericFailure(400);
  }

  // Flag A gate (fail-closed). A disabled/unreadable tenant cannot start a consent flow.
  const enabled = await isSchedulingEnabledForTenant(claims.tenant_id);
  if (!enabled) {
    log('connect_scheduling_disabled', { tenant_id: claims.tenant_id });
    return page(403, 'Scheduling unavailable', '<p>Online scheduling isn’t enabled for this organization.</p>');
  }

  let app;
  try {
    app = await secrets.readPlatformApp();
  } catch (err) {
    warn('connect_platform_app_unavailable', { tenant_id: claims.tenant_id, name: err && err.name });
    return genericFailure(500);
  }
  const redirectUri = app.redirect_uri || OAUTH_REDIRECT_URI;
  if (!redirectUri || !redirectUri.startsWith('https://')) {
    warn('connect_redirect_uri_unconfigured', { tenant_id: claims.tenant_id });
    return genericFailure(500);
  }

  let signedState;
  try {
    signedState = await state.sign({
      typ: 'state',
      claims: {
        tenant_id: claims.tenant_id,
        coordinator_id: claims.coordinator_id,
        coordinator_email: claims.coordinator_email,
      },
      ttlSeconds: STATE_TTL_SECONDS,
    });
  } catch (err) {
    warn('connect_state_sign_failed', { tenant_id: claims.tenant_id, name: err && err.name });
    return genericFailure(500);
  }

  const authUrl = oauth.buildAuthUrl({
    clientId: app.client_id,
    clientSecret: app.client_secret,
    redirectUri,
    state: signedState,
  });
  // Defense-in-depth: only ever 302 the browser to Google's consent host. Guards against a
  // tampered platform-app secret / future library change steering the redirect elsewhere.
  if (!authUrl.startsWith('https://accounts.google.com/')) {
    warn('connect_unexpected_auth_url', { tenant_id: claims.tenant_id });
    return genericFailure(500);
  }
  log('connect_redirecting', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id) });
  return redirect(authUrl);
}

// ─── route: GET /oauth/callback ───────────────────────────────────────────────────────
async function handleCallback(event) {
  const q = getQuery(event);

  // `state` is REQUIRED and verified FIRST — on the success AND the error path. Google echoes
  // `state` on error redirects (RFC 6749 §4.1.2.1), so a genuine decline still carries a valid
  // signed state; an anonymous caller hitting /oauth/callback?error=… with no/forged state gets
  // a 400, not a free 200 page (integrator directive #2).
  if (!q.state) {
    log('callback_missing_params', {});
    return genericFailure(400);
  }
  let claims;
  try {
    claims = await state.verify(q.state, { expectedType: 'state' });
  } catch (err) {
    log('callback_state_rejected', { code: err && err.code });
    return genericFailure(400);
  }

  // User declined consent (or Google returned an error) → friendly "not connected", no write.
  if (q.error) {
    const known = KNOWN_OAUTH_ERRORS.has(String(q.error)) ? String(q.error) : 'unknown';
    log('callback_user_declined', { tenant_id: claims.tenant_id, error: known });
    return page(200, 'Calendar not connected', '<p>You didn’t finish connecting your calendar. You can return to the dashboard and try again anytime.</p>');
  }
  if (!q.code) {
    log('callback_missing_code', { tenant_id: claims.tenant_id });
    return genericFailure(400);
  }

  let app;
  try {
    app = await secrets.readPlatformApp();
  } catch (err) {
    warn('callback_platform_app_unavailable', { tenant_id: claims.tenant_id, name: err && err.name });
    return genericFailure(500);
  }
  const redirectUri = app.redirect_uri || OAUTH_REDIRECT_URI;
  if (!redirectUri || !redirectUri.startsWith('https://')) {
    // Symmetry with /connect: a missing/blank redirect_uri is a misconfiguration → an obvious
    // 500, not a confusing 502 from Google rejecting an empty redirect.
    warn('callback_redirect_uri_unconfigured', { tenant_id: claims.tenant_id });
    return genericFailure(500);
  }

  let tokens;
  try {
    tokens = await oauth.exchangeCode({
      clientId: app.client_id,
      clientSecret: app.client_secret,
      redirectUri,
      code: q.code,
    });
  } catch (err) {
    warn('callback_code_exchange_failed', { tenant_id: claims.tenant_id, name: err && err.name });
    return genericFailure(502);
  }
  if (!tokens.refresh_token) {
    // prompt=consent should guarantee one; if missing, do NOT write a useless secret.
    warn('callback_no_refresh_token', { tenant_id: claims.tenant_id });
    return page(400, 'Couldn’t finish connecting', '<p>We didn’t receive the access we need. Please return to the dashboard and connect again.</p>');
  }

  // Granted-scope validation (integrator directive #5 / Security N-1): the user can grant a SUBSET
  // of the requested scopes (deselect on the consent screen / a manipulated screen). Writing a
  // "connected" secret missing calendar.events or calendar.freebusy would silently fail every
  // downstream booking/availability call. Require BOTH before persisting; else fail loudly.
  const grantedScopes = tokens.scope ? tokens.scope.split(' ').filter(Boolean) : [];
  const missing = oauth.SCOPES.filter((s) => !grantedScopes.includes(s));
  if (missing.length > 0) {
    warn('callback_insufficient_scope', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id), granted_count: grantedScopes.length });
    return page(403, 'Couldn’t finish connecting', '<p>We need access to your calendar’s events and free/busy times to schedule. Please return to the dashboard and connect again, accepting all the requested permissions.</p>');
  }

  const nowIso = new Date().toISOString();
  try {
    await secrets.writeCoordinator({
      tenantId: claims.tenant_id,
      coordinatorId: claims.coordinator_id,
      coordinatorEmail: claims.coordinator_email,
      refreshToken: tokens.refresh_token,
      clientId: app.client_id,
      clientSecret: app.client_secret,
      scopes: grantedScopes,
      calendarId: 'primary',
      nowIso,
    });
  } catch (err) {
    warn('callback_secret_write_failed', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id), name: err && err.name });
    return genericFailure(500);
  }

  const watch = await fireOnboarder({
    tenantId: claims.tenant_id,
    coordinatorId: claims.coordinator_id,
    calendarId: 'primary',
  });
  log('callback_connected', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id), watch_ok: watch.ok });

  if (!DASHBOARD_RETURN_URL || !DASHBOARD_RETURN_URL.startsWith('https://')) {
    // Connected, but no configured place to send the user. Render a success page instead of a bad redirect.
    return page(200, 'Calendar connected', '<p>Your Google Calendar is connected. You can close this tab and return to the dashboard.</p>');
  }
  const sep = DASHBOARD_RETURN_URL.includes('?') ? '&' : '?';
  return redirect(`${DASHBOARD_RETURN_URL}${sep}calendar=connected&watch=${watch.ok ? 'ok' : 'pending'}`);
}

// ─── route: GET /connection/status ────────────────────────────────────────────────────
async function handleStatus(event) {
  const initToken = getQuery(event).init;
  let claims;
  try {
    claims = await state.verify(initToken, { expectedType: 'init' });
  } catch (err) {
    log('status_init_rejected', { code: err && err.code });
    return json(400, { error: 'invalid_request' });
  }

  let secret;
  try {
    secret = await secrets.readCoordinator({ tenantId: claims.tenant_id, coordinatorId: claims.coordinator_id });
  } catch (err) {
    warn('status_secret_read_failed', { tenant_id: claims.tenant_id, name: err && err.name });
    return json(500, { error: 'internal' });
  }
  if (!secret || !secret.refresh_token) {
    return json(200, { status: 'disconnected', bookable: false });
  }
  if (secret.status === 'revoked') {
    return json(200, { status: 'disconnected', bookable: false, reason: 'revoked' });
  }

  try {
    await oauth.probeRefresh({
      clientId: secret.client_id,
      clientSecret: secret.client_secret,
      refreshToken: secret.refresh_token,
    });
    log('status_connected', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id) });
    // calendar_id (G3/E16): the ACTUALLY-connected calendar's id (D2 field, written at connect).
    // The E16 embed renders this calendar — it may differ from the login email (calendar_email_
    // override / a different connected Google account), so the UI must read it from here, not
    // assume coordinator_id. Falls back to coordinator_id (= v1 calendar id) for older secrets
    // written before the D2 field landed (schema-discipline).
    return json(200, {
      status: 'connected',
      scopes: secret.scopes || null,
      calendar_id: secret.calendar_id || claims.coordinator_id || null,
    });
  } catch (err) {
    const { permanent, platform, httpStatus } = classifyTokenError(err);
    if (platform) {
      // Platform-app credential failure (invalid_client) — NOT this coordinator's fault. Do NOT
      // stamp the secret (that would mass-revoke everyone). Loud operator-alarm log; report stale.
      warn('status_platform_credential_error', { tenant_id: claims.tenant_id, http_status: httpStatus });
      return json(200, { status: 'stale_connected' });
    }
    if (permanent) {
      // Confirmed per-coordinator revocation → stamp the secret + report disconnected (bookable:false).
      try {
        await secrets.markDisconnected({ tenantId: claims.tenant_id, coordinatorId: claims.coordinator_id, nowIso: new Date().toISOString() });
      } catch (markErr) {
        warn('status_mark_disconnected_failed', { tenant_id: claims.tenant_id, name: markErr && markErr.name });
      }
      warn('status_revoked', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id), http_status: httpStatus });
      return json(200, { status: 'disconnected', bookable: false, reason: 'revoked' });
    }
    // Transient / 5xx / network — NOT a confirmed revocation. Leave the secret; report stale.
    warn('status_stale', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id), http_status: httpStatus });
    return json(200, { status: 'stale_connected' });
  }
}

// ─── handler ───────────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  const path = getPath(event);
  // All three routes are GET (browser navigation + a status fetch). Reject other methods.
  const method = ((event && event.requestContext && event.requestContext.http && event.requestContext.http.method) || 'GET').toUpperCase();
  if (method !== 'GET') {
    return { statusCode: 405, headers: { allow: 'GET', 'cache-control': 'no-store' }, body: '' };
  }
  try {
    if (path === '/connect') return await handleConnect(event);
    if (path === '/oauth/callback') return await handleCallback(event);
    if (path === '/connection/status') return await handleStatus(event);
    log('unknown_path', { path });
    return page(404, 'Not found', '<p>This page doesn’t exist.</p>');
  } catch (err) {
    // Last-resort guard — never leak a stack/detail to the browser.
    warn('unhandled_error', { path, name: err && err.name });
    return genericFailure(500);
  }
};

// Exported for tests / integrator wiring.
exports._internal = {
  getPath,
  getQuery,
  page,
  json,
  redirect,
  genericFailure,
  coordHash,
  fireOnboarder,
  handleConnect,
  handleCallback,
  handleStatus,
  STATE_TTL_SECONDS,
  ONBOARDER_FUNCTION_NAME,
};
