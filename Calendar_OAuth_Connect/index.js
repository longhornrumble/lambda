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
 *   POST /connection/disconnect   body { init:<initToken> }
 *     §E11b user-initiated disconnect. METHOD-ENFORCED (POST only -- body-carried token). Identity
 *     is claims-sourced only (token, not query/body). Steps (contract-ordered):
 *       1. state.verify(init, typ:'init')
 *       2. Best-effort Google revoke: oauth.revokeToken() -- failure logged WARN, never blocks.
 *       3. secrets.markDisconnected() -- the shipped stamp (status:'revoked' + disconnected_at).
 *       4. Best-effort async-invoke Calendar_Watch_Offboarder -- mirrors callback's Onboarder.
 *     Idempotent: already-revoked or missing secret → 200 { status:'disconnected', watch:'none' }.
 *     NO jti burn (replay = re-disconnect = idempotent + harmless; token is server-held only).
 *     Generic errors only -- never leaks the secret path, URL, or failure detail.
 *     Response: 200 { status:'disconnected', watch:'stopped'|'pending'|'none' }.
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
const { burnJti } = require('./jti');
// Backend scheduling feature gate (Flag A — fail-closed, like Forms). Flag B
// (calendar_integration_enabled, tenant-admin) is integrator glue, enforced upstream.
const { isSchedulingEnabledForTenant } = require('../shared/scheduling/featureGate');

// ─── config ───────────────────────────────────────────────────────────────────────
const ENV = process.env.ENVIRONMENT || 'staging';
const OAUTH_REDIRECT_URI = process.env.OAUTH_REDIRECT_URI || '';
const DASHBOARD_RETURN_URL = process.env.DASHBOARD_RETURN_URL || '';
const ONBOARDER_FUNCTION_NAME = process.env.ONBOARDER_FUNCTION_NAME || `Calendar_Watch_Onboarder-${ENV}`;
const OFFBOARDER_FUNCTION_NAME = process.env.OFFBOARDER_FUNCTION_NAME || `Calendar_Watch_Offboarder-${ENV}`;
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
    return page(403, 'Scheduling unavailable', '<p>Online scheduling isn&apos;t enabled for this organization.</p>');
  }

  let app;
  try {
    app = await secrets.readPlatformApp();
  } catch (err) {
    warn('connect_platform_app_unavailable', { tenant_id: claims.tenant_id, name: err && err.name });
    // readPlatformApp failed — do NOT burn the jti. A transient Secrets Manager failure must
    // not consume the coordinator's single-use token; they must be able to retry from the dashboard.
    return genericFailure(500);
  }

  // ── Single-use enforcement (Beta-blocker §E0 / DEPLOY_NOTES §5) ──────────────────────────────
  // Burn the jti AFTER readPlatformApp so a transient Secrets Manager failure does NOT consume
  // the token (user can retry). Burn happens BEFORE the Google redirect so a leaked token cannot
  // be replayed from this point forward.
  //
  // Residual window: if a failure occurs AFTER the burn (e.g. state signing fails, or Google
  // rejects the redirect), the token IS consumed and the user must re-initiate from the dashboard.
  // This is the accepted trade-off — the burn-before-redirect guarantee is more important than
  // recovery from the narrow post-burn failure window.
  //
  // Forward-compatible: tokens without a `jti` claim (minted before this deploy) are exempt —
  // they were valid under the old contract; burning them would break in-flight connects.
  // Fail-open on DDB error: connecting twice in an outage is lower-harm than blocking onboarding
  // for all coordinators (see jti.js for the full rationale).
  if (claims.jti) {
    const burnResult = await burnJti({
      tenantId: claims.tenant_id,
      jti: claims.jti,
      expSeconds: claims.exp,
    });
    if (!burnResult.burned) {
      // Already used — replay detected. Serve the generic failure page (no detail leak).
      // WARN level: replay attempts are a security signal, not a routine user error.
      warn('connect_jti_replayed', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id) });
      return genericFailure(400);
    }
    // burnResult.warn ('unavailable' or 'unconfigured') — already logged inside burnJti; continue (fail-open).
  } else {
    // Token predates jti minting — treat as exempt, log a warn for visibility.
    // Pre-jti tokens age out within the token TTL window; this warn naturally disappears after that.
    // Its presence LATER (long after the deploy) would indicate someone minting tokens without jti.
    warn('connect_init_token_no_jti', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id) });
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
    return page(200, 'Calendar not connected', '<p>You didn&apos;t finish connecting your calendar. You can return to the dashboard and try again anytime.</p>');
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
    return page(400, "Couldn't finish connecting", "<p>We didn't receive the access we need. Please return to the dashboard and connect again.</p>");
  }

  // Granted-scope validation (integrator directive #5 / Security N-1): the user can grant a SUBSET
  // of the requested scopes (deselect on the consent screen / a manipulated screen). Writing a
  // "connected" secret missing calendar.events or calendar.freebusy would silently fail every
  // downstream booking/availability call. Require BOTH before persisting; else fail loudly.
  const grantedScopes = tokens.scope ? tokens.scope.split(' ').filter(Boolean) : [];
  const missing = oauth.SCOPES.filter((s) => !grantedScopes.includes(s));
  if (missing.length > 0) {
    warn('callback_insufficient_scope', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id), granted_count: grantedScopes.length });
    return page(403, "Couldn't finish connecting", "<p>We need access to your calendar's events and free/busy times to schedule. Please return to the dashboard and connect again, accepting all the requested permissions.</p>");
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

// ─── fire Offboarder (best-effort async -- §E11b) ──────────────────────────────────────
// Async (InvocationType:'Event') fire-and-forget. The SDK returns 202 on successful dispatch
// and throws on any SDK/network error -- it never returns FunctionError on an Event invocation
// (FunctionError only appears on RequestResponse). So: successful dispatch → ok:true (the
// disconnect route reports watch:'pending' = dispatched, not confirmed); SDK throw → ok:false
// (watch:'none' = offboard not dispatched; watch channel cleaned on next expiry/sweep).
// A failure MUST NOT block or alter the disconnect response -- the stamp is already written.
async function fireOffboarder({ tenantId, coordinatorId }) {
  try {
    await lambda.send(
      new InvokeCommand({
        FunctionName: OFFBOARDER_FUNCTION_NAME,
        InvocationType: 'Event', // async -- 202 on dispatch; never blocks; never FunctionError
        Payload: Buffer.from(
          JSON.stringify({ tenant_id: tenantId, coordinator_id: coordinatorId })
        ),
      })
    );
    return { ok: true };
  } catch (err) {
    warn('offboarder_invoke_failed', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), name: err && err.name });
    return { ok: false };
  }
}

// ─── route: POST /connection/disconnect (§E11b) ───────────────────────────────────────
// Body-carried init token (never query-string -- see §E11b contract). Idempotent. Generic
// errors only -- no secret-path, URL, or failure-detail leak.
async function handleDisconnect(event) {
  // Parse JSON body -- tolerate null/empty/invalid (return 400 if init absent/invalid).
  let body;
  try {
    body = JSON.parse((event && event.body) || '{}');
  } catch (_) {
    body = {};
  }
  const initToken = body && body.init;

  // Step 1: verify the init token (claims-sourced identity -- never body fields).
  let claims;
  try {
    claims = await state.verify(initToken, { expectedType: 'init' });
  } catch (err) {
    log('disconnect_init_rejected', { code: err && err.code });
    return json(400, { error: 'invalid_request' });
  }

  // §E11b cross-purpose replay defense: reject tokens that lack purpose:'disconnect'.
  // ADA mints disconnect tokens with this claim. A leaked connect/status URL token (which
  // has no 'purpose' claim) is structurally invalid here -- prevents token cross-use.
  // /connect and /connection/status remain unchanged (tokens without 'purpose' still work there).
  if (claims.purpose !== 'disconnect') {
    log('disconnect_wrong_purpose', { tenant_id: claims.tenant_id, coordinator_id_hash: coordHash(claims.coordinator_id) });
    return json(400, { error: 'invalid_request' });
  }

  const { tenant_id: tenantId, coordinator_id: coordinatorId } = claims;

  // Step 2: best-effort Google revocation. Failure is WARN-logged, never blocks disconnect.
  let secret;
  try {
    secret = await secrets.readCoordinator({ tenantId, coordinatorId });
  } catch (readErr) {
    // Secrets Manager unavailable -- we can't read the refresh_token to revoke it. secret=null
    // falls through to the idempotent early-return below (watch:'none', no markDisconnected call).
    // This is correct: an SM outage produces an idempotent 200 {watch:'none'} with NO stamp --
    // we cannot confirm the current state, so we do NOT write a stamp that might be wrong.
    warn('disconnect_secret_read_failed', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), name: readErr && readErr.name });
    secret = null;
  }

  // Idempotency: if already disconnected (no secret, no refresh_token, or status=revoked),
  // skip the Google call and return success immediately with watch:'none'.
  if (!secret || !secret.refresh_token || secret.status === 'revoked') {
    log('disconnect_already_disconnected', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId) });
    return json(200, { status: 'disconnected', watch: 'none' });
  }

  // Best-effort Google revoke.
  try {
    await oauth.revokeToken({ refreshToken: secret.refresh_token });
    log('disconnect_google_revoked', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId) });
  } catch (revokeErr) {
    // Non-blocking -- log and continue.
    warn('disconnect_google_revoke_failed', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), name: revokeErr && revokeErr.name });
  }

  // Step 3: stamp the secret (the authoritative disconnect signal).
  try {
    await secrets.markDisconnected({ tenantId, coordinatorId, nowIso: new Date().toISOString() });
  } catch (stampErr) {
    // Secrets Manager write failure -- the stamp didn't land; report a generic error but
    // do NOT leak the secret name or path.
    warn('disconnect_mark_failed', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), name: stampErr && stampErr.name });
    return json(500, { error: 'internal' });
  }

  // Step 4: best-effort async Offboarder invoke (mirrors callback's Onboarder).
  // InvocationType:'Event' -- dispatch is fire-and-forget. ok:true = async dispatch accepted
  // (watch:'pending' = cleanup dispatched, not confirmed); ok:false = SDK throw (watch:'none').
  // The §E11b enum is: stopped (watch stopped, confirmed) | pending (dispatched) | none (not dispatched).
  // With async invoke we can only confirm dispatch, not completion -- so 'pending' is correct.
  const offboard = await fireOffboarder({ tenantId, coordinatorId });
  log('disconnect_complete', { tenant_id: tenantId, coordinator_id_hash: coordHash(coordinatorId), offboard_ok: offboard.ok });

  return json(200, {
    status: 'disconnected',
    watch: offboard.ok ? 'pending' : 'none',
  });
}

// ─── handler ───────────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  const path = getPath(event);
  const method = ((event && event.requestContext && event.requestContext.http && event.requestContext.http.method) || 'GET').toUpperCase();

  // /connection/disconnect is POST-only (body-carried token). All other routes are GET.
  if (path === '/connection/disconnect') {
    if (method !== 'POST') {
      return { statusCode: 405, headers: { allow: 'POST', 'cache-control': 'no-store' }, body: '' };
    }
    try {
      return await handleDisconnect(event);
    } catch (err) {
      warn('unhandled_error', { path, name: err && err.name });
      return json(500, { error: 'internal' });
    }
  }

  // All other routes are GET.
  if (method !== 'GET') {
    return { statusCode: 405, headers: { allow: 'GET', 'cache-control': 'no-store' }, body: '' };
  }
  try {
    if (path === '/connect') return await handleConnect(event);
    if (path === '/oauth/callback') return await handleCallback(event);
    if (path === '/connection/status') return await handleStatus(event);
    log('unknown_path', { path });
    return page(404, 'Not found', "<p>This page doesn't exist.</p>");
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
  fireOffboarder,
  handleConnect,
  handleCallback,
  handleStatus,
  handleDisconnect,
  STATE_TTL_SECONDS,
  ONBOARDER_FUNCTION_NAME,
  OFFBOARDER_FUNCTION_NAME,
  burnJti,
};
