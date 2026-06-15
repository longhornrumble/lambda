'use strict';

/**
 * Scheduling_Redemption_Handler — WS-D4 (impl plan D4; canonical §13.7/§13.8/§13.9, §9.4).
 *
 * The redemption endpoint behind the WS-D3 `schedule.myrecruiter.ai` CloudFront
 * distribution (Function URL origin). Serves the six per-purpose one-tap action paths
 * (§13.8). For every request it:
 *   1. Maps the URL slug → expected token `purpose` (§13.8 LOCKED table). No match → 404.
 *   2. Validates + ATOMICALLY ONE-TIME-REDEEMS the token via the shipped
 *      shared/scheduling/tokens.js `redeem()` (§13.7 conditional PutItem to the jti
 *      blacklist; HS256 + iss + purpose + expiry checks inside verify()).
 *   3. On success: for volunteer purposes fetches the Booking, writes the §B10
 *      session-context binding row, and redirects into chat (token authenticates
 *      ENTRY only — the calendar op runs in-chat after confirm, WS-D6/D7).
 *      For interviewer-attendance purposes the security path is real but the
 *      disposition is TODO(E6) — a thin "got it" page; the booking is NOT transitioned.
 *
 * ── Token API note (flagged for integrator) ──
 *   The work-order describes `verify(token, {expectedPurpose, tenantId, ddb})` as doing
 *   the one-time PutItem. The SHIPPED tokens.js splits that: `verify()` is stateless and
 *   `redeem()` = verify() + the §13.7 conditional PutItem. This handler calls `redeem()`
 *   (the one-time path) with `{ expectedPurpose }`. ddb is module-internal to tokens.js;
 *   we never touch the jti table directly (work-order OUT OF SCOPE).
 *
 * ── HTTP mapping note (flagged) ──
 *   The work-order pins the endpoint's HTTP contract (§13.9): bad-sig/expired/wrong-iss →
 *   401, purpose↔URL mismatch → 403, already-redeemed → 410 Gone, tampered/garbage → 400.
 *   tokens.js's internal `TokenError.status` groups expired WITH reused (both 410). This
 *   handler therefore maps by `TokenError.code` via CODE_TO_STATUS below — it does NOT use
 *   `err.status` — so the endpoint honors the work-order contract (notably expired → 401).
 *
 * ── §B10 binding-row notes (flagged) ──
 *   • The session table's real key schema is (tenantId PK · session_id SK). §B10 labels
 *     the SK `session_binding_id` with value `binding#<session_id>`; that attribute NAME
 *     is unimplementable (DynamoDB rejects a PutItem whose key attr ≠ schema). We write the
 *     namespaced value `binding#<session_id>` under the REAL `session_id` key — preserving
 *     §B10's namespacing intent while writing a valid row. (Same implement-to-ground-truth +
 *     flag pattern tokens.js used for the composite jti key.) Suggest tightening §B10 prose.
 *   • The table has NO DynamoDB TTL in v1 (module comment: server-side retention = sub-phase
 *     F). So the binding does NOT auto-clean server-side; enforcement is the chat session
 *     comparing `expires_at` to now (mirrors the §9.5 FormModeContext mount-time pattern).
 *     We still write `ttl` (epoch SECONDS — correct DDB-TTL units) for forward-compat; §B10
 *     said "ttl = expires_at" but expires_at is epoch MS, so ttl = floor(expires_at/1000).
 *
 * ── PII hygiene (§5.7) ──
 *   The token, attendee/coordinator emails, and form data are never logged. We log only
 *   path, purpose, outcome, status, and opaque references (tenant_id, booking_id).
 *
 * Env (deploy note — IaC is the integrator's):
 *   ENVIRONMENT, BOOKING_TABLE, CONVERSATION_SCHEDULING_SESSION_TABLE, TENANT_REGISTRY_TABLE,
 *   CHAT_REDIRECT_BASE_URL, SESSION_BINDING_TTL_SECONDS, JTI_BLACKLIST_TABLE (tokens.js),
 *   JWT_SECRET_KEY_NAME (tokens.js; = picasso/staging/jwt/signing-key, the #343 fix),
 *   AWS_REQUEST_TIMEOUT_MS / AWS_CONNECTION_TIMEOUT_MS / AWS_MAX_ATTEMPTS.
 */

const crypto = require('crypto');
const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

// SoT token validator + one-time-use (§B4, lambda#186/#192). Never re-implement.
const { redeem, TokenError } = require('../shared/scheduling/tokens.js');
// Backend scheduling feature gate (fail-closed). Scheduling is OFF unless the tenant
// config sets feature_flags.scheduling_enabled (like Forms).
const { isSchedulingEnabledForTenant } = require('../shared/scheduling/featureGate');
// WS-E-ATTEND E6 — interviewer disposition (wires this handler's stubbed /attended/* action
// to a real Booking.status transition per §11.2). Self-contained (its own DDB + token-sign +
// notify defaults), so this stays a one-line call.
const { applyDisposition } = require('../shared/scheduling/disposition');

// ─── config ───────────────────────────────────────────────────────────────────────

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
const SESSION_TABLE =
  process.env.CONVERSATION_SCHEDULING_SESSION_TABLE ||
  `picasso-conversation-scheduling-session-${ENV}`;
// Tenant registry — reverse-maps tenant_id → public tenant HASH for the scheduling-page
// redirect (the page is identified publicly by the hash, never the raw tenant_id). Same
// table shape Master_Function / bedrock-core use (PK tenantId, attr tenantHash).
const TENANT_REGISTRY_TABLE =
  process.env.TENANT_REGISTRY_TABLE || `picasso-tenant-registry-${ENV}`;
// Integration seam: the chat-widget bootstrap target. Default staging per work-order.
const CHAT_REDIRECT_BASE_URL =
  process.env.CHAT_REDIRECT_BASE_URL || 'https://staging.chat.myrecruiter.ai';
// §9.4 — reschedule/cancel binding lives 30 minutes. Validate at module load (SR-2): a
// blank env var falls back to 1800 (ok), but a non-numeric value → NaN (DDB rejects the N
// write → 500) and an explicit '0'/'-5' → already-expired/negative bindings. Fail fast.
const BINDING_TTL_SECONDS = (() => {
  const v = Number(process.env.SESSION_BINDING_TTL_SECONDS || 1800);
  if (!Number.isFinite(v) || v <= 0) {
    throw new Error(
      `Invalid SESSION_BINDING_TTL_SECONDS: ${process.env.SESSION_BINDING_TTL_SECONDS}`
    );
  }
  return v;
})();

// Bounded SDK client (#202: @smithy/node-http-handler must BUNDLE, not externalize).
const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const MAX_ATTEMPTS = Number(process.env.AWS_MAX_ATTEMPTS || 2);
const ddb = new DynamoDBClient({
  maxAttempts: MAX_ATTEMPTS,
  requestHandler: new NodeHttpHandler({
    connectionTimeout: CONNECTION_TIMEOUT_MS,
    requestTimeout: REQUEST_TIMEOUT_MS,
  }),
});

// §13.8 URL slug → expected purpose (LOCKED — verbatim).
const SLUG_TO_PURPOSE = {
  '/cancel': 'cancel',
  '/reschedule': 'reschedule',
  '/resume': 'post_application_recovery',
  '/attended/met': 'attended_yes',
  '/attended/noshow': 'no_show',
  '/attended/noconnect': 'didnt_connect',
};

// §B10 purpose → intent for the volunteer-facing binding row.
const PURPOSE_TO_INTENT = {
  cancel: 'cancellation_intent',
  reschedule: 'rescheduling_intent',
  post_application_recovery: 'recovery_intent',
};
const VOLUNTEER_PURPOSES = new Set([
  'cancel',
  'reschedule',
  'post_application_recovery',
]);
const ATTENDANCE_PURPOSES = new Set(['attended_yes', 'no_show', 'didnt_connect']);

// §13.9 endpoint HTTP contract: map TokenError.code → status (NOT err.status — see header).
const CODE_TO_STATUS = {
  malformed: 400, // tampered / garbage — generic, no detail leak
  unknown_purpose: 400,
  invalid_signature: 401,
  invalid_issuer: 401, // cross-class chat-session JWT
  expired: 401,
  purpose_mismatch: 403, // valid token, wrong slug
  tenant_mismatch: 403,
  reused: 410, // replay — already redeemed (ConditionalCheckFailed)
  signing_key_unavailable: 500,
};

// ─── structured logging (no PII) ────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── request parsing (Lambda Function URL, payload format 2.0) ──────────────────────

function getPath(event) {
  const raw =
    (event && event.rawPath) ||
    (event &&
      event.requestContext &&
      event.requestContext.http &&
      event.requestContext.http.path) ||
    '/';
  // Normalize a trailing slash (but keep root). Path matching is case-sensitive per §13.8.
  if (raw.length > 1 && raw.endsWith('/')) return raw.slice(0, -1);
  return raw;
}

function getToken(event) {
  const qs = (event && event.queryStringParameters) || {};
  return typeof qs.t === 'string' ? qs.t : null;
}

// ─── responses ──────────────────────────────────────────────────────────────────────

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
      // Thin static page — no Picasso widget, no external scripts (§13.9).
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'",
      referrer: 'no-referrer',
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

function redirect(location) {
  return {
    statusCode: 302,
    headers: { location, 'cache-control': 'no-store' },
    body: '',
  };
}

// §13.9 friendly failure pages — thin, low-information; tampered → generic, no detail leak.
// `coordinator` is the D5 seam: render name/work-email only if already present, never phone.
function failurePage(status, { coordinator } = {}) {
  let title;
  let lead;
  switch (status) {
    case 401:
      title = 'This link has expired';
      lead = "This link is no longer valid. It may have expired, or it wasn't issued by us.";
      break;
    case 403:
      title = "This link doesn't match this action";
      lead = 'This link was issued for a different action.';
      break;
    case 410:
      title = 'This link was already used';
      lead = 'Looks like this one-time link has already been used.';
      break;
    case 500:
      title = 'Something went wrong';
      lead = "We couldn't process this link right now. Please try again in a moment.";
      break;
    case 400:
    default:
      status = status || 400;
      title = "This link isn't valid";
      lead = "We couldn't read this link.";
      break;
  }
  // TODO(D5): WS-D5 polishes coordinator-contact embedding (name + work email only, never
  // phone) from the validated booking lookup. The seam is wired here; D4 renders it only
  // when a coordinator object is already supplied (no live caller does so yet — expired/
  // tampered tokens yield no validated booking).
  let body = `<p>${htmlEscape(lead)}</p>`;
  if (coordinator && (coordinator.name || coordinator.email)) {
    const who = coordinator.name ? htmlEscape(coordinator.name) : 'your coordinator';
    body += `<p>You can reach out to ${who}`;
    if (coordinator.email) {
      body += ` at <a href="mailto:${htmlEscape(coordinator.email)}">${htmlEscape(
        coordinator.email
      )}</a>`;
    }
    body += '.</p>';
  }
  body += '<p>You can always return to the chat to check your current status.</p>';
  return page(status, title, body);
}

// §11.2 disposition result page (interviewer-facing). Friendly, low-information; never leaks
// volunteer PII (the no_show outreach detail is generic). already_resolved is the idempotent
// second-click / admin-after-interviewer ack.
function attendanceResultPage(purpose, disposition) {
  if (disposition && disposition.outcome === 'already_resolved') {
    return page(
      200,
      'Already recorded',
      '<p>Thanks — this appointment was already recorded. No further action is needed.</p>'
    );
  }
  const lead =
    purpose === 'no_show'
      ? "Thanks for letting us know — we've reached out to the volunteer about finding a new time."
      : 'Thanks for letting us know. No further action is needed.';
  return page(200, 'Thanks — got it', `<p>${lead}</p>`);
}

// ─── booking read (defensive — schema discipline) ───────────────────────────────────

async function getBooking(tenantId, bookingId) {
  const res = await ddb.send(
    new GetItemCommand({
      TableName: BOOKING_TABLE,
      Key: { tenantId: { S: tenantId }, booking_id: { S: bookingId } },
    })
  );
  if (!res || !res.Item) return null;
  const it = res.Item;
  return {
    tenantId: it.tenantId?.S ?? tenantId,
    booking_id: it.booking_id?.S ?? bookingId,
    coordinator_email: it.coordinator_email?.S ?? null,
    coordinator_name: it.coordinator_name?.S ?? null, // may be absent (§A non-key)
    start_at: it.start_at?.S ?? null,
    status: it.status?.S ?? null,
  };
}

// ─── tenant hash reverse-lookup (public-surface identifier) ─────────────────────────
// The branded scheduling page is identified publicly by the tenant HASH (?t=), never the
// raw tenant_id. Map tenant_id → tenantHash via a PK GetItem on the tenant registry.
// Returns null when the row or the attribute is absent (caller fails closed → 500).
async function getTenantHashForId(tenantId) {
  const res = await ddb.send(
    new GetItemCommand({
      TableName: TENANT_REGISTRY_TABLE,
      Key: { tenantId: { S: tenantId } },
    })
  );
  return (res && res.Item && res.Item.tenantHash && res.Item.tenantHash.S) || null;
}

// ─── §B10 session-context binding write ─────────────────────────────────────────────

async function writeBinding({ purpose, claims, sessionId, nowMs }) {
  const intent = PURPOSE_TO_INTENT[purpose];
  const isRecovery = purpose === 'post_application_recovery';

  // expires_at is epoch MS (app-level enforcement, per §B10 + stateMachine.js Date.now()
  // convention). recovery uses the token's own exp (tokens.js exp is epoch SECONDS → ×1000).
  const expiresAtMs = isRecovery
    ? claims.exp * 1000
    : nowMs + BINDING_TTL_SECONDS * 1000;
  // ttl is epoch SECONDS (the only unit a DynamoDB TTL accepts) — forward-compat; the
  // table has no TTL enabled today (see header). recovery exp is already seconds.
  const ttlSeconds = isRecovery ? claims.exp : Math.floor(expiresAtMs / 1000);

  const item = {
    tenantId: { S: claims.tenant_id },
    // §B10 SK: namespaced value under the REAL `session_id` key (see header).
    session_id: { S: `binding#${sessionId}` },
    intent: { S: intent },
    expires_at: { N: String(expiresAtMs) },
    created_at: { N: String(nowMs) },
    ttl: { N: String(ttlSeconds) },
  };
  // booking_id is the single booking this binding authorizes (§B10 — cross-booking reject).
  // Recovery may legitimately carry no booking_id (pre-booking resume — §13.3).
  if (claims.booking_id != null) item.booking_id = { S: String(claims.booking_id) };
  if (isRecovery && claims.form_submission_id != null) {
    item.form_submission_id = { S: String(claims.form_submission_id) };
  }

  await ddb.send(new PutItemCommand({ TableName: SESSION_TABLE, Item: item }));
}

// ─── handler ──────────────────────────────────────────────────────────────────────

exports.handler = async (event) => {
  const path = getPath(event);
  const purpose = SLUG_TO_PURPOSE[path];

  // Unknown path → 404 (no slug match, §13.8).
  if (!purpose) {
    log('redemption_unknown_path', { path });
    return page(404, 'Not found', '<p>This page doesn’t exist.</p>');
  }

  const token = getToken(event);
  if (!token) {
    // Missing token is indistinguishable from garbage → generic 400 (no detail leak).
    log('redemption_missing_token', { path, purpose });
    return failurePage(400);
  }

  // Validate + atomically one-time-redeem (§13.7). redeem() throws BEFORE the jti PutItem
  // on any invalid/expired/wrong-purpose token, so those never burn the jti; only a true
  // replay (ConditionalCheckFailed) yields `reused`.
  let claims;
  try {
    claims = await redeem(token, { expectedPurpose: purpose });
  } catch (err) {
    if (err instanceof TokenError && CODE_TO_STATUS[err.code]) {
      const status = CODE_TO_STATUS[err.code];
      log('redemption_rejected', { path, purpose, code: err.code, status });
      return failurePage(status);
    }
    // Unexpected (e.g. a raw DynamoDB error from the jti PutItem) → 500, no detail leak.
    warn('redemption_error', { path, purpose, name: err && err.name });
    return failurePage(500);
  }

  // ── Feature gate: scheduling is OFF unless the tenant config enables it (like Forms). ──
  // The token validated, but a tenant without feature_flags.scheduling_enabled must not be
  // able to drive any scheduling action. Fail-closed (a config we can't read → unavailable).
  // Checked AFTER redeem() so a disabled tenant's one-time token is still burned (the link
  // is single-use regardless), but BEFORE any binding write / attendance ack / booking read.
  const schedulingEnabled = await isSchedulingEnabledForTenant(claims.tenant_id);
  if (!schedulingEnabled) {
    log('redemption_scheduling_disabled', { purpose, tenant_id: claims.tenant_id });
    return page(
      403,
      'Scheduling unavailable',
      '<p>Online scheduling isn’t available for this organization right now. ' +
        'Please contact them directly to change your appointment.</p>'
    );
  }

  // ── Interviewer attendance disposition (E6 — WS-E-ATTEND wires the stubbed action). ──
  // attended_yes → completed · no_show → no_show (+ volunteer reoffer) · didnt_connect →
  // coordinator_no_show. The transition is a conditional booked→terminal write (idempotent:
  // a second click / admin-after-interviewer → already_resolved). NO auto-completion (§11.1).
  if (ATTENDANCE_PURPOSES.has(purpose)) {
    // An attendance token without a booking_id targets no booking → benign ack (the jti is
    // already burned by redeem(); nothing to transition).
    if (claims.booking_id == null) {
      log('redemption_attendance_no_booking', { purpose, tenant_id: claims.tenant_id });
      return page(
        200,
        'Thanks — got it',
        '<p>Thanks for letting us know. No further action is needed.</p>'
      );
    }
    let disposition;
    try {
      disposition = await applyDisposition({
        tenantId: claims.tenant_id,
        bookingId: String(claims.booking_id),
        purpose,
      });
    } catch (err) {
      // The jti is already burned (redeem succeeded); a re-click will 410. Rare write failure.
      warn('redemption_disposition_error', {
        purpose,
        tenant_id: claims.tenant_id,
        booking_id: claims.booking_id,
        name: err && err.name,
      });
      return failurePage(500);
    }
    log('redemption_attendance_dispositioned', {
      purpose,
      tenant_id: claims.tenant_id,
      booking_id: claims.booking_id,
      outcome: disposition.outcome,
    });
    return attendanceResultPage(purpose, disposition);
  }

  // ── Volunteer-facing: bind + redirect into chat (no calendar op here, §13.4). ──
  // C-3: cancel/reschedule MUST carry a booking_id (C8 always sets it); a token minted
  // without one targets no booking → a clear "invalid link" 400, not a misleading 404.
  // (post_application_recovery legitimately has none — it carries form_submission_id.)
  // The jti is already consumed by redeem() above; harmless for a booking_id-less token
  // (it's unusable — nothing to retry). Mint-time enforcement is a follow-up tokens.js sign-guard.
  if (purpose !== 'post_application_recovery' && claims.booking_id == null) {
    log('redemption_missing_booking_id', { path, purpose });
    return failurePage(400);
  }

  let booking = null;
  if (claims.booking_id != null) {
    try {
      booking = await getBooking(claims.tenant_id, String(claims.booking_id));
    } catch (err) {
      warn('redemption_booking_lookup_error', {
        purpose,
        tenant_id: claims.tenant_id,
        booking_id: claims.booking_id,
        name: err && err.name,
      });
      return failurePage(500);
    }
  }

  // cancel/reschedule require an existing booking; recovery may legitimately have none.
  if (purpose !== 'post_application_recovery' && !booking) {
    log('redemption_booking_not_found', {
      purpose,
      tenant_id: claims.tenant_id,
      booking_id: claims.booking_id,
    });
    return page(
      404,
      "We couldn't find that booking",
      '<p>This booking may have already been canceled or removed. ' +
        'Return to the chat to check your current status.</p>'
    );
  }

  const sessionId = crypto.randomUUID();
  const nowMs = Date.now();
  try {
    await writeBinding({ purpose, claims, sessionId, nowMs });
  } catch (err) {
    // jti is already burned (redeem succeeded); a re-click will 410. Rare DDB-write failure.
    warn('redemption_binding_write_error', {
      purpose,
      tenant_id: claims.tenant_id,
      booking_id: claims.booking_id,
      name: err && err.name,
    });
    return failurePage(500);
  }

  log('redemption_bound', {
    purpose,
    intent: PURPOSE_TO_INTENT[purpose],
    tenant_id: claims.tenant_id,
    booking_id: claims.booking_id,
  });

  // ── Redirect into the bound session (§13.4). ──
  // reschedule/cancel land on the branded hosted scheduling page (/schedule/), identified
  // publicly by the tenant HASH (?t=) — never the raw tenant_id — plus the §B10 binding
  // session id (?session=). The page resolves the hash → config/branding and replays the
  // binding through the streaming flow. post_application_recovery keeps the chat-widget
  // target for now (M2 — the recovery surface is a separate concern).
  if (purpose === 'reschedule' || purpose === 'cancel') {
    let tenantHash;
    try {
      tenantHash = await getTenantHashForId(claims.tenant_id);
    } catch (err) {
      // Binding is already written + jti burned (a re-click 410s). Rare registry-read fail.
      warn('redemption_tenant_hash_lookup_error', {
        purpose,
        tenant_id: claims.tenant_id,
        name: err && err.name,
      });
      return failurePage(500);
    }
    if (!tenantHash) {
      log('redemption_tenant_hash_not_found', { purpose, tenant_id: claims.tenant_id });
      return failurePage(500);
    }
    // Target the explicit index.html: the widget-bucket CloudFront serves S3 via a REST
    // origin with NO directory-index rewrite, so /schedule/ would 403 (verified: /go/ 403s,
    // /go/index.html 200s). /schedule/index.html is the servable key.
    const location =
      `${CHAT_REDIRECT_BASE_URL}/schedule/index.html?session=${encodeURIComponent(sessionId)}` +
      `&t=${encodeURIComponent(tenantHash)}` +
      `&purpose=${encodeURIComponent(purpose)}`;
    return redirect(location);
  }

  // post_application_recovery — unchanged chat-widget target (M2).
  const location = `${CHAT_REDIRECT_BASE_URL}/?session=${encodeURIComponent(sessionId)}`;
  return redirect(location);
};

// Exported for tests / integrator wiring.
exports._internal = {
  SLUG_TO_PURPOSE,
  PURPOSE_TO_INTENT,
  CODE_TO_STATUS,
  getPath,
  getToken,
  failurePage,
  page,
  BINDING_TTL_SECONDS,
};
