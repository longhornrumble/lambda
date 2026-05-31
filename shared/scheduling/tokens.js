'use strict';

/**
 * tokens.js — Unified signed-token middleware (WS-D1a).
 *
 * Canonical §13; frozen contract FROZEN_CONTRACTS.md §B4. The single source of
 * truth for scheduling one-tap action tokens — the mirrored signer + verifier
 * every D consumer (cancel / reschedule / attendance / disposition endpoint)
 * imports. This module owns the token format and NOTHING else (it does NOT
 * build the consumer endpoints or the schedule.myrecruiter.ai infra — §13.8,
 * work-order OUT OF SCOPE).
 *
 * Exports:
 *   - TOKEN_PURPOSES   the LOCKED 6-purpose enum (§13.4). Mirrored across
 *                      sign + verify so the issuer and verifier can never drift
 *                      (CI-3d contract test asserts this list verbatim).
 *   - sign(purpose, claims, opts)   mint an HS256 token with a per-purpose exp
 *                                   (§13.6) and a fresh jti (one-time-use key).
 *   - verify(token, opts)           stateless validation: signature (constant
 *                                   time) + alg + issuer + purpose + expiry.
 *                                   Returns the claims. NO DynamoDB — consumers
 *                                   doing entry-auth don't pay a DDB round-trip.
 *   - redeem(token, opts)           verify(), THEN one-time-use enforcement
 *                                   (§13.7): atomic conditional PutItem to the
 *                                   EXISTING jti-blacklist table. Second click
 *                                   → 410, the action does NOT execute.
 *   - TokenError                    typed error carrying { code, status } so the
 *                                   later-D endpoint handlers map to friendly
 *                                   failure pages (§13.9) without leaking detail.
 *
 * ── HMAC mechanism (§13.2) ──
 *   HS256 with the EXISTING `picasso/jwt/signing-key` (no new key infra). Same
 *   secret as chat-session JWTs; the `iss` claim is what distinguishes the two
 *   token classes — scheduling tokens are `myrecruiter-scheduling`, chat tokens
 *   are `myrecruiter-chat`. verify() REJECTS any token whose iss isn't ours, so
 *   a chat-session JWT can never be replayed against a scheduling endpoint. The
 *   key resolver mirrors Master_Function_Staging.get_jwt_signing_key() exactly:
 *   SecretString parsed as JSON → `.signingKey` field, with raw-string fallback.
 *   Implemented on Node's built-in `crypto` (no `jsonwebtoken` dep added — the
 *   format is a plain HS256 JWS; "concrete-first", canonical §4.3).
 *
 * ── Interpretations layered on the frozen §B4 contract (flagged in the PR for
 *    integrator confirmation; none redefine a frozen contract) ──
 *   • sign/verify/redeem take an optional 3rd `opts` arg used by tests +
 *     key-injection + the §13.8 context check:
 *       { signingKey, now, expectedPurpose, expectedTenantId }.
 *     Consumers still call sign(purpose, claims) / verify(token) exactly per §B4
 *     — the arg is additive, not a redefinition. The §13.8 endpoint handler
 *     passes expectedPurpose (the URL's purpose) so a valid token can't be
 *     replayed against the wrong action; expectedTenantId binds it to the
 *     tenant when known. Omitted ⇒ not checked (backward-compatible).
 *   • Fail-closed key handling: getSigningKey() throws (never caches) on a
 *     Secrets-Manager error OR an empty/too-short secret — we never HMAC with
 *     unusable key material. verify() also rejects a missing/non-string jti
 *     (it is redeem()'s table sort key — must never reach DDB blank).
 *   • `claims` to sign() carries the PERSISTED custom claims (booking_id,
 *     tenant_id, form_submission_id — §13.3) PLUS the expiry-driver inputs
 *     (start_at, cancellation_window_hours, event_end). The drivers are consumed
 *     to compute `exp` and are NOT written into the payload — §13.3's "no PII /
 *     references only" claim set is preserved verbatim.
 *   • redeem() writes to the SHIPPED jti table, whose key is COMPOSITE
 *     `(tenantId HASH, jti RANGE)` per infra/modules/ddb-token-jti-blacklist +
 *     the A6 runbook — NOT single-`jti`. §B4 / §13.7 say "keyed by jti"; the
 *     canonical §13.7 reconciliation note (design ~L1629) already flags that as
 *     a doc bug to align to the composite key. Built to ground-truth (token's
 *     tenant_id claim is the PK); `attribute_not_exists(jti)` is the §13.7
 *     condition and is correct for a composite-key PutItem. ⚑ Integrator may
 *     wish to tighten the §B4 wording — NOT forked here.
 */

const crypto = require('crypto');
const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

// ─── Frozen enum (§13.4 / §B4 — LOCKED 2026-05-30) ────────────────────────────────
// The SIX purposes, verbatim. Adding/removing one must be a deliberate edit that
// also updates the CI-3d frozen-list test (tokens.test.js) — that is the "red CI"
// guard the work-order requires.
const TOKEN_PURPOSES = [
  'cancel', 'reschedule', 'post_application_recovery', // volunteer-facing
  'attended_yes', 'no_show', 'didnt_connect', // interviewer-facing
];
const PURPOSE_SET = new Set(TOKEN_PURPOSES);

// §13.3 — issuer that distinguishes scheduling tokens from chat-session JWTs.
const ISSUER = 'myrecruiter-scheduling';
const ALG = 'HS256';

const DAY_SECONDS = 24 * 60 * 60;

// Min-lifetime floor (§B4): a freshly-minted action link is ALWAYS usable for at
// least this long, even when its natural deadline (cancel→start_at,
// reschedule→start_at−window) has already passed or is imminent. Without the
// floor a reschedule token for a soon/large-window booking can be signed already
// expired (start_at−window < iat) — a dead-on-arrival link in the user's email.
const MIN_TOKEN_LIFETIME_SECONDS = 15 * 60; // 900s

const ENV = process.env.ENVIRONMENT || 'staging';
const JTI_BLACKLIST_TABLE =
  process.env.JTI_BLACKLIST_TABLE || `picasso-token-jti-blacklist-${ENV}`;
const JWT_SECRET_KEY_NAME =
  process.env.JWT_SECRET_KEY_NAME || 'picasso/jwt/signing-key';

// Created once at module load; reused across warm invocations (Calendar_Watch_* style).
const ddb = new DynamoDBClient({});
const secrets = new SecretsManagerClient({});

// ─── Typed error (§13.9 — friendly failure pages, no detail leak) ──────────────────

class TokenError extends Error {
  constructor(code, status, message) {
    super(message || code);
    this.name = 'TokenError';
    // 'malformed' | 'invalid_signature' | 'invalid_issuer' | 'unknown_purpose'
    // | 'expired' | 'reused' | 'purpose_mismatch' | 'tenant_mismatch'
    // | 'signing_key_unavailable'
    this.code = code;
    this.status = status; // HTTP status the endpoint handler maps to
  }
}

// ─── Signing-key resolver (§13.2 — mirrors Master_Function get_jwt_signing_key) ─────

// Reject an empty / obviously-unusable key rather than silently HMAC-ing with it.
// HS256 needs real key material; a blank or trivially-short secret is a
// misconfiguration we must FAIL CLOSED on, never sign/verify against.
const MIN_SIGNING_KEY_LENGTH = 16;

let _cachedKey = null;

async function getSigningKey() {
  if (_cachedKey) return _cachedKey;
  let res;
  try {
    res = await secrets.send(
      new GetSecretValueCommand({ SecretId: JWT_SECRET_KEY_NAME })
    );
  } catch (_) {
    // Secrets Manager unavailable → fail closed; do NOT cache (a transient
    // outage must not poison every later call).
    throw new TokenError('signing_key_unavailable', 500, 'signing key unavailable');
  }
  const secretString = (res && res.SecretString) || '';
  let key = secretString;
  try {
    const parsed = JSON.parse(secretString);
    // Mirror Python: secret_data.get('signingKey', secret_string).
    if (parsed && typeof parsed === 'object' && parsed.signingKey) {
      key = parsed.signingKey;
    }
  } catch (_) {
    // Plain-string secret — use as-is.
  }
  if (typeof key !== 'string' || key.length < MIN_SIGNING_KEY_LENGTH) {
    // Empty / too-short → deny and do NOT cache, so fixing the secret takes
    // effect without a cold restart.
    throw new TokenError('signing_key_unavailable', 500, 'signing key empty or too short');
  }
  _cachedKey = key;
  return key;
}

async function resolveKey(opts) {
  return (opts && opts.signingKey) || getSigningKey();
}

function nowSeconds(opts) {
  if (opts && typeof opts.now === 'number') return opts.now;
  return Math.floor(Date.now() / 1000);
}

// ─── base64url + HMAC primitives ────────────────────────────────────────────────────

function b64urlEncode(buf) {
  return Buffer.from(buf)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function b64urlDecode(str) {
  return Buffer.from(str, 'base64url').toString('utf8');
}

function hmac(signingInput, key) {
  return crypto.createHmac('sha256', key).update(signingInput).digest();
}

// ─── Per-purpose expiry (§13.6 — exp is epoch SECONDS, set at sign time) ─────────────

// Expiry drivers (booking.start_at, event_end) arrive as ISO8601 (how the Booking
// row stores them) OR epoch seconds; normalize to epoch seconds.
function toEpochSeconds(value, field) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.floor(value);
  }
  if (typeof value === 'string') {
    const ms = Date.parse(value);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  throw new TokenError('malformed', 400, `invalid ${field}: ${value}`);
}

function computeExpiry(purpose, claims, iat) {
  let raw;
  switch (purpose) {
    case 'cancel':
      raw = toEpochSeconds(claims.start_at, 'start_at');
      break;
    case 'reschedule': {
      const windowHours = claims.cancellation_window_hours || 0;
      raw = toEpochSeconds(claims.start_at, 'start_at') - windowHours * 60 * 60;
      break;
    }
    case 'attended_yes':
    case 'no_show':
    case 'didnt_connect':
      raw = toEpochSeconds(claims.event_end, 'event_end') + DAY_SECONDS;
      break;
    case 'post_application_recovery':
      raw = iat + 14 * DAY_SECONDS;
      break;
    default:
      // Unreachable — caller validates purpose first; kept for defense in depth.
      throw new TokenError('unknown_purpose', 400, `unknown purpose: ${purpose}`);
  }
  // §B4 min-lifetime floor — never mint an already-expired / near-expired link.
  return Math.max(raw, iat + MIN_TOKEN_LIFETIME_SECONDS);
}

// ─── sign (§13.1/§13.3/§13.6) ───────────────────────────────────────────────────────

async function sign(purpose, claims, opts) {
  if (!PURPOSE_SET.has(purpose)) {
    throw new TokenError('unknown_purpose', 400, `unknown purpose: ${purpose}`);
  }
  const c = claims || {};
  if (!c.tenant_id) {
    throw new TokenError('malformed', 400, 'tenant_id is required');
  }
  if (purpose === 'post_application_recovery' && !c.form_submission_id) {
    throw new TokenError(
      'malformed',
      400,
      'form_submission_id is required for post_application_recovery'
    );
  }

  const iat = nowSeconds(opts);
  const exp = computeExpiry(purpose, c, iat);

  // §13.3 payload: standard claims + the reference-only custom claims. Expiry
  // drivers (start_at / event_end / window) are NOT persisted.
  const payload = {
    iss: ISSUER,
    iat,
    exp,
    jti: crypto.randomUUID(),
    purpose,
    tenant_id: c.tenant_id,
    booking_id: c.booking_id != null ? c.booking_id : null,
  };
  if (purpose === 'post_application_recovery') {
    payload.form_submission_id = c.form_submission_id;
  }

  const key = await resolveKey(opts);
  const encHeader = b64urlEncode(JSON.stringify({ alg: ALG, typ: 'JWT' }));
  const encPayload = b64urlEncode(JSON.stringify(payload));
  const signingInput = `${encHeader}.${encPayload}`;
  const encSig = b64urlEncode(hmac(signingInput, key));
  return `${signingInput}.${encSig}`;
}

// ─── verify (§13.2 signature + §13.3 issuer + §13.4 purpose + §13.6 expiry) ──────────

async function verify(token, opts) {
  if (typeof token !== 'string') {
    throw new TokenError('malformed', 400, 'token must be a string');
  }
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new TokenError('malformed', 400, 'token must have 3 segments');
  }
  const [encHeader, encPayload, encSig] = parts;

  // Header: enforce HS256 — reject alg:none / alg-confusion.
  let header;
  try {
    header = JSON.parse(b64urlDecode(encHeader));
  } catch (_) {
    throw new TokenError('malformed', 400, 'undecodable header');
  }
  if (!header || header.alg !== ALG) {
    throw new TokenError('invalid_signature', 400, 'unexpected alg');
  }

  // Signature: constant-time compare over header.payload. A bad base64url sig
  // decodes to a wrong-length / non-matching buffer and fails the check below;
  // a length mismatch short-circuits before timingSafeEqual (which throws on
  // unequal lengths).
  const key = await resolveKey(opts);
  const expectedSig = hmac(`${encHeader}.${encPayload}`, key);
  const providedSig = Buffer.from(encSig, 'base64url');
  if (
    providedSig.length !== expectedSig.length ||
    !crypto.timingSafeEqual(providedSig, expectedSig)
  ) {
    throw new TokenError('invalid_signature', 400, 'signature mismatch');
  }

  // Payload.
  let payload;
  try {
    payload = JSON.parse(b64urlDecode(encPayload));
  } catch (_) {
    throw new TokenError('malformed', 400, 'undecodable payload');
  }

  // Issuer — rejects chat-session JWTs (§13.3).
  if (payload.iss !== ISSUER) {
    throw new TokenError('invalid_issuer', 400, 'unexpected issuer');
  }
  // jti must be a non-empty string — it is the one-time-use key material redeem()
  // writes as the table sort key; a missing/non-string jti must never reach DDB.
  if (typeof payload.jti !== 'string' || payload.jti.length === 0) {
    throw new TokenError('malformed', 400, 'missing or invalid jti');
  }
  // Purpose — verifier side of the CI-3d enum lock.
  if (!PURPOSE_SET.has(payload.purpose)) {
    throw new TokenError('unknown_purpose', 400, `unknown purpose: ${payload.purpose}`);
  }
  // Expiry (§13.6) — exp is epoch seconds; expired groups with reused (§13.9).
  if (typeof payload.exp !== 'number' || nowSeconds(opts) >= payload.exp) {
    throw new TokenError('expired', 410, 'token expired');
  }

  // §13.8 defense-in-depth: the endpoint handler passes the URL's expected
  // purpose (and, when known, the bound tenant) so a valid token can't be
  // replayed against the wrong action/tenant. Optional — omitted ⇒ not checked.
  if (opts && opts.expectedPurpose != null && payload.purpose !== opts.expectedPurpose) {
    throw new TokenError('purpose_mismatch', 403, 'purpose does not match expected');
  }
  if (opts && opts.expectedTenantId != null && payload.tenant_id !== opts.expectedTenantId) {
    throw new TokenError('tenant_mismatch', 403, 'tenant does not match expected');
  }

  // Forward-compatible read: tolerate missing optional fields (schema discipline).
  return {
    iss: payload.iss,
    iat: payload.iat,
    exp: payload.exp,
    jti: payload.jti,
    purpose: payload.purpose,
    tenant_id: payload.tenant_id,
    booking_id: payload.booking_id != null ? payload.booking_id : null,
    form_submission_id:
      payload.form_submission_id != null ? payload.form_submission_id : null,
  };
}

// ─── redeem (§13.7 — one-time-use, atomic conditional PutItem) ───────────────────────

async function redeem(token, opts) {
  // verify() throws on any invalid/expired token BEFORE we touch the table, so a
  // bad token never consumes a jti slot.
  const claims = await verify(token, opts);

  try {
    await ddb.send(
      new PutItemCommand({
        TableName: JTI_BLACKLIST_TABLE,
        // Composite key (tenantId HASH, jti RANGE) — shipped schema.
        Item: {
          tenantId: { S: claims.tenant_id },
          jti: { S: claims.jti },
          // TTL — DynamoDB auto-deletes the row when the token would expire.
          expires_at: { N: String(claims.exp) },
          consumed_at: { N: String(nowSeconds(opts)) },
          reason: { S: 'one_time_use' },
        },
        // Atomic: succeeds only if this jti hasn't been consumed/revoked yet.
        ConditionExpression: 'attribute_not_exists(jti)',
      })
    );
  } catch (err) {
    if (
      err &&
      (err.name === 'ConditionalCheckFailedException' ||
        err.code === 'ConditionalCheckFailedException')
    ) {
      // Second click (or admin-revoked jti) → 410; the action does NOT execute.
      throw new TokenError('reused', 410, 'token already used');
    }
    throw err;
  }

  return claims;
}

module.exports = {
  TOKEN_PURPOSES,
  TokenError,
  sign,
  verify,
  redeem,
};
