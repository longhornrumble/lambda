'use strict';

/**
 * state.js — HMAC-signed, short-lived tokens for the OAuth consent flow.
 *
 * TWO token types share one signing key but are kept distinct by a `typ` claim so one can
 * never be replayed as the other:
 *
 *   • 'init'  — minted by the Clerk-authed dashboard backend (Analytics_Dashboard_API,
 *               INTEGRATOR GLUE), verified by GET /connect and GET /connection/status.
 *               Carries the coordinator identity the authenticated mint has already proven.
 *               Because /connect reads tenant_id/coordinator_id/coordinator_email ONLY from
 *               this signed token (never from query params), an anonymous caller cannot point
 *               a consent flow at someone else's secret slot — calendar-slot poisoning is
 *               structurally impossible.
 *
 *   • 'state' — minted by GET /connect, verified by GET /oauth/callback. The OAuth 2.0 CSRF
 *               token (RFC 6749 §10.12); also carries the coordinator identity forward so the
 *               callback writes the right secret without trusting the redirect's query.
 *
 * Wire format (compact, URL-safe, dependency-free):
 *   base64url(JSON payload) + '.' + base64url(HMAC-SHA256(payload_b64))
 *
 * The signing key is a dedicated secret (NOT the booking-token JWT key — separate trust
 * domain) at OAUTH_STATE_SIGNING_SECRET_NAME, shared only between the integrator's mint and
 * this verifier. Fetched once and cached for the container lifetime (DI-seamed for tests).
 */

const crypto = require('crypto');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const SIGNING_SECRET_NAME =
  process.env.OAUTH_STATE_SIGNING_SECRET_NAME || 'picasso/scheduling/oauth/_state-signing-key';

// Bounded client (mirror index.js): a slow Secrets Manager must not hang the route to the full
// Lambda timeout.
const sm = new SecretsManagerClient({
  maxAttempts: Number(process.env.AWS_MAX_ATTEMPTS || 2),
  requestHandler: new NodeHttpHandler({
    connectionTimeout: Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000),
    requestTimeout: Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000),
  }),
});

// Container-lifetime cache of the raw signing key. A rotated key needs a cold start to pick
// up — acceptable for an HMAC signing key (rotation is rare + operator-driven).
let _cachedKey = null;

/**
 * Fetch the HMAC signing key from Secrets Manager. The SecretString may be the raw key, or a
 * JSON object { key: "..." } — accept both. Throws (caller fail-closes) if absent/empty.
 * @param {object} [deps] - { getKey } injectable for tests
 */
async function getSigningKey(deps = {}) {
  if (deps.getKey) return deps.getKey();
  if (_cachedKey) return _cachedKey;
  const res = await sm.send(new GetSecretValueCommand({ SecretId: SIGNING_SECRET_NAME }));
  if (!res || typeof res.SecretString !== 'string' || res.SecretString.length === 0) {
    throw new Error('OAuth state signing key secret has no SecretString');
  }
  let key = res.SecretString;
  // Tolerate a JSON-wrapped key without requiring it. If the value parses as a JSON OBJECT, the
  // key MUST come from its `key` field — a JSON object with a missing/empty `key` is a
  // misconfiguration and must fail fast, NOT silently fall back to HMAC-ing the JSON text itself
  // (that would "work" self-consistently here but never verify tokens the integrator's mint signs).
  const trimmed = key.trim();
  if (trimmed.startsWith('{')) {
    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new Error('OAuth state signing key looks like JSON but does not parse');
    }
    if (!parsed || typeof parsed.key !== 'string' || parsed.key.length === 0) {
      throw new Error('OAuth state signing key JSON is missing a non-empty "key" field');
    }
    key = parsed.key;
  }
  if (!key) throw new Error('OAuth state signing key is empty');
  _cachedKey = key;
  return key;
}

function b64urlEncode(buf) {
  return Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function b64urlToBuffer(s) {
  const pad = s.length % 4 === 0 ? '' : '='.repeat(4 - (s.length % 4));
  return Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/') + pad, 'base64');
}

function hmac(key, payloadB64) {
  return crypto.createHmac('sha256', key).update(payloadB64).digest();
}

/**
 * Sign a payload object into a compact token. Adds `typ`, `iat`, `exp`, `nonce`.
 * @param {object} args - { typ, claims, ttlSeconds, nowMs, deps }
 */
async function sign({ typ, claims, ttlSeconds, nowMs = Date.now(), deps = {} }) {
  const key = await getSigningKey(deps);
  const iatSec = Math.floor(nowMs / 1000);
  const payload = {
    typ,
    ...claims,
    iat: iatSec,
    exp: iatSec + ttlSeconds,
    nonce: crypto.randomBytes(16).toString('hex'),
  };
  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  const sig = b64urlEncode(hmac(key, payloadB64));
  return `${payloadB64}.${sig}`;
}

class StateError extends Error {
  constructor(code) {
    super(code);
    this.name = 'StateError';
    this.code = code; // 'malformed' | 'bad_signature' | 'wrong_type' | 'expired'
  }
}

/**
 * Verify a token: structural shape, HMAC, `typ` match, and expiry. Returns the claims.
 * Throws StateError on any failure — the caller maps that to a generic 4xx (no detail leak).
 * @param {string} token
 * @param {object} args - { expectedType, nowMs, deps }
 */
async function verify(token, { expectedType, nowMs = Date.now(), deps = {} }) {
  if (typeof token !== 'string' || token.length === 0 || token.length > 4096) {
    throw new StateError('malformed');
  }
  const dot = token.indexOf('.');
  if (dot <= 0 || dot === token.length - 1 || token.indexOf('.', dot + 1) !== -1) {
    throw new StateError('malformed');
  }
  const payloadB64 = token.slice(0, dot);
  const sigB64 = token.slice(dot + 1);

  const key = await getSigningKey(deps);
  const expectedSig = hmac(key, payloadB64);
  const givenSig = b64urlToBuffer(sigB64);
  if (givenSig.length !== expectedSig.length || !crypto.timingSafeEqual(givenSig, expectedSig)) {
    throw new StateError('bad_signature');
  }

  let claims;
  try {
    claims = JSON.parse(b64urlToBuffer(payloadB64).toString('utf8'));
  } catch {
    throw new StateError('malformed');
  }
  if (!claims || typeof claims !== 'object') throw new StateError('malformed');
  if (claims.typ !== expectedType) throw new StateError('wrong_type');
  if (typeof claims.exp !== 'number' || claims.exp * 1000 <= nowMs) throw new StateError('expired');
  return claims;
}

module.exports = {
  sign,
  verify,
  getSigningKey,
  StateError,
  SIGNING_SECRET_NAME,
  // test-only
  _b64urlEncode: b64urlEncode,
  _resetKeyCache: () => {
    _cachedKey = null;
  },
};
