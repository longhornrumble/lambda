/**
 * CloudFront-origin header validator — mirrors the MFS Python pattern at
 * Master_Function_Staging/lambda_function.py:137-169
 * (validate_cf_origin_header) so BSH can reject requests that bypass
 * CloudFront and call the Function URL directly.
 *
 * Behavior:
 *  - When REQUIRE_CF_ORIGIN_HEADER is unset or !== 'true', validation is
 *    skipped: validateCfOriginHeader() returns { valid: true, reason: null }.
 *    This is the default during rollout and matches MFS's default-off shape.
 *  - When enabled, the request must carry header `x-picasso-cf-origin` whose
 *    value matches the Secrets Manager secret. Missing, mismatched, or
 *    secret-unavailable all fail closed (403 at the call site).
 *
 * Secrets Manager caching mirrors MFS:
 *  - Success: cached for the lifetime of the Lambda instance. Rotation
 *    requires publishing a new Lambda version (forces cold-start).
 *  - Failure: cached for SECRET_FAILURE_TTL_MS (60s) so a transient SM
 *    brownout doesn't trigger O(RPS) SM calls. After the TTL, the next
 *    call retries.
 *
 * Constant-time compare via crypto.timingSafeEqual. Different lengths are
 * rejected before the compare (timingSafeEqual throws on length mismatch),
 * so length itself is not a side-channel here either.
 */

const crypto = require('crypto');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const CF_ORIGIN_HEADER_NAME = 'x-picasso-cf-origin';
const SECRET_FAILURE_TTL_MS = 60 * 1000;

const smClient = new SecretsManagerClient({});

// Sentinel for "fetch failed, fail closed until TTL expires"
const SECRET_UNAVAILABLE = Symbol('secret-unavailable');

let cachedSecret = null;
let cachedSecretAt = 0;
let secretLoadingPromise = null;

function resetCacheForTests() {
  cachedSecret = null;
  cachedSecretAt = 0;
  secretLoadingPromise = null;
}

function findHeaderValue(headers, name) {
  if (!headers) return null;
  const target = name.toLowerCase();
  for (const key of Object.keys(headers)) {
    if (typeof key === 'string' && key.toLowerCase() === target) {
      return headers[key];
    }
  }
  return null;
}

async function getCfOriginSecret() {
  if (cachedSecret === SECRET_UNAVAILABLE) {
    if (Date.now() - cachedSecretAt < SECRET_FAILURE_TTL_MS) {
      return null;
    }
    cachedSecret = null;
  }
  if (cachedSecret !== null) {
    return cachedSecret;
  }
  if (secretLoadingPromise) {
    return secretLoadingPromise;
  }

  const secretName = process.env.CF_ORIGIN_SECRET_NAME || 'picasso/bsh/cf-origin-secret';
  secretLoadingPromise = (async () => {
    try {
      const result = await smClient.send(new GetSecretValueCommand({ SecretId: secretName }));
      const raw = result.SecretString || '';
      // Console-created secrets store JSON like {"secret": "..."}; plaintext
      // secrets are the raw string. Mirror MFS's handling of both shapes.
      let candidate = raw;
      try {
        const parsed = JSON.parse(raw);
        candidate = parsed.secret || parsed.value || raw;
      } catch (_) { /* not JSON — use raw */ }

      if (!candidate || !String(candidate).trim()) {
        console.error('SECURITY: CF origin secret is empty or whitespace-only; treating as unavailable');
        cachedSecret = SECRET_UNAVAILABLE;
        cachedSecretAt = Date.now();
        return null;
      }

      cachedSecret = String(candidate);
      cachedSecretAt = Date.now();
      return cachedSecret;
    } catch (e) {
      console.error(`SECURITY: failed to retrieve CF origin secret: ${e.message}`);
      cachedSecret = SECRET_UNAVAILABLE;
      cachedSecretAt = Date.now();
      return null;
    } finally {
      secretLoadingPromise = null;
    }
  })();
  return secretLoadingPromise;
}

async function validateCfOriginHeader(event) {
  if (String(process.env.REQUIRE_CF_ORIGIN_HEADER || 'false').toLowerCase() !== 'true') {
    return { valid: true, reason: null };
  }

  const received = findHeaderValue(event?.headers, CF_ORIGIN_HEADER_NAME);
  if (!received) {
    return { valid: false, reason: 'missing CF origin header' };
  }

  const expected = await getCfOriginSecret();
  if (!expected) {
    return { valid: false, reason: 'CF origin secret unavailable (failing closed)' };
  }

  const a = Buffer.from(String(received));
  const b = Buffer.from(String(expected));
  if (a.length !== b.length) {
    return { valid: false, reason: 'CF origin header mismatch' };
  }
  if (!crypto.timingSafeEqual(a, b)) {
    return { valid: false, reason: 'CF origin header mismatch' };
  }
  return { valid: true, reason: null };
}

module.exports = {
  validateCfOriginHeader,
  getCfOriginSecret,
  CF_ORIGIN_HEADER_NAME,
  SECRET_FAILURE_TTL_MS,
  resetCacheForTests,
};
