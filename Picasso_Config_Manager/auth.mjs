/**
 * Clerk JWT Authentication Module for Picasso Config Manager
 *
 * Verifies Clerk RS256 JWTs via JWKS and reads claims from the
 * 'picasso-config' JWT template. No Clerk API calls needed.
 *
 * Required env vars:
 * - CLERK_JWKS_URL (optional): Override JWKS endpoint
 */

import crypto from 'crypto';
import https from 'https';

// ─── JWKS cache ─────────────────────────────────────────────────────────────
const CLERK_JWKS_URL = process.env.CLERK_JWKS_URL ||
  'https://present-skunk-55.clerk.accounts.dev/.well-known/jwks.json';
const JWKS_CACHE_TTL = 3600_000; // 1 hour
let jwksCache = null;
let jwksCacheTime = 0;

// ─── HTTP helper ────────────────────────────────────────────────────────────
function httpsGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers, timeout: 5000 }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
        } else {
          resolve(JSON.parse(data));
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
  });
}

// ─── JWKS fetch ─────────────────────────────────────────────────────────────
async function fetchJwks() {
  const now = Date.now();
  if (jwksCache && (now - jwksCacheTime) < JWKS_CACHE_TTL) {
    return jwksCache;
  }
  console.log(`[clerk-auth] Fetching JWKS from ${CLERK_JWKS_URL}`);
  const data = await httpsGet(CLERK_JWKS_URL, { Accept: 'application/json' });
  jwksCache = data;
  jwksCacheTime = now;
  return data;
}

// ─── JWT verification ───────────────────────────────────────────────────────
function base64urlDecode(str) {
  return Buffer.from(str, 'base64url');
}

async function verifyClerkJwt(token) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('Invalid token format');

  const [headerB64, payloadB64, signatureB64] = parts;
  const header = JSON.parse(base64urlDecode(headerB64).toString());
  const payload = JSON.parse(base64urlDecode(payloadB64).toString());

  if (!header.alg || !header.alg.startsWith('RS')) {
    throw new Error(`Unsupported algorithm: ${header.alg}`);
  }

  const kid = header.kid;
  if (!kid) throw new Error('JWT missing kid header');

  // Find matching key in JWKS
  const jwks = await fetchJwks();
  const jwk = (jwks.keys || []).find((k) => k.kid === kid);
  if (!jwk) throw new Error(`Unknown key id: ${kid}`);

  // Convert JWK to public key and verify signature
  const publicKey = crypto.createPublicKey({ key: jwk, format: 'jwk' });
  const signatureInput = Buffer.from(`${headerB64}.${payloadB64}`);
  const signature = base64urlDecode(signatureB64);

  const isValid = crypto.verify('sha256', signatureInput, publicKey, signature);
  if (!isValid) throw new Error('Invalid JWT signature');

  // Verify expiry
  const now = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < now) throw new Error('Token has expired');
  if (payload.nbf && payload.nbf > now) throw new Error('Token not yet valid');

  return payload;
}

// ─── Main auth function ─────────────────────────────────────────────────────
/**
 * Authenticate request by verifying Clerk JWT.
 * Claims come from the 'picasso-config' JWT template — no API calls needed.
 *
 * @param {Object} event - Lambda event
 * @returns {Promise<Object>} - Auth result
 */
export async function authenticateRequest(event) {
  try {
    const headers = event.headers || {};
    const authHeader = headers.Authorization || headers.authorization;

    if (!authHeader) {
      return { success: false, error: 'Missing Authorization header' };
    }

    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (!match) {
      return { success: false, error: 'Invalid Authorization header format' };
    }

    const token = match[1];
    const payload = await verifyClerkJwt(token);

    // Claims come directly from the picasso-config JWT template
    const email = payload.email || '';
    const name = payload.name || '';
    const role = payload.role || 'member';
    const tenantId = payload.tenant_id || null;
    const tenantHash = payload.tenant_hash || null;
    const company = payload.company || '';

    // Build tenants array — for super_admin all tenants are accessible,
    // for regular users just their org's tenant
    const tenants = tenantId ? [tenantId] : [];

    console.log(`[clerk-auth] Authenticated: ${email} role=${role} tenant=${tenantId}`);

    return {
      success: true,
      email,
      name: name || undefined,
      role,
      tenant_id: tenantId,
      tenant_hash: tenantHash,
      company,
      tenants,
    };
  } catch (error) {
    console.error('[clerk-auth] Authentication failed:', error.message);
    return { success: false, error: error.message };
  }
}
