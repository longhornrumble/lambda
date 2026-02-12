/**
 * JWT Authentication Module for Picasso Config Manager
 * Implements HS256 JWT validation using native Node.js crypto
 *
 * JWT Structure:
 * - Header: {alg: "HS256", typ: "JWT"}
 * - Payload: {tenant_id, tenant_hash, email, name, role, company, features, tenants, exp, iat}
 * - Signature: HMAC-SHA256(header.payload, signing_key)
 */

import crypto from 'crypto';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';

// In-memory cache for signing key
let signingKeyCache = null;
let signingKeyCacheTime = 0;
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

const secretsClient = new SecretsManagerClient({ region: 'us-east-1' });

/**
 * Fetch JWT signing key from AWS Secrets Manager with 5-minute caching
 * @returns {Promise<string>} - The signing key
 */
async function getSigningKey() {
  const now = Date.now();

  // Return cached key if valid
  if (signingKeyCache && (now - signingKeyCacheTime) < CACHE_TTL) {
    console.log('Using cached signing key');
    return signingKeyCache;
  }

  console.log('Fetching signing key from Secrets Manager');
  try {
    const command = new GetSecretValueCommand({
      SecretId: 'picasso/staging/jwt/signing-key'
    });

    const response = await secretsClient.send(command);
    const secret = response.SecretString;

    // Secret may be JSON or plain string
    let signingKey;
    try {
      const parsed = JSON.parse(secret);
      signingKey = parsed.signing_key || parsed.key || secret;
    } catch {
      signingKey = secret;
    }

    // Update cache
    signingKeyCache = signingKey;
    signingKeyCacheTime = now;

    return signingKey;
  } catch (error) {
    console.error('Failed to fetch signing key:', error);
    throw new Error('Unable to fetch JWT signing key');
  }
}

/**
 * Decode base64url string to UTF-8 string
 * @param {string} str - Base64url encoded string
 * @returns {string} - Decoded string
 */
function base64urlDecode(str) {
  // Convert base64url to base64
  const base64 = str.replace(/-/g, '+').replace(/_/g, '/');
  // Decode base64 to buffer, then to UTF-8
  return Buffer.from(base64, 'base64').toString('utf-8');
}

/**
 * Validate and decode JWT token
 * @param {string} token - JWT token string
 * @returns {Promise<Object>} - Decoded JWT payload
 * @throws {Error} - If token is invalid or expired
 */
export async function validateJwt(token) {
  if (!token) {
    throw new Error('Token is required');
  }

  // Split token into parts
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new Error('Invalid token format');
  }

  const [headerB64, payloadB64, signatureB64] = parts;

  // Decode header and payload
  let header, payload;
  try {
    header = JSON.parse(base64urlDecode(headerB64));
    payload = JSON.parse(base64urlDecode(payloadB64));
  } catch (error) {
    throw new Error('Invalid token encoding');
  }

  // Verify algorithm
  if (header.alg !== 'HS256') {
    throw new Error(`Unsupported algorithm: ${header.alg}`);
  }

  // Fetch signing key
  const signingKey = await getSigningKey();

  // Compute expected signature using HMAC-SHA256
  const expectedSig = crypto
    .createHmac('sha256', signingKey)
    .update(`${headerB64}.${payloadB64}`)
    .digest();

  // Decode actual signature from base64url
  const actualSig = Buffer.from(signatureB64, 'base64url');

  // Constant-time comparison to prevent timing attacks
  if (expectedSig.length !== actualSig.length) {
    throw new Error('Invalid signature');
  }

  if (!crypto.timingSafeEqual(expectedSig, actualSig)) {
    throw new Error('Invalid signature');
  }

  // Check expiration
  const now = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < now) {
    throw new Error('Token has expired');
  }

  // Verify required claims
  if (!payload.tenant_id && !payload.role) {
    throw new Error('Token missing required claims');
  }

  return payload;
}

/**
 * Authenticate request by extracting and validating JWT from Authorization header
 * @param {Object} event - Lambda event object
 * @returns {Promise<Object>} - Authentication result
 *
 * Success: {success: true, tenant_id, tenant_hash, email, role, tenants, ...}
 * Failure: {success: false, error: string}
 */
export async function authenticateRequest(event) {
  try {
    // Extract Authorization header (case-insensitive)
    const headers = event.headers || {};
    const authHeader = headers.Authorization || headers.authorization;

    if (!authHeader) {
      return {
        success: false,
        error: 'Missing Authorization header'
      };
    }

    // Extract Bearer token
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (!match) {
      return {
        success: false,
        error: 'Invalid Authorization header format. Expected: Bearer <token>'
      };
    }

    const token = match[1];

    // Validate JWT
    const payload = await validateJwt(token);

    // Extract relevant fields
    const {
      tenant_id,
      tenant_hash,
      email,
      name,
      role,
      company,
      features,
      tenants,
      exp,
      iat
    } = payload;

    return {
      success: true,
      tenant_id,
      tenant_hash,
      email,
      name,
      role,
      company,
      features,
      tenants: tenants || [],
      exp,
      iat
    };
  } catch (error) {
    console.error('Authentication failed:', error.message);
    return {
      success: false,
      error: error.message
    };
  }
}
