/**
 * CORS helper — static allowlist replacing 16 hardcoded `*` wildcards across
 * index.js. Mirrors the MFS Python allowlist
 * (Master_Function_Staging/lambda_function.py `_CORS_ALLOWED_ORIGINS_DEFAULT`)
 * because both Lambdas serve traffic from the same widget origin set; drift
 * between the two is a latent bug class.
 *
 * Coupling note: `Access-Control-Allow-Credentials: true` is set here AND at
 * the Lambda URL CORS layer. Disabling one without the other produces a
 * confusing split-brain. Keep both in sync.
 */

const ALLOWED_ORIGINS = new Set([
  'http://localhost:8000',
  'http://localhost:5173',
  'http://localhost:3000',
  'https://chat.myrecruiter.ai',
  'https://staging.chat.myrecruiter.ai',
  'https://picassocode.s3.amazonaws.com',
  'https://picassostaging.s3.amazonaws.com',
]);

const DEFAULT_ORIGIN = 'https://chat.myrecruiter.ai';

function pickOrigin(event) {
  const headers = event?.headers || {};
  const origin = headers.origin ?? headers.Origin ?? headers.ORIGIN;
  if (!origin) return DEFAULT_ORIGIN;
  if (origin.startsWith('http://localhost:') || origin.startsWith('https://localhost:')) {
    return origin;
  }
  if (ALLOWED_ORIGINS.has(origin)) return origin;
  return DEFAULT_ORIGIN;
}

function corsHeaders(event, extras = {}) {
  return {
    'Access-Control-Allow-Origin': pickOrigin(event),
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Allow-Credentials': 'true',
    ...extras,
  };
}

module.exports = { corsHeaders, pickOrigin, ALLOWED_ORIGINS, DEFAULT_ORIGIN };
