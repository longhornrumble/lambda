/**
 * CORS helper — static allowlist replacing 16 hardcoded `*` wildcards across
 * index.js. Mirrors the MFS Python allowlist
 * (Master_Function_Staging/lambda_function.py `_CORS_ALLOWED_ORIGINS_DEFAULT`)
 * because both Lambdas serve traffic from the same widget origin set; drift
 * between the two is a latent bug class. There is no automated cross-repo
 * enforcement — when changing MFS's list, manually mirror it here.
 *
 * Streaming-path note: in BSH's streaming export
 * (`streamifyResponse(streamingHandler)` at index.js:1170), the handler's
 * returned `{statusCode, headers, body}` has its `headers` field silently
 * dropped by AWS's streamifyResponse wrapper. The Lambda URL CORS config is
 * the active CORS gatekeeper for the streaming path. The corsHeaders() calls
 * in index.js are live only in the bufferedHandler fallback path (and in
 * unit tests). Keep the URL config and this helper in sync regardless — the
 * helper is the source of truth for the buffered path AND for the URL config
 * (the human running `aws lambda update-function-url-config` mirrors this
 * helper's values).
 *
 * Credentials coupling: `Access-Control-Allow-Credentials: true` is set here
 * AND at the Lambda URL CORS layer. Disabling one without the other produces
 * a confusing split-brain. Keep both in sync.
 */

const ALLOWED_ORIGINS = new Set([
  'http://localhost:8000',
  'http://localhost:5173',
  'http://localhost:3000',
  'https://chat.myrecruiter.ai',
  'https://staging.chat.myrecruiter.ai',
  'https://picassocode.s3.amazonaws.com',
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

// Extras are spread FIRST so security-critical CORS keys cannot be
// overridden by callers. extras can add supplemental headers
// (Content-Type, X-Accel-Buffering, etc.) but never clobber Origin/
// Methods/Headers/Credentials.
function corsHeaders(event, extras = {}) {
  return {
    ...extras,
    'Access-Control-Allow-Origin': pickOrigin(event),
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept',
    'Access-Control-Allow-Credentials': 'true',
  };
}

module.exports = { corsHeaders, pickOrigin, ALLOWED_ORIGINS, DEFAULT_ORIGIN };
