'use strict';

/**
 * revocation.js — classify a refresh/probe failure as a PERMANENT revocation vs anything else.
 *
 * Marker set + dual-error-shape coercion are mirrored from the shipped, battle-tested
 * Booking_Commit_Handler/calendar-events.js `classifyAuthError` (lambda#231; the 2026-06-04
 * UAT proved the string-vs-object `response.data.error` coercion matters). It is duplicated
 * here rather than imported because that function lives inside the BCH Lambda bundle, not in
 * shared/scheduling/ — cross-Lambda-dir imports are the anti-pattern. If classifyAuthError is
 * ever promoted to shared/scheduling/, replace this with an import.
 *
 * Decision semantics (conservative — only a CONFIRMED revocation disconnects):
 *   • permanent === true  → the grant is revoked/invalid (invalid_grant, etc.) → /connection/status
 *                           marks the secret revoked and reports bookable:false. RE-AUTH required.
 *   • permanent === false → transient 401, 5xx, timeout, network — NOT a confirmed revocation →
 *                           reported as "stale_connected"; the stored secret is left untouched.
 *     This is exactly the work-order's "401 invalid_grant → disconnect" vs "5xx → stale-connected".
 */

// PERMANENT = a per-COORDINATOR revocation: this coordinator's grant is gone → stamp their
// secret revoked. NOTE the deliberate divergence from the shipped BCH classifyAuthError, which
// also lists `invalid_client`: that error means the PLATFORM app credentials are wrong, NOT a
// per-coordinator revocation. Treating it as permanent here would mass-stamp EVERY polling
// coordinator `revoked` (irreversible) the moment the platform-app secret breaks. So
// `invalid_client` is classified PLATFORM (transient for the per-coordinator decision) and
// surfaced separately for an operator alarm (integrator directive #3).
const PERMANENT_MARKERS = [
  'invalid_grant',
  'unauthorized_client',
  'Token has been expired or revoked',
];

// Platform-level credential failure — NOT a per-coordinator revocation.
const PLATFORM_MARKERS = ['invalid_client'];

/**
 * @param {any} err - a thrown GaxiosError (or any error) from a refresh/probe attempt
 * @returns {{ permanent: boolean, platform: boolean, httpStatus: number|null }}
 *   permanent = per-coordinator revocation (stamp the secret revoked)
 *   platform  = platform-app credential failure (operator alarm; do NOT stamp the coordinator)
 */
function classifyTokenError(err) {
  const httpStatus = err?.code ?? err?.response?.status ?? null;

  // Google's two error shapes differ: the OAuth token endpoint returns `response.data.error`
  // as a STRING ('invalid_grant', ...), the Calendar API as an OBJECT ({ code, message, ... }).
  // Coerce to a searchable string so .includes can't crash on an object.
  const rawOauthError = err?.response?.data?.error;
  const oauthError =
    typeof rawOauthError === 'string'
      ? rawOauthError
      : (rawOauthError && (rawOauthError.message || rawOauthError.status)) || '';
  const message = String(err?.message || '');
  const matches = (markers) => markers.some((m) => oauthError.includes(m) || message.includes(m));

  return {
    permanent: matches(PERMANENT_MARKERS),
    platform: matches(PLATFORM_MARKERS),
    httpStatus: typeof httpStatus === 'number' ? httpStatus : null,
  };
}

module.exports = {
  classifyTokenError,
  PERMANENT_MARKERS,
  PLATFORM_MARKERS,
};
