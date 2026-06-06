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

const PERMANENT_MARKERS = [
  'invalid_grant',
  'unauthorized_client',
  'invalid_client',
  'Token has been expired or revoked',
];

/**
 * @param {any} err - a thrown GaxiosError (or any error) from a refresh/probe attempt
 * @returns {{ permanent: boolean, httpStatus: number|null }}
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

  const permanent = PERMANENT_MARKERS.some(
    (m) => oauthError.includes(m) || message.includes(m)
  );
  return { permanent, httpStatus: typeof httpStatus === 'number' ? httpStatus : null };
}

module.exports = {
  classifyTokenError,
  PERMANENT_MARKERS,
};
