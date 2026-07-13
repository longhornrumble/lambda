'use strict';

/**
 * Meta Send API error classification — M-Ha channel health (G1).
 *
 * Meta emits NO token-invalidation webhook: a disconnected/expired Page token
 * just makes every send fail. Without classification that is silent channel
 * death — discovered by a tenant, not an alarm. Each classified failure is
 * logged as a structured `META_SEND_FAILURE` line; CloudWatch metric filters
 * (picasso repo, ops-alarms-meta-staging) turn those into per-class metrics +
 * alarms.
 *
 * Reconciled error set (research pack 01-04 + plan M-Ha):
 *   190              — Page token invalid/expired  → channel is DEAD (alarm)
 *   551              — user unavailable            → benign per-user noise (no alarm)
 *   613              — rate limited                → sustained bursts alarm
 *   10 / 1545041     — outside messaging window    → policy signal (no alarm; 24h guard owns it)
 *   10 / 1893063     — Page messaging-restricted   → severe policy action (alarm)
 */

const CLASSIFICATIONS = {
  TOKEN_DEAD: 'token_dead',
  USER_UNAVAILABLE: 'user_unavailable',
  RATE_LIMITED: 'rate_limited',
  WINDOW_CLOSED: 'window_closed',
  PAGE_RESTRICTED: 'page_restricted',
  UNCLASSIFIED: 'unclassified',
};

/**
 * Classify a Meta Send API error body ({ error: { code, error_subcode, … } }).
 * Pure; tolerates any malformed shape (returns UNCLASSIFIED).
 *
 * @param {object} errorBody - parsed JSON error response from the Send API
 * @returns {{classification: string, code: number|null, subcode: number|null}}
 */
function classifyMetaSendError(errorBody) {
  const err = errorBody && typeof errorBody === 'object' ? errorBody.error : null;
  const code = err && typeof err.code === 'number' ? err.code : null;
  const subcode = err && typeof err.error_subcode === 'number' ? err.error_subcode : null;

  let classification = CLASSIFICATIONS.UNCLASSIFIED;
  if (code === 190) classification = CLASSIFICATIONS.TOKEN_DEAD;
  else if (code === 551) classification = CLASSIFICATIONS.USER_UNAVAILABLE;
  else if (code === 613) classification = CLASSIFICATIONS.RATE_LIMITED;
  else if (code === 10 && subcode === 1545041) classification = CLASSIFICATIONS.WINDOW_CLOSED;
  else if (code === 10 && subcode === 1893063) classification = CLASSIFICATIONS.PAGE_RESTRICTED;

  return { classification, code, subcode };
}

module.exports = { classifyMetaSendError, CLASSIFICATIONS };
