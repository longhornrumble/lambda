'use strict';

/**
 * revocation-observer.js — the token-revocation cycle (§5.1), OPERATOR-TRIGGERED.
 *
 * Verifies §13.7 one-time-use enforcement: a one-time token redeemed once succeeds, and a
 * REPLAY returns 410 Gone. This cycle is OPERATOR-triggered and NEVER auto-mints or
 * auto-revokes — the monitor does not hold the JWT signing key and must not consume jtis on
 * a schedule. The operator supplies ONE real token (e.g. a cancel/attendance link from a
 * synthetic booking's confirmation email) and the slug it was minted for; the observer
 * redeems it twice against the Scheduling_Redemption_Handler endpoint and asserts the
 * success→410 transition.
 *
 * PII: the token is a one-time credential — it is NEVER logged (only the slug + the two
 * HTTP status codes).
 */

const alerts = require('./alerts');

const REDEMPTION_BASE_URL = process.env.REDEMPTION_BASE_URL || 'https://schedule.myrecruiter.ai';

// §13.8 LOCKED slug table (mirrors Scheduling_Redemption_Handler SLUG_TO_PURPOSE).
const VALID_SLUGS = new Set([
  '/cancel',
  '/reschedule',
  '/resume',
  '/attended/met',
  '/attended/noshow',
  '/attended/noconnect',
]);

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

async function runRevocationObserve(input = {}, deps = {}) {
  const fetchImpl = deps.fetch || globalThis.fetch;
  const emit = deps.emitCycleResult || alerts.emitCycleResult;
  const alert = deps.alert || alerts.alert;
  const baseUrl = deps.baseUrl || REDEMPTION_BASE_URL;
  const { slug, token } = input;

  try {
    if (!slug || !VALID_SLUGS.has(slug)) {
      throw new Error(`invalid or missing slug: ${slug}`);
    }
    if (!token) {
      throw new Error('missing token (operator must supply a real one-time token)');
    }
    if (typeof fetchImpl !== 'function') {
      throw new Error('no fetch implementation available');
    }

    const url = `${baseUrl}${slug}?t=${encodeURIComponent(token)}`;

    // First redemption: a volunteer purpose redirects (302), an attendance purpose renders
    // a 200 "got it" page. Either counts as a successful one-time use.
    const first = await fetchImpl(url, { method: 'GET', redirect: 'manual' });
    if (first.status !== 200 && first.status !== 302) {
      throw new Error(`first redemption expected 200/302, got ${first.status}`);
    }

    // Replay: the jti is now blacklisted → 410 Gone (§13.7).
    const second = await fetchImpl(url, { method: 'GET', redirect: 'manual' });
    if (second.status !== 410) {
      throw new Error(`replay expected 410 Gone, got ${second.status}`);
    }

    log('revocation_cycle_ok', { slug, first_status: first.status, replay_status: second.status });
    await emit('revocation', true);
    return { cycle: 'revocation', success: true, firstStatus: first.status, replayStatus: second.status };
  } catch (err) {
    warn('revocation_cycle_failed', { slug, error: err.message });
    await emit('revocation', false);
    await alert('Scheduling synthetic: revocation cycle FAILED', { slug, error: err.message });
    return { cycle: 'revocation', success: false, error: err.message };
  }
}

module.exports = { runRevocationObserve, _VALID_SLUGS: VALID_SLUGS };
