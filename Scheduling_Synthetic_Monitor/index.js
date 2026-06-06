'use strict';

/**
 * Scheduling_Synthetic_Monitor — CI-6 §5.1 synthetic monitoring Lambda (PHASE 1).
 *
 * Continuously exercises the LIVE scheduling surfaces in staging so cross-repo drift and
 * regressions surface in burn-in within hours (§5.2). PHASE 1 covers the three cycles whose
 * producers already ship LIVE:
 *
 *   • cancel             (EventBridge, hourly)  — book→cancel→§14.2 status flip
 *   • revocation_observe (operator-invoked)     — one-time-token success→410 (§13.7)
 *   • cleanup            (EventBridge, nightly) — delete synthetic bookings >7d (§5.1 hygiene)
 *
 * PHASE 2 (DEFERRED — blocked on WS-E-REMIND + WS-E-ATTEND and the FROZEN_CONTRACTS §E1/§E6
 * lock): the attendance, reminder-cadence, and missed-event-disposition cycles, which
 * require the STAGING_TEST_MODE time-compression in the (unbuilt) reminder dispatcher.
 *
 * HARD prod-guard: assertSafeMode() runs at MODULE LOAD (init refusal) and at handler entry
 * (defense-in-depth) — STAGING_TEST_MODE in production aborts cold-start (prod-guard.js).
 *
 * Invocation: { cycle: 'cancel' | 'cleanup' }, or for the operator-triggered token cycle
 * { cycle: 'revocation_observe', slug: '/cancel', token: '<one-time token>' }.
 */

const { assertSafeMode } = require('./prod-guard');

// INIT refusal: a throw here fails the Lambda cold-start, so the function is structurally
// incapable of running synthetic logic in a production environment with test-mode enabled.
assertSafeMode({
  environment: process.env.ENVIRONMENT,
  stagingTestMode: process.env.STAGING_TEST_MODE,
});

const { runCancelCycle } = require('./cancel-cycle');
const { runRevocationObserve } = require('./revocation-observer');
const { runCleanup } = require('./cleanup');

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

exports.handler = async function handler(event = {}, _ctx, injected = {}) {
  // Defense-in-depth: re-assert the guard at entry (the runtime env could differ from
  // module-load time, e.g. an in-process test invocation).
  assertSafeMode({
    environment: process.env.ENVIRONMENT,
    stagingTestMode: process.env.STAGING_TEST_MODE,
  });

  const cycle = event && event.cycle;
  log('monitor_invoked', { cycle });

  switch (cycle) {
    case 'cancel':
      return (injected.runCancelCycle || runCancelCycle)(injected);
    case 'revocation_observe':
      return (injected.runRevocationObserve || runRevocationObserve)(
        { slug: event.slug, token: event.token },
        injected
      );
    case 'cleanup':
      return (injected.runCleanup || runCleanup)(injected);
    default:
      log('monitor_unknown_cycle', { cycle });
      return { success: false, error: `unknown cycle: ${cycle}` };
  }
};

exports._test = { assertSafeMode };
