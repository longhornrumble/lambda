'use strict';

/**
 * Scheduling_Synthetic_Monitor — CI-6 §5.1 synthetic monitoring Lambda.
 *
 * Continuously exercises the LIVE scheduling surfaces in staging so cross-repo drift and
 * regressions surface in burn-in within hours (§5.2). ALL FIVE CI-6 cycles now ship LIVE:
 *
 *   - cancel             (EventBridge, hourly)  — book→cancel→§14.2 status flip
 *   - reminder           (EventBridge, daily)   — book (compressed)→reminder pending→sent
 *                                                  flip (firing-path proof; §5.1 cadence)
 *   - revocation_observe (operator-invoked)     — one-time-token success→410 (§13.7)
 *   - cleanup            (EventBridge, nightly) — delete synthetic bookings >7d (§5.1 hygiene)
 *   - disposition        (EventBridge, daily)   — attend check→pending_attendance→no_show
 *                                                  disposition→idempotency (WS-T3-DISP / CI-6)
 *
 * The `reminder` cycle requires STAGING_TEST_MODE=true on BCH + a longer Lambda timeout
 * (it polls ~7min for the fire). The `disposition` cycle requires ATTEND_FUNCTION_NAME env
 * pointing at Attendance_Disposition_Handler + lambda:InvokeFunction grant (integrator glue).
 * DEFERRED Phase-2 work: email/SMS RECEIPT verification (SES inbound / Gmail) and the
 * §4.3 DST/volume soak. See README.
 *
 * HARD prod-guard: assertSafeMode() runs at MODULE LOAD (init refusal) and at handler entry
 * (defense-in-depth) — STAGING_TEST_MODE in production aborts cold-start (prod-guard.js).
 *
 * Invocation: { cycle: 'cancel' | 'reminder' | 'cleanup' | 'disposition' }, or for the
 * operator-triggered token cycle { cycle: 'revocation_observe', slug: '/cancel', token: '<one-time token>' }.
 * The `disposition` cycle (CI-6 5th cycle, WS-T3-DISP) is EventBridge-triggered (daily
 * recommended): book → attendance_check → no_show disposition → idempotency assert.
 * Requires STAGING_TEST_MODE=true + ATTEND_FUNCTION_NAME pointing at Attendance_Disposition_Handler.
 */

const { assertSafeMode } = require('./prod-guard');

// INIT refusal: a throw here fails the Lambda cold-start, so the function is structurally
// incapable of running synthetic logic in a production environment with test-mode enabled.
assertSafeMode({
  environment: process.env.ENVIRONMENT,
  stagingTestMode: process.env.STAGING_TEST_MODE,
});

const { runCancelCycle } = require('./cancel-cycle');
const { runReminderCycle } = require('./reminder-cycle');
const { runRevocationObserve } = require('./revocation-observer');
const { runCleanup } = require('./cleanup');
const { runDispositionCycle } = require('./disposition-cycle');

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
    case 'reminder':
      return (injected.runReminderCycle || runReminderCycle)(injected);
    case 'revocation_observe':
      return (injected.runRevocationObserve || runRevocationObserve)(
        { slug: event.slug, token: event.token },
        injected
      );
    case 'cleanup':
      return (injected.runCleanup || runCleanup)(injected);
    case 'disposition':
      return (injected.runDispositionCycle || runDispositionCycle)(injected);
    default:
      log('monitor_unknown_cycle', { cycle });
      return { success: false, error: `unknown cycle: ${cycle}` };
  }
};

exports._test = { assertSafeMode };
