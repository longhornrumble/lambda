'use strict';

/**
 * disposition-cycle.js — the missed-event disposition cycle (§5.1, CI-6 5th cycle).
 *
 *   book (real propose→commit, is_synthetic:true) with event_end ALREADY IN THE PAST
 *   (or immediate — set via cyclePrefix='disposition' so BCH time-compression is active
 *   when STAGING_TEST_MODE is set) → invoke Attendance_Disposition_Handler with
 *   action:'attendance_check' → assert attendance_state='pending_attendance' → drive ONE
 *   disposition: applyDisposition({purpose:'didnt_connect'}) → assert
 *   Booking.status='coordinator_no_show' + Booking.attendance_state='resolved' → assert
 *   idempotency: second applyDisposition yields outcome='already_resolved' → emit
 *   CycleSuccess/CycleFailure + SNS on failure.
 *
 * ── Design choice: applyDisposition (shared module), not HTTP redemption endpoint ──
 *   The work-order offered two paths: redeem a token against the HTTP redemption surface
 *   (like revocation-observer.js), OR invoke the handler-side path. We choose the
 *   handler-side path (applyDisposition from shared/scheduling/disposition.js) for two
 *   reasons:
 *     1. Less new surface: no HTTP fetch client in the monitor, no live REDEMPTION_BASE_URL
 *        dependency, no token extraction from the prompt email (which is not machine-readable
 *        in the synthetic path). The HTTP path would require the monitor to hold the JWT
 *        signing key or extract the token from the confirmation email — both introduce state
 *        the monitor must not own.
 *     2. Token hygiene: the attendance tokens are single-use (§13.7). Burning one inside a
 *        synthetic cycle would consume a real jti. The applyDisposition call is the same code
 *        path the Scheduling_Redemption_Handler calls after token redemption (§E4 §11.2),
 *        so we exercise the full disposition logic without touching the token layer.
 *   The applyDisposition dep is injected at the DI seam so tests stay AWS-free.
 *
 * ── Why 'didnt_connect' (not 'no_show') ──
 *   §11.2: didnt_connect → coordinator_no_show sets status + attendance_state='resolved' with
 *   NO outbound at all (no volunteer reoffer notice, no reschedule-token mint). Choosing
 *   'no_show' would reach tokens.sign (requires the JWT signing key — the monitor must never
 *   hold it) and notify.dispatchVolunteerNotice → defaultInvokeEmail (requires
 *   lambda:InvokeFunction on send_email — ungranted on the monitor role). The
 *   volunteer-notice outbound path (no_show/reschedule-token + email) stays covered by the
 *   ATTEND/redemption unit tests; the synthetic cycle proves: attendance_check conditional
 *   write, the status transition (via the single DDB UpdateItem that is the ONLY live
 *   dependency here), attendance_state=resolved, and idempotency — with the monitor holding
 *   NO signing key and sending NO mail.
 *
 * ── Synthetic booking shape ──
 *   createSyntheticBooking is reused verbatim. The resulting booking has status='booked' and
 *   is in the past enough that attendance_check fires immediately (the EventBridge rule fires
 *   at event_end+30min — for CI we invoke the handler directly, not through EventBridge, so
 *   the timing constraint is the ATTEND handler's own `status==='booked'` guard).
 *
 * ── Cleanup ──
 *   The synthetic booking ends with status='coordinator_no_show' (a terminal status — never
 *   'canceled'). The nightly cleanup cycle (cleanup.js) deletes is_synthetic rows regardless
 *   of their terminal status: querySyntheticOlderThan filters on is_synthetic=true +
 *   item_type=booking + created_at < cutoff; status is irrelevant. coordinator_no_show rows
 *   beyond the retention window are cleaned up exactly like canceled rows — no new cleanup
 *   logic needed.
 *
 * ── Prod-guard ──
 *   The cycle lives inside the same prod-guarded Lambda (index.js). The double-gate
 *   (is_synthetic on the row + STAGING_TEST_MODE on BCH) still applies. The disposition
 *   itself is a real DDB write — it only runs on a booking we just created as synthetic.
 *
 * ── Build note ──
 *   The runtime require('../shared/scheduling/disposition') is resolved at build time by the
 *   esbuild bundle (npm run build), same as other lambdas bundling shared modules — no
 *   deploy-zip gap.
 */

const syntheticBooking = require('./synthetic-booking');
const attendClient = require('./attend-client');
const bookingTable = require('./booking-table');
const alerts = require('./alerts');

// applyDisposition default: lazily resolved on first call (lazy require) so tests that
// inject the dep never instantiate the production DDB client. This also avoids a hard
// module-load failure if shared/scheduling/disposition is not on the path at require time.
// In production the Lambda always has shared/ available via the bundle (esbuild.config.mjs).
let _dispositionModule;
function defaultApplyDisposition(args) {
  if (!_dispositionModule) {
    // Resolve relative to this file's directory, one level up to the lambda repo root.
    _dispositionModule = require('../shared/scheduling/disposition');
  }
  return _dispositionModule.applyDisposition(args);
}

const POLL_ATTEMPTS = Number(process.env.DISPOSITION_POLL_ATTEMPTS || 8);
const POLL_INTERVAL_MS = Number(process.env.DISPOSITION_POLL_INTERVAL_MS || 3000);

// The §E4 disposition we drive in the synthetic cycle. 'didnt_connect' → coordinator_no_show:
// sets status + attendance_state='resolved' with NO outbound (no reschedule-token mint, no
// volunteer reoffer email, no lambda:InvokeFunction on send_email). The monitor role has no
// JWT signing key grant and no send_email invoke grant — using 'no_show' would reach both.
// The volunteer-notice path stays covered by the ATTEND/redemption unit tests.
const SYNTHETIC_DISPOSITION_PURPOSE = 'didnt_connect';
const EXPECTED_TERMINAL_STATUS = 'coordinator_no_show'; // §E4: didnt_connect → coordinator_no_show
const EXPECTED_ATTENDANCE_STATE = 'resolved'; // §E4: disposition clears the flow label

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runDispositionCycle(deps = {}) {
  const create = deps.createSyntheticBooking || syntheticBooking.createSyntheticBooking;
  const invokeAttend = deps.invokeAttend || attendClient.invokeAttend;
  const getBooking = deps.getBooking || bookingTable.getBooking;
  const applyDisposition =
    deps.applyDisposition || defaultApplyDisposition;
  const emit = deps.emitCycleResult || alerts.emitCycleResult;
  const alert = deps.alert || alerts.alert;
  const sleep = deps.sleep || defaultSleep;
  const pollAttempts = deps.pollAttempts || POLL_ATTEMPTS;
  const pollIntervalMs = deps.pollIntervalMs != null ? deps.pollIntervalMs : POLL_INTERVAL_MS;

  let tenantId;
  let bookingId;
  try {
    // ── Step 1: create a synthetic booking ─────────────────────────────────────────────
    const created = await create({ cyclePrefix: 'disposition' }, deps);
    ({ tenantId, bookingId } = created);
    const { booking } = created;
    if (!booking) {
      throw new Error('synthetic booking row missing (read-back returned null)');
    }

    // ── Step 2: attendance_check → pending_attendance + 3 tokens minted ────────────────
    //   Invoke the ATTEND Lambda directly (not via EventBridge — we exercise the handler's
    //   action path, not the scheduler lifecycle; the scheduler rule is integrator IaC glue).
    const attendResult = await invokeAttend({
      action: 'attendance_check',
      tenantId,
      booking_id: bookingId,
    });
    if (
      !attendResult ||
      (attendResult.outcome !== 'pending_attendance_set' &&
        attendResult.outcome !== 'skipped_already_marked')
    ) {
      throw new Error(
        `attendance_check returned unexpected outcome: ${
          attendResult && attendResult.outcome
        } (expected pending_attendance_set)`
      );
    }
    log('disposition_cycle_attend_check_ok', {
      tenant_id: tenantId,
      booking_id: bookingId,
      outcome: attendResult.outcome,
    });

    // ── Step 3: assert attendance_state='pending_attendance' on the row ─────────────────
    //   A DDB read-back confirms the ATTEND handler wrote the non-key attribute (§E4).
    const rowAfterCheck = await getBooking(tenantId, bookingId);
    if (!rowAfterCheck || rowAfterCheck.attendance_state !== 'pending_attendance') {
      throw new Error(
        `expected attendance_state='pending_attendance' after attendance_check; ` +
          `got '${rowAfterCheck && rowAfterCheck.attendance_state}'`
      );
    }
    if (rowAfterCheck.status !== 'booked') {
      throw new Error(
        `expected status='booked' before disposition; got '${rowAfterCheck.status}'`
      );
    }
    log('disposition_cycle_pending_attendance_confirmed', {
      tenant_id: tenantId,
      booking_id: bookingId,
    });

    // ── Step 4: drive ONE disposition (didnt_connect → coordinator_no_show) ────────────
    //   applyDisposition is the same function the Scheduling_Redemption_Handler calls after
    //   token redemption (§E4 §11.2). We inject it as a dep so tests stay AWS-free. The
    //   production default runs against the real BOOKING_TABLE (shared env var).
    //   'didnt_connect' has no outbound (no token mint, no volunteer email) — the only live
    //   dependency is the conditional DDB UpdateItem on the Booking row.
    const disposition = await applyDisposition({
      tenantId,
      bookingId,
      purpose: SYNTHETIC_DISPOSITION_PURPOSE,
      deps: deps.dispositionDeps || {},
    });
    if (!disposition || disposition.outcome !== EXPECTED_TERMINAL_STATUS) {
      throw new Error(
        `applyDisposition returned unexpected outcome: ${
          disposition && disposition.outcome
        } (expected ${EXPECTED_TERMINAL_STATUS})`
      );
    }
    log('disposition_cycle_disposition_applied', {
      tenant_id: tenantId,
      booking_id: bookingId,
      outcome: disposition.outcome,
    });

    // ── Step 5: poll for status + attendance_state to be written ───────────────────────
    //   applyDisposition is synchronous (single UpdateItem) so the row should be readable
    //   immediately. We still poll briefly to handle any DDB propagation / eventually-
    //   consistent read edge (mirrors cancel-cycle.js's bounded polling pattern).
    let finalRow = null;
    for (let i = 0; i < pollAttempts; i += 1) {
      finalRow = await getBooking(tenantId, bookingId);
      if (
        finalRow &&
        finalRow.status === EXPECTED_TERMINAL_STATUS &&
        finalRow.attendance_state === EXPECTED_ATTENDANCE_STATE
      ) {
        break;
      }
      if (i < pollAttempts - 1) await sleep(pollIntervalMs);
    }
    if (
      !finalRow ||
      finalRow.status !== EXPECTED_TERMINAL_STATUS ||
      finalRow.attendance_state !== EXPECTED_ATTENDANCE_STATE
    ) {
      throw new Error(
        `Booking did not reach status='${EXPECTED_TERMINAL_STATUS}' + attendance_state='${EXPECTED_ATTENDANCE_STATE}' ` +
          `within ${pollAttempts} polls ` +
          `(last status='${finalRow && finalRow.status}' attendance_state='${
            finalRow && finalRow.attendance_state
          }')`
      );
    }
    log('disposition_cycle_row_verified', {
      tenant_id: tenantId,
      booking_id: bookingId,
      status: finalRow.status,
      attendance_state: finalRow.attendance_state,
    });

    // ── Step 6: idempotency — second applyDisposition must yield 'already_resolved' ─────
    const idempotentResult = await applyDisposition({
      tenantId,
      bookingId,
      purpose: SYNTHETIC_DISPOSITION_PURPOSE,
      deps: deps.dispositionDeps || {},
    });
    if (!idempotentResult || idempotentResult.outcome !== 'already_resolved') {
      throw new Error(
        `idempotency check: second applyDisposition returned '${
          idempotentResult && idempotentResult.outcome
        }' (expected 'already_resolved')`
      );
    }
    log('disposition_cycle_idempotency_ok', {
      tenant_id: tenantId,
      booking_id: bookingId,
      idempotent_outcome: idempotentResult.outcome,
    });

    log('disposition_cycle_ok', { tenant_id: tenantId, booking_id: bookingId });
    await emit('disposition', true);
    return { cycle: 'disposition', success: true, bookingId };
  } catch (err) {
    warn('disposition_cycle_failed', {
      tenant_id: tenantId,
      booking_id: bookingId,
      error: err.message,
    });
    await emit('disposition', false);
    await alert('Scheduling synthetic: disposition cycle FAILED', {
      tenantId,
      bookingId,
      error: err.message,
    });
    return { cycle: 'disposition', success: false, bookingId, error: err.message };
  }
}

module.exports = {
  runDispositionCycle,
  // Exported for tests:
  _SYNTHETIC_DISPOSITION_PURPOSE: SYNTHETIC_DISPOSITION_PURPOSE,
  _EXPECTED_TERMINAL_STATUS: EXPECTED_TERMINAL_STATUS,
  _EXPECTED_ATTENDANCE_STATE: EXPECTED_ATTENDANCE_STATE,
};
