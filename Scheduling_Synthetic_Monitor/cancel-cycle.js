'use strict';

/**
 * cancel-cycle.js — the hourly cancel cycle (§5.1).
 *
 *   book (real propose→commit) → cancel via BCH scheduling_mutate → the §14.2 cal-lifecycle
 *   listener flips Booking.status=canceled ASYNCHRONOUSLY on the calendar delete push →
 *   poll the row until status=canceled.
 *
 * No time-compression — this cycle is immediate book-and-cancel, so it does NOT depend on
 * the (Phase-2) STAGING_TEST_MODE reminder dispatcher. The status flip is the §14.2
 * listener's job (cancel.js deliberately does not flip it), so the verification POLLS with
 * a bounded retry; a flip that never arrives is a real finding (listener lag/breakage),
 * which is exactly what burn-in should surface.
 */

const syntheticBooking = require('./synthetic-booking');
const bchClient = require('./bch-client');
const bookingTable = require('./booking-table');
const alerts = require('./alerts');

const POLL_ATTEMPTS = Number(process.env.CANCEL_POLL_ATTEMPTS || 12);
const POLL_INTERVAL_MS = Number(process.env.CANCEL_POLL_INTERVAL_MS || 5000);

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runCancelCycle(deps = {}) {
  const create = deps.createSyntheticBooking || syntheticBooking.createSyntheticBooking;
  const invoke = deps.invokeBch || bchClient.invokeBch;
  const getBooking = deps.getBooking || bookingTable.getBooking;
  const emit = deps.emitCycleResult || alerts.emitCycleResult;
  const alert = deps.alert || alerts.alert;
  const sleep = deps.sleep || defaultSleep;
  const pollAttempts = deps.pollAttempts || POLL_ATTEMPTS;
  const pollIntervalMs = deps.pollIntervalMs != null ? deps.pollIntervalMs : POLL_INTERVAL_MS;

  let tenantId;
  let bookingId;
  try {
    const created = await create({ cyclePrefix: 'cancel' }, deps);
    ({ tenantId, bookingId } = created);
    const { booking } = created;
    if (!booking || !booking.coordinator_email || !booking.external_event_id) {
      throw new Error('synthetic booking row missing coordinator_email/external_event_id');
    }

    // cancel via the real Tier-2 executor (BCH scheduling_mutate). v1: resource_id ==
    // coordinator_email, so coordinatorId (the OAuth secret-path key) == coordinator_email.
    const cancelRes = await invoke({
      action: 'scheduling_mutate',
      mutation: 'cancel',
      tenantId,
      coordinatorId: booking.coordinator_email,
      bookingId,
      booking,
    });
    if (
      !cancelRes ||
      (cancelRes.outcome !== 'deleted' && cancelRes.outcome !== 'pending_calendar_sync')
    ) {
      throw new Error(`cancel mutate failed (outcome=${cancelRes && cancelRes.outcome})`);
    }

    // poll for the §14.2 listener's async status flip.
    let status;
    for (let i = 0; i < pollAttempts; i += 1) {
      const row = await getBooking(tenantId, bookingId);
      status = row && row.status;
      if (status === 'canceled') break;
      if (i < pollAttempts - 1) await sleep(pollIntervalMs);
    }
    if (status !== 'canceled') {
      throw new Error(
        `Booking.status did not reach canceled (last=${status}) within ${pollAttempts} polls`
      );
    }

    log('cancel_cycle_ok', { tenant_id: tenantId, booking_id: bookingId });
    await emit('cancel', true);
    return { cycle: 'cancel', success: true, bookingId };
  } catch (err) {
    warn('cancel_cycle_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
    await emit('cancel', false);
    await alert('Scheduling synthetic: cancel cycle FAILED', { tenantId, bookingId, error: err.message });
    return { cycle: 'cancel', success: false, bookingId, error: err.message };
  }
}

module.exports = { runCancelCycle };
