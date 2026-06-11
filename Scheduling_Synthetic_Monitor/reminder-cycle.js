'use strict';

/**
 * reminder-cycle.js — the reminder cadence cycle (§5.1, CI-6 Phase-2 dispatch-proof slice).
 *
 *   book (real propose→commit, is_synthetic:true → COMPRESSED cadence) → the scheduler
 *   writes picasso-scheduled-messages rows (status:'pending') + EventBridge one-time
 *   schedules at the compressed fire times (t24h→+1m, t1h→+3m …) → EventBridge fires
 *   Scheduled_Message_Sender → it dispatches + flips the row status to 'sent' → poll the
 *   rows until a cadence-reminder row reaches 'sent'.
 *
 * The `pending → sent` flip IS the dispatch proof — it exercises the firing path the S6
 * activation smoke did NOT cover (EventBridge → Sender). This is the infra-light variant:
 * it does NOT verify email/SMS RECEIPT (SES inbound / Gmail polling — deferred, README).
 *
 * Time-compression is REQUIRED: the synthetic booking commits with is_synthetic:true, and
 * BCH must run with STAGING_TEST_MODE=true so scheduleReminders compresses the fire times.
 * Without it, reminders schedule at real 24h/1h offsets and this cycle's bounded poll can
 * never observe a fire (a clean failure — the burn-in signal that compression is off).
 *
 * A row stuck 'pending' past the poll window is a REAL finding (EventBridge/Sender lag or
 * breakage) — exactly what burn-in should surface.
 */

const syntheticBooking = require('./synthetic-booking');
const scheduledMessages = require('./scheduled-messages-table');
const bchClient = require('./bch-client');
const alerts = require('./alerts');

// Compressed fires land at +1m..+5m; EventBridge Scheduler adds ~1m delivery latency. Poll
// ~7min so a fire is comfortably observed. Lambda timeout must exceed this (see INFRA_NOTES).
const POLL_ATTEMPTS = Number(process.env.REMINDER_POLL_ATTEMPTS || 42);
const POLL_INTERVAL_MS = Number(process.env.REMINDER_POLL_INTERVAL_MS || 10000);

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// A cadence reminder (NOT the attendance check): moment='reminder' with a tier. The
// attendance row also carries moment='reminder' but attendance_check=true / no tier.
function isCadenceReminder(row) {
  return row && row.moment === 'reminder' && !row.attendance_check && !!row.tier;
}

async function runReminderCycle(deps = {}) {
  const create = deps.createSyntheticBooking || syntheticBooking.createSyntheticBooking;
  const queryByAppointment = deps.queryByAppointment || scheduledMessages.queryByAppointment;
  const invoke = deps.invokeBch || bchClient.invokeBch;
  const emit = deps.emitCycleResult || alerts.emitCycleResult;
  const alert = deps.alert || alerts.alert;
  const sleep = deps.sleep || defaultSleep;
  const pollAttempts = deps.pollAttempts || POLL_ATTEMPTS;
  const pollIntervalMs = deps.pollIntervalMs != null ? deps.pollIntervalMs : POLL_INTERVAL_MS;

  let tenantId;
  let bookingId;
  let booking;
  try {
    const created = await create({ cyclePrefix: 'reminder' }, deps);
    ({ tenantId, bookingId, booking } = created);
    if (!booking || !booking.coordinator_email) {
      throw new Error('synthetic booking row missing coordinator_email (cleanup needs it)');
    }

    // Assert the scheduler created cadence-reminder rows. None = scheduling didn't run
    // (compression off, scheduler broken, or a <1h lead) — a clean, alerting failure.
    const initial = await queryByAppointment(tenantId, bookingId);
    const reminderRows = (initial || []).filter(isCadenceReminder);
    if (reminderRows.length === 0) {
      throw new Error(
        `no cadence-reminder rows scheduled for the synthetic booking (got ${
          (initial || []).length
        } rows) — STAGING_TEST_MODE off, scheduler broken, or lead <1h`
      );
    }
    log('reminder_rows_scheduled', {
      tenant_id: tenantId,
      booking_id: bookingId,
      tiers: reminderRows.map((r) => r.tier),
    });

    // Poll until a cadence-reminder row flips to 'sent' (the EventBridge→Sender proof).
    let sentRow = null;
    let lastStatuses = [];
    for (let i = 0; i < pollAttempts; i += 1) {
      const rows = (await queryByAppointment(tenantId, bookingId)).filter(isCadenceReminder);
      lastStatuses = rows.map((r) => ({ tier: r.tier, status: r.status }));
      sentRow = rows.find((r) => r.status === 'sent') || null;
      if (sentRow) break;
      if (i < pollAttempts - 1) await sleep(pollIntervalMs);
    }
    if (!sentRow) {
      throw new Error(
        `no reminder dispatched within ${pollAttempts} polls (last statuses=${JSON.stringify(
          lastStatuses
        )}) — EventBridge/Sender lag or breakage`
      );
    }

    log('reminder_cycle_ok', {
      tenant_id: tenantId,
      booking_id: bookingId,
      fired_tier: sentRow.tier,
    });
    await emit('reminder', true);
    return { cycle: 'reminder', success: true, bookingId, firedTier: sentRow.tier };
  } catch (err) {
    warn('reminder_cycle_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
    await emit('reminder', false);
    await alert('Scheduling synthetic: reminder cycle FAILED', { tenantId, bookingId, error: err.message });
    return { cycle: 'reminder', success: false, bookingId, error: err.message };
  } finally {
    // Best-effort cleanup: cancel the synthetic booking so its remaining schedules tear
    // down (S2 lifecycle) and the row is canceled for nightly hygiene. NEVER masks the
    // cycle's own result — a cleanup failure only logs.
    if (tenantId && bookingId && booking && booking.coordinator_email) {
      try {
        await invoke({
          action: 'scheduling_mutate',
          mutation: 'cancel',
          tenantId,
          coordinatorId: booking.coordinator_email,
          bookingId,
          booking,
        });
      } catch (cleanupErr) {
        warn('reminder_cycle_cleanup_failed', { tenant_id: tenantId, booking_id: bookingId, error: cleanupErr.message });
      }
    }
  }
}

module.exports = { runReminderCycle };
