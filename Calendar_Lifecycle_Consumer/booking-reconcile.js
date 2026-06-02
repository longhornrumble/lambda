'use strict';

/**
 * booking-reconcile.js — per-event reconciliation for the three Booking-mutating
 * calendar-lifecycle events (§14.2). Maps each typed envelope to the right
 * `booking-store` conditional write, then handles the §5.1 agent-of-CoR notification
 * narrowing (volunteer-notify stubbed with TODO(Y) — WS-SCHED-FOUNDATIONS owns the
 * notification-dispatch contract; reassigned fires NO platform notification at all).
 *
 *   - reconcileDeleted    (booking.calendar_deleted)    — cancel + TODO(Y) reschedule-link notice.
 *   - reconcileMoved      (booking.calendar_moved, v1)  — cancel + self-anchor + TODO(Y) reschedule path.
 *   - reconcileReassigned (booking.calendar_reassigned) — repoint organizer, NO notify.
 *
 * No PII (coordinator emails) is logged on the normal path — only `tenant_id` / `booking_id`
 * and outcome booleans. The (Y) stubs LOG the intended payload shape (no PII values) so the
 * follow-on workstream can wire dispatch against a concrete contract.
 */

const bookingStore = require('./booking-store');
const { dispatchVolunteerNotice } = require('../shared/scheduling/notify'); // (Y) §B8
const { sign } = require('../shared/scheduling/tokens'); // §13.4 signed reschedule link

const SCHEDULE_BASE_URL = process.env.SCHEDULE_BASE_URL || 'https://schedule.myrecruiter.ai';

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// Require non-empty string fields. Applied ONLY to the fields an action actually needs
// (schema discipline — optional/additive fields are tolerated when absent). A miss throws
// a `malformed`-tagged error so index.js routes the record to the DLQ.
function requireStrings(env, fields) {
  const missing = fields.filter((f) => typeof env[f] !== 'string' || env[f].length === 0);
  if (missing.length) {
    const err = new Error(`envelope missing required field(s): ${missing.join(', ')}`);
    err.malformed = true;
    throw err;
  }
}

// ─── booking.calendar_deleted → cancel + volunteer reschedule-link notice (Y) ────────────

async function reconcileDeleted(env) {
  requireStrings(env, ['tenant_id', 'booking_id']);
  const tenantId = env.tenant_id;
  const bookingId = env.booking_id;

  const canceled = await bookingStore.cancelOnCoordinatorDelete({ tenantId, bookingId });
  if (canceled) {
    log('calendar_deleted_canceled', { tenant_id: tenantId, booking_id: bookingId });
    // (Y) gap C — volunteer cancel-notice with an embedded reschedule link. §5.1 value-add:
    // Google's cancellation email lacks the reschedule link. Best-effort — the cancel
    // (the durable outcome) already succeeded, so a notice failure must NOT redrive.
    await sendCancelNotice(tenantId, bookingId);
  } else {
    log('calendar_deleted_noop', { tenant_id: tenantId, booking_id: bookingId });
  }
}

// Best-effort volunteer cancel-notice (kind 'cancel_notice' with a §13.4 reschedule link).
// Swallows its own errors — the cancellation is the durable outcome; a courtesy notice
// failure must never fail/redrive the reconcile record.
async function sendCancelNotice(tenantId, bookingId) {
  try {
    const ctx = await bookingStore.getNoticeContext({ tenantId, bookingId });
    if (!ctx || !ctx.attendeeEmail) {
      log('notify_skipped_no_attendee_email', { tenant_id: tenantId, booking_id: bookingId });
      return;
    }
    if (!ctx.startAt) {
      // No start_at → sign('reschedule', {start_at: null}) would throw on expiry compute.
      log('notify_skipped_no_start_at', { tenant_id: tenantId, booking_id: bookingId });
      return;
    }
    const token = await sign('reschedule', { tenant_id: tenantId, booking_id: bookingId, start_at: ctx.startAt });
    const rescheduleUrl = `${SCHEDULE_BASE_URL}/reschedule?t=${encodeURIComponent(token)}`;
    const result = await dispatchVolunteerNotice({
      kind: 'cancel_notice',
      tenantId,
      booking: {
        booking_id: bookingId,
        attendee_email: ctx.attendeeEmail,
        attendee_name: ctx.attendeeName,
        reschedule_url: rescheduleUrl, // cancel_notice uses this as the optional rebook link
      },
    });
    log('cancel_notice_dispatched', {
      tenant_id: tenantId, booking_id: bookingId,
      email_dispatched: Boolean(result && result.dispatched && result.dispatched.email),
    });
  } catch (err) {
    warn('cancel_notice_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
  }
}

// ─── booking.calendar_moved (v1 SCOPE) → cancel + self-anchor + reschedule path (Y) ──────

async function reconcileMoved(env) {
  requireStrings(env, ['tenant_id', 'booking_id']);
  const tenantId = env.tenant_id;
  const bookingId = env.booking_id;

  const canceled = await bookingStore.cancelOnCoordinatorMove({ tenantId, bookingId });
  if (canceled) {
    log('calendar_moved_canceled', { tenant_id: tenantId, booking_id: bookingId });
    // (Y) gap C — §5.1/§14.2: email-only volunteers rely on Google's native event-update
    // email (the 'moved' email kind is agent-of-CoR-suppressed in Y); the platform value-add
    // is an OPT-IN SMS with the new time + reschedule link. Y's SMS path is a stub until the
    // sub-phase-E/SMS twin lands, so this dispatch is INERT today (returns {stub:true}, no
    // send) — but the wire is in place. Best-effort: never fails/redrives the reconcile.
    try {
      const result = await dispatchVolunteerNotice({
        kind: 'move_optin_sms',
        tenantId,
        booking: { booking_id: bookingId, new_start_at: env.new_start_at, new_end_at: env.new_end_at },
      });
      log('moved_notice_dispatched', {
        tenant_id: tenantId, booking_id: bookingId,
        sms_stub: Boolean(result && result.dispatched && result.dispatched.sms),
        suppressed: Boolean(result && result.suppressed),
      });
    } catch (err) {
      warn('moved_notice_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
    }
  } else {
    log('calendar_moved_noop', { tenant_id: tenantId, booking_id: bookingId });
  }
}

// ─── booking.calendar_reassigned → repoint organizer, NO notification (§5.1) ─────────────

async function reconcileReassigned(env) {
  requireStrings(env, ['tenant_id', 'booking_id', 'previous_resource_id', 'new_resource_id']);
  const tenantId = env.tenant_id;
  const bookingId = env.booking_id;

  const reassigned = await bookingStore.reassignCoordinator({
    tenantId,
    bookingId,
    previousResourceId: env.previous_resource_id,
    newResourceId: env.new_resource_id,
  });
  // No PII in the log — resource_id values are coordinator emails. NO platform notification
  // (agent-of-CoR §5.1: Google's attendee-update email already covers the reassignment).
  if (reassigned) {
    log('calendar_reassigned_updated', { tenant_id: tenantId, booking_id: bookingId });
  } else {
    log('calendar_reassigned_noop', { tenant_id: tenantId, booking_id: bookingId });
  }
}

module.exports = {
  reconcileDeleted,
  reconcileMoved,
  reconcileReassigned,
  requireStrings,
};
