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

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
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
    // TODO(Y) — enqueue a volunteer reschedule-link notice via the WS-SCHED-FOUNDATIONS
    // notification-dispatch contract. §5.1 value-add: Google's cancellation email lacks the
    // reschedule link, so this platform notice is NOT a duplicate of native comms.
    // Intended payload (no contract yet — STUBBED, not sent):
    //   { tenant_id, booking_id, channel: 'volunteer', template: 'coordinator_canceled_reschedule',
    //     token_purpose: 'reschedule' /* §13.4 signed-token link, minted by (Y) */ }
    log('notify_stub_skipped', {
      reason: 'TODO(Y) notification-dispatch contract not built (WS-SCHED-FOUNDATIONS)',
      tenant_id: tenantId, booking_id: bookingId,
      intent: 'volunteer_reschedule_link_notice', token_purpose: 'reschedule',
    });
  } else {
    log('calendar_deleted_noop', { tenant_id: tenantId, booking_id: bookingId });
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
    // TODO(Y) — emit the reschedule path via the WS-SCHED-FOUNDATIONS notification-dispatch
    // contract. §5.1/§14.2: do NOT auto-create the replacement booking (deferred — C8 + a
    // re-pool). Email-only volunteers rely on Google's native event-update email; the (Y)
    // value-add is an opt-in SMS with the new time + a reschedule link.
    // Intended payload (STUBBED, not sent):
    //   { tenant_id, booking_id, channel: 'volunteer_sms_opt_in',
    //     template: 'coordinator_moved_reschedule', token_purpose: 'reschedule',
    //     new_start_at: env.new_start_at, new_end_at: env.new_end_at }
    log('notify_stub_skipped', {
      reason: 'TODO(Y) notification-dispatch contract not built (WS-SCHED-FOUNDATIONS)',
      tenant_id: tenantId, booking_id: bookingId,
      intent: 'volunteer_moved_reschedule_path', token_purpose: 'reschedule',
    });
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
