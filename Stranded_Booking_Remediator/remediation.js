'use strict';

/**
 * remediation.js — the three stranded-booking handlings + the default cascade
 * (canonical §7.3), as pure-orchestration callable operations.
 *
 * Every external effect is an INJECTED collaborator (deps), so each handling is unit-
 * testable with DDB-seeded fixtures + mocked Google — exactly the coverage the plan
 * B11 done-bar requires ("integration test uses DDB-seeded fixture data, not real
 * bookings; C's exit criteria re-test against the real C8 write path").
 *
 * deps = {
 *   resolveAlternate(booking) → { resourceId, coordinatorEmail } | null,
 *   getOAuthClient({ tenantId, coordinatorId }) → authClient,
 *   calendarOps: { transferEvent, deleteEvent },
 *   bookingStore: { reassignBookingResource, isConditionalCheckFailed },
 *   now() → ISO8601 string,   // injected so reassign's mutation timestamp is testable
 *   log, warn,
 * }
 *
 * The three handlings (canonical §7.3):
 *   (a) reassign — re-run routing for an alternate eligible coordinator free at the
 *       same slot; if found, transfer the calendar event + repoint the Booking row.
 *       No platform notification — the volunteer sees Google's attendee-update email.
 *   (b) cancel   — delete the calendar event, which TRIGGERS the §14.2 cancellation
 *       path (the listener + C-phase consumer set Booking.status = canceled and send
 *       the reschedule link). B11 does NOT write status itself — that would fork §14.2.
 *   (c) leave    — no-op; the event fires as scheduled (amicable departure).
 *
 * Default cascade (no admin choice) = (a) → (b): try reassign; if no eligible
 * coordinator, cancel. Try the lowest-blast-radius option first.
 *
 * Each handling returns a result object whose `outcome` is one of:
 *   'reassigned' | 'no_eligible_coordinator' | 'canceled' | 'left'
 */

const OUTCOMES = Object.freeze({
  REASSIGNED: 'reassigned',
  NO_ELIGIBLE: 'no_eligible_coordinator',
  CANCELED: 'canceled',
  LEFT: 'left',
});

// (a) Reassign via re-run routing.
async function reassign(booking, deps) {
  const alternate = await deps.resolveAlternate(booking);
  if (!alternate) {
    return { outcome: OUTCOMES.NO_ELIGIBLE };
  }

  if (!booking.externalEventId) {
    // No calendar event to move (malformed/partial booking). Can't reassign; signal
    // so the cascade falls through to cancel rather than claiming a phantom transfer.
    deps.warn('reassign_no_external_event', { booking_id: booking.bookingId });
    return { outcome: OUTCOMES.NO_ELIGIBLE };
  }

  // The move is performed with the DEPARTED coordinator's OAuth client — it owns the
  // source calendar the event currently lives on. (Cross-calendar move auth within one
  // Workspace domain is the v1 pilot assumption; flagged in the PR for the integrator.)
  const authClient = await deps.getOAuthClient({
    tenantId: booking.tenantId,
    coordinatorId: booking.coordinatorEmail,
  });
  try {
    await deps.calendarOps.transferEvent(authClient, {
      eventId: booking.externalEventId,
      fromCalendarId: booking.coordinatorEmail,
      toCalendarId: alternate.coordinatorEmail,
    });
  } catch (err) {
    // The source event vanished (404/410) between detection and transfer — it can't
    // be reassigned, so fall through to the cancel handling (NO_ELIGIBLE drives the
    // default cascade to (b)). A genuine 401/403 (revoked/denied token) is NOT
    // already-gone → propagate so the booking lands in failed[] for admin attention.
    if (deps.calendarOps.isAlreadyGone(err)) {
      deps.warn('reassign_event_already_gone', { booking_id: booking.bookingId });
      return { outcome: OUTCOMES.NO_ELIGIBLE };
    }
    throw err;
  }

  // Repoint the Booking row. Conditional (still booked + still the departed resource);
  // a ConditionalCheckFailed means newer state already won — treat as already-handled,
  // not a failure (the calendar event is already moved, which is the source of truth).
  try {
    await deps.bookingStore.reassignBookingResource({
      tenantId: booking.tenantId,
      bookingId: booking.bookingId,
      fromResourceId: booking.resourceId,
      newResourceId: alternate.resourceId,
      newCoordinatorEmail: alternate.coordinatorEmail,
      mutationAt: deps.now(),
    });
  } catch (err) {
    if (!deps.bookingStore.isConditionalCheckFailed(err)) throw err;
    deps.warn('reassign_booking_row_already_changed', { booking_id: booking.bookingId });
  }

  return {
    outcome: OUTCOMES.REASSIGNED,
    newResourceId: alternate.resourceId,
    newCoordinatorEmail: alternate.coordinatorEmail,
  };
}

// (b) Treat as coordinator-side cancel: delete the calendar event → §14.2 path.
async function cancel(booking, deps) {
  if (!booking.externalEventId) {
    // Nothing on the calendar to delete; the §14.2 path can't be triggered. Surface
    // as canceled-equivalent (no event = no active calendar commitment) so the
    // offboarding flow doesn't strand it, but flag it for ops visibility.
    deps.warn('cancel_no_external_event', { booking_id: booking.bookingId });
    return { outcome: OUTCOMES.CANCELED, note: 'no_calendar_event' };
  }
  const authClient = await deps.getOAuthClient({
    tenantId: booking.tenantId,
    coordinatorId: booking.coordinatorEmail,
  });
  await deps.calendarOps.deleteEvent(authClient, {
    eventId: booking.externalEventId,
    calendarId: booking.coordinatorEmail,
  });
  // Deliberately no Booking.status write here — deleting the event triggers the §14.2
  // cancellation path (B2 listener → C-phase consumer), which owns the status
  // transition + the volunteer reschedule-link notification. Forking that would
  // double-write status and double-notify.
  return { outcome: OUTCOMES.CANCELED };
}

// (c) Leave the booking — amicable departure; coordinator honors the commitment.
async function leave(booking) {
  return { outcome: OUTCOMES.LEFT, bookingId: booking.bookingId };
}

/**
 * Apply a handling to one booking. choice ∈ {'reassign','cancel','leave'} or null/undefined
 * for the default cascade (a)→(b).
 */
async function remediate(booking, choice, deps) {
  switch (choice) {
    case 'reassign':
      return reassign(booking, deps);
    case 'cancel':
      return cancel(booking, deps);
    case 'leave':
      return leave(booking, deps);
    case undefined:
    case null:
    case 'cascade': {
      // Default cascade: (a) reassign, fall back to (b) cancel when no eligible coord.
      const reassignResult = await reassign(booking, deps);
      if (reassignResult.outcome === OUTCOMES.NO_ELIGIBLE) {
        const cancelResult = await cancel(booking, deps);
        return { ...cancelResult, cascadedFrom: 'reassign' };
      }
      return reassignResult;
    }
    default:
      throw new Error(`unknown remediation choice: ${choice}`);
  }
}

module.exports = {
  reassign,
  cancel,
  leave,
  remediate,
  OUTCOMES,
};
