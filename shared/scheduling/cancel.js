'use strict';

/**
 * cancel.js — (§B9) cancel execution module (WS-D7).
 *
 * Canonical §9.4 (cancel-through-calendar, single-path, agent-of-Calendar-of-Record
 * §5.1); frozen contract FROZEN_CONTRACTS.md §B9. This is the cancel that runs
 * IN-CHAT after the volunteer confirms — NOT at the redemption endpoint. The signed
 * token only authenticated ENTRY (§13.4); WS-D4 owns token validation + the jti
 * write, so this module performs NO token validation and NO jti write.
 *
 *   executeCancel({ booking, deps }) → { outcome: 'deleted' | 'pending_calendar_sync', booking }
 *
 * ── Single-path through Google events.delete (§9.4) ──
 *   The cancel is exactly one calendar mutation: delete the platform-owned event.
 *   On success → outcome 'deleted'.
 *
 * ── The status flip is NOT ours (the crux, §9.4 "one transition, one notification") ──
 *   This module does NOT write Booking.status, cancel_reason, or dispatch the notice.
 *   The §14.2 listener — the ALREADY-BUILT cal-lifecycle consumer
 *   (Calendar_Lifecycle_Consumer, lambda#196) — picks up the `calendar_deleted` push,
 *   flips Booking.status=canceled (+ cancel_reason) and dispatches the volunteer
 *   notice. A second status writer here would race that single source of truth. This
 *   module's job ends at the calendar delete; it persists NOTHING (no ddb write) and
 *   returns the updated booking for the caller to persist.
 *
 * ── API-unreachable failure path (§9.4) ──
 *   If the delete throws (Google Calendar unreachable / transient), set
 *   booking.pending_calendar_sync = true and return outcome 'pending_calendar_sync'.
 *   The E9 reconciler retries events.delete until it succeeds, after which the listener
 *   catches the eventual deletion. The frozen §B9 contract has exactly TWO outcomes —
 *   there is no 'failed': any thrown delete is the retry-able pending path.
 *
 * ── DI seam ──
 *   deps = { calendar, ddb, logger } — INJECTED; no module-level AWS/Google clients.
 *     • deps.calendar.deleteEvent(calendarId, eventId) → resolves on success (idempotent:
 *       an already-deleted event / 404 / 410 resolves, it does NOT throw), throws when the
 *       Google API is unreachable. This module resolves calendarId (coordinator_email) +
 *       eventId (external_event_id) from the booking and passes them; the §B13 facade
 *       (buildCalendarFacade, WS-FACADE) curries the per-(tenant,coordinator) auth in.
 *       (§C reconciliation 2026-06-03: §B9/§B13 standardized on the TWO-arg shape that
 *       WS-D6 reschedule.js + §B13 already use; cancel.js re-synced to match. NOT a fork.)
 *     • deps.ddb is accepted for §B9 signature symmetry with reschedule.js but is
 *       intentionally UNUSED here — this module persists nothing (the caller does).
 *     • deps.logger?.info/.warn — optional; logs booking_id + outcome ONLY, never the
 *       attendee email/phone/name (PII discipline).
 */

// Forward-compatible read (schema discipline): accept either the in-memory camelCase
// booking or a DDB snake_case row; a missing field reads as undefined, never crashes.
function pick(booking, camel, snake) {
  if (!booking) return undefined;
  return booking[camel] != null ? booking[camel] : booking[snake];
}

async function executeCancel({ booking, deps } = {}) {
  // Caller-contract guards (programmer errors, distinct from the API-unreachable path):
  // these throw rather than returning an outcome — a malformed call is not a
  // retry-able pending sync.
  if (!booking) {
    throw new Error('executeCancel: booking is required');
  }
  const calendar = deps && deps.calendar;
  if (!calendar || typeof calendar.deleteEvent !== 'function') {
    throw new Error('executeCancel: deps.calendar.deleteEvent is required');
  }
  // The Google event id (§A booking non-key attr `external_event_id`) is REQUIRED to
  // delete — without a platform-owned event there is no single-path cancel for the
  // listener to react to. Absence is a malformed booking (caller bug), not pending.
  const externalEventId = pick(booking, 'externalEventId', 'external_event_id');
  if (!externalEventId) {
    throw new Error('executeCancel: booking.external_event_id is required');
  }
  // The calendar id = the coordinator's email (v1: resourceId == coordinatorEmail == cal id, §B7).
  const calendarId =
    pick(booking, 'coordinatorEmail', 'coordinator_email') ||
    pick(booking, 'resourceId', 'resource_id');
  if (!calendarId) {
    throw new Error('executeCancel: booking.coordinator_email is required');
  }

  const logger = deps.logger;
  const bookingId = pick(booking, 'bookingId', 'booking_id');

  try {
    // Single-path: delete the platform-owned calendar event. The facade resolves an
    // already-deleted event idempotently (no throw); the §14.2 listener will see the
    // deletion and own the status flip + notice.
    await calendar.deleteEvent(calendarId, externalEventId);
    if (logger && typeof logger.info === 'function') {
      logger.info('executeCancel: calendar event deleted', { booking_id: bookingId, outcome: 'deleted' });
    }
    return { outcome: 'deleted', booking };
  } catch (err) {
    // API-unreachable / transient: flag for the E9 reconciler to retry. Do NOT flip
    // status here — the listener still owns that once the eventual delete lands.
    if (logger && typeof logger.warn === 'function') {
      // SR-2 (S-4): log a non-PII discriminator, NOT err.message — Google errors embed the
      // calendar id (coordinator email): "Calendar 'maya@org' not found". Mirrors reschedule.js.
      logger.warn('executeCancel: calendar delete failed, marking pending_calendar_sync', {
        booking_id: bookingId,
        outcome: 'pending_calendar_sync',
        error: (err && (err.code || err.name)) || 'error',
      });
    }
    const updated = { ...booking, pending_calendar_sync: true };
    return { outcome: 'pending_calendar_sync', booking: updated };
  }
}

module.exports = { executeCancel };
