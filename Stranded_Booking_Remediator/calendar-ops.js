'use strict';

/**
 * calendar-ops.js — Google Calendar mutations for the B11 stranded-booking remediator.
 *
 * Two calendar operations, one per stranded-booking handling that touches Google:
 *
 *   transferEvent — handling (a) "reassign": move the booking's calendar event from
 *                   the departed coordinator's calendar to the new coordinator's via
 *                   events.move (canonical §7.3 "transfer the calendar event via
 *                   Google API"). sendUpdates:'all' so the volunteer receives Google's
 *                   standard attendee-update email — the design's chosen notification
 *                   for reassign ("no platform notification needed"). The
 *                   extendedProperties.private.booking_id ownership tag (FROZEN §A)
 *                   travels with the event through a move, so the B2 listener still
 *                   attributes future changes to the same Booking.
 *
 *   deleteEvent   — handling (b) "cancel": delete the calendar event. Deleting the
 *                   event is what TRIGGERS the §14.2 cancellation path (the B2 listener
 *                   sees the deletion → the C-phase cancellation consumer transitions
 *                   Booking.status = canceled and sends the volunteer the reschedule
 *                   link). B11 therefore does NOT write Booking.status itself for a
 *                   cancel — that would fork the §14.2 contract. sendUpdates:'none' so
 *                   Google's generic cancellation email does not race the platform's
 *                   reschedule-link notification (the platform owns that message).
 *                   404/410 ⇒ already gone ⇒ idempotent success.
 *
 * This module is WRITE-side; it never reads calendar bodies (that is the listener's
 * read-only calendar-api.js). Distinct copy per Lambda so B11 bundles against its own
 * dedicated execution role (CLAUDE.md never-share-roles rule).
 */

const calendarApi = require('@googleapis/calendar');

const calendar = calendarApi.calendar('v3');

function isAlreadyGone(err) {
  const code = err?.code ?? err?.response?.status;
  return code === 404 || code === 410 || code === '404' || code === '410';
}

// Normalize a raw Google API error to a {code}-only Error. Raw google-auth /
// @googleapis errors embed the calendarId (= a coordinator's email) in `.message`,
// and that flows to warn() + the handler's failed[] — a PII leak. We re-throw a
// redacted Error that keeps ONLY the HTTP status code on `.code`, so callers can
// still classify (already-gone vs 401/403) without the email ever reaching a log.
function redactCalendarError(operation, err) {
  const code = err?.code ?? err?.response?.status ?? 'unknown';
  const redacted = new Error(`calendar ${operation} failed (code=${code})`);
  redacted.code = code;
  return redacted;
}

/**
 * Move the event to a different coordinator's calendar (handling (a) reassign).
 *   { eventId, fromCalendarId, toCalendarId } — calendar ids are the coordinators'
 *   calendar emails (C8 inserts with calendarId = coordinator email).
 * Returns the moved event resource. Errors are re-thrown REDACTED (code-only): a
 * 404/410 means the source event is already gone — reassign() classifies that via
 * isAlreadyGone and falls through to the cancel handling; a 401/403 (revoked/denied
 * token) keeps its code so reassign() propagates it to failed[].
 */
async function transferEvent(authClient, { eventId, fromCalendarId, toCalendarId }) {
  if (!authClient || !eventId || !fromCalendarId || !toCalendarId) {
    throw new Error('authClient, eventId, fromCalendarId, and toCalendarId are required');
  }
  try {
    const response = await calendar.events.move({
      auth: authClient,
      calendarId: fromCalendarId,
      eventId,
      destination: toCalendarId,
      sendUpdates: 'all',
    });
    return response.data;
  } catch (err) {
    throw redactCalendarError('move', err);
  }
}

/**
 * Delete the event (handling (b) cancel). 404/410 ⇒ already gone ⇒ idempotent
 * success (matches the C8 compensating-delete posture). sendUpdates:'none' — the
 * platform's §14.2 reschedule-link email is the single volunteer notification.
 */
async function deleteEvent(authClient, { eventId, calendarId }) {
  if (!authClient || !eventId || !calendarId) {
    throw new Error('authClient, eventId, and calendarId are required');
  }
  try {
    await calendar.events.delete({
      auth: authClient,
      calendarId,
      eventId,
      sendUpdates: 'none',
    });
  } catch (err) {
    if (isAlreadyGone(err)) return; // already gone — the end state we want
    throw redactCalendarError('delete', err); // redact: raw error embeds calendarId (PII)
  }
}

module.exports = {
  transferEvent,
  deleteEvent,
  isAlreadyGone,
  redactCalendarError,
};
