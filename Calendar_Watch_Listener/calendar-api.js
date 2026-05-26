'use strict';

/**
 * calendar-api.js — Google Calendar API wrapper.
 *
 * Phase 2a (B2 plumbing only — handler does not call this yet; Phase 2b wires it in).
 *
 * `getEvent` returns one of three discriminated statuses so the caller can
 * derive the matching event_type from listener_dispatch_interface.md:
 *
 *   { status: 'found',   event: {...} }
 *   { status: 'deleted', event: null }   // 404 / event.status === 'cancelled'
 *   { status: 'private', event: null }   // 403 OR event.visibility === 'private'
 *
 * Any other API error propagates as-is so the caller can DLQ + alarm.
 *
 * `listChangedEvents` is the events.list+syncToken delta-discovery primitive
 * that Phase 2b will need to map a calendar-level Google push to the specific
 * event_id that changed. The canonical design (§14.2) and listener_dispatch_interface.md
 * speak in terms of `events.get(eventId)` but Google Calendar push notifications
 * identify the calendar, not the event — syncToken-based delta discovery is the
 * standard pattern. Surfaced as Phase 2b design-gap in scheduling plan.
 *
 * NOT exercised by handler in Phase 2a — these are foundation primitives only.
 */

const calendarApi = require('@googleapis/calendar');

const calendar = calendarApi.calendar('v3');

async function getEvent(authClient, calendarId, eventId) {
  if (!authClient || !calendarId || !eventId) {
    throw new Error('authClient, calendarId, and eventId are required');
  }
  try {
    const response = await calendar.events.get({
      auth: authClient,
      calendarId,
      eventId,
    });
    const event = response.data;
    if (event.status === 'cancelled') {
      return { status: 'deleted', event: null };
    }
    if (event.visibility === 'private' || event.visibility === 'confidential') {
      return { status: 'private', event: null };
    }
    return { status: 'found', event };
  } catch (err) {
    const code = err.code ?? err.response?.status;
    if (code === 404 || code === 410) {
      return { status: 'deleted', event: null };
    }
    if (code === 403) {
      return { status: 'private', event: null };
    }
    throw err;
  }
}

async function listChangedEvents(authClient, calendarId, syncToken) {
  if (!authClient || !calendarId) {
    throw new Error('authClient and calendarId are required');
  }
  const params = { auth: authClient, calendarId };
  if (syncToken) {
    params.syncToken = syncToken;
  } else {
    params.showDeleted = true;
    params.singleEvents = true;
  }
  const response = await calendar.events.list(params);
  return {
    events: response.data.items ?? [],
    nextSyncToken: response.data.nextSyncToken ?? null,
    nextPageToken: response.data.nextPageToken ?? null,
  };
}

module.exports = {
  getEvent,
  listChangedEvents,
};
