'use strict';

/**
 * calendar-api.js — Google Calendar API wrapper.
 *
 * Both `getEvent` and `listChangedEvents` are wired into and exercised by the
 * Phase 2b handler (processDelta calls listChangedEvents for delta discovery;
 * getEvent is available for single-event resolution when a caller has an eventId).
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
 * `listChangedEvents` is the events.list+syncToken delta-discovery primitive used
 * by Phase 2b to map a calendar-level Google push to the specific events that changed.
 * Google Calendar push notifications identify the calendar, not the event; syncToken-based
 * delta discovery is the standard pattern (see §14.2 of the scheduling plan).
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

// pageToken is optional: when provided it continues a paginated incremental sync.
// syncToken and pageToken are mutually exclusive in the Google API; when paging
// through a large initial list the caller passes the nextPageToken from the
// prior page and omits the syncToken on continuation calls.
async function listChangedEvents(authClient, calendarId, syncToken, pageToken) {
  if (!authClient || !calendarId) {
    throw new Error('authClient and calendarId are required');
  }
  // singleEvents MUST be false on EVERY path (sub-phase B audit row code#2,
  // 2026-05-30). The Onboarder's seedInitialSyncToken mints the token with
  // singleEvents:false (recurring masters, not expanded instances — a deliberate
  // page-count choice). A syncToken can only be continued/refreshed in the SAME
  // singleEvents mode it was created in; mixing true here (the prior code on the
  // pageToken + full-list paths) against a false-mode token makes Google return a
  // permanent 410 on any calendar with paginated recurring-event history. Keep all
  // three branches false so the seed, the incremental sync, its continuation pages,
  // and the 410-recovery re-seed are all the same mode.
  const params = { auth: authClient, calendarId, singleEvents: false };
  if (pageToken) {
    // Continuation page — no syncToken; just carry the pageToken.
    params.pageToken = pageToken;
    params.showDeleted = true;
  } else if (syncToken) {
    params.syncToken = syncToken;
  } else {
    params.showDeleted = true;
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
