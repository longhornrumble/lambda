'use strict';

/**
 * calendar-watch.js — Google Calendar onboarding helpers.
 *
 * Used by the B5 onboarding Lambda only. The Listener (B2) and Renewer (B3)
 * read from the watch channel; only the onboarding/renewal path creates one.
 *
 * `registerWatch` wraps events.watch. Successful response shape:
 *   { kind, id, resourceId, resourceUri, token?, expiration }
 * `expiration` is a millisecond-epoch string. Channels max out around 7 days
 * (Google may pick less); B3 Renewer watches `tenant-expiration-index` GSI
 * and re-watches before expiry.
 *
 * `stopWatch` wraps channels.stop — used by the Onboarder to revoke a channel
 * it just created when a downstream step (DDB write) fails, so a live Google
 * channel is never stranded without an authenticating DDB row.
 *
 * `seedInitialSyncToken` pages events.list until `nextSyncToken` appears.
 * Google only emits `nextSyncToken` on the final page; for any calendar with
 * more than one page of events, pagination is required. Bounded by `maxPages`
 * so a runaway calendar can't OOM the Lambda. `singleEvents` is left false:
 * the sync token is returned regardless, and expanding recurring events into
 * instances would needlessly multiply the page count on long-lived calendars.
 */

const calendarApi = require('@googleapis/calendar');

const calendar = calendarApi.calendar('v3');

async function registerWatch(authClient, calendarId, channelId, channelToken, listenerUrl) {
  if (!authClient || !calendarId || !channelId || !channelToken || !listenerUrl) {
    throw new Error('authClient, calendarId, channelId, channelToken, and listenerUrl are required');
  }
  const response = await calendar.events.watch({
    auth: authClient,
    calendarId,
    requestBody: {
      id: channelId,
      type: 'web_hook',
      address: listenerUrl,
      token: channelToken,
    },
  });
  return {
    resourceId: response.data.resourceId ?? null,
    resourceUri: response.data.resourceUri ?? null,
    expiration: response.data.expiration ?? null,
  };
}

async function stopWatch(authClient, channelId, resourceId) {
  if (!authClient || !channelId || !resourceId) {
    throw new Error('authClient, channelId, and resourceId are required');
  }
  await calendar.channels.stop({
    auth: authClient,
    requestBody: {
      id: channelId,
      resourceId,
    },
  });
}

async function seedInitialSyncToken(authClient, calendarId, maxPages = 50) {
  if (!authClient || !calendarId) {
    throw new Error('authClient and calendarId are required');
  }
  let pageToken = null;
  let pages = 0;
  let totalSeen = 0;
  while (pages < maxPages) {
    const params = {
      auth: authClient,
      calendarId,
      showDeleted: true,
      singleEvents: false,
    };
    if (pageToken) {
      params.pageToken = pageToken;
    }
    const response = await calendar.events.list(params);
    pages += 1;
    totalSeen += (response.data.items ?? []).length;
    if (response.data.nextSyncToken) {
      return { syncToken: response.data.nextSyncToken, pages, totalSeen };
    }
    pageToken = response.data.nextPageToken ?? null;
    if (!pageToken) {
      // No more pages but also no sync token — degenerate response from Google.
      return { syncToken: null, pages, totalSeen };
    }
  }
  throw new Error(`Initial sync-token seed exceeded maxPages=${maxPages}`);
}

module.exports = {
  registerWatch,
  stopWatch,
  seedInitialSyncToken,
};
