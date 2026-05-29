'use strict';

/**
 * calendar-watch.js — Google Calendar watch-channel helpers (Renewer copy).
 *
 * Scheduling sub-phase B Task B3. Duplicated from Calendar_Watch_Onboarder
 * (registerWatch + stopWatch only — the Renewer carries the existing
 * last_sync_token forward and does NOT re-seed, so seedInitialSyncToken is
 * intentionally omitted here).
 *
 * NOTE (Layer extraction now due): the Renewer is the 3rd consumer of these
 * Google-Calendar helpers (Listener, Onboarder, Renewer). Per the tech-lead
 * recommendation recorded in the B5 handoff, oauth-client.js + these wrappers
 * should be extracted to a shared Lambda Layer rather than copied a 3rd time.
 * Duplicated here deliberately to keep B3 surgical (no change to the shipped
 * Listener/Onboarder bundles); Layer extraction tracked as a B-phase follow-up.
 *
 * `registerWatch` wraps events.watch. Successful response shape:
 *   { resourceId, resourceUri, expiration }
 * `expiration` is a millisecond-epoch string; Google caps channels around 7
 * days (may pick less). The Renewer re-watches before that expiry.
 *
 * `stopWatch` wraps channels.stop — used by the Renewer to (a) revoke a
 * freshly-created channel when the subsequent DDB write fails (so no live
 * Google channel is stranded without an authenticating row) and (b) revoke
 * the old channel once its replacement is live.
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

module.exports = {
  registerWatch,
  stopWatch,
};
