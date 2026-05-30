'use strict';

/**
 * calendar-watch.js — Google Calendar watch-channel helpers (Offboarder copy).
 *
 * Scheduling sub-phase B Task B6. The Offboarder only ever REVOKES channels, so
 * this copy carries `stopWatch` alone — `registerWatch` / `seedInitialSyncToken`
 * (present in the Onboarder/Renewer copies) are intentionally omitted.
 *
 * NOTE (Layer extraction overdue): this is the 4th consumer of these
 * Google-Calendar helpers (Listener, Onboarder, Renewer, Offboarder). Per the
 * tech-lead recommendation recorded in the B3/B5 handoffs, oauth-client.js +
 * these wrappers should be extracted to a shared Lambda Layer rather than copied
 * a 4th time. Duplicated here deliberately to keep B6 surgical (no change to the
 * already-shipped Listener/Onboarder/Renewer bundles); Layer extraction stays a
 * B-phase follow-up.
 *
 * `stopWatch` wraps channels.stop. Google returns 204 on success. A 404/410
 * means the channel is already gone (expired or previously stopped) — the
 * Offboarder treats that as success-equivalent (see index.js). channels.stop
 * needs BOTH the channel id and the resourceId Google returned at watch time.
 */

const calendarApi = require('@googleapis/calendar');

const calendar = calendarApi.calendar('v3');

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
  stopWatch,
};
