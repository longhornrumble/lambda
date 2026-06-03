'use strict';

/**
 * calendarFacade.js — auth-bound Google Calendar facade (FROZEN_CONTRACTS §B13, §B9).
 *
 * A thin wrapper over Booking_Commit_Handler/calendar-events.js (whose insertEvent /
 * deleteEvent take `authClient` FIRST) that curries the per-(tenant, coordinator) OAuth
 * client in via Booking_Commit_Handler/oauth-client.js getOAuthClient({tenantId,coordinatorId}).
 *
 * The integrator builds ONE facade per conversation turn and injects it as the §B9
 * `deps.calendar` consumed (unchanged) by the in-chat reschedule/cancel flow + C8.
 *
 * Security (per the WS-D7 #203 Security pass): the OAuth client is bound to the
 * tenantId/coordinatorId this facade was built with — it is NEVER caller-supplied. The
 * callers pass only a calendarId/requestBody/eventId, so a mis-passed calendarId can
 * only ever reach a calendar this coordinator's grant already permits (Google rejects
 * the rest). No process-level / fixed-tenant client; auth is resolved fresh per op from
 * the bound identity (oauth-client.js owns the 50-min token cache). Errors surface
 * Google's own PII-free messages — this module embeds no attendee PII.
 */

function buildCalendarFacade({ tenantId, coordinatorId, deps }) {
  if (!tenantId || !coordinatorId) {
    throw new Error('buildCalendarFacade requires tenantId and coordinatorId');
  }
  if (!deps || typeof deps.getOAuthClient !== 'function' || !deps.calendarEvents) {
    throw new Error('buildCalendarFacade requires deps.getOAuthClient and deps.calendarEvents');
  }
  const { getOAuthClient, calendarEvents } = deps;

  // Auth is resolved from the BOUND (tenantId, coordinatorId) — never a caller arg.
  // A facade built for tenant A can never produce tenant-B auth.
  const authClient = () => getOAuthClient({ tenantId, coordinatorId });

  return {
    // §5.7-compliant request body + ownership tag. No auth needed — pure pass-through.
    buildEventBody(params) {
      return calendarEvents.buildEventBody(params);
    },

    // authClient curried in (calendar-events takes it FIRST). Returns the inserted event.
    async insertEvent(calendarId, requestBody) {
      return calendarEvents.insertEvent(await authClient(), calendarId, requestBody);
    },

    // authClient curried in. Idempotent (404/410 resolves) — inherited from calendar-events.
    async deleteEvent(calendarId, eventId) {
      return calendarEvents.deleteEvent(await authClient(), calendarId, eventId);
    },

    // Pull the Meet join URL out of an inserted event. No auth needed — pure pass-through.
    extractMeetJoinUrl(event) {
      return calendarEvents.extractMeetJoinUrl(event);
    },
  };
}

module.exports = { buildCalendarFacade };
