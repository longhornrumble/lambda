'use strict';

/**
 * Tier-2 calendar-mutation executor (architecture option d — extend
 * Booking_Commit_Handler rather than bundle googleapis into BSH or stand up a new
 * Lambda). Invoked by the Bedrock Streaming Handler via Lambda InvokeCommand AFTER
 * the §B14 boundary has validated the state transition in BSH. BCH is a PURE executor:
 * it does NOT re-run the state machine — it trusts the already-authorized mutation
 * (Lambda-to-Lambda, IAM-enforced, same account).
 *
 * It owns what BSH structurally cannot bundle: the per-tenant Google OAuth client, the
 * §B13 calendar facade (curries authClient into calendar-events), the §B6 conference
 * provider, the §B15 Zoom start-time PATCH, and — for reschedule — the Booking-row
 * write (option A; cancel's Booking.status flip is the §14.2 listener's job).
 *
 * Invocation payload (BSH → BCH):
 *   { action:'scheduling_mutate', mutation:'reschedule'|'cancel', tenantId,
 *     coordinatorId, bookingId, booking, newSlot? }
 * Response:
 *   { outcome:'success'|'pending_calendar_sync'|'failed'|'canceled'|..., booking?, error? }
 */

const calendarEventsModule = require('./calendar-events');
const { resolveProvider } = require('./conference-providers');
const zoomClientModule = require('./zoom-client');
const oauthClientModule = require('./oauth-client');
const bookingStoreModule = require('./booking-store');
const { executeReschedule } = require('../shared/scheduling/reschedule');
const { executeCancel } = require('../shared/scheduling/cancel');

// tolerate camel OR snake on the inbound booking (schema discipline)
function pick(b, camel, snake) {
  if (!b) return undefined;
  return b[camel] != null ? b[camel] : b[snake];
}

async function handleSchedulingMutate(event = {}, injected = {}) {
  const calendarEvents = injected.calendarEvents || calendarEventsModule;
  const zoomClient = injected.zoomClient || zoomClientModule;
  const getOAuthClient = injected.getOAuthClient || oauthClientModule.getOAuthClient;
  const bookingStore = injected.bookingStore || bookingStoreModule;
  const _executeReschedule = injected.executeReschedule || executeReschedule;
  const _executeCancel = injected.executeCancel || executeCancel;
  const _resolveProvider = injected.resolveProvider || resolveProvider;
  const logger = injected.logger || console;

  const { mutation, tenantId, coordinatorId, booking } = event;
  if (!tenantId || !coordinatorId || !booking || !mutation) {
    return { outcome: 'failed', error: 'missing_required_fields' };
  }
  if (mutation === 'reschedule' && (!event.newSlot || !event.newSlot.start || !event.newSlot.end)) {
    return { outcome: 'failed', error: 'missing_newSlot' };
  }
  if (mutation !== 'reschedule' && mutation !== 'cancel') {
    return { outcome: 'failed', error: 'unknown_mutation' };
  }
  // SR-1 (cross-tenant): BCH trusts the BSH payload, so assert the outer tenantId (used for
  // OAuth-secret lookup + the DDB write) matches the booking's own tenant_id (used by
  // executeReschedule for calendar resolution). A divergent payload would auth as tenant A
  // against tenant B's calendar — refuse it. tenant_id is in the §B-projected booking.
  const bookingTenant = pick(booking, 'tenantId', 'tenant_id');
  if (bookingTenant && bookingTenant !== tenantId) {
    (injected.logger || console).error('scheduling_mutate_tenant_mismatch', { mutation });
    return { outcome: 'failed', error: 'tenant_mismatch' };
  }

  // An UNEXPECTED throw (bad OAuth secret, Google/Zoom client error, etc.) becomes a
  // clean { outcome:'failed' } the BSH email-fallback already handles — NOT a Lambda
  // FunctionError (which would trip the Errors alarm). A calendar-op that fails the
  // normal way already returns outcome:'failed'/'pending_calendar_sync' from execute*.
  try {
    // Per-tenant Google auth + the §B13 facade (auth curried; BSH cannot build this).
    const authClient = await getOAuthClient({ tenantId, coordinatorId });
    const calendar = {
      buildEventBody: calendarEvents.buildEventBody,
      insertEvent: (calId, body) => calendarEvents.insertEvent(authClient, calId, body),
      deleteEvent: (calId, eid) => calendarEvents.deleteEvent(authClient, calId, eid),
      extractMeetJoinUrl: calendarEvents.extractMeetJoinUrl,
    };

    if (mutation === 'cancel') {
      const result = await _executeCancel({ booking, deps: { calendar, logger } });
      // §14.2: the cal-lifecycle listener flips Booking.status on the calendar delete —
      // NOT us (cancel.js deliberately doesn't flip status). No Booking write here.
      return { outcome: result.outcome, booking: result.booking };
    }

    // reschedule
    const { newSlot } = event;
    const conferenceType = pick(booking, 'conferenceProvider', 'conference_provider') || 'google_meet';
    const conference = _resolveProvider(conferenceType, { zoomClient });
    const result = await _executeReschedule({ booking, newSlot, deps: { calendar, conference, logger } });

    if (result.outcome === 'success' || result.outcome === 'pending_calendar_sync') {
      const b = result.booking || booking;
      // §B15: PATCH the reused Zoom meeting's start time (join URL already preserved by
      // reschedule.js). Non-fatal — the move already happened; a stale Zoom time recovers.
      // Explicit-provider check only (no numeric-id heuristic like BSH's zoomMeetingIdOf):
      // every executor-path booking carries an explicit conference_provider
      // (booking-store writes 'zoom'|'google_meet'|'null'), so the legacy unset-provider
      // numeric fallback can't occur here.
      const provider = pick(b, 'conferenceProvider', 'conference_provider');
      const confId = pick(b, 'conferenceId', 'conference_id');
      if (provider && String(provider).toLowerCase() === 'zoom' && confId) {
        try {
          await zoomClient.updateMeeting({
            tenantId, meetingId: String(confId),
            start: newSlot.start, end: newSlot.end,
            timezone: pick(b, 'timeZone', 'timezone'),
          });
        } catch (err) {
          (logger.warn || logger.error || (() => {}))('zoom_update_failed', { error_name: (err && err.name) || 'unknown' });
        }
      }
      // Option A: persist the reschedule fields (cancel is listener-owned). Non-fatal —
      // the calendar is the source of truth + the §14.2 listener is the backstop.
      try {
        await bookingStore.updateBookingReschedule(tenantId, pick(b, 'bookingId', 'booking_id'), {
          startAt: newSlot.start,
          externalEventId: pick(b, 'externalEventId', 'external_event_id'),
          pendingCalendarSync: result.outcome === 'pending_calendar_sync',
        });
      } catch (err) {
        (logger.warn || logger.error || (() => {}))('booking_persist_failed', { error_name: (err && err.name) || 'unknown' });
      }
    }
    return { outcome: result.outcome, booking: result.booking };
  } catch (err) {
    (logger.error || (() => {}))('scheduling_mutate_failed', { mutation, error_name: (err && err.name) || 'unknown' });
    return { outcome: 'failed', error: 'executor_error' };
  }
}

module.exports = { handleSchedulingMutate };
