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
 * Invocation payload (BSH → BCH, or ADA → BCH for the G6 portal actions):
 *   { action:'scheduling_mutate', mutation:'reschedule'|'cancel'|'reschedule_link', tenantId,
 *     coordinatorId, bookingId, booking, newSlot?, reason?, canceled_by? }
 *   - cancel        : delete the calendar event (listener flips status); G6 also persists
 *                     reason/canceled_by (audit-only attribute write — not the status flip).
 *   - reschedule_link: NO calendar op — mint a fresh §B4 reschedule token + email the guest a
 *                     self-serve link (G6 admin/staff "send reschedule link"). short-circuits
 *                     before the OAuth/facade build.
 * Response:
 *   { outcome:'success'|'pending_calendar_sync'|'failed'|'canceled'|..., booking?, sent?, error? }
 */

const calendarEventsModule = require('./calendar-events');
const { resolveProvider } = require('./conference-providers');
const zoomClientModule = require('./zoom-client');
const oauthClientModule = require('./oauth-client');
const bookingStoreModule = require('./booking-store');
const { executeReschedule } = require('../shared/scheduling/reschedule');
const { executeCancel } = require('../shared/scheduling/cancel');
// G6 reschedule_link: mint a fresh §B4 reschedule token + email the guest a self-serve link.
// Reuses the SoT signer (tokens.sign) + notice dispatcher — never re-implemented.
const { sign: signRescheduleToken } = require('../shared/scheduling/tokens');
const { dispatchVolunteerNotice } = require('../shared/scheduling/notify');

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
  const _signRescheduleToken = injected.signRescheduleToken || signRescheduleToken;
  const _dispatchVolunteerNotice = injected.dispatchVolunteerNotice || dispatchVolunteerNotice;
  const logger = injected.logger || console;

  const { mutation, tenantId, coordinatorId, booking } = event;
  if (!tenantId || !coordinatorId || !booking || !mutation) {
    return { outcome: 'failed', error: 'missing_required_fields' };
  }
  if (mutation === 'reschedule' && (!event.newSlot || !event.newSlot.start || !event.newSlot.end)) {
    return { outcome: 'failed', error: 'missing_newSlot' };
  }
  if (mutation !== 'reschedule' && mutation !== 'cancel' && mutation !== 'reschedule_link') {
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
    // reschedule_link is a notify-only action (mint a fresh §B4 token + email the guest a
    // self-serve link) — it performs NO calendar mutation, so it short-circuits BEFORE the
    // per-tenant OAuth/facade build that cancel/reschedule require.
    if (mutation === 'reschedule_link') {
      return await handleRescheduleLink({
        booking,
        tenantId,
        signRescheduleToken: _signRescheduleToken,
        dispatchVolunteerNotice: _dispatchVolunteerNotice,
        logger,
      });
    }
    // Per-tenant Google auth + the §B13 facade (auth curried; BSH cannot build this).
    // NOTE: the calendarId for cancel/reschedule is resolved by cancel.js/reschedule.js
    // from Booking.coordinator_email (persisted by the commit path), NOT re-fetched from
    // the OAuth secret — so this path needs getOAuthClient only (coordinatorId is the
    // secret-path key). Do NOT add getCoordinatorCalendarId here; it would be redundant.
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
      // NOT us (cancel.js deliberately doesn't flip status). We DO persist the audit-only
      // G6 cancel-with-reason fields (an attribute write, never the status flip). Non-fatal:
      // the calendar delete already succeeded; a failed reason write must not undo the cancel.
      if (event.reason) {
        try {
          await bookingStore.updateBookingCancelReason(tenantId, pick(booking, 'bookingId', 'booking_id'), {
            reason: event.reason,
            canceledBy: event.canceled_by,
          });
        } catch (err) {
          (logger.warn || logger.error || (() => {}))('cancel_reason_persist_failed', { error_name: (err && err.name) || 'unknown' });
        }
      }
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

// G6 reschedule_link — notify-only (NO calendar mutation). Mints a fresh §B4 reschedule token
// (MINT is stateless: NO jti write — the jti is written only at REDEEM) and emails the guest a
// self-serve reschedule link. Mirrors Attendance_Disposition_Handler's reschedule-link flow.
async function handleRescheduleLink({ booking, tenantId, signRescheduleToken, dispatchVolunteerNotice, logger }) {
  const bookingId = pick(booking, 'bookingId', 'booking_id');
  const startAt = pick(booking, 'startAt', 'start_at');

  let rescheduleUrl;
  try {
    const token = await signRescheduleToken('reschedule', {
      tenant_id: tenantId,
      booking_id: bookingId,
      start_at: startAt,
    });
    // BCH already sets SCHEDULE_BASE_URL (the staging redemption / WS-D3 domain); reuse it
    // rather than introduce a second base-URL env. Falls back to the prod redemption host.
    const baseUrl = process.env.SCHEDULE_BASE_URL || 'https://schedule.myrecruiter.ai';
    rescheduleUrl = `${baseUrl}/reschedule?t=${encodeURIComponent(token)}`;
  } catch (err) {
    // A token we can't mint (e.g. missing start_at) — surface as a clean failure; no calendar
    // state changed, so nothing to roll back.
    (logger.error || (() => {}))('reschedule_link_mint_failed', { error_name: (err && err.name) || 'unknown' });
    return { outcome: 'failed', error: 'token_mint_failed' };
  }

  // Email the guest the self-serve link (best-effort — notify appends the STOP footer and never
  // throws on a send failure). buildEmailPayload requires booking.rescheduleUrl, supplied here.
  try {
    const result = await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId, booking: { ...booking, rescheduleUrl } },
      {}
    );
    const sent = !!(result && result.dispatched && result.dispatched.email === 'sent');
    return { outcome: 'success', sent };
  } catch (err) {
    // Defensive: dispatch is best-effort and shouldn't throw, but a throw here must not become
    // a Lambda FunctionError — the token was minted; report sent:false.
    (logger.error || (() => {}))('reschedule_link_notify_failed', { error_name: (err && err.name) || 'unknown' });
    return { outcome: 'success', sent: false };
  }
}

module.exports = { handleSchedulingMutate };
