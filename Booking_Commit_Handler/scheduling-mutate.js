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
 *   { outcome:'success'|'deleted'|'pending_calendar_sync'|'failed'|'rate_limited', booking?, sent?, error? }
 *   (executeCancel emits 'deleted'|'pending_calendar_sync'; reschedule emits 'success'|'pending_calendar_sync';
 *    reschedule_link emits 'success'|'failed'|'rate_limited'. There is NO 'canceled' outcome.)
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
// G7b: SMS is the opt-in supplement on the reschedule-link notice. selectChannels (§E3 TCPA
// gate, PURE) decides email-floor + consent/quiet-hours-gated SMS; readSmsConsent is the
// consent read it needs. Both fail-closed → email always sends, SMS only when truly permitted.
const { selectChannels } = require('../shared/scheduling/channels');
const { readSmsConsent } = require('./sms-consent');
// Track 1 (§E1): an in-chat reschedule mutates start_at in place (same booking_id) → re-bind
// the reminder schedules to the NEW time. The ONLY re-bind trigger (a calendar move is a
// cancel, handled by the cal-lifecycle consumer).
const reminderScheduler = require('../Reminder_Scheduler/scheduler');
// G1: mint the rebound reminders' one-tap action links for the NEW start_at (same per-purpose
// token contract + URL format as the commit-path confirmation email).
const { buildActionLinks } = require('./confirmation-email');

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
  const _selectChannels = injected.selectChannels || selectChannels;
  const _readSmsConsent = injected.readSmsConsent || readSmsConsent;
  const _rebindReminders = injected.rebindReminders || reminderScheduler.rebindReminders;
  const _buildActionLinks = injected.buildActionLinks || buildActionLinks;
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
  // STRICT: refuse if the booking's own tenant is ABSENT or mismatched (defense-in-depth — a
  // booking row missing tenantId must not silently bypass the cross-tenant guard). tenantId is
  // the PK on every real booking row + the §B-projected booking, so absence = a malformed call.
  const bookingTenant = pick(booking, 'tenantId', 'tenant_id');
  if (!bookingTenant || bookingTenant !== tenantId) {
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
        // G7b: ADA resolves the tenant's org-level SMS toggle (notificationPrefs.sms) and
        // passes it; absent/false → SMS suppressed (email-only), the safe default.
        orgSmsEnabled: event.org_sms_enabled === true,
        bookingStore,
        signRescheduleToken: _signRescheduleToken,
        dispatchVolunteerNotice: _dispatchVolunteerNotice,
        selectChannels: _selectChannels,
        readSmsConsent: _readSmsConsent,
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
          // Log the booking_id so an operator can backfill the lost attribution (the calendar
          // delete + the listener status-flip already succeeded; only the audit field is missing).
          (logger.warn || logger.error || (() => {}))('cancel_reason_persist_failed', {
            booking_id: pick(booking, 'bookingId', 'booking_id'),
            error_name: (err && err.name) || 'unknown',
          });
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
      // Track 1 (§E1): re-bind reminders to the NEW start_at. Best-effort — the calendar +
      // §14.2 listener remain the source of truth; a rebind failure must not undo the move.
      // Build the view from the NEW slot explicitly (do not trust result.booking to carry it).
      // tenantPrefs.org SMS = the event-passed org flag (ADA-resolved, mirrors reschedule_link).
      try {
        // G1: mint fresh action links for the NEW start_at (best-effort — a mint failure just
        // omits the links; the rebound reminders still carry time + join). join link rides the
        // reused conference (preserved by reschedule.js) → carry it on the rebind view.
        let rescheduleUrl = '';
        let cancelUrl = '';
        try {
          ({ rescheduleUrl, cancelUrl } = await _buildActionLinks(
            {
              tenantId,
              bookingId: pick(b, 'bookingId', 'booking_id'),
              startAt: newSlot.start,
              cancellationWindowHours: event.cancellation_window_hours || 0,
            },
            injected.signOpts
          ));
        } catch (linkErr) {
          (logger.warn || logger.error || (() => {}))('reminder_link_mint_failed', { error_name: (linkErr && linkErr.name) || 'unknown' });
        }
        await _rebindReminders({
          booking: {
            tenant_id: tenantId,
            booking_id: pick(b, 'bookingId', 'booking_id'),
            start_at: newSlot.start,
            end_at: newSlot.end,
            timezone: pick(b, 'timeZone', 'timezone'),
            attendee_email: pick(b, 'attendeeEmail', 'attendee_email'),
            attendee_phone: pick(b, 'attendeePhone', 'attendee_phone'),
            attendee_name: pick(b, 'attendeeName', 'attendee_name'),
            coordinator_email: pick(b, 'coordinatorEmail', 'coordinator_email'),
            appointment_type_name: pick(b, 'appointmentTypeName', 'appointment_type_name'),
            organization_name: pick(b, 'organizationName', 'organization_name'),
            // join URL is persisted on the Booking row as channel_details (booking-store) — check
            // both names. pick() here is the 2-key (camel, snake) local helper, so OR two calls.
            join_url: pick(b, 'joinUrl', 'join_url') || pick(b, 'channelDetails', 'channel_details'),
          },
          tenantPrefs: {
            notificationPrefs: { sms: event.org_sms_enabled === true },
            sms_quiet_hours: event.sms_quiet_hours || null,
          },
          rescheduleUrl,
          cancelUrl,
        });
      } catch (err) {
        (logger.warn || logger.error || (() => {}))('reminder_rebind_failed', { error_name: (err && err.name) || 'unknown' });
      }
    }
    return { outcome: result.outcome, booking: result.booking };
  } catch (err) {
    (logger.error || (() => {}))('scheduling_mutate_failed', { mutation, error_name: (err && err.name) || 'unknown' });
    return { outcome: 'failed', error: 'executor_error' };
  }
}

// Reschedule-link anti-abuse cooldown: at most one send per booking per this window (a second
// portal click within the window is refused WITHOUT minting a fresh token → no email-bombing).
const RESCHEDULE_LINK_COOLDOWN_SECONDS = 60;

// G7b: resolve the notice channels for the reschedule link. Email is the unconditional floor;
// SMS is the opt-in supplement — attempted ONLY when the tenant enabled org SMS AND the guest
// has live consent AND it is not quiet-hours (selectChannels, §E3). FAIL-CLOSED at every step:
// no org-flag / no phone / consent-read error / selectChannels throw → { email:true, sms:false }.
async function resolveNoticeChannels({ tenantId, booking, orgSmsEnabled, readSmsConsent, selectChannels, logger }) {
  if (orgSmsEnabled !== true) return { email: true, sms: false };
  const phone = pick(booking, 'attendeePhone', 'attendee_phone');
  if (!phone) return { email: true, sms: false };
  let consentRecord = null;
  try {
    consentRecord = await readSmsConsent(tenantId, phone);
  } catch (err) {
    (logger.warn || logger.error || (() => {}))('reschedule_link_consent_read_failed', { error_name: (err && err.name) || 'unknown' });
    return { email: true, sms: false };
  }
  try {
    // fireTime = now: the link is sent immediately; quiet-hours is evaluated in the guest's tz.
    return selectChannels({ tenantId, booking, orgSmsEnabled, consentRecord, fireTime: new Date() });
  } catch (err) {
    (logger.warn || logger.error || (() => {}))('reschedule_link_select_channels_failed', { error_name: (err && err.name) || 'unknown' });
    return { email: true, sms: false };
  }
}

// G6 reschedule_link — notify-only (NO calendar mutation). Rate-limited; mints a fresh §B4
// reschedule token (MINT is stateless: NO jti write — the jti is written only at REDEEM) and
// emails the guest a self-serve reschedule link. Mirrors Attendance_Disposition_Handler's flow.
async function handleRescheduleLink({ booking, tenantId, orgSmsEnabled, bookingStore, signRescheduleToken, dispatchVolunteerNotice, selectChannels, readSmsConsent, logger }) {
  const bookingId = pick(booking, 'bookingId', 'booking_id');
  const startAt = pick(booking, 'startAt', 'start_at');
  // §B4 reschedule expiry = start_at − cancellation_window_hours (MIN-floored to 15min in tokens.js).
  // Source the window from the booking like disposition.js does (was hard-0, giving exp=start_at).
  const cancellationWindowHours = pick(booking, 'cancellationWindowHours', 'cancellation_window_hours');

  // Anti email-bombing (atomic claim BEFORE minting): refuse a repeat send within the cooldown.
  try {
    const allowed = await bookingStore.touchRescheduleLinkSentAt(tenantId, bookingId, RESCHEDULE_LINK_COOLDOWN_SECONDS);
    if (!allowed) {
      return { outcome: 'rate_limited' };
    }
  } catch (err) {
    // A failed cooldown write must not become a FunctionError; treat as a clean failure (no token
    // minted, no email sent). The booking row may have vanished or DDB is unreachable.
    (logger.error || (() => {}))('reschedule_link_cooldown_failed', { booking_id: bookingId, error_name: (err && err.name) || 'unknown' });
    return { outcome: 'failed', error: 'cooldown_write_failed' };
  }

  let rescheduleUrl;
  try {
    const claims = { tenant_id: tenantId, booking_id: bookingId, start_at: startAt };
    if (cancellationWindowHours != null) claims.cancellation_window_hours = cancellationWindowHours;
    const token = await signRescheduleToken('reschedule', claims);
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

  // G7b: decide channels (email floor + TCPA-gated SMS supplement) before dispatch.
  const channels = await resolveNoticeChannels({
    tenantId, booking, orgSmsEnabled, readSmsConsent, selectChannels, logger,
  });

  // Notify the guest the self-serve link (best-effort — notify appends the STOP footer and never
  // throws on a send failure). buildEmailPayload requires booking.rescheduleUrl, supplied here.
  // The guest phone (for the SMS supplement) is read off the booking inside notify.
  try {
    const result = await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId, booking: { ...booking, rescheduleUrl }, channels },
      {}
    );
    // "sent" = the guest was reached on at least one channel (email floor or the SMS supplement).
    const d = (result && result.dispatched) || {};
    const sent = d.email === 'sent' || d.sms === 'sent';
    return { outcome: 'success', sent };
  } catch (err) {
    // Defensive: dispatch is best-effort and shouldn't throw, but a throw here must not become
    // a Lambda FunctionError — the token was minted; report sent:false.
    (logger.error || (() => {}))('reschedule_link_notify_failed', { error_name: (err && err.name) || 'unknown' });
    return { outcome: 'success', sent: false };
  }
}

module.exports = { handleSchedulingMutate };
