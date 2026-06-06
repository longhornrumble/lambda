'use strict';

/**
 * zoomOutagePaging.js — WS-E-ATTEND C13: Zoom-outage T-15min coordinator auto-page.
 *
 * Canonical §5.5 row 3 (Zoom outage). Folded into sub-phase E per the kanban because the
 * SMS path it needs only exists now (it was SMS-blocked in sub-phase C).
 *
 * THE T-15 BRANCH (this module's scope per the WS-E-ATTEND work-order):
 *   At T-15min before event_start, if the conference is a Zoom meeting and Zoom is still
 *   unreachable / unprovisioned, the platform:
 *     (a) PAGES THE COORDINATOR (staff) via SMS with the volunteer's contact info, and
 *     (b) sends the volunteer a fallback message ("Zoom is having issues — your coordinator
 *         will reach out at the number on file").
 *   This is transactional, now-or-never urgency → it BYPASSES §12.2 quiet hours (NOT consent).
 *
 * ── Coordinator page is the GUARANTEED action (staff / internal) ──
 *   The coordinator page uses `sendType:'internal'` — the staff path that bypasses the
 *   contact consent gate (§E3). It is the one message that must always go out. The volunteer
 *   fallback is contact-facing: it requires consent (selectChannels), but is marked URGENT so
 *   the §E3 quiet-hours window is skipped. ⚑ The urgent quiet-hours-bypass is a §E3
 *   selectChannels extension WS-E-TCPA must honor (`urgent:true`) — FLAGGED until it merges;
 *   the fail-closed default here sends the volunteer fallback only when consent is already known.
 *
 * ── DI seam (pure logic) ──
 *   deps = {
 *     checkZoomReachable(booking) -> Promise<boolean>,   // probe (zoom-client getMeeting); injected
 *     sendSms({ tenantId, to, body, sendType }),
 *     selectChannels(args) -> Promise<{ email, sms }>,   // §E3 contact gate (volunteer fallback)
 *     log, now
 *   }
 *
 * ── OUT OF SCOPE (flagged) ──
 *   The complementary §5.5 "provisioning clears within 30min → urgent volunteer SMS" branch is
 *   the C8/conference-provisioning side, not the missed-event workstream. Left to that path.
 */

function pick(booking, camel, snake) {
  if (!booking) return undefined;
  return booking[camel] != null ? booking[camel] : booking[snake];
}

// Is this booking a Zoom conference? Tolerant read across the shapes a Booking row may carry
// (schema discipline): an explicit provider field, or a join URL that is a zoom.us link.
function isZoomConference(booking) {
  const provider = (
    pick(booking, 'conferenceProvider', 'conference_provider') || ''
  )
    .toString()
    .toLowerCase();
  if (provider === 'zoom') return true;
  const joinUrl = (
    pick(booking, 'joinUrl', 'join_url') ||
    pick(booking, 'channelDetails', 'channel_details') ||
    ''
  ).toString();
  return /zoom\.us/i.test(joinUrl);
}

// Fail-closed default until WS-E-TCPA §E3 lands: no contact SMS without the real consent gate.
async function failClosedSelectChannels() {
  return { email: true, sms: false };
}

/**
 * @param {{ booking, deps }} args
 * @returns {Promise<{ outcome, dispatched }>}
 *   outcome ∈ 'paged' | 'zoom_ok' | 'skipped_not_zoom'
 */
async function pageCoordinatorOnZoomOutage({ booking, deps = {} } = {}) {
  const {
    checkZoomReachable,
    sendSms,
    selectChannels = failClosedSelectChannels,
    log = console,
  } = deps;

  const tenantId = pick(booking, 'tenantId', 'tenant_id');
  const bookingId = pick(booking, 'bookingId', 'booking_id');

  // Only Zoom bookings have a Zoom-outage failure mode.
  if (!isZoomConference(booking)) {
    log.info(`[zoom-page] skip non-zoom booking=${bookingId}`);
    return { outcome: 'skipped_not_zoom', dispatched: {} };
  }

  // Probe reachability. A probe error is treated as UNREACHABLE (fail-toward-paging — better a
  // false page than a silent no-show when Zoom is actually down).
  let reachable;
  try {
    reachable = await checkZoomReachable(booking);
  } catch (err) {
    log.error(`[zoom-page] reachability probe threw booking=${bookingId}: ${err.message} — treating as unreachable`);
    reachable = false;
  }
  if (reachable) {
    log.info(`[zoom-page] zoom reachable booking=${bookingId} — no page`);
    return { outcome: 'zoom_ok', dispatched: {} };
  }

  const dispatched = {};
  const coordinatorPhone = pick(booking, 'coordinatorPhone', 'coordinator_phone');
  const attendeePhone = pick(booking, 'attendeePhone', 'attendee_phone');
  const attendeeName = pick(booking, 'attendeeName', 'attendee_name');

  // (a) PAGE THE COORDINATOR — staff/internal, always attempted (the now-or-never action).
  if (coordinatorPhone && typeof sendSms === 'function') {
    const contactBits = [attendeeName, attendeePhone].filter(Boolean).join(' · ') || 'on file';
    try {
      await sendSms({
        tenantId,
        to: coordinatorPhone,
        body:
          `Zoom looks down for your upcoming appointment. Please reach the volunteer ` +
          `directly: ${contactBits}.`,
        sendType: 'internal',
      });
      dispatched.coordinator_sms = 'sent';
    } catch (err) {
      log.error(`[zoom-page] coordinator page failed booking=${bookingId}: ${err.message}`);
      dispatched.coordinator_sms = 'failed';
    }
  } else {
    log.warn(`[zoom-page] no coordinator phone booking=${bookingId}`);
    dispatched.coordinator_sms = 'skipped_no_phone';
  }

  // (b) VOLUNTEER FALLBACK — contact-facing, consent-gated, but URGENT (quiet-hours bypass).
  if (attendeePhone && typeof sendSms === 'function') {
    let channels = { email: true, sms: false };
    try {
      channels = await selectChannels({
        tenantId,
        attendee: { phone: attendeePhone, name: attendeeName },
        moment: 'zoom_outage',
        urgent: true, // ⚑ §E3 extension WS-E-TCPA must honor (skip quiet hours, keep consent)
      });
    } catch (err) {
      log.error(`[zoom-page] selectChannels failed booking=${bookingId}: ${err.message}`);
    }
    if (channels.sms) {
      try {
        await sendSms({
          tenantId,
          to: attendeePhone,
          body:
            `Zoom is having issues for your upcoming appointment — your coordinator will ` +
            `reach out at the number on file. Reply STOP to opt out.`,
          sendType: 'contact',
        });
        dispatched.volunteer_sms = 'sent';
      } catch (err) {
        log.error(`[zoom-page] volunteer fallback failed booking=${bookingId}: ${err.message}`);
        dispatched.volunteer_sms = 'failed';
      }
    } else {
      dispatched.volunteer_sms = 'suppressed_no_consent';
    }
  } else {
    dispatched.volunteer_sms = 'skipped_no_phone';
  }

  log.info(
    `[zoom-page] paged booking=${bookingId} coordinator=${dispatched.coordinator_sms} ` +
      `volunteer=${dispatched.volunteer_sms}`
  );
  return { outcome: 'paged', dispatched };
}

module.exports = {
  pageCoordinatorOnZoomOutage,
  isZoomConference,
  failClosedSelectChannels,
  pick,
};
