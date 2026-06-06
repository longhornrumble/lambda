'use strict';

/**
 * Attendance_Disposition_Handler — WS-E-ATTEND (impl plan E5/E10/C13; canonical §9.2/§11/§5.5).
 *
 * The EventBridge Scheduler TARGET for the missed-event loop's time-based fires. It is thin
 * glue: it loads the Booking, wires the AWS side-effects (booking-store.js) into the pure
 * logic modules (shared/scheduling/{attendance,escalation,zoomOutagePaging}.js), and returns.
 *
 * Event `action` routing (set on the schedule's input payload by the rule creator):
 *   attendance_check  { tenantId, booking_id }                 — E5: set non-key attendance_state + 3-option prompt
 *   escalate          { tenantId, booking_id, tier }           — E10: t24h resend+cc / t72h urgent+inbox
 *   weekly_digest     { tenantId }                             — E10: t7d admin digest (bounded GSI enumeration)
 *   zoom_outage_check { tenantId, booking_id, zoom_unreachable? } — C13: T-15 coordinator page
 *
 * ── ⚑ E5-TRIGGER SEAM (WS-E-REMIND not merged) ──
 *   The EventBridge Scheduler RULES that fire these actions, and the re-bind/delete lifecycle,
 *   are WS-E-REMIND's §E1 ownership. The §E1 contract lists the attendance rule's target as
 *   Scheduled_Message_Sender — but the attendance check must SET attendance_state + MINT three
 *   tokens, which that pure dispatcher cannot do. So this handler is the correct target for
 *   `sched-attendance-{booking_id}` (and the escalation/zoom schedules). FLAGGED to the
 *   integrator: point those rules here, or reconcile §E1. Recurrence (t24h→t72h→t7d, weekly
 *   t7d) + next-tier scheduling is WS-E-REMIND's lifecycle — this handler performs ONE fire.
 *
 * Env (deploy note — IaC is the integrator's):
 *   ENVIRONMENT, BOOKING_TABLE, BOOKING_START_AT_INDEX, REDEMPTION_BASE_URL,
 *   SEND_EMAIL_FUNCTION, SMS_SENDER_FUNCTION, JWT_SECRET_KEY_NAME (tokens.js),
 *   JTI_BLACKLIST_TABLE (unused here — tokens.sign only), CONFIG_BUCKET (admin-email lookup).
 *   IAM: dynamodb GetItem/UpdateItem/Query on the Booking table + GSI; lambda:InvokeFunction
 *   on send_email + SMS_Sender; secretsmanager:GetSecretValue on the jwt signing key;
 *   s3:GetObject on the tenant-config bucket. (Dedicated execution role — never shared.)
 */

const tokens = require('../shared/scheduling/tokens');
const { runAttendanceCheck } = require('../shared/scheduling/attendance');
const { escalateSilence, buildWeeklyDigest } = require('../shared/scheduling/escalation');
const { pageCoordinatorOnZoomOutage } = require('../shared/scheduling/zoomOutagePaging');
const store = require('./booking-store');

const REDEMPTION_BASE_URL =
  process.env.REDEMPTION_BASE_URL || 'https://schedule.myrecruiter.ai';

// §E3 selectChannels (WS-E-TCPA) is not merged → fail-closed default (email floor, no SMS).
// ⚑ FLAGGED: the integrator wires the real §E3 gate at WS-E-TCPA merge.
async function failClosedSelectChannels() {
  return { email: true, sms: false };
}

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

function nowSeconds() {
  return Math.floor(Date.now() / 1000);
}

exports.handler = async (event) => {
  const action = event && event.action;
  const tenantId = event && event.tenantId;
  const bookingId = event && event.booking_id;

  if (!action || !tenantId) {
    log('attendance_handler_bad_event', { action: action || null, hasTenant: !!tenantId });
    return { outcome: 'bad_event' };
  }

  // weekly_digest is per-tenant (no booking_id); the rest are per-booking.
  if (action === 'weekly_digest') {
    const now = nowSeconds();
    const pendingBookings = await store.queryPendingAttendance({ tenantId, now: Date.now() });
    const r = await buildWeeklyDigest({
      tenantId,
      pendingBookings,
      deps: {
        sendEmail: store.sendEmail,
        getAdminEmails: store.getAdminEmails,
        now,
      },
    });
    log('attendance_weekly_digest', { tenant_id: tenantId, count: r.count, email: r.dispatched.email });
    return { action, ...r };
  }

  if (!bookingId) {
    log('attendance_handler_missing_booking', { action, tenant_id: tenantId });
    return { outcome: 'bad_event' };
  }

  const booking = await store.getBooking(tenantId, bookingId);
  if (!booking) {
    log('attendance_handler_booking_not_found', { action, tenant_id: tenantId, booking_id: bookingId });
    return { action, outcome: 'booking_not_found' };
  }

  const now = nowSeconds();

  if (action === 'attendance_check') {
    const r = await runAttendanceCheck({
      booking,
      deps: {
        setAttendanceState: store.setAttendanceState,
        signToken: tokens.sign,
        sendEmail: store.sendEmail,
        sendSms: store.sendSms,
        baseUrl: REDEMPTION_BASE_URL,
        now,
      },
    });
    log('attendance_check_done', { tenant_id: tenantId, booking_id: bookingId, outcome: r.outcome });
    return { action, ...r };
  }

  if (action === 'escalate') {
    const r = await escalateSilence({
      booking,
      tier: event.tier,
      deps: {
        signToken: tokens.sign,
        sendEmail: store.sendEmail,
        getAdminEmails: store.getAdminEmails,
        writePortalInboxAlert: store.writePortalInboxAlert,
        baseUrl: REDEMPTION_BASE_URL,
        now,
      },
    });
    log('attendance_escalate_done', {
      tenant_id: tenantId,
      booking_id: bookingId,
      tier: event.tier,
      outcome: r.outcome,
    });
    return { action, ...r };
  }

  if (action === 'zoom_outage_check') {
    const r = await pageCoordinatorOnZoomOutage({
      booking,
      deps: {
        checkZoomReachable: store.makeZoomReachableProbe(event),
        sendSms: store.sendSms,
        selectChannels: failClosedSelectChannels,
        now,
      },
    });
    log('attendance_zoom_outage_done', { tenant_id: tenantId, booking_id: bookingId, outcome: r.outcome });
    return { action, ...r };
  }

  log('attendance_handler_unknown_action', { action, tenant_id: tenantId });
  return { action, outcome: 'unknown_action' };
};

exports._internal = { failClosedSelectChannels, nowSeconds };
