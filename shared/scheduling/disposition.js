'use strict';

/**
 * disposition.js — WS-E-ATTEND E6: the three-option interviewer disposition.
 *
 * Canonical §11.2 / §11.1; FROZEN_CONTRACTS §E4 (LOCKED 2026-06-05) + §B4 (token purposes).
 *
 * Wires the SHIPPED WS-D4 `/attended/*` redemption stub (TODO(E6) in
 * Scheduling_Redemption_Handler/index.js) to a REAL Booking.status transition:
 *
 *   attended_yes  → Booking.status = completed              (no outbound)
 *   no_show       → Booking.status = no_show                + auto-message the volunteer
 *                                                             with a reschedule link (§12.3)
 *   didnt_connect → Booking.status = coordinator_no_show    (no outbound — §11.2)
 *
 * The token security path (validate + one-time-redeem + feature-gate) is owned by the
 * shipped WS-D4 handler; this module runs AFTER a successful redeem and performs only the
 * state transition + the no_show courtesy notice.
 *
 * ── NO auto-completion (§11.1) ──
 *   This module transitions a booking ONLY in response to an explicit human disposition
 *   (interviewer click, or admin click on the same tokens). It never rolls a silent booking
 *   forward to `completed`. A booking nobody dispositions stays `booked` / non-key
 *   `attendance_state='pending_attendance'` forever — the escalation cadence (escalation.js)
 *   pushes harder for a human answer; it does not fabricate one.
 *
 * ── Idempotent (one-time tokens + at-least-once admin/interviewer clicks) ──
 *   The transition is a conditional UpdateItem guarded on `status == booked`. A second click
 *   (or the admin clicking after the interviewer already answered) finds a non-`booked` row →
 *   ConditionalCheckFailed → returns `{ outcome:'already_resolved' }`. The caller renders a
 *   benign "already recorded" page; no double transition, no duplicate volunteer message.
 *
 * ── Booking.status vs attendance_state ──
 *   Disposition sets the §A-locked Booking.status AND clears the non-key flow label to
 *   `attendance_state='resolved'` so the escalation cadence stops and analytics can tell a
 *   resolved booking from an unresolved one. We import shared/booking-status (CI-3c SoT) so
 *   the target literals can never drift from the canonical vocabulary.
 *
 * ── DI seam ──
 *   Production callers (the WS-D4 handler) call `applyDisposition({ tenantId, bookingId,
 *   purpose })` with NO deps — the module defaults (DynamoDB client, tokens.sign,
 *   notify.dispatchVolunteerNotice) keep the handler edit surgical. Tests inject `deps` to
 *   stay AWS-free. The defaults are references to already-shipped primitives, never new
 *   AWS-touching logic defined here (except the fail-closed selectChannels stub — see below).
 *
 * ── WS-E-TCPA seam (selectChannels, §E3 — NOT yet merged) ──
 *   The no_show volunteer message's SMS supplement is gated by §E3 selectChannels. Until
 *   WS-E-TCPA lands, the default selectChannels here is FAIL-CLOSED: `{ email:true, sms:false }`
 *   — the email floor always sends; SMS is never sent without the real consent gate. The
 *   integrator wires the real selectChannels at merge. ⚑ FLAGGED in the PR.
 */

const {
  DynamoDBClient,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { isBookingStatus } = require('../booking-status');
const tokens = require('./tokens');
const notify = require('./notify');

// ─── config / defaults ─────────────────────────────────────────────────────────────────

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
// The WS-D3 redemption distribution that fronts the WS-D4 endpoints. The reschedule link in
// the no_show notice points here (https-only; notify.safeUrl drops anything else).
const REDEMPTION_BASE_URL =
  process.env.REDEMPTION_BASE_URL || 'https://schedule.myrecruiter.ai';

// Module-level client (reused across warm invocations; Calendar_Watch_* style). Tests inject deps.ddb.
const defaultDdb = new DynamoDBClient({});

// §11.2 disposition map. Validated against the CI-3c SoT at module load so a vocabulary drift
// trips here, not in production (mirrors Calendar_Lifecycle_Consumer/booking-store.js).
const STATUS_BOOKED = 'booked';
const ATTENDANCE_STATE_RESOLVED = 'resolved';
const DISPOSITION_BY_PURPOSE = Object.freeze({
  attended_yes: 'completed',
  no_show: 'no_show',
  didnt_connect: 'coordinator_no_show',
});
for (const v of [STATUS_BOOKED, ...Object.values(DISPOSITION_BY_PURPOSE)]) {
  if (!isBookingStatus(v)) {
    throw new Error(
      `disposition: '${v}' is not a canonical Booking.status (shared/booking-status drift)`
    );
  }
}

// Human label for the post-disposition confirmation email to the interviewer (§11.2).
const ACTION_LABEL = Object.freeze({
  attended_yes: 'attended',
  no_show: 'no-show',
  didnt_connect: "didn't connect",
});

// Fail-closed default channel gate until WS-E-TCPA §E3 lands (email floor, no SMS).
async function failClosedSelectChannels() {
  return { email: true, sms: false };
}

// ─── helpers ─────────────────────────────────────────────────────────────────────────

function s(value) {
  return { S: String(value) };
}

// Read a string attribute off a DDB ALL_NEW item (schema discipline — tolerate absence).
function attr(item, name) {
  return item && item[name] && item[name].S != null ? item[name].S : null;
}

function isConditionalCheckFailed(err) {
  return Boolean(err) && err.name === 'ConditionalCheckFailedException';
}

// ─── applyDisposition ──────────────────────────────────────────────────────────────────

/**
 * @param {{ tenantId, bookingId, purpose, deps? }} args
 *   purpose ∈ 'attended_yes' | 'no_show' | 'didnt_connect'
 * @returns {Promise<{ outcome, transitioned, status?, volunteerNotice? }>}
 *   outcome ∈ '<targetStatus>' | 'already_resolved'
 */
async function applyDisposition({ tenantId, bookingId, purpose, deps = {} } = {}) {
  const targetStatus = DISPOSITION_BY_PURPOSE[purpose];
  if (!targetStatus) {
    // Caller bug — the WS-D4 slug→purpose map only ever passes the three above.
    throw new Error(`disposition: unknown purpose '${purpose}'`);
  }
  if (!tenantId || !bookingId) {
    throw new Error('disposition: tenantId and bookingId are required');
  }

  const {
    ddb = defaultDdb,
    signToken = tokens.sign,
    dispatchVolunteerNotice = notify.dispatchVolunteerNotice,
    selectChannels = failClosedSelectChannels,
    baseUrl = REDEMPTION_BASE_URL,
    cancellationWindowHours = 0,
    now,
    log = console,
  } = deps;

  const at = new Date().toISOString();

  // Conditional transition: only from `booked`. ReturnValues ALL_NEW gives us the attendee +
  // coordinator fields for the no_show notice + the interviewer confirmation without a second read.
  let item;
  try {
    const res = await ddb.send(
      new UpdateItemCommand({
        TableName: BOOKING_TABLE,
        Key: { tenantId: s(tenantId), booking_id: s(bookingId) },
        UpdateExpression:
          'SET #st = :target, attendance_state = :resolved, dispositioned_at = :at, disposition_purpose = :p',
        ConditionExpression: 'attribute_exists(booking_id) AND #st = :booked',
        ExpressionAttributeNames: { '#st': 'status' },
        ExpressionAttributeValues: {
          ':target': s(targetStatus),
          ':resolved': s(ATTENDANCE_STATE_RESOLVED),
          ':at': s(at),
          ':p': s(purpose),
          ':booked': s(STATUS_BOOKED),
        },
        ReturnValues: 'ALL_NEW',
      })
    );
    item = (res && res.Attributes) || {};
  } catch (err) {
    if (isConditionalCheckFailed(err)) {
      // Second click / admin-after-interviewer / volunteer already canceled → idempotent ack.
      log.info(
        `[disposition] no-op (already resolved or not booked) booking=${bookingId} purpose=${purpose}`
      );
      return { outcome: 'already_resolved', transitioned: false };
    }
    throw err;
  }

  log.info(
    `[disposition] booking=${bookingId} purpose=${purpose} -> status=${targetStatus}`
  );

  const result = { outcome: targetStatus, transitioned: true, status: targetStatus };

  // no_show → auto-message the volunteer with a fresh reschedule link (§11.2 / §12.3).
  if (purpose === 'no_show') {
    result.volunteerNotice = await sendNoShowReoffer({
      tenantId,
      bookingId,
      item,
      signToken,
      dispatchVolunteerNotice,
      selectChannels,
      baseUrl,
      cancellationWindowHours,
      now,
      log,
    });
  }

  // Post-disposition confirmation to the interviewer (§11.2): action + applicant + program.
  result.interviewerConfirmation = await sendInterviewerConfirmation({
    tenantId,
    bookingId,
    purpose,
    item,
    deps,
    log,
  });

  return result;
}

// ─── no_show volunteer reoffer (best-effort) ───────────────────────────────────────────

async function sendNoShowReoffer({
  tenantId,
  bookingId,
  item,
  signToken,
  dispatchVolunteerNotice,
  selectChannels,
  baseUrl,
  cancellationWindowHours,
  now,
  log,
}) {
  const startAt = attr(item, 'start_at');
  const attendeeEmail = attr(item, 'attendee_email');
  const attendeePhone = attr(item, 'attendee_phone');
  const attendeeName = attr(item, 'attendee_name');
  const apptType = attr(item, 'appointment_type_name');
  const whenLabel = attr(item, 'when_label');

  // Mint a fresh reschedule token so the volunteer can pick a new time one-tap (§B4).
  let rescheduleUrl;
  try {
    const token = await signToken(
      'reschedule',
      {
        tenant_id: tenantId,
        booking_id: bookingId,
        start_at: startAt,
        cancellation_window_hours: cancellationWindowHours,
      },
      typeof now === 'number' ? { now } : undefined
    );
    rescheduleUrl = `${baseUrl}/reschedule?t=${encodeURIComponent(token)}`;
  } catch (err) {
    // A token we can't mint (e.g. missing start_at) must not throw — the status transition
    // already succeeded; log + skip the courtesy notice.
    log.error(`[disposition] reschedule-token mint failed booking=${bookingId}: ${err.message}`);
    return { dispatched: {}, suppressed: true, reason: 'token_mint_failed' };
  }

  // §E3 contact-consent gate for the SMS supplement (email is the floor). Fail-closed default
  // until WS-E-TCPA merges. selectChannels failure must not block the email-floor send.
  let channels = { email: true, sms: false };
  try {
    channels = await selectChannels({
      tenantId,
      attendee: { email: attendeeEmail, phone: attendeePhone },
      moment: 'reschedule',
    });
  } catch (err) {
    log.error(`[disposition] selectChannels failed booking=${bookingId}: ${err.message}`);
  }

  // notify.js (§B8) owns the compliance-injected reoffer template (reschedule link + STOP).
  return dispatchVolunteerNotice(
    {
      kind: 'reoffer',
      tenantId,
      booking: {
        booking_id: bookingId,
        attendee_email: attendeeEmail,
        attendee_name: attendeeName,
        appointment_type_name: apptType,
        when_label: whenLabel,
        reoffer_url: rescheduleUrl,
        reschedule_url: rescheduleUrl,
      },
      channels,
    },
    { log }
  );
}

// ─── interviewer confirmation (best-effort, §11.2) ─────────────────────────────────────

async function sendInterviewerConfirmation({ tenantId, bookingId, purpose, item, deps, log }) {
  const sendEmail = deps.sendEmail || notify.defaultInvokeEmail;
  const to = attr(item, 'coordinator_email');
  if (!to) {
    return { email: 'skipped_no_recipient' };
  }
  // Subject + text body are PLAIN TEXT — use raw values (HTML-escaping them would render
  // `O'Brien` as `O&#39;Brien` in the subject line). Only the html body escapes (B1 fix).
  const applicantRaw = attr(item, 'attendee_name') || 'the volunteer';
  const programRaw = attr(item, 'appointment_type_name') || 'appointment';
  const action = ACTION_LABEL[purpose];
  const subject = `Recorded: ${applicantRaw} — ${action}`;
  const text_body =
    `Thanks — we've recorded that ${applicantRaw} ` +
    `was marked "${action}" for the ${programRaw}. ` +
    `No further action is needed.`;
  const html_body =
    `<p>Thanks — we've recorded that <strong>${notify.escapeHtml(applicantRaw)}</strong> was marked ` +
    `"<strong>${notify.escapeHtml(action)}</strong>" for the ${notify.escapeHtml(programRaw)}.</p>` +
    `<p>No further action is needed.</p>`;
  try {
    await sendEmail({ tenantId, to, subject, html_body, text_body });
    return { email: 'sent' };
  } catch (err) {
    log.error(`[disposition] interviewer confirmation failed booking=${bookingId}: ${err.message}`);
    return { email: 'failed' };
  }
}

module.exports = {
  applyDisposition,
  DISPOSITION_BY_PURPOSE,
  ACTION_LABEL,
  ATTENDANCE_STATE_RESOLVED,
  // exported for unit coverage:
  failClosedSelectChannels,
  attr,
  isConditionalCheckFailed,
  _BOOKING_TABLE: BOOKING_TABLE,
};
