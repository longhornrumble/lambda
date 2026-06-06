'use strict';

/**
 * attendance.js — WS-E-ATTEND E5: the missed-event attendance check + the
 * three-option interviewer prompt builder.
 *
 * Canonical §9.2 / §11 / §11.1 / §11.2; FROZEN_CONTRACTS §E4 (LOCKED 2026-06-05).
 *
 * At `event_end + 30min` the platform asks the interviewer (coordinator) whether the
 * volunteer attended. This module owns two pieces:
 *
 *   buildInterviewerPrompt(...)  — mint the THREE §B4 interviewer tokens
 *     (attended_yes / no_show / didnt_connect), each pointing at the SHIPPED WS-D4
 *     `/attended/*` redemption endpoint, and render the email + SMS prompt bodies.
 *     (Reused verbatim by the E10 T+24h escalation resend — escalation.js imports it.)
 *
 *   runAttendanceCheck(...)      — set the NON-KEY Booking attribute
 *     `attendance_state='pending_attendance'` (NOT a Booking.status value — §E4 amendment
 *     #7; status STAYS `booked` until a human dispositions, see disposition.js E6) and
 *     dispatch the prompt. Idempotent: a re-fired schedule that finds the attribute already
 *     set (or the booking no longer `booked`) does NOT re-send.
 *
 * ── attendance_state is a flow label, NOT Booking.status ──
 *   §A locks Booking.status to exactly five values (booked/canceled/completed/no_show/
 *   coordinator_no_show). `pending_attendance` is the ConversationSchedulingSession / flow
 *   label that §9.2 + §11.2 describe — it lives as an additive non-key attribute on the
 *   Booking row so analytics + the escalation cadence can find unresolved bookings without
 *   polluting the status vocabulary. This module NEVER writes Booking.status.
 *
 * ── DI seam (pure logic — no module-level AWS clients) ──
 *   Every side-effect is injected via `deps` so the module is unit-testable without AWS:
 *     setAttendanceState({ tenantId, bookingId, now }) -> Promise<boolean>   // idempotent conditional write; true iff THIS call set it
 *     signToken(purpose, claims, opts) -> Promise<string>                    // = shared/scheduling/tokens.js sign()
 *     sendEmail({ tenantId, to, cc?, subject, html_body, text_body }) -> Promise<void>
 *     sendSms({ tenantId, to, body, sendType }) -> Promise<void>
 *     selectChannels(args) -> Promise<{ email, sms }>                        // §E3 (WS-E-TCPA) — contact-facing only
 *     log, now                                                              // now = epoch SECONDS (tokens.js convention)
 *
 * ── Interviewer (coordinator) is STAFF, not a contact ──
 *   The prompt goes to the coordinator. Email is the floor (always). The optional SMS to the
 *   coordinator uses `sendType:'internal'` (staff path — §E3: internal sends bypass the
 *   contact consent gate; staff opt-in is governed by their own profile flag, not
 *   picasso-sms-consent). selectChannels (the contact consent gate) is therefore NOT used
 *   for the interviewer prompt — it is used for the volunteer-facing no_show reoffer
 *   (disposition.js) and the C13 volunteer fallback (zoomOutagePaging.js).
 *
 * ── PII hygiene (§5.7) ──
 *   The prompt names the volunteer + coordinator by FIRST NAME only. We log only opaque
 *   references (tenant_id, booking_id) and outcomes — never email/phone/token/full name.
 */

// The three interviewer-facing §B4 token purposes, in render order. (Volunteer purposes
// cancel/reschedule/post_application_recovery are NOT minted here.)
const ATTENDANCE_OPTIONS = [
  { purpose: 'attended_yes', slug: '/attended/met', label: 'Yes, they came' },
  { purpose: 'no_show', slug: '/attended/noshow', label: "No, they didn't show" },
  { purpose: 'didnt_connect', slug: '/attended/noconnect', label: "We didn't connect" },
];

// The non-key flow label written to the Booking row (NOT a Booking.status value — §E4).
const ATTENDANCE_STATE_PENDING = 'pending_attendance';

// ─── small helpers (local; mirrors notify.js so the two render the same way) ───────────

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[c]));
}

// Forward-compatible read: accept either an in-memory camelCase booking or a DDB snake_case
// row (schema discipline — tolerate either shape; a missing field → undefined).
function pick(booking, camel, snake) {
  if (!booking) return undefined;
  return booking[camel] != null ? booking[camel] : booking[snake];
}

function firstNameOf(fullName) {
  if (typeof fullName !== 'string') return '';
  return fullName.trim().split(/\s+/)[0] || '';
}

// ─── buildInterviewerPrompt (E5 + reused by E10 t24h) ──────────────────────────────────

/**
 * Mint the three §B4 interviewer tokens and render the prompt bodies.
 * @returns {Promise<{ to, subject, text_body, html_body, sms_body, links }>}
 *   links = { attended_yes, no_show, didnt_connect } (https redemption URLs)
 */
async function buildInterviewerPrompt({ booking, baseUrl, signToken, now }) {
  const tenantId = pick(booking, 'tenantId', 'tenant_id');
  const bookingId = pick(booking, 'bookingId', 'booking_id');
  // Attendance-token expiry driver (§13.6): event_end + 24h (floored by tokens.js). Tolerate
  // either an explicit end (end_at / event_end) or fall back to start_at if the row predates
  // end_at — a slightly-shorter-lived link is acceptable and the min-lifetime floor still applies.
  const eventEnd =
    pick(booking, 'endAt', 'end_at') ||
    pick(booking, 'eventEnd', 'event_end') ||
    pick(booking, 'startAt', 'start_at');

  const coordinatorEmail = pick(booking, 'coordinatorEmail', 'coordinator_email');
  const coordinatorFirst = firstNameOf(pick(booking, 'coordinatorName', 'coordinator_name'));
  const volunteerFirst = firstNameOf(pick(booking, 'attendeeName', 'attendee_name'));
  const apptType =
    pick(booking, 'appointmentTypeName', 'appointment_type_name') || 'appointment';
  const whenLabel = pick(booking, 'whenLabel', 'when_label');

  // Mint the three one-tap tokens. signToken is tokens.js sign(purpose, claims, opts); opts.now
  // (epoch seconds) keeps the expiry deterministic in tests. claims carry references only (§13.3)
  // plus the event_end expiry driver (NOT persisted into the token payload).
  const links = {};
  for (const opt of ATTENDANCE_OPTIONS) {
    const token = await signToken(
      opt.purpose,
      { tenant_id: tenantId, booking_id: bookingId, event_end: eventEnd },
      typeof now === 'number' ? { now } : undefined
    );
    links[opt.purpose] = `${baseUrl}${opt.slug}?t=${encodeURIComponent(token)}`;
  }

  const who = volunteerFirst || 'the volunteer';
  const whenSuffix = whenLabel ? ` on ${whenLabel}` : '';
  const greeting = coordinatorFirst ? `Hi ${coordinatorFirst},` : 'Hi,';

  const subject = `Did ${who} make it? — quick attendance check`;

  const text_body =
    `${greeting}\n\n` +
    `How did your ${apptType}${whenSuffix} with ${who} go? Tap one:\n\n` +
    `• ${ATTENDANCE_OPTIONS[0].label}: ${links.attended_yes}\n` +
    `• ${ATTENDANCE_OPTIONS[1].label}: ${links.no_show}\n` +
    `• ${ATTENDANCE_OPTIONS[2].label}: ${links.didnt_connect}\n`;

  const html_body =
    `<p>${escapeHtml(greeting)}</p>` +
    `<p>How did your ${escapeHtml(apptType)}${escapeHtml(whenSuffix)} with ` +
    `${escapeHtml(who)} go?</p>` +
    `<p><a href="${escapeHtml(links.attended_yes)}">${escapeHtml(ATTENDANCE_OPTIONS[0].label)}</a></p>` +
    `<p><a href="${escapeHtml(links.no_show)}">${escapeHtml(ATTENDANCE_OPTIONS[1].label)}</a></p>` +
    `<p><a href="${escapeHtml(links.didnt_connect)}">${escapeHtml(ATTENDANCE_OPTIONS[2].label)}</a></p>`;

  // SMS keeps it to one tap per line; concise (Telnyx caps at 1600 chars in SMS_Sender).
  const sms_body =
    `Did ${who} make it to your ${apptType}? ` +
    `Yes: ${links.attended_yes} | No-show: ${links.no_show} | ` +
    `Didn't connect: ${links.didnt_connect}`;

  return { to: coordinatorEmail, subject, text_body, html_body, sms_body, links };
}

// ─── runAttendanceCheck (E5) ───────────────────────────────────────────────────────────

/**
 * Fired at event_end + 30min. Sets the non-key `attendance_state='pending_attendance'`
 * (idempotent) and dispatches the three-option interviewer prompt.
 *
 * @returns {Promise<{ outcome, dispatched }>}
 *   outcome ∈ 'pending_attendance_set' | 'skipped_not_booked' | 'skipped_already_marked'
 */
async function runAttendanceCheck({ booking, deps }) {
  const {
    setAttendanceState,
    signToken,
    sendEmail,
    sendSms,
    baseUrl,
    log = console,
    now,
  } = deps || {};

  const tenantId = pick(booking, 'tenantId', 'tenant_id');
  const bookingId = pick(booking, 'bookingId', 'booking_id');
  const status = pick(booking, 'status', 'status');

  // Only an active (booked) appointment is eligible. A terminal/canceled booking (the
  // coordinator already dispositioned, or the volunteer canceled) must never be prompted.
  if (status !== 'booked') {
    log.info(
      `[attendance] skip non-booked booking=${bookingId} status=${status}`
    );
    return { outcome: 'skipped_not_booked', dispatched: {} };
  }

  // Idempotent conditional write: returns false if the attribute is already present (a
  // re-fired schedule, or the reconciler already marked it) → do NOT re-send the prompt.
  const wasSet = await setAttendanceState({ tenantId, bookingId, now });
  if (!wasSet) {
    log.info(`[attendance] already marked booking=${bookingId} — no re-send`);
    return { outcome: 'skipped_already_marked', dispatched: {} };
  }

  const prompt = await buildInterviewerPrompt({ booking, baseUrl, signToken, now });
  const dispatched = {};

  // Email is the floor — always sent to the coordinator (interviewer).
  if (!prompt.to) {
    log.warn(`[attendance] no coordinator email booking=${bookingId}`);
    dispatched.email = 'skipped_no_recipient';
  } else {
    try {
      await sendEmail({
        tenantId,
        to: prompt.to,
        subject: prompt.subject,
        html_body: prompt.html_body,
        text_body: prompt.text_body,
      });
      dispatched.email = 'sent';
    } catch (err) {
      // Best-effort: the attendance_state is already set; a failed prompt must not throw
      // (the escalation cadence will resend). PII-redacted log.
      log.error(`[attendance] email failed booking=${bookingId}: ${err.message}`);
      dispatched.email = 'failed';
    }
  }

  // Optional SMS to the coordinator (STAFF → sendType:'internal', bypasses the contact
  // consent gate). Only when a coordinator phone is on the booking + sendSms is wired.
  const coordinatorPhone = pick(booking, 'coordinatorPhone', 'coordinator_phone');
  if (coordinatorPhone && typeof sendSms === 'function') {
    try {
      await sendSms({
        tenantId,
        to: coordinatorPhone,
        body: prompt.sms_body,
        sendType: 'internal',
      });
      dispatched.sms = 'sent';
    } catch (err) {
      log.error(`[attendance] sms failed booking=${bookingId}: ${err.message}`);
      dispatched.sms = 'failed';
    }
  }

  log.info(
    `[attendance] pending_attendance set + prompt dispatched booking=${bookingId} ` +
      `email=${dispatched.email || 'n/a'} sms=${dispatched.sms || 'n/a'}`
  );
  return { outcome: 'pending_attendance_set', dispatched };
}

module.exports = {
  runAttendanceCheck,
  buildInterviewerPrompt,
  ATTENDANCE_OPTIONS,
  ATTENDANCE_STATE_PENDING,
  // exported for unit coverage + reuse:
  escapeHtml,
  firstNameOf,
  pick,
};
