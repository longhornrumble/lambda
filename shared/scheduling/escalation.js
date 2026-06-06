'use strict';

/**
 * escalation.js — WS-E-ATTEND E10: the missed-event silence-escalation cadence.
 *
 * Canonical §11 / §11.1 / §11.2 (disposition cadence table); FROZEN_CONTRACTS §E4.
 *
 * After the E5 attendance prompt fires (attendance.js), if NO ONE dispositions the booking,
 * the platform escalates WHO is asked and HOW LOUDLY — it NEVER auto-flips Booking.status
 * (§11.1 operational principle). This module owns the three silence tiers:
 *
 *   t24h — resend the interviewer prompt + cc the admin (admin can disposition on staff's behalf)
 *   t72h — urgent admin email + a Customer-Portal inbox alert (consumed by WS-E-PORTAL)
 *   t7d  — weekly digest to admin enumerating ALL pending_attendance bookings >7d, oldest
 *          first; recurs every 7d until resolved
 *
 * ── Stop condition (visibility before state-change, §11.2) ──
 *   Each per-booking tier (t24h/t72h) first checks the booking is STILL unresolved
 *   (`status == booked` AND non-key `attendance_state == 'pending_attendance'`). Once a human
 *   dispositions (disposition.js sets a terminal status + `attendance_state='resolved'`), the
 *   next fired tier is a no-op (`stopped_resolved`). The cadence escalates attention; it does
 *   not roll state forward.
 *
 * ── Recurrence + scheduling is WS-E-REMIND's (E1) ──
 *   This module performs the DISPATCH for a tier and reports the suggested `nextTier`. Creating
 *   / deleting the EventBridge Scheduler rules that fire these tiers — including the recurring
 *   weekly t7d — is the WS-E-REMIND §E1 lifecycle. ⚑ Until WS-E-REMIND merges, the handler
 *   stubs the next-tier scheduling and flags the seam.
 *
 * ── DI seam (pure logic) ──
 *   deps = {
 *     signToken,                                       // tokens.js sign() — for the t24h prompt resend
 *     sendEmail({ tenantId, to, cc?, subject, html_body, text_body }),  // to/cc may be string|string[]
 *     getAdminEmails(tenantId) -> Promise<string[]>,   // tenant-config notification recipients
 *     writePortalInboxAlert({ tenantId, bookingId, kind, createdAt }),  // WS-E-PORTAL surface
 *     baseUrl, log, now                                // now = epoch SECONDS
 *   }
 *
 * ── PII hygiene (§5.7) ──
 *   Escalation recipients are STAFF/ADMIN (internal), so the digest may name the volunteer by
 *   FIRST NAME only + appointment type. We log only opaque references (tenant_id, booking_id).
 */

const { buildInterviewerPrompt, escapeHtml, firstNameOf, pick } = require('./attendance');

const REDEMPTION_BASE_URL =
  process.env.REDEMPTION_BASE_URL || 'https://schedule.myrecruiter.ai';

const ATTENDANCE_STATE_PENDING = 'pending_attendance';
const DAY_SECONDS = 24 * 60 * 60;
// Cap the enumerated rows in the weekly digest email so a tenant with a huge backlog can't
// produce an oversized SES message / exhaust memory (S5). The full count is still reported.
const MAX_DIGEST_ROWS = 100;

function nowSeconds(now) {
  return typeof now === 'number' ? now : Math.floor(Date.now() / 1000);
}

// Still-unresolved gate: only escalate a booking that is booked AND pending_attendance.
function isUnresolved(booking) {
  const status = pick(booking, 'status', 'status');
  const attendanceState = pick(booking, 'attendanceState', 'attendance_state');
  return status === 'booked' && attendanceState === ATTENDANCE_STATE_PENDING;
}

// ─── escalateSilence (per-booking: t24h, t72h) ─────────────────────────────────────────

/**
 * @param {{ booking, tier, deps }} args  tier ∈ 't24h' | 't72h'
 * @returns {Promise<object>} { outcome, tier, ... }
 */
async function escalateSilence({ booking, tier, deps = {} } = {}) {
  const {
    signToken,
    sendEmail,
    getAdminEmails,
    writePortalInboxAlert,
    baseUrl = REDEMPTION_BASE_URL,
    log = console,
    now,
  } = deps;

  const tenantId = pick(booking, 'tenantId', 'tenant_id');
  const bookingId = pick(booking, 'bookingId', 'booking_id');

  if (tier !== 't24h' && tier !== 't72h') {
    throw new Error(`escalation: unknown tier '${tier}'`);
  }

  // Stop the cadence the moment the booking is resolved (§11.2 visibility-before-state-change).
  if (!isUnresolved(booking)) {
    log.info(`[escalation] stop ${tier} — booking=${bookingId} already resolved`);
    return { outcome: 'stopped_resolved', tier, dispatched: {} };
  }

  const adminEmails = (await safeAdminEmails(getAdminEmails, tenantId, log)) || [];
  const coordinatorEmail = pick(booking, 'coordinatorEmail', 'coordinator_email');
  const dispatched = {};

  if (tier === 't24h') {
    // Resend the interviewer prompt (fresh tokens) + cc admin so either can disposition.
    const prompt = await buildInterviewerPrompt({ booking, baseUrl, signToken, now });
    if (!prompt.to) {
      log.warn(`[escalation] t24h no coordinator email booking=${bookingId}`);
      dispatched.email = 'skipped_no_recipient';
    } else {
      try {
        await sendEmail({
          tenantId,
          to: prompt.to,
          cc: adminEmails,
          subject: `Reminder: ${prompt.subject}`,
          html_body: prompt.html_body,
          text_body: prompt.text_body,
        });
        dispatched.email = 'sent';
      } catch (err) {
        log.error(`[escalation] t24h resend failed booking=${bookingId}: ${err.message}`);
        dispatched.email = 'failed';
      }
    }
    return {
      outcome: 'resent',
      tier: 't24h',
      adminCc: adminEmails.length > 0,
      dispatched,
      nextTier: 't72h',
    };
  }

  // tier === 't72h' — urgent admin email + Customer-Portal inbox alert.
  const volunteerFirst = firstNameOf(pick(booking, 'attendeeName', 'attendee_name')) || 'a volunteer';
  const apptType = pick(booking, 'appointmentTypeName', 'appointment_type_name') || 'appointment';
  const whenLabel = pick(booking, 'whenLabel', 'when_label');
  const whenSuffix = whenLabel ? ` (${whenLabel})` : '';
  const subject = `Action needed: unresolved appointment for ${volunteerFirst}`;
  const text_body =
    `An appointment still needs a yes / no-show / didn't-connect answer.\n\n` +
    `${apptType}${whenSuffix} with ${volunteerFirst}.\n\n` +
    (coordinatorEmail ? `Assigned coordinator: ${coordinatorEmail}\n` : '') +
    `Open the Customer Portal to record the outcome.`;
  const html_body =
    `<p>An appointment still needs a yes / no-show / didn't-connect answer.</p>` +
    `<p><strong>${escapeHtml(apptType)}</strong>${escapeHtml(whenSuffix)} with ` +
    `${escapeHtml(volunteerFirst)}.</p>` +
    (coordinatorEmail ? `<p>Assigned coordinator: ${escapeHtml(coordinatorEmail)}</p>` : '') +
    `<p>Open the Customer Portal to record the outcome.</p>`;

  if (adminEmails.length === 0) {
    log.warn(`[escalation] t72h no admin recipients booking=${bookingId}`);
    dispatched.email = 'skipped_no_recipient';
  } else {
    try {
      await sendEmail({ tenantId, to: adminEmails, subject, html_body, text_body });
      dispatched.email = 'sent';
    } catch (err) {
      log.error(`[escalation] t72h urgent failed booking=${bookingId}: ${err.message}`);
      dispatched.email = 'failed';
    }
  }

  // Customer-Portal inbox alert surface (PRODUCED here, consumed by WS-E-PORTAL).
  let portalInboxAlert = false;
  if (typeof writePortalInboxAlert === 'function') {
    try {
      await writePortalInboxAlert({
        tenantId,
        bookingId,
        kind: 'attendance_unresolved',
        createdAt: nowSeconds(now),
      });
      portalInboxAlert = true;
    } catch (err) {
      log.error(`[escalation] t72h portal-inbox alert failed booking=${bookingId}: ${err.message}`);
    }
  }

  return {
    outcome: 'urgent',
    tier: 't72h',
    portalInboxAlert,
    dispatched,
    nextTier: 't7d',
  };
}

// ─── buildWeeklyDigest (t7d, per-tenant) ───────────────────────────────────────────────

/**
 * @param {{ tenantId, pendingBookings, deps }} args
 *   pendingBookings = the enumerated still-pending_attendance rows (>7d). The bounded GSI
 *   query that produces this list is the WS-E-REMIND E9 reconciler / handler seam — ⚑ FLAGGED.
 * @returns {Promise<{ outcome:'digest', count, recur:true, dispatched }>}
 */
async function buildWeeklyDigest({ tenantId, pendingBookings, deps = {} } = {}) {
  const { sendEmail, getAdminEmails, log = console, now } = deps;
  const list = Array.isArray(pendingBookings) ? pendingBookings.slice() : [];

  // Oldest first (§11.2). Sort by start_at ascending; rows without start_at sink to the end.
  list.sort((a, b) => {
    const ta = Date.parse(pick(a, 'startAt', 'start_at') || '') || Infinity;
    const tb = Date.parse(pick(b, 'startAt', 'start_at') || '') || Infinity;
    return ta - tb;
  });

  const adminEmails = (await safeAdminEmails(getAdminEmails, tenantId, log)) || [];

  if (list.length === 0) {
    // Recurs regardless — but nothing to send this week.
    return { outcome: 'digest', count: 0, recur: true, dispatched: { email: 'skipped_empty' } };
  }
  if (adminEmails.length === 0) {
    log.warn(`[escalation] t7d digest no admin recipients tenant=${tenantId}`);
    return { outcome: 'digest', count: list.length, recur: true, dispatched: { email: 'skipped_no_recipient' } };
  }

  const nowS = nowSeconds(now);
  const capped = list.slice(0, MAX_DIGEST_ROWS);
  const overflow = list.length - capped.length;
  const rows = capped.map((b) => {
    const startAt = pick(b, 'startAt', 'start_at');
    const startMs = Date.parse(startAt || '');
    const daysPending = Number.isNaN(startMs)
      ? '?'
      : Math.max(0, Math.floor((nowS - Math.floor(startMs / 1000)) / DAY_SECONDS));
    const who = firstNameOf(pick(b, 'attendeeName', 'attendee_name')) || 'volunteer';
    const apptType = pick(b, 'appointmentTypeName', 'appointment_type_name') || 'appointment';
    return { who, apptType, startAt: startAt || 'unknown', daysPending };
  });

  const shown = overflow > 0 ? ` (showing oldest ${capped.length})` : '';
  const overflowText = overflow > 0 ? `\n…and ${overflow} more.` : '';
  const overflowHtml = overflow > 0 ? `<p>…and ${overflow} more.</p>` : '';
  const text_body =
    `${list.length} appointment(s) still need an attendance answer${shown} (oldest first):\n\n` +
    rows
      .map((r) => `• ${r.who} — ${r.apptType} — ${r.startAt} — ${r.daysPending}d pending`)
      .join('\n') +
    overflowText +
    `\n\nOpen the Customer Portal to record outcomes.`;
  const html_body =
    `<p>${list.length} appointment(s) still need an attendance answer${shown} (oldest first):</p>` +
    `<ul>` +
    rows
      .map(
        (r) =>
          `<li>${escapeHtml(r.who)} — ${escapeHtml(r.apptType)} — ` +
          `${escapeHtml(r.startAt)} — ${escapeHtml(String(r.daysPending))}d pending</li>`
      )
      .join('') +
    `</ul>` +
    overflowHtml +
    `<p>Open the Customer Portal to record outcomes.</p>`;

  const dispatched = {};
  try {
    await sendEmail({
      tenantId,
      to: adminEmails,
      subject: `Weekly digest: ${list.length} unresolved appointment(s)`,
      html_body,
      text_body,
    });
    dispatched.email = 'sent';
  } catch (err) {
    log.error(`[escalation] t7d digest failed tenant=${tenantId}: ${err.message}`);
    dispatched.email = 'failed';
  }

  return { outcome: 'digest', count: list.length, recur: true, dispatched };
}

// ─── helpers ─────────────────────────────────────────────────────────────────────────

// Admin-recipient resolution must never throw the escalation: a config-load failure → [].
async function safeAdminEmails(getAdminEmails, tenantId, log) {
  if (typeof getAdminEmails !== 'function') return [];
  try {
    const emails = await getAdminEmails(tenantId);
    return Array.isArray(emails) ? emails.filter(Boolean) : [];
  } catch (err) {
    log.error(`[escalation] admin-email resolve failed tenant=${tenantId}: ${err.message}`);
    return [];
  }
}

module.exports = {
  escalateSilence,
  buildWeeklyDigest,
  isUnresolved,
  safeAdminEmails,
  ATTENDANCE_STATE_PENDING,
};
