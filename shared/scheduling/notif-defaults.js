'use strict';

/**
 * notif-defaults.js — §E14 notification-template DEFAULTS + compliance strings
 * (pure data, no AWS, no I/O).
 *
 * THE single JS source for every §E14-overridable moment's default copy and for
 * the STOP/unsubscribe compliance strings. Consumers:
 *   - shared/scheduling/notify.js          (reschedule_link / reoffer / cancel_notice,
 *                                           email TEMPLATES + SMS_TEMPLATES + footers)
 *   - Scheduled_Message_Sender/index.mjs   (REMINDER_TEMPLATES + SMS footer)
 *   - Booking_Commit_Handler/confirmation-email.js (CONFIRMATION_DEFAULTS)
 *
 * The ADA editor (Analytics_Dashboard_API `_SCHED_NOTIF_DEFAULTS` /
 * `_SCHED_NOTIF_SMS_DEFAULTS`) shows these as the reset/preview copy — its Python
 * copy MUST stay byte-in-sync or the editor lies about what dispatch sends. The
 * parity test (__tests__/notif-defaults-parity.test.js) reads the ADA source and
 * fails CI on any drift, for EVERY moment, email + SMS. (Extraction closes S4b
 * audit row 16 / S4c rows 11-12: previously three per-Lambda copies with three
 * regex parity tests.)
 *
 * Do NOT add a new §E14 moment as a local copy in a dispatcher — add it here.
 *
 * Compliance invariants (enforced by the consumers, data lives here):
 *   - STOP/unsubscribe lines are appended AFTER render, OUTSIDE the editable
 *     body, exactly once (STOP_MARKER_RE dedup) — an override can neither
 *     remove nor duplicate them.
 *   - The confirmation's .ics + signed action links are assembled outside the
 *     editable region (confirmation-email.js).
 */

// ─── email defaults: the notify.js-dispatched volunteer notices ────────────────────────

const TEMPLATES = {
  reschedule_link: {
    subject: 'Need a different time? — {{org}}',
    text:
      'Hi {{firstName}},\n\nNeed to change your {{apptType}}{{whenSuffix}}? ' +
      'You can pick a new time here:\n{{actionUrl}}',
    html:
      '<p>Hi {{firstName}},</p>' +
      '<p>Need to change your {{apptType}}{{whenSuffix}}? You can pick a new time here:</p>' +
      '<p><a href="{{actionUrl}}">Reschedule</a></p>',
  },
  reoffer: {
    subject: "Let's find you a new time — {{org}}",
    text:
      'Hi {{firstName}},\n\nThe time you picked for your {{apptType}} is no longer ' +
      'available. Pick a new one here:\n{{actionUrl}}',
    html:
      '<p>Hi {{firstName}},</p>' +
      '<p>The time you picked for your {{apptType}} is no longer available. ' +
      'Pick a new one here:</p>' +
      '<p><a href="{{actionUrl}}">Choose a new time</a></p>',
  },
  cancel_notice: {
    subject: 'Your {{apptType}} was canceled — {{org}}',
    text:
      'Hi {{firstName}},\n\nYour {{apptType}}{{whenSuffix}} has been canceled.' +
      '{{rebookText}}',
    html:
      '<p>Hi {{firstName}},</p>' +
      '<p>Your {{apptType}}{{whenSuffix}} has been canceled.</p>' +
      '{{rebookHtml}}',
  },
};

// ─── SMS defaults: the notify.js-dispatched volunteer notices (G7b) ────────────────────
// STOP/HELP is NOT baked in — appended (SMS_STOP_FOOTER) after render.

const SMS_TEMPLATES = {
  reschedule_link:
    'Hi {{firstName}}, need a different time for your {{apptType}}? Pick a new one: {{actionUrl}}',
  reoffer:
    'Hi {{firstName}}, your {{apptType}} time is no longer available. Pick a new one: {{actionUrl}}',
  cancel_notice:
    'Hi {{firstName}}, your {{apptType}}{{whenSuffix}} was canceled.{{rebookText}}',
};

// ─── reminder defaults: dispatched by Scheduled_Message_Sender at fire time (S4b) ──────
// The tier implies the lead ("tomorrow" / "in about an hour") — no {{when}} var.

const REMINDER_TEMPLATES = {
  reminder_24h: {
    subject: 'Reminder: your {{apptType}} is tomorrow — {{org}}',
    text: 'Hi {{firstName}},\n\nThis is a reminder that your {{apptType}} with {{org}} is tomorrow.',
    html: '<p>Hi {{firstName}},</p><p>This is a reminder that your {{apptType}} with {{org}} is tomorrow.</p>',
    sms: 'Reminder: your {{apptType}} with {{org}} is tomorrow.',
  },
  reminder_1h: {
    subject: 'Reminder: your {{apptType}} is in about an hour — {{org}}',
    text: 'Hi {{firstName}},\n\nThis is a reminder that your {{apptType}} with {{org}} is in about an hour.',
    html: '<p>Hi {{firstName}},</p><p>This is a reminder that your {{apptType}} with {{org}} is in about an hour.</p>',
    sms: 'Reminder: your {{apptType}} with {{org}} is in about an hour.',
  },
};

// ─── confirmation default: dispatched by Booking_Commit_Handler at commit (S4c) ────────
// Editable region ONLY — the .ics + cancel/reschedule/join links + sign-off are
// appended outside it by confirmation-email.js.

const CONFIRMATION_DEFAULTS = {
  subject: "You're confirmed — {{org}}",
  text: "Hi {{firstName}},\n\nYou're confirmed for your {{apptType}} {{whenLabel}}.",
  html: "<p>Hi {{firstName}},</p><p>You're confirmed for your {{apptType}} {{whenLabel}}.</p>",
};

// ─── compliance strings (single source — previously duplicated notify.js ↔ Sender) ─────
// STOP must appear EXACTLY once: always appended (an override can't remove it), but
// STOP_MARKER_RE detects an override that already carries the canonical phrase so it
// is never double-injected.

const STOP_LINE_TEXT = '\n\nTo stop receiving these emails, reply STOP.';
const STOP_LINE_HTML =
  '<p style="margin-top:24px;font-size:12px;color:#64748B;">To stop receiving these emails, reply STOP.</p>';
const SMS_STOP_FOOTER = '\nReply STOP to opt out, HELP for help.';
const STOP_MARKER_RE = /reply\s+STOP/i;

function appendStopOnce(rendered, stopLine) {
  return STOP_MARKER_RE.test(rendered) ? rendered : rendered + stopLine;
}

module.exports = {
  TEMPLATES,
  SMS_TEMPLATES,
  REMINDER_TEMPLATES,
  CONFIRMATION_DEFAULTS,
  STOP_LINE_TEXT,
  STOP_LINE_HTML,
  SMS_STOP_FOOTER,
  STOP_MARKER_RE,
  appendStopOnce,
};
