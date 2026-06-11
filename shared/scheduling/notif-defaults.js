'use strict';

/**
 * notif-defaults.js — §E14 per-tenant notification-template DEFAULTS (pure data, no AWS).
 *
 * These are the dispatcher-side defaults for §E14-overridable moments: the copy that
 * actually sends when a tenant has no override. They MUST stay byte-in-sync with the ADA
 * editor's display defaults (`_SCHED_NOTIF_DEFAULTS` / `_SCHED_NOTIF_SMS_DEFAULTS` in
 * Analytics_Dashboard_API/lambda_function.py) — otherwise the editor's reset/preview lies
 * about what dispatch sends. The parity test
 * (__tests__/notif-defaults-parity.test.js) reads the ADA Python source and fails CI on
 * any drift.
 *
 * Scope: starts with `confirmation` (S4c — consumed by Booking_Commit_Handler's
 * confirmation-email.js). Migrating notify.js TEMPLATES/SMS_TEMPLATES and
 * Scheduled_Message_Sender REMINDER_TEMPLATES here is the scheduled extraction follow-up
 * (S4b audit row 16) — do NOT add a new §E14 moment with its own local copy; add it here.
 *
 * Compliance invariant (same as STOP): the editable body is ONLY the greeting/confirmation
 * copy. Action links (cancel/reschedule/join), the .ics attachment, and sign-offs are
 * appended by the dispatcher OUTSIDE these templates, so an override can never drop them.
 */

const CONFIRMATION_DEFAULTS = {
  subject: "You're confirmed — {{org}}",
  text: "Hi {{firstName}},\n\nYou're confirmed for your {{apptType}} {{whenLabel}}.",
  html: "<p>Hi {{firstName}},</p><p>You're confirmed for your {{apptType}} {{whenLabel}}.</p>",
};

module.exports = { CONFIRMATION_DEFAULTS };
