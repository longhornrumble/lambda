'use strict';

/**
 * notify.js — (Y) volunteer-notice dispatch primitive (WS-SCHED-FOUNDATIONS).
 *
 * Canonical §5.1 (agent-of-CoR), §12.1/§12.2/§12.3 (cadence shape / TCPA STOP /
 * content). A SINGLE dispatch the scheduling consumers (B9 reoffer, B11 reassign,
 * WS-CAL-LIFECYCLE) call to send ONE volunteer notice. It is a primitive, NOT the
 * sub-phase-E reminder scheduler — it sends one message now; cadence/scheduling is
 * the caller's job.
 *
 *   dispatchVolunteerNotice({ kind, tenantId, booking, channels }, deps)
 *     kind ∈ { reschedule_link, reoffer, cancel_notice, reengagement, move_optin_sms }
 *
 * ── Agent-of-CoR guard (§5.1) ──
 *   We notify the volunteer ONLY where the platform adds value beyond Google's native
 *   email. `reassigned` and plain `moved` are covered by Google's attendee-update
 *   email — calling this for them is suppressed (no-op, logged). The four `kind`s
 *   above ARE the value-adds (embedded reschedule link, reoffer, cancellation context,
 *   opted-in SMS) that calendar/conferencing tools can't produce. An unknown kind is a
 *   caller bug → throws.
 *
 * ── Channels ──
 *   Email routes through the existing **`send_email`** Lambda (invoked async / Event,
 *   best-effort) — NOT direct SES. (C8's booking-confirmation email uses direct SES +
 *   .ics; these post-booking notices deliberately reuse the shared transactional
 *   sender per the work-order. Flagged for the integrator: the two scheduling email
 *   paths could later converge.)
 *   SMS routes to the existing `SMS_Sender` Lambda — but that twin lands in
 *   sub-phase-E/SMS, so the SMS path here is a **TODO(SMS-E) STUB**: it logs and
 *   returns a stub marker, never sends, never throws. `move_optin_sms` is therefore
 *   entirely stubbed today.
 *
 * ── Compliance injection (§12.2/§12.3) ──
 *   Email bodies embed the reschedule/manage link (the §12.3 "easy reschedule path")
 *   and a STOP/unsubscribe line. SMS (when SMS-E lands) must carry STOP/HELP — the
 *   stub records that requirement.
 *
 * ── Best-effort ──
 *   A send failure is NON-FATAL: caught, logged PII-redacted (booking_id + kind only —
 *   never the attendee email/phone/name), and reflected in the return value. The
 *   caller's primary workflow (the calendar mutation) already succeeded; a failed
 *   courtesy notice must not roll it back.
 *
 * ── Templates ──
 *   The three notice templates live locally (concrete-first, §4.3) in the `{{var}}`
 *   shape of `Master_Function_Staging/notification_templates.json`. They are NOT yet
 *   in that file (it has no reschedule/reoffer/cancel scheduling templates, only
 *   `appointment_reminder`); editing it is outside this workstream's ownership.
 *   **Flagged for the integrator:** migrate these into notification_templates.json so
 *   it stays the single source of truth.
 *
 * ── DI seam ──
 *   `deps.invokeEmail` / `deps.invokeSms` / `deps.log` are injectable; the module is
 *   fully unit-testable without AWS. The defaults are the only AWS-touching code.
 */

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

// Created once at module load; reused across warm invocations.
const lambda = new LambdaClient({});

const SEND_EMAIL_FUNCTION = process.env.SEND_EMAIL_FUNCTION || 'send_email';
// The eventual SMS target (sub-phase-E/SMS twin). Referenced by the stub for clarity.
const SMS_SENDER_FUNCTION = process.env.SMS_SENDER_FUNCTION || 'SMS_Sender';

// Value-add notices the platform sends (agent-of-CoR §5.1 — beyond Google's email).
const EMAIL_KINDS = new Set(['reschedule_link', 'reoffer', 'cancel_notice', 'reengagement']);
const SMS_KINDS = new Set(['move_optin_sms']);
// Mutations Google's native email already covers → never platform-notified (§5.1).
const COR_NATIVE_KINDS = new Set(['reassigned', 'moved']);

// ─── small helpers ───────────────────────────────────────────────────────────────────

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[c]));
}

// {{var}} substitution. `vars` values are inserted verbatim into the text body and
// HTML-escaped into the html body by the caller pre-escaping html-bound vars.
function render(template, vars) {
  return template.replace(/\{\{(\w+)\}\}/g, (_, key) =>
    vars[key] != null ? String(vars[key]) : ''
  );
}

// Forward-compatible read: accept either the in-memory camelCase booking or a
// DDB-snake_case row (schema discipline — tolerate either; missing → undefined).
function pick(booking, camel, snake) {
  if (!booking) return undefined;
  return booking[camel] != null ? booking[camel] : booking[snake];
}

function firstNameOf(fullName) {
  if (typeof fullName !== 'string') return '';
  return fullName.trim().split(/\s+/)[0] || '';
}

// Action links land in an email href — only https is allowed through. A
// javascript:/data:/mailto: (etc.) scheme is dropped to '' so a malformed or
// hostile link can never become an executable href. Tokenised reschedule/reoffer
// links are always https, so this is a no-op for the real path.
function safeUrl(url) {
  return typeof url === 'string' && /^https:\/\//i.test(url.trim()) ? url.trim() : '';
}

// ─── templates ({{var}} — mirrors notification_templates.json; see header) ────────────

const STOP_LINE_TEXT = '\n\nTo stop receiving these emails, reply STOP.';
const STOP_LINE_HTML =
  '<p style="margin-top:24px;font-size:12px;color:#64748B;">To stop receiving these emails, reply STOP.</p>';

const TEMPLATES = {
  reschedule_link: {
    subject: 'Need a different time? — {{org}}',
    text:
      'Hi {{firstName}},\n\nNeed to change your {{apptType}}{{whenSuffix}}? ' +
      'You can pick a new time here:\n{{actionUrl}}' +
      STOP_LINE_TEXT,
    html:
      '<p>Hi {{firstName}},</p>' +
      '<p>Need to change your {{apptType}}{{whenSuffix}}? You can pick a new time here:</p>' +
      '<p><a href="{{actionUrl}}">Reschedule</a></p>' +
      STOP_LINE_HTML,
  },
  reoffer: {
    subject: "Let's find you a new time — {{org}}",
    text:
      'Hi {{firstName}},\n\nThe time you picked for your {{apptType}} is no longer ' +
      'available. Pick a new one here:\n{{actionUrl}}' +
      STOP_LINE_TEXT,
    html:
      '<p>Hi {{firstName}},</p>' +
      '<p>The time you picked for your {{apptType}} is no longer available. ' +
      'Pick a new one here:</p>' +
      '<p><a href="{{actionUrl}}">Choose a new time</a></p>' +
      STOP_LINE_HTML,
  },
  cancel_notice: {
    subject: 'Your {{apptType}} was canceled — {{org}}',
    text:
      'Hi {{firstName}},\n\nYour {{apptType}}{{whenSuffix}} has been canceled.' +
      '{{rebookText}}' +
      STOP_LINE_TEXT,
    html:
      '<p>Hi {{firstName}},</p>' +
      '<p>Your {{apptType}}{{whenSuffix}} has been canceled.</p>' +
      '{{rebookHtml}}' +
      STOP_LINE_HTML,
  },
};

// ─── payload builders ────────────────────────────────────────────────────────────────

// Returns { to, subject, html_body, text_body } for an email kind, or throws if a
// required action link for the kind is missing (a caller contract error, distinct from
// a best-effort send failure).
function buildEmailPayload({ kind, booking }) {
  const attendeeEmail = pick(booking, 'attendeeEmail', 'attendee_email');
  const firstName =
    pick(booking, 'attendeeFirstName', 'attendee_first_name') ||
    firstNameOf(pick(booking, 'attendeeName', 'attendee_name'));
  const org =
    pick(booking, 'organizationName', 'organization_name') ||
    pick(booking, 'orgName', 'org_name') ||
    'us';
  const apptType =
    pick(booking, 'appointmentTypeName', 'appointment_type_name') || 'appointment';

  // reengagement (§E8): WS-E-COPY's reengagement.js generates the diplomatic body WITH the
  // reschedule CTA already embedded (its compliance invariant). notify owns ONLY the STOP
  // footer — it never injects a second CTA (no double-injection). The body is REQUIRED
  // (a caller contract error if absent, like the missing-link kinds).
  if (kind === 'reengagement') {
    const body = pick(booking, 'reengagementBody', 'reengagement_body');
    if (typeof body !== 'string' || !body.trim()) {
      throw new Error('reengagement requires booking.reengagement_body (WS-E-COPY generates it)');
    }
    const htmlBody =
      '<p>' +
      escapeHtml(body).replace(/\n{2,}/g, '</p><p>').replace(/\n/g, '<br>') +
      '</p>' +
      STOP_LINE_HTML;
    return {
      to: attendeeEmail,
      subject: render('A note about your {{apptType}} — {{org}}', { org, apptType }),
      text_body: body + STOP_LINE_TEXT,
      html_body: htmlBody,
    };
  }

  const whenLabel = pick(booking, 'whenLabel', 'when_label');
  // https-only: a non-https (javascript:/data:/…) link is dropped to '' → it is then
  // treated as a MISSING link (throws for the kinds that require one, omitted for the
  // optional cancel-notice rebook). No hostile scheme can reach an href.
  const rescheduleUrl = safeUrl(pick(booking, 'rescheduleUrl', 'reschedule_url'));
  const reofferUrl = safeUrl(pick(booking, 'reofferUrl', 'reoffer_url'));

  let actionUrl;
  if (kind === 'reschedule_link') {
    actionUrl = rescheduleUrl;
    if (!actionUrl) {
      throw new Error('reschedule_link requires booking.rescheduleUrl');
    }
  } else if (kind === 'reoffer') {
    actionUrl = reofferUrl || rescheduleUrl;
    if (!actionUrl) {
      throw new Error('reoffer requires booking.reofferUrl or booking.rescheduleUrl');
    }
  }

  const whenSuffix = whenLabel ? ` on ${whenLabel}` : '';

  // cancel_notice: rebook link is optional (the cancellation confirmation stands alone).
  const rebookUrl = rescheduleUrl;
  const rebookText = rebookUrl ? ` Want to rebook? ${rebookUrl}` : '';
  const rebookHtml = rebookUrl
    ? `<p>Want to rebook? <a href="${escapeHtml(rebookUrl)}">Find a new time</a></p>`
    : '';

  const tpl = TEMPLATES[kind];

  // Text body uses raw values; HTML body uses HTML-escaped values (the rebook block is
  // pre-escaped above, so it is passed through render unescaped).
  const textVars = {
    firstName,
    org,
    apptType,
    whenSuffix,
    actionUrl,
    rebookText,
  };
  const htmlVars = {
    firstName: escapeHtml(firstName),
    org: escapeHtml(org),
    apptType: escapeHtml(apptType),
    whenSuffix: escapeHtml(whenSuffix),
    actionUrl: escapeHtml(actionUrl),
    rebookHtml,
  };

  return {
    to: attendeeEmail,
    // Subjects are plain text, not HTML — render with raw values (never HTML-escaped).
    subject: render(tpl.subject, textVars),
    text_body: render(tpl.text, textVars),
    html_body: render(tpl.html, htmlVars),
  };
}

// ─── default DI implementations (the only AWS-touching code) ──────────────────────────

// Invoke the send_email Lambda async (Event) — best-effort, fire-and-forget. send_email
// reads `event.body` as a JSON string (API-Gateway-shaped), so we wrap accordingly.
async function defaultInvokeEmail({ tenantId, to, subject, html_body, text_body }) {
  const inner = {
    to: [to],
    subject,
    html_body,
    text_body,
    tags: { tenant_id: String(tenantId || 'unknown').slice(0, 256), email_type: 'scheduling_notice' },
  };
  await lambda.send(
    new InvokeCommand({
      FunctionName: SEND_EMAIL_FUNCTION,
      InvocationType: 'Event',
      Payload: Buffer.from(JSON.stringify({ body: JSON.stringify(inner) })),
    })
  );
}

// TODO(SMS-E): wire to the SMS_Sender Lambda (event { to, body, tenantId, type:'reminder',
// sessionId, ... }, async Event; SMS_Sender enforces consent + STOP/opt-out + quiet
// hours). Until the sub-phase-E/SMS twin lands this is a no-send STUB. Never throws.
// eslint-disable-next-line no-unused-vars -- args are the contract the real impl will use
async function defaultInvokeSms({ tenantId, booking, kind, log }) {
  (log || console).warn(
    `[notify] SMS path not yet implemented (TODO SMS-E) — kind=${kind} would route to ${SMS_SENDER_FUNCTION}`
  );
  return { stub: true };
}

// ─── dispatchVolunteerNotice ───────────────────────────────────────────────────────────

/**
 * @param {{ kind: string, tenantId: string, booking: object, channels?: { email?: boolean, sms?: boolean } }} args
 * @param {object} [deps] - { invokeEmail, invokeSms, log }
 * @returns {Promise<{ kind: string, suppressed: boolean, reason?: string, dispatched: { email?: string, sms?: string } }>}
 */
async function dispatchVolunteerNotice(
  { kind, tenantId, booking, channels } = {},
  deps = {}
) {
  const {
    invokeEmail = defaultInvokeEmail,
    invokeSms = defaultInvokeSms,
    log = console,
  } = deps;

  if (!tenantId) {
    throw new Error('tenantId is required');
  }

  // Agent-of-CoR guard (§5.1): Google's native email already covers these.
  if (COR_NATIVE_KINDS.has(kind)) {
    log.info(
      `[notify] suppressed kind=${kind} (agent-of-CoR: Google native email covers it)`
    );
    return {
      kind,
      suppressed: true,
      reason: 'agent_of_cor_native_email',
      dispatched: {},
    };
  }

  const isEmailKind = EMAIL_KINDS.has(kind);
  const isSmsKind = SMS_KINDS.has(kind);
  if (!isEmailKind && !isSmsKind) {
    throw new Error(`unknown notice kind: ${kind}`);
  }

  const bookingId = pick(booking, 'bookingId', 'booking_id') || 'unknown';
  // Default channels per kind; an explicit `channels` overrides which to attempt.
  // An SMS-native kind sends UNLESS `channels.sms:false` (mirrors the email guard,
  // so `channels.sms:false` actually suppresses); an email kind sends SMS ONLY when
  // `channels.sms:true` is opted in.
  const attemptEmail =
    isEmailKind && (channels ? channels.email !== false : true);
  const attemptSms =
    (isSmsKind && (channels ? channels.sms !== false : true)) ||
    (isEmailKind && !!channels && channels.sms === true);

  const dispatched = {};

  if (attemptEmail) {
    const payload = buildEmailPayload({ kind, booking }); // throws on missing required link
    if (!payload.to) {
      log.warn(`[notify] no recipient email — kind=${kind} booking=${bookingId}`);
      dispatched.email = 'skipped_no_recipient';
    } else {
      try {
        await invokeEmail({ tenantId, ...payload });
        dispatched.email = 'sent';
      } catch (err) {
        // Best-effort: never propagate. PII-redacted (booking_id + kind only).
        log.error(
          `[notify] email dispatch failed — kind=${kind} booking=${bookingId}: ${err.message}`
        );
        dispatched.email = 'failed';
      }
    }
  }

  if (attemptSms) {
    try {
      await invokeSms({ tenantId, booking, kind, log });
      dispatched.sms = 'stubbed_todo_sms_e';
    } catch (err) {
      // Defensive: the stub never throws, but a future real impl must stay best-effort.
      log.error(
        `[notify] sms dispatch failed — kind=${kind} booking=${bookingId}: ${err.message}`
      );
      dispatched.sms = 'failed';
    }
  }

  return { kind, suppressed: false, dispatched };
}

module.exports = {
  dispatchVolunteerNotice,
  // exported for unit coverage + reuse:
  buildEmailPayload,
  defaultInvokeEmail,
  defaultInvokeSms,
  render,
  escapeHtml,
  EMAIL_KINDS,
  SMS_KINDS,
  COR_NATIVE_KINDS,
  _SEND_EMAIL_FUNCTION: SEND_EMAIL_FUNCTION,
  _SMS_SENDER_FUNCTION: SMS_SENDER_FUNCTION,
};
