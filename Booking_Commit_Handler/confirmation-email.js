'use strict';

/**
 * confirmation-email.js — booking confirmation email (AC #7).
 *
 * Delivered within the 60-second SLA after commit: an `.ics` attachment + the
 * conference join link + one-tap reschedule/cancel links signed by the shared
 * D1a tokens module (purposes `cancel` / `reschedule`, per-purpose expiry §13.6).
 *
 * SES SendRawEmail (not SendEmail) because we attach a file (.ics) — the same SES
 * sender/config-set the platform already uses (form_handler.js: SES_FROM_EMAIL,
 * ConfigurationSet `picasso-emails`).
 *
 * Output sanitization (§5.7 / C10 overlap): values rendered into the HTML body are
 * HTML-entity-encoded; values placed in MIME/ICS header-like fields are stripped of
 * CR/LF so a malicious name field can't inject headers (Bcc) or HTML/JS. C10 owns
 * the exhaustive pass; this is the C8-local defensive minimum so the keystone never
 * emits an injectable confirmation.
 */

const crypto = require('crypto');
const { SESClient, SendRawEmailCommand } = require('@aws-sdk/client-ses');
const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');

const { sign } = require('../shared/scheduling/tokens');
const { CONFIRMATION_DEFAULTS } = require('../shared/scheduling/notif-defaults');
const { sdkConfig } = require('./aws-client-config');

const SES_REGION = process.env.AWS_REGION || 'us-east-1';
const ses = new SESClient(sdkConfig({ region: SES_REGION }));
// §E14 S4c override read — same bounded sdkConfig as every commit-path client (the
// confirmation rides the 60s SLA; requestTimeout caps the GetItem, fail-safe → defaults).
const ddb = new DynamoDBClient(sdkConfig({ region: SES_REGION }));

const FROM_EMAIL = process.env.SES_FROM_EMAIL || 'notify@myrecruiter.ai';
const CONFIG_SET = process.env.SES_CONFIGURATION_SET || 'picasso-emails';
// §13.8 — token action endpoints live on the greenfield schedule.* host.
const SCHEDULE_BASE_URL = process.env.SCHEDULE_BASE_URL || 'https://schedule.myrecruiter.ai';
// §E14 S4c: per-tenant confirmation template overrides. Already wired on BCH (G6 grant
// DDBReadSchedulingNotifTemplate + env). Unset/empty → override system off, defaults send.
const SCHED_NOTIF_TEMPLATE_TABLE = process.env.SCHED_NOTIF_TEMPLATE_TABLE || '';

// ─── sanitization helpers ──────────────────────────────────────────────────────────

// Reject CR/LF from any header-context value (subject, MIME headers, ICS lines).
function stripCrlf(value) {
  if (value == null) return '';
  // eslint-disable-next-line no-control-regex
  return String(value).replace(/[\r\n]+/g, ' ').trim();
}

function escapeHtml(value) {
  if (value == null) return '';
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// RFC 5545 §3.3.11 TEXT escaping: backslash, semicolon, comma, and newline.
function escapeIcsText(value) {
  if (value == null) return '';
  return String(value)
    .replace(/\\/g, '\\\\')
    .replace(/;/g, '\\;')
    .replace(/,/g, '\\,')
    .replace(/\r?\n/g, '\\n');
}

// ─── .ics generation ────────────────────────────────────────────────────────────────

// ISO8601 → ICS UTC stamp (YYYYMMDDTHHMMSSZ).
function toIcsUtc(iso) {
  const d = new Date(iso);
  return `${d.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z')}`;
}

function buildIcs({ bookingId, summary, description, location, start, end, organizerEmail, attendeeEmail, dtstamp }) {
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//MyRecruiter//Scheduling//EN',
    'METHOD:REQUEST',
    'BEGIN:VEVENT',
    `UID:${escapeIcsText(bookingId)}@myrecruiter.ai`,
    // RFC 5545 §3.8.7.2: DTSTAMP is when the iCalendar object was CREATED (now),
    // NOT the event start. SEQUENCE:0 is the initial revision (future
    // reschedule/cancel .ics for the same UID increment it so clients update).
    `DTSTAMP:${toIcsUtc(dtstamp || new Date().toISOString())}`,
    `DTSTART:${toIcsUtc(start)}`,
    `DTEND:${toIcsUtc(end)}`,
    `SEQUENCE:0`,
    `SUMMARY:${escapeIcsText(summary)}`,
  ];
  if (description) lines.push(`DESCRIPTION:${escapeIcsText(description)}`);
  if (location) lines.push(`LOCATION:${escapeIcsText(location)}`);
  if (organizerEmail) lines.push(`ORGANIZER:mailto:${stripCrlf(organizerEmail)}`);
  if (attendeeEmail) lines.push(`ATTENDEE;RSVP=TRUE:mailto:${stripCrlf(attendeeEmail)}`);
  lines.push('STATUS:CONFIRMED', 'END:VEVENT', 'END:VCALENDAR');
  // ICS lines are CRLF-delimited.
  return lines.join('\r\n');
}

// ─── signed action links (§13) ───────────────────────────────────────────────────────

async function buildActionLinks({ tenantId, bookingId, startAt, cancellationWindowHours }, signOpts) {
  const baseClaims = {
    tenant_id: tenantId,
    booking_id: bookingId,
    start_at: startAt,
    cancellation_window_hours: cancellationWindowHours || 0,
  };
  const [cancelToken, rescheduleToken] = await Promise.all([
    sign('cancel', baseClaims, signOpts),
    sign('reschedule', baseClaims, signOpts),
  ]);
  return {
    cancelUrl: `${SCHEDULE_BASE_URL}/cancel?t=${encodeURIComponent(cancelToken)}`,
    rescheduleUrl: `${SCHEDULE_BASE_URL}/reschedule?t=${encodeURIComponent(rescheduleToken)}`,
  };
}

// ─── MIME assembly ────────────────────────────────────────────────────────────────────

function buildRawMime({ from, to, subject, textBody, htmlBody, icsContent, icsFilename }) {
  const boundaryMixed = `mixed_${crypto.randomBytes(12).toString('hex')}`;
  const boundaryAlt = `alt_${crypto.randomBytes(12).toString('hex')}`;
  const safeSubject = stripCrlf(subject);
  const safeTo = stripCrlf(to);
  const safeFrom = stripCrlf(from);
  const icsB64 = Buffer.from(icsContent, 'utf8').toString('base64');

  return [
    `From: ${safeFrom}`,
    `To: ${safeTo}`,
    `Subject: ${safeSubject}`,
    'MIME-Version: 1.0',
    `Content-Type: multipart/mixed; boundary="${boundaryMixed}"`,
    '',
    `--${boundaryMixed}`,
    `Content-Type: multipart/alternative; boundary="${boundaryAlt}"`,
    '',
    `--${boundaryAlt}`,
    'Content-Type: text/plain; charset=UTF-8',
    'Content-Transfer-Encoding: 7bit',
    '',
    textBody,
    '',
    `--${boundaryAlt}`,
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: 7bit',
    '',
    htmlBody,
    '',
    `--${boundaryAlt}--`,
    '',
    `--${boundaryMixed}`,
    `Content-Type: text/calendar; charset=UTF-8; method=REQUEST; name="${icsFilename}"`,
    'Content-Transfer-Encoding: base64',
    `Content-Disposition: attachment; filename="${icsFilename}"`,
    '',
    icsB64,
    '',
    `--${boundaryMixed}--`,
    '',
  ].join('\r\n');
}

// ─── content templates (§E14 S4c: override → else shared defaults) ────────────────────

// {{var}} substitution; unknown vars render '' (the §E14 editor contract — a var used in
// the wrong moment renders empty, never a literal {{...}} in a volunteer's inbox).
function renderVars(template, vars) {
  return template.replace(/\{\{(\w+)\}\}/g, (_, key) =>
    vars[key] != null ? String(vars[key]) : ''
  );
}

/**
 * §E14 S4c loader — GetItem of the tenant's `confirmation` override. FAIL-SAFE: unset
 * table / miss / error → null (defaults send); the bounded sdkConfig client caps the
 * read so a template-store hang can never threaten the 60s commit SLA. Mirrors
 * notify.js defaultLoadTemplateOverride (raw client; that loader gates on ITS moments,
 * so confirmation needs this local one).
 */
async function loadTemplateOverride({ tenantId, log = console } = {}) {
  if (!SCHED_NOTIF_TEMPLATE_TABLE || !tenantId) return null;
  try {
    const res = await ddb.send(new GetItemCommand({
      TableName: SCHED_NOTIF_TEMPLATE_TABLE,
      Key: { tenantId: { S: String(tenantId) }, moment: { S: 'confirmation' } },
    }));
    const it = res.Item;
    if (!it) return null;
    const s = (a) => (a && typeof a.S === 'string' ? a.S : undefined);
    return { subject: s(it.subject), text: s(it.body_text), html: s(it.body_html) };
  } catch (err) {
    // err.name (not .message): SDK messages can embed the role/table ARN.
    (log || console).warn(`[confirmation] template override load failed: ${err.name || 'error'} (using default copy)`);
    return null;
  }
}

/**
 * The EDITABLE region is only the greeting/confirmation copy (override field → else the
 * shared CONFIRMATION_DEFAULTS, byte-in-sync with the ADA editor). The join link, the
 * signed reschedule/cancel links, and the org sign-off are appended OUTSIDE it —
 * compliance/functionality invariant (like STOP): no override can drop them. The .ics
 * attachment is likewise untouched by overrides (assembled in sendConfirmationEmail).
 */
function buildBodies({ firstName, orgName, apptTypeName, whenLabel, joinUrl, rescheduleUrl, cancelUrl, templateOverride }) {
  const vars = {
    firstName: firstName || 'there',
    org: orgName || 'the team',
    apptType: apptTypeName || 'appointment',
    whenLabel: whenLabel || '',
  };
  const htmlVars = {
    firstName: escapeHtml(vars.firstName),
    org: escapeHtml(vars.org),
    apptType: escapeHtml(vars.apptType),
    whenLabel: escapeHtml(vars.whenLabel),
  };
  const pickField = (k) =>
    templateOverride && typeof templateOverride[k] === 'string' && templateOverride[k].trim()
      ? templateOverride[k]
      : CONFIRMATION_DEFAULTS[k];

  const subject = stripCrlf(renderVars(pickField('subject'), vars));
  const editableText = renderVars(pickField('text'), vars);
  const editableHtml = renderVars(pickField('html'), htmlVars);

  const textBody = [
    editableText,
    '',
    ...(joinUrl ? [`Join: ${joinUrl}`] : []),
    `Reschedule: ${rescheduleUrl}`,
    `Cancel: ${cancelUrl}`,
    '',
    `— ${orgName || 'MyRecruiter'}`,
  ].join('\n');

  const htmlBody = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
${editableHtml}
${joinUrl ? `<p><a href="${escapeHtml(joinUrl)}">Join the meeting</a></p>` : ''}
<p>
  <a href="${escapeHtml(rescheduleUrl)}">Reschedule</a> &nbsp;|&nbsp;
  <a href="${escapeHtml(cancelUrl)}">Cancel</a>
</p>
<p style="color:#999;font-size:12px;">&mdash; ${escapeHtml(orgName) || 'the team'}</p>
</body></html>`;

  return { subject, textBody, htmlBody };
}

// ─── public: send the confirmation email ─────────────────────────────────────────────

/**
 * sendConfirmationEmail(args, opts) — assemble + SES SendRawEmail.
 *   args = {
 *     tenantId, bookingId, attendeeEmail, attendeeFirstName,
 *     appointmentTypeName, orgName, coordinatorName, coordinatorEmail,
 *     start, end, whenLabel, joinUrl, deepLink,
 *     startAt, cancellationWindowHours
 *   }
 *   opts = { signOpts } — passed through to tokens.sign (test key injection).
 * Returns { messageId, rescheduleUrl, cancelUrl }.
 */
async function sendConfirmationEmail(args, opts = {}) {
  const {
    tenantId, bookingId, attendeeEmail, attendeeFirstName,
    appointmentTypeName, orgName, coordinatorName, coordinatorEmail,
    start, end, whenLabel, joinUrl, deepLink,
    startAt, cancellationWindowHours,
  } = args;

  if (!attendeeEmail) throw new Error('attendeeEmail is required for the confirmation email');

  const { cancelUrl, rescheduleUrl } = await buildActionLinks(
    { tenantId, bookingId, startAt: startAt || start, cancellationWindowHours },
    opts.signOpts
  );

  const summary = `${appointmentTypeName || 'Appointment'}${attendeeFirstName ? ` — ${attendeeFirstName}` : ''}`;
  const ics = buildIcs({
    bookingId,
    summary,
    description: deepLink ? `Manage this booking: ${deepLink}` : '',
    location: joinUrl || '',
    start,
    end,
    organizerEmail: coordinatorEmail,
    attendeeEmail,
    // dtstamp defaults to now() inside buildIcs (RFC 5545 — not the event start).
  });

  // §E14 S4c: per-tenant override (fail-safe → shared defaults). DI via
  // opts.loadTemplateOverride for tests; a throwing injected loader also degrades.
  let templateOverride = null;
  try {
    templateOverride = await (opts.loadTemplateOverride || loadTemplateOverride)({ tenantId, log: opts.log });
  } catch (err) {
    (opts.log || console).warn(`[confirmation] template override load threw: ${err.name || 'error'} (using default copy)`);
  }

  const { subject, textBody, htmlBody } = buildBodies({
    firstName: attendeeFirstName,
    orgName,
    apptTypeName: appointmentTypeName,
    whenLabel,
    joinUrl,
    rescheduleUrl,
    cancelUrl,
    templateOverride,
  });

  const raw = buildRawMime({
    from: FROM_EMAIL,
    to: attendeeEmail,
    subject,
    textBody,
    htmlBody,
    icsContent: ics,
    icsFilename: 'invite.ics',
  });

  const res = await ses.send(new SendRawEmailCommand({
    Source: FROM_EMAIL,
    Destinations: [stripCrlf(attendeeEmail)],
    RawMessage: { Data: Buffer.from(raw, 'utf8') },
    ConfigurationSetName: CONFIG_SET,
    Tags: [
      { Name: 'tenant_id', Value: String(tenantId || 'unknown').slice(0, 256) },
      { Name: 'email_type', Value: 'booking_confirmation' },
    ],
  }));

  return { messageId: res.MessageId, rescheduleUrl, cancelUrl };
}

module.exports = {
  sendConfirmationEmail,
  buildIcs,
  buildActionLinks,
  buildRawMime,
  buildBodies,
  loadTemplateOverride,
  renderVars,
  escapeHtml,
  escapeIcsText,
  stripCrlf,
  toIcsUtc,
};
