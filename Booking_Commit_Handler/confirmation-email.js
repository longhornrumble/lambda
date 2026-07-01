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
if (!process.env.SES_FROM_EMAIL) {
  console.warn('SENDER_ENV_MISSING — using hardcoded fallback notify@myrecruiter.ai; set SES_FROM_EMAIL');
}
const CONFIG_SET = process.env.SES_CONFIGURATION_SET || 'picasso-emails';
// §13.8 — token action endpoints live on the greenfield schedule.* host.
const SCHEDULE_BASE_URL = process.env.SCHEDULE_BASE_URL || 'https://schedule.myrecruiter.ai';
// §E14 S4c: per-tenant confirmation template overrides. Already wired on BCH (G6 grant
// DDBReadSchedulingNotifTemplate + env). Unset/empty → override system off, defaults send.
const SCHED_NOTIF_TEMPLATE_TABLE = process.env.SCHED_NOTIF_TEMPLATE_TABLE || '';

// ─── sanitization helpers ──────────────────────────────────────────────────────────

// FIX 9: scheme guard — only https:// URLs are emitted as <a href> or plain-text links.
// Blocks javascript:/data: XSS vectors in joinUrl/rescheduleUrl/cancelUrl.
function isHttpsUrl(u) {
  return typeof u === 'string' && /^https:\/\//i.test(u);
}

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
// FIX 7: escape ALL line-break forms (CR+LF, lone CR, lone LF) so attacker-controlled
// agenda / attendee name values cannot inject ICS properties via a bare 0x0D.
function escapeIcsText(value) {
  if (value == null) return '';
  return String(value)
    .replace(/\\/g, '\\\\')
    .replace(/;/g, '\\;')
    .replace(/,/g, '\\,')
    .replace(/\r\n|\r|\n/g, '\\n');
}

// ─── .ics generation ────────────────────────────────────────────────────────────────

// ISO8601 → ICS UTC stamp (YYYYMMDDTHHMMSSZ).
function toIcsUtc(iso) {
  const d = new Date(iso);
  return `${d.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z')}`;
}

function buildIcs({ bookingId, summary, description, location, start, end, organizerEmail, attendeeEmail, dtstamp, sequence = 0 }) {
  // RFC 5545 §3.8.7.4: SEQUENCE is a non-negative integer; an UPDATE to an existing
  // event (same UID) MUST carry a higher SEQUENCE than the revision the client already
  // holds, or the client ignores it. Commit = 0; each reschedule passes the booking's
  // bumped ics_sequence so the re-sent invite updates in place. Clamp defensively.
  const seq = Number.isInteger(sequence) && sequence >= 0 ? sequence : 0;
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//MyRecruiter//Scheduling//EN',
    'METHOD:REQUEST',
    'BEGIN:VEVENT',
    `UID:${escapeIcsText(bookingId)}@myrecruiter.ai`,
    // RFC 5545 §3.8.7.2: DTSTAMP is when the iCalendar object was CREATED (now),
    // NOT the event start.
    `DTSTAMP:${toIcsUtc(dtstamp || new Date().toISOString())}`,
    `DTSTART:${toIcsUtc(start)}`,
    `DTEND:${toIcsUtc(end)}`,
    `SEQUENCE:${seq}`,
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

/**
 * A `From` header that reads as the org while the mailbox stays our verified sender:
 * `"Atlanta Angels" <notify@myrecruiter.ai>`. The display name is RFC 2047 encoded when it has
 * non-ASCII, quoted (with \ and " escaped) when plain ASCII, and dropped entirely when blank —
 * so the address alone (a verified SES identity) is always what SES validates against.
 */
function formatFromHeader(name, email) {
  const addr = stripCrlf(email);
  const raw = stripCrlf(name || '').trim();
  if (!raw) return addr;
  if (/^[\x20-\x7E]*$/.test(raw)) {
    return `"${raw.replace(/([\\"])/g, '\\$1')}" <${addr}>`;
  }
  return `=?UTF-8?B?${Buffer.from(raw, 'utf8').toString('base64')}?= <${addr}>`;
}

function buildRawMime({ from, to, subject, textBody, htmlBody, icsContent, icsFilename, replyTo }) {
  const boundaryMixed = `mixed_${crypto.randomBytes(12).toString('hex')}`;
  const boundaryAlt = `alt_${crypto.randomBytes(12).toString('hex')}`;
  const safeSubject = stripCrlf(subject);
  const safeTo = stripCrlf(to);
  const safeFrom = stripCrlf(from);
  const safeReplyTo = replyTo ? stripCrlf(replyTo) : '';
  const icsB64 = Buffer.from(icsContent, 'utf8').toString('base64');

  return [
    `From: ${safeFrom}`,
    ...(safeReplyTo ? [`Reply-To: ${safeReplyTo}`] : []),
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
    // 8bit (not 7bit): §E14 overrides are tenant-authored UTF-8 (emoji, accents) — a
    // 7bit declaration over multi-byte content gets mangled/rejected by strict gateways.
    'Content-Transfer-Encoding: 8bit',
    '',
    textBody,
    '',
    `--${boundaryAlt}`,
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
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
    // `enabled` is the on/off toggle; absent → enabled (a moment turned off has a stored row).
    return {
      subject: s(it.subject), text: s(it.body_text), html: s(it.body_html),
      enabled: !(it.enabled && it.enabled.BOOL === false),
    };
  } catch (err) {
    // err.name (not .message): SDK messages can embed the role/table ARN.
    (log || console).warn(`[confirmation] template override load failed: ${err.name || 'error'} (using default copy)`);
    return null;
  }
}

/**
 * Sanitize the override-supplied html region so it cannot interfere with the appended
 * action-links block (the greeting region has no legitimate need for any of these):
 *  - HTML comments (incl. an UNCLOSED `<!--` that would swallow the appended links),
 *  - <style> blocks (could display:none the real links),
 *  - <a> opening tags (an admin-authored fake cancel/reschedule link could phish the
 *    volunteer's signed-token click; inner text is preserved, stray </a> is harmless).
 * Deliberately stricter than the notify.js §E14 kinds: the confirmation carries signed
 * action tokens, so the editable region is copy-only here.
 */
function sanitizeOverrideHtml(s) {
  return String(s)
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<!--[\s\S]*$/, '')
    .replace(/<style[\s\S]*?(?:<\/style>|$)/gi, '')
    .replace(/<a\s[^>]*>/gi, '');
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
  // sanitize is a no-op for the shared default (no comments/styles/links by construction).
  const editableHtml = sanitizeOverrideHtml(renderVars(pickField('html'), htmlVars));

  // FIX 9 + FIX 10: only render links when present AND https (blocks javascript:/data: XSS
  // and suppresses empty "Reschedule: " / empty <a href=""> lines).
  const textBody = [
    editableText,
    '',
    ...(isHttpsUrl(joinUrl) ? [`Join: ${joinUrl}`] : []),
    ...(isHttpsUrl(rescheduleUrl) ? [`Reschedule: ${rescheduleUrl}`] : []),
    ...(isHttpsUrl(cancelUrl) ? [`Cancel: ${cancelUrl}`] : []),
    '',
    `— ${vars.org}`,
  ].join('\n');

  const htmlBody = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
${editableHtml}
${isHttpsUrl(joinUrl) ? `<p><a href="${escapeHtml(joinUrl)}">Join the meeting</a></p>` : ''}
${(isHttpsUrl(rescheduleUrl) || isHttpsUrl(cancelUrl)) ? `<p>
  ${isHttpsUrl(rescheduleUrl) ? `<a href="${escapeHtml(rescheduleUrl)}">Reschedule</a>` : ''}${isHttpsUrl(rescheduleUrl) && isHttpsUrl(cancelUrl) ? ' &nbsp;|&nbsp; ' : ''}${isHttpsUrl(cancelUrl) ? `<a href="${escapeHtml(cancelUrl)}">Cancel</a>` : ''}
</p>` : ''}
<p style="color:#999;font-size:12px;">&mdash; ${htmlVars.org}</p>
</body></html>`;

  return { subject, textBody, htmlBody };
}

// ─── public: send the confirmation email ─────────────────────────────────────────────

/**
 * sendConfirmationEmail(args, opts) — assemble + SES SendRawEmail.
 *   args = {
 *     tenantId, bookingId, attendeeEmail, attendeeFirstName,
 *     appointmentTypeName, orgName, coordinatorEmail,
 *     start, end, whenLabel, joinUrl, deepLink,
 *     startAt, cancellationWindowHours,
 *     agenda?,               // G2: optional plain-text from AppointmentType.agenda
 *   }
 *   (callers may still pass coordinatorName — accepted but unused since §E14 S4c: the
 *   editable body's ADA vocabulary has no {{coordinator}} var; coordinatorEmail is
 *   still the .ics ORGANIZER.)
 *   opts = {
 *     signOpts,              // passed through to tokens.sign (test key injection)
 *     loadTemplateOverride,  // DI seam for the §E14 override loader (tests)
 *     log,                   // logger for the loader's fail-safe warn path
 *   }
 * Returns { messageId, rescheduleUrl, cancelUrl }.
 */
async function sendConfirmationEmail(args, opts = {}) {
  const {
    tenantId, bookingId, attendeeEmail, attendeeFirstName,
    appointmentTypeName, orgName, coordinatorEmail,
    start, end, whenLabel, joinUrl, deepLink,
    startAt, cancellationWindowHours,
    agenda,
    // .ics revision (RFC 5545 SEQUENCE). Commit omits it → 0; reschedule passes the
    // booking's bumped ics_sequence so the re-sent invite updates the entry in place.
    sequence,
    // SES email_type tag — distinguishes the initial booking confirmation from a
    // reschedule confirmation in SES Event Destinations / metrics. Default unchanged.
    emailType = 'booking_confirmation',
  } = args;

  if (!attendeeEmail) throw new Error('attendeeEmail is required for the confirmation email');

  // §E14 S4c: the override read runs IN PARALLEL with the token signing (independent —
  // keeps the worst-case added latency on the 60s commit SLA at zero on the warm path).
  // Fail-safe: any loader rejection (incl. a throwing injected loader) → defaults.
  const [{ cancelUrl, rescheduleUrl }, templateOverride] = await Promise.all([
    buildActionLinks(
      { tenantId, bookingId, startAt: startAt || start, cancellationWindowHours },
      opts.signOpts
    ),
    Promise.resolve()
      .then(() => (opts.loadTemplateOverride || loadTemplateOverride)({ tenantId, log: opts.log }))
      .catch((err) => {
        (opts.log || console).warn(`[confirmation] template override load threw: ${err.name || 'error'} (using default copy)`);
        return null;
      }),
  ]);

  // On/off toggle: an admin who turned the confirmation OFF in "Messages we send" → skip the
  // send (the booking is already committed; this is best-effort). Fail-safe defaults send.
  if (templateOverride && templateOverride.enabled === false) {
    (opts.log || console).info('[confirmation] moment disabled for tenant — skipping send');
    return { messageId: null, skipped: true };
  }

  const summary = `${appointmentTypeName || 'Appointment'}${attendeeFirstName ? ` — ${attendeeFirstName}` : ''}`;
  // G2: compose .ics DESCRIPTION = agenda (when present) + the existing "Manage this
  // booking" line, joined by \n. escapeIcsText in buildIcs handles RFC 5545 escaping.
  // Absent agenda → today's behavior (only the "Manage" line, or empty).
  const icsManageLine = deepLink ? `Manage this booking: ${deepLink}` : '';
  const icsDescription = agenda
    ? [String(agenda), ...(icsManageLine ? [icsManageLine] : [])].join('\n')
    : icsManageLine;
  const ics = buildIcs({
    bookingId,
    summary,
    description: icsDescription,
    location: joinUrl || '',
    start,
    end,
    organizerEmail: coordinatorEmail,
    attendeeEmail,
    sequence, // 0 (commit) or the reschedule's bumped ics_sequence
    // dtstamp defaults to now() inside buildIcs (RFC 5545 — not the event start).
  });

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
    // Show the org as the sender (mailbox stays our verified FROM_EMAIL); replies go to the
    // coordinator hosting the booking — who is already the .ics ORGANIZER above.
    from: formatFromHeader(orgName, FROM_EMAIL),
    replyTo: coordinatorEmail,
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
      // SES tag values are restricted to [A-Za-z0-9_-]; clamp to the known set.
      { Name: 'email_type', Value: emailType === 'reschedule_confirmation' ? 'reschedule_confirmation' : 'booking_confirmation' },
    ],
  }));

  return { messageId: res.MessageId, rescheduleUrl, cancelUrl };
}

module.exports = {
  FROM_EMAIL,
  sendConfirmationEmail,
  buildIcs,
  buildActionLinks,
  buildRawMime,
  buildBodies,
  loadTemplateOverride,
  sanitizeOverrideHtml,
  renderVars,
  escapeHtml,
  escapeIcsText,
  isHttpsUrl,
  stripCrlf,
  toIcsUtc,
  formatFromHeader,
};
