/**
 * Scheduled_Message_Sender Lambda
 *
 * Triggered by EventBridge Scheduler one-time schedules to send appointment
 * reminders, attendance checks, and other time-based messages.
 *
 * Flow:
 * 1. EventBridge Scheduler fires at the scheduled time with { pk, sk, message_id }
 * 2. This Lambda reads the message record from picasso-scheduled-messages
 * 3. Checks status (skip if cancelled/sent — defence-in-depth vs a surviving rule)
 * 4. Resolves the send channels at FIRE TIME:
 *    - When the row carries the §E3 gate context (tenant_prefs) AND a selectChannels gate
 *      is wired, call it: EMAIL is the floor (always); SMS is the opt-in supplement
 *      (org-flag && live consent && !quiet-hours, computed from the row's timezone NOW).
 *    - Otherwise (legacy single-channel rows) fall back to the row's `channel` + the
 *      bare fail-closed consent check.
 * 5. Sends email via send_email (the §E1 email-as-floor branch) and/or SMS via SMS_Sender.
 * 6. Updates status to 'sent' / 'suppressed' / 'failed'.
 *
 * The schedules + rows are created by Reminder_Scheduler (WS-E-REMIND, §E1). This Lambda
 * only CONSUMES them. selectChannels (§E3) is produced by WS-E-TCPA and injected here by
 * the integrator once it merges — until then SMS is fail-closed and only the email floor
 * sends (a TCPA-safe default).
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
// §E3 TCPA gate (WS-E-TCPA, FROZEN) + the shared E.164 normalizer (single source of truth —
// keys the consent lookup identically to the writer). Both are PURE (no top-level @aws-sdk),
// so this reader-only Lambda imports them without dragging the consent WRITER's AWS deps in.
// This Lambda is esbuild-bundled (the raw-zip deploy can't see ../shared) — see esbuild.config.mjs.
import { selectChannels } from '../shared/scheduling/channels.js';
import { toE164 } from '../shared/scheduling/phone.js';
// §E14 single-source defaults + compliance strings (extraction — was a local copy with
// regex parity tests; now structural):
import {
  REMINDER_TEMPLATES,
  STOP_LINE_TEXT,
  STOP_LINE_HTML,
  SMS_STOP_FOOTER,
  appendStopOnce,
} from '../shared/scheduling/notif-defaults.js';
// Single-source {{var}} substitution for editor-authored copy (unknown → '').
// NB: `renderTemplate` below is the DIFFERENT baked-row renderer and stays local.
import { render } from '../shared/scheduling/render.js';

const region = process.env.AWS_REGION || 'us-east-1';
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);
const lambdaClient = new LambdaClient({ region });

const SCHEDULED_MESSAGES_TABLE = process.env.SCHEDULED_MESSAGES_TABLE || 'picasso-scheduled-messages';
const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';
const SMS_SENDER_FUNCTION = process.env.SMS_SENDER_FUNCTION || 'SMS_Sender';
const SEND_EMAIL_FUNCTION = process.env.SEND_EMAIL_FUNCTION || 'send_email';
// §E14 S4b: per-tenant template-override table. Deliberately '' (not a default name) when
// the env is absent — the override read is skipped entirely until the IaC grant+env land,
// so the code degrades to the local default copy instead of erroring on a denied GetItem.
const SCHED_NOTIF_TEMPLATE_TABLE = process.env.SCHED_NOTIF_TEMPLATE_TABLE || '';

/**
 * Bare fail-closed consent check (legacy single-channel rows only — the §E3
 * selectChannels gate supersedes this for reminder rows that carry tenant_prefs).
 */
async function checkConsent(ddb, tenantId, phoneRaw) {
  // Normalize to E.164 so the key matches the writer's (a bare 10-digit row-phone would
  // otherwise miss the +1-prefixed consent record and silently suppress a legit SMS).
  const phoneE164 = toE164(phoneRaw);
  if (!phoneE164) return false;
  try {
    const result = await ddb.send(new GetCommand({
      TableName: SMS_CONSENT_TABLE,
      Key: { pk: `TENANT#${tenantId}`, sk: `CONSENT#transactional#${phoneE164}` },
    }));
    if (!result.Item) return false; // No consent record = no consent = no send
    if (result.Item.consent_given === false || result.Item.opted_out_at) return false;
    return true;
  } catch (error) {
    console.error(`Consent check failed: ${error.name || 'error'}`);
    return false; // Fail closed
  }
}

/**
 * Read the recipient's SMS consent RECORD for the §E3 gate (selectChannels is PURE — it
 * needs the record passed in). Keyed on the SAME picasso-sms-consent key the consent.js
 * writer + SMS_Sender use (pk=TENANT#{tenantId}, sk=CONSENT#transactional#{E.164}), via the
 * shared toE164 so the lookup matches the writer exactly. Document client → plain values.
 * FAIL-SAFE: a bad phone / missing record / DDB error returns null → consentValid() reads
 * "no consent" → SMS suppressed, the email floor still sends.
 * @returns {Promise<{consent_given: boolean, opted_out_at?: string} | null>}
 */
async function readConsentRecord(ddb, tenantId, phoneRaw, logger) {
  const phoneE164 = toE164(phoneRaw);
  if (!tenantId || !phoneE164) return null;
  try {
    const result = await ddb.send(new GetCommand({
      TableName: SMS_CONSENT_TABLE,
      Key: { pk: `TENANT#${tenantId}`, sk: `CONSENT#transactional#${phoneE164}` },
    }));
    if (!result.Item) return null;
    return {
      consent_given: result.Item.consent_given === true,
      // ?? (not ||): pass a real opt-out timestamp through; coerce only DDB NULL→null to
      // undefined. || would also swallow a wrong-typed falsy (e.g. "" / false) as "not opted
      // out" — ?? preserves it so consentValid (!opted_out_at) still fail-closes correctly.
      opted_out_at: result.Item.opted_out_at ?? undefined,
    };
  } catch (error) {
    (logger || console).error(`consent record read failed: ${error.name || 'error'}`);
    return null; // fail-safe
  }
}

/**
 * Resolve which channels to send on at fire time.
 * @returns {Promise<{ email: boolean, sms: boolean }>}
 */
async function resolveChannels(message, deps) {
  const tenantId = message.tenant_id;
  // 'sms' default is the legacy pre-§E3 contract (single-channel rows whose intent WAS sms);
  // every scheduler-written row sets channel:'email' explicitly, so this only governs rows
  // that predate the field. Do not "fix" to 'email' — it would silently re-channel them.
  const channel = message.channel || 'sms';

  // §E3 fire-time gate path — only when the row carries the gate context AND a gate is wired.
  if (deps.selectChannels && message.tenant_prefs) {
    try {
      // Capture the fire instant BEFORE the consent I/O — quiet-hours is TCPA-sensitive, so a
      // slow GetItem must not drift fireTime past the 8pm/8am boundary into a wrong decision.
      const fireTime = deps.now();
      // org-level toggle from the row snapshot (captured at schedule-creation).
      const orgSmsEnabled = message.tenant_prefs?.notificationPrefs?.sms === true;
      // Only the consent read is I/O — skip it when SMS can't win anyway (org off / no phone):
      // fail-closed + avoids a needless GetItem on every email-only reminder.
      const consentRecord =
        orgSmsEnabled && message.recipient_phone
          ? await readConsentRecord(deps.ddb, tenantId, message.recipient_phone, deps.logger)
          : null;
      // selectChannels is PURE: pass the REAL fire instant + booking.timezone; it does the
      // volunteer-local quiet-hours conversion itself (§E3 SEAM-1, fixed 8pm–8am).
      const result = await deps.selectChannels({
        tenantId,
        booking: { timezone: message.timezone },
        orgSmsEnabled,
        consentRecord,
        fireTime,
      });
      // Email is the floor — always true per §E3, but never email a row with no recipient.
      return {
        email: result.email !== false && !!message.recipient_email,
        sms: result.sms === true && !!message.recipient_phone,
      };
    } catch (err) {
      // Gate error → email floor still sends; SMS fails closed.
      deps.logger.error(`selectChannels failed (${message.message_id}): ${err.message}`);
      return { email: !!message.recipient_email, sms: false };
    }
  }

  // Legacy fall-back: single channel from the row; SMS gated by the bare consent check.
  if (channel === 'email') {
    return { email: !!message.recipient_email, sms: false };
  }
  const hasConsent = await checkConsent(deps.ddb, tenantId, message.recipient_phone);
  return { email: false, sms: hasConsent && !!message.recipient_phone };
}

function renderTemplate(template, variables) {
  let result = template;
  for (const [key, value] of Object.entries(variables)) {
    result = result.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), String(value ?? ''));
  }
  return result;
}

// ─── §E14 S4b: fire-time reminder template overrides ──────────────────────────────────
// The t24h/t1h reminder copy is per-tenant editable via the ADA §E14 editor
// (picasso-scheduling-notif-template, key {tenantId, moment}). At fire time this Lambda
// resolves: tenant override field → else the LOCAL default below. The defaults MUST stay
// byte-in-sync with ADA `_SCHED_NOTIF_DEFAULTS` / `_SCHED_NOTIF_SMS_DEFAULTS` (the editor's
// reset/preview) — index.test.mjs has a parity test that reads the ADA source, mirroring
// shared/scheduling/__tests__/notify-sms-parity.test.js. t4h / t15m / attendance / legacy
// rows are NOT overridable in v1 and keep the row's baked body.
// The small helpers (escapeHtml / render / STOP footer) are duplicated from
// shared/scheduling/notify.js rather than imported — notify.js carries top-level AWS
// clients and this bundled Lambda only imports PURE shared modules (see header). The
// parity test pins the footer to notify's export so they cannot drift.

// REMINDER_TEMPLATES: imported from shared/scheduling/notif-defaults.js.

// TCPA/CTIA compliance footers — appended AFTER render in sendSms/sendEmail, structurally
// OUTSIDE the editable override body, so a tenant override can never remove them; the marker
// check means an override that already carries STOP is not double-footed. All three strings
// and the marker regex are byte-pinned to notify.js by the parity tests. The email lines are
// appended only to ATTENDEE-facing reminder emails — the attendance check is a coordinator
// operational prompt (staff can't opt out of disposition asks), and notify.js owns its own
// kinds.
// Footers + appendStopOnce: imported from shared/scheduling/notif-defaults.js.

// Subjects feed an email header — strip CRLF so a hostile override/org-name can neither
// inject headers nor crash send_email's MIME assembly (mirrors confirmation-email.js).
function stripCrlf(s) {
  return String(s ?? '').replace(/[\r\n]+/g, ' ').trim();
}

// FIX 9: scheme guard — only https:// URLs are emitted as <a href> or plain-text links.
// Blocks javascript:/data: XSS in join_url/reschedule_url/cancel_url (mirrored from
// confirmation-email.js isHttpsUrl).
function isHttpsUrl(u) {
  return typeof u === 'string' && /^https:\/\//i.test(u);
}

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[c]));
}

/**
 * G1: build the action block appended to attendee-facing reminders OUTSIDE the editable
 * body. Mirrors the confirmation-email.js discipline — the consumer, not the template,
 * owns this region, so an override can neither remove nor duplicate these lines.
 *
 * Old-shape rows (no link fields) → all values are falsy → returns '' for all formats.
 * STOP footer is still appended after this block (STOP stays last — see sendEmail/sendSms).
 */
function buildActionBlock(message) {
  const whenLabel = message.when_label || '';
  // FIX 9: apply https gate to all bearer href fields — blocks javascript:/data: XSS.
  // Only present-AND-https URLs are rendered as links (plain text or <a href>).
  const joinUrl = isHttpsUrl(message.join_url) ? message.join_url : '';
  const rescheduleUrl = isHttpsUrl(message.reschedule_url) ? message.reschedule_url : '';
  const cancelUrl = isHttpsUrl(message.cancel_url) ? message.cancel_url : '';
  // No fields present → no block (old-shape row, or minting failed at schedule-create,
  // or a javascript: URL was rejected by the scheme guard above).
  if (!whenLabel && !joinUrl && !rescheduleUrl && !cancelUrl) {
    return { text: '', html: '', sms: '' };
  }
  const textLines = [];
  if (whenLabel) textLines.push(`When: ${whenLabel}`);
  if (joinUrl) textLines.push(`Join: ${joinUrl}`);
  if (rescheduleUrl) textLines.push(`Reschedule: ${rescheduleUrl}`);
  if (cancelUrl) textLines.push(`Cancel: ${cancelUrl}`);

  const htmlParts = [];
  if (whenLabel) htmlParts.push(`<p>When: ${escapeHtml(whenLabel)}</p>`);
  // joinUrl already scheme-gated above; escapeHtml still applied for entity safety.
  if (joinUrl) htmlParts.push(`<p><a href="${escapeHtml(joinUrl)}">Join the meeting</a></p>`);
  if (rescheduleUrl) htmlParts.push(`<a href="${escapeHtml(rescheduleUrl)}">Reschedule</a>`);
  if (cancelUrl) {
    const sep = rescheduleUrl ? ' &nbsp;|&nbsp; ' : '';
    htmlParts.push(`${sep}<a href="${escapeHtml(cancelUrl)}">Cancel</a>`);
  }

  const smsLinks = [];
  if (rescheduleUrl) smsLinks.push(`Reschedule: ${rescheduleUrl}`);
  if (cancelUrl) smsLinks.push(`Cancel: ${cancelUrl}`);
  if (joinUrl) smsLinks.push(`Join: ${joinUrl}`);

  // FIX 11 (comment only — soak watch-item, NO behavior change):
  // Two HS256 token URLs (reschedule + cancel) + a join URL can push this SMS body to
  // ~6-7 segments once the STOP footer rides last (appendStopOnce in sendSms appends it).
  // If carriers (Telnyx) start filtering or truncating multi-segment URL SMS, the
  // follow-up is a short-link service (e.g. Dub) or email-only action links. Do NOT
  // reduce the URL count here — the plan's done-bar explicitly includes time + join +
  // reschedule + cancel in SMS. This is intentionally a SOAK WATCH-ITEM only.

  return {
    text: textLines.length ? '\n\n' + textLines.join('\n') : '',
    html: htmlParts.length ? '\n' + htmlParts.join('\n') : '',
    sms: smsLinks.length ? ' ' + smsLinks.join(' ') : '',
  };
}

/**
 * Map a scheduled-message row to its §E14 overridable moment, or null when the row's copy
 * is not editable (t4h/t15m tiers, attendance checks, legacy rows without a tier).
 */
function reminderMomentFromRow(message) {
  if (message.moment !== 'reminder' || message.attendance_check) return null;
  if (message.tier === 't24h') return 'reminder_24h';
  if (message.tier === 't1h') return 'reminder_1h';
  return null;
}

/**
 * §E14 override loader — GetItem on the scheduling-notif-template table. FAIL-SAFE: unset
 * table (IaC not applied yet) / miss / error → null, so a template-store problem can never
 * block a reminder send (mirrors notify.js defaultLoadTemplateOverride; DocumentClient here).
 */
async function loadTemplateOverride({ tenantId, moment, ddb, logger }) {
  if (!SCHED_NOTIF_TEMPLATE_TABLE || !tenantId || !moment) return null;
  try {
    const result = await ddb.send(new GetCommand({
      TableName: SCHED_NOTIF_TEMPLATE_TABLE,
      Key: { tenantId: String(tenantId), moment },
    }));
    const it = result.Item;
    if (!it) return null;
    const s = (v) => (typeof v === 'string' ? v : undefined);
    // On/off toggle; absent → enabled (a disabled moment has an explicit stored row).
    return { subject: s(it.subject), text: s(it.body_text), html: s(it.body_html), sms: s(it.sms_text), enabled: it.enabled !== false };
  } catch (error) {
    // error.name (not .message): SDK messages can embed the role ARN / account id / table
    // ARN — keep CloudWatch reconnaissance-free; the code (e.g. AccessDeniedException) is
    // the actionable part.
    (logger || console).warn(`template override load failed moment=${moment}: ${error.name || 'error'} (using default copy)`);
    return null;
  }
}

/**
 * Resolve the effective {subject,text,html,sms} for an overridable reminder moment:
 * per-field override-wins merge (mirrors notify.js mergeNoticeTemplate), then {{var}}
 * render — plain values into subject/text/sms, HTML-escaped values into html (firstName
 * is attendee-supplied; unescaped it would be an HTML-injection vector in the email body).
 */
function buildReminderContent(moment, templateVars, override, whenLabel = '') {
  const tpl = REMINDER_TEMPLATES[moment];
  const pickField = (k) =>
    override && typeof override[k] === 'string' && override[k].trim() ? override[k] : tpl[k];
  const vars = {
    firstName: templateVars.first_name || '',
    org: templateVars.organization_name || '',
    apptType: templateVars.appointment_type || '',
    // Universal context tokens: whenLabel from the row (message.when_label); programName
    // from template_vars (stamped at commit). Absent → '' (the §E14 unknown-var contract).
    whenLabel: whenLabel || '',
    programName: templateVars.program_name || '',
  };
  const htmlVars = {
    firstName: escapeHtml(vars.firstName),
    org: escapeHtml(vars.org),
    apptType: escapeHtml(vars.apptType),
    whenLabel: escapeHtml(vars.whenLabel),
    programName: escapeHtml(vars.programName),
  };
  return {
    subject: render(pickField('subject'), vars),
    text: render(pickField('text'), vars),
    html: render(pickField('html'), htmlVars),
    sms: render(pickField('sms'), vars),
  };
}

async function updateMessageStatus(ddb, pk, sk, status, error = '') {
  const now = new Date().toISOString();
  const base = status === 'cancelled'
    ? 'SET #status = :status, cancelled_at = :now, updated_at = :now'
    : 'SET #status = :status, updated_at = :now';
  const values = { ':status': status, ':now': now };
  if (error) values[':error'] = error;
  await ddb.send(new UpdateCommand({
    TableName: SCHEDULED_MESSAGES_TABLE,
    Key: { pk, sk },
    UpdateExpression: error ? `${base}, error_detail = :error` : base,
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: values,
  }));
}

// Invoke send_email (SES) — the §E1 email-as-floor branch. send_email reads `event.body`
// as a JSON string (API-Gateway-shaped), so wrap accordingly (mirrors notify.js).
async function sendEmail(deps, message, content) {
  // Attendee-facing reminder emails carry the action block + unsubscribe line OUTSIDE the
  // editable body. The action block (G1 when/join/reschedule/cancel) is appended first,
  // then appendStopOnce ensures STOP stays last. Coordinator attendance prompts get neither.
  const attendeeFacing = message.moment === 'reminder' && !message.attendance_check;
  let textBody = content.text;
  let htmlBody = content.html;
  if (attendeeFacing) {
    const actionBlock = buildActionBlock(message);
    // Append action block (may be '' for old-shape rows), then STOP (always last).
    textBody = appendStopOnce(textBody + actionBlock.text, STOP_LINE_TEXT);
    htmlBody = appendStopOnce(htmlBody + actionBlock.html, STOP_LINE_HTML);
  }
  const inner = {
    to: [message.recipient_email],
    subject: stripCrlf(content.subject),
    text_body: textBody,
    html_body: htmlBody,
    tags: {
      tenant_id: String(message.tenant_id || 'unknown').slice(0, 256),
      email_type: 'scheduled_reminder',
    },
  };
  await deps.lambda.send(new InvokeCommand({
    FunctionName: SEND_EMAIL_FUNCTION,
    InvocationType: 'Event',
    Payload: Buffer.from(JSON.stringify({ body: JSON.stringify(inner) })),
  }));
}

async function sendSms(deps, message, content) {
  // G1: append the action block for attendee-facing reminders before STOP (STOP stays last).
  const attendeeFacing = message.moment === 'reminder' && !message.attendance_check;
  let smsBody = content.sms;
  if (attendeeFacing) {
    const actionBlock = buildActionBlock(message);
    smsBody = smsBody + actionBlock.sms; // may be '' → no change for old-shape rows
  }
  await deps.lambda.send(new InvokeCommand({
    FunctionName: SMS_SENDER_FUNCTION,
    InvocationType: 'Event',
    Payload: Buffer.from(JSON.stringify({
      // E.164-normalize the send target so it matches the consent-lookup key — SMS_Sender
      // rejects a bare 10-digit number, and the Event invoke would swallow that silently.
      to: toE164(message.recipient_phone) || message.recipient_phone,
      // The STOP/HELP footer rides OUTSIDE the (possibly tenant-overridden) body — §E3/§E14
      // compliance invariant: an override can neither remove nor duplicate it.
      body: appendStopOnce(smsBody, SMS_STOP_FOOTER),
      tenantId: message.tenant_id,
      formId: message.template || 'scheduled',
      submissionId: message.appointment_id || message.message_id,
      sessionId: '',
      type: 'reminder',
      sendType: 'contact', // activates the shipped SMS_Sender consent gate (§E3)
      fromNumber: message.from_number || '',
    })),
  }));
}

/**
 * Testable dispatch core. `handler` wraps this with the real AWS clients.
 */
export async function dispatch(event, deps) {
  const { pk, sk, message_id } = event;
  if (!pk || !sk) {
    deps.logger.error('❌ Missing pk/sk in event payload');
    return { success: false, error: 'missing_keys' };
  }

  let message;
  try {
    const result = await deps.ddb.send(new GetCommand({
      TableName: SCHEDULED_MESSAGES_TABLE,
      Key: { pk, sk },
    }));
    message = result.Item;
  } catch (error) {
    deps.logger.error('❌ Failed to read scheduled message:', error);
    return { success: false, error: 'read_failed' };
  }

  if (!message) {
    deps.logger.warn(`⚠️ Scheduled message not found: ${pk} / ${sk}`);
    return { success: false, error: 'not_found' };
  }

  // Status-gate — skip if not pending (defence-in-depth vs a surviving rule, §E1).
  if (message.status !== 'pending') {
    deps.logger.log(`ℹ️ Message ${message_id} is '${message.status}' — skipping`);
    return { success: true, skipped: true, reason: message.status };
  }

  const tenantId = message.tenant_id;
  const templateVars = message.template_vars || {};
  const renderedBody = message.body
    ? renderTemplate(message.body, templateVars)
    : `Reminder from ${templateVars.organization_name || tenantId}`;

  // §E14 S4b: overridable moments (t24h/t1h) render override-or-default at fire time —
  // the ADA editor's effective copy. Everything else keeps the row's baked body. The
  // loader is fail-safe (null on miss/error/unset table) and additionally guarded here
  // so a throwing injected loader still degrades to the default copy, never a failed send.
  let content = null;
  const overrideMoment = reminderMomentFromRow(message);
  if (overrideMoment) {
    let override = null;
    try {
      override = deps.loadTemplateOverride
        ? await deps.loadTemplateOverride({ tenantId, moment: overrideMoment, ddb: deps.ddb, logger: deps.logger })
        : null;
    } catch (error) {
      deps.logger.warn(`template override load threw (${message_id}): ${error.message} (using default copy)`);
    }
    // On/off toggle: an admin who turned this reminder OFF in "Messages we send" → suppress it.
    // Fail-safe: a null override (miss/error) → send the default.
    if (override && override.enabled === false) {
      deps.logger.log(`🚫 Moment ${overrideMoment} disabled by tenant — suppressing scheduled message ${message_id}`);
      await updateMessageStatus(deps.ddb, pk, sk, 'suppressed');
      return { success: true, suppressed: true };
    }
    content = buildReminderContent(overrideMoment, templateVars, override, message.when_label || '');
  } else {
    content = {
      subject: message.subject || `Reminder from ${templateVars.organization_name || tenantId}`,
      text: renderedBody,
      // escapeHtml: template_vars values land here unescaped otherwise (first_name is
      // attendee-supplied — HTML-injection guard, same as the override path's htmlVars).
      html: `<p>${escapeHtml(renderedBody)}</p>`,
      sms: renderedBody,
    };
  }

  let channels;
  try {
    channels = await resolveChannels(message, deps);
  } catch (error) {
    deps.logger.error(`❌ Channel resolution failed (${message_id}):`, error);
    await updateMessageStatus(deps.ddb, pk, sk, 'failed', error.message);
    return { success: false, error: error.message };
  }

  if (!channels.email && !channels.sms) {
    deps.logger.log(`🚫 No eligible channel — suppressing scheduled message ${message_id}`);
    await updateMessageStatus(deps.ddb, pk, sk, 'suppressed');
    return { success: true, suppressed: true };
  }

  const dispatched = { email: false, sms: false };
  try {
    if (channels.email) {
      await sendEmail(deps, message, content);
      dispatched.email = true;
    }
    if (channels.sms) {
      await sendSms(deps, message, content);
      dispatched.sms = true;
    }
    await updateMessageStatus(deps.ddb, pk, sk, 'sent');
    return { success: true, dispatched, message_id };
  } catch (error) {
    deps.logger.error(`❌ Scheduled message send failed (${message_id}):`, error);
    await updateMessageStatus(deps.ddb, pk, sk, 'failed', error.message);
    return { success: false, error: error.message, dispatched };
  }
}

function defaultDeps() {
  return {
    ddb: dynamodb,
    lambda: lambdaClient,
    now: () => Date.now(),
    logger: console,
    // §E3 TCPA gate wired in (S3). Reminder rows that carry tenant_prefs now go through the
    // real channel selection: email floor + org-flag/consent/quiet-hours-gated SMS supplement.
    selectChannels,
    // §E14 S4b: fire-time per-tenant template override read (fail-safe → default copy).
    loadTemplateOverride,
  };
}

// exported for unit coverage:
export {
  reminderMomentFromRow,
  loadTemplateOverride,
  buildReminderContent,
  buildActionBlock,
  REMINDER_TEMPLATES,
  SMS_STOP_FOOTER,
  STOP_LINE_TEXT,
  STOP_LINE_HTML,
};

/**
 * Lambda handler — invoked by EventBridge Scheduler with { pk, sk, message_id }.
 */
export async function handler(event) {
  return dispatch(event, defaultDeps());
}
