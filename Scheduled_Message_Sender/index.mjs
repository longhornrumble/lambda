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

const region = process.env.AWS_REGION || 'us-east-1';
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);
const lambdaClient = new LambdaClient({ region });

const SCHEDULED_MESSAGES_TABLE = process.env.SCHEDULED_MESSAGES_TABLE || 'picasso-scheduled-messages';
const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';
const SMS_SENDER_FUNCTION = process.env.SMS_SENDER_FUNCTION || 'SMS_Sender';
const SEND_EMAIL_FUNCTION = process.env.SEND_EMAIL_FUNCTION || 'send_email';

/**
 * Bare fail-closed consent check (legacy single-channel rows only — the §E3
 * selectChannels gate supersedes this for reminder rows that carry tenant_prefs).
 */
async function checkConsent(ddb, tenantId, phoneE164) {
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
    console.error('Consent check failed:', error);
    return false; // Fail closed
  }
}

/**
 * "now" in the booking's local timezone, as a Date whose UTC fields carry the local
 * wall clock (so a consumer reads the local hour via getUTCHours()). Computed AT FIRE
 * TIME from the row's snapshotted timezone (§E3: quiet-hours is fire-time, never
 * schedule-creation). Shape is an integration seam confirmed with WS-E-TCPA at weave.
 */
function localNow(nowMs, timezone) {
  try {
    const fmt = new Intl.DateTimeFormat('en-CA', {
      timeZone: timezone || 'UTC',
      hour12: false,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
    const p = Object.fromEntries(fmt.formatToParts(new Date(nowMs)).map((x) => [x.type, x.value]));
    const hh = p.hour === '24' ? '00' : p.hour;
    return new Date(`${p.year}-${p.month}-${p.day}T${hh}:${p.minute}:${p.second}Z`);
  } catch {
    return new Date(nowMs);
  }
}

/**
 * Resolve which channels to send on at fire time.
 * @returns {Promise<{ email: boolean, sms: boolean }>}
 */
async function resolveChannels(message, deps) {
  const tenantId = message.tenant_id;
  const channel = message.channel || 'sms';

  // §E3 fire-time gate path — only when the row carries the gate context AND a gate is wired.
  if (deps.selectChannels && message.tenant_prefs) {
    try {
      const nowLocal = localNow(deps.now(), message.timezone);
      const result = await deps.selectChannels({
        tenantId,
        attendee: { phone: message.recipient_phone, email: message.recipient_email },
        moment: message.moment || 'reminder',
        nowLocal,
        tenantPrefs: message.tenant_prefs,
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
async function sendEmail(deps, message, renderedBody) {
  const inner = {
    to: [message.recipient_email],
    subject: message.subject || `Reminder from ${message.template_vars?.organization_name || message.tenant_id}`,
    text_body: renderedBody,
    html_body: `<p>${renderedBody}</p>`,
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

async function sendSms(deps, message, renderedBody) {
  await deps.lambda.send(new InvokeCommand({
    FunctionName: SMS_SENDER_FUNCTION,
    InvocationType: 'Event',
    Payload: JSON.stringify({
      to: message.recipient_phone,
      body: renderedBody,
      tenantId: message.tenant_id,
      formId: message.template || 'scheduled',
      submissionId: message.appointment_id || message.message_id,
      sessionId: '',
      type: 'reminder',
      sendType: 'contact', // activates the shipped SMS_Sender consent gate (§E3)
      fromNumber: message.from_number || '',
    }),
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
      await sendEmail(deps, message, renderedBody);
      dispatched.email = true;
    }
    if (channels.sms) {
      await sendSms(deps, message, renderedBody);
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
    // selectChannels (§E3, WS-E-TCPA) is wired here by the integrator once it merges.
    // Until then it is undefined → reminder rows fall back to email-floor only (SMS
    // fail-closed). Example wiring: selectChannels: (await import('...')).selectChannels
    selectChannels: undefined,
  };
}

/**
 * Lambda handler — invoked by EventBridge Scheduler with { pk, sk, message_id }.
 */
export async function handler(event) {
  return dispatch(event, defaultDeps());
}
