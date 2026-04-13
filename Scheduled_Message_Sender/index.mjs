/**
 * Scheduled_Message_Sender Lambda
 *
 * Triggered by EventBridge Scheduler one-time schedules to send appointment
 * reminders and other time-based messages.
 *
 * Flow:
 * 1. EventBridge Scheduler fires at the scheduled time with the message_id
 * 2. This Lambda reads the message record from picasso-scheduled-messages
 * 3. Checks status (skip if cancelled/sent)
 * 4. Checks consent (skip if contact opted out)
 * 5. Invokes SMS_Sender or sends email via SES
 * 6. Updates status to 'sent' or 'suppressed'
 *
 * The EventBridge schedule is created by the caller (e.g., calendar integration)
 * which also writes the message record to DynamoDB. This Lambda only consumes.
 *
 * Cancellation: caller deletes the EventBridge schedule and updates DynamoDB
 * status to 'cancelled'. If the schedule fires before cancellation, this Lambda
 * checks status and skips cancelled messages.
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

/**
 * Check consent for a contact phone number.
 * Returns false if the contact has opted out.
 */
async function checkConsent(tenantId, phoneE164) {
  if (!phoneE164) return false;

  try {
    const result = await dynamodb.send(new GetCommand({
      TableName: SMS_CONSENT_TABLE,
      Key: {
        pk: `TENANT#${tenantId}`,
        sk: `CONSENT#transactional#${phoneE164}`,
      },
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
 * Render a template string with variable substitution.
 */
function renderTemplate(template, variables) {
  let result = template;
  for (const [key, value] of Object.entries(variables)) {
    result = result.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), String(value ?? ''));
  }
  return result;
}

/**
 * Update the scheduled message status in DynamoDB.
 */
async function updateMessageStatus(pk, sk, status, error = '') {
  const now = new Date().toISOString();
  const updateExpr = status === 'cancelled'
    ? 'SET #status = :status, cancelled_at = :now, updated_at = :now'
    : 'SET #status = :status, updated_at = :now';

  const values = { ':status': status, ':now': now };
  if (error) {
    values[':error'] = error;
  }

  await dynamodb.send(new UpdateCommand({
    TableName: SCHEDULED_MESSAGES_TABLE,
    Key: { pk, sk },
    UpdateExpression: error
      ? `${updateExpr}, error_detail = :error`
      : updateExpr,
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: values,
  }));
}

/**
 * Lambda handler — invoked by EventBridge Scheduler.
 *
 * Event payload (set when the schedule was created):
 * {
 *   "pk": "TENANT#AUS123957",
 *   "sk": "SCHEDULED#2026-04-15T14:00:00Z#01JXYZ...",
 *   "message_id": "01JXYZ..."
 * }
 */
export async function handler(event) {
  const { pk, sk, message_id } = event;

  if (!pk || !sk) {
    console.error('❌ Missing pk/sk in event payload');
    return { success: false, error: 'missing_keys' };
  }

  console.log(`📅 Processing scheduled message: ${message_id || sk}`);

  // 1. Read the message record
  let message;
  try {
    const result = await dynamodb.send(new GetCommand({
      TableName: SCHEDULED_MESSAGES_TABLE,
      Key: { pk, sk },
    }));
    message = result.Item;
  } catch (error) {
    console.error('❌ Failed to read scheduled message:', error);
    return { success: false, error: 'read_failed' };
  }

  if (!message) {
    console.warn(`⚠️ Scheduled message not found: ${pk} / ${sk}`);
    return { success: false, error: 'not_found' };
  }

  // 2. Check status — skip if not pending
  if (message.status !== 'pending') {
    console.log(`ℹ️ Message ${message_id} is '${message.status}' — skipping`);
    return { success: true, skipped: true, reason: message.status };
  }

  const tenantId = message.tenant_id;
  const channel = message.channel || 'sms';

  // 3. Check consent for SMS sends
  if (channel === 'sms' && message.recipient_phone) {
    const hasConsent = await checkConsent(tenantId, message.recipient_phone);
    if (!hasConsent) {
      console.log(`🚫 Contact opted out — suppressing scheduled message ${message_id}`);
      await updateMessageStatus(pk, sk, 'suppressed');
      return { success: true, suppressed: true };
    }
  }

  // 4. Render template if variables are provided
  const templateVars = message.template_vars || {};
  const renderedBody = message.body
    ? renderTemplate(message.body, templateVars)
    : `Reminder from ${templateVars.organization_name || tenantId}`;

  // 5. Send via appropriate channel
  try {
    if (channel === 'sms') {
      await lambdaClient.send(new InvokeCommand({
        FunctionName: SMS_SENDER_FUNCTION,
        InvocationType: 'Event',
        Payload: JSON.stringify({
          to: message.recipient_phone,
          body: renderedBody,
          tenantId,
          formId: message.template || 'scheduled',
          submissionId: message.appointment_id || message.message_id,
          sessionId: '',
          type: 'reminder',
          sendType: 'contact',
          fromNumber: message.from_number || '',
        }),
      }));
      console.log(`✅ Scheduled SMS invoked for ${message.recipient_phone} (${message_id})`);
    }
    // Future: email channel via SES

    // 6. Update status
    await updateMessageStatus(pk, sk, 'sent');
    return { success: true, channel, message_id };

  } catch (error) {
    console.error(`❌ Scheduled message send failed (${message_id}):`, error);
    await updateMessageStatus(pk, sk, 'failed', error.message);
    return { success: false, error: error.message };
  }
}
