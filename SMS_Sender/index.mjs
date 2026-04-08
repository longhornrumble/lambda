/**
 * SMS_Sender Lambda
 *
 * Shared Telnyx SMS service invoked by:
 * - form_handler.js for internal staff notifications (type: "internal")
 * - Future: appointment scheduling for reminders (type: "reminder")
 * - Future: applicant-facing confirmations (type: "applicant")
 *
 * Invoked asynchronously (InvocationType: 'Event') — callers don't wait for delivery.
 * Delivery status tracked via Telnyx webhooks → SMS_Webhook_Handler.
 *
 * DLQ configured to capture failed async invocations (throttling, crashes).
 *
 * Telnyx API: POST https://api.telnyx.com/v2/messages
 * Auth: Bearer API key
 * Delivery events: message.sent, message.finalized (delivered/failed)
 * Webhook configured per-message via webhook_url param
 */

import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

const region = process.env.AWS_REGION || 'us-east-1';
const secretsClient = new SecretsManagerClient({ region });
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);

const TELNYX_SECRET_NAME = process.env.TELNYX_SECRET_NAME || 'picasso/telnyx';
const NOTIFICATION_SENDS_TABLE = process.env.NOTIFICATION_SENDS_TABLE || 'picasso-notification-sends';
const WEBHOOK_BASE_URL = process.env.WEBHOOK_BASE_URL || '';

// Module-level cache for Telnyx credentials (persists across warm invocations)
let cachedCredentials = null;

/**
 * Load Telnyx credentials from Secrets Manager (cached after first call)
 *
 * Secret structure: { "apiKey": "KEY...", "fromNumber": "+1..." }
 */
async function getTelnyxCredentials() {
  if (cachedCredentials) return cachedCredentials;

  const response = await secretsClient.send(new GetSecretValueCommand({
    SecretId: TELNYX_SECRET_NAME
  }));

  cachedCredentials = JSON.parse(response.SecretString);
  return cachedCredentials;
}

/**
 * Send SMS via Telnyx REST API (no SDK dependency — simple POST)
 */
async function sendViaTelnyx(apiKey, from, to, text, webhookUrl) {
  const body = {
    from,
    to,
    text: text.substring(0, 1600),
    type: 'SMS',
  };

  if (webhookUrl) {
    body.webhook_url = webhookUrl;
  }

  const response = await fetch('https://api.telnyx.com/v2/messages', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Telnyx API error ${response.status}: ${errorBody}`);
  }

  const result = await response.json();
  return result.data;
}

/**
 * Write a send record to picasso-notification-sends for audit trail
 */
async function writeAuditRecord(tenantId, messageId, payload, status, error = '') {
  const now = new Date().toISOString();
  try {
    await dynamodb.send(new PutCommand({
      TableName: NOTIFICATION_SENDS_TABLE,
      Item: {
        pk: `TENANT#${tenantId}`,
        sk: `${now.slice(0, 10)}#sms#${messageId || `failed-${Date.now()}`}`,
        channel: 'sms',
        recipient: payload.to,
        form_id: payload.formId || 'unknown',
        submission_id: payload.submissionId || 'unknown',
        session_id: payload.sessionId || '',
        template: payload.type || 'internal',
        status,
        error,
        message_id: messageId || '',
        timestamp: now,
        ttl: Math.floor(Date.now() / 1000) + 90 * 24 * 3600, // 90 days
      }
    }));
  } catch (ddbErr) {
    console.error('Failed to write SMS audit record to DynamoDB:', ddbErr);
  }
}

/**
 * Lambda handler
 */
export async function handler(event) {
  const { to, body, tenantId, formId, submissionId, sessionId, type } = event;

  // Validate required fields
  if (!to || !body || !tenantId) {
    const error = `Missing required fields: ${!to ? 'to' : ''} ${!body ? 'body' : ''} ${!tenantId ? 'tenantId' : ''}`.trim();
    console.error(`❌ ${error}`);
    return { success: false, error };
  }

  // Validate E.164 format
  if (!/^\+\d{10,15}$/.test(to)) {
    const error = `Invalid phone number format: ${to}. Must be E.164 (e.g. +15125551234)`;
    console.error(`❌ ${error}`);
    await writeAuditRecord(tenantId, null, event, 'failed', error);
    return { success: false, error };
  }

  try {
    const credentials = await getTelnyxCredentials();

    // Build webhook URL with context params for delivery status tracking
    const callbackParams = new URLSearchParams({
      tenantId,
      formId: formId || '',
      submissionId: submissionId || '',
      sessionId: sessionId || '',
    });
    const webhookUrl = WEBHOOK_BASE_URL
      ? `${WEBHOOK_BASE_URL}?${callbackParams.toString()}`
      : undefined;

    // Send via Telnyx API
    const message = await sendViaTelnyx(
      credentials.apiKey,
      credentials.fromNumber,
      to,
      body,
      webhookUrl
    );

    const messageId = message.id;
    console.log(`✅ SMS sent to ${to} (ID: ${messageId}, type: ${type || 'internal'})`);

    // Write success audit record
    await writeAuditRecord(tenantId, messageId, event, 'sent');

    return { success: true, messageId };

  } catch (error) {
    console.error(`❌ SMS send failed for ${to}:`, error.message);

    // Write failure audit record
    await writeAuditRecord(tenantId, null, event, 'failed', error.message);

    return { success: false, error: error.message };
  }
}
