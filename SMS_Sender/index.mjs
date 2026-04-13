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
import { DynamoDBDocumentClient, PutCommand, GetCommand } from '@aws-sdk/lib-dynamodb';

const region = process.env.AWS_REGION || 'us-east-1';
const secretsClient = new SecretsManagerClient({ region });
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);

const TELNYX_SECRET_NAME = process.env.TELNYX_SECRET_NAME || 'picasso/telnyx';
const NOTIFICATION_SENDS_TABLE = process.env.NOTIFICATION_SENDS_TABLE || 'picasso-notification-sends';
const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';
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
 * Calculate SMS segment count based on message content.
 * GSM-7 encoding: 160 chars for single segment, 153 for multi-segment.
 * UCS-2 encoding (non-GSM characters): 70 chars for single, 67 for multi.
 */
function calculateSegmentCount(text) {
  if (!text) return 0;

  // GSM-7 basic character set (simplified check — covers most Latin text)
  // If any character falls outside GSM-7, the entire message uses UCS-2.
  const GSM7_PATTERN = /^[@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ ÆæßÉ!"#¤%&'()*+,\-.\/0-9:;<=>?¡A-ZÄÖÑÜa-zäöñüà\x1B\f^{}\\[\]~|€]*$/;
  const isGSM7 = GSM7_PATTERN.test(text);

  const len = text.length;

  if (isGSM7) {
    if (len <= 160) return 1;
    return Math.ceil(len / 153);
  } else {
    if (len <= 70) return 1;
    return Math.ceil(len / 67);
  }
}

/**
 * Write a send record to picasso-notification-sends for audit trail
 */
async function writeAuditRecord(tenantId, messageId, payload, status, error = '', segmentCount = 0) {
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
        segment_count: segmentCount,
        timestamp: now,
        ttl: Math.floor(Date.now() / 1000) + 90 * 24 * 3600, // 90 days
      }
    }));
  } catch (ddbErr) {
    console.error('Failed to write SMS audit record to DynamoDB:', ddbErr);
  }
}

/**
 * Check consent for contact-facing sends.
 * Returns true only if an active consent record exists (consent_given: true, no opt-out).
 * Returns false if no record, opted out, or consent table unavailable.
 *
 * TCPA requires consent BEFORE sending. No record = no consent = no send.
 *
 * Only called for sendType 'contact'. Staff notifications (sendType 'internal')
 * bypass this check — they use Clerk opt-in flags instead.
 */
async function checkConsent(tenantId, phoneE164) {
  try {
    const result = await dynamodb.send(new GetCommand({
      TableName: SMS_CONSENT_TABLE,
      Key: {
        pk: `TENANT#${tenantId}`,
        sk: `CONSENT#transactional#${phoneE164}`,
      },
    }));

    if (!result.Item) {
      console.log(`🚫 No consent record for ${phoneE164} (tenant: ${tenantId}) — suppressing send`);
      return false;
    }

    if (result.Item.consent_given === false || result.Item.opted_out_at) {
      console.log(`🚫 Contact ${phoneE164} has opted out (tenant: ${tenantId}) — suppressing send`);
      return false;
    }

    return true;
  } catch (error) {
    console.error('Consent check failed:', error);
    // Fail closed — no consent verification means no send
    return false;
  }
}

/**
 * Lambda handler
 */
export async function handler(event) {
  const { to, body, tenantId, formId, submissionId, sessionId, type, sendType, fromNumber } = event;

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

  // Pre-send consent check for contact-facing sends only.
  // Staff notifications (type: 'internal') use Clerk opt-in flags, not the consent table.
  if (sendType === 'contact') {
    const hasConsent = await checkConsent(tenantId, to);
    if (!hasConsent) {
      await writeAuditRecord(tenantId, null, event, 'suppressed', 'contact opted out');
      return { success: false, error: 'suppressed', reason: 'opted_out' };
    }
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

    // Use per-org number if provided, otherwise fall back to Secrets Manager default
    const senderNumber = fromNumber || credentials.fromNumber;

    // Send via Telnyx API
    const message = await sendViaTelnyx(
      credentials.apiKey,
      senderNumber,
      to,
      body,
      webhookUrl
    );

    const messageId = message.id;
    const segments = calculateSegmentCount(body);
    console.log(`✅ SMS sent to ${to} (ID: ${messageId}, type: ${type || 'internal'}, segments: ${segments})`);

    // Write success audit record
    await writeAuditRecord(tenantId, messageId, event, 'sent', '', segments);

    return { success: true, messageId, segments };

  } catch (error) {
    console.error(`❌ SMS send failed for ${to}:`, error.message);

    // Write failure audit record
    const segments = calculateSegmentCount(body);
    await writeAuditRecord(tenantId, null, event, 'failed', error.message, segments);

    return { success: false, error: error.message };
  }
}
