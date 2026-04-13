/**
 * SMS_Webhook_Handler Lambda
 *
 * Receives Telnyx messaging webhook events via Lambda Function URL.
 * Writes SMS delivery events to picasso-notification-events DynamoDB table
 * using the same schema pattern as SES events from ses_event_handler.
 *
 * Deployed with AuthType: NONE (Telnyx sends unauthenticated POSTs).
 * Security: validates Telnyx Ed25519 signature before processing.
 *
 * Telnyx webhook events for outbound SMS:
 * - message.sent — accepted and sent to carrier
 * - message.finalized — terminal state (delivered, failed, etc.)
 *   Delivery status in payload.to[].status: delivered, sending_failed,
 *   delivery_failed, delivery_unconfirmed
 *
 * Inbound SMS (STOP/UNSTOP/HELP):
 * - message.received — inbound message from contact
 *   STOP/UNSTOP keywords update consent records in picasso-sms-consent.
 *   Telnyx handles the auto-response; we mirror suppression state locally.
 *
 * Separate Lambda from SMS_Sender — different responsibility (inbound webhooks
 * vs outbound sends), different IAM role (no Secrets Manager access needed).
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { createPublicKey, verify } from 'node:crypto';

const region = process.env.AWS_REGION || 'us-east-1';
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);

const TELNYX_PUBLIC_KEY = process.env.TELNYX_PUBLIC_KEY || '';
const NOTIFICATION_EVENTS_TABLE = process.env.NOTIFICATION_EVENTS_TABLE || 'picasso-notification-events';
const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';

/**
 * Verify Telnyx Ed25519 webhook signature.
 *
 * Telnyx signs: "{timestamp}|{json_payload}"
 * Headers: telnyx-signature-ed25519, telnyx-timestamp
 * Public key from Mission Control → Keys & Credentials
 */
function verifyTelnyxSignature(payload, signature, timestamp, publicKeyBase64) {
  if (!publicKeyBase64 || !signature || !timestamp) return false;

  try {
    const signedPayload = `${timestamp}|${payload}`;
    const signatureBuffer = Buffer.from(signature, 'base64');

    // Telnyx provides a raw 32-byte Ed25519 public key (base64-encoded).
    // Node.js crypto.createPublicKey needs SPKI DER format, which is the
    // raw key prefixed with a fixed 12-byte Ed25519 SPKI header.
    const ED25519_SPKI_HEADER = Buffer.from('302a300506032b6570032100', 'hex');
    const rawKey = Buffer.from(publicKeyBase64, 'base64');
    const spkiDer = Buffer.concat([ED25519_SPKI_HEADER, rawKey]);

    const publicKey = createPublicKey({
      key: spkiDer,
      format: 'der',
      type: 'spki',
    });

    return verify(null, Buffer.from(signedPayload), publicKey, signatureBuffer);
  } catch (err) {
    console.error('Signature verification error:', err.message);
    return false;
  }
}

/**
 * Map Telnyx delivery status to normalized event type
 */
function normalizeEventType(eventType, deliveryStatus) {
  if (eventType === 'message.sent') return 'sent';
  if (eventType === 'message.finalized') {
    const statusMap = {
      delivered: 'delivered',
      sending_failed: 'failed',
      delivery_failed: 'failed',
      delivery_unconfirmed: 'unconfirmed',
    };
    return statusMap[deliveryStatus] || deliveryStatus || 'unknown';
  }
  return eventType || 'unknown';
}

// Keywords that Telnyx recognizes for opt-out/opt-in (case-insensitive)
const STOP_KEYWORDS = ['stop', 'stopall', 'unsubscribe', 'cancel', 'end', 'quit'];
const UNSTOP_KEYWORDS = ['unstop', 'start'];
const HELP_KEYWORDS = ['help', 'info'];

/**
 * Handle inbound STOP keyword: suppress consent across all tenants for this phone.
 * Uses phone-lookup GSI to find all tenant consent records, then updates each.
 */
async function handleStop(phoneE164) {
  const now = new Date().toISOString();

  // Query GSI to find all tenant consent records for this phone
  const queryResult = await dynamodb.send(new QueryCommand({
    TableName: SMS_CONSENT_TABLE,
    IndexName: 'phone-lookup',
    KeyConditionExpression: 'phone_e164 = :phone',
    ExpressionAttributeValues: { ':phone': phoneE164 },
  }));

  const records = queryResult.Items || [];
  if (records.length === 0) {
    console.log(`ℹ️ STOP from ${phoneE164} — no consent records found`);
    return;
  }

  for (const record of records) {
    try {
      await dynamodb.send(new UpdateCommand({
        TableName: SMS_CONSENT_TABLE,
        Key: { pk: record.pk, sk: record.sk },
        UpdateExpression: 'SET consent_given = :false, opted_out_at = :now, opt_out_source = :source, updated_at = :now',
        ExpressionAttributeValues: {
          ':false': false,
          ':now': now,
          ':source': 'inbound_keyword',
        },
      }));
      console.log(`✅ STOP processed: ${phoneE164} opted out of ${record.pk}`);
    } catch (err) {
      console.error(`Failed to update consent for ${record.pk}:`, err);
    }
  }
}

/**
 * Handle inbound UNSTOP keyword: restore consent across all tenants for this phone.
 * Uses upsert pattern — handles the edge case where UNSTOP arrives before any consent record exists.
 */
async function handleUnstop(phoneE164) {
  const now = new Date().toISOString();

  const queryResult = await dynamodb.send(new QueryCommand({
    TableName: SMS_CONSENT_TABLE,
    IndexName: 'phone-lookup',
    KeyConditionExpression: 'phone_e164 = :phone',
    ExpressionAttributeValues: { ':phone': phoneE164 },
  }));

  const records = queryResult.Items || [];

  if (records.length === 0) {
    // No prior consent records — this is an edge case (UNSTOP before any interaction).
    // Log but don't create a record — we don't know which tenant to associate it with.
    console.log(`ℹ️ UNSTOP from ${phoneE164} — no consent records to restore`);
    return;
  }

  for (const record of records) {
    try {
      await dynamodb.send(new UpdateCommand({
        TableName: SMS_CONSENT_TABLE,
        Key: { pk: record.pk, sk: record.sk },
        UpdateExpression: 'SET consent_given = :true, opted_out_at = :null, opt_out_source = :null, updated_at = :now',
        ExpressionAttributeValues: {
          ':true': true,
          ':null': null,
          ':now': now,
        },
      }));
      console.log(`✅ UNSTOP processed: ${phoneE164} re-opted in to ${record.pk}`);
    } catch (err) {
      console.error(`Failed to update consent for ${record.pk}:`, err);
    }
  }
}

/**
 * Process an inbound SMS message for opt-out/opt-in keywords.
 * Returns true if a keyword was matched and handled.
 */
async function processInboundMessage(payload) {
  const text = (payload.text || '').trim().toLowerCase();
  const fromPhone = payload.from?.phone_number || '';

  if (!fromPhone || !text) return false;

  if (STOP_KEYWORDS.includes(text)) {
    console.log(`📱 STOP keyword received from ${fromPhone}`);
    await handleStop(fromPhone);
    return true;
  }

  if (UNSTOP_KEYWORDS.includes(text)) {
    console.log(`📱 UNSTOP keyword received from ${fromPhone}`);
    await handleUnstop(fromPhone);
    return true;
  }

  if (HELP_KEYWORDS.includes(text)) {
    console.log(`📱 HELP keyword received from ${fromPhone} — Telnyx handles auto-response`);
    return true;
  }

  // Not a keyword — log for visibility but no action
  console.log(`📱 Inbound SMS from ${fromPhone}: "${text.substring(0, 50)}..." — not a keyword, ignoring`);
  return false;
}

/**
 * Lambda Function URL handler
 *
 * Telnyx sends POST with JSON body:
 * {
 *   "data": {
 *     "event_type": "message.finalized",
 *     "id": "unique-event-id",
 *     "occurred_at": "2024-01-15T20:16:07.588+00:00",
 *     "payload": {
 *       "id": "message-uuid",
 *       "to": [{ "phone_number": "+1...", "status": "delivered" }],
 *       "from": { "phone_number": "+1..." },
 *       "errors": []
 *     }
 *   }
 * }
 *
 * Context params embedded in the webhook_url query string by SMS_Sender:
 * - tenantId, formId, submissionId, sessionId
 */
export async function handler(event) {
  try {
    const method = event.requestContext?.http?.method || '';
    if (method !== 'POST') {
      return { statusCode: 405, body: 'Method not allowed' };
    }

    // Parse body
    const rawBody = event.isBase64Encoded
      ? Buffer.from(event.body, 'base64').toString('utf-8')
      : event.body || '';

    // Verify Telnyx signature
    if (TELNYX_PUBLIC_KEY) {
      const signature = event.headers?.['telnyx-signature-ed25519'] || '';
      const timestamp = event.headers?.['telnyx-timestamp'] || '';

      const isValid = verifyTelnyxSignature(rawBody, signature, timestamp, TELNYX_PUBLIC_KEY);
      if (!isValid) {
        console.warn('⚠️ Invalid Telnyx signature — rejecting request');
        return { statusCode: 403, body: 'Invalid signature' };
      }
    } else {
      console.warn('⚠️ TELNYX_PUBLIC_KEY not set — skipping signature validation');
    }

    const body = JSON.parse(rawBody);
    const webhookData = body.data || {};
    const eventType = webhookData.event_type || '';
    const payload = webhookData.payload || {};
    const telnyxEventId = webhookData.id || '';

    // Only process message events
    if (!eventType.startsWith('message.')) {
      return { statusCode: 200, body: 'OK' };
    }

    // Handle inbound messages (STOP/UNSTOP/HELP keywords)
    if (eventType === 'message.received') {
      await processInboundMessage(payload);
      return { statusCode: 200, body: 'OK' };
    }

    // --- Outbound message events below (message.sent, message.finalized) ---

    // Extract context from query string (embedded by SMS_Sender)
    const queryParams = event.queryStringParameters || {};
    const tenantId = queryParams.tenantId || 'unknown';
    const formId = queryParams.formId || '';
    const submissionId = queryParams.submissionId || '';
    const sessionId = queryParams.sessionId || '';

    // Extract message details
    const messageId = payload.id || '';
    const recipient = payload.to?.[0]?.phone_number || '';
    const deliveryStatus = payload.to?.[0]?.status || '';
    const errors = payload.errors || [];
    const errorDetail = errors.length > 0
      ? errors.map(e => `${e.code}: ${e.title}`).join('; ')
      : '';

    const normalizedEvent = normalizeEventType(eventType, deliveryStatus);
    const now = new Date().toISOString();

    console.log(`📱 SMS event: ${normalizedEvent} for ${messageId} (tenant: ${tenantId})`);

    // Write to picasso-notification-events (same schema pattern as SES events)
    await dynamodb.send(new PutCommand({
      TableName: NOTIFICATION_EVENTS_TABLE,
      Item: {
        pk: `TENANT#${tenantId}`,
        sk: `${now.slice(0, 10)}#sms#${normalizedEvent}#${messageId}`,
        channel: 'sms',
        event_type: normalizedEvent,
        telnyx_event_type: eventType,
        telnyx_event_id: telnyxEventId,
        recipient,
        message_id: messageId,
        delivery_status: deliveryStatus,
        error_code: errorDetail,
        context: {
          form_id: formId,
          submission_id: submissionId,
          session_id: sessionId,
        },
        timestamp: webhookData.occurred_at || now,
        ttl: Math.floor(Date.now() / 1000) + 90 * 24 * 3600, // 90 days
      }
    }));

    console.log(`✅ SMS event recorded: ${normalizedEvent} for ${messageId}`);

    // Telnyx expects 200 response within 2 seconds
    return { statusCode: 200, body: 'OK' };

  } catch (error) {
    console.error('❌ Webhook handler error:', error);
    // Return 200 to Telnyx even on error to prevent retries flooding us
    return { statusCode: 200, body: 'OK' };
  }
}
