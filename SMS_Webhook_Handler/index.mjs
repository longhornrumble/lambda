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
 * Separate Lambda from SMS_Sender — different responsibility (inbound webhooks
 * vs outbound sends), different IAM role (no Secrets Manager access needed).
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { createPublicKey, verify } from 'node:crypto';

const region = process.env.AWS_REGION || 'us-east-1';
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);

const TELNYX_PUBLIC_KEY = process.env.TELNYX_PUBLIC_KEY || '';
const NOTIFICATION_EVENTS_TABLE = process.env.NOTIFICATION_EVENTS_TABLE || 'picasso-notification-events';

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

    // Only process outbound message events
    if (!eventType.startsWith('message.')) {
      return { statusCode: 200, body: 'OK' };
    }

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
