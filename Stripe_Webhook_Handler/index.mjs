/**
 * Stripe_Webhook_Handler Lambda
 *
 * Receives Stripe webhook events via Lambda Function URL.
 * Updates tenant registry (subscriptionTier, status) on subscription
 * and payment events. Writes audit records to picasso-notification-events.
 *
 * Deployed with AuthType: NONE (Stripe sends unauthenticated POSTs).
 * Security: validates Stripe HMAC-SHA256 signature before processing.
 *
 * Stripe events handled:
 * - customer.subscription.created — new subscription, set tier
 * - customer.subscription.updated — plan change, update tier
 * - customer.subscription.deleted — cancelled, set tier to free
 * - invoice.paid — confirm active status
 * - invoice.payment_failed — suspend tenant
 *
 * Tenant resolution: looks up stripeCustomerId via GSI on tenant registry.
 * No Stripe SDK — signature verification is HMAC-SHA256 via node:crypto.
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { createHmac, timingSafeEqual } from 'node:crypto';

const region = process.env.AWS_REGION || 'us-east-1';
const ddbClient = new DynamoDBClient({ region });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);
const secretsClient = new SecretsManagerClient({ region });

const TENANT_REGISTRY_TABLE = process.env.TENANT_REGISTRY_TABLE || `picasso-tenant-registry-${process.env.ENVIRONMENT || 'staging'}`;
const NOTIFICATION_EVENTS_TABLE = process.env.NOTIFICATION_EVENTS_TABLE || 'picasso-notification-events';
const STRIPE_SECRET_NAME = process.env.STRIPE_SECRET_NAME || 'picasso/stripe';
const TIMESTAMP_TOLERANCE = 300; // 5 minutes

// Stripe product ID → registry tier mapping (from env var JSON)
let tierMap = {};
try {
  tierMap = JSON.parse(process.env.STRIPE_TIER_MAP || '{}');
} catch (e) {
  console.warn('Failed to parse STRIPE_TIER_MAP, tier mapping disabled');
}

// Cache webhook secret (loaded once per Lambda instance)
let cachedWebhookSecret = null;

/**
 * Load Stripe webhook signing secret from Secrets Manager.
 * Cached for the lifetime of the Lambda instance.
 */
async function getWebhookSecret() {
  if (cachedWebhookSecret) return cachedWebhookSecret;

  const result = await secretsClient.send(new GetSecretValueCommand({
    SecretId: STRIPE_SECRET_NAME,
  }));
  const secrets = JSON.parse(result.SecretString);
  cachedWebhookSecret = secrets.webhook_secret;
  return cachedWebhookSecret;
}

/**
 * Verify Stripe webhook signature (HMAC-SHA256).
 *
 * Header format: t={timestamp},v1={hmac_hex}
 * Signed message: {timestamp}.{raw_body}
 */
function verifyStripeSignature(rawBody, signatureHeader, secret) {
  if (!signatureHeader || !secret) return false;

  try {
    // Parse header components
    const parts = {};
    for (const item of signatureHeader.split(',')) {
      const [key, value] = item.split('=', 2);
      parts[key.trim()] = value;
    }

    const timestamp = parts.t;
    const signature = parts.v1;
    if (!timestamp || !signature) return false;

    // Check timestamp tolerance
    const age = Math.abs(Math.floor(Date.now() / 1000) - parseInt(timestamp, 10));
    if (age > TIMESTAMP_TOLERANCE) {
      console.warn(`Stripe signature timestamp too old: ${age}s`);
      return false;
    }

    // Compute expected signature
    const signedPayload = `${timestamp}.${rawBody}`;
    const expected = createHmac('sha256', secret)
      .update(signedPayload, 'utf8')
      .digest('hex');

    // Constant-time comparison
    const sigBuf = Buffer.from(signature, 'hex');
    const expBuf = Buffer.from(expected, 'hex');
    if (sigBuf.length !== expBuf.length) return false;
    return timingSafeEqual(sigBuf, expBuf);
  } catch (err) {
    console.error('Signature verification error:', err.message);
    return false;
  }
}

/**
 * Look up tenant by Stripe customer ID via GSI.
 * Returns { tenantId, ... } or null.
 */
async function resolveTenantByCustomerId(customerId) {
  const result = await dynamodb.send(new QueryCommand({
    TableName: TENANT_REGISTRY_TABLE,
    IndexName: 'StripeCustomerIdIndex',
    KeyConditionExpression: 'stripeCustomerId = :cid',
    ExpressionAttributeValues: { ':cid': customerId },
    Limit: 1,
  }));

  const items = result.Items || [];
  return items.length > 0 ? items[0] : null;
}

/**
 * Resolve Stripe product/price to a subscription tier.
 */
function resolveSubscriptionTier(subscription) {
  // Try product ID from the first subscription item
  const items = subscription.items?.data || [];
  if (items.length > 0) {
    const productId = items[0].price?.product || items[0].plan?.product;
    if (productId && tierMap[productId]) {
      return tierMap[productId];
    }
    // Try price ID as fallback
    const priceId = items[0].price?.id || items[0].plan?.id;
    if (priceId && tierMap[priceId]) {
      return tierMap[priceId];
    }
  }

  console.warn('No tier mapping found for subscription:', JSON.stringify(items.map(i => ({
    product: i.price?.product || i.plan?.product,
    price: i.price?.id || i.plan?.id,
  }))));
  return null;
}

/**
 * Update tenant registry record.
 */
async function updateTenantRegistry(tenantId, updates) {
  const expressions = [];
  const names = {};
  const values = {};
  let i = 0;

  for (const [key, value] of Object.entries(updates)) {
    if (value === undefined) continue;
    expressions.push(`#f${i} = :v${i}`);
    names[`#f${i}`] = key;
    values[`:v${i}`] = value;
    i++;
  }

  if (expressions.length === 0) return;

  // Always update timestamp
  expressions.push('#upd = :now');
  names['#upd'] = 'updatedAt';
  values[':now'] = new Date().toISOString();

  await dynamodb.send(new UpdateCommand({
    TableName: TENANT_REGISTRY_TABLE,
    Key: { tenantId },
    UpdateExpression: 'SET ' + expressions.join(', '),
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }));

  console.log(`Registry updated: ${tenantId}`, updates);
}

/**
 * Write audit event to picasso-notification-events.
 */
async function writeAuditEvent(tenantId, stripeEvent) {
  const now = new Date();
  const isoDate = now.toISOString().split('T')[0];
  const eventType = stripeEvent.type.replace(/\./g, '_');

  await dynamodb.send(new PutCommand({
    TableName: NOTIFICATION_EVENTS_TABLE,
    Item: {
      pk: `TENANT#${tenantId}`,
      sk: `${isoDate}#stripe#${eventType}#${stripeEvent.id}`,
      channel: 'stripe',
      event_type: eventType,
      stripe_event_id: stripeEvent.id,
      stripe_event_type: stripeEvent.type,
      stripe_customer_id: stripeEvent.data?.object?.customer || '',
      detail: extractEventDetail(stripeEvent),
      timestamp: new Date(stripeEvent.created * 1000).toISOString(),
      ttl: Math.floor(Date.now() / 1000) + (90 * 24 * 3600),
    },
  }));
}

/**
 * Extract relevant detail from a Stripe event for audit storage.
 */
function extractEventDetail(event) {
  const obj = event.data?.object || {};
  const detail = {
    stripe_event_type: event.type,
  };

  if (obj.id) detail.object_id = obj.id;
  if (obj.status) detail.status = obj.status;
  if (obj.currency) detail.currency = obj.currency;

  // Subscription events
  if (obj.items?.data?.[0]) {
    const item = obj.items.data[0];
    detail.product_id = item.price?.product || item.plan?.product;
    detail.price_id = item.price?.id || item.plan?.id;
    detail.interval = item.price?.recurring?.interval || item.plan?.interval;
  }

  // Invoice events
  if (obj.amount_due !== undefined) detail.amount_due = obj.amount_due;
  if (obj.amount_paid !== undefined) detail.amount_paid = obj.amount_paid;
  if (obj.subscription) detail.subscription_id = obj.subscription;
  if (obj.due_date) detail.due_date = new Date(obj.due_date * 1000).toISOString();
  if (obj.period_end) detail.period_end = new Date(obj.period_end * 1000).toISOString();
  if (obj.next_payment_attempt) detail.next_payment_attempt = new Date(obj.next_payment_attempt * 1000).toISOString();
  if (obj.description) detail.description = obj.description;
  if (obj.number) detail.invoice_number = obj.number;

  // Subscription-level dates
  if (obj.current_period_start) detail.period_start = new Date(obj.current_period_start * 1000).toISOString();
  if (obj.current_period_end) detail.period_end = new Date(obj.current_period_end * 1000).toISOString();

  return detail;
}

/**
 * Main handler.
 */
export async function handler(event) {
  const method = event.requestContext?.http?.method || event.httpMethod || 'UNKNOWN';

  // Health check
  if (method === 'GET') {
    return { statusCode: 200, body: JSON.stringify({ status: 'healthy', service: 'stripe-webhook-handler' }) };
  }

  if (method !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  // Get raw body and signature
  const rawBody = event.isBase64Encoded
    ? Buffer.from(event.body, 'base64').toString('utf-8')
    : event.body;

  const headers = Object.fromEntries(
    Object.entries(event.headers || {}).map(([k, v]) => [k.toLowerCase(), v])
  );
  const signatureHeader = headers['stripe-signature'];

  if (!signatureHeader) {
    console.warn('Missing stripe-signature header');
    return { statusCode: 401, body: 'Missing signature' };
  }

  // Verify signature
  let webhookSecret;
  try {
    webhookSecret = await getWebhookSecret();
  } catch (err) {
    console.error('Failed to load webhook secret:', err.message);
    return { statusCode: 500, body: 'Configuration error' };
  }

  if (!webhookSecret) {
    console.error('Webhook secret is empty — not yet configured');
    return { statusCode: 500, body: 'Webhook secret not configured' };
  }

  if (!verifyStripeSignature(rawBody, signatureHeader, webhookSecret)) {
    console.warn('Invalid Stripe signature');
    return { statusCode: 401, body: 'Invalid signature' };
  }

  // Parse event
  let stripeEvent;
  try {
    stripeEvent = JSON.parse(rawBody);
  } catch (err) {
    console.error('Failed to parse event body:', err.message);
    return { statusCode: 400, body: 'Invalid JSON' };
  }

  console.log(`Stripe event: ${stripeEvent.type} (${stripeEvent.id})`);

  // Extract customer ID
  const obj = stripeEvent.data?.object || {};
  const customerId = obj.customer || obj.id; // invoice.* has customer, subscription.* has customer

  if (!customerId || typeof customerId !== 'string' || !customerId.startsWith('cus_')) {
    console.warn(`No valid customer ID in event: ${customerId}`);
    // Still return 200 — we don't want Stripe retrying events we can't process
    return { statusCode: 200, body: JSON.stringify({ received: true, skipped: 'no_customer_id' }) };
  }

  // Resolve tenant
  let tenant;
  try {
    tenant = await resolveTenantByCustomerId(customerId);
  } catch (err) {
    console.error(`Tenant lookup failed for ${customerId}:`, err.message);
    return { statusCode: 500, body: 'Tenant lookup failed' };
  }

  if (!tenant) {
    console.warn(`No tenant found for Stripe customer ${customerId}`);
    return { statusCode: 200, body: JSON.stringify({ received: true, skipped: 'unknown_customer' }) };
  }

  const tenantId = tenant.tenantId;
  console.log(`Resolved tenant: ${tenantId} for customer ${customerId}`);

  // Process event
  try {
    const updates = {};

    switch (stripeEvent.type) {
      case 'customer.subscription.created':
      case 'customer.subscription.updated': {
        const tier = resolveSubscriptionTier(obj);
        if (tier) updates.subscriptionTier = tier;

        // Map Stripe subscription status to registry status
        if (obj.status === 'active' || obj.status === 'trialing') {
          updates.status = 'active';
        } else if (obj.status === 'past_due') {
          updates.status = 'suspended';
        } else if (obj.status === 'canceled' || obj.status === 'unpaid') {
          updates.status = 'churned';
        }
        break;
      }

      case 'customer.subscription.deleted':
        updates.subscriptionTier = 'free';
        updates.status = 'churned';
        break;

      case 'invoice.paid':
        updates.status = 'active';
        break;

      case 'invoice.payment_failed':
        updates.status = 'suspended';
        break;

      default:
        console.log(`Unhandled event type: ${stripeEvent.type}`);
    }

    // Update registry if there are changes
    if (Object.keys(updates).length > 0) {
      await updateTenantRegistry(tenantId, updates);
    }

    // Write audit event (all events, even unhandled ones)
    await writeAuditEvent(tenantId, stripeEvent);

    return { statusCode: 200, body: JSON.stringify({ received: true, tenantId }) };

  } catch (err) {
    console.error(`Failed to process event ${stripeEvent.id}:`, err);
    // Return 500 so Stripe retries
    return { statusCode: 500, body: 'Processing failed' };
  }
}
