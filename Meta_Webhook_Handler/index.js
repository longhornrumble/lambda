'use strict';

/**
 * Meta_Webhook_Handler
 *
 * Receives and validates inbound webhooks from Meta (Facebook Messenger / Instagram).
 * Phase 1D of the Picasso Meta Messenger integration.
 *
 * GET  - Webhook verification challenge (called once during Meta App setup)
 * POST - Inbound message events (called for every user message)
 *
 * Security model:
 *   - GET:  token compared with MESSENGER_VERIFY_TOKEN env var
 *   - POST: HMAC-SHA256 signature validated against App Secret from Secrets Manager
 *           using crypto.timingSafeEqual to prevent timing attacks
 *
 * Must return 200 within ~5 s or Meta will retry the delivery.
 * The actual AI response is handled async by Meta_Response_Processor.
 */

const crypto = require('crypto');
const { DynamoDBClient, GetItemCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

// ─── AWS clients (created once at module load, reused across warm invocations) ───

const dynamo = new DynamoDBClient({});
const lambdaClient = new LambdaClient({});
const secretsClient = new SecretsManagerClient({});

// ─── Environment ────────────────────────────────────────────────────────────────

const ENV                      = process.env.ENVIRONMENT || 'staging';
const CHANNEL_MAPPINGS_TABLE   = process.env.CHANNEL_MAPPINGS_TABLE || `picasso-channel-mappings-${ENV}`;
const DEDUP_TABLE              = process.env.DEDUP_TABLE || `picasso-webhook-dedup-${ENV}`;
const RESPONSE_PROCESSOR_FN    = process.env.RESPONSE_PROCESSOR_FUNCTION || 'Meta_Response_Processor';
const META_APP_SECRET_ARN      = process.env.META_APP_SECRET_ARN || '';
const MESSENGER_VERIFY_TOKEN   = process.env.MESSENGER_VERIFY_TOKEN || '';

// ─── App Secret cache (module-scope, refreshed every 5 minutes) ─────────────────

let _appSecretCache = null;
let _appSecretFetchedAt = 0;
const SECRET_TTL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * Retrieve the Meta App Secret from Secrets Manager.
 * The resolved value is cached for 5 minutes to reduce cold-start latency
 * and avoid Secrets Manager throttling on high-traffic pages.
 *
 * The secret string is expected to be the raw hex/base64 HMAC key as set in
 * the Meta App Dashboard → Webhooks → App Secret field.
 *
 * @returns {Promise<string>} The app secret string.
 */
async function getAppSecret() {
  const now = Date.now();
  if (_appSecretCache && now - _appSecretFetchedAt < SECRET_TTL_MS) {
    return _appSecretCache;
  }

  if (!META_APP_SECRET_ARN) {
    throw new Error('META_APP_SECRET_ARN environment variable is not set');
  }

  const result = await secretsClient.send(
    new GetSecretValueCommand({ SecretId: META_APP_SECRET_ARN })
  );

  // SecretString may be a plain string or a JSON envelope {"appSecret":"..."}
  let secret = result.SecretString;
  try {
    const parsed = JSON.parse(secret);
    if (parsed.appSecret) secret = parsed.appSecret;
  } catch (_) {
    // Not JSON — use the raw string
  }

  _appSecretCache = secret;
  _appSecretFetchedAt = now;
  return secret;
}

// ─── Signature validation ────────────────────────────────────────────────────────

/**
 * Validate the X-Hub-Signature-256 header sent by Meta on every POST.
 *
 * Meta computes: sha256(rawBody, appSecret) and sends "sha256=<hex>"
 * We must use the RAW request body string (not re-serialised JSON) to
 * reproduce the same digest.  A timing-safe comparison prevents timing attacks.
 *
 * @param {string} rawBody      - Raw request body string exactly as received.
 * @param {string} signatureHdr - Value of X-Hub-Signature-256 header.
 * @param {string} appSecret    - The Meta App Secret.
 * @returns {boolean} True if the signature is valid.
 */
function validateSignature(rawBody, signatureHdr, appSecret) {
  if (!signatureHdr || !signatureHdr.startsWith('sha256=')) {
    return false;
  }

  const receivedHex = signatureHdr.slice('sha256='.length);
  const expected = crypto
    .createHmac('sha256', appSecret)
    .update(rawBody, 'utf8')
    .digest('hex');

  // Both buffers must be the same length for timingSafeEqual to work correctly.
  // If lengths differ the signature is definitely invalid — return false early.
  if (receivedHex.length !== expected.length) {
    return false;
  }

  return crypto.timingSafeEqual(
    Buffer.from(receivedHex, 'hex'),
    Buffer.from(expected, 'hex')
  );
}

// ─── DynamoDB helpers ────────────────────────────────────────────────────────────

/**
 * Look up the channel-mapping record for a given Meta Page ID.
 *
 * Table: picasso-channel-mappings-{ENV}
 *   PK: "PAGE#{pageId}"
 *   SK: "CHANNEL#messenger"
 *
 * Expected attributes on success:
 *   tenantId   {S}  — internal tenant identifier
 *   tenantHash {S}  — short hash used in JWT / session routing
 *   enabled    {BOOL} — if false, page is paused; skip processing
 *
 * @param {string} pageId
 * @returns {Promise<object|null>} Parsed item attributes, or null if not found.
 */
async function getChannelMapping(pageId) {
  const result = await dynamo.send(
    new GetItemCommand({
      TableName: CHANNEL_MAPPINGS_TABLE,
      Key: {
        PK: { S: `PAGE#${pageId}` },
        SK: { S: 'CHANNEL#messenger' },
      },
      // Only fetch what we need — reduces read cost
      ProjectionExpression: 'tenantId, tenantHash, enabled',
    })
  );

  if (!result.Item) return null;

  return {
    tenantId:   result.Item.tenantId?.S   || null,
    tenantHash: result.Item.tenantHash?.S || null,
    enabled:    result.Item.enabled?.BOOL !== false, // default true if attr missing
  };
}

/**
 * Attempt an idempotent write of a dedup record for the given message ID.
 *
 * Uses a conditional PutItem that succeeds ONLY if the `mid` key does not
 * already exist.  If the condition fails (ConditionalCheckFailedException),
 * the message is a duplicate and should be silently dropped.
 *
 * Table: picasso-webhook-dedup-{ENV}
 *   PK: "mid"
 *   TTL attribute: "ttl" (Unix timestamp, 24 h from now)
 *
 * @param {string} mid - The message ID from Meta (globally unique per message).
 * @returns {Promise<boolean>} True if the record was written (message is new),
 *                             false if the record already existed (duplicate).
 */
async function recordDedupOrSkip(mid) {
  const ttl = Math.floor(Date.now() / 1000) + 86400; // 24 hours
  try {
    await dynamo.send(
      new PutItemCommand({
        TableName: DEDUP_TABLE,
        Item: {
          mid: { S: mid },
          ttl: { N: String(ttl) },
        },
        ConditionExpression: 'attribute_not_exists(mid)',
      })
    );
    return true; // New message
  } catch (err) {
    if (err.name === 'ConditionalCheckFailedException') {
      return false; // Duplicate
    }
    throw err; // Unexpected error — bubble up
  }
}

// ─── Lambda async invoke ─────────────────────────────────────────────────────────

/**
 * Asynchronously invoke the Meta_Response_Processor Lambda with the
 * normalised event payload.  Uses InvocationType 'Event' so this call
 * returns immediately (~100 ms) and the processing happens in a separate
 * execution context, well within Meta's 5-second timeout.
 *
 * @param {object} payload - The event object to forward.
 * @returns {Promise<void>}
 */
async function invokeResponseProcessor(payload) {
  await lambdaClient.send(
    new InvokeCommand({
      FunctionName:   RESPONSE_PROCESSOR_FN,
      InvocationType: 'Event', // async — do not wait for response
      Payload:        Buffer.from(JSON.stringify(payload)),
    })
  );
}

// ─── Response helpers ────────────────────────────────────────────────────────────

function ok(body = 'EVENT_RECEIVED') {
  return { statusCode: 200, body };
}

function forbidden(reason) {
  console.warn('[Meta_Webhook_Handler] 403 Forbidden:', reason);
  return { statusCode: 403, body: 'Forbidden' };
}

// ─── GET: webhook verification ───────────────────────────────────────────────────

/**
 * Handle the one-time GET verification request Meta sends when a webhook URL
 * is configured in the Meta App Dashboard.
 *
 * Meta sends:
 *   ?hub.mode=subscribe
 *   &hub.verify_token=<token we configured>
 *   &hub.challenge=<random string>
 *
 * We must echo back hub.challenge with HTTP 200.
 *
 * @param {object} queryParams - Parsed query string parameters.
 * @returns {object} Lambda HTTP response.
 */
function handleVerification(queryParams) {
  const mode         = queryParams['hub.mode'];
  const verifyToken  = queryParams['hub.verify_token'];
  const challenge    = queryParams['hub.challenge'];

  if (mode !== 'subscribe') {
    return forbidden(`Unexpected hub.mode: ${mode}`);
  }

  if (!verifyToken || verifyToken !== MESSENGER_VERIFY_TOKEN) {
    return forbidden('hub.verify_token mismatch');
  }

  console.log('[Meta_Webhook_Handler] Webhook verified successfully');
  return { statusCode: 200, body: challenge };
}

// ─── POST: inbound message processing ───────────────────────────────────────────

/**
 * Process a single messaging event from the Meta webhook payload.
 *
 * Supports:
 *   - Standard text messages (messaging[].message.text)
 *   - Postback events    (messaging[].postback.payload)
 *
 * Flow:
 *   1. Resolve pageId → tenantId via DynamoDB channel-mappings
 *   2. Guard against duplicates via dedup table (conditional PutItem)
 *   3. Async-invoke Meta_Response_Processor with normalised payload
 *
 * Returns silently (no throw) on all recoverable errors so the caller
 * can always return 200 to Meta.
 *
 * @param {object} messagingEvent  - Single entry from entry.messaging[].
 * @param {string} pageId          - The Facebook Page ID from entry.id.
 * @param {string} [objectType]    - The webhook object type ('page' or 'instagram').
 * @returns {Promise<void>}
 */
async function processMessagingEvent(messagingEvent, pageId, objectType) {
  const sender = messagingEvent.sender?.id;
  if (!sender) {
    console.warn('[Meta_Webhook_Handler] messaging event missing sender.id — skipping');
    return;
  }

  // Determine event type and extract content
  let messageText = null;
  let messageMid  = null;
  let isPostback  = false;

  if (messagingEvent.message) {
    // Standard inbound message
    messageText = messagingEvent.message.text || null;
    messageMid  = messagingEvent.message.mid  || null;

    // Ignore delivery/read receipts (no text, no mid of interest)
    if (!messageText && !messagingEvent.message.attachments) {
      console.log('[Meta_Webhook_Handler] Skipping echo/receipt event');
      return;
    }

    // Ignore message echoes (sent by the page itself)
    if (messagingEvent.message.is_echo) {
      return;
    }
  } else if (messagingEvent.postback) {
    // Postback from a persistent menu or button template
    messageText = messagingEvent.postback.payload || null;
    messageMid  = `postback_${sender}_${Date.now()}`;
    isPostback  = true;
  } else {
    // Delivery reports, seen events, reactions — silently skip
    return;
  }

  if (!messageText) {
    console.log('[Meta_Webhook_Handler] No text content — skipping event from psid:', sender);
    return;
  }

  // ── 1. Channel mapping lookup ──
  let mapping;
  try {
    mapping = await getChannelMapping(pageId);
  } catch (err) {
    console.error('[Meta_Webhook_Handler] DynamoDB getChannelMapping error:', err.message);
    return; // Non-retryable from Meta's perspective
  }

  if (!mapping) {
    console.warn(`[Meta_Webhook_Handler] No channel mapping found for pageId=${pageId} — skipping`);
    return;
  }

  if (!mapping.enabled) {
    console.log(`[Meta_Webhook_Handler] Page ${pageId} is disabled — skipping`);
    return;
  }

  // ── 2. Idempotency guard ──
  if (messageMid) {
    let isNew;
    try {
      isNew = await recordDedupOrSkip(messageMid);
    } catch (err) {
      console.error('[Meta_Webhook_Handler] Dedup table error:', err.message);
      // On dedup failure, allow processing to continue — better to process
      // a duplicate than to silently drop a real message.
      isNew = true;
    }

    if (!isNew) {
      console.log(`[Meta_Webhook_Handler] Duplicate mid=${messageMid} — skipping`);
      return;
    }
  }

  // ── 3. Async invoke response processor ──
  const processorPayload = {
    psid:        sender,
    messageText,
    pageId,
    tenantId:    mapping.tenantId,
    tenantHash:  mapping.tenantHash,
    channelType: objectType === 'instagram' ? 'instagram' : 'messenger',
    messageMid,
    isPostback,
  };

  try {
    await invokeResponseProcessor(processorPayload);
    console.log(
      `[Meta_Webhook_Handler] Queued message for processing — psid=${sender} ` +
      `tenantId=${mapping.tenantId} mid=${messageMid} isPostback=${isPostback}`
    );
  } catch (err) {
    // Log but do NOT propagate — the response processor has its own DLQ.
    console.error('[Meta_Webhook_Handler] Failed to invoke response processor:', err.message);
  }
}

/**
 * Handle the POST webhook delivery from Meta.
 *
 * Meta sends a JSON body containing one or more "entries", each with zero or
 * more "messaging" events.  The body.object field is either "page" (Messenger)
 * or "instagram".  Both are processed through the same processMessagingEvent
 * pipeline; the objectType is forwarded so channelType is set correctly.
 *
 * @param {string} rawBody       - Raw request body (used for HMAC verification).
 * @param {object} headers       - HTTP headers from the Lambda event.
 * @returns {Promise<object>}    - Lambda HTTP response.
 */
async function handlePost(rawBody, headers) {
  // ── Signature validation ──
  let appSecret;
  try {
    appSecret = await getAppSecret();
  } catch (err) {
    console.error('[Meta_Webhook_Handler] Could not fetch app secret:', err.message);
    return { statusCode: 500, body: 'Internal Server Error' };
  }

  const signatureHeader =
    headers['x-hub-signature-256'] || headers['X-Hub-Signature-256'] || '';

  if (!validateSignature(rawBody, signatureHeader, appSecret)) {
    return forbidden('Invalid X-Hub-Signature-256');
  }

  // ── Parse body ──
  let body;
  try {
    body = JSON.parse(rawBody);
  } catch (err) {
    console.error('[Meta_Webhook_Handler] Failed to parse request body:', err.message);
    return { statusCode: 400, body: 'Bad Request' };
  }

  const objectType = body.object;

  if (objectType !== 'page' && objectType !== 'instagram') {
    console.warn(`[Meta_Webhook_Handler] Unknown object type: ${objectType} — acknowledging`);
    return ok();
  }

  if (objectType === 'instagram') {
    console.log('[Meta_Webhook_Handler] Instagram webhook received — routing through processMessagingEvent pipeline');
  }

  // ── Process each entry / messaging event ──
  const entries = Array.isArray(body.entry) ? body.entry : [];

  // Process all events concurrently within this invocation.
  // Individual event failures are caught inside processMessagingEvent and
  // do not prevent other events in the same batch from being processed.
  const eventPromises = [];

  for (const entry of entries) {
    const pageId    = entry.id;
    const messaging = Array.isArray(entry.messaging) ? entry.messaging : [];

    for (const messagingEvent of messaging) {
      eventPromises.push(processMessagingEvent(messagingEvent, pageId, objectType));
    }
  }

  await Promise.allSettled(eventPromises);

  // Always return 200 — Meta will retry on non-2xx responses.
  return ok();
}

// ─── Main Lambda handler ─────────────────────────────────────────────────────────

/**
 * Lambda entry point.
 *
 * Expects an HTTP API Gateway v2 (payload format 2.0) or REST API Gateway event.
 * Both formats are handled: queryStringParameters vs rawQueryString,
 * body (may be base64-encoded when isBase64Encoded = true).
 *
 * @param {object} event   - AWS Lambda event object.
 * @returns {Promise<object>} HTTP response { statusCode, body }.
 */
exports.handler = async function handler(event) {
  const method = event.requestContext?.http?.method || event.httpMethod || 'UNKNOWN';

  console.log(`[Meta_Webhook_Handler] ${method} ${event.rawPath || event.path || '/'}`);

  // ── GET: Webhook verification ──
  if (method === 'GET') {
    // API GW v2 sends rawQueryString; v1 sends queryStringParameters object
    let queryParams = event.queryStringParameters || {};
    if (event.rawQueryString) {
      queryParams = Object.fromEntries(new URLSearchParams(event.rawQueryString));
    }
    return handleVerification(queryParams);
  }

  // ── POST: Inbound message ──
  if (method === 'POST') {
    let rawBody = event.body || '';
    if (event.isBase64Encoded) {
      rawBody = Buffer.from(rawBody, 'base64').toString('utf8');
    }

    const headers = event.headers || {};
    return handlePost(rawBody, headers);
  }

  // ── Unsupported methods ──
  console.warn(`[Meta_Webhook_Handler] Unsupported HTTP method: ${method}`);
  return { statusCode: 405, body: 'Method Not Allowed' };
};
