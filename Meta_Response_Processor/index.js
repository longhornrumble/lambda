'use strict';

/**
 * Meta_Response_Processor — Phase 1E
 *
 * Invoked asynchronously by Meta_Webhook_Handler. Processes a Messenger user
 * message through the shared RAG pipeline (KB retrieval + Bedrock InvokeModel)
 * and delivers the AI response back via the Meta Graph API Send API.
 *
 * Input event shape:
 * {
 *   psid:        string  — Page-Scoped User ID (the Messenger sender)
 *   messageText: string  — Raw message text from the user
 *   pageId:      string  — Facebook Page ID
 *   tenantId:    string  — Picasso tenant identifier
 *   tenantHash:  string  — Tenant hash used for config/KB lookup
 *   channelType: string  — Always "messenger" for this handler
 *   messageMid:  string  — Meta message_id for deduplication / logging
 * }
 *
 * Processing order:
 *   0. Input validation
 *   0a. 24-hour window check (drop stale DLQ retries)
 *   0b. Postback handling — GET_STARTED sends welcome_message; other payloads fall through to RAG
 *   1. Load page access token from DynamoDB (decrypt with KMS)
 *   2. Send typing indicator + start refresh interval (every 8 s)
 *   3. Load conversation context from DynamoDB recent-messages table
 *   4. Run RAG pipeline via shared bedrock-core (loadConfig → retrieveKB → InvokeModel)
 *   5. Stop typing refresh, send response via Meta Send API (splitting at 2000-char boundary if needed)
 *   6. Persist Q&A pair to recent-messages (keep last 10 pairs)
 *   7. Update lastUserMessageAt on the channel-mapping record
 *
 * Environment variables:
 *   ENVIRONMENT              — staging | production (default: staging)
 *   CHANNEL_MAPPINGS_TABLE   — DynamoDB table for page tokens
 *   RECENT_MESSAGES_TABLE    — DynamoDB table for conversation context
 *   KMS_KEY_ID               — KMS key alias or ARN for page token decryption
 *   AWS_REGION               — AWS region (default: us-east-1)
 */

const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
const { DynamoDBClient, GetItemCommand, PutItemCommand, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { KMSClient, DecryptCommand } = require('@aws-sdk/client-kms');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { loadConfig, retrieveKB, sanitizeUserInput } = require('../shared/bedrock-core');
const { MESSAGE_CHAR_LIMITS } = require('./capabilities');
const conversationLock = require('./conversationLock');
const { classifyMetaSendError } = require('./metaSendErrors');
const { buildMessengerV5Prompt } = require('./prompt_messenger');
const { computeSessionWindow } = require('./sessionWindow');
const { createTailParser } = require('../shared/prompt/streamTail');
const { validateActionIds } = require('../shared/prompt/prompt_v5');
const { selectActionsV4 } = require('../shared/prompt/prompt_v4');
const crypto = require('crypto');

// ─── AWS client initialisation ────────────────────────────────────────────────

const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const ENVIRONMENT = process.env.ENVIRONMENT || 'staging';

const bedrockRuntime = new BedrockRuntimeClient({ region: AWS_REGION });

const dynamodbRaw = new DynamoDBClient({ region: AWS_REGION });
const dynamodb = DynamoDBDocumentClient.from(dynamodbRaw, {
  marshallOptions: { removeUndefinedValues: true },
});

const kms = new KMSClient({ region: AWS_REGION });

const sqs = new SQSClient({ region: AWS_REGION });

// ─── Configuration constants ──────────────────────────────────────────────────

const CHANNEL_MAPPINGS_TABLE =
  process.env.CHANNEL_MAPPINGS_TABLE || 'picasso-channel-mappings';
const RECENT_MESSAGES_TABLE =
  process.env.RECENT_MESSAGES_TABLE || 'picasso-recent-messages';
// C7 serialization (M1c). Empty ⇒ serialization disabled (fail-open for local/dev).
const CONVERSATION_STATE_TABLE = process.env.CONVERSATION_STATE_TABLE || '';
const KMS_KEY_ID = process.env.KMS_KEY_ID || 'alias/picasso-channel-tokens';
const ANALYTICS_QUEUE_URL =
  process.env.ANALYTICS_QUEUE_URL ||
  'https://sqs.us-east-1.amazonaws.com/614056832592/picasso-analytics-events';

const DEFAULT_MODEL_ID = 'global.anthropic.claude-haiku-4-5-20251001-v1:0';
const DEFAULT_TONE = 'You are a helpful assistant.';
const DEFAULT_FALLBACK_MESSAGE =
  "I'm sorry, I'm having trouble right now. Please try again in a moment.";

// ── M1b (messenger_behavior strings, C2) — defaults; config overrides win ──
const DEFAULT_DISCLOSURE_LINE =
  "Just a heads up — you're chatting with an automated assistant.";

/** Default C7 drain-cap reply (config: messenger_behavior.strings.rate_limited) */
const DEFAULT_RATE_LIMITED = "You're sending messages faster than I can keep up — one moment please.";
const DEFAULT_UNSUPPORTED_INPUT_FALLBACK =
  "Sorry, I can't read that kind of message yet — could you type it instead?";

const META_GRAPH_VERSION = 'v21.0';
const META_SEND_API_BASE = `https://graph.facebook.com/${META_GRAPH_VERSION}`;

/** Number of most-recent conversation turns to include in the prompt */
const MAX_HISTORY_TURNS = 5;

/** Number of Q&A pairs to retain in DynamoDB (rolling window) */
const MAX_STORED_PAIRS = 10;

/** Delay between sequential Messenger messages when splitting (ms) */
const SPLIT_MESSAGE_DELAY_MS = 200;

/** Meta messaging window: bot may only send within 24 h of the user's last message */
const MESSAGE_WINDOW_MS = 24 * 60 * 60 * 1000;

/** How often to refresh the typing indicator while waiting for Bedrock (ms) */
const TYPING_REFRESH_INTERVAL_MS = 8000;

// ─── Observability helpers ────────────────────────────────────────────────────

/**
 * Structured log line. All entries include the correlation key so CloudWatch
 * Logs Insights can group events from the same invocation.
 * @param {'INFO'|'WARN'|'ERROR'} level
 * @param {string} message
 * @param {Record<string, unknown>} [meta]
 */
function log(level, message, meta = {}) {
  // eslint-disable-next-line no-console
  console[level === 'ERROR' ? 'error' : level === 'WARN' ? 'warn' : 'log'](
    JSON.stringify({ level, message, service: 'MetaResponseProcessor', ...meta })
  );
}

// ─── Analytics emission ───────────────────────────────────────────────────────

/**
 * Best-effort emission of an analytics event to the shared SQS analytics queue.
 * Failures are logged as warnings and never propagate — analytics must not affect
 * the critical message-delivery path.
 *
 * Event envelope matches the schema expected by Analytics_Event_Processor:
 *   { schema_version, session_id, tenant_id, timestamp, event: { type, payload } }
 *
 * @param {string} eventType — e.g. 'MESSENGER_MESSAGE_RECEIVED'
 * @param {Record<string, unknown>} payload — Event-specific fields
 * @returns {Promise<void>}
 */
async function emitAnalyticsEvent(eventType, payload) {
  try {
    const body = JSON.stringify({
      schema_version: '1.0',
      session_id: payload.session_id,
      tenant_id: payload.tenant_id,
      timestamp: new Date().toISOString(),
      event: {
        type: eventType,
        payload,
      },
    });

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: ANALYTICS_QUEUE_URL,
        MessageBody: body,
      })
    );

    log('INFO', 'Analytics event emitted', { eventType });
  } catch (err) {
    log('WARN', 'Failed to emit analytics event — continuing', {
      eventType,
      error: err.message,
    });
  }
}

// ─── KMS decryption ───────────────────────────────────────────────────────────

/**
 * Decrypt a KMS-encrypted ciphertext blob (base64-encoded) and return the
 * plaintext as a UTF-8 string.
 * @param {string} ciphertextBase64 — Base64-encoded KMS ciphertext
 * @returns {Promise<string>}
 */
async function decryptWithKms(ciphertextBase64) {
  const ciphertextBlob = Buffer.from(ciphertextBase64, 'base64');
  const result = await kms.send(
    new DecryptCommand({
      CiphertextBlob: ciphertextBlob,
      KeyId: KMS_KEY_ID,
    })
  );
  return Buffer.from(result.Plaintext).toString('utf-8');
}

// ─── Channel-mapping token lookup ─────────────────────────────────────────────

/**
 * Load and decrypt the Facebook Page Access Token for the given page.
 *
 * DynamoDB schema:
 *   PK:  PAGE#{pageId}
 *   SK:  CHANNEL#{channelType}
 *   Attributes: encryptedPageToken (string, base64 KMS ciphertext)
 *
 * @param {string} pageId
 * @param {string} channelType — e.g. "messenger"
 * @returns {Promise<string>} plaintext page access token
 * @throws if the record is missing or decryption fails
 */
async function loadPageAccessToken(pageId, channelType) {
  log('INFO', 'Loading page access token', { pageId, channelType, table: CHANNEL_MAPPINGS_TABLE });

  const result = await dynamodb.send(
    new GetCommand({
      TableName: CHANNEL_MAPPINGS_TABLE,
      Key: {
        PK: `PAGE#${pageId}`,
        SK: `CHANNEL#${channelType}`,
      },
    })
  );

  if (!result.Item) {
    throw new Error(
      `Channel mapping not found for pageId=${pageId} channelType=${channelType}`
    );
  }

  const { encryptedPageToken } = result.Item;
  if (!encryptedPageToken) {
    throw new Error(
      `encryptedPageToken missing on channel mapping for pageId=${pageId}`
    );
  }

  const token = await decryptWithKms(encryptedPageToken);
  log('INFO', 'Page access token decrypted successfully', { pageId });
  return token;
}

// ─── Conversation context ─────────────────────────────────────────────────────

/**
 * Load conversation history for this Messenger thread from DynamoDB.
 *
 * DynamoDB schema (existing picasso-recent-messages table):
 *   sessionId        (HASH)  — e.g. "meta:{pageId}:{psid}"
 *   messageTimestamp  (RANGE) — ISO timestamp
 *   role, content     — message data
 *
 * @param {string} pageId
 * @param {string} psid
 * @returns {Promise<Array<{role: string, content: string, timestamp: string}>>}
 */
async function loadConversationContext(pageId, psid) {
  const sessionId = `meta:${pageId}:${psid}`;
  log('INFO', 'Loading conversation context', {
    sessionId,
    table: RECENT_MESSAGES_TABLE,
  });

  const result = await dynamodb.send(
    new QueryCommand({
      TableName: RECENT_MESSAGES_TABLE,
      KeyConditionExpression: 'sessionId = :sid',
      ExpressionAttributeValues: { ':sid': sessionId },
      ScanIndexForward: true, // oldest first
      Limit: MAX_STORED_PAIRS * 2, // last N messages
    })
  );

  const messages = (result.Items || []).map((item) => ({
    role: item.role || 'user',
    content: item.content || '',
    timestamp: item.messageTimestamp ? new Date(item.messageTimestamp).toISOString() : '',
    // C8 session windowing needs the raw epoch (sessionWindow.js); additive —
    // prompt builders read role/content only.
    messageTimestamp: typeof item.messageTimestamp === 'number' ? item.messageTimestamp : undefined,
  }));
  log('INFO', 'Conversation context loaded', { sessionId, messageCount: messages.length });
  return messages;
}

/**
 * Persist the latest Q&A pair to the recent-messages table as individual rows.
 *
 * M1b TTL fix: the table's TTL attribute is `expires_at` (epoch seconds), not
 * `ttl` — writing `ttl` meant rows never expired. `mid` is stored on the user
 * row only (assistant rows have no inbound mid) so edit/delete events (C1)
 * can find the row to mutate/remove.
 *
 * @param {string} pageId
 * @param {string} psid
 * @param {string} userText — sanitised user input
 * @param {string} assistantText — AI response
 * @param {string|null} [messageMid] — inbound message mid, stored on the user row only
 * @returns {Promise<void>}
 */
async function storeConversationContext(pageId, psid, userText, assistantText, messageMid) {
  const sessionId = `meta:${pageId}:${psid}`;
  const now = Date.now(); // epoch ms — matches messageTimestamp Number type
  const expires_at = Math.floor(now / 1000) + 60 * 60 * 24 * 7; // 7-day TTL (table TTL attribute)

  // Write user message
  await dynamodb.send(
    new PutCommand({
      TableName: RECENT_MESSAGES_TABLE,
      Item: {
        sessionId,
        messageTimestamp: now,
        role: 'user',
        content: userText,
        // §E5 Chain 1: English-equivalent slot (v1: text_en = content, verbatim). Readers tolerate absence.
        text_en: userText,
        expires_at,
        ...(messageMid ? { mid: messageMid } : {}),
      },
    })
  );

  // Write assistant response (offset timestamp by 1ms to ensure ordering)
  await dynamodb.send(
    new PutCommand({
      TableName: RECENT_MESSAGES_TABLE,
      Item: {
        sessionId,
        messageTimestamp: now + 1,
        role: 'assistant',
        content: assistantText,
        // §E5 Chain 1: English-equivalent slot (v1: text_en = content, verbatim). Readers tolerate absence.
        text_en: assistantText,
        expires_at,
      },
    })
  );

  log('INFO', 'Conversation context stored', {
    sessionId,
    storedMessages: 2,
  });
}

// ─── Edit / delete (C1 v1.1 — Meta redeliveries mean these MUST be idempotent) ─

/**
 * Query all rows for a session and return only those whose `mid` matches.
 * Never touches a table row outside a `meta:`-prefixed sessionId — the table
 * is SHARED with live widget chat.
 *
 * @param {string} sessionId — MUST already start with 'meta:'
 * @param {string} mid — target message mid
 * @returns {Promise<Array<object>>}
 */
async function findMessageRowsByMid(sessionId, mid) {
  if (!sessionId.startsWith('meta:')) {
    // Structurally unreachable (sessionId is always built as
    // `meta:${pageId}:${psid}`), but guarded explicitly per C1/M1b — the
    // recent-messages table is shared with live widget chat.
    throw new Error(`Refusing to query non-meta sessionId: ${sessionId}`);
  }

  const result = await dynamodb.send(
    new QueryCommand({
      TableName: RECENT_MESSAGES_TABLE,
      KeyConditionExpression: 'sessionId = :sid',
      ExpressionAttributeValues: { ':sid': sessionId },
    })
  );

  return (result.Items || []).filter((item) => item.mid === mid);
}

/**
 * Handle a `delete` event (C1): remove stored rows matching targetMid.
 * Idempotent — Meta redeliveries (edit/delete bypass webhook dedup) may
 * invoke this more than once for the same mid; zero matches is success.
 *
 * @param {string} sessionId
 * @param {string|null} targetMid
 * @returns {Promise<void>}
 */
async function handleDeleteEvent(sessionId, targetMid) {
  if (!sessionId.startsWith('meta:')) {
    log('ERROR', 'Refusing to process delete for non-meta sessionId', { sessionId });
    return;
  }
  if (!targetMid) {
    log('WARN', 'Delete event with no targetMid — nothing to do', { sessionId });
    return;
  }

  const matches = await findMessageRowsByMid(sessionId, targetMid);
  if (matches.length === 0) {
    log('INFO', 'Delete event: no matching rows (idempotent no-op)', { sessionId, targetMid });
    return;
  }

  for (const item of matches) {
    await dynamodb.send(
      new DeleteCommand({
        TableName: RECENT_MESSAGES_TABLE,
        Key: { sessionId: item.sessionId, messageTimestamp: item.messageTimestamp },
      })
    );
  }
  log('INFO', 'Delete event: removed matching rows', {
    sessionId,
    targetMid,
    deletedCount: matches.length,
  });
}

/**
 * Handle an `edit` event (C1): update the stored copy's content/text_en.
 * Idempotent — zero matches is success (row may have already expired or been
 * deleted).
 *
 * @param {string} sessionId
 * @param {string|null} targetMid
 * @param {string|null} editedText
 * @returns {Promise<void>}
 */
async function handleEditEvent(sessionId, targetMid, editedText) {
  if (!sessionId.startsWith('meta:')) {
    log('ERROR', 'Refusing to process edit for non-meta sessionId', { sessionId });
    return;
  }
  if (!targetMid) {
    log('WARN', 'Edit event with no targetMid — nothing to do', { sessionId });
    return;
  }

  const matches = await findMessageRowsByMid(sessionId, targetMid);
  if (matches.length === 0) {
    log('INFO', 'Edit event: no matching rows (idempotent no-op)', { sessionId, targetMid });
    return;
  }

  for (const item of matches) {
    await dynamodb.send(
      new UpdateCommand({
        TableName: RECENT_MESSAGES_TABLE,
        Key: { sessionId: item.sessionId, messageTimestamp: item.messageTimestamp },
        UpdateExpression: 'SET content = :c, text_en = :c',
        ExpressionAttributeValues: { ':c': editedText },
      })
    );
  }
  log('INFO', 'Edit event: updated matching rows', {
    sessionId,
    targetMid,
    updatedCount: matches.length,
  });
}

/**
 * Read a `messenger_behavior` string honoring C2 precedence:
 * channel_overrides.{channelType}.strings.{key} > messenger_behavior.strings.{key} > default.
 *
 * @param {object} config — tenant config
 * @param {string} channelType — 'messenger' | 'instagram'
 * @param {string} key — string key (C2 MessengerStrings)
 * @param {string} fallback — code-owned default
 * @returns {string}
 */
function getMessengerString(config, channelType, key, fallback) {
  const behavior = config.messenger_behavior || {};
  const channelOverride = behavior.channel_overrides?.[channelType]?.strings?.[key];
  if (channelOverride !== undefined) return channelOverride;
  const topLevel = behavior.strings?.[key];
  if (topLevel !== undefined) return topLevel;
  return fallback;
}

/**
 * Handle an unsupported-input event (attachment/sticker/unsupported, C1) when
 * `feature_flags.MESSENGER_CHANNEL` is on: reply with the configured fallback
 * string (30-second rule) — no Bedrock call, no history write.
 *
 * @param {{pageId: string, psid: string, channelType: string, config: object, sessionId: string, eventKind: string}} params
 * @returns {Promise<void>}
 */
async function handleUnsupportedInputFallback({ pageId, psid, channelType, config, sessionId, eventKind }) {
  let pageAccessToken;
  try {
    pageAccessToken = await loadPageAccessToken(pageId, channelType);
  } catch (tokenErr) {
    log('ERROR', 'Failed to load page access token for unsupported-input fallback — dropping', {
      pageId,
      channelType,
      error: tokenErr.message,
    });
    return;
  }

  const fallbackText = getMessengerString(
    config,
    channelType,
    'unsupported_input_fallback',
    DEFAULT_UNSUPPORTED_INPUT_FALLBACK
  );

  try {
    await sendResponseMessages(pageId, psid, fallbackText, pageAccessToken, channelType);
    log('INFO', 'Sent unsupported-input fallback', { sessionId, eventKind, channelType });
  } catch (sendErr) {
    log('ERROR', 'Failed to send unsupported-input fallback — will retry', {
      sessionId,
      error: sendErr.message,
    });
    throw sendErr;
  }
}

// ─── Channel-mapping metadata update ─────────────────────────────────────────

/**
 * Update lastUserMessageAt on the channel-mapping record. This timestamp is
 * used for 24-hour messaging-window enforcement (Meta policy).
 *
 * @param {string} pageId
 * @param {string} channelType
 * @returns {Promise<void>}
 */
async function updateLastUserMessageAt(pageId, channelType) {
  await dynamodb.send(
    new UpdateCommand({
      TableName: CHANNEL_MAPPINGS_TABLE,
      Key: {
        PK: `PAGE#${pageId}`,
        SK: `CHANNEL#${channelType}`,
      },
      UpdateExpression: 'SET lastUserMessageAt = :ts',
      ExpressionAttributeValues: {
        ':ts': new Date().toISOString(),
      },
    })
  );
  log('INFO', 'Updated lastUserMessageAt', { pageId, channelType });
}

// ─── Meta Graph API helpers ───────────────────────────────────────────────────

/**
 * POST a single action or message to the Meta Graph API messages endpoint.
 *
 * @param {string} pageId
 * @param {object} payload — Full request body (minus Authorization header)
 * @param {string} accessToken — Page access token
 * @param {number} [attempt=1] — Current retry attempt (1-indexed)
 * @returns {Promise<object>} — Parsed JSON response from Meta
 * @throws on 4xx (no retry) or after exhausting 5xx retries
 */
async function callMetaSendApi(pageId, payload, accessToken, attempt = 1, channelType = 'messenger') {
  const url = `${META_SEND_API_BASE}/${pageId}/messages`;
  const MAX_RETRIES = 3;

  let response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...payload, access_token: accessToken }),
    });
  } catch (networkErr) {
    log('ERROR', 'Network error calling Meta Send API', {
      pageId,
      attempt,
      error: networkErr.message,
    });
    throw networkErr;
  }

  if (response.ok) {
    return response.json();
  }

  const errorBody = await response.json().catch(() => ({}));
  const status = response.status;

  if (status >= 500 && attempt < MAX_RETRIES) {
    const delay = Math.pow(2, attempt - 1) * 1000; // 1s, 2s, 4s
    log('WARN', 'Meta Send API 5xx — retrying', { pageId, status, attempt, delayMs: delay });
    await sleep(delay);
    return callMetaSendApi(pageId, payload, accessToken, attempt + 1, channelType);
  }

  // M-Ha channel health: classified, structured failure line — CloudWatch
  // metric filters (ops-alarms-meta-staging) turn these into per-class
  // metrics + alarms (token_dead/page_restricted = channel death signals).
  const { classification, code, subcode } = classifyMetaSendError(errorBody);
  log('ERROR', 'META_SEND_FAILURE', {
    classification,
    code,
    subcode,
    status,
    pageId,
    channelType,
    attempt,
  });

  log('ERROR', 'Meta Send API error', { pageId, status, attempt, errorBody });
  throw new Error(
    `Meta Send API error: ${status} — ${JSON.stringify(errorBody)}`
  );
}

/**
 * Send a typing indicator to the given PSID.
 *
 * @param {string} pageId
 * @param {string} psid
 * @param {string} accessToken
 * @returns {Promise<void>}
 */
async function sendTypingIndicator(pageId, psid, accessToken, channelType) {
  // Instagram API doesn't support typing indicators via the same endpoint
  if (channelType === 'instagram') return;

  await callMetaSendApi(
    pageId,
    {
      recipient: { id: psid },
      sender_action: 'typing_on',
    },
    accessToken
  ).catch((err) => {
    // Typing indicators are best-effort; don't abort on failure
    log('WARN', 'Failed to send typing indicator', { pageId, psid, error: err.message });
  });
}

/**
 * Send a single text message to a Messenger recipient.
 *
 * @param {string} pageId
 * @param {string} psid
 * @param {string} text — Message text (<= MESSENGER_MAX_CHARS)
 * @param {string} accessToken
 * @returns {Promise<object>}
 */
async function sendMessengerMessage(pageId, psid, text, accessToken, channelType) {
  let url, headers, body;

  if (channelType === 'instagram') {
    // Instagram Messaging via Messenger Platform: SAME Send API as Messenger
    // (graph.facebook.com + Page access token). '/me' resolves to the Page the
    // token belongs to — the IG-account id from entry.id is NOT addressable
    // here. graph.instagram.com expects an Instagram-Login user token, which
    // this Page-linked integration does not hold (401 "Cannot parse access
    // token", found live 2026-07-12).
    url = `${META_SEND_API_BASE}/me/messages`;
    headers = { 'Content-Type': 'application/json' };
    body = JSON.stringify({
      recipient: { id: psid },
      message: { text },
      access_token: accessToken,
    });
  } else {
    // Facebook Messenger uses graph.facebook.com with access_token in body
    url = `${META_SEND_API_BASE}/${pageId}/messages`;
    headers = { 'Content-Type': 'application/json' };
    body = JSON.stringify({
      recipient: { id: psid },
      message: { text },
      messaging_type: 'RESPONSE',
      access_token: accessToken,
    });
  }

  const response = await fetch(url, { method: 'POST', headers, body });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    // M-Ha channel health: this is the path real replies take (typing rides
    // callMetaSendApi) — classify + emit the structured failure line the
    // ops-alarms metric filters watch (token_dead/page_restricted = channel
    // death signals).
    const { classification, code, subcode } = classifyMetaSendError(error);
    log('ERROR', 'META_SEND_FAILURE', {
      classification,
      code,
      subcode,
      status: response.status,
      pageId,
      channelType: channelType === 'instagram' ? 'instagram' : 'messenger',
    });
    throw new Error(`Meta Send API error: ${response.status} — ${JSON.stringify(error)}`);
  }
  return response.json();
}

// ─── Message splitting ────────────────────────────────────────────────────────

/**
 * Split a response into channel-safe chunks (<= maxChars each). Splits at the
 * last sentence-ending punctuation before the limit; falls back to the last
 * space if no sentence boundary is found.
 *
 * @param {string} text — Full response text
 * @param {number} maxChars — Per-message cap for the target channel (C5)
 * @returns {string[]} — Ordered array of chunks, each <= maxChars
 */
function splitMessage(text, maxChars = MESSAGE_CHAR_LIMITS.messenger) {
  if (text.length <= maxChars) {
    return [text];
  }

  const chunks = [];
  let remaining = text;

  while (remaining.length > maxChars) {
    const slice = remaining.slice(0, maxChars);

    // Find last sentence boundary within the slice
    const sentenceMatch = slice.match(/^([\s\S]*[.!?])\s/);
    let splitAt;

    if (sentenceMatch && sentenceMatch[1].length > 0) {
      splitAt = sentenceMatch[1].length + 1; // include the trailing space
    } else {
      // Fall back to last space
      const lastSpace = slice.lastIndexOf(' ');
      splitAt = lastSpace > 0 ? lastSpace + 1 : maxChars;
    }

    chunks.push(remaining.slice(0, splitAt).trimEnd());
    remaining = remaining.slice(splitAt).trimStart();
  }

  if (remaining.length > 0) {
    chunks.push(remaining);
  }

  return chunks;
}

/**
 * Send a (potentially long) response as one or more sequential Messenger
 * messages, with a brief delay between each to preserve ordering.
 *
 * @param {string} pageId
 * @param {string} psid
 * @param {string} text — Full response text
 * @param {string} accessToken
 * @returns {Promise<void>}
 */
async function sendResponseMessages(pageId, psid, text, accessToken, channelType) {
  const maxChars = MESSAGE_CHAR_LIMITS[channelType] || MESSAGE_CHAR_LIMITS.messenger;
  const chunks = splitMessage(text, maxChars);
  log('INFO', 'Sending response', { pageId, psid, chunks: chunks.length, totalChars: text.length, channelType });

  for (let i = 0; i < chunks.length; i++) {
    if (i > 0) {
      await sleep(SPLIT_MESSAGE_DELAY_MS);
    }
    await sendMessengerMessage(pageId, psid, chunks[i], accessToken, channelType);
    log('INFO', 'Sent message chunk', { pageId, psid, chunk: i + 1, of: chunks.length });
  }
}

// ─── RAG pipeline ─────────────────────────────────────────────────────────────

/**
 * Build the Bedrock prompt for a Messenger response.
 *
 * Combines the tenant tone prompt, a Messenger-specific system instruction,
 * KB context retrieved via RAG, and recent conversation history.
 *
 * @param {string} userInput — Sanitised user message
 * @param {string} kbContext — Retrieved knowledge-base passages
 * @param {Array<{role: string, content: string}>} history — Recent turns
 * @param {object} config — Tenant config object
 * @returns {Array<object>} — Messages array for the Bedrock Converse/Messages API
 */
function buildMessengerPrompt(userInput, kbContext, history, config) {
  const tonePrompt = config.tone_prompt || DEFAULT_TONE;

  const systemContent = [
    tonePrompt,
    'You are responding via Facebook Messenger where the chat window is very small. STRICT RULES: Respond in 2-3 short sentences maximum. Be friendly but direct. No lists, no bullet points, no headers, no markdown. Never write more than 3 sentences in a single response. If the user wants more detail, they will ask a follow-up question.',
    kbContext
      ? `Relevant information from the knowledge base:\n${kbContext}`
      : '',
  ]
    .filter(Boolean)
    .join('\n\n');

  // Build messages array from history (last MAX_HISTORY_TURNS pairs)
  const recentHistory = history.slice(-MAX_HISTORY_TURNS * 2);
  const messages = recentHistory.map((m) => ({
    role: m.role,
    content: [{ type: 'text', text: m.content }],
  }));

  // Append the current user turn
  messages.push({
    role: 'user',
    content: [{ type: 'text', text: userInput }],
  });

  return { systemContent, messages };
}

/**
 * Strip residual markdown formatting markers the model may emit despite the
 * plain-text rule (M3a evidence: 0/40 post-hardening, this is belt-and-
 * suspenders) - Messenger/IG render these characters literally.
 */
function stripFormattingMarkers(text) {
  return String(text || '')
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/__([^_]+)__/g, '$1');
}

/**
 * Messenger V5 single-pass turn (M3b): build the Messenger V5 prompt over
 * SESSION-SCOPED history (C8), invoke buffered, then parse the ACTION tail
 * the chunking-invariant way (feed(full)+end()).
 *
 * Fallback ladder (frozen, mirrors BSH): valid tail -> validateActionIds;
 * malformed/missing tail while CTAs exist -> ONE selectActionsV4 call ->
 * on any failure, no actions. The reply text is always served.
 *
 * @returns {Promise<{responseText: string, actionIds: string[], tailStatus: string}>}
 */
async function generateMessengerV5Response(userInput, kbContext, sessionHistory, config, channelType) {
  const modelId = config.model_id || DEFAULT_MODEL_ID;
  const maxTokens = config.streaming?.max_tokens || 1000;
  const temperature = config.streaming?.temperature ?? 0;

  const { systemContent, messages, v5Active } = buildMessengerV5Prompt(
    userInput,
    kbContext,
    config,
    sessionHistory,
    channelType
  );

  const requestBody = {
    anthropic_version: 'bedrock-2023-05-31',
    max_tokens: maxTokens,
    temperature,
    system: systemContent,
    messages,
  };

  log('INFO', 'Calling Bedrock InvokeModel (Messenger V5)', {
    modelId,
    v5Active,
    sessionTurns: Math.floor(messages.length / 2),
  });

  const command = new InvokeModelCommand({
    modelId,
    contentType: 'application/json',
    accept: 'application/json',
    body: JSON.stringify(requestBody),
  });
  const result = await bedrockRuntime.send(command);
  const responseBody = JSON.parse(Buffer.from(result.body).toString('utf-8'));
  const rawText = responseBody.content?.[0]?.text || responseBody.completion || '';

  if (!v5Active) {
    // No ai_available CTAs: plain short-form prompt, no tail to parse.
    return { responseText: stripFormattingMarkers(rawText), actionIds: [], tailStatus: 'no_catalog' };
  }

  const parser = createTailParser();
  const released = parser.feed(rawText);
  const tail = parser.end();
  const visible = stripFormattingMarkers(released + (tail.remainingText || ''));

  if (tail.status === 'actions') {
    const ids = validateActionIds(tail.actionIds, config);
    log('INFO', 'Messenger V5 tail parsed', { tailStatus: tail.status, selected: ids });
    return { responseText: visible, actionIds: ids, tailStatus: tail.status };
  }

  // Malformed or missing tail: ONE fail-soft V4 selector call, then give up
  // on actions (never on the reply).
  log('WARN', 'Messenger V5 tail missing/malformed - fail-soft selectActionsV4', {
    tailStatus: tail.status,
  });
  try {
    const ids = await selectActionsV4(visible, sessionHistory, config, bedrockRuntime);
    return { responseText: visible, actionIds: Array.isArray(ids) ? ids : [], tailStatus: tail.status };
  } catch (fallbackErr) {
    log('WARN', 'selectActionsV4 fallback failed - no actions', { error: fallbackErr.message });
    return { responseText: visible, actionIds: [], tailStatus: tail.status };
  }
}

/**
 * Invoke Bedrock InvokeModel (non-streaming / buffered) and return the
 * assistant's reply text.
 *
 * @param {string} userInput — Sanitised user message
 * @param {string} kbContext — KB retrieval result
 * @param {Array<object>} history — Recent conversation turns
 * @param {object} config — Tenant config
 * @returns {Promise<string>} — Assistant response text
 */
async function generateBedrockResponse(userInput, kbContext, history, config) {
  const modelId = config.model_id || DEFAULT_MODEL_ID;
  const maxTokens = config.streaming?.max_tokens || 1000;
  const temperature = config.streaming?.temperature ?? 0;

  const { systemContent, messages } = buildMessengerPrompt(
    userInput,
    kbContext,
    history,
    config
  );

  const requestBody = {
    anthropic_version: 'bedrock-2023-05-31',
    max_tokens: maxTokens,
    temperature,
    system: systemContent,
    messages,
  };

  log('INFO', 'Calling Bedrock InvokeModel', {
    modelId,
    maxTokens,
    temperature,
    historyTurns: Math.floor(messages.length / 2),
  });

  const command = new InvokeModelCommand({
    modelId,
    contentType: 'application/json',
    accept: 'application/json',
    body: JSON.stringify(requestBody),
  });

  const result = await bedrockRuntime.send(command);
  const responseBody = JSON.parse(Buffer.from(result.body).toString('utf-8'));

  const text =
    responseBody.content?.[0]?.text ||
    responseBody.completion ||
    '';

  log('INFO', 'Bedrock response received', {
    modelId,
    outputTokens: responseBody.usage?.output_tokens,
    inputTokens: responseBody.usage?.input_tokens,
    responseLength: text.length,
  });

  return text;
}

// ─── Utilities ────────────────────────────────────────────────────────────────

/**
 * Resolve a millisecond delay.
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Basic input validation on the incoming Lambda event.
 * @param {object} event
 * @throws if required fields are missing or obviously invalid
 */
function validateEvent(event) {
  const required = ['psid', 'messageText', 'pageId', 'tenantId', 'tenantHash'];
  for (const field of required) {
    if (!event[field] || typeof event[field] !== 'string') {
      throw new Error(`Invalid event: missing or non-string field "${field}"`);
    }
  }
  if (typeof event.messageText !== 'string' || event.messageText.trim().length === 0) {
    throw new Error('Invalid event: messageText is empty');
  }
}

/**
 * Shape validation for v2 event kinds that legitimately carry
 * messageText:null (edit/delete/echo/attachment/sticker/unsupported — C1).
 * Deliberately does NOT require messageText — validateEvent above still owns
 * that check for the text-turn path.
 * @param {object} event
 * @throws if required v1 fields are missing
 */
function validateV2BaseEvent(event) {
  const required = ['psid', 'pageId', 'tenantId', 'tenantHash'];
  for (const field of required) {
    if (!event[field] || typeof event[field] !== 'string') {
      throw new Error(`Invalid v2 event: missing or non-string field "${field}"`);
    }
  }
}

// ─── Lambda handler ───────────────────────────────────────────────────────────

/**
 * Main Lambda handler — invoked asynchronously by Meta_Webhook_Handler.
 *
 * Because this function is invoked asynchronously (InvokeType=Event), any
 * exception causes Lambda to retry up to 2 times. For unrecoverable errors
 * (bad event shape, missing token) we log and return cleanly to avoid
 * wasting retries. For transient errors (Bedrock timeout, Meta 5xx) we throw
 * so Lambda retries naturally (or the DLQ captures the failure).
 *
 * @param {object} event — Incoming event from Meta_Webhook_Handler
 * @returns {Promise<void>}
 */
exports.handler = async function handler(event) {
  const startTime = Date.now();

  log('INFO', 'Handler invoked', {
    pageId: event.pageId,
    psid: event.psid,
    tenantId: event.tenantId,
    tenantHash: event.tenantHash?.substring(0, 8),
    channelType: event.channelType || 'messenger',
    messageMid: event.messageMid,
  });

  // ── v2 event-kind routing (C1; M1b hygiene) ──────────────────────────────
  // Handled BEFORE validateEvent because several v2 kinds legitimately carry
  // messageText:null (edit/delete/echo/attachment/sticker/unsupported).
  // v1 payloads (no `v`/`eventKind`) fall straight through to the unchanged
  // legacy path below.
  if (event && event.v === 2 && typeof event.eventKind === 'string') {
    try {
      validateV2BaseEvent(event);
    } catch (validationErr) {
      // Shape-only diagnostics — NEVER the event object: v2 payloads carry
      // user content (messageText/editedText/replyTo) that must not reach
      // CloudWatch (G-P1).
      log('ERROR', 'Event validation failed — dropping message', {
        error: validationErr.message,
        eventKind: event.eventKind,
        pageId: event.pageId,
        psid: event.psid,
        messageMid: event.messageMid,
        keys: Object.keys(event),
      });
      return;
    }

    const eventKind = event.eventKind;
    const v2PageId = event.pageId;
    const v2Psid = event.psid;
    const v2ChannelType = event.channelType || 'messenger';
    const sessionId = `meta:${v2PageId}:${v2Psid}`;

    // ── echo / standby: staff/self traffic — never answer, never store ────
    if (eventKind === 'echo' || event.isStandby === true) {
      log('INFO', 'Echo or standby event — no reply, no history write', {
        sessionId,
        eventKind,
        isStandby: event.isStandby === true,
      });
      return;
    }

    // ── delete: Meta terms require removing stored copies (C5) ────────────
    if (eventKind === 'delete') {
      await handleDeleteEvent(sessionId, event.targetMid);
      return;
    }

    // ── edit: update the stored copy in place ──────────────────────────────
    if (eventKind === 'edit') {
      await handleEditEvent(sessionId, event.targetMid, event.editedText);
      return;
    }

    // ── attachment / sticker / unsupported: flag-gated 30-second fallback ──
    if (eventKind === 'attachment' || eventKind === 'sticker' || eventKind === 'unsupported') {
      // 24h send-window guard (same rule as the legacy path's step 0a): a
      // stale/DLQ-redelivered event must never trigger an outbound send.
      // delete/edit/echo above are exempt on purpose — they never send.
      if (event.timestamp && Date.now() - event.timestamp > MESSAGE_WINDOW_MS) {
        log('WARN', '24-hour messaging window exceeded — dropping fallback reply', {
          sessionId,
          eventKind,
          messageAgeHours: Math.round((Date.now() - event.timestamp) / (60 * 60 * 1000)),
        });
        return;
      }

      let v2Config = {};
      try {
        v2Config = (await loadConfig(event.tenantHash)) || {};
      } catch (configErr) {
        log('WARN', 'Failed to load tenant config for unsupported-input fallback — using defaults', {
          tenantHash: event.tenantHash,
          error: configErr.message,
        });
        v2Config = {};
      }

      if (v2Config.feature_flags?.MESSENGER_CHANNEL === true) {
        await handleUnsupportedInputFallback({
          pageId: v2PageId,
          psid: v2Psid,
          channelType: v2ChannelType,
          config: v2Config,
          sessionId,
          eventKind,
        });
        return;
      }
      // Flag off: fall through to the legacy pipeline below — validateEvent's
      // messageText requirement drops this exactly as it did before v2 (C1
      // deploy-ordering guarantee — byte-identical baseline).
    }
    // quick_reply / postback / text eventKinds: no special M1b handling here —
    // fall through to the pipeline below unchanged.
  }

  // ── 0. Input validation ──────────────────────────────────────────────────
  try {
    validateEvent(event);
  } catch (validationErr) {
    log('ERROR', 'Event validation failed — dropping message', {
      error: validationErr.message,
      event,
    });
    // Return without throwing: bad events should not be retried
    return;
  }

  const { psid, messageText, pageId, tenantId, tenantHash, messageMid } = event;
  const channelType = event.channelType || 'messenger';

  // ── 0a. 24-hour messaging-window check ───────────────────────────────────
  // Protects against stale DLQ retries arriving long after the user messaged us.
  // Meta policy: bots may only send within 24 h of the user's last message.
  if (event.timestamp) {
    const messageAge = Date.now() - event.timestamp;
    if (messageAge > MESSAGE_WINDOW_MS) {
      log('WARN', '24-hour messaging window exceeded — dropping response', {
        pageId,
        psid,
        messageMid,
        messageAgeHours: Math.round(messageAge / (60 * 60 * 1000)),
      });
      return;
    }
  }

  // Sanitise user input before any processing
  const sanitizedInput = sanitizeUserInput(messageText);
  if (!sanitizedInput) {
    log('WARN', 'sanitizeUserInput returned empty string — dropping message', {
      pageId,
      psid,
      messageMid,
    });
    return;
  }

  // Emit MESSENGER_MESSAGE_RECEIVED after successful input validation (best-effort)
  emitAnalyticsEvent('MESSENGER_MESSAGE_RECEIVED', {
    session_id: `meta:${pageId}:${psid}`,
    tenant_id: tenantId,
    channel_type: channelType,
    page_id: pageId,
    psid,
    message_length: sanitizedInput.length,
    is_postback: event.isPostback === true,
  });

  // ── 1a. Per-conversation serialization (contract C7, M1c) ────────────────
  // One winner per sessionId runs turns; concurrent invokes coalesce their
  // message onto the winner's pending list and exit. Fail-open: lock-table
  // errors must never block replies (serialization is an optimization of
  // correctness, not a gate on it).
  const lockSessionId = `meta:${pageId}:${psid}`;
  let lockCtx = null;
  let inheritedPending = [];
  if (CONVERSATION_STATE_TABLE) {
    const lockOwner = crypto.randomUUID();
    try {
      const gate = await conversationLock.acquireOrCoalesce({
        client: dynamodb,
        tableName: CONVERSATION_STATE_TABLE,
        sessionId: lockSessionId,
        owner: lockOwner,
        pendingItem: {
          timestamp: typeof event.timestamp === 'number' ? event.timestamp : Date.now(),
          mid: messageMid,
          text: sanitizedInput,
          ...(event.eventKind ? { eventKind: event.eventKind } : {}),
          ...(event.quickReplyPayload ? { quickReplyPayload: event.quickReplyPayload } : {}),
        },
      });
      if (gate.role === 'coalesced') {
        log('INFO', 'Coalesced onto the in-flight turn — exiting (C7)', {
          sessionId: lockSessionId,
        });
        return;
      }
      lockCtx = {
        client: dynamodb,
        tableName: CONVERSATION_STATE_TABLE,
        sessionId: lockSessionId,
        owner: lockOwner,
      };
      inheritedPending = Array.isArray(gate.inheritedPending) ? gate.inheritedPending : [];
      if (gate.degraded) {
        log('WARN', 'Lock races exhausted — proceeding unserialized (C7 no-drop over serialization)', {
          sessionId: lockSessionId,
        });
        lockCtx = null; // we do not own the row — never drain/release it
      }
      if (inheritedPending.length > 0) {
        log('INFO', 'Inherited pending items from a stale lock takeover', {
          sessionId: lockSessionId,
          count: inheritedPending.length,
        });
      }
    } catch (lockErr) {
      log('WARN', 'Serialization lock error — proceeding unserialized (fail-open)', {
        sessionId: lockSessionId,
        error: lockErr.message,
      });
      lockCtx = null;
    }
  }

  /**
   * Drain-and-release (C7 steps 3-6). Runs after the winner's own turn (and
   * at early-return sites): claims coalesced pending items, answers them in
   * combined turns (Bedrock) up to DRAIN_CAP cycles, then answers further
   * bursts with the rate_limited string (bounded spend, no drops), and
   * finally releases the lock CONDITIONALLY on the pending list being empty.
   */
  async function finalizeConversationLock(runTurnFn) {
    if (!lockCtx) return;
    let batch = inheritedPending;
    inheritedPending = [];
    let bedrockCycles = 0;
    for (let guard = 0; guard < conversationLock.DRAIN_CAP + 5; guard++) {
      try {
        if (batch.length === 0) {
          batch = await conversationLock.claimPending(lockCtx);
        }
        if (batch.length === 0) {
          const { released } = await conversationLock.releaseIfIdle(lockCtx);
          if (released) return;
          continue; // new pending raced in between claim and release — drain again
        }
        const texts = batch
          .map((i) => (i && typeof i.text === 'string' ? i.text : null))
          .filter((t) => t && t.length > 0);
        const lastMid = batch.length > 0 ? batch[batch.length - 1].mid || null : null;
        batch = [];
        if (texts.length === 0) continue;
        bedrockCycles++;
        if (bedrockCycles <= conversationLock.DRAIN_CAP && runTurnFn) {
          await runTurnFn(texts.join('\n'), lastMid, { withTyping: false, withDisclosure: false });
        } else {
          const rateLimitedText = getMessengerString(
            config,
            channelType,
            'rate_limited',
            DEFAULT_RATE_LIMITED
          );
          await sendResponseMessages(pageId, psid, rateLimitedText, pageAccessToken, channelType);
          log('WARN', 'Drain cap exceeded — answered burst with rate_limited (no Bedrock)', {
            sessionId: lockSessionId,
            bedrockCycles,
          });
        }
      } catch (drainErr) {
        // Leave the lock for TTL takeover (the next winner inherits pending).
        log('WARN', 'Drain/release error — leaving lock to TTL self-heal', {
          sessionId: lockSessionId,
          error: drainErr.message,
        });
        return;
      }
    }
    log('WARN', 'Drain guard bound hit — leaving lock to TTL self-heal', {
      sessionId: lockSessionId,
    });
  }

  // ── 1. Load page access token ────────────────────────────────────────────
  let pageAccessToken;
  try {
    pageAccessToken = await loadPageAccessToken(pageId, channelType);
  } catch (tokenErr) {
    log('ERROR', 'Failed to load/decrypt page access token — dropping message', {
      pageId,
      channelType,
      error: tokenErr.message,
    });
    // Release the lock if idle; with pending present we leave it to TTL
    // takeover (every coalesced invoke would fail on the same missing token).
    if (lockCtx) {
      try {
        await conversationLock.releaseIfIdle(lockCtx);
      } catch (releaseErr) {
        log('WARN', 'Lock release after token failure failed — TTL will heal', {
          sessionId: lockSessionId,
          error: releaseErr.message,
        });
      }
    }
    // Non-retryable: no token means we cannot respond
    return;
  }

  // ── 0b. Load tenant config (needed for postback handling and RAG) ─────────
  // Config is loaded early so the GET_STARTED postback can read welcome_message
  // without running the full RAG pipeline.
  let config = {};
  try {
    config = await loadConfig(tenantHash);
    if (!config) {
      log('WARN', 'loadConfig returned null — using defaults', { tenantHash });
      config = {};
    }
  } catch (configErr) {
    log('WARN', 'Failed to load tenant config — using defaults', {
      tenantHash,
      error: configErr.message,
    });
    config = {};
  }

  // ── 0c. Postback handling ────────────────────────────────────────────────
  // GET_STARTED: short-circuit to welcome_message; skip RAG pipeline entirely.
  // Other postback payloads fall through to the RAG pipeline so the AI can
  // respond contextually (the payload string becomes the user query).
  if (event.isPostback === true && messageText === 'GET_STARTED') {
    const welcomeMessage =
      config.welcome_message ||
      "Hello! I'm here to help. What can I do for you today?";

    log('INFO', 'GET_STARTED postback — sending welcome message', {
      pageId,
      psid,
      welcomeLength: welcomeMessage.length,
    });

    // Send typing indicator (best-effort) then welcome message
    await sendTypingIndicator(pageId, psid, pageAccessToken, channelType);

    try {
      await sendResponseMessages(pageId, psid, welcomeMessage, pageAccessToken, channelType);
    } catch (sendErr) {
      log('ERROR', 'Failed to send welcome message — will retry', {
        pageId, psid, error: sendErr.message,
      });
      throw sendErr;
    }

    // Still store context and update lastUserMessageAt
    try {
      await storeConversationContext(pageId, psid, messageText, welcomeMessage, messageMid);
    } catch (storeErr) {
      log('WARN', 'Failed to store GET_STARTED context', { pageId, psid, error: storeErr.message });
    }
    try {
      await updateLastUserMessageAt(pageId, channelType);
    } catch (updateErr) {
      log('WARN', 'Failed to update lastUserMessageAt after GET_STARTED', {
        pageId, channelType, error: updateErr.message,
      });
    }

    // Drain any messages that coalesced while we handled the postback, then
    // release (C7) — runTurn is hoisted, and config/token are loaded above.
    await finalizeConversationLock(runTurn);

    const durationMs = Date.now() - startTime;
    log('INFO', 'Handler complete (GET_STARTED postback)', { pageId, psid, durationMs });
    return;
  }

  /**
   * One full conversational turn — steps 2–7 (typing, history, RAG, disclosure,
   * send, store, lastUserMessageAt). Extracted so C7 drain cycles can answer
   * coalesced bursts through the identical pipeline (M1c). Drain cycles skip
   * typing + disclosure.
   */
  async function runTurn(turnText, turnMid, { withTyping = true, withDisclosure = true } = {}) {
    // ── 2. Send typing indicator (best-effort) ─────────────────────────────
    let typingRefreshInterval = null;
    if (withTyping) {
      await sendTypingIndicator(pageId, psid, pageAccessToken, channelType);

      // Start typing-indicator refresh so the indicator stays alive while
      // Bedrock processes (Meta expires typing_on after ~10 s; refresh 8 s).
      typingRefreshInterval = setInterval(async () => {
        try {
          await sendTypingIndicator(pageId, psid, pageAccessToken, channelType);
        } catch (e) {
          // Non-fatal — don't disrupt the main pipeline
          log('WARN', 'Typing indicator refresh failed', { pageId, psid, error: e.message });
        }
      }, TYPING_REFRESH_INTERVAL_MS);
    }

    // ── 3. Load conversation context ───────────────────────────────────────
    let conversationHistory = [];
    try {
      conversationHistory = await loadConversationContext(pageId, psid);
    } catch (ctxErr) {
      // Non-fatal: proceed without history
      log('WARN', 'Failed to load conversation context', {
        pageId,
        psid,
        error: ctxErr.message,
      });
    }

    // ── 4. RAG pipeline: retrieve KB → generate response ───────────────────
    // Config was already loaded in step 0b.
    let responseText;
    let kbContextLength = 0;
    let selectedActionIds = [];

    // C8 session window (flag-gated lane only): prompt history, turn-check
    // counting, and the disclosure trigger all see the CURRENT SESSION, never
    // the lifetime thread.
    const messengerFlagOn = config.feature_flags?.MESSENGER_CHANNEL === true;
    const sessionWindow = messengerFlagOn
      ? computeSessionWindow(conversationHistory)
      : { sessionMessages: conversationHistory, isSessionFirstTurn: conversationHistory.length === 0 };

    try {
      const kbContext = await retrieveKB(turnText, config);
      kbContextLength = kbContext?.length || 0;

      log('INFO', 'KB retrieval complete', {
        tenantHash: tenantHash.substring(0, 8),
        kbContextLength,
      });

      if (messengerFlagOn) {
        // Messenger V5 single-pass turn (M3b). Until M4 renders them, the
        // validated action ids are logged only.
        const v5 = await generateMessengerV5Response(
          turnText,
          kbContext,
          sessionWindow.sessionMessages,
          config,
          channelType
        );
        responseText = v5.responseText;
        selectedActionIds = v5.actionIds;
        if (selectedActionIds.length > 0) {
          log('INFO', 'Messenger V5 actions selected (rendered from M4)', {
            selected: selectedActionIds,
            tailStatus: v5.tailStatus,
          });
        }
      } else {
        responseText = await generateBedrockResponse(
          turnText,
          kbContext,
          conversationHistory,
          config
        );
      }
    } catch (bedrockErr) {
      log('ERROR', 'Bedrock pipeline failed — sending fallback message', {
        tenantHash: tenantHash.substring(0, 8),
        error: bedrockErr.message,
      });
      responseText =
        config.bedrock_instructions?.fallback_message || DEFAULT_FALLBACK_MESSAGE;
    }

    if (!responseText || responseText.trim().length === 0) {
      responseText = config.bedrock_instructions?.fallback_message || DEFAULT_FALLBACK_MESSAGE;
    }

    // ── 5. Stop typing refresh and send response via Meta Send API ─────────
    if (typingRefreshInterval) clearInterval(typingRefreshInterval);

    // ── 4b. Disclosure line (C2 strings, C8 session-first-turn) ────────────
    // Flag-gated: sent as its own message, BEFORE the normal reply, only on
    // the first turn of a session (empty loaded history). Flag off ⇒ no
    // disclosure (byte-identical to pre-program baseline).
    if (
      withDisclosure &&
      messengerFlagOn &&
      sessionWindow.isSessionFirstTurn
    ) {
      const disclosureText = getMessengerString(
        config,
        channelType,
        'disclosure_line',
        DEFAULT_DISCLOSURE_LINE
      );
      try {
        await sendResponseMessages(pageId, psid, disclosureText, pageAccessToken, channelType);
        log('INFO', 'Sent disclosure line (first turn of session)', {
          pageId,
          psid,
          sessionId: `meta:${pageId}:${psid}`,
        });
      } catch (discErr) {
        // Non-fatal: don't block the real reply on the disclosure send failing.
        log('WARN', 'Failed to send disclosure line — continuing with reply', {
          pageId,
          psid,
          error: discErr.message,
        });
      }
    }

    try {
      await sendResponseMessages(pageId, psid, responseText, pageAccessToken, channelType);
      log('INFO', 'Response delivered to Messenger', {
        pageId,
        psid,
        responseLength: responseText.length,
      });

      // Emit MESSENGER_RESPONSE_SENT after successful delivery (best-effort)
      emitAnalyticsEvent('MESSENGER_RESPONSE_SENT', {
        session_id: `meta:${pageId}:${psid}`,
        tenant_id: tenantId,
        channel_type: channelType,
        page_id: pageId,
        psid,
        response_length: responseText.length,
        model_used: config.model_id || DEFAULT_MODEL_ID,
        kb_context_length: kbContextLength,
      });
    } catch (sendErr) {
      // Re-throw so Lambda retries (or DLQ captures)
      log('ERROR', 'Failed to deliver response to Messenger — will retry', {
        pageId,
        psid,
        error: sendErr.message,
      });
      throw sendErr;
    }

    // ── 6. Store conversation context ───────────────────────────────────────
    try {
      await storeConversationContext(pageId, psid, turnText, responseText, turnMid);
    } catch (storeErr) {
      // Non-fatal: log and continue
      log('WARN', 'Failed to store conversation context', {
        pageId,
        psid,
        error: storeErr.message,
      });
    }

    // ── 7. Update lastUserMessageAt ─────────────────────────────────────────
    try {
      await updateLastUserMessageAt(pageId, channelType);
    } catch (updateErr) {
      // Non-fatal: log and continue
      log('WARN', 'Failed to update lastUserMessageAt', {
        pageId,
        channelType,
        error: updateErr.message,
      });
    }
  }

  await runTurn(sanitizedInput, messageMid);

  // ── 8. Drain coalesced messages + conditional release (C7, M1c) ──────────
  await finalizeConversationLock(runTurn);

  const durationMs = Date.now() - startTime;
  log('INFO', 'Handler complete', {
    pageId,
    psid,
    tenantId,
    messageMid,
    durationMs,
  });
};
