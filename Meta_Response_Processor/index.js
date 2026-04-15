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
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { KMSClient, DecryptCommand } = require('@aws-sdk/client-kms');
const { loadConfig, retrieveKB, sanitizeUserInput } = require('../shared/bedrock-core');

// ─── AWS client initialisation ────────────────────────────────────────────────

const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const ENVIRONMENT = process.env.ENVIRONMENT || 'staging';

const bedrockRuntime = new BedrockRuntimeClient({ region: AWS_REGION });

const dynamodbRaw = new DynamoDBClient({ region: AWS_REGION });
const dynamodb = DynamoDBDocumentClient.from(dynamodbRaw, {
  marshallOptions: { removeUndefinedValues: true },
});

const kms = new KMSClient({ region: AWS_REGION });

// ─── Configuration constants ──────────────────────────────────────────────────

const CHANNEL_MAPPINGS_TABLE =
  process.env.CHANNEL_MAPPINGS_TABLE || `picasso-channel-mappings-${ENVIRONMENT}`;
const RECENT_MESSAGES_TABLE =
  process.env.RECENT_MESSAGES_TABLE || `${ENVIRONMENT}-recent-messages`;
const KMS_KEY_ID = process.env.KMS_KEY_ID || 'alias/picasso-channel-tokens';

const DEFAULT_MODEL_ID = 'global.anthropic.claude-haiku-4-5-20251001-v1:0';
const DEFAULT_TONE = 'You are a helpful assistant.';
const DEFAULT_FALLBACK_MESSAGE =
  "I'm sorry, I'm having trouble right now. Please try again in a moment.";

const META_GRAPH_VERSION = 'v21.0';
const META_SEND_API_BASE = `https://graph.facebook.com/${META_GRAPH_VERSION}`;

/** Maximum characters per Messenger message (Meta platform limit) */
const MESSENGER_MAX_CHARS = 2000;

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
 * DynamoDB schema (mirrors the web handler's recent-messages table):
 *   PK:      session_key  (string) — e.g. "meta:{pageId}:{psid}"
 *   messages (list)       — [{role, content, timestamp}, ...]
 *
 * @param {string} pageId
 * @param {string} psid
 * @returns {Promise<Array<{role: string, content: string, timestamp: string}>>}
 */
async function loadConversationContext(pageId, psid) {
  const sessionKey = `meta:${pageId}:${psid}`;
  log('INFO', 'Loading conversation context', {
    sessionKey,
    table: RECENT_MESSAGES_TABLE,
  });

  const result = await dynamodb.send(
    new GetCommand({
      TableName: RECENT_MESSAGES_TABLE,
      Key: { session_key: sessionKey },
    })
  );

  const messages = result.Item?.messages || [];
  log('INFO', 'Conversation context loaded', { sessionKey, messageCount: messages.length });
  return messages;
}

/**
 * Persist the latest Q&A pair to the recent-messages table, keeping only the
 * last MAX_STORED_PAIRS entries (rolling window).
 *
 * @param {string} pageId
 * @param {string} psid
 * @param {string} userText — sanitised user input
 * @param {string} assistantText — AI response
 * @returns {Promise<void>}
 */
async function storeConversationContext(pageId, psid, userText, assistantText) {
  const sessionKey = `meta:${pageId}:${psid}`;
  const now = new Date().toISOString();

  // Load existing messages first
  const existing = await loadConversationContext(pageId, psid);

  const updated = [
    ...existing,
    { role: 'user', content: userText, timestamp: now },
    { role: 'assistant', content: assistantText, timestamp: now },
  ];

  // Keep rolling window: most-recent MAX_STORED_PAIRS pairs = 2*MAX_STORED_PAIRS messages
  const trimmed = updated.slice(-MAX_STORED_PAIRS * 2);

  await dynamodb.send(
    new PutCommand({
      TableName: RECENT_MESSAGES_TABLE,
      Item: {
        session_key: sessionKey,
        messages: trimmed,
        updatedAt: now,
        ttl: Math.floor(Date.now() / 1000) + 60 * 60 * 24 * 7, // 7-day TTL
      },
    })
  );

  log('INFO', 'Conversation context stored', {
    sessionKey,
    storedMessages: trimmed.length,
  });
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
async function callMetaSendApi(pageId, payload, accessToken, attempt = 1) {
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
    return callMetaSendApi(pageId, payload, accessToken, attempt + 1);
  }

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
async function sendTypingIndicator(pageId, psid, accessToken) {
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
async function sendMessengerMessage(pageId, psid, text, accessToken) {
  const url = `${META_SEND_API_BASE}/${pageId}/messages`;
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      recipient: { id: psid },
      message: { text },
      messaging_type: 'RESPONSE',
      access_token: accessToken,
    }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(`Meta Send API error: ${response.status} — ${JSON.stringify(error)}`);
  }
  return response.json();
}

// ─── Message splitting ────────────────────────────────────────────────────────

/**
 * Split a response into Messenger-safe chunks (<= MESSENGER_MAX_CHARS each).
 * Splits at the last sentence-ending punctuation before the limit; falls back
 * to the last space if no sentence boundary is found.
 *
 * @param {string} text — Full response text
 * @returns {string[]} — Ordered array of chunks, each <= MESSENGER_MAX_CHARS
 */
function splitMessage(text) {
  if (text.length <= MESSENGER_MAX_CHARS) {
    return [text];
  }

  const chunks = [];
  let remaining = text;

  while (remaining.length > MESSENGER_MAX_CHARS) {
    const slice = remaining.slice(0, MESSENGER_MAX_CHARS);

    // Find last sentence boundary within the slice
    const sentenceMatch = slice.match(/^([\s\S]*[.!?])\s/);
    let splitAt;

    if (sentenceMatch && sentenceMatch[1].length > 0) {
      splitAt = sentenceMatch[1].length + 1; // include the trailing space
    } else {
      // Fall back to last space
      const lastSpace = slice.lastIndexOf(' ');
      splitAt = lastSpace > 0 ? lastSpace + 1 : MESSENGER_MAX_CHARS;
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
async function sendResponseMessages(pageId, psid, text, accessToken) {
  const chunks = splitMessage(text);
  log('INFO', 'Sending response', { pageId, psid, chunks: chunks.length, totalChars: text.length });

  for (let i = 0; i < chunks.length; i++) {
    if (i > 0) {
      await sleep(SPLIT_MESSAGE_DELAY_MS);
    }
    await sendMessengerMessage(pageId, psid, chunks[i], accessToken);
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
    'You are responding via Facebook Messenger. Keep responses concise and conversational. Do not use markdown formatting — Messenger renders plain text only.',
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
    await sendTypingIndicator(pageId, psid, pageAccessToken);

    try {
      await sendResponseMessages(pageId, psid, welcomeMessage, pageAccessToken);
    } catch (sendErr) {
      log('ERROR', 'Failed to send welcome message — will retry', {
        pageId, psid, error: sendErr.message,
      });
      throw sendErr;
    }

    // Still store context and update lastUserMessageAt
    try {
      await storeConversationContext(pageId, psid, messageText, welcomeMessage);
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

    const durationMs = Date.now() - startTime;
    log('INFO', 'Handler complete (GET_STARTED postback)', { pageId, psid, durationMs });
    return;
  }

  // ── 2. Send typing indicator (best-effort) ───────────────────────────────
  await sendTypingIndicator(pageId, psid, pageAccessToken);

  // Start typing-indicator refresh so the indicator stays alive while Bedrock
  // processes (Meta expires typing_on after ~10 s; we refresh every 8 s).
  const typingRefreshInterval = setInterval(async () => {
    try {
      await sendTypingIndicator(pageId, psid, pageAccessToken);
    } catch (e) {
      // Non-fatal — don't disrupt the main pipeline
      log('WARN', 'Typing indicator refresh failed', { pageId, psid, error: e.message });
    }
  }, TYPING_REFRESH_INTERVAL_MS);

  // ── 3. Load conversation context ─────────────────────────────────────────
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

  // ── 4. RAG pipeline: retrieve KB → generate response ─────────────────────
  // Config was already loaded in step 0b.
  let responseText;

  try {
    const kbContext = await retrieveKB(sanitizedInput, config);

    log('INFO', 'KB retrieval complete', {
      tenantHash: tenantHash.substring(0, 8),
      kbContextLength: kbContext?.length || 0,
    });

    responseText = await generateBedrockResponse(
      sanitizedInput,
      kbContext,
      conversationHistory,
      config
    );
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

  // ── 5. Stop typing refresh and send response via Meta Send API ───────────
  clearInterval(typingRefreshInterval);

  try {
    await sendResponseMessages(pageId, psid, responseText, pageAccessToken);
    log('INFO', 'Response delivered to Messenger', {
      pageId,
      psid,
      responseLength: responseText.length,
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

  // ── 6. Store conversation context ─────────────────────────────────────────
  try {
    await storeConversationContext(pageId, psid, sanitizedInput, responseText);
  } catch (storeErr) {
    // Non-fatal: log and continue
    log('WARN', 'Failed to store conversation context', {
      pageId,
      psid,
      error: storeErr.message,
    });
  }

  // ── 7. Update lastUserMessageAt ───────────────────────────────────────────
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

  const durationMs = Date.now() - startTime;
  log('INFO', 'Handler complete', {
    pageId,
    psid,
    tenantId,
    messageMid,
    durationMs,
  });
};
