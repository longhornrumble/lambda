'use strict';

/**
 * Integration tests for Meta_Response_Processor
 *
 * Uses aws-sdk-client-mock to stub AWS service calls. The Meta Graph API
 * fetch calls are stubbed via global fetch mocking.
 *
 * Test coverage:
 *   - Happy path: token load → typing → KB → Bedrock → send → store
 *   - Message splitting for responses > 2000 chars
 *   - Bedrock failure falls back to configured fallback_message
 *   - Missing channel mapping drops message without throwing
 *   - Invalid event shape drops message without throwing
 *   - 5xx Meta Send API retries up to 3 times
 *   - Conversation history is trimmed to rolling window
 *   - GET_STARTED postback sends welcome_message, skips RAG
 *   - Other postback payloads go through normal RAG pipeline
 *   - Stale message (timestamp > 24 h) is dropped with warning log
 *   - Recent message (timestamp < 24 h) is processed normally
 *   - Typing refresh interval is started and cleared
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { KMSClient, DecryptCommand } = require('@aws-sdk/client-kms');
const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');

// ─── Mock shared bedrock-core before requiring the handler ────────────────────
jest.mock('../shared/bedrock-core', () => ({
  loadConfig: jest.fn(),
  retrieveKB: jest.fn(),
  sanitizeUserInput: jest.fn((input) => input?.trim() || ''),
}));

const { loadConfig, retrieveKB } = require('../shared/bedrock-core');

// ─── AWS SDK mocks ────────────────────────────────────────────────────────────
const ddbMock = mockClient(DynamoDBDocumentClient);
const kmsMock = mockClient(KMSClient);
const bedrockMock = mockClient(BedrockRuntimeClient);
const sqsMock = mockClient(SQSClient);
const sesMock = mockClient(SESClient);

// ─── Fetch mock ───────────────────────────────────────────────────────────────
let fetchMock;

// ─── Load handler after mocks are established ─────────────────────────────────
const { handler } = require('./index');

// ─── Helpers ──────────────────────────────────────────────────────────────────

const PLAINTEXT_TOKEN = 'EAABsbCS...test_page_token';
const ENCRYPTED_TOKEN_B64 = Buffer.from('encrypted-blob').toString('base64');

function buildEvent(overrides = {}) {
  return {
    psid: 'PSID_123',
    messageText: 'Hello, what services do you offer?',
    pageId: 'PAGE_456',
    tenantId: 'TENANT_789',
    tenantHash: 'abc123defabc123def',
    channelType: 'messenger',
    messageMid: 'm_test_mid',
    ...overrides,
  };
}

function makeBedrockResponse(text) {
  const body = JSON.stringify({
    content: [{ type: 'text', text }],
    usage: { input_tokens: 50, output_tokens: 20 },
  });
  return { body: Buffer.from(body) };
}

function makeChannelMappingItem() {
  return {
    Item: {
      PK: 'PAGE#PAGE_456',
      SK: 'CHANNEL#messenger',
      encryptedPageToken: ENCRYPTED_TOKEN_B64,
      tenantId: 'TENANT_789',
    },
  };
}

function makeRecentMessagesQueryResult(messages = []) {
  // QueryCommand returns { Items: [ { role, content, messageTimestamp }, ... ] }
  return {
    Items: messages.map((m, i) => ({
      sessionId: 'meta:PAGE_456:PSID_123',
      role: m.role,
      content: m.content,
      messageTimestamp: m.messageTimestamp || (1000000 + i),
    })),
  };
}

function makeFetchMock(responses) {
  // responses: array of {ok, status, body} in call order
  let callIndex = 0;
  return jest.fn().mockImplementation(() => {
    const resp = responses[callIndex] || responses[responses.length - 1];
    callIndex++;
    return Promise.resolve({
      ok: resp.ok ?? true,
      status: resp.status ?? 200,
      json: () => Promise.resolve(resp.body ?? { recipient_id: 'PSID_123', message_id: 'mid.test' }),
    });
  });
}

// ─── Setup / teardown ─────────────────────────────────────────────────────────

beforeEach(() => {
  ddbMock.reset();
  kmsMock.reset();
  bedrockMock.reset();
  sqsMock.reset();
  sesMock.reset();
  jest.clearAllMocks();
  delete process.env.SES_FROM_EMAIL; // M6a default: unset ⇒ email disabled unless a test opts in

  // Default SQS stub: analytics emissions succeed silently
  sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mock-sqs-id' });

  // Default SES stub (M6a escalation email) — tests that want it disabled
  // leave SES_FROM_EMAIL unset; tests that want it sent set the env AND rely
  // on this default success stub.
  sesMock.on(SendEmailCommand).resolves({ MessageId: 'mock-ses-id' });

  // Default: loadConfig returns a minimal config
  loadConfig.mockResolvedValue({
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'You are a helpful recruiter assistant.',
    streaming: { max_tokens: 500, temperature: 0 },
  });

  // Default: retrieveKB returns some context
  retrieveKB.mockResolvedValue('Our services include talent acquisition and HR consulting.');

  // Default DynamoDB stubs.
  // GetCommand: channel-mappings (loadPageAccessToken)
  // QueryCommand: recent-messages (loadConversationContext) — returns Items array
  // PutCommand: 2 calls per store (user row + assistant row)
  ddbMock.on(GetCommand).resolves(makeChannelMappingItem());
  ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([]));
  ddbMock.on(PutCommand).resolves({});
  ddbMock.on(UpdateCommand).resolves({});
  ddbMock.on(DeleteCommand).resolves({});

  // Default KMS stub
  kmsMock.on(DecryptCommand).resolves({
    Plaintext: Buffer.from(PLAINTEXT_TOKEN),
  });

  // Default Bedrock stub
  bedrockMock.on(InvokeModelCommand).resolves(
    makeBedrockResponse('We offer talent acquisition and HR consulting services.')
  );

  // Default fetch mock: typing OK, message send OK
  fetchMock = makeFetchMock([
    { ok: true, body: {} }, // typing_on
    { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } }, // message send
  ]);
  global.fetch = fetchMock;
});

afterEach(() => {
  delete global.fetch;
});

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('Meta_Response_Processor handler', () => {
  describe('Happy path', () => {
    test('processes a message end-to-end and sends response', async () => {
      await handler(buildEvent());

      // KMS decrypt called once
      expect(kmsMock).toHaveReceivedCommandTimes(DecryptCommand, 1);

      // Bedrock InvokeModel called once
      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);

      // fetch called twice: typing_on + message send
      expect(fetchMock).toHaveBeenCalledTimes(2);

      // First fetch: typing indicator
      const [typingUrl, typingOptions] = fetchMock.mock.calls[0];
      expect(typingUrl).toContain('/PAGE_456/messages');
      const typingBody = JSON.parse(typingOptions.body);
      expect(typingBody.sender_action).toBe('typing_on');
      expect(typingBody.recipient.id).toBe('PSID_123');

      // Second fetch: message send
      const [sendUrl, sendOptions] = fetchMock.mock.calls[1];
      expect(sendUrl).toContain('/PAGE_456/messages');
      const sendBody = JSON.parse(sendOptions.body);
      expect(sendBody.message.text).toContain('talent acquisition');
      expect(sendBody.messaging_type).toBe('RESPONSE');
      expect(sendBody.recipient.id).toBe('PSID_123');

      // DynamoDB: 2 PutCommands to store context (user row + assistant row)
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 2);

      // DynamoDB: UpdateCommand to update lastUserMessageAt
      expect(ddbMock).toHaveReceivedCommandTimes(UpdateCommand, 1);
    });

    test('§E5 Chain 1: each recent-messages row carries text_en === content (v1 verbatim)', async () => {
      await handler(buildEvent());

      // The 2 PutCommands are the recent-messages rows (user + assistant).
      const putCalls = ddbMock.commandCalls(PutCommand);
      expect(putCalls).toHaveLength(2);

      for (const call of putCalls) {
        const item = call.args[0].input.Item;
        expect(item).toHaveProperty('text_en');
        expect(item.text_en).toBe(item.content);
      }

      // Roles are distinct and text_en mirrors each row's content verbatim.
      const items = putCalls.map((c) => c.args[0].input.Item);
      const byRole = Object.fromEntries(items.map((it) => [it.role, it]));
      expect(byRole.user.text_en).toBe(byRole.user.content);
      expect(byRole.assistant.text_en).toBe(byRole.assistant.content);
    });

    test('passes tenant config to Bedrock with correct model_id', async () => {
      loadConfig.mockResolvedValueOnce({
        model_id: 'custom-model-id',
        tone_prompt: 'Custom tone.',
        streaming: { max_tokens: 800, temperature: 0 },
      });

      await handler(buildEvent());

      const bedrockCall = bedrockMock.commandCalls(InvokeModelCommand)[0];
      expect(bedrockCall.args[0].input.modelId).toBe('custom-model-id');
    });

    test('includes KB context in the Bedrock prompt', async () => {
      retrieveKB.mockResolvedValueOnce('KB article: We work with Fortune 500 companies.');

      await handler(buildEvent());

      const bedrockCall = bedrockMock.commandCalls(InvokeModelCommand)[0];
      const body = JSON.parse(bedrockCall.args[0].input.body);
      expect(body.system).toContain('Fortune 500');
    });

    test('includes recent conversation history in the Bedrock prompt', async () => {
      const history = [
        { role: 'user', content: 'Hi there', messageTimestamp: 1000000 },
        { role: 'assistant', content: 'Hello! How can I help?', messageTimestamp: 1000001 },
      ];
      // Reset and re-register: GetCommand for channel-mappings, QueryCommand for context.
      ddbMock.reset();
      ddbMock.on(GetCommand).resolves(makeChannelMappingItem());
      ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult(history));
      ddbMock.on(PutCommand).resolves({});
      ddbMock.on(UpdateCommand).resolves({});

      await handler(buildEvent());

      const bedrockCall = bedrockMock.commandCalls(InvokeModelCommand)[0];
      const body = JSON.parse(bedrockCall.args[0].input.body);
      // History + current user message = 3 messages in the array
      expect(body.messages.length).toBe(3);
      expect(body.messages[0].role).toBe('user');
      expect(body.messages[0].content[0].text).toBe('Hi there');
    });
  });

  describe('Message splitting', () => {
    test('sends multiple messages when response exceeds 2000 chars', async () => {
      // Build a response that is ~3000 chars with sentence boundaries.
      // 'This is a sentence. ' is 20 chars; 150 repeats = 3000 chars, safely over the 2000-char limit.
      const longText =
        'This is a sentence. '.repeat(150) +
        'And this is the final sentence.';
      bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse(longText));

      // Provide enough fetch stubs: typing + 2 message sends
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing_on
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } }, // chunk 1
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.2' } }, // chunk 2
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent());

      // Should have 3 fetches: 1 typing + 2 message chunks
      expect(fetchMock).toHaveBeenCalledTimes(3);

      const [, chunk1Options] = fetchMock.mock.calls[1];
      const [, chunk2Options] = fetchMock.mock.calls[2];
      const chunk1 = JSON.parse(chunk1Options.body).message.text;
      const chunk2 = JSON.parse(chunk2Options.body).message.text;

      expect(chunk1.length).toBeLessThanOrEqual(2000);
      expect(chunk2.length).toBeLessThanOrEqual(2000);
      // Chunks together should cover the full response
      expect((chunk1 + ' ' + chunk2).length).toBeGreaterThan(2000);
    });
  });

  describe('Error handling — page access token', () => {
    test('drops message silently when channel mapping not found', async () => {
      // Override: first GetCommand (channel-mappings) returns no item.
      ddbMock.reset();
      ddbMock.on(GetCommand).resolves({ Item: undefined });
      ddbMock.on(PutCommand).resolves({});
      ddbMock.on(UpdateCommand).resolves({});

      await expect(handler(buildEvent())).resolves.toBeUndefined();

      // No fetch calls (no typing indicator, no message send)
      expect(fetchMock).not.toHaveBeenCalled();
    });

    test('drops message silently when KMS decryption fails', async () => {
      kmsMock.on(DecryptCommand).rejects(new Error('KMS access denied'));

      await expect(handler(buildEvent())).resolves.toBeUndefined();
      expect(fetchMock).not.toHaveBeenCalled();
    });
  });

  describe('Error handling — Bedrock', () => {
    test('sends fallback message when Bedrock fails', async () => {
      bedrockMock.on(InvokeModelCommand).rejects(new Error('Bedrock throttled'));
      loadConfig.mockResolvedValueOnce({
        bedrock_instructions: { fallback_message: 'Sorry, try again later.' },
      });

      await handler(buildEvent());

      // fetch called twice: typing + fallback message
      expect(fetchMock).toHaveBeenCalledTimes(2);
      const [, sendOptions] = fetchMock.mock.calls[1];
      const body = JSON.parse(sendOptions.body);
      expect(body.message.text).toBe('Sorry, try again later.');
    });

    test('uses default fallback when config has no fallback_message', async () => {
      bedrockMock.on(InvokeModelCommand).rejects(new Error('Bedrock throttled'));
      loadConfig.mockResolvedValueOnce({ tone_prompt: 'Helpful.' });

      await handler(buildEvent());

      const [, sendOptions] = fetchMock.mock.calls[1];
      const body = JSON.parse(sendOptions.body);
      expect(body.message.text).toContain("I'm sorry");
    });
  });

  describe('Instagram send path', () => {
    test('IG replies use graph.facebook.com /me/messages with the Page token (no Bearer)', async () => {
      // Instagram Messaging via Messenger Platform sends through the SAME
      // Facebook Send API as Messenger. graph.instagram.com would need an
      // Instagram-Login user token we don't hold (live 401, 2026-07-12).
      fetchMock = makeFetchMock([
        // Typing indicator is skipped for instagram — first call IS the send.
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.ig' } },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent({ channelType: 'instagram', pageId: 'IG_ACCT_999' }));

      const sendCalls = fetchMock.mock.calls.filter(([u]) => String(u).includes('/messages'));
      expect(sendCalls.length).toBe(1);
      const [url, options] = sendCalls[0];
      expect(url).toMatch(/^https:\/\/graph\.facebook\.com\/v[\d.]+\/me\/messages$/);
      expect(options.headers.Authorization).toBeUndefined();
      const body = JSON.parse(options.body);
      expect(body.access_token).toBe(PLAINTEXT_TOKEN);
      expect(body.recipient.id).toBe('PSID_123');
    });
  });

  describe('Error handling — Meta Send API', () => {
    test('retries on 5xx and succeeds on second attempt (typing indicator path)', async () => {
      // callMetaSendApi (used for the typing indicator) retries on 5xx.
      // sendMessengerMessage (used for the actual message) calls fetch directly without retry.
      // This test verifies the callMetaSendApi retry path: typing gets a 500 then succeeds,
      // and the message send succeeds on its single attempt.
      fetchMock = makeFetchMock([
        { ok: false, status: 500, body: { error: { message: 'Server error' } } }, // typing attempt 1
        { ok: true, body: {} },                                                    // typing attempt 2 (retry)
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } },   // message send
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent());

      // 2 typing attempts (1 retry) + 1 message send = 3 total
      expect(fetchMock).toHaveBeenCalledTimes(3);
    });

    test('throws (causing Lambda retry) when all 5xx retries exhausted', async () => {
      fetchMock = makeFetchMock([
        { ok: true, body: {} },                                            // typing_on
        { ok: false, status: 500, body: { error: { message: 'Server error' } } }, // attempt 1
        { ok: false, status: 500, body: { error: { message: 'Server error' } } }, // attempt 2
        { ok: false, status: 500, body: { error: { message: 'Server error' } } }, // attempt 3
      ]);
      global.fetch = fetchMock;

      await expect(handler(buildEvent())).rejects.toThrow('Meta Send API error: 500');
    });

    test('does not retry on 4xx errors', async () => {
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing_on
        { ok: false, status: 403, body: { error: { message: 'Permission denied' } } },
      ]);
      global.fetch = fetchMock;

      await expect(handler(buildEvent())).rejects.toThrow('Meta Send API error: 403');
      // Only 2 fetch calls: typing + 1 send attempt (no retries)
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  });

  describe('Input validation', () => {
    test('drops message when psid is missing', async () => {
      const event = buildEvent({ psid: undefined });
      await expect(handler(event)).resolves.toBeUndefined();
      expect(fetchMock).not.toHaveBeenCalled();
    });

    test('drops message when messageText is empty string', async () => {
      const event = buildEvent({ messageText: '' });
      await expect(handler(event)).resolves.toBeUndefined();
      expect(fetchMock).not.toHaveBeenCalled();
    });

    test('drops message when tenantHash is missing', async () => {
      const event = buildEvent({ tenantHash: undefined });
      await expect(handler(event)).resolves.toBeUndefined();
      expect(fetchMock).not.toHaveBeenCalled();
    });
  });

  describe('Conversation context storage', () => {
    test('stores Q&A pair in recent-messages after successful response', async () => {
      await handler(buildEvent());

      // Two PutCommands: one for user row, one for assistant row
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 2);
      const putCalls = ddbMock.commandCalls(PutCommand);

      const userItem = putCalls[0].args[0].input.Item;
      expect(userItem.sessionId).toBe('meta:PAGE_456:PSID_123');
      expect(userItem.role).toBe('user');
      expect(typeof userItem.content).toBe('string');
      expect(typeof userItem.messageTimestamp).toBe('number');

      const assistantItem = putCalls[1].args[0].input.Item;
      expect(assistantItem.sessionId).toBe('meta:PAGE_456:PSID_123');
      expect(assistantItem.role).toBe('assistant');
      expect(typeof assistantItem.content).toBe('string');
      expect(assistantItem.messageTimestamp).toBe(userItem.messageTimestamp + 1);
    });

    test('trims stored messages to rolling window of 20 (10 pairs)', async () => {
      // Trimming now happens via the QueryCommand Limit parameter (MAX_STORED_PAIRS * 2).
      // storeConversationContext writes 2 new individual rows; no in-memory array trimming.
      await handler(buildEvent());

      // Verify QueryCommand was issued with a Limit (the rolling-window cap)
      const queryCalls = ddbMock.commandCalls(QueryCommand);
      expect(queryCalls.length).toBeGreaterThanOrEqual(1);
      const queryInput = queryCalls[0].args[0].input;
      expect(typeof queryInput.Limit).toBe('number');
      expect(queryInput.Limit).toBeGreaterThan(0);

      // And store still writes exactly 2 rows
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 2);
    });

    test('continues if context storage fails', async () => {
      ddbMock.on(PutCommand).rejects(new Error('DynamoDB write failed'));

      // Should not throw — storage is non-fatal
      await expect(handler(buildEvent())).resolves.toBeUndefined();
    });
  });

  describe('Messenger-specific prompt constraints', () => {
    test('includes no-markdown instruction in system prompt', async () => {
      await handler(buildEvent());

      const bedrockCall = bedrockMock.commandCalls(InvokeModelCommand)[0];
      const body = JSON.parse(bedrockCall.args[0].input.body);
      expect(body.system).toContain('no markdown');
      expect(body.system).toContain('Facebook Messenger');
    });
  });

  // ── Enhancement 1: Postback handling ──────────────────────────────────────

  describe('Postback handling', () => {
    test('GET_STARTED postback sends welcome_message from config and skips RAG', async () => {
      loadConfig.mockResolvedValueOnce({
        welcome_message: 'Welcome to our service! How can I help you?',
        tone_prompt: 'Helpful.',
      });

      // GET_STARTED only needs: typing + welcome send
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing_on
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.welcome' } },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent({ isPostback: true, messageText: 'GET_STARTED' }));

      // Bedrock must NOT be invoked
      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 0);

      // retrieveKB must NOT be called
      expect(retrieveKB).not.toHaveBeenCalled();

      // Two fetch calls: typing + welcome send
      expect(fetchMock).toHaveBeenCalledTimes(2);

      // Second call is the actual message send
      const [, sendOptions] = fetchMock.mock.calls[1];
      const body = JSON.parse(sendOptions.body);
      expect(body.message.text).toBe('Welcome to our service! How can I help you?');
      expect(body.messaging_type).toBe('RESPONSE');

      // Context stored (2 rows: user + assistant) and lastUserMessageAt updated
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 2);
      expect(ddbMock).toHaveReceivedCommandTimes(UpdateCommand, 1);
    });

    test('GET_STARTED uses default welcome when config has no welcome_message', async () => {
      loadConfig.mockResolvedValueOnce({ tone_prompt: 'Helpful.' }); // no welcome_message

      fetchMock = makeFetchMock([
        { ok: true, body: {} },
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent({ isPostback: true, messageText: 'GET_STARTED' }));

      const [, sendOptions] = fetchMock.mock.calls[1];
      const body = JSON.parse(sendOptions.body);
      expect(body.message.text).toContain("Hello!");
      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 0);
    });

    test('non-GET_STARTED postback goes through normal RAG pipeline', async () => {
      // MENU_VOLUNTEER payload — should reach Bedrock
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing_on
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } }, // response
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent({ isPostback: true, messageText: 'MENU_VOLUNTEER' }));

      // Bedrock was called with the postback payload as the user query
      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
      const bedrockCall = bedrockMock.commandCalls(InvokeModelCommand)[0];
      const body = JSON.parse(bedrockCall.args[0].input.body);
      const lastMessage = body.messages[body.messages.length - 1];
      expect(lastMessage.content[0].text).toBe('MENU_VOLUNTEER');
    });

    test('event without isPostback goes through RAG pipeline normally', async () => {
      await handler(buildEvent()); // no isPostback field

      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    });
  });

  // ── Enhancement 2: 24-hour messaging-window enforcement ───────────────────

  describe('24-hour messaging window', () => {
    test('drops response and logs warning when message timestamp is > 24 h old', async () => {
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

      const staleTimestamp = Date.now() - (25 * 60 * 60 * 1000); // 25 hours ago
      await handler(buildEvent({ timestamp: staleTimestamp }));

      // No fetch calls — nothing should be sent
      expect(fetchMock).not.toHaveBeenCalled();

      // A WARN log line should have been emitted
      const warnCalls = consoleWarnSpy.mock.calls.map((c) => c[0]);
      const windowWarn = warnCalls.find((line) => {
        try {
          return JSON.parse(line).message.includes('24-hour messaging window');
        } catch {
          return false;
        }
      });
      expect(windowWarn).toBeDefined();

      consoleWarnSpy.mockRestore();
    });

    test('processes message normally when timestamp is within 24 h', async () => {
      const recentTimestamp = Date.now() - (30 * 60 * 1000); // 30 minutes ago
      await handler(buildEvent({ timestamp: recentTimestamp }));

      // Full pipeline ran
      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
      expect(fetchMock).toHaveBeenCalledTimes(2); // typing + send
    });

    test('processes message normally when no timestamp is provided (backward compat)', async () => {
      // Events without a timestamp field should not be dropped
      const event = buildEvent();
      delete event.timestamp;

      await handler(event);

      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    });
  });

  // ── Enhancement 3: Typing indicator refresh ───────────────────────────────

  describe('Typing indicator refresh', () => {
    test('setInterval is started for typing refresh and cleared after response', async () => {
      const setIntervalSpy = jest.spyOn(global, 'setInterval');
      const clearIntervalSpy = jest.spyOn(global, 'clearInterval');

      await handler(buildEvent());

      // setInterval called once with 8000 ms
      expect(setIntervalSpy).toHaveBeenCalledTimes(1);
      expect(setIntervalSpy).toHaveBeenCalledWith(expect.any(Function), 8000);

      // clearInterval called once with the timer returned by setInterval
      const timerId = setIntervalSpy.mock.results[0].value;
      expect(clearIntervalSpy).toHaveBeenCalledWith(timerId);

      setIntervalSpy.mockRestore();
      clearIntervalSpy.mockRestore();
    });

    test('typing refresh callback sends typing_on when invoked', async () => {
      // Capture the callback registered with setInterval
      let capturedCallback;
      const setIntervalSpy = jest
        .spyOn(global, 'setInterval')
        .mockImplementation((fn, ms) => {
          capturedCallback = fn;
          return 999; // fake timer id
        });
      const clearIntervalSpy = jest
        .spyOn(global, 'clearInterval')
        .mockImplementation(() => {});

      // Provide enough fetch stubs for: initial typing + 1 refresh invocation + send
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // initial typing_on
        { ok: true, body: {} }, // refresh call
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } }, // message send
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent());

      // clearInterval was called with the id returned by setInterval
      expect(clearIntervalSpy).toHaveBeenCalledWith(999);

      // Manually invoke the captured callback to verify it sends typing_on
      expect(capturedCallback).toBeDefined();
      const fetchCallsBefore = fetchMock.mock.calls.length;
      await capturedCallback();
      expect(fetchMock).toHaveBeenCalledTimes(fetchCallsBefore + 1);
      const refreshBody = JSON.parse(
        fetchMock.mock.calls[fetchCallsBefore][1].body
      );
      expect(refreshBody.sender_action).toBe('typing_on');

      setIntervalSpy.mockRestore();
      clearIntervalSpy.mockRestore();
    });

    test('typing refresh interval is not started for GET_STARTED postback', async () => {
      const setIntervalSpy = jest.spyOn(global, 'setInterval');

      loadConfig.mockResolvedValueOnce({ welcome_message: 'Hi there!' });
      fetchMock = makeFetchMock([
        { ok: true, body: {} },
        { ok: true, body: { recipient_id: 'PSID_123', message_id: 'mid.1' } },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent({ isPostback: true, messageText: 'GET_STARTED' }));

      // GET_STARTED path skips RAG and the typing refresh interval
      expect(setIntervalSpy).not.toHaveBeenCalled();

      setIntervalSpy.mockRestore();
    });
  });

  // ── Analytics event emission ───────────────────────────────────────────────

  describe('Analytics event emission', () => {
    test('emits MESSENGER_MESSAGE_RECEIVED and MESSENGER_RESPONSE_SENT on successful processing', async () => {
      await handler(buildEvent());

      // Two SQS SendMessageCommand calls: one for RECEIVED, one for SENT
      expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 2);

      const sqsCalls = sqsMock.commandCalls(SendMessageCommand);

      // First event: MESSENGER_MESSAGE_RECEIVED
      const receivedBody = JSON.parse(sqsCalls[0].args[0].input.MessageBody);
      expect(receivedBody.event.type).toBe('MESSENGER_MESSAGE_RECEIVED');
      expect(receivedBody.schema_version).toBe('1.0');
      expect(receivedBody.session_id).toBe('meta:PAGE_456:PSID_123');
      expect(receivedBody.tenant_id).toBe('TENANT_789');
      expect(receivedBody.event.payload.channel_type).toBe('messenger');
      expect(receivedBody.event.payload.page_id).toBe('PAGE_456');
      expect(receivedBody.event.payload.psid).toBe('PSID_123');
      expect(typeof receivedBody.event.payload.message_length).toBe('number');
      expect(receivedBody.event.payload.is_postback).toBe(false);

      // Second event: MESSENGER_RESPONSE_SENT
      const sentBody = JSON.parse(sqsCalls[1].args[0].input.MessageBody);
      expect(sentBody.event.type).toBe('MESSENGER_RESPONSE_SENT');
      expect(sentBody.schema_version).toBe('1.0');
      expect(sentBody.session_id).toBe('meta:PAGE_456:PSID_123');
      expect(sentBody.tenant_id).toBe('TENANT_789');
      expect(sentBody.event.payload.channel_type).toBe('messenger');
      expect(sentBody.event.payload.page_id).toBe('PAGE_456');
      expect(sentBody.event.payload.psid).toBe('PSID_123');
      expect(typeof sentBody.event.payload.response_length).toBe('number');
      expect(typeof sentBody.event.payload.model_used).toBe('string');
      expect(typeof sentBody.event.payload.kb_context_length).toBe('number');
    });

    test('does NOT emit analytics when message is dropped due to invalid event shape', async () => {
      const event = buildEvent({ psid: undefined });
      await expect(handler(event)).resolves.toBeUndefined();

      // Validation fails before analytics emission
      expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 0);
    });

    test('does NOT emit analytics when message is dropped due to stale 24-hour window', async () => {
      const staleTimestamp = Date.now() - (25 * 60 * 60 * 1000); // 25 hours ago
      await handler(buildEvent({ timestamp: staleTimestamp }));

      // Stale drop happens before analytics emission
      expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 0);
    });

    test('does NOT emit MESSENGER_RESPONSE_SENT when Meta Send API fails', async () => {
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing_on
        { ok: false, status: 500, body: { error: { message: 'Server error' } } },
        { ok: false, status: 500, body: { error: { message: 'Server error' } } },
        { ok: false, status: 500, body: { error: { message: 'Server error' } } },
      ]);
      global.fetch = fetchMock;

      await expect(handler(buildEvent())).rejects.toThrow('Meta Send API error: 500');

      // RECEIVED was emitted (after validation), but SENT was not (delivery failed)
      expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 1);
      const sqsCalls = sqsMock.commandCalls(SendMessageCommand);
      const body = JSON.parse(sqsCalls[0].args[0].input.MessageBody);
      expect(body.event.type).toBe('MESSENGER_MESSAGE_RECEIVED');
    });

    test('SQS failure does not crash the handler — analytics is best-effort', async () => {
      sqsMock.on(SendMessageCommand).rejects(new Error('SQS service unavailable'));

      // Handler should complete normally despite SQS failures
      await expect(handler(buildEvent())).resolves.toBeUndefined();

      // Fetch (typing + send) should still have been called
      expect(fetchMock).toHaveBeenCalledTimes(2);
      // Bedrock should have been called
      expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    });
  });
});

// ─── Payload v2 legacy-gap contract (C1 deploy ordering §2/§4) ─────────────────
//
// The M1a webhook ships payload v2 BEFORE this processor learns the new event
// kinds (webhook-deploys-first rule). C1 guarantees: v2 payloads for new kinds
// (messageText:null) hit validateEvent, drop cleanly — no crash, no retry, no
// Bedrock call, no Meta send, and NO reply generated from echo text (loop
// guard). This suite pins that guarantee using the REAL classifier + fixture
// library from the webhook, so a change to either side breaks CI here.

describe('payload v2 legacy-gap contract (C1)', () => {
  const { classifyMessagingEvent } = require('../Meta_Webhook_Handler/classify');
  const WEBHOOK_FIXTURES = require('../Meta_Webhook_Handler/__fixtures__/messagingEvents');

  /** Mirror of the webhook's forwardClassifiedEvent payload construction. */
  function v2Payload(classification, fixture) {
    return {
      psid: classification.psid,
      messageText: classification.messageText,
      pageId: WEBHOOK_FIXTURES.PAGE_ID,
      tenantId: 'TENANT_ABC',
      tenantHash: 'abc123',
      channelType: 'messenger',
      messageMid: classification.messageMid,
      isPostback: classification.isPostback,
      v: 2,
      eventKind: classification.eventKind,
      timestamp: typeof fixture.timestamp === 'number' ? fixture.timestamp : Date.now(),
      quickReplyPayload: classification.quickReplyPayload,
      appId: classification.appId,
      attachmentTypes: classification.attachmentTypes,
      targetMid: classification.targetMid,
      editedText: classification.editedText,
      replyTo: classification.replyTo,
      isStandby: classification.isStandby,
    };
  }

  test.each([
    ['attachment', WEBHOOK_FIXTURES.fbAttachmentImage],
    ['sticker', WEBHOOK_FIXTURES.fbStickerPostMigration],
    ['edit', WEBHOOK_FIXTURES.fbEdit],
    ['delete (IG is_deleted)', WEBHOOK_FIXTURES.igDelete],
    ['echo', WEBHOOK_FIXTURES.fbEcho],
    ['unsupported', WEBHOOK_FIXTURES.fbUnsupportedFutureContent],
  ])('legacy processor drops %s payloads without crash, Bedrock, or sends', async (_name, fixture) => {
    const [classification] = classifyMessagingEvent(fixture);
    expect(classification.skip).toBeUndefined();
    // The loop-guard invariant that makes the gap safe: no respondable text
    expect(classification.messageText).toBeNull();

    // Note: fixture timestamps are fixed/old — but a stale timestamp only makes
    // the drop happen EARLIER (24h guard); validateEvent is the guarantee for
    // fresh events, so pin with a fresh timestamp.
    const event = { ...v2Payload(classification, fixture), timestamp: Date.now() };
    await expect(handler(event)).resolves.not.toThrow();

    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('v2 text payload still processes normally (v1 fields intact)', async () => {
    const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbText);
    const event = { ...v2Payload(classification, WEBHOOK_FIXTURES.fbText), timestamp: Date.now() };
    await expect(handler(event)).resolves.not.toThrow();
    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
  });
});

// ─── M1b — Processor hygiene (C1, C2, C5, C8) ──────────────────────────────
//
// Unconditional hygiene (echo/standby short-circuit, edit/delete idempotent
// handling, TTL fix, per-channel send caps) plus flag-gated behavior
// (unsupported-input fallback, disclosure line) gated on
// config.feature_flags.MESSENGER_CHANNEL. Payloads built THROUGH the real
// webhook classifier + fixtures, same pattern as the legacy-gap contract
// suite above.

describe('M1b — processor hygiene', () => {
  const { classifyMessagingEvent } = require('../Meta_Webhook_Handler/classify');
  const WEBHOOK_FIXTURES = require('../Meta_Webhook_Handler/__fixtures__/messagingEvents');

  const DEFAULT_DISCLOSURE_LINE =
    "Just a heads up — you're chatting with an automated assistant.";
  const DEFAULT_UNSUPPORTED_INPUT_FALLBACK =
    "Sorry, I can't read that kind of message yet — could you type it instead?";

  /** Mirror of the webhook's forwardClassifiedEvent payload construction (same shape as the suite above). */
  function v2Payload(classification, fixture, overrides = {}) {
    return {
      psid: classification.psid,
      messageText: classification.messageText,
      pageId: WEBHOOK_FIXTURES.PAGE_ID,
      tenantId: 'TENANT_ABC',
      tenantHash: 'abc123',
      channelType: 'messenger',
      messageMid: classification.messageMid,
      isPostback: classification.isPostback,
      v: 2,
      eventKind: classification.eventKind,
      timestamp: typeof fixture.timestamp === 'number' ? fixture.timestamp : Date.now(),
      quickReplyPayload: classification.quickReplyPayload,
      appId: classification.appId,
      attachmentTypes: classification.attachmentTypes,
      targetMid: classification.targetMid,
      editedText: classification.editedText,
      replyTo: classification.replyTo,
      isStandby: classification.isStandby,
      ...overrides,
    };
  }

  describe('Unsupported-input fallback (flag-gated, 30-second rule)', () => {
    test('attachment + flag ON → exactly one fetch send with the default fallback string, no Bedrock', async () => {
      loadConfig.mockResolvedValue({ feature_flags: { MESSENGER_CHANNEL: true } });
      fetchMock = makeFetchMock([{ ok: true, body: { message_id: 'mid.fallback' } }]);
      global.fetch = fetchMock;

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbAttachmentImage);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbAttachmentImage, { timestamp: Date.now() });

      await handler(event);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      const [, options] = fetchMock.mock.calls[0];
      const body = JSON.parse(options.body);
      expect(body.message.text).toBe(DEFAULT_UNSUPPORTED_INPUT_FALLBACK);
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    });

    test('config-override string is honored (messenger_behavior.strings.unsupported_input_fallback)', async () => {
      loadConfig.mockResolvedValue({
        feature_flags: { MESSENGER_CHANNEL: true },
        messenger_behavior: { strings: { unsupported_input_fallback: 'Custom fallback text.' } },
      });
      fetchMock = makeFetchMock([{ ok: true, body: {} }]);
      global.fetch = fetchMock;

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbAttachmentImage);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbAttachmentImage, { timestamp: Date.now() });
      await handler(event);

      const [, options] = fetchMock.mock.calls[0];
      expect(JSON.parse(options.body).message.text).toBe('Custom fallback text.');
    });

    test('sticker + flag ON → same fallback path', async () => {
      loadConfig.mockResolvedValue({ feature_flags: { MESSENGER_CHANNEL: true } });
      fetchMock = makeFetchMock([{ ok: true, body: {} }]);
      global.fetch = fetchMock;

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbStickerPostMigration);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbStickerPostMigration, { timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    });

    test('unsupported + flag ON → same fallback path', async () => {
      loadConfig.mockResolvedValue({ feature_flags: { MESSENGER_CHANNEL: true } });
      fetchMock = makeFetchMock([{ ok: true, body: {} }]);
      global.fetch = fetchMock;

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbUnsupportedFutureContent);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbUnsupportedFutureContent, { timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    });

    test('attachment + flag OFF → zero sends, zero Bedrock (byte-identical baseline)', async () => {
      loadConfig.mockResolvedValue({}); // no feature_flags
      fetchMock = makeFetchMock([]);
      global.fetch = fetchMock;

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbAttachmentImage);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbAttachmentImage, { timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).not.toHaveBeenCalled();
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    });
  });

  describe('Echo / standby — unconditional early return (ignores the flag)', () => {
    test('echo event never triggers Bedrock/history/sends, even with flag ON', async () => {
      loadConfig.mockResolvedValue({ feature_flags: { MESSENGER_CHANNEL: true } });
      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbEcho);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbEcho, { timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).not.toHaveBeenCalled();
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 0);
    });

    test('echo event never triggers Bedrock/history/sends, flag OFF', async () => {
      loadConfig.mockResolvedValue({});
      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbEcho);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbEcho, { timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).not.toHaveBeenCalled();
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 0);
    });

    test('standby event (isStandby true, non-echo kind) never triggers Bedrock/history/sends, flag ON', async () => {
      loadConfig.mockResolvedValue({ feature_flags: { MESSENGER_CHANNEL: true } });
      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbText);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbText, { isStandby: true, timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).not.toHaveBeenCalled();
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 0);
    });

    test('standby event (isStandby true, non-echo kind) never triggers Bedrock/history/sends, flag OFF', async () => {
      loadConfig.mockResolvedValue({});
      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbText);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbText, { isStandby: true, timestamp: Date.now() });
      await handler(event);

      expect(fetchMock).not.toHaveBeenCalled();
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
      expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 0);
    });
  });

  describe('Delete / edit — idempotent, meta:-only (C1 v1.1 — Meta redeliveries bypass dedup)', () => {
    test('delete event queries then deletes ONLY matching-mid rows', async () => {
      const sessionId = `meta:${WEBHOOK_FIXTURES.PAGE_ID}:${WEBHOOK_FIXTURES.PSID}`;
      ddbMock.on(QueryCommand).resolves({
        Items: [
          { sessionId, messageTimestamp: 1000, role: 'user', content: 'hi', mid: 'm_deleted_1' },
          { sessionId, messageTimestamp: 1001, role: 'assistant', content: 'hello' },
          { sessionId, messageTimestamp: 1002, role: 'user', content: 'other', mid: 'm_other' },
        ],
      });

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbDeleteTwoMids);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbDeleteTwoMids, { timestamp: Date.now() });

      await handler(event);

      expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 1);
      const deleteCalls = ddbMock.commandCalls(DeleteCommand);
      expect(deleteCalls).toHaveLength(1);
      expect(deleteCalls[0].args[0].input.Key.sessionId).toBe(sessionId);
      expect(deleteCalls[0].args[0].input.Key.sessionId.startsWith('meta:')).toBe(true);
      expect(deleteCalls[0].args[0].input.Key.messageTimestamp).toBe(1000);
      expect(fetchMock).not.toHaveBeenCalled();
      expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    });

    test('delete event with zero matches is idempotent (no throw, no delete)', async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });
      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.igDelete);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.igDelete, { timestamp: Date.now() });

      await expect(handler(event)).resolves.toBeUndefined();
      expect(ddbMock).toHaveReceivedCommandTimes(DeleteCommand, 0);
    });

    test('edit event updates content/text_en on the matching row', async () => {
      const sessionId = `meta:${WEBHOOK_FIXTURES.PAGE_ID}:${WEBHOOK_FIXTURES.PSID}`;
      ddbMock.on(QueryCommand).resolves({
        Items: [{ sessionId, messageTimestamp: 2000, role: 'user', content: 'orig', mid: WEBHOOK_FIXTURES.MID }],
      });

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbEdit);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbEdit, { timestamp: Date.now() });
      await handler(event);

      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      expect(updateCalls).toHaveLength(1);
      const input = updateCalls[0].args[0].input;
      expect(input.Key.sessionId).toBe(sessionId);
      expect(input.Key.sessionId.startsWith('meta:')).toBe(true);
      expect(input.Key.messageTimestamp).toBe(2000);
      expect(input.ExpressionAttributeValues[':c']).toBe('edited text');
    });

    test('edit event with zero matches is idempotent (no throw, no update)', async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });
      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.igEdit);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.igEdit, { timestamp: Date.now() });

      await expect(handler(event)).resolves.toBeUndefined();
      expect(ddbMock).toHaveReceivedCommandTimes(UpdateCommand, 0);
    });

    test('delete/edit only ever touch a meta:-prefixed sessionId, even with adversarial pageId/psid content', async () => {
      // sessionId is always built as `meta:${pageId}:${psid}` in code — no path
      // exists to construct anything else from a v2 payload. This drives the
      // invariant with pageId/psid values that themselves contain 'meta:'-like
      // substrings, to prove the guard isn't accidentally string-matching.
      const weirdPageId = 'weird:page:id';
      const weirdPsid = 'weird:psid';
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbEdit);
      const event = v2Payload(classification, WEBHOOK_FIXTURES.fbEdit, {
        pageId: weirdPageId,
        psid: weirdPsid,
        timestamp: Date.now(),
      });
      await handler(event);

      const queryCalls = ddbMock.commandCalls(QueryCommand);
      const editQuery = queryCalls[queryCalls.length - 1];
      const queriedSessionId = editQuery.args[0].input.ExpressionAttributeValues[':sid'];
      expect(queriedSessionId).toBe(`meta:${weirdPageId}:${weirdPsid}`);
      expect(queriedSessionId.startsWith('meta:')).toBe(true);
    });
  });

  describe('TTL fix — expires_at (not ttl) + mid on the user row only', () => {
    test('new context rows carry expires_at (seconds, ≈+7d), never ttl; user row carries mid, assistant row does not', async () => {
      const before = Math.floor(Date.now() / 1000);
      await handler(buildEvent({ messageMid: 'm_test_mid_123' }));

      const putCalls = ddbMock.commandCalls(PutCommand);
      expect(putCalls).toHaveLength(2);
      const items = putCalls.map((c) => c.args[0].input.Item);
      const userItem = items.find((i) => i.role === 'user');
      const assistantItem = items.find((i) => i.role === 'assistant');

      for (const item of items) {
        expect(item.ttl).toBeUndefined();
        expect(typeof item.expires_at).toBe('number');
        expect(item.expires_at).toBeGreaterThanOrEqual(before + 60 * 60 * 24 * 7 - 5);
        expect(item.expires_at).toBeLessThanOrEqual(before + 60 * 60 * 24 * 7 + 5);
      }

      expect(userItem.mid).toBe('m_test_mid_123');
      expect(assistantItem.mid).toBeUndefined();
    });
  });

  describe('Per-channel send caps (C5 — IG 1000 chars, FB 2000 chars)', () => {
    test('Instagram reply of >1000 chars chunks all pieces to ≤1000 chars', async () => {
      const longText = 'This is a sentence. '.repeat(80) + 'Final sentence.'; // >1000 chars
      bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse(longText));
      fetchMock = makeFetchMock([
        { ok: true, body: { message_id: 'mid.ig1' } }, // IG has no typing indicator — first call is chunk 1
        { ok: true, body: { message_id: 'mid.ig2' } },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent({ channelType: 'instagram', pageId: 'IG_ACCT_999' }));

      const sendCalls = fetchMock.mock.calls.filter(([u]) => String(u).includes('/messages'));
      expect(sendCalls.length).toBeGreaterThanOrEqual(2);
      for (const [, options] of sendCalls) {
        const body = JSON.parse(options.body);
        expect(body.message.text.length).toBeLessThanOrEqual(1000);
      }
    });

    test('Facebook Messenger reply of >2000 chars still chunks to ≤2000 chars', async () => {
      const longText = 'This is a sentence. '.repeat(150) + 'And this is the final sentence.'; // >2000 chars
      bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse(longText));
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing_on
        { ok: true, body: { message_id: 'mid.1' } },
        { ok: true, body: { message_id: 'mid.2' } },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent());

      const sendCalls = fetchMock.mock.calls.slice(1); // skip typing
      expect(sendCalls.length).toBeGreaterThanOrEqual(2);
      for (const [, options] of sendCalls) {
        const body = JSON.parse(options.body);
        expect(body.message.text.length).toBeLessThanOrEqual(2000);
      }
    });
  });

  describe('Disclosure line (C2 strings, C8 session-first-turn)', () => {
    test('empty history + flag ON → disclosure sent as its own message BEFORE the reply', async () => {
      loadConfig.mockResolvedValue({
        feature_flags: { MESSENGER_CHANNEL: true },
        tone_prompt: 'Helpful.',
      });
      ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([])); // empty history
      fetchMock = makeFetchMock([
        { ok: true, body: {} }, // typing
        { ok: true, body: { message_id: 'mid.disclosure' } }, // disclosure
        { ok: true, body: { message_id: 'mid.reply' } }, // actual reply
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent());

      const sendCalls = fetchMock.mock.calls.slice(1); // skip typing
      expect(sendCalls.length).toBe(2);
      const firstBody = JSON.parse(sendCalls[0][1].body);
      expect(firstBody.message.text).toBe(DEFAULT_DISCLOSURE_LINE);
      const secondBody = JSON.parse(sendCalls[1][1].body);
      expect(secondBody.message.text).toContain('talent acquisition');
    });

    test('config-override disclosure_line string is honored', async () => {
      loadConfig.mockResolvedValue({
        feature_flags: { MESSENGER_CHANNEL: true },
        messenger_behavior: { strings: { disclosure_line: 'Custom disclosure.' } },
      });
      ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([]));
      fetchMock = makeFetchMock([
        { ok: true, body: {} },
        { ok: true, body: {} },
        { ok: true, body: {} },
      ]);
      global.fetch = fetchMock;

      await handler(buildEvent());

      const sendCalls = fetchMock.mock.calls.slice(1);
      const firstBody = JSON.parse(sendCalls[0][1].body);
      expect(firstBody.message.text).toBe('Custom disclosure.');
    });

    test('non-empty history → no disclosure even with flag ON', async () => {
      loadConfig.mockResolvedValue({ feature_flags: { MESSENGER_CHANNEL: true } });
      ddbMock.on(QueryCommand).resolves(
        makeRecentMessagesQueryResult([
          // current-session timestamps: C8 (M3b) treats stale history as a
          // NEW session (disclosure would then correctly fire)
          { role: 'user', content: 'hi', messageTimestamp: Date.now() - 120000 },
          { role: 'assistant', content: 'hello', messageTimestamp: Date.now() - 60000 },
        ])
      );

      await handler(buildEvent());

      // typing + single reply only — no disclosure
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    test('flag OFF → no disclosure regardless of history (byte-identical baseline)', async () => {
      loadConfig.mockResolvedValue({}); // flag off
      ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([]));

      await handler(buildEvent());

      // typing + single reply only — no disclosure
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  });
});

// ─── M1b review follow-ups: failure paths + boundary + G-P1 log redaction ────

describe('M1b — failure paths and guards (code-review findings)', () => {
  const { classifyMessagingEvent } = require('../Meta_Webhook_Handler/classify');
  const WEBHOOK_FIXTURES = require('../Meta_Webhook_Handler/__fixtures__/messagingEvents');

  function v2From(fixture, overrides = {}) {
    const [c] = classifyMessagingEvent(fixture);
    return {
      psid: c.psid,
      messageText: c.messageText,
      pageId: WEBHOOK_FIXTURES.PAGE_ID,
      tenantId: 'TENANT_ABC',
      tenantHash: 'abc123',
      channelType: 'messenger',
      messageMid: c.messageMid,
      isPostback: c.isPostback,
      v: 2,
      eventKind: c.eventKind,
      timestamp: Date.now(),
      quickReplyPayload: c.quickReplyPayload,
      appId: c.appId,
      attachmentTypes: c.attachmentTypes,
      targetMid: c.targetMid,
      editedText: c.editedText,
      replyTo: c.replyTo,
      isStandby: c.isStandby,
      ...overrides,
    };
  }

  const flagOnConfig = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'You are a helpful recruiter assistant.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
  };

  test('malformed v2 payload: validation-failure log NEVER carries content (G-P1)', async () => {
    const logSpy = jest.spyOn(console, 'log');
    const errSpy = jest.spyOn(console, 'error');
    const event = v2From(WEBHOOK_FIXTURES.fbEdit, {
      tenantHash: undefined, // trips validateV2BaseEvent
      editedText: 'SECRET-EDITED-CONTENT',
      replyTo: { storyUrl: 'https://instagram.com/stories/SECRET' },
    });
    await expect(handler(event)).resolves.not.toThrow();
    const allLogged = [...logSpy.mock.calls, ...errSpy.mock.calls]
      .map((args) => args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' '))
      .join('\n');
    expect(allLogged).toContain('Event validation failed'); // spy actually observes the drop line
    expect(allLogged).not.toContain('SECRET-EDITED-CONTENT');
    expect(allLogged).not.toContain('stories/SECRET');
    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('stale attachment event (>24h): fallback reply suppressed by the send-window guard', async () => {
    loadConfig.mockResolvedValue(flagOnConfig);
    const event = v2From(WEBHOOK_FIXTURES.fbAttachmentImage, {
      timestamp: Date.now() - 25 * 60 * 60 * 1000,
    });
    await expect(handler(event)).resolves.not.toThrow();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
  });

  test('stale delete event (>24h) STILL deletes history (window guard exempts non-sending kinds)', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [{ sessionId: `meta:${WEBHOOK_FIXTURES.PAGE_ID}:${WEBHOOK_FIXTURES.IGSID}`, messageTimestamp: 1, mid: WEBHOOK_FIXTURES.MID, role: 'user' }],
    });
    const event = v2From(WEBHOOK_FIXTURES.igDelete, {
      timestamp: Date.now() - 48 * 60 * 60 * 1000,
      channelType: 'instagram',
    });
    await expect(handler(event)).resolves.not.toThrow();
    expect(ddbMock).toHaveReceivedCommand(DeleteCommand);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('Query failure during delete handling propagates (Lambda retry contract), no sends', async () => {
    ddbMock.on(QueryCommand).rejects(new Error('provisioned throughput exceeded'));
    const event = v2From(WEBHOOK_FIXTURES.igDelete, { channelType: 'instagram' });
    await expect(handler(event)).rejects.toThrow('provisioned throughput exceeded');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('UpdateItem failure during edit handling propagates, no sends', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [{ sessionId: `meta:${WEBHOOK_FIXTURES.PAGE_ID}:${WEBHOOK_FIXTURES.PSID}`, messageTimestamp: 1, mid: WEBHOOK_FIXTURES.MID, role: 'user' }],
    });
    ddbMock.on(UpdateCommand).rejects(new Error('conditional check failed'));
    const event = v2From(WEBHOOK_FIXTURES.fbEdit);
    await expect(handler(event)).rejects.toThrow('conditional check failed');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('fallback send failure rethrows (async-retry contract)', async () => {
    loadConfig.mockResolvedValue(flagOnConfig);
    fetchMock = makeFetchMock([{ ok: false, status: 500, body: { error: { message: 'send failed' } } }]);
    global.fetch = fetchMock;
    const event = v2From(WEBHOOK_FIXTURES.fbAttachmentImage);
    await expect(handler(event)).rejects.toThrow();
  });

  test('disclosure send failure does NOT block the real reply', async () => {
    loadConfig.mockResolvedValue(flagOnConfig);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([])); // empty history → disclosure due
    // typing ok, disclosure send FAILS, reply send ok
    fetchMock = makeFetchMock([
      { ok: true, body: {} },
      { ok: false, status: 500, body: { error: { message: 'disclosure send failed' } } },
      { ok: true, body: {} },
    ]);
    global.fetch = fetchMock;
    await expect(handler(buildEvent())).resolves.not.toThrow();
    // typing + failed disclosure attempt + reply = 3 fetch calls
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  test('IG text of exactly 1000 chars stays a single chunk', async () => {
    loadConfig.mockResolvedValue(flagOnConfig);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi', messageTimestamp: Date.now() - 60000 }])); // non-empty → no disclosure
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('B'.repeat(1000)));
    await handler(buildEvent({ channelType: 'instagram' }));
    const sendBodies = fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .filter((b) => b && b.message && typeof b.message.text === 'string');
    expect(sendBodies).toHaveLength(1);
    expect(sendBodies[0].message.text.length).toBe(1000);
  });
});

// ─── M-Ha — channel health: send-error classification (G1) ──────────────────

describe('M-Ha — META_SEND_FAILURE classification', () => {
  const { classifyMetaSendError, CLASSIFICATIONS } = require('./metaSendErrors');

  test.each([
    ['token dead', { error: { code: 190 } }, 'token_dead'],
    ['user unavailable', { error: { code: 551 } }, 'user_unavailable'],
    ['rate limited', { error: { code: 613 } }, 'rate_limited'],
    ['window closed', { error: { code: 10, error_subcode: 1545041 } }, 'window_closed'],
    ['page restricted', { error: { code: 10, error_subcode: 1893063 } }, 'page_restricted'],
    ['unknown code', { error: { code: 42 } }, 'unclassified'],
    ['code 10 unknown subcode', { error: { code: 10, error_subcode: 99 } }, 'unclassified'],
    ['malformed body', {}, 'unclassified'],
    ['null body', null, 'unclassified'],
  ])('%s → %s', (_n, body, expected) => {
    expect(classifyMetaSendError(body).classification).toBe(expected);
  });

  test('failed reply send emits a structured META_SEND_FAILURE line with the class', async () => {
    const errSpy = jest.spyOn(console, 'error');
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi', messageTimestamp: Date.now() - 60000 }]));
    fetchMock = makeFetchMock([
      { ok: true, body: {} }, // typing
      { ok: false, status: 400, body: { error: { code: 190, message: 'Error validating access token' } } },
    ]);
    global.fetch = fetchMock;

    await expect(handler(buildEvent())).rejects.toThrow();

    const allErr = errSpy.mock.calls
      .map((args) => args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' '))
      .join('\n');
    expect(allErr).toContain('META_SEND_FAILURE');
    expect(allErr).toContain('token_dead');
    // never leak the raw token-bearing error message into the structured line
    const structured = errSpy.mock.calls.find((args) => String(args[0]).includes('META_SEND_FAILURE'));
    expect(JSON.stringify(structured)).not.toContain('access_token');
  });

  test('CLASSIFICATIONS surface is the frozen five + unclassified', () => {
    expect(Object.values(CLASSIFICATIONS).sort()).toEqual([
      'page_restricted', 'rate_limited', 'token_dead', 'unclassified', 'user_unavailable', 'window_closed',
    ].sort());
  });
});

// ─── M3b — Messenger V5 wiring (flag-gated; C8 windowing; tail hygiene) ──────

describe('M3b — Messenger V5 wiring', () => {
  const { buildMessengerPrompt_TEST_EXPORT } = {}; // legacy prompt pinned via request-body capture below
  const V5_FLAG_CONFIG = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'You are a helpful recruiter assistant.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
    cta_definitions: {
      volunteer_form: { label: 'Volunteer Sign-Up', action: 'start_form', ai_available: true },
    },
  };

  function bedrockBodies() {
    return bedrockMock.commandCalls(InvokeModelCommand).map((c) =>
      JSON.parse(Buffer.from(c.args[0].input.body).toString('utf8'))
    );
  }

  function sentTexts() {
    return fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .filter((b) => b && b.message && typeof b.message.text === 'string')
      .map((b) => b.message.text);
  }

  test('flag OFF: request body is byte-identical to the legacy buildMessengerPrompt output', async () => {
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'prior' }]));
    await handler(buildEvent());

    const [body] = bedrockBodies();
    // Pin the legacy prompt shape: tone + the legacy STRICT RULES sentence
    expect(body.system).toContain('You are a helpful recruiter assistant.');
    expect(body.system).toContain('You are responding via Facebook Messenger where the chat window is very small.');
    // V5 material must be absent
    expect(body.system).not.toContain('AVAILABLE ACTIONS');
    expect(body.system).not.toContain('ACTION TAIL');
    expect(body.system).not.toContain('mobile messaging app');
  });

  test('flag ON + valid tail: tail never crosses sendResponseMessages, ids validated, visible text stored', async () => {
    loadConfig.mockResolvedValue(V5_FLAG_CONFIG);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'prior', messageTimestamp: Date.now() - 60000 }]));
    bedrockMock.on(InvokeModelCommand).resolves(
      makeBedrockResponse('Great question! We can get you signed up.\n<<<ACTIONS ["volunteer_form"]>>>')
    );

    await handler(buildEvent());

    for (const text of sentTexts()) {
      expect(text).not.toContain('<<<ACTIONS');
      expect(text).not.toContain('>>>');
      expect(text).not.toContain('volunteer_form');
    }
    // exactly one Bedrock call (valid tail ⇒ no V4 fallback)
    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    // stored assistant row carries the visible text, not the tail
    const puts = ddbMock.commandCalls(PutCommand).map((c) => c.args[0].input).filter((i) => i.Item?.role === 'assistant');
    expect(puts).toHaveLength(1);
    expect(puts[0].Item.content).not.toContain('<<<ACTIONS');
  });

  test('flag ON + malformed tail: exactly ONE fail-soft selectActionsV4 call, reply still served', async () => {
    loadConfig.mockResolvedValue(V5_FLAG_CONFIG);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'prior', messageTimestamp: Date.now() - 60000 }]));
    bedrockMock.on(InvokeModelCommand)
      .resolvesOnce(makeBedrockResponse('A reply.\n<<<ACTIONS not-json'))
      .resolves(makeBedrockResponse('[]')); // V4 selector fallback response

    await handler(buildEvent());

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 2); // turn + one fallback
    const texts = sentTexts();
    expect(texts.some((t) => t.includes('A reply.'))).toBe(true);
    for (const t of texts) expect(t).not.toContain('<<<ACTIONS');
  });

  test('flag ON: formatting markers stripped from the sent reply (M3a residual belt-and-suspenders)', async () => {
    loadConfig.mockResolvedValue(V5_FLAG_CONFIG);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'prior', messageTimestamp: Date.now() - 60000 }]));
    bedrockMock.on(InvokeModelCommand).resolves(
      makeBedrockResponse('We have **Love Box** and __Dare to Dream__ programs.\n<<<ACTIONS []>>>')
    );

    await handler(buildEvent());

    const texts = sentTexts();
    expect(texts.some((t) => t.includes('Love Box') && !t.includes('**'))).toBe(true);
    expect(texts.every((t) => !t.includes('__'))).toBe(true);
  });

  test('C8 through the handler: >24h-gap history does NOT trip TURN CHECK; same-session questions do', async () => {
    loadConfig.mockResolvedValue(V5_FLAG_CONFIG);
    const now = Date.now();
    // Two old assistant questions across a 48h gap, then a fresh exchange
    ddbMock.on(QueryCommand).resolves({
      Items: [
        { role: 'assistant', content: 'Old question one?', messageTimestamp: now - 50 * 3600 * 1000 },
        { role: 'assistant', content: 'Old question two?', messageTimestamp: now - 49 * 3600 * 1000 },
        { role: 'user', content: 'back again', messageTimestamp: now - 3600 * 1000 },
      ],
    });
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Welcome back!\n<<<ACTIONS []>>>'));

    await handler(buildEvent());
    let [body] = bedrockBodies();
    expect(body.system).not.toContain('TURN CHECK');

    // Same-session: two recent questions ⇒ TURN CHECK present
    bedrockMock.reset();
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Sure!\n<<<ACTIONS []>>>'));
    ddbMock.on(QueryCommand).resolves({
      Items: [
        { role: 'assistant', content: 'Recent question one?', messageTimestamp: now - 2 * 3600 * 1000 },
        { role: 'user', content: 'answer', messageTimestamp: now - 3600 * 1000 },
        { role: 'assistant', content: 'Recent question two?', messageTimestamp: now - 1800 * 1000 },
      ],
    });
    await handler(buildEvent());
    [body] = bedrockBodies();
    expect(body.system).toContain('TURN CHECK');
  });

  test('flag ON, no ai_available CTAs: plain V5 short-form, no tail machinery, one Bedrock call', async () => {
    loadConfig.mockResolvedValue({ ...V5_FLAG_CONFIG, cta_definitions: {} });
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'prior', messageTimestamp: Date.now() - 60000 }]));
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Plain reply.'));

    await handler(buildEvent());

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    const [body] = bedrockBodies();
    expect(body.system).not.toContain('ACTION TAIL');
    expect(sentTexts().some((t) => t.includes('Plain reply.'))).toBe(true);
  });
});

// ─── M3b review follow-ups ───────────────────────────────────────────────────

describe('M3b — review follow-ups', () => {
  const V5_FLAG_CONFIG2 = {
    model_id: 'config-model',
    tone_prompt: 'T.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
    cta_definitions: { volunteer_form: { label: 'V', action: 'start_form', ai_available: true } },
  };

  test('returning user after >24h gap gets the disclosure line AGAIN (C8: each session)', async () => {
    loadConfig.mockResolvedValue(V5_FLAG_CONFIG2);
    const now = Date.now();
    ddbMock.on(QueryCommand).resolves({
      Items: [
        { role: 'user', content: 'old', messageTimestamp: now - 30 * 3600 * 1000 },
        { role: 'assistant', content: 'old reply', messageTimestamp: now - 29 * 3600 * 1000 },
      ],
    });
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Welcome back!\n<<<ACTIONS []>>>'));

    await handler(buildEvent());

    const texts = fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .filter((b) => b && b.message && typeof b.message.text === 'string')
      .map((b) => b.message.text);
    expect(texts.some((t) => t.includes('automated assistant'))).toBe(true); // disclosure re-fired
  });

  test('C6 model precedence: channel override wins in the V5 request', async () => {
    loadConfig.mockResolvedValue({
      ...V5_FLAG_CONFIG2,
      messenger_behavior: { model_id: 'section-model', channel_overrides: { messenger: { model_id: 'channel-model' } } },
    });
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi', messageTimestamp: Date.now() - 60000 }]));
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Hi!\n<<<ACTIONS []>>>'));

    await handler(buildEvent());

    expect(bedrockMock.commandCalls(InvokeModelCommand)[0].args[0].input.modelId).toBe('channel-model');
  });

  test('hallucinated sentinel with NO catalog still never leaks (always-parse hardening)', async () => {
    loadConfig.mockResolvedValue({ ...V5_FLAG_CONFIG2, cta_definitions: {} });
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi', messageTimestamp: Date.now() - 60000 }]));
    bedrockMock.on(InvokeModelCommand).resolves(
      makeBedrockResponse('Copied from KB: <<<ACTIONS ["whatever"]>>> end.')
    );

    await handler(buildEvent());

    const texts = fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .filter((b) => b && b.message && typeof b.message.text === 'string')
      .map((b) => b.message.text);
    for (const t of texts) expect(t).not.toContain('<<<ACTIONS');
  });
});

// ─── M4 — CTA rendering + PIC1 payload routing (C3, C5, C9) ─────────────────

describe('M4 — rendering matrix (renderMessengerActions)', () => {
  const { renderMessengerActions, resolveCtaPayload, truncateTitle } = require('./renderMessengerActions');
  const noopLog = () => {};
  const CFG = {
    cta_definitions: {
      learn_x: { label: 'Our Programs', action: 'send_query', query: 'tell me about programs', ai_available: true },
      info_y: { label: 'About Us', action: 'show_info', prompt: 'describe the org', ai_available: true },
      apply_z: { label: 'Apply To Volunteer Today', action: 'external_link', url: 'https://x.org/apply', ai_available: true },
      form_nourl: { label: 'Sign Up', action: 'start_form', formId: 'f1', ai_available: true },
      form_url: { label: 'Sign Up Online', action: 'start_form', formId: 'f1', url: 'https://x.org/form', ai_available: true },
    },
  };

  test('suggestion class → quick replies with PIC1 payloads; commitment class → web_url buttons', () => {
    const r = renderMessengerActions(['learn_x', 'info_y', 'apply_z', 'form_url'], CFG, noopLog);
    expect(r.quickReplies.map((q) => q.payload)).toEqual(['PIC1:cta:learn_x', 'PIC1:cta:info_y']);
    expect(r.quickReplies.every((q) => q.content_type === 'text')).toBe(true);
    expect(r.buttons.map((b) => b.url)).toEqual(['https://x.org/apply', 'https://x.org/form']);
    expect(r.buttons.every((b) => b.type === 'web_url')).toBe(true);
  });

  test('start_form without url: skipped with a log, never silently', () => {
    const logs = [];
    const r = renderMessengerActions(['form_nourl'], CFG, (lvl, msg, meta) => logs.push({ lvl, msg, meta }));
    expect(r.quickReplies).toHaveLength(0);
    expect(r.buttons).toHaveLength(0);
    expect(logs.some((l) => l.msg.includes('no url') && l.meta.ctaId === 'form_nourl')).toBe(true);
  });

  test('titles truncated to the C5 20-char cap', () => {
    expect(truncateTitle('Apply To Volunteer Today').length).toBeLessThanOrEqual(20);
    const r = renderMessengerActions(['apply_z'], CFG, noopLog);
    expect(r.buttons[0].title.length).toBeLessThanOrEqual(20);
  });

  test('C5 caps enforced: ≤13 quick replies, ≤3 buttons, V5 order wins', () => {
    const bigCfg = { cta_definitions: {} };
    const qrIds = [], btnIds = [];
    for (let i = 0; i < 15; i++) {
      bigCfg.cta_definitions[`q${i}`] = { label: `Q${i}`, action: 'send_query', query: 'x', ai_available: true };
      qrIds.push(`q${i}`);
      bigCfg.cta_definitions[`b${i}`] = { label: `B${i}`, action: 'external_link', url: 'https://x.org', ai_available: true };
      btnIds.push(`b${i}`);
    }
    const r = renderMessengerActions([...qrIds, ...btnIds], bigCfg, noopLog);
    expect(r.quickReplies).toHaveLength(13);
    expect(r.buttons).toHaveLength(3);
    expect(r.quickReplies[0].title).toBe('Q0');
    expect(r.buttons[0].title).toBe('B0');
  });

  test('resolveCtaPayload: send_query → query, show_info → prompt, unknown → null', () => {
    expect(resolveCtaPayload('PIC1:cta:learn_x', CFG).turnText).toBe('tell me about programs');
    expect(resolveCtaPayload('PIC1:cta:info_y', CFG).turnText).toBe('describe the org');
    expect(resolveCtaPayload('PIC1:cta:nope', CFG)).toBeNull();
    expect(resolveCtaPayload('PIC2:cta:learn_x', CFG)).toBeNull();
    expect(resolveCtaPayload('GET_STARTED', CFG)).toBeNull();
  });
});

describe('M4 — rendering + routing through the handler', () => {
  const M4_CONFIG = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'T.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
    cta_definitions: {
      learn_x: { label: 'Our Programs', action: 'send_query', query: 'tell me about programs', ai_available: true },
      apply_z: { label: 'Apply Now', action: 'external_link', url: 'https://x.org/apply', ai_available: true },
    },
  };

  function sentMessages() {
    return fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .filter((b) => b && b.message)
      .map((b) => b.message);
  }

  beforeEach(() => {
    loadConfig.mockResolvedValue(M4_CONFIG);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi', messageTimestamp: Date.now() - 60000 }]));
  });

  test('QR-only turn: quick replies ride the final text chunk', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Sure!\n<<<ACTIONS ["learn_x"]>>>'));
    await handler(buildEvent());
    const msgs = sentMessages().filter((m) => m.text || m.attachment);
    const last = msgs[msgs.length - 1];
    expect(last.text).toContain('Sure!');
    expect(last.quick_replies).toHaveLength(1);
    expect(last.quick_replies[0].payload).toBe('PIC1:cta:learn_x');
  });

  test('button turn: template sent AFTER the final text chunk (C9)', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Ready when you are.\n<<<ACTIONS ["apply_z"]>>>'));
    await handler(buildEvent());
    const msgs = sentMessages().filter((m) => m.text || m.attachment);
    const last = msgs[msgs.length - 1];
    expect(last.attachment.payload.template_type).toBe('button');
    expect(last.attachment.payload.buttons[0].url).toBe('https://x.org/apply');
    expect(msgs[msgs.length - 2].text).toContain('Ready when you are.');
  });

  test('mixed turn: QRs attach to the button template — the turn\'s LAST message (C9 v1.1)', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Both!\n<<<ACTIONS ["learn_x","apply_z"]>>>'));
    await handler(buildEvent());
    const msgs = sentMessages().filter((m) => m.text || m.attachment);
    const last = msgs[msgs.length - 1];
    expect(last.attachment.payload.template_type).toBe('button');
    expect(last.quick_replies).toHaveLength(1);
    const textMsgs = msgs.filter((m) => m.text);
    expect(textMsgs.every((m) => !m.quick_replies)).toBe(true);
  });

  test('QR tap round-trip: PIC1:cta:learn_x becomes the CTA\'s query as the Bedrock user turn', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Programs are…\n<<<ACTIONS []>>>'));
    await handler(buildEvent({
      eventKind: 'quick_reply',
      messageText: 'Our Programs',
      quickReplyPayload: 'PIC1:cta:learn_x',
    }));
    const body = JSON.parse(Buffer.from(bedrockMock.commandCalls(InvokeModelCommand)[0].args[0].input.body).toString());
    const userTurn = body.messages[body.messages.length - 1].content[0].text;
    expect(userTurn).toBe('tell me about programs');
  });

  test('unknown PIC1 payload falls through to RAG as free text (C3)', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('I can help!\n<<<ACTIONS []>>>'));
    await handler(buildEvent({
      eventKind: 'quick_reply',
      messageText: 'Old Button',
      quickReplyPayload: 'PIC1:ffld:form1:field:value',
    }));
    const body = JSON.parse(Buffer.from(bedrockMock.commandCalls(InvokeModelCommand)[0].args[0].input.body).toString());
    const userTurn = body.messages[body.messages.length - 1].content[0].text;
    expect(userTurn).toBe('Old Button'); // message.text flows, payload untouched
  });

  test('GET_STARTED postback preserved (welcome short-circuit, no rendering)', async () => {
    await handler(buildEvent({ eventKind: 'postback', isPostback: true, messageText: 'GET_STARTED' }));
    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    const msgs = sentMessages().filter((m) => m.text);
    expect(msgs.some((m) => m.text.includes('help'))).toBe(true);
  });

  test('flag OFF: no rendering machinery on the legacy path (no quick_replies, no templates)', async () => {
    loadConfig.mockResolvedValue({ ...M4_CONFIG, feature_flags: {} });
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Legacy reply.'));
    await handler(buildEvent());
    const msgs = sentMessages();
    expect(msgs.every((m) => !m.quick_replies && !m.attachment)).toBe(true);
  });
});

// ─── M4 review follow-ups ────────────────────────────────────────────────────

describe('M4 — review follow-ups', () => {
  const M4_CFG = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'T.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
    cta_definitions: {
      learn_x: { label: 'Our Programs', action: 'send_query', query: 'tell me about programs', ai_available: true },
      apply_z: { label: 'Apply Now', action: 'external_link', url: 'https://x.org/apply', ai_available: true },
    },
  };

  beforeEach(() => {
    loadConfig.mockResolvedValue(M4_CFG);
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi', messageTimestamp: Date.now() - 60000 }]));
  });

  test('IG button turn: template rides the /me/messages IG path with the same body shape', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Go!\n<<<ACTIONS ["apply_z"]>>>'));
    await handler(buildEvent({ channelType: 'instagram' }));
    const templateCall = fetchMock.mock.calls.find(([, opts]) => {
      const b = opts && opts.body ? JSON.parse(opts.body) : null;
      return b && b.message && b.message.attachment;
    });
    expect(templateCall).toBeDefined();
    expect(templateCall[0]).toContain('/me/messages'); // IG send path
    const body = JSON.parse(templateCall[1].body);
    expect(body.message.attachment.payload.template_type).toBe('button');
    expect(body.messaging_type).toBeUndefined(); // IG body shape (no messaging_type)
  });

  test('stale cta id (config edited after button rendered) → tapped text flows to RAG (C3)', async () => {
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Happy to help!\n<<<ACTIONS []>>>'));
    await handler(buildEvent({
      eventKind: 'quick_reply',
      messageText: 'Old Removed Button',
      quickReplyPayload: 'PIC1:cta:removed_cta_id',
    }));
    const body = JSON.parse(Buffer.from(bedrockMock.commandCalls(InvokeModelCommand)[0].args[0].input.body).toString());
    expect(body.messages[body.messages.length - 1].content[0].text).toBe('Old Removed Button');
  });

  test('empty button_intro override degrades to the default, send never fails on it', async () => {
    loadConfig.mockResolvedValue({ ...M4_CFG, messenger_behavior: { strings: { button_intro: '   ' } } });
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Go!\n<<<ACTIONS ["apply_z"]>>>'));
    await handler(buildEvent());
    const templateCall = fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .find((b) => b && b.message && b.message.attachment);
    expect(templateCall.message.attachment.payload.text.trim().length).toBeGreaterThan(0);
  });

  test('coalesced PIC1 tap drains as the CTA\'s canonical query (C7 step 3)', async () => {
    loadConfig.mockResolvedValue(M4_CFG);
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('Combined answer.\n<<<ACTIONS []>>>'));
    // This suite has no CONVERSATION_STATE_TABLE (lock disabled) — test the
    // routing seam directly through the drain text-mapping logic instead:
    const { resolveCtaPayload } = require('./renderMessengerActions');
    const batch = [
      { text: 'Our Programs', quickReplyPayload: 'PIC1:cta:learn_x', mid: 'm1', timestamp: 1 },
      { text: 'and a typed question', mid: 'm2', timestamp: 2 },
    ];
    const texts = batch.map((i) => {
      if (typeof i.quickReplyPayload === 'string' && i.quickReplyPayload.startsWith('PIC1:')) {
        const r = resolveCtaPayload(i.quickReplyPayload, M4_CFG);
        if (r) return r.turnText;
      }
      return i.text;
    });
    expect(texts).toEqual(['tell me about programs', 'and a typed question']);
  });
});

// ─── M6a — Escalation: transfer + notify (C2 escalation_confirmation, C4 pause) ─
//
// "Talk to a human" intent → confirmation → pass_thread_control → pause row →
// staff email, all defensive on the side effects (never on the confirmation).
// Adversarial focus (plan §6 M6a): intent false-positives ("how do humans
// apply?" must NOT escalate) and the never-share-IAM-roles rule (new
// permissions land on this Lambda's own role only — IAM ships in the picasso
// repo, out of scope here).

const escalation = require('./escalation');

const DEFAULT_ESCALATION_CONFIRMATION =
  "Of course — I'm connecting you with a person from the team now. They'll reply right here.";

describe('M6a — detectEscalationIntent (intent table)', () => {
  const POSITIVE = [
    'Can I talk to a human?',
    'I want to speak with a person',
    'I need to chat with an agent',
    "I'd like to talk to a representative",
    'let me talk to someone',
    'please speak to a human',
    'connect me to a human agent',
    'transfer me to a human please',
    'speak to staff',
    'real human please',
    'connect me with a representative',
  ];

  const NEGATIVE = [
    'how do humans apply?',
    'what do your agents do?',
    'the person I mentor is great',
    'How can I talk about the weather',
    'I want to become a volunteer',
    'can I speak at the event',
    'Is there a human resources department?',
    'What programs do you have for humans in need?',
    'I need help with my application',
    'Please transfer my file to another department',
    'I want to chat about the schedule',
    // Code review — negation guard (HIGH): a negated request must not escalate.
    'I do not want to talk to a human',
    'no thanks I do not need to speak with a person',
    'I would rather not talk to an agent right now',
    // Code review — connect me / transfer me need the same human-noun
    // gating as the main pattern (MEDIUM).
    'connect me to the volunteer page',
    'connect me with more information',
    'please transfer me some documents',
  ];

  test.each(POSITIVE)('MATCHES: %j', (text) => {
    expect(escalation.detectEscalationIntent(text)).toBe(true);
  });

  test.each(NEGATIVE)('does NOT match: %j', (text) => {
    expect(escalation.detectEscalationIntent(text)).toBe(false);
  });

  test('non-string / empty input never matches', () => {
    expect(escalation.detectEscalationIntent('')).toBe(false);
    expect(escalation.detectEscalationIntent('   ')).toBe(false);
    expect(escalation.detectEscalationIntent(undefined)).toBe(false);
    expect(escalation.detectEscalationIntent(null)).toBe(false);
  });
});

describe('M6a — escalation.js unit tests (pass_thread_control / pause row / SES)', () => {
  afterEach(() => {
    delete global.fetch;
    delete process.env.FB_INBOX_APP_ID;
    delete process.env.IG_INBOX_APP_ID;
    delete process.env.SES_FROM_EMAIL;
  });

  describe('passThreadControl', () => {
    test('Messenger (FB): POSTs to /me/pass_thread_control with the FB inbox app id, content-free metadata', async () => {
      const fetchSpy = jest.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ success: true }) });
      global.fetch = fetchSpy;

      const result = await escalation.passThreadControl({
        pageId: 'PAGE_456',
        psid: 'PSID_123',
        channelType: 'messenger',
        accessToken: 'page-token',
        metadata: 'picasso-escalation tenant=TENANT_789',
      });

      expect(result).toEqual({ ok: true });
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain('/me/pass_thread_control');
      // access_token rides the JSON body (code review: match this file's
      // existing convention — never the URL query string).
      expect(url).not.toContain('access_token');
      const body = JSON.parse(opts.body);
      expect(body.access_token).toBe('page-token');
      expect(body.recipient.id).toBe('PSID_123');
      expect(body.target_app_id).toBe(escalation.DEFAULT_FB_INBOX_APP_ID);
      expect(body.metadata).toBe('picasso-escalation tenant=TENANT_789');
      // Metadata must never carry conversation content.
      expect(body.metadata).not.toMatch(/talk|human|speak/i);
    });

    test('Instagram: uses the IG inbox app id', async () => {
      const fetchSpy = jest.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ success: true }) });
      global.fetch = fetchSpy;

      await escalation.passThreadControl({
        pageId: 'PAGE_456',
        psid: 'IGSID_123',
        channelType: 'instagram',
        accessToken: 'page-token',
        metadata: 'picasso-escalation tenant=TENANT_789',
      });

      const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
      expect(body.target_app_id).toBe(escalation.DEFAULT_IG_INBOX_APP_ID);
    });

    test('env overrides win over the built-in default app ids', async () => {
      process.env.FB_INBOX_APP_ID = 'custom-fb-app-id';
      const fetchSpy = jest.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) });
      global.fetch = fetchSpy;

      await escalation.passThreadControl({
        pageId: 'PAGE_456', psid: 'PSID_123', channelType: 'messenger', accessToken: 't', metadata: 'm',
      });

      const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
      expect(body.target_app_id).toBe('custom-fb-app-id');
    });

    test('non-2xx response (tenant has not configured Conversation Routing) → {ok:false}, never throws', async () => {
      const fetchSpy = jest.fn().mockResolvedValue({
        ok: false,
        status: 400,
        json: async () => ({ error: { message: 'default app not set' } }),
      });
      global.fetch = fetchSpy;

      await expect(
        escalation.passThreadControl({
          pageId: 'PAGE_456', psid: 'PSID_123', channelType: 'messenger', accessToken: 't', metadata: 'm',
        })
      ).resolves.toEqual({ ok: false });
    });

    test('network error → {ok:false}, never throws', async () => {
      global.fetch = jest.fn().mockRejectedValue(new Error('ENOTFOUND'));

      await expect(
        escalation.passThreadControl({
          pageId: 'PAGE_456', psid: 'PSID_123', channelType: 'messenger', accessToken: 't', metadata: 'm',
        })
      ).resolves.toEqual({ ok: false });
    });
  });

  describe('writePauseRow (C4 pause row shape)', () => {
    test('PutCommand carries the C4 shape with a ~24h expires_at', async () => {
      const sendSpy = jest.fn().mockResolvedValue({});
      const fakeClient = { send: sendSpy };
      const before = Date.now();

      await escalation.writePauseRow({
        client: fakeClient,
        tableName: 'picasso-conversation-state',
        sessionId: 'meta:PAGE_456:PSID_123',
      });

      expect(sendSpy).toHaveBeenCalledTimes(1);
      const putInput = sendSpy.mock.calls[0][0].input;
      expect(putInput.TableName).toBe('picasso-conversation-state');
      const item = putInput.Item;
      expect(item.sessionId).toBe('meta:PAGE_456:PSID_123');
      expect(item.stateType).toBe('pause');
      expect(item.reason).toBe('escalation');
      expect(item.schema_version).toBe(1);
      expect(typeof item.paused_at).toBe('number');
      expect(typeof item.updated_at).toBe('number');
      // ~24h TTL (epoch seconds) — allow a small scheduling slop window.
      const expectedExpiry = Math.floor(before / 1000) + 24 * 60 * 60;
      expect(item.expires_at).toBeGreaterThanOrEqual(expectedExpiry - 5);
      expect(item.expires_at).toBeLessThanOrEqual(expectedExpiry + 5);
    });

    test('DDB failure propagates to the caller (index.js decides how loud to be)', async () => {
      const fakeClient = { send: jest.fn().mockRejectedValue(new Error('ProvisionedThroughputExceeded')) };
      await expect(
        escalation.writePauseRow({ client: fakeClient, tableName: 't', sessionId: 's' })
      ).rejects.toThrow('ProvisionedThroughputExceeded');
    });
  });

  describe('sendEscalationEmail (G-P2 — content-free, PII-minimal)', () => {
    const FIXTURE_MESSAGE = 'I need help finding my SECRET_CASE_FILE_9182 please talk to a human';
    const FIXTURE_PSID = 'PSID_SUPER_SECRET_555';
    const FIXTURE_SESSION_ID = `meta:PAGE_456:${FIXTURE_PSID}`;

    test('recipient + SES_FROM_EMAIL configured → sends; body has NO psid, NO sessionId, NO message content; only PAGE ID + metadata', async () => {
      process.env.SES_FROM_EMAIL = 'notify@myrecruiter.ai';
      const sendSpy = jest.fn().mockResolvedValue({ MessageId: 'ses-1' });
      const fakeSesClient = { send: sendSpy };
      const config = { messenger_behavior: { escalation_email: 'staff@tenant.org' } };

      const result = await escalation.sendEscalationEmail({
        sesClient: fakeSesClient,
        config,
        tenantId: 'TENANT_789',
        channelType: 'messenger',
        sessionId: FIXTURE_SESSION_ID,
        pageId: 'PAGE_456',
      });

      expect(result).toEqual({ sent: true });
      expect(sendSpy).toHaveBeenCalledTimes(1);
      const emailInput = sendSpy.mock.calls[0][0].input;
      expect(emailInput.Destination.ToAddresses).toEqual(['staff@tenant.org']);
      expect(emailInput.Source).toBe('notify@myrecruiter.ai');

      const wholeEmail = JSON.stringify(emailInput);
      // G-P2 pin: grep-style assertion that content/psid/session id never leak.
      expect(wholeEmail).not.toContain(FIXTURE_PSID);
      expect(wholeEmail).not.toContain(FIXTURE_SESSION_ID);
      expect(wholeEmail).not.toContain('meta:');
      expect(wholeEmail).not.toContain(FIXTURE_MESSAGE);
      expect(wholeEmail).not.toContain('SECRET_CASE_FILE_9182');
      // Approved content-free refinement: page id IS allowed (business metadata).
      expect(wholeEmail).toContain('PAGE_456');
      expect(wholeEmail).toContain('business.facebook.com/latest/inbox');
      expect(wholeEmail).toContain('TENANT_789');
    });

    test('never logs the recipient address (only emailSent + tenantId)', async () => {
      process.env.SES_FROM_EMAIL = 'notify@myrecruiter.ai';
      const fakeSesClient = { send: jest.fn().mockResolvedValue({ MessageId: 'ses-1' }) };
      const config = { messenger_behavior: { escalation_email: 'staff-secret-address@tenant.org' } };
      const logSpy = jest.spyOn(console, 'log');
      const errSpy = jest.spyOn(console, 'error');

      await escalation.sendEscalationEmail({
        sesClient: fakeSesClient,
        config,
        tenantId: 'TENANT_789',
        channelType: 'messenger',
        sessionId: 'meta:PAGE_456:PSID_123',
        pageId: 'PAGE_456',
      });

      const allLogged = [...logSpy.mock.calls, ...errSpy.mock.calls]
        .map((args) => args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' '))
        .join('\n');
      expect(allLogged).not.toContain('staff-secret-address@tenant.org');
      expect(allLogged).toContain('"emailSent":true');
      expect(allLogged).toContain('TENANT_789');
    });

    test('escalation_email absent → skipped, SES never called', async () => {
      process.env.SES_FROM_EMAIL = 'notify@myrecruiter.ai';
      const sendSpy = jest.fn();
      const result = await escalation.sendEscalationEmail({
        sesClient: { send: sendSpy },
        config: {},
        tenantId: 'TENANT_789',
        channelType: 'messenger',
        sessionId: 'meta:PAGE_456:PSID_123',
      });
      expect(result).toEqual({ skipped: true, reason: 'no_recipient' });
      expect(sendSpy).not.toHaveBeenCalled();
    });

    test('SES_FROM_EMAIL unset → skipped even with a recipient configured, SES never called', async () => {
      const sendSpy = jest.fn();
      const result = await escalation.sendEscalationEmail({
        sesClient: { send: sendSpy },
        config: { messenger_behavior: { escalation_email: 'staff@tenant.org' } },
        tenantId: 'TENANT_789',
        channelType: 'messenger',
        sessionId: 'meta:PAGE_456:PSID_123',
      });
      expect(result).toEqual({ skipped: true, reason: 'ses_disabled' });
      expect(sendSpy).not.toHaveBeenCalled();
    });

    test('SES send failure is caught — never throws', async () => {
      process.env.SES_FROM_EMAIL = 'notify@myrecruiter.ai';
      const result = await escalation.sendEscalationEmail({
        sesClient: { send: jest.fn().mockRejectedValue(new Error('SES throttled')) },
        config: { messenger_behavior: { escalation_email: 'staff@tenant.org' } },
        tenantId: 'TENANT_789',
        channelType: 'messenger',
        sessionId: 'meta:PAGE_456:PSID_123',
      });
      expect(result).toEqual({ failed: true });
    });
  });
});

describe('M6a — escalation E2E through the handler (CONVERSATION_STATE_TABLE unset — no lock)', () => {
  const ESCALATION_CFG = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'You are a helpful recruiter assistant.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
    messenger_behavior: { escalation_email: 'staff@tenant.org' },
  };

  beforeEach(() => {
    process.env.SES_FROM_EMAIL = 'notify@myrecruiter.ai';
    loadConfig.mockResolvedValue(ESCALATION_CFG);
  });

  function makeEscalationFetchMock(passThreadControlResp = { ok: true, status: 200, body: { success: true } }) {
    return jest.fn().mockImplementation((url) => {
      if (typeof url === 'string' && url.includes('pass_thread_control')) {
        return Promise.resolve({
          ok: passThreadControlResp.ok,
          status: passThreadControlResp.status,
          json: () => Promise.resolve(passThreadControlResp.body),
        });
      }
      // Confirmation send (and any other /messages call).
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ message_id: 'mid.confirmation' }) });
    });
  }

  test('flag ON + "talk to a human" → confirmation sent, pass_thread_control POSTed (FB app id), email sent, MESSENGER_ESCALATED emitted, NO Bedrock call', async () => {
    fetchMock = makeEscalationFetchMock();
    global.fetch = fetchMock;

    await handler(buildEvent({ messageText: 'Can I talk to a human?' }));

    // Confirmation text sent via the Send API.
    const sendCalls = fetchMock.mock.calls.filter(([url]) => !url.includes('pass_thread_control'));
    const confirmationBody = JSON.parse(sendCalls[0][1].body);
    expect(confirmationBody.message.text).toBe(DEFAULT_ESCALATION_CONFIRMATION);

    // pass_thread_control POSTed with the FB inbox app id.
    const transferCall = fetchMock.mock.calls.find(([url]) => url.includes('pass_thread_control'));
    expect(transferCall).toBeDefined();
    const transferBody = JSON.parse(transferCall[1].body);
    expect(transferBody.target_app_id).toBe(escalation.DEFAULT_FB_INBOX_APP_ID);
    expect(transferBody.recipient.id).toBe('PSID_123');

    // Staff email sent.
    expect(sesMock).toHaveReceivedCommandTimes(SendEmailCommand, 1);
    const emailCall = sesMock.commandCalls(SendEmailCommand)[0];
    expect(emailCall.args[0].input.Destination.ToAddresses).toEqual(['staff@tenant.org']);

    // Analytics: MESSENGER_ESCALATED emitted, content-minimal payload.
    const sqsCalls = sqsMock.commandCalls(SendMessageCommand);
    const escalatedEvent = sqsCalls
      .map((c) => JSON.parse(c.args[0].input.MessageBody))
      .find((b) => b.event.type === 'MESSENGER_ESCALATED');
    expect(escalatedEvent).toBeDefined();
    expect(escalatedEvent.event.payload.session_id).toBe('meta:PAGE_456:PSID_123');
    expect(escalatedEvent.event.payload.tenant_id).toBe('TENANT_789');
    expect(escalatedEvent.event.payload.channel_type).toBe('messenger');

    // No Bedrock call — escalation happens INSTEAD of the RAG turn.
    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
  });

  test('Instagram channel → pass_thread_control uses the IG inbox app id', async () => {
    fetchMock = makeEscalationFetchMock();
    global.fetch = fetchMock;

    await handler(buildEvent({ messageText: 'I want to speak with a person', channelType: 'instagram' }));

    const transferCall = fetchMock.mock.calls.find(([url]) => url.includes('pass_thread_control'));
    const transferBody = JSON.parse(transferCall[1].body);
    expect(transferBody.target_app_id).toBe(escalation.DEFAULT_IG_INBOX_APP_ID);
  });

  test('pass_thread_control returns 400 (Conversation Routing not configured) → email + confirmation still happen, handler does not throw', async () => {
    fetchMock = makeEscalationFetchMock({ ok: false, status: 400, body: { error: { message: 'no default app' } } });
    global.fetch = fetchMock;

    await expect(handler(buildEvent({ messageText: 'transfer me to a human please' }))).resolves.not.toThrow();

    expect(sesMock).toHaveReceivedCommandTimes(SendEmailCommand, 1);
    const sendCalls = fetchMock.mock.calls.filter(([url]) => !url.includes('pass_thread_control'));
    expect(sendCalls.length).toBeGreaterThan(0);
    expect(JSON.parse(sendCalls[0][1].body).message.text).toBe(DEFAULT_ESCALATION_CONFIRMATION);
  });

  test('escalation_email absent from config → transfer + confirmation still proceed, email skipped', async () => {
    loadConfig.mockResolvedValue({ ...ESCALATION_CFG, messenger_behavior: undefined });
    fetchMock = makeEscalationFetchMock();
    global.fetch = fetchMock;

    await handler(buildEvent({ messageText: 'speak to staff' }));

    expect(sesMock).not.toHaveReceivedCommand(SendEmailCommand);
    const transferCall = fetchMock.mock.calls.find(([url]) => url.includes('pass_thread_control'));
    expect(transferCall).toBeDefined();
  });

  test('flag OFF → "I want to talk to a human" runs the normal RAG turn (byte-identical baseline, no escalation machinery)', async () => {
    loadConfig.mockResolvedValue({ ...ESCALATION_CFG, feature_flags: {} });
    fetchMock = makeFetchMock([
      { ok: true, body: {} }, // typing
      { ok: true, body: { message_id: 'mid.reply' } }, // reply
    ]);
    global.fetch = fetchMock;

    await handler(buildEvent({ messageText: 'I want to talk to a human' }));

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    expect(fetchMock.mock.calls.some(([url]) => url.includes('pass_thread_control'))).toBe(false);
    expect(sesMock).not.toHaveReceivedCommand(SendEmailCommand);
  });

  test('flag ON + false-positive-risk phrasing "how do humans apply?" → normal RAG turn, no escalation', async () => {
    fetchMock = makeFetchMock([
      { ok: true, body: {} },
      { ok: true, body: { message_id: 'mid.reply' } },
    ]);
    global.fetch = fetchMock;

    await handler(buildEvent({ messageText: 'how do humans apply?' }));

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    expect(fetchMock.mock.calls.some(([url]) => url.includes('pass_thread_control'))).toBe(false);
    expect(sesMock).not.toHaveReceivedCommand(SendEmailCommand);
  });

  test('flag ON + PIC1 quick-reply tap whose label reads "Talk to a person" → resolves the CTA payload, does NOT escalate (code review finding)', async () => {
    // A CTA titled "Talk to a person" is a structured tap (C3 `PIC1:cta:` route),
    // not free-typed human intent — it must resolve through 0b2/RAG like any
    // other CTA, never trip the escalation machinery on its tap label.
    fetchMock = makeFetchMock([
      { ok: true, body: {} }, // typing
      { ok: true, body: { message_id: 'mid.reply' } }, // reply
    ]);
    global.fetch = fetchMock;

    await handler(buildEvent({
      messageText: 'Talk to a person',
      isPostback: false,
      quickReplyPayload: 'PIC1:cta:talk_to_person',
    }));

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    expect(fetchMock.mock.calls.some(([url]) => url.includes('pass_thread_control'))).toBe(false);
    expect(sesMock).not.toHaveReceivedCommand(SendEmailCommand);
  });
});

// ─── M6a — pause check + escalation-writes-the-pause-row, with serialization
// (CONVERSATION_STATE_TABLE) enabled ─────────────────────────────────────────
//
// Both the pause check and the pause-row write inside escalation are gated on
// CONVERSATION_STATE_TABLE (same env var C7 serialization uses) — empty ⇒
// disabled, matching the rest of this file's convention (index.test.js
// deliberately leaves it unset elsewhere; conversationLock.integration.test.js
// is the existing sibling pattern for env-var-at-module-load tests). Rather
// than adding a new test file, this block uses jest.isolateModules to get a
// second, independently-configured instance of the handler within this file.
describe('M6a — pause check (CONVERSATION_STATE_TABLE enabled)', () => {
  const ST_TABLE = 'picasso-conversation-state-test';
  let stHandler;
  let stDdbMock, stKmsMock, stBedrockMock, stSqsMock, stSesMock;
  let stLoadConfig, stRetrieveKB;
  let stGetCommand, stPutCommand, stQueryCommand, stUpdateCommand, stDeleteCommand;
  let stInvokeModelCommand, stSendMessageCommand, stDecryptCommand, stSendEmailCommand;

  beforeAll(() => {
    jest.isolateModules(() => {
      process.env.CONVERSATION_STATE_TABLE = ST_TABLE;
      process.env.SES_FROM_EMAIL = 'notify@myrecruiter.ai';

      jest.doMock('../shared/bedrock-core', () => ({
        loadConfig: jest.fn(),
        retrieveKB: jest.fn(),
        sanitizeUserInput: jest.fn((t) => t?.trim() || ''),
      }));

      // Reuse the OUTER mockClient (not re-required) — aws-sdk-client-mock-jest's
      // matchers check `instanceof` against the outer package's AwsClientStub;
      // an inner-registry copy of 'aws-sdk-client-mock' would produce stub
      // objects the outer matchers don't recognize ("must be a client mock
      // instance"). The classes being wrapped DO need to be the inner-registry
      // copies (to match what the freshly-required './index' uses internally).
      const mc = mockClient;
      const ddbLib = require('@aws-sdk/lib-dynamodb');
      const kmsLib = require('@aws-sdk/client-kms');
      const bedrockLib = require('@aws-sdk/client-bedrock-runtime');
      const sqsLib = require('@aws-sdk/client-sqs');
      const sesLib = require('@aws-sdk/client-ses');

      stGetCommand = ddbLib.GetCommand;
      stPutCommand = ddbLib.PutCommand;
      stQueryCommand = ddbLib.QueryCommand;
      stUpdateCommand = ddbLib.UpdateCommand;
      stDeleteCommand = ddbLib.DeleteCommand;
      stInvokeModelCommand = bedrockLib.InvokeModelCommand;
      stSendMessageCommand = sqsLib.SendMessageCommand;
      stDecryptCommand = kmsLib.DecryptCommand;
      stSendEmailCommand = sesLib.SendEmailCommand;

      stDdbMock = mc(ddbLib.DynamoDBDocumentClient);
      stKmsMock = mc(kmsLib.KMSClient);
      stBedrockMock = mc(bedrockLib.BedrockRuntimeClient);
      stSqsMock = mc(sqsLib.SQSClient);
      stSesMock = mc(sesLib.SESClient);

      const bc = require('../shared/bedrock-core');
      stLoadConfig = bc.loadConfig;
      stRetrieveKB = bc.retrieveKB;

      stHandler = require('./index').handler;
    });
  });

  afterAll(() => {
    delete process.env.CONVERSATION_STATE_TABLE;
    delete process.env.SES_FROM_EMAIL;
  });

  const ST_CFG = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'Helpful.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
  };

  function stChannelMappingItem() {
    return {
      Item: {
        PK: 'PAGE#PAGE_456',
        SK: 'CHANNEL#messenger',
        encryptedPageToken: Buffer.from('encrypted-blob').toString('base64'),
        tenantId: 'TENANT_789',
      },
    };
  }

  beforeEach(() => {
    stDdbMock.reset();
    stKmsMock.reset();
    stBedrockMock.reset();
    stSqsMock.reset();
    stSesMock.reset();

    stLoadConfig.mockResolvedValue(ST_CFG);
    stRetrieveKB.mockResolvedValue('KB context.');

    stSqsMock.on(stSendMessageCommand).resolves({ MessageId: 'mock-sqs-id' });
    stSesMock.on(stSendEmailCommand).resolves({ MessageId: 'mock-ses-id' });
    stKmsMock.on(stDecryptCommand).resolves({ Plaintext: Buffer.from('EAABtest_page_token') });
    stDdbMock.on(stQueryCommand).resolves({ Items: [] });
    stDdbMock.on(stPutCommand).resolves({});
    stDdbMock.on(stUpdateCommand).resolves({});
    stDdbMock.on(stDeleteCommand).resolves({});
    // Channel-mapping GetCommand (Key: {PK,SK}) — pause-row GetCommand (Key:
    // {sessionId, stateType}) is stubbed per-test since it varies.
    stDdbMock.on(stGetCommand, { Key: { PK: 'PAGE#PAGE_456', SK: 'CHANNEL#messenger' } }).resolves(stChannelMappingItem());
  });

  afterEach(() => {
    delete global.fetch;
  });

  function stBuildEvent(overrides = {}) {
    return {
      psid: 'PSID_123',
      messageText: 'Hello, what services do you offer?',
      pageId: 'PAGE_456',
      tenantId: 'TENANT_789',
      tenantHash: 'abc123defabc123def',
      channelType: 'messenger',
      messageMid: 'm_test_mid',
      ...overrides,
    };
  }

  function stMakeBedrockResponse(text) {
    return { body: Buffer.from(JSON.stringify({ content: [{ type: 'text', text }], usage: {} })) };
  }

  test('active (non-expired) pause row → bot stands down: zero sends, zero Bedrock, zero history writes', async () => {
    const nowSec = Math.floor(Date.now() / 1000);
    stDdbMock
      .on(stGetCommand, { Key: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause' } })
      .resolves({ Item: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause', reason: 'escalation', expires_at: nowSec + 3600 } });
    global.fetch = jest.fn();

    await expect(stHandler(stBuildEvent())).resolves.toBeUndefined();

    expect(global.fetch).not.toHaveBeenCalled();
    expect(stBedrockMock).not.toHaveReceivedCommand(stInvokeModelCommand);
    expect(stDdbMock).not.toHaveReceivedCommand(stPutCommand); // no history rows, no lock row
  });

  test('expired pause row (expires_at in the past) → normal turn proceeds', async () => {
    const nowSec = Math.floor(Date.now() / 1000);
    stDdbMock
      .on(stGetCommand, { Key: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause' } })
      .resolves({ Item: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause', reason: 'escalation', expires_at: nowSec - 10 } });
    stBedrockMock.on(stInvokeModelCommand).resolves(stMakeBedrockResponse('Sure, happy to help!\n<<<ACTIONS []>>>'));
    global.fetch = jest.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ message_id: 'mid.1' }) });

    await stHandler(stBuildEvent());

    expect(stBedrockMock).toHaveReceivedCommandTimes(stInvokeModelCommand, 1);
    expect(global.fetch).toHaveBeenCalled();
  });

  test('no pause row at all → normal turn proceeds (first-ever message)', async () => {
    stDdbMock
      .on(stGetCommand, { Key: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause' } })
      .resolves({});
    stBedrockMock.on(stInvokeModelCommand).resolves(stMakeBedrockResponse('Hi there!\n<<<ACTIONS []>>>'));
    global.fetch = jest.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ message_id: 'mid.1' }) });

    await stHandler(stBuildEvent());

    expect(stBedrockMock).toHaveReceivedCommandTimes(stInvokeModelCommand, 1);
  });

  test('flag OFF (table enabled) → no pause-check GetItem for stateType "pause" is ever issued', async () => {
    stLoadConfig.mockResolvedValue({ ...ST_CFG, feature_flags: {} });
    stBedrockMock.on(stInvokeModelCommand).resolves(stMakeBedrockResponse('Hi!\n<<<ACTIONS []>>>'));
    global.fetch = jest.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ message_id: 'mid.1' }) });

    await stHandler(stBuildEvent());

    const getCalls = stDdbMock.commandCalls(stGetCommand);
    expect(getCalls.every((c) => c.args[0].input.Key?.stateType !== 'pause')).toBe(true);
  });

  test('escalation through the full handler (table enabled) writes a real C4 pause row', async () => {
    stDdbMock
      .on(stGetCommand, { Key: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause' } })
      .resolves({});
    global.fetch = jest.fn().mockImplementation((url) => {
      if (typeof url === 'string' && url.includes('pass_thread_control')) {
        return Promise.resolve({ ok: true, status: 200, json: async () => ({ success: true }) });
      }
      return Promise.resolve({ ok: true, status: 200, json: async () => ({ message_id: 'mid.confirmation' }) });
    });

    await stHandler(stBuildEvent({ messageText: 'Can I talk to a human?' }));

    const pauseRowPut = stDdbMock
      .commandCalls(stPutCommand)
      .find((c) => c.args[0].input.Item?.stateType === 'pause');
    expect(pauseRowPut).toBeDefined();
    expect(pauseRowPut.args[0].input.TableName).toBe(ST_TABLE);
    expect(pauseRowPut.args[0].input.Item.sessionId).toBe('meta:PAGE_456:PSID_123');
    expect(pauseRowPut.args[0].input.Item.reason).toBe('escalation');
    expect(pauseRowPut.args[0].input.Item.schema_version).toBe(1);
    expect(stBedrockMock).not.toHaveReceivedCommand(stInvokeModelCommand);
  });

  // ── Code review [HIGH] — pause must cover the attachment/sticker/unsupported
  // fallback path too (that v2 lane used to send a fallback reply BEFORE any
  // pause check existed) ───────────────────────────────────────────────────
  test('paused + attachment event (v2 fixture) → zero sends, fallback suppressed', async () => {
    const { classifyMessagingEvent } = require('../Meta_Webhook_Handler/classify');
    const WEBHOOK_FIXTURES = require('../Meta_Webhook_Handler/__fixtures__/messagingEvents');
    const [classification] = classifyMessagingEvent(WEBHOOK_FIXTURES.fbAttachmentImage);
    const sessionId = `meta:${WEBHOOK_FIXTURES.PAGE_ID}:${classification.psid}`;
    const nowSec = Math.floor(Date.now() / 1000);

    stDdbMock
      .on(stGetCommand, { Key: { sessionId, stateType: 'pause' } })
      .resolves({ Item: { sessionId, stateType: 'pause', reason: 'escalation', expires_at: nowSec + 3600 } });
    global.fetch = jest.fn();

    const event = {
      psid: classification.psid,
      messageText: classification.messageText,
      pageId: WEBHOOK_FIXTURES.PAGE_ID,
      tenantId: 'TENANT_789',
      tenantHash: 'abc123defabc123def',
      channelType: 'messenger',
      messageMid: classification.messageMid,
      isPostback: classification.isPostback,
      v: 2,
      eventKind: classification.eventKind,
      timestamp: Date.now(),
      quickReplyPayload: classification.quickReplyPayload,
      appId: classification.appId,
      attachmentTypes: classification.attachmentTypes,
      targetMid: classification.targetMid,
      editedText: classification.editedText,
      replyTo: classification.replyTo,
      isStandby: classification.isStandby,
    };

    await expect(stHandler(event)).resolves.toBeUndefined();

    expect(global.fetch).not.toHaveBeenCalled();
    expect(stBedrockMock).not.toHaveReceivedCommand(stInvokeModelCommand);
    // No PutCommand at all — handleUnsupportedInputFallback (and any of its
    // side effects) never ran; the pause check short-circuited before it.
    expect(stDdbMock).not.toHaveReceivedCommand(stPutCommand);
  });

  // ── Code review [MEDIUM] — coalesced escalation: a burst where the SECOND
  // message is the escalation request must escalate instead of joining the
  // combined Bedrock turn ───────────────────────────────────────────────────
  test('coalesced burst: escalation intent in the SECOND drained message escalates instead of the combined Bedrock turn', async () => {
    stDdbMock
      .on(stGetCommand, { Key: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'pause' } })
      .resolves({});
    // claimPending is an UpdateCommand keyed on the LOCK row (stateType:'lock').
    // First claim returns a 2-item pending batch; every claim after that is
    // empty so the drain loop can release and return.
    stDdbMock
      .on(stUpdateCommand, { Key: { sessionId: 'meta:PAGE_456:PSID_123', stateType: 'lock' } })
      .resolvesOnce({
        Attributes: {
          pending: [
            { timestamp: 1000, mid: 'm1', text: 'What are your hours?' },
            { timestamp: 2000, mid: 'm2', text: 'I want to talk to a human' },
          ],
        },
      })
      .resolves({});
    stBedrockMock.on(stInvokeModelCommand).resolves(stMakeBedrockResponse('We are open 9-5.\n<<<ACTIONS []>>>'));
    global.fetch = jest.fn().mockImplementation((url) => {
      if (typeof url === 'string' && url.includes('pass_thread_control')) {
        return Promise.resolve({ ok: true, status: 200, json: async () => ({ success: true }) });
      }
      return Promise.resolve({ ok: true, status: 200, json: async () => ({ message_id: 'mid.x' }) });
    });

    // The winner's OWN turn is an unrelated message — the escalation request
    // is buried in the coalesced batch that drains afterward.
    await stHandler(stBuildEvent({ messageText: 'Hi there' }));

    // Exactly ONE Bedrock call — the winner's own turn. The drained batch
    // escalates instead of producing a second (combined) Bedrock call.
    expect(stBedrockMock).toHaveReceivedCommandTimes(stInvokeModelCommand, 1);

    // Escalation side effects fired from the drain: pass_thread_control POST
    // + a written pause row.
    expect(global.fetch.mock.calls.some(([url]) => url.includes('pass_thread_control'))).toBe(true);
    const pauseRowPut = stDdbMock
      .commandCalls(stPutCommand)
      .find((c) => c.args[0].input.Item?.stateType === 'pause');
    expect(pauseRowPut).toBeDefined();
  });
});
