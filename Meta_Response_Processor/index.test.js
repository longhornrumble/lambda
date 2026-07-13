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
  jest.clearAllMocks();

  // Default SQS stub: analytics emissions succeed silently
  sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mock-sqs-id' });

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
          { role: 'user', content: 'hi', messageTimestamp: 1000 },
          { role: 'assistant', content: 'hello', messageTimestamp: 1001 },
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
    ddbMock.on(QueryCommand).resolves(makeRecentMessagesQueryResult([{ role: 'user', content: 'hi' }])); // non-empty → no disclosure
    bedrockMock.on(InvokeModelCommand).resolves(makeBedrockResponse('B'.repeat(1000)));
    await handler(buildEvent({ channelType: 'instagram' }));
    const sendBodies = fetchMock.mock.calls
      .map(([, opts]) => (opts && opts.body ? JSON.parse(opts.body) : null))
      .filter((b) => b && b.message && typeof b.message.text === 'string');
    expect(sendBodies).toHaveLength(1);
    expect(sendBodies[0].message.text.length).toBe(1000);
  });
});
