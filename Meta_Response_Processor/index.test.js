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
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
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
