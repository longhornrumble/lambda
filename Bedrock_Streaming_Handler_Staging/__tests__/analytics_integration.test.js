/**
 * Coverage gap tests — Issue #5 PR A
 *
 * Gap 1: handleAnalyticsEvent no-op guard (ANALYTICS_QUEUE_URL unset)
 * Gap 3: writeSessionSummary calls in the buffered handler path
 *
 * Both use jest.isolateModules + jest.doMock to control the ANALYTICS_QUEUE_URL
 * constant captured at module-load time in index.js.
 *
 * NOTE: aws-sdk-client-mock patches are not effective inside jest.isolateModules
 * because each isolated registry gets its own copy of the AWS SDK constructor.
 * All AWS SDK clients used within isolated modules are mocked via jest.doMock
 * with hand-rolled factories that return controllable spy functions.
 */

// prompt_v4 is not mocked in the isolated registries — its version constants are
// plain deterministic strings, so the top-level require yields the same values
// index.js stamps into the QA_COMPLETE log (sub-phase 1.1 assertion below).
const {
  V4_CONVERSATION_PROMPT_VERSION,
  ACTION_SELECTOR_PROMPT_VERSION,
} = require('../prompt_v4');

// ─────────────────────────────────────────────────────────────────────
// Gap 1: handleAnalyticsEvent no-op guard
// ─────────────────────────────────────────────────────────────────────

describe('handleAnalyticsEvent — no-op guard (ANALYTICS_QUEUE_URL unset)', () => {
  let handler;
  let sqsSendMock;

  beforeAll(() => {
    jest.isolateModules(() => {
      delete process.env.ANALYTICS_QUEUE_URL;

      sqsSendMock = jest.fn().mockResolvedValue({ MessageId: 'test' });

      jest.doMock('@aws-sdk/client-sqs', () => ({
        SQSClient: jest.fn(function () { this.send = sqsSendMock; }),
        SendMessageCommand: jest.fn(),
        SendMessageBatchCommand: jest.fn(),
      }));
      jest.doMock('../../shared/bedrock-core', () => ({
        loadConfig: jest.fn().mockResolvedValue(null),
        retrieveKB: jest.fn().mockResolvedValue(''),
        sanitizeUserInput: jest.fn((x) => x),
        getCacheKey: jest.fn(),
        isCacheValid: jest.fn(),
        evictOldestCacheEntries: jest.fn(),
        CACHE_TTL: 300000,
        MAX_CACHE_SIZE: 100,
      }));
      jest.doMock('../form_handler', () => ({ handleFormMode: jest.fn() }));
      jest.doMock('../response_enhancer', () => ({ enhanceResponse: jest.fn() }));
      jest.doMock('../analytics_writer', () => ({
        writeSessionSummary: jest.fn().mockResolvedValue(true),
      }));
      jest.doMock('@aws-sdk/client-bedrock-runtime', () => ({
        BedrockRuntimeClient: jest.fn(function () { this.send = jest.fn(); }),
        InvokeModelWithResponseStreamCommand: jest.fn(),
      }));

      const mod = require('../index.js');
      handler = mod.handler;
    });
  });

  afterAll(() => {
    jest.resetModules();
  });

  beforeEach(() => {
    sqsSendMock && sqsSendMock.mockClear();
  });

  test('returns noop status when ANALYTICS_QUEUE_URL is not set', async () => {
    const event = {
      queryStringParameters: { action: 'analytics' },
      body: JSON.stringify({
        session_id: 'sess_abc',
        event: { type: 'MESSAGE_SENT', payload: {} },
      }),
    };

    const result = await handler(event, {}, {});

    expect(result.statusCode).toBe(200);
    const body = JSON.parse(result.body);
    expect(body.status).toBe('noop');
    expect(body.reason).toBe('analytics_queue_not_configured');
  });

  test('SQS send is never called when ANALYTICS_QUEUE_URL is not set', async () => {
    const event = {
      queryStringParameters: { action: 'analytics' },
      body: JSON.stringify({
        session_id: 'sess_abc',
        event: { type: 'MESSAGE_SENT', payload: {} },
      }),
    };

    await handler(event, {}, {});

    expect(sqsSendMock).not.toHaveBeenCalled();
  });
});

describe('handleAnalyticsEvent — SQS path runs when ANALYTICS_QUEUE_URL is set (regression guard)', () => {
  let handler;
  let sqsSendMock;

  beforeAll(() => {
    jest.isolateModules(() => {
      process.env.ANALYTICS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123/test-queue';

      sqsSendMock = jest.fn().mockResolvedValue({ MessageId: 'test-msg-id' });

      jest.doMock('@aws-sdk/client-sqs', () => ({
        SQSClient: jest.fn(function () { this.send = sqsSendMock; }),
        SendMessageCommand: jest.fn(function (params) { this.input = params; }),
        SendMessageBatchCommand: jest.fn(function (params) { this.input = params; }),
      }));
      jest.doMock('../../shared/bedrock-core', () => ({
        loadConfig: jest.fn().mockResolvedValue(null),
        retrieveKB: jest.fn().mockResolvedValue(''),
        sanitizeUserInput: jest.fn((x) => x),
        getCacheKey: jest.fn(),
        isCacheValid: jest.fn(),
        evictOldestCacheEntries: jest.fn(),
        CACHE_TTL: 300000,
        MAX_CACHE_SIZE: 100,
      }));
      jest.doMock('../form_handler', () => ({ handleFormMode: jest.fn() }));
      jest.doMock('../response_enhancer', () => ({ enhanceResponse: jest.fn() }));
      jest.doMock('../analytics_writer', () => ({
        writeSessionSummary: jest.fn().mockResolvedValue(true),
      }));
      jest.doMock('@aws-sdk/client-bedrock-runtime', () => ({
        BedrockRuntimeClient: jest.fn(function () { this.send = jest.fn(); }),
        InvokeModelWithResponseStreamCommand: jest.fn(),
      }));

      const mod = require('../index.js');
      handler = mod.handler;
    });
  });

  afterAll(() => {
    delete process.env.ANALYTICS_QUEUE_URL;
    jest.resetModules();
  });

  beforeEach(() => {
    sqsSendMock && sqsSendMock.mockClear();
  });

  test('single event: returns success and calls SQS send once', async () => {
    const event = {
      queryStringParameters: { action: 'analytics' },
      body: JSON.stringify({
        session_id: 'sess_abc',
        event: { type: 'MESSAGE_SENT', payload: {} },
      }),
    };

    const result = await handler(event, {}, {});

    expect(result.statusCode).toBe(200);
    const body = JSON.parse(result.body);
    expect(body.status).toBe('success');
    expect(sqsSendMock).toHaveBeenCalledTimes(1);
  });
});

// ─────────────────────────────────────────────────────────────────────
// Gap 3: writeSessionSummary calls in the buffered handler path
//
// The buffered handler (bufferedHandler) runs when awslambda.streamifyResponse
// is absent. In the Jest environment this is the default since no global
// awslambda is defined in this isolated module context. The buffered handler
// is what drives the writeSessionSummary calls we test here.
// This is documented intentionally per the task brief.
// ─────────────────────────────────────────────────────────────────────

describe('Gap 3 — buffered handler: writeSessionSummary called after QA_COMPLETE', () => {
  let handler;
  let writeSessionSummaryMock;
  let bedrockSendMock;

  const minimalConfig = {
    tenant_id: 'TEST123',
    aws: {
      knowledge_base_id: 'KB123',
      model_id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0',
    },
    streaming: { max_tokens: 500, temperature: 0 },
    tone_prompt: 'You are a helpful assistant.',
  };

  // Build a fake Bedrock response stream that emits text then message_stop
  function makeFakeBedrockResponse(textChunks) {
    const events = [
      { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'content_block_start' })) } },
      ...textChunks.map((text) => ({
        chunk: {
          bytes: Buffer.from(
            JSON.stringify({ type: 'content_block_delta', delta: { type: 'text_delta', text } })
          ),
        },
      })),
      { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'message_stop' })) } },
    ];
    return {
      body: {
        [Symbol.asyncIterator]: async function* () {
          for (const ev of events) yield ev;
        },
      },
    };
  }

  beforeAll(() => {
    jest.isolateModules(() => {
      delete process.env.ANALYTICS_QUEUE_URL;

      writeSessionSummaryMock = jest.fn().mockResolvedValue(true);
      bedrockSendMock = jest.fn().mockResolvedValue(makeFakeBedrockResponse(['Hello', ' world']));

      jest.doMock('@aws-sdk/client-sqs', () => ({
        SQSClient: jest.fn(function () { this.send = jest.fn(); }),
        SendMessageCommand: jest.fn(),
        SendMessageBatchCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-bedrock-runtime', () => ({
        BedrockRuntimeClient: jest.fn(function () { this.send = bedrockSendMock; }),
        InvokeModelWithResponseStreamCommand: jest.fn(function (p) { this.input = p; }),
      }));
      jest.doMock('../../shared/bedrock-core', () => ({
        loadConfig: jest.fn().mockResolvedValue(minimalConfig),
        retrieveKB: jest.fn().mockResolvedValue('Knowledge base context.'),
        sanitizeUserInput: jest.fn((x) => x),
        getCacheKey: jest.fn(),
        isCacheValid: jest.fn(),
        evictOldestCacheEntries: jest.fn(),
        CACHE_TTL: 300000,
        MAX_CACHE_SIZE: 100,
      }));
      jest.doMock('../form_handler', () => ({ handleFormMode: jest.fn() }));
      jest.doMock('../response_enhancer', () => ({
        enhanceResponse: jest.fn().mockResolvedValue({ message: '', ctaButtons: [], metadata: {} }),
      }));
      jest.doMock('../analytics_writer', () => ({
        writeSessionSummary: writeSessionSummaryMock,
      }));

      const mod = require('../index.js');
      handler = mod.handler;
    });
  });

  afterAll(() => {
    jest.resetModules();
  });

  beforeEach(() => {
    writeSessionSummaryMock.mockClear();
    bedrockSendMock.mockClear();
    bedrockSendMock.mockResolvedValue(makeFakeBedrockResponse(['Hello', ' world']));
  });

  test('writeSessionSummary called twice — MESSAGE_SENT then MESSAGE_RECEIVED', async () => {
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: 'How do I apply?',
        session_id: 'sess_test_123',
        client_timestamp: '2026-05-04T20:00:00.000Z',
      }),
    };

    await handler(event, { awsRequestId: 'req-unit-test-001' });

    expect(writeSessionSummaryMock).toHaveBeenCalledTimes(2);

    const eventTypes = writeSessionSummaryMock.mock.calls.map((c) => c[0].event_type);
    expect(eventTypes).toContain('MESSAGE_SENT');
    expect(eventTypes).toContain('MESSAGE_RECEIVED');
  });

  test('MESSAGE_SENT call includes first_question in event_payload', async () => {
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: 'What programs do you offer?',
        session_id: 'sess_test_456',
        client_timestamp: '2026-05-04T20:00:00.000Z',
      }),
    };

    await handler(event, { awsRequestId: 'req-unit-test-002' });

    const sentCall = writeSessionSummaryMock.mock.calls.find(
      (c) => c[0].event_type === 'MESSAGE_SENT'
    );
    expect(sentCall).toBeDefined();
    expect(sentCall[0].event_payload.first_question).toBeDefined();
    expect(typeof sentCall[0].event_payload.first_question).toBe('string');
  });

  test('MESSAGE_RECEIVED call includes response_time_ms in event_payload', async () => {
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: 'Tell me about volunteering.',
        session_id: 'sess_test_789',
        client_timestamp: '2026-05-04T20:00:00.000Z',
      }),
    };

    await handler(event, { awsRequestId: 'req-unit-test-003' });

    const receivedCall = writeSessionSummaryMock.mock.calls.find(
      (c) => c[0].event_type === 'MESSAGE_RECEIVED'
    );
    expect(receivedCall).toBeDefined();
    expect(receivedCall[0].event_payload).toHaveProperty('response_time_ms');
  });

  test('client_timestamp falls back to startTime ISO string when body.client_timestamp is absent', async () => {
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: 'No timestamp test.',
        session_id: 'sess_no_ts',
        // client_timestamp intentionally omitted
      }),
    };

    await handler(event, { awsRequestId: 'req-unit-test-004' });

    const sentCall = writeSessionSummaryMock.mock.calls.find(
      (c) => c[0].event_type === 'MESSAGE_SENT'
    );
    expect(sentCall).toBeDefined();
    // Should be a valid ISO string (fallback from startTime)
    expect(() => new Date(sentCall[0].client_timestamp).toISOString()).not.toThrow();
  });

  test('request_id is set from context.awsRequestId', async () => {
    // bufferedHandler signature is (event, context) — context is second arg.
    // (streamifyResponse is absent in isolated modules, so the export IS bufferedHandler.)
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: 'Request id test.',
        session_id: 'sess_req_id',
        client_timestamp: '2026-05-04T20:00:00.000Z',
      }),
    };

    await handler(event, { awsRequestId: 'req-specific-id-xyz' });

    for (const call of writeSessionSummaryMock.mock.calls) {
      expect(call[0].request_id).toBe('req-specific-id-xyz');
    }
  });

  // ─── Review B1: QA_COMPLETE log must redact email + phone ───
  // Note: global.console.log is replaced by setup.js with a persistent jest.fn()
  // that accumulates calls across all tests. Must mockClear() before handler()
  // and read mock.calls directly — DO NOT spyOn (returns same mock, doesn't reset).
  test('QA_COMPLETE structured log redacts PII from user question + bot answer', async () => {
    const piiInput = 'Contact me at jane.doe@example.com or call (512) 555-1234.';
    bedrockSendMock.mockResolvedValueOnce(
      makeFakeBedrockResponse(['Sure, I will email ', 'jane.doe@example.com', ' shortly.'])
    );

    console.log.mockClear();
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: piiInput,
        session_id: 'sess_pii_test',
        client_timestamp: '2026-05-04T20:00:00.000Z',
      }),
    };

    await handler(event, { awsRequestId: 'req-pii-1' });

    const qaLog = console.log.mock.calls
      .map((c) => c[0])
      .find((s) => typeof s === 'string' && s.includes('"QA_COMPLETE"'));

    expect(qaLog).toBeDefined();
    const parsed = JSON.parse(qaLog);
    expect(parsed.question).not.toMatch(/jane\.doe@example\.com/);
    expect(parsed.question).not.toMatch(/512.{0,2}555.{0,2}1234/);
    expect(parsed.question).toMatch(/\[EMAIL\]/);
    expect(parsed.question).toMatch(/\[PHONE\]/);
    // Bedrock answer (Bedrock-generated) can echo prompt PII — also redacted
    expect(parsed.answer).not.toMatch(/jane\.doe@example\.com/);
    expect(parsed.answer).toMatch(/\[EMAIL\]/);
  });

  // ─── Sub-phase 1.1: QA_COMPLETE log stamps both prompt versions ───
  // Eval baselines key on the prompt text version a response was produced under.
  test('QA_COMPLETE structured log stamps both prompt versions', async () => {
    console.log.mockClear();
    const event = {
      body: JSON.stringify({
        tenant_hash: 'my87674d777bf9',
        user_input: 'What programs do you offer?',
        session_id: 'sess_prompt_version',
        client_timestamp: '2026-05-04T20:00:00.000Z',
      }),
    };

    await handler(event, { awsRequestId: 'req-prompt-version-1' });

    const qaLog = console.log.mock.calls
      .map((c) => c[0])
      .find((s) => typeof s === 'string' && s.includes('"QA_COMPLETE"'));

    expect(qaLog).toBeDefined();
    const parsed = JSON.parse(qaLog);
    expect(parsed.prompt_versions).toEqual({
      conversation: V4_CONVERSATION_PROMPT_VERSION,
      action_selector: ACTION_SELECTOR_PROMPT_VERSION,
    });
  });
});
