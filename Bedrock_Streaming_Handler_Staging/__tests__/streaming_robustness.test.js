/**
 * Streaming-robustness tests (CS1 + CS4).
 *
 * CS1: the Bedrock streaming client is constructed with connect/request
 *      timeouts AND throwOnRequestTimeout, so a hang fails fast instead of
 *      holding the SSE stream open until the Lambda timeout.
 * CS4: a malformed request body produces the handler's error contract (SSE
 *      error frame for the streaming handler, a 400 for the buffered handler)
 *      instead of an unhandled throw that bypasses it.
 *
 * Harness mirrors cf_origin_wiring.test.js (awslambda global + mocked deps).
 */

const { mockClient } = require('aws-sdk-client-mock');
const { BedrockRuntimeClient } = require('@aws-sdk/client-bedrock-runtime');

jest.mock('../cf-origin-validator', () => ({
  validateCfOriginHeader: jest.fn().mockResolvedValue({ valid: true, reason: null }),
}));

jest.mock('../../shared/bedrock-core', () => ({
  loadConfig: jest.fn(),
  retrieveKB: jest.fn(),
  sanitizeUserInput: jest.fn((input) => input),
  getCacheKey: jest.fn(),
  isCacheValid: jest.fn(),
  evictOldestCacheEntries: jest.fn(),
  CACHE_TTL: 300000,
  MAX_CACHE_SIZE: 100,
}));

jest.mock('../form_handler', () => ({ handleFormMode: jest.fn() }));
jest.mock('../response_enhancer', () => ({ enhanceResponse: jest.fn() }));

mockClient(BedrockRuntimeClient);

global.awslambda = {
  streamifyResponse: jest.fn((handler) => async (event, responseStream, context) => {
    return handler(event, responseStream, context);
  }),
};

const { validateCfOriginHeader } = require('../cf-origin-validator');
const { loadConfig, retrieveKB } = require('../../shared/bedrock-core');

function createMockResponseStream() {
  const chunks = [];
  let ended = false;
  return {
    write: jest.fn((data) => chunks.push(data)),
    end: jest.fn((data) => {
      if (data !== undefined) chunks.push(data);
      ended = true;
    }),
    getChunks: () => chunks,
    isEnded: () => ended,
  };
}

describe('CS1 — Bedrock streaming client timeout contract', () => {
  let indexModule;
  beforeAll(() => {
    indexModule = require('../index.js');
  });

  test('BEDROCK_STREAM_TIMEOUTS sets connect + request timeouts and enables abort', () => {
    const t = indexModule.BEDROCK_STREAM_TIMEOUTS;
    expect(t).toBeDefined();
    expect(t.connectionTimeout).toBeGreaterThan(0);
    expect(t.requestTimeout).toBeGreaterThan(0);
    // Must sit below the 300s Lambda timeout to be useful.
    expect(t.requestTimeout).toBeLessThan(300000);
    // Without this the timeout only WARNS and never aborts — the exact CS1 trap.
    expect(t.throwOnRequestTimeout).toBe(true);
  });
});

describe('CS4 — streamingHandler malformed body', () => {
  let indexModule;
  beforeAll(() => {
    indexModule = require('../index.js');
  });

  beforeEach(() => {
    validateCfOriginHeader.mockReset();
    loadConfig.mockReset();
    retrieveKB.mockReset();
    validateCfOriginHeader.mockResolvedValue({ valid: true, reason: null });
  });

  test('emits SSE error contract and does NOT reject or dispatch downstream', async () => {
    const event = { body: '{ this is not valid json ' };
    const responseStream = createMockResponseStream();

    // Pre-fix this threw synchronously in the handler → the wrapped promise
    // rejected. Post-fix it resolves after writing the error contract.
    await expect(
      indexModule.handler(event, responseStream, {})
    ).resolves.toBeUndefined();

    const chunks = responseStream.getChunks();
    expect(chunks.some((c) => typeof c === 'string' && c.includes('"type":"error"'))).toBe(true);
    expect(chunks.some((c) => typeof c === 'string' && c.includes('[DONE]'))).toBe(true);
    expect(responseStream.isEnded()).toBe(true);
    // Never reached config/KB with an unparseable request.
    expect(loadConfig).not.toHaveBeenCalled();
    expect(retrieveKB).not.toHaveBeenCalled();
  });

  test('a well-formed body still passes the parse (no false positive)', async () => {
    loadConfig.mockResolvedValue(null); // early-return after parse; we only assert parse succeeded
    retrieveKB.mockResolvedValue('');
    const event = { body: JSON.stringify({ tenant_hash: 'abc', user_input: 'hi' }) };
    const responseStream = createMockResponseStream();

    await indexModule.handler(event, responseStream, {});

    const chunks = responseStream.getChunks();
    // No "Invalid request body" error — the parse accepted valid JSON.
    expect(chunks.some((c) => typeof c === 'string' && c.includes('Invalid request body'))).toBe(false);
  });
});

describe('CS4 — bufferedHandler malformed body', () => {
  test('returns a clean 400 instead of an unhandled throw', async () => {
    jest.resetModules();
    const prevAwsLambda = global.awslambda;
    global.awslambda = undefined; // force the buffered fallback export
    jest.mock('../cf-origin-validator', () => ({
      validateCfOriginHeader: jest.fn().mockResolvedValue({ valid: true, reason: null }),
    }));
    const mod = require('../index.js');

    const event = {
      body: '{ malformed',
      headers: { origin: 'https://chat.myrecruiter.ai' },
    };
    const result = await mod.handler(event, {});

    global.awslambda = prevAwsLambda;

    expect(result.statusCode).toBe(400);
    expect(result.headers['Content-Type']).toBe('application/json');
    expect(JSON.parse(result.body)).toHaveProperty('error');
  });
});
