/**
 * Wiring test for cf-origin-validator into index.js.
 *
 * Asserts that BOTH the streaming handler (via the awslambda-wrapped
 * indexModule.handler) AND the buffered handler reject requests when
 * the validator returns invalid, BEFORE any route dispatch fires.
 *
 * The validator's own unit-test coverage lives in cf_origin_validator.test.js.
 * This test only verifies the call-site wiring.
 */

const { mockClient } = require('aws-sdk-client-mock');
const { BedrockRuntimeClient } = require('@aws-sdk/client-bedrock-runtime');

// Mock cf-origin-validator BEFORE index.js loads so the require resolves
// to the mock module.
jest.mock('../cf-origin-validator', () => ({
  validateCfOriginHeader: jest.fn().mockResolvedValue({ valid: true, reason: null }),
}));

// Mock bedrock-core (same shape as index.test.js)
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

// awslambda global — same shape as index.test.js (HttpResponseStream.from
// is left undefined so the streaming-handler 403 falls through to the
// responseStream.end(...) branch, which is what runs in test env)
global.awslambda = {
  streamifyResponse: jest.fn((handler) => async (event, responseStream, context) => {
    return handler(event, responseStream, context);
  }),
};

const { validateCfOriginHeader } = require('../cf-origin-validator');
const { loadConfig, retrieveKB } = require('../../shared/bedrock-core');

let indexModule;

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

describe('cf-origin-validator wiring / streamingHandler', () => {
  beforeAll(() => {
    indexModule = require('../index.js');
  });

  beforeEach(() => {
    validateCfOriginHeader.mockReset();
    loadConfig.mockReset();
    retrieveKB.mockReset();
  });

  test('admits request when validator returns valid (existing behavior preserved)', async () => {
    validateCfOriginHeader.mockResolvedValue({ valid: true, reason: null });
    loadConfig.mockResolvedValue(null); // forces early-return path; we only care that validator passed
    retrieveKB.mockResolvedValue('');

    const event = { body: JSON.stringify({ tenant_hash: 'abc', user_input: 'hi' }) };
    const responseStream = createMockResponseStream();

    await indexModule.handler(event, responseStream, {});

    expect(validateCfOriginHeader).toHaveBeenCalledTimes(1);
    expect(validateCfOriginHeader).toHaveBeenCalledWith(event);
    // Should NOT have emitted a forbidden response
    const chunks = responseStream.getChunks();
    const hasForbidden = chunks.some((c) => typeof c === 'string' && c.includes('forbidden'));
    expect(hasForbidden).toBe(false);
  });

  test('rejects with forbidden body when validator returns invalid', async () => {
    validateCfOriginHeader.mockResolvedValue({ valid: false, reason: 'missing CF origin header' });

    const event = { body: JSON.stringify({ tenant_hash: 'abc', user_input: 'hi' }) };
    const responseStream = createMockResponseStream();

    await indexModule.handler(event, responseStream, {});

    expect(validateCfOriginHeader).toHaveBeenCalledTimes(1);
    // Bedrock / KB should NOT be touched after rejection
    expect(loadConfig).not.toHaveBeenCalled();
    expect(retrieveKB).not.toHaveBeenCalled();
    // Stream should have ended with a forbidden body
    expect(responseStream.end).toHaveBeenCalled();
    const chunks = responseStream.getChunks();
    const forbiddenChunk = chunks.find(
      (c) => typeof c === 'string' && c.includes('forbidden')
    );
    expect(forbiddenChunk).toBeDefined();
  });

  test('runs validator BEFORE OPTIONS short-circuit (validator can reject OPTIONS attacks)', async () => {
    validateCfOriginHeader.mockResolvedValue({ valid: false, reason: 'missing CF origin header' });

    const event = {
      httpMethod: 'OPTIONS',
      body: JSON.stringify({}),
    };
    const responseStream = createMockResponseStream();

    await indexModule.handler(event, responseStream, {});

    expect(validateCfOriginHeader).toHaveBeenCalledTimes(1);
    // Stream still ends, but with forbidden body — NOT the empty-OPTIONS body
    const chunks = responseStream.getChunks();
    const forbiddenChunk = chunks.find(
      (c) => typeof c === 'string' && c.includes('forbidden')
    );
    expect(forbiddenChunk).toBeDefined();
  });
});

describe('cf-origin-validator wiring / bufferedHandler', () => {
  let bufferedExports;

  beforeAll(() => {
    // Force the buffered fallback by clearing awslambda before re-requiring.
    // The buffered path is exercised in non-streaming Lambda invocations.
    bufferedExports = require('../index.js');
  });

  beforeEach(() => {
    validateCfOriginHeader.mockReset();
  });

  test('rejects with 403 + quiet body when validator returns invalid (no CORS-header leak)', async () => {
    validateCfOriginHeader.mockResolvedValue({ valid: false, reason: 'missing CF origin header' });

    // Re-require with awslambda cleared so exports.handler resolves to bufferedHandler
    jest.resetModules();
    const prevAwsLambda = global.awslambda;
    global.awslambda = undefined;
    jest.mock('../cf-origin-validator', () => ({
      validateCfOriginHeader: jest.fn().mockResolvedValue({ valid: false, reason: 'missing CF origin header' }),
    }));
    const mod = require('../index.js');

    const event = {
      body: JSON.stringify({ tenant_hash: 'abc', user_input: 'hi' }),
      headers: { origin: 'https://chat.myrecruiter.ai' },
    };
    const result = await mod.handler(event, {});

    global.awslambda = prevAwsLambda;

    expect(result.statusCode).toBe(403);
    expect(result.headers['Content-Type']).toBe('application/json');
    // No CORS-header leak on the reject path (matches streamingHandler precedent)
    expect(result.headers).not.toHaveProperty('Access-Control-Allow-Origin');
    expect(result.headers).not.toHaveProperty('Access-Control-Allow-Methods');
    expect(result.headers).not.toHaveProperty('Access-Control-Allow-Headers');
    expect(result.headers).not.toHaveProperty('Access-Control-Allow-Credentials');
    expect(JSON.parse(result.body)).toEqual({ error: 'forbidden' });
  });
});
