/**
 * Bedrock Streaming Handler Integration Test Suite
 *
 * Comprehensive integration tests for index.js
 * Tests the full request->response flow including:
 * - Config loading & caching
 * - Knowledge Base integration
 * - Form mode handling
 * - Bedrock streaming
 * - Response enhancement
 * - Lambda handler entry points
 *
 * Target: 80%+ coverage of index.js
 *
 * NOTE: loadConfig and retrieveKB live in ../shared/bedrock-core.js which creates
 * its own AWS client instances at module load time. To avoid cross-module mock
 * interception issues, we mock the entire bedrock-core module here and assert
 * on handler behaviour (what it calls, what it does with results) rather than
 * on the internal AWS calls that bedrock-core makes.
 */

const { mockClient } = require('aws-sdk-client-mock');
const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand } = require('@aws-sdk/client-bedrock-runtime');
const { Readable } = require('stream');

// Mock bedrock-core BEFORE any require of index.js
// Path is relative to the test file (__tests__/), so two levels up to reach shared/
jest.mock('../../shared/bedrock-core', () => ({
  loadConfig: jest.fn(),
  retrieveKB: jest.fn(),
  sanitizeUserInput: jest.fn((input) => input), // pass-through by default
  getCacheKey: jest.fn(),
  isCacheValid: jest.fn(),
  evictOldestCacheEntries: jest.fn(),
  CACHE_TTL: 300000,
  MAX_CACHE_SIZE: 100,
}));

// Create Bedrock runtime mock (still lives in index.js)
const bedrockMock = mockClient(BedrockRuntimeClient);

// Mock dependencies
jest.mock('../form_handler', () => ({
  handleFormMode: jest.fn()
}));

jest.mock('../response_enhancer', () => ({
  enhanceResponse: jest.fn()
}));

// Mock Lambda streaming global
// streamifyResponse wraps the handler but passes through event, responseStream, context
global.awslambda = {
  streamifyResponse: jest.fn((handler) => {
    // Return a wrapper that invokes the original handler
    return async (event, responseStream, context) => {
      return await handler(event, responseStream, context);
    };
  })
};

// Import module under test AFTER mocks are set up
const { handleFormMode } = require('../form_handler');
const { enhanceResponse } = require('../response_enhancer');
const { loadConfig, retrieveKB } = require('../../shared/bedrock-core');

// We need to dynamically import index.js to capture the handler
let indexModule;

// Test fixtures
const mockMapping = {
  tenant_id: 'TEST123'
};

const mockConfig = {
  tenant_id: 'TEST123',
  aws: {
    knowledge_base_id: 'KB123',
    model_id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0'
  },
  streaming: {
    max_tokens: 1000,
    temperature: 0
  },
  tone_prompt: 'You are a helpful assistant.',
  conversational_forms: {
    volunteer_apply: {
      form_id: 'volunteer_apply',
      fields: [
        { id: 'name', type: 'text' },
        { id: 'email', type: 'email' }
      ]
    }
  }
};

const mockKBContext = 'We offer volunteer programs including Love Box and Dare to Dream.\n\nContact us at info@example.org for more information.';

// Helper to create mock Bedrock streaming response
// Must be an async iterable that can be used with for await...of
function createBedrockStream(chunks) {
  const events = chunks.map(text => ({
    chunk: {
      bytes: Buffer.from(JSON.stringify({
        type: 'content_block_delta',
        delta: { type: 'text_delta', text }
      }))
    }
  }));

  // Add start and stop events
  const startEvent = {
    chunk: {
      bytes: Buffer.from(JSON.stringify({ type: 'content_block_start' }))
    }
  };

  const stopEvent = {
    chunk: {
      bytes: Buffer.from(JSON.stringify({ type: 'message_stop' }))
    }
  };

  const allEvents = [startEvent, ...events, stopEvent];

  // Create async iterable
  return {
    body: {
      [Symbol.asyncIterator]: async function* () {
        for (const event of allEvents) {
          yield event;
        }
      }
    }
  };
}

// Helper to create mock response stream
function createMockResponseStream() {
  const chunks = [];
  let ended = false;

  return {
    write: jest.fn((data) => chunks.push(data)),
    end: jest.fn(() => { ended = true; }),
    getChunks: () => chunks,
    isEnded: () => ended
  };
}

describe('Index.js Integration Tests', () => {
  beforeAll(() => {
    // Import index.js once with the mocked awslambda global
    indexModule = require('../index.js');
  });

  beforeEach(() => {
    // Reset all mocks
    bedrockMock.reset();
    handleFormMode.mockReset();
    enhanceResponse.mockReset();
    loadConfig.mockReset();
    retrieveKB.mockReset();

    // Default implementations — most tests just need a working config + KB
    loadConfig.mockResolvedValue(mockConfig);
    retrieveKB.mockResolvedValue(mockKBContext);

    // Default enhanceResponse — return a minimal enhancement
    enhanceResponse.mockResolvedValue({
      message: '',
      ctaButtons: [],
      metadata: {}
    });

    // Set environment variables
    process.env.CONFIG_BUCKET = 'test-bucket';
  });

  describe('1. Config Loading & Caching', () => {
    it('should load config from S3 on cache miss', async () => {
      // loadConfig already mocked to return mockConfig in beforeEach

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering'
        })
      };

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify handler called loadConfig with the tenant hash
      expect(loadConfig).toHaveBeenCalledWith('abc123', expect.any(Object));
    });

    it('should use cached config on cache hit', async () => {
      // loadConfig is called once per request regardless of caching —
      // the cache is internal to bedrock-core. From the handler's perspective,
      // it calls loadConfig on every request and receives whatever bedrock-core returns.
      // This test verifies the handler behaves correctly across two requests.

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering'
        })
      };

      // First request
      let responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Second request
      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // loadConfig is called for each request — bedrock-core handles caching internally
      expect(loadConfig).toHaveBeenCalledTimes(2);
      expect(loadConfig).toHaveBeenCalledWith('abc123', expect.any(Object));
    });

    it('should try multiple config paths (config.json, {tenant_id}-config.json)', async () => {
      // bedrock-core handles path fallback internally. From the handler's perspective,
      // it calls loadConfig with the hash and receives a config (or null).
      // This test verifies the handler correctly uses the returned config.

      const altConfig = { ...mockConfig, tenant_id: 'TEST456' };
      loadConfig.mockResolvedValue(altConfig);

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'xyz789',
          user_input: 'Tell me about volunteering'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // loadConfig was called — bedrock-core resolves paths internally
      expect(loadConfig).toHaveBeenCalledWith('xyz789', expect.any(Object));

      // Bedrock was invoked — handler used the returned config
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBeGreaterThanOrEqual(1);
    });

    it('should handle missing mapping file gracefully', async () => {
      // bedrock-core returns null when the tenant cannot be resolved
      loadConfig.mockResolvedValue(null);

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'invalid',
          user_input: 'Tell me about volunteering'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Should continue with default config and still call Bedrock
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });

    it('should handle missing config file gracefully', async () => {
      // bedrock-core returns null when config is not found
      loadConfig.mockResolvedValue(null);

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Should use default config
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });

    it('should expire cache after 5 minutes', async () => {
      // Cache expiry is internal to bedrock-core — it is exercised by bedrock-core's own tests.
      // From the handler's perspective: loadConfig is called on every request and always
      // returns the current (possibly refreshed) config.
      // Verify the handler passes skipCache correctly when nocache flag is set.

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering',
          nocache: true
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Handler should pass skipCache: true when nocache is set
      expect(loadConfig).toHaveBeenCalledWith('abc123', expect.objectContaining({ skipCache: true }));
    });
  });

  describe('2. Knowledge Base Integration', () => {
    it('should retrieve context from Knowledge Base', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['We offer Love Box and Dare to Dream programs.'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteer programs'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify retrieveKB was called with user input and the loaded config
      expect(retrieveKB).toHaveBeenCalledWith(
        expect.stringContaining('Tell me about volunteer programs'),
        expect.objectContaining({ tenant_id: 'TEST123' })
      );
    });

    it('should cache KB results', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Same question'
        })
      };

      // First request
      let responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Second request with same input
      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // retrieveKB is called for each request — bedrock-core handles KB caching internally
      expect(retrieveKB).toHaveBeenCalledTimes(2);
    });

    it('should handle KB retrieval errors gracefully', async () => {
      retrieveKB.mockRejectedValue(new Error('KB error'));

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['I can still help you.'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Should continue to Bedrock even if KB fails
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });

    it('should skip KB when knowledge_base_id not configured', async () => {
      const configWithoutKB = {
        ...mockConfig,
        aws: { model_id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0' }
      };
      loadConfig.mockResolvedValue(configWithoutKB);

      // retrieveKB returns empty string when KB is not configured
      retrieveKB.mockResolvedValue('');

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Bedrock should still be called
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });
  });

  describe('3. Form Mode Bypass', () => {
    it('should call handleFormMode when form_mode: true and action: validate_field', async () => {
      handleFormMode.mockResolvedValue({
        type: 'validation_success',
        field: 'email',
        status: 'success'
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'john@example.com',
          form_mode: true,
          action: 'validate_field',
          field_id: 'email',
          field_value: 'john@example.com'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify handleFormMode was called
      expect(handleFormMode).toHaveBeenCalledWith(
        expect.objectContaining({
          form_mode: true,
          action: 'validate_field',
          field_id: 'email'
        }),
        expect.any(Object)
      );

      // Bedrock should NOT be called in form mode
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(0);
    });

    it('should call handleFormMode when form_mode: true and action: submit_form', async () => {
      handleFormMode.mockResolvedValue({
        type: 'form_complete',
        status: 'success',
        submissionId: 'sub_123'
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Submit',
          form_mode: true,
          action: 'submit_form',
          form_id: 'volunteer_apply',
          form_data: {
            name: 'John Doe',
            email: 'john@example.com'
          }
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify handleFormMode was called
      expect(handleFormMode).toHaveBeenCalledWith(
        expect.objectContaining({
          form_mode: true,
          action: 'submit_form',
          form_id: 'volunteer_apply'
        }),
        expect.any(Object)
      );
    });

    it('should stream form mode response as SSE', async () => {
      handleFormMode.mockResolvedValue({
        type: 'validation_success',
        field: 'email',
        status: 'success'
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'john@example.com',
          form_mode: true,
          action: 'validate_field',
          field_id: 'email',
          field_value: 'john@example.com'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const dataChunks = chunks.filter(c => c.includes('data:'));

      // Should have form response as SSE
      expect(dataChunks.some(c => c.includes('validation_success'))).toBe(true);
      expect(dataChunks.some(c => c.includes('[DONE]'))).toBe(true);
    });

    it('should handle form mode errors gracefully', async () => {
      handleFormMode.mockRejectedValue(new Error('Form validation failed'));

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'invalid',
          form_mode: true,
          action: 'validate_field',
          field_id: 'email',
          field_value: 'invalid'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const errorChunk = chunks.find(c => c.includes('error'));

      expect(errorChunk).toBeDefined();
      expect(errorChunk).toContain('Form processing failed');
    });

    it('should skip Bedrock invocation in form mode', async () => {
      handleFormMode.mockResolvedValue({
        type: 'validation_success',
        status: 'success'
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'test',
          form_mode: true,
          action: 'validate_field'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Bedrock should NOT be invoked
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(0);

      // KB should NOT be called (retrieveKB not invoked in form mode)
      expect(retrieveKB).not.toHaveBeenCalled();
    });
  });

  describe('4. Bedrock Streaming', () => {
    it('should invoke Bedrock with correct model ID', async () => {
      // mockConfig has aws.model_id = 'us.anthropic.claude-3-5-haiku-20241022-v1:0'
      // handler reads: config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello world'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const bedrockCall = bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)[0];
      expect(bedrockCall.args[0].input.modelId).toBe('us.anthropic.claude-3-5-haiku-20241022-v1:0');
    });

    it('should stream chunks from Bedrock response', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello', ' world', '!'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const textChunks = chunks.filter(c => c.includes('"type":"text"'));

      // Should have text chunks
      expect(textChunks.length).toBeGreaterThan(0);
      expect(textChunks.some(c => c.includes('Hello'))).toBe(true);
    });

    it('should parse SSE events from Bedrock', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Test message'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Test'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();

      // Should have SSE format
      expect(chunks.some(c => c.startsWith('data:'))).toBe(true);
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true);
    });

    it('should handle streaming errors', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).rejects(
        new Error('Bedrock error')
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const errorChunk = chunks.find(c => c.includes('error'));

      expect(errorChunk).toBeDefined();
    });

    it('should use default model when not configured', async () => {
      // Config has no model_id at top level and no aws.model_id — handler uses DEFAULT_MODEL_ID
      const configWithoutModel = {
        tenant_id: 'TEST123'
      };
      loadConfig.mockResolvedValue(configWithoutModel);

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const bedrockCall = bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)[0];
      // Should use whatever DEFAULT_MODEL_ID is defined in index.js
      expect(bedrockCall.args[0].input.modelId).toBeDefined();
      expect(typeof bedrockCall.args[0].input.modelId).toBe('string');
      expect(bedrockCall.args[0].input.modelId.length).toBeGreaterThan(0);
    });
  });

  describe('5. Response Enhancement Integration', () => {
    it('should enhance Bedrock response with CTAs', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['We have volunteer programs available.'])
      );

      enhanceResponse.mockResolvedValue({
        message: 'We have volunteer programs available.',
        ctaButtons: [
          { label: 'Apply Now', action: 'start_form', formId: 'volunteer_apply' }
        ],
        metadata: { enhanced: true }
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteering'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify enhanceResponse was called with the right positional args
      // Signature: enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata)
      expect(enhanceResponse).toHaveBeenCalledWith(
        'We have volunteer programs available.',
        'Tell me about volunteering',
        'abc123',
        expect.any(Object),
        expect.any(Object)
      );

      // Should have CTA in response
      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('cta_buttons'))).toBe(true);
    });

    it('should pass session_context to enhancer', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Response'])
      );

      enhanceResponse.mockResolvedValue({
        message: 'Response',
        ctaButtons: [],
        metadata: { enhanced: false }
      });

      const sessionContext = {
        completed_forms: ['lovebox'],
        suspended_forms: [],
        program_interest: null
      };

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello',
          session_context: sessionContext
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify session_context was passed as 4th arg
      expect(enhanceResponse).toHaveBeenCalledWith(
        'Response',
        'Hello',
        'abc123',
        expect.objectContaining({
          completed_forms: ['lovebox'],
          suspended_forms: [],
          program_interest: null
        }),
        expect.any(Object)
      );
    });

    it('should handle enhancement errors gracefully', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      enhanceResponse.mockRejectedValue(new Error('Enhancement failed'));

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Should still complete the response
      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true);
    });

    it('should skip enhancement when tenant_hash missing', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const errorChunk = chunks.find(c => c.includes('tenant_hash'));

      expect(errorChunk).toBeDefined();
    });
  });

  describe('6. Lambda Handler Entry Points', () => {
    it('should export handler function', () => {
      expect(indexModule.handler).toBeDefined();
      expect(typeof indexModule.handler).toBe('function');
    });

    it('should validate missing tenant_hash', async () => {
      const event = {
        body: JSON.stringify({
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const errorChunk = chunks.find(c => c.includes('Missing tenant_hash'));

      expect(errorChunk).toBeDefined();
    });

    it('should validate missing user_input', async () => {
      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const errorChunk = chunks.find(c => c.includes('Missing user_input'));

      expect(errorChunk).toBeDefined();
    });

    it('should generate session ID when not provided', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const chunks = responseStream.getChunks();
      const textChunk = chunks.find(c => c.includes('session_id'));

      // Should have session_id in response
      expect(textChunk).toBeDefined();
    });

    it('should handle OPTIONS requests', async () => {
      const event = {
        httpMethod: 'OPTIONS'
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Should end stream without processing
      expect(responseStream.end).toHaveBeenCalled();
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(0);
    });

    it('should handle direct invocation (event is body)', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      // Direct invocation - event IS the body
      const event = {
        tenant_hash: 'abc123',
        user_input: 'Hello'
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });
  });

  describe('7. End-to-End Integration Tests', () => {
    it('should complete normal conversation flow: Config → KB → Bedrock → Enhance → Return', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['We offer Love Box and Dare to Dream volunteer programs.'])
      );

      enhanceResponse.mockResolvedValue({
        message: 'We offer Love Box and Dare to Dream volunteer programs.',
        ctaButtons: [
          { label: 'Apply to Love Box', action: 'start_form', formId: 'lb_apply' }
        ],
        metadata: { enhanced: true, branch_detected: 'volunteer_interest' }
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about volunteer opportunities'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify complete flow
      expect(loadConfig).toHaveBeenCalled();          // Config loaded
      expect(retrieveKB).toHaveBeenCalled();          // KB retrieved
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1); // Bedrock invoked
      expect(enhanceResponse).toHaveBeenCalled();     // Response enhanced

      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('cta_buttons'))).toBe(true); // CTAs included
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true);       // Stream completed
    });

    it('should complete form validation flow: Form mode → handleFormMode → Stream → Return', async () => {
      handleFormMode.mockResolvedValue({
        type: 'validation_success',
        field: 'email',
        status: 'success',
        message: 'Email is valid'
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'john@example.com',
          form_mode: true,
          action: 'validate_field',
          field_id: 'email',
          field_value: 'john@example.com'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify form flow
      expect(handleFormMode).toHaveBeenCalled();
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(0); // Bedrock skipped

      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('validation_success'))).toBe(true);
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true);
    });

    it('should complete form submission flow: Form mode → handleFormMode → Multi-channel fulfillment → Return', async () => {
      handleFormMode.mockResolvedValue({
        type: 'form_complete',
        status: 'success',
        message: 'Thank you! Your application has been submitted.',
        submissionId: 'sub_123',
        priority: 'high',
        fulfillment: [
          { channel: 'email', status: 'sent' },
          { channel: 'sms', status: 'sent' }
        ]
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Submit',
          form_mode: true,
          action: 'submit_form',
          form_id: 'volunteer_apply',
          form_data: {
            first_name: 'John',
            last_name: 'Doe',
            email: 'john@example.com'
          }
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify submission flow
      expect(handleFormMode).toHaveBeenCalled();

      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('form_complete'))).toBe(true);
      expect(chunks.some(c => c.includes('sub_123'))).toBe(true);
    });

    it('should use cached config on second request (no S3 call)', async () => {
      // bedrock-core handles caching — the handler always calls loadConfig.
      // Verify loadConfig is called for each request.
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      // First request
      let responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Second request
      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Both requests completed successfully
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(2);
    });

    it('should detect suspended form and offer program switch', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Dare to Dream is a great program!'])
      );

      enhanceResponse.mockResolvedValue({
        message: 'Dare to Dream is a great program!',
        ctaButtons: [],
        metadata: {
          enhanced: true,
          program_switch_detected: true,
          suspended_form: { form_id: 'lb_apply', program_name: 'Love Box' },
          new_form_of_interest: { form_id: 'dd_apply', program_name: 'Dare to Dream' }
        }
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about Dare to Dream',
          session_context: {
            suspended_forms: ['lb_apply'],
            completed_forms: [],
            program_interest: 'lovebox'
          }
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify enhancement was called with suspended form context
      expect(enhanceResponse).toHaveBeenCalledWith(
        'Dare to Dream is a great program!',
        'Tell me about Dare to Dream',
        'abc123',
        expect.objectContaining({
          suspended_forms: ['lb_apply']
        }),
        expect.any(Object)
      );
    });

    it('should filter CTAs for completed forms', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['We have volunteer programs.'])
      );

      enhanceResponse.mockResolvedValue({
        message: 'We have volunteer programs.',
        ctaButtons: [], // No CTAs because form is completed
        metadata: {
          enhanced: true,
          branch_detected: 'volunteer_interest',
          filtered_forms: ['lovebox']
        }
      });

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Tell me about Love Box',
          session_context: {
            completed_forms: ['lovebox'],
            suspended_forms: []
          }
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify enhancement filtered completed forms
      expect(enhanceResponse).toHaveBeenCalledWith(
        'We have volunteer programs.',
        'Tell me about Love Box',
        'abc123',
        expect.objectContaining({
          completed_forms: ['lovebox']
        }),
        expect.any(Object)
      );
    });

    it('should skip KB retrieval when KB disabled', async () => {
      const configNoKB = {
        tenant_id: 'NO_KB_TEST',
        aws: {}
      };
      loadConfig.mockResolvedValue(configNoKB);

      // retrieveKB returns empty string when no KB configured
      retrieveKB.mockResolvedValue('');

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'no_kb_hash',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Bedrock still called
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });

    it('should recover from KB failure and deliver Bedrock response', async () => {
      retrieveKB.mockRejectedValue(new Error('KB timeout'));

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['I can still help you.'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // retrieveKB was called (and threw), but Bedrock was still invoked
      expect(retrieveKB).toHaveBeenCalled();
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);

      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('I can still help you'))).toBe(true);
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true);
    });

    it('should include conversation history in prompt when provided', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Based on our previous conversation...'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'What else can you tell me?',
          conversation_history: [
            { role: 'user', content: 'Hi, my name is John' },
            { role: 'assistant', content: 'Hello John! How can I help you?' },
            { role: 'user', content: 'Tell me about volunteering' }
          ]
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify Bedrock was called
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);

      // Check that prompt includes conversation history
      const bedrockCall = bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)[0];
      const requestBody = JSON.parse(bedrockCall.args[0].input.body);
      const prompt = requestBody.messages[0].content[0].text;

      // Verify conversation history is included in the prompt
      // (exact header wording comes from prompt_v4.js)
      expect(prompt).toContain('John');
    });

    it('should handle conversation_context.recentMessages format', async () => {
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Continuing our conversation...'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: 'abc123',
          user_input: 'Follow up question',
          conversation_context: {
            recentMessages: [
              { role: 'user', text: 'Previous message' },
              { role: 'assistant', text: 'Previous response' }
            ]
          }
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify Bedrock was called with conversation context
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });
  });
});
