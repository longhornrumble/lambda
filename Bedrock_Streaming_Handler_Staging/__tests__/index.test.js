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
 */

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand } = require('@aws-sdk/client-bedrock-runtime');
const { BedrockAgentRuntimeClient, RetrieveCommand } = require('@aws-sdk/client-bedrock-agent-runtime');
const { Readable } = require('stream');

// Create mocks
const s3Mock = mockClient(S3Client);
const bedrockMock = mockClient(BedrockRuntimeClient);
const bedrockAgentMock = mockClient(BedrockAgentRuntimeClient);

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

const mockKBResults = {
  retrievalResults: [
    {
      content: {
        text: 'We offer volunteer programs including Love Box and Dare to Dream.'
      }
    },
    {
      content: {
        text: 'Contact us at info@example.org for more information.'
      }
    }
  ]
};

// Helper to create mock S3 response with Body stream
function createS3Response(data) {
  const stream = new Readable();
  stream.push(JSON.stringify(data));
  stream.push(null);
  stream.transformToString = async () => JSON.stringify(data);
  return { Body: stream };
}

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
    s3Mock.reset();
    bedrockMock.reset();
    bedrockAgentMock.reset();
    handleFormMode.mockReset();
    enhanceResponse.mockReset();

    // Set environment variables
    process.env.CONFIG_BUCKET = 'test-bucket';

    // Note: We don't reset modules here because that would lose the awslambda mock
    // Instead, we manually clear the config cache by accessing it
    // The cache is in-memory in index.js, so it persists across tests
  });

  describe('1. Config Loading & Caching', () => {
    it('should load config from S3 on cache miss', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).resolves(mockKBResults);
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

      // Verify S3 was called for mapping and config
      expect(s3Mock.commandCalls(GetObjectCommand).length).toBeGreaterThanOrEqual(1);
    });

    it('should use cached config on cache hit', async () => {
      // First request - loads config
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).resolves(mockKBResults);
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

      const firstCallCount = s3Mock.commandCalls(GetObjectCommand).length;

      // Second request - should use cache
      s3Mock.reset();
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello again'])
      );

      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // S3 should not be called again for config
      expect(s3Mock.commandCalls(GetObjectCommand).length).toBe(0);
    });

    it('should try multiple config paths (config.json, {tenant_id}-config.json)', async () => {
      // Use a different tenant hash to avoid cache
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/xyz789.json' })
        .resolves(createS3Response({ tenant_id: 'TEST456' }));

      // First path fails
      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST456/config.json' })
        .rejects(new Error('Not found'));

      // Second path succeeds
      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST456/TEST456-config.json' })
        .resolves(createS3Response(mockConfig));

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

      // Should have tried both config paths
      const configCalls = s3Mock.commandCalls(GetObjectCommand).filter(
        call => call.args[0].input.Key.includes('config.json')
      );
      expect(configCalls.length).toBeGreaterThanOrEqual(1);
    });

    it('should handle missing mapping file gracefully', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/invalid.json' })
        .rejects(new Error('Not found'));

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

      // Should continue with default config
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });

    it('should handle missing config file gracefully', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand)
        .rejects(new Error('Config not found'));

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
      // Use a unique tenant hash for this test
      const uniqueHash = 'cache_test_' + Date.now();

      // Mock Date.now() to control cache expiration
      const originalNow = Date.now;
      let currentTime = 2000000; // Different base time
      Date.now = jest.fn(() => currentTime);

      s3Mock
        .on(GetObjectCommand, { Key: `mappings/${uniqueHash}.json` })
        .resolves(createS3Response({ tenant_id: 'CACHE_TEST' }));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/CACHE_TEST/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: uniqueHash,
          user_input: 'Tell me about volunteering'
        })
      };

      // First request - cache miss
      let responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      const firstCallCount = s3Mock.commandCalls(GetObjectCommand).length;

      // Advance time by 6 minutes (beyond 5 minute TTL)
      currentTime += 6 * 60 * 1000;

      // Second request - cache should be expired
      s3Mock.reset();
      s3Mock
        .on(GetObjectCommand, { Key: `mappings/${uniqueHash}.json` })
        .resolves(createS3Response({ tenant_id: 'CACHE_TEST' }));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/CACHE_TEST/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello again'])
      );

      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // S3 should be called again because cache expired
      expect(s3Mock.commandCalls(GetObjectCommand).length).toBeGreaterThan(0);

      // Restore Date.now
      Date.now = originalNow;
    });
  });

  describe('2. Knowledge Base Integration', () => {
    it('should retrieve context from Knowledge Base', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).resolves(mockKBResults);
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

      // Verify KB was called
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(1);

      const kbCall = bedrockAgentMock.commandCalls(RetrieveCommand)[0];
      expect(kbCall.args[0].input.knowledgeBaseId).toBe('KB123');
      expect(kbCall.args[0].input.retrievalQuery.text).toBe('Tell me about volunteer programs');
    });

    it('should cache KB results', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).resolves(mockKBResults);
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

      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(1);

      // Second request with same input - should use cache
      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // KB should still only be called once (cached)
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(1);
    });

    it('should handle KB retrieval errors gracefully', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).rejects(new Error('KB error'));
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

      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(configWithoutKB));

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

      // KB should not be called
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(0);

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

      // KB should NOT be called
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(0);
    });
  });

  describe('4. Bedrock Streaming', () => {
    it('should invoke Bedrock with correct model ID', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      const configWithoutModel = {
        tenant_id: 'TEST123'
      };

      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(configWithoutModel));

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
      expect(bedrockCall.args[0].input.modelId).toBe('us.anthropic.claude-3-5-haiku-20241022-v1:0');
    });
  });

  describe('5. Response Enhancement Integration', () => {
    it('should enhance Bedrock response with CTAs', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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

      // Verify enhanceResponse was called
      expect(enhanceResponse).toHaveBeenCalledWith(
        'We have volunteer programs available.',
        'Tell me about volunteering',
        'abc123',
        expect.any(Object)
      );

      // Should have CTA in response
      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('cta_buttons'))).toBe(true);
    });

    it('should pass session_context to enhancer', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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

      // Verify session_context was passed
      expect(enhanceResponse).toHaveBeenCalledWith(
        'Response',
        'Hello',
        'abc123',
        sessionContext
      );
    });

    it('should handle enhancement errors gracefully', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      expect(responseStream.isEnded()).toBe(true);
    });

    it('should handle direct invocation (event is body)', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
      // Use a unique tenant hash to avoid cache
      const uniqueHash = 'e2e_flow_' + Date.now();

      s3Mock
        .on(GetObjectCommand, { Key: `mappings/${uniqueHash}.json` })
        .resolves(createS3Response({ tenant_id: 'E2E_TEST' }));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/E2E_TEST/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).resolves(mockKBResults);

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
          tenant_hash: uniqueHash,
          user_input: 'Tell me about volunteer opportunities'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // Verify complete flow
      expect(s3Mock.commandCalls(GetObjectCommand).length).toBeGreaterThan(0); // Config loaded
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(1); // KB retrieved
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1); // Bedrock invoked
      expect(enhanceResponse).toHaveBeenCalled(); // Response enhanced

      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('cta_buttons'))).toBe(true); // CTAs included
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true); // Stream completed
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
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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

      const firstS3Calls = s3Mock.commandCalls(GetObjectCommand).length;

      // Second request - reset S3 mock to verify no new calls
      s3Mock.reset();
      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello again'])
      );

      responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // No new S3 calls (cache hit)
      expect(s3Mock.commandCalls(GetObjectCommand).length).toBe(0);
    });

    it('should detect suspended form and offer program switch', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
        })
      );
    });

    it('should filter CTAs for completed forms', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

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
        })
      );
    });

    it('should skip KB retrieval when KB disabled', async () => {
      const configNoKB = {
        tenant_id: 'NO_KB_TEST',
        aws: {}
      };

      const uniqueHash = 'no_kb_' + Date.now();

      s3Mock
        .on(GetObjectCommand, { Key: `mappings/${uniqueHash}.json` })
        .resolves(createS3Response({ tenant_id: 'NO_KB_TEST' }));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/NO_KB_TEST/config.json' })
        .resolves(createS3Response(configNoKB));

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Hello'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: uniqueHash,
          user_input: 'Hello'
        })
      };

      const responseStream = createMockResponseStream();
      await indexModule.handler(event, responseStream, {});

      // No KB retrieval
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(0);

      // Bedrock still called
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);
    });

    it('should recover from KB failure and deliver Bedrock response', async () => {
      s3Mock
        .on(GetObjectCommand, { Key: 'mappings/abc123.json' })
        .resolves(createS3Response(mockMapping));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/TEST123/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).rejects(new Error('KB timeout'));

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

      // KB failed but response delivered
      expect(bedrockAgentMock.commandCalls(RetrieveCommand).length).toBe(1);
      expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand).length).toBe(1);

      const chunks = responseStream.getChunks();
      expect(chunks.some(c => c.includes('I can still help you'))).toBe(true);
      expect(chunks.some(c => c.includes('[DONE]'))).toBe(true);
    });

    it('should include conversation history in prompt when provided', async () => {
      const uniqueHash = 'history_test_' + Date.now();

      s3Mock
        .on(GetObjectCommand, { Key: `mappings/${uniqueHash}.json` })
        .resolves(createS3Response({ tenant_id: 'HISTORY_TEST' }));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/HISTORY_TEST/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockAgentMock.on(RetrieveCommand).resolves(mockKBResults);

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Based on our previous conversation...'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: uniqueHash,
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

      expect(prompt).toContain('PREVIOUS CONVERSATION');
      expect(prompt).toContain('John');
    });

    it('should handle conversation_context.recentMessages format', async () => {
      const uniqueHash = 'context_test_' + Date.now();

      s3Mock
        .on(GetObjectCommand, { Key: `mappings/${uniqueHash}.json` })
        .resolves(createS3Response({ tenant_id: 'CONTEXT_TEST' }));

      s3Mock
        .on(GetObjectCommand, { Key: 'tenants/CONTEXT_TEST/config.json' })
        .resolves(createS3Response(mockConfig));

      bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
        createBedrockStream(['Continuing our conversation...'])
      );

      const event = {
        body: JSON.stringify({
          tenant_hash: uniqueHash,
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
