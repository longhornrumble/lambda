/**
 * Coverage gap test — Issue #5 PR A
 *
 * Gap 2: shared/bedrock-core.js KB_RETRIEVER_ROLE_ARN assume-role branch
 *
 * bedrock-core.js runs the assume-role block at module-load time (not inside
 * a function), so the only way to exercise both branches deterministically is
 * jest.isolateModules() + jest.doMock() to control the env var and intercept
 * the AWS SDK constructor before the module executes.
 *
 * jest.doMock uses the same module specifier that bedrock-core.js uses in its
 * require() calls. Because jest resolves mock keys globally (not relative to
 * the requireing file), the package-name keys '@aws-sdk/...' work correctly
 * even though bedrock-core.js lives one level up in shared/.
 *
 * Placed here (under Bedrock_Streaming_Handler_Staging/__tests__/) so the
 * existing CI workflow picks it up without changes.
 */

describe('shared/bedrock-core — KB_RETRIEVER_ROLE_ARN assume-role branch', () => {
  let originalRoleArn;

  beforeEach(() => {
    originalRoleArn = process.env.KB_RETRIEVER_ROLE_ARN;
  });

  afterEach(() => {
    if (originalRoleArn === undefined) {
      delete process.env.KB_RETRIEVER_ROLE_ARN;
    } else {
      process.env.KB_RETRIEVER_ROLE_ARN = originalRoleArn;
    }
    jest.resetModules();
  });

  // ── Branch: env var unset → no credentials field on the client config ──

  test('when KB_RETRIEVER_ROLE_ARN is unset, BedrockAgentRuntimeClient is constructed WITHOUT credentials', () => {
    let capturedConfig;

    jest.isolateModules(() => {
      delete process.env.KB_RETRIEVER_ROLE_ARN;

      jest.doMock('@aws-sdk/client-bedrock-agent-runtime', () => {
        const MockClient = jest.fn(function (config) {
          capturedConfig = config;
        });
        MockClient.prototype.send = jest.fn();
        return {
          BedrockAgentRuntimeClient: MockClient,
          RetrieveCommand: jest.fn(),
        };
      });
      jest.doMock('@aws-sdk/client-s3', () => ({
        S3Client: jest.fn(function () {}),
        GetObjectCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-dynamodb', () => ({
        DynamoDBClient: jest.fn(function () {}),
        QueryCommand: jest.fn(),
      }));

      require('../../shared/bedrock-core');
    });

    expect(capturedConfig).toBeDefined();
    expect(capturedConfig.credentials).toBeUndefined();
  });

  // ── Branch: env var set → credentials field injected via fromTemporaryCredentials ──

  test('when KB_RETRIEVER_ROLE_ARN is set, BedrockAgentRuntimeClient is constructed WITH credentials', () => {
    let capturedConfig;
    const TEST_ROLE_ARN = 'arn:aws:iam::999999999999:role/test-retriever-role';
    const fakeCreds = { accessKeyId: 'FAKE', secretAccessKey: 'FAKESECRET' };
    const fromTemporaryCredentialsMock = jest.fn().mockReturnValue(fakeCreds);

    jest.isolateModules(() => {
      process.env.KB_RETRIEVER_ROLE_ARN = TEST_ROLE_ARN;

      jest.doMock('@aws-sdk/client-bedrock-agent-runtime', () => {
        const MockClient = jest.fn(function (config) {
          capturedConfig = config;
        });
        MockClient.prototype.send = jest.fn();
        return {
          BedrockAgentRuntimeClient: MockClient,
          RetrieveCommand: jest.fn(),
        };
      });
      jest.doMock('@aws-sdk/credential-providers', () => ({
        fromTemporaryCredentials: fromTemporaryCredentialsMock,
      }));
      jest.doMock('@aws-sdk/client-s3', () => ({
        S3Client: jest.fn(function () {}),
        GetObjectCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-dynamodb', () => ({
        DynamoDBClient: jest.fn(function () {}),
        QueryCommand: jest.fn(),
      }));

      require('../../shared/bedrock-core');
    });

    expect(capturedConfig).toBeDefined();
    expect(capturedConfig.credentials).toBe(fakeCreds);
  });

  test('when KB_RETRIEVER_ROLE_ARN is set, fromTemporaryCredentials is called with correct params', () => {
    const TEST_ROLE_ARN = 'arn:aws:iam::999999999999:role/test-retriever-role';
    const fromTemporaryCredentialsMock = jest.fn().mockReturnValue({});

    jest.isolateModules(() => {
      process.env.KB_RETRIEVER_ROLE_ARN = TEST_ROLE_ARN;

      jest.doMock('@aws-sdk/client-bedrock-agent-runtime', () => ({
        BedrockAgentRuntimeClient: jest.fn(function () {}),
        RetrieveCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/credential-providers', () => ({
        fromTemporaryCredentials: fromTemporaryCredentialsMock,
      }));
      jest.doMock('@aws-sdk/client-s3', () => ({
        S3Client: jest.fn(function () {}),
        GetObjectCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-dynamodb', () => ({
        DynamoDBClient: jest.fn(function () {}),
        QueryCommand: jest.fn(),
      }));

      require('../../shared/bedrock-core');
    });

    expect(fromTemporaryCredentialsMock).toHaveBeenCalledTimes(1);
    const callArgs = fromTemporaryCredentialsMock.mock.calls[0][0];
    expect(callArgs.params.RoleArn).toBe(TEST_ROLE_ARN);
    expect(callArgs.params.RoleSessionName).toBe('bedrock-kb-retriever');
    expect(callArgs.params.DurationSeconds).toBe(3600);
  });

  // ── Error branch: credential-providers not installed → logs warning, falls back ──

  test('when credential-providers require throws, logs error and falls back (no credentials field)', () => {
    let capturedConfig;
    const TEST_ROLE_ARN = 'arn:aws:iam::999999999999:role/fallback-role';
    const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    jest.isolateModules(() => {
      process.env.KB_RETRIEVER_ROLE_ARN = TEST_ROLE_ARN;

      jest.doMock('@aws-sdk/client-bedrock-agent-runtime', () => {
        const MockClient = jest.fn(function (config) {
          capturedConfig = config;
        });
        MockClient.prototype.send = jest.fn();
        return {
          BedrockAgentRuntimeClient: MockClient,
          RetrieveCommand: jest.fn(),
        };
      });
      // Simulate the package not being installed by throwing on require
      jest.doMock('@aws-sdk/credential-providers', () => {
        throw new Error('Cannot find module @aws-sdk/credential-providers');
      });
      jest.doMock('@aws-sdk/client-s3', () => ({
        S3Client: jest.fn(function () {}),
        GetObjectCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-dynamodb', () => ({
        DynamoDBClient: jest.fn(function () {}),
        QueryCommand: jest.fn(),
      }));

      require('../../shared/bedrock-core');
    });

    expect(capturedConfig).toBeDefined();
    expect(capturedConfig.credentials).toBeUndefined();
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining('KB_RETRIEVER_ROLE_ARN is set but @aws-sdk/credential-providers is not installed'),
      expect.any(String)
    );

    consoleErrorSpy.mockRestore();
  });

  // ── Review B3: kb_creds_init_failed structured signal on every retrieveKB call after init failure ──

  test('when init failed, retrieveKB emits kb_creds_init_failed signal on every call', async () => {
    const TEST_ROLE_ARN = 'arn:aws:iam::999999999999:role/fallback-role';
    const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    console.log.mockClear();
    let bedrockCore;

    jest.isolateModules(() => {
      process.env.KB_RETRIEVER_ROLE_ARN = TEST_ROLE_ARN;

      jest.doMock('@aws-sdk/client-bedrock-agent-runtime', () => {
        const MockClient = jest.fn(function () {});
        MockClient.prototype.send = jest.fn().mockResolvedValue({ retrievalResults: [] });
        return {
          BedrockAgentRuntimeClient: MockClient,
          RetrieveCommand: jest.fn(function (p) { this.input = p; }),
        };
      });
      jest.doMock('@aws-sdk/credential-providers', () => {
        throw new Error('Cannot find module @aws-sdk/credential-providers');
      });
      jest.doMock('@aws-sdk/client-s3', () => ({
        S3Client: jest.fn(function () {}),
        GetObjectCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-dynamodb', () => ({
        DynamoDBClient: jest.fn(function () {}),
        QueryCommand: jest.fn(),
      }));

      bedrockCore = require('../../shared/bedrock-core');
    });

    // Two retrieveKB calls — both must emit the structured signal
    await bedrockCore.retrieveKB('first query', { aws: { knowledge_base_id: 'KB-TEST-1' } });
    await bedrockCore.retrieveKB('second query', { aws: { knowledge_base_id: 'KB-TEST-2' } });

    const signals = console.log.mock.calls
      .map((c) => c[0])
      .filter((s) => typeof s === 'string' && s.includes('kb_creds_init_failed'));

    expect(signals.length).toBe(2);
    const first = JSON.parse(signals[0]);
    expect(first.evt).toBe('kb_creds_init_failed');
    expect(first.kb_id).toBe('KB-TEST-1');
    expect(first.role_arn).toBe(TEST_ROLE_ARN);

    consoleErrorSpy.mockRestore();
  });

  test('when init succeeded, retrieveKB does NOT emit kb_creds_init_failed signal', async () => {
    const TEST_ROLE_ARN = 'arn:aws:iam::999999999999:role/working-role';
    console.log.mockClear();
    let bedrockCore;

    jest.isolateModules(() => {
      process.env.KB_RETRIEVER_ROLE_ARN = TEST_ROLE_ARN;

      jest.doMock('@aws-sdk/client-bedrock-agent-runtime', () => {
        const MockClient = jest.fn(function () {});
        MockClient.prototype.send = jest.fn().mockResolvedValue({ retrievalResults: [] });
        return {
          BedrockAgentRuntimeClient: MockClient,
          RetrieveCommand: jest.fn(function (p) { this.input = p; }),
        };
      });
      jest.doMock('@aws-sdk/credential-providers', () => ({
        fromTemporaryCredentials: jest.fn().mockReturnValue({}),
      }));
      jest.doMock('@aws-sdk/client-s3', () => ({
        S3Client: jest.fn(function () {}),
        GetObjectCommand: jest.fn(),
      }));
      jest.doMock('@aws-sdk/client-dynamodb', () => ({
        DynamoDBClient: jest.fn(function () {}),
        QueryCommand: jest.fn(),
      }));

      bedrockCore = require('../../shared/bedrock-core');
    });

    await bedrockCore.retrieveKB('a query', { aws: { knowledge_base_id: 'KB-OK' } });

    const signals = console.log.mock.calls
      .map((c) => c[0])
      .filter((s) => typeof s === 'string' && s.includes('kb_creds_init_failed'));

    expect(signals.length).toBe(0);
  });
});
