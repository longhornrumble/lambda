'use strict';

/**
 * Integration tests for Meta_Webhook_Handler
 *
 * Uses aws-sdk-client-mock to intercept DynamoDB, Lambda, and Secrets Manager
 * calls without hitting real AWS endpoints.
 *
 * Run: npm test
 */

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

// ─── Mock setup ──────────────────────────────────────────────────────────────────

const ddbMock     = mockClient(DynamoDBClient);
const lambdaMock  = mockClient(LambdaClient);
const secretsMock = mockClient(SecretsManagerClient);

// ─── Test constants ───────────────────────────────────────────────────────────────

const APP_SECRET    = 'test-app-secret-12345';
const VERIFY_TOKEN  = 'my-verify-token';
const PAGE_ID       = '112233445566778';
const TENANT_ID     = 'TENANT_ABC';
const TENANT_HASH   = 'abc123';
const PSID          = '987654321012345';
const MESSAGE_MID   = 'm_abc123def456';
const MESSAGE_TEXT  = 'Hello from Messenger';

// Build a valid HMAC-SHA256 signature for a given raw body
function makeSignature(body) {
  const hmac = crypto.createHmac('sha256', APP_SECRET).update(body, 'utf8').digest('hex');
  return `sha256=${hmac}`;
}

// Build a minimal API Gateway event for POST.
// Pass signature=null to omit the header entirely, or a string (including '') to set it.
// Omitting signature (undefined) defaults to a valid HMAC.
function makePostEvent({ body, signature, isBase64Encoded = false } = {}) {
  const rawBody = body || JSON.stringify(makeMessengerPayload());
  const sigHeader = signature === undefined ? makeSignature(rawBody) : signature;
  const headers = { 'content-type': 'application/json' };
  if (sigHeader !== null) {
    headers['x-hub-signature-256'] = sigHeader;
  }
  return {
    requestContext: { http: { method: 'POST' } },
    rawPath: '/webhook',
    headers,
    body: isBase64Encoded ? Buffer.from(rawBody).toString('base64') : rawBody,
    isBase64Encoded,
    queryStringParameters: {},
  };
}

// Build a minimal Meta Messenger webhook payload
function makeMessengerPayload({ mid = MESSAGE_MID, text = MESSAGE_TEXT, pageId = PAGE_ID, psid = PSID } = {}) {
  return {
    object: 'page',
    entry: [
      {
        id: pageId,
        time: Date.now(),
        messaging: [
          {
            sender: { id: psid },
            recipient: { id: pageId },
            timestamp: Date.now(),
            message: {
              mid,
              text,
            },
          },
        ],
      },
    ],
  };
}

// Build a GET verification event
function makeGetEvent({ mode = 'subscribe', token = VERIFY_TOKEN, challenge = 'abc123challenge' } = {}) {
  return {
    requestContext: { http: { method: 'GET' } },
    rawPath: '/webhook',
    headers: {},
    queryStringParameters: {
      'hub.mode': mode,
      'hub.verify_token': token,
      'hub.challenge': challenge,
    },
  };
}

// ─── Module setup ─────────────────────────────────────────────────────────────────

// Set env vars before requiring the handler so module-scope constants are correct
beforeAll(() => {
  process.env.ENVIRONMENT                = 'test';
  process.env.CHANNEL_MAPPINGS_TABLE     = 'picasso-channel-mappings-test';
  process.env.DEDUP_TABLE                = 'picasso-webhook-dedup-test';
  process.env.RESPONSE_PROCESSOR_FUNCTION = 'Meta_Response_Processor';
  process.env.META_APP_SECRET_ARN        = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:test';
  process.env.MESSENGER_VERIFY_TOKEN     = VERIFY_TOKEN;
});

// The handler module caches the app secret at module scope.
// Re-require after env is set so the module sees the right env vars.
let handler;
beforeAll(() => {
  handler = require('./index').handler;
});

beforeEach(() => {
  ddbMock.reset();
  lambdaMock.reset();
  secretsMock.reset();

  // Default: Secrets Manager returns the test app secret
  secretsMock.on(GetSecretValueCommand).resolves({ SecretString: APP_SECRET });
});

// ─── Tests: GET (verification) ────────────────────────────────────────────────────

describe('GET /webhook — verification', () => {
  test('returns challenge when token matches', async () => {
    const event = makeGetEvent({ challenge: 'challenge_xyz' });
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('challenge_xyz');
  });

  test('returns 403 when token does not match', async () => {
    const event = makeGetEvent({ token: 'wrong-token' });
    const res = await handler(event);
    expect(res.statusCode).toBe(403);
  });

  test('returns 403 when hub.mode is not subscribe', async () => {
    const event = makeGetEvent({ mode: 'unsubscribe' });
    const res = await handler(event);
    expect(res.statusCode).toBe(403);
  });
});

// ─── Tests: POST — signature validation ──────────────────────────────────────────

describe('POST /webhook — signature validation', () => {
  test('returns 403 when signature header is missing', async () => {
    const rawBody = JSON.stringify(makeMessengerPayload());
    // signature: null omits the header entirely
    const event = makePostEvent({ body: rawBody, signature: null });
    const res = await handler(event);
    expect(res.statusCode).toBe(403);
  });

  test('returns 403 when signature header is empty string', async () => {
    const rawBody = JSON.stringify(makeMessengerPayload());
    // signature: '' sends the header with an empty value
    const event = makePostEvent({ body: rawBody, signature: '' });
    const res = await handler(event);
    expect(res.statusCode).toBe(403);
  });

  test('returns 403 when signature is wrong', async () => {
    const rawBody = JSON.stringify(makeMessengerPayload());
    const event = makePostEvent({ body: rawBody, signature: 'sha256=badhex00000000' });
    const res = await handler(event);
    expect(res.statusCode).toBe(403);
  });

  test('returns 200 with valid signature', async () => {
    // Provide mapping + dedup stubs so processing completes
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const event = makePostEvent();
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('EVENT_RECEIVED');
  });

  test('accepts base64-encoded body', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const rawBody = JSON.stringify(makeMessengerPayload());
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody), isBase64Encoded: true });
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
  });
});

// ─── Tests: POST — channel mapping ───────────────────────────────────────────────

describe('POST /webhook — channel mapping', () => {
  test('returns 200 and skips processing when page is not found', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: undefined });

    const event = makePostEvent();
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });

  test('returns 200 and skips when page is disabled', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: false },
      },
    });

    const event = makePostEvent();
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });
});

// ─── Tests: POST — deduplication ─────────────────────────────────────────────────

describe('POST /webhook — deduplication', () => {
  test('processes new messages (dedup write succeeds)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const event = makePostEvent();
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);

    // Verify invoke payload contains expected fields
    const invokeCall = lambdaMock.commandCalls(InvokeCommand)[0];
    const payload = JSON.parse(Buffer.from(invokeCall.args[0].input.Payload).toString('utf8'));
    expect(payload.psid).toBe(PSID);
    expect(payload.tenantId).toBe(TENANT_ID);
    expect(payload.tenantHash).toBe(TENANT_HASH);
    expect(payload.channelType).toBe('messenger');
    expect(payload.messageText).toBe(MESSAGE_TEXT);
    expect(payload.isPostback).toBe(false);
  });

  test('drops duplicate messages (ConditionalCheckFailedException)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });

    const conditionalError = new Error('The conditional request failed');
    conditionalError.name = 'ConditionalCheckFailedException';
    ddbMock.on(PutItemCommand).rejects(conditionalError);

    const event = makePostEvent();
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });
});

// ─── Tests: POST — postback events ───────────────────────────────────────────────

describe('POST /webhook — postback events', () => {
  test('processes postback payloads with isPostback=true', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const postbackBody = {
      object: 'page',
      entry: [
        {
          id: PAGE_ID,
          time: Date.now(),
          messaging: [
            {
              sender: { id: PSID },
              recipient: { id: PAGE_ID },
              timestamp: Date.now(),
              postback: {
                payload: 'GET_STARTED',
                title: 'Get Started',
              },
            },
          ],
        },
      ],
    };

    const rawBody = JSON.stringify(postbackBody);
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);

    const invokeCall = lambdaMock.commandCalls(InvokeCommand)[0];
    const payload = JSON.parse(Buffer.from(invokeCall.args[0].input.Payload).toString('utf8'));
    expect(payload.messageText).toBe('GET_STARTED');
    expect(payload.isPostback).toBe(true);
  });
});

// ─── Tests: POST — object types ──────────────────────────────────────────────────

describe('POST /webhook — object types', () => {
  test('acknowledges instagram webhook without processing (MVP)', async () => {
    const igBody = { object: 'instagram', entry: [] };
    const rawBody = JSON.stringify(igBody);
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });

    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });

  test('acknowledges unknown object types without crashing', async () => {
    const weirdBody = { object: 'whatsapp', entry: [] };
    const rawBody = JSON.stringify(weirdBody);
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });

    const res = await handler(event);
    expect(res.statusCode).toBe(200);
  });
});

// ─── Tests: POST — error resilience ──────────────────────────────────────────────

describe('POST /webhook — error resilience', () => {
  test('returns 200 even when response processor invoke fails', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).rejects(new Error('Lambda invoke failed'));

    const event = makePostEvent();
    const res = await handler(event);
    // Must still return 200 — Meta must not retry on our internal errors
    expect(res.statusCode).toBe(200);
  });

  test('returns 200 even when DynamoDB mapping lookup fails', async () => {
    ddbMock.on(GetItemCommand).rejects(new Error('DynamoDB unavailable'));

    const event = makePostEvent();
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });

  test('ignores message echo events', async () => {
    const echoBody = {
      object: 'page',
      entry: [
        {
          id: PAGE_ID,
          time: Date.now(),
          messaging: [
            {
              sender: { id: PAGE_ID }, // Page is the sender in echo events
              recipient: { id: PSID },
              timestamp: Date.now(),
              message: {
                mid: MESSAGE_MID,
                text: 'Echo of our own message',
                is_echo: true,
              },
            },
          ],
        },
      ],
    };
    const rawBody = JSON.stringify(echoBody);
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });

    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });
});

// ─── Tests: unsupported methods ───────────────────────────────────────────────────

describe('Unsupported HTTP methods', () => {
  test('returns 405 for PUT', async () => {
    const event = {
      requestContext: { http: { method: 'PUT' } },
      rawPath: '/webhook',
      headers: {},
      body: '',
    };
    const res = await handler(event);
    expect(res.statusCode).toBe(405);
  });
});
