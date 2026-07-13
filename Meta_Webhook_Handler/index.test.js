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
  test('processes instagram webhook through the messaging pipeline', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const igBody = {
      object: 'instagram',
      entry: [
        {
          id: PAGE_ID,
          time: Date.now(),
          messaging: [
            {
              sender: { id: PSID },
              recipient: { id: PAGE_ID },
              timestamp: Date.now(),
              message: { mid: MESSAGE_MID, text: MESSAGE_TEXT },
            },
          ],
        },
      ],
    };
    const rawBody = JSON.stringify(igBody);
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });

    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);
  });

  test('sets channelType instagram in payload for instagram object type', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const igBody = {
      object: 'instagram',
      entry: [
        {
          id: PAGE_ID,
          time: Date.now(),
          messaging: [
            {
              sender: { id: PSID },
              recipient: { id: PAGE_ID },
              timestamp: Date.now(),
              message: { mid: MESSAGE_MID, text: MESSAGE_TEXT },
            },
          ],
        },
      ],
    };
    const rawBody = JSON.stringify(igBody);
    const event = makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });

    await handler(event);

    const invokeCall = lambdaMock.commandCalls(InvokeCommand)[0];
    const payload = JSON.parse(Buffer.from(invokeCall.args[0].input.Payload).toString('utf8'));
    expect(payload.channelType).toBe('instagram');
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

  test('forwards echo events with messageText:null and psid from recipient.id (C1)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId:   { S: TENANT_ID },
        tenantHash: { S: TENANT_HASH },
        enabled:    { BOOL: true },
      },
    });
    ddbMock.on(PutItemCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

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
                app_id: 1122334455,
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
    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);

    const invokeCall = lambdaMock.commandCalls(InvokeCommand)[0];
    const payload = JSON.parse(Buffer.from(invokeCall.args[0].input.Payload).toString('utf8'));
    expect(payload.eventKind).toBe('echo');
    expect(payload.psid).toBe(PSID);          // customer, NOT the page (C1 inversion rule)
    expect(payload.messageText).toBeNull();   // never forward echo text (loop guard, C1 v1.1)
    expect(payload.appId).toBe('1122334455');
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

// ─── Tests: payload v2 (contract C1 — M1a) ───────────────────────────────────────

const FIXTURES = require('./__fixtures__/messagingEvents');

/** Wrap a single messaging event in a signed webhook POST. */
function makeEventPost(messagingEvent, { objectType = 'page', standby = false } = {}) {
  const entry = { id: PAGE_ID, time: Date.now() };
  if (standby) entry.standby = [messagingEvent];
  else entry.messaging = [messagingEvent];
  const rawBody = JSON.stringify({ object: objectType, entry: [entry] });
  return makePostEvent({ body: rawBody, signature: makeSignature(rawBody) });
}

function mockHappyPath() {
  ddbMock.on(GetItemCommand).resolves({
    Item: {
      tenantId:   { S: TENANT_ID },
      tenantHash: { S: TENANT_HASH },
      enabled:    { BOOL: true },
    },
  });
  ddbMock.on(PutItemCommand).resolves({});
  lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
}

function invokedPayloads() {
  return lambdaMock
    .commandCalls(InvokeCommand)
    .map((call) => JSON.parse(Buffer.from(call.args[0].input.Payload).toString('utf8')));
}

describe('POST /webhook — payload v2 (C1)', () => {
  test('v1 fields are byte-identical for a text message (pinned contract)', async () => {
    mockHappyPath();
    const res = await handler(makeEventPost(FIXTURES.fbText));
    expect(res.statusCode).toBe(200);

    const [payload] = invokedPayloads();
    // Pinned v1 subset — any change here breaks the legacy processor contract
    const v1 = (({ psid, messageText, pageId, tenantId, tenantHash, channelType, messageMid, isPostback }) =>
      ({ psid, messageText, pageId, tenantId, tenantHash, channelType, messageMid, isPostback }))(payload);
    expect(v1).toEqual({
      psid: FIXTURES.PSID,
      messageText: 'Hello from Messenger',
      pageId: PAGE_ID,
      tenantId: TENANT_ID,
      tenantHash: TENANT_HASH,
      channelType: 'messenger',
      messageMid: FIXTURES.MID,
      isPostback: false,
    });
    // v2 additions present
    expect(payload.v).toBe(2);
    expect(payload.eventKind).toBe('text');
    expect(payload.timestamp).toBe(FIXTURES.TS);
  });

  test('attachment-only message → v2 invoke with eventKind attachment, messageText null', async () => {
    mockHappyPath();
    await handler(makeEventPost(FIXTURES.fbAttachmentImage));
    const [payload] = invokedPayloads();
    expect(payload.eventKind).toBe('attachment');
    expect(payload.messageText).toBeNull();
    expect(payload.attachmentTypes).toEqual(['image']);
  });

  test('edit event bypasses the dedup guard (idempotent downstream)', async () => {
    mockHappyPath();
    await handler(makeEventPost(FIXTURES.fbEdit));
    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);
    // The only PutItem in this Lambda is the dedup write — it must NOT run for edits
    expect(ddbMock).not.toHaveReceivedCommand(PutItemCommand);
    const [payload] = invokedPayloads();
    expect(payload.eventKind).toBe('edit');
    expect(payload.targetMid).toBe(FIXTURES.MID);
    expect(payload.editedText).toBe('edited text');
  });

  test('FB message_deletions with 2 mids → 2 delete invokes, no dedup writes', async () => {
    mockHappyPath();
    await handler(makeEventPost(FIXTURES.fbDeleteTwoMids));
    const payloads = invokedPayloads();
    expect(payloads).toHaveLength(2);
    expect(payloads.map((p) => p.targetMid).sort()).toEqual(['m_deleted_1', 'm_deleted_2']);
    expect(ddbMock).not.toHaveReceivedCommand(PutItemCommand);
  });

  test('standby-channel event → invoke with isStandby true', async () => {
    mockHappyPath();
    await handler(makeEventPost(FIXTURES.fbText, { standby: true }));
    const [payload] = invokedPayloads();
    expect(payload.eventKind).toBe('text');
    expect(payload.isStandby).toBe(true);
  });

  test('metadata-only events (reaction, receipts, referral, feedback) → 200, zero invokes', async () => {
    mockHappyPath();
    for (const fixture of [
      FIXTURES.fbReaction,
      FIXTURES.fbDeliveryReceipt,
      FIXTURES.fbReadReceipt,
      FIXTURES.fbStandaloneReferral,
      FIXTURES.fbResponseFeedback,
    ]) {
      const res = await handler(makeEventPost(fixture));
      expect(res.statusCode).toBe(200);
    }
    expect(lambdaMock).not.toHaveReceivedCommand(InvokeCommand);
  });

  test('quick-reply tap → quickReplyPayload populated, messageText preserved (v1 compat)', async () => {
    mockHappyPath();
    await handler(makeEventPost(FIXTURES.igQuickReply, { objectType: 'instagram' }));
    const [payload] = invokedPayloads();
    expect(payload.eventKind).toBe('quick_reply');
    expect(payload.quickReplyPayload).toBe('PIC1:cta:apply');
    expect(payload.messageText).toBe('How do I apply?');
    expect(payload.channelType).toBe('instagram');
  });

  test('event without Meta timestamp → receipt-time fallback (timestamp always present)', async () => {
    mockHappyPath();
    const noTs = { ...FIXTURES.fbText };
    delete noTs.timestamp;
    const before = Date.now();
    await handler(makeEventPost(noTs));
    const [payload] = invokedPayloads();
    expect(typeof payload.timestamp).toBe('number');
    expect(payload.timestamp).toBeGreaterThanOrEqual(before);
  });
});
