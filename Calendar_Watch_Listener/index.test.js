'use strict';

/**
 * Unit tests for Calendar_Watch_Listener (sub-phase B Task B2).
 *
 * Covers:
 *  - GET probe returns 200
 *  - POST with missing headers returns 400 + emits malformed_payload
 *  - POST with unknown channel returns 403
 *  - POST with channel-token hash mismatch returns 403 (constant-time)
 *  - POST sync handshake returns 200, no dispatch
 *  - POST happy path: channel lookup OK, hash match, replay window OK,
 *    SQS SendMessage invoked with correct MessageGroupId + MessageDeduplicationId
 *  - Unsupported HTTP method returns 405
 *  - Pure-helper tests: sha256Hex, validateToken, isWithinReplayWindow,
 *    getHeader case-insensitivity
 *
 * Uses aws-sdk-client-mock to intercept DDB + SQS without hitting AWS.
 */

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const ddbMock = mockClient(DynamoDBClient);
const sqsMock = mockClient(SQSClient);

// ─── Env setup ──────────────────────────────────────────────────────────────────

process.env.ENVIRONMENT                   = 'staging';
process.env.CALENDAR_WATCH_CHANNELS_TABLE = 'picasso-calendar-watch-channels-staging';
process.env.EVENTS_QUEUE_URL              = 'https://sqs.us-east-1.amazonaws.com/525409062831/picasso-calendar-watch-events-staging.fifo';
process.env.REPLAY_WINDOW_SECONDS         = '300';

// Lazy-require after env so module-load reads our test values
const handlerModule = require('./index');
const { handler, _test } = handlerModule;

// ─── Test fixtures ──────────────────────────────────────────────────────────────

const CHANNEL_ID    = '550e8400-e29b-41d4-a716-446655440000';
const CHANNEL_TOKEN = 'a'.repeat(64); // 64-char hex per secrets.token_hex(32)
const TENANT_ID     = 'MYR384719';
const CALENDAR_ID   = 'coordinator@myr.example.com';
const RESOURCE_ID   = 'resource-id-xyz';
const RESOURCE_URI  = 'https://www.googleapis.com/calendar/v3/calendars/coordinator@myr.example.com/events?alt=json';

const tokenSha256 = crypto.createHash('sha256').update(CHANNEL_TOKEN, 'utf8').digest('hex');

function makeChannelRow(overrides = {}) {
  return {
    Item: {
      tenant_id:            { S: TENANT_ID },
      calendar_id:          { S: CALENDAR_ID },
      calendar_provider:    { S: 'google' },
      channel_token_sha256: { S: tokenSha256 },
      status:               { S: 'active' },
      expiration:           { N: String(Date.now() + 86400000) },
      ...overrides,
    },
  };
}

function makePostEvent(headers, body = '') {
  return {
    requestContext: { http: { method: 'POST' } },
    headers,
    body,
    isBase64Encoded: false,
  };
}

function makeGetEvent() {
  return {
    requestContext: { http: { method: 'GET' } },
    headers: {},
  };
}

function validHeaders(overrides = {}) {
  return {
    'x-goog-channel-id':     CHANNEL_ID,
    'x-goog-channel-token':  CHANNEL_TOKEN,
    'x-goog-resource-state': 'exists',
    'x-goog-message-number': '42',
    'x-goog-resource-id':    RESOURCE_ID,
    'x-goog-resource-uri':   RESOURCE_URI,
    ...overrides,
  };
}

beforeEach(() => {
  ddbMock.reset();
  sqsMock.reset();
});

// ─── Pure helpers ───────────────────────────────────────────────────────────────

describe('getHeader', () => {
  test('case-insensitive lookup', () => {
    expect(_test.getHeader({ 'X-Goog-Channel-ID': 'abc' }, 'x-goog-channel-id')).toBe('abc');
    expect(_test.getHeader({ 'x-goog-channel-id': 'abc' }, 'X-GOOG-CHANNEL-ID')).toBe('abc');
  });
  test('returns undefined when header absent', () => {
    expect(_test.getHeader({}, 'x-goog-channel-id')).toBeUndefined();
  });
  test('returns undefined for missing headers object', () => {
    expect(_test.getHeader(undefined, 'x')).toBeUndefined();
    expect(_test.getHeader(null, 'x')).toBeUndefined();
  });
});

describe('sha256Hex', () => {
  test('produces stable 64-char hex digest', () => {
    const h = _test.sha256Hex('hello');
    expect(h).toMatch(/^[0-9a-f]{64}$/);
    expect(_test.sha256Hex('hello')).toBe(h);
  });
  test('different inputs produce different outputs', () => {
    expect(_test.sha256Hex('a')).not.toBe(_test.sha256Hex('b'));
  });
});

describe('validateToken', () => {
  test('matches valid token against its stored hash', () => {
    const hash = _test.sha256Hex(CHANNEL_TOKEN);
    expect(_test.validateToken(CHANNEL_TOKEN, hash)).toBe(true);
  });
  test('rejects mismatched token', () => {
    const hash = _test.sha256Hex(CHANNEL_TOKEN);
    expect(_test.validateToken(CHANNEL_TOKEN + 'X', hash)).toBe(false);
  });
  test('rejects empty token', () => {
    expect(_test.validateToken('', _test.sha256Hex('x'))).toBe(false);
  });
  test('rejects missing stored hash', () => {
    expect(_test.validateToken(CHANNEL_TOKEN, null)).toBe(false);
    expect(_test.validateToken(CHANNEL_TOKEN, '')).toBe(false);
  });
});

describe('isWithinReplayWindow', () => {
  test('accepts age 0', () => {
    const now = Date.now();
    expect(_test.isWithinReplayWindow(now, now)).toBe(true);
  });
  test('accepts age exactly at window', () => {
    const now = Date.now();
    expect(_test.isWithinReplayWindow(now - 300_000, now)).toBe(true);
  });
  test('rejects age 1 ms over window', () => {
    const now = Date.now();
    expect(_test.isWithinReplayWindow(now - 300_001, now)).toBe(false);
  });
});

// ─── GET ────────────────────────────────────────────────────────────────────────

describe('GET probe', () => {
  test('returns 200', async () => {
    const response = await handler(makeGetEvent());
    expect(response.statusCode).toBe(200);
  });
});

// ─── POST: malformed payloads ───────────────────────────────────────────────────

describe('POST malformed_payload', () => {
  test('missing channel-id returns 400', async () => {
    const headers = validHeaders();
    delete headers['x-goog-channel-id'];
    const response = await handler(makePostEvent(headers));
    expect(response.statusCode).toBe(400);
    expect(ddbMock.calls()).toHaveLength(0);
    expect(sqsMock.calls()).toHaveLength(0);
  });

  test('missing channel-token returns 400', async () => {
    const headers = validHeaders();
    delete headers['x-goog-channel-token'];
    const response = await handler(makePostEvent(headers));
    expect(response.statusCode).toBe(400);
  });

  test('missing resource-state returns 400', async () => {
    const headers = validHeaders();
    delete headers['x-goog-resource-state'];
    const response = await handler(makePostEvent(headers));
    expect(response.statusCode).toBe(400);
  });

  test('missing message-number returns 400', async () => {
    const headers = validHeaders();
    delete headers['x-goog-message-number'];
    const response = await handler(makePostEvent(headers));
    expect(response.statusCode).toBe(400);
  });
});

// ─── POST: auth failures ────────────────────────────────────────────────────────

describe('POST auth_rejected', () => {
  test('unknown channel returns 403', async () => {
    ddbMock.on(GetItemCommand).resolves({}); // empty Item
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(403);
    expect(sqsMock.calls()).toHaveLength(0);
  });

  test('channel-token hash mismatch returns 403', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    const response = await handler(makePostEvent(validHeaders({ 'x-goog-channel-token': 'wrong-token' })));
    expect(response.statusCode).toBe(403);
    expect(sqsMock.calls()).toHaveLength(0);
  });
});

// ─── POST: sync handshake ───────────────────────────────────────────────────────

describe('POST sync', () => {
  test('sync state returns 200 without dispatching', async () => {
    // sync is handled BEFORE channel lookup (no DDB call needed)
    const response = await handler(makePostEvent(validHeaders({ 'x-goog-resource-state': 'sync' })));
    expect(response.statusCode).toBe(200);
    expect(ddbMock.calls()).toHaveLength(0);
    expect(sqsMock.calls()).toHaveLength(0);
  });
});

// ─── POST: happy path ──────────────────────────────────────────────────────────

describe('POST dispatched_raw_push', () => {
  test('happy path: lookup + hash match + SQS send', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mid-test' });

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 1);

    const sentCall = sqsMock.commandCalls(SendMessageCommand)[0];
    expect(sentCall.args[0].input.MessageGroupId).toBe(CHANNEL_ID);
    expect(sentCall.args[0].input.MessageDeduplicationId).toMatch(/^[0-9a-f]{64}$/);

    const body = JSON.parse(sentCall.args[0].input.MessageBody);
    expect(body.event_type).toBe('raw.calendar_push');
    expect(body.tenant_id).toBe(TENANT_ID);
    expect(body.calendar_id).toBe(CALENDAR_ID);
    expect(body.calendar_provider).toBe('google');
    expect(body.channel_id).toBe(CHANNEL_ID);
    expect(body.resource_state).toBe('exists');
    expect(body.message_number).toBe('42');
    expect(body.resource_id).toBe(RESOURCE_ID);
    expect(body.resource_uri).toBe(RESOURCE_URI);
  });

  test('non-active channel status logs warning but still dispatches', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow({
      status: { S: 'event_body_private' },
    }));
    sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mid-test' });
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 1);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('channel_in_non_active_state'));

    warnSpy.mockRestore();
  });
});

// ─── POST: dispatch failure ─────────────────────────────────────────────────────

describe('POST dispatch_failed', () => {
  test('SQS failure returns 500 so Google retries', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    sqsMock.on(SendMessageCommand).rejects(new Error('SQS throttled'));

    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(500);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('dispatch_failed'));
    errSpy.mockRestore();
  });
});

// ─── POST: channel lookup failure ───────────────────────────────────────────────

describe('POST channel_lookup_failed', () => {
  test('DDB failure returns 503 so Google retries', async () => {
    ddbMock.on(GetItemCommand).rejects(new Error('DDB unavailable'));
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(503);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('channel_lookup_failed'));
    errSpy.mockRestore();
  });
});

// ─── Unsupported methods ────────────────────────────────────────────────────────

describe('Unsupported HTTP methods', () => {
  test('PUT returns 405', async () => {
    const response = await handler({
      requestContext: { http: { method: 'PUT' } },
      headers: {},
    });
    expect(response.statusCode).toBe(405);
  });
});
