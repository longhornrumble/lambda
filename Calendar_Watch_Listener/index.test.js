'use strict';

/**
 * Unit tests for Calendar_Watch_Listener (sub-phase B Task B2, Phase 2b).
 *
 * Covers:
 *  - GET probe returns 200
 *  - POST with missing headers returns 400 + emits malformed_payload
 *  - POST with unknown channel returns 403
 *  - POST with channel-token hash mismatch returns 403 (constant-time)
 *  - POST sync handshake returns 200, no dispatch
 *  - POST happy path: channel lookup OK, hash match, replay window OK,
 *    OAuth client built, listChangedEvents called, typed event dispatched to
 *    SQS FIFO with MessageGroupId=booking_id (NOT channel_id)
 *  - Unsupported HTTP method returns 405
 *  - Pure-helper tests: sha256Hex, validateToken, isWithinReplayWindow,
 *    getHeader case-insensitivity
 *  - Phase 2b derivation tests: resolveResourceId, deriveTypedEnvelopes
 *  - Phase 2b integration tests: processDelta scenarios
 *  - CI-3b contract test: all 7 event_types have a derivation branch
 *
 * Uses aws-sdk-client-mock to intercept DDB + SQS without hitting AWS.
 * Mocks oauth-client and calendar-api for handler integration tests.
 */

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, GetItemCommand, UpdateItemCommand, QueryCommand } = require('@aws-sdk/client-dynamodb');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

// ─── Mock oauth-client and calendar-api before requiring index ──────────────────
// These are module-level mocks; jest.mock() is hoisted above all requires.

jest.mock('./oauth-client', () => ({
  getOAuthClient: jest.fn(),
  // Y1: clearCacheEntry is called on the oauth_client_failed error path
  clearCacheEntry: jest.fn(),
}));

jest.mock('./calendar-api', () => ({
  listChangedEvents: jest.fn(),
}));

const { getOAuthClient, clearCacheEntry } = require('./oauth-client');
const { listChangedEvents } = require('./calendar-api');

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

const CHANNEL_ID     = '550e8400-e29b-41d4-a716-446655440000';
const CHANNEL_TOKEN  = 'a'.repeat(64); // 64-char hex per secrets.token_hex(32)
const TENANT_ID      = 'MYR384719';
const CALENDAR_ID    = 'coordinator@myr.example.com';
const COORDINATOR_ID = 'coordinator@myr.example.com';
const RESOURCE_ID    = 'resource-id-xyz';
const RESOURCE_URI   = 'https://www.googleapis.com/calendar/v3/calendars/coordinator@myr.example.com/events?alt=json';
const BOOKING_ID     = 'bk-aaaaaa-111111';
const FAKE_AUTH      = { _kind: 'fake-oauth2' };

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
      coordinator_id:       { S: COORDINATOR_ID },
      last_sync_token:      { S: 'sync-tok-1' },
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
  getOAuthClient.mockReset();
  clearCacheEntry.mockReset();
  listChangedEvents.mockReset();
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

// ─── POST: happy path (Phase 2b typed dispatch) ────────────────────────────────
// Verifies the full delta-discovery path: lookupChannel → getOAuthClient →
// listChangedEvents → advanceSyncToken → deriveTypedEnvelopes → SQS typed event.
// MessageGroupId is now booking_id (NOT channel_id) per Phase 2b dispatch contract.

describe('POST typed dispatch', () => {
  // A minimal platform-owned calendar event that will produce booking.calendar_deleted.
  function makeCancelledCalEvent(bookingId) {
    return {
      id: 'google-evt-1',
      status: 'cancelled',
      updated: '2026-05-29T12:00:00Z',
      extendedProperties: { private: { booking_id: bookingId } },
    };
  }

  test('happy path: lookup + hash match + typed SQS dispatch with MessageGroupId=booking_id', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    // advanceSyncToken UpdateItem succeeds
    ddbMock.on(UpdateItemCommand).resolves({});
    sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mid-typed' });
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({
      events: [makeCancelledCalEvent(BOOKING_ID)],
      nextSyncToken: 'sync-tok-2',
      nextPageToken: null,
    });

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(getOAuthClient).toHaveBeenCalledWith({ tenantId: TENANT_ID, coordinatorId: COORDINATOR_ID });
    expect(listChangedEvents).toHaveBeenCalledWith(FAKE_AUTH, CALENDAR_ID, 'sync-tok-1', null);
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 1);

    const sentCall = sqsMock.commandCalls(SendMessageCommand)[0];
    // MessageGroupId MUST be booking_id, not channel_id (Phase 2b contract change)
    expect(sentCall.args[0].input.MessageGroupId).toBe(BOOKING_ID);

    // Y2: dedup basis MUST include the platform-controlled channel_id so an
    // attacker cannot forge a dedup-collision by controlling booking_id + updated.
    const body = JSON.parse(sentCall.args[0].input.MessageBody);
    const expectedDedupBasis = `${CHANNEL_ID}:${BOOKING_ID}:${body.last_calendar_mutation_at}`;
    const expectedDedupId = crypto.createHash('sha256').update(expectedDedupBasis).digest('hex');
    expect(sentCall.args[0].input.MessageDeduplicationId).toBe(expectedDedupId);
    // channel_id must NOT appear in the message body
    expect(body.channel_id).toBeUndefined();

    expect(body.event_type).toBe('booking.calendar_deleted');
    expect(body.event_id).toBe(BOOKING_ID);
    expect(body.booking_id).toBe(BOOKING_ID);
    expect(body.tenant_id).toBe(TENANT_ID);
    expect(body.calendar_provider).toBe('google');
  });

  test('no changed events → 200, no SQS dispatch', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({ events: [], nextSyncToken: 'sync-tok-2', nextPageToken: null });

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 0);
  });

  test('non-platform event (no booking_id) is skipped, 200 returned', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({
      events: [{ id: 'ext-evt', status: 'confirmed', updated: '2026-05-29T12:00:00Z' }],
      nextSyncToken: 'sync-tok-2',
      nextPageToken: null,
    });

    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 0);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('skipped_non_platform_event'));
    logSpy.mockRestore();
  });

  test('non-active channel status logs warning but still processes delta', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow({ status: { S: 'event_body_private' } }));
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({ events: [], nextSyncToken: 'sync-tok-2', nextPageToken: null });
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('channel_in_non_active_state'));
    warnSpy.mockRestore();
  });

  test('sync_token_race_lost (ConditionalCheckFailedException) → 200; dispatch already succeeded before race detected', async () => {
    // R2: dispatch runs BEFORE advanceSyncToken. So when the conditional update
    // fails (another invocation already advanced), the SQS dispatch has already
    // succeeded. We return 200; SQS FIFO dedup + idempotent consumers handle the
    // concurrent double-dispatch.
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    const condErr = new Error('ConditionalCheckFailed');
    condErr.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(condErr);
    sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mid-race' });
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({
      events: [makeCancelledCalEvent(BOOKING_ID)],
      nextSyncToken: 'sync-tok-2',
      nextPageToken: null,
    });
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    // R2: SQS dispatch succeeded before the race was detected
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 1);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('sync_token_race_lost'));
    logSpy.mockRestore();
  });

  test('listChangedEvents failure → 500 so Google retries', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockRejectedValue(new Error('Calendar API unavailable'));
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(500);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('list_changed_events_failed'));
    errSpy.mockRestore();
  });

  test('oauth_client_failed → 500 so Google retries', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    getOAuthClient.mockRejectedValue(new Error('AccessDenied'));
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(500);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('oauth_client_failed'));
    errSpy.mockRestore();
  });

  test('no coordinator_id in channel row → 200 with warning, no dispatch', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow({ coordinator_id: undefined }));
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('delta_skipped_no_coordinator_id'));
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 0);
    warnSpy.mockRestore();
  });

  test('pagination: loops until nextPageToken exhausted, dispatches all events', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    ddbMock.on(UpdateItemCommand).resolves({});
    sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mid' });
    getOAuthClient.mockResolvedValue(FAKE_AUTH);

    // First call (syncToken) returns nextPageToken
    const bk1 = 'bk-page-1';
    const bk2 = 'bk-page-2';
    listChangedEvents
      .mockResolvedValueOnce({ events: [makeCancelledCalEvent(bk1)], nextSyncToken: null, nextPageToken: 'page-2' })
      .mockResolvedValueOnce({ events: [makeCancelledCalEvent(bk2)], nextSyncToken: 'sync-tok-2', nextPageToken: null });

    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    // First call: syncToken passed, no pageToken
    expect(listChangedEvents.mock.calls[0]).toEqual([FAKE_AUTH, CALENDAR_ID, 'sync-tok-1', null]);
    // Second call: no syncToken, pageToken from first page
    expect(listChangedEvents.mock.calls[1]).toEqual([FAKE_AUTH, CALENDAR_ID, null, 'page-2']);
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 2);
  });

  test('R2: SQS dispatch failure → 500 and stops the loop (syncToken NOT advanced)', async () => {
    // R2: if any dispatchTypedEvent throws, the loop must stop and return 500
    // WITHOUT advancing the syncToken. Google retries; SQS FIFO dedup prevents
    // double-dispatch of the events that were already sent.
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    // UpdateItemCommand (advanceSyncToken) should NOT be called
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    // Two cancelled events, SQS fails on first
    listChangedEvents.mockResolvedValue({
      events: [makeCancelledCalEvent('bk-fail'), makeCancelledCalEvent('bk-ok')],
      nextSyncToken: 'sync-tok-2',
      nextPageToken: null,
    });
    sqsMock.on(SendMessageCommand)
      .rejectsOnce(new Error('SQS throttled'))
      .resolves({ MessageId: 'mid-ok' });

    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    // R2: must return 500 so Google retries
    expect(response.statusCode).toBe(500);
    // Only one SQS attempt was made before the loop stopped
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 1);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('dispatch_typed_event_failed'));
    // syncToken must NOT have been advanced
    expect(ddbMock).not.toHaveReceivedCommand(UpdateItemCommand);
    errSpy.mockRestore();
  });
});

// ─── POST: dispatch failure (infrastructure) ────────────────────────────────────

describe('POST dispatch infrastructure failures', () => {
  test('advanceSyncToken DDB failure (non-conditional) → 500 so Google retries', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    ddbMock.on(UpdateItemCommand).rejects(new Error('DDB write failed'));
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({
      events: [],
      nextSyncToken: 'sync-tok-2',
      nextPageToken: null,
    });

    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(500);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('advance_sync_token_failed'));
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

// ─── resolveResourceId ──────────────────────────────────────────────────────────

describe('resolveResourceId', () => {
  test('returns organizer email when present', () => {
    const evt = { organizer: { email: 'org@x.com' }, attendees: [{ email: 'a@x.com', responseStatus: 'accepted' }] };
    expect(_test.resolveResourceId(evt)).toBe('org@x.com');
  });

  test('falls back to first accepted attendee when no organizer', () => {
    const evt = { attendees: [{ email: 'declined@x.com', responseStatus: 'declined' }, { email: 'acc@x.com', responseStatus: 'accepted' }] };
    expect(_test.resolveResourceId(evt)).toBe('acc@x.com');
  });

  test('returns null when no organizer and no accepted attendee', () => {
    const evt = { attendees: [{ email: 'nt@x.com', responseStatus: 'needsAction' }] };
    expect(_test.resolveResourceId(evt)).toBeNull();
  });

  test('returns null for empty event', () => {
    expect(_test.resolveResourceId({})).toBeNull();
  });
});

// ─── deriveTypedEnvelopes ───────────────────────────────────────────────────────

describe('deriveTypedEnvelopes', () => {
  const PROVIDER = 'google';

  // Booking lookup mock: default returns null (no booking record)
  beforeEach(() => {
    ddbMock.reset();
    sqsMock.reset();
  });

  function makeCalEvent(overrides = {}) {
    return {
      id: 'google-evt-x',
      status: 'confirmed',
      updated: '2026-05-29T12:00:00Z',
      extendedProperties: { private: { booking_id: BOOKING_ID } },
      ...overrides,
    };
  }

  test('cancelled event → booking.calendar_deleted', async () => {
    const envelopes = await _test.deriveTypedEnvelopes(
      makeCalEvent({ status: 'cancelled' }), TENANT_ID, COORDINATOR_ID, PROVIDER
    );
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].event_type).toBe('booking.calendar_deleted');
    expect(envelopes[0].booking_id).toBe(BOOKING_ID);
    expect(envelopes[0].event_id).toBe(BOOKING_ID);
    expect(envelopes[0].calendar_provider).toBe('google');
  });

  test('private event → booking.event_made_private', async () => {
    const envelopes = await _test.deriveTypedEnvelopes(
      makeCalEvent({ visibility: 'private' }), TENANT_ID, COORDINATOR_ID, PROVIDER
    );
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].event_type).toBe('booking.event_made_private');
  });

  test('confidential event → booking.event_made_private', async () => {
    const envelopes = await _test.deriveTypedEnvelopes(
      makeCalEvent({ visibility: 'confidential' }), TENANT_ID, COORDINATOR_ID, PROVIDER
    );
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].event_type).toBe('booking.event_made_private');
  });

  test('event with no booking_id → skipped (empty array)', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    const envelopes = await _test.deriveTypedEnvelopes(
      { id: 'ext', status: 'confirmed', updated: '2026-05-29T12:00:00Z' }, TENANT_ID, COORDINATOR_ID, PROVIDER
    );
    expect(envelopes).toHaveLength(0);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('skipped_non_platform_event'));
    logSpy.mockRestore();
  });

  test('OOO event with overlapping bookings → booking.ooo_overlap_detected', async () => {
    // Mock QueryCommand to return one overlapping booking
    ddbMock.on(QueryCommand).resolves({
      Items: [{ booking_id: { S: 'bk-overlap-1' } }],
    });
    const oooEvent = {
      id: 'ooo-evt',
      eventType: 'outOfOffice',
      updated: '2026-05-29T12:00:00Z',
      start: { dateTime: '2026-05-30T09:00:00Z' },
      end:   { dateTime: '2026-05-30T17:00:00Z' },
    };
    const envelopes = await _test.deriveTypedEnvelopes(oooEvent, TENANT_ID, COORDINATOR_ID, PROVIDER);
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].event_type).toBe('booking.ooo_overlap_detected');
    expect(envelopes[0].booking_id).toBe('bk-overlap-1');
    expect(envelopes[0].ooo_start_at).toBe('2026-05-30T09:00:00Z');
    expect(envelopes[0].overlapping_booking_ids).toEqual(['bk-overlap-1']);
  });

  test('OOO event with NO overlapping bookings → empty array', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    const oooEvent = {
      id: 'ooo-evt',
      eventType: 'outOfOffice',
      updated: '2026-05-29T12:00:00Z',
      start: { dateTime: '2026-05-30T09:00:00Z' },
      end:   { dateTime: '2026-05-30T17:00:00Z' },
    };
    const envelopes = await _test.deriveTypedEnvelopes(oooEvent, TENANT_ID, COORDINATOR_ID, PROVIDER);
    expect(envelopes).toHaveLength(0);
  });

  test('attendee accepted → booking.attendee_accepted', async () => {
    ddbMock.on(GetItemCommand).resolves({}); // no booking record
    const evt = makeCalEvent({
      attendees: [{ email: 'a@x.com', responseStatus: 'accepted' }],
    });
    const envelopes = await _test.deriveTypedEnvelopes(evt, TENANT_ID, COORDINATOR_ID, PROVIDER);
    expect(envelopes.some(e => e.event_type === 'booking.attendee_accepted')).toBe(true);
    const ae = envelopes.find(e => e.event_type === 'booking.attendee_accepted');
    expect(ae.attendee_email).toBe('a@x.com');
    expect(ae.response_status).toBe('accepted');
  });

  test('attendee declined → booking.attendee_declined', async () => {
    ddbMock.on(GetItemCommand).resolves({}); // no booking record
    const evt = makeCalEvent({
      attendees: [{ email: 'b@x.com', responseStatus: 'declined' }],
    });
    const envelopes = await _test.deriveTypedEnvelopes(evt, TENANT_ID, COORDINATOR_ID, PROVIDER);
    expect(envelopes.some(e => e.event_type === 'booking.attendee_declined')).toBe(true);
    const de = envelopes.find(e => e.event_type === 'booking.attendee_declined');
    expect(de.attendee_email).toBe('b@x.com');
  });

  test('booking.calendar_moved when start changed vs Booking record', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        booking_id:  { S: BOOKING_ID },
        tenant_id:   { S: TENANT_ID },
        resource_id: { S: 'org@x.com' },
        start_at:    { S: '2026-05-30T09:00:00Z' },
        end_at:      { S: '2026-05-30T10:00:00Z' },
        status:      { S: 'booked' },
      },
    });
    const evt = makeCalEvent({
      start: { dateTime: '2026-05-30T11:00:00Z' },
      end:   { dateTime: '2026-05-30T12:00:00Z' },
      organizer: { email: 'org@x.com' },
    });
    const envelopes = await _test.deriveTypedEnvelopes(evt, TENANT_ID, COORDINATOR_ID, PROVIDER);
    const moved = envelopes.find(e => e.event_type === 'booking.calendar_moved');
    expect(moved).toBeDefined();
    expect(moved.previous_start_at).toBe('2026-05-30T09:00:00Z');
    expect(moved.new_start_at).toBe('2026-05-30T11:00:00Z');
    expect(moved.previous_end_at).toBe('2026-05-30T10:00:00Z');
    expect(moved.new_end_at).toBe('2026-05-30T12:00:00Z');
  });

  test('booking.calendar_reassigned when organizer differs from Booking.resource_id', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        booking_id:  { S: BOOKING_ID },
        tenant_id:   { S: TENANT_ID },
        resource_id: { S: 'original@x.com' },
        start_at:    { S: '2026-05-30T09:00:00Z' },
        end_at:      { S: '2026-05-30T10:00:00Z' },
        status:      { S: 'booked' },
      },
    });
    const evt = makeCalEvent({
      start: { dateTime: '2026-05-30T09:00:00Z' },  // same start — no move event
      organizer: { email: 'neworg@x.com' },
    });
    const envelopes = await _test.deriveTypedEnvelopes(evt, TENANT_ID, COORDINATOR_ID, PROVIDER);
    const reassigned = envelopes.find(e => e.event_type === 'booking.calendar_reassigned');
    expect(reassigned).toBeDefined();
    expect(reassigned.previous_resource_id).toBe('original@x.com');
    expect(reassigned.new_resource_id).toBe('neworg@x.com');
  });

  test('no Booking record → skipped_no_booking_record logged, attendee events still emitted', async () => {
    ddbMock.on(GetItemCommand).resolves({}); // empty Item → no booking
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    const evt = makeCalEvent({
      attendees: [{ email: 'a@x.com', responseStatus: 'accepted' }],
    });
    const envelopes = await _test.deriveTypedEnvelopes(evt, TENANT_ID, COORDINATOR_ID, PROVIDER);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('skipped_no_booking_record'));
    // Attendee events still fire
    expect(envelopes.some(e => e.event_type === 'booking.attendee_accepted')).toBe(true);
    logSpy.mockRestore();
  });
});

// ─── CI-3b contract test ─────────────────────────────────────────────────────────
// Ensures every event_type the listener can emit has a corresponding derivation
// branch in deriveTypedEnvelopes.  Adding a new type without a handler fails here.

describe('CI-3b: every exported event_type has a reachable derivation branch', () => {
  test('all 7 event_types are covered by deriveTypedEnvelopes', async () => {
    const { EVENT_TYPES } = _test;

    // For each type, verify there exists at least one input configuration that
    // produces that type from deriveTypedEnvelopes.

    // booking.calendar_deleted
    ddbMock.reset();
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'x', status: 'cancelled', updated: '2026-05-29T00:00:00Z', extendedProperties: { private: { booking_id: 'bk-1' } } },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.calendar_deleted')).toBe(true);
    }

    // booking.calendar_moved — requires booking with different start_at
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        booking_id: { S: 'bk-mv' }, tenant_id: { S: TENANT_ID },
        resource_id: { S: 'r@x' }, start_at: { S: '2026-05-30T09:00:00Z' },
        end_at: { S: '2026-05-30T10:00:00Z' }, status: { S: 'booked' },
      },
    });
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'x', status: 'confirmed', updated: '2026-05-29T00:00:00Z',
          extendedProperties: { private: { booking_id: 'bk-mv' } },
          start: { dateTime: '2026-05-30T11:00:00Z' }, organizer: { email: 'r@x' } },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.calendar_moved')).toBe(true);
    }

    // booking.calendar_reassigned — requires booking with different resource_id
    ddbMock.reset();
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        booking_id: { S: 'bk-re' }, tenant_id: { S: TENANT_ID },
        resource_id: { S: 'old@x' }, start_at: { S: '2026-05-30T09:00:00Z' },
        end_at: { S: '2026-05-30T10:00:00Z' }, status: { S: 'booked' },
      },
    });
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'x', status: 'confirmed', updated: '2026-05-29T00:00:00Z',
          extendedProperties: { private: { booking_id: 'bk-re' } },
          start: { dateTime: '2026-05-30T09:00:00Z' }, organizer: { email: 'new@x' } },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.calendar_reassigned')).toBe(true);
    }

    // booking.ooo_overlap_detected
    ddbMock.reset();
    ddbMock.on(QueryCommand).resolves({ Items: [{ booking_id: { S: 'bk-ooo' } }] });
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'ooo', eventType: 'outOfOffice', updated: '2026-05-29T00:00:00Z',
          start: { dateTime: '2026-05-30T09:00:00Z' }, end: { dateTime: '2026-05-30T17:00:00Z' } },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.ooo_overlap_detected')).toBe(true);
    }

    // booking.attendee_accepted
    ddbMock.reset();
    ddbMock.on(GetItemCommand).resolves({});
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'x', status: 'confirmed', updated: '2026-05-29T00:00:00Z',
          extendedProperties: { private: { booking_id: 'bk-aa' } },
          attendees: [{ email: 'a@x', responseStatus: 'accepted' }] },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.attendee_accepted')).toBe(true);
    }

    // booking.attendee_declined
    ddbMock.reset();
    ddbMock.on(GetItemCommand).resolves({});
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'x', status: 'confirmed', updated: '2026-05-29T00:00:00Z',
          extendedProperties: { private: { booking_id: 'bk-ad' } },
          attendees: [{ email: 'b@x', responseStatus: 'declined' }] },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.attendee_declined')).toBe(true);
    }

    // booking.event_made_private
    ddbMock.reset();
    {
      const envelopes = await _test.deriveTypedEnvelopes(
        { id: 'x', status: 'confirmed', visibility: 'private', updated: '2026-05-29T00:00:00Z',
          extendedProperties: { private: { booking_id: 'bk-priv' } } },
        TENANT_ID, COORDINATOR_ID, 'google'
      );
      expect(envelopes.some(e => e.event_type === 'booking.event_made_private')).toBe(true);
    }

    // Assert the contract: the enumerated set equals the implemented set
    expect(EVENT_TYPES).toHaveLength(7);
    expect(EVENT_TYPES).toEqual(expect.arrayContaining([
      'booking.calendar_deleted',
      'booking.calendar_moved',
      'booking.calendar_reassigned',
      'booking.ooo_overlap_detected',
      'booking.attendee_accepted',
      'booking.attendee_declined',
      'booking.event_made_private',
    ]));
  });
});

// ─── Error-path coverage for catch blocks ───────────────────────────────────────

describe('deriveTypedEnvelopes error paths', () => {
  beforeEach(() => { ddbMock.reset(); sqsMock.reset(); });

  test('booking_lookup_failed: DDB error is warned, attendee events still emitted', async () => {
    ddbMock.on(GetItemCommand).rejects(new Error('DDB read error'));
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const evt = {
      id: 'x', status: 'confirmed', updated: '2026-05-29T00:00:00Z',
      extendedProperties: { private: { booking_id: BOOKING_ID } },
      attendees: [{ email: 'a@x', responseStatus: 'accepted' }],
    };
    const envelopes = await _test.deriveTypedEnvelopes(evt, TENANT_ID, COORDINATOR_ID, 'google');
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('booking_lookup_failed'));
    // Attendee events still fire despite booking lookup failure
    expect(envelopes.some(e => e.event_type === 'booking.attendee_accepted')).toBe(true);
    warnSpy.mockRestore();
  });

  test('ooo_booking_query_failed: DDB query error is warned, empty overlapping_ids → no envelope', async () => {
    ddbMock.on(QueryCommand).rejects(new Error('DDB query error'));
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const oooEvent = {
      id: 'ooo', eventType: 'outOfOffice', updated: '2026-05-29T00:00:00Z',
      start: { dateTime: '2026-05-30T09:00:00Z' }, end: { dateTime: '2026-05-30T17:00:00Z' },
    };
    const envelopes = await _test.deriveTypedEnvelopes(oooEvent, TENANT_ID, COORDINATOR_ID, 'google');
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('ooo_booking_query_failed'));
    expect(envelopes).toHaveLength(0);
    warnSpy.mockRestore();
  });
});

describe('handler edge cases', () => {
  test('base64-encoded body is decoded before processing', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);
    listChangedEvents.mockResolvedValue({ events: [], nextSyncToken: 'tok', nextPageToken: null });

    const base64Body = Buffer.from('{}').toString('base64');
    const response = await handler({
      requestContext: { http: { method: 'POST' } },
      headers: validHeaders(),
      body: base64Body,
      isBase64Encoded: true,
    });
    expect(response.statusCode).toBe(200);
  });
});

// ─── R1: cross-tenant booking guard ─────────────────────────────────────────────

describe('R1: cross-tenant booking guard', () => {
  beforeEach(() => { ddbMock.reset(); sqsMock.reset(); });

  test('booking whose tenantId differs from channel tenantId yields no calendar_moved or calendar_reassigned', async () => {
    // booking_id on the calendar event belongs to a DIFFERENT tenant
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        booking_id:  { S: BOOKING_ID },
        tenant_id:   { S: 'FOREIGN_TENANT' },   // <-- different from TENANT_ID
        resource_id: { S: 'original@x.com' },
        start_at:    { S: '2026-05-30T09:00:00Z' },
        end_at:      { S: '2026-05-30T10:00:00Z' },
        status:      { S: 'booked' },
      },
    });

    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const calEvent = {
      id: 'google-evt-cross',
      status: 'confirmed',
      updated: '2026-05-29T12:00:00Z',
      extendedProperties: { private: { booking_id: BOOKING_ID } },
      start: { dateTime: '2026-05-30T11:00:00Z' }, // different start
      organizer: { email: 'neworg@x.com' },          // different organizer
    };
    const envelopes = await _test.deriveTypedEnvelopes(calEvent, TENANT_ID, COORDINATOR_ID, 'google');

    // The cross-tenant guard must have triggered a warning
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('cross_tenant_booking_id_detected')
    );
    // Booking treated as null → no calendar_moved and no calendar_reassigned
    expect(envelopes.find(e => e.event_type === 'booking.calendar_moved')).toBeUndefined();
    expect(envelopes.find(e => e.event_type === 'booking.calendar_reassigned')).toBeUndefined();
    warnSpy.mockRestore();
  });
});

// ─── R3: Google 410 syncToken expiry ────────────────────────────────────────────

describe('R3: syncToken 410 expiry clears token and returns 200', () => {
  test('410 from listChangedEvents: REMOVE last_sync_token, warn, return 200', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    // listChangedEvents throws a 410
    const err410 = new Error('Sync token expired');
    err410.code = 410;
    listChangedEvents.mockRejectedValue(err410);
    // DDB UpdateItemCommand for REMOVE succeeds
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);

    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));

    expect(response.statusCode).toBe(200);
    // The REMOVE UpdateItemCommand must have been sent
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
    const updateCall = ddbMock.commandCalls(UpdateItemCommand)[0];
    expect(updateCall.args[0].input.UpdateExpression).toBe('REMOVE last_sync_token');
    // Warning emitted
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('sync_token_expired_cleared'));
    // No SQS dispatch
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageCommand, 0);
    warnSpy.mockRestore();
  });

  test('410 via err.response.status also triggers the clear path', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    const err410 = new Error('Gone');
    err410.response = { status: 410 };
    listChangedEvents.mockRejectedValue(err410);
    ddbMock.on(UpdateItemCommand).resolves({});
    getOAuthClient.mockResolvedValue(FAKE_AUTH);

    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(200);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('sync_token_expired_cleared'));
    warnSpy.mockRestore();
  });

  test('non-410 error still returns 500', async () => {
    ddbMock.on(GetItemCommand).resolves(makeChannelRow());
    const err500 = new Error('Internal Server Error');
    err500.code = 500;
    listChangedEvents.mockRejectedValue(err500);
    getOAuthClient.mockResolvedValue(FAKE_AUTH);

    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handler(makePostEvent(validHeaders()));
    expect(response.statusCode).toBe(500);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('list_changed_events_failed'));
    errSpy.mockRestore();
  });
});
