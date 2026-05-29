'use strict';

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

// Mock oauth-client + calendar-watch BEFORE requiring index.
const mockOauth = jest.fn();
jest.mock('./oauth-client', () => ({
  getOAuthClient: (...args) => mockOauth(...args),
}));

const mockRegisterWatch = jest.fn();
const mockSeedSync = jest.fn();
const mockStopWatch = jest.fn();
jest.mock('./calendar-watch', () => ({
  registerWatch: (...args) => mockRegisterWatch(...args),
  seedInitialSyncToken: (...args) => mockSeedSync(...args),
  stopWatch: (...args) => mockStopWatch(...args),
}));

process.env.LISTENER_URL = 'https://listener.example/';
process.env.CALENDAR_WATCH_CHANNELS_TABLE = 'picasso-calendar-watch-channels-staging';
process.env.ENVIRONMENT = 'staging';

const { handler, _test } = require('./index');

const ddbMock = mockClient(DynamoDBClient);

beforeEach(() => {
  ddbMock.reset();
  mockOauth.mockReset();
  mockRegisterWatch.mockReset();
  mockSeedSync.mockReset();
  mockStopWatch.mockReset();
});

// ─── _test helpers (pure functions) ─────────────────────────────────────────────

describe('validateInput', () => {
  test('returns fields with calendarId defaulting to primary', () => {
    const result = _test.validateInput({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });
    expect(result).toEqual({
      tenantId: 'MYR384719',
      coordinatorId: 'test-coordinator',
      calendarId: 'primary',
    });
  });

  test('honors explicit calendar_id', () => {
    const result = _test.validateInput({
      tenant_id: 'MYR',
      coordinator_id: 'coord_1',
      calendar_id: 'shared@group.calendar.google.com',
    });
    expect(result.calendarId).toBe('shared@group.calendar.google.com');
  });

  test('accepts email-like coordinator_id', () => {
    const result = _test.validateInput({ tenant_id: 'T1', coordinator_id: 'maya@org.org' });
    expect(result.coordinatorId).toBe('maya@org.org');
  });

  test.each([
    [null, 'Input must be a JSON object'],
    [undefined, 'Input must be a JSON object'],
    ['string', 'Input must be a JSON object'],
    [{}, 'tenant_id is required'],
    [{ tenant_id: 'X' }, 'coordinator_id is required'],
    [{ tenant_id: 123, coordinator_id: 'c' }, 'tenant_id is required'],
    [{ tenant_id: 'X', coordinator_id: 99 }, 'coordinator_id is required'],
  ])('rejects malformed input: %p', (input, expectedMsg) => {
    expect(() => _test.validateInput(input)).toThrow(expectedMsg);
  });

  test.each([
    ['../../../other-tenant'],
    ['tenant/with/slash'],
    ['tenant with space'],
    ['a'.repeat(65)],
  ])('rejects path-injection / malformed tenant_id: %p', (badTenant) => {
    expect(() => _test.validateInput({ tenant_id: badTenant, coordinator_id: 'c' }))
      .toThrow('tenant_id is required and must match');
  });

  test.each([
    ['coord/../../escape'],
    ['coord with space'],
    ['a'.repeat(129)],
  ])('rejects path-injection / malformed coordinator_id: %p', (badCoord) => {
    expect(() => _test.validateInput({ tenant_id: 'T1', coordinator_id: badCoord }))
      .toThrow('coordinator_id is required and must match');
  });
});

describe('generateChannelId / generateChannelToken / sha256Hex', () => {
  test('channel id is a UUID', () => {
    expect(_test.generateChannelId())
      .toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  test('subsequent channel ids differ', () => {
    expect(_test.generateChannelId()).not.toBe(_test.generateChannelId());
  });

  test('channel token is 64 hex chars (256 bits of entropy)', () => {
    expect(_test.generateChannelToken()).toMatch(/^[0-9a-f]{64}$/);
  });

  test('subsequent tokens differ', () => {
    expect(_test.generateChannelToken()).not.toBe(_test.generateChannelToken());
  });

  test('sha256Hex produces stable 64-char hex', () => {
    const a = _test.sha256Hex('hello');
    expect(a).toBe('2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824');
    expect(a).toBe(_test.sha256Hex('hello'));
    expect(a).not.toBe(_test.sha256Hex('world'));
  });
});

// ─── full handler flow ──────────────────────────────────────────────────────────

function setUpHappyPath() {
  mockOauth.mockResolvedValue({ _authClient: 'mock' });
  mockSeedSync.mockResolvedValue({ syncToken: 'seed-tok-xyz', pages: 1, totalSeen: 5 });
  mockRegisterWatch.mockResolvedValue({
    resourceId: 'res-abc',
    resourceUri: 'https://www.googleapis.com/calendar/v3/calendars/primary/events',
    expiration: '1735776000000',
  });
  ddbMock.on(PutItemCommand).resolves({});
}

describe('handler — happy path', () => {
  test('orchestrates onboarding, returns channel_id + expiration, NO secret_id', async () => {
    setUpHappyPath();

    const result = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });

    expect(result).toMatchObject({ expiration: '1735776000000', last_sync_token_seeded: true });
    expect(result.channel_id).toMatch(/^[0-9a-f-]{36}$/);
    // G6/G10: no raw-token secret is created, so no secret_id leaks in the response
    expect(result.secret_id).toBeUndefined();

    expect(mockOauth).toHaveBeenCalledWith({ tenantId: 'MYR384719', coordinatorId: 'test-coordinator' });
    expect(mockSeedSync).toHaveBeenCalledWith({ _authClient: 'mock' }, 'primary');

    // events.watch receives the raw channel token; nothing else stores it
    const watchArgs = mockRegisterWatch.mock.calls[0];
    expect(watchArgs[0]).toEqual({ _authClient: 'mock' });
    expect(watchArgs[1]).toBe('primary');
    expect(watchArgs[2]).toBe(result.channel_id);
    const rawToken = watchArgs[3];
    expect(rawToken).toMatch(/^[0-9a-f]{64}$/);
    expect(watchArgs[4]).toBe('https://listener.example/');

    // DDB row holds the SHA-256 hash, never the raw token
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(1);
    const ddbInput = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(ddbInput.TableName).toBe('picasso-calendar-watch-channels-staging');
    expect(ddbInput.ConditionExpression).toBe('attribute_not_exists(channel_id)');
    expect(ddbInput.Item.channel_id.S).toBe(result.channel_id);
    expect(ddbInput.Item.tenant_id.S).toBe('MYR384719');
    expect(ddbInput.Item.coordinator_id.S).toBe('test-coordinator');
    expect(ddbInput.Item.calendar_id.S).toBe('primary');
    expect(ddbInput.Item.calendar_provider.S).toBe('google');
    expect(ddbInput.Item.status.S).toBe('active');
    expect(ddbInput.Item.expiration.N).toBe('1735776000000');
    expect(ddbInput.Item.last_sync_token.S).toBe('seed-tok-xyz');
    expect(ddbInput.Item.resource_id.S).toBe('res-abc');
    expect(ddbInput.Item.resource_uri.S).toBe('https://www.googleapis.com/calendar/v3/calendars/primary/events');

    // Security-critical: raw token absent from DDB; hash present + correct
    expect(JSON.stringify(ddbInput.Item)).not.toContain(rawToken);
    const expectedHash = crypto.createHash('sha256').update(rawToken, 'utf8').digest('hex');
    expect(ddbInput.Item.channel_token_sha256.S).toBe(expectedHash);

    // No compensation on success
    expect(mockStopWatch).not.toHaveBeenCalled();
  });

  test('omits last_sync_token when seed returns null (and still writes the row)', async () => {
    setUpHappyPath();
    mockSeedSync.mockResolvedValue({ syncToken: null, pages: 1, totalSeen: 0 });

    const result = await handler({ tenant_id: 'T1', coordinator_id: 'c' });
    expect(result.last_sync_token_seeded).toBe(false);

    const ddbInput = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(ddbInput.Item.last_sync_token).toBeUndefined();
  });

  test('omits resource_id / resource_uri when watch response lacks them', async () => {
    setUpHappyPath();
    mockRegisterWatch.mockResolvedValue({ resourceId: null, resourceUri: null, expiration: '999' });

    await handler({ tenant_id: 'T1', coordinator_id: 'c' });

    const ddbInput = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(ddbInput.Item.resource_id).toBeUndefined();
    expect(ddbInput.Item.resource_uri).toBeUndefined();
    expect(ddbInput.Item.expiration.N).toBe('999');
  });
});

describe('handler — failure paths', () => {
  test('rejects malformed input before any AWS or Google call', async () => {
    await expect(handler({})).rejects.toThrow('tenant_id is required');
    expect(mockOauth).not.toHaveBeenCalled();
    expect(mockSeedSync).not.toHaveBeenCalled();
    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
  });

  test('propagates OAuth fetch failure', async () => {
    mockOauth.mockRejectedValue(new Error('AccessDeniedException'));
    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('AccessDeniedException');
    expect(mockSeedSync).not.toHaveBeenCalled();
  });

  test('propagates sync-token seeding failure before watch or DDB write', async () => {
    mockOauth.mockResolvedValue({ _authClient: 'mock' });
    mockSeedSync.mockRejectedValue(new Error('Initial sync-token seed exceeded maxPages=50'));

    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('maxPages=50');

    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).not.toHaveBeenCalled();
  });

  test('events.watch failure: no DDB write, no compensation (nothing to revoke yet)', async () => {
    setUpHappyPath();
    mockRegisterWatch.mockRejectedValue(new Error('Google quota exceeded'));

    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('Google quota exceeded');

    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).not.toHaveBeenCalled();
  });

  // G3: the dangerous case — watch is LIVE, DDB write fails → must revoke
  test('PutItem failure AFTER watch → compensating events.stop revokes the channel', async () => {
    setUpHappyPath();
    ddbMock.on(PutItemCommand).rejects(
      Object.assign(new Error('ddb throttled'), { name: 'ProvisionedThroughputExceededException' })
    );
    mockStopWatch.mockResolvedValue(undefined);

    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('ddb throttled');

    // Compensation fired with the channel + resource id from the live watch
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
    const stopArgs = mockStopWatch.mock.calls[0];
    expect(stopArgs[0]).toEqual({ _authClient: 'mock' });
    expect(stopArgs[1]).toBe(mockRegisterWatch.mock.calls[0][2]); // channelId
    expect(stopArgs[2]).toBe('res-abc'); // resourceId
  });

  test('PutItem failure AND compensation failure → original error still propagates', async () => {
    setUpHappyPath();
    ddbMock.on(PutItemCommand).rejects(new Error('ddb down'));
    mockStopWatch.mockRejectedValue(new Error('stop also failed'));

    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('ddb down'); // not 'stop also failed'
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
  });

  // G1: null expiration would write String(null)="null" into a DDB Number —
  // catch it BEFORE the write and revoke the live channel.
  test('null expiration from watch → throws + compensates (no DDB write)', async () => {
    setUpHappyPath();
    mockRegisterWatch.mockResolvedValue({ resourceId: 'res-x', resourceUri: 'u', expiration: null });
    mockStopWatch.mockResolvedValue(undefined);

    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('non-numeric expiration');

    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
    expect(mockStopWatch.mock.calls[0][2]).toBe('res-x');
  });

  test('non-numeric expiration string → throws + compensates', async () => {
    setUpHappyPath();
    mockRegisterWatch.mockResolvedValue({ resourceId: 'res-y', resourceUri: 'u', expiration: 'soon' });
    mockStopWatch.mockResolvedValue(undefined);

    await expect(handler({ tenant_id: 'T1', coordinator_id: 'c' }))
      .rejects.toThrow('non-numeric expiration');
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
  });
});

describe('handler — env requirements', () => {
  function withEnv(overrides, fn) {
    const saved = { ...process.env };
    Object.assign(process.env, overrides);
    for (const k of Object.keys(overrides)) {
      if (overrides[k] === undefined) delete process.env[k];
    }
    return jest.isolateModulesAsync(async () => {
      const { handler: freshHandler } = require('./index');
      await fn(freshHandler);
    }).finally(() => {
      process.env = saved;
    });
  }

  test('throws if CALENDAR_WATCH_CHANNELS_TABLE is unset', async () => {
    await withEnv({ CALENDAR_WATCH_CHANNELS_TABLE: undefined }, async (h) => {
      await expect(h({ tenant_id: 'T1', coordinator_id: 'c' }))
        .rejects.toThrow('CALENDAR_WATCH_CHANNELS_TABLE env var is required');
    });
  });

  test('throws if LISTENER_URL is unset', async () => {
    await withEnv({ LISTENER_URL: undefined }, async (h) => {
      await expect(h({ tenant_id: 'T1', coordinator_id: 'c' }))
        .rejects.toThrow('LISTENER_URL env var is required and must be https://');
    });
  });

  test('throws if LISTENER_URL is not https', async () => {
    await withEnv({ LISTENER_URL: 'http://insecure.example/' }, async (h) => {
      await expect(h({ tenant_id: 'T1', coordinator_id: 'c' }))
        .rejects.toThrow('must be https://');
    });
  });
});
