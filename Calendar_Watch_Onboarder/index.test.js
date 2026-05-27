'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  SecretsManagerClient,
  CreateSecretCommand,
} = require('@aws-sdk/client-secrets-manager');
const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

// Mock oauth-client + calendar-watch BEFORE requiring index.
const mockOauth = jest.fn();
jest.mock('./oauth-client', () => ({
  getOAuthClient: (...args) => mockOauth(...args),
  _resetCacheForTests: jest.fn(),
}));

const mockRegisterWatch = jest.fn();
const mockSeedSync = jest.fn();
jest.mock('./calendar-watch', () => ({
  registerWatch: (...args) => mockRegisterWatch(...args),
  seedInitialSyncToken: (...args) => mockSeedSync(...args),
}));

process.env.LISTENER_URL = 'https://listener.example/';
process.env.CALENDAR_WATCH_CHANNELS_TABLE = 'picasso-calendar-watch-channels-staging';
process.env.CHANNEL_TOKEN_SECRET_PREFIX = 'picasso/scheduling/channel-token';
process.env.ENVIRONMENT = 'staging';

const handlerModule = require('./index');
const { handler, _test } = handlerModule;

const smMock = mockClient(SecretsManagerClient);
const ddbMock = mockClient(DynamoDBClient);

beforeEach(() => {
  smMock.reset();
  ddbMock.reset();
  mockOauth.mockReset();
  mockRegisterWatch.mockReset();
  mockSeedSync.mockReset();
});

// ─── _test helpers (pure functions) ─────────────────────────────────────────────

describe('validateInput', () => {
  test('returns trimmed fields with calendarId defaulting to primary', () => {
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
      coordinator_id: 'c',
      calendar_id: 'shared@group.calendar.google.com',
    });
    expect(result.calendarId).toBe('shared@group.calendar.google.com');
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
});

describe('generateChannelId / generateChannelToken / sha256Hex', () => {
  test('channel id is a UUID', () => {
    const id = _test.generateChannelId();
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  test('channel token is 64 hex chars (256 bits of entropy)', () => {
    const tok = _test.generateChannelToken();
    expect(tok).toMatch(/^[0-9a-f]{64}$/);
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
  smMock.on(CreateSecretCommand).resolves({ ARN: 'arn:...:channel-token-abc' });
  ddbMock.on(PutItemCommand).resolves({});
}

describe('handler — happy path', () => {
  test('orchestrates the full onboarding flow and returns channel_id + expiration', async () => {
    setUpHappyPath();

    const result = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });

    expect(result).toMatchObject({
      expiration: '1735776000000',
      last_sync_token_seeded: true,
    });
    expect(result.channel_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(result.secret_id).toBe(`picasso/scheduling/channel-token/${result.channel_id}`);

    // OAuth fetched once
    expect(mockOauth).toHaveBeenCalledWith({
      tenantId: 'MYR384719',
      coordinatorId: 'test-coordinator',
    });

    // Sync-token seeded once against primary
    expect(mockSeedSync).toHaveBeenCalledWith({ _authClient: 'mock' }, 'primary');

    // Secret created before watch
    expect(smMock.commandCalls(CreateSecretCommand)).toHaveLength(1);
    const secretCall = smMock.commandCalls(CreateSecretCommand)[0].args[0].input;
    expect(secretCall.Name).toBe(result.secret_id);
    const secretBody = JSON.parse(secretCall.SecretString);
    expect(secretBody.channel_token).toMatch(/^[0-9a-f]{64}$/);
    expect(secretBody.tenant_id).toBe('MYR384719');
    expect(secretCall.Tags).toEqual(expect.arrayContaining([
      { Key: 'Subphase', Value: 'B5' },
      { Key: 'tenant_id', Value: 'MYR384719' },
      { Key: 'channel_id', Value: result.channel_id },
    ]));

    // events.watch called with the same token stored in Secrets Manager
    const watchArgs = mockRegisterWatch.mock.calls[0];
    expect(watchArgs[0]).toEqual({ _authClient: 'mock' });
    expect(watchArgs[1]).toBe('primary');
    expect(watchArgs[2]).toBe(result.channel_id);
    expect(watchArgs[3]).toBe(secretBody.channel_token);
    expect(watchArgs[4]).toBe('https://listener.example/');

    // DDB row written with the SHA-256 hash, not the raw token, plus all
    // expected fields and the conditional write.
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

    // Raw token must NEVER be written to DDB
    const ddbJson = JSON.stringify(ddbInput.Item);
    expect(ddbJson).not.toContain(secretBody.channel_token);
    // SHA-256 hash present
    const crypto = require('crypto');
    const expectedHash = crypto.createHash('sha256').update(secretBody.channel_token, 'utf8').digest('hex');
    expect(ddbInput.Item.channel_token_sha256.S).toBe(expectedHash);
  });

  test('omits last_sync_token when seed returns null', async () => {
    setUpHappyPath();
    mockSeedSync.mockResolvedValue({ syncToken: null, pages: 1, totalSeen: 0 });

    const result = await handler({ tenant_id: 'T', coordinator_id: 'c' });
    expect(result.last_sync_token_seeded).toBe(false);

    const ddbInput = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(ddbInput.Item.last_sync_token).toBeUndefined();
  });

  test('omits resource_id / resource_uri when watch response lacks them', async () => {
    setUpHappyPath();
    mockRegisterWatch.mockResolvedValue({ resourceId: null, resourceUri: null, expiration: '999' });

    await handler({ tenant_id: 'T', coordinator_id: 'c' });

    const ddbInput = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(ddbInput.Item.resource_id).toBeUndefined();
    expect(ddbInput.Item.resource_uri).toBeUndefined();
    expect(ddbInput.Item.expiration.N).toBe('999');
  });
});

describe('handler — failure paths', () => {
  test('propagates malformed input as thrown error before any AWS or Google call', async () => {
    await expect(handler({})).rejects.toThrow('tenant_id is required');
    expect(mockOauth).not.toHaveBeenCalled();
    expect(mockSeedSync).not.toHaveBeenCalled();
    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(smMock.commandCalls(CreateSecretCommand)).toHaveLength(0);
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
  });

  test('propagates OAuth fetch failure (Secrets Manager 403)', async () => {
    mockOauth.mockRejectedValue(new Error('AccessDeniedException'));
    await expect(handler({ tenant_id: 'T', coordinator_id: 'c' }))
      .rejects.toThrow('AccessDeniedException');
    expect(mockSeedSync).not.toHaveBeenCalled();
    expect(smMock.commandCalls(CreateSecretCommand)).toHaveLength(0);
  });

  test('propagates sync-token seeding failure before any state mutation', async () => {
    mockOauth.mockResolvedValue({ _authClient: 'mock' });
    mockSeedSync.mockRejectedValue(new Error('Initial sync-token seed exceeded maxPages=50'));

    await expect(handler({ tenant_id: 'T', coordinator_id: 'c' }))
      .rejects.toThrow('maxPages=50');

    expect(smMock.commandCalls(CreateSecretCommand)).toHaveLength(0);
    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
  });

  test('propagates secret-create failure before events.watch is called', async () => {
    setUpHappyPath();
    smMock.on(CreateSecretCommand).rejects(new Error('ResourceExistsException'));

    await expect(handler({ tenant_id: 'T', coordinator_id: 'c' }))
      .rejects.toThrow('ResourceExistsException');
    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
  });

  test('propagates events.watch failure leaving secret-create-side-effect (acknowledged)', async () => {
    setUpHappyPath();
    mockRegisterWatch.mockRejectedValue(new Error('Google quota exceeded'));

    await expect(handler({ tenant_id: 'T', coordinator_id: 'c' }))
      .rejects.toThrow('Google quota exceeded');

    // Secret WAS created (acceptable: caller may retry; orphaned secrets are
    // cleaned up by B6 offboarding or by future housekeeping)
    expect(smMock.commandCalls(CreateSecretCommand)).toHaveLength(1);
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
  });

  test('propagates DDB PutItem ConditionalCheckFailed (UUID collision; astronomically rare)', async () => {
    setUpHappyPath();
    ddbMock.on(PutItemCommand).rejects(Object.assign(new Error('cond fail'), { name: 'ConditionalCheckFailedException' }));

    await expect(handler({ tenant_id: 'T', coordinator_id: 'c' }))
      .rejects.toThrow('cond fail');
  });
});

describe('handler — env requirements', () => {
  test('throws if LISTENER_URL is unset', async () => {
    delete process.env.LISTENER_URL;
    jest.resetModules();
    const { handler: freshHandler } = require('./index');
    await expect(freshHandler({ tenant_id: 'T', coordinator_id: 'c' }))
      .rejects.toThrow('LISTENER_URL env var is required');
    process.env.LISTENER_URL = 'https://listener.example/';
  });
});
