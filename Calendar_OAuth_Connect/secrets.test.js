'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
  CreateSecretCommand,
  PutSecretValueCommand,
  DescribeSecretCommand,
} = require('@aws-sdk/client-secrets-manager');

const secrets = require('./secrets');

const smMock = mockClient(SecretsManagerClient);
beforeEach(() => smMock.reset());

const notFound = Object.assign(new Error('not found'), { name: 'ResourceNotFoundException' });

describe('readPlatformApp', () => {
  test('returns client_id/secret/redirect_uri', async () => {
    smMock.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ client_id: 'cid', client_secret: 'cs', redirect_uri: 'https://r' }),
    });
    await expect(secrets.readPlatformApp()).resolves.toEqual({ client_id: 'cid', client_secret: 'cs', redirect_uri: 'https://r' });
  });

  test('redirect_uri optional → null', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ client_id: 'cid', client_secret: 'cs' }) });
    expect((await secrets.readPlatformApp()).redirect_uri).toBeNull();
  });

  test('missing client_secret → throws', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ client_id: 'cid' }) });
    await expect(secrets.readPlatformApp()).rejects.toThrow(/client_secret/);
  });

  test('no SecretString → throws', async () => {
    smMock.on(GetSecretValueCommand).resolves({});
    await expect(secrets.readPlatformApp()).rejects.toThrow(/SecretString/);
  });
});

describe('buildSecretPath / slot-poisoning guard', () => {
  test('canonical path', () => {
    expect(secrets.buildSecretPath('MYR384719', 'maya@example.org')).toBe('picasso/scheduling/oauth/MYR384719/maya@example.org');
  });
  test.each([
    ['../etc', 'maya'],
    ['MYR', 'a/b'],
    ['', 'maya'],
    ['MYR', ''],
    ['has space', 'maya'],
  ])('rejects path-traversal / invalid (%p,%p)', (t, c) => {
    expect(() => secrets.buildSecretPath(t, c)).toThrow();
  });

  test('rejects reserved _-prefixed tenant (cannot clobber platform/state secrets)', () => {
    expect(() => secrets.buildSecretPath('_platform', 'google-app')).toThrow(/reserved/);
  });
});

describe('writeCoordinator', () => {
  const base = {
    tenantId: 'MYR384719',
    coordinatorId: 'maya@example.org',
    coordinatorEmail: 'maya@example.org',
    refreshToken: '1//refresh',
    clientId: 'cid',
    clientSecret: 'cs',
    scopes: ['https://www.googleapis.com/auth/calendar.events'],
    nowIso: '2026-06-05T00:00:00.000Z',
  };

  test('NEW secret → CreateSecret with back-compat shape (incl. client_id/client_secret)', async () => {
    smMock.on(DescribeSecretCommand).rejects(notFound);
    smMock.on(CreateSecretCommand).resolves({});
    const path = await secrets.writeCoordinator(base);
    expect(path).toBe('picasso/scheduling/oauth/MYR384719/maya@example.org');
    expect(smMock).toHaveReceivedCommandTimes(CreateSecretCommand, 1);
    expect(smMock).toHaveReceivedCommandTimes(PutSecretValueCommand, 0);
    const written = JSON.parse(smMock.commandCalls(CreateSecretCommand)[0].args[0].input.SecretString);
    // Shipped reader contract (oauth-client.js + availability.js): these MUST be present.
    expect(written.client_id).toBe('cid');
    expect(written.client_secret).toBe('cs');
    expect(written.refresh_token).toBe('1//refresh');
    expect(written.coordinator_email).toBe('maya@example.org');
    // D2 additive fields.
    expect(written.calendar_id).toBe('primary');
    expect(written.connected_at).toBe('2026-06-05T00:00:00.000Z');
    expect(written.status).toBe('connected');
    expect(written.provider).toBe('google');
  });

  test('EXISTING secret → PutSecretValue (reconnect)', async () => {
    smMock.on(DescribeSecretCommand).resolves({ ARN: 'arn:...' });
    smMock.on(PutSecretValueCommand).resolves({});
    await secrets.writeCoordinator(base);
    expect(smMock).toHaveReceivedCommandTimes(PutSecretValueCommand, 1);
    expect(smMock).toHaveReceivedCommandTimes(CreateSecretCommand, 0);
  });

  test('invalid coordinatorId → throws before any write', async () => {
    await expect(secrets.writeCoordinator({ ...base, coordinatorId: 'bad/slot' })).rejects.toThrow();
    expect(smMock).toHaveReceivedCommandTimes(DescribeSecretCommand, 0);
  });

  test('DescribeSecret non-NotFound error propagates (no blind create)', async () => {
    smMock.on(DescribeSecretCommand).rejects(Object.assign(new Error('throttled'), { name: 'ThrottlingException' }));
    await expect(secrets.writeCoordinator(base)).rejects.toThrow('throttled');
    expect(smMock).toHaveReceivedCommandTimes(CreateSecretCommand, 0);
  });

  test('CreateSecret TOCTOU race (ResourceExistsException) → falls back to PutSecretValue', async () => {
    smMock.on(DescribeSecretCommand).rejects(notFound);
    smMock.on(CreateSecretCommand).rejects(Object.assign(new Error('exists'), { name: 'ResourceExistsException' }));
    smMock.on(PutSecretValueCommand).resolves({});
    await expect(secrets.writeCoordinator(base)).resolves.toContain('MYR384719');
    expect(smMock).toHaveReceivedCommandTimes(PutSecretValueCommand, 1);
  });
});

describe('readCoordinator', () => {
  test('absent → null', async () => {
    smMock.on(GetSecretValueCommand).rejects(notFound);
    await expect(secrets.readCoordinator({ tenantId: 'MYR', coordinatorId: 'maya' })).resolves.toBeNull();
  });
  test('present → parsed', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ refresh_token: 'x', status: 'connected' }) });
    await expect(secrets.readCoordinator({ tenantId: 'MYR', coordinatorId: 'maya' })).resolves.toMatchObject({ status: 'connected' });
  });

  test('unexpected (non-NotFound) error rethrows', async () => {
    smMock.on(GetSecretValueCommand).rejects(Object.assign(new Error('throttled'), { name: 'ThrottlingException' }));
    await expect(secrets.readCoordinator({ tenantId: 'MYR', coordinatorId: 'maya' })).rejects.toThrow('throttled');
  });

  test('present-but-non-string SecretString → null (treated as absent)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretBinary: new Uint8Array([1, 2]) });
    await expect(secrets.readCoordinator({ tenantId: 'MYR', coordinatorId: 'maya' })).resolves.toBeNull();
  });
});

describe('markDisconnected', () => {
  test('stamps status:revoked + disconnected_at, preserves other fields', async () => {
    smMock.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ client_id: 'cid', refresh_token: 'rt', status: 'connected', connected_at: 'X' }),
    });
    smMock.on(PutSecretValueCommand).resolves({});
    const out = await secrets.markDisconnected({ tenantId: 'MYR', coordinatorId: 'maya', nowIso: '2026-06-05T01:00:00.000Z' });
    expect(out.found).toBe(true);
    const written = JSON.parse(smMock.commandCalls(PutSecretValueCommand)[0].args[0].input.SecretString);
    expect(written.status).toBe('revoked');
    expect(written.disconnected_at).toBe('2026-06-05T01:00:00.000Z');
    expect(written.connected_at).toBe('X'); // preserved for audit
    expect(written.client_id).toBe('cid');
  });

  test('absent secret → { found:false }, no write', async () => {
    smMock.on(GetSecretValueCommand).rejects(notFound);
    const out = await secrets.markDisconnected({ tenantId: 'MYR', coordinatorId: 'maya', nowIso: 'X' });
    expect(out.found).toBe(false);
    expect(smMock).toHaveReceivedCommandTimes(PutSecretValueCommand, 0);
  });

  test('unexpected (non-NotFound) read error rethrows', async () => {
    smMock.on(GetSecretValueCommand).rejects(Object.assign(new Error('kms denied'), { name: 'AccessDeniedException' }));
    await expect(secrets.markDisconnected({ tenantId: 'MYR', coordinatorId: 'maya', nowIso: 'X' })).rejects.toThrow('kms denied');
  });

  test('present-but-non-string SecretString → { found:false }, no write (no JSON.parse crash)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretBinary: new Uint8Array([1]) });
    const out = await secrets.markDisconnected({ tenantId: 'MYR', coordinatorId: 'maya', nowIso: 'X' });
    expect(out.found).toBe(false);
    expect(smMock).toHaveReceivedCommandTimes(PutSecretValueCommand, 0);
  });
});
