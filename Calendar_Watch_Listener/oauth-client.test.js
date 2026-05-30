'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

jest.mock('google-auth-library', () => {
  const setCredentials = jest.fn();
  function OAuth2Client(opts) {
    this.opts = opts;
    this.credentials = null;
    this.setCredentials = (creds) => {
      this.credentials = creds;
      setCredentials(creds);
    };
  }
  OAuth2Client._setCredentialsSpy = setCredentials;
  return { OAuth2Client };
});

const { OAuth2Client } = require('google-auth-library');
const oauthClient = require('./oauth-client');

const smMock = mockClient(SecretsManagerClient);

beforeEach(() => {
  smMock.reset();
  oauthClient._resetCacheForTests();
  OAuth2Client._setCredentialsSpy.mockClear();
});

const VALID_PAYLOAD = JSON.stringify({
  client_id: 'cid.apps.googleusercontent.com',
  client_secret: 'cs-xyz',
  refresh_token: '1//refresh-token-abc',
  scopes: ['https://www.googleapis.com/auth/calendar'],
  coordinator_email: 'test@example.com',
});

describe('buildSecretPath', () => {
  test('builds the canonical secret path with default prefix', () => {
    expect(oauthClient.buildSecretPath('MYR384719', 'test-coordinator'))
      .toBe('picasso/scheduling/oauth/MYR384719/test-coordinator');
  });

  test('throws when tenantId is missing', () => {
    expect(() => oauthClient.buildSecretPath('', 'coord'))
      .toThrow('tenantId and coordinatorId are required');
  });

  test('throws when coordinatorId is missing', () => {
    expect(() => oauthClient.buildSecretPath('MYR384719', ''))
      .toThrow('tenantId and coordinatorId are required');
  });
});

describe('fetchOAuthSecret', () => {
  test('returns parsed payload when all required fields present', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    const result = await oauthClient.fetchOAuthSecret('picasso/scheduling/oauth/MYR/coord');
    expect(result.client_id).toBe('cid.apps.googleusercontent.com');
    expect(result.refresh_token).toBe('1//refresh-token-abc');
  });

  test('throws when SecretString missing', async () => {
    smMock.on(GetSecretValueCommand).resolves({});
    await expect(oauthClient.fetchOAuthSecret('p/a/b/c'))
      .rejects.toThrow('OAuth secret has no SecretString');
  });

  test('throws on malformed JSON', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'not-json' });
    await expect(oauthClient.fetchOAuthSecret('p/a/b/c'))
      .rejects.toThrow('OAuth secret is not valid JSON');
  });

  test.each([
    ['client_id'],
    ['client_secret'],
    ['refresh_token'],
  ])('throws when %s is missing', async (missingField) => {
    const payload = JSON.parse(VALID_PAYLOAD);
    delete payload[missingField];
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify(payload) });
    // new message (code#3 stronger validation) + must NOT echo the secret path (code#1)
    await expect(oauthClient.fetchOAuthSecret('p/a/b/c'))
      .rejects.toThrow(`OAuth secret missing/empty required field "${missingField}" for the requested coordinator`);
  });

  test('error messages never echo the secret path (code#1)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: '' });
    await expect(oauthClient.fetchOAuthSecret('picasso/scheduling/oauth/MYR384719/jane@x.org'))
      .rejects.toThrow(/no SecretString for the requested coordinator/);
    await expect(oauthClient.fetchOAuthSecret('picasso/scheduling/oauth/MYR384719/jane@x.org'))
      .rejects.not.toThrow(/jane@x\.org|MYR384719/);
  });
});

describe('getOAuthClient', () => {
  test('constructs OAuth2Client and applies refresh token', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    const client = await oauthClient.getOAuthClient({
      tenantId: 'MYR384719',
      coordinatorId: 'test-coordinator',
    });
    expect(client).toBeInstanceOf(OAuth2Client);
    expect(client.opts).toEqual({
      clientId: 'cid.apps.googleusercontent.com',
      clientSecret: 'cs-xyz',
    });
    expect(client.credentials).toEqual({ refresh_token: '1//refresh-token-abc' });
    expect(OAuth2Client._setCredentialsSpy).toHaveBeenCalledWith({
      refresh_token: '1//refresh-token-abc',
    });
  });

  test('caches clients across calls for the same secret path', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    const a = await oauthClient.getOAuthClient({ tenantId: 'T1', coordinatorId: 'c' });
    const b = await oauthClient.getOAuthClient({ tenantId: 'T1', coordinatorId: 'c' });
    expect(a).toBe(b);
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);
  });

  test('different (tenantId, coordinatorId) pairs are cached separately', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    const a = await oauthClient.getOAuthClient({ tenantId: 'T1', coordinatorId: 'c' });
    const b = await oauthClient.getOAuthClient({ tenantId: 'T2', coordinatorId: 'c' });
    expect(a).not.toBe(b);
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(2);
  });

  test('throws when secret fetch fails (no cache poisoning)', async () => {
    smMock.on(GetSecretValueCommand).rejects(new Error('AccessDenied'));
    await expect(oauthClient.getOAuthClient({ tenantId: 'T', coordinatorId: 'c' }))
      .rejects.toThrow('AccessDenied');

    smMock.reset();
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    const client = await oauthClient.getOAuthClient({ tenantId: 'T', coordinatorId: 'c' });
    expect(client).toBeInstanceOf(OAuth2Client);
  });

  // Y1: TTL-miss — an entry older than CACHE_TTL_MS is treated as a miss
  test('Y1: cache entry older than CACHE_TTL_MS triggers re-fetch', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    // First fetch populates the cache
    const clientA = await oauthClient.getOAuthClient({ tenantId: 'T-ttl', coordinatorId: 'c' });
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);

    // Advance time past the TTL by replacing Date.now for this test
    const realNow = Date.now;
    const originalCachedAt = Date.now();
    Date.now = jest.fn(() => originalCachedAt + oauthClient._CACHE_TTL_MS + 1);

    const clientB = await oauthClient.getOAuthClient({ tenantId: 'T-ttl', coordinatorId: 'c' });
    // A second Secrets Manager call must have been made
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(2);
    // clientB is a new instance (different cachedAt)
    expect(clientB).toBeInstanceOf(OAuth2Client);

    Date.now = realNow;
  });

  // Y1: within TTL — re-uses the cached entry
  test('Y1: cache entry within CACHE_TTL_MS is reused without re-fetching', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    const clientA = await oauthClient.getOAuthClient({ tenantId: 'T-fresh', coordinatorId: 'c' });
    // Advance time to just UNDER the TTL
    const realNow = Date.now;
    const originalCachedAt = Date.now();
    Date.now = jest.fn(() => originalCachedAt + oauthClient._CACHE_TTL_MS - 1000);

    const clientB = await oauthClient.getOAuthClient({ tenantId: 'T-fresh', coordinatorId: 'c' });
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1); // no second fetch
    expect(clientA).toBe(clientB);

    Date.now = realNow;
  });
});

// Y1: clearCacheEntry
describe('clearCacheEntry', () => {
  test('evicting a cached entry causes re-fetch on next getOAuthClient call', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    await oauthClient.getOAuthClient({ tenantId: 'T-evict', coordinatorId: 'c' });
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);

    // Evict
    oauthClient.clearCacheEntry({ tenantId: 'T-evict', coordinatorId: 'c' });

    // Next call must re-fetch
    await oauthClient.getOAuthClient({ tenantId: 'T-evict', coordinatorId: 'c' });
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(2);
  });

  test('clearCacheEntry with missing args does not throw', () => {
    // Should swallow the buildSecretPath error gracefully
    expect(() => oauthClient.clearCacheEntry({ tenantId: '', coordinatorId: '' })).not.toThrow();
    expect(() => oauthClient.clearCacheEntry({})).not.toThrow();
  });

  test('clearCacheEntry for non-existent path is a no-op', () => {
    expect(() => oauthClient.clearCacheEntry({ tenantId: 'T-nonexistent', coordinatorId: 'c' })).not.toThrow();
  });
});
