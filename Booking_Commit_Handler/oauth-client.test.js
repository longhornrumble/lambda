'use strict';

/**
 * Unit tests for oauth-client.js — per-(tenant, coordinator) Google OAuth client.
 * google-auth-library is mocked (no real token exchange); Secrets Manager is mocked
 * with aws-sdk-client-mock.
 */

const mockSetCredentials = jest.fn();
jest.mock('google-auth-library', () => ({
  OAuth2Client: jest.fn().mockImplementation(() => ({ setCredentials: mockSetCredentials })),
}));

const { mockClient } = require('aws-sdk-client-mock');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const smMock = mockClient(SecretsManagerClient);

const oauth = require('./oauth-client');

const SECRET = JSON.stringify({
  client_id: 'cid', client_secret: 'csecret', refresh_token: 'rtok',
  coordinator_email: 'maya@org.org',
});

beforeEach(() => {
  smMock.reset();
  mockSetCredentials.mockReset();
  oauth._resetCacheForTests();
});

describe('buildSecretPath', () => {
  it('builds the canonical per-tenant per-coordinator path', () => {
    expect(oauth.buildSecretPath('AUS123957', 'maya@org.org'))
      .toBe('picasso/scheduling/oauth/AUS123957/maya@org.org');
  });
  it('throws on missing args', () => {
    expect(() => oauth.buildSecretPath('', 'x')).toThrow();
  });
});

describe('fetchOAuthSecret', () => {
  it('parses + validates required fields', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: SECRET });
    const parsed = await oauth.fetchOAuthSecret('picasso/scheduling/oauth/AUS123957/maya@org.org');
    expect(parsed.client_id).toBe('cid');
  });

  it('rejects a secret missing required fields — and the error omits the secret path', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ client_id: 'x' }) });
    await expect(oauth.fetchOAuthSecret('picasso/scheduling/oauth/AUS123957/maya@org.org'))
      .rejects.toThrow(/missing\/empty required field/);
    try {
      await oauth.fetchOAuthSecret('picasso/scheduling/oauth/AUS123957/maya@org.org');
    } catch (err) {
      expect(err.message).not.toContain('maya@org.org'); // no cross-tenant existence oracle
    }
  });

  it('rejects non-JSON secret', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'not json' });
    await expect(oauth.fetchOAuthSecret('p')).rejects.toThrow(/not valid JSON/);
  });
});

describe('getOAuthClient — cache + eviction', () => {
  it('fetches once and caches across calls', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: SECRET });
    await oauth.getOAuthClient({ tenantId: 'AUS123957', coordinatorId: 'maya@org.org' });
    await oauth.getOAuthClient({ tenantId: 'AUS123957', coordinatorId: 'maya@org.org' });
    expect(smMock.commandCalls(GetSecretValueCommand).length).toBe(1);
    expect(mockSetCredentials).toHaveBeenCalledWith({ refresh_token: 'rtok' });
  });

  it('clearCacheEntry forces a re-fetch (revoked-credential path)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: SECRET });
    await oauth.getOAuthClient({ tenantId: 'AUS123957', coordinatorId: 'maya@org.org' });
    oauth.clearCacheEntry({ tenantId: 'AUS123957', coordinatorId: 'maya@org.org' });
    await oauth.getOAuthClient({ tenantId: 'AUS123957', coordinatorId: 'maya@org.org' });
    expect(smMock.commandCalls(GetSecretValueCommand).length).toBe(2);
  });

  it('clearCacheEntry swallows bad args (error-path safety)', () => {
    expect(() => oauth.clearCacheEntry({})).not.toThrow();
  });
});
