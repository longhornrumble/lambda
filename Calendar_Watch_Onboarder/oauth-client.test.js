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

  test('error messages do NOT leak the secret path (cross-tenant oracle)', async () => {
    smMock.on(GetSecretValueCommand).resolves({});
    await expect(oauthClient.fetchOAuthSecret('picasso/scheduling/oauth/SECRET-TENANT/coord'))
      .rejects.not.toThrow(/SECRET-TENANT/);
  });

  test.each([
    ['client_id'],
    ['client_secret'],
    ['refresh_token'],
  ])('throws when %s is missing', async (missingField) => {
    const payload = JSON.parse(VALID_PAYLOAD);
    delete payload[missingField];
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify(payload) });
    await expect(oauthClient.fetchOAuthSecret('p/a/b/c'))
      .rejects.toThrow(`OAuth secret missing/empty required field "${missingField}"`);
  });

  test.each([
    ['client_id'],
    ['client_secret'],
    ['refresh_token'],
  ])('throws when %s is empty string', async (emptyField) => {
    const payload = JSON.parse(VALID_PAYLOAD);
    payload[emptyField] = '';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify(payload) });
    await expect(oauthClient.fetchOAuthSecret('p/a/b/c'))
      .rejects.toThrow(`OAuth secret missing/empty required field "${emptyField}"`);
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

  test('fetches fresh on every call (NO cache — picks up rotated secrets)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: VALID_PAYLOAD });
    await oauthClient.getOAuthClient({ tenantId: 'T1', coordinatorId: 'c' });
    await oauthClient.getOAuthClient({ tenantId: 'T1', coordinatorId: 'c' });
    // Two fetches for two calls — a rotated refresh_token in Secrets Manager
    // takes effect on the very next invocation, not after a cold-start.
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(2);
  });

  test('a rotated refresh_token is reflected on the next call', async () => {
    smMock.on(GetSecretValueCommand).resolvesOnce({ SecretString: VALID_PAYLOAD });
    const first = await oauthClient.getOAuthClient({ tenantId: 'T', coordinatorId: 'c' });
    expect(first.credentials).toEqual({ refresh_token: '1//refresh-token-abc' });

    const rotated = JSON.parse(VALID_PAYLOAD);
    rotated.refresh_token = '1//rotated-token-xyz';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify(rotated) });
    const second = await oauthClient.getOAuthClient({ tenantId: 'T', coordinatorId: 'c' });
    expect(second.credentials).toEqual({ refresh_token: '1//rotated-token-xyz' });
  });

  test('throws when secret fetch fails', async () => {
    smMock.on(GetSecretValueCommand).rejects(new Error('AccessDenied'));
    await expect(oauthClient.getOAuthClient({ tenantId: 'T', coordinatorId: 'c' }))
      .rejects.toThrow('AccessDenied');
  });
});
