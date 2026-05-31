'use strict';

/**
 * Unit tests for zoom-client.js — Zoom S2S OAuth + meeting create/delete.
 * global.fetch is mocked (no network); Secrets Manager is mocked. The headline
 * test is read-before-write idempotency: createMeeting(existingMeetingId) must NOT
 * POST a new meeting (no duplicate on retry).
 */

const { mockClient } = require('aws-sdk-client-mock');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const smMock = mockClient(SecretsManagerClient);
const zoom = require('./zoom-client');

const S2S_SECRET = JSON.stringify({
  provider: 'zoom', account_id: 'acct-1', client_id: 'cid', client_secret: 'csec',
});
const OAUTH_SECRET = JSON.stringify({
  provider: 'zoom', refresh_token: 'rtok', client_id: 'cid', client_secret: 'csec',
});

function jsonResponse(body, ok = true, status = 200) {
  return { ok, status, json: async () => body };
}

beforeEach(() => {
  smMock.reset();
  zoom._resetForTests();
  global.fetch = jest.fn();
});
afterEach(() => {
  delete global.fetch;
});

describe('token acquisition by secret shape (runbook Model)', () => {
  it('S2S secret ⇒ account_credentials grant', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
    global.fetch.mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }));
    const tok = await zoom.getAccessToken('MYR384719');
    expect(tok).toBe('tok');
    const body = global.fetch.mock.calls[0][1].body;
    expect(body).toContain('grant_type=account_credentials');
    expect(body).toContain('account_id=acct-1');
  });

  it('OAuth refresh_token secret ⇒ refresh_token grant (identical call shape downstream)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: OAUTH_SECRET });
    global.fetch.mockResolvedValueOnce(jsonResponse({ access_token: 'tok2', expires_in: 3600 }));
    await zoom.getAccessToken('MYR384719');
    expect(global.fetch.mock.calls[0][1].body).toContain('grant_type=refresh_token');
  });

  it('caches the token across calls (per-container, early-refresh)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
    global.fetch.mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }));
    await zoom.getAccessToken('MYR384719');
    await zoom.getAccessToken('MYR384719');
    expect(global.fetch).toHaveBeenCalledTimes(1); // token reused
  });

  it('rejects a secret carrying neither account_id nor refresh_token', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ client_id: 'c', client_secret: 's' }) });
    await expect(zoom.getAccessToken('MYR384719')).rejects.toThrow(/account_id .* or refresh_token/);
  });
});

describe('createMeeting', () => {
  beforeEach(() => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
  });

  it('creates a meeting via POST /users/{id}/meetings', async () => {
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 })) // token
      .mockResolvedValueOnce(jsonResponse({ id: 12345, join_url: 'https://zoom.us/j/12345' })); // create
    const m = await zoom.createMeeting({
      tenantId: 'MYR384719', coordinatorId: 'maya@org.org',
      topic: 'Intake', start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z', timezone: 'UTC',
    });
    expect(m).toEqual({ meetingId: '12345', joinUrl: 'https://zoom.us/j/12345' });
    const createCall = global.fetch.mock.calls[1];
    expect(createCall[0]).toContain('/users/maya%40org.org/meetings');
    expect(createCall[1].method).toBe('POST');
  });

  it('READ-BEFORE-WRITE: existingMeetingId reuses via GET, never POSTs a duplicate', async () => {
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 })) // token (for GET)
      .mockResolvedValueOnce(jsonResponse({ id: 55, join_url: 'https://zoom.us/j/55' })); // GET meeting
    const m = await zoom.createMeeting({
      tenantId: 'MYR384719', coordinatorId: 'maya@org.org',
      start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z',
      existingMeetingId: '55',
    });
    expect(m.meetingId).toBe('55');
    // No POST anywhere — only the token call + the GET.
    const posts = global.fetch.mock.calls.filter((c) => c[1] && c[1].method === 'POST' && String(c[0]).includes('/meetings'));
    expect(posts.length).toBe(0);
    const getCall = global.fetch.mock.calls.find((c) => String(c[0]).includes('/meetings/55'));
    expect(getCall[1].method).toBeUndefined(); // GET (default)
  });

  it('throws on a Zoom API failure (caller compensates)', async () => {
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse({}, false, 429));
    await expect(zoom.createMeeting({
      tenantId: 'MYR384719', coordinatorId: 'maya@org.org',
      start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z',
    })).rejects.toThrow(/Zoom create-meeting failed: 429/);
  });
});

describe('deleteMeeting — idempotent compensation', () => {
  beforeEach(() => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
  });
  it('treats 404 as success (already gone)', async () => {
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse({}, false, 404));
    await expect(zoom.deleteMeeting('MYR384719', '55')).resolves.toBeUndefined();
  });
  it('throws on a non-404 failure', async () => {
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse({}, false, 500));
    await expect(zoom.deleteMeeting('MYR384719', '55')).rejects.toThrow(/500/);
  });
});

describe('error/edge branches', () => {
  it('rejects a secret with no SecretString (omits the path)', async () => {
    smMock.on(GetSecretValueCommand).resolves({});
    await expect(zoom.getAccessToken('MYR384719')).rejects.toThrow(/no SecretString/);
  });
  it('rejects a non-JSON secret', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'nope' });
    await expect(zoom.getAccessToken('MYR384719')).rejects.toThrow(/not valid JSON/);
  });
  it('rejects a secret missing client_id/secret', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ account_id: 'a' }) });
    await expect(zoom.getAccessToken('MYR384719')).rejects.toThrow(/client_id\/client_secret/);
  });
  it('throws when the OAuth token endpoint is not ok', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
    global.fetch.mockResolvedValueOnce(jsonResponse({}, false, 401));
    await expect(zoom.getAccessToken('MYR384719')).rejects.toThrow(/Zoom OAuth token request failed: 401/);
  });
  it('throws when the OAuth response has no access_token', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
    global.fetch.mockResolvedValueOnce(jsonResponse({ expires_in: 3600 }));
    await expect(zoom.getAccessToken('MYR384719')).rejects.toThrow(/missing access_token/);
  });
  it('createMeeting throws on missing required args', async () => {
    await expect(zoom.createMeeting({ tenantId: 'T' })).rejects.toThrow(/required/);
  });
  it('getMeeting throws on a failed read', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse({}, false, 404));
    await expect(zoom.getMeeting('MYR384719', 'maya@org.org', '55')).rejects.toThrow(/get-meeting failed/);
  });
  it('createMeeting throws when the create response lacks id/join_url', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: S2S_SECRET });
    global.fetch
      .mockResolvedValueOnce(jsonResponse({ access_token: 'tok', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse({ id: 1 })); // no join_url
    await expect(zoom.createMeeting({
      tenantId: 'MYR384719', coordinatorId: 'm', start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z',
    })).rejects.toThrow(/missing id\/join_url/);
  });
});
