'use strict';

// Mirror oauth-client.test.js's google-auth-library mocking approach.
jest.mock('google-auth-library', () => {
  const generateAuthUrl = jest.fn();
  const getToken = jest.fn();
  const getAccessToken = jest.fn();
  const setCredentials = jest.fn();
  function OAuth2Client(opts) {
    this.opts = opts;
    this.generateAuthUrl = generateAuthUrl;
    this.getToken = getToken;
    this.getAccessToken = getAccessToken;
    this.setCredentials = setCredentials;
  }
  OAuth2Client._spies = { generateAuthUrl, getToken, getAccessToken, setCredentials };
  return { OAuth2Client };
});

const { OAuth2Client } = require('google-auth-library');
const oauth = require('./oauth');

const spies = OAuth2Client._spies;

beforeEach(() => {
  Object.values(spies).forEach((s) => s.mockReset());
});

describe('buildAuthUrl', () => {
  test('requests offline access, forced consent, minimized scopes, and the state', () => {
    spies.generateAuthUrl.mockReturnValue('https://accounts.google.com/o/oauth2/v2/auth?x=1');
    const url = oauth.buildAuthUrl({
      clientId: 'cid',
      clientSecret: 'cs',
      redirectUri: 'https://staging.schedule.myrecruiter.ai/oauth/callback',
      state: 'signed-state',
    });
    expect(url).toContain('accounts.google.com');
    const arg = spies.generateAuthUrl.mock.calls[0][0];
    expect(arg.access_type).toBe('offline');
    expect(arg.prompt).toBe('consent');
    expect(arg.include_granted_scopes).toBe(false);
    expect(arg.state).toBe('signed-state');
    expect(arg.scope).toEqual([
      'https://www.googleapis.com/auth/calendar.events',
      'https://www.googleapis.com/auth/calendar.freebusy',
    ]);
  });

  test('SCOPES excludes the full auth/calendar scope (D2 minimization)', () => {
    expect(oauth.SCOPES).not.toContain('https://www.googleapis.com/auth/calendar');
  });
});

describe('exchangeCode', () => {
  test('returns the refresh_token + granted scope', async () => {
    spies.getToken.mockResolvedValue({
      tokens: { refresh_token: '1//refresh', scope: 'a b', token_type: 'Bearer', access_token: 'at' },
    });
    const out = await oauth.exchangeCode({ clientId: 'c', clientSecret: 's', redirectUri: 'https://x', code: 'authcode' });
    expect(out).toEqual({ refresh_token: '1//refresh', scope: 'a b', token_type: 'Bearer' });
    expect(spies.getToken).toHaveBeenCalledWith('authcode');
  });

  test('no refresh_token in response → refresh_token null (caller guards)', async () => {
    spies.getToken.mockResolvedValue({ tokens: { access_token: 'at' } });
    const out = await oauth.exchangeCode({ clientId: 'c', clientSecret: 's', redirectUri: 'https://x', code: 'authcode' });
    expect(out.refresh_token).toBeNull();
  });

  test('getToken throw propagates', async () => {
    spies.getToken.mockRejectedValue(new Error('invalid_grant'));
    await expect(oauth.exchangeCode({ clientId: 'c', clientSecret: 's', redirectUri: 'https://x', code: 'bad' })).rejects.toThrow('invalid_grant');
  });
});

describe('probeRefresh', () => {
  test('success: sets credentials + mints an access token', async () => {
    spies.getAccessToken.mockResolvedValue({ token: 'fresh' });
    await oauth.probeRefresh({ clientId: 'c', clientSecret: 's', refreshToken: '1//rt' });
    expect(spies.setCredentials).toHaveBeenCalledWith({ refresh_token: '1//rt' });
    expect(spies.getAccessToken).toHaveBeenCalled();
  });

  test('invalid_grant throw propagates (caller classifies)', async () => {
    spies.getAccessToken.mockRejectedValue({ response: { status: 400, data: { error: 'invalid_grant' } } });
    await expect(oauth.probeRefresh({ clientId: 'c', clientSecret: 's', refreshToken: '1//rt' })).rejects.toMatchObject({
      response: { data: { error: 'invalid_grant' } },
    });
  });
});

describe('withTimeout (bounds the google-auth calls)', () => {
  afterEach(() => jest.useRealTimers());

  test('a hanging getAccessToken rejects with a timeout (not an infinite hang)', async () => {
    jest.useFakeTimers();
    spies.getAccessToken.mockReturnValue(new Promise(() => {})); // never settles
    const p = oauth.probeRefresh({ clientId: 'c', clientSecret: 's', refreshToken: '1//rt' });
    p.catch(() => {}); // pre-attach so the eventual reject is not "unhandled"
    await jest.advanceTimersByTimeAsync(8001);
    await expect(p).rejects.toThrow(/timed out/);
  });

  test('a hanging getToken rejects with a timeout', async () => {
    jest.useFakeTimers();
    spies.getToken.mockReturnValue(new Promise(() => {}));
    const p = oauth.exchangeCode({ clientId: 'c', clientSecret: 's', redirectUri: 'https://x', code: 'authcode' });
    p.catch(() => {});
    await jest.advanceTimersByTimeAsync(8001);
    await expect(p).rejects.toThrow(/timed out/);
  });
});
