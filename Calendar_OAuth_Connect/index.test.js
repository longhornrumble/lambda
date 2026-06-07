'use strict';

// Env must be set BEFORE requiring index (module reads these at load).
process.env.DASHBOARD_RETURN_URL = 'https://app.example.com/scheduling';
delete process.env.OAUTH_REDIRECT_URI; // → '' ; redirect_uri supplied via the platform secret mock

jest.mock('./state', () => ({ verify: jest.fn(), sign: jest.fn() }));
jest.mock('./oauth', () => ({
  buildAuthUrl: jest.fn(),
  exchangeCode: jest.fn(),
  probeRefresh: jest.fn(),
  SCOPES: ['https://www.googleapis.com/auth/calendar.events', 'https://www.googleapis.com/auth/calendar.freebusy'],
}));
jest.mock('./secrets', () => ({
  readPlatformApp: jest.fn(),
  writeCoordinator: jest.fn(),
  readCoordinator: jest.fn(),
  markDisconnected: jest.fn(),
}));
jest.mock('../shared/scheduling/featureGate', () => ({ isSchedulingEnabledForTenant: jest.fn() }));

const { mockClient } = require('aws-sdk-client-mock');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const state = require('./state');
const oauth = require('./oauth');
const secrets = require('./secrets');
const { isSchedulingEnabledForTenant } = require('../shared/scheduling/featureGate');
const { handler, _internal } = require('./index');

const lambdaMock = mockClient(LambdaClient);

const INIT_CLAIMS = { tenant_id: 'MYR384719', coordinator_id: 'maya@example.org', coordinator_email: 'maya@example.org' };
const ev = (rawPath, queryStringParameters = {}) => ({ rawPath, queryStringParameters });

beforeEach(() => {
  jest.clearAllMocks();
  lambdaMock.reset();
  lambdaMock.on(InvokeCommand).resolves({});
  // sensible defaults; individual tests override
  state.verify.mockResolvedValue(INIT_CLAIMS);
  state.sign.mockResolvedValue('signed-state');
  oauth.buildAuthUrl.mockReturnValue('https://accounts.google.com/o/oauth2/v2/auth?state=signed-state');
  oauth.exchangeCode.mockResolvedValue({ refresh_token: '1//refresh-secret', scope: 'https://www.googleapis.com/auth/calendar.events https://www.googleapis.com/auth/calendar.freebusy', token_type: 'Bearer' });
  oauth.probeRefresh.mockResolvedValue(undefined);
  secrets.readPlatformApp.mockResolvedValue({ client_id: 'cid', client_secret: 'cs', redirect_uri: 'https://staging.schedule.myrecruiter.ai/oauth/callback' });
  secrets.writeCoordinator.mockResolvedValue('picasso/scheduling/oauth/MYR384719/maya@example.org');
  secrets.readCoordinator.mockResolvedValue(null);
  secrets.markDisconnected.mockResolvedValue({ found: true });
  isSchedulingEnabledForTenant.mockResolvedValue(true);
});

describe('routing', () => {
  test('unknown path → 404', async () => {
    const res = await handler(ev('/nope'));
    expect(res.statusCode).toBe(404);
  });

  test('top-level guard: an unexpected throw → generic 500 (no leak)', async () => {
    const evil = { rawPath: '/connect', get queryStringParameters() { throw new Error('boom'); } };
    const res = await handler(evil);
    expect(res.statusCode).toBe(500);
    expect(res.body).not.toContain('boom');
  });

  test('non-GET method → 405, route not executed', async () => {
    const res = await handler({ rawPath: '/connect', queryStringParameters: { init: 'ok' }, requestContext: { http: { method: 'POST' } } });
    expect(res.statusCode).toBe(405);
    expect(res.headers.allow).toBe('GET');
    expect(oauth.buildAuthUrl).not.toHaveBeenCalled();
  });

  test('trailing slash is normalized (/connect/ → /connect)', async () => {
    const res = await handler(ev('/connect/', { init: 'ok' }));
    expect(res.statusCode).toBe(302); // routed, not 404
  });

  test('requestContext.http.path fallback (no rawPath) still routes', async () => {
    const res = await handler({ requestContext: { http: { path: '/connect', method: 'GET' } }, queryStringParameters: { init: 'ok' } });
    expect(res.statusCode).toBe(302);
  });

  test('responses carry referrer-policy: no-referrer (B1 — token not leaked in Referer)', async () => {
    const res = await handler(ev('/connect', { init: 'ok' }));
    expect(res.headers['referrer-policy']).toBe('no-referrer');
  });
});

describe('GET /connect', () => {
  test('invalid init-token → 400, NO redirect to Google', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('bad_signature'), { code: 'bad_signature' }));
    const res = await handler(ev('/connect', { init: 'tampered' }));
    expect(res.statusCode).toBe(400);
    expect(oauth.buildAuthUrl).not.toHaveBeenCalled();
  });

  test('scheduling disabled (Flag A) → 403, no consent URL', async () => {
    isSchedulingEnabledForTenant.mockResolvedValue(false);
    const res = await handler(ev('/connect', { init: 'ok' }));
    expect(res.statusCode).toBe(403);
    expect(oauth.buildAuthUrl).not.toHaveBeenCalled();
  });

  test('valid init → 302 to Google with the signed state', async () => {
    const res = await handler(ev('/connect', { init: 'ok' }));
    expect(res.statusCode).toBe(302);
    expect(res.headers.location).toContain('accounts.google.com');
    expect(isSchedulingEnabledForTenant).toHaveBeenCalledWith('MYR384719');
    expect(oauth.buildAuthUrl).toHaveBeenCalledWith(expect.objectContaining({ clientId: 'cid', clientSecret: 'cs', state: 'signed-state' }));
  });

  test('SLOT-POISONING: identity comes from the token, query params are ignored', async () => {
    // Attacker appends their own coordinator/tenant to the URL; the token says maya@MYR384719.
    const res = await handler(ev('/connect', { init: 'ok', coordinator_id: 'victim@evil.com', tenant_id: 'EVILCORP' }));
    expect(res.statusCode).toBe(302);
    expect(isSchedulingEnabledForTenant).toHaveBeenCalledWith('MYR384719'); // NOT EVILCORP
    expect(state.sign).toHaveBeenCalledWith(
      expect.objectContaining({ typ: 'state', claims: expect.objectContaining({ coordinator_id: 'maya@example.org', tenant_id: 'MYR384719' }) })
    );
  });

  test('platform app unavailable → 500', async () => {
    secrets.readPlatformApp.mockRejectedValue(new Error('boom'));
    expect((await handler(ev('/connect', { init: 'ok' }))).statusCode).toBe(500);
  });

  test('redirect_uri unconfigured (no platform value, no env) → 500', async () => {
    secrets.readPlatformApp.mockResolvedValue({ client_id: 'cid', client_secret: 'cs', redirect_uri: null });
    expect((await handler(ev('/connect', { init: 'ok' }))).statusCode).toBe(500);
  });

  test('state signing failure → 500', async () => {
    state.sign.mockRejectedValue(new Error('no key'));
    expect((await handler(ev('/connect', { init: 'ok' }))).statusCode).toBe(500);
  });

  test('non-Google auth URL → 500, no redirect (S4 defense-in-depth)', async () => {
    oauth.buildAuthUrl.mockReturnValue('https://evil.example.com/o/oauth2/auth');
    const res = await handler(ev('/connect', { init: 'ok' }));
    expect(res.statusCode).toBe(500);
  });
});

describe('GET /oauth/callback', () => {
  test('user declined (error + valid state) → friendly 200, no exchange/write', async () => {
    const res = await handler(ev('/oauth/callback', { error: 'access_denied', state: 's' }));
    expect(res.statusCode).toBe(200);
    expect(oauth.exchangeCode).not.toHaveBeenCalled();
    expect(secrets.writeCoordinator).not.toHaveBeenCalled();
  });

  test('error WITHOUT state → 400 (no free page to anonymous callers; directive #2)', async () => {
    const res = await handler(ev('/oauth/callback', { error: 'access_denied' }));
    expect(res.statusCode).toBe(400);
  });

  test('error with FORGED state → 400 (state verified before the decline page)', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('bad_signature'), { code: 'bad_signature' }));
    const res = await handler(ev('/oauth/callback', { error: 'access_denied', state: 'forged' }));
    expect(res.statusCode).toBe(400);
  });

  test('partial granted scope → 403, NO secret written (directive #5)', async () => {
    oauth.exchangeCode.mockResolvedValue({ refresh_token: '1//r', scope: 'https://www.googleapis.com/auth/calendar.events' }); // freebusy missing
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(403);
    expect(secrets.writeCoordinator).not.toHaveBeenCalled();
  });

  test('no scope returned at all → 403, NO secret written', async () => {
    oauth.exchangeCode.mockResolvedValue({ refresh_token: '1//r', scope: null });
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(403);
    expect(secrets.writeCoordinator).not.toHaveBeenCalled();
  });

  test('missing code or state → 400', async () => {
    expect((await handler(ev('/oauth/callback', { state: 's' }))).statusCode).toBe(400);
    expect((await handler(ev('/oauth/callback', { code: 'c' }))).statusCode).toBe(400);
  });

  test('invalid state → 400, no exchange', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('expired'), { code: 'expired' }));
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 'bad' }));
    expect(res.statusCode).toBe(400);
    expect(oauth.exchangeCode).not.toHaveBeenCalled();
  });

  test('happy path → writes back-compat secret, fires B5, 302 to dashboard', async () => {
    state.verify.mockResolvedValue(INIT_CLAIMS);
    const res = await handler(ev('/oauth/callback', { code: 'authcode', state: 'signed' }));
    expect(secrets.writeCoordinator).toHaveBeenCalledWith(
      expect.objectContaining({
        tenantId: 'MYR384719',
        coordinatorId: 'maya@example.org',
        coordinatorEmail: 'maya@example.org',
        refreshToken: '1//refresh-secret',
        clientId: 'cid',
        clientSecret: 'cs',
        scopes: ['https://www.googleapis.com/auth/calendar.events', 'https://www.googleapis.com/auth/calendar.freebusy'],
        calendarId: 'primary',
      })
    );
    expect(lambdaMock.commandCalls(InvokeCommand).length).toBe(1);
    expect(res.statusCode).toBe(302);
    expect(res.headers.location).toBe('https://app.example.com/scheduling?calendar=connected&watch=ok');
  });

  test('code exchange failure → 502, no write', async () => {
    oauth.exchangeCode.mockRejectedValue(new Error('invalid_grant'));
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(502);
    expect(secrets.writeCoordinator).not.toHaveBeenCalled();
  });

  test('no refresh_token returned → 400, NO partial secret written', async () => {
    oauth.exchangeCode.mockResolvedValue({ refresh_token: null, scope: null });
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(400);
    expect(secrets.writeCoordinator).not.toHaveBeenCalled();
  });

  test('secret write failure → 500', async () => {
    secrets.writeCoordinator.mockRejectedValue(new Error('ddb'));
    expect((await handler(ev('/oauth/callback', { code: 'c', state: 's' }))).statusCode).toBe(500);
  });

  test('platform app unavailable on callback → 500, no exchange', async () => {
    secrets.readPlatformApp.mockRejectedValue(new Error('sm down'));
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(500);
    expect(oauth.exchangeCode).not.toHaveBeenCalled();
  });

  test('redirect_uri unconfigured on callback → 500, no exchange', async () => {
    secrets.readPlatformApp.mockResolvedValue({ client_id: 'cid', client_secret: 'cs', redirect_uri: null });
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(500);
    expect(oauth.exchangeCode).not.toHaveBeenCalled();
  });

  test('callback identity comes from the state token, not query params (slot-poisoning)', async () => {
    state.verify.mockResolvedValue(INIT_CLAIMS);
    await handler(ev('/oauth/callback', { code: 'c', state: 's', coordinator_id: 'victim@evil.com', tenant_id: 'EVIL' }));
    expect(secrets.writeCoordinator).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: 'MYR384719', coordinatorId: 'maya@example.org' })
    );
  });

  test('unknown ?error= is logged as "unknown" (no log-injection of attacker value)', async () => {
    const lines = [];
    const logSpy = jest.spyOn(console, 'log').mockImplementation((...a) => lines.push(a.join(' ')));
    await handler(ev('/oauth/callback', { error: 'INJECT"}{evil', state: 's' }));
    logSpy.mockRestore();
    const all = lines.join('\n');
    expect(all).toContain('"error":"unknown"');
    expect(all).not.toContain('INJECT');
  });

  test('B5 onboarder FunctionError → still connected, watch=pending', async () => {
    lambdaMock.on(InvokeCommand).resolves({ FunctionError: 'Unhandled' });
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(302);
    expect(res.headers.location).toContain('watch=pending');
  });

  test('B5 invoke throws → still connected (best-effort), watch=pending', async () => {
    lambdaMock.on(InvokeCommand).rejects(new Error('throttled'));
    const res = await handler(ev('/oauth/callback', { code: 'c', state: 's' }));
    expect(res.statusCode).toBe(302);
    expect(res.headers.location).toContain('watch=pending');
  });

  test('no dashboard URL configured → success page (200)', async () => {
    await jest.isolateModulesAsync(async () => {
      process.env.DASHBOARD_RETURN_URL = '';
      jest.doMock('./state', () => ({ verify: jest.fn().mockResolvedValue(INIT_CLAIMS), sign: jest.fn() }));
      jest.doMock('./oauth', () => ({ exchangeCode: jest.fn().mockResolvedValue({ refresh_token: '1//r', scope: null }), SCOPES: [] }));
      jest.doMock('./secrets', () => ({ readPlatformApp: jest.fn().mockResolvedValue({ client_id: 'c', client_secret: 's', redirect_uri: 'https://r' }), writeCoordinator: jest.fn().mockResolvedValue('p') }));
      const idx = require('./index');
      const res = await idx.handler(ev('/oauth/callback', { code: 'c', state: 's' }));
      expect(res.statusCode).toBe(200);
      expect(res.body.toLowerCase()).toContain('connected');
    });
    process.env.DASHBOARD_RETURN_URL = 'https://app.example.com/scheduling';
  });
});

describe('GET /connection/status (revocation detection)', () => {
  test('invalid init → 400 json', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('malformed'), { code: 'malformed' }));
    const res = await handler(ev('/connection/status', { init: 'x' }));
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body)).toEqual({ error: 'invalid_request' });
  });

  test('no secret → disconnected + bookable:false', async () => {
    secrets.readCoordinator.mockResolvedValue(null);
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', bookable: false });
  });

  test('already-revoked secret → disconnected (no probe)', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', status: 'revoked' });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(JSON.parse(res.body)).toMatchObject({ status: 'disconnected', bookable: false, reason: 'revoked' });
    expect(oauth.probeRefresh).not.toHaveBeenCalled();
  });

  test('probe success → connected (calendar_id from the secret; G3/E16)', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected', scopes: ['a'], calendar_id: 'maya.work@gmail.com' });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(JSON.parse(res.body)).toEqual({ status: 'connected', scopes: ['a'], calendar_id: 'maya.work@gmail.com' });
  });

  test('connected with NO calendar_id on the secret → falls back to coordinator_id (G3/E16, schema-discipline)', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(JSON.parse(res.body)).toMatchObject({ status: 'connected', calendar_id: 'maya@example.org' });
  });

  test('probe invalid_grant (permanent) → marks revoked + disconnected/bookable:false', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    oauth.probeRefresh.mockRejectedValue({ code: 400, response: { status: 400, data: { error: 'invalid_grant' } } });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(secrets.markDisconnected).toHaveBeenCalled();
    expect(JSON.parse(res.body)).toMatchObject({ status: 'disconnected', bookable: false, reason: 'revoked' });
  });

  test('probe invalid_grant via Calendar-API OBJECT error shape (not just STRING) → disconnected', async () => {
    // The dual-shape coercion is why the BCH classifier exists; exercise it END-TO-END here.
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    oauth.probeRefresh.mockRejectedValue({ response: { status: 401, data: { error: { code: 401, message: 'Token has been expired or revoked' } } } });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(secrets.markDisconnected).toHaveBeenCalled();
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', bookable: false, reason: 'revoked' });
  });

  test('probe 5xx (transient) → stale_connected, secret NOT touched', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    oauth.probeRefresh.mockRejectedValue({ code: 503, response: { status: 503, data: { error: 'backendError' } } });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(secrets.markDisconnected).not.toHaveBeenCalled();
    expect(JSON.parse(res.body)).toEqual({ status: 'stale_connected' });
  });

  test('probe invalid_client (PLATFORM) → stale_connected, secret NOT stamped (no mass-revoke)', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    oauth.probeRefresh.mockRejectedValue({ code: 401, response: { status: 401, data: { error: 'invalid_client' } } });
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(secrets.markDisconnected).not.toHaveBeenCalled();
    expect(JSON.parse(res.body)).toEqual({ status: 'stale_connected' });
  });

  test('status identity comes from the init token, not query params (slot-poisoning)', async () => {
    secrets.readCoordinator.mockResolvedValue(null);
    await handler(ev('/connection/status', { init: 'ok', coordinator_id: 'victim@evil.com', tenant_id: 'EVIL' }));
    expect(secrets.readCoordinator).toHaveBeenCalledWith(expect.objectContaining({ tenantId: 'MYR384719', coordinatorId: 'maya@example.org' }));
  });

  test('secret read failure → 500 json', async () => {
    secrets.readCoordinator.mockRejectedValue(new Error('boom'));
    expect((await handler(ev('/connection/status', { init: 'ok' }))).statusCode).toBe(500);
  });

  test('mark-disconnected failure is swallowed (still reports disconnected)', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    oauth.probeRefresh.mockRejectedValue({ response: { data: { error: 'invalid_grant' } } });
    secrets.markDisconnected.mockRejectedValue(new Error('write fail'));
    const res = await handler(ev('/connection/status', { init: 'ok' }));
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', bookable: false, reason: 'revoked' });
  });
});

describe('PII hygiene — no secrets/PII in logs', () => {
  test('happy callback logs never contain refresh_token, coordinator_email, code, or state', async () => {
    const lines = [];
    const cap = (...a) => lines.push(a.map(String).join(' '));
    const logSpy = jest.spyOn(console, 'log').mockImplementation(cap);
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(cap);
    state.verify.mockResolvedValue(INIT_CLAIMS);
    await handler(ev('/oauth/callback', { code: 'AUTHCODE-SECRET', state: 'STATE-TOKEN-SECRET' }));
    const all = lines.join('\n');
    expect(all).not.toContain('1//refresh-secret');
    expect(all).not.toContain('maya@example.org'); // coordinator_email / id (logged only as a hash)
    expect(all).not.toContain('AUTHCODE-SECRET');
    expect(all).not.toContain('STATE-TOKEN-SECRET');
    logSpy.mockRestore();
    warnSpy.mockRestore();
  });

  test('status revocation path logs no raw coordinator_email (hash only)', async () => {
    const lines = [];
    const cap = (...a) => lines.push(a.map(String).join(' '));
    const logSpy = jest.spyOn(console, 'log').mockImplementation(cap);
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(cap);
    state.verify.mockResolvedValue(INIT_CLAIMS); // coordinator_email = maya@example.org
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', client_id: 'c', client_secret: 's', status: 'connected' });
    oauth.probeRefresh.mockRejectedValue({ response: { status: 400, data: { error: 'invalid_grant' } } });
    await handler(ev('/connection/status', { init: 'ok' }));
    logSpy.mockRestore();
    warnSpy.mockRestore();
    expect(lines.join('\n')).not.toContain('maya@example.org');
  });

  test('coordHash is a 12-char prefix, not the raw id', () => {
    const h = _internal.coordHash('maya@example.org');
    expect(h).toHaveLength(12);
    expect(h).not.toContain('maya');
  });
});
