'use strict';

/**
 * disconnect.test.js — §E11b POST /connection/disconnect route tests.
 *
 * Contract assertions (all from FROZEN_CONTRACTS §E11b):
 *  1. Route dispatch — POST /connection/disconnect routes correctly
 *  2. Method enforcement — GET/PUT/DELETE → 405; only POST allowed
 *  3. verify-fail → 4xx (init absent, bad signature, expired)
 *  4. revoke-fail-still-disconnects — Google revoke throws → WARN logged, stamp still proceeds
 *  5. offboarder-fail-still-200 — Offboarder invoke fails → 200 { status:'disconnected', watch:'pending' }
 *  6. Idempotency — already-revoked secret → 200 { status:'disconnected', watch:'none' }
 *  7. Ordering — verify → revoke → stamp → offboard (mock call-order assertion)
 *  8. Generic errors only — no secret-path, URL, or detail leak
 *  9. PII hygiene — refresh_token and coordinator_email never appear in logs
 * 10. No jti burn (§E11b: replay = re-disconnect = idempotent + harmless)
 */

// Env must be set BEFORE requiring index (module reads these at load).
process.env.DASHBOARD_RETURN_URL = 'https://app.example.com/scheduling';
process.env.JTI_BLACKLIST_TABLE = 'picasso-token-jti-blacklist';
delete process.env.OAUTH_REDIRECT_URI;

jest.mock('./state', () => ({ verify: jest.fn(), sign: jest.fn() }));
jest.mock('./oauth', () => ({
  buildAuthUrl: jest.fn(),
  exchangeCode: jest.fn(),
  probeRefresh: jest.fn(),
  revokeToken: jest.fn(),
  SCOPES: ['https://www.googleapis.com/auth/calendar.events', 'https://www.googleapis.com/auth/calendar.freebusy'],
}));
jest.mock('./secrets', () => ({
  readPlatformApp: jest.fn(),
  writeCoordinator: jest.fn(),
  readCoordinator: jest.fn(),
  markDisconnected: jest.fn(),
}));
jest.mock('../shared/scheduling/featureGate', () => ({ isSchedulingEnabledForTenant: jest.fn() }));
jest.mock('./jti', () => ({ burnJti: jest.fn() }));

const { mockClient } = require('aws-sdk-client-mock');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const state = require('./state');
const oauth = require('./oauth');
const secrets = require('./secrets');
const { burnJti } = require('./jti');
const { handler, _internal } = require('./index');

const lambdaMock = mockClient(LambdaClient);

const CLAIMS = {
  tenant_id: 'MYR384719',
  coordinator_id: 'maya@example.org',
  coordinator_email: 'maya@example.org',
  purpose: 'disconnect', // §E11b: ADA mints disconnect tokens with this claim
};
const CONNECTED_SECRET = {
  refresh_token: 'rt-secret-value',
  client_id: 'cid',
  client_secret: 'cs',
  status: 'connected',
};

// Build a POST /connection/disconnect event (body-carried token).
function discEvent(body) {
  return {
    rawPath: '/connection/disconnect',
    requestContext: { http: { method: 'POST' } },
    body: JSON.stringify(body || { init: 'valid-init-token' }),
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  lambdaMock.reset();
  // Default async invoke resolves with status 202 (Event invocation type).
  lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

  state.verify.mockResolvedValue(CLAIMS);
  oauth.revokeToken.mockResolvedValue(undefined);
  secrets.readCoordinator.mockResolvedValue(CONNECTED_SECRET);
  secrets.markDisconnected.mockResolvedValue({ found: true });
  burnJti.mockResolvedValue({ burned: true });
});

// ─────────────────────────────────────────────────────────────────────────────
// 1. Route dispatch
// ─────────────────────────────────────────────────────────────────────────────
describe('route dispatch', () => {
  test('POST /connection/disconnect routes to handleDisconnect', async () => {
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
  });

  test('/connection/disconnect is distinct from /connection/status', async () => {
    // /connection/status is a GET — confirm we don't accidentally route a POST there.
    const res = await handler({
      rawPath: '/connection/status',
      requestContext: { http: { method: 'POST' } },
      queryStringParameters: { init: 'tok' },
    });
    // status route is GET-only → 405 from the outer GET guard
    expect(res.statusCode).toBe(405);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. Method enforcement
// ─────────────────────────────────────────────────────────────────────────────
describe('method enforcement', () => {
  test('GET /connection/disconnect → 405', async () => {
    const res = await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'GET' } },
      body: null,
    });
    expect(res.statusCode).toBe(405);
    expect(res.headers.allow).toBe('POST');
  });

  test('PUT /connection/disconnect → 405', async () => {
    const res = await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'PUT' } },
      body: null,
    });
    expect(res.statusCode).toBe(405);
  });

  test('DELETE /connection/disconnect → 405', async () => {
    const res = await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'DELETE' } },
      body: null,
    });
    expect(res.statusCode).toBe(405);
  });

  test('state.verify not called when method is rejected', async () => {
    await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'GET' } },
      body: null,
    });
    expect(state.verify).not.toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. verify-fail → 4xx
// ─────────────────────────────────────────────────────────────────────────────
describe('verify-fail → 4xx', () => {
  test('missing init body → 400', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('malformed'), { code: 'malformed' }));
    const res = await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'POST' } },
      body: '{}',
    });
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body)).toEqual({ error: 'invalid_request' });
  });

  test('bad signature → 400, no secret read', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('bad_signature'), { code: 'bad_signature' }));
    const res = await handler(discEvent({ init: 'tampered' }));
    expect(res.statusCode).toBe(400);
    expect(secrets.readCoordinator).not.toHaveBeenCalled();
    expect(secrets.markDisconnected).not.toHaveBeenCalled();
  });

  test('expired token → 400, no revoke attempted', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('expired'), { code: 'expired' }));
    const res = await handler(discEvent({ init: 'old-tok' }));
    expect(res.statusCode).toBe(400);
    expect(oauth.revokeToken).not.toHaveBeenCalled();
  });

  test('wrong type (state token) → 400', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('wrong_type'), { code: 'wrong_type' }));
    const res = await handler(discEvent({ init: 'state-tok' }));
    expect(res.statusCode).toBe(400);
  });

  test('malformed JSON body → 400 (verify called with undefined init)', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('malformed'), { code: 'malformed' }));
    const res = await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'POST' } },
      body: 'NOT-JSON',
    });
    expect(res.statusCode).toBe(400);
  });

  test('event.body field absent (undefined) → 400 (gracefully treated as empty body)', async () => {
    // Lambda Function URL can omit the body field entirely on a POST with no body.
    // The route must not throw and must return 400 (verify is called with undefined init).
    state.verify.mockRejectedValue(Object.assign(new Error('malformed'), { code: 'malformed' }));
    const res = await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'POST' } },
      // no body field at all
    });
    expect(res.statusCode).toBe(400);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. revoke-fail-still-disconnects
// ─────────────────────────────────────────────────────────────────────────────
describe('revoke-fail-still-disconnects', () => {
  test('Google revoke network failure → WARN logged, stamp still called, 200', async () => {
    oauth.revokeToken.mockRejectedValue(new Error('network error'));
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const res = await handler(discEvent());
    const warnLines = warnSpy.mock.calls.map((a) => a.join(' ')).join('\n');
    warnSpy.mockRestore();
    // Must warn about the revoke failure
    expect(warnLines).toContain('disconnect_google_revoke_failed');
    // Must still stamp the secret
    expect(secrets.markDisconnected).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: CLAIMS.tenant_id, coordinatorId: CLAIMS.coordinator_id })
    );
    // Must still 200
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toMatchObject({ status: 'disconnected' });
  });

  test('Google revoke 4xx error → still stamps, 200', async () => {
    const err = new Error('400');
    err.status = 400;
    oauth.revokeToken.mockRejectedValue(err);
    const res = await handler(discEvent());
    expect(secrets.markDisconnected).toHaveBeenCalled();
    expect(res.statusCode).toBe(200);
  });

  test('revoke failure does NOT leak the error detail in the response body', async () => {
    oauth.revokeToken.mockRejectedValue(new Error('super-secret-reason'));
    const res = await handler(discEvent());
    expect(res.body).not.toContain('super-secret-reason');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. offboarder-fail-still-200
// ─────────────────────────────────────────────────────────────────────────────
describe('offboarder-fail-still-200', () => {
  test('Offboarder SDK throw (throttle/network) → 200 { status:disconnected, watch:none }', async () => {
    // InvocationType:'Event' -- SDK throws on error (no FunctionError on async invocations).
    // ok:false → watch:'none' (dispatch not accepted; cleanup deferred to next expiry/sweep).
    lambdaMock.on(InvokeCommand).rejects(new Error('throttled'));
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', watch: 'none' });
  });

  test('markDisconnected failure → 500 generic (stamp is authoritative; offboard never reached)', async () => {
    secrets.markDisconnected.mockRejectedValue(new Error('sm write fail'));
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(500);
    // Generic error, no detail leak
    const body = JSON.parse(res.body);
    expect(body.error).toBe('internal');
    expect(res.body).not.toContain('sm write fail');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. Idempotency
// ─────────────────────────────────────────────────────────────────────────────
describe('idempotency', () => {
  test('already-revoked secret → 200 { status:disconnected, watch:none }, no Google call', async () => {
    secrets.readCoordinator.mockResolvedValue({ refresh_token: 'rt', status: 'revoked' });
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', watch: 'none' });
    expect(oauth.revokeToken).not.toHaveBeenCalled();
    expect(secrets.markDisconnected).not.toHaveBeenCalled();
  });

  test('missing secret (never connected) → 200 { status:disconnected, watch:none }', async () => {
    secrets.readCoordinator.mockResolvedValue(null);
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', watch: 'none' });
    expect(oauth.revokeToken).not.toHaveBeenCalled();
  });

  test('secret exists but has no refresh_token → 200 { watch:none }', async () => {
    secrets.readCoordinator.mockResolvedValue({ status: 'connected' }); // no refresh_token
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', watch: 'none' });
    expect(oauth.revokeToken).not.toHaveBeenCalled();
  });

  test('secrets read failure → treats as no-secret → 200 { watch:none }, NO markDisconnected', async () => {
    // SM outage: secret=null falls through to the idempotent early-return.
    // Critically, markDisconnected must NOT be called -- we cannot confirm state,
    // so we must not write a stamp that might be wrong (item 1 locking assertion).
    secrets.readCoordinator.mockRejectedValue(new Error('sm down'));
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const res = await handler(discEvent());
    warnSpy.mockRestore();
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', watch: 'none' });
    // LOCKING ASSERTION: SM outage must NOT call markDisconnected (no stamp on uncertain state).
    expect(secrets.markDisconnected).not.toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. Ordering — verify → revoke → stamp → offboard
// ─────────────────────────────────────────────────────────────────────────────
describe('ordering', () => {
  test('verify → (readCoordinator) → revokeToken → markDisconnected → Offboarder invoke', async () => {
    const order = [];
    state.verify.mockImplementation(async () => { order.push('verify'); return CLAIMS; });
    secrets.readCoordinator.mockImplementation(async () => { order.push('readCoordinator'); return CONNECTED_SECRET; });
    oauth.revokeToken.mockImplementation(async () => { order.push('revokeToken'); });
    secrets.markDisconnected.mockImplementation(async () => { order.push('markDisconnected'); return { found: true }; });
    // Capture the Offboarder InvokeCommand in the same order array so we can assert stamp-before-offboard.
    lambdaMock.on(InvokeCommand).callsFake(() => { order.push('offboarder'); return Promise.resolve({ StatusCode: 202 }); });

    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    // Core ordering contract (§E11b): verify → revoke → stamp → offboard
    expect(order.indexOf('verify')).toBeLessThan(order.indexOf('revokeToken'));
    expect(order.indexOf('revokeToken')).toBeLessThan(order.indexOf('markDisconnected'));
    // STAMP-BEFORE-OFFBOARD: the contract requires markDisconnected precedes the Offboarder invoke.
    expect(order.indexOf('markDisconnected')).toBeLessThan(order.indexOf('offboarder'));
  });

  test('verify is called with typ:init (not typ:state)', async () => {
    await handler(discEvent());
    expect(state.verify).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ expectedType: 'init' })
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. Generic errors — no secret-path / URL / detail leak
// ─────────────────────────────────────────────────────────────────────────────
describe('generic errors', () => {
  test('no secret-path or coordinator_email in error responses', async () => {
    secrets.markDisconnected.mockRejectedValue(new Error('boom'));
    const res = await handler(discEvent());
    expect(res.body).not.toContain('picasso/scheduling/oauth');
    expect(res.body).not.toContain('maya@example.org');
  });

  test('verify-fail response body contains only error:invalid_request', async () => {
    state.verify.mockRejectedValue(Object.assign(new Error('bad'), { code: 'bad_signature' }));
    const body = JSON.parse((await handler(discEvent())).body);
    expect(Object.keys(body)).toEqual(['error']);
    expect(body.error).toBe('invalid_request');
  });

  test('Offboarder invoked with { tenant_id, coordinator_id } only (no email in payload)', async () => {
    await handler(discEvent());
    const calls = lambdaMock.commandCalls(InvokeCommand);
    expect(calls.length).toBe(1);
    const p = JSON.parse(Buffer.from(calls[0].args[0].input.Payload).toString());
    expect(p).toHaveProperty('tenant_id', CLAIMS.tenant_id);
    expect(p).toHaveProperty('coordinator_id', CLAIMS.coordinator_id);
    // coordinator_email must NOT be in the Offboarder payload (its interface is {tenant_id, coordinator_id})
    expect(p).not.toHaveProperty('coordinator_email');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. PII hygiene — no secrets or PII in logs
// ─────────────────────────────────────────────────────────────────────────────
describe('PII hygiene', () => {
  test('refresh_token and coordinator_email never appear in any log line', async () => {
    const lines = [];
    const cap = (...a) => lines.push(a.map(String).join(' '));
    const logSpy = jest.spyOn(console, 'log').mockImplementation(cap);
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(cap);

    await handler(discEvent());

    logSpy.mockRestore();
    warnSpy.mockRestore();

    const all = lines.join('\n');
    expect(all).not.toContain('rt-secret-value');       // refresh_token
    expect(all).not.toContain('maya@example.org');      // coordinator_email / coordinator_id
  });

  test('revoke-fail path still redacts refresh_token from logs', async () => {
    oauth.revokeToken.mockRejectedValue(new Error('timeout'));
    const lines = [];
    const cap = (...a) => lines.push(a.map(String).join(' '));
    const logSpy = jest.spyOn(console, 'log').mockImplementation(cap);
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(cap);

    await handler(discEvent());

    logSpy.mockRestore();
    warnSpy.mockRestore();

    expect(lines.join('\n')).not.toContain('rt-secret-value');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. No jti burn
// ─────────────────────────────────────────────────────────────────────────────
describe('no jti burn', () => {
  test('burnJti is never called on /connection/disconnect (replay = idempotent re-disconnect)', async () => {
    await handler(discEvent());
    expect(burnJti).not.toHaveBeenCalled();
  });

  test('burnJti not called even when verify succeeds with a jti-bearing token', async () => {
    state.verify.mockResolvedValue({ ...CLAIMS, jti: 'deadbeef1234' });
    await handler(discEvent());
    expect(burnJti).not.toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 11. purpose claim (§E11b cross-purpose replay defense)
// ─────────────────────────────────────────────────────────────────────────────
describe('purpose claim', () => {
  test('token without purpose claim → 400 (connect/status token is invalid here)', async () => {
    // A connect or status URL token has no 'purpose' claim; must be rejected by disconnect.
    state.verify.mockResolvedValue({ ...CLAIMS, purpose: undefined });
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body)).toEqual({ error: 'invalid_request' });
    // No secrets work must happen on a wrong-purpose token
    expect(secrets.readCoordinator).not.toHaveBeenCalled();
    expect(secrets.markDisconnected).not.toHaveBeenCalled();
  });

  test('token with wrong purpose → 400', async () => {
    state.verify.mockResolvedValue({ ...CLAIMS, purpose: 'connect' });
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(400);
    expect(secrets.readCoordinator).not.toHaveBeenCalled();
  });

  test('token with purpose:disconnect → proceeds normally', async () => {
    state.verify.mockResolvedValue({ ...CLAIMS, purpose: 'disconnect' });
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    expect(secrets.readCoordinator).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Happy path end-to-end
// ─────────────────────────────────────────────────────────────────────────────
describe('happy path', () => {
  test('full success → 200 { status:disconnected, watch:pending } (async dispatch confirmed, not completion)', async () => {
    // InvocationType:'Event' returns 202 on successful dispatch; ok:true → watch:'pending'.
    // 'pending' = cleanup dispatched (not yet confirmed complete). The dashboard FE should
    // treat 'pending' as success-with-cleanup-in-progress -- the stamp is authoritative.
    const res = await handler(discEvent());
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: 'disconnected', watch: 'pending' });
  });

  test('identity comes from the token, not from body fields (slot-poisoning defense)', async () => {
    // Attacker adds their own tenant/coordinator to the body beyond the init token.
    state.verify.mockResolvedValue(CLAIMS);
    await handler({
      rawPath: '/connection/disconnect',
      requestContext: { http: { method: 'POST' } },
      body: JSON.stringify({ init: 'valid-tok', tenant_id: 'EVIL', coordinator_id: 'victim@evil.com' }),
    });
    // secret read must use the token-sourced identity, not the body fields
    expect(secrets.readCoordinator).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: CLAIMS.tenant_id, coordinatorId: CLAIMS.coordinator_id })
    );
    expect(secrets.markDisconnected).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: CLAIMS.tenant_id, coordinatorId: CLAIMS.coordinator_id })
    );
  });

  test('Offboarder is invoked with Event (async) invocation type', async () => {
    await handler(discEvent());
    const calls = lambdaMock.commandCalls(InvokeCommand);
    expect(calls.length).toBe(1);
    expect(calls[0].args[0].input.InvocationType).toBe('Event');
  });
});
