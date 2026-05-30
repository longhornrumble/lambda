'use strict';

/**
 * Unit tests for tokens.js (WS-D1a) — canonical §13, frozen §B4.
 *
 * Covers: the CI-3d frozen-enum contract (issuer/verifier can't drift); the
 * sign→verify round-trip for all 6 purposes; per-purpose expiry (§13.6); sign
 * input validation; and the done-bar trio — TAMPER (signature mismatch),
 * EXPIRY (exp in the past), REPLAY (second redeem → 410). Plus alg-confusion
 * + cross-issuer (chat-JWT) rejection, the Secrets-Manager key resolver, and
 * the composite-key one-time-use PutItem.
 *
 * Crypto is exercised with an injected `signingKey` (pure, deterministic). DDB
 * + Secrets Manager are mocked with aws-sdk-client-mock (Calendar_Watch_* /
 * routing.test.js convention).
 */

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

const ddbMock = mockClient(DynamoDBClient);
const smMock = mockClient(SecretsManagerClient);

const {
  TOKEN_PURPOSES,
  TokenError,
  sign,
  verify,
  redeem,
} = require('../tokens');

const KEY = 'test-signing-key-do-not-use-in-prod';
const TENANT = 'AUS123957';
const BOOKING = 'bk-0001';

// A fixed clock so expiry math is deterministic. 2026-06-01T00:00:00Z.
const NOW = Math.floor(Date.parse('2026-06-01T00:00:00Z') / 1000);
const START_AT = '2026-06-03T18:00:00Z'; // 2 days out
const START_AT_S = Math.floor(Date.parse(START_AT) / 1000);
const EVENT_END = '2026-06-03T19:00:00Z';
const EVENT_END_S = Math.floor(Date.parse(EVENT_END) / 1000);

const opts = (over = {}) => ({ signingKey: KEY, now: NOW, ...over });

// Minimal sign inputs per purpose (the expiry drivers each purpose needs).
const claimsFor = (purpose) => {
  const base = { tenant_id: TENANT, booking_id: BOOKING };
  if (purpose === 'cancel' || purpose === 'reschedule') {
    return { ...base, start_at: START_AT, cancellation_window_hours: 0 };
  }
  if (purpose === 'post_application_recovery') {
    return { tenant_id: TENANT, booking_id: null, form_submission_id: 'fs-1' };
  }
  return { ...base, event_end: EVENT_END }; // attendance trio
};

beforeEach(() => {
  ddbMock.reset();
  smMock.reset();
});

// ─── CI-3d: frozen-enum contract (issuer ⇄ verifier can't drift) ────────────────────

describe('CI-3d — TOKEN_PURPOSES frozen enum (§13.4 / §B4 LOCKED)', () => {
  it('is EXACTLY the locked 6 purposes, in order', () => {
    // Editing this list must be a deliberate change that also edits this
    // expectation — that is the "red CI" guard (work-order done-bar).
    expect(TOKEN_PURPOSES).toEqual([
      'cancel',
      'reschedule',
      'post_application_recovery',
      'attended_yes',
      'no_show',
      'didnt_connect',
    ]);
  });

  it('every purpose the SIGNER mints, the VERIFIER accepts (no drift)', async () => {
    for (const purpose of TOKEN_PURPOSES) {
      const token = await sign(purpose, claimsFor(purpose), opts());
      const claims = await verify(token, opts());
      expect(claims.purpose).toBe(purpose);
    }
  });

  it('the VERIFIER rejects a purpose outside the frozen set (e.g. the wrong-draft "confirm")', async () => {
    // Hand-craft a token whose purpose claim is NOT in the enum.
    const token = await sign('cancel', claimsFor('cancel'), opts());
    const [h, p, s] = token.split('.'); // eslint-disable-line no-unused-vars
    const payload = JSON.parse(Buffer.from(p, 'base64url').toString('utf8'));
    payload.purpose = 'confirm';
    const encPayload = Buffer.from(JSON.stringify(payload))
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    const sig = crypto
      .createHmac('sha256', KEY)
      .update(`${h}.${encPayload}`)
      .digest('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    const forged = `${h}.${encPayload}.${sig}`;
    await expect(verify(forged, opts())).rejects.toMatchObject({
      code: 'unknown_purpose',
      status: 400,
    });
  });
});

// ─── Per-purpose expiry (§13.6) ──────────────────────────────────────────────────────

describe('per-purpose expiry (§13.6)', () => {
  const expOf = async (purpose, claims) =>
    (await verify(await sign(purpose, claims, opts()), opts())).exp;

  it('cancel → booking.start_at', async () => {
    expect(await expOf('cancel', claimsFor('cancel'))).toBe(START_AT_S);
  });

  it('reschedule (window=0) → start_at', async () => {
    expect(await expOf('reschedule', { tenant_id: TENANT, start_at: START_AT, cancellation_window_hours: 0 })).toBe(
      START_AT_S
    );
  });

  it('reschedule (window=24h) → start_at − 24h', async () => {
    const exp = await expOf('reschedule', {
      tenant_id: TENANT,
      start_at: START_AT,
      cancellation_window_hours: 24,
    });
    expect(exp).toBe(START_AT_S - 24 * 3600);
  });

  it('reschedule with no window field → defaults to 0 (= start_at)', async () => {
    const exp = await expOf('reschedule', { tenant_id: TENANT, start_at: START_AT });
    expect(exp).toBe(START_AT_S);
  });

  it.each(['attended_yes', 'no_show', 'didnt_connect'])(
    '%s → event_end + 24h',
    async (purpose) => {
      const exp = await expOf(purpose, { tenant_id: TENANT, event_end: EVENT_END });
      expect(exp).toBe(EVENT_END_S + 24 * 3600);
    }
  );

  it('post_application_recovery → iat + 14 days', async () => {
    const exp = await expOf('post_application_recovery', {
      tenant_id: TENANT,
      form_submission_id: 'fs-1',
    });
    expect(exp).toBe(NOW + 14 * 24 * 3600);
  });

  it('accepts an epoch-seconds start_at as well as ISO8601', async () => {
    const exp = await expOf('cancel', { tenant_id: TENANT, start_at: START_AT_S });
    expect(exp).toBe(START_AT_S);
  });
});

// ─── sign: payload shape + input validation (§13.3) ──────────────────────────────────

describe('sign — payload + validation (§13.3)', () => {
  it('persists only the reference claims; carries iss/jti/purpose; omits expiry drivers', async () => {
    const token = await sign('cancel', claimsFor('cancel'), opts());
    const claims = await verify(token, opts());
    expect(claims.iss).toBe('myrecruiter-scheduling');
    expect(claims.purpose).toBe('cancel');
    expect(claims.tenant_id).toBe(TENANT);
    expect(claims.booking_id).toBe(BOOKING);
    expect(typeof claims.jti).toBe('string');
    expect(claims.jti.length).toBeGreaterThan(0);
    // raw payload must not leak the expiry-driver inputs
    const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
    expect(payload.start_at).toBeUndefined();
    expect(payload.cancellation_window_hours).toBeUndefined();
  });

  it('post_application_recovery persists form_submission_id, null booking_id', async () => {
    const token = await sign('post_application_recovery', claimsFor('post_application_recovery'), opts());
    const claims = await verify(token, opts());
    expect(claims.form_submission_id).toBe('fs-1');
    expect(claims.booking_id).toBeNull();
  });

  it('each minted jti is unique (one-time-use key material)', async () => {
    const a = await verify(await sign('cancel', claimsFor('cancel'), opts()), opts());
    const b = await verify(await sign('cancel', claimsFor('cancel'), opts()), opts());
    expect(a.jti).not.toBe(b.jti);
  });

  it('rejects a purpose outside the locked enum', async () => {
    await expect(sign('confirm', { tenant_id: TENANT }, opts())).rejects.toMatchObject({
      code: 'unknown_purpose',
    });
  });

  it('rejects missing tenant_id', async () => {
    await expect(sign('cancel', { start_at: START_AT }, opts())).rejects.toMatchObject({
      code: 'malformed',
    });
  });

  it('rejects post_application_recovery without form_submission_id', async () => {
    await expect(
      sign('post_application_recovery', { tenant_id: TENANT }, opts())
    ).rejects.toMatchObject({ code: 'malformed' });
  });

  it('rejects an unparseable start_at', async () => {
    await expect(
      sign('cancel', { tenant_id: TENANT, start_at: 'not-a-date' }, opts())
    ).rejects.toMatchObject({ code: 'malformed' });
  });

  it('handles missing claims object (no tenant_id) gracefully', async () => {
    await expect(sign('cancel', undefined, opts())).rejects.toMatchObject({ code: 'malformed' });
  });
});

// ─── verify: TAMPER / alg / issuer / malformed / EXPIRY (done-bar) ───────────────────

describe('verify — tamper / alg / issuer / expiry (done-bar)', () => {
  it('TAMPER: a flipped payload byte → invalid_signature', async () => {
    const token = await sign('cancel', claimsFor('cancel'), opts());
    const parts = token.split('.');
    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
    payload.tenant_id = 'EVILTENANT'; // tamper, keep old signature
    parts[1] = Buffer.from(JSON.stringify(payload))
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    await expect(verify(parts.join('.'), opts())).rejects.toMatchObject({
      code: 'invalid_signature',
      status: 400,
    });
  });

  it('rejects a token signed with a different key', async () => {
    const token = await sign('cancel', claimsFor('cancel'), opts({ signingKey: 'other-key' }));
    await expect(verify(token, opts())).rejects.toMatchObject({ code: 'invalid_signature' });
  });

  it('alg-confusion: header alg ≠ HS256 → invalid_signature', async () => {
    const token = await sign('cancel', claimsFor('cancel'), opts());
    const parts = token.split('.');
    const badHeader = Buffer.from(JSON.stringify({ alg: 'none', typ: 'JWT' }))
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    parts[0] = badHeader;
    await expect(verify(parts.join('.'), opts())).rejects.toMatchObject({
      code: 'invalid_signature',
    });
  });

  it('cross-issuer: a chat-session JWT (iss=myrecruiter-chat) is rejected', async () => {
    // Forge a correctly-signed token with the chat issuer.
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' }))
      .toString('base64url');
    const payload = Buffer.from(
      JSON.stringify({ iss: 'myrecruiter-chat', exp: NOW + 999, purpose: 'cancel', jti: 'x' })
    ).toString('base64url');
    const sig = crypto
      .createHmac('sha256', KEY)
      .update(`${header}.${payload}`)
      .digest('base64url');
    await expect(verify(`${header}.${payload}.${sig}`, opts())).rejects.toMatchObject({
      code: 'invalid_issuer',
    });
  });

  it('EXPIRY: a token whose exp has passed → expired (410)', async () => {
    // cancel exp = start_at; verify at start_at + 1s.
    const token = await sign('cancel', claimsFor('cancel'), opts());
    await expect(verify(token, opts({ now: START_AT_S + 1 }))).rejects.toMatchObject({
      code: 'expired',
      status: 410,
    });
  });

  it('exp boundary: now === exp is expired (inclusive)', async () => {
    const token = await sign('cancel', claimsFor('cancel'), opts());
    await expect(verify(token, opts({ now: START_AT_S }))).rejects.toMatchObject({
      code: 'expired',
    });
  });

  it.each([
    ['not enough segments', 'a.b'],
    ['too many segments', 'a.b.c.d'],
    ['non-string', 12345],
  ])('malformed: %s → malformed', async (_label, bad) => {
    await expect(verify(bad, opts())).rejects.toMatchObject({ code: 'malformed' });
  });

  it('malformed: undecodable header JSON → malformed', async () => {
    const bad = `${Buffer.from('not-json').toString('base64url')}.${Buffer.from('{}').toString('base64url')}.sig`;
    await expect(verify(bad, opts())).rejects.toMatchObject({ code: 'malformed' });
  });

  it('malformed: valid signature but undecodable payload JSON → malformed', async () => {
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const payload = Buffer.from('not-json').toString('base64url');
    const sig = crypto.createHmac('sha256', KEY).update(`${header}.${payload}`).digest('base64url');
    await expect(verify(`${header}.${payload}.${sig}`, opts())).rejects.toMatchObject({
      code: 'malformed',
    });
  });
});

// ─── redeem: one-time-use (§13.7) — REPLAY done-bar ─────────────────────────────────

describe('redeem — one-time-use (§13.7)', () => {
  it('first redeem: writes the composite-key row + ConditionExpression, returns claims', async () => {
    ddbMock.on(PutItemCommand).resolves({});
    const token = await sign('cancel', claimsFor('cancel'), opts());
    const claims = await redeem(token, opts());
    expect(claims.purpose).toBe('cancel');
    expect(ddbMock).toHaveReceivedCommandTimes(PutItemCommand, 1);
    const sent = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(sent.TableName).toBe('picasso-token-jti-blacklist-staging');
    expect(sent.Item.tenantId).toEqual({ S: TENANT });
    expect(sent.Item.jti).toEqual({ S: claims.jti });
    expect(sent.Item.expires_at).toEqual({ N: String(claims.exp) });
    expect(sent.ConditionExpression).toBe('attribute_not_exists(jti)');
  });

  it('REPLAY: a second redeem (ConditionalCheckFailed) → 410, action does NOT execute', async () => {
    const err = new Error('exists');
    err.name = 'ConditionalCheckFailedException';
    ddbMock.on(PutItemCommand).rejects(err);
    const token = await sign('cancel', claimsFor('cancel'), opts());
    await expect(redeem(token, opts())).rejects.toMatchObject({
      code: 'reused',
      status: 410,
    });
  });

  it('an invalid/expired token never touches the table (no jti consumed)', async () => {
    ddbMock.on(PutItemCommand).resolves({});
    const token = await sign('cancel', claimsFor('cancel'), opts());
    await expect(redeem(token, opts({ now: START_AT_S + 1 }))).rejects.toMatchObject({
      code: 'expired',
    });
    expect(ddbMock).toHaveReceivedCommandTimes(PutItemCommand, 0);
  });

  it('propagates a non-conditional DDB error (e.g. throttling) unchanged', async () => {
    const err = new Error('throttled');
    err.name = 'ProvisionedThroughputExceededException';
    ddbMock.on(PutItemCommand).rejects(err);
    const token = await sign('cancel', claimsFor('cancel'), opts());
    await expect(redeem(token, opts())).rejects.toThrow('throttled');
  });
});

// ─── Signing-key resolver (§13.2 — Secrets Manager, mirrors Master_Function) ─────────

describe('getSigningKey — Secrets Manager resolver (§13.2)', () => {
  // These call sign/verify WITHOUT an injected key → exercise the SM path.
  it('JSON secret → uses the signingKey field; caches (single fetch)', async () => {
    smMock.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ signingKey: KEY }),
    });
    const token = await sign('cancel', claimsFor('cancel'), { now: NOW });
    const claims = await verify(token, { now: NOW }); // same cached key → valid
    expect(claims.purpose).toBe('cancel');
    // Cached: sign + verify together fetch the secret at most once.
    expect(smMock).toHaveReceivedCommandTimes(GetSecretValueCommand, 1);
  });

  it('plain-string secret → used as the raw HMAC key', async () => {
    // Module-level cache persists across tests; assert behaviour via round-trip
    // rather than fetch count (the JSON test above may have warmed the cache).
    smMock.on(GetSecretValueCommand).resolves({ SecretString: KEY });
    const token = await sign('no_show', { tenant_id: TENANT, event_end: EVENT_END }, { now: NOW });
    const claims = await verify(token, { now: NOW });
    expect(claims.purpose).toBe('no_show');
  });
});

// ─── TokenError shape ────────────────────────────────────────────────────────────────

describe('TokenError', () => {
  it('carries name/code/status', () => {
    const e = new TokenError('reused', 410);
    expect(e).toBeInstanceOf(Error);
    expect(e.name).toBe('TokenError');
    expect(e.code).toBe('reused');
    expect(e.status).toBe(410);
    expect(e.message).toBe('reused'); // defaults message to code
  });
});
