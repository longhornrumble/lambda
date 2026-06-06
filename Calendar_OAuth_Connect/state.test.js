'use strict';

const { mockClient } = require('aws-sdk-client-mock');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const state = require('./state');

const KEY = 'test-signing-key-0123456789abcdef';
const deps = { getKey: () => KEY };
const smMock = mockClient(SecretsManagerClient);

beforeEach(() => {
  smMock.reset();
  state._resetKeyCache();
});

describe('sign/verify roundtrip', () => {
  test('init token roundtrips with claims intact', async () => {
    const token = await state.sign({
      typ: 'init',
      claims: { tenant_id: 'MYR384719', coordinator_id: 'maya@example.org', coordinator_email: 'maya@example.org' },
      ttlSeconds: 300,
      nowMs: 1_000_000_000_000,
      deps,
    });
    const claims = await state.verify(token, { expectedType: 'init', nowMs: 1_000_000_000_000, deps });
    expect(claims.tenant_id).toBe('MYR384719');
    expect(claims.coordinator_id).toBe('maya@example.org');
    expect(claims.typ).toBe('init');
    expect(typeof claims.nonce).toBe('string');
    expect(claims.exp).toBe(1_000_000_000 + 300);
  });

  test('two signs of the same claims differ (random nonce)', async () => {
    const a = await state.sign({ typ: 'state', claims: { tenant_id: 't' }, ttlSeconds: 60, deps });
    const b = await state.sign({ typ: 'state', claims: { tenant_id: 't' }, ttlSeconds: 60, deps });
    expect(a).not.toBe(b);
  });
});

describe('verify rejects', () => {
  test('type confusion: an init token cannot be replayed as state', async () => {
    const initTok = await state.sign({ typ: 'init', claims: { tenant_id: 't' }, ttlSeconds: 300, deps });
    await expect(state.verify(initTok, { expectedType: 'state', deps })).rejects.toMatchObject({ code: 'wrong_type' });
  });

  test('expired token', async () => {
    const tok = await state.sign({ typ: 'state', claims: { tenant_id: 't' }, ttlSeconds: 60, nowMs: 1_000_000_000_000, deps });
    await expect(
      state.verify(tok, { expectedType: 'state', nowMs: 1_000_000_000_000 + 61_000, deps })
    ).rejects.toMatchObject({ code: 'expired' });
  });

  test('tampered payload → bad_signature', async () => {
    const tok = await state.sign({ typ: 'state', claims: { tenant_id: 't' }, ttlSeconds: 60, deps });
    const [payload, sig] = tok.split('.');
    const forged = state._b64urlEncode(JSON.stringify({ typ: 'state', tenant_id: 'EVIL', exp: 9_999_999_999 }));
    await expect(state.verify(`${forged}.${sig}`, { expectedType: 'state', deps })).rejects.toMatchObject({ code: 'bad_signature' });
    void payload;
  });

  test('wrong key → bad_signature', async () => {
    const tok = await state.sign({ typ: 'state', claims: { tenant_id: 't' }, ttlSeconds: 60, deps });
    await expect(state.verify(tok, { expectedType: 'state', deps: { getKey: () => 'different-key' } })).rejects.toMatchObject({ code: 'bad_signature' });
  });

  test.each([
    ['', 'malformed'],
    ['nodot', 'malformed'],
    ['a.b.c', 'malformed'],
    ['.sig', 'malformed'],
    ['payload.', 'malformed'],
  ])('structurally invalid %p → %s', async (tok, code) => {
    await expect(state.verify(tok, { expectedType: 'init', deps })).rejects.toMatchObject({ code });
  });

  test('non-string token → malformed', async () => {
    await expect(state.verify(null, { expectedType: 'init', deps })).rejects.toMatchObject({ code: 'malformed' });
  });

  test('valid signature over non-JSON payload → malformed', async () => {
    // Sign a non-JSON payload with the real key so the HMAC passes but JSON.parse fails.
    const crypto = require('crypto');
    const payloadB64 = state._b64urlEncode('not json');
    const sig = crypto.createHmac('sha256', KEY).update(payloadB64).digest();
    const sigB64 = state._b64urlEncode(sig);
    await expect(state.verify(`${payloadB64}.${sigB64}`, { expectedType: 'init', deps })).rejects.toMatchObject({ code: 'malformed' });
  });
});

describe('getSigningKey (Secrets Manager)', () => {
  test('raw string secret', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'raw-key' });
    await expect(state.getSigningKey()).resolves.toBe('raw-key');
  });

  test('JSON-wrapped { key }', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ key: 'wrapped-key' }) });
    await expect(state.getSigningKey()).resolves.toBe('wrapped-key');
  });

  test('caches after first fetch', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'cached' });
    await state.getSigningKey();
    await state.getSigningKey();
    expect(smMock.commandCalls(GetSecretValueCommand).length).toBe(1);
  });

  test('missing SecretString → throws', async () => {
    smMock.on(GetSecretValueCommand).resolves({});
    await expect(state.getSigningKey()).rejects.toThrow();
  });

  test('JSON with empty "key" → throws (does NOT fall back to HMAC-ing the JSON text)', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ key: '' }) });
    await expect(state.getSigningKey()).rejects.toThrow(/key/);
  });

  test('looks-like-JSON but unparseable → throws', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: '{not valid json' });
    await expect(state.getSigningKey()).rejects.toThrow(/JSON/);
  });
});
