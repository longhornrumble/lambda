const { mockClient } = require('aws-sdk-client-mock');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const smMock = mockClient(SecretsManagerClient);

// Validator module imports SecretsManagerClient at top-level, so we require
// it AFTER mockClient is established but jest hoists imports anyway. The
// aws-sdk-client-mock pattern intercepts the .send() call on the shared
// client instance, so order is fine.
const {
  validateCfOriginHeader,
  CF_ORIGIN_HEADER_NAME,
  SECRET_FAILURE_TTL_MS,
  resetCacheForTests,
} = require('../cf-origin-validator');

const SECRET_VALUE = 'r0t13-d3adb33fc4f3b4be-r0t13';

beforeEach(() => {
  smMock.reset();
  resetCacheForTests();
  delete process.env.REQUIRE_CF_ORIGIN_HEADER;
  delete process.env.CF_ORIGIN_SECRET_NAME;
});

describe('validateCfOriginHeader / flag off (default rollout state)', () => {
  test('admits requests when REQUIRE_CF_ORIGIN_HEADER unset (no SM call)', async () => {
    const result = await validateCfOriginHeader({ headers: {} });
    expect(result).toEqual({ valid: true, reason: null });
    expect(smMock.calls()).toHaveLength(0);
  });

  test('admits requests when REQUIRE_CF_ORIGIN_HEADER=false (no SM call)', async () => {
    process.env.REQUIRE_CF_ORIGIN_HEADER = 'false';
    const result = await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': 'whatever' } });
    expect(result).toEqual({ valid: true, reason: null });
    expect(smMock.calls()).toHaveLength(0);
  });

  test('admits requests when REQUIRE_CF_ORIGIN_HEADER set to non-true (no SM call)', async () => {
    process.env.REQUIRE_CF_ORIGIN_HEADER = 'TRUE  ';  // trailing whitespace, mixed-case
    const result = await validateCfOriginHeader({ headers: {} });
    expect(result).toEqual({ valid: true, reason: null });
    expect(smMock.calls()).toHaveLength(0);
  });
});

describe('validateCfOriginHeader / flag on / header handling', () => {
  beforeEach(() => {
    process.env.REQUIRE_CF_ORIGIN_HEADER = 'true';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: SECRET_VALUE });
  });

  test('rejects when header missing entirely', async () => {
    const result = await validateCfOriginHeader({ headers: {} });
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/missing CF origin header/);
  });

  test('rejects when headers field absent on event', async () => {
    const result = await validateCfOriginHeader({});
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/missing CF origin header/);
  });

  test('admits when header matches secret', async () => {
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result).toEqual({ valid: true, reason: null });
  });

  test('admits with case-variant header key', async () => {
    const result = await validateCfOriginHeader({
      headers: { 'X-Picasso-CF-Origin': SECRET_VALUE },
    });
    expect(result).toEqual({ valid: true, reason: null });
  });

  test('rejects when header value wrong (same length as secret)', async () => {
    // Same length as SECRET_VALUE to exercise the timingSafeEqual path,
    // not the length-mismatch short-circuit.
    const sameLength = 'X'.repeat(SECRET_VALUE.length);
    expect(sameLength.length).toBe(SECRET_VALUE.length);
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': sameLength },
    });
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/CF origin header mismatch/);
  });

  test('rejects when header value wrong length (length-mismatch short-circuit)', async () => {
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': 'too-short' },
    });
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/CF origin header mismatch/);
  });
});

describe('validateCfOriginHeader / secret unavailable', () => {
  beforeEach(() => {
    process.env.REQUIRE_CF_ORIGIN_HEADER = 'true';
  });

  test('fails closed when Secrets Manager throws', async () => {
    smMock.on(GetSecretValueCommand).rejects(new Error('ResourceNotFoundException'));
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/CF origin secret unavailable/);
  });

  test('fails closed on empty SecretString', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: '' });
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/CF origin secret unavailable/);
  });

  test('fails closed on whitespace-only SecretString', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: '   \n\t  ' });
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/CF origin secret unavailable/);
  });

  test('60s failure TTL: failure cached, second call within window skips SM', async () => {
    smMock.on(GetSecretValueCommand).rejects(new Error('throttled'));
    await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } });
    await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } });
    await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } });
    // Only ONE SM call despite three validations within the 60s TTL window
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);
  });

  test('failure TTL expires: SM is retried after window', async () => {
    smMock
      .on(GetSecretValueCommand)
      .rejectsOnce(new Error('throttled'))
      .resolves({ SecretString: SECRET_VALUE });

    // First call: SM throws, cache poisoned
    const first = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(first.valid).toBe(false);

    // Move time forward past the TTL (mock Date.now)
    const realNow = Date.now;
    try {
      Date.now = () => realNow() + SECRET_FAILURE_TTL_MS + 1000;
      const second = await validateCfOriginHeader({
        headers: { 'x-picasso-cf-origin': SECRET_VALUE },
      });
      expect(second).toEqual({ valid: true, reason: null });
      expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(2);
    } finally {
      Date.now = realNow;
    }
  });
});

describe('validateCfOriginHeader / secret JSON-or-raw handling', () => {
  beforeEach(() => {
    process.env.REQUIRE_CF_ORIGIN_HEADER = 'true';
  });

  test('handles plaintext secret', async () => {
    smMock.on(GetSecretValueCommand).resolves({ SecretString: SECRET_VALUE });
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result).toEqual({ valid: true, reason: null });
  });

  test('handles console-created JSON secret with .secret key', async () => {
    smMock.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ secret: SECRET_VALUE }),
    });
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result).toEqual({ valid: true, reason: null });
  });

  test('handles console-created JSON secret with .value key', async () => {
    smMock.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ value: SECRET_VALUE }),
    });
    const result = await validateCfOriginHeader({
      headers: { 'x-picasso-cf-origin': SECRET_VALUE },
    });
    expect(result).toEqual({ valid: true, reason: null });
  });
});

describe('validateCfOriginHeader / lifetime cache (success path)', () => {
  beforeEach(() => {
    process.env.REQUIRE_CF_ORIGIN_HEADER = 'true';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: SECRET_VALUE });
  });

  test('secret cached after first successful fetch (no second SM call)', async () => {
    await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } });
    await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } });
    await validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } });
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);
  });

  test('concurrent first-fetch requests share a single in-flight SM call', async () => {
    const results = await Promise.all([
      validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } }),
      validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } }),
      validateCfOriginHeader({ headers: { 'x-picasso-cf-origin': SECRET_VALUE } }),
    ]);
    expect(results.every(r => r.valid)).toBe(true);
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);
  });
});

describe('validateCfOriginHeader / exports', () => {
  test('exports the canonical header name constant', () => {
    expect(CF_ORIGIN_HEADER_NAME).toBe('x-picasso-cf-origin');
  });

  test('exports the failure-TTL constant', () => {
    expect(SECRET_FAILURE_TTL_MS).toBe(60 * 1000);
  });
});
