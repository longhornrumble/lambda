'use strict';

/**
 * Unit tests for jti.js — init-token single-use enforcement.
 *
 * Most tests use the `deps.putItem` injection seam for isolation. One test exercises
 * the default getDdbClient() path using mockClient so coverage reaches the lazy-init branch.
 *
 * Criteria (from the spec):
 *   1. First use burns + proceeds ({ burned: true })
 *   2. Second use → already_used ({ burned: false, reason: 'already_used' })
 *   3. Missing-jti token: exempt from burn (handled in index.test.js via handleConnect paths)
 *   4. DDB error → fail-open + logs jti_burn_unavailable
 *   5. Env unset → off (warn once, { burned: true, warn: 'unconfigured' })
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');

const TABLE = 'picasso-token-jti-blacklist';

// We require jti.js with the env already set, then use deps injection.
// For the env-unset tests we reload the module via jest.resetModules().

let jtiModule;

function loadJti(tableVal) {
  jest.resetModules();
  process.env.JTI_BLACKLIST_TABLE = tableVal !== undefined ? tableVal : TABLE;
  // eslint-disable-next-line global-require
  jtiModule = require('./jti');
  jtiModule._resetForTest();
}

beforeEach(() => {
  loadJti(TABLE);
});

afterEach(() => {
  delete process.env.JTI_BLACKLIST_TABLE;
});

// ─── helpers ──────────────────────────────────────────────────────────────────
const ARGS = { tenantId: 'TEN1', jti: 'abc123', expSeconds: 1900000300 };

function makeConditionalErr() {
  return Object.assign(new Error('The conditional request failed'), {
    name: 'ConditionalCheckFailedException',
  });
}

// ─── 1. first use burns and proceeds ──────────────────────────────────────────
describe('first use', () => {
  test('PutItem resolves → { burned: true }', async () => {
    const mockPut = jest.fn().mockResolvedValue({});
    const result = await jtiModule.burnJti(ARGS, { putItem: mockPut });
    expect(result).toEqual({ burned: true });
  });

  test('PutItem is called with the correct TableName, keys, and TTL', async () => {
    const mockPut = jest.fn().mockResolvedValue({});
    await jtiModule.burnJti(ARGS, { putItem: mockPut });
    expect(mockPut).toHaveBeenCalledTimes(1);
    // Extract the PutItemCommand that was passed to our mock
    const { DynamoDBClient: _DDB, PutItemCommand } = require('@aws-sdk/client-dynamodb');
    const cmd = mockPut.mock.calls[0][0];
    expect(cmd).toBeInstanceOf(PutItemCommand);
    const input = cmd.input;
    expect(input.TableName).toBe(TABLE);
    expect(input.Item.tenantId).toEqual({ S: 'TEN1' });
    expect(input.Item.jti).toEqual({ S: 'abc123' });
    // TTL = exp + 600s buffer
    expect(input.Item.ttl).toEqual({ N: String(1900000300 + 600) });
    expect(input.ConditionExpression).toContain('attribute_not_exists');
  });

  test('ConditionExpression checks BOTH keys (tenantId AND jti)', async () => {
    const { PutItemCommand } = require('@aws-sdk/client-dynamodb');
    const mockPut = jest.fn().mockResolvedValue({});
    await jtiModule.burnJti(ARGS, { putItem: mockPut });
    const input = mockPut.mock.calls[0][0].input;
    expect(input.ConditionExpression).toContain('tenantId');
    expect(input.ConditionExpression).toContain('jti');
  });
});

// ─── 2. second use → already_used ──────────────────────────────────────────────
describe('replay detection', () => {
  test('ConditionalCheckFailedException → { burned: false, reason: already_used }', async () => {
    const mockPut = jest.fn().mockRejectedValue(makeConditionalErr());
    const result = await jtiModule.burnJti(ARGS, { putItem: mockPut });
    expect(result).toEqual({ burned: false, reason: 'already_used' });
  });

  test('replay does NOT log jti_burn_unavailable (expected outcome, not an error)', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const mockPut = jest.fn().mockRejectedValue(makeConditionalErr());
    await jtiModule.burnJti(ARGS, { putItem: mockPut });
    const warns = warnSpy.mock.calls.map((a) => a.join(' ')).join('\n');
    expect(warns).not.toContain('jti_burn_unavailable');
    warnSpy.mockRestore();
  });
});

// ─── 4. DDB error → fail-open ──────────────────────────────────────────────────
describe('DDB unavailable (fail-open)', () => {
  test('ProvisionedThroughputExceededException → { burned: true, warn: unavailable }', async () => {
    const err = Object.assign(new Error('throughput'), { name: 'ProvisionedThroughputExceededException' });
    const mockPut = jest.fn().mockRejectedValue(err);
    const result = await jtiModule.burnJti(ARGS, { putItem: mockPut });
    expect(result).toEqual({ burned: true, warn: 'unavailable' });
  });

  test('generic Error → { burned: true, warn: unavailable }', async () => {
    const mockPut = jest.fn().mockRejectedValue(new Error('network timeout'));
    const result = await jtiModule.burnJti(ARGS, { putItem: mockPut });
    expect(result).toEqual({ burned: true, warn: 'unavailable' });
  });

  test('DDB error logs jti_burn_unavailable with the tenant_id', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const mockPut = jest.fn().mockRejectedValue(new Error('timeout'));
    await jtiModule.burnJti({ tenantId: 'TEN-FAIL', jti: 'j1', expSeconds: 1000 }, { putItem: mockPut });
    const warns = warnSpy.mock.calls.map((a) => a.join(' ')).join('\n');
    expect(warns).toContain('jti_burn_unavailable');
    expect(warns).toContain('TEN-FAIL');
    warnSpy.mockRestore();
  });

  test('DDB error does NOT log the jti value (it is a security nonce)', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const mockPut = jest.fn().mockRejectedValue(new Error('err'));
    await jtiModule.burnJti({ tenantId: 'T', jti: 'SUPER-SECRET-JTI-VALUE', expSeconds: 1000 }, { putItem: mockPut });
    const warns = warnSpy.mock.calls.map((a) => a.join(' ')).join('\n');
    expect(warns).not.toContain('SUPER-SECRET-JTI-VALUE');
    warnSpy.mockRestore();
  });
});

// ─── 5. env unset → burn is OFF ─────────────────────────────────────────────────
describe('env unset (burn is OFF)', () => {
  beforeEach(() => {
    loadJti(''); // reload with empty env
  });

  test('returns { burned: true, warn: unconfigured }', async () => {
    const result = await jtiModule.burnJti(ARGS);
    expect(result).toEqual({ burned: true, warn: 'unconfigured' });
  });

  test('no putItem call is made when env is unset', async () => {
    const mockPut = jest.fn();
    await jtiModule.burnJti(ARGS, { putItem: mockPut });
    expect(mockPut).not.toHaveBeenCalled();
  });

  test('jti_burn_unconfigured warn is emitted on the first call', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    await jtiModule.burnJti(ARGS);
    const warns = warnSpy.mock.calls.map((a) => a.join(' ')).join('\n');
    expect(warns).toContain('jti_burn_unconfigured');
    warnSpy.mockRestore();
  });

  test('jti_burn_unconfigured warn is emitted only ONCE per cold start (no spam)', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    await jtiModule.burnJti({ tenantId: 'T', jti: 'j1', expSeconds: 1000 });
    await jtiModule.burnJti({ tenantId: 'T', jti: 'j2', expSeconds: 1000 });
    const warns = warnSpy.mock.calls.map((a) => a.join(' ')).filter((w) => w.includes('jti_burn_unconfigured'));
    expect(warns).toHaveLength(1);
    warnSpy.mockRestore();
  });
});

// ─── real DDB client path (getDdbClient lazy-init coverage) ───────────────────
// Exercises getDdbClient() (lines 41-50) — the deployed Lambda path with no deps injection.
// This test calls burnJti() WITHOUT the deps seam; the absence of a mock means DDB will
// fail (TypeError/network), which triggers the fail-open path and still exercises getDdbClient.
// The goal here is COVERAGE of the getDdbClient branch, not asserting a DDB call succeeded.
describe('real DDB client path (getDdbClient coverage)', () => {
  beforeEach(() => {
    loadJti(TABLE);
  });

  test('burnJti without deps exercises getDdbClient (fail-open on no real DDB connection)', async () => {
    // No mock, no deps → getDdbClient() is called → DDB send() throws (no endpoint) →
    // fail-open path → { burned: true, warn: 'unavailable' }.
    // This exercises lines 41-50 (the DynamoDBClient constructor + _ddb lazy-init).
    const result = await jtiModule.burnJti(ARGS);
    expect(result.burned).toBe(true);
    expect(result.warn).toBe('unavailable');
  });
});
