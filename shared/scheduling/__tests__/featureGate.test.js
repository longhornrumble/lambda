'use strict';

/**
 * featureGate tests — the backend scheduling feature gate.
 *
 * Two surfaces, mirroring sessionBinding.test.js:
 *  1. The DI seam (deps injected) — the contract surface every consumer uses.
 *  2. The module's own default S3 path, exercised via aws-sdk-client-mock so the
 *     real GetObject → Body.transformToString → JSON.parse path is covered.
 *
 * The invariant under test is FAIL-CLOSED: anything that isn't an explicit
 * feature_flags.scheduling_enabled === true resolves to DISABLED.
 */

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

const s3Mock = mockClient(S3Client);

const {
  isSchedulingEnabled,
  isSchedulingEnabledForTenant,
  loadTenantConfig,
} = require('../featureGate');

beforeEach(() => {
  s3Mock.reset();
  jest.restoreAllMocks();
});

describe('isSchedulingEnabled (pure predicate)', () => {
  test('flag === true → enabled', () => {
    expect(isSchedulingEnabled({ feature_flags: { scheduling_enabled: true } })).toBe(true);
  });
  test('flag false → disabled', () => {
    expect(isSchedulingEnabled({ feature_flags: { scheduling_enabled: false } })).toBe(false);
  });
  test('flag absent (feature_flags present) → disabled', () => {
    expect(isSchedulingEnabled({ feature_flags: {} })).toBe(false);
  });
  test('no feature_flags block → disabled', () => {
    expect(isSchedulingEnabled({})).toBe(false);
  });
  test('null / undefined → disabled (never throws)', () => {
    expect(isSchedulingEnabled(null)).toBe(false);
    expect(isSchedulingEnabled(undefined)).toBe(false);
  });
  test('truthy non-true → disabled (strict ===)', () => {
    expect(isSchedulingEnabled({ feature_flags: { scheduling_enabled: 'true' } })).toBe(false);
    expect(isSchedulingEnabled({ feature_flags: { scheduling_enabled: 1 } })).toBe(false);
  });
});

describe('isSchedulingEnabledForTenant — DI seam (deps.loadTenantConfig injected)', () => {
  test('missing tenantId → false (no load attempted)', async () => {
    const load = jest.fn();
    expect(await isSchedulingEnabledForTenant('', { loadTenantConfig: load })).toBe(false);
    expect(await isSchedulingEnabledForTenant(undefined, { loadTenantConfig: load })).toBe(false);
    expect(load).not.toHaveBeenCalled();
  });

  test('config enabled → true', async () => {
    const load = jest.fn().mockResolvedValue({ feature_flags: { scheduling_enabled: true } });
    expect(await isSchedulingEnabledForTenant('MYR384719', { loadTenantConfig: load })).toBe(true);
    expect(load).toHaveBeenCalledWith('MYR384719', expect.any(Object));
  });

  test('config disabled → false', async () => {
    const load = jest.fn().mockResolvedValue({ feature_flags: { scheduling_enabled: false } });
    expect(await isSchedulingEnabledForTenant('MYR384719', { loadTenantConfig: load })).toBe(false);
  });

  test('config has no feature_flags → false', async () => {
    const load = jest.fn().mockResolvedValue({ tenant_id: 'MYR384719' });
    expect(await isSchedulingEnabledForTenant('MYR384719', { loadTenantConfig: load })).toBe(false);
  });

  test('load throws (S3 miss / access denied) → false (fail-closed), logs discriminator only', async () => {
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const load = jest.fn().mockRejectedValue(Object.assign(new Error('boom'), { name: 'NoSuchKey' }));
    expect(await isSchedulingEnabledForTenant('MYR384719', { loadTenantConfig: load })).toBe(false);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('fail-closed'));
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining('NoSuchKey'));
    // PII: the tenantId must NOT appear in the log line
    expect(errSpy.mock.calls[0][0]).not.toContain('MYR384719');
  });

  test('load throws with no name → fail-closed with "unknown" discriminator', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const load = jest.fn().mockRejectedValue('not-an-error-object');
    expect(await isSchedulingEnabledForTenant('MYR384719', { loadTenantConfig: load })).toBe(false);
  });
});

describe('loadTenantConfig + isSchedulingEnabledForTenant — default S3 path (aws-sdk-client-mock)', () => {
  test('default path: reads tenants/{id}/config.json and resolves enabled', async () => {
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToString: async () => JSON.stringify({ feature_flags: { scheduling_enabled: true } }) },
    });
    // no deps → exercises defaultS3 + DEFAULT_CONFIG_BUCKET + GetObjectCommand + transformToString + JSON.parse
    expect(await isSchedulingEnabledForTenant('MYR384719')).toBe(true);
    const call = s3Mock.commandCalls(GetObjectCommand)[0];
    expect(call.args[0].input.Key).toBe('tenants/MYR384719/config.json');
  });

  test('default path: falls back to tenants/{id}/{id}-config.json when config.json 404s', async () => {
    // The MYR384719-style tenant: only the {id}-config.json key exists.
    s3Mock
      .on(GetObjectCommand, { Key: 'tenants/MYR384719/config.json' })
      .rejects(Object.assign(new Error('missing'), { name: 'NoSuchKey' }));
    s3Mock
      .on(GetObjectCommand, { Key: 'tenants/MYR384719/MYR384719-config.json' })
      .resolves({ Body: { transformToString: async () => JSON.stringify({ feature_flags: { scheduling_enabled: true } }) } });
    expect(await isSchedulingEnabledForTenant('MYR384719')).toBe(true);
    const keysTried = s3Mock.commandCalls(GetObjectCommand).map((c) => c.args[0].input.Key);
    expect(keysTried).toEqual([
      'tenants/MYR384719/config.json',
      'tenants/MYR384719/MYR384719-config.json',
    ]);
  });

  test('default path: BOTH keys 404 → fail-closed false', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    s3Mock.on(GetObjectCommand).rejects(Object.assign(new Error('missing'), { name: 'NoSuchKey' }));
    expect(await isSchedulingEnabledForTenant('MYR384719')).toBe(false);
    expect(s3Mock.commandCalls(GetObjectCommand).length).toBe(2); // tried both keys
  });

  test('default path: malformed JSON → fail-closed false', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToString: async () => 'not json{' },
    });
    expect(await isSchedulingEnabledForTenant('MYR384719')).toBe(false);
  });

  test('default path: S3 send rejects → fail-closed false', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    s3Mock.on(GetObjectCommand).rejects(Object.assign(new Error('denied'), { name: 'AccessDenied' }));
    expect(await isSchedulingEnabledForTenant('MYR384719')).toBe(false);
  });

  test('loadTenantConfig honors an injected bucket', async () => {
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToString: async () => JSON.stringify({ feature_flags: { scheduling_enabled: true } }) },
    });
    const cfg = await loadTenantConfig('MYR384719', { bucket: 'custom-bucket' });
    expect(cfg.feature_flags.scheduling_enabled).toBe(true);
    const call = s3Mock.commandCalls(GetObjectCommand)[0];
    expect(call.args[0].input.Bucket).toBe('custom-bucket');
  });
});
