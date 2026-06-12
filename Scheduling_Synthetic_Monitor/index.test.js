'use strict';

// index.js runs assertSafeMode at MODULE LOAD, so each test that needs a specific env
// loads the module in isolation with that env set.

const SAFE_ENV = { ENVIRONMENT: 'staging', STAGING_TEST_MODE: 'true' };

function loadIndexWith(env) {
  let mod;
  jest.isolateModules(() => {
    const prev = { ENVIRONMENT: process.env.ENVIRONMENT, STAGING_TEST_MODE: process.env.STAGING_TEST_MODE };
    process.env.ENVIRONMENT = env.ENVIRONMENT;
    process.env.STAGING_TEST_MODE = env.STAGING_TEST_MODE;
    try {
      mod = require('./index');
    } finally {
      process.env.ENVIRONMENT = prev.ENVIRONMENT;
      process.env.STAGING_TEST_MODE = prev.STAGING_TEST_MODE;
    }
  });
  return mod;
}

describe('index — module-load prod-guard (INIT refusal)', () => {
  test('REFUSES to load when STAGING_TEST_MODE is set in production', () => {
    expect(() => loadIndexWith({ ENVIRONMENT: 'production', STAGING_TEST_MODE: 'true' })).toThrow(/REFUSING/);
  });

  test('REFUSES to load with the legacy `prod` alias + test-mode', () => {
    expect(() => loadIndexWith({ ENVIRONMENT: 'prod', STAGING_TEST_MODE: 'true' })).toThrow(/REFUSING/);
  });

  test('REFUSES to load on an unknown env + test-mode (fail-closed)', () => {
    expect(() => loadIndexWith({ ENVIRONMENT: 'qa', STAGING_TEST_MODE: 'true' })).toThrow(/REFUSING/);
  });

  test('loads fine in staging with test-mode on', () => {
    expect(() => loadIndexWith(SAFE_ENV)).not.toThrow();
  });

  test('loads fine in production when test-mode is OFF', () => {
    expect(() => loadIndexWith({ ENVIRONMENT: 'production', STAGING_TEST_MODE: 'false' })).not.toThrow();
  });
});

describe('index — handler cycle dispatch', () => {
  let handler;
  beforeEach(() => {
    handler = loadIndexWith(SAFE_ENV).handler;
  });

  test('routes cancel → runCancelCycle', async () => {
    const runCancelCycle = jest.fn().mockResolvedValue({ cycle: 'cancel', success: true });
    const res = await handler({ cycle: 'cancel' }, {}, { runCancelCycle });
    expect(runCancelCycle).toHaveBeenCalled();
    expect(res.cycle).toBe('cancel');
  });

  test('routes reminder → runReminderCycle', async () => {
    const runReminderCycle = jest.fn().mockResolvedValue({ cycle: 'reminder', success: true });
    const res = await handler({ cycle: 'reminder' }, {}, { runReminderCycle });
    expect(runReminderCycle).toHaveBeenCalled();
    expect(res.cycle).toBe('reminder');
  });

  test('routes cleanup → runCleanup', async () => {
    const runCleanup = jest.fn().mockResolvedValue({ cycle: 'cleanup', success: true });
    await handler({ cycle: 'cleanup' }, {}, { runCleanup });
    expect(runCleanup).toHaveBeenCalled();
  });

  test('routes revocation_observe → runRevocationObserve with slug+token', async () => {
    const runRevocationObserve = jest.fn().mockResolvedValue({ cycle: 'revocation', success: true });
    await handler({ cycle: 'revocation_observe', slug: '/cancel', token: 'tok' }, {}, { runRevocationObserve });
    expect(runRevocationObserve).toHaveBeenCalledWith({ slug: '/cancel', token: 'tok' }, expect.any(Object));
  });

  test('routes disposition → runDispositionCycle', async () => {
    const runDispositionCycle = jest.fn().mockResolvedValue({ cycle: 'disposition', success: true });
    const res = await handler({ cycle: 'disposition' }, {}, { runDispositionCycle });
    expect(runDispositionCycle).toHaveBeenCalled();
    expect(res.cycle).toBe('disposition');
  });

  test('unknown cycle → structured failure (no throw)', async () => {
    const res = await handler({ cycle: 'nope' }, {});
    expect(res).toMatchObject({ success: false });
    expect(res.error).toMatch(/unknown cycle/);
  });

  test('undefined event → default {} → unknown cycle (no throw)', async () => {
    const res = await handler(undefined, {});
    expect(res).toMatchObject({ success: false });
    expect(res.error).toMatch(/unknown cycle/);
  });

  test('handler re-asserts the guard at entry (defense-in-depth)', async () => {
    handler = loadIndexWith(SAFE_ENV).handler;
    const prev = { ENVIRONMENT: process.env.ENVIRONMENT, STAGING_TEST_MODE: process.env.STAGING_TEST_MODE };
    process.env.ENVIRONMENT = 'production';
    process.env.STAGING_TEST_MODE = 'true';
    try {
      await expect(handler({ cycle: 'cleanup' }, {}, { runCleanup: jest.fn() })).rejects.toThrow(/REFUSING/);
    } finally {
      process.env.ENVIRONMENT = prev.ENVIRONMENT;
      process.env.STAGING_TEST_MODE = prev.STAGING_TEST_MODE;
    }
  });
});
