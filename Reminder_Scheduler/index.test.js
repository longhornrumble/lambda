'use strict';

/**
 * Unit tests for index.js — the prod-synthetic guard (§E1, SR-3) + library re-exports.
 */

const mod = require('./index');

describe('assertNotProdSynthetic (§E1 prod guard)', () => {
  test('throws when STAGING_TEST_MODE=true AND ENVIRONMENT=production', () => {
    expect(() =>
      mod.assertNotProdSynthetic({ STAGING_TEST_MODE: 'true', ENVIRONMENT: 'production' })
    ).toThrow(/refusing to start/);
  });

  test('allows STAGING_TEST_MODE=true on staging', () => {
    expect(() =>
      mod.assertNotProdSynthetic({ STAGING_TEST_MODE: 'true', ENVIRONMENT: 'staging' })
    ).not.toThrow();
  });

  test('allows production when STAGING_TEST_MODE is not "true"', () => {
    expect(() =>
      mod.assertNotProdSynthetic({ STAGING_TEST_MODE: 'false', ENVIRONMENT: 'production' })
    ).not.toThrow();
    expect(() => mod.assertNotProdSynthetic({ ENVIRONMENT: 'production' })).not.toThrow();
  });
});

describe('library re-exports (integrator wiring points)', () => {
  test('exposes the §E1 lifecycle + the E9 reconciler', () => {
    expect(typeof mod.scheduleReminders).toBe('function');
    expect(typeof mod.rebindReminders).toBe('function');
    expect(typeof mod.deleteReminders).toBe('function');
    expect(typeof mod.runReconcile).toBe('function');
    expect(typeof mod.handler).toBe('function');
  });
});
