'use strict';

const { assertSafeMode, ProdGuardError, _isTestModeOn, _isProduction } = require('./prod-guard');

describe('prod-guard — assertSafeMode (HARD production safety guard, §5.1)', () => {
  // ── THE refusal: test-mode ON in a production environment ───────────────────────────
  describe('REFUSES (throws ProdGuardError) when STAGING_TEST_MODE is enabled in production', () => {
    const productionForms = ['production', 'Production', 'PRODUCTION', 'prod', 'Prod', ' production '];
    const testModeOnForms = ['true', 'TRUE', 'True', '1', 'yes', 'on', true, ' true '];

    for (const environment of productionForms) {
      for (const stagingTestMode of testModeOnForms) {
        test(`environment=${JSON.stringify(environment)} stagingTestMode=${JSON.stringify(stagingTestMode)} → throws`, () => {
          expect(() => assertSafeMode({ environment, stagingTestMode })).toThrow(ProdGuardError);
        });
      }
    }

    test('the thrown error names the environment and is a ProdGuardError', () => {
      try {
        assertSafeMode({ environment: 'production', stagingTestMode: 'true' });
        throw new Error('expected assertSafeMode to throw');
      } catch (err) {
        expect(err).toBeInstanceOf(ProdGuardError);
        expect(err.name).toBe('ProdGuardError');
        expect(err.message).toMatch(/production/i);
        expect(err.message).toMatch(/REFUSING/);
      }
    });
  });

  // ── ALLOWED: every other combination must NOT throw ─────────────────────────────────
  describe('ALLOWS (does not throw) in safe combinations', () => {
    const cases = [
      ['staging', 'true'],
      ['staging', true],
      ['development', 'true'],
      ['dev', '1'],
      ['test', 'yes'],
      ['', 'true'], // unset env defaults to staging-class — not production
      [undefined, 'true'],
      // test-mode OFF, even in production, is allowed (the guard is the COMBO)
      ['production', 'false'],
      ['production', '0'],
      ['production', 'no'],
      ['production', ''],
      ['production', undefined],
      ['prod', undefined],
      ['staging', 'false'],
      [undefined, undefined],
    ];
    for (const [environment, stagingTestMode] of cases) {
      test(`environment=${JSON.stringify(environment)} stagingTestMode=${JSON.stringify(stagingTestMode)} → no throw`, () => {
        expect(() => assertSafeMode({ environment, stagingTestMode })).not.toThrow();
      });
    }

    test('no-arg call does not throw', () => {
      expect(() => assertSafeMode()).not.toThrow();
    });
  });

  // ── helper coverage ─────────────────────────────────────────────────────────────────
  describe('_isTestModeOn', () => {
    test.each([['true', true], ['TRUE', true], ['1', true], ['yes', true], ['on', true], [true, true]])(
      '%s → on',
      (v, expected) => expect(_isTestModeOn(v)).toBe(expected)
    );
    test.each([['false', false], ['0', false], ['', false], ['nope', false], [undefined, false], [null, false], [false, false]])(
      '%s → off',
      (v, expected) => expect(_isTestModeOn(v)).toBe(expected)
    );
  });

  describe('_isProduction', () => {
    test.each([['production', true], ['Production', true], ['PROD', true], ['prod', true], [' production ', true]])(
      '%s → production',
      (v, expected) => expect(_isProduction(v)).toBe(expected)
    );
    test.each([['staging', false], ['development', false], ['', false], [undefined, false], [null, false], ['productionish', false]])(
      '%s → not production',
      (v, expected) => expect(_isProduction(v)).toBe(expected)
    );
  });
});
