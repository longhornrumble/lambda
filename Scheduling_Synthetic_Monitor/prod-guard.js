'use strict';

/**
 * prod-guard.js — the HARD production safety guard for the synthetic monitor (CI-6 §5.1).
 *
 * The synthetic monitor CREATES and CANCELS real bookings, and (in Phase 2) drives the
 * STAGING_TEST_MODE time-compression that fires reminder/attendance windows in seconds.
 * NONE of that may ever touch a production environment. §5.1's hygiene is a double-gate —
 * `is_synthetic` on the row AND `STAGING_TEST_MODE` on the dispatcher; this module is the
 * ENVIRONMENT half of that gate, asserted by the monitor itself:
 *
 *   if STAGING_TEST_MODE is enabled while ENVIRONMENT is production → REFUSE to initialize.
 *
 * index.js calls assertSafeMode() at MODULE LOAD (a true init refusal — the Lambda
 * cold-start throws, so the function is structurally incapable of running synthetic logic
 * in prod) AND again at handler entry (defense-in-depth against a mutated runtime env).
 *
 * Safety bias: the production check matches BOTH the platform-canonical `production` and
 * the legacy `prod` typo (case-insensitive). The naming-alignment convention forbids
 * `prod`, but a SAFETY guard must fail safe — a misconfigured `ENVIRONMENT=prod` must not
 * slip the gate. (The 2026-06 `ENVIRONMENT=staging` prod defect is precedent that env
 * values do get misconfigured in this platform.)
 */

class ProdGuardError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ProdGuardError';
  }
}

// Truthy forms a deployer might use for the flag. Anything else (unset, 'false', '0',
// 'no', arbitrary) is treated as OFF — the guard only fires on an explicit enable.
function isTestModeOn(value) {
  if (value === true) return true;
  const v = String(value == null ? '' : value).trim().toLowerCase();
  return v === 'true' || v === '1' || v === 'yes' || v === 'on';
}

// Production-like environment. Matches the canonical `production` and the legacy `prod`
// alias (see header — safety bias). Case-insensitive; whitespace-tolerant.
function isProduction(value) {
  const v = String(value == null ? '' : value).trim().toLowerCase();
  return v === 'production' || v === 'prod';
}

/**
 * Throw ProdGuardError iff STAGING_TEST_MODE is enabled in a production environment.
 * Pure (no env reads, no I/O) so it is exhaustively unit-testable; callers pass the env.
 *
 * @param {{environment?: string, stagingTestMode?: string|boolean}} env
 */
function assertSafeMode({ environment, stagingTestMode } = {}) {
  if (isTestModeOn(stagingTestMode) && isProduction(environment)) {
    throw new ProdGuardError(
      'REFUSING to initialize Scheduling_Synthetic_Monitor: STAGING_TEST_MODE is enabled ' +
        `in a production environment (ENVIRONMENT=${environment}). Synthetic bookings and ` +
        'time-compression must never run against production.'
    );
  }
}

module.exports = {
  assertSafeMode,
  ProdGuardError,
  _isTestModeOn: isTestModeOn,
  _isProduction: isProduction,
};
