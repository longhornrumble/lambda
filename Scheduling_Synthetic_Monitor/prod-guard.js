'use strict';

/**
 * prod-guard.js — the HARD production safety guard for the synthetic monitor (CI-6 §5.1).
 *
 * The synthetic monitor CREATES and CANCELS real bookings, and (in Phase 2) drives the
 * STAGING_TEST_MODE time-compression that fires reminder/attendance windows in seconds.
 * NONE of that may ever touch a production environment. §5.1's hygiene is a double-gate —
 * `is_synthetic` on the row AND `STAGING_TEST_MODE` on the dispatcher; this module is the
 * ENVIRONMENT half of that gate, asserted by the monitor itself.
 *
 * FAIL-CLOSED posture: if `STAGING_TEST_MODE` is enabled, the monitor refuses to initialize
 * UNLESS `ENVIRONMENT` is a recognized safe (non-production) environment. So production,
 * the legacy `prod` typo, AND any unknown/misspelled value (`prd`, `prod-1`, `qa`, …) all
 * refuse — only the known-safe set passes. A safety guard must fail safe on the unknown,
 * not allow it (the 2026-06 `ENVIRONMENT=staging` prod defect is precedent that env values
 * get misconfigured in this platform).
 *
 * index.js calls assertSafeMode() at MODULE LOAD (a true init refusal — the Lambda
 * cold-start throws) AND at handler entry (defense-in-depth against a mutated runtime env).
 */

class ProdGuardError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ProdGuardError';
  }
}

// Recognized NON-production environments. An empty/unset ENVIRONMENT is staging-class
// (booking-table.js defaults the table suffix to `staging` when unset) → safe.
const KNOWN_SAFE_ENVIRONMENTS = new Set(['staging', 'development', 'dev', 'test', 'local', '']);

function normalizeEnv(value) {
  return String(value == null ? '' : value).trim().toLowerCase();
}

// Truthy forms a deployer might use for the flag. Anything else (unset, 'false', '0',
// 'no', arbitrary) is treated as OFF — the guard only fires on an explicit enable.
function isTestModeOn(value) {
  if (value === true) return true;
  const v = normalizeEnv(value);
  return v === 'true' || v === '1' || v === 'yes' || v === 'on';
}

// Production-like (used only to phrase the refusal message; the gate is isKnownSafeEnv).
function isProduction(value) {
  const v = normalizeEnv(value);
  return v === 'production' || v === 'prod';
}

function isKnownSafeEnv(value) {
  return KNOWN_SAFE_ENVIRONMENTS.has(normalizeEnv(value));
}

/**
 * Throw ProdGuardError when STAGING_TEST_MODE is enabled and ENVIRONMENT is NOT a
 * recognized safe environment (fail-closed). Pure (no env reads, no I/O) — exhaustively
 * unit-testable; callers pass the env.
 *
 * @param {{environment?: string, stagingTestMode?: string|boolean}} env
 */
function assertSafeMode({ environment, stagingTestMode } = {}) {
  if (!isTestModeOn(stagingTestMode)) return; // test-mode off → always safe
  if (isKnownSafeEnv(environment)) return; // explicitly recognized non-prod env → safe
  const detail = isProduction(environment)
    ? `in a production environment (ENVIRONMENT=${environment})`
    : `but ENVIRONMENT=${JSON.stringify(environment)} is not a recognized safe environment (fail-closed)`;
  throw new ProdGuardError(
    'REFUSING to initialize Scheduling_Synthetic_Monitor: STAGING_TEST_MODE is enabled ' +
      `${detail}. Synthetic bookings and time-compression must never run outside staging/dev.`
  );
}

module.exports = {
  assertSafeMode,
  ProdGuardError,
  _isTestModeOn: isTestModeOn,
  _isProduction: isProduction,
  _isKnownSafeEnv: isKnownSafeEnv,
  _KNOWN_SAFE_ENVIRONMENTS: KNOWN_SAFE_ENVIRONMENTS,
};
