'use strict';

/**
 * featureGate.js — the backend scheduling feature gate.
 *
 * Scheduling is a configured feature (like Forms): OFF for a tenant unless its config
 * sets `feature_flags.scheduling_enabled === true`. The widget + config-builder already
 * honor this flag; this module is the BACKEND enforcement for the Lambdas that activate
 * scheduling but do NOT already hold the loaded tenant config — the redemption handler
 * (D4), the calendar-watch onboarder, and the booking-commit handler (C8). (The Bedrock
 * Streaming Handler already has `config` in hand and uses `bindingContext.isSchedulingEnabled`
 * directly — same predicate, no load.)
 *
 * FAIL-CLOSED: a missing config object, a missing feature_flags block, a missing/non-true
 * flag, OR any error loading the config (S3 miss, malformed JSON, access denied) → the
 * tenant is treated as scheduling-DISABLED. The safe default for a feature gate is "off":
 * a config we cannot read must never silently enable a calendar-mutating path.
 *
 * The tenant config object lives at s3://{CONFIG_BUCKET}/tenants/{tenantId}/config.json —
 * the same object `shared/bedrock-core.loadConfig` ultimately resolves, but keyed directly
 * by tenantId (these Lambdas hold the raw tenantId, not the tenant hash, so they skip the
 * registry/hash indirection).
 *
 * DI seam (mirrors sessionBinding.js §B12): `deps = { s3, bucket, loadTenantConfig }`.
 * Production callers pass nothing and get the module defaults; tests inject a fake.
 */

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// Module-level default client (reused across warm invocations). Tests inject deps.s3.
const defaultS3 = new S3Client({});

const DEFAULT_CONFIG_BUCKET = process.env.CONFIG_BUCKET || process.env.S3_CONFIG_BUCKET || 'myrecruiter-picasso';

/**
 * Load and parse a tenant config object from S3. Throws on miss / malformed JSON — the
 * caller (isSchedulingEnabledForTenant) converts any throw into the fail-closed default.
 * @param {string} tenantId
 * @param {object} [deps] - { s3, bucket }
 * @returns {Promise<object>} the parsed tenant config
 */
async function loadTenantConfig(tenantId, { s3 = defaultS3, bucket = DEFAULT_CONFIG_BUCKET } = {}) {
  const res = await s3.send(new GetObjectCommand({
    Bucket: bucket,
    Key: `tenants/${tenantId}/config.json`,
  }));
  const raw = await res.Body.transformToString();
  return JSON.parse(raw);
}

/**
 * The pure gate predicate over an already-loaded config. Strict `=== true` so a truthy
 * non-boolean ("true", 1) does NOT enable the feature.
 * @param {object|null|undefined} config
 * @returns {boolean}
 */
function isSchedulingEnabled(config) {
  return config?.feature_flags?.scheduling_enabled === true;
}

/**
 * Resolve, fail-closed, whether scheduling is enabled for a tenant by loading its config.
 * Any error (missing tenantId, S3 miss, malformed JSON) → false. PII-safe: never logs the
 * tenantId (the config key encodes it); logs only an error discriminator.
 * @param {string} tenantId
 * @param {object} [deps] - { s3, bucket, loadTenantConfig } (loadTenantConfig injectable for tests)
 * @returns {Promise<boolean>}
 */
async function isSchedulingEnabledForTenant(tenantId, deps = {}) {
  if (!tenantId) return false;
  const load = deps.loadTenantConfig || loadTenantConfig;
  try {
    const config = await load(tenantId, deps);
    return isSchedulingEnabled(config);
  } catch (err) {
    console.error(
      `[scheduling-gate] config load failed → fail-closed (disabled): error_name=${(err && err.name) || 'unknown'}`
    );
    return false;
  }
}

module.exports = {
  isSchedulingEnabledForTenant,
  isSchedulingEnabled,
  loadTenantConfig,
};
