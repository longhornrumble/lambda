'use strict';

/**
 * aws-client-config.js — bounded AWS SDK v3 client config for the B11 remediator.
 *
 * Mirrors the C8 / B9B10 sdkConfig: every SDK client (DynamoDB, Secrets Manager)
 * MUST bound its requests. B11 is a direct-invoke; an upstream hang (DDB / Secrets
 * Manager) on a naked client with the SDK's unbounded default would stall the whole
 * invoke until the Lambda timeout instead of failing fast. requestTimeout caps a
 * single attempt; maxAttempts:2 gives one bounded retry (the SDK's own backoff).
 *
 * Values are env-overridable for ops tuning; defaults are deliberately tight (well
 * under the Lambda timeout).
 */

const { NodeHttpHandler } = require('@smithy/node-http-handler');

const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const MAX_ATTEMPTS = Number(process.env.AWS_MAX_ATTEMPTS || 2);

function sdkConfig(extra = {}) {
  return {
    maxAttempts: MAX_ATTEMPTS,
    requestHandler: new NodeHttpHandler({
      connectionTimeout: CONNECTION_TIMEOUT_MS,
      requestTimeout: REQUEST_TIMEOUT_MS,
    }),
    ...extra,
  };
}

module.exports = {
  sdkConfig,
  _CONNECTION_TIMEOUT_MS: CONNECTION_TIMEOUT_MS,
  _REQUEST_TIMEOUT_MS: REQUEST_TIMEOUT_MS,
  _MAX_ATTEMPTS: MAX_ATTEMPTS,
};
