'use strict';

/**
 * aws-client-config.js — shared AWS SDK v3 client config for Calendar_Lifecycle_Consumer.
 *
 * Every SDK client bounds its requests: an SQS consumer holds a message in flight for
 * the visibility timeout, and an upstream hang (DDB / SNS) without a per-attempt timeout
 * would stall the Lambda until the function timeout, delaying the rest of the batch.
 * requestTimeout caps a single attempt; maxAttempts:2 gives one bounded retry (the SDK's
 * own backoff), not the unbounded default.
 *
 * Values are env-overridable for ops tuning; defaults are deliberately tight (well under
 * the Lambda timeout) so a hang fails fast into the SQS redrive path. Each Lambda owns its
 * OWN copy of this module (never a shared cross-Lambda module — CLAUDE.md "never-share")
 * — mirrors the sibling `Calendar_Event_Consumer/aws-client-config.js` convention.
 */

const { NodeHttpHandler } = require('@smithy/node-http-handler');

const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const MAX_ATTEMPTS = Number(process.env.AWS_MAX_ATTEMPTS || 2);

// Returns a fresh config object per call (a NodeHttpHandler is cheap; callers each
// instantiate their own client once at module load).
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
