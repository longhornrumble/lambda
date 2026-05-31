'use strict';

/**
 * aws-client-config.js — shared AWS SDK v3 client config for C8.
 *
 * Every SDK client in the commit path MUST bound its requests: the confirmation
 * email rides a 60s SLA and the whole transaction holds a slot lock, so an
 * upstream hang (DDB / Secrets Manager / SES / SNS) without a timeout leaves an
 * orphan lock and a stuck Lambda until the function timeout. requestTimeout caps a
 * single attempt; maxAttempts:2 gives one bounded retry (the SDK's own backoff),
 * not the unbounded default.
 *
 * Values are env-overridable for ops tuning; defaults are deliberately tight
 * (well under the Lambda timeout) so a hang fails fast into the compensating path.
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
