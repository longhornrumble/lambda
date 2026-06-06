'use strict';

/**
 * aws-clients.js — bounded AWS SDK v3 client config for the synthetic monitor.
 *
 * Mirrors Booking_Commit_Handler/aws-client-config.js: every SDK call bounds its request
 * (connection + request timeout) and caps retries, so an upstream hang (DynamoDB / SNS /
 * CloudWatch / Lambda invoke) fails fast instead of pinning the monitor until the Lambda
 * timeout. Defaults are env-overridable for ops tuning.
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

module.exports = { sdkConfig };
