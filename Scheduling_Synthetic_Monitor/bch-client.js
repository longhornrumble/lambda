'use strict';

/**
 * bch-client.js — invoke Booking_Commit_Handler (C8) via Lambda InvokeCommand.
 *
 * The synthetic monitor exercises the BCH commit/cancel boundary DIRECTLY: it invokes
 * BCH's `scheduling_propose` (availability), default commit (the full transaction), and
 * `scheduling_mutate` cancel via the same Lambda-to-Lambda interface BSH uses. NOTE: this
 * is NOT the full public path — the BSH conversation flow, the §B14 action boundary, and
 * the widget/session threading are bypassed (they are covered by §5.2 manual exercise and,
 * later, a BSH-level cycle). The monitor never bundles googleapis or holds OAuth; BCH owns
 * all calendar/conference I/O.
 *
 * A BCH FunctionError (an actual Lambda crash, not a graceful { outcome:'failed' }) is
 * surfaced as a thrown Error so the calling cycle records a failure + alerts.
 */

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { sdkConfig } = require('./aws-clients');

// BCH's commit does a live freeBusy re-check + Google Calendar insert + conference +
// confirmation email, so a single invoke can take well over the default 5s request
// timeout. Give the Lambda client a longer per-request budget (the cancel cycle's poll
// has its own bounded retry on top).
const BCH_INVOKE_TIMEOUT_MS = Number(process.env.BCH_INVOKE_TIMEOUT_MS || 30000);
const BCH_CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);

const lambda = new LambdaClient(
  sdkConfig({
    requestHandler: new NodeHttpHandler({
      connectionTimeout: BCH_CONNECTION_TIMEOUT_MS,
      requestTimeout: BCH_INVOKE_TIMEOUT_MS,
    }),
  })
);
const BCH_FUNCTION_NAME = process.env.BOOKING_COMMIT_FUNCTION_NAME || 'Booking_Commit_Handler';

async function invokeBch(payload, { client = lambda, functionName = BCH_FUNCTION_NAME } = {}) {
  const res = await client.send(
    new InvokeCommand({
      FunctionName: functionName,
      InvocationType: 'RequestResponse',
      Payload: Buffer.from(JSON.stringify(payload)),
    })
  );
  const raw = res.Payload ? Buffer.from(res.Payload).toString('utf8') : 'null';
  if (res.FunctionError) {
    throw new Error(`BCH FunctionError (${res.FunctionError}): ${raw.slice(0, 300)}`);
  }
  try {
    return JSON.parse(raw);
  } catch (err) {
    throw new Error(`BCH returned non-JSON payload: ${raw.slice(0, 120)}`);
  }
}

module.exports = { invokeBch, _BCH_FUNCTION_NAME: BCH_FUNCTION_NAME };
