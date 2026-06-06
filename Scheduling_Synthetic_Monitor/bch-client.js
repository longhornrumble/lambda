'use strict';

/**
 * bch-client.js — invoke Booking_Commit_Handler (C8) via Lambda InvokeCommand.
 *
 * The synthetic monitor drives the REAL booking path at the commit boundary: it invokes
 * BCH's `scheduling_propose` (availability), default commit (the full transaction), and
 * `scheduling_mutate` cancel — exactly as BSH does in production. Lambda-to-Lambda,
 * IAM-enforced, same account. The monitor never bundles googleapis or holds OAuth; BCH
 * owns all calendar/conference I/O.
 *
 * A BCH FunctionError (an actual Lambda crash, not a graceful { outcome:'failed' }) is
 * surfaced as a thrown Error so the calling cycle records a failure + alerts.
 */

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { sdkConfig } = require('./aws-clients');

const lambda = new LambdaClient(sdkConfig());
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
