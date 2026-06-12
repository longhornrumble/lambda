'use strict';

/**
 * attend-client.js — invoke Attendance_Disposition_Handler via Lambda InvokeCommand.
 *
 * Mirrors bch-client.js: a thin Lambda-to-Lambda shim used by the disposition cycle to
 * fire the attendance_check action against the SHIPPED WS-E-ATTEND handler. The handler
 * sets the non-key attendance_state='pending_attendance' (idempotent conditional write),
 * mints the three §B4 tokens, and dispatches the interviewer prompt. The synthetic context
 * suppresses actual email/SMS delivery for the synthetic booking (the ATTEND handler sends
 * to the synthetic attendee alias — scheduling-monitor@myrecruiter.ai — which is an
 * expected side effect of the burn-in cycle).
 *
 * A FunctionError (Lambda crash) is surfaced as a thrown Error so the calling cycle records
 * a failure + alerts. A graceful handler return with outcome:'booking_not_found' or
 * outcome:'bad_event' is treated as a failure by the disposition cycle.
 *
 * Env:
 *   ATTEND_FUNCTION_NAME — defaults to 'Attendance_Disposition_Handler'
 *   (no per-invoke timeout needed: the attendance check does a DDB write + email invoke,
 *    well within the default Lambda client timeout)
 */

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { sdkConfig } = require('./aws-clients');

const lambda = new LambdaClient(sdkConfig());
const ATTEND_FUNCTION_NAME =
  process.env.ATTEND_FUNCTION_NAME || 'Attendance_Disposition_Handler';

async function invokeAttend(
  payload,
  { client = lambda, functionName = ATTEND_FUNCTION_NAME } = {}
) {
  const res = await client.send(
    new InvokeCommand({
      FunctionName: functionName,
      InvocationType: 'RequestResponse',
      Payload: Buffer.from(JSON.stringify(payload)),
    })
  );
  const raw = res.Payload ? Buffer.from(res.Payload).toString('utf8') : 'null';
  if (res.FunctionError) {
    throw new Error(
      `Attendance_Disposition_Handler FunctionError (${res.FunctionError}): ${raw.slice(0, 300)}`
    );
  }
  try {
    return JSON.parse(raw);
  } catch (err) {
    throw new Error(
      `Attendance_Disposition_Handler returned non-JSON payload: ${raw.slice(0, 120)}`
    );
  }
}

module.exports = { invokeAttend, _ATTEND_FUNCTION_NAME: ATTEND_FUNCTION_NAME };
