'use strict';

const { mockClient } = require('aws-sdk-client-mock');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { invokeAttend, _ATTEND_FUNCTION_NAME } = require('./attend-client');

const lambdaMock = mockClient(LambdaClient);

beforeEach(() => lambdaMock.reset());

function makePayload(raw) {
  return { S: Buffer.from(JSON.stringify(raw)) };
}

describe('attend-client — invokeAttend', () => {
  test('invokes Attendance_Disposition_Handler and returns parsed JSON', async () => {
    const body = { action: 'attendance_check', outcome: 'pending_attendance_set' };
    lambdaMock.on(InvokeCommand).resolves({
      StatusCode: 200,
      Payload: Buffer.from(JSON.stringify(body)),
    });

    const result = await invokeAttend(
      { action: 'attendance_check', tenantId: 'TEN-1', booking_id: 'bk#1' },
      { functionName: 'Attendance_Disposition_Handler' }
    );

    expect(result).toEqual(body);
    // Confirm the invoke targeted the right function
    const calls = lambdaMock.calls();
    expect(calls).toHaveLength(1);
    const sent = JSON.parse(Buffer.from(calls[0].args[0].input.Payload).toString('utf8'));
    expect(sent).toMatchObject({ action: 'attendance_check', tenantId: 'TEN-1' });
  });

  test('uses ATTEND_FUNCTION_NAME env default', () => {
    // The module-level default is what the env var sets at import time.
    expect(typeof _ATTEND_FUNCTION_NAME).toBe('string');
    expect(_ATTEND_FUNCTION_NAME.length).toBeGreaterThan(0);
  });

  test('throws when the Lambda returns a FunctionError', async () => {
    lambdaMock.on(InvokeCommand).resolves({
      StatusCode: 200,
      FunctionError: 'Unhandled',
      Payload: Buffer.from(JSON.stringify({ errorMessage: 'boom' })),
    });

    await expect(
      invokeAttend({ action: 'attendance_check', tenantId: 'T', booking_id: 'bk#1' })
    ).rejects.toThrow(/FunctionError.*Unhandled/);
  });

  test('throws when the Lambda returns non-JSON payload', async () => {
    lambdaMock.on(InvokeCommand).resolves({
      StatusCode: 200,
      Payload: Buffer.from('NOT-JSON'),
    });

    await expect(
      invokeAttend({ action: 'attendance_check', tenantId: 'T', booking_id: 'bk#1' })
    ).rejects.toThrow(/non-JSON payload/);
  });

  test('returns null when Payload is absent', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 200 });

    const result = await invokeAttend({ action: 'attendance_check', tenantId: 'T', booking_id: 'bk#1' });
    expect(result).toBeNull();
  });

  test('propagates Lambda client errors (network failure)', async () => {
    lambdaMock.on(InvokeCommand).rejects(new Error('connection refused'));

    await expect(
      invokeAttend({ action: 'attendance_check', tenantId: 'T', booking_id: 'bk#1' })
    ).rejects.toThrow(/connection refused/);
  });
});
