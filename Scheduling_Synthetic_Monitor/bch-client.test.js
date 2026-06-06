'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const lambdaMock = mockClient(LambdaClient);
const { invokeBch } = require('./bch-client');

const enc = (obj) => new TextEncoder().encode(JSON.stringify(obj));

beforeEach(() => lambdaMock.reset());

describe('bch-client.invokeBch', () => {
  test('invokes RequestResponse and parses the JSON payload', async () => {
    lambdaMock.on(InvokeCommand).resolves({ Payload: enc({ status: 'BOOKED', bookingId: 'b1' }) });
    const res = await invokeBch({ action: 'scheduling_propose' });
    expect(res).toEqual({ status: 'BOOKED', bookingId: 'b1' });
    expect(lambdaMock).toHaveReceivedCommandWith(InvokeCommand, { InvocationType: 'RequestResponse' });
  });

  test('throws on a Lambda FunctionError', async () => {
    lambdaMock.on(InvokeCommand).resolves({ FunctionError: 'Unhandled', Payload: enc({ errorMessage: 'boom' }) });
    await expect(invokeBch({})).rejects.toThrow(/BCH FunctionError \(Unhandled\)/);
  });

  test('throws on a non-JSON payload', async () => {
    lambdaMock.on(InvokeCommand).resolves({ Payload: new TextEncoder().encode('not json') });
    await expect(invokeBch({})).rejects.toThrow(/non-JSON/);
  });
});
