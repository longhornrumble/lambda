'use strict';

/**
 * Unit tests for booking-updates.js — Booking-table conditional writes for B9 + B10.
 * DynamoDB is mocked with aws-sdk-client-mock (Calendar_Watch_* / C8 convention).
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const {
  DynamoDBClient,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);

const updates = require('./booking-updates');

function conditionalFail() {
  const e = new Error('The conditional request failed');
  e.name = 'ConditionalCheckFailedException';
  return e;
}

beforeEach(() => {
  ddbMock.reset();
});

describe('flagOooConflict — B9 OOO conflict flag (idempotent)', () => {
  const base = {
    tenantId: 'AUS123957',
    bookingId: 'booking#abc',
    mutationAt: '2026-06-03T18:00:00.000Z',
    now: '2026-06-01T00:00:00.000Z',
  };

  it('returns true and issues the guarded conditional UpdateItem on a fresh flag', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await updates.flagOooConflict(base);

    expect(result).toBe(true);
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);

    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.TableName).toBe(updates._BOOKING_TABLE);
    expect(call.Key).toEqual({ tenantId: { S: 'AUS123957' }, booking_id: { S: 'booking#abc' } });
    // Guards: row must exist, be booked, and not already flagged for THIS mutation.
    expect(call.ConditionExpression).toContain('attribute_exists(booking_id)');
    expect(call.ConditionExpression).toContain('#st = :booked');
    expect(call.ConditionExpression).toContain('ooo_conflict_mutation_at <> :m');
    expect(call.ExpressionAttributeNames).toEqual({ '#st': 'status' });
    expect(call.ExpressionAttributeValues[':booked']).toEqual({ S: 'booked' });
    expect(call.ExpressionAttributeValues[':m']).toEqual({ S: base.mutationAt });
    expect(call.ExpressionAttributeValues[':at']).toEqual({ S: base.now });
    expect(call.UpdateExpression).toContain('ooo_conflict_status = :flagged');
    expect(call.UpdateExpression).toContain('ooo_conflict_mutation_at = :m');
  });

  it('includes optional ooo window bounds when provided', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await updates.flagOooConflict({ ...base, oooStartAt: '2026-06-03T17:00:00Z', oooEndAt: '2026-06-03T19:00:00Z' });

    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.UpdateExpression).toContain('ooo_conflict_start_at = :os');
    expect(call.UpdateExpression).toContain('ooo_conflict_end_at = :oe');
    expect(call.ExpressionAttributeValues[':os']).toEqual({ S: '2026-06-03T17:00:00Z' });
    expect(call.ExpressionAttributeValues[':oe']).toEqual({ S: '2026-06-03T19:00:00Z' });
  });

  it('omits the window clauses when bounds are absent', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await updates.flagOooConflict(base);

    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.UpdateExpression).not.toContain('ooo_conflict_start_at');
    expect(call.UpdateExpression).not.toContain('ooo_conflict_end_at');
    expect(call.ExpressionAttributeValues[':os']).toBeUndefined();
  });

  it('returns false on ConditionalCheckFailed (absent / not-booked / already-flagged)', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await expect(updates.flagOooConflict(base)).resolves.toBe(false);
  });

  it('propagates a non-conditional DDB error so SQS redrives', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('ProvisionedThroughputExceededException'));
    await expect(updates.flagOooConflict(base)).rejects.toThrow('ProvisionedThroughputExceededException');
  });

  it('throws on missing required args', async () => {
    await expect(updates.flagOooConflict({ bookingId: 'b', mutationAt: 'm' })).rejects.toThrow(/requires/);
    await expect(updates.flagOooConflict({ tenantId: 't', mutationAt: 'm' })).rejects.toThrow(/requires/);
    await expect(updates.flagOooConflict({ tenantId: 't', bookingId: 'b' })).rejects.toThrow(/requires/);
  });
});

describe('cancelOnDecline — B10 booked→canceled (idempotent)', () => {
  const base = { tenantId: 'AUS123957', bookingId: 'booking#abc', now: '2026-06-01T00:00:00.000Z' };

  it('returns true and issues a status==booked-guarded UpdateItem', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await updates.cancelOnDecline(base);

    expect(result).toBe(true);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.Key).toEqual({ tenantId: { S: 'AUS123957' }, booking_id: { S: 'booking#abc' } });
    expect(call.ConditionExpression).toBe('attribute_exists(booking_id) AND #st = :booked');
    expect(call.ExpressionAttributeNames).toEqual({ '#st': 'status' });
    expect(call.ExpressionAttributeValues[':canceled']).toEqual({ S: 'canceled' });
    expect(call.ExpressionAttributeValues[':booked']).toEqual({ S: 'booked' });
    expect(call.ExpressionAttributeValues[':r']).toEqual({ S: 'attendee_declined' });
    expect(call.ExpressionAttributeValues[':at']).toEqual({ S: base.now });
  });

  it('returns false on ConditionalCheckFailed (already canceled / terminal / absent)', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await expect(updates.cancelOnDecline(base)).resolves.toBe(false);
  });

  it('propagates a non-conditional DDB error so SQS redrives', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('InternalServerError'));
    await expect(updates.cancelOnDecline(base)).rejects.toThrow('InternalServerError');
  });

  it('throws on missing required args', async () => {
    await expect(updates.cancelOnDecline({ bookingId: 'b' })).rejects.toThrow(/requires/);
    await expect(updates.cancelOnDecline({ tenantId: 't' })).rejects.toThrow(/requires/);
  });
});

describe('isConditionalCheckFailed', () => {
  it('recognizes the DDB conditional failure by name', () => {
    expect(updates.isConditionalCheckFailed(conditionalFail())).toBe(true);
    expect(updates.isConditionalCheckFailed(new Error('other'))).toBe(false);
    expect(updates.isConditionalCheckFailed(null)).toBe(false);
  });
});

describe('canonical status binding', () => {
  it('uses only canonical Booking.status literals (CI-3c SoT)', () => {
    const { BOOKING_STATUSES } = require('../shared/booking-status');
    expect(BOOKING_STATUSES).toContain('booked');
    expect(BOOKING_STATUSES).toContain('canceled');
  });
});
