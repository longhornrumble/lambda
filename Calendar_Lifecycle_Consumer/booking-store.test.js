'use strict';

/**
 * Unit tests for booking-store.js — Booking-table conditional writes for the §14.2
 * calendar-lifecycle reconciliation. DynamoDB is mocked with aws-sdk-client-mock.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);

const store = require('./booking-store');

function conditionalFail() {
  const e = new Error('The conditional request failed');
  e.name = 'ConditionalCheckFailedException';
  return e;
}

beforeEach(() => {
  ddbMock.reset();
});

describe('cancelOnCoordinatorDelete — calendar_deleted booked→canceled (idempotent)', () => {
  const base = { tenantId: 'AUS123957', bookingId: 'booking#abc', now: '2026-06-01T00:00:00.000Z' };

  it('returns true and issues a status==booked-guarded UpdateItem with coordinator_deleted reason', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await store.cancelOnCoordinatorDelete(base);

    expect(result).toBe(true);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.TableName).toBe(store._BOOKING_TABLE);
    expect(call.Key).toEqual({ tenantId: { S: 'AUS123957' }, booking_id: { S: 'booking#abc' } });
    expect(call.ConditionExpression).toBe('attribute_exists(booking_id) AND #st = :booked');
    expect(call.ExpressionAttributeNames).toEqual({ '#st': 'status' });
    expect(call.ExpressionAttributeValues[':canceled']).toEqual({ S: 'canceled' });
    expect(call.ExpressionAttributeValues[':booked']).toEqual({ S: 'booked' });
    expect(call.ExpressionAttributeValues[':r']).toEqual({ S: 'coordinator_deleted' });
    expect(call.ExpressionAttributeValues[':at']).toEqual({ S: base.now });
    expect(call.UpdateExpression).not.toContain('rescheduleOfBookingId');
  });

  it('returns false on ConditionalCheckFailed (already canceled / terminal / absent)', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await expect(store.cancelOnCoordinatorDelete(base)).resolves.toBe(false);
  });

  it('propagates a non-conditional DDB error so SQS redrives', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('InternalServerError'));
    await expect(store.cancelOnCoordinatorDelete(base)).rejects.toThrow('InternalServerError');
  });

  it('throws on missing required args', async () => {
    await expect(store.cancelOnCoordinatorDelete({ bookingId: 'b' })).rejects.toThrow(/requires/);
    await expect(store.cancelOnCoordinatorDelete({ tenantId: 't' })).rejects.toThrow(/requires/);
  });
});

describe('cancelOnCoordinatorMove — calendar_moved cancel + self-anchor (idempotent)', () => {
  const base = { tenantId: 'AUS123957', bookingId: 'booking#abc', now: '2026-06-01T00:00:00.000Z' };

  it('returns true, cancels with coordinator_moved, and self-anchors rescheduleOfBookingId', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await store.cancelOnCoordinatorMove(base);

    expect(result).toBe(true);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.ConditionExpression).toBe('attribute_exists(booking_id) AND #st = :booked');
    expect(call.ExpressionAttributeValues[':canceled']).toEqual({ S: 'canceled' });
    expect(call.ExpressionAttributeValues[':r']).toEqual({ S: 'coordinator_moved' });
    // self-anchor: rescheduleOfBookingId == its own booking_id (v1 sentinel).
    expect(call.UpdateExpression).toContain('rescheduleOfBookingId = :self');
    expect(call.ExpressionAttributeValues[':self']).toEqual({ S: 'booking#abc' });
  });

  it('returns false on ConditionalCheckFailed', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await expect(store.cancelOnCoordinatorMove(base)).resolves.toBe(false);
  });

  it('propagates a non-conditional DDB error', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('Throttled'));
    await expect(store.cancelOnCoordinatorMove(base)).rejects.toThrow('Throttled');
  });

  it('throws on missing required args', async () => {
    await expect(store.cancelOnCoordinatorMove({ tenantId: 't' })).rejects.toThrow(/requires/);
  });
});

describe('reassignCoordinator — calendar_reassigned repoint organizer (idempotent)', () => {
  const base = {
    tenantId: 'AUS123957',
    bookingId: 'booking#abc',
    previousResourceId: 'old@org.example',
    newResourceId: 'new@org.example',
    now: '2026-06-01T00:00:00.000Z',
  };

  it('returns true and repoints resource_id + coordinator_email guarded on the previous organizer', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await store.reassignCoordinator(base);

    expect(result).toBe(true);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.ConditionExpression).toBe('attribute_exists(booking_id) AND resource_id = :prev');
    expect(call.UpdateExpression).toContain('resource_id = :new');
    expect(call.UpdateExpression).toContain('coordinator_email = :new');
    expect(call.UpdateExpression).toContain('reassigned_at = :at');
    expect(call.ExpressionAttributeValues[':new']).toEqual({ S: 'new@org.example' });
    expect(call.ExpressionAttributeValues[':prev']).toEqual({ S: 'old@org.example' });
    // No status change on reassignment.
    expect(call.UpdateExpression).not.toContain('#st');
  });

  it('returns false on ConditionalCheckFailed (already repointed / stale / absent)', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await expect(store.reassignCoordinator(base)).resolves.toBe(false);
  });

  it('propagates a non-conditional DDB error', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('InternalServerError'));
    await expect(store.reassignCoordinator(base)).rejects.toThrow('InternalServerError');
  });

  it('throws on any missing required arg', async () => {
    await expect(store.reassignCoordinator({ ...base, newResourceId: undefined })).rejects.toThrow(/requires/);
    await expect(store.reassignCoordinator({ ...base, previousResourceId: '' })).rejects.toThrow(/requires/);
  });
});

describe('isConditionalCheckFailed', () => {
  it('recognizes the DDB conditional failure by name', () => {
    expect(store.isConditionalCheckFailed(conditionalFail())).toBe(true);
    expect(store.isConditionalCheckFailed(new Error('other'))).toBe(false);
    expect(store.isConditionalCheckFailed(null)).toBe(false);
  });
});

describe('canonical status binding (CI-3c SoT)', () => {
  it('uses only canonical Booking.status literals', () => {
    const { BOOKING_STATUSES } = require('../shared/booking-status');
    expect(BOOKING_STATUSES).toContain('booked');
    expect(BOOKING_STATUSES).toContain('canceled');
  });
});
