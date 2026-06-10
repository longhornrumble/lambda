'use strict';

/**
 * Unit tests for booking-store.js — Booking-table conditional writes for the §14.2
 * calendar-lifecycle reconciliation. DynamoDB is mocked with aws-sdk-client-mock.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, UpdateItemCommand, GetItemCommand } = require('@aws-sdk/client-dynamodb');

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

describe('getNoticeContext — attendee/appt read for the (Y) cancel notice (gap C)', () => {
  it('projects + unmarshals the notice fields the deleted envelope lacks', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        attendee_email: { S: 'vol@example.org' },
        attendee_name: { S: 'Vol' },
        appointment_type_id: { S: 'apt-1' },
        start_at: { S: '2026-06-10T15:00:00Z' },
      },
    });
    const ctx = await store.getNoticeContext({ tenantId: 'AUS123957', bookingId: 'b1' });
    expect(ctx).toEqual({ attendeeEmail: 'vol@example.org', attendeeName: 'Vol', appointmentTypeId: 'apt-1', startAt: '2026-06-10T15:00:00Z' });
    expect(ddbMock.commandCalls(GetItemCommand)[0].args[0].input.ProjectionExpression).toContain('attendee_email');
  });

  it('returns null when the booking row is absent', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    expect(await store.getNoticeContext({ tenantId: 'AUS123957', bookingId: 'gone' })).toBeNull();
  });

  it('tolerates an old-shape row missing optional fields (forward-compat)', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { attendee_email: { S: 'v@x' } } });
    expect(await store.getNoticeContext({ tenantId: 'AUS123957', bookingId: 'b2' }))
      .toEqual({ attendeeEmail: 'v@x', attendeeName: null, appointmentTypeId: null, startAt: null });
  });

  it('throws on missing identifiers', async () => {
    await expect(store.getNoticeContext({ tenantId: 'AUS123957' })).rejects.toThrow('requires tenantId, bookingId');
  });
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
    // G6: if_not_exists preserves an admin cancel-with-reason written just before this fires.
    expect(call.UpdateExpression).toContain('cancel_reason = if_not_exists(cancel_reason, :r)');
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

  it('returns true and cancels with coordinator_moved, WITHOUT writing rescheduleOfBookingId (F2)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await store.cancelOnCoordinatorMove(base);

    expect(result).toBe(true);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.ConditionExpression).toBe('attribute_exists(booking_id) AND #st = :booked');
    expect(call.ExpressionAttributeValues[':canceled']).toEqual({ S: 'canceled' });
    expect(call.ExpressionAttributeValues[':r']).toEqual({ S: 'coordinator_moved' });
    // F2: no self-anchor — the attribute means NEW→original; self-anchoring inverts it.
    expect(call.UpdateExpression).not.toContain('rescheduleOfBookingId');
    expect(call.ExpressionAttributeValues[':self']).toBeUndefined();
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

  it('rejects a non-email newResourceId (F6) — malformed, never written to the GSI field', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await expect(store.reassignCoordinator({ ...base, newResourceId: 'not-an-email' }))
      .rejects.toMatchObject({ malformed: true });
    // never reached the write — the bad value cannot pollute tenantId-coordinator_email-index.
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 0);
  });

  it('accepts a well-formed email newResourceId (F6 happy path)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await expect(store.reassignCoordinator({ ...base, newResourceId: 'a.b+x@sub.org.example' }))
      .resolves.toBe(true);
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
