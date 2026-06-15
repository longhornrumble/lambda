'use strict';

// Direct DDB-marshalling test for the Tier-2 updateBookingReschedule (option A persist).
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);
let bookingStore;
beforeAll(() => { bookingStore = require('../booking-store'); });
beforeEach(() => ddbMock.reset());

describe('updateBookingReschedule', () => {
  it('UpdateItem with start_at + end_at + event id + sync flag + ics_sequence bump, guarded on existence', async () => {
    ddbMock.on(UpdateItemCommand).resolves({ Attributes: { ics_sequence: { N: '1' } } });
    const out = await bookingStore.updateBookingReschedule('T1', 'booking#abc', {
      startAt: '2026-07-01T15:00:00Z',
      endAt: '2026-07-01T15:30:00Z',
      externalEventId: 'evt-new',
      pendingCalendarSync: true,
    });
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.Key).toEqual({ tenantId: { S: 'T1' }, booking_id: { S: 'booking#abc' } });
    expect(input.ConditionExpression).toBe('attribute_exists(booking_id)'); // never create
    expect(input.UpdateExpression).toContain('start_at = :sa');
    expect(input.UpdateExpression).toContain('end_at = :ea'); // end moves with start (no stale end)
    expect(input.UpdateExpression).toContain('external_event_id = :eid');
    expect(input.UpdateExpression).toContain('pending_calendar_sync = :pcs');
    expect(input.UpdateExpression).toContain('ADD ics_sequence :one'); // RFC5545 revision bump
    expect(input.ReturnValues).toBe('UPDATED_NEW');
    expect(input.ExpressionAttributeValues[':sa']).toEqual({ S: '2026-07-01T15:00:00Z' });
    expect(input.ExpressionAttributeValues[':ea']).toEqual({ S: '2026-07-01T15:30:00Z' });
    expect(input.ExpressionAttributeValues[':eid']).toEqual({ S: 'evt-new' });
    expect(input.ExpressionAttributeValues[':pcs']).toEqual({ BOOL: true });
    expect(input.ExpressionAttributeValues[':one']).toEqual({ N: '1' });
    expect(out.icsSequence).toBe(1); // post-increment value returned for the confirmation .ics
  });

  it('omits unset optional fields (still stamps last_calendar_mutation_at + bumps the counter)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({}); // no Attributes back
    const out = await bookingStore.updateBookingReschedule('T1', 'bk1', {});
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.UpdateExpression).toContain('last_calendar_mutation_at = :now');
    expect(input.UpdateExpression).toContain('ADD ics_sequence :one'); // counter always bumps
    expect(input.UpdateExpression).not.toContain('start_at');
    expect(input.UpdateExpression).not.toContain('end_at');
    expect(input.UpdateExpression).not.toContain('external_event_id');
    expect(out.icsSequence).toBeUndefined(); // no Attributes returned → undefined (handled gracefully)
  });

  it('throws on missing keys (never a keyless UpdateItem)', async () => {
    await expect(bookingStore.updateBookingReschedule('', 'bk1', {})).rejects.toThrow();
    await expect(bookingStore.updateBookingReschedule('T1', '', {})).rejects.toThrow();
  });
});

describe('updateBookingCancelReason (G6 cancel-with-reason)', () => {
  it('writes cancel_reason + canceled_by on (tenantId, booking_id), guarded on existence', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await bookingStore.updateBookingCancelReason('T1', 'booking#abc', {
      reason: 'Volunteer requested',
      canceledBy: 'admin@org.com',
    });
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.Key).toEqual({ tenantId: { S: 'T1' }, booking_id: { S: 'booking#abc' } });
    expect(input.ConditionExpression).toBe('attribute_exists(booking_id)'); // never create
    expect(input.UpdateExpression).toContain('cancel_reason = :r');
    expect(input.UpdateExpression).toContain('canceled_by = :by');
    expect(input.ExpressionAttributeValues[':r']).toEqual({ S: 'Volunteer requested' });
    expect(input.ExpressionAttributeValues[':by']).toEqual({ S: 'admin@org.com' });
  });

  it('omits canceled_by when not supplied (reason-only write)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await bookingStore.updateBookingCancelReason('T1', 'bk1', { reason: 'x' });
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.UpdateExpression).toContain('cancel_reason = :r');
    expect(input.UpdateExpression).not.toContain('canceled_by');
  });

  it('throws on missing keys (never a keyless UpdateItem)', async () => {
    await expect(bookingStore.updateBookingCancelReason('', 'bk1', { reason: 'x' })).rejects.toThrow();
    await expect(bookingStore.updateBookingCancelReason('T1', '', { reason: 'x' })).rejects.toThrow();
  });

  it('throws on a non-string / empty reason (never marshals "undefined"/"null" into DDB)', async () => {
    await expect(bookingStore.updateBookingCancelReason('T1', 'bk1', {})).rejects.toThrow();
    await expect(bookingStore.updateBookingCancelReason('T1', 'bk1', { reason: undefined })).rejects.toThrow();
    await expect(bookingStore.updateBookingCancelReason('T1', 'bk1', { reason: '' })).rejects.toThrow();
    await expect(bookingStore.updateBookingCancelReason('T1', 'bk1', { reason: 123 })).rejects.toThrow();
  });
});

describe('touchRescheduleLinkSentAt (G6 reschedule-link rate limit)', () => {
  it('claims the slot: conditional UpdateItem (exists AND no-prior-or-stale) and returns true', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const ok = await bookingStore.touchRescheduleLinkSentAt('T1', 'booking#abc', 60, Date.UTC(2026, 0, 1, 0, 0, 0));
    expect(ok).toBe(true);
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.Key).toEqual({ tenantId: { S: 'T1' }, booking_id: { S: 'booking#abc' } });
    expect(input.UpdateExpression).toBe('SET reschedule_link_sent_at = :now');
    expect(input.ConditionExpression).toContain('attribute_exists(booking_id)');
    expect(input.ConditionExpression).toContain('attribute_not_exists(reschedule_link_sent_at) OR reschedule_link_sent_at < :cutoff');
    expect(input.ExpressionAttributeValues[':now']).toEqual({ S: '2026-01-01T00:00:00.000Z' });
    expect(input.ExpressionAttributeValues[':cutoff']).toEqual({ S: '2025-12-31T23:59:00.000Z' }); // now - 60s
  });

  it('returns false (not throw) when the cooldown condition fails', async () => {
    const err = new Error('conditional'); err.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(err);
    const ok = await bookingStore.touchRescheduleLinkSentAt('T1', 'bk1', 60);
    expect(ok).toBe(false);
  });

  it('rethrows a non-conditional DDB error', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('ProvisionedThroughputExceeded'));
    await expect(bookingStore.touchRescheduleLinkSentAt('T1', 'bk1', 60)).rejects.toThrow('ProvisionedThroughputExceeded');
  });

  it('throws on missing keys', async () => {
    await expect(bookingStore.touchRescheduleLinkSentAt('', 'bk1', 60)).rejects.toThrow();
    await expect(bookingStore.touchRescheduleLinkSentAt('T1', '', 60)).rejects.toThrow();
  });
});
