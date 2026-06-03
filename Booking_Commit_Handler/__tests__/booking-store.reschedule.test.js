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
  it('UpdateItem on (tenantId, booking_id) with start_at + event id + sync flag, guarded on existence', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await bookingStore.updateBookingReschedule('T1', 'booking#abc', {
      startAt: '2026-07-01T15:00:00Z',
      externalEventId: 'evt-new',
      pendingCalendarSync: true,
    });
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.Key).toEqual({ tenantId: { S: 'T1' }, booking_id: { S: 'booking#abc' } });
    expect(input.ConditionExpression).toBe('attribute_exists(booking_id)'); // never create
    expect(input.UpdateExpression).toContain('start_at = :sa');
    expect(input.UpdateExpression).toContain('external_event_id = :eid');
    expect(input.UpdateExpression).toContain('pending_calendar_sync = :pcs');
    expect(input.ExpressionAttributeValues[':sa']).toEqual({ S: '2026-07-01T15:00:00Z' });
    expect(input.ExpressionAttributeValues[':eid']).toEqual({ S: 'evt-new' });
    expect(input.ExpressionAttributeValues[':pcs']).toEqual({ BOOL: true });
  });

  it('omits unset optional fields (only stamps last_calendar_mutation_at)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await bookingStore.updateBookingReschedule('T1', 'bk1', {});
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.UpdateExpression).toContain('last_calendar_mutation_at = :now');
    expect(input.UpdateExpression).not.toContain('start_at');
    expect(input.UpdateExpression).not.toContain('external_event_id');
  });

  it('throws on missing keys (never a keyless UpdateItem)', async () => {
    await expect(bookingStore.updateBookingReschedule('', 'bk1', {})).rejects.toThrow();
    await expect(bookingStore.updateBookingReschedule('T1', '', {})).rejects.toThrow();
  });
});
