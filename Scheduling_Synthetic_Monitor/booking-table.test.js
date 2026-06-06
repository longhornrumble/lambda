'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);
const bookingTable = require('./booking-table');

beforeEach(() => ddbMock.reset());

describe('booking-table.getBooking', () => {
  test('unmarshals a row to a plain object (camel + snake tenant; nullable fields)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId: { S: 'TEN-SYNTH' },
        booking_id: { S: 'booking#1' },
        status: { S: 'booked' },
        coordinator_email: { S: 'c@x.org' },
        external_event_id: { S: 'evt1' },
        created_at: { S: '2026-07-01T00:00:00Z' },
        item_type: { S: 'booking' },
        is_synthetic: { BOOL: true },
      },
    });
    const row = await bookingTable.getBooking('TEN-SYNTH', 'booking#1');
    expect(row).toMatchObject({
      tenantId: 'TEN-SYNTH',
      tenant_id: 'TEN-SYNTH',
      booking_id: 'booking#1',
      status: 'booked',
      coordinator_email: 'c@x.org',
      external_event_id: 'evt1',
      is_synthetic: true,
    });
    expect(row.timezone).toBeNull(); // absent field tolerated
  });

  test('returns null when the row is absent', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    expect(await bookingTable.getBooking('T', 'b')).toBeNull();
  });
});

describe('booking-table.stampSynthetic', () => {
  test('sets is_synthetic=true guarded on existence', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await bookingTable.stampSynthetic('TEN-SYNTH', 'booking#1');
    expect(ddbMock).toHaveReceivedCommandWith(UpdateItemCommand, {
      UpdateExpression: 'SET is_synthetic = :t',
      ExpressionAttributeValues: { ':t': { BOOL: true } },
      ConditionExpression: 'attribute_exists(booking_id)',
    });
  });
});

describe('booking-table.querySyntheticOlderThan', () => {
  test('filters to synthetic booking rows older than cutoff and paginates', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({
        Items: [{ tenantId: { S: 'TEN-SYNTH' }, booking_id: { S: 'b1' }, created_at: { S: '2026-06-01T00:00:00Z' } }],
        LastEvaluatedKey: { tenantId: { S: 'TEN-SYNTH' }, booking_id: { S: 'b1' } },
      })
      .resolvesOnce({
        Items: [{ tenantId: { S: 'TEN-SYNTH' }, booking_id: { S: 'b2' }, created_at: { S: '2026-06-02T00:00:00Z' } }],
      });

    const rows = await bookingTable.querySyntheticOlderThan('TEN-SYNTH', '2026-07-01T00:00:00Z');
    expect(rows).toEqual([
      { tenantId: 'TEN-SYNTH', booking_id: 'b1', created_at: '2026-06-01T00:00:00Z', status: null },
      { tenantId: 'TEN-SYNTH', booking_id: 'b2', created_at: '2026-06-02T00:00:00Z', status: null },
    ]);
    const call = ddbMock.commandCalls(QueryCommand)[0].args[0].input;
    expect(call.KeyConditionExpression).toBe('tenantId = :t');
    expect(call.FilterExpression).toContain('is_synthetic = :true');
    expect(call.FilterExpression).toContain("item_type = :booking");
    expect(call.FilterExpression).toContain('created_at < :cut');
    expect(call.ExpressionAttributeValues[':cut']).toEqual({ S: '2026-07-01T00:00:00Z' });
  });

  test('empty partition → empty list', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    expect(await bookingTable.querySyntheticOlderThan('TEN-SYNTH', '2026-07-01T00:00:00Z')).toEqual([]);
  });
});

describe('booking-table.deleteBooking', () => {
  test('deletes by composite key, conditional on is_synthetic (defense-in-depth)', async () => {
    ddbMock.on(DeleteItemCommand).resolves({});
    await bookingTable.deleteBooking('TEN-SYNTH', 'booking#1');
    expect(ddbMock).toHaveReceivedCommandWith(DeleteItemCommand, {
      Key: { tenantId: { S: 'TEN-SYNTH' }, booking_id: { S: 'booking#1' } },
      ConditionExpression: 'is_synthetic = :true',
      ExpressionAttributeValues: { ':true': { BOOL: true } },
    });
  });
});

describe('booking-table.getBooking — key fallbacks', () => {
  test('falls back to the param tenantId/bookingId when the Item omits them', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { status: { S: 'booked' } } });
    const row = await bookingTable.getBooking('TEN-FALLBACK', 'booking#fb');
    expect(row.tenantId).toBe('TEN-FALLBACK');
    expect(row.tenant_id).toBe('TEN-FALLBACK');
    expect(row.booking_id).toBe('booking#fb');
    expect(row.is_synthetic).toBe(false); // absent attr → forward-compatible default
  });
});
