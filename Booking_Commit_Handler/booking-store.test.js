'use strict';

/**
 * Unit tests for booking-store.js — Booking-table reads/writes for the C8 commit.
 * DynamoDB is mocked with aws-sdk-client-mock (Calendar_Watch_* convention).
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');

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

describe('buildBookingId — deterministic idempotency key (AC #6)', () => {
  it('is stable for the same (tenant, session, start)', () => {
    const a = store.buildBookingId('AUS123957', 'sess-1', '2026-06-03T18:00:00Z');
    const b = store.buildBookingId('AUS123957', 'sess-1', '2026-06-03T18:00:00Z');
    expect(a).toBe(b);
    expect(a.startsWith('booking#')).toBe(true);
  });
  it('differs across session or slot', () => {
    const a = store.buildBookingId('AUS123957', 'sess-1', '2026-06-03T18:00:00Z');
    const b = store.buildBookingId('AUS123957', 'sess-2', '2026-06-03T18:00:00Z');
    const c = store.buildBookingId('AUS123957', 'sess-1', '2026-06-03T19:00:00Z');
    expect(a).not.toBe(b);
    expect(a).not.toBe(c);
  });
  it('throws on missing inputs', () => {
    expect(() => store.buildBookingId('', 's', 'x')).toThrow();
  });
});

describe('buildBookingItem — GSI keys + status discipline', () => {
  const fields = {
    tenantId: 'AUS123957', bookingId: 'booking#x', sessionId: 'sess-1',
    status: 'booked', start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z',
    coordinatorEmail: 'maya@org.org', resourceId: 'maya@org.org',
    attendeeEmail: 'sam@example.com', externalEventId: 'evt-1',
    conferenceProvider: 'zoom', createdAt: '2026-06-01T00:00:00Z',
  };

  it('writes the GSI key attributes start_at + coordinator_email', () => {
    const item = store.buildBookingItem(fields);
    expect(item.start_at.S).toBe('2026-06-03T18:00:00Z'); // tenantId-start_at-index
    expect(item.coordinator_email.S).toBe('maya@org.org'); // tenantId-coordinator_email-index
    expect(item.status.S).toBe('booked');
    expect(item.item_type.S).toBe('booking');
  });

  it('refuses an illegal Booking.status (booking-status SoT guard)', () => {
    expect(() => store.buildBookingItem({ ...fields, status: 'cancelled' })).toThrow(/illegal Booking.status/);
    expect(() => store.buildBookingItem({ ...fields, status: 'pending' })).toThrow();
  });

  it('only sets optional PII fields when present (schema discipline)', () => {
    const item = store.buildBookingItem(fields);
    expect(item.attendee_phone).toBeUndefined();
    const withPhone = store.buildBookingItem({ ...fields, attendeePhone: '+15125551234', attendeeName: 'Sam Patel' });
    expect(withPhone.attendee_phone.S).toBe('+15125551234');
    expect(withPhone.attendee_name.S).toBe('Sam Patel');
  });
});

describe('getBookingById — idempotency gate', () => {
  it('returns the item when present', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { booking_id: { S: 'booking#x' }, status: { S: 'booked' } } });
    const item = await store.getBookingById('AUS123957', 'booking#x');
    expect(item.status.S).toBe('booked');
  });
  it('returns null when absent', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    expect(await store.getBookingById('AUS123957', 'booking#x')).toBeNull();
  });
});

describe('writeBooking — conditional write', () => {
  it('puts with attribute_not_exists(booking_id)', async () => {
    ddbMock.on(PutItemCommand).resolves({});
    await store.writeBooking({
      tenantId: 'AUS123957', bookingId: 'booking#x', sessionId: 's', status: 'booked',
      start: 'a', end: 'b', coordinatorEmail: 'm', resourceId: 'm',
      attendeeEmail: 'sam@x', externalEventId: 'e', conferenceProvider: 'null', createdAt: 'c',
    });
    const call = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(call.ConditionExpression).toBe('attribute_not_exists(booking_id)');
  });

  it('propagates ConditionalCheckFailed for the caller to handle', async () => {
    ddbMock.on(PutItemCommand).rejects(conditionalFail());
    await expect(store.writeBooking({
      tenantId: 'AUS123957', bookingId: 'booking#x', sessionId: 's', status: 'booked',
      start: 'a', end: 'b', coordinatorEmail: 'm', resourceId: 'm',
      attendeeEmail: 'sam@x', externalEventId: 'e', conferenceProvider: 'null', createdAt: 'c',
    })).rejects.toMatchObject({ name: 'ConditionalCheckFailedException' });
  });
});

describe('slot-lock lifecycle (C6→C8 deferral)', () => {
  it('releaseLock deletes the lock item (idempotent)', async () => {
    ddbMock.on(DeleteItemCommand).resolves({});
    await store.releaseLock('AUS123957', 'slot_lock#one_to_one#m#a#b');
    expect(ddbMock).toHaveReceivedCommandTimes(DeleteItemCommand, 1);
  });

  it('recordConferenceOnLock persists the conference id (Zoom read-before-write)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await store.recordConferenceOnLock('AUS123957', 'slot_lock#x', { conferenceId: '99', provider: 'zoom' });
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.ExpressionAttributeValues[':c'].S).toBe('99');
  });

  it('recordConferenceOnLock is a no-op without a conference id', async () => {
    await store.recordConferenceOnLock('AUS123957', 'slot_lock#x', { conferenceId: null });
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 0);
  });

  it('readLock returns the lock item to recover a prior conference id', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { conference_id: { S: '77' } } });
    const lock = await store.readLock('AUS123957', 'slot_lock#x');
    expect(lock.conference_id.S).toBe('77');
  });

  it('flagLockForReconciliation marks the orphan lock for the ops sweep', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await store.flagLockForReconciliation('AUS123957', 'slot_lock#x', 'release_failed');
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.ExpressionAttributeValues[':t'].BOOL).toBe(true);
  });
});

describe('writeDegradedMarker — §5.5 row 4 durable record', () => {
  it('writes a discriminated coordinator_degraded item (not under GSI keys)', async () => {
    ddbMock.on(PutItemCommand).resolves({});
    await store.writeDegradedMarker('AUS123957', 'maya@org.org', 'oauth_revoked');
    const item = ddbMock.commandCalls(PutItemCommand)[0].args[0].input.Item;
    expect(item.item_type.S).toBe('coordinator_degraded');
    expect(item.booking_id.S).toBe('coordinator_degraded#maya@org.org');
    expect(item.start_at).toBeUndefined(); // invisible to the booking GSIs
    expect(item.coordinator_email).toBeUndefined();
  });
});
