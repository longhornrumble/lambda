'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  DynamoDBClient,
  QueryCommand,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');

process.env.BOOKING_TABLE = 'picasso-booking-staging';

const {
  findStrandedBookings,
  reassignBookingResource,
  parseBookingRow,
  isStranded,
  isConditionalCheckFailed,
  _COORDINATOR_INDEX,
} = require('./booking-store');

const ddbMock = mockClient(DynamoDBClient);

beforeEach(() => ddbMock.reset());

function bookingItem(over = {}) {
  const item = {
    tenantId: { S: over.tenantId ?? 'TEN1' },
    booking_id: { S: over.bookingId ?? 'booking#abc' },
    item_type: { S: over.itemType ?? 'booking' },
    status: { S: over.status ?? 'booked' },
    start_at: { S: over.startAt ?? '2026-06-01T15:00:00Z' },
    end_at: { S: over.endAt ?? '2026-06-01T15:30:00Z' },
    coordinator_email: { S: over.coordinatorEmail ?? 'maya@org.com' },
    resource_id: { S: over.resourceId ?? 'res-maya' },
    appointment_type_id: { S: over.appointmentTypeId ?? 'apt-1' },
    external_event_id: { S: over.externalEventId ?? 'evt-1' },
    timezone: { S: over.timezone ?? 'America/New_York' },
    attendee_email: { S: 'sam@x.com' },
    session_id: { S: 'sess-1' },
  };
  if (over.lastMut !== null) {
    item.last_calendar_mutation_at = { S: over.lastMut ?? '2026-05-01T00:00:00Z' };
  }
  return item;
}

const OFF = '2026-05-15T00:00:00Z';

describe('findStrandedBookings', () => {
  test('queries the coordinator GSI and keeps only bookings mutated before offboarding', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [
        bookingItem({ bookingId: 'booking#old', lastMut: '2026-05-01T00:00:00Z' }), // stranded
        bookingItem({ bookingId: 'booking#new', lastMut: '2026-05-20T00:00:00Z' }), // admin handled
        bookingItem({ bookingId: 'booking#nomut', lastMut: null }),                 // stranded (can't prove handled)
      ],
    });

    const stranded = await findStrandedBookings({
      tenantId: 'TEN1',
      coordinatorEmail: 'maya@org.com',
      offboardingTime: OFF,
    });

    expect(stranded.map((b) => b.bookingId).sort()).toEqual(['booking#nomut', 'booking#old']);

    const call = ddbMock.commandCalls(QueryCommand)[0].args[0].input;
    expect(call.IndexName).toBe(_COORDINATOR_INDEX);
    expect(call.KeyConditionExpression).toBe('tenantId = :t AND coordinator_email = :email');
    expect(call.FilterExpression).toContain('item_type = :bk');
    expect(call.ExpressionAttributeValues[':booked']).toEqual({ S: 'booked' });
  });

  test('paginates across LastEvaluatedKey', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({ Items: [bookingItem({ bookingId: 'booking#p1' })], LastEvaluatedKey: { x: { S: '1' } } })
      .resolvesOnce({ Items: [bookingItem({ bookingId: 'booking#p2' })] });

    const stranded = await findStrandedBookings({
      tenantId: 'TEN1',
      coordinatorEmail: 'maya@org.com',
      offboardingTime: OFF,
    });
    expect(stranded.map((b) => b.bookingId).sort()).toEqual(['booking#p1', 'booking#p2']);
    expect(ddbMock.commandCalls(QueryCommand)).toHaveLength(2);
  });

  test('empty result set returns []', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    const stranded = await findStrandedBookings({
      tenantId: 'TEN1', coordinatorEmail: 'maya@org.com', offboardingTime: OFF,
    });
    expect(stranded).toEqual([]);
  });

  test('rejects an unparseable offboarding_time', async () => {
    await expect(
      findStrandedBookings({ tenantId: 'TEN1', coordinatorEmail: 'maya@org.com', offboardingTime: 'not-a-date' })
    ).rejects.toThrow('offboarding_time must be a parseable date/time');
  });
});

describe('isStranded', () => {
  const base = { itemType: 'booking', status: 'booked', lastCalendarMutationAt: '2026-05-01T00:00:00Z' };
  const offMs = Date.parse(OFF);

  test('booking + booked + mutated-before-offboarding → stranded', () => {
    expect(isStranded({ ...base }, offMs)).toBe(true);
  });
  test('non-booking item_type → not stranded', () => {
    expect(isStranded({ ...base, itemType: 'slot_lock' }, offMs)).toBe(false);
  });
  test('non-booked status → not stranded', () => {
    expect(isStranded({ ...base, status: 'canceled' }, offMs)).toBe(false);
  });
  test('mutated after offboarding → not stranded', () => {
    expect(isStranded({ ...base, lastCalendarMutationAt: '2026-05-20T00:00:00Z' }, offMs)).toBe(false);
  });
  test('missing mutation timestamp → stranded (cannot prove handled)', () => {
    expect(isStranded({ ...base, lastCalendarMutationAt: null }, offMs)).toBe(true);
  });
  test('unparseable mutation timestamp → stranded', () => {
    expect(isStranded({ ...base, lastCalendarMutationAt: 'garbage' }, offMs)).toBe(true);
  });
});

describe('parseBookingRow', () => {
  test('maps all fields', () => {
    const b = parseBookingRow(bookingItem());
    expect(b).toMatchObject({
      tenantId: 'TEN1',
      bookingId: 'booking#abc',
      status: 'booked',
      coordinatorEmail: 'maya@org.com',
      resourceId: 'res-maya',
      externalEventId: 'evt-1',
      appointmentTypeId: 'apt-1',
    });
  });

  test('forward-compatible: an old row missing optional fields defaults rather than crashes', () => {
    const b = parseBookingRow({
      tenantId: { S: 'TEN1' },
      booking_id: { S: 'booking#x' },
      status: { S: 'booked' },
      coordinator_email: { S: 'maya@org.com' },
    });
    expect(b.itemType).toBe('booking'); // defaulted
    expect(b.timezone).toBe('UTC'); // defaulted
    expect(b.appointmentTypeId).toBe('');
    expect(b.externalEventId).toBeNull();
  });
});

describe('reassignBookingResource', () => {
  test('issues a guarded UpdateItem repointing resource + coordinator_email + mutation', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await reassignBookingResource({
      tenantId: 'TEN1',
      bookingId: 'booking#abc',
      fromResourceId: 'res-maya',
      newResourceId: 'res-diego',
      newCoordinatorEmail: 'diego@org.com',
      mutationAt: '2026-05-15T01:00:00Z',
    });
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.ConditionExpression).toBe('#st = :booked AND resource_id = :old');
    expect(input.ExpressionAttributeValues[':new']).toEqual({ S: 'res-diego' });
    expect(input.ExpressionAttributeValues[':email']).toEqual({ S: 'diego@org.com' });
    expect(input.ExpressionAttributeValues[':old']).toEqual({ S: 'res-maya' });
  });
});

describe('isConditionalCheckFailed', () => {
  test('detects the ConditionalCheckFailedException', () => {
    expect(isConditionalCheckFailed({ name: 'ConditionalCheckFailedException' })).toBe(true);
    expect(isConditionalCheckFailed(new Error('other'))).toBe(false);
    expect(isConditionalCheckFailed(null)).toBe(false);
  });
});
