'use strict';

/**
 * Unit tests for Attendance_Disposition_Handler/booking-store.js — the AWS glue.
 * Mocks DynamoDB + Lambda via aws-sdk-client-mock and featureGate.loadTenantConfig.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const {
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

jest.mock('../shared/scheduling/featureGate', () => ({ loadTenantConfig: jest.fn() }));
const { loadTenantConfig } = require('../shared/scheduling/featureGate');

const store = require('./booking-store');

const ddbMock = mockClient(DynamoDBClient);
const lambdaMock = mockClient(LambdaClient);

beforeEach(() => {
  ddbMock.reset();
  lambdaMock.reset();
  jest.clearAllMocks();
  lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
});

describe('getBooking / fromItem', () => {
  test('maps a projected row', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId: { S: 'T' },
        booking_id: { S: 'bk-1' },
        status: { S: 'booked' },
        attendee_name: { S: 'Sam Patel' },
        conference_provider: { S: 'zoom' },
      },
    });
    const b = await store.getBooking('T', 'bk-1');
    expect(b).toMatchObject({ tenantId: 'T', booking_id: 'bk-1', status: 'booked', conference_provider: 'zoom' });
    expect(b.coordinator_email).toBeNull(); // absent → null (schema discipline)
  });
  test('null when absent', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    expect(await store.getBooking('T', 'bk-x')).toBeNull();
    expect(store.fromItem(null)).toBeNull();
  });
});

describe('setAttendanceState', () => {
  test('success → true with idempotent guard', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const r = await store.setAttendanceState({ tenantId: 'T', bookingId: 'bk-1' });
    expect(r).toBe(true);
    const cmd = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(cmd.ConditionExpression).toContain('attribute_not_exists(attendance_state)');
    expect(cmd.ExpressionAttributeValues[':pending'].S).toBe('pending_attendance');
  });
  test('ConditionalCheckFailed → false', async () => {
    const e = new Error('cond'); e.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(e);
    expect(await store.setAttendanceState({ tenantId: 'T', bookingId: 'bk-1' })).toBe(false);
  });
  test('other error propagates', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('throttled'));
    await expect(store.setAttendanceState({ tenantId: 'T', bookingId: 'bk-1' })).rejects.toThrow(/throttled/);
  });
});

describe('getAdminEmails', () => {
  test('collects + dedupes across config shapes', async () => {
    loadTenantConfig.mockResolvedValue({
      scheduling: { notification_emails: ['a@x', 'b@x'], admin_email: 'a@x' },
      notification_emails: ['c@x'],
    });
    expect(await store.getAdminEmails('T')).toEqual(['a@x', 'b@x', 'c@x']);
  });
  test('config load failure → []', async () => {
    loadTenantConfig.mockRejectedValue(new Error('s3 miss'));
    expect(await store.getAdminEmails('T')).toEqual([]);
  });
  test('no scheduling block → []', async () => {
    loadTenantConfig.mockResolvedValue({});
    expect(await store.getAdminEmails('T')).toEqual([]);
  });
});

describe('queryPendingAttendance (bounded GSI, paginated)', () => {
  test('paginates + maps + filters in the query', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({ Items: [{ booking_id: { S: 'bk-1' }, start_at: { S: '2026-05-01T00:00:00Z' } }], LastEvaluatedKey: { k: { S: '1' } } })
      .resolvesOnce({ Items: [{ booking_id: { S: 'bk-2' }, start_at: { S: '2026-05-02T00:00:00Z' } }] });
    const rows = await store.queryPendingAttendance({ tenantId: 'T', olderThanDays: 7, now: Date.parse('2026-06-01T00:00:00Z') });
    expect(rows.map((r) => r.booking_id)).toEqual(['bk-1', 'bk-2']);
    const cmd = ddbMock.commandCalls(QueryCommand)[0].args[0].input;
    expect(cmd.IndexName).toBe('tenantId-start_at-index');
    expect(cmd.FilterExpression).toContain('attendance_state = :pending');
  });
  test('empty result → []', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    expect(await store.queryPendingAttendance({ tenantId: 'T' })).toEqual([]);
  });
});

describe('sendEmail / sendSms (Lambda invoke)', () => {
  test('sendEmail wraps body + cc array', async () => {
    await store.sendEmail({ tenantId: 'T', to: 'a@x', cc: ['admin@x'], subject: 's', html_body: 'h', text_body: 't' });
    const payload = JSON.parse(Buffer.from(lambdaMock.commandCalls(InvokeCommand)[0].args[0].input.Payload).toString());
    const inner = JSON.parse(payload.body);
    expect(inner.to).toEqual(['a@x']);
    expect(inner.cc).toEqual(['admin@x']);
  });
  test('sendEmail with array to + no cc', async () => {
    await store.sendEmail({ tenantId: 'T', to: ['x@x', 'y@x'], subject: 's', text_body: 't' });
    const inner = JSON.parse(JSON.parse(Buffer.from(lambdaMock.commandCalls(InvokeCommand)[0].args[0].input.Payload).toString()).body);
    expect(inner.to).toEqual(['x@x', 'y@x']);
    expect(inner.cc).toBeUndefined();
  });
  test('sendSms invokes SMS_Sender with sendType', async () => {
    await store.sendSms({ tenantId: 'T', to: '+1', body: 'hi', sendType: 'internal' });
    const payload = JSON.parse(Buffer.from(lambdaMock.commandCalls(InvokeCommand)[0].args[0].input.Payload).toString());
    expect(payload).toMatchObject({ to: '+1', sendType: 'internal', type: 'reminder' });
  });
  test('sendSms defaults sendType to contact', async () => {
    await store.sendSms({ tenantId: 'T', to: '+1', body: 'hi' });
    const payload = JSON.parse(Buffer.from(lambdaMock.commandCalls(InvokeCommand)[0].args[0].input.Payload).toString());
    expect(payload.sendType).toBe('contact');
  });
});

describe('writePortalInboxAlert / makeZoomReachableProbe', () => {
  test('portal inbox alert is a logged stub', async () => {
    expect(await store.writePortalInboxAlert({ tenantId: 'T', bookingId: 'bk-1', kind: 'attendance_unresolved', createdAt: 1 })).toEqual({ stub: true });
  });
  test('zoom probe honors event signal', async () => {
    expect(await store.makeZoomReachableProbe({ zoom_unreachable: true })()).toBe(false);
    expect(await store.makeZoomReachableProbe({})()).toBe(true);
    expect(await store.makeZoomReachableProbe(undefined)()).toBe(true);
  });
});
