'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, QueryCommand } = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);
const scheduledMessages = require('./scheduled-messages-table');

beforeEach(() => ddbMock.reset());

describe('scheduled-messages-table.queryByAppointment', () => {
  test('queries the by-appointment GSI scoped to the tenant partition', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [
        {
          sk: { S: 'SCHEDULED#2026-07-01T15:00:00Z#booking#1#t24h' },
          message_id: { S: 'booking#1#t24h' },
          status: { S: 'pending' },
          moment: { S: 'reminder' },
          tier: { S: 't24h' },
          fire_at: { S: '2026-06-30T15:00:00Z' },
          channel: { S: 'email' },
        },
      ],
    });

    const rows = await scheduledMessages.queryByAppointment('TEN-SYNTH', 'booking#1');
    expect(rows).toEqual([
      {
        sk: 'SCHEDULED#2026-07-01T15:00:00Z#booking#1#t24h',
        message_id: 'booking#1#t24h',
        status: 'pending',
        moment: 'reminder',
        tier: 't24h',
        fire_at: '2026-06-30T15:00:00Z',
        channel: 'email',
        attendance_check: false,
      },
    ]);

    const input = ddbMock.commandCalls(QueryCommand)[0].args[0].input;
    expect(input.IndexName).toBe('by-appointment');
    expect(input.KeyConditionExpression).toBe('appointment_id = :a AND pk = :p');
    expect(input.ExpressionAttributeValues).toEqual({
      ':a': { S: 'booking#1' },
      ':p': { S: 'TENANT#TEN-SYNTH' },
    });
  });

  test('reads the attendance row (attendance_check BOOL, no tier) forward-compatibly', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [
        {
          sk: { S: 'SCHEDULED#2026-07-01T15:00:00Z#booking#1#attendance' },
          message_id: { S: 'booking#1#attendance' },
          status: { S: 'pending' },
          moment: { S: 'reminder' },
          attendance_check: { BOOL: true },
          // tier + channel absent → tolerated (null)
        },
      ],
    });
    const rows = await scheduledMessages.queryByAppointment('TEN-SYNTH', 'booking#1');
    expect(rows[0]).toMatchObject({ attendance_check: true, tier: null, channel: null });
  });

  test('paginates fully (LastEvaluatedKey) and concatenates pages', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({
        Items: [{ sk: { S: 'a' }, status: { S: 'pending' }, tier: { S: 't24h' }, moment: { S: 'reminder' } }],
        LastEvaluatedKey: { appointment_id: { S: 'booking#1' }, pk: { S: 'TENANT#TEN-SYNTH' } },
      })
      .resolvesOnce({
        Items: [{ sk: { S: 'b' }, status: { S: 'sent' }, tier: { S: 't1h' }, moment: { S: 'reminder' } }],
      });
    const rows = await scheduledMessages.queryByAppointment('TEN-SYNTH', 'booking#1');
    expect(rows.map((r) => r.sk)).toEqual(['a', 'b']);
    expect(ddbMock.commandCalls(QueryCommand)).toHaveLength(2);
  });

  test('no rows → empty list', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    expect(await scheduledMessages.queryByAppointment('TEN-SYNTH', 'booking#none')).toEqual([]);
  });
});
