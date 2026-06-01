'use strict';

/**
 * Unit tests for channel-degrade.js — booking.event_made_private → channels-table status
 * degrade + best-effort admin alert. DynamoDB + SNS mocked with aws-sdk-client-mock.
 */

process.env.OPS_ALERTS_TOPIC_ARN = 'arn:aws:sns:us-east-1:525409062831:picasso-ops-alerts-staging';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const ddbMock = mockClient(DynamoDBClient);
const snsMock = mockClient(SNSClient);

const degrade = require('./channel-degrade');
// Nothing is jest.mock'd in this file, so requiring index here gives a genuine
// end-to-end path (index → channel-degrade → DDB/SNS mocks) for the F5a redelivery test.
const idx = require('./index');

function conditionalFail() {
  const e = new Error('The conditional request failed');
  e.name = 'ConditionalCheckFailedException';
  return e;
}

beforeEach(() => {
  ddbMock.reset();
  snsMock.reset();
  snsMock.on(PublishCommand).resolves({ MessageId: 'sns-1' });
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe('degradeChannel — conditional channels-table UpdateItem', () => {
  it('returns true and issues a tenant-guarded, active-guarded UpdateItem keyed on channel_id', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await degrade.degradeChannel({ channelId: 'chan-123', tenantId: 'AUS123957', now: '2026-06-01T00:00:00.000Z' });

    expect(result).toBe(true);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.TableName).toBe(degrade._CHANNELS_TABLE);
    expect(call.Key).toEqual({ channel_id: { S: 'chan-123' } });
    // F3: tenant guard prevents a cross-tenant channel_id degrading another tenant's row.
    expect(call.ConditionExpression).toBe('attribute_exists(channel_id) AND tenant_id = :tid AND (attribute_not_exists(#st) OR #st = :active)');
    expect(call.ExpressionAttributeNames).toEqual({ '#st': 'status' });
    expect(call.ExpressionAttributeValues[':private']).toEqual({ S: 'event_body_private' });
    expect(call.ExpressionAttributeValues[':active']).toEqual({ S: 'active' });
    expect(call.ExpressionAttributeValues[':tid']).toEqual({ S: 'AUS123957' });
    expect(call.UpdateExpression).toContain('event_body_private_at = :at');
  });

  it('returns false on ConditionalCheckFailed (absent / wrong tenant / already private / renewal-failed)', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await expect(degrade.degradeChannel({ channelId: 'chan-123', tenantId: 'AUS123957' })).resolves.toBe(false);
  });

  it('does NOT clobber an unwatched_renewal_failed channel (F5b — guard is #st = :active only)', async () => {
    // The condition admits only attribute_not_exists(#st) OR #st='active'; a
    // renewal-failed row matches neither → DDB rejects ConditionalCheckFailed → false, no
    // status overwrite. (The mock cannot evaluate the expression, so we assert BOTH the
    // active-only guard string AND the false return on the conditional failure.)
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    const result = await degrade.degradeChannel({ channelId: 'chan-rf', tenantId: 'AUS123957' });
    expect(result).toBe(false);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.ConditionExpression).toContain('#st = :active');
    expect(call.ConditionExpression).not.toContain('unwatched_renewal_failed'); // never widened
    expect(call.ExpressionAttributeValues[':active']).toEqual({ S: 'active' });
  });

  it('propagates a non-conditional DDB error so SQS redrives', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('Throttled'));
    await expect(degrade.degradeChannel({ channelId: 'chan-123', tenantId: 'AUS123957' })).rejects.toThrow('Throttled');
  });

  it('throws when channelId or tenantId is missing', async () => {
    await expect(degrade.degradeChannel({ tenantId: 'AUS123957' })).rejects.toThrow(/requires channelId, tenantId/);
    await expect(degrade.degradeChannel({ channelId: 'chan-123' })).rejects.toThrow(/requires channelId, tenantId/);
  });
});

describe('degradeOnEventPrivate — channel_id PRESENT (happy path)', () => {
  const env = { tenant_id: 'AUS123957', booking_id: 'b1', channel_id: 'chan-123' };

  it('degrades the channel and fires the admin alert exactly once', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    await degrade.degradeOnEventPrivate(env);

    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 1);
    const detail = JSON.parse(snsMock.commandCalls(PublishCommand)[0].args[0].input.Message);
    expect(detail).toMatchObject({
      kind: 'booking.event_made_private', tenant_id: 'AUS123957', booking_id: 'b1',
      channel_degraded: true, channel_id_present: true,
    });
  });

  it('is a silent no-op (no alert) on a re-delivery once already private', async () => {
    ddbMock.on(UpdateItemCommand).rejects(conditionalFail());
    await degrade.degradeOnEventPrivate(env);

    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 0); // dedupe — no duplicate alert
  });

  it('propagates a genuine DDB failure (so index.js redrives the record)', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('InternalServerError'));
    await expect(degrade.degradeOnEventPrivate(env)).rejects.toThrow('InternalServerError');
  });
});

describe('degradeOnEventPrivate — channel_id ABSENT (contract gap, escalated)', () => {
  const env = { tenant_id: 'AUS123957', booking_id: 'b1' }; // no channel_id

  it('does NOT write the channel, still alerts, and never DLQs (no throw)', async () => {
    await expect(degrade.degradeOnEventPrivate(env)).resolves.toBeUndefined();

    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 0); // can't key without channel_id
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 1);    // gap surfaced to admin
    const detail = JSON.parse(snsMock.commandCalls(PublishCommand)[0].args[0].input.Message);
    expect(detail).toMatchObject({ channel_degraded: false, channel_id_present: false });
  });
});

describe('degradeOnEventPrivate — malformed envelope (F1)', () => {
  it('throws malformed when tenant_id is missing (→ index DLQs, no silent process)', async () => {
    await expect(degrade.degradeOnEventPrivate({ booking_id: 'b1', channel_id: 'chan-1' }))
      .rejects.toMatchObject({ malformed: true });
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 0);
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 0);
  });

  it('throws malformed when booking_id is missing', async () => {
    await expect(degrade.degradeOnEventPrivate({ tenant_id: 'AUS123957', channel_id: 'chan-1' }))
      .rejects.toMatchObject({ malformed: true });
  });
});

describe('end-to-end via index.handler — SQS redelivery dedupe (F5a)', () => {
  function privateRecord() {
    return {
      messageId: 'm-priv',
      body: JSON.stringify({
        event_type: 'booking.event_made_private',
        tenant_id: 'AUS123957', booking_id: 'b1', channel_id: 'chan-123',
        last_calendar_mutation_at: '2026-06-03T18:00:00.000Z',
      }),
    };
  }

  it('same envelope twice → batchItemFailures=[] both times, exactly one write + one alert', async () => {
    // 1st delivery degrades (write succeeds); 2nd delivery hits the conditional (already
    // private) → no write effect, no duplicate alert. Neither delivery fails the record.
    ddbMock.on(UpdateItemCommand).resolvesOnce({}).rejects(conditionalFail());
    const rec = privateRecord();

    const res1 = await idx.handler({ Records: [rec] });
    expect(res1.batchItemFailures).toEqual([]);
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 1);

    const res2 = await idx.handler({ Records: [rec] });
    expect(res2.batchItemFailures).toEqual([]);
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 1); // still 1 — no double-alert
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 2); // attempted twice, effective once
  });
});

describe('admin alert routing', () => {
  it('skips the publish when no topic is configured (still degrades + no throw)', async () => {
    const prev = process.env.OPS_ALERTS_TOPIC_ARN;
    delete process.env.OPS_ALERTS_TOPIC_ARN;
    jest.resetModules();
    // After resetModules the SDK classes are re-required fresh — mock the FRESH classes
    // (the ones the fresh channel-degrade will resolve), or its client.send hits the real
    // credential provider.
    const ddbSdk = require('@aws-sdk/client-dynamodb');
    const snsSdk = require('@aws-sdk/client-sns');
    const ddb2 = mockClient(ddbSdk.DynamoDBClient);
    const sns2 = mockClient(snsSdk.SNSClient);
    ddb2.on(ddbSdk.UpdateItemCommand).resolves({});
    sns2.on(snsSdk.PublishCommand).resolves({});
    try {
      const fresh = require('./channel-degrade');
      await fresh.degradeOnEventPrivate({ tenant_id: 'AUS123957', booking_id: 'b1', channel_id: 'chan-9' });
      expect(ddb2).toHaveReceivedCommandTimes(ddbSdk.UpdateItemCommand, 1);
      expect(sns2).toHaveReceivedCommandTimes(snsSdk.PublishCommand, 0);
    } finally {
      process.env.OPS_ALERTS_TOPIC_ARN = prev;
      jest.resetModules();
    }
  });

  it('a best-effort admin-alert failure does NOT fail the record', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    snsMock.on(PublishCommand).rejects(new Error('SNS down'));
    await expect(degrade.degradeOnEventPrivate({ tenant_id: 'AUS123957', booking_id: 'b1', channel_id: 'chan-123' }))
      .resolves.toBeUndefined();
  });
});
