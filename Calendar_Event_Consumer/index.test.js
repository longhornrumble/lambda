'use strict';

/**
 * Unit tests for index.js — the SQS consumer router for B9 + B10.
 * booking-updates is mocked (persistent mock fns so module re-loads keep the same
 * handles); SNS is mocked with aws-sdk-client-mock.
 */

// Set BEFORE requiring index — OPS_ALERTS_TOPIC_ARN is read at module load.
process.env.OPS_ALERTS_TOPIC_ARN = 'arn:aws:sns:us-east-1:525409062831:picasso-ops-alerts-staging';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

// Persistent mock fns (the `mock` prefix lets the hoisted factory reference them).
const mockFlag = jest.fn();
const mockCancel = jest.fn();
jest.mock('./booking-updates', () => ({
  flagOooConflict: mockFlag,
  cancelOnDecline: mockCancel,
}));

const snsMock = mockClient(SNSClient);

const idx = require('./index');

const MUTATION = '2026-06-03T18:00:00.000Z';

function rec(messageId, obj) {
  return { messageId, body: JSON.stringify(obj) };
}
function oooRecord(ids, extra = {}) {
  return rec('m-ooo', {
    event_type: 'booking.ooo_overlap_detected',
    tenant_id: 'AUS123957',
    booking_id: ids[0],
    last_calendar_mutation_at: MUTATION,
    ooo_start_at: '2026-06-03T17:00:00Z',
    ooo_end_at: '2026-06-03T19:00:00Z',
    overlapping_booking_ids: ids,
    ...extra,
  });
}
function declinedRecord(bookingId, extra = {}) {
  return rec('m-dec', {
    event_type: 'booking.attendee_declined',
    tenant_id: 'AUS123957',
    booking_id: bookingId,
    last_calendar_mutation_at: MUTATION,
    attendee_email: 'volunteer@example.org',
    response_status: 'declined',
    ...extra,
  });
}

beforeEach(() => {
  snsMock.reset();
  snsMock.on(PublishCommand).resolves({ MessageId: 'sns-1' });
  mockFlag.mockReset().mockResolvedValue(true);
  mockCancel.mockReset().mockResolvedValue(true);
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe('B9 — booking.ooo_overlap_detected', () => {
  it('flags EVERY booking in overlapping_booking_ids and alerts per newly-flagged', async () => {
    const res = await idx.handler({ Records: [oooRecord(['b1', 'b2', 'b3'])] });

    expect(res.batchItemFailures).toEqual([]);
    expect(mockFlag).toHaveBeenCalledTimes(3);
    expect(mockFlag.mock.calls.map((c) => c[0].bookingId)).toEqual(['b1', 'b2', 'b3']);
    expect(mockFlag).toHaveBeenCalledWith(expect.objectContaining({
      tenantId: 'AUS123957', bookingId: 'b1', mutationAt: MUTATION,
      oooStartAt: '2026-06-03T17:00:00Z', oooEndAt: '2026-06-03T19:00:00Z',
    }));
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 3);
  });

  it('acts on the FULL array, NOT the non-deterministic envelope booking_id', async () => {
    // envelope booking_id is bX, but the real overlap set is [b1, b2].
    const r = oooRecord(['b1', 'b2'], { booking_id: 'bX' });
    await idx.handler({ Records: [r] });

    const flagged = mockFlag.mock.calls.map((c) => c[0].bookingId);
    expect(flagged).toEqual(['b1', 'b2']);
    expect(flagged).not.toContain('bX');
  });

  it('falls back to [booking_id] when overlapping_booking_ids is absent (schema discipline)', async () => {
    const r = rec('m-ooo', {
      event_type: 'booking.ooo_overlap_detected',
      tenant_id: 'AUS123957',
      booking_id: 'solo',
      last_calendar_mutation_at: MUTATION,
    });
    const res = await idx.handler({ Records: [r] });

    expect(res.batchItemFailures).toEqual([]);
    expect(mockFlag).toHaveBeenCalledTimes(1);
    expect(mockFlag.mock.calls[0][0].bookingId).toBe('solo');
  });

  it('suppresses the admin alert for an already-flagged / non-booked / absent booking', async () => {
    mockFlag.mockResolvedValueOnce(true).mockResolvedValueOnce(false);
    await idx.handler({ Records: [oooRecord(['b1', 'b2'])] });

    expect(mockFlag).toHaveBeenCalledTimes(2);
    // only b1 (true) alerts; b2 (false) is a dedupe/no-op.
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 1);
  });

  it('treats an OOO event with no usable booking ids as malformed → DLQ', async () => {
    const r = rec('m-ooo', {
      event_type: 'booking.ooo_overlap_detected',
      tenant_id: 'AUS123957',
      last_calendar_mutation_at: MUTATION,
      overlapping_booking_ids: [],
    });
    const res = await idx.handler({ Records: [r] });

    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm-ooo' }]);
    expect(mockFlag).not.toHaveBeenCalled();
  });

  it('ignores non-string ids inside the array', async () => {
    const r = oooRecord(['b1'], { overlapping_booking_ids: ['b1', 42, null, ''] });
    await idx.handler({ Records: [r] });
    expect(mockFlag.mock.calls.map((c) => c[0].bookingId)).toEqual(['b1']);
  });

  it('a downstream (non-conditional) failure marks the record for redrive', async () => {
    mockFlag.mockRejectedValue(new Error('DDB throttled'));
    const res = await idx.handler({ Records: [oooRecord(['b1'])] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm-ooo' }]);
  });

  it('a best-effort admin-alert failure does NOT fail the record', async () => {
    snsMock.on(PublishCommand).rejects(new Error('SNS down'));
    const res = await idx.handler({ Records: [oooRecord(['b1'])] });
    expect(res.batchItemFailures).toEqual([]); // flag is durable; alert is best-effort
  });
});

describe('B10 — booking.attendee_declined', () => {
  it('transitions booked→canceled and never publishes an alert or notifies the volunteer', async () => {
    const res = await idx.handler({ Records: [declinedRecord('b1')] });

    expect(res.batchItemFailures).toEqual([]);
    expect(mockCancel).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'b1' });
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 0);
  });

  it('no-ops idempotently when the booking is already canceled/terminal/absent', async () => {
    mockCancel.mockResolvedValue(false);
    const res = await idx.handler({ Records: [declinedRecord('b1')] });
    expect(res.batchItemFailures).toEqual([]);
    expect(mockCancel).toHaveBeenCalledTimes(1);
  });

  it('a downstream failure marks the record for redrive', async () => {
    mockCancel.mockRejectedValue(new Error('DDB error'));
    const res = await idx.handler({ Records: [declinedRecord('b1')] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm-dec' }]);
  });
});

describe('attendee_accepted — known no-op (B10 accept side)', () => {
  it('makes no Booking write and does not fail', async () => {
    const r = rec('m-acc', {
      event_type: 'booking.attendee_accepted',
      tenant_id: 'AUS123957',
      booking_id: 'b1',
      last_calendar_mutation_at: MUTATION,
      response_status: 'accepted',
    });
    const res = await idx.handler({ Records: [r] });
    expect(res.batchItemFailures).toEqual([]);
    expect(mockFlag).not.toHaveBeenCalled();
    expect(mockCancel).not.toHaveBeenCalled();
  });
});

describe('non-owned event types', () => {
  it.each([
    'booking.calendar_deleted',
    'booking.calendar_moved',
    'booking.calendar_reassigned',
    'booking.event_made_private',
  ])('logs + discards %s without DLQ or a Booking write', async (eventType) => {
    const r = rec('m-other', { event_type: eventType, tenant_id: 'AUS123957', booking_id: 'b1', last_calendar_mutation_at: MUTATION });
    const res = await idx.handler({ Records: [r] });
    expect(res.batchItemFailures).toEqual([]);
    expect(mockFlag).not.toHaveBeenCalled();
    expect(mockCancel).not.toHaveBeenCalled();
  });
});

describe('malformed payloads → DLQ (error contract)', () => {
  it('non-JSON body', async () => {
    const res = await idx.handler({ Records: [{ messageId: 'bad', body: 'not json{' }] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'bad' }]);
  });

  it('JSON array body (not an object)', async () => {
    const res = await idx.handler({ Records: [{ messageId: 'arr', body: '[1,2,3]' }] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'arr' }]);
  });

  it('missing event_type', async () => {
    const res = await idx.handler({ Records: [rec('noet', { tenant_id: 'AUS123957', booking_id: 'b1' })] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'noet' }]);
  });

  it('declined missing tenant_id', async () => {
    const res = await idx.handler({ Records: [rec('not', { event_type: 'booking.attendee_declined', booking_id: 'b1' })] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'not' }]);
    expect(mockCancel).not.toHaveBeenCalled();
  });
});

describe('batch behavior', () => {
  it('reports only the failed records — the rest of the batch still processes', async () => {
    mockFlag.mockResolvedValue(true);
    mockCancel.mockRejectedValueOnce(new Error('boom')); // the declined record fails
    const res = await idx.handler({
      Records: [oooRecord(['b1']), declinedRecord('b2'), { messageId: 'bad', body: '{' }],
    });
    expect(res.batchItemFailures).toEqual([
      { itemIdentifier: 'm-dec' },
      { itemIdentifier: 'bad' },
    ]);
    // the good OOO record still flagged + alerted
    expect(mockFlag).toHaveBeenCalledTimes(1);
    expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 1);
  });

  it('empty / missing Records → empty failures', async () => {
    expect(await idx.handler({})).toEqual({ batchItemFailures: [] });
    expect(await idx.handler({ Records: [] })).toEqual({ batchItemFailures: [] });
  });
});

describe('admin alert routing', () => {
  it('publishes to OPS_ALERTS_TOPIC_ARN with a bounded subject + JSON detail', async () => {
    await idx.handler({ Records: [oooRecord(['b1'])] });
    const call = snsMock.commandCalls(PublishCommand)[0].args[0].input;
    expect(call.TopicArn).toBe('arn:aws:sns:us-east-1:525409062831:picasso-ops-alerts-staging');
    expect(call.Subject.length).toBeLessThanOrEqual(100);
    const detail = JSON.parse(call.Message);
    expect(detail).toMatchObject({ kind: 'booking.ooo_overlap_detected', tenant_id: 'AUS123957', booking_id: 'b1' });
  });

  it('skips the publish (and still flags) when no topic is configured', async () => {
    const prev = process.env.OPS_ALERTS_TOPIC_ARN;
    delete process.env.OPS_ALERTS_TOPIC_ARN;
    jest.resetModules();
    snsMock.reset();
    snsMock.on(PublishCommand).resolves({});
    mockFlag.mockResolvedValue(true);
    try {
      const freshIdx = require('./index');
      const res = await freshIdx.handler({ Records: [oooRecord(['b1'])] });
      expect(res.batchItemFailures).toEqual([]);
      expect(snsMock).toHaveReceivedCommandTimes(PublishCommand, 0);
      expect(mockFlag).toHaveBeenCalledTimes(1);
    } finally {
      process.env.OPS_ALERTS_TOPIC_ARN = prev;
      jest.resetModules();
    }
  });
});
