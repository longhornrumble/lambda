'use strict';

/**
 * Unit tests for index.js — the SQS consumer router for the four calendar-lifecycle
 * events. booking-reconcile + channel-degrade are mocked (the per-handler behavior is
 * covered by their own suites); these tests assert routing, partial-batch, malformed→DLQ,
 * and PII-redacted logging.
 */

const mockDeleted = jest.fn();
const mockMoved = jest.fn();
const mockReassigned = jest.fn();
const mockDegrade = jest.fn();
jest.mock('./booking-reconcile', () => ({
  reconcileDeleted: mockDeleted,
  reconcileMoved: mockMoved,
  reconcileReassigned: mockReassigned,
}));
jest.mock('./channel-degrade', () => ({
  degradeOnEventPrivate: mockDegrade,
}));

const idx = require('./index');

const MUTATION = '2026-06-03T18:00:00.000Z';

function rec(messageId, obj) {
  return { messageId, body: JSON.stringify(obj) };
}
function envOf(eventType, extra = {}) {
  return {
    event_type: eventType, event_id: 'b1', tenant_id: 'AUS123957', booking_id: 'b1',
    last_calendar_mutation_at: MUTATION, dispatched_at: MUTATION, calendar_provider: 'google', ...extra,
  };
}

let warnSpy;
beforeEach(() => {
  mockDeleted.mockReset().mockResolvedValue(undefined);
  mockMoved.mockReset().mockResolvedValue(undefined);
  mockReassigned.mockReset().mockResolvedValue(undefined);
  mockDegrade.mockReset().mockResolvedValue(undefined);
  jest.spyOn(console, 'log').mockImplementation(() => {});
  warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('routing — the four owned event types', () => {
  it('routes booking.calendar_deleted → reconcileDeleted with the parsed envelope', async () => {
    const res = await idx.handler({ Records: [rec('m', envOf('booking.calendar_deleted'))] });
    expect(res.batchItemFailures).toEqual([]);
    expect(mockDeleted).toHaveBeenCalledWith(expect.objectContaining({
      event_type: 'booking.calendar_deleted', booking_id: 'b1',
    }));
    expect(mockMoved).not.toHaveBeenCalled();
  });

  it('routes booking.calendar_moved → reconcileMoved with the parsed envelope', async () => {
    await idx.handler({ Records: [rec('m', envOf('booking.calendar_moved'))] });
    expect(mockMoved).toHaveBeenCalledWith(expect.objectContaining({
      event_type: 'booking.calendar_moved', booking_id: 'b1',
    }));
  });

  it('routes booking.calendar_reassigned → reconcileReassigned with the parsed envelope', async () => {
    await idx.handler({ Records: [rec('m', envOf('booking.calendar_reassigned', {
      previous_resource_id: 'old@org.example', new_resource_id: 'new@org.example',
    }))] });
    expect(mockReassigned).toHaveBeenCalledWith(expect.objectContaining({
      event_type: 'booking.calendar_reassigned', booking_id: 'b1', new_resource_id: 'new@org.example',
    }));
  });

  it('routes booking.event_made_private → degradeOnEventPrivate with the parsed envelope', async () => {
    await idx.handler({ Records: [rec('m', envOf('booking.event_made_private', { channel_id: 'chan-1' }))] });
    expect(mockDegrade).toHaveBeenCalledWith(expect.objectContaining({
      event_type: 'booking.event_made_private', booking_id: 'b1', channel_id: 'chan-1',
    }));
  });
});

describe('non-owned event types — logged + discarded (NOT DLQ)', () => {
  it.each([
    'booking.ooo_overlap_detected',
    'booking.attendee_accepted',
    'booking.attendee_declined',
  ])('logs + discards %s without a handler call or DLQ', async (eventType) => {
    const res = await idx.handler({ Records: [rec('m', envOf(eventType))] });
    expect(res.batchItemFailures).toEqual([]);
    expect(mockDeleted).not.toHaveBeenCalled();
    expect(mockMoved).not.toHaveBeenCalled();
    expect(mockReassigned).not.toHaveBeenCalled();
    expect(mockDegrade).not.toHaveBeenCalled();
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
  it('a malformed error raised by a handler routes the record to the DLQ', async () => {
    mockDeleted.mockRejectedValue(Object.assign(new Error('missing field'), { malformed: true }));
    const res = await idx.handler({ Records: [rec('mf', envOf('booking.calendar_deleted'))] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'mf' }]);
  });
});

describe('downstream failures → redrive', () => {
  it('a non-malformed handler error marks the record for redrive', async () => {
    mockMoved.mockRejectedValue(new Error('DDB throttled'));
    const res = await idx.handler({ Records: [rec('m', envOf('booking.calendar_moved'))] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm' }]);
  });

  it('logs err.name, NOT the raw message (F4 — AccessDenied message embeds the table ARN)', async () => {
    const e = new Error('User: arn:aws:sts::525409062831:assumed-role/x is not authorized to perform dynamodb:UpdateItem on arn:aws:dynamodb:us-east-1:525409062831:table/picasso-booking-staging');
    e.name = 'AccessDeniedException';
    mockDeleted.mockRejectedValue(e);
    const res = await idx.handler({ Records: [rec('m', envOf('booking.calendar_deleted'))] });

    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm' }]);
    const logged = warnSpy.mock.calls.map((c) => String(c[0])).join(' ');
    expect(logged).toContain('event_processing_failed');
    expect(logged).toContain('AccessDeniedException');
    expect(logged).not.toContain('arn:aws:dynamodb'); // ARN must not leak into logs
  });
});

describe('PII-redacted logging (SR-2)', () => {
  it('strips coordinator emails from a malformed reassigned event log', async () => {
    // valid JSON, but reconcileReassigned will reject for a missing field — still carries PII.
    mockReassigned.mockRejectedValue(Object.assign(new Error('missing field'), { malformed: true }));
    const r = rec('pii', envOf('booking.calendar_reassigned', {
      previous_resource_id: 'old@org.example', new_resource_id: 'leak@org.example',
    }));
    const res = await idx.handler({ Records: [r] });

    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'pii' }]);
    const logged = warnSpy.mock.calls.map((c) => String(c[0])).join(' ');
    expect(logged).toContain('event_malformed');
    expect(logged).not.toContain('old@org.example');
    expect(logged).not.toContain('leak@org.example');
    expect(logged).toContain('"booking_id":"b1"'); // non-PII fields preserved
  });

  it('logs [unparseable] (not the raw body) for non-JSON malformed bodies', async () => {
    await idx.handler({ Records: [{ messageId: 'np', body: 'not json{ new_resource_id: leak@org.example' }] });
    const logged = warnSpy.mock.calls.map((c) => String(c[0])).join(' ');
    expect(logged).toContain('[unparseable]');
    expect(logged).not.toContain('leak@org.example');
  });

  it('redactBody strips all four PII fields directly', () => {
    const out = idx.redactBody(JSON.stringify({
      booking_id: 'b1', attendee_email: 'a@x', coordinator_email: 'c@x',
      previous_resource_id: 'p@x', new_resource_id: 'n@x',
    }));
    expect(out).toEqual({ booking_id: 'b1' });
  });
});

describe('batch behavior', () => {
  it('reports only the failed records — the rest of the batch still processes', async () => {
    mockMoved.mockRejectedValueOnce(new Error('boom'));
    const res = await idx.handler({
      Records: [
        rec('ok', envOf('booking.calendar_deleted')),
        rec('fail', envOf('booking.calendar_moved')),
        { messageId: 'bad', body: '{' },
      ],
    });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'fail' }, { itemIdentifier: 'bad' }]);
    expect(mockDeleted).toHaveBeenCalledTimes(1); // the good record still processed
  });

  it('empty / missing Records → empty failures', async () => {
    expect(await idx.handler({})).toEqual({ batchItemFailures: [] });
    expect(await idx.handler({ Records: [] })).toEqual({ batchItemFailures: [] });
  });
});

describe('parseEnvelope (exported)', () => {
  it('parses a valid object body', () => {
    expect(idx.parseEnvelope('{"event_type":"x"}')).toEqual({ event_type: 'x' });
  });
  it('throws malformed on non-JSON and on arrays', () => {
    expect(() => idx.parseEnvelope('nope')).toThrow();
    expect(() => idx.parseEnvelope('[1]')).toThrow();
  });
});
