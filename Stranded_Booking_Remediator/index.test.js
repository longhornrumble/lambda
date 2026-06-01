'use strict';

// Mock every collaborator so the handler test exercises validation + orchestration
// only (each collaborator has its own focused suite).
const mockFindStranded = jest.fn();
const mockRemediate = jest.fn();

jest.mock('./booking-store', () => ({
  findStrandedBookings: (...a) => mockFindStranded(...a),
  reassignBookingResource: jest.fn(),
  isConditionalCheckFailed: jest.fn(),
}));
jest.mock('./remediation', () => ({
  remediate: (...a) => mockRemediate(...a),
  OUTCOMES: { REASSIGNED: 'reassigned', NO_ELIGIBLE: 'no_eligible_coordinator', CANCELED: 'canceled', LEFT: 'left' },
}));
jest.mock('./routing-context', () => ({ buildResolveAlternate: () => jest.fn() }));
jest.mock('./calendar-ops', () => ({ transferEvent: jest.fn(), deleteEvent: jest.fn() }));
jest.mock('./oauth-client', () => ({ getOAuthClient: jest.fn() }));

const { handler, _test } = require('./index');

beforeEach(() => {
  mockFindStranded.mockReset();
  mockRemediate.mockReset();
});

const VALID = {
  tenant_id: 'TEN1',
  coordinator_email: 'maya@org.com',
  offboarding_time: '2026-05-15T00:00:00Z',
};

describe('validateInput', () => {
  test('accepts a valid input and defaults choice to null (cascade)', () => {
    expect(_test.validateInput(VALID)).toEqual({
      tenantId: 'TEN1', coordinatorEmail: 'maya@org.com', offboardingTime: '2026-05-15T00:00:00Z', choice: null,
    });
  });
  test('accepts an explicit choice', () => {
    expect(_test.validateInput({ ...VALID, choice: 'cancel' }).choice).toBe('cancel');
  });
  test.each([
    ['non-object', null, 'JSON object'],
    ['missing tenant_id', { ...VALID, tenant_id: undefined }, 'tenant_id'],
    ['bad tenant_id charset', { ...VALID, tenant_id: 'bad id!' }, 'tenant_id'],
    ['missing coordinator_email', { ...VALID, coordinator_email: undefined }, 'coordinator_email'],
    ['bad offboarding_time', { ...VALID, offboarding_time: 'nope' }, 'offboarding_time'],
    ['invalid choice', { ...VALID, choice: 'delete-everything' }, 'choice must be one of'],
  ])('rejects %s', (_label, input, msg) => {
    expect(() => _test.validateInput(input)).toThrow(msg);
  });
});

describe('handler orchestration', () => {
  test('applies the choice to every stranded booking and aggregates results', async () => {
    mockFindStranded.mockResolvedValue([
      { bookingId: 'booking#1' },
      { bookingId: 'booking#2' },
    ]);
    mockRemediate
      .mockResolvedValueOnce({ outcome: 'canceled' })
      .mockResolvedValueOnce({ outcome: 'canceled' });

    const out = await handler({ ...VALID, choice: 'cancel' });

    expect(out).toEqual({
      tenant_id: 'TEN1',
      stranded: 2,
      applied: 'cancel',
      results: [
        { booking_id: 'booking#1', outcome: 'canceled' },
        { booking_id: 'booking#2', outcome: 'canceled' },
      ],
      failed: [],
    });
    expect(mockRemediate).toHaveBeenCalledWith({ bookingId: 'booking#1' }, 'cancel', expect.any(Object));
  });

  test('default cascade reported as applied=cascade when choice omitted', async () => {
    mockFindStranded.mockResolvedValue([{ bookingId: 'booking#1' }]);
    mockRemediate.mockResolvedValue({ outcome: 'reassigned', newResourceId: 'res-diego' });
    const out = await handler(VALID);
    expect(out.applied).toBe('cascade');
    expect(mockRemediate).toHaveBeenCalledWith({ bookingId: 'booking#1' }, null, expect.any(Object));
    expect(out.results[0]).toMatchObject({ booking_id: 'booking#1', outcome: 'reassigned' });
  });

  test('a per-booking failure is captured in failed[] without aborting the run', async () => {
    mockFindStranded.mockResolvedValue([{ bookingId: 'booking#1' }, { bookingId: 'booking#2' }]);
    mockRemediate
      .mockRejectedValueOnce(new Error('google 500'))
      .mockResolvedValueOnce({ outcome: 'left' });
    const out = await handler(VALID);
    expect(out.failed).toEqual([{ booking_id: 'booking#1', error: 'google 500' }]);
    expect(out.results).toEqual([{ booking_id: 'booking#2', outcome: 'left' }]);
  });

  test('no stranded bookings → empty results, no remediate calls', async () => {
    mockFindStranded.mockResolvedValue([]);
    const out = await handler(VALID);
    expect(out.stranded).toBe(0);
    expect(out.results).toEqual([]);
    expect(mockRemediate).not.toHaveBeenCalled();
  });

  test('invalid input rejects before any table access', async () => {
    await expect(handler({ tenant_id: 'TEN1' })).rejects.toThrow('coordinator_email');
    expect(mockFindStranded).not.toHaveBeenCalled();
  });
});

describe('buildDeps', () => {
  test('wires defaults including a now() that yields an ISO timestamp', () => {
    const deps = _test.buildDeps();
    expect(typeof deps.now()).toBe('string');
    expect(deps.calendarOps).toBeDefined();
    expect(typeof deps.resolveAlternate).toBe('function');
  });
});

describe('hashId', () => {
  test('never returns the raw email and is stable', () => {
    const h = _test.hashId('maya@org.com');
    expect(h).toHaveLength(12);
    expect(h).not.toContain('maya');
    expect(_test.hashId('maya@org.com')).toBe(h);
  });
});
