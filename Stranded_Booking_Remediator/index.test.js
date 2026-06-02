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
// (X) gap-C wire: mock the candidate resolver so the loadCandidates adapter is testable
// without touching DDB.
const mockResolveCandidates = jest.fn();
jest.mock('../shared/scheduling/candidate-resolver', () => ({
  resolveCandidates: (...a) => mockResolveCandidates(...a),
}));

const { handler, _test } = require('./index');

beforeEach(() => {
  mockFindStranded.mockReset();
  mockRemediate.mockReset();
  mockResolveCandidates.mockReset();
});

// ─── (X) gap-C wire: loadCandidates adapter ───────────────────────────────────────────

describe('loadCandidatesViaResolver (X wire)', () => {
  test('maps the loadCandidates seam args to resolveCandidates({tenantId, routingPolicyId})', async () => {
    const roster = [{ resourceId: 'r1@x', scheduling_tags: ['mentor'], coordinatorEmail: 'r1@x' }];
    mockResolveCandidates.mockResolvedValue(roster);

    // realistic seam args: routing-context passes the appointmentType + routingPolicy objects
    // it already resolved (the adapter uses only routingPolicy.id and ignores appointmentType).
    const out = await _test.loadCandidatesViaResolver(
      'TEN1',
      { appointmentTypeId: 'apt-1', routingPolicyId: 'rp-7' },
      { id: 'rp-7', tag_conditions: [] }
    );

    // routing_policy_id is routingPolicy.id; appointmentType is already resolved upstream.
    expect(mockResolveCandidates).toHaveBeenCalledWith({ tenantId: 'TEN1', routingPolicyId: 'rp-7' });
    expect(out).toEqual(roster); // shape passes straight through to resolveAlternate's filter
  });
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

  test('a reassigned result hashes newCoordinatorEmail in the return payload (no PII leak)', async () => {
    mockFindStranded.mockResolvedValue([{ bookingId: 'booking#1' }]);
    mockRemediate.mockResolvedValue({
      outcome: 'reassigned', newResourceId: 'res-diego', newCoordinatorEmail: 'diego@org.com',
    });
    const out = await handler(VALID);
    const row = out.results[0];
    expect(row.newCoordinatorEmail).toBeUndefined();
    expect(row.newCoordinatorEmailHash).toHaveLength(12);
    expect(JSON.stringify(out)).not.toContain('diego@org.com');
    expect(row.newResourceId).toBe('res-diego'); // internal id retained
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
