'use strict';

/**
 * C8 feature-gate tests — scheduling is OFF unless feature_flags.scheduling_enabled.
 *
 * Covers both handler routes × both gate states:
 *   - commit route: disabled → { status:'SCHEDULING_DISABLED' }, no commit work
 *   - scheduling_mutate route: disabled → { outcome:'failed', error:'scheduling_disabled' }
 *   - scheduling_mutate route: enabled → delegates to handleSchedulingMutate
 *   - injected.isSchedulingEnabledForTenant override is honored (left side of the `||`)
 *
 * index.test.js mocks the gate to ALWAYS-enabled to exercise the commit logic; this
 * file owns the refuse paths so the gate's failure modes are not happy-path-only.
 */

jest.mock('../../shared/scheduling/featureGate');
jest.mock('../scheduling-mutate', () => ({
  handleSchedulingMutate: jest.fn().mockResolvedValue({ outcome: 'success', booking: { booking_id: 'b1' } }),
}));
// Heavy commit deps are never reached on the disabled path; mock as no-ops for safety.
jest.mock('../../shared/scheduling/pool', () => ({ lockSlot: jest.fn(), recordFreeBusySuccess: jest.fn(), recordFreeBusyFailure: jest.fn() }));

const featureGate = require('../../shared/scheduling/featureGate');
const { handleSchedulingMutate } = require('../scheduling-mutate');
const pool = require('../../shared/scheduling/pool');
const { handler } = require('../index');

function commitEvent(overrides = {}) {
  return {
    tenant_id: 'AUS123957',
    session_id: 'sess-1',
    slot: { start: '2026-06-03T18:00:00.000Z', end: '2026-06-03T18:30:00.000Z', candidateResourceIds: ['res-a'] },
    pool_size: 1,
    appointment_type: { id: 'apt', name: 'Intake', format: 'one_to_one', timezone: 'America/Chicago', cancellation_window_hours: 0 },
    tie_breaker: 'first_available',
    attendee: { first_name: 'Sam', last_name: 'Patel', email: 'sam@example.com' },
    conference_type: 'null',
    coordinator_emails: { 'res-a': 'maya@org.org' },
    org_name: 'Austin Angels',
    ...overrides,
  };
}

const MUTATE_EVENT = {
  action: 'scheduling_mutate',
  mutation: 'cancel',
  tenantId: 'AUS123957',
  coordinatorId: 'maya@org.org',
  booking: { booking_id: 'b1', tenant_id: 'AUS123957', external_event_id: 'evt', coordinator_email: 'maya@org.org' },
};

beforeEach(() => {
  jest.clearAllMocks();
  featureGate.isSchedulingEnabledForTenant.mockResolvedValue(false); // default: disabled
});

describe('commit route gating', () => {
  test('scheduling disabled → SCHEDULING_DISABLED, no commit work', async () => {
    const res = await handler(commitEvent());
    expect(res).toEqual({ status: 'SCHEDULING_DISABLED', reason: 'feature_not_enabled' });
    expect(pool.lockSlot).not.toHaveBeenCalled();
    expect(featureGate.isSchedulingEnabledForTenant).toHaveBeenCalledWith('AUS123957', expect.any(Object));
  });

  test('injected.isSchedulingEnabledForTenant override is honored (and short-circuits the module default)', async () => {
    const injected = { isSchedulingEnabledForTenant: jest.fn().mockResolvedValue(false) };
    const res = await handler(commitEvent(), {}, injected);
    expect(res.status).toBe('SCHEDULING_DISABLED');
    expect(injected.isSchedulingEnabledForTenant).toHaveBeenCalledWith('AUS123957', injected);
    expect(featureGate.isSchedulingEnabledForTenant).not.toHaveBeenCalled();
  });
});

describe('scheduling_mutate route gating', () => {
  test('disabled → failed/scheduling_disabled, executor never invoked', async () => {
    const res = await handler(MUTATE_EVENT);
    expect(res).toEqual({ outcome: 'failed', error: 'scheduling_disabled' });
    expect(handleSchedulingMutate).not.toHaveBeenCalled();
  });

  test('enabled → delegates to handleSchedulingMutate', async () => {
    featureGate.isSchedulingEnabledForTenant.mockResolvedValue(true);
    const injected = { foo: 'bar' };
    const res = await handler(MUTATE_EVENT, {}, injected);
    expect(res).toEqual({ outcome: 'success', booking: { booking_id: 'b1' } });
    expect(handleSchedulingMutate).toHaveBeenCalledWith(MUTATE_EVENT, injected);
  });
});
