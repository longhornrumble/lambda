'use strict';

const { createSyntheticBooking } = require('./synthetic-booking');

const PROPOSE_OK = {
  outcome: 'ok',
  poolSize: 2,
  tieBreaker: 'round_robin',
  roundRobinCursor: { routingPolicyId: 'rp1' },
  slots: [{ slotId: 's1', start: '2026-07-01T15:00:00Z', end: '2026-07-01T15:30:00Z', resourceId: 'coord@example.org' }],
};

function makeDeps(overrides = {}) {
  const invokeBch = jest.fn(async (payload) => {
    if (payload.action === 'scheduling_propose') return PROPOSE_OK;
    return { status: 'BOOKED', bookingId: 'booking#xyz' };
  });
  return {
    invokeBch,
    stampSynthetic: jest.fn().mockResolvedValue(),
    getBooking: jest.fn().mockResolvedValue({ booking_id: 'booking#xyz', coordinator_email: 'coord@example.org' }),
    tenantId: 'TEN-SYNTH',
    appointmentTypeId: 'apt-1',
    ...overrides,
  };
}

describe('synthetic-booking.createSyntheticBooking', () => {
  test('propose → commit → stamp is_synthetic → read back', async () => {
    const deps = makeDeps();
    const res = await createSyntheticBooking({ cyclePrefix: 'cancel' }, deps);

    expect(res).toMatchObject({ tenantId: 'TEN-SYNTH', bookingId: 'booking#xyz', coordinatorId: 'coord@example.org' });

    // propose call
    expect(deps.invokeBch).toHaveBeenNthCalledWith(1, expect.objectContaining({ action: 'scheduling_propose', appointmentTypeId: 'apt-1' }));
    // commit call carries the chip's slot + candidateResourceIds + pool_size
    expect(deps.invokeBch).toHaveBeenNthCalledWith(2, expect.objectContaining({
      tenant_id: 'TEN-SYNTH',
      slot: { start: '2026-07-01T15:00:00Z', end: '2026-07-01T15:30:00Z', candidateResourceIds: ['coord@example.org'] },
      pool_size: 2,
      tie_breaker: 'round_robin',
    }));
    // session id is namespaced + carries the cycle prefix
    expect(deps.invokeBch.mock.calls[1][0].session_id).toMatch(/^synthetic-cancel-/);
    expect(deps.stampSynthetic).toHaveBeenCalledWith('TEN-SYNTH', 'booking#xyz');
  });

  test('throws when config (tenant/appointment-type) is missing', async () => {
    await expect(createSyntheticBooking({ cyclePrefix: 'cancel' }, makeDeps({ tenantId: '' }))).rejects.toThrow(/required/);
  });

  test('throws when propose returns no slots', async () => {
    const deps = makeDeps({
      invokeBch: jest.fn(async (p) => (p.action === 'scheduling_propose' ? { outcome: 'no_availability', slots: [] } : { status: 'BOOKED' })),
    });
    await expect(createSyntheticBooking({ cyclePrefix: 'cancel' }, deps)).rejects.toThrow(/no slots/);
  });

  test('throws when commit does not BOOK', async () => {
    const deps = makeDeps({
      invokeBch: jest.fn(async (p) => (p.action === 'scheduling_propose' ? PROPOSE_OK : { status: 'SLOT_UNAVAILABLE' })),
    });
    await expect(createSyntheticBooking({ cyclePrefix: 'cancel' }, deps)).rejects.toThrow(/did not BOOK/);
    expect(deps.stampSynthetic).not.toHaveBeenCalled();
  });

  test('throws when a propose slot is missing resourceId', async () => {
    const deps = makeDeps({
      invokeBch: jest.fn(async (p) =>
        p.action === 'scheduling_propose'
          ? { outcome: 'ok', poolSize: 1, slots: [{ start: 'a', end: 'b' }] }
          : { status: 'BOOKED' }
      ),
    });
    await expect(createSyntheticBooking({ cyclePrefix: 'cancel' }, deps)).rejects.toThrow(/resourceId/);
  });
});
