'use strict';

const { runCancelCycle } = require('./cancel-cycle');

function baseBooking(overrides = {}) {
  return {
    tenant_id: 'TEN-SYNTH',
    tenantId: 'TEN-SYNTH',
    booking_id: 'booking#abc',
    status: 'booked',
    coordinator_email: 'coord@example.org',
    external_event_id: 'evt-1',
    start_at: '2026-07-01T15:00:00Z',
    ...overrides,
  };
}

function makeDeps(overrides = {}) {
  const emitCycleResult = jest.fn().mockResolvedValue();
  const alert = jest.fn().mockResolvedValue();
  const sleep = jest.fn().mockResolvedValue();
  return {
    createSyntheticBooking: jest.fn().mockResolvedValue({
      tenantId: 'TEN-SYNTH',
      bookingId: 'booking#abc',
      booking: baseBooking(),
    }),
    invokeBch: jest.fn().mockResolvedValue({ outcome: 'deleted' }),
    getBooking: jest.fn().mockResolvedValue(baseBooking({ status: 'canceled' })),
    emitCycleResult,
    alert,
    sleep,
    pollAttempts: 3,
    pollIntervalMs: 0,
    ...overrides,
  };
}

describe('cancel-cycle', () => {
  test('happy path: book → cancel → status flips to canceled → success metric', async () => {
    const deps = makeDeps();
    const res = await runCancelCycle(deps);

    expect(res).toMatchObject({ cycle: 'cancel', success: true, bookingId: 'booking#abc' });
    expect(deps.invokeBch).toHaveBeenCalledWith(
      expect.objectContaining({ action: 'scheduling_mutate', mutation: 'cancel', coordinatorId: 'coord@example.org' })
    );
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cancel', true);
    expect(deps.alert).not.toHaveBeenCalled();
  });

  test('accepts pending_calendar_sync as a valid cancel outcome', async () => {
    const deps = makeDeps({ invokeBch: jest.fn().mockResolvedValue({ outcome: 'pending_calendar_sync' }) });
    const res = await runCancelCycle(deps);
    expect(res.success).toBe(true);
  });

  test('polls until the listener flips status, sleeping between attempts', async () => {
    const getBooking = jest
      .fn()
      .mockResolvedValueOnce(baseBooking({ status: 'booked' }))
      .mockResolvedValueOnce(baseBooking({ status: 'booked' }))
      .mockResolvedValueOnce(baseBooking({ status: 'canceled' }));
    const deps = makeDeps({ getBooking });
    const res = await runCancelCycle(deps);

    expect(res.success).toBe(true);
    expect(getBooking).toHaveBeenCalledTimes(3);
    expect(deps.sleep).toHaveBeenCalledTimes(2);
  });

  test('fails + alerts when status never reaches canceled (listener lag/breakage)', async () => {
    const deps = makeDeps({ getBooking: jest.fn().mockResolvedValue(baseBooking({ status: 'booked' })) });
    const res = await runCancelCycle(deps);

    expect(res).toMatchObject({ cycle: 'cancel', success: false });
    expect(res.error).toMatch(/did not reach canceled/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cancel', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when the cancel mutate returns a bad outcome', async () => {
    const deps = makeDeps({ invokeBch: jest.fn().mockResolvedValue({ outcome: 'failed' }) });
    const res = await runCancelCycle(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/cancel mutate failed/);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when the synthetic booking row lacks coordinator_email/external_event_id', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockResolvedValue({
        tenantId: 'TEN-SYNTH',
        bookingId: 'booking#abc',
        booking: baseBooking({ coordinator_email: null }),
      }),
    });
    const res = await runCancelCycle(deps);
    expect(res.success).toBe(false);
    expect(deps.invokeBch).not.toHaveBeenCalled();
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cancel', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when createSyntheticBooking returns booking:null (read-back miss)', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockResolvedValue({ tenantId: 'TEN-SYNTH', bookingId: 'booking#abc', booking: null }),
    });
    const res = await runCancelCycle(deps);
    expect(res.success).toBe(false);
    expect(deps.invokeBch).not.toHaveBeenCalled();
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cancel', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when external_event_id is missing (coordinator present)', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockResolvedValue({
        tenantId: 'TEN-SYNTH',
        bookingId: 'booking#abc',
        booking: baseBooking({ external_event_id: null }),
      }),
    });
    const res = await runCancelCycle(deps);
    expect(res.success).toBe(false);
    expect(deps.invokeBch).not.toHaveBeenCalled();
  });

  test('fails + alerts when booking creation throws', async () => {
    const deps = makeDeps({ createSyntheticBooking: jest.fn().mockRejectedValue(new Error('no slots')) });
    const res = await runCancelCycle(deps);
    expect(res).toMatchObject({ cycle: 'cancel', success: false });
    expect(res.error).toMatch(/no slots/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cancel', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('pollAttempts:1 → single GET, no sleep, clean failure if not yet canceled', async () => {
    const deps = makeDeps({
      pollAttempts: 1,
      getBooking: jest.fn().mockResolvedValue(baseBooking({ status: 'booked' })),
    });
    const res = await runCancelCycle(deps);
    expect(res.success).toBe(false);
    expect(deps.getBooking).toHaveBeenCalledTimes(1);
    expect(deps.sleep).not.toHaveBeenCalled();
  });
});
