'use strict';

const { runDispositionCycle } = require('./disposition-cycle');

const TENANT = 'TEN-SYNTH';
const BOOKING_ID = 'booking#disp-test';

function baseBooking(overrides = {}) {
  return {
    tenant_id: TENANT,
    tenantId: TENANT,
    booking_id: BOOKING_ID,
    status: 'booked',
    coordinator_email: 'coord@example.org',
    external_event_id: 'evt-disp-1',
    start_at: '2026-05-01T14:00:00Z', // past
    end_at: '2026-05-01T14:30:00Z',   // past — eligible for attendance_check
    attendance_state: null,
    is_synthetic: true,
    ...overrides,
  };
}

// Row state after attendance_check has set the non-key attribute.
function pendingAttendanceRow(overrides = {}) {
  return baseBooking({ attendance_state: 'pending_attendance', ...overrides });
}

// Row state after applyDisposition(no_show).
function noShowRow(overrides = {}) {
  return baseBooking({ status: 'no_show', attendance_state: 'resolved', ...overrides });
}

function makeDeps(overrides = {}) {
  const emitCycleResult = jest.fn().mockResolvedValue();
  const alert = jest.fn().mockResolvedValue();
  const sleep = jest.fn().mockResolvedValue();

  return {
    createSyntheticBooking: jest.fn().mockResolvedValue({
      tenantId: TENANT,
      bookingId: BOOKING_ID,
      booking: baseBooking(),
    }),
    invokeAttend: jest.fn().mockResolvedValue({ action: 'attendance_check', outcome: 'pending_attendance_set' }),
    getBooking: jest
      .fn()
      // first call: assert pending_attendance on the row
      .mockResolvedValueOnce(pendingAttendanceRow())
      // subsequent calls: the poll after disposition resolves immediately
      .mockResolvedValue(noShowRow()),
    applyDisposition: jest
      .fn()
      // first call: the real no_show transition
      .mockResolvedValueOnce({ outcome: 'no_show', transitioned: true, status: 'no_show' })
      // second call: the idempotency check
      .mockResolvedValueOnce({ outcome: 'already_resolved', transitioned: false }),
    emitCycleResult,
    alert,
    sleep,
    pollAttempts: 3,
    pollIntervalMs: 0,
    ...overrides,
  };
}

describe('disposition-cycle — happy path', () => {
  test('full cycle: book → attend check → pending_attendance → no_show → idempotency ok → success metric', async () => {
    const deps = makeDeps();
    const res = await runDispositionCycle(deps);

    expect(res).toMatchObject({ cycle: 'disposition', success: true, bookingId: BOOKING_ID });

    // Step 2: attendance_check invoked with the right shape
    expect(deps.invokeAttend).toHaveBeenCalledWith(
      expect.objectContaining({
        action: 'attendance_check',
        tenantId: TENANT,
        booking_id: BOOKING_ID,
      })
    );

    // Step 4: first applyDisposition call is the real no_show transition
    expect(deps.applyDisposition).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        tenantId: TENANT,
        bookingId: BOOKING_ID,
        purpose: 'no_show',
      })
    );

    // Step 6: second applyDisposition call is the idempotency check
    expect(deps.applyDisposition).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        tenantId: TENANT,
        bookingId: BOOKING_ID,
        purpose: 'no_show',
      })
    );
    expect(deps.applyDisposition).toHaveBeenCalledTimes(2);

    // Metrics + no alert
    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', true);
    expect(deps.alert).not.toHaveBeenCalled();
  });

  test('accepts skipped_already_marked from attend handler (re-fire idempotency)', async () => {
    const deps = makeDeps({
      invokeAttend: jest.fn().mockResolvedValue({
        action: 'attendance_check',
        outcome: 'skipped_already_marked',
      }),
    });
    const res = await runDispositionCycle(deps);
    expect(res.success).toBe(true);
  });

  test('polls the booking row until status + attendance_state match (eventual consistency)', async () => {
    const notYetRow = noShowRow({ status: 'booked', attendance_state: 'pending_attendance' });
    const getBooking = jest
      .fn()
      // Step 3: pending_attendance assertion (first read)
      .mockResolvedValueOnce(pendingAttendanceRow())
      // Step 5 poll: first two reads are still transitioning
      .mockResolvedValueOnce(notYetRow)
      .mockResolvedValueOnce(notYetRow)
      // Step 5 poll: third read is terminal
      .mockResolvedValueOnce(noShowRow());

    const deps = makeDeps({ getBooking, pollAttempts: 5 });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(true);
    // 1 (pending_attendance assert) + 3 (poll iterations) = 4
    expect(getBooking).toHaveBeenCalledTimes(4);
    // sleep is called between each poll attempt (but not the last one that matched)
    expect(deps.sleep).toHaveBeenCalledTimes(2);
  });
});

describe('disposition-cycle — failure modes', () => {
  test('fails when createSyntheticBooking throws (no slots)', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockRejectedValue(new Error('no slots')),
    });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/no slots/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when createSyntheticBooking returns booking:null (read-back miss)', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest
        .fn()
        .mockResolvedValue({ tenantId: TENANT, bookingId: BOOKING_ID, booking: null }),
    });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/read-back returned null/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', false);
    expect(deps.alert).toHaveBeenCalled();
    // attend_check must not be invoked if booking is null
    expect(deps.invokeAttend).not.toHaveBeenCalled();
  });

  test('fails when invokeAttend throws (FunctionError)', async () => {
    const deps = makeDeps({
      invokeAttend: jest.fn().mockRejectedValue(new Error('FunctionError (Unhandled)')),
    });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/FunctionError/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when attendance_check returns an unexpected outcome', async () => {
    const deps = makeDeps({
      invokeAttend: jest.fn().mockResolvedValue({ action: 'attendance_check', outcome: 'booking_not_found' }),
    });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/booking_not_found/);
    expect(deps.applyDisposition).not.toHaveBeenCalled();
  });

  test('fails when row does not have attendance_state=pending_attendance after check', async () => {
    // Step 3 read-back: attendance_state is still null (write missed)
    const getBooking = jest
      .fn()
      .mockResolvedValueOnce(baseBooking({ attendance_state: null }))
      .mockResolvedValue(noShowRow());

    const deps = makeDeps({ getBooking });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/pending_attendance/);
    expect(deps.applyDisposition).not.toHaveBeenCalled();
  });

  test('fails when row status is not booked before disposition (already terminal)', async () => {
    const getBooking = jest
      .fn()
      .mockResolvedValueOnce(pendingAttendanceRow({ status: 'canceled' }))
      .mockResolvedValue(noShowRow());

    const deps = makeDeps({ getBooking });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/status='booked' before disposition/);
    expect(deps.applyDisposition).not.toHaveBeenCalled();
  });

  test('fails when applyDisposition returns unexpected outcome (not no_show)', async () => {
    const deps = makeDeps({
      applyDisposition: jest
        .fn()
        .mockResolvedValue({ outcome: 'completed', transitioned: true }),
    });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/completed/);
    expect(res.error).toMatch(/expected no_show/);
  });

  test('fails when applyDisposition throws', async () => {
    const deps = makeDeps({
      applyDisposition: jest
        .fn()
        .mockRejectedValue(new Error('DDB write failed: throttled')),
    });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/throttled/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when row never reaches no_show+resolved within poll window', async () => {
    const getBooking = jest
      .fn()
      // Step 3: pending_attendance assertion passes
      .mockResolvedValueOnce(pendingAttendanceRow())
      // Step 5 poll: status never transitions
      .mockResolvedValue(pendingAttendanceRow({ status: 'booked' }));

    const deps = makeDeps({ getBooking, pollAttempts: 2 });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/no_show.*resolved.*within 2 polls/);
    // idempotency step must not run if the row never resolved
    expect(deps.applyDisposition).toHaveBeenCalledTimes(1);
  });

  test('fails when idempotency check returns an unexpected outcome', async () => {
    const applyDisposition = jest
      .fn()
      .mockResolvedValueOnce({ outcome: 'no_show', transitioned: true })
      // second call: returns something unexpected instead of already_resolved
      .mockResolvedValueOnce({ outcome: 'no_show', transitioned: true });

    const deps = makeDeps({ applyDisposition });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/already_resolved/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', false);
  });

  test('pollAttempts:1 → single poll, fails cleanly if row not resolved', async () => {
    const getBooking = jest
      .fn()
      .mockResolvedValueOnce(pendingAttendanceRow())   // step 3
      .mockResolvedValueOnce(pendingAttendanceRow()); // step 5 poll (1 attempt)

    const deps = makeDeps({ getBooking, pollAttempts: 1 });
    const res = await runDispositionCycle(deps);

    expect(res.success).toBe(false);
    // 1 read for pending_attendance assertion + 1 read for the poll
    expect(getBooking).toHaveBeenCalledTimes(2);
    expect(deps.sleep).not.toHaveBeenCalled();
  });

  test('emits CycleFailure metric + alerts on any failure', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockRejectedValue(new Error('boom')),
    });
    await runDispositionCycle(deps);

    expect(deps.emitCycleResult).toHaveBeenCalledWith('disposition', false);
    expect(deps.alert).toHaveBeenCalledWith(
      expect.stringContaining('disposition cycle FAILED'),
      expect.objectContaining({ error: 'boom' })
    );
  });
});

describe('disposition-cycle — edge cases', () => {
  test('cycle returns bookingId even on failure (for debugging)', async () => {
    const deps = makeDeps({
      invokeAttend: jest.fn().mockRejectedValue(new Error('network error')),
    });
    const res = await runDispositionCycle(deps);

    expect(res.bookingId).toBe(BOOKING_ID);
    expect(res.success).toBe(false);
  });

  test('alert payload includes tenantId, bookingId, and error', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockRejectedValue(new Error('no slots available')),
    });
    await runDispositionCycle(deps);

    expect(deps.alert).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ error: 'no slots available' })
    );
  });
});
