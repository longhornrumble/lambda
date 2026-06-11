'use strict';

const { runReminderCycle } = require('./reminder-cycle');

function reminderRow(status, tier = 't24h', overrides = {}) {
  return {
    moment: 'reminder',
    tier,
    status,
    attendance_check: false,
    sk: `SCHEDULED#x#booking#abc#${tier}`,
    message_id: `booking#abc#${tier}`,
    channel: 'email',
    fire_at: '2026-06-30T15:00:00Z',
    ...overrides,
  };
}
function attendanceRow(status = 'pending') {
  return { moment: 'reminder', attendance_check: true, tier: null, status, sk: 'SCHEDULED#x#booking#abc#attendance' };
}

function makeDeps(overrides = {}) {
  return {
    createSyntheticBooking: jest.fn().mockResolvedValue({
      tenantId: 'TEN-SYNTH',
      bookingId: 'booking#abc',
      booking: { coordinator_email: 'coord@example.org', external_event_id: 'evt-1' },
    }),
    // default: initial assert sees a pending reminder, first poll sees it sent.
    queryByAppointment: jest
      .fn()
      .mockResolvedValueOnce([reminderRow('pending')])
      .mockResolvedValueOnce([reminderRow('sent')]),
    invokeBch: jest.fn().mockResolvedValue({ outcome: 'deleted' }),
    emitCycleResult: jest.fn().mockResolvedValue(),
    alert: jest.fn().mockResolvedValue(),
    sleep: jest.fn().mockResolvedValue(),
    pollAttempts: 5,
    pollIntervalMs: 0,
    ...overrides,
  };
}

describe('reminder-cycle', () => {
  test('happy path: book → rows scheduled → reminder flips to sent → success + cleanup cancel', async () => {
    const deps = makeDeps();
    const res = await runReminderCycle(deps);

    expect(res).toMatchObject({ cycle: 'reminder', success: true, bookingId: 'booking#abc', firedTier: 't24h' });
    expect(deps.createSyntheticBooking).toHaveBeenCalledWith({ cyclePrefix: 'reminder' }, deps);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('reminder', true);
    expect(deps.alert).not.toHaveBeenCalled();
    // best-effort cleanup cancels the synthetic booking via the Tier-2 executor — the full
    // arg set (tenantId + booking) is asserted: BCH's cancel route needs the booking object.
    expect(deps.invokeBch).toHaveBeenCalledWith(
      expect.objectContaining({
        action: 'scheduling_mutate',
        mutation: 'cancel',
        tenantId: 'TEN-SYNTH',
        coordinatorId: 'coord@example.org',
        bookingId: 'booking#abc',
        booking: expect.objectContaining({ coordinator_email: 'coord@example.org' }),
      })
    );
  });

  test('a reminder row already sent on the first read → immediate success (fast path)', async () => {
    const deps = makeDeps({ queryByAppointment: jest.fn().mockResolvedValue([reminderRow('sent')]) });
    const res = await runReminderCycle(deps);
    expect(res).toMatchObject({ success: true, firedTier: 't24h' });
    expect(deps.sleep).not.toHaveBeenCalled(); // found on poll 0, never sleeps
  });

  test('polls until a reminder row flips to sent, sleeping between attempts', async () => {
    const queryByAppointment = jest
      .fn()
      .mockResolvedValueOnce([reminderRow('pending')]) // initial assert
      .mockResolvedValueOnce([reminderRow('pending')]) // poll 0
      .mockResolvedValueOnce([reminderRow('pending')]) // poll 1
      .mockResolvedValueOnce([reminderRow('sent')]); // poll 2
    const deps = makeDeps({ queryByAppointment });
    const res = await runReminderCycle(deps);

    expect(res.success).toBe(true);
    expect(queryByAppointment).toHaveBeenCalledTimes(4); // 1 initial + 3 polls
    expect(deps.sleep).toHaveBeenCalledTimes(2); // between poll 0→1 and 1→2
  });

  test('fails + alerts when no reminder dispatches within the poll window (stuck pending)', async () => {
    const queryByAppointment = jest.fn().mockResolvedValue([reminderRow('pending')]);
    const deps = makeDeps({ queryByAppointment, pollAttempts: 3 });
    const res = await runReminderCycle(deps);

    expect(res).toMatchObject({ cycle: 'reminder', success: false });
    expect(res.error).toMatch(/no reminder dispatched/);
    expect(res.error).toMatch(/EventBridge\/Sender lag/); // distinguished from a terminal flip
    expect(deps.emitCycleResult).toHaveBeenCalledWith('reminder', false);
    expect(deps.alert).toHaveBeenCalled();
    expect(deps.invokeBch).toHaveBeenCalled(); // cleanup still attempted (booking exists)
    // last poll (i=2) does not sleep — 3 attempts → 2 sleeps.
    expect(deps.sleep).toHaveBeenCalledTimes(2);
  });

  test('a reminder that reaches terminal failed → fail, distinguished from EventBridge lag', async () => {
    const deps = makeDeps({ queryByAppointment: jest.fn().mockResolvedValue([reminderRow('failed')]), pollAttempts: 2 });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/terminal status 'failed'/);
    expect(res.error).toMatch(/Sender ran but did not send/);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('a reminder that reaches terminal suppressed → fail, distinguished from EventBridge lag', async () => {
    const deps = makeDeps({ queryByAppointment: jest.fn().mockResolvedValue([reminderRow('suppressed')]), pollAttempts: 2 });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/terminal status 'suppressed'/);
  });

  test('a later tier still flips to sent after an earlier tier failed → success', async () => {
    // poll sees t24h failed, then on the next poll t1h is sent → success (ANY tier sent).
    const queryByAppointment = jest
      .fn()
      .mockResolvedValueOnce([reminderRow('failed', 't24h'), reminderRow('pending', 't1h')]) // initial
      .mockResolvedValueOnce([reminderRow('failed', 't24h'), reminderRow('pending', 't1h')]) // poll 0
      .mockResolvedValueOnce([reminderRow('failed', 't24h'), reminderRow('sent', 't1h')]); // poll 1
    const deps = makeDeps({ queryByAppointment, pollAttempts: 5 });
    const res = await runReminderCycle(deps);
    expect(res).toMatchObject({ success: true, firedTier: 't1h' });
  });

  test('fails when the scheduler created no cadence-reminder rows (compression off / broken)', async () => {
    const deps = makeDeps({ queryByAppointment: jest.fn().mockResolvedValue([]) });
    const res = await runReminderCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/no cadence-reminder rows scheduled/);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('attendance-only rows do NOT count as a cadence reminder', async () => {
    const deps = makeDeps({ queryByAppointment: jest.fn().mockResolvedValue([attendanceRow('pending')]) });
    const res = await runReminderCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/no cadence-reminder rows scheduled/);
  });

  test('a sent attendance row does NOT satisfy the proof (must be a cadence reminder)', async () => {
    // both queries return a sent attendance row + a stuck-pending reminder → never green.
    const rows = [attendanceRow('sent'), reminderRow('pending', 't1h')];
    const deps = makeDeps({ queryByAppointment: jest.fn().mockResolvedValue(rows), pollAttempts: 2 });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/no reminder dispatched/);
  });

  test('fails + alerts when booking creation throws — no cleanup invoke (no booking)', async () => {
    const deps = makeDeps({ createSyntheticBooking: jest.fn().mockRejectedValue(new Error('no slots')) });
    const res = await runReminderCycle(deps);

    expect(res).toMatchObject({ cycle: 'reminder', success: false });
    expect(res.error).toMatch(/no slots/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('reminder', false);
    expect(deps.queryByAppointment).not.toHaveBeenCalled();
    expect(deps.invokeBch).not.toHaveBeenCalled();
  });

  test('fails when the booking row lacks coordinator_email — no query, no cleanup', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockResolvedValue({
        tenantId: 'TEN-SYNTH',
        bookingId: 'booking#abc',
        booking: { coordinator_email: null },
      }),
    });
    const res = await runReminderCycle(deps);

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/coordinator_email/);
    expect(deps.queryByAppointment).not.toHaveBeenCalled();
    expect(deps.invokeBch).not.toHaveBeenCalled();
  });

  test('booking:null read-back miss → fail, no cleanup', async () => {
    const deps = makeDeps({
      createSyntheticBooking: jest.fn().mockResolvedValue({ tenantId: 'TEN-SYNTH', bookingId: 'booking#abc', booking: null }),
    });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/coordinator_email/);
    expect(deps.invokeBch).not.toHaveBeenCalled();
  });

  test('a cleanup-cancel failure does NOT flip a successful result (best-effort finally)', async () => {
    const deps = makeDeps({ invokeBch: jest.fn().mockRejectedValue(new Error('cancel boom')) });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(true);
    expect(res.firedTier).toBe('t24h');
  });

  test('uses the real defaultSleep when none is injected (covers the sleep helper)', async () => {
    const queryByAppointment = jest
      .fn()
      .mockResolvedValueOnce([reminderRow('pending')]) // initial
      .mockResolvedValueOnce([reminderRow('pending')]) // poll 0 → sleeps via real defaultSleep(0)
      .mockResolvedValueOnce([reminderRow('sent')]); // poll 1 → success
    const deps = makeDeps({ queryByAppointment, pollIntervalMs: 0, sleep: undefined });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(true);
  });

  test('pollAttempts:1 → single poll, no sleep, clean failure if not yet sent', async () => {
    const deps = makeDeps({
      pollAttempts: 1,
      queryByAppointment: jest
        .fn()
        .mockResolvedValueOnce([reminderRow('pending')]) // initial
        .mockResolvedValueOnce([reminderRow('pending')]), // single poll
    });
    const res = await runReminderCycle(deps);
    expect(res.success).toBe(false);
    expect(deps.sleep).not.toHaveBeenCalled();
  });
});
