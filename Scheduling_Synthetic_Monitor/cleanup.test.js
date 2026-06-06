'use strict';

const { runCleanup } = require('./cleanup');

const NOW = Date.parse('2026-07-10T00:00:00Z');

function makeDeps(overrides = {}) {
  return {
    querySyntheticOlderThan: jest.fn().mockResolvedValue([
      { tenantId: 'TEN-SYNTH', booking_id: 'booking#1', status: 'canceled' },
      { tenantId: 'TEN-SYNTH', booking_id: 'booking#2', status: 'canceled' },
    ]),
    deleteBooking: jest.fn().mockResolvedValue(),
    emitCycleResult: jest.fn().mockResolvedValue(),
    alert: jest.fn().mockResolvedValue(),
    tenantId: 'TEN-SYNTH',
    nowMs: NOW,
    ...overrides,
  };
}

describe('cleanup (nightly synthetic hygiene, §5.1)', () => {
  test('deletes every stale synthetic booking and reports the count', async () => {
    const deps = makeDeps();
    const res = await runCleanup(deps);

    expect(res).toMatchObject({ cycle: 'cleanup', success: true, deleted: 2, stragglers: 0 });
    expect(deps.deleteBooking).toHaveBeenCalledTimes(2);
    expect(deps.deleteBooking).toHaveBeenCalledWith('TEN-SYNTH', 'booking#1');
    expect(deps.deleteBooking).toHaveBeenCalledWith('TEN-SYNTH', 'booking#2');
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cleanup', true);
    expect(deps.alert).not.toHaveBeenCalled();
  });

  test('counts + alerts on non-canceled stragglers (possible orphaned calendar events)', async () => {
    const deps = makeDeps({
      querySyntheticOlderThan: jest.fn().mockResolvedValue([
        { tenantId: 'TEN-SYNTH', booking_id: 'booking#1', status: 'canceled' },
        { tenantId: 'TEN-SYNTH', booking_id: 'booking#2', status: 'booked' },
      ]),
    });
    const res = await runCleanup(deps);
    expect(res).toMatchObject({ success: true, deleted: 2, stragglers: 1 });
    expect(deps.alert).toHaveBeenCalledWith(expect.stringMatching(/non-canceled/), expect.objectContaining({ stragglers: 1 }));
  });

  test('partial delete failure mid-loop → cycle fails + alerts (one already deleted)', async () => {
    const deleteBooking = jest
      .fn()
      .mockResolvedValueOnce()
      .mockRejectedValueOnce(new Error('ConditionalCheckFailed'));
    const deps = makeDeps({ deleteBooking });
    const res = await runCleanup(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/ConditionalCheckFailed/);
    expect(deleteBooking).toHaveBeenCalledTimes(2);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cleanup', false);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('computes a 7-day cutoff by default (ISO8601, lexicographic-safe)', async () => {
    const deps = makeDeps();
    await runCleanup(deps);
    const cutoff = deps.querySyntheticOlderThan.mock.calls[0][1];
    expect(cutoff).toBe('2026-07-03T00:00:00.000Z'); // NOW - 7d
  });

  test('honors a custom retention window', async () => {
    const deps = makeDeps({ retentionDays: 1 });
    await runCleanup(deps);
    expect(deps.querySyntheticOlderThan.mock.calls[0][1]).toBe('2026-07-09T00:00:00.000Z');
  });

  test('no stale rows → deletes nothing, still succeeds', async () => {
    const deps = makeDeps({ querySyntheticOlderThan: jest.fn().mockResolvedValue([]) });
    const res = await runCleanup(deps);
    expect(res).toMatchObject({ success: true, deleted: 0 });
    expect(deps.deleteBooking).not.toHaveBeenCalled();
  });

  test('refuses to run without a synthetic tenant id', async () => {
    const deps = makeDeps({ tenantId: '' });
    const res = await runCleanup(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/SYNTHETIC_TENANT_ID is required/);
    expect(deps.querySyntheticOlderThan).not.toHaveBeenCalled();
  });

  test('fails + alerts when the query throws', async () => {
    const deps = makeDeps({ querySyntheticOlderThan: jest.fn().mockRejectedValue(new Error('ddb down')) });
    const res = await runCleanup(deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/ddb down/);
    expect(deps.emitCycleResult).toHaveBeenCalledWith('cleanup', false);
    expect(deps.alert).toHaveBeenCalled();
  });
});
