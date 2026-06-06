'use strict';

/**
 * Unit tests for reconciler.js (WS-E-REMIND, plan E9; FROZEN_CONTRACTS §E1/§E4).
 *
 * Covers:
 *   • tenant enumeration (event.tenant_ids / SCHEDULING_TENANT_IDS; invalid id rejected);
 *   • bounded GSI query (tenantId-start_at-index, LOOKBACK window, paginated, item_type filter);
 *   • (a) attendance backstop — conditional pending_attendance, idempotent vs WS-E-ATTEND;
 *   • (b) terminal-state schedule cleanup (>7d only);
 *   • (c) D6-outcome-(ii) orphan recovery via injected deps.deleteCalendarEvent.
 */

const { runReconcile, resolveTenantIds, defaultConfig } = require('./reconciler');

const NOW = Date.parse('2026-06-10T12:00:00Z');
const DAY = 24 * 60 * 60 * 1000;
const iso = (ms) => new Date(ms).toISOString();

function makeDeps({ items = [], pages, deleteCalendarEvent } = {}) {
  const ddbCalls = [];
  const queryPages = pages || [{ Items: items }];
  let pageIdx = 0;
  const ddb = {
    send: jest.fn(async (command) => {
      const name = command.constructor.name;
      ddbCalls.push({ name, input: command.input });
      if (name === 'QueryCommand') {
        const page = queryPages[pageIdx] || { Items: [] };
        pageIdx += 1;
        return page;
      }
      if (name === 'GetCommand') return { Item: null };
      return {};
    }),
  };
  const deps = {
    ddb,
    now: () => NOW,
    logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
    config: {
      ...defaultConfig({ ENVIRONMENT: 'staging' }),
    },
    scheduler: {
      createSchedule: jest.fn(async () => {}),
      deleteSchedule: jest.fn(async () => {}),
    },
    deleteCalendarEvent: deleteCalendarEvent || jest.fn(async () => {}),
  };
  return { deps, ddbCalls };
}

const booking = (o = {}) => ({
  item_type: 'booking',
  tenantId: 'AUS123957',
  booking_id: 'booking#1',
  status: 'booked',
  start_at: iso(NOW - 2 * 60 * 60 * 1000),
  end_at: iso(NOW - 60 * 60 * 1000), // ended 1h ago
  coordinator_email: 'coord@example.com',
  ...o,
});

// ─── tenant enumeration ────────────────────────────────────────────────────────────────

describe('resolveTenantIds', () => {
  test('from event.tenant_ids', () => {
    expect(resolveTenantIds({ tenant_ids: ['AUS123957', 'FOS402334'] }, {})).toEqual(['AUS123957', 'FOS402334']);
  });
  test('from SCHEDULING_TENANT_IDS env (comma-separated, trimmed)', () => {
    expect(resolveTenantIds({}, { SCHEDULING_TENANT_IDS: 'AUS123957, FOS402334 ' }))
      .toEqual(['AUS123957', 'FOS402334']);
  });
  test('none provided → throws', () => {
    expect(() => resolveTenantIds({}, {})).toThrow(/No tenant ids/);
  });
  test('invalid tenant id → throws', () => {
    expect(() => resolveTenantIds({ tenant_ids: ['bad id!'] }, {})).toThrow(/Invalid tenant_id/);
  });
});

// ─── bounded query ─────────────────────────────────────────────────────────────────────

describe('queryRecentBookings (bounded GSI scan)', () => {
  test('queries the start_at GSI with a bounded BETWEEN window, paginates, filters item_type', async () => {
    const { deps, ddbCalls } = makeDeps({
      pages: [
        { Items: [booking(), { item_type: 'slot_lock', tenantId: 'AUS123957', booking_id: 'lock#1' }], LastEvaluatedKey: { k: 1 } },
        { Items: [booking({ booking_id: 'booking#2' })] },
      ],
    });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    const q = ddbCalls.filter((c) => c.name === 'QueryCommand');
    expect(q).toHaveLength(2); // paginated
    expect(q[0].input.IndexName).toBe('tenantId-start_at-index');
    expect(q[0].input.KeyConditionExpression).toContain('start_at BETWEEN');
    expect(q[0].input.ExpressionAttributeValues[':lo']).toBe(iso(NOW - 14 * DAY)); // default lookback
    // slot_lock filtered out → only the 2 real bookings scanned.
    expect(summary.bookings_scanned).toBe(2);
  });
});

// ─── (a) attendance backstop ───────────────────────────────────────────────────────────

describe('attendance backstop', () => {
  test('booked + ended >35min ago + no attendance_state → conditional pending_attendance', async () => {
    const { deps, ddbCalls } = makeDeps({ items: [booking()] });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.attendance_flagged).toBe(1);
    const upd = ddbCalls.find((c) => c.name === 'UpdateCommand');
    expect(upd.input.UpdateExpression).toContain('attendance_state = :p');
    expect(upd.input.ConditionExpression).toBe('attribute_not_exists(attendance_state)');
    expect(upd.input.ExpressionAttributeValues[':p']).toBe('pending_attendance');
  });

  test('already has attendance_state → not flagged', async () => {
    const { deps } = makeDeps({ items: [booking({ attendance_state: 'pending_attendance' })] });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.attendance_flagged).toBe(0);
  });

  test('ended <35min ago → not flagged (within grace)', async () => {
    const { deps } = makeDeps({ items: [booking({ end_at: iso(NOW - 10 * 60 * 1000) })] });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.attendance_flagged).toBe(0);
  });

  test('idempotent: ConditionalCheckFailed (WS-E-ATTEND already set it) → not counted', async () => {
    const { deps } = makeDeps({ items: [booking()] });
    deps.ddb.send = jest.fn(async (command) => {
      const name = command.constructor.name;
      if (name === 'QueryCommand') return { Items: [booking()] };
      if (name === 'UpdateCommand') {
        const err = new Error('exists');
        err.name = 'ConditionalCheckFailedException';
        throw err;
      }
      return {};
    });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.attendance_flagged).toBe(0);
    expect(summary.errors).toBe(0); // conditional-fail is NOT an error
  });
});

// ─── (b) terminal cleanup ──────────────────────────────────────────────────────────────

describe('terminal-state schedule cleanup', () => {
  test('terminal + >7d old → schedules swept (deleteReminders)', async () => {
    const old = booking({ status: 'canceled', start_at: iso(NOW - 9 * DAY), end_at: iso(NOW - 9 * DAY) });
    const { deps } = makeDeps({ items: [old] });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.terminal_cleaned).toBe(1);
    expect(deps.scheduler.deleteSchedule).toHaveBeenCalled(); // deterministic-name sweep
  });

  test('terminal but <7d old → not cleaned', async () => {
    const recent = booking({ status: 'completed', start_at: iso(NOW - 2 * DAY), end_at: iso(NOW - 2 * DAY) });
    const { deps } = makeDeps({ items: [recent] });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.terminal_cleaned).toBe(0);
  });

  test('terminal bookings never get the attendance backstop', async () => {
    const old = booking({ status: 'no_show', start_at: iso(NOW - 9 * DAY), end_at: iso(NOW - 9 * DAY) });
    const { deps } = makeDeps({ items: [old] });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.attendance_flagged).toBe(0);
  });
});

// ─── (c) D6-outcome-(ii) orphan recovery ───────────────────────────────────────────────

describe('D6-outcome-(ii) orphan recovery', () => {
  test('pending_calendar_sync + old event id → retry delete + clear flags', async () => {
    const orphan = booking({
      pending_calendar_sync: true,
      rescheduled_old_event_id: 'gcal-old-123',
    });
    const deleteCalendarEvent = jest.fn(async () => {});
    const { deps, ddbCalls } = makeDeps({ items: [orphan], deleteCalendarEvent });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.orphans_recovered).toBe(1);
    expect(deleteCalendarEvent).toHaveBeenCalledWith({
      tenantId: 'AUS123957', coordinatorEmail: 'coord@example.com', eventId: 'gcal-old-123',
    });
    const clear = ddbCalls.find((c) => c.name === 'UpdateCommand' && c.input.UpdateExpression.startsWith('REMOVE'));
    expect(clear.input.UpdateExpression).toContain('pending_calendar_sync');
    expect(clear.input.UpdateExpression).toContain('rescheduled_old_event_id');
  });

  test('calendar delete fails → not counted, flags NOT cleared', async () => {
    const orphan = booking({ pending_calendar_sync: true, rescheduled_old_event_id: 'gcal-old-123' });
    const deleteCalendarEvent = jest.fn(async () => { throw new Error('google 503'); });
    const { deps, ddbCalls } = makeDeps({ items: [orphan], deleteCalendarEvent });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.orphans_recovered).toBe(0);
    expect(ddbCalls.some((c) => c.name === 'UpdateCommand' && c.input.UpdateExpression.startsWith('REMOVE'))).toBe(false);
  });

  test('no deleteCalendarEvent dep wired → skipped gracefully (no throw)', async () => {
    const orphan = booking({ pending_calendar_sync: true, rescheduled_old_event_id: 'gcal-old-123' });
    const { deps } = makeDeps({ items: [orphan] });
    deps.deleteCalendarEvent = undefined;
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.orphans_recovered).toBe(0);
    expect(summary.errors).toBe(0);
  });
});

// ─── resilience ────────────────────────────────────────────────────────────────────────

describe('resilience', () => {
  test('a query failure for one tenant is counted and does not abort the run', async () => {
    const { deps } = makeDeps({ items: [booking()] });
    deps.ddb.send = jest.fn(async (command) => {
      if (command.constructor.name === 'QueryCommand') throw new Error('throttled');
      return {};
    });
    const summary = await runReconcile({ tenantIds: ['AUS123957'] }, deps);
    expect(summary.errors).toBe(1);
    expect(summary.bookings_scanned).toBe(0);
  });
});
