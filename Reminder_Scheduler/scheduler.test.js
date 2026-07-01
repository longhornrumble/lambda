'use strict';

/**
 * Unit tests for scheduler.js (WS-E-REMIND, FROZEN_CONTRACTS §E1).
 *
 * Covers the locked done-bar:
 *   • commit (E2): N rows (§E1 write shape) + N schedules + attendance; deterministic names;
 *     rule input = { pk, sk, message_id } (EXACT shipped consumer shape);
 *   • RE-BIND on TOKEN-RESCHEDULE re-derives the schedule ◀ the NAMED exit criterion;
 *   • calendar_moved = CANCEL = DELETE (NOT a re-bind) — deletes all, creates nothing;
 *   • delete uses exact reminder_schedule_state SKs when present, deterministic names when not;
 *   • is_synthetic double-gate (STAGING_TEST_MODE && is_synthetic) compresses fire times;
 *   • forward-compatible booking reads (snake_case + camelCase).
 */

const scheduler = require('./scheduler');

// ─── recording deps ─────────────────────────────────────────────────────────────────

function makeDeps({ nowMs = Date.parse('2026-06-10T12:00:00Z'), config = {}, bookingForGet } = {}) {
  const created = [];
  const deleted = [];
  const ddbCalls = [];
  const ddb = {
    send: jest.fn(async (command) => {
      const name = command.constructor.name;
      ddbCalls.push({ name, input: command.input });
      if (name === 'GetCommand') return { Item: bookingForGet || null };
      return {};
    }),
  };
  const deps = {
    ddb,
    now: () => nowMs,
    logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
    config: {
      scheduledMessagesTable: 'picasso-scheduled-messages',
      bookingTable: 'picasso-booking-staging',
      targetArn: 'arn:aws:lambda:us-east-1:111:function:Scheduled_Message_Sender',
      roleArn: 'arn:aws:iam::111:role/picasso-scheduler-exec',
      groupName: 'picasso-scheduling',
      fromNumber: '+15125550000',
      stagingTestMode: false,
      ...config,
    },
    scheduler: {
      createSchedule: jest.fn(async (params) => { created.push(params); }),
      deleteSchedule: jest.fn(async (params) => { deleted.push(params); }),
    },
  };
  return { deps, created, deleted, ddbCalls };
}

const baseBooking = (overrides = {}) => ({
  tenant_id: 'AUS123957',
  booking_id: 'booking#abc123',
  start_at: '2026-06-12T12:00:00Z', // 48h after NOW → t24h + t1h
  end_at: '2026-06-12T12:30:00Z',
  timezone: 'America/Chicago',
  attendee_email: 'vol@example.com',
  attendee_phone: '+15125551234',
  attendee_name: 'Sam Patel',
  coordinator_email: 'coord@example.com',
  status: 'booked',
  ...overrides,
});

const putRows = (ddbCalls) => ddbCalls.filter((c) => c.name === 'PutCommand').map((c) => c.input.Item);

// ─── commit (E2) ──────────────────────────────────────────────────────────────────────

describe('scheduleReminders — commit (E2)', () => {
  test('writes reminder rows + schedules + attendance; deterministic names; exact input shape', async () => {
    const { deps, created, ddbCalls } = makeDeps();
    const state = await scheduler.scheduleReminders({ booking: baseBooking() }, deps);

    expect(state.tiers).toEqual(['t24h', 't1h']);
    expect(state.reminders).toHaveLength(2);
    expect(state.attendance).not.toBeNull();

    // 3 schedules created: 2 reminders + 1 attendance.
    expect(created).toHaveLength(3);
    const names = created.map((c) => c.Name);
    expect(names).toContain('sched-reminder-t24h-booking-abc123'); // '#' sanitised to '-'
    expect(names).toContain('sched-reminder-t1h-booking-abc123');
    expect(names).toContain('sched-attendance-booking-abc123');

    // Every schedule targets the consumer with the EXACT { pk, sk, message_id } input.
    created.forEach((c) => {
      expect(c.Target.Arn).toBe(deps.config.targetArn);
      expect(c.Target.RoleArn).toBe(deps.config.roleArn);
      expect(c.GroupName).toBe('picasso-scheduling');
      expect(c.FlexibleTimeWindow).toEqual({ Mode: 'OFF' });
      expect(c.ActionAfterCompletion).toBe('DELETE');
      const input = JSON.parse(c.Target.Input);
      expect(Object.keys(input).sort()).toEqual(['message_id', 'pk', 'sk']);
      expect(input.pk).toBe('TENANT#AUS123957');
    });
  });

  test('row matches the §E1 write shape', async () => {
    const { deps, ddbCalls } = makeDeps();
    await scheduler.scheduleReminders({ booking: baseBooking() }, deps);
    const rows = putRows(ddbCalls);
    const reminder = rows.find((r) => r.tier === 't24h');
    expect(reminder.pk).toBe('TENANT#AUS123957');
    expect(reminder.sk).toBe('SCHEDULED#2026-06-12T12:00:00Z#booking#abc123#t24h');
    expect(reminder.tenant_id).toBe('AUS123957');
    expect(reminder.channel).toBe('email'); // §E3 floor
    expect(reminder.recipient_phone).toBe('+15125551234'); // E.164 copied from Booking.attendee_phone
    expect(reminder.recipient_email).toBe('vol@example.com');
    expect(reminder.appointment_id).toBe('booking#abc123');
    expect(reminder.message_id).toBe('booking#abc123#t24h');
    expect(reminder.status).toBe('pending');
    expect(reminder.moment).toBe('reminder');
    expect(reminder.timezone).toBe('America/Chicago');
  });

  test('ScheduleExpression is a UTC at() with no millis/Z', async () => {
    const { deps, created } = makeDeps();
    await scheduler.scheduleReminders({ booking: baseBooking() }, deps);
    const t1h = created.find((c) => c.Name === 'sched-reminder-t1h-booking-abc123');
    // start_at 2026-06-12T12:00:00Z minus 1h = 11:00:00.
    expect(t1h.ScheduleExpression).toBe('at(2026-06-12T11:00:00)');
    expect(t1h.ScheduleExpressionTimezone).toBe('UTC');
  });

  test('snapshots the tenant SMS prefs the fire-time §E3 gate needs', async () => {
    const { deps, ddbCalls } = makeDeps();
    await scheduler.scheduleReminders(
      { booking: baseBooking(), tenantPrefs: { notificationPrefs: { sms: true }, sms_quiet_hours: { start: 20, end: 8 } } },
      deps
    );
    const reminder = putRows(ddbCalls).find((r) => r.tier === 't24h');
    expect(reminder.tenant_prefs.notificationPrefs.sms).toBe(true);
    expect(reminder.tenant_prefs.sms_quiet_hours).toEqual({ start: 20, end: 8 });
  });

  test('persists reminder_schedule_state onto the Booking (E6 bookkeeping)', async () => {
    const { deps, ddbCalls } = makeDeps();
    await scheduler.scheduleReminders({ booking: baseBooking() }, deps);
    const upd = ddbCalls.find((c) => c.name === 'UpdateCommand' && c.input.TableName === 'picasso-booking-staging');
    expect(upd).toBeTruthy();
    expect(upd.input.Key).toEqual({ tenantId: 'AUS123957', booking_id: 'booking#abc123' });
    expect(upd.input.ExpressionAttributeValues[':s'].reminders).toHaveLength(2);
  });

  test('forward-compatible: camelCase booking fields are read', async () => {
    const { deps, created } = makeDeps();
    const camel = {
      tenantId: 'AUS123957', bookingId: 'booking#xyz', startAt: '2026-06-12T12:00:00Z',
      endAt: '2026-06-12T12:30:00Z', attendeeEmail: 'v@e.com', coordinatorEmail: 'c@e.com',
    };
    const state = await scheduler.scheduleReminders({ booking: camel }, deps);
    expect(state.tiers).toEqual(['t24h', 't1h']);
    expect(created.some((c) => c.Name === 'sched-reminder-t24h-booking-xyz')).toBe(true);
  });

  test('required-field guards', async () => {
    const { deps } = makeDeps();
    await expect(scheduler.scheduleReminders({ booking: { booking_id: 'b', start_at: 'x' } }, deps))
      .rejects.toThrow(/tenant_id/);
    await expect(scheduler.scheduleReminders({ booking: { tenant_id: 't', start_at: 'x' } }, deps))
      .rejects.toThrow(/booking_id/);
    await expect(scheduler.scheduleReminders({ booking: { tenant_id: 't', booking_id: 'b' } }, deps))
      .rejects.toThrow(/start_at/);
  });
});

// ─── is_synthetic double-gate (§E1) ────────────────────────────────────────────────────

describe('scheduleReminders — is_synthetic time-compression', () => {
  test('STAGING_TEST_MODE && is_synthetic → compressed near-future fire times', async () => {
    const nowMs = Date.parse('2026-06-10T12:00:00Z');
    const { deps, created } = makeDeps({ nowMs, config: { stagingTestMode: true } });
    await scheduler.scheduleReminders({ booking: baseBooking({ is_synthetic: true }) }, deps);
    const t24 = created.find((c) => c.Name === 'sched-reminder-t24h-booking-abc123');
    // compressed to now + 1 min.
    expect(t24.ScheduleExpression).toBe('at(2026-06-10T12:01:00)');
  });

  test('is_synthetic WITHOUT STAGING_TEST_MODE → normal (real) fire times (single-gate is not enough)', async () => {
    const { deps, created } = makeDeps({ config: { stagingTestMode: false } });
    await scheduler.scheduleReminders({ booking: baseBooking({ is_synthetic: true }) }, deps);
    const t1h = created.find((c) => c.Name === 'sched-reminder-t1h-booking-abc123');
    expect(t1h.ScheduleExpression).toBe('at(2026-06-12T11:00:00)'); // NOT compressed
  });
});

// ─── delete (cancel + calendar_moved) ──────────────────────────────────────────────────

describe('deleteReminders — cancel / calendar_moved', () => {
  test('with reminder_schedule_state → deletes the EXACT schedules + rows', async () => {
    const { deps, deleted, ddbCalls } = makeDeps();
    const booking = baseBooking({
      reminder_schedule_state: {
        reminders: [
          { tier: 't24h', scheduleName: 'sched-reminder-t24h-booking-abc123', sk: 'SCHEDULED#x#booking#abc123#t24h' },
          { tier: 't1h', scheduleName: 'sched-reminder-t1h-booking-abc123', sk: 'SCHEDULED#x#booking#abc123#t1h' },
        ],
        attendance: { scheduleName: 'sched-attendance-booking-abc123', sk: 'SCHEDULED#x#booking#abc123#attendance' },
      },
    });
    await scheduler.deleteReminders({ booking }, deps);
    expect(deleted.map((d) => d.Name).sort()).toEqual([
      'sched-attendance-booking-abc123',
      'sched-reminder-t1h-booking-abc123',
      'sched-reminder-t24h-booking-abc123',
    ]);
    const rowDeletes = ddbCalls.filter((c) => c.name === 'DeleteCommand');
    expect(rowDeletes).toHaveLength(3);
  });

  test('without state → deletes by the deterministic names (rows are consumer status-gated)', async () => {
    const { deps, deleted } = makeDeps();
    await scheduler.deleteReminders({ tenantId: 'AUS123957', bookingId: 'booking#abc123' }, deps);
    expect(deleted.map((d) => d.Name).sort()).toEqual([
      'sched-attendance-booking-abc123',
      'sched-reminder-t15m-booking-abc123',
      'sched-reminder-t1h-booking-abc123',
      'sched-reminder-t24h-booking-abc123',
      'sched-reminder-t4h-booking-abc123',
    ]);
  });

  test('calendar_moved is a DELETE, never a re-bind — nothing is created', async () => {
    const { deps, created } = makeDeps();
    // The cal-lifecycle consumer cancels on coordinator move → wires deleteReminders.
    await scheduler.deleteReminders({ booking: baseBooking({ status: 'canceled', cancel_reason: 'coordinator_moved' }) }, deps);
    expect(created).toHaveLength(0);
  });

  test('requires a booking or { tenantId, bookingId }', async () => {
    const { deps } = makeDeps();
    await expect(scheduler.deleteReminders({}, deps)).rejects.toThrow(/requires a booking/);
  });
});

// ─── RE-BIND on token-reschedule — THE NAMED EXIT CRITERION ─────────────────────────────

describe('rebindReminders — TOKEN-RESCHEDULE re-derives the schedule (WS-E-REMIND exit criterion)', () => {
  test('deletes the old schedules+rows and creates fresh ones against the NEW start_at', async () => {
    const nowMs = Date.parse('2026-06-10T12:00:00Z');
    const { deps, created, deleted } = makeDeps({ nowMs });

    // §B9 executeReschedule mutated start_at IN PLACE (same booking_id) and the caller
    // carries the prior reminder_schedule_state on the booking object.
    const rescheduled = baseBooking({
      start_at: '2026-06-20T18:00:00Z', // moved ~10 days out → still {t24h, t1h}
      end_at: '2026-06-20T18:30:00Z',
      reminder_schedule_state: {
        reminders: [
          { tier: 't24h', scheduleName: 'sched-reminder-t24h-booking-abc123', sk: 'SCHEDULED#2026-06-12T12:00:00Z#booking#abc123#t24h' },
          { tier: 't1h', scheduleName: 'sched-reminder-t1h-booking-abc123', sk: 'SCHEDULED#2026-06-12T12:00:00Z#booking#abc123#t1h' },
        ],
        attendance: { scheduleName: 'sched-attendance-booking-abc123', sk: 'SCHEDULED#2026-06-12T12:00:00Z#booking#abc123#attendance' },
      },
    });

    const state = await scheduler.rebindReminders({ booking: rescheduled }, deps);

    // OLD schedules were deleted (the prior bind), then FRESH ones created.
    expect(deleted.map((d) => d.Name)).toEqual(expect.arrayContaining([
      'sched-reminder-t24h-booking-abc123',
      'sched-reminder-t1h-booking-abc123',
      'sched-attendance-booking-abc123',
    ]));
    expect(created).toHaveLength(3);

    // The fresh schedules fire relative to the NEW start_at (2026-06-20T18:00).
    const t24 = created.find((c) => c.Name === 'sched-reminder-t24h-booking-abc123');
    const t1h = created.find((c) => c.Name === 'sched-reminder-t1h-booking-abc123');
    expect(t24.ScheduleExpression).toBe('at(2026-06-19T18:00:00)'); // new start − 24h
    expect(t1h.ScheduleExpression).toBe('at(2026-06-20T17:00:00)'); // new start − 1h

    // The persisted state reflects the NEW SKs (keyed on the new start_at).
    expect(state.reminders[0].sk).toBe('SCHEDULED#2026-06-20T18:00:00Z#booking#abc123#t24h');
  });

  test('re-derives a SMALLER tier set when the reschedule lands closer in', async () => {
    const nowMs = Date.parse('2026-06-10T12:00:00Z');
    const { deps, created } = makeDeps({ nowMs });
    const rescheduled = baseBooking({
      start_at: '2026-06-10T15:00:00Z', // now only 3h out → {t15m}
      end_at: '2026-06-10T15:30:00Z',
      reminder_schedule_state: { reminders: [], attendance: null },
    });
    const state = await scheduler.rebindReminders({ booking: rescheduled }, deps);
    expect(state.tiers).toEqual(['t15m']);
    expect(created.some((c) => c.Name === 'sched-reminder-t15m-booking-abc123')).toBe(true);
  });
});

// ── G1: reminder row enrichment (reschedule/cancel/join links + whenLabel) ─────────────

describe('G1 — reminder row enrichment (action links + whenLabel)', () => {
  // G1: action links are minted by the COMMIT path (BCH buildActionLinks) and PASSED IN —
  // scheduler.js imports no signing SDK. whenLabel is pure (Intl) and always computed.
  const rescheduleUrl = 'https://schedule.myrecruiter.ai/reschedule?t=RTOKEN';
  const cancelUrl = 'https://schedule.myrecruiter.ai/cancel?t=CTOKEN';

  function makeG1Deps(extra = {}) {
    return makeDeps({ ...extra });
  }

  test('G1: reminder rows carry reschedule_url, cancel_url, join_url, when_label when links passed', async () => {
    const { deps, ddbCalls } = makeG1Deps();
    await scheduler.scheduleReminders(
      {
        booking: baseBooking({ join_url: 'https://meet.google.com/abc' }),
        rescheduleUrl,
        cancelUrl,
      },
      deps
    );
    const rows = putRows(ddbCalls).filter((r) => r.tier !== undefined);
    expect(rows.length).toBeGreaterThan(0);
    // Every reminder row carries the passed-in action links + the computed time label.
    rows.forEach((row) => {
      expect(row).toHaveProperty('reschedule_url', rescheduleUrl);
      expect(row).toHaveProperty('cancel_url', cancelUrl);
      expect(row).toHaveProperty('join_url', 'https://meet.google.com/abc');
      expect(row).toHaveProperty('when_label');
      expect(typeof row.when_label).toBe('string');
      expect(row.when_label.length).toBeGreaterThan(0);
    });
  });

  test('G1: no links passed (mint failed / reconciler path) → rows still created, action links omitted, time still shown', async () => {
    const { deps, ddbCalls } = makeG1Deps();
    await scheduler.scheduleReminders(
      { booking: baseBooking({ join_url: 'https://meet.google.com/xyz' }) },
      deps
    );
    const rows = putRows(ddbCalls).filter((r) => r.tier !== undefined);
    expect(rows.length).toBeGreaterThan(0);
    rows.forEach((row) => {
      // No reschedule/cancel links when none were minted/passed (fail-soft by absence).
      expect(row).not.toHaveProperty('reschedule_url');
      expect(row).not.toHaveProperty('cancel_url');
      // when_label is pure (Intl) — always present so the reminder still shows the time.
      expect(row).toHaveProperty('when_label');
      // join still rides the booking row.
      expect(row).toHaveProperty('join_url', 'https://meet.google.com/xyz');
    });
  });

  test('G1: readBooking projects join_url (snake_case AND camelCase)', () => {
    const snakeCase = scheduler.readBooking({ tenant_id: 'T', booking_id: 'B', start_at: 'S', join_url: 'https://join.example' });
    expect(snakeCase.joinUrl).toBe('https://join.example');

    const camelCase = scheduler.readBooking({ tenantId: 'T', bookingId: 'B', startAt: 'S', joinUrl: 'https://join.camel' });
    expect(camelCase.joinUrl).toBe('https://join.camel');

    // Old-shape row (missing join_url) → undefined, no crash.
    const oldShape = scheduler.readBooking({ tenant_id: 'T', booking_id: 'B', start_at: 'S' });
    expect(oldShape.joinUrl).toBeUndefined();
  });

  test('G1: buildReminderRow omits action-link fields when values are empty (old-shape compat)', () => {
    const b = scheduler.readBooking(baseBooking());
    const row = scheduler.buildReminderRow({
      b, tier: 't24h', fireAtMs: Date.now() + 86400000,
      tenantPrefsSnap: { notificationPrefs: { sms: false }, sms_quiet_hours: null },
      config: { fromNumber: '', scheduledMessagesTable: 'x', stagingTestMode: false },
      rescheduleUrl: '', cancelUrl: '', joinUrl: '', whenLabel: '',
    });
    // All falsy values → fields NOT set on the row
    expect(row).not.toHaveProperty('reschedule_url');
    expect(row).not.toHaveProperty('cancel_url');
    expect(row).not.toHaveProperty('join_url');
    expect(row).not.toHaveProperty('when_label');
  });

  test('program_name carries into reminder-row template_vars for the {{programName}} token', () => {
    const rowArgs = {
      tier: 't24h', fireAtMs: Date.now() + 86400000,
      tenantPrefsSnap: { notificationPrefs: { sms: false }, sms_quiet_hours: null },
      config: { fromNumber: '' }, rescheduleUrl: '', cancelUrl: '', joinUrl: '', whenLabel: '',
    };
    const withProgram = scheduler.buildReminderRow({ b: scheduler.readBooking(baseBooking({ program_name: 'Family Support' })), ...rowArgs });
    expect(withProgram.template_vars.program_name).toBe('Family Support');
    // Old-shape booking without program_name → '' (forward-compatible, never a crash).
    const withoutProgram = scheduler.buildReminderRow({ b: scheduler.readBooking(baseBooking()), ...rowArgs });
    expect(withoutProgram.template_vars.program_name).toBe('');
  });

  test('G1: attendance row does NOT get action-link fields (coordinator-facing, not attendee)', async () => {
    const { deps, ddbCalls } = makeG1Deps();
    await scheduler.scheduleReminders(
      { booking: baseBooking(), rescheduleUrl, cancelUrl },
      deps
    );
    const rows = putRows(ddbCalls);
    const attendanceRow = rows.find((r) => r.message_id && r.message_id.endsWith('#attendance'));
    expect(attendanceRow).toBeDefined();
    // attendance rows must NOT have action-link fields (coordinator can't opt out)
    expect(attendanceRow).not.toHaveProperty('reschedule_url');
    expect(attendanceRow).not.toHaveProperty('cancel_url');
    expect(attendanceRow).not.toHaveProperty('join_url');
  });

  test('G1: forward-compatible read — booking rows without join_url field → no crash, no join link', async () => {
    const { deps, ddbCalls } = makeG1Deps();
    // baseBooking() has no join_url field
    await scheduler.scheduleReminders(
      { booking: baseBooking(), rescheduleUrl, cancelUrl },
      deps
    );
    const rows = putRows(ddbCalls).filter((r) => r.tier !== undefined);
    expect(rows.length).toBeGreaterThan(0);
    // join_url omitted from rows (empty from readBooking → buildReminderRow omits it)
    rows.forEach((row) => {
      expect(row).not.toHaveProperty('join_url');
    });
  });
});
