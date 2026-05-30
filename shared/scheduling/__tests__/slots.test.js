'use strict';

/**
 * slots.test.js — WS-C7 unit tests for slots.js (frozen §B3 generateSlots).
 *
 * Pure functions, no mocks. The crux is DST: spring-forward gaps and fall-back
 * ambiguity are tested in BOTH directions against America/Chicago, whose 2026 US
 * transitions are well-defined:
 *   - spring forward: 2026-03-08, 02:00 → 03:00 (02:00–02:59 does NOT exist)
 *   - fall back:      2026-11-01, 02:00 → 01:00 (01:00–01:59 occurs twice)
 *
 * Tests that assert an exact per-day slot set pin `searchDays: 1` so the forward
 * scan (which collects up to 5 chips across days in production) stays on the day
 * under test.
 */

const {
  generateSlots,
  _zoneOffsetMs,
  _zonedWallTimeToUtc,
  _formatLabel,
} = require('../slots');

const CHI = 'America/Chicago';
const NYC = 'America/New_York';
const HOUR = 60 * 60 * 1000;

// availability_windows where every weekday carries the same window(s), so a test
// does not need to know which weekday the scanned start day lands on.
function everyDay(windows) {
  return { sun: windows, mon: windows, tue: windows, wed: windows, thu: windows, fri: windows, sat: windows };
}

// Chicago wall-clock hour ("HH") of an ISO instant — for asserting DST behavior.
function chicagoHour(iso) {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: CHI,
    hourCycle: 'h23',
    hour: '2-digit',
  }).format(new Date(iso));
}

// ─── Happy path / output shape (frozen §B3) ───────────────────────────────────────

describe('generateSlots — output shape', () => {
  const apptType = {
    duration_minutes: 60,
    timezone: CHI,
    availability_windows: everyDay([{ start: '09:00', end: '12:00' }]),
  };

  test('returns the frozen §B3 shape with display-ready label (3–5 chips)', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: apptType,
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z', // 07:00 CDT Monday — before the window
      resourceId: 'coord-1',
    });
    expect(slots.length).toBeGreaterThanOrEqual(3);
    expect(slots.length).toBeLessThanOrEqual(5);
    for (const s of slots) {
      expect(Object.keys(s).sort()).toEqual(['end', 'label', 'resourceId', 'slotId', 'start']);
      expect(s.resourceId).toBe('coord-1');
      expect(s.start).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
      expect(s.end).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
      expect(s.label).toMatch(/^[A-Z][a-z]{2}, [A-Z][a-z]{2} \d{1,2} · \d{1,2}:\d{2} (AM|PM)$/);
      expect(s.slotId).toBe(`coord-1|${s.start}`);
      // duration honored: end − start === 60 min.
      expect(Date.parse(s.end) - Date.parse(s.start)).toBe(60 * 60 * 1000);
    }
  });

  test('resourceId defaults to null when not supplied (frozen 4-key call)', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: apptType,
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
    });
    expect(slots[0].resourceId).toBeNull();
    expect(slots[0].slotId).toBe(`|${slots[0].start}`);
  });
});

// ─── Label format + user-timezone respect (snapshot, §9.3) ─────────────────────────

describe('chip label format', () => {
  // 2026-06-03 18:00Z is a Wednesday; 13:00 CDT / 14:00 EDT.
  const instant = Date.UTC(2026, 5, 3, 18, 0);

  test('exact "Wed, Jun 3 · 1:00 PM" format in the business zone', () => {
    expect(_formatLabel(instant, CHI)).toBe('Wed, Jun 3 · 1:00 PM');
  });

  test('label is rendered in the VOLUNTEER timezone, not the business zone', () => {
    // Same instant, shown to a New York volunteer → one hour later.
    expect(_formatLabel(instant, NYC)).toBe('Wed, Jun 3 · 2:00 PM');
  });
});

// ─── DST spring-forward (gap) — 2026-03-08, America/Chicago ────────────────────────

describe('DST spring-forward (02:00–02:59 does not exist)', () => {
  test('helper: nonexistent wall clock resolves to null', () => {
    expect(_zonedWallTimeToUtc(2026, 3, 8, 2, 0, CHI)).toBeNull();
    expect(_zonedWallTimeToUtc(2026, 3, 8, 2, 30, CHI)).toBeNull();
  });

  test('helper: bracketing real times resolve to the correct instants', () => {
    // 01:00 CST (UTC-6) → 07:00Z ; 03:00 CDT (UTC-5) → 08:00Z
    expect(new Date(_zonedWallTimeToUtc(2026, 3, 8, 1, 0, CHI)).toISOString()).toBe(
      '2026-03-08T07:00:00.000Z'
    );
    expect(new Date(_zonedWallTimeToUtc(2026, 3, 8, 3, 0, CHI)).toISOString()).toBe(
      '2026-03-08T08:00:00.000Z'
    );
  });

  test('generateSlots never offers a slot inside the spring-forward gap', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 30,
        slot_granularity_minutes: 30,
        timezone: CHI,
        availability_windows: everyDay([{ start: '01:00', end: '04:00' }]),
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-03-08T06:00:00Z', // 00:00 CST on the transition day
      searchDays: 1,
    });
    // Candidates 02:00 and 02:30 are skipped → 01:00, 01:30, 03:00, 03:30.
    expect(slots.map((s) => chicagoHour(s.start))).toEqual(['01', '01', '03', '03']);
    expect(slots.some((s) => chicagoHour(s.start) === '02')).toBe(false);
  });
});

// ─── DST fall-back (ambiguity) — 2026-11-01, America/Chicago ───────────────────────

describe('DST fall-back (01:00–01:59 occurs twice)', () => {
  test('helper: ambiguous wall clock resolves to the EARLIER instant', () => {
    // First 01:00 is CDT (UTC-5) → 06:00Z, NOT the second 01:00 CST → 07:00Z.
    expect(new Date(_zonedWallTimeToUtc(2026, 11, 1, 1, 0, CHI)).toISOString()).toBe(
      '2026-11-01T06:00:00.000Z'
    );
  });

  test('generateSlots offers an ambiguous time exactly once (earlier instant)', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 30,
        slot_granularity_minutes: 30,
        timezone: CHI,
        availability_windows: everyDay([{ start: '00:30', end: '02:00' }]),
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-11-01T05:00:00Z', // 00:00 CDT on the transition day
      searchDays: 1,
    });
    // 00:30, 01:00, 01:30 — each once; the 01:00 is the earlier (06:00Z) occurrence.
    expect(slots.map((s) => s.start)).toEqual([
      '2026-11-01T05:30:00.000Z',
      '2026-11-01T06:00:00.000Z',
      '2026-11-01T06:30:00.000Z',
    ]);
    const ones = slots.filter((s) => chicagoHour(s.start) === '01');
    expect(ones.length).toBe(2); // 01:00 and 01:30 — no duplicate of either
    expect(ones.every((s) => !s.start.endsWith('07:00:00.000Z'))).toBe(true);
  });
});

// ─── busyIntervals exclusion + buffer padding ──────────────────────────────────────

describe('busy-interval exclusion and buffer', () => {
  const base = {
    duration_minutes: 60,
    slot_granularity_minutes: 60,
    timezone: CHI,
    availability_windows: everyDay([{ start: '09:00', end: '12:00' }]),
  };
  // On 2026-06-08 (CDT, UTC-5): 09:00→14:00Z, 10:00→15:00Z, 11:00→16:00Z.
  const busy = [{ start: '2026-06-08T15:00:00Z', end: '2026-06-08T16:00:00Z' }]; // 10:00–11:00 CDT

  test('overlapping candidate is excluded (buffer 0)', () => {
    const slots = generateSlots({
      busyIntervals: busy,
      appointmentType: base,
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    expect(slots.map((s) => chicagoHour(s.start))).toEqual(['09', '11']);
  });

  test('buffer pads the busy interval on both sides', () => {
    const slots = generateSlots({
      busyIntervals: busy,
      appointmentType: { ...base, buffer_minutes: 30 },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    // 09:00 (ends 15:00Z, within 30m of busy start) and 11:00 (starts 16:00Z, within
    // 30m of busy end) both fall inside the padded busy window → no slots remain.
    expect(slots).toEqual([]);
  });
});

// ─── Rejected-slot dedup ───────────────────────────────────────────────────────────

describe('alreadyRejected dedup', () => {
  const apptType = {
    duration_minutes: 60,
    slot_granularity_minutes: 60,
    timezone: CHI,
    availability_windows: everyDay([{ start: '09:00', end: '12:00' }]),
  };
  const callArgs = {
    busyIntervals: [],
    appointmentType: apptType,
    userTimeZone: CHI,
    now: '2026-06-08T12:00:00Z',
    searchDays: 1,
    resourceId: 'c1',
  };

  test('a previously-rejected slotId is not re-offered', () => {
    const first = generateSlots({ ...callArgs, alreadyRejected: [] });
    const rejectedId = first[0].slotId;
    const second = generateSlots({ ...callArgs, alreadyRejected: [rejectedId] });
    expect(second.some((s) => s.slotId === rejectedId)).toBe(false);
    expect(second.length).toBe(first.length - 1);
  });

  test('dedup also honors a rejected start ISO (defensive)', () => {
    const first = generateSlots({ ...callArgs, alreadyRejected: [] });
    const rejectedStart = first[0].start;
    const second = generateSlots({ ...callArgs, alreadyRejected: [rejectedStart] });
    expect(second.some((s) => s.start === rejectedStart)).toBe(false);
  });
});

// ─── min_lead_minutes, granularity default, closed days, caps ──────────────────────

describe('window mechanics', () => {
  test('min_lead_minutes pushes the earliest offered start', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 60,
        slot_granularity_minutes: 60,
        min_lead_minutes: 60,
        timezone: CHI,
        availability_windows: everyDay([{ start: '09:00', end: '12:00' }]),
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T13:30:00Z', // 08:30 CDT; +60m lead → earliest 09:30 CDT
      searchDays: 1,
    });
    // 09:00 (14:00Z) is before the lead cutoff (14:30Z) → first offered is 10:00.
    expect(slots.map((s) => chicagoHour(s.start))).toEqual(['10', '11']);
  });

  test('slot_granularity_minutes defaults to duration (back-to-back)', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 90,
        timezone: CHI,
        availability_windows: everyDay([{ start: '09:00', end: '12:00' }]),
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    // 90-min back-to-back in a 3-hour window → 09:00 and 10:30 only.
    expect(slots.map((s) => chicagoHour(s.start))).toEqual(['09', '10']);
    expect(slots.length).toBe(2);
  });

  test('missing weekday and empty-array days are skipped', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 60,
        timezone: CHI,
        availability_windows: {}, // every day closed
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 3,
    });
    expect(slots).toEqual([]);
  });

  test('malformed window strings and inverted ranges are skipped', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 60,
        slot_granularity_minutes: 60,
        timezone: CHI,
        availability_windows: everyDay([
          { start: 'nope', end: '12:00' }, // malformed start
          { start: '13:00', end: '10:00' }, // end <= start
          { start: '99:99', end: '12:00' }, // out-of-range
          { start: '14:00', end: '16:00' }, // the only valid window
        ]),
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    expect(slots.map((s) => chicagoHour(s.start))).toEqual(['14', '15']);
  });

  test('maxSlots caps the result on a busy day', () => {
    const capped = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 30,
        slot_granularity_minutes: 30,
        timezone: CHI,
        availability_windows: everyDay([{ start: '09:00', end: '17:00' }]),
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
      maxSlots: 3,
    });
    expect(capped.length).toBe(3);
  });

  test('the forward scan collects across days up to maxSlots', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 60,
        slot_granularity_minutes: 60,
        timezone: CHI,
        availability_windows: everyDay([{ start: '09:00', end: '10:00' }]), // 1 slot/day
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 14,
    });
    // 1 slot/day × 5 distinct days (maxSlots default).
    expect(slots.length).toBe(5);
    const days = new Set(slots.map((s) => s.start.slice(0, 10)));
    expect(days.size).toBe(5);
  });

  test('a sparse horizon returns fewer than the 3-chip target without throwing', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 60,
        slot_granularity_minutes: 60,
        timezone: CHI,
        availability_windows: everyDay([{ start: '09:00', end: '10:00' }]), // 1 slot/day
      },
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    expect(slots.length).toBe(1);
  });
});

// ─── Cross-timezone slot generation (business ≠ volunteer) ─────────────────────────

describe('cross-timezone generation', () => {
  test('slots computed in the business zone, labelled in the volunteer zone', () => {
    const slots = generateSlots({
      busyIntervals: [],
      appointmentType: {
        duration_minutes: 60,
        slot_granularity_minutes: 60,
        timezone: CHI,
        availability_windows: everyDay([{ start: '09:00', end: '11:00' }]),
      },
      userTimeZone: NYC,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    // 09:00 CDT === 10:00 EDT, 10:00 CDT === 11:00 EDT.
    expect(slots.map((s) => s.label)).toEqual([
      'Mon, Jun 8 · 10:00 AM',
      'Mon, Jun 8 · 11:00 AM',
    ]);
  });
});

// ─── Helper: zoneOffsetMs ──────────────────────────────────────────────────────────

describe('_zoneOffsetMs', () => {
  test('CDT in summer is UTC-5; CST in winter is UTC-6', () => {
    expect(_zoneOffsetMs(Date.UTC(2026, 5, 3, 18, 0), CHI)).toBe(-5 * HOUR);
    expect(_zoneOffsetMs(Date.UTC(2026, 0, 15, 18, 0), CHI)).toBe(-6 * HOUR);
  });
});

// ─── Validation ────────────────────────────────────────────────────────────────────

describe('validation', () => {
  const ok = {
    duration_minutes: 60,
    timezone: CHI,
    availability_windows: everyDay([{ start: '09:00', end: '12:00' }]),
  };
  const callable = (over) => () =>
    generateSlots({
      busyIntervals: [],
      appointmentType: ok,
      userTimeZone: CHI,
      alreadyRejected: [],
      now: '2026-06-08T12:00:00Z',
      ...over,
    });

  test('no args throws (appointmentType required)', () => {
    expect(() => generateSlots()).toThrow(/appointmentType is required/);
  });
  test('missing appointmentType throws', () => {
    expect(callable({ appointmentType: undefined })).toThrow(/appointmentType is required/);
  });
  test('missing userTimeZone throws', () => {
    expect(callable({ userTimeZone: '' })).toThrow(/userTimeZone is required/);
  });
  test('non-positive duration throws', () => {
    expect(() =>
      generateSlots({
        busyIntervals: [],
        appointmentType: { ...ok, duration_minutes: 0 },
        userTimeZone: CHI,
        now: '2026-06-08T12:00:00Z',
      })
    ).toThrow(/duration_minutes must be a positive number/);
  });
  test('missing timezone throws', () => {
    expect(() =>
      generateSlots({
        busyIntervals: [],
        appointmentType: { ...ok, timezone: undefined },
        userTimeZone: CHI,
        now: '2026-06-08T12:00:00Z',
      })
    ).toThrow(/timezone is required/);
  });
  test('missing availability_windows throws', () => {
    expect(() =>
      generateSlots({
        busyIntervals: [],
        appointmentType: { ...ok, availability_windows: undefined },
        userTimeZone: CHI,
        now: '2026-06-08T12:00:00Z',
      })
    ).toThrow(/availability_windows is required/);
  });
  test('invalid IANA timezone throws', () => {
    expect(() =>
      generateSlots({
        busyIntervals: [],
        appointmentType: { ...ok, timezone: 'Not/AZone' },
        userTimeZone: CHI,
        now: '2026-06-08T12:00:00Z',
      })
    ).toThrow(/Invalid IANA timeZone/);
  });
  test('invalid now throws', () => {
    expect(callable({ now: 'not-a-date' })).toThrow(/now must be a valid ISO8601 timestamp/);
  });
  test('non-array busyIntervals and alreadyRejected are tolerated', () => {
    const slots = generateSlots({
      busyIntervals: null,
      appointmentType: ok,
      userTimeZone: CHI,
      alreadyRejected: null,
      now: '2026-06-08T12:00:00Z',
      searchDays: 1,
    });
    expect(slots.length).toBeGreaterThan(0);
  });
});
