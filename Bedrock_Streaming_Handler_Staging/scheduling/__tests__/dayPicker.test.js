'use strict';

/**
 * dayPicker.js — Tests for fix items #3, #8, #9 (audit fixes for PR lambda#293).
 *
 * #3: dateWindowForDay — calendar-valid date validation (round-trip check + year bounds).
 *     Kills 2026-02-31→Mar 3, 2026-13-01, 2026-00-01 and bounds year to [2020, 2100].
 * #8: buildDayStrip — maxAdvanceDays clamped to Math.max(1, ...) (negative/zero/fractional
 *     config must not produce an empty strip).
 * #9: buildDayStrip — "tomorrow" anchored in user_time_zone (Intl-based, no tz lib).
 *     A Pacific user at 11 PM UTC must see their local tomorrow first.
 */

const {
  buildDayStrip,
  dateWindowForDay,
  _formatDayLabel,
} = require('../dayPicker');

// ─── #3: dateWindowForDay — calendar-valid date validation ───────────────────────────

describe('dateWindowForDay — calendar-valid civil date validation (fix #3)', () => {
  // Valid dates must still work (regression guard).
  test('valid date: returns correct UTC window', () => {
    const { startISO, endISO } = dateWindowForDay('2026-06-15');
    expect(startISO).toBe('2026-06-15T00:00:00.000Z');
    expect(endISO).toBe('2026-06-16T00:00:00.000Z');
  });

  test('valid date at Feb end of non-leap year: 2026-02-28 → correct window', () => {
    const { startISO } = dateWindowForDay('2026-02-28');
    expect(startISO).toBe('2026-02-28T00:00:00.000Z');
  });

  test('valid date at Feb end of leap year: 2028-02-29 → accepted', () => {
    const { startISO } = dateWindowForDay('2028-02-29');
    expect(startISO).toBe('2028-02-29T00:00:00.000Z');
  });

  // The key regression cases.
  test('2026-02-31 (rolls to Mar 3 without round-trip check) → throws', () => {
    expect(() => dateWindowForDay('2026-02-31')).toThrow(/invalid civil date/);
  });

  test('2026-13-01 (invalid month 13) → throws', () => {
    expect(() => dateWindowForDay('2026-13-01')).toThrow(/invalid civil date/);
  });

  test('2026-00-01 (month 0 rolls back to Dec of prior year) → throws', () => {
    expect(() => dateWindowForDay('2026-00-01')).toThrow(/invalid civil date/);
  });

  test('9999-12-31 (year out of [2020, 2100] bound) → throws', () => {
    expect(() => dateWindowForDay('9999-12-31')).toThrow(/year out of range/);
  });

  test('2019-06-01 (year < 2020) → throws', () => {
    expect(() => dateWindowForDay('2019-06-01')).toThrow(/year out of range/);
  });

  test('2101-01-01 (year > 2100) → throws', () => {
    expect(() => dateWindowForDay('2101-01-01')).toThrow(/year out of range/);
  });

  test('badly formatted string → throws (regex guard)', () => {
    expect(() => dateWindowForDay('not-a-date')).toThrow(/invalid date/);
    expect(() => dateWindowForDay('06-15-2026')).toThrow(/invalid date/);
  });
});

// ─── #8: buildDayStrip — maxAdvanceDays clamped (negative/zero/fractional) ──────────

describe('buildDayStrip — maxAdvanceDays clamped to ≥ 1 (fix #8)', () => {
  const TZ = 'America/Chicago';

  test('maxAdvanceDays=0 → at least 1 day returned (clamp to 1)', () => {
    const days = buildDayStrip({ userTimeZone: TZ, maxAdvanceDays: 0 });
    expect(days.length).toBeGreaterThanOrEqual(1);
  });

  test('maxAdvanceDays=-10 → at least 1 day returned (clamp to 1)', () => {
    const days = buildDayStrip({ userTimeZone: TZ, maxAdvanceDays: -10 });
    expect(days.length).toBeGreaterThanOrEqual(1);
  });

  test('maxAdvanceDays=0.5 (fractional) → at least 1 day returned (clamp to 1)', () => {
    const days = buildDayStrip({ userTimeZone: TZ, maxAdvanceDays: 0.5 });
    expect(days.length).toBeGreaterThanOrEqual(1);
  });

  test('normal maxAdvanceDays=60 returns up to 7 days', () => {
    const days = buildDayStrip({ userTimeZone: TZ, maxAdvanceDays: 60 });
    expect(days.length).toBe(7);
  });

  test('each returned day has a valid YYYY-MM-DD date and non-empty label', () => {
    const days = buildDayStrip({ userTimeZone: TZ, maxAdvanceDays: 60 });
    for (const d of days) {
      expect(d.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(typeof d.label).toBe('string');
      expect(d.label.length).toBeGreaterThan(0);
    }
  });
});

// ─── #9: buildDayStrip — "tomorrow" anchored in user_time_zone ───────────────────────

describe('buildDayStrip — tomorrow anchored in user local time (fix #9)', () => {
  // Scenario: it is 2026-06-14 at 23:00 UTC = 2026-06-14 at 16:00 Pacific (PDT, UTC-7).
  // In UTC, tomorrow would be "Jun 15".
  // In Pacific time, today is still "Jun 14", so tomorrow is also "Jun 15".
  // BUT: at 2026-06-15 04:00 UTC = 2026-06-14 21:00 Pacific, today in Pacific is
  // "Jun 14" → tomorrow in Pacific is "Jun 15". UTC already says "Jun 15".
  //
  // The key test case: 2026-06-15 at 06:00 UTC = 2026-06-14 at 23:00 Pacific.
  // UTC says "today is Jun 15" → UTC tomorrow would be "Jun 16".
  // Pacific says "today is Jun 14" → Pacific tomorrow is "Jun 15".
  // A UTC-anchored strip (old code) would START with Jun 16, skipping Jun 15 entirely
  // for the Pacific user. The fixed code MUST start with Jun 15 (the user's local tomorrow).

  test('UTC-midnight boundary: Pacific user at 11 PM local time sees their local tomorrow first', () => {
    // 2026-06-15T06:00:00Z = Jun 14, 23:00 Pacific (UTC-7 PDT)
    // UTC "today" is Jun 15, so UTC-anchored strip starts Jun 16.
    // Local Pacific "today" is Jun 14, so local-anchored strip MUST start Jun 15.
    const nowMs = Date.UTC(2026, 5, 15, 6, 0, 0); // 2026-06-15T06:00:00Z
    const days = buildDayStrip({
      userTimeZone: 'America/Los_Angeles',
      nowMs,
      maxAdvanceDays: 60,
      stripSize: 7,
    });

    // First day must be Jun 15 (Pacific "tomorrow"), not Jun 16 (UTC "tomorrow").
    expect(days[0].date).toBe('2026-06-15');
  });

  test('UTC user at midnight UTC: first day is tomorrow in UTC', () => {
    // 2026-06-14T23:00:00Z → UTC today is Jun 14, tomorrow is Jun 15
    const nowMs = Date.UTC(2026, 5, 14, 23, 0, 0);
    const days = buildDayStrip({
      userTimeZone: 'UTC',
      nowMs,
      maxAdvanceDays: 60,
    });
    expect(days[0].date).toBe('2026-06-15');
  });

  test('days are in ascending order and all unique', () => {
    const nowMs = Date.now();
    const days = buildDayStrip({ userTimeZone: 'America/Chicago', nowMs, maxAdvanceDays: 60 });
    for (let i = 1; i < days.length; i++) {
      expect(days[i].date > days[i - 1].date).toBe(true);
    }
    const dates = days.map((d) => d.date);
    expect(new Set(dates).size).toBe(dates.length);
  });

  test('no day in the strip is today (in the user timezone)', () => {
    const nowMs = Date.now();
    const TZ = 'America/New_York';
    const todayFmt = new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' });
    const todayDate = todayFmt.format(new Date(nowMs));
    const days = buildDayStrip({ userTimeZone: TZ, nowMs, maxAdvanceDays: 60 });
    expect(days.map((d) => d.date)).not.toContain(todayDate);
  });
});

// ─── Track-D fix 2 (WS-TRACKD-BE): formatDayLabel formats the CIVIL date, not the ────────
//     UTC-midnight instant (QA P1-4 — labels rendered one day behind in US timezones).

describe('formatDayLabel — civil date, not UTC-midnight instant (Track-D fix 2)', () => {
  test('2026-06-15 in America/Chicago (UTC-5) renders "Mon, Jun 15" (not "Sun, Jun 14")', () => {
    expect(_formatDayLabel('2026-06-15', 'America/Chicago')).toBe('Mon, Jun 15');
  });

  test('2026-06-15 in America/New_York (UTC-4) renders "Mon, Jun 15"', () => {
    expect(_formatDayLabel('2026-06-15', 'America/New_York')).toBe('Mon, Jun 15');
  });

  test('2026-06-15 in UTC renders "Mon, Jun 15" (no regression)', () => {
    expect(_formatDayLabel('2026-06-15', 'UTC')).toBe('Mon, Jun 15');
  });

  test('TZ-shift regression: DST fall-back date 2026-11-01 in America/Chicago still formats the correct civil day', () => {
    // 2026-11-01 spans the midnight rollback (CDT→CST) in America/Chicago. The old code
    // formatted the UTC-midnight INSTANT (2026-11-01T00:00:00Z = Oct 31, 7 PM CDT) and
    // rendered "Sat, Oct 31" — one civil day behind. The civil date must render Nov 1.
    expect(_formatDayLabel('2026-11-01', 'America/Chicago')).toBe('Sun, Nov 1');
  });

  test('extreme-offset zones render the same civil day (label is tz-independent by construction)', () => {
    // Pago Pago (UTC-11) and Kiritimati (UTC+14) bracket the full tz range: the old
    // instant-based formatting disagreed across them by up to two calendar days.
    expect(_formatDayLabel('2026-06-15', 'Pacific/Pago_Pago')).toBe('Mon, Jun 15');
    expect(_formatDayLabel('2026-06-15', 'Pacific/Kiritimati')).toBe('Mon, Jun 15');
  });

  test('buildDayStrip labels agree with their civil dates in a US timezone (the P1-4 regression)', () => {
    // 2026-06-14T18:00:00Z = 1 PM CDT — strip starts Mon Jun 15 (Chicago tomorrow).
    const nowMs = Date.UTC(2026, 5, 14, 18, 0, 0);
    const days = buildDayStrip({ userTimeZone: 'America/Chicago', nowMs, maxAdvanceDays: 60 });
    expect(days.length).toBe(7);
    for (const d of days) {
      // The label's day-of-month must equal the civil date's day-of-month. Under the old
      // instant-based formatting every Chicago label showed the PREVIOUS day.
      const civilDayOfMonth = String(Number(d.date.slice(8, 10)));
      expect(d.label.endsWith(` ${civilDayOfMonth}`)).toBe(true);
      // And the full label must equal the civil-date rendering exactly.
      expect(d.label).toBe(_formatDayLabel(d.date, 'America/Chicago'));
    }
    expect(days[0]).toEqual({ date: '2026-06-15', label: 'Mon, Jun 15' });
  });
});
