'use strict';

/**
 * dayPicker.js — Surface-4 day-picker fallback helpers (WS-T3-DAYPICK-BE, §B16e).
 *
 * Canonical §9.3 (async escape) + FROZEN_CONTRACTS.md §B16e (the seam).
 *
 * Pure-logic helpers consumed by newBookingFlow.js. This module:
 *   • builds the 7-day strip (§B16e: next 7 candidate days from `now`, clipped to
 *     `max_advance_days`; labels in the user's timezone via native Intl — no tz lib).
 *   • emits the `scheduling_day_picker` SSE message on the BSH write stream.
 *   • emits the `scheduling_notice` async escape when picker cycles exceed the threshold.
 *   • computes the `dateWindow` { startISO, endISO } for a selected date in the
 *     coordinator/business timezone, so generateSlots is constrained to that day.
 *
 * ── §B16e SSE shape ──
 *   { type: 'scheduling_day_picker',
 *     days: [ { date: 'YYYY-MM-DD', label: '<Intl-formatted>' } x7 ],
 *     user_time_zone: '<IANA tz>' }
 *
 * ── Cycle counting (strand-prevention) ──
 *   The picker-cycle count rides in the existing saved session state (same object as
 *   `candidate_slots`, `rejected_slot_ids`, `proposal`) under the key `picker_cycles`.
 *   The FLOW increments and persists it alongside the existing state fields on each
 *   picker emit. >3 total cycles → `scheduling_notice` escape (§9.3). The count is
 *   NOT a separate state field — it is additive (schema discipline: readers tolerate
 *   absence, default 0).
 *
 * ── No-tz-lib constraint ──
 *   Labels use Intl.DateTimeFormat (same as formatLabel in slots.js) so the module
 *   stays esbuild-safe with no new npm deps. The date strip is UTC midnight ISO strings
 *   so the widget can format them in whatever locale it needs.
 */

// Maximum picker cycles before the §9.3 async escape fires.
const MAX_PICKER_CYCLES = 3;

// ─── Day-strip generation ─────────────────────────────────────────────────────────

/**
 * Format a civil date ('YYYY-MM-DD') as "Mon, Jun 15". Uses native Intl — no tz library.
 *
 * Track-D fix 2 (WS-TRACKD-BE, QA P1-4): this used to format the UTC-MIDNIGHT INSTANT of
 * the date in the user's timezone, rendering one calendar day behind for any zone west of
 * UTC (e.g. '2026-06-15T00:00:00Z' → "Sun, Jun 14" in America/Chicago). The strip's `date`
 * values are already CIVIL dates in the user's zone (localDateString), so the label must
 * render that civil date directly: parse Y/M/D from the string, anchor at UTC midnight,
 * and format IN UTC — the label is the same civil day in every timezone by construction.
 *
 * @param {string} dateStr - civil date 'YYYY-MM-DD' (already the user's local calendar day)
 * @param {string} [_userTimeZone] - accepted for call-site symmetry; intentionally UNUSED —
 *   the civil day must not shift with the viewer's timezone (that was the regression).
 */
function formatDayLabel(dateStr, _userTimeZone) {
  const [y, m, d] = String(dateStr).split('-').map(Number);
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  }).format(new Date(Date.UTC(y, m - 1, d)));
}

/**
 * Return the YYYY-MM-DD date string for `epochMs` in the given IANA timezone,
 * using native Intl (no tz lib). This is the "local date" the user sees.
 */
function localDateString(epochMs, timeZone) {
  const fmt = new Intl.DateTimeFormat('en-CA', { // en-CA yields YYYY-MM-DD natively
    timeZone: timeZone || 'UTC',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return fmt.format(new Date(epochMs)); // "2026-06-15"
}

/**
 * Build the 7-day candidate strip starting from `now`, respecting `max_advance_days`.
 *
 * @param {object} params
 * @param {string} params.userTimeZone   - IANA tz for the day labels
 * @param {number} [params.nowMs]        - epoch ms reference (default: Date.now())
 * @param {number} [params.maxAdvanceDays] - upper bound on days ahead (default 60)
 * @param {number} [params.stripSize]    - number of days in the strip (default 7)
 * @returns {{ date: string, label: string }[]}  YYYY-MM-DD + Intl-formatted label
 *
 * §9: "tomorrow" is anchored in the user's local timezone (Intl-based, no tz lib)
 * rather than UTC midnight. A Pacific user at 11 PM on Jun 14 UTC sees "Jun 15"
 * (their local tomorrow) as the first strip day, not "Jun 15 UTC" which may already
 * be "Jun 15" in UTC but still "Jun 14" in their local time.
 */
function buildDayStrip({ userTimeZone, nowMs, maxAdvanceDays = 60, stripSize = 7 } = {}) {
  const ref = nowMs != null ? nowMs : Date.now();
  const tz = userTimeZone || 'UTC';

  // §8: clamp maxAdvanceDays — negative/zero/fractional config must not produce an
  // empty strip. Math.max(1, ...) ensures at least one day is always available.
  const effectiveMaxDays = Math.max(1, Number(maxAdvanceDays) || 60);

  // Find "today" as a YYYY-MM-DD in the user's local timezone.
  const todayLocalDate = localDateString(ref, tz);

  // Build a set of consecutive day strings starting from tomorrow (today + 1 day).
  // We advance one calendar day at a time by adding 24h in epoch ms, then re-derive
  // the local date string. This correctly handles DST transitions (a 23h or 25h day
  // in the user's zone doesn't skip or double a calendar date because we always
  // re-project through Intl rather than assuming 24h === 1 day in wall-clock time).
  const cutoffMs = ref + effectiveMaxDays * 24 * 60 * 60 * 1000;

  const days = [];
  let probeMs = ref + 24 * 60 * 60 * 1000; // start 24h ahead; will skip if still today
  let safetyLimit = stripSize + 3; // never loop more than stripSize + a DST buffer
  while (days.length < stripSize && safetyLimit-- > 0) {
    const localDate = localDateString(probeMs, tz);
    if (localDate === todayLocalDate) {
      // Still "today" in local time (e.g. we advanced 24h but DST spring-forward
      // means it's still the same local date) — advance another hour and retry.
      probeMs += 60 * 60 * 1000;
      continue;
    }
    if (probeMs > cutoffMs) break; // clipped to max_advance_days (exclusive upper bound)
    // Track-D fix 2: label the CIVIL date string itself (tz-independent) — formatting the
    // UTC-midnight instant in the user's tz rendered the previous day for US zones (P1-4).
    const label = formatDayLabel(localDate, tz);
    days.push({ date: localDate, label });
    probeMs += 24 * 60 * 60 * 1000;
  }
  return days;
}

// ─── dateWindow for a selected day ───────────────────────────────────────────────

/**
 * Convert a picked YYYY-MM-DD into a UTC { startISO, endISO } window for generateSlots.
 * The window is [midnight UTC of the date, midnight UTC of the next day) — i.e., the
 * full calendar day in UTC, which is conservative (slightly wider than a single timezone
 * day but always contains it). v1 doesn't precompute per-day availability (that is a v2
 * enhancement per §B16e), so a full UTC day is the correct window.
 *
 * @param {string} datePicked - 'YYYY-MM-DD'
 * @returns {{ startISO: string, endISO: string }}
 */
function dateWindowForDay(datePicked) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(datePicked)) {
    throw new Error(`dayPicker.dateWindowForDay: invalid date '${datePicked}'`);
  }
  const [year, month, day] = datePicked.split('-').map(Number);

  // Calendar-valid civil-date check: round-trip through Date.UTC and verify
  // the components match exactly. This kills overflow dates like 2026-02-31
  // (silently rolls to Mar 3), 2026-13-01 (invalid month), 2026-00-01 (month 0).
  // Also bounds the year to [2020, 2100] so pathological values are rejected early.
  if (year < 2020 || year > 2100) {
    throw new Error(`dayPicker.dateWindowForDay: year out of range '${datePicked}'`);
  }
  const startMs = Date.UTC(year, month - 1, day);
  const check = new Date(startMs);
  if (
    check.getUTCFullYear() !== year ||
    check.getUTCMonth() + 1 !== month ||
    check.getUTCDate() !== day
  ) {
    throw new Error(`dayPicker.dateWindowForDay: invalid civil date '${datePicked}'`);
  }

  const endMs = startMs + 24 * 60 * 60 * 1000;
  return {
    startISO: new Date(startMs).toISOString(),
    endISO: new Date(endMs).toISOString(),
  };
}

// ─── SSE emitters ─────────────────────────────────────────────────────────────────

/**
 * Emit the §B16e `scheduling_day_picker` SSE message.
 *
 * @param {Function} write     - the BSH stream writer (same seam as scheduling_slots)
 * @param {string}   sessionId
 * @param {object[]} days      - the strip from buildDayStrip
 * @param {string}   userTimeZone
 */
function emitDayPicker(write, sessionId, days, userTimeZone) {
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({
      type: 'scheduling_day_picker',
      days,
      user_time_zone: userTimeZone,
      session_id: sessionId,
    })}\n\n`);
  }
}

/**
 * Emit the `scheduling_notice` async escape (§9.3 "we'll follow up" fallback).
 * Mirrors the existing `_emitFallbackNotice` in schedulingFlow.js and newBookingFlow.js.
 *
 * @param {Function} write
 * @param {string}   sessionId
 */
function emitPickerEscapeNotice(write, sessionId) {
  if (typeof write === 'function') {
    write(`data: ${JSON.stringify({
      type: 'scheduling_notice',
      notice: 'request_received_email_followup',
      session_id: sessionId,
    })}\n\n`);
  }
}

module.exports = {
  MAX_PICKER_CYCLES,
  buildDayStrip,
  dateWindowForDay,
  emitDayPicker,
  emitPickerEscapeNotice,
  // exported for tests:
  _formatDayLabel: formatDayLabel,
};
