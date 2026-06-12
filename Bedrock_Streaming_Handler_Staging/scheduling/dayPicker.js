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
 * Format a UTC date as "Mon, Jun 15" in the given IANA timezone.
 * Uses native Intl — no tz library.
 */
function formatDayLabel(utcMs, userTimeZone) {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: userTimeZone,
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  }).format(new Date(utcMs));
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
 */
function buildDayStrip({ userTimeZone, nowMs, maxAdvanceDays = 60, stripSize = 7 } = {}) {
  const ref = nowMs != null ? nowMs : Date.now();
  // "Tomorrow" starts at the next UTC midnight; strip starts day+1 from now.
  const todayMidnightUtc = new Date(ref);
  todayMidnightUtc.setUTCHours(0, 0, 0, 0);
  const tomorrowMs = todayMidnightUtc.getTime() + 24 * 60 * 60 * 1000;
  const cutoffMs = ref + maxAdvanceDays * 24 * 60 * 60 * 1000;

  const days = [];
  for (let i = 0; i < stripSize; i++) {
    const dayMs = tomorrowMs + i * 24 * 60 * 60 * 1000;
    if (dayMs >= cutoffMs) break; // clipped to max_advance_days
    const d = new Date(dayMs);
    const date = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
    const label = formatDayLabel(dayMs, userTimeZone || 'UTC');
    days.push({ date, label });
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
  const startMs = Date.UTC(year, month - 1, day);
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
