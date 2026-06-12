'use strict';

/**
 * slots.js — Candidate slot generation (WS-C7).
 *
 * Canonical §9.3; frozen contract FROZEN_CONTRACTS.md §B3. Pure-logic library
 * module consumed by C6 (pool-at-commit) + WS-EUI (customer portal). This module
 * owns ONE export and nothing else:
 *
 *   generateSlots({ busyIntervals, appointmentType, userTimeZone, alreadyRejected })
 *     → [ { slotId, start: ISO8601, end: ISO8601, label: "Tue, Jun 3 · 2:00 PM", resourceId } ]
 *
 * Pure + deterministic: NO API/DB calls, NO timezone-library dependency. All
 * timezone + DST arithmetic uses the platform-native `Intl.DateTimeFormat`
 * (the scaffold package.json carries no tz lib, and the work-order forbids adding
 * one without an integrator decision — §4.3 "concrete-first").
 *
 * ── DST safety (the crux, canonical §9.3 "respect the user's local timezone") ──
 *   • availability_windows clock-times are in the AppointmentType `timezone`
 *     (business/coordinator zone). Each wall-clock candidate is resolved to a UTC
 *     instant via a two-pass offset solve (zonedWallTimeToUtc):
 *       - spring-forward gap: a wall-clock time that does NOT exist in the business
 *         zone (e.g. 02:30 on a US spring-forward day) round-trips to a DIFFERENT
 *         wall clock → it is SKIPPED, never offered.
 *       - fall-back ambiguity: a wall-clock time that occurs TWICE resolves to the
 *         FIRST (earlier-offset) instant deterministically → offered exactly once.
 *   • slot end = start + duration computed in epoch milliseconds, so the real
 *     elapsed duration is correct even across a transition inside the window.
 *   • the display `label` is rendered in the VOLUNTEER's `userTimeZone` (also via
 *     Intl, so its own DST is handled), never the business zone or UTC (§9.3).
 *
 * ── Interpretations layered on the frozen §B3 contract (flagged in the PR for
 *    integrator confirmation; none redefine the contract) ──
 *   • §B3's OUTPUT requires `resourceId` but its INPUT omits one — a genuine
 *     contract gap. Resolved minimally: an OPTIONAL `resourceId` input is threaded
 *     into each slot (and seeds the deterministic slotId). The frozen 4-key call
 *     still works; resourceId is `null` when not supplied. ESCALATED in the PR per
 *     FROZEN_CONTRACTS §C — not forked.
 *   • `alreadyRejected` = an array of previously-emitted `slotId` strings. slotId is
 *     deterministic (`${resourceId}|${startISO}`) so a re-offer dedupes a rejected
 *     slot by id. Entries that happen to equal a candidate `start` ISO are also
 *     honored (defensive, in case a caller dedupes by start).
 *   • OPTIONAL additive params (do NOT change the frozen call or output shape):
 *       now (ISO8601, default = current time) — generation reference for min_lead +
 *         deterministic tests; searchDays (default 14) — forward horizon scanned to
 *         collect chips; maxSlots (default 5) / minSlots (default 3, advisory only).
 *     Fewer than minSlots may be returned when the horizon is genuinely sparse;
 *     the tiered no-slots fallback (§9.3) is the caller's job, not C7's.
 *   • SLOT-GEN EXTENSION (§B16e backwards-compatible, mirrors the §B3 resourceId
 *     precedent): dateWindow: { startISO, endISO } constrains candidate generation
 *     to the window (inclusive start, exclusive end). When absent the scan is
 *     unchanged — the frozen 4-key call is byte-identical in behavior (regression
 *     tests verify no drift). Neither key is required; a partial window (only
 *     startISO OR only endISO) applies only the supplied bound.
 *   • `busyIntervals` = array of { start: ISO8601, end: ISO8601 } (the §B1 `.busy`
 *     array for the chosen resource). buffer_minutes pads each busy interval on both
 *     sides so a candidate keeps the configured gap from existing commitments.
 */

const DAY_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

// ─── Timezone helpers (native Intl only) ──────────────────────────────────────────

// Offset (localWallClock − UTC), in ms, for a given UTC instant in `timeZone`.
function zoneOffsetMs(utcMs, timeZone) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const parts = dtf.formatToParts(new Date(utcMs));
  const f = {};
  for (const p of parts) {
    if (p.type !== 'literal') f[p.type] = p.value;
  }
  const hour = f.hour === '24' ? 0 : Number(f.hour); // some engines emit '24' for midnight
  const asUtc = Date.UTC(
    Number(f.year),
    Number(f.month) - 1,
    Number(f.day),
    hour,
    Number(f.minute),
    Number(f.second)
  );
  return asUtc - utcMs;
}

// Resolve a business-zone wall clock (civil y/mo/d h:mi) to a UTC epoch ms.
// Returns null when that wall clock does NOT exist in the zone (spring-forward gap).
// On a fall-back day the wall clock exists twice; the earlier (pre-transition)
// instant is returned deterministically.
function zonedWallTimeToUtc(year, month, day, hour, minute, timeZone) {
  const guess = Date.UTC(year, month - 1, day, hour, minute, 0);
  const off1 = zoneOffsetMs(guess, timeZone);
  let utc = guess - off1;
  const off2 = zoneOffsetMs(utc, timeZone);
  if (off2 !== off1) {
    utc = guess - off2;
  }
  // Existence check: round-trip the resolved instant back to the zone's wall clock
  // and confirm it matches what was requested. A single civil-epoch compare (not a
  // field-by-field OR) keeps this one branch. A mismatch means the requested wall
  // clock does not exist in the zone (spring-forward gap) → skip.
  const d = new Date(utc + zoneOffsetMs(utc, timeZone));
  const roundTrip = Date.UTC(
    d.getUTCFullYear(),
    d.getUTCMonth(),
    d.getUTCDate(),
    d.getUTCHours(),
    d.getUTCMinutes()
  );
  if (roundTrip !== Date.UTC(year, month - 1, day, hour, minute)) {
    return null; // nonexistent local time (spring-forward gap)
  }
  return utc;
}

// Civil weekday for a y/mo/d (DST-agnostic — pure calendar math).
function weekdayKey(year, month, day) {
  return DAY_KEYS[new Date(Date.UTC(year, month - 1, day)).getUTCDay()];
}

// Civil date components of a UTC instant AS SEEN in `timeZone` (for the start day).
function civilDateInZone(utcMs, timeZone) {
  const off = zoneOffsetMs(utcMs, timeZone);
  const d = new Date(utcMs + off);
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate() };
}

// Advance a civil date by n days (UTC civil math; no DST involved).
function addCivilDays(year, month, day, n) {
  const d = new Date(Date.UTC(year, month - 1, day + n));
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate() };
}

// "HH:MM" → minutes-of-day. Returns null on a malformed value (tolerated → skipped).
function parseHm(hm) {
  if (typeof hm !== 'string') return null;
  const m = /^(\d{1,2}):(\d{2})$/.exec(hm.trim());
  if (!m) return null;
  const h = Number(m[1]);
  const mi = Number(m[2]);
  if (h > 23 || mi > 59) return null;
  return h * 60 + mi;
}

// ─── Label rendering (in the VOLUNTEER's timezone) ────────────────────────────────

// "Tue, Jun 3 · 2:00 PM" — date part and time part joined with a middle dot so the
// exact frozen §B3 format is produced (Intl's single-format output inserts a comma
// before the time and would add the year).
function formatLabel(utcMs, userTimeZone) {
  const d = new Date(utcMs);
  const datePart = new Intl.DateTimeFormat('en-US', {
    timeZone: userTimeZone,
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  }).format(d);
  const timePart = new Intl.DateTimeFormat('en-US', {
    timeZone: userTimeZone,
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  }).format(d);
  return `${datePart} · ${timePart}`;
}

// ─── generateSlots (frozen §B3) ───────────────────────────────────────────────────

function generateSlots({
  busyIntervals,
  appointmentType,
  userTimeZone,
  alreadyRejected,
  // additive optional params (see header) — frozen call passes only the 4 keys above.
  resourceId = null,
  now = undefined,
  searchDays = 14,
  maxSlots = 5,
  // §B16e: OPTIONAL dateWindow — constrains generation to the specified UTC range.
  // Absent → unchanged behavior (frozen 4-key call is byte-identical).
  dateWindow = undefined,
} = {}) {
  if (!appointmentType || typeof appointmentType !== 'object') {
    throw new Error('appointmentType is required');
  }
  if (typeof userTimeZone !== 'string' || userTimeZone.length === 0) {
    throw new Error('userTimeZone is required');
  }

  const duration = appointmentType.duration_minutes;
  if (typeof duration !== 'number' || !(duration > 0)) {
    throw new Error('appointmentType.duration_minutes must be a positive number');
  }
  const businessTz = appointmentType.timezone;
  if (typeof businessTz !== 'string' || businessTz.length === 0) {
    throw new Error('appointmentType.timezone is required');
  }
  const windows = appointmentType.availability_windows;
  if (!windows || typeof windows !== 'object') {
    throw new Error('appointmentType.availability_windows is required');
  }

  // Validate the IANA zones up front so a bad value fails with a clear message
  // rather than deep inside the loop.
  try {
    zoneOffsetMs(0, businessTz);
    zoneOffsetMs(0, userTimeZone);
  } catch (err) {
    throw new Error(`Invalid IANA timeZone: ${err.message}`);
  }

  // Optional config (schema discipline — tolerate missing → documented defaults).
  const buffer = Number(appointmentType.buffer_minutes) || 0;
  const granularity =
    Number(appointmentType.slot_granularity_minutes) > 0
      ? Number(appointmentType.slot_granularity_minutes)
      : duration;
  const minLead = Number(appointmentType.min_lead_minutes) || 0;

  const nowMs = now != null ? Date.parse(now) : Date.now();
  if (Number.isNaN(nowMs)) {
    throw new Error('now must be a valid ISO8601 timestamp');
  }
  const earliestStartMs = nowMs + minLead * 60 * 1000;

  // §B16e dateWindow: parse the optional bounds once. NaN / absent → no bound applied.
  const windowStartMs =
    dateWindow && dateWindow.startISO != null ? Date.parse(dateWindow.startISO) : NaN;
  const windowEndMs =
    dateWindow && dateWindow.endISO != null ? Date.parse(dateWindow.endISO) : NaN;
  // The effective lower bound is the LATER of earliestStart and the window start.
  const effectiveEarliestMs =
    !Number.isNaN(windowStartMs) && windowStartMs > earliestStartMs
      ? windowStartMs
      : earliestStartMs;

  // Pre-parse busy intervals once. buffer pads each on both sides.
  const bufMs = buffer * 60 * 1000;
  const busy = (Array.isArray(busyIntervals) ? busyIntervals : [])
    .map((iv) => ({ start: Date.parse(iv && iv.start), end: Date.parse(iv && iv.end) }))
    .filter((iv) => !Number.isNaN(iv.start) && !Number.isNaN(iv.end));

  const rejected = new Set(Array.isArray(alreadyRejected) ? alreadyRejected : []);

  const durationMs = duration * 60 * 1000;

  function conflictsWithBusy(startMs, endMs) {
    for (const iv of busy) {
      if (startMs < iv.end + bufMs && endMs + bufMs > iv.start) return true;
    }
    return false;
  }

  const slots = [];
  const seenStarts = new Set(); // dedupe identical instants (e.g. fall-back ambiguity)

  const startCivil = civilDateInZone(effectiveEarliestMs, businessTz);

  for (let dayOffset = 0; dayOffset < searchDays && slots.length < maxSlots; dayOffset += 1) {
    const { year, month, day } = addCivilDays(
      startCivil.year,
      startCivil.month,
      startCivil.day,
      dayOffset
    );
    const dayWindows = windows[weekdayKey(year, month, day)];
    if (!Array.isArray(dayWindows) || dayWindows.length === 0) continue;

    for (const win of dayWindows) {
      if (slots.length >= maxSlots) break;
      const winStart = parseHm(win && win.start);
      const winEnd = parseHm(win && win.end);
      if (winStart == null || winEnd == null || winEnd <= winStart) continue;

      // Candidate start times step by granularity; the slot must fit the window
      // by wall-clock (start + duration <= window end).
      for (
        let mins = winStart;
        mins + duration <= winEnd && slots.length < maxSlots;
        mins += granularity
      ) {
        const hour = Math.floor(mins / 60);
        const minute = mins % 60;
        const startMs = zonedWallTimeToUtc(year, month, day, hour, minute, businessTz);
        if (startMs == null) continue; // nonexistent wall clock (spring-forward gap)
        if (startMs < effectiveEarliestMs) continue;
        // §B16e: apply the upper bound of the dateWindow when supplied.
        if (!Number.isNaN(windowEndMs) && startMs >= windowEndMs) continue;

        const endMs = startMs + durationMs;
        if (conflictsWithBusy(startMs, endMs)) continue;

        const startISO = new Date(startMs).toISOString();
        if (seenStarts.has(startMs)) continue; // ambiguous-time double / overlapping grid
        const slotId = `${resourceId == null ? '' : resourceId}|${startISO}`;
        if (rejected.has(slotId) || rejected.has(startISO)) continue;

        seenStarts.add(startMs);
        slots.push({
          slotId,
          start: startISO,
          end: new Date(endMs).toISOString(),
          label: formatLabel(startMs, userTimeZone),
          resourceId,
        });
      }
    }
  }

  return slots;
}

module.exports = {
  generateSlots,
  // Shipped DST-correct civil-time resolver — consumed by the agent day-part bounds
  // (agentTools.js after_time/before_time → date_window instants; 2026-06-12).
  zonedWallTimeToUtc,
  // exported for unit coverage of the DST-critical helpers:
  _zoneOffsetMs: zoneOffsetMs,
  _zonedWallTimeToUtc: zonedWallTimeToUtc,
  _formatLabel: formatLabel,
};
