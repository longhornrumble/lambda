'use strict';

/**
 * cadence.js — pure reminder-tier computation (FROZEN_CONTRACTS §E2).
 *
 * No AWS, no I/O. Given a Booking's `start_at` and "now", returns the reminder
 * tiers that should fire and the wall-clock instant each one fires at.
 *
 * §E2 cadence (computed from `start_at − now` at commit / on every reschedule):
 *   ≥24h → {t24h, t1h} · 4–24h → {t1h} · 1–4h → {t15m} · <1h → {} (too late)
 *
 * Each tier's fire time = `start_at − offset(tier)`; `start_at` is the authoritative
 * appointment instant (read at fire time from the Booking by the dispatch consumer —
 * never snapshotted into the reminder body — but the SCHEDULE itself must fire at a
 * fixed instant, so the fire instant is computed here from the current `start_at`).
 *
 * is_synthetic time-compression (§E1, SR-3, CI-6): when STAGING_TEST_MODE=true AND
 * booking.is_synthetic=true, the SAME tier set is produced but every tier fires at
 * `now + N_min` (small, distinct per tier) so a full cadence exercises within minutes.
 * DOUBLE-gated — real bookings are never compressed. The prod-guard refusal lives at
 * the handler-init layer (index.js), not here (this module is pure).
 */

const MINUTE_MS = 60 * 1000;
const HOUR_MS = 60 * MINUTE_MS;

// Lead-time offset of each tier from `start_at`. The §E1 rule-name vocabulary is
// {t24h, t4h, t1h, t15m}; the §E2 cadence only ever emits t24h/t1h/t15m, but t4h is
// kept here so the offset table is the single source of truth for every legal name.
const TIER_OFFSET_MS = Object.freeze({
  t24h: 24 * HOUR_MS,
  t4h: 4 * HOUR_MS,
  t1h: 1 * HOUR_MS,
  t15m: 15 * MINUTE_MS,
});

// Synthetic compressed fire offsets from `now` (distinct per tier so the cadence
// fires in sequence, not all at once). Overridable via env for CI-6 tuning.
const SYNTHETIC_FIRE_OFFSET_MIN = Object.freeze({
  t24h: 1,
  t4h: 2,
  t1h: 3,
  t15m: 4,
});

function parseInstant(value) {
  if (value instanceof Date) return value.getTime();
  if (typeof value === 'number') return value;
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) {
    throw new Error(`cadence: unparseable start_at "${value}"`);
  }
  return ms;
}

/**
 * Which reminder tiers apply for a given lead time (§E2). Returns an ordered array
 * of tier names (soonest-to-appointment last), or [] when it is too late (<1h).
 */
function tiersForLead(leadMs) {
  if (leadMs >= 24 * HOUR_MS) return ['t24h', 't1h'];
  if (leadMs >= 4 * HOUR_MS) return ['t1h'];
  if (leadMs >= 1 * HOUR_MS) return ['t15m'];
  return [];
}

/**
 * Compute the reminder schedule for a booking.
 *
 * @param {object} args
 * @param {string|number|Date} args.startAt  - the Booking's start_at (appointment instant)
 * @param {number}             [args.nowMs]  - "now" in epoch ms (default Date.now())
 * @param {boolean}            [args.synthetic=false] - honour is_synthetic compression
 *        (caller must ALREADY have applied the STAGING_TEST_MODE && is_synthetic double-gate)
 * @param {object}             [args.syntheticOffsetMin] - per-tier minute overrides
 * @returns {{ tier: string, fireAtMs: number, fireAtIso: string }[]}
 *        tiers whose fire time is still in the future, soonest last.
 */
function computeReminderTiers({ startAt, nowMs = Date.now(), synthetic = false, syntheticOffsetMin } = {}) {
  const startMs = parseInstant(startAt);
  const leadMs = startMs - nowMs;
  const tiers = tiersForLead(leadMs);

  const offsets = syntheticOffsetMin
    ? { ...SYNTHETIC_FIRE_OFFSET_MIN, ...syntheticOffsetMin }
    : SYNTHETIC_FIRE_OFFSET_MIN;

  const out = [];
  for (const tier of tiers) {
    const fireAtMs = synthetic
      ? nowMs + offsets[tier] * MINUTE_MS
      : startMs - TIER_OFFSET_MS[tier];
    // Never create a schedule that fires in the past (EventBridge Scheduler rejects
    // past one-time `at()` expressions). In the normal path tiersForLead already
    // guarantees a future fire; this guard covers clock skew and synthetic overrides.
    if (fireAtMs <= nowMs) continue;
    out.push({ tier, fireAtMs, fireAtIso: new Date(fireAtMs).toISOString() });
  }
  return out;
}

module.exports = {
  computeReminderTiers,
  tiersForLead,
  TIER_OFFSET_MS,
  SYNTHETIC_FIRE_OFFSET_MIN,
  MINUTE_MS,
  HOUR_MS,
};
