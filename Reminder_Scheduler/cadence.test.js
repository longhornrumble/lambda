'use strict';

/**
 * Unit tests for cadence.js (WS-E-REMIND, FROZEN_CONTRACTS §E2).
 * Pure tier computation — no AWS, no I/O.
 */

const { computeReminderTiers, tiersForLead, TIER_OFFSET_MS, HOUR_MS, MINUTE_MS } = require('./cadence');

const NOW = Date.parse('2026-06-10T12:00:00Z');
const iso = (ms) => new Date(ms).toISOString();

describe('tiersForLead — §E2 mapping', () => {
  test('≥24h → {t24h, t1h}', () => {
    expect(tiersForLead(24 * HOUR_MS)).toEqual(['t24h', 't1h']);
    expect(tiersForLead(48 * HOUR_MS)).toEqual(['t24h', 't1h']);
  });
  test('4–24h → {t1h}', () => {
    expect(tiersForLead(4 * HOUR_MS)).toEqual(['t1h']);
    expect(tiersForLead(23 * HOUR_MS)).toEqual(['t1h']);
  });
  test('1–4h → {t15m}', () => {
    expect(tiersForLead(1 * HOUR_MS)).toEqual(['t15m']);
    expect(tiersForLead(3 * HOUR_MS)).toEqual(['t15m']);
  });
  test('<1h → {} (too late)', () => {
    expect(tiersForLead(59 * MINUTE_MS)).toEqual([]);
    expect(tiersForLead(0)).toEqual([]);
    expect(tiersForLead(-HOUR_MS)).toEqual([]);
  });
});

describe('computeReminderTiers — normal cadence (fireAt = start_at − offset)', () => {
  test('appointment 48h out → t24h and t1h, each fired before start_at', () => {
    const startAt = iso(NOW + 48 * HOUR_MS);
    const out = computeReminderTiers({ startAt, nowMs: NOW });
    expect(out.map((t) => t.tier)).toEqual(['t24h', 't1h']);
    const startMs = Date.parse(startAt);
    expect(out[0].fireAtMs).toBe(startMs - TIER_OFFSET_MS.t24h);
    expect(out[1].fireAtMs).toBe(startMs - TIER_OFFSET_MS.t1h);
    out.forEach((t) => expect(t.fireAtMs).toBeLessThan(startMs));
    out.forEach((t) => expect(t.fireAtMs).toBeGreaterThan(NOW));
  });

  test('appointment 6h out → only t1h', () => {
    const startAt = iso(NOW + 6 * HOUR_MS);
    const out = computeReminderTiers({ startAt, nowMs: NOW });
    expect(out.map((t) => t.tier)).toEqual(['t1h']);
    expect(out[0].fireAtMs).toBe(Date.parse(startAt) - TIER_OFFSET_MS.t1h);
  });

  test('appointment 2h out → only t15m', () => {
    const startAt = iso(NOW + 2 * HOUR_MS);
    const out = computeReminderTiers({ startAt, nowMs: NOW });
    expect(out.map((t) => t.tier)).toEqual(['t15m']);
  });

  test('appointment 30m out → no reminders', () => {
    const startAt = iso(NOW + 30 * MINUTE_MS);
    expect(computeReminderTiers({ startAt, nowMs: NOW })).toEqual([]);
  });

  test('fireAtIso is the ISO of fireAtMs', () => {
    const startAt = iso(NOW + 48 * HOUR_MS);
    const out = computeReminderTiers({ startAt, nowMs: NOW });
    expect(out[0].fireAtIso).toBe(iso(out[0].fireAtMs));
  });

  test('accepts Date and epoch-ms start_at', () => {
    const startMs = NOW + 48 * HOUR_MS;
    expect(computeReminderTiers({ startAt: new Date(startMs), nowMs: NOW }).map((t) => t.tier))
      .toEqual(['t24h', 't1h']);
    expect(computeReminderTiers({ startAt: startMs, nowMs: NOW }).map((t) => t.tier))
      .toEqual(['t24h', 't1h']);
  });

  test('unparseable start_at throws', () => {
    expect(() => computeReminderTiers({ startAt: 'not-a-date', nowMs: NOW })).toThrow(/unparseable/);
  });
});

describe('computeReminderTiers — is_synthetic time-compression (§E1)', () => {
  test('synthetic → same tier set but fired at now + small per-tier offsets', () => {
    const startAt = iso(NOW + 48 * HOUR_MS); // would normally produce t24h + t1h
    const out = computeReminderTiers({ startAt, nowMs: NOW, synthetic: true });
    expect(out.map((t) => t.tier)).toEqual(['t24h', 't1h']);
    expect(out[0].fireAtMs).toBe(NOW + 1 * MINUTE_MS); // t24h compressed → +1m
    expect(out[1].fireAtMs).toBe(NOW + 3 * MINUTE_MS); // t1h compressed → +3m
    out.forEach((t) => expect(t.fireAtMs).toBeGreaterThan(NOW));
  });

  test('synthetic offsets are overridable', () => {
    const startAt = iso(NOW + 6 * HOUR_MS); // → t1h
    const out = computeReminderTiers({
      startAt, nowMs: NOW, synthetic: true, syntheticOffsetMin: { t1h: 10 },
    });
    expect(out[0].fireAtMs).toBe(NOW + 10 * MINUTE_MS);
  });
});

describe('computeReminderTiers — past-fire guard', () => {
  test('drops any tier whose fire instant is already past (clock skew)', () => {
    // 24h01m out: t24h fires in 1 min (kept), t1h fires 23h from now (kept).
    const startAt = iso(NOW + 24 * HOUR_MS + 1 * MINUTE_MS);
    const out = computeReminderTiers({ startAt, nowMs: NOW });
    expect(out.map((t) => t.tier)).toEqual(['t24h', 't1h']);
    out.forEach((t) => expect(t.fireAtMs).toBeGreaterThan(NOW));
  });
});
