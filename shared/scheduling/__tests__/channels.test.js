'use strict';

/**
 * channels.test.js — selectChannels (WS-E-TCPA, FROZEN_CONTRACTS §E3 / SEAM-1).
 *
 * Covers: email-floor invariant; the three sms AND-conditions (org flag, consent
 * fail-closed, quiet-hours); volunteer-tz fire-time quiet-hours incl. wrap-around +
 * boundaries; fail-closed on unresolvable tz/fireTime. Pure logic — no AWS.
 */

const {
  selectChannels,
  consentValid,
  inQuietHours,
  localHour,
  DEFAULT_QUIET_HOURS,
} = require('../channels');

// A valid, opted-in consent record (the shipped picasso-sms-consent shape).
const OK_CONSENT = { consent_given: true, opted_out_at: null };

// Fire times chosen so the volunteer-local hour is known per timezone.
//   In June: America/New_York = EDT (UTC-4), America/Los_Angeles = PDT (UTC-7).
const NOON_UTC = '2026-06-05T12:00:00Z';      // NY 08:00 (allowed) · LA 05:00 (quiet) · UTC 12:00 (allowed)
const NY_8PM = '2026-06-06T00:00:00Z';        // NY 20:00 (quiet, boundary)
const NY_7PM = '2026-06-05T23:00:00Z';        // NY 19:00 (allowed, just before window)
const NY_MIDNIGHT = '2026-06-05T04:00:00Z';   // NY 00:00 (quiet)
const UTC_3AM = '2026-06-05T03:00:00Z';       // UTC 03:00 (quiet when tz falls back to UTC)

const ALLOWED = { booking: { timezone: 'America/New_York' }, fireTime: NOON_UTC };

describe('selectChannels — email floor', () => {
  test('email is ALWAYS true, regardless of every sms input', () => {
    expect(selectChannels().email).toBe(true); // no-arg: defaults engage, email floor holds
    expect(selectChannels({}).email).toBe(true);
    expect(
      selectChannels({ orgSmsEnabled: false, consentRecord: null, fireTime: NY_8PM }).email
    ).toBe(true);
    expect(
      selectChannels({ orgSmsEnabled: true, consentRecord: OK_CONSENT, ...ALLOWED }).email
    ).toBe(true);
  });
});

describe('selectChannels — sms requires ALL of org-flag AND consent AND not-quiet', () => {
  test('sms true when org enabled + valid consent + outside quiet hours', () => {
    const r = selectChannels({ orgSmsEnabled: true, consentRecord: OK_CONSENT, ...ALLOWED });
    expect(r).toEqual({ email: true, sms: true });
  });

  test('sms false when orgSmsEnabled is not strictly true', () => {
    for (const flag of [false, undefined, 'true', 1]) {
      const r = selectChannels({ orgSmsEnabled: flag, consentRecord: OK_CONSENT, ...ALLOWED });
      expect(r.sms).toBe(false);
    }
  });

  test('sms false when consent invalid (fail-closed)', () => {
    const r = selectChannels({ orgSmsEnabled: true, consentRecord: null, ...ALLOWED });
    expect(r.sms).toBe(false);
  });

  test('sms false when inside quiet hours even with org+consent', () => {
    const r = selectChannels({
      orgSmsEnabled: true,
      consentRecord: OK_CONSENT,
      booking: { timezone: 'America/New_York' },
      fireTime: NY_8PM,
    });
    expect(r.sms).toBe(false);
  });
});

describe('consentValid — fail-closed (§E3)', () => {
  test('true only for consent_given===true and no opted_out_at', () => {
    expect(consentValid({ consent_given: true, opted_out_at: null })).toBe(true);
    expect(consentValid({ consent_given: true })).toBe(true); // absent opted_out_at = still in
  });

  test('false for absent record', () => {
    expect(consentValid(null)).toBe(false);
    expect(consentValid(undefined)).toBe(false);
  });

  test('false when consent_given is not strictly true', () => {
    expect(consentValid({ consent_given: false })).toBe(false);
    expect(consentValid({ consent_given: undefined })).toBe(false);
    expect(consentValid({})).toBe(false);
    expect(consentValid({ consent_given: 'true' })).toBe(false);
  });

  test('false when opted_out_at is present', () => {
    expect(consentValid({ consent_given: true, opted_out_at: '2026-06-01T00:00:00Z' })).toBe(false);
  });
});

describe('inQuietHours — volunteer-tz fixed 8pm–8am, fire-time', () => {
  test('default window is 20:00–08:00', () => {
    expect(DEFAULT_QUIET_HOURS).toEqual({ startHour: 20, endHour: 8 });
  });

  test('quiet at 8pm boundary (inclusive start)', () => {
    expect(inQuietHours(NY_8PM, 'America/New_York')).toBe(true);
  });

  test('allowed at 7pm (just before window)', () => {
    expect(inQuietHours(NY_7PM, 'America/New_York')).toBe(false);
  });

  test('allowed at 8am boundary (exclusive end)', () => {
    // NOON_UTC = 08:00 in New_York → not quiet (h===endHour).
    expect(inQuietHours(NOON_UTC, 'America/New_York')).toBe(false);
  });

  test('quiet at midnight (wrap-around)', () => {
    expect(inQuietHours(NY_MIDNIGHT, 'America/New_York')).toBe(true);
  });

  test('same fireTime differs by volunteer timezone', () => {
    expect(inQuietHours(NOON_UTC, 'America/New_York')).toBe(false); // 08:00 EDT
    expect(inQuietHours(NOON_UTC, 'America/Los_Angeles')).toBe(true); // 05:00 PDT
  });

  test('falls back to UTC when timezone absent', () => {
    expect(inQuietHours(UTC_3AM, undefined)).toBe(true); // 03:00 UTC quiet
    expect(inQuietHours(NOON_UTC, undefined)).toBe(false); // 12:00 UTC allowed
  });

  test('honors a custom same-day window (start <= end)', () => {
    // 09:00–17:00 quiet window; NOON_UTC = 12:00 UTC.
    expect(inQuietHours(NOON_UTC, 'UTC', { startHour: 9, endHour: 17 })).toBe(true);
    expect(inQuietHours(UTC_3AM, 'UTC', { startHour: 9, endHour: 17 })).toBe(false);
  });

  test('FAILS CLOSED on an unresolvable timezone (treated as in-window)', () => {
    expect(inQuietHours(NOON_UTC, 'Not/AZone')).toBe(true);
  });

  test('FAILS CLOSED on an unparseable fireTime', () => {
    expect(inQuietHours('not-a-date', 'America/New_York')).toBe(true);
    expect(inQuietHours(undefined, 'America/New_York')).toBe(true);
  });
});

describe('localHour', () => {
  test('returns the volunteer-local hour', () => {
    expect(localHour(NOON_UTC, 'America/New_York')).toBe(8);
    expect(localHour(NOON_UTC, 'America/Los_Angeles')).toBe(5);
    expect(localHour(NOON_UTC, 'UTC')).toBe(12);
  });

  test('normalizes midnight to 0', () => {
    expect(localHour(NY_MIDNIGHT, 'America/New_York')).toBe(0);
  });

  test('returns null for an invalid timezone', () => {
    expect(localHour(NOON_UTC, 'Not/AZone')).toBeNull();
  });

  test('returns null for an unparseable fireTime', () => {
    expect(localHour('garbage', 'UTC')).toBeNull();
  });
});
