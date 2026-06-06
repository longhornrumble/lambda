'use strict';

/**
 * channels.js — TCPA channel-selection (WS-E-TCPA).
 *
 * FROZEN_CONTRACTS §E3 + SEAM-1. Decides which channels a scheduling notice may use for
 * ONE moment (confirmation / reminder / cancel / reschedule). EMAIL is the floor — it is
 * ALWAYS true (it carries the .ics + full detail and is never gated). SMS is an opt-in
 * SUPPLEMENT, never the sole channel.
 *
 *   selectChannels({ tenantId, booking, orgSmsEnabled, consentRecord, quietHours, fireTime }, deps)
 *     → { email: true, sms: <bool> }
 *
 *   sms = orgSmsEnabled === true                          // org-level (tenant notificationPrefs.sms)
 *      && consentValid(consentRecord)                     // recipient-level, FAIL-CLOSED
 *      && !inQuietHours(fireTime, booking.timezone, ...)  // volunteer-local, fixed 8pm–8am
 *
 * PURE given its inputs — consent is PASSED IN (the caller does the picasso-sms-consent
 * read; SEAM-1), quiet-hours is evaluated from the passed `fireTime`. No I/O, no clients.
 *
 * ── Consumers (own their call-sites; this module owns only the decision) ──
 *   WS-E-REMIND wires this into `Scheduled_Message_Sender` at FIRE TIME (the row `channel`
 *   is the *requested* channel; the actual SMS send is gated here). WS-E-ATTEND wires it
 *   into escalation. The downstream SMS send goes through SMS_Sender with sendType:'contact'
 *   — that field re-checks the SAME consent record server-side (defense in depth); a
 *   `sms:false` here suppresses before we ever invoke it.
 *
 * ── consentValid: FAIL-CLOSED (§E3) ──
 *   absent record OR consent_given !== true OR opted_out_at present ⇒ false. (An absent
 *   `opted_out_at` means still-opted-in.) This intentionally MIRRORS the shipped SMS_Sender
 *   gate but is *stricter* by one notch (requires consent_given === true rather than
 *   !== false), so this pre-filter can only ever suppress MORE than the authoritative
 *   server gate — never send when SMS_Sender would not. SMS_Sender remains the final word.
 *
 * ── Quiet-hours: volunteer-local, fire-time, fixed 8pm–8am (SEAM-1) ──
 *   nowLocal is computed AT FIRE TIME from `booking.timezone` (captured at booking;
 *   fallback UTC) using native Intl (no tz lib — §B convention). The window is the FIXED
 *   8pm–8am (20:00–08:00) — the VOLUNTEER's local night, NOT the coordinator's
 *   notificationPrefs.sms_quiet_hours (SEAM-1 correction). `quietHours` is accepted for
 *   forward-compatibility but defaults to that fixed window in v1. SMS is dropped in-window;
 *   email always sends. An unresolvable timezone or fireTime FAILS CLOSED (treated as
 *   in-window ⇒ SMS suppressed) — the TCPA-safe direction; the email floor is unaffected.
 *
 * ── Timezone resolution (SEAM-1 drops tenantPrefs) ──
 *   §E3 specifies `booking.timezone → tenant scheduling.timezone → UTC`. SEAM-1's params do
 *   not carry tenant prefs, so this module does only `booking.timezone || 'UTC'`. The CALLER
 *   (WS-E-REMIND's fire-time call-site) owns resolving the tenant-scheduling-timezone middle
 *   hop INTO `booking.timezone` before calling — keeping this module pure. `tenantId` is part
 *   of the locked SEAM-1 signature (caller symmetry + log context); it carries no I/O here.
 */

// v1 fixed quiet-hours window (SEAM-1): 8pm–8am, volunteer-local. Hour granularity —
// "8pm–8am" is on-the-hour, so 20:00:00–07:59:59 is quiet, 08:00 onward is allowed.
const DEFAULT_QUIET_HOURS = Object.freeze({ startHour: 20, endHour: 8 });

// ─── consent (fail-closed) ──────────────────────────────────────────────────────────────

function consentValid(consentRecord) {
  return (
    !!consentRecord &&
    consentRecord.consent_given === true &&
    !consentRecord.opted_out_at
  );
}

// ─── quiet-hours (volunteer-local, fire-time) ───────────────────────────────────────────

// The volunteer's local hour-of-day [0..23] at `fireTime` in `timezone`, via native Intl.
// Returns null if the timezone or fireTime is unresolvable (caller fails closed).
function localHour(fireTime, timezone) {
  const d = new Date(fireTime);
  if (Number.isNaN(d.getTime())) return null;
  try {
    const hourStr = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone,
      hour: 'numeric',
      hour12: false,
    }).format(d);
    // A valid IANA tz always yields a numeric hour. Intl can render midnight as '24'
    // in some ICU builds — normalize to 0.
    return parseInt(hourStr, 10) % 24;
  } catch {
    // Invalid IANA timezone ⇒ unresolvable.
    return null;
  }
}

// True when fireTime falls inside the quiet window, evaluated in the volunteer's tz.
// Fails CLOSED: an unresolvable tz/fireTime ⇒ true (in-window ⇒ SMS suppressed).
function inQuietHours(fireTime, timezone, quietHours = DEFAULT_QUIET_HOURS) {
  const tz = timezone || 'UTC';
  const h = localHour(fireTime, tz);
  if (h == null) return true; // fail-closed
  const { startHour, endHour } = quietHours;
  // Wrap-around window (start > end, e.g. 20→08) vs same-day window (start <= end).
  return startHour > endHour
    ? h >= startHour || h < endHour
    : h >= startHour && h < endHour;
}

// ─── selectChannels ─────────────────────────────────────────────────────────────────────

/**
 * @param {{ tenantId?: string, booking?: object, orgSmsEnabled?: boolean,
 *           consentRecord?: object|null, quietHours?: {startHour:number,endHour:number},
 *           fireTime?: Date|string|number }} args
 *   - tenantId: part of the locked SEAM-1 signature; unused in the pure decision (caller
 *     symmetry + log context). The caller pre-resolves the tenant-tz middle hop into
 *     booking.timezone (see "Timezone resolution" above).
 * @param {object} [deps] - reserved for log; the decision itself is pure.
 * @returns {{ email: true, sms: boolean }}
 */
function selectChannels(
  { tenantId, booking, orgSmsEnabled, consentRecord, quietHours, fireTime } = {},
  deps = {}
) {
  const sms =
    orgSmsEnabled === true &&
    consentValid(consentRecord) &&
    !inQuietHours(fireTime, booking && booking.timezone, quietHours);

  // EMAIL is the floor — always available.
  return { email: true, sms };
}

module.exports = {
  selectChannels,
  // exported for unit coverage + REMIND/ATTEND reuse:
  consentValid,
  inQuietHours,
  localHour,
  DEFAULT_QUIET_HOURS,
};
