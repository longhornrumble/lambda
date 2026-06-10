'use strict';

/**
 * phone.js — E.164 phone normalization (single source of truth).
 *
 * Extracted from consent.js (Track 1 S3) so the consent WRITER (consent.js, which
 * require()s @aws-sdk) and the reminder-dispatcher READER (Scheduled_Message_Sender, a lean
 * reader-only Lambda) share ONE implementation instead of byte-parity copies that could drift
 * and silently mis-key the consent lookup. PURE — zero deps — so a reader can import it
 * without dragging the AWS-touching consent writer into its bundle.
 *
 * The consent store keys SMS consent on sk=CONSENT#transactional#{E.164}; writer and every
 * reader MUST normalize identically or the lookup misses (fail-closed → SMS suppressed).
 */

// Returns the E.164 string, or null if it cannot be normalized to a valid E.164 number.
// Mirrors form_handler.js writeConsentRecord: a bare 10-digit US number gets a +1; an
// already-+-prefixed number keeps its country code; an 11-digit number with the leading US
// country code (leading 1) keeps it (just prepends +) rather than double-prefixing. Anything
// outside +<10..15 digits> is rejected (null) — never written.
function toE164(raw) {
  if (typeof raw !== 'string' || raw.trim() === '') return null;
  const trimmed = raw.trim();
  let phone;
  if (trimmed.startsWith('+')) {
    phone = trimmed;
  } else {
    const digits = trimmed.replace(/\D/g, '');
    phone =
      digits.length === 11 && digits.startsWith('1')
        ? `+${digits}`
        : `+1${digits}`;
  }
  return /^\+\d{10,15}$/.test(phone) ? phone : null;
}

module.exports = { toE164 };
