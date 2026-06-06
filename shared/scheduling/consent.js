'use strict';

/**
 * consent.js — booking-time SMS opt-in consent-writer (WS-E-TCPA).
 *
 * FROZEN_CONTRACTS §E3 (TCPA consent gate) + SEAM-2 (opt-in capture). The volunteer
 * opts into transactional SMS at the Booking-commit CONFIRM path; this module is the
 * single WRITE-ONLY primitive that records that consent. It does NOT send SMS — the
 * confirmation/opt-in message (sendType:'contact', with the STOP/HELP footer) is the
 * integrator-wired BCH call-site's concern (SEAM-2: "The BCH call-site is integrator-
 * wired glue"). This module owns the RECORD only.
 *
 *   recordBookingSmsConsent({ tenantId, phone, bookingId, consentLanguage, source }, deps)
 *     → { written: boolean, reason?: string, phone_e164?: string }
 *
 * ── Why the shipped shape is reused verbatim (SEAM-2) ──
 *   The shipped SMS_Sender consent gate (SMS_Sender/index.mjs) does a GetItem on
 *   `pk=TENANT#{tenantId}` · `sk=CONSENT#transactional#{phoneE164}` and sends only when
 *   `consent_given !== false && !opted_out_at`. To make a booking opt-in ACTUALLY gate
 *   the send, this writer MUST land on that exact key. It is therefore the same record
 *   shape the BSH form path writes (form_handler.js writeConsentRecord) — only the
 *   provenance fields differ (`consent_method:'scheduling_booking'`, `booking_id`) and
 *   `ttl` is set (§E3: TTL = now + 4yr + 30d). One consent record per (tenant, phone)
 *   covers all four moments (confirmation/reminder/cancel/reschedule) — §E3.
 *
 * ── E.164-before-write ──
 *   The phone is normalized to E.164 and VALIDATED before any write. An un-normalizable
 *   number is NEVER written (returns { written:false, reason:'invalid_phone' }) — a bad
 *   number on the consent record would either mis-key the gate or be un-revocable.
 *
 * ── Idempotent / immutable ──
 *   Conditional put (`attribute_not_exists(pk)`): an existing consent record is NOT
 *   overwritten (consent is immutable once given; a prior form opt-in already covers the
 *   phone). ConditionalCheckFailed → { written:false, reason:'already_exists' } (success,
 *   not an error — the volunteer is already opted in).
 *
 * ── throw vs best-effort (mirrors §B8 notify.js) ──
 *   A missing `tenantId`/`phone` is a CALLER CONTRACT bug → throws. A transport/DDB
 *   failure is BEST-EFFORT → caught, logged PII-redacted (tenant + booking only, never
 *   the phone), returns { written:false, reason:'write_failed' }. Rationale: the booking
 *   already committed; a failed consent write must never roll it back — and it FAILS SAFE
 *   (no record ⇒ selectChannels reads no consent ⇒ SMS suppressed ⇒ email floor stands).
 *
 * ── DI seam ──
 *   `deps.putConsent` / `deps.now` / `deps.log` are injectable; the module is fully
 *   unit-testable without AWS. The default `putConsent` is the only AWS-touching code.
 */

const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

// Created once at module load; reused across warm invocations.
const ddb = new DynamoDBClient({});

const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';

// TCPA retention window (§E3): auto-expire 4 years + 30 days after capture. The +30d is
// a grace beyond the 4-yr retention floor; epoch SECONDS for the DynamoDB TTL attribute.
const CONSENT_TTL_SECONDS = (4 * 365 + 30) * 24 * 60 * 60;

// ─── phone normalization (E.164-before-write) ──────────────────────────────────────────

// Returns the E.164 string, or null if it cannot be normalized to a valid E.164 number.
// Mirrors form_handler.js writeConsentRecord: a bare 10-digit US number gets a +1; an
// already-+-prefixed number keeps its country code. Anything outside +<10..15 digits> is
// rejected (null) — never written.
function toE164(raw) {
  if (typeof raw !== 'string' || raw.trim() === '') return null;
  const trimmed = raw.trim();
  const phone = trimmed.startsWith('+')
    ? trimmed
    : `+1${trimmed.replace(/\D/g, '')}`;
  return /^\+\d{10,15}$/.test(phone) ? phone : null;
}

// ─── default DI implementation (the only AWS-touching code) ─────────────────────────────

// Conditional PutItem against picasso-sms-consent. Returns true on write, false if a
// record already exists (ConditionalCheckFailed). Any other error propagates to the
// caller's best-effort catch.
async function defaultPutConsent(item) {
  try {
    await ddb.send(
      new PutItemCommand({
        TableName: SMS_CONSENT_TABLE,
        Item: {
          pk: { S: item.pk },
          sk: { S: item.sk },
          phone_e164: { S: item.phone_e164 },
          consent_given: { BOOL: true },
          consent_timestamp: { S: item.consent_timestamp },
          consent_method: { S: item.consent_method },
          consent_language: { S: item.consent_language },
          consent_type: { S: item.consent_type },
          opted_out_at: { NULL: true },
          opt_out_source: { NULL: true },
          booking_id: { S: item.booking_id },
          created_at: { S: item.created_at },
          updated_at: { S: item.updated_at },
          ttl: { N: String(item.ttl) },
        },
        ConditionExpression: 'attribute_not_exists(pk)',
      })
    );
    return true;
  } catch (err) {
    if (err.name === 'ConditionalCheckFailedException') {
      return false;
    }
    throw err;
  }
}

// ─── recordBookingSmsConsent ────────────────────────────────────────────────────────────

/**
 * @param {{ tenantId: string, phone: string, bookingId?: string, consentLanguage?: string, source?: string }} args
 * @param {object} [deps] - { putConsent, now, log }
 * @returns {Promise<{ written: boolean, reason?: string, phone_e164?: string }>}
 */
async function recordBookingSmsConsent(
  { tenantId, phone, bookingId, consentLanguage, source } = {},
  deps = {}
) {
  const {
    putConsent = defaultPutConsent,
    now = () => Date.now(),
    log = console,
  } = deps;

  // Caller-contract bugs throw (mirrors §B8 notify.js). Distinct from the data-driven
  // invalid-phone case below, which is a soft skip (the phone came from user input).
  if (!tenantId) {
    throw new Error('recordBookingSmsConsent: tenantId is required');
  }
  if (phone == null) {
    throw new Error('recordBookingSmsConsent: phone is required');
  }

  const e164 = toE164(phone);
  if (!e164) {
    // NEVER write a non-E.164 number. Fail-safe: no record ⇒ no SMS consent.
    log.warn(
      `[consent] invalid phone — not writing consent (tenant=${tenantId} booking=${bookingId || 'unknown'})`
    );
    return { written: false, reason: 'invalid_phone' };
  }

  const nowIso = new Date(now()).toISOString();
  const ttl = Math.floor(now() / 1000) + CONSENT_TTL_SECONDS;

  const item = {
    pk: `TENANT#${tenantId}`,
    // The gate-activating key SMS_Sender reads (SEAM-2 — reuse the shipped shape).
    sk: `CONSENT#transactional#${e164}`,
    phone_e164: e164,
    consent_timestamp: nowIso,
    // Provenance: distinguishes a booking opt-in from a web-form opt-in.
    consent_method: source || 'scheduling_booking',
    consent_language: consentLanguage || '',
    consent_type: 'transactional',
    booking_id: bookingId || 'unknown',
    created_at: nowIso,
    updated_at: nowIso,
    ttl,
  };

  try {
    const created = await putConsent(item);
    if (!created) {
      // Already opted in — not an error; the existing record (form or prior booking) stands.
      return { written: false, reason: 'already_exists', phone_e164: e164 };
    }
    return { written: true, phone_e164: e164 };
  } catch (err) {
    // Best-effort: never propagate a transport failure. PII-redacted (no phone in the log).
    log.error(
      `[consent] write failed (tenant=${tenantId} booking=${bookingId || 'unknown'}): ${err.message}`
    );
    return { written: false, reason: 'write_failed' };
  }
}

module.exports = {
  recordBookingSmsConsent,
  // exported for unit coverage + reuse:
  toE164,
  defaultPutConsent,
  CONSENT_TTL_SECONDS,
  _SMS_CONSENT_TABLE: SMS_CONSENT_TABLE,
};
