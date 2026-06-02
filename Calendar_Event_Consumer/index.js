'use strict';

/**
 * Calendar_Event_Consumer — scheduling sub-phase B Tasks B9 + B10.
 *
 * SQS consumer of the typed `booking.*` events the `Calendar_Watch_Listener` (B2)
 * dispatches — see `scheduling/docs/listener_dispatch_interface.md`. This Lambda owns
 * exactly TWO of the seven event types:
 *
 *   - `booking.ooo_overlap_detected`  (B9, canonical §14.2) — a coordinator OOO event
 *     overlaps ≥1 `booked` booking. For EVERY booking in `overlapping_booking_ids`
 *     (NOT just the envelope `booking_id` — that is the non-deterministic first of the
 *     GSI page, per the dispatch-interface OOO consumer-guidance note): flag the
 *     conflict on the Booking row + fire an admin alert.
 *   - `booking.attendee_declined`     (B10, canonical §14.2) — volunteer declined the
 *     calendar invite → transition `Booking.status = canceled` (conditional, idempotent).
 *
 * Every OTHER event type (`calendar_deleted` / `calendar_moved` / `calendar_reassigned`,
 * `attendee_accepted`, `event_made_private`) is NOT this consumer's concern — it is
 * logged and discarded (NOT sent to the DLQ; another consumer/workstream owns it). The
 * Lambda never throws on a non-owned type, so the dispatch topology (sole consumer vs.
 * SNS fan-out vs. per-consumer queues) stays the integrator's decision without a code
 * change here.
 *
 * Idempotency (dispatch-interface "Idempotency Expectations"): SQS is at-least-once and
 * Google may resend a push. Rather than a separate processed-events table (no frozen
 * contract exists for one; the integrator wires only queue + event-source-mapping + DLQ
 * + IAM), each action is made idempotent by a CONDITIONAL write on the Booking row
 * itself, keyed on the dedupe basis. Re-processing the same (event, calendar-mutation)
 * is a no-op with the same outcome — the contract's idempotency requirement is met
 * without provisioning a dedupe table.
 *
 * Error contract (dispatch-interface "Error Contract"): a malformed/invalid payload OR a
 * genuine downstream failure marks that record as a batch-item failure so SQS redrives
 * it (→ DLQ after max-receive-count). The Lambda does NOT crash on one bad record — the
 * rest of the batch still processes. **Integrator dependency:** the event-source-mapping
 * MUST set `FunctionResponseTypes: ['ReportBatchItemFailures']`, or a non-empty
 * `batchItemFailures` return is ignored and failures will not redrive.
 *
 * B9 volunteer reoffer (gap C, wired 2026-06-02): on a flagged OOO conflict, re-resolve
 * the pool from the booking's appointment_type (X = resolveCandidates), and if a candidate
 * can still serve it, send the volunteer a §13.4 signed reschedule link (Y =
 * dispatchVolunteerNotice, kind 'reoffer'). Best-effort — the durable conflict flag + admin
 * alert remain the safety mechanism; a reoffer failure never redrives the record.
 *
 * OUT OF SCOPE (operator decision 2026-05-31):
 *   - B10 reminder-suppression + defensive `responseStatus` poll → `TODO(E)` (sub-phase E
 *     does not exist yet).
 *
 * Consumes the frozen `shared/booking-status` SoT (via booking-updates.js); never
 * modifies it (FROZEN_CONTRACTS §C: a wrong contract is FLAGGED, not forked).
 */

const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const { sdkConfig } = require('./aws-client-config');
const bookingUpdates = require('./booking-updates');
const { resolveCandidates } = require('../shared/scheduling/candidate-resolver'); // (X) §B7
const { dispatchVolunteerNotice } = require('../shared/scheduling/notify'); // (Y) §B8
const { sign } = require('../shared/scheduling/tokens'); // §13.4 signed reschedule link

const OPS_ALERTS_TOPIC_ARN = process.env.OPS_ALERTS_TOPIC_ARN || '';
const SCHEDULE_BASE_URL = process.env.SCHEDULE_BASE_URL || 'https://schedule.myrecruiter.ai';
const sns = new SNSClient(sdkConfig());

const EVENT_OOO_OVERLAP = 'booking.ooo_overlap_detected';
const EVENT_ATTENDEE_DECLINED = 'booking.attendee_declined';
const EVENT_ATTENDEE_ACCEPTED = 'booking.attendee_accepted';

// ─── structured logging (Calendar_Watch_* / C8 convention) ────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── envelope parsing (schema-tolerant on optional fields; strict on the basis) ───────

// A malformed payload is tagged so the handler routes it to the DLQ rather than
// retrying a record that can never succeed.
function malformed(message) {
  const err = new Error(message);
  err.malformed = true;
  return err;
}

function parseEnvelope(body) {
  let env;
  try {
    env = JSON.parse(body);
  } catch (_) {
    throw malformed('SQS message body is not valid JSON');
  }
  if (!env || typeof env !== 'object' || Array.isArray(env)) {
    throw malformed('SQS message body is not a JSON object');
  }
  return env;
}

// Require non-empty string fields. Per schema discipline this is applied ONLY to the
// fields an action actually needs (the producer's required envelope keys) — optional
// fields are tolerated when absent.
function requireStrings(env, fields) {
  const missing = fields.filter((f) => typeof env[f] !== 'string' || env[f].length === 0);
  if (missing.length) {
    throw malformed(`envelope missing required field(s): ${missing.join(', ')}`);
  }
}

// SR-2 PII-log hygiene (sub-phase B audit): a malformed envelope may still be valid JSON
// carrying `attendee_email` (declined/accepted events do). NEVER log the raw body — strip
// the PII-bearing fields before logging, or mark it unparseable when it is not JSON.
function redactBody(body) {
  let obj;
  try {
    obj = JSON.parse(body);
  } catch (_) {
    return '[unparseable]';
  }
  if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
    const { attendee_email, coordinator_email, ...rest } = obj;
    return rest;
  }
  return obj; // primitive / array — no named PII fields to strip
}

// ─── admin alert (mirrors C8 alertAdmin — best-effort, never fails the record) ────────
//
// The durable conflict flag on the Booking row is the source of truth; the SNS alert is
// a best-effort notification on top of it. Swallowing the SNS error (and NOT failing the
// record) keeps idempotency clean: the flag is the dedupe anchor, so a redrive would not
// re-alert anyway. A flagged-but-un-alerted conflict is still discoverable on the row.
async function alertAdmin(subject, detail) {
  if (!OPS_ALERTS_TOPIC_ARN) {
    warn('admin_alert_skipped_no_topic', { subject });
    return;
  }
  try {
    await sns.send(new PublishCommand({
      TopicArn: OPS_ALERTS_TOPIC_ARN,
      Subject: String(subject).slice(0, 100),
      Message: JSON.stringify(detail),
    }));
  } catch (err) {
    warn('admin_alert_failed', { subject, error: err.message });
  }
}

// ─── B9 reoffer (gap C): proactively offer the volunteer a new time (§14.2) ───────────
// Called per flagged booking. Best-effort value-add on top of the durable conflict flag:
//   1. read the attendee/appt context (the OOO envelope carries none),
//   2. (X) re-resolve the pool from the booking's appointment_type — if NO candidate can
//      serve it, do NOT send a false-hope reoffer (the conflict flag + admin alert stand),
//   3. mint a §13.4 signed reschedule link (the volunteer re-picks; the pool re-runs fresh
//      at that point — we don't pre-compute slots, matching the (Y) reoffer template),
//   4. (Y) send the reoffer notice (Y is itself best-effort + agent-of-CoR guarded).
async function sendReoffer({ tenantId, bookingId }) {
  const ctx = await bookingUpdates.getReofferContext({ tenantId, bookingId });
  if (!ctx || !ctx.attendeeEmail) {
    warn('reoffer_skipped_no_attendee_email', { tenant_id: tenantId, booking_id: bookingId });
    return;
  }
  if (!ctx.appointmentTypeId) {
    warn('reoffer_skipped_no_appointment_type', { tenant_id: tenantId, booking_id: bookingId });
    return;
  }
  if (!ctx.startAt) {
    // No start_at → sign('reschedule', {start_at: null}) would throw on expiry compute.
    // Skip explicitly (consistent with the guards above; schema-discipline forward-compat).
    warn('reoffer_skipped_no_start_at', { tenant_id: tenantId, booking_id: bookingId });
    return;
  }

  // (X) gate — only offer a new time if the pool can still serve this appointment type.
  let candidates = [];
  try {
    candidates = await resolveCandidates({ tenantId, appointmentTypeId: ctx.appointmentTypeId });
  } catch (err) {
    warn('reoffer_resolve_candidates_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
    return;
  }
  if (!candidates.length) {
    log('reoffer_skipped_no_candidates', { tenant_id: tenantId, booking_id: bookingId });
    return;
  }

  // (§13.4) signed reschedule link — same claims shape C8 mints for the confirmation email.
  const token = await sign('reschedule', {
    tenant_id: tenantId,
    booking_id: bookingId,
    start_at: ctx.startAt,
  });
  const reofferUrl = `${SCHEDULE_BASE_URL}/reschedule?t=${encodeURIComponent(token)}`;

  // (Y) best-effort reoffer notice (no PII in our own logs — booking_id only).
  const result = await dispatchVolunteerNotice({
    kind: 'reoffer',
    tenantId,
    booking: {
      booking_id: bookingId,
      attendee_email: ctx.attendeeEmail,
      attendee_name: ctx.attendeeName,
      reoffer_url: reofferUrl,
    },
  });
  log('reoffer_dispatched', {
    tenant_id: tenantId,
    booking_id: bookingId,
    suppressed: Boolean(result && result.suppressed),
    email_dispatched: Boolean(result && result.dispatched && result.dispatched.email),
  });
}

// ─── B9: coordinator OOO event overlaps booked appointments (§14.2) ───────────────────

async function handleOooOverlap(env) {
  requireStrings(env, ['tenant_id', 'last_calendar_mutation_at']);
  const tenantId = env.tenant_id;
  const mutationAt = env.last_calendar_mutation_at;

  // Act on the FULL overlapping_booking_ids array — the dispatch-interface OOO
  // consumer-guidance note: the envelope `booking_id` is only the non-deterministic
  // first of the GSI page, NOT a priority. Fall back to the single `booking_id` when the
  // array is absent (schema discipline — tolerate the missing field on old-shape events).
  const bookingIds = Array.isArray(env.overlapping_booking_ids) && env.overlapping_booking_ids.length
    ? env.overlapping_booking_ids.filter((id) => typeof id === 'string' && id.length)
    : (typeof env.booking_id === 'string' && env.booking_id.length ? [env.booking_id] : []);

  if (!bookingIds.length) {
    throw malformed('ooo_overlap_detected has no usable overlapping_booking_ids or booking_id');
  }

  let flaggedCount = 0;
  for (const bookingId of bookingIds) {
    const flagged = await bookingUpdates.flagOooConflict({
      tenantId,
      bookingId,
      mutationAt,
      oooStartAt: env.ooo_start_at,
      oooEndAt: env.ooo_end_at,
    });
    if (flagged) {
      flaggedCount += 1;
      await alertAdmin('Scheduling: OOO conflict on booked appointment', {
        kind: EVENT_OOO_OVERLAP,
        tenant_id: tenantId,
        booking_id: bookingId,
        ooo_start_at: env.ooo_start_at,
        ooo_end_at: env.ooo_end_at,
      });
      // (reoffer) gap C — the two contracts the original TODO was blocked on now exist:
      // (X) resolveCandidates resolves the pool from the booking's appointment_type_id
      // (no routing_policy_id on the row), and (Y) dispatchVolunteerNotice is the reoffer
      // dispatch. Best-effort: the durable conflict flag + admin alert above are the
      // safety mechanism; a reoffer failure must NOT redrive the record (which would only
      // re-flag/re-alert idempotently). So this is wrapped + swallowed.
      try {
        await sendReoffer({ tenantId, bookingId });
      } catch (err) {
        warn('reoffer_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
      }
    } else {
      // {absent | not 'booked' | already flagged for THIS calendar-mutation} — idempotent
      // no-op; the admin alert fires at-most-once per (booking, calendar-mutation).
      log('ooo_overlap_skip_or_dedupe', { tenant_id: tenantId, booking_id: bookingId, mutation_at: mutationAt });
    }
  }
  log('ooo_overlap_processed', {
    tenant_id: tenantId,
    bookings: bookingIds.length,
    flagged: flaggedCount,
    mutation_at: mutationAt,
  });
}

// ─── B10: volunteer declined the calendar invite (§14.2) ──────────────────────────────

async function handleAttendeeDeclined(env) {
  requireStrings(env, ['tenant_id', 'booking_id']);
  const tenantId = env.tenant_id;
  const bookingId = env.booking_id;

  const canceled = await bookingUpdates.cancelOnDecline({ tenantId, bookingId });
  if (canceled) {
    log('attendee_declined_canceled', { tenant_id: tenantId, booking_id: bookingId });
    // TODO(E) — suppress upcoming reminders + add the defensive responseStatus poll at
    // reminder-send time (canonical §14.2). OUT OF SCOPE: sub-phase E (reminders) does
    // not exist yet (operator decision 2026-05-31). Per §14.2 there is intentionally NO
    // platform-side volunteer notification on decline — the volunteer just declined, and
    // the coordinator gets Google's native attendee-response email.
  } else {
    // Already canceled / terminal / absent — conditional made re-delivery a no-op.
    log('attendee_declined_noop', { tenant_id: tenantId, booking_id: bookingId });
  }
}

// ─── per-record router ────────────────────────────────────────────────────────────────

async function processRecord(record) {
  const env = parseEnvelope(record.body);
  requireStrings(env, ['event_type']);

  switch (env.event_type) {
    case EVENT_OOO_OVERLAP:
      return handleOooOverlap(env);
    case EVENT_ATTENDEE_DECLINED:
      return handleAttendeeDeclined(env);
    case EVENT_ATTENDEE_ACCEPTED:
      // B10 covers accept/decline detection, but an accept causes NO Booking.status
      // change (the row is already `booked`). Acknowledge explicitly so it is a
      // deliberate no-op, not a silent skip.
      log('attendee_accepted_noop', { tenant_id: env.tenant_id, booking_id: env.booking_id });
      return undefined;
    default:
      // Not owned by this consumer. Log + discard (do NOT DLQ — another consumer owns it).
      log('event_skipped_not_consumed', { event_type: env.event_type, tenant_id: env.tenant_id, booking_id: env.booking_id });
      return undefined;
  }
}

// ─── SQS batch entry point (partial-batch response) ───────────────────────────────────

async function handler(event) {
  const records = (event && Array.isArray(event.Records)) ? event.Records : [];
  const batchItemFailures = [];

  for (const record of records) {
    try {
      await processRecord(record);
    } catch (err) {
      if (err && err.malformed) {
        // Log the (PII-redacted) payload + validation error (error contract) before redriving.
        warn('event_malformed', { message_id: record.messageId, error: err.message, body: redactBody(record.body) });
      } else {
        warn('event_processing_failed', { message_id: record.messageId, error: err && err.message });
      }
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
}

module.exports = {
  handler,
  // exported for unit tests
  processRecord,
  parseEnvelope,
};
