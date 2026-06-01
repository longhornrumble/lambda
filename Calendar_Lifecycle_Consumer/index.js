'use strict';

/**
 * Calendar_Lifecycle_Consumer — scheduling §14.2 calendar-as-SoR reconciliation consumer.
 *
 * The second SQS consumer of the typed `booking.*` events the `Calendar_Watch_Listener`
 * (B2) dispatches — see `scheduling/docs/listener_dispatch_interface.md`. It owns exactly
 * the FOUR coordinator-side calendar-lifecycle events the sibling `Calendar_Event_Consumer`
 * deliberately skips:
 *
 *   - `booking.calendar_deleted`     → cancel the booking (`cancel_reason=coordinator_deleted`)
 *                                       + TODO(Y) volunteer reschedule-link notice.
 *   - `booking.calendar_moved` (v1)  → cancel (`cancel_reason=coordinator_moved`) + self-anchor
 *                                       `rescheduleOfBookingId`; NO auto-rebook (deferred);
 *                                       + TODO(Y) reschedule path.
 *   - `booking.calendar_reassigned`  → repoint `resource_id`/`coordinator_email`; NO notify (§5.1).
 *   - `booking.event_made_private`   → degrade the channels-table row (NOT a Booking write)
 *                                       + admin alert. (channel_id contract gap — see channel-degrade.js.)
 *
 * Every OTHER event type (`ooo_overlap_detected` / `attendee_accepted` / `attendee_declined`)
 * belongs to the sibling consumer — it is logged and discarded here (NOT sent to the DLQ).
 * The integrator wires an SNS fan-out (or per-type queues) with an `event_type` filter
 * policy so only the four owned types reach this Lambda; the default-discard is
 * belt-and-suspenders so a loose filter never DLQ-storms a non-owned type.
 *
 * Idempotency (dispatch-interface "Idempotency Expectations"): SQS is at-least-once and
 * Google may resend a push. Each action is made idempotent by a CONDITIONAL write (Booking
 * row / channels row) keyed on its dedupe anchor — re-processing the same event is a no-op
 * with the same outcome, without provisioning a separate dedupe table.
 *
 * Error contract (dispatch-interface "Error Contract"): a malformed/invalid payload OR a
 * genuine downstream failure marks that record as a batch-item failure so SQS redrives it
 * (→ DLQ after max-receive-count). The Lambda does NOT crash on one bad record. **Integrator
 * dependency:** the event-source-mapping MUST set
 * `FunctionResponseTypes: ['ReportBatchItemFailures']`, or a non-empty `batchItemFailures`
 * return is ignored and failures will not redrive.
 *
 * Consumes the frozen `shared/booking-status` SoT (via booking-store.js); never modifies it
 * (FROZEN_CONTRACTS §C: a wrong contract is FLAGGED, not forked).
 */

const reconcile = require('./booking-reconcile');
const channelDegrade = require('./channel-degrade');

const EVENT_DELETED = 'booking.calendar_deleted';
const EVENT_MOVED = 'booking.calendar_moved';
const EVENT_REASSIGNED = 'booking.calendar_reassigned';
const EVENT_PRIVATE = 'booking.event_made_private';

// ─── structured logging (Calendar_Watch_* / sibling-consumer convention) ─────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── envelope parsing (schema-tolerant on optional fields; strict on the basis) ──────────

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

// SR-2 PII-log hygiene (sub-phase B audit): a malformed envelope may still be valid JSON
// carrying coordinator emails (`calendar_reassigned` does, via previous/new_resource_id).
// NEVER log the raw body — strip the PII-bearing fields, or mark it unparseable.
function redactBody(body) {
  let obj;
  try {
    obj = JSON.parse(body);
  } catch (_) {
    return '[unparseable]';
  }
  if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
    const {
      attendee_email, coordinator_email, previous_resource_id, new_resource_id, ...rest
    } = obj;
    return rest;
  }
  return obj; // primitive / array — no named PII fields to strip
}

// ─── per-record router ────────────────────────────────────────────────────────────────

async function processRecord(record) {
  const env = parseEnvelope(record.body);
  if (typeof env.event_type !== 'string' || env.event_type.length === 0) {
    throw malformed('envelope missing required field(s): event_type');
  }

  switch (env.event_type) {
    case EVENT_DELETED:
      return reconcile.reconcileDeleted(env);
    case EVENT_MOVED:
      return reconcile.reconcileMoved(env);
    case EVENT_REASSIGNED:
      return reconcile.reconcileReassigned(env);
    case EVENT_PRIVATE:
      return channelDegrade.degradeOnEventPrivate(env);
    default:
      // Not owned by this consumer (the sibling owns ooo_overlap / attendee_*). Log +
      // discard — do NOT DLQ (another consumer owns it).
      log('event_skipped_not_consumed', {
        event_type: env.event_type, tenant_id: env.tenant_id, booking_id: env.booking_id,
      });
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
  redactBody,
};
