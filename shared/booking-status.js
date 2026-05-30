'use strict';

/**
 * booking-status.js — canonical Booking.status vocabulary (single source of truth).
 *
 * Scheduling sub-phase B Task CI-3c (stub). Every Lambda that reads or writes
 * `Booking.status` MUST import these constants rather than hardcoding string
 * literals, so the vocabulary cannot drift (e.g. `cancelled` vs the canonical
 * `canceled`) across separately-shipped Lambdas.
 *
 * The five values are the ONLY legal `Booking.status` values in v1, per the
 * canonical design (`scheduling/docs/scheduling_design.md`):
 *   - §"States intentionally NOT first-class": `completed` is terminal on
 *     Booking.status; `pending_attendance`/`booking_pending`/etc. are
 *     ConversationSchedulingSession states, NOT Booking.status.
 *   - §11.2 disposition: `Booking.status` resolves to `completed`, `no_show`,
 *     or `coordinator_no_show`.
 *   - §"Cancel goes through the calendar": cancel ⇒ `Booking.status = canceled`.
 *
 * Booking rows are created in `booked` (C8 commit); there is no pre-booked
 * Booking.status (the row does not exist until committed).
 *
 * CI-3c is a STUB at sub-phase B: it locks this vocabulary. The full
 * state-machine *transition table* contract — asserting every consumer handles
 * every reachable transition — lands with the first transition consumer (C9),
 * where this module gains a `TRANSITIONS` map and the contract test graduates
 * from "vocabulary lock" to "transition-table consistency across all Lambdas
 * that read/write Booking.status".
 */

// Frozen so a consumer can't accidentally mutate the shared vocabulary.
const BOOKING_STATUSES = Object.freeze([
  'booked',              // committed + active (set at C8 commit)
  'canceled',            // terminal — volunteer or coordinator cancel (single 'l', US spelling)
  'completed',           // terminal — attended
  'no_show',             // terminal — volunteer did not attend
  'coordinator_no_show', // terminal — coordinator did not attend
]);

function isBookingStatus(value) {
  return typeof value === 'string' && BOOKING_STATUSES.includes(value);
}

module.exports = {
  BOOKING_STATUSES,
  isBookingStatus,
};
