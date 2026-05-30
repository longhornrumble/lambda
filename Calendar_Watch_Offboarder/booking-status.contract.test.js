'use strict';

/**
 * CI-3c (Booking state-machine contract test) — STUB at sub-phase B.
 *
 * Per the implementation plan B-table CI-3c row: "Asserts the transition table
 * is consistent across all Lambdas that read or write Booking.status. Lands when
 * first state-machine consumer code lands (likely C9, but stub the test in B for
 * the states already known)."
 *
 * At sub-phase B no Lambda implements Booking.status *transitions* yet (the
 * Listener only READS `booked`; C8 writes the first row, C9 owns the transition
 * table). So this stub locks the VOCABULARY only: it pins the canonical
 * `shared/booking-status.js` set so that when C8/C9 land, any drift (a new or
 * mis-spelled status) trips CI. The transition-table assertion is added here
 * when C9's state machine lands.
 *
 * Co-located in the Offboarder suite because that is a CI-wired node-tests job;
 * a change to `shared/` forces every Lambda's test job (incl. this one), so the
 * tripwire fires regardless of which consumer introduces a new status.
 */

const { BOOKING_STATUSES, isBookingStatus } = require('../shared/booking-status');

describe('CI-3c stub: Booking.status canonical vocabulary', () => {
  test('is exactly the five canonical states (locks the vocabulary)', () => {
    expect([...BOOKING_STATUSES].sort()).toEqual(
      ['booked', 'canceled', 'completed', 'coordinator_no_show', 'no_show']
    );
  });

  test('uses the canonical US spelling "canceled" (not "cancelled")', () => {
    expect(BOOKING_STATUSES).toContain('canceled');
    expect(BOOKING_STATUSES).not.toContain('cancelled');
  });

  test('is frozen so consumers cannot mutate the shared vocabulary', () => {
    expect(Object.isFrozen(BOOKING_STATUSES)).toBe(true);
  });

  test('isBookingStatus accepts canonical values and rejects everything else', () => {
    for (const s of BOOKING_STATUSES) {
      expect(isBookingStatus(s)).toBe(true);
    }
    for (const bad of ['cancelled', 'pending', 'confirmed', 'BOOKED', '', null, undefined, 42]) {
      expect(isBookingStatus(bad)).toBe(false);
    }
  });

  test('the only sub-phase-B consumer reads a canonical status (Listener OOO query)', () => {
    // The Listener's OOO-overlap query filters Booking rows on status === 'booked'.
    // That literal must be a canonical value; when C8/C9 land they import
    // BOOKING_STATUSES instead of hardcoding, and this assertion is replaced by
    // the full transition-table contract.
    expect(isBookingStatus('booked')).toBe(true);
  });
});
