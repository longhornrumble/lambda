'use strict';

/**
 * CI-3c (Booking state-machine contract test) — co-located in the B11 suite.
 *
 * Per the implementation-plan CI-3c row, the canonical `Booking.status` vocabulary is
 * the single source of truth; every Lambda that reads or writes a status literal must
 * trip CI if that vocabulary drifts. B11's booking-store filters stranded rows on
 * `status === 'booked'` and reasons about the terminal `canceled` state (handling (b)
 * delegates the actual write to the §14.2 path). This test locks the literals B11
 * depends on against `shared/booking-status.js`.
 *
 * Mirrors the Offboarder's CI-3c stub: a change to `shared/` forces this Lambda's test
 * job (once the integrator adds it + shared/** to the CI detect-changes filter), so the
 * vocabulary tripwire fires regardless of which consumer introduces a new status.
 */

const { BOOKING_STATUSES, isBookingStatus } = require('../shared/booking-status');

describe('CI-3c: Booking.status vocabulary B11 depends on', () => {
  test('is exactly the five canonical states', () => {
    expect([...BOOKING_STATUSES].sort()).toEqual(
      ['booked', 'canceled', 'completed', 'coordinator_no_show', 'no_show']
    );
  });

  test('uses the canonical US spelling "canceled" (not "cancelled")', () => {
    expect(BOOKING_STATUSES).toContain('canceled');
    expect(BOOKING_STATUSES).not.toContain('cancelled');
  });

  test("B11's stranded-set filter literal 'booked' is canonical", () => {
    // booking-store.findStrandedBookings keeps only status === 'booked' rows.
    expect(isBookingStatus('booked')).toBe(true);
  });

  test("the cancel handling's terminal target 'canceled' is canonical", () => {
    // Handling (b) deletes the event so the §14.2 path sets status = 'canceled'.
    expect(isBookingStatus('canceled')).toBe(true);
  });

  test('rejects non-canonical / mis-spelled statuses', () => {
    for (const bad of ['cancelled', 'pending', 'confirmed', 'BOOKED', '', null, 42]) {
      expect(isBookingStatus(bad)).toBe(false);
    }
  });
});
