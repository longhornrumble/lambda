'use strict';

/**
 * synthetic-booking.js — create one synthetic booking through the REAL commit path.
 *
 * Drives BCH exactly as BSH does: scheduling_propose (real availability for the synthetic
 * tenant) → pick the first generic slot → default commit (the full transaction: live
 * freeBusy → slot-lock → conference → Google Calendar insert → Booking write → email).
 * Then stamps `is_synthetic=true` on the resulting row (§E6) and reads it back (commit
 * omits coordinator_email per §5.7; the cancel cycle needs it).
 *
 * Operational precondition (not code): the synthetic tenant (SYNTHETIC_TENANT_ID) must be
 * provisioned with scheduling_enabled, a coordinator with a live OAuth grant, the named
 * appointment type, and a routing policy — i.e. the §5.2 staging burn-in tenant. Without
 * it, propose returns no slots and the cycle reports a clean failure (no crash).
 */

const crypto = require('crypto');
const bchClient = require('./bch-client');
const bookingTable = require('./booking-table');

const SYNTHETIC_TENANT_ID = process.env.SYNTHETIC_TENANT_ID || '';
const SYNTHETIC_APPOINTMENT_TYPE_ID = process.env.SYNTHETIC_APPOINTMENT_TYPE_ID || '';
const SYNTHETIC_APPOINTMENT_TYPE_NAME =
  process.env.SYNTHETIC_APPOINTMENT_TYPE_NAME || 'Synthetic Monitor Check';
// Confirmation emails land at the §8 resolved alias scheduling-monitor@myrecruiter.ai.
const SYNTHETIC_ATTENDEE_EMAIL =
  process.env.SYNTHETIC_MONITOR_EMAIL || 'scheduling-monitor@myrecruiter.ai';
const SYNTHETIC_TIME_ZONE = process.env.SYNTHETIC_TIME_ZONE || 'America/Chicago';
// 'null' = NullConferenceProvider (synthetic ids, no real Meet/Zoom). Override to
// 'google_meet' to exercise real conference creation in burn-in.
const SYNTHETIC_CONFERENCE_TYPE = process.env.SYNTHETIC_CONFERENCE_TYPE || 'null';

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}

async function createSyntheticBooking({ cyclePrefix }, deps = {}) {
  const invoke = deps.invokeBch || bchClient.invokeBch;
  const stamp = deps.stampSynthetic || bookingTable.stampSynthetic;
  const getBooking = deps.getBooking || bookingTable.getBooking;
  const tenantId = deps.tenantId || SYNTHETIC_TENANT_ID;
  const appointmentTypeId = deps.appointmentTypeId || SYNTHETIC_APPOINTMENT_TYPE_ID;

  if (!tenantId || !appointmentTypeId) {
    throw new Error(
      'SYNTHETIC_TENANT_ID and SYNTHETIC_APPOINTMENT_TYPE_ID are required to create a synthetic booking'
    );
  }

  const sessionId = `synthetic-${cyclePrefix}-${crypto.randomUUID()}`;

  // 1. propose — real availability (generic slots, each carrying its resourceId per §B3).
  const proposed = await invoke({
    action: 'scheduling_propose',
    tenantId,
    appointmentTypeId,
    userTimeZone: SYNTHETIC_TIME_ZONE,
  });
  if (
    !proposed ||
    proposed.outcome !== 'ok' ||
    !Array.isArray(proposed.slots) ||
    proposed.slots.length === 0
  ) {
    throw new Error(`propose returned no slots (outcome=${proposed && proposed.outcome})`);
  }
  const chip = proposed.slots[0];
  // §B3-shipped chip shape is { slotId, start, end, label, candidateResourceIds } — the
  // tie-broken pool the commit route locks against (shared/scheduling/pool.js:314). NOTE:
  // FROZEN_CONTRACTS §B3 prose still says singular `resourceId`; the shipped producer uses
  // `candidateResourceIds` (flag to integrator to tighten §B3 — built to shipped reality).
  const candidateResourceIds = chip && chip.candidateResourceIds;
  if (!chip || !chip.start || !chip.end || !Array.isArray(candidateResourceIds) || candidateResourceIds.length === 0) {
    throw new Error('propose slot missing start/end/candidateResourceIds[]');
  }

  // 2. commit — the full transactional path (snake_case keys per the commit route).
  const committed = await invoke({
    tenant_id: tenantId,
    session_id: sessionId,
    slot: { start: chip.start, end: chip.end, candidateResourceIds },
    attendee: { email: SYNTHETIC_ATTENDEE_EMAIL, first_name: 'Synthetic', last_name: 'Monitor' },
    conference_type: SYNTHETIC_CONFERENCE_TYPE,
    pool_size: proposed.poolSize || 1,
    appointment_type: {
      id: appointmentTypeId,
      name: SYNTHETIC_APPOINTMENT_TYPE_NAME,
      timezone: SYNTHETIC_TIME_ZONE,
    },
    tie_breaker: proposed.tieBreaker,
    round_robin_cursor: proposed.roundRobinCursor,
  });
  if (!committed || committed.status !== 'BOOKED') {
    throw new Error(`commit did not BOOK (status=${committed && committed.status})`);
  }
  const bookingId = committed.bookingId;

  // 3. stamp is_synthetic (§E6) on the monitor's own row.
  await stamp(tenantId, bookingId);

  // 4. read the row back (commit omits coordinator_email per §5.7; cancel needs it).
  const booking = await getBooking(tenantId, bookingId);
  log('synthetic_booking_created', {
    tenant_id: tenantId,
    booking_id: bookingId,
    session_id: sessionId,
  });
  return {
    tenantId,
    bookingId,
    booking,
    coordinatorId: booking && booking.coordinator_email,
  };
}

module.exports = {
  createSyntheticBooking,
  _config: {
    SYNTHETIC_TENANT_ID,
    SYNTHETIC_APPOINTMENT_TYPE_ID,
    SYNTHETIC_ATTENDEE_EMAIL,
    SYNTHETIC_CONFERENCE_TYPE,
  },
};
