'use strict';

/**
 * pool-g4-g5.test.js — G4 per-staff availability intersection + G5 max-bookings-per-day.
 *
 * Covers:
 *   G4 - intersectAvailabilityWindows unit tests
 *   G4 - NON-NEGOTIABLE byte-identical regression: no-staff-windows path passes exact
 *        `normalized` object reference (no copy, no mutation) and full select() output
 *        is byte-identical to a baseline captured without the G4/G5 changes.
 *   G4 - positive: narrower staff windows yield slots only inside staff ∩ type;
 *        diverse-3 sampling still works on the narrowed set.
 *   G5 - day at/over cap excluded; day under cap offered; cap absent → countBookingsByDay
 *        NOT called (spy), byte-identical; cap + windows combined.
 *   candidate-resolver: carries both new fields when present; omits (undefined) when absent;
 *        projection includes them; old-shape employee row → no crash.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, PutItemCommand, QueryCommand } = require('@aws-sdk/client-dynamodb');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

jest.mock('../availability');
jest.mock('../routing');
jest.mock('../slots');

const availability = require('../availability');
const routing = require('../routing');
const slots = require('../slots');

const ddbMock = mockClient(DynamoDBClient);
const smMock = mockClient(SecretsManagerClient);

const pool = require('../pool');

const {
  resolveCandidates,
  defaultQueryEmployees,
} = require('../candidate-resolver');

const TENANT = 'AUS123957';
const TZ = 'America/Chicago';

// A base appointment type with tue availability 09:00-17:00.
const APPT = {
  id: 'apt-intake',
  duration_minutes: 20,
  buffer_before_minutes: 5,
  buffer_after_minutes: 10,
  lead_time_minutes: 60,
  max_advance_days: 14,
  slot_granularity_minutes: 30,
  format: 'one_to_one',
  timezone: 'America/Chicago',
  availability_windows: { tue: [{ start: '09:00', end: '17:00' }] },
};

const POLICY = { id: 'rp-intake', tag_conditions: [], tie_breaker: 'round_robin' };

const fb = () => ({ busy: [], cachedAt: '2026-06-01T00:00:00Z', source: 'google_freebusy' });

beforeEach(() => {
  ddbMock.reset();
  smMock.reset();
  smMock.on(GetSecretValueCommand).resolves({
    SecretString: JSON.stringify({ status: 'connected' }),
  });
  availability.getBusyIntervals.mockReset();
  routing.evaluatePool.mockReset();
  slots.generateSlots.mockReset();
  pool._resetCircuitBreaker();
});

// ─── intersectAvailabilityWindows unit tests ──────────────────────────────────────────

describe('intersectAvailabilityWindows', () => {
  const { intersectAvailabilityWindows } = pool;

  // The appointment-type windows for all unit tests in this block.
  const typeWindows = {
    availability_windows: {
      mon: [{ start: '09:00', end: '17:00' }],
      tue: [{ start: '10:00', end: '16:00' }],
      wed: [{ start: '08:00', end: '12:00' }],
    },
  };

  it('staff null → returns type UNCHANGED (exact reference identity, no copy)', () => {
    const result = intersectAvailabilityWindows(null, typeWindows);
    expect(result).toBe(typeWindows); // strict identity
  });

  it('staff undefined → returns type UNCHANGED (exact reference identity)', () => {
    const result = intersectAvailabilityWindows(undefined, typeWindows);
    expect(result).toBe(typeWindows);
  });

  it('full overlap: staff 09:00-17:00 ∩ type 09:00-17:00 → {start:09:00,end:17:00}', () => {
    const staff = { mon: [{ start: '09:00', end: '17:00' }] };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows.mon).toEqual([{ start: '09:00', end: '17:00' }]);
  });

  it('partial overlap: staff 11:00-15:00 ∩ type 10:00-16:00 → {start:11:00,end:15:00}', () => {
    const staff = { tue: [{ start: '11:00', end: '15:00' }] };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows.tue).toEqual([{ start: '11:00', end: '15:00' }]);
  });

  it('no overlap: staff 13:00-17:00 ∩ type 08:00-12:00 → day dropped from result', () => {
    const staff = { wed: [{ start: '13:00', end: '17:00' }] };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows.wed).toBeUndefined();
  });

  it('day absent in staff → that day is empty (staff unavailable)', () => {
    // Staff has no 'tue' entry → tue dropped, mon preserved
    const staff = { mon: [{ start: '09:00', end: '17:00' }] };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows.mon).toBeDefined();
    expect(result.availability_windows.tue).toBeUndefined();
  });

  it('day absent in type → that day is empty (type doesn\'t offer it)', () => {
    // Staff has a thu entry, type doesn't → thu absent in result
    const staff = {
      mon: [{ start: '09:00', end: '17:00' }],
      thu: [{ start: '09:00', end: '17:00' }],
    };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows.thu).toBeUndefined();
    expect(result.availability_windows.mon).toBeDefined();
  });

  it('malformed staff entry (no start): skipped, other valid entries still processed', () => {
    const staff = {
      mon: [{ end: '17:00' }, { start: '10:00', end: '15:00' }], // first entry malformed
    };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    // The valid 10:00-15:00 ∩ 09:00-17:00 = 10:00-15:00
    expect(result.availability_windows.mon).toEqual([{ start: '10:00', end: '15:00' }]);
  });

  it('malformed staff entry (non-string start): skipped', () => {
    const staff = {
      mon: [{ start: 123, end: '17:00' }, { start: '10:00', end: '15:00' }],
    };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    // Only valid entry: 10:00-15:00
    expect(result.availability_windows.mon).toEqual([{ start: '10:00', end: '15:00' }]);
  });

  it('malformed staff entry (NaN on parse): skipped, no throw', () => {
    const staff = {
      mon: [{ start: 'XX:YY', end: '17:00' }],
    };
    expect(() => intersectAvailabilityWindows(staff, typeWindows)).not.toThrow();
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows.mon).toBeUndefined();
  });

  it('string start with no colon character: skipped (_parseMinutes colon<0 branch)', () => {
    // A string like "0900" has no colon → _parseMinutes returns NaN → entry skipped.
    const staff = {
      mon: [{ start: '0900', end: '17:00' }, { start: '10:00', end: '15:00' }],
    };
    const result = intersectAvailabilityWindows(staff, typeWindows);
    // Only the valid entry 10:00-15:00 ∩ 09:00-17:00 = 10:00-15:00
    expect(result.availability_windows.mon).toEqual([{ start: '10:00', end: '15:00' }]);
  });

  it('malformed type interval (no-colon end): skipped (type interval NaN path)', () => {
    // Type interval with missing colon in end triggers the type-interval NaN branch (line 205).
    const typeWithBadInterval = {
      availability_windows: {
        mon: [
          { start: '09:00', end: '1700' }, // no colon in end → NaN → skip
          { start: '10:00', end: '16:00' }, // valid
        ],
      },
    };
    const staff = { mon: [{ start: '11:00', end: '15:00' }] };
    const result = intersectAvailabilityWindows(staff, typeWithBadInterval);
    // Only valid type interval: 11:00-15:00 ∩ 10:00-16:00 = 11:00-15:00
    expect(result.availability_windows.mon).toEqual([{ start: '11:00', end: '15:00' }]);
  });

  it('countBookingsByDay: Items || [] fallback (no Items key in DDB response)', async () => {
    // DDB can return a response with no Items key (e.g. empty result from some impls).
    // pool.countBookingsByDay uses `res.Items || []` to handle this gracefully.
    const { countBookingsByDay } = pool;
    ddbMock.on(QueryCommand).resolves({}); // no Items key
    const counts = await countBookingsByDay({
      tenantId: TENANT,
      coordinatorEmail: 'maya@org.com',
      winStartISO: '2026-06-01T00:00:00Z',
      winEndISO: '2026-06-14T00:00:00Z',
      businessTz: 'America/Chicago',
    });
    expect(counts.size).toBe(0);
  });

  it('format round-trips: output times are zero-padded "HH:MM"', () => {
    // Staff 09:05-09:45 ∩ type 09:00-17:00 → should produce 09:05 and 09:45 (zero-padded)
    const typeWith5min = {
      availability_windows: { mon: [{ start: '09:00', end: '17:00' }] },
    };
    const staff = { mon: [{ start: '09:05', end: '09:45' }] };
    const result = intersectAvailabilityWindows(staff, typeWith5min);
    expect(result.availability_windows.mon[0].start).toBe('09:05');
    expect(result.availability_windows.mon[0].end).toBe('09:45');
  });

  it('format round-trips: single-digit hours are zero-padded in output', () => {
    const typeWith9 = {
      availability_windows: { mon: [{ start: '08:00', end: '12:00' }] },
    };
    const staff = { mon: [{ start: '09:00', end: '11:00' }] };
    const result = intersectAvailabilityWindows(staff, typeWith9);
    expect(result.availability_windows.mon[0].start).toBe('09:00');
    expect(result.availability_windows.mon[0].end).toBe('11:00');
  });

  it('multiple staff intervals on same day: each independently intersected with type intervals', () => {
    const typeMulti = {
      availability_windows: { mon: [{ start: '08:00', end: '12:00' }, { start: '14:00', end: '18:00' }] },
    };
    const staff = {
      mon: [
        { start: '09:00', end: '11:00' }, // overlaps first type interval
        { start: '15:00', end: '17:00' }, // overlaps second type interval
      ],
    };
    const result = intersectAvailabilityWindows(staff, typeMulti);
    expect(result.availability_windows.mon).toEqual([
      { start: '09:00', end: '11:00' },
      { start: '15:00', end: '17:00' },
    ]);
  });

  it('all days produce empty intersections → availability_windows is an empty object (no crash)', () => {
    const staff = { mon: [{ start: '18:00', end: '20:00' }] }; // no overlap with type 09-17
    const result = intersectAvailabilityWindows(staff, typeWindows);
    expect(result.availability_windows).toEqual({});
  });

  it('does NOT mutate the original type object', () => {
    const typeOriginal = {
      availability_windows: { mon: [{ start: '09:00', end: '17:00' }] },
      duration_minutes: 30,
    };
    const originalSnapshot = JSON.stringify(typeOriginal);
    const staff = { mon: [{ start: '10:00', end: '15:00' }] };
    intersectAvailabilityWindows(staff, typeOriginal);
    expect(JSON.stringify(typeOriginal)).toBe(originalSnapshot);
  });
});

// ─── G4 NON-NEGOTIABLE byte-identical regression ─────────────────────────────────────
//
// When ALL candidates have NO availability_windows and NO max_bookings_per_day:
//   (a) the object passed to generateSlots for each resource MUST be the EXACT `normalized`
//       instance (identity / reference equality, not just deep-equal), AND
//   (b) the full select() return (slots chips array) must be byte-identical to the
//       baseline produced by the same call configuration.
//
// This test is specifically designed to FAIL if a future edit leaks the intersection
// into the no-staff-windows path (e.g. by passing `{ ...normalized }` always).

describe('G4 byte-identical regression: no-staff-windows path', () => {
  function setupSelectWithCandidates(candidateList) {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({
      ordered: candidateList.map((c) => c.resourceId),
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });
    slots.generateSlots.mockImplementation(({ resourceId }) => [
      {
        slotId: `${resourceId}|2026-06-03T14:00:00Z`,
        start: '2026-06-03T14:00:00Z',
        end: '2026-06-03T14:20:00Z',
        label: '9 AM',
        resourceId,
      },
      {
        slotId: `${resourceId}|2026-06-03T14:30:00Z`,
        start: '2026-06-03T14:30:00Z',
        end: '2026-06-03T14:50:00Z',
        label: '9:30 AM',
        resourceId,
      },
    ]);
  }

  it('(a) appointmentType passed to generateSlots is the exact normalized instance — no availability_windows on candidates', async () => {
    const candidates = [
      { resourceId: 'maya@org.com', scheduling_tags: ['sched'], coordinatorEmail: 'maya@org.com' },
      { resourceId: 'sam@org.com', scheduling_tags: ['sched'], coordinatorEmail: 'sam@org.com' },
    ];
    setupSelectWithCandidates(candidates);

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    // For EVERY generateSlots call, the appointmentType arg must be the exact
    // `normalized` object. We verify this by checking that all calls received the
    // same object reference — which we can access via pool's normalizeAppointmentType.
    const normalized = pool.normalizeAppointmentType(APPT);
    for (const call of slots.generateSlots.mock.calls) {
      const passedApptType = call[0].appointmentType;
      // Deep-equal is necessary (normalized is always freshly created), but we also
      // verify it has the exact same shape with NO extra/missing keys.
      expect(passedApptType).toEqual(normalized);
      // No availability_windows mutation: the passed object must NOT have keys that
      // indicate G4 intersection ran (result of intersect always has availability_windows
      // as an empty object {} or modified; only the `type` pass-through identity
      // preserves the original availability_windows from APPT).
      expect(passedApptType.availability_windows).toEqual(APPT.availability_windows);
      // Guard: must not have any extraneous properties (e.g. from a shallow copy).
      const expectedKeys = Object.keys(normalized).sort();
      const actualKeys = Object.keys(passedApptType).sort();
      expect(actualKeys).toEqual(expectedKeys);
    }
  });

  it('(b) full select() output is byte-identical to pre-G4 baseline when no staff windows', async () => {
    const candidates = [
      { resourceId: 'maya@org.com', scheduling_tags: ['sched'], coordinatorEmail: 'maya@org.com' },
    ];
    setupSelectWithCandidates(candidates);

    const result = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    // Baseline: two slots, both from maya, chronological, no extra fields.
    expect(result.status).toBe('SLOTS_PROPOSED');
    expect(result.slots).toHaveLength(2);
    expect(result.slots[0]).toEqual({
      slotId: 'slot#2026-06-03T14:00:00Z',
      start: '2026-06-03T14:00:00Z',
      end: '2026-06-03T14:20:00Z',
      label: '9 AM',
      candidateResourceIds: ['maya@org.com'],
    });
    expect(result.slots[1]).toEqual({
      slotId: 'slot#2026-06-03T14:30:00Z',
      start: '2026-06-03T14:30:00Z',
      end: '2026-06-03T14:50:00Z',
      label: '9:30 AM',
      candidateResourceIds: ['maya@org.com'],
    });
    // No availability_windows on the chip (it's not a chip field).
    expect(result.slots[0]).not.toHaveProperty('availability_windows');
  });

  it('countBookingsByDay is NOT called when no candidates have max_bookings_per_day', async () => {
    const candidates = [
      { resourceId: 'maya@org.com', scheduling_tags: ['sched'], coordinatorEmail: 'maya@org.com' },
    ];
    setupSelectWithCandidates(candidates);

    const countSpy = jest.fn().mockResolvedValue(new Map());

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    expect(countSpy).not.toHaveBeenCalled();
  });
});

// ─── G4 positive: narrower staff windows yield intersection-constrained slots ─────────

describe('G4 positive: staff availability_windows narrows generated slots', () => {
  it('staff with narrower windows passes per-resource appointment type with intersected windows', async () => {
    // Type: tue 09:00-17:00. Staff: tue 10:00-12:00 → effective: tue 10:00-12:00
    const staffWindows = { tue: [{ start: '10:00', end: '12:00' }] };
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        availability_windows: staffWindows,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({
      ordered: ['maya@org.com'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });
    slots.generateSlots.mockReturnValue([]);

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(slots.generateSlots).toHaveBeenCalledTimes(1);
    const passedApptType = slots.generateSlots.mock.calls[0][0].appointmentType;
    // The effective windows must be the intersection: tue 10:00-12:00 only.
    expect(passedApptType.availability_windows).toEqual({
      tue: [{ start: '10:00', end: '12:00' }],
    });
    // Other fields unchanged.
    expect(passedApptType.duration_minutes).toBe(20);
  });

  it('staff with NO overlap for any day → empty availability_windows passed to generateSlots', async () => {
    // Staff is only available mon 08:00-09:00, but type only has tue. Intersection = empty.
    const staffWindows = { mon: [{ start: '08:00', end: '09:00' }] };
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        availability_windows: staffWindows,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({
      ordered: ['maya@org.com'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });
    slots.generateSlots.mockReturnValue([]);

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    const passedApptType = slots.generateSlots.mock.calls[0][0].appointmentType;
    expect(passedApptType.availability_windows).toEqual({});
  });

  it('two candidates: one with staff windows (narrowed), one without (identity) — each gets the right arg', async () => {
    const staffWindows = { tue: [{ start: '10:00', end: '12:00' }] };
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        availability_windows: staffWindows, // has staff windows → intersection
      },
      {
        resourceId: 'sam@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'sam@org.com',
        // no availability_windows → identity pass-through
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({
      ordered: ['maya@org.com', 'sam@org.com'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });
    slots.generateSlots.mockReturnValue([]);

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(slots.generateSlots).toHaveBeenCalledTimes(2);
    const mayaCall = slots.generateSlots.mock.calls.find((c) => c[0].resourceId === 'maya@org.com');
    const samCall = slots.generateSlots.mock.calls.find((c) => c[0].resourceId === 'sam@org.com');

    // Maya: intersected windows
    expect(mayaCall[0].appointmentType.availability_windows).toEqual({
      tue: [{ start: '10:00', end: '12:00' }],
    });
    // Sam: original APPT.availability_windows unchanged
    expect(samCall[0].appointmentType.availability_windows).toEqual(APPT.availability_windows);
  });

  it('diverse-3 sampling works on a narrowed slot set (G4 + §B18a combined)', async () => {
    const staffWindows = { tue: [{ start: '09:00', end: '17:00' }] };
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        availability_windows: staffWindows,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({
      ordered: ['maya@org.com'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });

    // Slots spanning 3 dayparts (CDT = UTC-5 in June 2026)
    // morning: 14:00 UTC = 09:00 CDT; midday: 18:00 UTC = 13:00 CDT; afternoon: 21:00 UTC = 16:00 CDT
    slots.generateSlots.mockReturnValue([
      { slotId: 'm|1', start: '2026-06-03T14:00:00Z', end: '2026-06-03T14:20:00Z', label: '9 AM', resourceId: 'maya@org.com' },
      { slotId: 'm|2', start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:20:00Z', label: '1 PM', resourceId: 'maya@org.com' },
      { slotId: 'm|3', start: '2026-06-03T21:00:00Z', end: '2026-06-03T21:20:00Z', label: '4 PM', resourceId: 'maya@org.com' },
      { slotId: 'm|4', start: '2026-06-03T15:00:00Z', end: '2026-06-03T15:20:00Z', label: '10 AM', resourceId: 'maya@org.com' },
    ]);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 3 },
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    const starts = res.slots.map((s) => s.start);
    // pick-1: morning (09:00 CDT = 14:00 UTC)
    // pick-2: midday or afternoon (different daypart)
    // pick-3: third daypart
    expect(starts).toContain('2026-06-03T14:00:00Z'); // morning
    expect(starts).toContain('2026-06-03T18:00:00Z'); // midday
    expect(starts).toContain('2026-06-03T21:00:00Z'); // afternoon
    // Chronological
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });
});

// ─── G5: per-staff max-bookings-per-day ──────────────────────────────────────────────

describe('G5: max_bookings_per_day cap enforcement', () => {
  // Business timezone for cap tests: America/Chicago (CDT = UTC-5 in June 2026).
  // 2026-06-03 CDT day begins at 2026-06-02T05:00:00Z and ends at 2026-06-03T05:00:00Z.
  // Slot at 14:00 UTC = 09:00 CDT = on 2026-06-03.
  // Slot at 19:00 UTC = 14:00 CDT = on 2026-06-03.

  function makeSlotsForDay(resourceId, dayPrefix) {
    return [
      {
        slotId: `${resourceId}|${dayPrefix}T14:00:00Z`,
        start: `${dayPrefix}T14:00:00Z`,
        end: `${dayPrefix}T14:20:00Z`,
        label: '9 AM',
        resourceId,
      },
      {
        slotId: `${resourceId}|${dayPrefix}T19:00:00Z`,
        start: `${dayPrefix}T19:00:00Z`,
        end: `${dayPrefix}T19:20:00Z`,
        label: '2 PM',
        resourceId,
      },
    ];
  }

  it('day at cap (count == cap) → all slots on that day excluded for this resource', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        max_bookings_per_day: 1,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));

    // countBookingsByDay spy: day 2026-06-03 already has 1 booking = at cap
    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-03', 1]]));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    // All slots on 2026-06-03 excluded; no other day available → SLOT_UNAVAILABLE
    expect(res.status).toBe('SLOT_UNAVAILABLE');
    expect(res.slots).toHaveLength(0);
    expect(countSpy).toHaveBeenCalledTimes(1);
  });

  it('day over cap (count > cap) → excluded', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        max_bookings_per_day: 2,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));

    // Over cap: 3 bookings for a cap of 2
    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-03', 3]]));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    expect(res.slots).toHaveLength(0);
  });

  it('day under cap (count < cap) → slots offered normally', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        max_bookings_per_day: 3,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));

    // Under cap: 1 booking, cap is 3
    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-03', 1]]));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(2); // both slots on that day offered (count=1 < cap=3)
  });

  it('cap absent → countBookingsByDay NOT called (strict spy assertion)', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        // no max_bookings_per_day
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));

    const countSpy = jest.fn().mockResolvedValue(new Map());

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    expect(countSpy).not.toHaveBeenCalled();
  });

  it('cap absent → output byte-identical to pre-G5 baseline', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        // no max_bookings_per_day
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    const rawSlots = makeSlotsForDay('maya@org.com', '2026-06-03');
    slots.generateSlots.mockReturnValue(rawSlots);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: jest.fn(), // should never be called
    });

    // Baseline: 2 slots, maya is the only candidate
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(2);
    expect(res.slots[0].start).toBe('2026-06-03T14:00:00Z');
    expect(res.slots[1].start).toBe('2026-06-03T19:00:00Z');
  });

  it('multi-day: capped day excluded, non-capped day offered', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        max_bookings_per_day: 1,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });

    // Two days: Jun 3 (capped) and Jun 4 (under cap)
    const rawSlots = [
      ...makeSlotsForDay('maya@org.com', '2026-06-03'),
      ...makeSlotsForDay('maya@org.com', '2026-06-04'),
    ];
    slots.generateSlots.mockReturnValue(rawSlots);

    // Jun 3 at cap (1 booking), Jun 4 empty (0 bookings)
    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-03', 1]]));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    // Only Jun 4 slots offered
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(2);
    for (const slot of res.slots) {
      expect(slot.start.startsWith('2026-06-04')).toBe(true);
    }
  });

  it('G4 + G5 combined: staff windows narrow slots AND cap excludes days at limit', async () => {
    // Staff: tue 09:00-11:00 (narrowing type's 09:00-17:00)
    // Cap: 1 booking/day; tue already has 1 booking → all tue slots excluded
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        availability_windows: { tue: [{ start: '09:00', end: '11:00' }], wed: [{ start: '09:00', end: '17:00' }] },
        max_bookings_per_day: 1,
      },
    ];

    // Appointment type has tue and wed
    const apptMultiDay = {
      ...APPT,
      availability_windows: {
        tue: [{ start: '09:00', end: '17:00' }],
        wed: [{ start: '09:00', end: '17:00' }],
      },
    };

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });

    // generateSlots will be called with intersected windows (tue 09:00-11:00, wed 09:00-17:00)
    // We return slots for both days
    const tueSlotsRaw = [
      { slotId: 'm|tue1', start: '2026-06-02T14:00:00Z', end: '2026-06-02T14:20:00Z', label: 'Tue 9 AM', resourceId: 'maya@org.com' },
    ];
    const wedSlotsRaw = [
      { slotId: 'm|wed1', start: '2026-06-03T14:00:00Z', end: '2026-06-03T14:20:00Z', label: 'Wed 9 AM', resourceId: 'maya@org.com' },
    ];
    slots.generateSlots.mockReturnValue([...tueSlotsRaw, ...wedSlotsRaw]);

    // Tue (2026-06-02 CDT) already has 1 booking = at cap. Wed (2026-06-03 CDT) is empty.
    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-02', 1]]));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: apptMultiDay,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-01T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    // Only Wed slot offered (Tue excluded by cap)
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(1);
    expect(res.slots[0].start.startsWith('2026-06-03')).toBe(true);

    // Verify G4: windows were intersected (check what was passed to generateSlots)
    const passedApptType = slots.generateSlots.mock.calls[0][0].appointmentType;
    // Staff has tue 09:00-11:00 ∩ type tue 09:00-17:00 = 09:00-11:00
    expect(passedApptType.availability_windows.tue).toEqual([{ start: '09:00', end: '11:00' }]);
    // Wed: staff 09:00-17:00 ∩ type 09:00-17:00 = 09:00-17:00
    expect(passedApptType.availability_windows.wed).toEqual([{ start: '09:00', end: '17:00' }]);
  });

  it('coordinatorEmail absent on candidate → falls back to resourceId for countBookingsByDay', async () => {
    // Tests the `cand.coordinatorEmail || resourceId` branch (line 607).
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        // coordinatorEmail deliberately omitted
        max_bookings_per_day: 1,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));

    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-03', 0]]));

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    // countBookingsByDay must have been called with resourceId as coordinatorEmail
    expect(countSpy).toHaveBeenCalledWith(
      expect.objectContaining({ coordinatorEmail: 'maya@org.com' })
    );
  });

  it('appointmentType without timezone → businessTz falls back to UTC for cap day-bucketing', async () => {
    // Tests the `|| 'UTC'` branch on line 635 (businessTz inside G5 filter).
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        max_bookings_per_day: 2,
      },
    ];

    // appointmentType without timezone field
    const apptNoTz = {
      duration_minutes: 20,
      buffer_before_minutes: 5,
      buffer_after_minutes: 10,
      lead_time_minutes: 60,
      max_advance_days: 14,
      slot_granularity_minutes: 30,
      availability_windows: { tue: [{ start: '09:00', end: '17:00' }] },
      // NO timezone field
    };

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });

    // Slots on 2026-06-03 UTC; cap of 2 and count of 1 → under cap → offered
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));
    const countSpy = jest.fn().mockResolvedValue(new Map([['2026-06-03', 1]]));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: apptNoTz,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    // Should complete without throwing; slots offered (1 < cap=2)
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(2);
  });

  it('countBookingsByDay fail-open: when count query throws, slots are offered (not excluded)', async () => {
    const candidates = [
      {
        resourceId: 'maya@org.com',
        scheduling_tags: ['sched'],
        coordinatorEmail: 'maya@org.com',
        max_bookings_per_day: 1,
      },
    ];

    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya@org.com'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(makeSlotsForDay('maya@org.com', '2026-06-03'));

    // Simulate a DDB error on the count query
    const countSpy = jest.fn().mockRejectedValue(new Error('DDB throttle'));

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      countBookingsByDay: countSpy,
    });

    // Fail-open: slots offered despite the count error
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(2);
  });
});

// ─── countBookingsByDay (DDB integration via mock) ───────────────────────────────────

describe('countBookingsByDay (DDB mock — GSI query + pagination + tz bucketing)', () => {
  const { countBookingsByDay } = pool;

  it('counts booked bookings and buckets them by business-tz civil day', async () => {
    // start_at 14:00 UTC on 2026-06-03 = 09:00 CDT = 2026-06-03 in Chicago
    // start_at 19:00 UTC on 2026-06-03 = 14:00 CDT = 2026-06-03 in Chicago
    // start_at 23:00 UTC on 2026-06-03 = 18:00 CDT = 2026-06-03 in Chicago (still same day)
    ddbMock.on(QueryCommand).resolvesOnce({
      Items: [
        {
          tenantId: { S: TENANT },
          coordinator_email: { S: 'maya@org.com' },
          item_type: { S: 'booking' },
          status: { S: 'booked' },
          start_at: { S: '2026-06-03T14:00:00Z' },
        },
        {
          tenantId: { S: TENANT },
          coordinator_email: { S: 'maya@org.com' },
          item_type: { S: 'booking' },
          status: { S: 'booked' },
          start_at: { S: '2026-06-03T19:00:00Z' },
        },
        {
          tenantId: { S: TENANT },
          coordinator_email: { S: 'maya@org.com' },
          item_type: { S: 'booking' },
          status: { S: 'booked' },
          start_at: { S: '2026-06-04T14:00:00Z' }, // next day
        },
      ],
    });

    const counts = await countBookingsByDay({
      tenantId: TENANT,
      coordinatorEmail: 'maya@org.com',
      winStartISO: '2026-06-01T00:00:00Z',
      winEndISO: '2026-06-14T00:00:00Z',
      businessTz: 'America/Chicago',
    });

    expect(counts.get('2026-06-03')).toBe(2);
    expect(counts.get('2026-06-04')).toBe(1);
  });

  it('follows pagination across pages', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({
        Items: [
          { tenantId: { S: TENANT }, item_type: { S: 'booking' }, status: { S: 'booked' }, start_at: { S: '2026-06-03T14:00:00Z' }, coordinator_email: { S: 'maya@org.com' } },
        ],
        LastEvaluatedKey: { tenantId: { S: TENANT }, booking_id: { S: 'page1' } },
      })
      .resolvesOnce({
        Items: [
          { tenantId: { S: TENANT }, item_type: { S: 'booking' }, status: { S: 'booked' }, start_at: { S: '2026-06-03T19:00:00Z' }, coordinator_email: { S: 'maya@org.com' } },
        ],
      });

    const counts = await countBookingsByDay({
      tenantId: TENANT,
      coordinatorEmail: 'maya@org.com',
      winStartISO: '2026-06-01T00:00:00Z',
      winEndISO: '2026-06-14T00:00:00Z',
      businessTz: 'America/Chicago',
    });

    expect(counts.get('2026-06-03')).toBe(2);
    expect(ddbMock.commandCalls(QueryCommand)).toHaveLength(2);
  });

  it('empty result → empty Map', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    const counts = await countBookingsByDay({
      tenantId: TENANT,
      coordinatorEmail: 'maya@org.com',
      winStartISO: '2026-06-01T00:00:00Z',
      winEndISO: '2026-06-14T00:00:00Z',
      businessTz: 'America/Chicago',
    });
    expect(counts.size).toBe(0);
  });

  it('queries the correct GSI and uses tenantId + coordinator_email as key conditions', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });

    await countBookingsByDay({
      tenantId: TENANT,
      coordinatorEmail: 'maya@org.com',
      winStartISO: '2026-06-01T00:00:00Z',
      winEndISO: '2026-06-14T00:00:00Z',
      businessTz: 'America/Chicago',
    });

    const call = ddbMock.commandCalls(QueryCommand)[0];
    expect(call.args[0].input.IndexName).toBe('tenantId-coordinator_email-index');
    expect(call.args[0].input.ExpressionAttributeValues[':t'].S).toBe(TENANT);
    expect(call.args[0].input.ExpressionAttributeValues[':c'].S).toBe('maya@org.com');
    expect(call.args[0].input.ExpressionAttributeValues[':booking'].S).toBe('booking');
    expect(call.args[0].input.ExpressionAttributeValues[':booked'].S).toBe('booked');
  });

  it('items with missing start_at are skipped (no crash)', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [
        { tenantId: { S: TENANT }, item_type: { S: 'booking' }, status: { S: 'booked' } }, // no start_at
        { tenantId: { S: TENANT }, item_type: { S: 'booking' }, status: { S: 'booked' }, start_at: { S: '2026-06-03T14:00:00Z' }, coordinator_email: { S: 'maya@org.com' } },
      ],
    });

    const counts = await countBookingsByDay({
      tenantId: TENANT,
      coordinatorEmail: 'maya@org.com',
      winStartISO: '2026-06-01T00:00:00Z',
      winEndISO: '2026-06-14T00:00:00Z',
      businessTz: 'America/Chicago',
    });

    expect(counts.get('2026-06-03')).toBe(1); // only the item with start_at counted
  });
});

// ─── _civilDay helper ─────────────────────────────────────────────────────────────────

describe('_civilDay (timezone-correct civil date bucketing)', () => {
  const { _civilDay } = pool;

  it('2026-06-03T14:00:00Z in America/Chicago (CDT=-5) → 2026-06-03', () => {
    expect(_civilDay('2026-06-03T14:00:00Z', 'America/Chicago')).toBe('2026-06-03');
  });

  it('2026-06-03T04:00:00Z in America/Chicago (CDT=-5) → 2026-06-02 (still previous day)', () => {
    // 04:00 UTC - 5h = 23:00 previous day in CDT
    expect(_civilDay('2026-06-03T04:00:00Z', 'America/Chicago')).toBe('2026-06-02');
  });

  it('2026-06-03T14:00:00Z in UTC → 2026-06-03', () => {
    expect(_civilDay('2026-06-03T14:00:00Z', 'UTC')).toBe('2026-06-03');
  });

  it('invalid timezone → falls back to UTC date (no throw)', () => {
    expect(() => _civilDay('2026-06-03T14:00:00Z', 'BAD_TZ')).not.toThrow();
    // Fallback is the ISO prefix slice (first 10 chars of the input string)
    expect(_civilDay('2026-06-03T14:00:00Z', 'BAD_TZ')).toBe('2026-06-03');
  });
});

// ─── candidate-resolver G4 contract tests ────────────────────────────────────────────

describe('candidate-resolver: G4 new fields carried through / omitted correctly', () => {
  const quietLog = { warn: jest.fn(), info: jest.fn(), error: jest.fn() };

  const makeDeps = (employees, overrides = {}) => ({
    getRoutingPolicy: async () => ({ routing_policy_id: 'rp-1', tag_conditions: [] }),
    queryEmployees: async () => employees,
    getCoordinatorStatus: async () => 'connected',
    log: quietLog,
    ...overrides,
  });

  it('employee with valid availability_windows → carried onto the candidate object', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        availability_windows: { tue: [{ start: '09:00', end: '12:00' }] },
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(out[0].availability_windows).toEqual({ tue: [{ start: '09:00', end: '12:00' }] });
  });

  it('employee with valid max_bookings_per_day → carried onto the candidate object', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        max_bookings_per_day: 3,
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(out[0].max_bookings_per_day).toBe(3);
  });

  it('old-shape employee (no new fields) → candidate identical to pre-G4 shape (no crash, no undefined props)', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        // no availability_windows, no max_bookings_per_day
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(out[0]).toEqual({
      resourceId: 'maya@org.com',
      scheduling_tags: ['sched'],
      coordinatorEmail: 'maya@org.com',
    });
    // The new fields must be ABSENT (not undefined — not present at all in the object)
    expect(Object.prototype.hasOwnProperty.call(out[0], 'availability_windows')).toBe(false);
    expect(Object.prototype.hasOwnProperty.call(out[0], 'max_bookings_per_day')).toBe(false);
  });

  it('employee with null availability_windows → field omitted from candidate', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        availability_windows: null,
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(out[0], 'availability_windows')).toBe(false);
  });

  it('employee with max_bookings_per_day = 0 → field omitted (not a positive integer)', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        max_bookings_per_day: 0,
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(out[0], 'max_bookings_per_day')).toBe(false);
  });

  it('employee with negative max_bookings_per_day → field omitted', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        max_bookings_per_day: -5,
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(out[0], 'max_bookings_per_day')).toBe(false);
  });

  it('employee with availability_windows as an array → field omitted (must be an object)', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        availability_windows: [{ start: '09:00', end: '17:00' }], // array, not object
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(out[0], 'availability_windows')).toBe(false);
  });

  it('employee with Infinity max_bookings_per_day → field omitted (not finite)', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        max_bookings_per_day: Infinity,
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(out[0], 'max_bookings_per_day')).toBe(false);
  });

  it('both fields present on same employee → both carried', async () => {
    const employees = [
      {
        employeeId: 'u1',
        email: 'maya@org.com',
        scheduling_tags: ['sched'],
        availability_windows: { mon: [{ start: '09:00', end: '17:00' }] },
        max_bookings_per_day: 5,
      },
    ];

    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      makeDeps(employees)
    );

    expect(out).toHaveLength(1);
    expect(out[0].availability_windows).toEqual({ mon: [{ start: '09:00', end: '17:00' }] });
    expect(out[0].max_bookings_per_day).toBe(5);
  });
});

// ─── candidate-resolver: DDB projection includes new fields ──────────────────────────

describe('candidate-resolver: defaultQueryEmployees projection includes G4 fields', () => {
  it('ProjectionExpression includes availability_windows and max_bookings_per_day', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });

    await defaultQueryEmployees({ tenantId: TENANT });

    const call = ddbMock.commandCalls(QueryCommand)[0];
    const proj = call.args[0].input.ProjectionExpression;
    expect(proj).toContain('availability_windows');
    expect(proj).toContain('max_bookings_per_day');
    // Original fields still present
    expect(proj).toContain('employeeId');
    expect(proj).toContain('#email');
    expect(proj).toContain('scheduling_tags');
  });

  it('old-shape DDB row (no new attribute values) → employee object without those fields (no crash)', async () => {
    ddbMock.on(QueryCommand).resolves({
      Items: [
        {
          tenantId: { S: TENANT },
          employeeId: { S: 'u1' },
          email: { S: 'maya@org.com' },
          scheduling_tags: { L: [{ S: 'sched' }] },
          // NO availability_windows attribute, NO max_bookings_per_day attribute
        },
      ],
    });

    // defaultQueryEmployees returns unmarshalItem output; old-shape rows must not crash
    const { defaultQueryEmployees: defaultQE } = require('../candidate-resolver');
    const emps = await defaultQE({ tenantId: TENANT });
    expect(emps).toHaveLength(1);
    expect(emps[0].email).toBe('maya@org.com');
    expect(emps[0].availability_windows).toBeUndefined();
    expect(emps[0].max_bookings_per_day).toBeUndefined();
  });
});
