'use strict';

/**
 * Unit tests for pool.js (WS-C6) — canonical §10.2 pool-at-commit + §5.4 layer 5
 * format-scoped slot lock + §5.5 contention backoff.
 *
 * The three consumed contracts (C4 availability / C5 routing / C7 slots) are
 * jest-mocked so this is a focused unit test of C6's orchestration + lock; the
 * Booking-table conditional write is mocked with aws-sdk-client-mock
 * (Calendar_Watch_* test convention). Their own modules carry their own coverage.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');

jest.mock('../availability');
jest.mock('../routing');
jest.mock('../slots');

const availability = require('../availability');
const routing = require('../routing');
const slots = require('../slots');

const ddbMock = mockClient(DynamoDBClient);

const pool = require('../pool');

const TENANT = 'AUS123957';
const TZ = 'America/Chicago';

// A config-shape appointmentType (pre-shim) with timezone/windows already attached.
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

// A ConditionalCheckFailedException look-alike (name is what pool.js keys on).
function conditionalFail() {
  const e = new Error('The conditional request failed');
  e.name = 'ConditionalCheckFailedException';
  return e;
}

beforeEach(() => {
  ddbMock.reset();
  availability.getBusyIntervals.mockReset();
  routing.evaluatePool.mockReset();
  slots.generateSlots.mockReset();
  pool._resetCircuitBreaker();
});

// ─── field-shim: normalizeAppointmentType ───────────────────────────────────────────

describe('normalizeAppointmentType (the C6-owned field-shim)', () => {
  it('maps buffer_before/after → max(buffer_minutes), lead_time → min_lead_minutes', () => {
    const n = pool.normalizeAppointmentType(APPT);
    expect(n.buffer_minutes).toBe(10); // max(5, 10)
    expect(n.min_lead_minutes).toBe(60);
    expect(n.duration_minutes).toBe(20);
    expect(n.slot_granularity_minutes).toBe(30);
    expect(n.timezone).toBe('America/Chicago'); // pass-through
    expect(n.availability_windows).toEqual(APPT.availability_windows); // pass-through
  });

  it('tolerates missing optional fields → documented defaults (schema discipline)', () => {
    const n = pool.normalizeAppointmentType({ duration_minutes: 30 });
    expect(n.buffer_minutes).toBe(0);
    expect(n.min_lead_minutes).toBe(0);
  });

  it('throws on a missing appointmentType', () => {
    expect(() => pool.normalizeAppointmentType(null)).toThrow(/appointmentType is required/);
  });
});

// ─── buildLockKey: format-scoped uniqueness key ─────────────────────────────────────

describe('buildLockKey (§5.4 layer 5 format scoping)', () => {
  const base = { resourceId: 'maya', start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z' };

  it('same tuple + same format → identical key', () => {
    expect(pool.buildLockKey({ ...base, format: 'one_to_one' })).toBe(
      pool.buildLockKey({ ...base, format: 'one_to_one' })
    );
  });

  it('same tuple + DIFFERENT format → different key (v2 Group readiness)', () => {
    expect(pool.buildLockKey({ ...base, format: 'one_to_one' })).not.toBe(
      pool.buildLockKey({ ...base, format: 'group' })
    );
  });
});

// ─── select: empty / single / multiple branch (§10.2 steps 3-5) ─────────────────────

describe('select — §10.2 empty/single/multiple branching', () => {
  it('empty intersection → SLOT_UNAVAILABLE, poolBranch "empty", no slots (step 3)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: [], tieBreaker: 'first_available', roundRobinCursor: null });

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(res.status).toBe('SLOT_UNAVAILABLE');
    expect(res.poolBranch).toBe('empty');
    expect(res.slots).toEqual([]);
    expect(slots.generateSlots).not.toHaveBeenCalled();
  });

  it('single member → poolBranch "single"; one candidate per chip (step 4)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: { routingPolicyId: 'rp-intake' } });
    slots.generateSlots.mockReturnValue([
      { slotId: 'maya|2026-06-03T19:00:00Z', start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z', label: 'Tue, Jun 3 · 2:00 PM', resourceId: 'maya' },
    ]);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.poolBranch).toBe('single');
    expect(res.slots).toHaveLength(1);
    expect(res.slots[0].candidateResourceIds).toEqual(['maya']);
    expect(res.slots[0].slotId).toBe('slot#2026-06-03T19:00:00Z'); // generic chip id
  });

  it('multiple members → poolBranch "multiple"; chip merges resources who share a time', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya', 'diego'], tieBreaker: 'round_robin', roundRobinCursor: { routingPolicyId: 'rp-intake' } });
    // Both free at 19:00; only maya free at 20:00.
    slots.generateSlots.mockImplementation(({ resourceId }) => {
      if (resourceId === 'maya') {
        return [
          { slotId: 'maya|t1', start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z', label: 'L1', resourceId: 'maya' },
          { slotId: 'maya|t2', start: '2026-06-03T20:00:00Z', end: '2026-06-03T20:20:00Z', label: 'L2', resourceId: 'maya' },
        ];
      }
      return [
        { slotId: 'diego|t1', start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z', label: 'L1', resourceId: 'diego' },
      ];
    });

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }, { resourceId: 'diego' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(res.poolBranch).toBe('multiple');
    expect(res.slots).toHaveLength(2);
    const t1 = res.slots.find((s) => s.start === '2026-06-03T19:00:00Z');
    const t2 = res.slots.find((s) => s.start === '2026-06-03T20:00:00Z');
    // candidateResourceIds preserves the tie-broken `ordered` priority.
    expect(t1.candidateResourceIds).toEqual(['maya', 'diego']);
    expect(t2.candidateResourceIds).toEqual(['maya']);
  });

  it('caps merged chips at maxSlots, earliest-first', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(
      ['08:00', '09:00', '10:00', '07:00'].map((h, i) => ({
        slotId: `maya|${i}`,
        start: `2026-06-03T${h}:00Z`,
        end: `2026-06-03T${h}:20Z`,
        label: `L${i}`,
        resourceId: 'maya',
      }))
    );

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      maxSlots: 2,
    });

    expect(res.slots).toHaveLength(2);
    expect(res.slots[0].start).toBe('2026-06-03T07:00:00Z'); // earliest first
    expect(res.slots[1].start).toBe('2026-06-03T08:00:00Z');
  });

  it('passes the normalized appointmentType + alreadyRejected through to C7', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue([]);

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      alreadyRejected: ['slot#x'],
      now: '2026-06-02T12:00:00Z',
    });

    const call = slots.generateSlots.mock.calls[0][0];
    expect(call.appointmentType.buffer_minutes).toBe(10); // shimmed, not raw before/after
    expect(call.appointmentType.min_lead_minutes).toBe(60);
    expect(call.alreadyRejected).toEqual(['slot#x']);
    expect(call.resourceId).toBe('maya');
  });

  it('validates required inputs', async () => {
    await expect(pool.select({ userTimeZone: TZ })).rejects.toThrow(/tenantId is required/);
    await expect(pool.select({ tenantId: TENANT })).rejects.toThrow(/userTimeZone is required/);
  });
});

// ─── select: freeBusy intersection + per-resource failure (§10.2 step 2) ────────────

describe('select — freeBusy intersection + per-resource failure isolation', () => {
  it("one coordinator's failed freeBusy excludes only them; the pool query continues", async () => {
    availability.getBusyIntervals.mockImplementation(async ({ resourceId }) => {
      if (resourceId === 'diego') throw new Error('OAuth expired');
      return fb();
    });
    // routing sees only the resource whose freeBusy succeeded.
    routing.evaluatePool.mockImplementation(async ({ freeBusyByResource }) => {
      expect(freeBusyByResource.maya).toBeDefined();
      expect(freeBusyByResource.diego).toBeUndefined(); // excluded
      return { ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null };
    });
    slots.generateSlots.mockReturnValue([]);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }, { resourceId: 'diego' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(res.orderedPool).toEqual(['maya']);
  });

  it('uses candidate.coordinatorId for the freeBusy query when present', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: [], tieBreaker: 'round_robin', roundRobinCursor: null });

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'res-1', coordinatorId: 'maya@org.org' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(availability.getBusyIntervals).toHaveBeenCalledWith(
      expect.objectContaining({ resourceId: 'res-1', coordinatorId: 'maya@org.org' })
    );
  });
});

// ─── select: circuit-breaker (§10.2 "repeated coordinator failures") ────────────────

describe('select — circuit-breaker (3 freeBusy failures in 5 min → degraded)', () => {
  it('trips after 3 failures and then excludes the coordinator from the pool', async () => {
    availability.getBusyIntervals.mockRejectedValue(new Error('transient'));
    routing.evaluatePool.mockResolvedValue({ ordered: [], tieBreaker: 'round_robin', roundRobinCursor: null });

    const args = {
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    };

    // 3 separate booking attempts, each failing maya's freeBusy.
    await pool.select(args);
    await pool.select(args);
    expect(pool.isResourceDegraded(TENANT, 'maya')).toBe(false); // 2 failures < threshold
    await pool.select(args);
    expect(pool.isResourceDegraded(TENANT, 'maya')).toBe(true); // 3rd trips it

    // Next attempt: maya is degraded → getBusyIntervals not called for her.
    availability.getBusyIntervals.mockClear();
    await pool.select(args);
    expect(availability.getBusyIntervals).not.toHaveBeenCalled();
  });

  it('a successful freeBusy clears the failure count', async () => {
    expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false);
    expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false);
    pool.recordFreeBusySuccess(TENANT, 'maya');
    expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false); // count reset → still under threshold
    expect(pool.isResourceDegraded(TENANT, 'maya')).toBe(false);
  });

  it('prunes only the stale failures, keeping ones still inside the window', () => {
    const realNow = Date.now;
    let t = 1_000_000_000_000;
    Date.now = () => t;
    try {
      pool._resetCircuitBreaker();
      pool.recordFreeBusyFailure(TENANT, 'maya'); // t0
      t += pool._CB_WINDOW_MS / 2;
      pool.recordFreeBusyFailure(TENANT, 'maya'); // t0 + half-window
      t += pool._CB_WINDOW_MS / 2 + 1; // t0 + window + 1: only the first failure is stale
      // 1 stale dropped, 1 still live → the partial-prune branch.
      expect(pool.isResourceDegraded(TENANT, 'maya')).toBe(false);
      expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false); // 2 live now (< 3)
    } finally {
      Date.now = realNow;
    }
  });

  it('failures outside the 5-min window age out (self-healing / half-open)', async () => {
    const realNow = Date.now;
    let t = 1_000_000_000_000;
    Date.now = () => t;
    try {
      pool._resetCircuitBreaker();
      pool.recordFreeBusyFailure(TENANT, 'maya');
      pool.recordFreeBusyFailure(TENANT, 'maya');
      t += pool._CB_WINDOW_MS + 1; // advance past the window
      // the two stale failures expire → a fresh failure is the only live one.
      expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false);
      expect(pool.isResourceDegraded(TENANT, 'maya')).toBe(false);
    } finally {
      Date.now = realNow;
    }
  });
});

// ─── lockSlot: the conditional-write slot lock + backoff (§5.4 layer 5 / §10.2) ──────

describe('lockSlot — conditional-write lock', () => {
  const SLOT = { start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z' };

  it('first candidate free → LOCKED with the format-scoped key + condition', async () => {
    ddbMock.on(PutItemCommand).resolves({});

    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      format: 'one_to_one',
      candidateResourceIds: ['maya', 'diego'],
      poolSize: 2,
    });

    expect(res.status).toBe('LOCKED');
    expect(res.resourceId).toBe('maya');
    expect(res.lockKey).toBe('slot_lock#one_to_one#maya#2026-06-03T19:00:00Z#2026-06-03T19:20:00Z');
    const put = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(put.ConditionExpression).toBe('attribute_not_exists(tenantId)');
    expect(put.Item.booking_id.S).toBe(res.lockKey);
    expect(put.Item.item_type.S).toBe('slot_lock');
    // lock item stays OUT of the Booking GSIs (no start_at / coordinator_email attrs).
    expect(put.Item.start_at).toBeUndefined();
    expect(put.Item.coordinator_email).toBeUndefined();
    expect(put.Item.lock_start_at.S).toBe(SLOT.start);
  });

  it('first candidate taken → silently locks the next pool member (§10.2)', async () => {
    ddbMock.on(PutItemCommand).rejectsOnce(conditionalFail()).resolves({});

    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      candidateResourceIds: ['maya', 'diego'],
      poolSize: 2,
    });

    expect(res.status).toBe('LOCKED');
    expect(res.resourceId).toBe('diego');
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(2);
  });

  it('all pool members taken → reoffer (poolExhausted), never silent-drop', async () => {
    ddbMock.on(PutItemCommand).rejects(conditionalFail());

    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      candidateResourceIds: ['maya', 'diego'],
      poolSize: 2,
    });

    expect(res.status).toBe('SLOT_UNAVAILABLE');
    expect(res.action).toBe('reoffer');
    expect(res.poolExhausted).toBe(true);
  });

  it('a non-ConditionalCheckFailed error propagates (not mistaken for "taken")', async () => {
    const throttle = new Error('throttled');
    throttle.name = 'ProvisionedThroughputExceededException';
    ddbMock.on(PutItemCommand).rejects(throttle);

    await expect(
      pool.lockSlot({ tenantId: TENANT, ...SLOT, candidateResourceIds: ['maya'], poolSize: 1 })
    ).rejects.toThrow(/throttled/);
  });

  it('validates inputs', async () => {
    await expect(pool.lockSlot({ start: 's', end: 'e', candidateResourceIds: ['x'] })).rejects.toThrow(/tenantId/);
    await expect(pool.lockSlot({ tenantId: TENANT, end: 'e', candidateResourceIds: ['x'] })).rejects.toThrow(/start and end/);
    await expect(pool.lockSlot({ tenantId: TENANT, start: 's', end: 'e', candidateResourceIds: [] })).rejects.toThrow(/non-empty/);
  });
});

// ─── lockSlot: solo-pool 3-retry → reoffer (§5.5 row / done-bar) ─────────────────────

describe('lockSlot — solo-pool backoff (size 1, 3 attempts → reoffer to proposing)', () => {
  const SLOT = { start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z' };

  it('solo lock taken, attempt < 3 → reoffer, not yet exhausted', async () => {
    ddbMock.on(PutItemCommand).rejects(conditionalFail());

    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      candidateResourceIds: ['maya'],
      poolSize: 1,
      attempt: 1,
    });

    expect(res.status).toBe('SLOT_UNAVAILABLE');
    expect(res.action).toBe('reoffer');
    expect(res.soloExhausted).toBe(false);
  });

  it('solo lock taken on the 3rd attempt → reoffer from proposing (exhausted)', async () => {
    ddbMock.on(PutItemCommand).rejects(conditionalFail());

    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      candidateResourceIds: ['maya'],
      poolSize: 1,
      attempt: 3,
    });

    expect(res.soloExhausted).toBe(true);
    expect(res.state).toBe('proposing');
  });

  it('poolSize defaults to candidate count → single candidate counts as solo', async () => {
    ddbMock.on(PutItemCommand).rejects(conditionalFail());

    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      candidateResourceIds: ['maya'], // no poolSize passed
      attempt: 3,
    });

    expect(res.soloExhausted).toBe(true);
  });
});

// ─── lockSlot: race resolution + format-scoped duplicate (done-bar headline) ────────

describe('lockSlot — race + duplicate (§10.2 slot-lock race resolution / §5.4 layer 5)', () => {
  const SLOT = { start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z' };

  // A model of DynamoDB's atomic conditional PutItem: a shared Set of taken lock
  // keys. The synchronous check-and-add is the atomic primitive (JS is single
  // threaded; Promise.all interleaves only at awaits) — exactly the (resource,
  // start, end, format) uniqueness the conditional write provides.
  function installAtomicLockModel() {
    const taken = new Set();
    ddbMock.on(PutItemCommand).callsFake((input) => {
      const key = input.Item.booking_id.S;
      if (taken.has(key)) throw conditionalFail();
      taken.add(key);
      return {};
    });
    return taken;
  }

  it('triple-collision on a SOLO slot → exactly one wins, the other two reoffer', async () => {
    installAtomicLockModel();

    const commit = () =>
      pool.lockSlot({ tenantId: TENANT, ...SLOT, candidateResourceIds: ['maya'], poolSize: 1, attempt: 1 });

    const results = await Promise.all([commit(), commit(), commit()]);
    const locked = results.filter((r) => r.status === 'LOCKED');
    const reoffered = results.filter((r) => r.status === 'SLOT_UNAVAILABLE');

    expect(locked).toHaveLength(1); // exactly one wins the (maya, slot) lock
    expect(reoffered).toHaveLength(2);
    expect(reoffered.every((r) => r.action === 'reoffer')).toBe(true);
  });

  it('triple-collision on a 3-member POOL → all three get distinct coordinators (pool absorbs)', async () => {
    installAtomicLockModel();

    const commit = () =>
      pool.lockSlot({
        tenantId: TENANT,
        ...SLOT,
        candidateResourceIds: ['maya', 'diego', 'priya'],
        poolSize: 3,
      });

    const results = await Promise.all([commit(), commit(), commit()]);
    const lockedResources = results.filter((r) => r.status === 'LOCKED').map((r) => r.resourceId);

    expect(lockedResources).toHaveLength(3);
    expect(new Set(lockedResources).size).toBe(3); // three distinct coordinators
  });

  it('duplicate insert: same (resource, start, end) + same format → 2nd rejected', async () => {
    installAtomicLockModel();
    const args = { tenantId: TENANT, ...SLOT, format: 'one_to_one', candidateResourceIds: ['maya'], poolSize: 1, attempt: 1 };

    const first = await pool.lockSlot(args);
    const second = await pool.lockSlot(args);

    expect(first.status).toBe('LOCKED');
    expect(second.status).toBe('SLOT_UNAVAILABLE');
  });

  it('same (resource, start, end) + DIFFERENT format → accepted (v2 Group readiness)', async () => {
    installAtomicLockModel();

    const oneToOne = await pool.lockSlot({ tenantId: TENANT, ...SLOT, format: 'one_to_one', candidateResourceIds: ['maya'], poolSize: 1 });
    const group = await pool.lockSlot({ tenantId: TENANT, ...SLOT, format: 'group', candidateResourceIds: ['maya'], poolSize: 1 });

    expect(oneToOne.status).toBe('LOCKED');
    expect(group.status).toBe('LOCKED'); // different format → different lock key → allowed
  });
});
