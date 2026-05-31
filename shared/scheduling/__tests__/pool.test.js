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

  it('hands C7 the SHIMMED appointmentType + resourceId; does NOT forward alreadyRejected (different namespace)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue([]);

    const res = await pool.select({
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
    expect(call.appointmentType.id).toBeUndefined(); // allowlist — no `...config` leak
    expect(call.alreadyRejected).toBeUndefined(); // pool dedups in its OWN namespace, not C7's
    expect(call.resourceId).toBe('maya');
    // generateSlots → [] for a non-empty pool → no chips → SLOT_UNAVAILABLE (assert it).
    expect(res.status).toBe('SLOT_UNAVAILABLE');
    expect(res.slots).toEqual([]);
    expect(res.poolBranch).toBe('single'); // pool was non-empty; the times were
  });

  it('dedups re-offered times at the pool layer by `slot#${start}` (§10.2 re-offer)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue([
      { slotId: 'maya|t1', start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z', label: 'L1', resourceId: 'maya' },
      { slotId: 'maya|t2', start: '2026-06-03T20:00:00Z', end: '2026-06-03T20:20:00Z', label: 'L2', resourceId: 'maya' },
    ]);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      alreadyRejected: ['slot#2026-06-03T19:00:00Z'], // user already rejected 19:00
      now: '2026-06-02T12:00:00Z',
    });

    expect(res.slots).toHaveLength(1);
    expect(res.slots[0].start).toBe('2026-06-03T20:00:00Z'); // rejected 19:00 dropped
  });

  it('validates required inputs', async () => {
    await expect(pool.select({ userTimeZone: TZ })).rejects.toThrow(/tenantId is required/);
    await expect(pool.select({ tenantId: TENANT })).rejects.toThrow(/userTimeZone is required/);
  });

  it('tolerates candidates: null (no candidates → empty pool)', async () => {
    routing.evaluatePool.mockResolvedValue({ ordered: [], tieBreaker: 'round_robin', roundRobinCursor: null });

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: null,
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
    });

    expect(res.poolBranch).toBe('empty');
    expect(availability.getBusyIntervals).not.toHaveBeenCalled();
  });

  it('omitting `now` uses the current time (no throw; queries a forward window)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: [], tieBreaker: 'round_robin', roundRobinCursor: null });

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      // now omitted
    });

    const fbCall = availability.getBusyIntervals.mock.calls[0][0];
    expect(typeof fbCall.windowStart).toBe('string');
    expect(Date.parse(fbCall.windowEnd)).toBeGreaterThan(Date.parse(fbCall.windowStart));
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

  it('a success does NOT clear in-window failures → a flapping resource still trips', async () => {
    expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false); // 1
    expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(false); // 2
    pool.recordFreeBusySuccess(TENANT, 'maya'); // success between failures must NOT reset
    expect(pool.recordFreeBusyFailure(TENANT, 'maya')).toBe(true); // 3rd in window → trips
    expect(pool.isResourceDegraded(TENANT, 'maya')).toBe(true);
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
    expect(put.TableName).toBe(pool._BOOKING_TABLE); // a wrong table silently mis-routes every lock
    expect(put.ConditionExpression).toBe('attribute_not_exists(booking_id)'); // condition on the SK
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

  it('validates inputs (incl. required poolSize — no candidate-count fallback)', async () => {
    await expect(pool.lockSlot({ start: 's', end: 'e', candidateResourceIds: ['x'], poolSize: 1 })).rejects.toThrow(/tenantId/);
    await expect(pool.lockSlot({ tenantId: TENANT, end: 'e', candidateResourceIds: ['x'], poolSize: 1 })).rejects.toThrow(/start and end/);
    await expect(pool.lockSlot({ tenantId: TENANT, start: 's', end: 'e', candidateResourceIds: [], poolSize: 1 })).rejects.toThrow(/non-empty/);
    await expect(pool.lockSlot({ tenantId: TENANT, start: 's', end: 'e', candidateResourceIds: ['x'] })).rejects.toThrow(/poolSize/);
    await expect(pool.lockSlot({ tenantId: TENANT, start: 's', end: 'e', candidateResourceIds: ['x'], poolSize: 0 })).rejects.toThrow(/poolSize/);
  });

  it('lock keys are tenant-scoped: the same slot in tenant B does not collide with tenant A', async () => {
    installAtomicLockModelFor(); // shared atomic model across both tenants
    const argsA = { tenantId: 'TENANT-A', ...SLOT, candidateResourceIds: ['maya'], poolSize: 1 };
    const argsB = { tenantId: 'TENANT-B', ...SLOT, candidateResourceIds: ['maya'], poolSize: 1 };

    const a = await pool.lockSlot(argsA);
    const b = await pool.lockSlot(argsB); // same resource+slot, different tenant → different PK
    const aAgain = await pool.lockSlot(argsA); // same tenant+slot → taken

    expect(a.status).toBe('LOCKED');
    expect(b.status).toBe('LOCKED');
    expect(aAgain.status).toBe('SLOT_UNAVAILABLE');
  });
});

// Atomic-lock model keyed on the FULL (tenantId, booking_id) item identity so a
// cross-tenant test exercises real PK isolation, not just the SK.
function installAtomicLockModelFor() {
  const taken = new Set();
  ddbMock.on(PutItemCommand).callsFake((input) => {
    const key = `${input.Item.tenantId.S}|${input.Item.booking_id.S}`;
    if (taken.has(key)) throw conditionalFail();
    taken.add(key);
    return {};
  });
  return taken;
}

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
    expect(res.nextAttempt).toBe(2); // C8 feeds this back so the 3-attempt cap counts
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

  it('a single servable candidate in a MULTI-member pool is NOT treated as solo', async () => {
    ddbMock.on(PutItemCommand).rejects(conditionalFail());

    // Only maya is free for this time, but the routing pool has 3 members → pool branch.
    const res = await pool.lockSlot({
      tenantId: TENANT,
      ...SLOT,
      candidateResourceIds: ['maya'],
      poolSize: 3,
      attempt: 1,
    });

    expect(res.poolExhausted).toBe(true); // pool branch, not solo
    expect(res.soloExhausted).toBeUndefined();
  });
});

// ─── lockSlot: race resolution + format-scoped duplicate (done-bar headline) ────────

describe('lockSlot — race + duplicate (§10.2 slot-lock race resolution / §5.4 layer 5)', () => {
  const SLOT = { start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:20:00Z' };

  // HONESTY NOTE: this exercises the pool-WALK logic (taken → next member → reoffer),
  // NOT live DynamoDB concurrency. Live conditional-write atomicity is a DDB property
  // verified at C8 integration (the DDB integration test is waived at this lib layer).
  // Here the shared Set + synchronous check-and-add stands in for a single item's
  // conditional uniqueness so the walk/backoff branches are driven deterministically.
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
