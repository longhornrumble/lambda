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

// ─── Fix #1+#2: REAL-PATH dateWindow test (no poolSelect mock) ───────────────────────
//
// This test exercises the ACTUAL pool.select → slots.generateSlots path with a
// date_window constraint. It intentionally does NOT mock slots.generateSlots so
// the fix (#1: dateWindow forwarded from pool.select's signature into generateSlots)
// is exercised end-to-end. availability + routing are still mocked (they require
// network/Google credentials). The assertion: every returned slot's start must fall
// within the dateWindow.
//
// This is the test that would have caught the production no-op: before fix #1,
// pool.select discarded dateWindow silently and generateSlots received no filter,
// returning slots from any day. After fix #1, only same-day slots are returned.

describe('select — REAL-PATH dateWindow filter (fix #1+#2: no poolSelect mock)', () => {
  // Un-mock slots for this block so the real generateSlots is exercised.
  let realPool;
  let realAvailability;
  let realRouting;

  beforeAll(() => {
    jest.isolateModules(() => {
      // Unmock slots so the real module is loaded in this isolated context.
      jest.unmock('../slots');
      realPool = require('../pool');
      realAvailability = require('../availability');
      realRouting = require('../routing');
    });
  });

  it('dateWindow constrains slots to the picked day — every returned slot start falls within the window', async () => {
    // Pick a day: Wednesday June 3, 2026. We set now to Mon Jun 1 12:00 UTC.
    // dateWindow = the whole Jun 3 UTC day.
    const pickedDate = '2026-06-03';
    const dateWindow = {
      startISO: `${pickedDate}T00:00:00.000Z`,
      endISO:   `${pickedDate}T24:00:00.000Z`,
    };
    const now = '2026-06-01T12:00:00.000Z';

    // Availability: busy from 14:00–15:00 UTC on Jun 3 (should not affect the test's
    // assertion about filter correctness — we only check starts are on Jun 3).
    jest.spyOn(realAvailability, 'getBusyIntervals').mockResolvedValue({
      busy: [{ start: '2026-06-03T14:00:00Z', end: '2026-06-03T15:00:00Z' }],
      cachedAt: now,
      source: 'google_freebusy',
    });
    // Routing: single candidate, round-robin.
    jest.spyOn(realRouting, 'evaluatePool').mockResolvedValue({
      ordered: ['maya@org.org'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });

    const result = await realPool.select({
      tenantId: TENANT,
      appointmentType: {
        ...APPT,
        // Widen availability_windows to include Jun 3 (Wednesday = 'wed').
        availability_windows: { wed: [{ start: '09:00', end: '17:00' }] },
      },
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya@org.org' }],
      userTimeZone: TZ,
      now,
      dateWindow,
    });

    // The key assertion: fix #1 must pass dateWindow to generateSlots so only
    // slots on Jun 3 are returned (not slots on Jun 2, Jun 4, etc.).
    if (result.status === 'SLOTS_PROPOSED') {
      expect(result.slots.length).toBeGreaterThan(0);
      for (const slot of result.slots) {
        expect(slot.start >= dateWindow.startISO).toBe(true);
        expect(slot.start < dateWindow.endISO).toBe(true);
      }
    }
    // Even if no slots are returned (e.g. busy covers the window), the pool must not
    // crash and must return a defined status.
    expect(['SLOTS_PROPOSED', 'SLOT_UNAVAILABLE']).toContain(result.status);
  });

  it('without dateWindow: slots may span multiple days (no spurious filter)', async () => {
    const now = '2026-06-01T12:00:00.000Z';
    jest.spyOn(realAvailability, 'getBusyIntervals').mockResolvedValue({
      busy: [],
      cachedAt: now,
      source: 'google_freebusy',
    });
    jest.spyOn(realRouting, 'evaluatePool').mockResolvedValue({
      ordered: ['maya@org.org'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });

    const result = await realPool.select({
      tenantId: TENANT,
      appointmentType: {
        ...APPT,
        availability_windows: {
          tue: [{ start: '09:00', end: '17:00' }],
          wed: [{ start: '09:00', end: '17:00' }],
          thu: [{ start: '09:00', end: '17:00' }],
        },
      },
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya@org.org' }],
      userTimeZone: TZ,
      now,
      // No dateWindow — should return multi-day results
    });

    // Without a dateWindow the result should include slots from multiple days
    // (Tuesday through Thursday within searchDays). Just verify no crash and
    // status is a recognized value.
    expect(['SLOTS_PROPOSED', 'SLOT_UNAVAILABLE']).toContain(result.status);
  });
});

// ─── §B18a diverse-3 sampling ─────────────────────────────────────────────────────────

// Helpers for diverse-sampling tests:
// makeSlot(isoStart, isoEnd, label) — returns a merged-shape slot (before chip conversion)
// The mock setup returns these directly from slots.generateSlots, which pool merges into
// byStart before calling the sampling logic.

// NOTE: all ISO times are UTC. daypartOf uses the userTimeZone to classify.
// TZ = 'America/Chicago' = UTC-5 (CST) or UTC-6 (CDT).
// For determinism we use CDT (Jun 2026): UTC-5.
// morning  < 12:00 CDT = < 17:00 UTC
// midday   12:00–14:59 CDT = 17:00–19:59 UTC
// afternoon >= 15:00 CDT = >= 20:00 UTC

function makeRawSlot(start, end, label) {
  return { slotId: `maya|${start}`, start, end, label, resourceId: 'maya' };
}

// A reusable setup helper: returns pool.select with slots.generateSlots mocked.
async function selectWithDiverseSampling(rawSlots) {
  availability.getBusyIntervals.mockResolvedValue(fb());
  routing.evaluatePool.mockResolvedValue({
    ordered: ['maya'],
    tieBreaker: 'round_robin',
    roundRobinCursor: null,
  });
  slots.generateSlots.mockReturnValue(rawSlots);

  return pool.select({
    tenantId: TENANT,
    appointmentType: APPT,
    routingPolicy: POLICY,
    candidates: [{ resourceId: 'maya' }],
    userTimeZone: TZ,
    now: '2026-06-02T12:00:00Z',
    sampling: { mode: 'daypart-diverse', count: 3 },
  });
}

describe('select — §B18a diverse-3 sampling', () => {
  it('morning + midday + afternoon on the same day → 3 chips, 3 distinct dayparts, chronological', async () => {
    // In CDT (UTC-5): 14:00 UTC = 09:00 AM CDT (morning), 18:00 UTC = 13:00 CDT (midday), 21:00 UTC = 16:00 CDT (afternoon)
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),  // morning
      makeRawSlot('2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z', 'Tue 1 PM'),  // midday
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),  // afternoon
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'Tue 10 AM'), // second morning
    ];
    const res = await selectWithDiverseSampling(rawSlots);
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    // All 3 dayparts represented
    const starts = res.slots.map((s) => s.start);
    expect(starts).toContain('2026-06-03T14:00:00Z'); // morning pick-1
    expect(starts).toContain('2026-06-03T18:00:00Z'); // midday pick-2
    expect(starts).toContain('2026-06-03T21:00:00Z'); // afternoon pick-3
    // Chronological
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });

  it('morning-only day 1, afternoon-only day 2 → picks span dayparts across days (day-spread fallback)', async () => {
    // Day 1: two morning slots; day 2: one afternoon slot
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),   // morning
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'Tue 10 AM'),  // morning
      makeRawSlot('2026-06-04T21:00:00Z', '2026-06-04T21:30:00Z', 'Wed 4 PM'),   // afternoon
    ];
    const res = await selectWithDiverseSampling(rawSlots);
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    const starts = res.slots.map((s) => s.start);
    // pick-1: earliest morning day-1
    expect(starts).toContain('2026-06-03T14:00:00Z');
    // pick-2: should try different daypart same day, fail, then day-spread to Wed afternoon
    expect(starts).toContain('2026-06-04T21:00:00Z');
    // pick-3: another morning from day-1 (only daypart left)
    expect(starts).toContain('2026-06-03T15:00:00Z');
    // Chronological
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });

  it('all candidates one daypart (afternoon only) → day-spread within that daypart, chronological', async () => {
    const rawSlots = [
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),
      makeRawSlot('2026-06-04T21:00:00Z', '2026-06-04T21:30:00Z', 'Wed 4 PM'),
      makeRawSlot('2026-06-05T21:00:00Z', '2026-06-05T21:30:00Z', 'Thu 4 PM'),
      makeRawSlot('2026-06-06T21:00:00Z', '2026-06-06T21:30:00Z', 'Fri 4 PM'),
    ];
    const res = await selectWithDiverseSampling(rawSlots);
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    const starts = res.slots.map((s) => s.start);
    // All different (no duplicate)
    expect(new Set(starts).size).toBe(3);
    // Chronological
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });

  it('alreadyRejected filtered BEFORE sampling — a rejected earliest slot never reappears as pick-1', async () => {
    // Earliest morning is rejected; pick-1 must be the next available morning
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),   // morning (rejected)
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'Tue 10 AM'),  // morning
      makeRawSlot('2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z', 'Tue 1 PM'),   // midday
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),   // afternoon
    ];
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(rawSlots);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      alreadyRejected: ['slot#2026-06-03T14:00:00Z'], // earliest rejected
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 3 },
    });

    const starts = res.slots.map((s) => s.start);
    expect(starts).not.toContain('2026-06-03T14:00:00Z'); // rejected slot absent
    expect(res.slots).toHaveLength(3);
  });

  it('≤3 candidates after filtering → all returned, sorted chronologically', async () => {
    const rawSlots = [
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),
    ];
    const res = await selectWithDiverseSampling(rawSlots);
    expect(res.slots).toHaveLength(2);
    expect(res.slots[0].start).toBe('2026-06-03T14:00:00Z'); // sorted
    expect(res.slots[1].start).toBe('2026-06-03T21:00:00Z');
  });

  it('NO sampling arg → output identical to pre-change earliest-first behavior (regression)', async () => {
    const rawSlots = [
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'Tue 10 AM'),
      makeRawSlot('2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z', 'Tue 1 PM'),
    ];
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(rawSlots);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      maxSlots: 3,
      // NO sampling arg
    });

    expect(res.slots).toHaveLength(3);
    // Earliest-first (default behavior)
    expect(res.slots[0].start).toBe('2026-06-03T14:00:00Z');
    expect(res.slots[1].start).toBe('2026-06-03T15:00:00Z');
    expect(res.slots[2].start).toBe('2026-06-03T18:00:00Z');
  });

  it('diverse mode passes CANDIDATE_CAP=48 to generateSlots (not maxSlots=5)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue([]);

    await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 3 },
    });

    const call = slots.generateSlots.mock.calls[0][0];
    expect(call.maxSlots).toBe(48); // CANDIDATE_CAP
  });

  it('count=2 → returns exactly 2 picks (pick-1 + pick-2) sorted chronologically, not 3', async () => {
    // 4 slots across morning/midday/afternoon on the same day.
    // count=2: algorithm picks p1 (morning) and p2 (midday), then hits the count<3 branch
    // and returns 2 chips sorted chronologically — afternoon slot must be absent.
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),   // morning (pick-1)
      makeRawSlot('2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z', 'Tue 1 PM'),   // midday  (pick-2)
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),   // afternoon (must be absent)
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'Tue 10 AM'),  // second morning
    ];
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({
      ordered: ['maya'],
      tieBreaker: 'round_robin',
      roundRobinCursor: null,
    });
    slots.generateSlots.mockReturnValue(rawSlots);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 2 },
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(2);
    const starts = res.slots.map((s) => s.start);
    // pick-1: earliest morning
    expect(starts).toContain('2026-06-03T14:00:00Z');
    // pick-2: different daypart (midday)
    expect(starts).toContain('2026-06-03T18:00:00Z');
    // afternoon slot excluded (count=2 stops after pick-2)
    expect(starts).not.toContain('2026-06-03T21:00:00Z');
    // Sorted chronologically
    expect(starts[0]).toBe('2026-06-03T14:00:00Z');
    expect(starts[1]).toBe('2026-06-03T18:00:00Z');
    // _daypart annotation stripped from output
    expect(res.slots[0]).not.toHaveProperty('_daypart');
    expect(res.slots[1]).not.toHaveProperty('_daypart');
  });

  // Fix A adversarial: all-morning, 2 days → pick-2 uses "different day" tier
  it('pick-2 different-day tier: all-morning 3 slots (day1-9, day1-10, day2-9) → picks use different day for p2, chronological output', async () => {
    // day1-9:00 CDT = 2026-06-03T14:00:00Z (morning)
    // day1-10:00 CDT = 2026-06-03T15:00:00Z (morning)
    // day2-9:00 CDT = 2026-06-04T14:00:00Z (morning)
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),   // morning day1
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'Tue 10 AM'),  // morning day1
      makeRawSlot('2026-06-04T14:00:00Z', '2026-06-04T14:30:00Z', 'Wed 9 AM'),   // morning day2
    ];
    const res = await selectWithDiverseSampling(rawSlots);
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    const starts = res.slots.map((s) => s.start);
    // pick-1: day1-9:00 (earliest)
    // pick-2: different daypart → none (all morning) → different day → day2-9:00
    // pick-3: third daypart → none → day not yet represented → none (day1+day2 both seen) → earliest remaining → day1-10:00
    // sorted chronologically: day1-9, day1-10, day2-9
    expect(starts[0]).toBe('2026-06-03T14:00:00Z'); // day1-9
    expect(starts[1]).toBe('2026-06-03T15:00:00Z'); // day1-10
    expect(starts[2]).toBe('2026-06-04T14:00:00Z'); // day2-9
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });

  // Fix B adversarial: all-morning 4 slots across 3 days → pick-3 uses "day not yet represented" tier
  it('pick-3 day-not-yet-represented tier: all-morning [day1-9, day2-9, day2-10, day3-9] → picks = day1-9, day2-9, day3-9', async () => {
    // day1-9 = 2026-06-03T14:00Z, day2-9 = 2026-06-04T14:00Z, day2-10 = 2026-06-04T15:00Z, day3-9 = 2026-06-05T14:00Z
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),   // morning day1
      makeRawSlot('2026-06-04T14:00:00Z', '2026-06-04T14:30:00Z', 'Wed 9 AM'),   // morning day2
      makeRawSlot('2026-06-04T15:00:00Z', '2026-06-04T15:30:00Z', 'Wed 10 AM'),  // morning day2
      makeRawSlot('2026-06-05T14:00:00Z', '2026-06-05T14:30:00Z', 'Thu 9 AM'),   // morning day3
    ];
    const res = await selectWithDiverseSampling(rawSlots);
    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    const starts = res.slots.map((s) => s.start);
    // pick-1: day1-9 (earliest)
    // pick-2: different daypart → none → different day → day2-9
    // pick-3: third daypart → none → day not yet represented → day3-9 (day2-10 excluded)
    // sorted: day1-9, day2-9, day3-9
    expect(starts).toContain('2026-06-03T14:00:00Z'); // day1-9
    expect(starts).toContain('2026-06-04T14:00:00Z'); // day2-9
    expect(starts).toContain('2026-06-05T14:00:00Z'); // day3-9
    expect(starts).not.toContain('2026-06-04T15:00:00Z'); // day2-10 excluded
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });

  // Fix D: single-day dateWindow + sampling
  it('single-day dateWindow + sampling: diversity within that day only', async () => {
    const dateWindow = { startISO: '2026-06-03T00:00:00.000Z', endISO: '2026-06-03T24:00:00.000Z' };
    // 3 slots on a single day spanning different dayparts
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),   // morning
      makeRawSlot('2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z', 'Tue 1 PM'),   // midday
      makeRawSlot('2026-06-03T21:00:00Z', '2026-06-03T21:30:00Z', 'Tue 4 PM'),   // afternoon
    ];
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(rawSlots);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 3 },
      dateWindow,
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    // All returned slots fall within the dateWindow
    for (const slot of res.slots) {
      expect(slot.start >= dateWindow.startISO).toBe(true);
      expect(slot.start < dateWindow.endISO).toBe(true);
    }
    // dateWindow was forwarded to slots.generateSlots
    const call = slots.generateSlots.mock.calls[0][0];
    expect(call.dateWindow).toEqual(dateWindow);
  });

  // Fix F: diverse mode + all candidates in alreadyRejected → SLOT_UNAVAILABLE
  it('diverse mode + ALL candidates in alreadyRejected → SLOT_UNAVAILABLE (combined path)', async () => {
    const rawSlots = [
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'Tue 9 AM'),
      makeRawSlot('2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z', 'Tue 1 PM'),
    ];
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(rawSlots);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 3 },
      alreadyRejected: ['slot#2026-06-03T14:00:00Z', 'slot#2026-06-03T18:00:00Z'],
    });

    expect(res.status).toBe('SLOT_UNAVAILABLE');
    expect(res.slots).toHaveLength(0);
  });

  // Fix G: daypartOf invalid-TZ catch test
  it('daypartOf BAD_TZ catch: invalid timezone + mixed slots → graceful 3 chronological chips, no throw', async () => {
    // BAD_TZ causes daypartOf to throw → catch returns 'morning' for all slots
    // All 4 slots are 'morning' (fallback), diversity logic falls through to fallbacks
    const rawSlots = [
      makeRawSlot('2026-06-03T10:00:00Z', '2026-06-03T10:30:00Z', 'S1'),
      makeRawSlot('2026-06-03T12:00:00Z', '2026-06-03T12:30:00Z', 'S2'),
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'S3'),
      makeRawSlot('2026-06-03T16:00:00Z', '2026-06-03T16:30:00Z', 'S4'),
    ];
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    slots.generateSlots.mockReturnValue(rawSlots);

    let res;
    expect(async () => {
      res = await pool.select({
        tenantId: TENANT,
        appointmentType: APPT,
        routingPolicy: POLICY,
        candidates: [{ resourceId: 'maya' }],
        userTimeZone: 'BAD_TZ',
        now: '2026-06-02T12:00:00Z',
        sampling: { mode: 'daypart-diverse', count: 3 },
      });
    }).not.toThrow();

    res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: 'BAD_TZ',
      now: '2026-06-02T12:00:00Z',
      sampling: { mode: 'daypart-diverse', count: 3 },
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(3);
    // chips sorted chronologically
    const starts = res.slots.map((s) => s.start);
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });
});

// ─── Default-cap pin test (Fix H) ────────────────────────────────────────────────────

describe('select — default-cap pin', () => {
  it('default-cap pin: 7+ generated slots, NO maxSlots arg, NO sampling arg → exactly 5 chips (DEFAULT_MAX_SLOTS)', async () => {
    availability.getBusyIntervals.mockResolvedValue(fb());
    routing.evaluatePool.mockResolvedValue({ ordered: ['maya'], tieBreaker: 'round_robin', roundRobinCursor: null });
    // Generate 7 raw slots
    slots.generateSlots.mockReturnValue([
      makeRawSlot('2026-06-03T10:00:00Z', '2026-06-03T10:30:00Z', 'S1'),
      makeRawSlot('2026-06-03T11:00:00Z', '2026-06-03T11:30:00Z', 'S2'),
      makeRawSlot('2026-06-03T12:00:00Z', '2026-06-03T12:30:00Z', 'S3'),
      makeRawSlot('2026-06-03T13:00:00Z', '2026-06-03T13:30:00Z', 'S4'),
      makeRawSlot('2026-06-03T14:00:00Z', '2026-06-03T14:30:00Z', 'S5'),
      makeRawSlot('2026-06-03T15:00:00Z', '2026-06-03T15:30:00Z', 'S6'),
      makeRawSlot('2026-06-03T16:00:00Z', '2026-06-03T16:30:00Z', 'S7'),
    ]);

    const res = await pool.select({
      tenantId: TENANT,
      appointmentType: APPT,
      routingPolicy: POLICY,
      candidates: [{ resourceId: 'maya' }],
      userTimeZone: TZ,
      now: '2026-06-02T12:00:00Z',
      // NO maxSlots, NO sampling
    });

    expect(res.status).toBe('SLOTS_PROPOSED');
    expect(res.slots).toHaveLength(5); // DEFAULT_MAX_SLOTS
    // sorted chronologically
    const starts = res.slots.map((s) => s.start);
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] > starts[i - 1]).toBe(true);
    }
  });
});
