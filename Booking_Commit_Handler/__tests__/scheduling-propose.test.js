'use strict';

const fs = require('fs');
const path = require('path');
const { handleSchedulingPropose } = require('../scheduling-propose');

// ── Fixtures (no real DDB / Google) ──────────────────────────────────────────────────

const APPT = {
  appointment_type_id: 'apt-1',
  routing_policy_id: 'rp-1',
  name: 'Intake',
  timezone: 'America/Chicago',
  duration_minutes: 30,
};
const POLICY = { routing_policy_id: 'rp-1', tag_conditions: [] };
const CANDIDATES = [
  { resourceId: 'maya@org.org', scheduling_tags: ['intake'], coordinatorEmail: 'maya@org.org' },
  { resourceId: 'sam@org.org', scheduling_tags: ['intake'], coordinatorEmail: 'sam@org.org' },
];
// pool.select's REAL chip shape: generic (label only, no coordinator name) + per-slot
// candidateResourceIds. Note slot[1] has 1 candidate but the ROUTING pool (orderedPool)
// has 2 — poolSize must reflect orderedPool, not the per-slot list.
const SLOTS = [
  { slotId: 'slot#2026-07-01T15:00:00.000Z', start: '2026-07-01T15:00:00.000Z', end: '2026-07-01T15:30:00.000Z', label: 'Wed, Jul 1, 10:00 AM CDT', candidateResourceIds: ['maya@org.org', 'sam@org.org'] },
  { slotId: 'slot#2026-07-01T16:00:00.000Z', start: '2026-07-01T16:00:00.000Z', end: '2026-07-01T16:30:00.000Z', label: 'Wed, Jul 1, 11:00 AM CDT', candidateResourceIds: ['sam@org.org'] },
];
const PROPOSED_RETURN = {
  status: 'SLOTS_PROPOSED',
  poolBranch: 'multiple',
  orderedPool: ['maya@org.org', 'sam@org.org'],
  tieBreaker: 'round_robin',
  roundRobinCursor: { routingPolicyId: 'rp-1', previousResourceId: 'maya@org.org' },
  slots: SLOTS,
};

function baseInjected(overrides = {}) {
  const calls = { resolve: [], resolveDeps: [], getAppt: [], getPolicy: [], select: [], logs: [] };
  const injected = {
    getAppointmentType: async (a) => { calls.getAppt.push(a); return APPT; },
    getRoutingPolicy: async (a) => { calls.getPolicy.push(a); return POLICY; },
    resolveCandidates: async (a, deps) => { calls.resolve.push(a); calls.resolveDeps.push(deps); return CANDIDATES; },
    poolSelect: async (a) => { calls.select.push(a); return PROPOSED_RETURN; },
    logger: { log: (l) => calls.logs.push(l) },
    ...overrides,
  };
  return { calls, injected };
}

const PROPOSE_EVENT = {
  action: 'scheduling_propose',
  tenantId: 'T1',
  sessionId: 'sess-1',
  appointmentTypeId: 'apt-1',
  userTimeZone: 'America/Chicago',
};

// ── happy path + mapping ──────────────────────────────────────────────────────────────

describe('handleSchedulingPropose — happy path', () => {
  it('maps SLOTS_PROPOSED → outcome ok with 3-5 generic chips passed through unchanged', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.outcome).toBe('ok');
    expect(out.slots).toBe(SLOTS); // passed through by reference — not re-shaped
    expect(out.slots).toHaveLength(2);
    // generic: chips carry no coordinator name, only label + server-internal candidateResourceIds
    for (const s of out.slots) {
      expect(s).toHaveProperty('slotId');
      expect(s).toHaveProperty('label');
      expect(Array.isArray(s.candidateResourceIds)).toBe(true);
      expect(s).not.toHaveProperty('coordinatorName');
    }
  });

  it('poolSize is TOP-LEVEL = orderedPool.length (NOT per-slot candidateResourceIds.length)', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.poolSize).toBe(2); // orderedPool has 2; slot[1].candidateResourceIds has 1
    expect(out).not.toHaveProperty('slots.poolSize');
    expect(out.slots[1].candidateResourceIds).toHaveLength(1); // guard the contrast
  });

  it('carries tieBreaker + roundRobinCursor forward for the commit', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.tieBreaker).toBe('round_robin');
    expect(out.roundRobinCursor).toEqual({ routingPolicyId: 'rp-1', previousResourceId: 'maya@org.org' });
  });

  it('resolves THREE things from appointmentTypeId and calls pool.select with them', async () => {
    const { injected, calls } = baseInjected();
    await handleSchedulingPropose(
      { ...PROPOSE_EVENT, alreadyRejected: ['slot#2026-06-30T15:00:00.000Z'], windowStart: 'W1', windowEnd: 'W2' },
      injected
    );
    expect(calls.getAppt[0]).toEqual({ tenantId: 'T1', appointmentTypeId: 'apt-1' });
    expect(calls.getPolicy[0]).toEqual({ tenantId: 'T1', routingPolicyId: 'rp-1' });
    expect(calls.resolve[0]).toEqual({ tenantId: 'T1', appointmentTypeId: 'apt-1' });
    expect(calls.select).toHaveLength(1);
    const arg = calls.select[0];
    expect(arg.appointmentType).toBe(APPT);
    expect(arg.routingPolicy).toBe(POLICY);
    expect(arg.candidates).toBe(CANDIDATES);
    expect(arg.userTimeZone).toBe('America/Chicago');
    expect(arg.alreadyRejected).toEqual(['slot#2026-06-30T15:00:00.000Z']); // forwarded unchanged
    expect(arg.windowStart).toBe('W1');
    expect(arg.windowEnd).toBe('W2');
  });

  it('does NOT double-read: reuses the appt-type + policy it already read (3 DDB reads, not 5)', async () => {
    const { injected, calls } = baseInjected();
    await handleSchedulingPropose(PROPOSE_EVENT, injected);
    // the route itself reads the appt-type once + the policy once …
    expect(calls.getAppt).toHaveLength(1);
    expect(calls.getPolicy).toHaveLength(1);
    // … and hands resolveCandidates cached readers returning those SAME rows, so its
    // internal appt-type/policy hops hit no DynamoDB (only its employee Query is a fresh
    // read → 3 reads total, eliminating the +2 redundant GetItems).
    const deps = calls.resolveDeps[0];
    expect(deps).toBeDefined();
    expect(await deps.getAppointmentType()).toBe(APPT);
    expect(await deps.getRoutingPolicy()).toBe(POLICY);
  });
});

// ── no_availability + failure mapping ────────────────────────────────────────────────

describe('handleSchedulingPropose — no_availability', () => {
  it('maps SLOT_UNAVAILABLE → no_availability with empty slots and poolSize from orderedPool', async () => {
    const { injected } = baseInjected({
      poolSelect: async () => ({ status: 'SLOT_UNAVAILABLE', poolBranch: 'empty', orderedPool: [], tieBreaker: 'first_available', roundRobinCursor: null, slots: [] }),
    });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.outcome).toBe('no_availability');
    expect(out.slots).toEqual([]);
    expect(out.poolSize).toBe(0);
  });
});

describe('handleSchedulingPropose — failure folds to outcome:failed (no FunctionError)', () => {
  it('missing required fields → failed (and NO resolution I/O)', async () => {
    const { injected, calls } = baseInjected();
    expect((await handleSchedulingPropose({ action: 'scheduling_propose' }, injected)).outcome).toBe('failed');
    expect((await handleSchedulingPropose({ ...PROPOSE_EVENT, tenantId: undefined }, injected)).outcome).toBe('failed');
    expect((await handleSchedulingPropose({ ...PROPOSE_EVENT, appointmentTypeId: undefined }, injected)).outcome).toBe('failed');
    expect((await handleSchedulingPropose({ ...PROPOSE_EVENT, userTimeZone: undefined }, injected)).outcome).toBe('failed');
    expect(calls.getAppt).toHaveLength(0); // never resolved → no calendar I/O
    expect(calls.select).toHaveLength(0);
  });

  it('appointment type not found → failed', async () => {
    const { injected } = baseInjected({ getAppointmentType: async () => null });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out).toEqual({ outcome: 'failed', slots: [], poolSize: 0, error: 'appointment_type_not_found' });
  });

  it('appointment type without routing_policy_id → failed', async () => {
    const { injected } = baseInjected({ getAppointmentType: async () => ({ appointment_type_id: 'apt-1' }) });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.error).toBe('no_routing_policy');
  });

  it('routing policy not found → failed', async () => {
    const { injected } = baseInjected({ getRoutingPolicy: async () => null });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.error).toBe('routing_policy_not_found');
  });

  it('a thrown resolveCandidates folds to failed (not a rejection)', async () => {
    const { injected } = baseInjected({ resolveCandidates: async () => { throw new Error('DDB down'); } });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out).toEqual({ outcome: 'failed', slots: [], poolSize: 0, error: 'propose_error' });
  });

  it('a thrown pool.select folds to failed (not a rejection)', async () => {
    const { injected } = baseInjected({ poolSelect: async () => { throw new Error('freeBusy 500'); } });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.outcome).toBe('failed');
    expect(out.error).toBe('propose_error');
  });
});

// ── defensive / forward-compatible reads (malformed pool.select return) ──────────────

describe('handleSchedulingPropose — tolerates an under-shaped pool.select return', () => {
  it('omits tieBreaker/roundRobinCursor when pool.select does not supply them', async () => {
    const { injected } = baseInjected({
      poolSelect: async () => ({ status: 'SLOTS_PROPOSED', orderedPool: ['a'], slots: [SLOTS[0]] }),
    });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.outcome).toBe('ok');
    expect(out.poolSize).toBe(1);
    expect(out).not.toHaveProperty('tieBreaker');
    expect(out).not.toHaveProperty('roundRobinCursor');
  });

  it('a missing/undefined pool.select return → no_availability, empty slots, poolSize 0', async () => {
    const { injected } = baseInjected({ poolSelect: async () => undefined });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out).toEqual({ outcome: 'no_availability', slots: [], poolSize: 0 });
  });

  it('logs via logger.info when logger.log is absent', async () => {
    const infoLines = [];
    const { injected } = baseInjected({ logger: { info: (l) => infoLines.push(l) } });
    await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(infoLines.length).toBeGreaterThan(0);
    expect(infoLines.join('\n')).toMatch(/scheduling_propose_result/);
  });

  it('a thrown value with no .name still folds to failed (error_name unknown)', async () => {
    const lines = [];
    const { injected } = baseInjected({
      poolSelect: async () => { throw { code: 503 }; }, // eslint-disable-line no-throw-literal
      logger: { log: (l) => lines.push(l) },
    });
    const out = await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(out.error).toBe('propose_error');
    expect(lines.join('\n')).toMatch(/"error_name":"unknown"/);
  });
});

// ── PII-clean: the route logs no attendee identity ───────────────────────────────────

describe('handleSchedulingPropose — PII-clean logging', () => {
  it('emits no log line carrying an email or attendee name', async () => {
    const { injected, calls } = baseInjected();
    await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(calls.logs.length).toBeGreaterThan(0);
    const blob = calls.logs.join('\n');
    expect(blob).not.toMatch(/@/); // no email anywhere
    expect(blob).not.toMatch(/"email"|"name"|"attendee/i);
  });
});

// ── READ-ONLY: the route contains NO write-op call sites ─────────────────────────────
// Structural proof (not a runtime spy that an injected pool.select would render vacuous):
// the propose module never references a slot lock, round-robin advance, or any Booking
// write — and never imports a write-capable module. A future edit that adds one fails here.

describe('scheduling-propose.js — read-only (no write call sites in the source)', () => {
  // Scan CODE only — strip comments first (the docstring deliberately names the write ops
  // it AVOIDS, e.g. "bound at commit by pool.lockSlot()", which would false-positive).
  const code = fs
    .readFileSync(path.join(__dirname, '..', 'scheduling-propose.js'), 'utf8')
    .replace(/\/\*[\s\S]*?\*\//g, '') // block comments
    .replace(/\/\/.*$/gm, ''); // line comments

  it('references no slot-lock, round-robin advance, or Booking write op', () => {
    expect(code).not.toMatch(/lockSlot|advanceRoundRobin|revertRoundRobin/);
    expect(code).not.toMatch(/writeBooking|updateBooking|writeDegradedMarker|PutItemCommand|UpdateItemCommand|DeleteItemCommand/);
  });

  it('imports no write-capable module (booking-store / routing)', () => {
    expect(code).not.toMatch(/require\(\s*['"]\.\/booking-store['"]\s*\)/);
    expect(code).not.toMatch(/require\(\s*['"][^'"]*scheduling\/routing['"]\s*\)/);
  });

  it('only consumes pool (for select) + the candidate-resolver (reads)', () => {
    // pool.select is the only pool member it may use; assert no other pool.* call slips in.
    const poolCalls = code.match(/pool\.\w+/g) || [];
    expect(poolCalls.every((c) => c === 'pool.select')).toBe(true);
  });
});

// ── dispatch + fail-closed gate (the index.js block this slice owns) ─────────────────
// Drives the REAL handler() so the dispatch + gate + sub-handler are exercised together
// via pure DI (no jest.mock) — the gate lives in index.js, mirroring scheduling_mutate.

describe('index.js scheduling_propose dispatch — fail-closed gate', () => {
  const { handler } = require('../index');

  it('gate disabled → { outcome:failed, error:scheduling_disabled } and NO resolution I/O', async () => {
    const { injected, calls } = baseInjected({ isSchedulingEnabledForTenant: async () => false });
    const res = await handler(PROPOSE_EVENT, {}, injected);
    expect(res).toEqual({ outcome: 'failed', error: 'scheduling_disabled' });
    expect(calls.getAppt).toHaveLength(0); // sub-handler never invoked → no calendar I/O
    expect(calls.select).toHaveLength(0);
  });

  it('gate enabled → delegates to handleSchedulingPropose (mapped ok response)', async () => {
    const { injected } = baseInjected({ isSchedulingEnabledForTenant: async () => true });
    const res = await handler(PROPOSE_EVENT, {}, injected);
    expect(res.outcome).toBe('ok');
    expect(res.poolSize).toBe(2);
    expect(res.slots).toHaveLength(2);
  });
});

// ─── §B16e date_window passthrough (WS-T3-DAYPICK-BE) ────────────────────────────────────

describe('handleSchedulingPropose — §B16e date_window passthrough', () => {
  it('absent date_window: pool.select is called WITHOUT dateWindow (shipped behavior unchanged)', async () => {
    const { calls, injected } = baseInjected();
    await handleSchedulingPropose(PROPOSE_EVENT, injected);
    expect(calls.select).toHaveLength(1);
    const selectArg = calls.select[0];
    // No dateWindow key should be present when date_window is absent.
    expect(selectArg.dateWindow).toBeUndefined();
  });

  it('present date_window: pool.select receives dateWindow with startISO/endISO mapped from start/end', async () => {
    const { calls, injected } = baseInjected();
    const eventWithWindow = {
      ...PROPOSE_EVENT,
      date_window: { start: '2026-07-06T00:00:00.000Z', end: '2026-07-07T00:00:00.000Z' },
    };
    await handleSchedulingPropose(eventWithWindow, injected);
    expect(calls.select).toHaveLength(1);
    const selectArg = calls.select[0];
    expect(selectArg.dateWindow).toEqual({
      startISO: '2026-07-06T00:00:00.000Z',
      endISO: '2026-07-07T00:00:00.000Z',
    });
  });

  it('date_window with only start: dateWindow.startISO is set, endISO is undefined', async () => {
    const { calls, injected } = baseInjected();
    const eventWithWindow = {
      ...PROPOSE_EVENT,
      date_window: { start: '2026-07-06T00:00:00.000Z' },
    };
    await handleSchedulingPropose(eventWithWindow, injected);
    const selectArg = calls.select[0];
    expect(selectArg.dateWindow.startISO).toBe('2026-07-06T00:00:00.000Z');
    expect(selectArg.dateWindow.endISO).toBeUndefined();
  });

  it('date_window: outcome and response shape are unchanged (no regression)', async () => {
    const { injected } = baseInjected();
    const eventWithWindow = {
      ...PROPOSE_EVENT,
      date_window: { start: '2026-07-06T00:00:00.000Z', end: '2026-07-07T00:00:00.000Z' },
    };
    const res = await handleSchedulingPropose(eventWithWindow, injected);
    // Response shape is identical to the no-window path.
    expect(res.outcome).toBe('ok');
    expect(res.poolSize).toBe(2);
    expect(res.slots).toHaveLength(2);
    expect(res.tieBreaker).toBe('round_robin');
    expect(res.roundRobinCursor).toBeDefined();
  });

  it('date_window: the frozen windowStart/windowEnd fields still pass through unchanged', async () => {
    const { calls, injected } = baseInjected();
    const eventWithBoth = {
      ...PROPOSE_EVENT,
      windowStart: '2026-07-06T09:00:00Z',
      windowEnd: '2026-07-06T17:00:00Z',
      date_window: { start: '2026-07-06T00:00:00.000Z', end: '2026-07-07T00:00:00.000Z' },
    };
    await handleSchedulingPropose(eventWithBoth, injected);
    const selectArg = calls.select[0];
    expect(selectArg.windowStart).toBe('2026-07-06T09:00:00Z');
    expect(selectArg.windowEnd).toBe('2026-07-06T17:00:00Z');
    expect(selectArg.dateWindow).toEqual({
      startISO: '2026-07-06T00:00:00.000Z',
      endISO: '2026-07-07T00:00:00.000Z',
    });
  });
});
