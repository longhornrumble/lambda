'use strict';

// Tier-2 executor-path tests: BSH delegates the already-§B14-authorized mutation to the
// BCH executor via deps.invokeSchedulingExecutor. The boundary must still gate FIRST.
const { _executeViaExecutor, _doReschedule, _doCancel, runSchedulingTurn } = require('../schedulingFlow');

const BOOKING = { booking_id: 'bk1', tenant_id: 'T1', coordinator_email: 'c@x.com', external_event_id: 'evt-old' };
const BINDING = { booking_id: 'bk1', coordinator_id: 'c@x.com', intent: 'reschedule' };
const SLOT = { start: '2026-07-01T15:00:00Z', end: '2026-07-01T15:30:00Z' };

describe('_executeViaExecutor', () => {
  it('builds the scheduling_mutate payload and maps a success response', async () => {
    const seen = [];
    const invoke = async (p) => { seen.push(p); return { outcome: 'success', booking: { ...BOOKING, start_at: SLOT.start } }; };
    const res = await _executeViaExecutor('reschedule', { tenantId: 'T1', binding: BINDING, booking: BOOKING, newSlot: SLOT, deps: { invokeSchedulingExecutor: invoke }, logger: console });
    expect(res).toEqual({ executed: true, outcome: 'success', booking: expect.any(Object) });
    expect(seen[0]).toMatchObject({ action: 'scheduling_mutate', mutation: 'reschedule', tenantId: 'T1', coordinatorId: 'c@x.com', bookingId: 'bk1' });
    expect(seen[0].newSlot).toEqual(SLOT);
  });

  it('cancel payload omits newSlot', async () => {
    const seen = [];
    await _executeViaExecutor('cancel', { tenantId: 'T1', binding: BINDING, booking: BOOKING, deps: { invokeSchedulingExecutor: async (p) => { seen.push(p); return { outcome: 'deleted' }; } }, logger: console });
    expect(seen[0].mutation).toBe('cancel');
    expect('newSlot' in seen[0]).toBe(false);
  });

  it("executor outcome 'failed' → executed:false + email fallback", async () => {
    const res = await _executeViaExecutor('reschedule', { tenantId: 'T1', binding: BINDING, booking: BOOKING, newSlot: SLOT, deps: { invokeSchedulingExecutor: async () => ({ outcome: 'failed' }) }, logger: console });
    expect(res).toEqual({ executed: false, outcome: 'failed', fallback: 'email' });
  });

  it('executor THROW → executed:false + email fallback (never claims success)', async () => {
    const res = await _executeViaExecutor('cancel', { tenantId: 'T1', binding: BINDING, booking: BOOKING, deps: { invokeSchedulingExecutor: async () => { throw new Error('invoke 500'); } }, logger: console });
    expect(res).toEqual({ executed: false, outcome: 'failed', fallback: 'email' });
  });
});

describe('_doReschedule / _doCancel route to the executor when wired', () => {
  it('_doReschedule uses the executor (no facade needed)', async () => {
    let called = false;
    const res = await _doReschedule({ tenantId: 'T1', binding: BINDING, booking: BOOKING, newSlot: SLOT, deps: { invokeSchedulingExecutor: async () => { called = true; return { outcome: 'success', booking: BOOKING }; } }, logger: console });
    expect(called).toBe(true);
    expect(res.executed).toBe(true);
  });
  it('_doCancel uses the executor', async () => {
    let called = false;
    await _doCancel({ tenantId: 'T1', binding: BINDING, booking: BOOKING, deps: { invokeSchedulingExecutor: async () => { called = true; return { outcome: 'deleted', booking: BOOKING }; } }, logger: console });
    expect(called).toBe(true);
  });
});

describe('§B14 boundary holds — executor never invoked on an illegal transition', () => {
  it('confirm_reschedule from an ILLEGAL state (already booked) rejects WITHOUT invoking the executor', async () => {
    let invoked = false;
    const deps = {
      resolveBinding: async () => ({ booking_id: 'bk1', coordinator_id: 'c@x.com', intent: 'rescheduling_intent' }),
      // prior state 'booked' → confirm_reschedule's transition(booked → booked) is illegal
      // (the re-fire guard) → IllegalStateTransition → rejected before any execution.
      loadState: async () => ({ state: 'booked', selected_slot: SLOT }),
      loadBooking: async () => BOOKING,
      invokeSchedulingExecutor: async () => { invoked = true; return { outcome: 'success' }; },
    };
    // Force the detector to return confirm_reschedule by stubbing bedrock to emit it.
    const bedrock = { send: async () => ({ body: new TextEncoder().encode(JSON.stringify({ content: [{ text: '{"action":"confirm_reschedule"}' }] })) }) };
    const res = await runSchedulingTurn({ responseText: 'ok', conversationHistory: [], tenantId: 'T1', sessionId: 's1', config: {}, bedrock, write: () => {}, deps });
    expect(res.rejected).toBe(true);        // IllegalStateTransition → rejected
    expect(res.executed).toBeFalsy();
    expect(invoked).toBe(false);            // boundary gated BEFORE the executor
  });
});
