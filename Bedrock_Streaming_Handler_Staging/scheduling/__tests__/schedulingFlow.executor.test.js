'use strict';

// Tier-2 executor-path tests: BSH delegates the already-§B14-authorized mutation to the
// BCH executor via deps.invokeSchedulingExecutor. The boundary must still gate FIRST.
// SR-3: _executeViaExecutor is no longer exported — exercise the executor path through
// _doReschedule / _doCancel (which enforce the executor-first check), never the raw helper.
const { _doReschedule, _doCancel, runSchedulingTurn } = require('../schedulingFlow');

const BOOKING = {
  booking_id: 'bk1', tenant_id: 'T1', coordinator_email: 'c@x.com', external_event_id: 'evt-old',
  attendee_email: 'vol@x.com', attendee_name: 'Vol Unteer', attendee_phone: '+15551234567',
};
const BINDING = { booking_id: 'bk1', coordinator_id: 'c@x.com', intent: 'reschedule' };
const SLOT = { start: '2026-07-01T15:00:00Z', end: '2026-07-01T15:30:00Z' };

describe('executor payload + outcome mapping (via _doReschedule/_doCancel)', () => {
  it('builds the scheduling_mutate payload and maps a success response', async () => {
    const seen = [];
    const invoke = async (p) => { seen.push(p); return { outcome: 'success', booking: { ...BOOKING, start_at: SLOT.start } }; };
    const res = await _doReschedule({ tenantId: 'T1', binding: BINDING, booking: BOOKING, newSlot: SLOT, deps: { invokeSchedulingExecutor: invoke }, logger: console });
    expect(res).toEqual({ executed: true, outcome: 'success', booking: expect.any(Object) });
    expect(seen[0]).toMatchObject({ action: 'scheduling_mutate', mutation: 'reschedule', tenantId: 'T1', coordinatorId: 'c@x.com' });
    expect(seen[0].newSlot).toEqual(SLOT);
    expect('bookingId' in seen[0]).toBe(false); // CR nit: dead top-level field removed
  });

  it('NTH1/S1.1: payload.booking is PII-projected — keeps attendee_email + attendee_phone + organization_name (reminders), drops un-listed fields', async () => {
    const seen = [];
    const booking = { ...BOOKING, organization_name: 'Austin Angels', reminder_schedule_state: { tiers: ['t24h'] } };
    await _doReschedule({ tenantId: 'T1', binding: BINDING, booking, newSlot: SLOT, deps: { invokeSchedulingExecutor: async (p) => { seen.push(p); return { outcome: 'success', booking }; } }, logger: console });
    expect(seen[0].booking.attendee_email).toBe('vol@x.com');       // needed to rebuild the invite
    expect(seen[0].booking.external_event_id).toBe('evt-old');
    // S1.1: phone + org are now CARRIED — the executor's reminder rebind needs them (phone →
    // TCPA-gated SMS supplement; org → real reminder copy instead of "your appointment with us").
    expect(seen[0].booking.attendee_phone).toBe('+15551234567');
    expect(seen[0].booking.organization_name).toBe('Austin Angels');
    // projection discipline still holds: a field NOT in _EXEC_BOOKING_FIELDS is stripped.
    expect('reminder_schedule_state' in seen[0].booking).toBe(false);
  });

  it('cancel payload omits newSlot', async () => {
    const seen = [];
    await _doCancel({ tenantId: 'T1', binding: BINDING, booking: BOOKING, deps: { invokeSchedulingExecutor: async (p) => { seen.push(p); return { outcome: 'deleted', booking: BOOKING }; } }, logger: console });
    expect(seen[0].mutation).toBe('cancel');
    expect('newSlot' in seen[0]).toBe(false);
  });

  it("executor outcome 'failed' → executed:false + email fallback", async () => {
    const res = await _doReschedule({ tenantId: 'T1', binding: BINDING, booking: BOOKING, newSlot: SLOT, deps: { invokeSchedulingExecutor: async () => ({ outcome: 'failed' }) }, logger: console });
    expect(res).toEqual({ executed: false, outcome: 'failed', fallback: 'email' });
  });

  it("FS7: executor 'slot_unavailable' (TOCTOU re-check blocked the move) → executed:false + reason, NO email fallback", async () => {
    const res = await _doReschedule({ tenantId: 'T1', binding: BINDING, booking: BOOKING, newSlot: SLOT, deps: { invokeSchedulingExecutor: async () => ({ outcome: 'slot_unavailable', reason: 'recheck_busy' }) }, logger: console });
    expect(res).toEqual({ executed: false, outcome: 'slot_unavailable', reason: 'slot_unavailable' });
    expect(res.fallback).toBeUndefined(); // NOT the email path — that would falsely advance the session to 'booked'
  });

  it("executor returns { error } on a non-failed outcome → still fallback (defensive guard)", async () => {
    const res = await _doCancel({ tenantId: 'T1', binding: BINDING, booking: BOOKING, deps: { invokeSchedulingExecutor: async () => ({ outcome: 'deleted', error: 'weird' }) }, logger: console });
    expect(res.fallback).toBe('email');
  });

  it('executor THROW → executed:false + email fallback (never claims success)', async () => {
    const res = await _doCancel({ tenantId: 'T1', binding: BINDING, booking: BOOKING, deps: { invokeSchedulingExecutor: async () => { throw new Error('invoke 500'); } }, logger: console });
    expect(res).toEqual({ executed: false, outcome: 'failed', fallback: 'email' });
  });

  it('CR-Low: unresolved coordinatorId → fallback WITHOUT invoking the executor', async () => {
    let invoked = false;
    const res = await _doCancel({
      tenantId: 'T1',
      binding: { booking_id: 'bk1', intent: 'cancellation_intent' }, // no coordinator_id
      booking: { booking_id: 'bk1', tenant_id: 'T1' },                // no coordinator_email/resource_id
      deps: { invokeSchedulingExecutor: async () => { invoked = true; return { outcome: 'deleted' }; } },
      logger: console,
    });
    expect(res.fallback).toBe('email');
    expect(invoked).toBe(false);
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

describe('SR-2: email-fallback advances state (terminal turn → no executor re-fire)', () => {
  it('a failed confirm_cancel advances session → booked + emits the email notice', async () => {
    const saved = [];
    const written = [];
    const deps = {
      resolveBinding: async () => ({ booking_id: 'bk1', coordinator_id: 'c@x.com', intent: 'cancellation_intent' }),
      loadState: async () => ({ state: 'canceling' }),
      loadBooking: async () => BOOKING,
      saveState: async (s) => { saved.push(s); },
      invokeSchedulingExecutor: async () => ({ outcome: 'failed' }), // → fallback:'email'
    };
    const bedrock = { send: async () => ({ body: new TextEncoder().encode(JSON.stringify({ content: [{ text: '{"action":"confirm_cancel"}' }] })) }) };
    const res = await runSchedulingTurn({ responseText: 'ok', conversationHistory: [], tenantId: 'T1', sessionId: 's1', config: {}, bedrock, write: (d) => written.push(d), deps });
    expect(res.fallback).toBe('email');
    expect(saved.some((s) => s.state === 'booked')).toBe(true);          // advanced → re-fire now rejects
    expect(written.some((d) => d.includes('scheduling_notice'))).toBe(true); // user told "we'll confirm by email"
  });
});

describe('FS7: reschedule slot taken between offer and confirm → no state advance + slot-gone notice', () => {
  it('confirm_reschedule with executor slot_unavailable → session NOT advanced to booked; guest told the time is gone', async () => {
    const saved = [];
    const written = [];
    const deps = {
      resolveBinding: async () => ({ booking_id: 'bk1', coordinator_id: 'c@x.com', intent: 'rescheduling_intent' }),
      loadState: async () => ({ state: 'confirming', selected_slot: SLOT }),
      loadBooking: async () => BOOKING,
      saveState: async (s) => { saved.push(s); },
      // BCH's freeBusy re-check found the NEW slot already taken → slot_unavailable (no calendar op).
      invokeSchedulingExecutor: async () => ({ outcome: 'slot_unavailable', reason: 'recheck_busy' }),
    };
    const bedrock = { send: async () => ({ body: new TextEncoder().encode(JSON.stringify({ content: [{ text: '{"action":"confirm_reschedule"}' }] })) }) };
    const res = await runSchedulingTurn({ responseText: 'ok', conversationHistory: [], tenantId: 'T1', sessionId: 's1', config: {}, bedrock, write: (d) => written.push(d), deps });
    expect(res).toMatchObject({ handled: true, executed: false, action: 'confirm_reschedule', reason: 'slot_unavailable' });
    expect(saved.some((s) => s.state === 'booked')).toBe(false);                    // booking kept its OLD time — never marked booked
    expect(written.some((d) => d.includes('"notice":"slot_unavailable"'))).toBe(true); // guest told the time is gone (not a false "moved!")
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
