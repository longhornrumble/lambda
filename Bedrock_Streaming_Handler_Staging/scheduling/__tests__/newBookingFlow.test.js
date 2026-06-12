/**
 * WS-NEWBOOK-FLOW — newBookingFlow (the in-chat new-booking flow + §B14 boundary) tests.
 *
 * Done-bar coverage (work-order WS-NEWBOOK-FLOW):
 *  - the §B16b action-detector GATES execution: free-text (action:'none') → NO commit;
 *    a structured {action:'confirm_book'} from 'confirming' → invokeBookingCommit once
 *  - confirm_book commits ONLY from 'confirming' (proposing/qualifying → illegal → rejected,
 *    NO commit)
 *  - double-fire guard: commit success advances to 'booked'; a second confirm_book → rejected
 *  - proposing delegates to §B16a invokeProposal; advance qualifying→proposing ONLY on
 *    outcome:'ok' (same saveState that persists slots); 'no_availability' STAYS in qualifying
 *  - the 'none' self-loop ACCUMULATES presented slotIds → alreadyRejected on re-propose
 *  - attendee-not-yet-known: confirm_book without attendee.email → rejected (no commit)
 *  - booked delegates to §B16c invokeBookingCommit; SLOT_UNAVAILABLE → back to proposing;
 *    COMMIT_FAILED / invoke error → "confirm by email" fallback + advance booked; BOOKED →
 *    reveals the assigned coordinator (resourceId)
 *  - no new-booking session / a recovery-loop session → { handled:false } (no-regression)
 *  - feature flag off → { handled:false } (dormant)
 *  - invoke seam unwired → execution skipped non-fatally (detection + transitions still run)
 *  - the §B14 detector is fail-closed (unparseable / no bedrock → 'none' → no commit)
 */

const {
  runNewBookingTurn,
  detectNewBookingAction,
  NEW_BOOKING_STATES,
} = require('../newBookingFlow');

// ── fakes ────────────────────────────────────────────────────────────────────────────

// Bedrock client whose post-stream call returns a canned §B16b action object.
function fakeBedrock(actionObj) {
  return {
    send: jest.fn().mockResolvedValue({
      body: new TextEncoder().encode(JSON.stringify({ content: [{ text: JSON.stringify(actionObj) }] })),
    }),
  };
}
// Bedrock client returning raw (possibly non-JSON) text.
function fakeBedrockRaw(text) {
  return {
    send: jest.fn().mockResolvedValue({
      body: new TextEncoder().encode(JSON.stringify({ content: [{ text }] })),
    }),
  };
}

const SLOT = Object.freeze({
  slotId: 's1',
  start: '2026-06-10T19:00:00Z',
  end: '2026-06-10T19:30:00Z',
  label: 'Wed, Jun 10 · 2:00 PM',
  candidateResourceIds: ['maya@org.example', 'sam@org.example'],
});

// The integrator-supplied resolved context (appt-type / routing / identity).
const QCTX = Object.freeze({
  appointmentTypeId: 'apt_intro',
  userTimeZone: 'America/Chicago',
  conference_type: 'google_meet',
  appointment_type: { id: 'apt_intro', name: 'Intro call', duration_minutes: 30 },
  attendee: { email: 'vol@example.com', first_name: 'Vol' },
});
// Same context but identity not yet resolved (§B16d attendee-not-yet-known).
const QCTX_NO_EMAIL = Object.freeze({ ...QCTX, attendee: { first_name: 'Vol' } });

const PROPOSE_OK = Object.freeze({
  outcome: 'ok',
  slots: [SLOT],
  poolSize: 2,
  tieBreaker: 'round_robin',
  roundRobinCursor: 'cur-7',
});

const ENABLED = { feature_flags: { scheduling_enabled: true } };

const baseTurn = (overrides) => ({
  responseText: 'ok',
  conversationHistory: [],
  tenantId: 'TEN',
  sessionId: 'sess-1',
  config: ENABLED,
  ...overrides,
});

// ── §B16b detector ─────────────────────────────────────────────────────────────────────

describe('detectNewBookingAction (§B14 focused post-stream call) — fail-closed', () => {
  test('no bedrock client → none', async () => {
    expect((await detectNewBookingAction({})).action).toBe('none');
  });
  test('unparseable model output → none', async () => {
    const out = await detectNewBookingAction({ bedrock: fakeBedrockRaw('Sounds good!') });
    expect(out.action).toBe('none');
  });
  test('unknown action value → none', async () => {
    const out = await detectNewBookingAction({ bedrock: fakeBedrock({ action: 'confirm_reschedule' }) });
    expect(out.action).toBe('none'); // recovery-loop vocab is NOT valid here
  });
  test('structured select_slot carries the slotId', async () => {
    const out = await detectNewBookingAction({ bedrock: fakeBedrock({ action: 'select_slot', slotId: 's1' }) });
    expect(out).toMatchObject({ action: 'select_slot', slotId: 's1' });
  });
  test('structured confirm_book parsed', async () => {
    const out = await detectNewBookingAction({ bedrock: fakeBedrock({ action: 'confirm_book' }) });
    expect(out.action).toBe('confirm_book');
  });
  test('bedrock.send THROWS → none, routed through the INJECTED logger (not console)', async () => {
    const logger = { error: jest.fn(), warn: jest.fn() };
    const bedrock = { send: jest.fn().mockRejectedValue(new Error('bedrock 5xx')) };
    const out = await detectNewBookingAction({ bedrock, logger });
    expect(out.action).toBe('none');
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error.mock.calls[0][0]).toContain('action detect failed');
  });
});

// ── no-regression gates ──────────────────────────────────────────────────────────────

describe('runNewBookingTurn — no-regression gates', () => {
  test('feature flag OFF → { handled:false }, nothing invoked', async () => {
    const invokeProposal = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      config: {}, // no feature_flags.scheduling_enabled
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), invokeProposal },
    }));
    expect(res).toEqual({ handled: false });
    expect(invokeProposal).not.toHaveBeenCalled();
  });

  test('no scheduling session (loadState → null) → { handled:false }', async () => {
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => null },
    }));
    expect(res).toEqual({ handled: false });
  });

  test('a recovery-loop session (state=rescheduling) is NOT ours → { handled:false }', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => ({ state: 'rescheduling' }), invokeBookingCommit, qualifyingContext: QCTX },
    }));
    expect(res).toEqual({ handled: false });
    expect(invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('missing tenantId/sessionId → { handled:false }', async () => {
    expect(await runNewBookingTurn({ tenantId: '', sessionId: 's', config: ENABLED })).toEqual({ handled: false });
  });
});

// ── qualifying → proposing (propose entry; strand-prevention) ──────────────────────────

describe('runNewBookingTurn — qualifying entry: propose + advance on ok', () => {
  test("action 'none' in qualifying → invokeProposal, advance to proposing, persist slots+meta, emit SSE", async () => {
    const write = jest.fn();
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue(PROPOSE_OK);
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX, invokeProposal, saveState },
    }));

    expect(invokeProposal).toHaveBeenCalledTimes(1);
    expect(invokeProposal.mock.calls[0][0]).toMatchObject({
      action: 'scheduling_propose', tenantId: 'TEN', sessionId: 'sess-1',
      appointmentTypeId: 'apt_intro', userTimeZone: 'America/Chicago', alreadyRejected: [],
    });
    expect(res).toMatchObject({ handled: true, executed: false, state: 'proposing' });
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({
      state: 'proposing',
      candidate_slots: [SLOT],
      proposal: { poolSize: 2, tieBreaker: 'round_robin', roundRobinCursor: 'cur-7' },
    }));
    expect(write).toHaveBeenCalledTimes(1);
    expect(write.mock.calls[0][0]).toContain('scheduling_slots');
  });

  test('forwards windowStart/windowEnd from qualifyingContext when present (camel + snake)', async () => {
    const invokeProposal = jest.fn().mockResolvedValue(PROPOSE_OK);
    // camelCase window on qctx
    await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying' }),
        qualifyingContext: { ...QCTX, windowStart: '2026-06-10T00:00:00Z', windowEnd: '2026-06-17T00:00:00Z' },
        invokeProposal, saveState: jest.fn(),
      },
    }));
    expect(invokeProposal.mock.calls[0][0]).toMatchObject({
      windowStart: '2026-06-10T00:00:00Z', windowEnd: '2026-06-17T00:00:00Z',
    });

    // snake_case window on qctx (schema-discipline)
    const invokeProposal2 = jest.fn().mockResolvedValue(PROPOSE_OK);
    await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying' }),
        qualifyingContext: { ...QCTX, window_start: 'S', window_end: 'E' },
        invokeProposal: invokeProposal2, saveState: jest.fn(),
      },
    }));
    expect(invokeProposal2.mock.calls[0][0]).toMatchObject({ windowStart: 'S', windowEnd: 'E' });
  });

  test('no window on qualifyingContext → payload omits windowStart/windowEnd (no explicit undefined)', async () => {
    const invokeProposal = jest.fn().mockResolvedValue(PROPOSE_OK);
    await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX, invokeProposal, saveState: jest.fn() },
    }));
    const payload = invokeProposal.mock.calls[0][0];
    expect('windowStart' in payload).toBe(false);
    expect('windowEnd' in payload).toBe(false);
  });

  test("outcome 'failed' (transient BCH/infra error) → graceful no-op: no picker, no saveState, stay in fromState (fix #4)", async () => {
    // Fix #4: outcome:'failed' is a transient infra error, NOT a §B16e trigger.
    // Pre-PR behavior restored: no picker emit, no picker_cycles increment, NO saveState.
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({ outcome: 'failed', error: 'pool_read_error' });
    const writes = [];
    const res = await runNewBookingTurn(baseTurn({
      write: (msg) => writes.push(msg),
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX, invokeProposal, saveState },
    }));
    expect(res.handled).toBe(true);
    expect(res.executed).toBe(false);
    expect(res.state).toBe('qualifying'); // state MUST stay qualifying (strand-prevention)
    expect(res.reason).toBe('propose_failed_outcome');
    // KEY assertions for fix #4: no picker emitted, no saveState called
    expect(writes.some((w) => w.includes('scheduling_day_picker'))).toBe(false);
    expect(writes.some((w) => w.includes('scheduling_notice'))).toBe(false);
    expect(saveState).not.toHaveBeenCalled(); // RESTORED pin: fix #4 requirement
  });

  test("outcome 'no_availability' → STAY in qualifying (no advance, no strand); §B16e emits picker", async () => {
    // §B16e replaces the scheduling_no_availability emit with the day-picker.
    const write = jest.fn();
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({ outcome: 'no_availability', slots: [] });
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX, invokeProposal, saveState },
    }));
    expect(res.handled).toBe(true);
    expect(res.executed).toBe(false);
    expect(res.state).toBe('qualifying'); // state MUST NOT advance (strand-prevention)
    // §B16e: scheduling_no_availability is replaced by scheduling_day_picker.
    expect(write.mock.calls.some((args) => args[0].includes('scheduling_no_availability'))).toBe(false);
    expect(write.mock.calls.some((args) => args[0].includes('scheduling_day_picker'))).toBe(true);
  });

  test('no appointmentTypeId resolved yet (multi-type tenant asking which) → STAY qualifying, no propose', async () => {
    const invokeProposal = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: { attendee: { email: 'v@e.com' } }, invokeProposal },
    }));
    expect(invokeProposal).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, state: 'qualifying', reason: 'awaiting_appointment_type' });
  });

  test('propose seam unwired → execution skipped non-fatally (stay qualifying)', async () => {
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX /* no invokeProposal */ },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, state: 'qualifying', reason: 'propose_seam_unwired' });
  });

  test('invokeProposal THROWS → non-fatal, stay qualifying (failure path)', async () => {
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockRejectedValue(new Error('Lambda timeout'));
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX, invokeProposal, saveState },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, state: 'qualifying', reason: 'propose_failed' });
    expect(saveState).not.toHaveBeenCalled(); // never advanced
  });
});

// ── proposing 'none' self-loop: alreadyRejected accumulation ───────────────────────────

describe("runNewBookingTurn — proposing 'none' self-loop accumulates alreadyRejected", () => {
  test('re-propose excludes prior rejects + the slots just presented', async () => {
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({ ...PROPOSE_OK, slots: [{ ...SLOT, slotId: 's9' }] });
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'proposing', candidate_slots: [{ slotId: 's1' }, { slotId: 's2' }], rejected_slot_ids: ['s0'] }),
        qualifyingContext: QCTX, invokeProposal, saveState,
      },
    }));
    // accumulated set = prior rejects ∪ just-presented ids
    expect(invokeProposal.mock.calls[0][0].alreadyRejected.sort()).toEqual(['s0', 's1', 's2']);
    expect(res).toMatchObject({ state: 'proposing' });
    // persists the accumulated rejects so the NEXT self-loop grows further
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({
      state: 'proposing', candidate_slots: [{ ...SLOT, slotId: 's9' }], rejected_slot_ids: ['s0', 's1', 's2'],
    }));
  });
});

// ── select_slot: proposing → confirming ────────────────────────────────────────────────

describe('runNewBookingTurn — select_slot advances proposing → confirming', () => {
  test('records the chosen slot (with candidateResourceIds), no commit', async () => {
    const saveState = jest.fn();
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'select_slot', slotId: 's1' }),
      deps: {
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT], proposal: { poolSize: 2 } }),
        qualifyingContext: QCTX, saveState, invokeBookingCommit,
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, action: 'select_slot', state: 'confirming' });
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({
      state: 'confirming',
      selected_slot: expect.objectContaining({ slotId: 's1', candidateResourceIds: SLOT.candidateResourceIds }),
      proposal: { poolSize: 2 },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('select_slot for an unknown slotId → rejected, nothing persisted as confirming', async () => {
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'select_slot', slotId: 'nope' }),
      deps: { loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT] }), qualifyingContext: QCTX, saveState: jest.fn() },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true, reason: 'unknown_slot' });
  });

  // §B14 exhaustive illegal-state coverage (mirrors the confirm_book set): select_slot is
  // legal ONLY from 'proposing'. From any other state → IllegalStateTransition → rejected,
  // BEFORE the candidate lookup (so even a matching slotId can't advance).
  test.each(['qualifying', 'confirming', 'booked'])(
    'select_slot from %s → IllegalStateTransition → rejected, nothing persisted',
    async (state) => {
      const saveState = jest.fn();
      const res = await runNewBookingTurn(baseTurn({
        bedrock: fakeBedrock({ action: 'select_slot', slotId: 's1' }),
        deps: { loadState: async () => ({ state, candidate_slots: [SLOT] }), qualifyingContext: QCTX, saveState },
      }));
      expect(res).toMatchObject({ handled: true, executed: false, rejected: true });
      expect(res.reason).toMatch(/Illegal/i);
      expect(saveState).not.toHaveBeenCalled();
    }
  );
});

// ── confirm_book: the commit (§B16c) — the highest-risk surface ─────────────────────────

describe('runNewBookingTurn — confirm_book commits ONLY from confirming', () => {
  const confirmingState = { state: 'confirming', selected_slot: SLOT, proposal: { poolSize: 2, tieBreaker: 'round_robin', roundRobinCursor: 'cur-7' } };

  test('confirm_book in confirming → invokeBookingCommit ONCE with the §B16c payload; advance booked; reveal coordinator', async () => {
    const write = jest.fn();
    const saveState = jest.fn();
    const invokeBookingCommit = jest.fn().mockResolvedValue({
      status: 'BOOKED', bookingId: 'bk_new', resourceId: 'maya@org.example', booking: { booking_id: 'bk_new' },
    });
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, invokeBookingCommit, saveState },
    }));

    expect(invokeBookingCommit).toHaveBeenCalledTimes(1);
    const payload = invokeBookingCommit.mock.calls[0][0];
    expect(payload).toMatchObject({
      tenant_id: 'TEN', session_id: 'sess-1',
      slot: { start: SLOT.start, end: SLOT.end, candidateResourceIds: SLOT.candidateResourceIds },
      attendee: QCTX.attendee,
      conference_type: 'google_meet',
      pool_size: 2,                 // §B16c: TOP-LEVEL poolSize, NOT candidateResourceIds.length (=2 here but proven distinct below)
      tie_breaker: 'round_robin',
      round_robin_cursor: 'cur-7',
    });
    expect(payload.action).toBeUndefined(); // commit is the DEFAULT route (no action discriminator)
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'booked' }));
    expect(res).toMatchObject({ handled: true, executed: true, action: 'confirm_book', outcome: 'booked', resourceId: 'maya@org.example', state: 'booked' });
    expect(write.mock.calls.some((c) => c[0].includes('scheduling_booked'))).toBe(true);
  });

  test('pool_size = propose TOP-LEVEL poolSize, NOT slot.candidateResourceIds.length', async () => {
    const invokeBookingCommit = jest.fn().mockResolvedValue({ status: 'BOOKED', bookingId: 'b', resourceId: 'r' });
    // selected slot has 2 candidateResourceIds, but the routing pool was 5.
    await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: {
        loadState: async () => ({ state: 'confirming', selected_slot: SLOT, proposal: { poolSize: 5 } }),
        qualifyingContext: QCTX, invokeBookingCommit, saveState: jest.fn(),
      },
    }));
    expect(invokeBookingCommit.mock.calls[0][0].pool_size).toBe(5);
  });

  test('confirm_book from PROPOSING → IllegalStateTransition → rejected, commit NOT called', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => ({ state: 'proposing', selected_slot: SLOT }), qualifyingContext: QCTX, invokeBookingCommit },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true });
    expect(res.reason).toMatch(/Illegal/i);
  });

  test('confirm_book from QUALIFYING → rejected, commit NOT called', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => ({ state: 'qualifying' }), qualifyingContext: QCTX, invokeBookingCommit },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true });
  });

  test('DOUBLE-FIRE GUARD: a second confirm_book (state already booked) → rejected, commit NOT called', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => ({ state: 'booked', selected_slot: SLOT }), qualifyingContext: QCTX, invokeBookingCommit },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled(); // booked→booked is illegal
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true });
  });

  test('attendee-not-yet-known: confirm_book without attendee.email → rejected (no commit, no advance)', async () => {
    const invokeBookingCommit = jest.fn();
    const saveState = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX_NO_EMAIL, invokeBookingCommit, saveState },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled();
    expect(saveState).not.toHaveBeenCalled(); // stays in confirming — user can supply email + retry
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true, reason: 'identity_required', state: 'confirming' });
  });

  test('free text in confirming (action none) → NO commit (the §B14 boundary)', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      responseText: "Great, you're all booked!", // LLM prose — must NOT commit
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, invokeBookingCommit },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false, action: 'none', state: 'confirming' });
  });
});

describe('runNewBookingTurn — confirm_book commit outcomes', () => {
  const confirmingState = { state: 'confirming', selected_slot: SLOT, proposal: { poolSize: 2 } };

  test('SLOT_UNAVAILABLE → return to proposing (re-offer), not booked', async () => {
    const saveState = jest.fn();
    const invokeBookingCommit = jest.fn().mockResolvedValue({ status: 'SLOT_UNAVAILABLE', action: 'reoffer', reason: 'lost_race' });
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: {
        loadState: async () => ({ ...confirmingState, candidate_slots: [SLOT] }),
        qualifyingContext: QCTX, invokeBookingCommit, saveState,
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, state: 'proposing', reason: 'slot_unavailable' });
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'proposing' }));
  });

  test('COMMIT_FAILED → "confirm by email" fallback notice + advance booked (no silent no-op)', async () => {
    const write = jest.fn();
    const saveState = jest.fn();
    const invokeBookingCommit = jest.fn().mockResolvedValue({ status: 'COMMIT_FAILED', action: 'graceful_error', reason: 'calendar_5xx' });
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, invokeBookingCommit, saveState },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, outcome: 'failed', fallback: 'email', state: 'booked' });
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'booked' })); // double-fire guard
    expect(write.mock.calls.some((c) => c[0].includes('request_received_email_followup'))).toBe(true);
  });

  test('invoke throws → fallback email + advance booked', async () => {
    const saveState = jest.fn();
    const invokeBookingCommit = jest.fn().mockRejectedValue(new Error('Lambda timeout'));
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, invokeBookingCommit, saveState },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, fallback: 'email', state: 'booked' });
  });

  test('ALREADY_CONFIRMED (C11 idempotent) → treated as executed, advance booked', async () => {
    const invokeBookingCommit = jest.fn().mockResolvedValue({ status: 'ALREADY_CONFIRMED', bookingId: 'bk_x', booking: {} });
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, invokeBookingCommit, saveState: jest.fn() },
    }));
    expect(res).toMatchObject({ handled: true, executed: true, outcome: 'already_confirmed', state: 'booked' });
  });

  test('commit seam unwired → execution skipped non-fatally (no advance)', async () => {
    const saveState = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, saveState /* no invokeBookingCommit */ },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, reason: 'commit_seam_unwired' });
    expect(saveState).not.toHaveBeenCalled(); // nothing committed → don't advance to booked
  });

  test('SCHEDULING_DISABLED (defense-in-depth) → rejected, no email, no advance', async () => {
    const write = jest.fn();
    const saveState = jest.fn();
    const invokeBookingCommit = jest.fn().mockResolvedValue({ status: 'SCHEDULING_DISABLED', reason: 'flag_off' });
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => confirmingState, qualifyingContext: QCTX, invokeBookingCommit, saveState },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, outcome: 'disabled', rejected: true });
    expect(res.state).toBe('confirming'); // did NOT advance to booked
    expect(saveState).not.toHaveBeenCalled();
    expect(write.mock.calls.some((c) => c[0].includes('request_received_email_followup'))).toBe(false);
  });

  test('missing pool_size (proposal meta not persisted) → fail LOUD, NOT a length floor; commit NOT called', async () => {
    const saveState = jest.fn();
    const logger = { error: jest.fn(), warn: jest.fn() };
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: {
        // selected_slot has 2 candidateResourceIds, but proposal.poolSize is absent (mis-wire).
        loadState: async () => ({ state: 'confirming', selected_slot: SLOT /* no proposal */ }),
        qualifyingContext: QCTX, invokeBookingCommit, saveState, logger,
      },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled(); // did NOT proceed with a fabricated pool_size
    expect(res).toMatchObject({ handled: true, executed: false, outcome: 'failed', error: 'missing_pool_size' });
    expect(res.state).toBe('confirming'); // did NOT advance to booked
    expect(saveState).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalled();
  });

  test('confirm_book with no slot persisted → rejected (no_slot_selected), commit NOT called', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => ({ state: 'confirming' /* no selected_slot */ }), qualifyingContext: QCTX, invokeBookingCommit },
    }));
    expect(invokeBookingCommit).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true, reason: 'no_slot_selected' });
  });

  test('loadState THROWS (outer catch) → non-fatal { handled:false, error:true }', async () => {
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_book' }),
      deps: { loadState: async () => { throw new Error('DDB down'); }, qualifyingContext: QCTX },
    }));
    expect(res).toEqual({ handled: false, error: true });
  });
});

// ── sanity: the exported state set ─────────────────────────────────────────────────────

describe('NEW_BOOKING_STATES', () => {
  test('drives the new-booking arc only (excludes the recovery-loop entry states)', () => {
    expect(NEW_BOOKING_STATES).toEqual(['qualifying', 'proposing', 'confirming', 'booked']);
    expect(NEW_BOOKING_STATES).not.toContain('rescheduling');
    expect(NEW_BOOKING_STATES).not.toContain('canceling');
  });
});

// ── audit fix #5: scheduling_day_selected validated against offered strip ────────────

describe('runNewBookingTurn — scheduling_day_selected validated against offered strip (fix #5)', () => {
  const OFFERED_DAYS = [
    { date: '2026-06-20', label: 'Sat, Jun 20' },
    { date: '2026-06-21', label: 'Sun, Jun 21' },
    { date: '2026-06-22', label: 'Mon, Jun 22' },
  ];

  test('past date (not in offered strip) → silent no-op: no propose, no cycle burn, no SSE', async () => {
    const invokeProposal = jest.fn();
    const saveState = jest.fn();
    const write = jest.fn();
    // A date that was never offered (2026-06-01 is not in OFFERED_DAYS)
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying', picker_days: OFFERED_DAYS }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
        schedulingDaySelected: '2026-06-01', // past/not-offered date
      },
    }));
    expect(res.handled).toBe(false); // fall-through: not handled → normal chat
    expect(res.reason).toBe('day_not_offered');
    expect(invokeProposal).not.toHaveBeenCalled(); // no propose
    expect(saveState).not.toHaveBeenCalled();      // no cycle burn
    expect(write).not.toHaveBeenCalled();           // no SSE
  });

  test('valid-format-but-not-offered date → same no-op behavior', async () => {
    const invokeProposal = jest.fn();
    const saveState = jest.fn();
    // 2026-07-15 is a valid YYYY-MM-DD but not in OFFERED_DAYS
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying', picker_days: OFFERED_DAYS }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
        schedulingDaySelected: '2026-07-15',
      },
    }));
    expect(res.handled).toBe(false);
    expect(res.reason).toBe('day_not_offered');
    expect(invokeProposal).not.toHaveBeenCalled();
    expect(saveState).not.toHaveBeenCalled();
  });

  test('offered date → proceeds to _handleDaySelected (invokeProposal called)', async () => {
    const invokeProposal = jest.fn().mockResolvedValue({ ...PROPOSE_OK });
    const saveState = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying', picker_days: OFFERED_DAYS }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
        schedulingDaySelected: '2026-06-20', // IS in OFFERED_DAYS
      },
    }));
    expect(invokeProposal).toHaveBeenCalledTimes(1);
    expect(res.handled).toBe(true);
  });

  test('no picker_days persisted yet → accept signal (first-turn: strip just emitted, not yet in state)', async () => {
    const invokeProposal = jest.fn().mockResolvedValue({ ...PROPOSE_OK });
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying' /* no picker_days */ }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState: jest.fn(),
        schedulingDaySelected: '2026-06-20',
      },
    }));
    expect(invokeProposal).toHaveBeenCalledTimes(1); // accepted (no strip to validate against)
    expect(res.handled).toBe(true);
  });
});

// ── audit fix #6: NaN guards for picker_cycles + proposing_none_count ────────────────

describe('runNewBookingTurn — NaN guards for picker_cycles and proposing_none_count (fix #6)', () => {
  test('picker_cycles = NaN in prior state → treated as 0, does not propagate NaN', async () => {
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({ outcome: 'no_availability', slots: [] });
    const res = await runNewBookingTurn(baseTurn({
      write: jest.fn(),
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'qualifying', picker_cycles: NaN }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
      },
    }));
    // NaN → treated as 0, incremented to 1 (under MAX_PICKER_CYCLES=3 → picker emitted)
    expect(res.reason).toMatch(/day_picker/);
    const saved = saveState.mock.calls[0] && saveState.mock.calls[0][0];
    expect(Number.isFinite(saved && saved.picker_cycles)).toBe(true); // no NaN persisted
  });

  test('proposing_none_count = NaN in prior state → treated as 0, does not propagate NaN', async () => {
    const saveState = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({ ...PROPOSE_OK });
    // In 'proposing' with noneCount NaN → should be treated as 0 (< 2 threshold → re-propose)
    const res = await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT], proposing_none_count: NaN }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
      },
    }));
    expect(invokeProposal).toHaveBeenCalledTimes(1); // re-proposed (not escaped to picker)
    expect(res.handled).toBe(true);
  });
});

// ── audit fix #7: shape-validate picker_days before re-emit ──────────────────────────
//
// Shape validation applies in _handleDaySelected (the no_availability re-emit path).
// Test via _handleDaySelected directly (bypassing the offered-strip gate in runNewBookingTurn)
// to isolate the re-emit shape-validation from the offered-day gate (fix #5).

describe('_handleDaySelected (no_availability path) — shape-validates picker_days before re-emitting (fix #7)', () => {
  const { _handleDaySelected } = require('../newBookingFlow');

  const OFFERED_DAYS = [
    { date: '2026-06-20', label: 'Sat, Jun 20' },
    { date: '2026-06-21', label: 'Sun, Jun 21' },
  ];

  test('corrupt picker_days (wrong shape) → rebuild fresh strip instead of emitting junk', async () => {
    const saveState = jest.fn();
    const writes = [];
    // Corrupt shape: objects have no 'date' field → isValidDayStrip returns false → rebuild
    const corruptDays = [{ day: 'Mon', lbl: 'bad' }, { day: 'Tue', lbl: 'also-bad' }];

    await _handleDaySelected({
      tenantId: 'TEN', sessionId: 'sess-1', state: 'qualifying',
      prior: { picker_cycles: 0, picker_days: corruptDays },
      qctx: QCTX,
      daySelected: '2026-06-20',
      deps: {
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [] }),
        saveState,
      },
      write: (msg) => writes.push(msg),
      logger: { warn: jest.fn(), info: jest.fn(), error: jest.fn() },
    });

    // A picker must still be emitted (rebuilt fresh)
    expect(writes.some((w) => w.includes('scheduling_day_picker'))).toBe(true);

    // The saved picker_days must be a valid fresh strip (not the corrupt one)
    const savedArg = saveState.mock.calls[0] && saveState.mock.calls[0][0];
    if (savedArg && savedArg.picker_days) {
      for (const d of savedArg.picker_days) {
        expect(typeof d.date).toBe('string');
        expect(d.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
        expect(typeof d.label).toBe('string');
      }
      // Specifically: the corrupt days must NOT have been persisted
      expect(savedArg.picker_days).not.toEqual(corruptDays);
    }
  });

  test('valid picker_days → re-emitted unchanged (same strip) on no_availability', async () => {
    const saveState = jest.fn();
    const writes = [];

    await _handleDaySelected({
      tenantId: 'TEN', sessionId: 'sess-1', state: 'qualifying',
      prior: { picker_cycles: 0, picker_days: OFFERED_DAYS },
      qctx: QCTX,
      daySelected: '2026-06-20',
      deps: {
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [] }),
        saveState,
      },
      write: (msg) => writes.push(msg),
      logger: { warn: jest.fn(), info: jest.fn(), error: jest.fn() },
    });

    // The emitted picker should contain the valid persisted days unchanged
    const emitted = writes.find((w) => w.includes('scheduling_day_picker'));
    expect(emitted).toBeDefined();
    const parsed = JSON.parse(emitted.replace(/^data: /, '').trim());
    expect(parsed.days).toEqual(OFFERED_DAYS);
  });

  test('null picker_days → rebuild fresh strip on no_availability', async () => {
    const saveState = jest.fn();
    const writes = [];

    await _handleDaySelected({
      tenantId: 'TEN', sessionId: 'sess-1', state: 'qualifying',
      prior: { picker_cycles: 0, picker_days: null },
      qctx: QCTX,
      daySelected: '2026-06-20',
      deps: {
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [] }),
        saveState,
      },
      write: (msg) => writes.push(msg),
      logger: { warn: jest.fn(), info: jest.fn(), error: jest.fn() },
    });

    // A picker must be emitted (rebuilt fresh from scratch)
    expect(writes.some((w) => w.includes('scheduling_day_picker'))).toBe(true);
  });
});

// ── audit fix #10: pin tests ──────────────────────────────────────────────────────────

describe('runNewBookingTurn — pin tests (fix #10)', () => {
  const confirmingState = { state: 'confirming', selected_slot: SLOT, proposal: { poolSize: 2 } };

  // (a) scheduling_day_selected arriving in 'confirming' state → silent no-op
  test('scheduling_day_selected in confirming state → silent no-op (no SSE, no state write)', async () => {
    const invokeProposal = jest.fn();
    const saveState = jest.fn();
    const write = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({ ...confirmingState, picker_days: [{ date: '2026-06-20', label: 'Sat, Jun 20' }] }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
        schedulingDaySelected: '2026-06-20', // valid format + offered, but state is 'confirming'
      },
    }));
    // _handleDaySelected returns { handled:false } when state !== qualifying/proposing
    expect(invokeProposal).not.toHaveBeenCalled();
    expect(saveState).not.toHaveBeenCalled();
    expect(write).not.toHaveBeenCalled();
    // Either handled:false (day_selected_wrong_state path) or normal chat (no SSE commit)
    expect(res.executed).toBeFalsy();
  });

  // (b) proposing_none_count reset after successful day-selection→ok advance is INTENDED
  // The reset happens because on a successful ok from _handleDaySelected, saveState is
  // called with the proposing state but WITHOUT proposing_none_count (not carried forward
  // in the ok path) — this is intentional: the user chose a day, slots were found, so
  // the none-count should be reset for the new context.
  test('proposing_none_count is NOT carried forward on successful day-selection→ok advance (intentional reset)', async () => {
    const saveState = jest.fn();
    const OFFERED_DAYS = [{ date: '2026-06-20', label: 'Sat, Jun 20' }];
    const invokeProposal = jest.fn().mockResolvedValue({ ...PROPOSE_OK });
    await runNewBookingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        loadState: async () => ({
          state: 'qualifying',
          picker_days: OFFERED_DAYS,
          // Some prior proposing_none_count from a previous turn
          proposing_none_count: 1,
        }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState,
        schedulingDaySelected: '2026-06-20',
      },
    }));
    expect(saveState).toHaveBeenCalledTimes(1);
    const savedArg = saveState.mock.calls[0][0];
    // proposing_none_count is intentionally absent from the ok-advance saveState
    // (only picker_cycles is carried forward in the ok path, per the implementation).
    // This reset is correct: after the user picks a day and slots are found, the none-
    // loop for the new context starts fresh.
    expect(savedArg.proposing_none_count).toBeUndefined(); // intentional reset
    expect(savedArg.state).toBe('proposing'); // did advance
  });
});

// ── §B16b amendment: deterministic widget actions (slot-chip / confirm clicks) ──────────
// The widget's scheduling_action (+ scheduling_slot_id) signal is consumed BEFORE the LLM
// detector — the detector model must never be invoked on a click turn, and every §B14
// guard (transition + persisted-candidate validation) still applies to the signal.

describe('runNewBookingTurn — deterministic widget actions (§B16b amendment)', () => {
  test('select_slot click: detector NOT called; proposing → confirming; scheduling_confirm emitted when identity known', async () => {
    const saveState = jest.fn();
    const bedrock = { send: jest.fn() };
    const writes = [];
    const res = await runNewBookingTurn(baseTurn({
      bedrock,
      write: (s) => writes.push(s),
      deps: {
        schedulingAction: 'select_slot',
        schedulingSlotId: 's1',
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT], proposal: { poolSize: 2 } }),
        qualifyingContext: QCTX,
        saveState,
      },
    }));
    expect(bedrock.send).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false, action: 'select_slot', state: 'confirming', identity: true });
    const confirmEvt = writes.find((w) => w.includes('"scheduling_confirm"'));
    expect(confirmEvt).toBeDefined();
    const parsed = JSON.parse(confirmEvt.replace(/^data: /, '').trim());
    expect(parsed).toMatchObject({
      type: 'scheduling_confirm',
      session_id: 'sess-1',
      slot: { slotId: 's1', label: SLOT.label },
      attendee_email: 'vol@example.com',
    });
  });

  test('select_slot click without identity: confirming + identity:false, NO scheduling_confirm yet', async () => {
    const writes = [];
    const res = await runNewBookingTurn(baseTurn({
      bedrock: { send: jest.fn() },
      write: (s) => writes.push(s),
      deps: {
        schedulingAction: 'select_slot',
        schedulingSlotId: 's1',
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT] }),
        qualifyingContext: QCTX_NO_EMAIL,
        saveState: jest.fn(),
      },
    }));
    expect(res).toMatchObject({ handled: true, state: 'confirming', identity: false });
    expect(writes.find((w) => w.includes('"scheduling_confirm"'))).toBeUndefined();
  });

  test('spoofed select_slot signal with unknown slotId → rejected unknown_slot (persisted-state guard holds)', async () => {
    const saveState = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: { send: jest.fn() },
      deps: {
        schedulingAction: 'select_slot',
        schedulingSlotId: 'not-a-candidate',
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT] }),
        qualifyingContext: QCTX,
        saveState,
      },
    }));
    expect(res).toMatchObject({ handled: true, rejected: true, reason: 'unknown_slot' });
    expect(saveState).not.toHaveBeenCalled();
  });

  test('confirm_book click from confirming with identity: commits deterministically (no detector)', async () => {
    const bedrock = { send: jest.fn() };
    const invokeBookingCommit = jest.fn().mockResolvedValue({
      status: 'BOOKED', bookingId: 'bk-1', resourceId: 'maya@org.example',
    });
    const saveState = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock,
      write: jest.fn(),
      deps: {
        schedulingAction: 'confirm_book',
        loadState: async () => ({ state: 'confirming', selected_slot: SLOT, proposal: { poolSize: 2 } }),
        qualifyingContext: QCTX,
        saveState,
        invokeBookingCommit,
      },
    }));
    expect(bedrock.send).not.toHaveBeenCalled();
    expect(invokeBookingCommit).toHaveBeenCalledTimes(1);
    expect(res).toMatchObject({ handled: true, action: 'confirm_book', executed: true, state: 'booked' });
  });

  test('confirm_book click without identity → identity_required, NO commit', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: { send: jest.fn() },
      deps: {
        schedulingAction: 'confirm_book',
        loadState: async () => ({ state: 'confirming', selected_slot: SLOT }),
        qualifyingContext: QCTX_NO_EMAIL,
        saveState: jest.fn(),
        invokeBookingCommit,
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true, reason: 'identity_required' });
    expect(invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('spoofed confirm_book click from proposing → IllegalStateTransition, NO commit', async () => {
    const invokeBookingCommit = jest.fn();
    const res = await runNewBookingTurn(baseTurn({
      bedrock: { send: jest.fn() },
      deps: {
        schedulingAction: 'confirm_book',
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT] }),
        qualifyingContext: QCTX,
        invokeBookingCommit,
      },
    }));
    expect(res).toMatchObject({ handled: true, rejected: true });
    expect(res.reason).toMatch(/Illegal/i);
    expect(invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('select_slot carries a prior chat-captured attendee_email forward on saveState', async () => {
    const saveState = jest.fn();
    await runNewBookingTurn(baseTurn({
      bedrock: { send: jest.fn() },
      deps: {
        schedulingAction: 'select_slot',
        schedulingSlotId: 's1',
        loadState: async () => ({ state: 'proposing', candidate_slots: [SLOT], attendee_email: 'kept@example.com' }),
        qualifyingContext: QCTX_NO_EMAIL,
        saveState,
      },
    }));
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ attendee_email: 'kept@example.com' }));
  });
});

// ─── §B18b context forwarding: _propose old-shape fixture tests ──────────────────────

describe('_propose — §B18b context forwarding (old-shape fixture)', () => {
  test('_propose result WITH context → scheduling_slots SSE carries context', async () => {
    const write = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({
      outcome: 'ok',
      slots: [SLOT],
      poolSize: 1,
      context: { duration_minutes: 30, conference_type: 'google_meet', conference_label: 'Google Meet', tz_label: 'Central Time' },
    });
    await runNewBookingTurn(baseTurn({
      write,
      deps: {
        loadState: async () => ({ state: 'qualifying' }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState: jest.fn(),
      },
    }));
    const slotFrames = write.mock.calls
      .map(([s]) => s)
      .filter((s) => typeof s === 'string' && s.includes('scheduling_slots'))
      .map((s) => JSON.parse(s.slice('data: '.length)));
    expect(slotFrames.length).toBeGreaterThan(0);
    expect(slotFrames[0].context).toEqual({ duration_minutes: 30, conference_type: 'google_meet', conference_label: 'Google Meet', tz_label: 'Central Time' });
  });

  test('_propose result WITHOUT context (old shape) → no context key on SSE, no crash', async () => {
    const write = jest.fn();
    const invokeProposal = jest.fn().mockResolvedValue({
      outcome: 'ok',
      slots: [SLOT],
      poolSize: 1,
      // context deliberately absent — old shape
    });
    await runNewBookingTurn(baseTurn({
      write,
      deps: {
        loadState: async () => ({ state: 'qualifying' }),
        qualifyingContext: QCTX,
        invokeProposal,
        saveState: jest.fn(),
      },
    }));
    const slotFrames = write.mock.calls
      .map(([s]) => s)
      .filter((s) => typeof s === 'string' && s.includes('scheduling_slots'))
      .map((s) => JSON.parse(s.slice('data: '.length)));
    expect(slotFrames.length).toBeGreaterThan(0);
    expect(slotFrames[0]).not.toHaveProperty('context'); // old-shape: absent, not null
  });
});
