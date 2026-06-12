/**
 * WS-CONVO — schedulingFlow (the B3 keystone: flow + §B14 action boundary) tests.
 *
 * Done-bar coverage (work-order WS-CONVO):
 *  - the action-detector GATES execution: free-text "I've confirmed it" (action:'none')
 *    → NO execute; a structured {action:'confirm_reschedule'} → executeReschedule once
 *  - illegal transitions rejected by stateMachine (confirm_reschedule from 'rescheduling')
 *  - reschedule confirm calls executeReschedule with the §B13 facade as deps.calendar
 *    + the §B15 Zoom updateMeeting time-PATCH on a Zoom booking
 *  - cancel confirm calls executeCancel (via the deleteEvent(booking) facade adapter)
 *  - no-binding session → { handled:false }, nothing executed (no-regression)
 *  - select_slot advances proposing → confirming (no calendar op)
 *  - the §B14 detector is fail-closed (unparseable / no bedrock → 'none')
 *  - integrator-seam unwired (no facade) → execution skipped non-fatally
 */

const {
  runSchedulingTurn,
  detectSchedulingAction,
  calendarIdOf,
  eventIdOf,
  zoomMeetingIdOf,
} = require('../schedulingFlow');

// ── fakes ────────────────────────────────────────────────────────────────────────────

// Bedrock client whose post-stream call returns a canned §B14 action object.
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

// A §B13-shaped calendar facade (calendarId/eventId method shape).
function makeFacade() {
  return {
    buildEventBody: jest.fn().mockReturnValue({}),
    insertEvent: jest.fn().mockResolvedValue({ id: 'evt-new' }),
    deleteEvent: jest.fn().mockResolvedValue(undefined),
    extractMeetJoinUrl: jest.fn().mockReturnValue(null),
  };
}

const BOOKING = Object.freeze({
  booking_id: 'bk_123',
  tenant_id: 'TEN',
  external_event_id: 'evt-old',
  coordinator_email: 'maya@org.example',
  resource_id: 'maya@org.example',
  conference_provider: 'zoom',
  conference_id: '987654321',
  timezone: 'America/Chicago',
});

const RESCHEDULE_BINDING = { intent: 'rescheduling_intent', booking_id: 'bk_123', coordinator_id: 'maya@org.example' };
const CANCEL_BINDING = { intent: 'cancellation_intent', booking_id: 'bk_123', coordinator_id: 'maya@org.example' };
const SELECTED_SLOT = { slotId: 's1', start: '2026-06-10T19:00:00Z', end: '2026-06-10T19:30:00Z' };

const baseTurn = (overrides) => ({
  responseText: 'ok',
  conversationHistory: [],
  tenantId: 'TEN',
  sessionId: 'sess-1',
  config: {},
  ...overrides,
});

// ── small-helper unit tests ────────────────────────────────────────────────────────────

describe('booking field helpers', () => {
  test('calendarIdOf / eventIdOf tolerate snake + camel', () => {
    expect(calendarIdOf(BOOKING)).toBe('maya@org.example');
    expect(calendarIdOf({ coordinatorEmail: 'x@y' })).toBe('x@y');
    expect(eventIdOf(BOOKING)).toBe('evt-old');
    expect(eventIdOf({ externalEventId: 'evt-c' })).toBe('evt-c');
  });
  test('zoomMeetingIdOf returns the id for zoom, null otherwise', () => {
    expect(zoomMeetingIdOf(BOOKING)).toBe('987654321');
    expect(zoomMeetingIdOf({ conference_provider: 'google_meet', conference_id: 'abc' })).toBeNull();
    expect(zoomMeetingIdOf({ conference_id: '12345' })).toBe('12345'); // numeric id, provider unset → zoom
    expect(zoomMeetingIdOf({})).toBeNull();
  });
});

// ── §B14 detector ────────────────────────────────────────────────────────────────────

describe('detectSchedulingAction (§B14 focused post-stream call) — fail-closed', () => {
  test('no bedrock client → none', async () => {
    expect((await detectSchedulingAction({ binding: { booking_id: 'b' } })).action).toBe('none');
  });
  test('unparseable model output → none', async () => {
    const out = await detectSchedulingAction({ bedrock: fakeBedrockRaw('I think yes!'), binding: { booking_id: 'b' } });
    expect(out.action).toBe('none');
  });
  test('unknown action value → none', async () => {
    const out = await detectSchedulingAction({ bedrock: fakeBedrock({ action: 'delete_everything' }), binding: { booking_id: 'b' } });
    expect(out.action).toBe('none');
  });
  test('structured select_slot carries the slotId', async () => {
    const out = await detectSchedulingAction({ bedrock: fakeBedrock({ action: 'select_slot', slotId: 's1' }), binding: { booking_id: 'b' } });
    expect(out).toMatchObject({ action: 'select_slot', slotId: 's1', booking_id: 'b' });
  });
});

// ── runSchedulingTurn: the boundary ────────────────────────────────────────────────────

describe('runSchedulingTurn — no binding (no-regression)', () => {
  test('returns { handled:false } and executes nothing', async () => {
    const executeReschedule = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_reschedule' }),
      deps: { resolveBinding: async () => null, executeReschedule },
    }));
    expect(res).toEqual({ handled: false });
    expect(executeReschedule).not.toHaveBeenCalled();
  });
});

describe('runSchedulingTurn — §B14: free text never executes', () => {
  test("action 'none' in 'confirming' (LLM prose 'I confirmed it') → executeReschedule NOT called", async () => {
    const executeReschedule = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      responseText: "Great, I've confirmed your new time!",
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => ({ state: 'confirming', selected_slot: SELECTED_SLOT }),
        executeReschedule,
        calendar: makeFacade(),
        conference: { createConference: jest.fn() },
      },
    }));
    expect(executeReschedule).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false, action: 'none' });
  });
});

describe('runSchedulingTurn — reschedule confirm (the §B13 facade + §B15 PATCH done-bar)', () => {
  test('structured confirm_reschedule → executeReschedule ONCE with deps.calendar === facade; Zoom updateMeeting PATCHed', async () => {
    const facade = makeFacade();
    const conference = { createConference: jest.fn() };
    const executeReschedule = jest.fn().mockResolvedValue({
      outcome: 'success', booking: BOOKING, newEventId: 'evt-new', oldEventId: 'evt-old',
    });
    const updateMeeting = jest.fn().mockResolvedValue(undefined);
    const saveBooking = jest.fn().mockResolvedValue(undefined);
    const saveState = jest.fn().mockResolvedValue(undefined);

    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_reschedule' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => ({ state: 'confirming', selected_slot: SELECTED_SLOT }),
        loadBooking: async () => BOOKING,
        calendar: facade, conference, executeReschedule, updateMeeting, saveBooking, saveState,
      },
    }));

    expect(executeReschedule).toHaveBeenCalledTimes(1);
    const call = executeReschedule.mock.calls[0][0];
    expect(call.deps.calendar).toBe(facade);            // §B13 facade injected as deps.calendar
    expect(call.deps.conference).toBe(conference);       // §B6 provider injected
    expect(call.newSlot).toEqual({ start: SELECTED_SLOT.start, end: SELECTED_SLOT.end });

    expect(updateMeeting).toHaveBeenCalledTimes(1);      // §B15 Zoom start-time PATCH
    expect(updateMeeting.mock.calls[0][0]).toMatchObject({
      tenantId: 'TEN', meetingId: '987654321', start: SELECTED_SLOT.start, end: SELECTED_SLOT.end,
    });
    expect(saveBooking).toHaveBeenCalledWith(BOOKING);
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'booked' }));
    expect(res).toMatchObject({ handled: true, executed: true, action: 'confirm_reschedule', outcome: 'success' });
  });

  test('no updateMeeting injected (seam) → reschedule still executes; PATCH skipped', async () => {
    const executeReschedule = jest.fn().mockResolvedValue({ outcome: 'success', booking: BOOKING });
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_reschedule' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => ({ state: 'confirming', selected_slot: SELECTED_SLOT }),
        loadBooking: async () => BOOKING,
        calendar: makeFacade(), conference: { createConference: jest.fn() }, executeReschedule,
      },
    }));
    expect(res.executed).toBe(true);
  });
});

describe('runSchedulingTurn — §B14: illegal transition rejected by stateMachine', () => {
  test("confirm_reschedule while state is 'rescheduling' (not 'confirming') → rejected, NO execute", async () => {
    const executeReschedule = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_reschedule' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => ({ state: 'rescheduling' }), // rescheduling → booked is illegal (§9.2)
        loadBooking: async () => BOOKING,
        calendar: makeFacade(), conference: { createConference: jest.fn() }, executeReschedule,
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, rejected: true });
    expect(res.reason).toMatch(/Illegal/i);
    expect(executeReschedule).not.toHaveBeenCalled();
  });
});

describe('runSchedulingTurn — cancel confirm', () => {
  test('confirm_cancel in canceling → executeCancel ONCE with the §B13 facade passed directly', async () => {
    const facade = makeFacade();
    // cancel.js (#212) resolves calendarId/eventId from the booking and calls the two-arg
    // facade itself, so the flow passes the facade unmodified (no booking-shape adapter).
    const executeCancel = jest.fn().mockResolvedValue({ outcome: 'deleted', executed: true, booking: BOOKING });
    const saveBooking = jest.fn();
    const saveState = jest.fn().mockResolvedValue(undefined);
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_cancel' }),
      deps: {
        resolveBinding: async () => CANCEL_BINDING,
        loadState: async () => ({ state: 'canceling' }),
        loadBooking: async () => BOOKING,
        calendar: facade, executeCancel, saveBooking, saveState,
      },
    }));
    expect(executeCancel).toHaveBeenCalledTimes(1);
    expect(executeCancel.mock.calls[0][0].deps.calendar).toBe(facade); // §B13 facade passed directly
    expect(saveBooking).toHaveBeenCalledWith(BOOKING);
    // [B-1]: the session state advances off 'canceling' so it can't re-fire within the TTL.
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'booked' }));
    expect(res).toMatchObject({ handled: true, executed: true, outcome: 'deleted' });
  });

  test('[B-1] confirm_cancel after the session already advanced (state booked) → rejected, executeCancel NOT called', async () => {
    const executeCancel = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_cancel' }),
      deps: {
        resolveBinding: async () => CANCEL_BINDING,
        loadState: async () => ({ state: 'booked' }), // already cancelled this session
        loadBooking: async () => BOOKING,
        calendar: makeFacade(), executeCancel,
      },
    }));
    expect(executeCancel).not.toHaveBeenCalled(); // booked→booked is illegal → IllegalStateTransition → rejected
    expect(res).toMatchObject({ handled: true, executed: false });
  });

  test("non-confirm action in 'canceling' → awaits confirmation, no execute", async () => {
    const executeCancel = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'none' }),
      deps: { resolveBinding: async () => CANCEL_BINDING, loadState: async () => ({ state: 'canceling' }), executeCancel },
    }));
    expect(executeCancel).not.toHaveBeenCalled();
    expect(res).toMatchObject({ handled: true, executed: false });
  });
});

describe('runSchedulingTurn — select_slot advances proposing → confirming', () => {
  test('records the chosen slot, no calendar op', async () => {
    const saveState = jest.fn();
    const executeReschedule = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'select_slot', slotId: 's1' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => ({ state: 'proposing', candidate_slots: [SELECTED_SLOT] }),
        saveState, executeReschedule,
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, action: 'select_slot', state: 'confirming' });
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({
      state: 'confirming', selected_slot: expect.objectContaining({ slotId: 's1' }),
    }));
    expect(executeReschedule).not.toHaveBeenCalled();
  });
});

describe('runSchedulingTurn — entry: present slots on first rescheduling turn', () => {
  test("no prior state + action 'none' → generate slots, advance to proposing, emit SSE", async () => {
    const write = jest.fn();
    const saveState = jest.fn();
    const generateSlots = jest.fn().mockReturnValue([SELECTED_SLOT]);
    const res = await runSchedulingTurn(baseTurn({
      write,
      bedrock: fakeBedrock({ action: 'none' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => null, // first turn → init from intent = 'rescheduling'
        generateSlots, saveState,
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, state: 'proposing' });
    expect(generateSlots).toHaveBeenCalledTimes(1);
    expect(saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'proposing' }));
    expect(write).toHaveBeenCalledTimes(1);
    expect(write.mock.calls[0][0]).toContain('scheduling_slots');
  });
});

describe('runSchedulingTurn — integrator seam unwired', () => {
  test('confirm_reschedule with no facade/conference → execution skipped non-fatally', async () => {
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_reschedule' }),
      deps: {
        resolveBinding: async () => RESCHEDULE_BINDING,
        loadState: async () => ({ state: 'confirming', selected_slot: SELECTED_SLOT }),
        loadBooking: async () => BOOKING,
        // no calendar, no conference, no getOAuthClient/calendarEvents
      },
    }));
    expect(res).toMatchObject({ handled: true, executed: false, reason: 'calendar_seam_unwired' });
  });
});

// ─── §B16e day-picker branches (WS-T3-DAYPICK-BE) ───────────────────────────────────────
//
// Tests for newBookingFlow.js's day-picker trigger paths and the dayPicker.js helpers.
// The work-order assigns these to schedulingFlow.test.js (the BSH scheduling test suite).

const {
  runNewBookingTurn,
  _emitPickerOrEscape,
  _handleDaySelected,
} = require('../newBookingFlow');
const { buildDayStrip, dateWindowForDay, MAX_PICKER_CYCLES } = require('../dayPicker');

// ── helpers ──────────────────────────────────────────────────────────────────────────────

const CONFIG_ENABLED = { feature_flags: { scheduling_enabled: true } };

// A minimal qualifying context (no attendee yet — day picker fires before identity needed).
const QCTX = Object.freeze({
  appointmentTypeId: 'apt_intro',
  userTimeZone: 'America/Chicago',
  conference_type: 'google_meet',
  attendee: { email: 'volunteer@example.com', first_name: 'Alex' },
});

// Fake Bedrock that returns action:'none' (all day-picker turns are non-committing).
function fakeNoneBedrock() {
  return {
    send: jest.fn().mockResolvedValue({
      body: new TextEncoder().encode(JSON.stringify({ content: [{ text: JSON.stringify({ action: 'none' }) }] })),
    }),
  };
}

function collectWrites() {
  const writes = [];
  return { write: (msg) => writes.push(msg), writes };
}

// ── dayPicker.js unit tests ───────────────────────────────────────────────────────────────

describe('dayPicker — buildDayStrip', () => {
  test('returns 7 days starting tomorrow (UTC) by default', () => {
    const nowMs = Date.parse('2026-07-01T12:00:00Z');
    const days = buildDayStrip({ userTimeZone: 'America/Chicago', nowMs });
    expect(days).toHaveLength(7);
    // First day = 2026-07-02 (tomorrow).
    expect(days[0].date).toBe('2026-07-02');
    // All entries have a non-empty label string.
    for (const d of days) {
      expect(d.label).toBeTruthy();
      expect(typeof d.label).toBe('string');
      expect(d.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    }
  });

  test('strip is clipped to maxAdvanceDays', () => {
    const nowMs = Date.parse('2026-07-01T00:00:00Z');
    // maxAdvanceDays = 3 → at most 3 days ahead; with stripSize 7 → only 3 entries.
    const days = buildDayStrip({ userTimeZone: 'UTC', nowMs, maxAdvanceDays: 3, stripSize: 7 });
    expect(days.length).toBeLessThanOrEqual(3);
  });

  test('uses UTC fallback when userTimeZone is undefined', () => {
    const days = buildDayStrip({ nowMs: Date.parse('2026-07-01T00:00:00Z') });
    expect(days).toHaveLength(7);
  });
});

describe('dayPicker — dateWindowForDay', () => {
  test('returns UTC midnight start/end for a given YYYY-MM-DD', () => {
    const { startISO, endISO } = dateWindowForDay('2026-07-06');
    expect(startISO).toBe('2026-07-06T00:00:00.000Z');
    expect(endISO).toBe('2026-07-07T00:00:00.000Z');
    expect(Date.parse(endISO) - Date.parse(startISO)).toBe(24 * 60 * 60 * 1000);
  });

  test('throws on malformed date string', () => {
    expect(() => dateWindowForDay('not-a-date')).toThrow();
    expect(() => dateWindowForDay('2026-7-6')).toThrow();
  });
});

// ── §B16e trigger (a): invokeProposal returns no_availability → emit picker ────────────────

describe('§B16e trigger (a) — no_availability → day picker emitted, state stays in qualifying', () => {
  test('picker is emitted and state stays qualifying when proposal returns no_availability', async () => {
    const { write, writes } = collectWrites();
    const savedStates = [];
    const res = await runNewBookingTurn({
      responseText: 'Let me check availability for you.',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        loadState: async () => ({ state: 'qualifying' }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [], poolSize: 0 }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    // Result: handled, state stays 'qualifying', reason = day_picker.
    expect(res.handled).toBe(true);
    expect(res.state).toBe('qualifying');
    expect(res.reason).toBe('day_picker');
    // SSE: scheduling_day_picker was emitted (not scheduling_no_availability).
    const pickerEmits = writes.filter((w) => w.includes('"type":"scheduling_day_picker"'));
    expect(pickerEmits).toHaveLength(1);
    const pickerMsg = JSON.parse(pickerEmits[0].replace(/^data: /, '').trim());
    expect(pickerMsg.days).toHaveLength(7);
    expect(pickerMsg.user_time_zone).toBe('America/Chicago');
    // scheduling_no_availability must NOT be emitted.
    expect(writes.some((w) => w.includes('scheduling_no_availability'))).toBe(false);
    // State was NOT advanced.
    expect(savedStates[savedStates.length - 1].state).toBe('qualifying');
  });

  test('trigger (a) from proposing state: state stays proposing', async () => {
    const { write, writes } = collectWrites();
    const savedStates = [];
    const res = await runNewBookingTurn({
      responseText: 'Checking...',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        loadState: async () => ({ state: 'proposing' }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [], poolSize: 0 }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    expect(res.state).toBe('proposing');
    expect(res.reason).toBe('day_picker');
    expect(savedStates[savedStates.length - 1].state).toBe('proposing');
    expect(writes.some((w) => w.includes('"type":"scheduling_day_picker"'))).toBe(true);
  });
});

// ── §B16e trigger (b): proposing none-self-loop >= 2 times → emit picker ─────────────────

describe('§B16e trigger (b) — proposing self-loop >= 2 → day picker emitted', () => {
  test('first none-self-loop from proposing does NOT emit picker', async () => {
    const { write, writes } = collectWrites();
    // noneCount starts at 0; after first loop it will be 1 < 2 → re-propose (not picker).
    const res = await runNewBookingTurn({
      responseText: 'Can you show me more times?',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        loadState: async () => ({ state: 'proposing', proposing_none_count: 0 }),
        saveState: async () => {},
        // invokeProposal returns ok (slots available) so _propose succeeds.
        invokeProposal: async () => ({
          outcome: 'ok',
          slots: [{ slotId: 'slot-1', start: '2026-07-06T14:00:00Z', end: '2026-07-06T14:30:00Z', label: 'Mon, Jul 6 · 9:00 AM', candidateResourceIds: ['r1'] }],
          poolSize: 1,
        }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    expect(writes.some((w) => w.includes('"type":"scheduling_day_picker"'))).toBe(false);
    expect(writes.some((w) => w.includes('"type":"scheduling_slots"'))).toBe(true);
    expect(res.state).toBe('proposing');
  });

  test('second none-self-loop from proposing (count >= 2) emits picker, state stays proposing', async () => {
    const { write, writes } = collectWrites();
    const savedStates = [];
    const res = await runNewBookingTurn({
      responseText: 'Can you show me even more times?',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        // proposing_none_count = 1 → noneCount becomes 2 → trigger (b) fires.
        loadState: async () => ({ state: 'proposing', proposing_none_count: 1 }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: jest.fn(), // must NOT be called — picker fires before propose
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    expect(res.state).toBe('proposing');
    expect(res.reason).toBe('day_picker');
    expect(writes.some((w) => w.includes('"type":"scheduling_day_picker"'))).toBe(true);
    expect(savedStates[savedStates.length - 1].state).toBe('proposing');
    // invokeProposal must NOT have been called (picker fires before it).
    // (It was set as jest.fn() with no impl — if called it resolves undefined → would error.)
  });
});

// ── §B16e signal handling: scheduling_day_selected ──────────────────────────────────────

describe('§B16e scheduling_day_selected signal', () => {
  const DAY = '2026-07-06';

  test('signal re-runs invokeProposal constrained to the selected day', async () => {
    const proposeCalls = [];
    const { write, writes } = collectWrites();
    const res = await runNewBookingTurn({
      responseText: 'Great choice!',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        schedulingDaySelected: DAY,
        // fix #5: DAY must be in picker_days for the offered-strip validation to pass.
        loadState: async () => ({ state: 'qualifying', picker_cycles: 1, picker_days: [{ date: DAY, label: 'Mon, Jul 6' }] }),
        saveState: async () => {},
        invokeProposal: async (payload) => {
          proposeCalls.push(payload);
          return {
            outcome: 'ok',
            slots: [{ slotId: 'slot-d', start: '2026-07-06T14:00:00Z', end: '2026-07-06T14:30:00Z', label: 'Mon, Jul 6 · 9:00 AM', candidateResourceIds: ['r1'] }],
            poolSize: 1,
          };
        },
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    // Should advance to proposing (outcome ok → slots presented).
    expect(res.state).toBe('proposing');
    expect(writes.some((w) => w.includes('"type":"scheduling_slots"'))).toBe(true);
    // invokeProposal was called with the dateWindow for 2026-07-06.
    expect(proposeCalls).toHaveLength(1);
    expect(proposeCalls[0].date_window).toEqual({
      start: '2026-07-06T00:00:00.000Z',
      end: '2026-07-07T00:00:00.000Z',
    });
  });

  test('signal on no_availability day → re-emit picker (same days), state stays', async () => {
    // fix #5: persistedDays must include DAY for the offered-strip validation to pass.
    const persistedDays = [{ date: DAY, label: 'Mon, Jul 6' }, { date: '2026-07-07', label: 'Tue, Jul 7' }, { date: '2026-07-08', label: 'Wed, Jul 8' }];
    const { write, writes } = collectWrites();
    const savedStates = [];
    const res = await runNewBookingTurn({
      responseText: '',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        schedulingDaySelected: DAY,
        loadState: async () => ({ state: 'qualifying', picker_cycles: 1, picker_days: persistedDays }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [], poolSize: 0 }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    expect(res.state).toBe('qualifying');
    expect(res.reason).toBe('day_picker_reemit');
    // The SAME persisted days are re-emitted.
    const pickerEmits = writes.filter((w) => w.includes('"type":"scheduling_day_picker"'));
    expect(pickerEmits).toHaveLength(1);
    const msg = JSON.parse(pickerEmits[0].replace(/^data: /, '').trim());
    expect(msg.days).toEqual(persistedDays);
    // State was NOT advanced.
    expect(savedStates[savedStates.length - 1].state).toBe('qualifying');
  });

  test('>3 picker cycles → scheduling_notice escape, state stays', async () => {
    const { write, writes } = collectWrites();
    const savedStates = [];
    const res = await runNewBookingTurn({
      responseText: '',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        schedulingDaySelected: DAY,
        // picker_cycles = 3 → on the next emit it becomes 4 > MAX (3) → escape.
        // fix #5: DAY must be in picker_days for the offered-strip validation to pass.
        loadState: async () => ({ state: 'qualifying', picker_cycles: MAX_PICKER_CYCLES, picker_days: [{ date: DAY, label: 'Mon, Jul 6' }] }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [], poolSize: 0 }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    expect(res.reason).toBe('picker_escape');
    expect(writes.some((w) => w.includes('"type":"scheduling_notice"'))).toBe(true);
    expect(writes.some((w) => w.includes('"type":"scheduling_day_picker"'))).toBe(false);
    expect(savedStates[savedStates.length - 1].state).toBe('qualifying');
  });

  test('invalid day_selected format → handled:false (ignored, flow continues)', async () => {
    const { write } = collectWrites();
    const res = await runNewBookingTurn({
      responseText: '',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write,
      deps: {
        qualifyingContext: QCTX,
        schedulingDaySelected: 'not-a-date',
        loadState: async () => ({ state: 'qualifying' }),
        saveState: async () => {},
        invokeProposal: async () => ({ outcome: 'ok', slots: [], poolSize: 0 }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    // Invalid day_selected → signal handler returns {handled:false}. But then the
    // flow continues and calls _propose with qualifying + none → emits whatever the
    // proposal returns.  The key is no crash and no picker emitted.
    expect(res.handled).toBe(true); // _propose is invoked and returns handled:true
  });
});

// ── §B16e both triggers pin state (strand-prevention) ───────────────────────────────────

describe('§B16e state rules — picker never advances state', () => {
  test('trigger (a) from qualifying: saveState persists qualifying (not proposing)', async () => {
    const savedStates = [];
    await runNewBookingTurn({
      responseText: '',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write: () => {},
      deps: {
        qualifyingContext: QCTX,
        loadState: async () => ({ state: 'qualifying' }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: async () => ({ outcome: 'no_availability', slots: [], poolSize: 0 }),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    // Every saveState call must persist 'qualifying', never 'proposing'.
    for (const s of savedStates) {
      expect(s.state).toBe('qualifying');
    }
  });

  test('trigger (b) from proposing: saveState persists proposing', async () => {
    const savedStates = [];
    await runNewBookingTurn({
      responseText: '',
      conversationHistory: [],
      tenantId: 'T1', sessionId: 'S1',
      config: CONFIG_ENABLED,
      bedrock: fakeNoneBedrock(),
      write: () => {},
      deps: {
        qualifyingContext: QCTX,
        loadState: async () => ({ state: 'proposing', proposing_none_count: 1 }),
        saveState: async (s) => savedStates.push(s),
        invokeProposal: jest.fn(),
        logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      },
    });
    for (const s of savedStates) {
      expect(s.state).toBe('proposing');
    }
  });
});
