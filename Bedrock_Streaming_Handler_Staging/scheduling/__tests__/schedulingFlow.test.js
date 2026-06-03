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
    const executeCancel = jest.fn().mockResolvedValue({ outcome: 'deleted', booking: BOOKING });
    const saveBooking = jest.fn();
    const res = await runSchedulingTurn(baseTurn({
      bedrock: fakeBedrock({ action: 'confirm_cancel' }),
      deps: {
        resolveBinding: async () => CANCEL_BINDING,
        loadState: async () => ({ state: 'canceling' }),
        loadBooking: async () => BOOKING,
        calendar: facade, executeCancel, saveBooking,
      },
    }));
    expect(executeCancel).toHaveBeenCalledTimes(1);
    expect(executeCancel.mock.calls[0][0].deps.calendar).toBe(facade); // §B13 facade passed directly
    expect(saveBooking).toHaveBeenCalledWith(BOOKING);
    expect(res).toMatchObject({ handled: true, executed: true, outcome: 'deleted' });
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
