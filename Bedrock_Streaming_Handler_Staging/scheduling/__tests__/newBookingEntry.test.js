'use strict';

/**
 * newBookingEntry.js — the integrator entry-hook glue (§B16d). Tests the ENGAGE / BOOTSTRAP /
 * NO-OP decision + qualifyingContext resolution. runNewBookingTurn is mocked so these assert
 * the entry logic in isolation (whether the flow is driven, and with what deps).
 */

jest.mock('../newBookingFlow', () => ({
  runNewBookingTurn: jest.fn().mockResolvedValue({ handled: true, state: 'proposing', action: 'none' }),
}));

const { runNewBookingTurn } = require('../newBookingFlow');
const { runNewBookingEntry, resolveQualifyingContext, IN_FLIGHT_STATES } = require('../newBookingEntry');

beforeEach(() => jest.clearAllMocks());

describe('resolveQualifyingContext', () => {
  test('sole configured appt-type → uses it + tz from the type + google_meet default', () => {
    const config = { scheduling: { appointment_types: { apt1: { timezone: 'America/Chicago' } } } };
    const q = resolveQualifyingContext({ config, routingMetadata: {} });
    expect(q.appointmentTypeId).toBe('apt1');
    expect(q.userTimeZone).toBe('America/Chicago');
    expect(q.conference_type).toBe('google_meet');
    expect(q.appointment_type).toEqual({ timezone: 'America/Chicago' });
    expect(q.attendee).toBeUndefined();
  });

  test('multiple types + CTA-requested id → uses the requested type + its conference', () => {
    const config = { scheduling: { appointment_types: { a: {}, b: { conference_type: 'null' } } } };
    const q = resolveQualifyingContext({ config, routingMetadata: { appointment_type_id: 'b' } });
    expect(q.appointmentTypeId).toBe('b');
    expect(q.conference_type).toBe('null');
  });

  test('multiple types, none requested → appointmentTypeId null (qualifying would ask)', () => {
    const config = { scheduling: { appointment_types: { a: {}, b: {} } } };
    expect(resolveQualifyingContext({ config, routingMetadata: {} }).appointmentTypeId).toBeNull();
  });

  test('absent / empty config → safe defaults (UTC, google_meet, no type)', () => {
    expect(resolveQualifyingContext({ config: {}, routingMetadata: {} })).toMatchObject({
      appointmentTypeId: null, userTimeZone: 'UTC', conference_type: 'google_meet',
    });
    expect(resolveQualifyingContext({})).toMatchObject({ userTimeZone: 'UTC' });
  });

  test('userTimeZone falls back to routingMetadata then UTC', () => {
    const config = { scheduling: { appointment_types: { a: {} } } };
    expect(resolveQualifyingContext({ config, routingMetadata: { user_time_zone: 'Europe/London' } }).userTimeZone).toBe('Europe/London');
  });

  test('attendee included ONLY when it carries an email (v1 holds otherwise)', () => {
    const config = { scheduling: { appointment_types: { a: {} } } };
    expect(resolveQualifyingContext({ config, attendee: { email: 'v@x.org' } }).attendee).toEqual({ email: 'v@x.org' });
    expect(resolveQualifyingContext({ config, attendee: { first_name: 'V' } }).attendee).toBeUndefined();
  });
});

describe('runNewBookingEntry — engage / bootstrap / no-op', () => {
  const base = {
    responseText: 'hi', conversationHistory: [], tenantId: 'T1', sessionId: 'S1',
    config: { feature_flags: { scheduling_enabled: true }, scheduling: { appointment_types: { apt1: {} } } },
    bedrock: {}, write: jest.fn(),
  };

  test('feature-gated (defense-in-depth): scheduling_enabled !== true → no-op even with a new_booking signal', async () => {
    const loadState = jest.fn().mockResolvedValue(null);
    const saveState = jest.fn();
    const off = { ...base, config: { scheduling: { appointment_types: { apt1: {} } } } }; // no feature_flags
    expect(await runNewBookingEntry({ ...off, routingMetadata: { scheduling_intent: 'new_booking' }, deps: { loadState, saveState } }))
      .toEqual({ handled: false });
    expect(loadState).not.toHaveBeenCalled();
    expect(saveState).not.toHaveBeenCalled();
    expect(runNewBookingTurn).not.toHaveBeenCalled();
  });

  test('IN_FLIGHT_STATES excludes the terminal booked + the recovery states', () => {
    expect(IN_FLIGHT_STATES).toEqual(['qualifying', 'proposing', 'confirming']);
  });

  test('missing tenantId/sessionId → handled:false, flow NOT called', async () => {
    expect(await runNewBookingEntry({ ...base, tenantId: '' })).toEqual({ handled: false });
    expect(await runNewBookingEntry({ ...base, sessionId: '' })).toEqual({ handled: false });
    expect(runNewBookingTurn).not.toHaveBeenCalled();
  });

  test('normal chat (no intent, no in-flight session) → no-op: handled:false, NO flow, NO saveState', async () => {
    const loadState = jest.fn().mockResolvedValue(null);
    const saveState = jest.fn();
    expect(await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState, saveState } })).toEqual({ handled: false });
    expect(runNewBookingTurn).not.toHaveBeenCalled();
    expect(saveState).not.toHaveBeenCalled();
  });

  test('fresh new_booking signal, nothing in flight → creates qualifying row + drives the flow', async () => {
    const loadState = jest.fn().mockResolvedValue(null);
    const saveState = jest.fn();
    const res = await runNewBookingEntry({ ...base, routingMetadata: { scheduling_intent: 'new_booking' }, deps: { loadState, saveState } });
    expect(saveState).toHaveBeenCalledWith({ tenantId: 'T1', sessionId: 'S1', state: 'qualifying' });
    expect(runNewBookingTurn).toHaveBeenCalledTimes(1);
    expect(runNewBookingTurn.mock.calls[0][0].deps.qualifyingContext.appointmentTypeId).toBe('apt1');
    expect(res.handled).toBe(true);
  });

  test('new_booking signal BUT a session is already in flight → does NOT reset (no qualifying write)', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'proposing' });
    const saveState = jest.fn();
    await runNewBookingEntry({ ...base, routingMetadata: { scheduling_intent: 'new_booking' }, deps: { loadState, saveState } });
    expect(saveState).not.toHaveBeenCalled();
    expect(runNewBookingTurn).toHaveBeenCalledTimes(1);
  });

  test('in-flight session, no signal → drives the flow (continuation)', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'confirming' });
    const saveState = jest.fn();
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState, saveState } });
    expect(saveState).not.toHaveBeenCalled();
    expect(runNewBookingTurn).toHaveBeenCalledTimes(1);
  });

  test('a terminal booked session is NOT re-engaged by a stray later turn', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'booked' });
    expect(await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState } })).toEqual({ handled: false });
    expect(runNewBookingTurn).not.toHaveBeenCalled();
  });

  test('deps pass through to the flow (invoke seams + state store + qualifyingContext)', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'qualifying' });
    const invokeProposal = jest.fn();
    const invokeBookingCommit = jest.fn();
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState, invokeProposal, invokeBookingCommit } });
    const passed = runNewBookingTurn.mock.calls[0][0].deps;
    expect(passed.invokeProposal).toBe(invokeProposal);
    expect(passed.invokeBookingCommit).toBe(invokeBookingCommit);
    expect(passed.qualifyingContext).toBeDefined();
  });

  test('loadState throws → non-fatal { handled:false, error:true } (never breaks chat)', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const loadState = jest.fn().mockRejectedValue(new Error('ddb'));
    expect(await runNewBookingEntry({ ...base, routingMetadata: { scheduling_intent: 'new_booking' }, deps: { loadState } }))
      .toEqual({ handled: false, error: true });
  });
});
