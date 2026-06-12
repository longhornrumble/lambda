'use strict';

/**
 * newBookingEntry.js — the integrator entry-hook glue (§B16d). Tests the ENGAGE / BOOTSTRAP /
 * NO-OP decision + qualifyingContext resolution. runNewBookingTurn is mocked so these assert
 * the entry logic in isolation (whether the flow is driven, and with what deps).
 */

jest.mock('../newBookingFlow', () => ({
  runNewBookingTurn: jest.fn(),
}));
// Mock the WS-C2 read primitives so no real DDB query runs. Default: no submissions → no
// attendee (the v1 hold). Per-test overrides drive the attendee-sourcing cases.
jest.mock('../formInjection', () => ({
  fetchSessionSubmissions: jest.fn(),
  pickLatest: jest.fn(),
}));

const { runNewBookingTurn } = require('../newBookingFlow');
const { fetchSessionSubmissions, pickLatest } = require('../formInjection');
const { runNewBookingEntry, resolveQualifyingContext, resolveSessionAttendee, IN_FLIGHT_STATES } = require('../newBookingEntry');

beforeEach(() => {
  jest.clearAllMocks();
  // Re-establish defaults each test (clearAllMocks keeps implementations, so reset explicitly
  // to prevent a per-test mockResolvedValue from leaking into the next test).
  runNewBookingTurn.mockResolvedValue({ handled: true, state: 'proposing', action: 'none' });
  fetchSessionSubmissions.mockResolvedValue([]);
  pickLatest.mockImplementation((items) => (Array.isArray(items) && items.length ? items[0] : null));
});

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

  test('§B16e: scheduling_day_selected rides routing_metadata into deps.schedulingDaySelected (in-flight turn)', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'proposing' });
    const res = await runNewBookingEntry({
      ...base,
      routingMetadata: { scheduling_day_selected: '2026-06-20' },
      deps: { loadState },
    });
    expect(runNewBookingTurn).toHaveBeenCalledTimes(1);
    expect(runNewBookingTurn.mock.calls[0][0].deps.schedulingDaySelected).toBe('2026-06-20');
    expect(res.handled).toBe(true);
  });

  test('§B16e: absent signal → deps.schedulingDaySelected undefined (schema discipline)', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'proposing' });
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState } });
    expect(runNewBookingTurn.mock.calls[0][0].deps.schedulingDaySelected).toBeUndefined();
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

describe('resolveSessionAttendee — §B16d form-injection read', () => {
  test('latest submission with canonical contact → { email, first_name, last_name, phone } trimmed', async () => {
    fetchSessionSubmissions.mockResolvedValue([
      { contact: { email: '  vol@x.org ', first_name: ' Vee ', last_name: ' Doe ', phone: ' 555-1234 ' } },
    ]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' }))
      .toEqual({ email: 'vol@x.org', first_name: 'Vee', last_name: 'Doe', phone: '555-1234' });
  });

  test('email only (no name/phone fields) → { email } (optional keys omitted, not empty)', async () => {
    fetchSessionSubmissions.mockResolvedValue([{ contact: { email: 'a@b.io' } }]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toEqual({ email: 'a@b.io' });
  });

  test('accepts plus-addressing, subdomains, and mixed case', async () => {
    for (const good of ['user+tag@sub.example.co', 'USER@X.ORG', 'a.b-c@mail.example.com']) {
      fetchSessionSubmissions.mockResolvedValue([{ contact: { email: good } }]);
      expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toEqual({ email: good });
    }
  });

  test('over-long email (> 254, RFC 5321 cap) → null even if otherwise well-shaped', async () => {
    const longLocal = 'a'.repeat(250);
    fetchSessionSubmissions.mockResolvedValue([{ contact: { email: `${longLocal}@x.org` } }]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toBeNull();
  });

  test('over-long name/phone are capped (defensive vs an oversized calendar invite)', async () => {
    fetchSessionSubmissions.mockResolvedValue([
      { contact: { email: 'a@b.io', first_name: 'x'.repeat(500), phone: '9'.repeat(200) } },
    ]);
    const a = await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' });
    expect(a.first_name).toHaveLength(100);
    expect(a.phone).toHaveLength(40);
  });

  test('no submissions → null (the identity hold)', async () => {
    fetchSessionSubmissions.mockResolvedValue([]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toBeNull();
  });

  test('submission missing email → null', async () => {
    fetchSessionSubmissions.mockResolvedValue([{ contact: { first_name: 'V' } }]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toBeNull();
  });

  test('malformed email → null (worse than none — would book a dead inbox)', async () => {
    // incl. the shapes the pre-audit regex wrongly passed: angle-bracket, trailing-dot,
    // IP-literal, <2-char TLD (audit A1).
    for (const bad of [
      'not-an-email', 'x@y', 'x@y.', '@x.org', 'a b@x.org',
      '<a@b.com>', 'a@b.com.', 'a@b.c', 'a@[10.0.0.1]',
    ]) {
      fetchSessionSubmissions.mockResolvedValue([{ contact: { email: bad } }]);
      expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toBeNull();
    }
  });

  test('with multiple submissions, the most recent (by submitted_at) supplies identity', async () => {
    // Use the REAL pickLatest so the recency contract at the seam is actually exercised.
    pickLatest.mockImplementation(jest.requireActual('../formInjection').pickLatest);
    fetchSessionSubmissions.mockResolvedValue([
      { contact: { email: 'old@x.org' }, submitted_at: '2026-05-01T00:00:00Z' },
      { contact: { email: 'new@x.org' }, submitted_at: '2026-05-30T00:00:00Z' },
    ]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toEqual({ email: 'new@x.org' });
  });

  test('non-string / absent contact → null (schema discipline, no throw)', async () => {
    fetchSessionSubmissions.mockResolvedValue([{ contact: { email: 12345 } }]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toBeNull();
    fetchSessionSubmissions.mockResolvedValue([{}]);
    expect(await resolveSessionAttendee({ tenantId: 'T1', sessionId: 'S1' })).toBeNull();
  });

  test('fetch throws → null (non-fatal); PII-safe log carries no email/name/tenant/session', async () => {
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    fetchSessionSubmissions.mockRejectedValue(new Error('contains secret vol@x.org'));
    expect(await resolveSessionAttendee({ tenantId: 'TENANT-SECRET', sessionId: 'SESSION-SECRET' })).toBeNull();
    const logged = errSpy.mock.calls.map((c) => c.join(' ')).join('\n');
    expect(logged).toMatch(/error_name=Error/);
    expect(logged).not.toMatch(/vol@x\.org|TENANT-SECRET|SESSION-SECRET/);
    errSpy.mockRestore();
  });
});

describe('runNewBookingEntry — attendee wiring into qualifyingContext', () => {
  const base = {
    responseText: 'hi', conversationHistory: [], tenantId: 'T1', sessionId: 'S1',
    config: { feature_flags: { scheduling_enabled: true }, scheduling: { appointment_types: { apt1: {} } } },
    bedrock: {}, write: jest.fn(),
  };

  test('sourced attendee with email → qctx.attendee forwarded to the flow', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'confirming' });
    const getSessionAttendee = jest.fn().mockResolvedValue({ email: 'vol@x.org', first_name: 'Vee' });
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState, getSessionAttendee } });
    expect(getSessionAttendee).toHaveBeenCalledWith({ tenantId: 'T1', sessionId: 'S1' });
    expect(runNewBookingTurn.mock.calls[0][0].deps.qualifyingContext.attendee).toEqual({ email: 'vol@x.org', first_name: 'Vee' });
  });

  test('no attendee sourced (null) → qctx.attendee unset (flow holds at confirming)', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'confirming' });
    const getSessionAttendee = jest.fn().mockResolvedValue(null);
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState, getSessionAttendee } });
    expect(runNewBookingTurn.mock.calls[0][0].deps.qualifyingContext.attendee).toBeUndefined();
  });

  test('default sourcing path (no deps.getSessionAttendee) uses the form-injection read', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'qualifying' });
    fetchSessionSubmissions.mockResolvedValue([{ contact: { email: 'def@x.org' } }]);
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState } });
    expect(fetchSessionSubmissions).toHaveBeenCalledWith({ tenantId: 'T1', sessionId: 'S1' });
    expect(runNewBookingTurn.mock.calls[0][0].deps.qualifyingContext.attendee).toEqual({ email: 'def@x.org' });
  });

  test('bootstrap turn (intent=new_booking, nothing in flight) + sourced attendee → qualifying row AND attendee forwarded', async () => {
    const loadState = jest.fn().mockResolvedValue(null);
    const saveState = jest.fn();
    const getSessionAttendee = jest.fn().mockResolvedValue({ email: 'boot@x.org', first_name: 'Boot' });
    await runNewBookingEntry({ ...base, routingMetadata: { scheduling_intent: 'new_booking' }, deps: { loadState, saveState, getSessionAttendee } });
    expect(saveState).toHaveBeenCalledWith({ tenantId: 'T1', sessionId: 'S1', state: 'qualifying' });
    expect(runNewBookingTurn.mock.calls[0][0].deps.qualifyingContext.attendee).toEqual({ email: 'boot@x.org', first_name: 'Boot' });
  });

  test('a freshly-sourced attendee CLOBBERS a stale deps.qualifyingContext from the caller', async () => {
    const loadState = jest.fn().mockResolvedValue({ state: 'confirming' });
    const getSessionAttendee = jest.fn().mockResolvedValue({ email: 'fresh@x.org' });
    // Caller passes a stale context (e.g. cached) — the fresh resolution must win (spread order).
    await runNewBookingEntry({ ...base, routingMetadata: {}, deps: { loadState, getSessionAttendee, qualifyingContext: { appointmentTypeId: 'STALE', attendee: { email: 'stale@x.org' } } } });
    const fwd = runNewBookingTurn.mock.calls[0][0].deps.qualifyingContext;
    expect(fwd.attendee).toEqual({ email: 'fresh@x.org' });
    expect(fwd.appointmentTypeId).toBe('apt1'); // freshly resolved, not the caller's 'STALE'
  });

  test('outer error (loadState throws) → non-fatal; PII-safe log carries no tenant/session', async () => {
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const loadState = jest.fn().mockRejectedValue(new Error('boom vol@x.org'));
    const res = await runNewBookingEntry({ ...base, tenantId: 'TENANT-SECRET', sessionId: 'SESSION-SECRET', routingMetadata: { scheduling_intent: 'new_booking' }, deps: { loadState } });
    expect(res).toEqual({ handled: false, error: true });
    const logged = errSpy.mock.calls.map((c) => c.join(' ')).join('\n');
    expect(logged).toMatch(/error_name=/);
    expect(logged).not.toMatch(/TENANT-SECRET|SESSION-SECRET|vol@x\.org/);
    errSpy.mockRestore();
  });
});
