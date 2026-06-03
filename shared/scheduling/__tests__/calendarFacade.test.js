'use strict';

/**
 * Unit tests for calendarFacade.js (WS-FACADE, FROZEN_CONTRACTS §B13 / §B9).
 *
 * Covers the locked done-bar:
 *   • the facade exposes the EXACT §B13 / §B9 shape
 *     ({ buildEventBody, insertEvent(calendarId, body), deleteEvent(calendarId, eventId),
 *       extractMeetJoinUrl }) so D6/D7/C8 can inject it unchanged;
 *   • insert/delete curry the per-(tenant, coordinator) OAuth client in as the FIRST arg
 *     to calendar-events — callers never supply an authClient;
 *   • buildEventBody / extractMeetJoinUrl pass through (no auth);
 *   • a facade built for tenant A NEVER resolves tenant-B auth (cross-tenant isolation);
 *   • deleteEvent idempotency is inherited (the wrapped resolve is forwarded, not re-thrown);
 *   • the unreachable/transient throw propagates (the cancel.js pending_calendar_sync path);
 *   • construction guards (missing tenantId / coordinatorId / deps throw).
 */

const { buildCalendarFacade } = require('../calendarFacade');

const TENANT_A = 'AUS123957';
const TENANT_B = 'FOS402334';
const COORD = 'coord-1';

// A distinct sentinel per (tenant, coordinator) so we can prove WHICH client was curried.
const makeDeps = () => {
  const calendarEvents = {
    buildEventBody: jest.fn((p) => ({ summary: 'built', _from: p })),
    insertEvent: jest.fn().mockResolvedValue({ id: 'evt-new' }),
    deleteEvent: jest.fn().mockResolvedValue(undefined),
    extractMeetJoinUrl: jest.fn().mockReturnValue('https://meet.example/abc'),
  };
  const getOAuthClient = jest.fn(({ tenantId, coordinatorId }) =>
    Promise.resolve({ __authFor: `${tenantId}/${coordinatorId}` })
  );
  return { getOAuthClient, calendarEvents };
};

describe('buildCalendarFacade — shape (§B13/§B9)', () => {
  test('returns exactly the four facade methods D6/D7/C8 consume', () => {
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps: makeDeps() });
    expect(Object.keys(facade).sort()).toEqual(
      ['buildEventBody', 'deleteEvent', 'extractMeetJoinUrl', 'insertEvent'].sort()
    );
    for (const k of ['buildEventBody', 'insertEvent', 'deleteEvent', 'extractMeetJoinUrl']) {
      expect(typeof facade[k]).toBe('function');
    }
  });
});

describe('buildCalendarFacade — insertEvent', () => {
  test('curries the per-tenant authClient as the FIRST arg, then (calendarId, body)', async () => {
    const deps = makeDeps();
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    const body = { summary: 'x' };
    const event = await facade.insertEvent('cal-A', body);

    expect(deps.getOAuthClient).toHaveBeenCalledWith({ tenantId: TENANT_A, coordinatorId: COORD });
    expect(deps.calendarEvents.insertEvent).toHaveBeenCalledWith(
      { __authFor: `${TENANT_A}/${COORD}` },
      'cal-A',
      body
    );
    expect(event).toEqual({ id: 'evt-new' });
  });

  test('callers never supply an authClient — the body is forwarded verbatim', async () => {
    const deps = makeDeps();
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });
    const body = { summary: 'verbatim', extendedProperties: { private: { booking_id: 'bk-9' } } };

    await facade.insertEvent('cal-A', body);

    const [, , forwardedBody] = deps.calendarEvents.insertEvent.mock.calls[0];
    expect(forwardedBody).toBe(body);
  });
});

describe('buildCalendarFacade — deleteEvent', () => {
  test('curries authClient FIRST, then (calendarId, eventId)', async () => {
    const deps = makeDeps();
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    await facade.deleteEvent('cal-A', 'evt-old');

    expect(deps.calendarEvents.deleteEvent).toHaveBeenCalledWith(
      { __authFor: `${TENANT_A}/${COORD}` },
      'cal-A',
      'evt-old'
    );
  });

  test('idempotency is inherited — a resolved (already-gone) delete resolves here too', async () => {
    const deps = makeDeps();
    deps.calendarEvents.deleteEvent.mockResolvedValue(undefined); // 404/410 resolves inside calendar-events
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    await expect(facade.deleteEvent('cal-A', 'evt-gone')).resolves.toBeUndefined();
  });

  test('an unreachable/transient throw propagates (cancel.js pending_calendar_sync path)', async () => {
    const deps = makeDeps();
    const boom = Object.assign(new Error('Service Unavailable'), { code: 503 });
    deps.calendarEvents.deleteEvent.mockRejectedValue(boom);
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    await expect(facade.deleteEvent('cal-A', 'evt-x')).rejects.toBe(boom);
  });
});

describe('buildCalendarFacade — pass-through methods (no auth)', () => {
  test('buildEventBody forwards params and result without resolving auth', () => {
    const deps = makeDeps();
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    const params = { bookingId: 'bk-1', start: 's', end: 'e' };
    const out = facade.buildEventBody(params);

    expect(deps.calendarEvents.buildEventBody).toHaveBeenCalledWith(params);
    expect(out).toEqual({ summary: 'built', _from: params });
    expect(deps.getOAuthClient).not.toHaveBeenCalled();
  });

  test('extractMeetJoinUrl forwards the event and result without resolving auth', () => {
    const deps = makeDeps();
    const facade = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    const event = { conferenceData: { entryPoints: [{ entryPointType: 'video', uri: 'u' }] } };
    const url = facade.extractMeetJoinUrl(event);

    expect(deps.calendarEvents.extractMeetJoinUrl).toHaveBeenCalledWith(event);
    expect(url).toBe('https://meet.example/abc');
    expect(deps.getOAuthClient).not.toHaveBeenCalled();
  });
});

describe('buildCalendarFacade — cross-tenant isolation (security done-bar)', () => {
  test('a facade built for tenant A never resolves tenant-B auth', async () => {
    const deps = makeDeps();
    const facadeA = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });

    await facadeA.insertEvent('cal-A', { summary: 'x' });
    await facadeA.deleteEvent('cal-A', 'evt-old');

    // Every auth resolution used tenant A; tenant B was never requested.
    for (const call of deps.getOAuthClient.mock.calls) {
      expect(call[0]).toEqual({ tenantId: TENANT_A, coordinatorId: COORD });
    }
    expect(deps.getOAuthClient).not.toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: TENANT_B })
    );
  });

  test('two facades for different tenants curry their OWN auth (no cross-bleed)', async () => {
    const deps = makeDeps();
    const facadeA = buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD, deps });
    const facadeB = buildCalendarFacade({ tenantId: TENANT_B, coordinatorId: COORD, deps });

    await facadeA.insertEvent('cal-A', { summary: 'a' });
    await facadeB.insertEvent('cal-B', { summary: 'b' });

    expect(deps.calendarEvents.insertEvent).toHaveBeenNthCalledWith(
      1, { __authFor: `${TENANT_A}/${COORD}` }, 'cal-A', { summary: 'a' }
    );
    expect(deps.calendarEvents.insertEvent).toHaveBeenNthCalledWith(
      2, { __authFor: `${TENANT_B}/${COORD}` }, 'cal-B', { summary: 'b' }
    );
  });
});

describe('buildCalendarFacade — construction guards', () => {
  test.each([
    ['missing tenantId', { tenantId: undefined, coordinatorId: COORD }],
    ['missing coordinatorId', { tenantId: TENANT_A, coordinatorId: undefined }],
  ])('throws on %s', (_label, partial) => {
    expect(() => buildCalendarFacade({ ...partial, deps: makeDeps() })).toThrow(
      /requires tenantId and coordinatorId/
    );
  });

  test('throws when deps.getOAuthClient is not a function', () => {
    expect(() =>
      buildCalendarFacade({
        tenantId: TENANT_A,
        coordinatorId: COORD,
        deps: { getOAuthClient: null, calendarEvents: {} },
      })
    ).toThrow(/requires deps.getOAuthClient and deps.calendarEvents/);
  });

  test('throws when deps.calendarEvents is missing', () => {
    expect(() =>
      buildCalendarFacade({
        tenantId: TENANT_A,
        coordinatorId: COORD,
        deps: { getOAuthClient: jest.fn() },
      })
    ).toThrow(/requires deps.getOAuthClient and deps.calendarEvents/);
  });

  test('throws when deps is omitted entirely', () => {
    expect(() => buildCalendarFacade({ tenantId: TENANT_A, coordinatorId: COORD })).toThrow(
      /requires deps.getOAuthClient and deps.calendarEvents/
    );
  });
});
