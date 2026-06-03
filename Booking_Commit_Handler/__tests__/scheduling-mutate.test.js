'use strict';

const { handleSchedulingMutate } = require('../scheduling-mutate');

// Minimal fakes for the injected seam (no real Google/Zoom/DDB).
function baseInjected(overrides = {}) {
  const calls = { reschedule: [], cancel: [], zoom: [], persist: [], facade: [] };
  const calendarEvents = {
    buildEventBody: (x) => ({ built: x }),
    insertEvent: (calId, body) => { calls.facade.push(['insert', calId]); return { id: 'evt-new' }; },
    deleteEvent: (calId, eid) => { calls.facade.push(['delete', calId, eid]); },
    extractMeetJoinUrl: () => 'https://meet',
  };
  return {
    calls,
    injected: {
      calendarEvents,
      getOAuthClient: async ({ tenantId, coordinatorId }) => { calls.facade.push(['auth', tenantId, coordinatorId]); return { auth: true }; },
      resolveProvider: () => ({ createConference: async () => ({ provider: 'google_meet' }) }),
      zoomClient: { updateMeeting: async (a) => { calls.zoom.push(a); } },
      bookingStore: { updateBookingReschedule: async (t, id, f) => { calls.persist.push([t, id, f]); } },
      executeReschedule: async ({ booking, newSlot }) => { calls.reschedule.push({ booking, newSlot }); return { outcome: 'success', booking: { ...booking, external_event_id: 'evt-new', start_at: newSlot.start } }; },
      executeCancel: async ({ booking }) => { calls.cancel.push({ booking }); return { outcome: 'deleted', booking }; },
      logger: { warn: () => {}, error: () => {} },
      ...overrides,
    },
  };
}

const RESCH_EVENT = {
  action: 'scheduling_mutate', mutation: 'reschedule',
  tenantId: 'T1', coordinatorId: 'coord@x.com', bookingId: 'bk1',
  booking: { booking_id: 'bk1', tenant_id: 'T1', coordinator_email: 'coord@x.com', external_event_id: 'evt-old' },
  newSlot: { start: '2026-07-01T15:00:00Z', end: '2026-07-01T15:30:00Z' },
};

describe('handleSchedulingMutate — validation', () => {
  it('fails on missing required fields', async () => {
    expect((await handleSchedulingMutate({ mutation: 'cancel' })).outcome).toBe('failed');
    expect((await handleSchedulingMutate({ action: 'scheduling_mutate', tenantId: 'T1' })).outcome).toBe('failed');
  });
  it('fails on unknown mutation', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingMutate({ mutation: 'frobnicate', tenantId: 'T1', coordinatorId: 'c', booking: {} }, injected);
    expect(out).toEqual({ outcome: 'failed', error: 'unknown_mutation' });
  });
  it('reschedule fails on missing newSlot', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingMutate({ ...RESCH_EVENT, newSlot: undefined }, injected);
    expect(out.outcome).toBe('failed');
  });
  it('SR-1: refuses a cross-tenant payload (event.tenantId != booking.tenant_id)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(
      { ...RESCH_EVENT, tenantId: 'T1', booking: { ...RESCH_EVENT.booking, tenant_id: 'T2' } },
      injected
    );
    expect(out).toEqual({ outcome: 'failed', error: 'tenant_mismatch' });
    expect(calls.reschedule).toHaveLength(0); // never reached the calendar op
  });
});

describe('handleSchedulingMutate — cancel', () => {
  it('runs executeCancel with the auth-curried facade and does NOT persist (listener owns status)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', bookingId: 'bk1', booking: { booking_id: 'bk1' } },
      injected
    );
    expect(out.outcome).toBe('deleted');
    expect(calls.cancel).toHaveLength(1);
    expect(calls.persist).toHaveLength(0); // §14.2 listener flips status; executor doesn't write
  });
});

describe('handleSchedulingMutate — reschedule', () => {
  it('runs executeReschedule then persists the new fields (option A)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('success');
    expect(calls.reschedule).toHaveLength(1);
    expect(calls.persist).toHaveLength(1);
    const [t, id, fields] = calls.persist[0];
    expect(t).toBe('T1');
    expect(id).toBe('bk1');
    expect(fields.startAt).toBe(RESCH_EVENT.newSlot.start);
    expect(fields.externalEventId).toBe('evt-new');
    expect(fields.pendingCalendarSync).toBe(false);
  });

  it('does NOT call zoom.updateMeeting for a non-zoom (google_meet) booking', async () => {
    const { injected, calls } = baseInjected(); // RESCH_EVENT booking has no conference_provider
    await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(calls.zoom).toHaveLength(0);
  });

  it('PATCHes Zoom start-time only for a zoom booking', async () => {
    const { injected, calls } = baseInjected({
      executeReschedule: async ({ booking, newSlot }) => ({ outcome: 'success', booking: { ...booking, conference_provider: 'zoom', conference_id: '99887766', external_event_id: 'evt-new' } }),
    });
    await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(calls.zoom).toHaveLength(1);
    expect(calls.zoom[0].meetingId).toBe('99887766');
  });

  it('does NOT persist on a failed reschedule', async () => {
    const { injected, calls } = baseInjected({
      executeReschedule: async () => ({ outcome: 'failed', booking: RESCH_EVENT.booking }),
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('failed');
    expect(calls.persist).toHaveLength(0);
  });

  it('persist failure is non-fatal (calendar already moved; listener backstops)', async () => {
    const { injected } = baseInjected({
      bookingStore: { updateBookingReschedule: async () => { throw new Error('ddb down'); } },
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('success'); // swallowed
  });
});

describe('handleSchedulingMutate — unexpected throw → clean failed (no FunctionError)', () => {
  it('getOAuthClient throwing yields { outcome: failed } (BSH fallback), not a propagated error', async () => {
    const { injected } = baseInjected({
      getOAuthClient: async () => { throw new Error('SecretsManagerAccessDenied'); },
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out).toEqual({ outcome: 'failed', error: 'executor_error' });
  });
  it('a thrown executeCancel also folds to failed (not a rejection)', async () => {
    const { injected } = baseInjected({
      executeCancel: async () => { throw new Error('google 500'); },
    });
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', booking: { booking_id: 'bk1' } },
      injected
    );
    expect(out.outcome).toBe('failed');
  });
});
