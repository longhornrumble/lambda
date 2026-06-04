'use strict';

/**
 * Unit tests for index.js — the C8 commit-transaction orchestration.
 *
 * Strategy: the shared/scheduling/* contracts (pool/routing/availability) and the
 * external boundaries (oauth-client, calendar events.insert/delete, SES email,
 * Booking DDB writes, zoom) are mocked; the REAL conference-providers are used and
 * driven via the `providerOverrides` DI seam — so the headline "NullConferenceProvider
 * injected → commit completes touching neither Google nor Zoom" test exercises the
 * actual interface, not a stub. The pure parts of calendar-events/booking-store stay
 * real (buildEventBody / buildBookingId / classifyAuthError).
 */

// Set BEFORE requiring ./index — OPS_ALERTS_TOPIC_ARN is read at module load, and
// these tests exercise the SNS admin-alert paths.
process.env.OPS_ALERTS_TOPIC_ARN = 'arn:aws:sns:us-east-1:525409062831:picasso-ops-alerts-staging';

// Feature gate: these tests exercise the commit LOGIC, so scheduling is enabled.
// The disabled/refuse paths live in index.gate.test.js.
jest.mock('../shared/scheduling/featureGate', () => ({
  isSchedulingEnabledForTenant: jest.fn().mockResolvedValue(true),
  isSchedulingEnabled: () => true,
}));
jest.mock('../shared/scheduling/pool', () => ({
  lockSlot: jest.fn(),
  recordFreeBusySuccess: jest.fn(),
  recordFreeBusyFailure: jest.fn(),
}));
jest.mock('../shared/scheduling/routing', () => ({
  advanceRoundRobin: jest.fn(),
  revertRoundRobin: jest.fn(),
}));
jest.mock('../shared/scheduling/availability', () => ({
  getBusyIntervals: jest.fn(),
}));
jest.mock('./oauth-client', () => ({
  getOAuthClient: jest.fn(async () => ({})),
  // Default: calendarId === coordinatorId (v1 convention / coordinator_email absent).
  // The decoupling test below overrides this to a distinct real-calendar email.
  getCoordinatorCalendarId: jest.fn(async ({ coordinatorId }) => coordinatorId),
  clearCacheEntry: jest.fn(),
}));
jest.mock('./zoom-client', () => ({
  deleteMeeting: jest.fn(async () => {}),
  createMeeting: jest.fn(),
}));
jest.mock('./calendar-events', () => {
  const actual = jest.requireActual('./calendar-events');
  return { ...actual, insertEvent: jest.fn(), deleteEvent: jest.fn(async () => {}) };
});
jest.mock('./booking-store', () => {
  const actual = jest.requireActual('./booking-store');
  return {
    ...actual,
    getBookingById: jest.fn(),
    writeBooking: jest.fn(),
    readLock: jest.fn(async () => null),
    recordConferenceOnLock: jest.fn(async () => {}),
    setLockTtl: jest.fn(async () => 0),
    releaseLock: jest.fn(async () => {}),
    flagLockForReconciliation: jest.fn(async () => {}),
    writeDegradedMarker: jest.fn(async () => {}),
  };
});
jest.mock('./confirmation-email', () => ({
  sendConfirmationEmail: jest.fn(async () => ({ messageId: 'msg-1', rescheduleUrl: 'r', cancelUrl: 'c' })),
}));

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const snsMock = mockClient(SNSClient);

const pool = require('../shared/scheduling/pool');
const routing = require('../shared/scheduling/routing');
const availability = require('../shared/scheduling/availability');
const oauth = require('./oauth-client');
const calendarEvents = require('./calendar-events');
const bookingStore = require('./booking-store');
const confirmationEmail = require('./confirmation-email');
const { NullConferenceProvider } = require('./conference-providers');

const { handler } = require('./index');

const FREE = { busy: [], cachedAt: 'x', source: 'google_freebusy' };
const EVENT = { id: 'evt-1', conferenceData: {}, updated: '2026-06-01T00:00:00.000Z' };

function baseEvent(overrides = {}) {
  return {
    tenant_id: 'AUS123957',
    session_id: 'sess-1',
    slot: { start: '2026-06-03T18:00:00.000Z', end: '2026-06-03T18:30:00.000Z', candidateResourceIds: ['res-a'] },
    pool_size: 1,
    appointment_type: { id: 'apt', name: 'Volunteer intake', format: 'one_to_one', timezone: 'America/Chicago', cancellation_window_hours: 0 },
    tie_breaker: 'first_available',
    round_robin_cursor: null,
    user_time_zone: 'America/Chicago',
    attendee: { first_name: 'Sam', last_name: 'Patel', email: 'sam@example.com' },
    conference_type: 'null',
    coordinator_emails: { 'res-a': 'maya@org.org', 'res-b': 'diego@org.org' },
    org_name: 'Austin Angels',
    ...overrides,
  };
}

function nullInjection() {
  return { providerOverrides: { null: new NullConferenceProvider() } };
}

beforeEach(() => {
  jest.clearAllMocks();
  snsMock.reset();
  snsMock.on(PublishCommand).resolves({ MessageId: 'sns-1' });
  availability.getBusyIntervals.mockResolvedValue(FREE);
  bookingStore.getBookingById.mockResolvedValue(null);
  bookingStore.readLock.mockResolvedValue(null);
  bookingStore.writeBooking.mockResolvedValue({ booking_id: { S: 'booking#x' }, status: { S: 'booked' } });
  calendarEvents.insertEvent.mockResolvedValue(EVENT);
  oauth.getOAuthClient.mockResolvedValue({});
  oauth.getCoordinatorCalendarId.mockImplementation(async ({ coordinatorId }) => coordinatorId);
});

// ─── validation ──────────────────────────────────────────────────────────────────────

describe('input validation', () => {
  it('rejects a bad tenant_id', async () => {
    await expect(handler(baseEvent({ tenant_id: 'bad/id' }))).rejects.toThrow(/tenant_id/);
  });
  it('requires slot.candidateResourceIds', async () => {
    await expect(handler(baseEvent({ slot: { start: 'a', end: 'b', candidateResourceIds: [] } }))).rejects.toThrow(/candidateResourceIds/);
  });
  it('requires attendee.email', async () => {
    await expect(handler(baseEvent({ attendee: { first_name: 'X' } }))).rejects.toThrow(/attendee.email/);
  });
  it('rejects an unknown conference_type', async () => {
    await expect(handler(baseEvent({ conference_type: 'teams' }))).rejects.toThrow(/conference_type/);
  });
  it('requires pool_size', async () => {
    await expect(handler(baseEvent({ pool_size: 0 }))).rejects.toThrow(/pool_size/);
  });
});

// ─── idempotency gate (AC #6 / C11) ────────────────────────────────────────────────────

describe('idempotency gate', () => {
  it('returns ALREADY_CONFIRMED when a booked row already exists; never locks', async () => {
    bookingStore.getBookingById.mockResolvedValue({ status: { S: 'booked' } });
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('ALREADY_CONFIRMED');
    expect(pool.lockSlot).not.toHaveBeenCalled();
    expect(calendarEvents.insertEvent).not.toHaveBeenCalled();
  });
});

// ─── step 1: live freeBusy re-check ─────────────────────────────────────────────────────

describe('live freeBusy re-check (§5.4 layer 2)', () => {
  it('reoffers when the chosen slot is now busy; never locks', async () => {
    availability.getBusyIntervals.mockResolvedValue({ busy: [{ start: '2026-06-03T18:00:00.000Z', end: '2026-06-03T18:30:00.000Z' }] });
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res).toMatchObject({ status: 'SLOT_UNAVAILABLE', reason: 'recheck_busy' });
    expect(pool.lockSlot).not.toHaveBeenCalled();
  });
});

// ─── the headline NullConferenceProvider injection test ─────────────────────────────────

describe('NullConferenceProvider DI — interface-seam verification', () => {
  it('commit completes, Booking written, NO Google/Zoom conference calls', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1', format: 'one_to_one' });
    const res = await handler(baseEvent(), {}, nullInjection());

    expect(res.status).toBe('BOOKED');
    expect(res.conferenceProvider).toBe('null');
    expect(bookingStore.writeBooking).toHaveBeenCalledTimes(1);
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1'); // lock released on success
    expect(confirmationEmail.sendConfirmationEmail).toHaveBeenCalledTimes(1);
    // No conference creation touched Zoom (Null is a no-op); no duplicate-meeting reuse needed.
    expect(require('./zoom-client').createMeeting).not.toHaveBeenCalled();
    // The Booking row carries the deterministic booking_id and status booked.
    const written = bookingStore.writeBooking.mock.calls[0][0];
    expect(written.status).toBe('booked');
    expect(written.externalEventId).toBe('evt-1');
  });
});

// ─── calendarId decoupled from the secret-path key (coordinator_email) ──────────────────
//
// The OAuth secret-path key (coordinatorId) can be an opaque routing label; the real
// writable calendar is the secret's coordinator_email. getCoordinatorCalendarId resolves
// it. These assert the commit writes the REAL calendar everywhere — else events.insert
// 404s on the label and a later cancel/reschedule (which reads Booking.coordinatorEmail)
// 404s too. Regression guard for the in-chat-booking UAT defect (2026-06-04).
describe('calendarId resolves from coordinator_email, not the secret-path label', () => {
  const REAL_CAL = 'coordinator-maya@org.example';

  it('insert + persisted coordinatorEmail target the real calendar, not coordinatorId', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1', format: 'one_to_one' });
    oauth.getCoordinatorCalendarId.mockResolvedValue(REAL_CAL); // label 'res-a' → real calendar
    const res = await handler(baseEvent(), {}, nullInjection());

    expect(res.status).toBe('BOOKED');
    // events.insert(authClient, calendarId, requestBody) — calendarId is the real
    // calendar, decoupled from the secret-path key coordinatorId ('maya@org.org').
    expect(calendarEvents.insertEvent.mock.calls[0][1]).toBe(REAL_CAL);
    expect(calendarEvents.insertEvent.mock.calls[0][1]).not.toBe('maya@org.org');
    // Booking.coordinatorEmail is what cancel/reschedule later use as the calendarId.
    expect(bookingStore.writeBooking.mock.calls[0][0].coordinatorEmail).toBe(REAL_CAL);
    // The confirmation email's ORGANIZER also carries the real calendar (not the label).
    expect(confirmationEmail.sendConfirmationEmail.mock.calls[0][0].coordinatorEmail).toBe(REAL_CAL);
    // The OAuth secret lookup still uses the path-key coordinatorId, NOT the calendar id.
    expect(oauth.getOAuthClient.mock.calls[0][0].coordinatorId).toBe('maya@org.org');
    expect(oauth.getCoordinatorCalendarId.mock.calls[0][0].coordinatorId).toBe('maya@org.org');
  });

  it('rollback deletes from the real calendar when the booking write fails', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1', format: 'one_to_one' });
    oauth.getCoordinatorCalendarId.mockResolvedValue(REAL_CAL);
    bookingStore.writeBooking.mockRejectedValue(new Error('DDB throttle'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    // events.delete(client, calendarId, eventId) — rollback targets the real calendar.
    expect(calendarEvents.deleteEvent).toHaveBeenCalled();
    expect(calendarEvents.deleteEvent.mock.calls[0][1]).toBe(REAL_CAL);
  });
});

// ─── slot lock (C6) outcomes ────────────────────────────────────────────────────────────

describe('slot-lock outcomes', () => {
  it('passes a SLOT_UNAVAILABLE reoffer straight back (no booking)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'SLOT_UNAVAILABLE', action: 'reoffer', nextAttempt: 2 });
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res).toMatchObject({ status: 'SLOT_UNAVAILABLE', nextAttempt: 2 });
    expect(bookingStore.writeBooking).not.toHaveBeenCalled();
  });
});

// ─── Google Meet path ───────────────────────────────────────────────────────────────────

describe('Google Meet — conference rides events.insert', () => {
  it('extracts the Meet join URL from the inserted event; no recordConferenceOnLock', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockResolvedValue({
      id: 'evt-9', updated: 'x',
      conferenceData: { conferenceId: 'cf-1', entryPoints: [{ entryPointType: 'video', uri: 'https://meet.google.com/abc' }] },
    });
    const res = await handler(baseEvent({ conference_type: 'google_meet' }));
    expect(res.status).toBe('BOOKED');
    expect(res.joinUrl).toBe('https://meet.google.com/abc');
    expect(res.conferenceProvider).toBe('google_meet');
    expect(bookingStore.recordConferenceOnLock).not.toHaveBeenCalled(); // Meet defers
    // events.insert carried the booking_id ownership tag + a createRequest.
    const body = calendarEvents.insertEvent.mock.calls[0][2];
    expect(body.extendedProperties.private.booking_id).toBeDefined();
    expect(body.conferenceData.createRequest).toBeDefined();
  });
});

// ─── Zoom path + read-before-write ──────────────────────────────────────────────────────

describe('Zoom — read-before-write idempotency', () => {
  function zoomProvider(createMeeting) {
    return { providerOverrides: { zoom: { createConference: async (ctx) => {
      const m = await createMeeting(ctx);
      return { provider: 'zoom', conferenceId: m.meetingId, joinUrl: m.joinUrl, deferToCalendarInsert: false };
    } } } };
  }

  it('records the conference id on the lock BEFORE insert (so a retry reuses it)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const createMeeting = jest.fn(async () => ({ meetingId: 'z-1', joinUrl: 'https://zoom.us/j/z-1' }));
    const res = await handler(baseEvent({ conference_type: 'zoom' }), {}, zoomProvider(createMeeting));
    expect(res.status).toBe('BOOKED');
    expect(res.joinUrl).toBe('https://zoom.us/j/z-1');
    expect(bookingStore.recordConferenceOnLock).toHaveBeenCalledWith('AUS123957', 'lk1', { conferenceId: 'z-1', provider: 'zoom' });
  });

  it('on a retry, passes the recovered conference id to the provider (existingConferenceId)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.readLock.mockResolvedValue({ conference_id: { S: 'z-prior' } });
    let seenCtx;
    const createMeeting = jest.fn(async (ctx) => { seenCtx = ctx; return { meetingId: ctx.existingConferenceId || 'new', joinUrl: 'u' }; });
    await handler(baseEvent({ conference_type: 'zoom' }), {}, zoomProvider(createMeeting));
    expect(seenCtx.existingConferenceId).toBe('z-prior');
  });

  it('Zoom create failure → compensate + release lock + graceful COMMIT_FAILED', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const overrides = { providerOverrides: { zoom: { createConference: async () => { throw new Error('Zoom 429'); } } } };
    const res = await handler(baseEvent({ conference_type: 'zoom' }), {}, overrides);
    expect(res.status).toBe('COMMIT_FAILED');
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1');
    expect(calendarEvents.insertEvent).not.toHaveBeenCalled();
  });
});

// ─── OAuth-401 threading (§5.5 row 4) ───────────────────────────────────────────────────

describe('OAuth-401 thread', () => {
  it('transient 401 → clear cache, refresh, retry once, succeed', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent
      .mockRejectedValueOnce({ code: 401 })
      .mockResolvedValueOnce(EVENT);
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('BOOKED');
    expect(oauth.clearCacheEntry).toHaveBeenCalledTimes(1);
    expect(oauth.getOAuthClient).toHaveBeenCalledTimes(2);
  });

  it('permanent revoke (single candidate) → degrade marker + reoffer all_degraded', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockRejectedValue({ response: { data: { error: 'invalid_grant' } } });
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res).toMatchObject({ status: 'SLOT_UNAVAILABLE', reason: 'all_candidates_degraded' });
    expect(bookingStore.writeDegradedMarker).toHaveBeenCalledWith('AUS123957', 'maya@org.org', 'oauth_revoked');
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1');
  });

  it('permanent revoke (pool) → degrade + re-pool against the next candidate → BOOKED', async () => {
    pool.lockSlot
      .mockResolvedValueOnce({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk-a' })
      .mockResolvedValueOnce({ status: 'LOCKED', resourceId: 'res-b', lockKey: 'lk-b' });
    calendarEvents.insertEvent
      .mockRejectedValueOnce({ response: { data: { error: 'invalid_grant' } } }) // res-a revoked
      .mockResolvedValueOnce(EVENT); // res-b succeeds
    const ev = baseEvent({ pool_size: 2, slot: { start: '2026-06-03T18:00:00.000Z', end: '2026-06-03T18:30:00.000Z', candidateResourceIds: ['res-a', 'res-b'] } });
    const res = await handler(ev, {}, nullInjection());
    expect(res.status).toBe('BOOKED');
    expect(res.resourceId).toBe('res-b');
    expect(bookingStore.writeDegradedMarker).toHaveBeenCalledWith('AUS123957', 'maya@org.org', 'oauth_revoked');
    expect(pool.lockSlot).toHaveBeenCalledTimes(2);
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk-a'); // degraded attempt released
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk-b'); // success released
  });
});

// ─── round-robin advance + compensating revert (§10.2) ──────────────────────────────────

describe('round-robin advance/revert', () => {
  const RR = {
    tie_breaker: 'round_robin',
    round_robin_cursor: { routingPolicyId: 'rp-1', previousResourceId: 'res-z', previousAt: 123 },
  };

  it('advances round-robin only after a successful commit', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const res = await handler(baseEvent(RR), {}, nullInjection());
    expect(res.status).toBe('BOOKED');
    expect(routing.advanceRoundRobin).toHaveBeenCalledWith({ tenantId: 'AUS123957', routingPolicyId: 'rp-1', assignedResourceId: 'res-a' });
    expect(routing.revertRoundRobin).not.toHaveBeenCalled();
  });

  it('reverts round-robin when the Booking write fails AFTER advance', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.writeBooking.mockRejectedValue(new Error('DDB throttle'));
    const res = await handler(baseEvent(RR), {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    expect(routing.advanceRoundRobin).toHaveBeenCalled();
    // compensating revert restores the prior cursor so res-a is not skipped next time
    expect(routing.revertRoundRobin).toHaveBeenCalledWith({ tenantId: 'AUS123957', routingPolicyId: 'rp-1', previousResourceId: 'res-z', previousAt: 123 });
    expect(calendarEvents.deleteEvent).toHaveBeenCalled(); // event rolled back
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1');
  });
});

// ─── Booking write race (concurrent confirm) ────────────────────────────────────────────

describe('concurrent double-confirm', () => {
  it('race-loser deletes its event, returns ALREADY_CONFIRMED for the winner', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const condErr = new Error('cond'); condErr.name = 'ConditionalCheckFailedException';
    bookingStore.writeBooking.mockRejectedValue(condErr);
    // first getBookingById (gate) → null; second (after race) → the winner's row.
    bookingStore.getBookingById.mockResolvedValueOnce(null).mockResolvedValueOnce({ status: { S: 'booked' } });
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('ALREADY_CONFIRMED');
    expect(calendarEvents.deleteEvent).toHaveBeenCalled(); // our duplicate event rolled back
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1');
  });
});

// ─── confirmation email failure is non-fatal (fail forward, never half-book) ────────────

describe('confirmation email failure', () => {
  it('booking stays committed even if the email send throws', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    confirmationEmail.sendConfirmationEmail.mockRejectedValue(new Error('SES down'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('BOOKED'); // not rolled back
    expect(calendarEvents.deleteEvent).not.toHaveBeenCalled();
    expect(routing.revertRoundRobin).not.toHaveBeenCalled();
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1');
  });
});

// ─── lock release on EVERY path ─────────────────────────────────────────────────────────

describe('unconditional lock release', () => {
  it('releases the lock when a non-auth calendar error occurs', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockRejectedValue(new Error('calendar timeout'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    expect(bookingStore.releaseLock).toHaveBeenCalledWith('AUS123957', 'lk1');
  });

  it('flags the lock for reconciliation if release itself fails (orphan-lock ops hook)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockRejectedValue(new Error('calendar timeout'));
    bookingStore.releaseLock.mockRejectedValue(new Error('DDB down'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    expect(bookingStore.flagLockForReconciliation).toHaveBeenCalled();
  });
});

// ─── audit fix-now: compensation + alert branch coverage ────────────────────────────────

describe('audit fixes — compensation + alert branches', () => {
  function zoomProvider(meetingId) {
    return { providerOverrides: { zoom: { createConference: async () => ({ provider: 'zoom', conferenceId: meetingId, joinUrl: 'https://zoom.us/j/' + meetingId, deferToCalendarInsert: false }) } } };
  }

  it('Fix 2: stamps a lock TTL right after acquisition', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    await handler(baseEvent(), {}, nullInjection());
    expect(bookingStore.setLockTtl).toHaveBeenCalledWith('AUS123957', 'lk1');
  });

  it('Fix 3: lock is released BEFORE the email step', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const order = [];
    bookingStore.releaseLock.mockImplementation(async () => { order.push('release'); });
    confirmationEmail.sendConfirmationEmail.mockImplementation(async () => { order.push('email'); return { messageId: 'm' }; });
    await handler(baseEvent(), {}, nullInjection());
    expect(order).toEqual(['release', 'email']);
  });

  it('Fix 4: BOOKED response carries no coordinatorEmail (PII boundary)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res).not.toHaveProperty('coordinatorEmail');
    expect(res.resourceId).toBe('res-a');
  });

  it('Zoom-meeting rollback: writeBooking fails → deleteMeeting fires', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.writeBooking.mockRejectedValue(new Error('DDB throttle'));
    const res = await handler(baseEvent({ conference_type: 'zoom' }), {}, zoomProvider('z-9'));
    expect(res.status).toBe('COMMIT_FAILED');
    expect(require('./zoom-client').deleteMeeting).toHaveBeenCalledWith('AUS123957', 'z-9');
  });

  it('race-loser WITH round-robin advanced → reverts RR and returns ALREADY_CONFIRMED', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const condErr = new Error('cond'); condErr.name = 'ConditionalCheckFailedException';
    bookingStore.writeBooking.mockRejectedValue(condErr);
    bookingStore.getBookingById.mockResolvedValueOnce(null).mockResolvedValueOnce({ status: { S: 'booked' } });
    const ev = baseEvent({ tie_breaker: 'round_robin', round_robin_cursor: { routingPolicyId: 'rp', previousResourceId: 'res-z', previousAt: 7 } });
    const res = await handler(ev, {}, nullInjection());
    expect(res.status).toBe('ALREADY_CONFIRMED');
    expect(routing.revertRoundRobin).toHaveBeenCalledWith({ tenantId: 'AUS123957', routingPolicyId: 'rp', previousResourceId: 'res-z', previousAt: 7 });
  });

  it('transient 401 then a NON-auth retry error → rethrow (not degrade)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent
      .mockRejectedValueOnce({ code: 401 })            // transient
      .mockRejectedValueOnce(new Error('calendar 500')); // retry → non-auth error
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    expect(bookingStore.writeDegradedMarker).not.toHaveBeenCalled(); // NOT a degrade
    expect(oauth.clearCacheEntry).toHaveBeenCalledTimes(1);
  });

  it('safeRevertRR swallows a revert failure (still COMMIT_FAILED, no throw)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.writeBooking.mockRejectedValue(new Error('DDB throttle'));
    routing.revertRoundRobin.mockRejectedValue(new Error('revert failed'));
    const ev = baseEvent({ tie_breaker: 'round_robin', round_robin_cursor: { routingPolicyId: 'rp', previousResourceId: 'res-z', previousAt: 7 } });
    const res = await handler(ev, {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    expect(routing.revertRoundRobin).toHaveBeenCalled();
  });

  it('writeDegradedMarker failure is swallowed (still reoffers all_degraded)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockRejectedValue({ response: { data: { error: 'invalid_grant' } } });
    bookingStore.writeDegradedMarker.mockRejectedValue(new Error('DDB down'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res).toMatchObject({ status: 'SLOT_UNAVAILABLE', reason: 'all_candidates_degraded' });
  });

  it('SNS degrade alert is published (and never logs the raw coordinator email)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockRejectedValue({ response: { data: { error: 'invalid_grant' } } });
    await handler(baseEvent(), {}, nullInjection());
    const degradeAlert = snsMock.commandCalls(PublishCommand).find((c) => /degraded/i.test(c.args[0].input.Subject));
    expect(degradeAlert).toBeDefined();
    expect(degradeAlert.args[0].input.Message).not.toContain('maya@org.org'); // hashed, not raw
  });

  it('calendar-event rollback failure is swallowed (still COMMIT_FAILED)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    calendarEvents.insertEvent.mockResolvedValue(EVENT);
    bookingStore.writeBooking.mockRejectedValue(new Error('DDB throttle'));
    calendarEvents.deleteEvent.mockRejectedValue(new Error('calendar delete 500')); // rollback fails
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('COMMIT_FAILED');
    expect(calendarEvents.deleteEvent).toHaveBeenCalled();
  });

  it('Zoom-meeting rollback failure is swallowed (orphan-meeting alert)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.writeBooking.mockRejectedValue(new Error('DDB throttle'));
    require('./zoom-client').deleteMeeting.mockRejectedValue(new Error('zoom delete 500'));
    const res = await handler(baseEvent({ conference_type: 'zoom' }), {}, zoomProvider('z-1'));
    expect(res.status).toBe('COMMIT_FAILED');
    expect(require('./zoom-client').deleteMeeting).toHaveBeenCalled();
  });

  it('setLockTtl failure is best-effort (commit still succeeds)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.setLockTtl.mockRejectedValue(new Error('DDB down'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('BOOKED');
  });

  it('readLock failure is swallowed (no prior conference recovered)', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    bookingStore.readLock.mockRejectedValue(new Error('DDB down'));
    const res = await handler(baseEvent(), {}, nullInjection());
    expect(res.status).toBe('BOOKED');
  });

  it('SLA-exceeded → publishes the SLA alert', async () => {
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-a', lockKey: 'lk1' });
    const realNow = Date.now;
    const t0 = realNow();
    // handler start → t0; the elapsed check after the email → t0 + 61s
    jest.spyOn(Date, 'now').mockReturnValueOnce(t0).mockReturnValue(t0 + 61 * 1000);
    try {
      const res = await handler(baseEvent(), {}, nullInjection());
      expect(res.status).toBe('BOOKED'); // booking still valid; SLA is an alert, not a failure
      const slaAlert = snsMock.commandCalls(PublishCommand).find((c) => /SLA/i.test(c.args[0].input.Subject));
      expect(slaAlert).toBeDefined();
    } finally {
      Date.now.mockRestore();
    }
  });
});

// ─── internal helpers + edge branches ───────────────────────────────────────────────────

describe('internal helpers', () => {
  const t = require('./index')._test;

  it('resolveCoordinatorId falls back to resourceId when no email mapped', () => {
    expect(t.resolveCoordinatorId('res-x', { 'res-y': 'y@x' })).toBe('res-x');
    expect(t.resolveCoordinatorId('res-y', { 'res-y': 'y@x' })).toBe('y@x');
  });

  it('intervalsOverlap detects overlap and clears free windows', () => {
    expect(t.intervalsOverlap([{ start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z' }], '2026-06-03T18:15:00Z', '2026-06-03T18:45:00Z')).toBe(true);
    expect(t.intervalsOverlap([{ start: '2026-06-03T19:00:00Z', end: '2026-06-03T19:30:00Z' }], '2026-06-03T18:00:00Z', '2026-06-03T18:30:00Z')).toBe(false);
    expect(t.intervalsOverlap([], 'a', 'b')).toBe(false);
  });

  it('formatWhen falls back to the raw ISO on an invalid timezone', () => {
    expect(t.formatWhen('2026-06-03T18:00:00Z', 'Not/AZone')).toBe('2026-06-03T18:00:00Z');
  });
});

describe('freeBusy re-check excludes a coordinator whose query fails', () => {
  it('drops the failing coordinator but proceeds with survivors', async () => {
    availability.getBusyIntervals
      .mockRejectedValueOnce(new Error('freebusy 500')) // res-a fails → excluded
      .mockResolvedValueOnce(FREE); // res-b free
    pool.lockSlot.mockResolvedValue({ status: 'LOCKED', resourceId: 'res-b', lockKey: 'lk-b' });
    const ev = baseEvent({ pool_size: 2, slot: { start: '2026-06-03T18:00:00.000Z', end: '2026-06-03T18:30:00.000Z', candidateResourceIds: ['res-a', 'res-b'] } });
    const res = await handler(ev, {}, nullInjection());
    expect(res.status).toBe('BOOKED');
    expect(pool.recordFreeBusyFailure).toHaveBeenCalledWith('AUS123957', 'res-a');
    // only res-b reached the lock
    expect(pool.lockSlot.mock.calls[0][0].candidateResourceIds).toEqual(['res-b']);
  });
});
