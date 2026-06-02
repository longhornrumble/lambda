'use strict';

/**
 * Unit tests for reschedule.js (WS-D6) — canonical §9.4, plan D6, frozen §B9/§B6.
 *
 * Covers: the four §D6 outcomes as executeReschedule paths (i success / ii
 * pending_calendar_sync / iii canceled_insert_failed / iv failed) incl. the exact
 * state-flag transitions; Zoom join-URL preservation via §B6 createConference
 * read-before-write; Meet fresh-link no-op + Null synthetic; the no-persist /
 * no-token / no-jti guarantee; input-precondition throws; and the pure helpers
 * (classifyOutcome / confToEventConference). All I/O is injected — no AWS, no Google.
 */

const {
  executeReschedule,
  classifyOutcome,
  confToEventConference,
  OUTCOME,
} = require('../reschedule');

const OLD_EVENT = 'evt-old-abc';
const NEW_EVENT = 'evt-new-xyz';

// A Booking row as persisted (snake_case). Zoom by default.
function booking(overrides = {}) {
  return {
    tenant_id: 'AUS123957',
    booking_id: 'booking#deadbeef',
    status: 'booked',
    external_event_id: OLD_EVENT,
    coordinator_email: 'maya@org.example',
    resource_id: 'maya@org.example',
    conference_provider: 'zoom',
    conference_id: 'zoom-meeting-555',
    channel_details: 'https://zoom.us/j/PRESERVED',
    appointment_type_name: 'Mentoring Intro',
    appointment_type_id: 'apt-1',
    attendee_email: 'sam@volunteer.example',
    attendee_name: 'Sam Patel',
    timezone: 'America/Chicago',
    ...overrides,
  };
}

const NEW_SLOT = {
  slotId: 's-2',
  start: '2026-06-10T14:00:00-05:00',
  end: '2026-06-10T14:30:00-05:00',
  label: 'Wed, Jun 10 · 2:00 PM',
};

const FIXED_NOW = '2026-06-09T00:00:00.000Z';

// Zoom conference result that PRESERVES the existing join URL (read-before-write reuse).
const ZOOM_PRESERVED = {
  provider: 'zoom',
  conferenceId: 'zoom-meeting-555',
  joinUrl: 'https://zoom.us/j/PRESERVED',
  deferToCalendarInsert: false,
};

function makeDeps(over = {}) {
  return {
    calendar: {
      buildEventBody: jest.fn((p) => ({ requestBody: true, ...p })),
      insertEvent: jest.fn(async () => ({ id: NEW_EVENT, conferenceData: { conferenceId: 'zoom-meeting-555' } })),
      deleteEvent: jest.fn(async () => undefined),
      extractMeetJoinUrl: jest.fn(() => 'https://meet.google.com/fresh-link'),
      ...(over.calendar || {}),
    },
    conference: {
      createConference: jest.fn(async () => over.conf || ZOOM_PRESERVED),
      ...(over.conference || {}),
    },
    ddb: { send: jest.fn() },
    alertAdmin: over.alertAdmin === undefined ? jest.fn(async () => undefined) : over.alertAdmin,
    logger: over.logger === undefined ? { info: jest.fn(), warn: jest.fn(), error: jest.fn() } : over.logger,
    now: () => FIXED_NOW,
  };
}

// ─── the four §D6 outcomes ──────────────────────────────────────────────────────────────

describe('executeReschedule — outcome (i) success (insert ✓ + delete ✓)', () => {
  it('points the booking at the new event, stamps the move, fires no admin alert', async () => {
    const deps = makeDeps();
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });

    expect(res.outcome).toBe('success');
    expect(res.newEventId).toBe(NEW_EVENT);
    expect(res.oldEventId).toBe(OLD_EVENT);
    expect(res.booking).toBe(b); // same object — caller persists

    expect(b.external_event_id).toBe(NEW_EVENT);
    expect(b.start_at).toBe(NEW_SLOT.start);
    expect(b.end_at).toBe(NEW_SLOT.end);
    expect(b.last_calendar_mutation_at).toBe(FIXED_NOW);
    expect(deps.calendar.buildEventBody).toHaveBeenCalledWith(
      expect.objectContaining({ bookingId: 'booking#deadbeef', start: NEW_SLOT.start, end: NEW_SLOT.end })
    );
    expect(b.status).toBe('booked'); // unchanged on a clean move
    expect(b.pending_calendar_sync).toBeFalsy();
    expect(b.rescheduled_old_event_id).toBeUndefined();

    expect(deps.calendar.insertEvent).toHaveBeenCalledTimes(1);
    expect(deps.calendar.deleteEvent).toHaveBeenCalledWith('maya@org.example', OLD_EVENT);
    expect(deps.alertAdmin).not.toHaveBeenCalled();
  });

  it('insert is attempted BEFORE delete (locked §D6 ordering)', async () => {
    const order = [];
    const deps = makeDeps({
      calendar: {
        buildEventBody: jest.fn((p) => p),
        extractMeetJoinUrl: jest.fn(() => null),
        insertEvent: jest.fn(async () => { order.push('insert'); return { id: NEW_EVENT }; }),
        deleteEvent: jest.fn(async () => { order.push('delete'); }),
      },
    });
    await executeReschedule({ booking: booking(), newSlot: NEW_SLOT, deps });
    expect(order).toEqual(['insert', 'delete']);
  });

  it('clears stale pending_calendar_sync + rescheduled_old_event_id from a prior attempt', async () => {
    const deps = makeDeps();
    const b = booking({ pending_calendar_sync: true, rescheduled_old_event_id: 'evt-stale' });
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(b.pending_calendar_sync).toBe(false);
    expect(b.rescheduled_old_event_id).toBeUndefined();
  });

  it('stamps a real ISO timestamp when no deps.now clock is injected', async () => {
    const deps = makeDeps();
    delete deps.now; // exercise the default wall-clock
    const b = booking();
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(b.last_calendar_mutation_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });
});

describe('executeReschedule — outcome (ii) pending_calendar_sync (insert ✓ + delete ✗)', () => {
  it('keeps the new event, flags pending sync, stores the orphaned old event id', async () => {
    const deps = makeDeps({
      calendar: {
        buildEventBody: jest.fn((p) => p),
        extractMeetJoinUrl: jest.fn(() => null),
        insertEvent: jest.fn(async () => ({ id: NEW_EVENT, conferenceData: { conferenceId: 'zoom-meeting-555' } })),
        deleteEvent: jest.fn(async () => { throw new Error('Google 503'); }),
      },
    });
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });

    expect(res.outcome).toBe('pending_calendar_sync');
    expect(b.external_event_id).toBe(NEW_EVENT); // new invite is live
    expect(b.start_at).toBe(NEW_SLOT.start);
    expect(b.pending_calendar_sync).toBe(true);
    expect(b.rescheduled_old_event_id).toBe(OLD_EVENT); // reconciler retries the delete
    expect(b.status).toBe('booked');
    expect(deps.alertAdmin).not.toHaveBeenCalled();
  });
});

describe('executeReschedule — outcome (iii) canceled_insert_failed (insert ✗ + delete ✓)', () => {
  it('cancels the booking and fires alertAdmin; leaves the old event id untouched', async () => {
    const alertAdmin = jest.fn(async () => undefined);
    const deps = makeDeps({
      alertAdmin,
      calendar: {
        buildEventBody: jest.fn((p) => p),
        extractMeetJoinUrl: jest.fn(() => null),
        insertEvent: jest.fn(async () => { throw new Error('Google 500 on insert'); }),
        deleteEvent: jest.fn(async () => undefined),
      },
    });
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });

    expect(res.outcome).toBe('canceled_insert_failed');
    expect(b.status).toBe('canceled');
    expect(b.external_event_id).toBe(OLD_EVENT); // not pointed at a non-existent new event
    expect(b.last_calendar_mutation_at).toBe(FIXED_NOW);
    expect(b.pending_calendar_sync).toBeUndefined();
    expect(alertAdmin).toHaveBeenCalledTimes(1);
    expect(alertAdmin).toHaveBeenCalledWith({
      kind: 'reschedule_insert_failed',
      tenantId: 'AUS123957',
      booking_id: 'booking#deadbeef',
      old_event_id: OLD_EVENT,
    });
  });

  it('an insert that returns no event id counts as insert ✗ (→ iii when delete ✓)', async () => {
    const deps = makeDeps({
      calendar: {
        buildEventBody: jest.fn((p) => p),
        extractMeetJoinUrl: jest.fn(() => null),
        insertEvent: jest.fn(async () => ({})), // no .id
        deleteEvent: jest.fn(async () => undefined),
      },
    });
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(res.outcome).toBe('canceled_insert_failed');
    expect(b.status).toBe('canceled');
  });

  it('still cancels (and logs) when no alertAdmin is injected — does not throw', async () => {
    const logger = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
    const deps = makeDeps({
      alertAdmin: null,
      logger,
      calendar: {
        buildEventBody: jest.fn((p) => p),
        extractMeetJoinUrl: jest.fn(() => null),
        insertEvent: jest.fn(async () => { throw new Error('insert down'); }),
        deleteEvent: jest.fn(async () => undefined),
      },
    });
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(res.outcome).toBe('canceled_insert_failed');
    expect(b.status).toBe('canceled');
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('reschedule_alert_admin_missing')
    );
  });
});

describe('executeReschedule — outcome (iv) failed (insert ✗ + delete ✗)', () => {
  it('makes no state change to the booking', async () => {
    const deps = makeDeps({
      calendar: {
        buildEventBody: jest.fn((p) => p),
        extractMeetJoinUrl: jest.fn(() => null),
        insertEvent: jest.fn(async () => { throw new Error('insert down'); }),
        deleteEvent: jest.fn(async () => { throw new Error('delete down'); }),
      },
    });
    const b = booking();
    const before = JSON.stringify(b);
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });

    expect(res.outcome).toBe('failed');
    expect(JSON.stringify(b)).toBe(before); // untouched
    expect(deps.alertAdmin).not.toHaveBeenCalled();
  });
});

// ─── conference: Zoom preserve / Meet no-op / Null synthetic ─────────────────────────────

describe('executeReschedule — conference join-URL handling (§B6)', () => {
  it('Zoom: preserves the join URL by reusing the existing meeting (read-before-write)', async () => {
    const deps = makeDeps({ conf: ZOOM_PRESERVED });
    const b = booking();
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });

    // createConference asked to REUSE the existing meeting → same join URL back.
    expect(deps.conference.createConference).toHaveBeenCalledWith(
      expect.objectContaining({
        existingConferenceId: 'zoom-meeting-555',
        start: NEW_SLOT.start,
        end: NEW_SLOT.end,
      })
    );
    // the preserved URL flows onto the new event body AND onto the persisted booking.
    expect(deps.calendar.buildEventBody).toHaveBeenCalledWith(
      expect.objectContaining({
        conference: { provider: 'zoom', joinUrl: 'https://zoom.us/j/PRESERVED', conferenceId: 'zoom-meeting-555' },
      })
    );
    expect(b.channel_details).toBe('https://zoom.us/j/PRESERVED'); // unchanged → preserved
    expect(b.conference_id).toBe('zoom-meeting-555');
  });

  it('Meet: mints a fresh link on the new insert (preservation is a no-op)', async () => {
    const MEET_CONF = {
      provider: 'google_meet',
      conferenceId: null,
      joinUrl: null,
      deferToCalendarInsert: true,
      calendarCreateRequest: { requestId: 'meet-req-1', conferenceSolutionKey: { type: 'hangoutsMeet' } },
    };
    const deps = makeDeps({
      conf: MEET_CONF,
      calendar: {
        buildEventBody: jest.fn((p) => p),
        insertEvent: jest.fn(async () => ({ id: NEW_EVENT, conferenceData: { conferenceId: 'meet-conf-9' } })),
        deleteEvent: jest.fn(async () => undefined),
        extractMeetJoinUrl: jest.fn(() => 'https://meet.google.com/fresh-link'),
      },
    });
    const b = booking({ conference_provider: 'google_meet', conference_id: 'old-meet', channel_details: 'https://meet.google.com/old-link' });
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });

    // body carries the createRequest (deferred), not a joinUrl.
    expect(deps.calendar.buildEventBody).toHaveBeenCalledWith(
      expect.objectContaining({
        conference: { provider: 'google_meet', calendarCreateRequest: MEET_CONF.calendarCreateRequest },
      })
    );
    expect(deps.calendar.extractMeetJoinUrl).toHaveBeenCalled();
    expect(b.channel_details).toBe('https://meet.google.com/fresh-link'); // fresh, not preserved
    expect(b.conference_id).toBe('meet-conf-9');
  });

  it('Meet with no resolvable conference data → conference_id null is not written', async () => {
    const MEET_CONF = { provider: 'google_meet', deferToCalendarInsert: true, calendarCreateRequest: { requestId: 'r' } };
    const deps = makeDeps({
      conf: MEET_CONF,
      calendar: {
        buildEventBody: jest.fn((p) => p),
        insertEvent: jest.fn(async () => ({ id: NEW_EVENT })), // no conferenceData
        deleteEvent: jest.fn(async () => undefined),
        extractMeetJoinUrl: jest.fn(() => null), // no link resolved
      },
    });
    const b = booking({ conference_provider: 'google_meet' });
    delete b.conference_id;
    delete b.channel_details;
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(b.conference_id).toBeUndefined(); // null not persisted
    expect(b.channel_details).toBeUndefined();
  });

  it('Null: carries the synthetic join URL', async () => {
    const NULL_CONF = { provider: 'null', conferenceId: 'null-conf-x', joinUrl: 'https://conference.invalid/x', deferToCalendarInsert: false };
    const deps = makeDeps({ conf: NULL_CONF });
    const b = booking({ conference_provider: 'null' });
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(b.channel_details).toBe('https://conference.invalid/x');
    expect(b.conference_id).toBe('null-conf-x');
  });

  it('a conference-provider failure before insert → insert ✗ (delete ✓ ⇒ iii)', async () => {
    const deps = makeDeps({
      conference: { createConference: jest.fn(async () => { throw new Error('zoom secret missing'); }) },
      calendar: {
        buildEventBody: jest.fn((p) => p),
        insertEvent: jest.fn(async () => ({ id: NEW_EVENT })),
        deleteEvent: jest.fn(async () => undefined),
        extractMeetJoinUrl: jest.fn(() => null),
      },
    });
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(res.outcome).toBe('canceled_insert_failed');
    expect(deps.calendar.insertEvent).not.toHaveBeenCalled(); // never reached the insert
  });
});

// ─── contract guarantees: no token / no jti / no persistence ─────────────────────────────

describe('executeReschedule — performs no token validation, no jti write, no persistence', () => {
  it('never touches deps.ddb and returns the booking for the caller to persist', async () => {
    const deps = makeDeps();
    const b = booking();
    const res = await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(deps.ddb.send).not.toHaveBeenCalled();
    expect(res.booking).toBe(b);
  });
});

// ─── forward-compatible reads (camelCase in-memory booking + name split) ──────────────────

describe('executeReschedule — schema discipline (camelCase booking + name handling)', () => {
  it('reads a camelCase booking and splits a full attendee name', async () => {
    const deps = makeDeps();
    const b = {
      tenantId: 'AUS123957',
      bookingId: 'booking#cc',
      status: 'booked',
      externalEventId: OLD_EVENT,
      coordinatorEmail: 'maya@org.example',
      resourceId: 'maya@org.example',
      conferenceId: 'zoom-meeting-555',
      attendeeName: 'Sam Q Patel',
      timeZone: 'America/Chicago',
    };
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(deps.calendar.buildEventBody).toHaveBeenCalledWith(
      expect.objectContaining({ attendeeFirstName: 'Sam', attendeeLastName: 'Q Patel' })
    );
    expect(b.external_event_id).toBe(NEW_EVENT);
  });

  it('uses explicit first/last fields when present and tolerates a missing name', async () => {
    const deps = makeDeps();
    const b = booking({ attendee_name: undefined, attendee_first_name: 'Lee', attendee_last_name: 'Ng' });
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(deps.calendar.buildEventBody).toHaveBeenCalledWith(
      expect.objectContaining({ attendeeFirstName: 'Lee', attendeeLastName: 'Ng' })
    );

    const deps2 = makeDeps();
    const b2 = booking({ attendee_name: undefined });
    await executeReschedule({ booking: b2, newSlot: NEW_SLOT, deps: deps2 });
    expect(deps2.calendar.buildEventBody).toHaveBeenCalledWith(
      expect.objectContaining({ attendeeFirstName: '', attendeeLastName: '' })
    );
  });

  it('falls back to resource_id as the calendar id when coordinator_email is absent', async () => {
    const deps = makeDeps();
    const b = booking({ coordinator_email: undefined, resource_id: 'cal-resource@org.example' });
    await executeReschedule({ booking: b, newSlot: NEW_SLOT, deps });
    expect(deps.calendar.insertEvent).toHaveBeenCalledWith('cal-resource@org.example', expect.anything());
    expect(deps.calendar.deleteEvent).toHaveBeenCalledWith('cal-resource@org.example', OLD_EVENT);
  });
});

// ─── input-precondition throws (caller-contract errors) ──────────────────────────────────

describe('executeReschedule — input preconditions throw', () => {
  it('throws without a booking', async () => {
    await expect(executeReschedule({ newSlot: NEW_SLOT, deps: makeDeps() })).rejects.toThrow(/requires booking/);
  });
  it('throws without newSlot.start/end', async () => {
    await expect(executeReschedule({ booking: booking(), newSlot: { start: 'x' }, deps: makeDeps() }))
      .rejects.toThrow(/newSlot\.start and newSlot\.end/);
  });
  it('throws without deps.calendar/conference', async () => {
    await expect(executeReschedule({ booking: booking(), newSlot: NEW_SLOT, deps: {} }))
      .rejects.toThrow(/deps\.calendar and deps\.conference/);
    await expect(executeReschedule({ booking: booking(), newSlot: NEW_SLOT }))
      .rejects.toThrow(/deps\.calendar and deps\.conference/);
  });
  it('throws when the booking carries no old external_event_id', async () => {
    await expect(executeReschedule({ booking: booking({ external_event_id: undefined }), newSlot: NEW_SLOT, deps: makeDeps() }))
      .rejects.toThrow(/external_event_id/);
  });
  it('throws when no calendar id (coordinator_email/resource_id) is resolvable', async () => {
    await expect(executeReschedule({
      booking: booking({ coordinator_email: undefined, resource_id: undefined }),
      newSlot: NEW_SLOT,
      deps: makeDeps(),
    })).rejects.toThrow(/calendar id/);
  });
});

// ─── logging (default console + warn/error/info sinks + log fallback) ─────────────────────

describe('executeReschedule — PII-redacted logging', () => {
  it('logs only booking_id + outcome (no attendee PII) on the outcome line', async () => {
    const logger = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
    const deps = makeDeps({ logger });
    await executeReschedule({ booking: booking(), newSlot: NEW_SLOT, deps });
    const line = logger.info.mock.calls.find((c) => c[0].includes('reschedule_outcome'))[0];
    expect(line).toContain('booking#deadbeef');
    expect(line).toContain('success');
    expect(line).not.toContain('sam@volunteer.example');
    expect(line).not.toContain('Sam Patel');
  });

  it('uses console when no logger is injected', async () => {
    const spy = jest.spyOn(console, 'info').mockImplementation(() => {});
    const deps = makeDeps({ logger: null });
    await executeReschedule({ booking: booking(), newSlot: NEW_SLOT, deps });
    expect(spy).toHaveBeenCalledWith(expect.stringContaining('reschedule_outcome'));
    spy.mockRestore();
  });

  it('falls back to .log when the logger lacks .info', async () => {
    const logger = { log: jest.fn() };
    const deps = makeDeps({ logger });
    await executeReschedule({ booking: booking(), newSlot: NEW_SLOT, deps });
    expect(logger.log).toHaveBeenCalledWith(expect.stringContaining('reschedule_outcome'));
  });
});

// ─── pure helpers ────────────────────────────────────────────────────────────────────────

describe('classifyOutcome — the four §D6 cells', () => {
  it('maps every (insert, delete) combination', () => {
    expect(classifyOutcome(true, true)).toBe(OUTCOME.SUCCESS);
    expect(classifyOutcome(true, false)).toBe(OUTCOME.PENDING_CALENDAR_SYNC);
    expect(classifyOutcome(false, true)).toBe(OUTCOME.CANCELED_INSERT_FAILED);
    expect(classifyOutcome(false, false)).toBe(OUTCOME.FAILED);
  });
});

describe('confToEventConference', () => {
  it('Meet (deferred) → carries the createRequest, no joinUrl', () => {
    expect(confToEventConference({ provider: 'google_meet', deferToCalendarInsert: true, calendarCreateRequest: { requestId: 'r' } }))
      .toEqual({ provider: 'google_meet', calendarCreateRequest: { requestId: 'r' } });
  });
  it('Zoom/Null (not deferred) → carries joinUrl + conferenceId', () => {
    expect(confToEventConference({ provider: 'zoom', deferToCalendarInsert: false, joinUrl: 'u', conferenceId: 'c' }))
      .toEqual({ provider: 'zoom', joinUrl: 'u', conferenceId: 'c' });
  });
});
