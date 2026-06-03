'use strict';

/**
 * Unit tests for cancel.js (WS-D7, FROZEN_CONTRACTS §B9).
 *
 * Covers the locked done-bar:
 *   • delete ✓ → outcome 'deleted' AND status NOT mutated by this module
 *     (the §14.2 listener owns the flip);
 *   • delete throws (API-unreachable) → booking.pending_calendar_sync = true +
 *     outcome 'pending_calendar_sync', status still NOT mutated;
 *   • the module performs NO token validation / NO jti write and persists NOTHING
 *     itself (deps.ddb is never touched) — it returns the updated booking;
 *   • forward-compatible booking reads (snake_case + camelCase external event id);
 *   • caller-contract guards (missing booking / facade / event id throw);
 *   • optional logger (works absent; logs booking_id + outcome only).
 */

const { executeCancel } = require('../cancel');

const baseBooking = () => ({
  booking_id: 'bk-1',
  tenantId: 'AUS123957',
  status: 'booked',
  external_event_id: 'gcal-evt-123',
  coordinator_email: 'coord@example.com',
  attendee_email: 'vol@example.com',
});

const quietLogger = () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() });

describe('executeCancel — success path', () => {
  test('delete ✓ → outcome "deleted" and the facade was called with (calendarId, eventId)', async () => {
    const booking = baseBooking();
    const deleteEvent = jest.fn().mockResolvedValue(undefined);
    const deps = { calendar: { deleteEvent }, ddb: {}, logger: quietLogger() };

    const result = await executeCancel({ booking, deps });

    expect(result.outcome).toBe('deleted');
    expect(deleteEvent).toHaveBeenCalledTimes(1);
    // §B13 two-arg facade: calendarId (coordinator_email) + eventId (external_event_id).
    expect(deleteEvent).toHaveBeenCalledWith('coord@example.com', 'gcal-evt-123');
  });

  test('does NOT mutate Booking.status (the listener owns the flip)', async () => {
    const booking = baseBooking();
    const deps = {
      calendar: { deleteEvent: jest.fn().mockResolvedValue(undefined) },
      logger: quietLogger(),
    };

    const result = await executeCancel({ booking, deps });

    expect(result.booking.status).toBe('booked'); // unchanged
    expect(result.booking).not.toHaveProperty('cancel_reason');
    expect(result.booking).not.toHaveProperty('canceled_at');
    expect(result.booking.pending_calendar_sync).toBeUndefined();
  });

  test('logs booking_id + outcome on success (PII-safe — no attendee email)', async () => {
    const booking = baseBooking();
    const logger = quietLogger();
    const deps = { calendar: { deleteEvent: jest.fn().mockResolvedValue(undefined) }, logger };

    await executeCancel({ booking, deps });

    expect(logger.info).toHaveBeenCalledTimes(1);
    const [, meta] = logger.info.mock.calls[0];
    expect(meta).toEqual({ booking_id: 'bk-1', outcome: 'deleted' });
    // The attendee email must never appear in any log argument.
    const logged = JSON.stringify(logger.info.mock.calls);
    expect(logged).not.toContain('vol@example.com');
  });
});

describe('executeCancel — API-unreachable path', () => {
  test('delete throws → pending_calendar_sync=true + outcome "pending_calendar_sync"', async () => {
    const booking = baseBooking();
    const deleteEvent = jest.fn().mockRejectedValue(new Error('ECONNRESET'));
    const deps = { calendar: { deleteEvent }, logger: quietLogger() };

    const result = await executeCancel({ booking, deps });

    expect(result.outcome).toBe('pending_calendar_sync');
    expect(result.booking.pending_calendar_sync).toBe(true);
  });

  test('on the failure path status is STILL not mutated (no double-write with the listener)', async () => {
    const booking = baseBooking();
    const deps = {
      calendar: { deleteEvent: jest.fn().mockRejectedValue(new Error('503')) },
      logger: quietLogger(),
    };

    const result = await executeCancel({ booking, deps });

    expect(result.booking.status).toBe('booked');
    expect(result.booking).not.toHaveProperty('cancel_reason');
  });

  test('does not mutate the caller-supplied booking object in place', async () => {
    const booking = baseBooking();
    const deps = {
      calendar: { deleteEvent: jest.fn().mockRejectedValue(new Error('timeout')) },
      logger: quietLogger(),
    };

    await executeCancel({ booking, deps });

    // The returned booking carries the flag; the original input does not.
    expect(booking.pending_calendar_sync).toBeUndefined();
  });

  test('logs booking_id + outcome + error message on failure (PII-safe)', async () => {
    const booking = baseBooking();
    const logger = quietLogger();
    const deps = {
      calendar: { deleteEvent: jest.fn().mockRejectedValue(new Error('ENOTFOUND')) },
      logger,
    };

    await executeCancel({ booking, deps });

    expect(logger.warn).toHaveBeenCalledTimes(1);
    const [, meta] = logger.warn.mock.calls[0];
    expect(meta).toMatchObject({ booking_id: 'bk-1', outcome: 'pending_calendar_sync', error: 'ENOTFOUND' });
    const logged = JSON.stringify(logger.warn.mock.calls);
    expect(logged).not.toContain('vol@example.com');
  });
});

describe('executeCancel — persists nothing / no token work', () => {
  test('never touches deps.ddb (the caller persists; this module does not)', async () => {
    const booking = baseBooking();
    const ddbHandler = jest.fn();
    const ddb = new Proxy({}, { get: () => ddbHandler });
    const deps = {
      calendar: { deleteEvent: jest.fn().mockResolvedValue(undefined) },
      ddb,
      logger: quietLogger(),
    };

    await executeCancel({ booking, deps });

    expect(ddbHandler).not.toHaveBeenCalled();
  });

  test('works without deps.ddb at all (it is unused)', async () => {
    const deps = { calendar: { deleteEvent: jest.fn().mockResolvedValue(undefined) } };
    const result = await executeCancel({ booking: baseBooking(), deps });
    expect(result.outcome).toBe('deleted');
  });
});

describe('executeCancel — forward-compatible booking reads (schema discipline)', () => {
  test('accepts a camelCase externalEventId + coordinatorEmail', async () => {
    const booking = { booking_id: 'bk-2', externalEventId: 'gcal-evt-camel', coordinatorEmail: 'maya@org.example', status: 'booked' };
    const deleteEvent = jest.fn().mockResolvedValue(undefined);
    const result = await executeCancel({ booking, deps: { calendar: { deleteEvent } } });
    expect(result.outcome).toBe('deleted');
    expect(deleteEvent).toHaveBeenCalledWith('maya@org.example', 'gcal-evt-camel');
  });

  test('reads camelCase bookingId for the log', async () => {
    const booking = { bookingId: 'bk-camel', external_event_id: 'gcal-evt-9', coordinator_email: 'coord@example.com', status: 'booked' };
    const logger = quietLogger();
    await executeCancel({ booking, deps: { calendar: { deleteEvent: jest.fn().mockResolvedValue() }, logger } });
    const [, meta] = logger.info.mock.calls[0];
    expect(meta.booking_id).toBe('bk-camel');
  });
});

describe('executeCancel — caller-contract guards (throw, not an outcome)', () => {
  test('missing booking throws', async () => {
    await expect(
      executeCancel({ deps: { calendar: { deleteEvent: jest.fn() } } })
    ).rejects.toThrow(/booking is required/);
  });

  test('called with no args throws', async () => {
    await expect(executeCancel()).rejects.toThrow(/booking is required/);
  });

  test('missing deps.calendar.deleteEvent throws', async () => {
    await expect(executeCancel({ booking: baseBooking(), deps: {} })).rejects.toThrow(
      /deps\.calendar\.deleteEvent is required/
    );
  });

  test('calendar without a deleteEvent function throws', async () => {
    await expect(
      executeCancel({ booking: baseBooking(), deps: { calendar: { deleteEvent: 'nope' } } })
    ).rejects.toThrow(/deps\.calendar\.deleteEvent is required/);
  });

  test('booking without an external event id throws (not a pending outcome)', async () => {
    const booking = { booking_id: 'bk-3', status: 'booked' };
    await expect(
      executeCancel({ booking, deps: { calendar: { deleteEvent: jest.fn() } } })
    ).rejects.toThrow(/external_event_id is required/);
  });

  test('booking without a coordinator (calendar id) throws (§B13 two-arg needs calendarId)', async () => {
    const booking = { booking_id: 'bk-4', external_event_id: 'gcal-evt-7', status: 'booked' };
    await expect(
      executeCancel({ booking, deps: { calendar: { deleteEvent: jest.fn() } } })
    ).rejects.toThrow(/coordinator_email is required/);
  });
});

describe('executeCancel — optional logger', () => {
  test('succeeds when no logger is provided', async () => {
    const result = await executeCancel({
      booking: baseBooking(),
      deps: { calendar: { deleteEvent: jest.fn().mockResolvedValue(undefined) } },
    });
    expect(result.outcome).toBe('deleted');
  });

  test('handles the failure path when no logger is provided', async () => {
    const result = await executeCancel({
      booking: baseBooking(),
      deps: { calendar: { deleteEvent: jest.fn().mockRejectedValue(new Error('x')) } },
    });
    expect(result.outcome).toBe('pending_calendar_sync');
  });

  test('tolerates a logger missing the info/warn methods', async () => {
    const result = await executeCancel({
      booking: baseBooking(),
      deps: { calendar: { deleteEvent: jest.fn().mockResolvedValue(undefined) }, logger: {} },
    });
    expect(result.outcome).toBe('deleted');
  });
});
