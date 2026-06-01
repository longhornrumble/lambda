'use strict';

const { reassign, cancel, leave, remediate, OUTCOMES } = require('./remediation');

function booking(over = {}) {
  return {
    tenantId: over.tenantId ?? 'TEN1',
    bookingId: over.bookingId ?? 'booking#abc',
    status: 'booked',
    resourceId: over.resourceId ?? 'res-maya',
    coordinatorEmail: over.coordinatorEmail ?? 'maya@org.com',
    externalEventId: 'externalEventId' in over ? over.externalEventId : 'evt-1',
    startAt: '2026-06-01T15:00:00Z',
    endAt: '2026-06-01T15:30:00Z',
    appointmentTypeId: 'apt-1',
  };
}

function makeDeps(over = {}) {
  return {
    resolveAlternate: over.resolveAlternate || jest.fn(),
    getOAuthClient: over.getOAuthClient || jest.fn().mockResolvedValue('AUTH'),
    calendarOps: over.calendarOps || {
      transferEvent: jest.fn().mockResolvedValue({ id: 'evt-1' }),
      deleteEvent: jest.fn().mockResolvedValue(undefined),
    },
    bookingStore: over.bookingStore || {
      reassignBookingResource: jest.fn().mockResolvedValue(undefined),
      isConditionalCheckFailed: jest.fn().mockReturnValue(false),
    },
    now: over.now || (() => 'NOW'),
    log: jest.fn(),
    warn: jest.fn(),
  };
}

describe('reassign (handling a)', () => {
  test('alternate found + event present → transfers event, repoints booking, returns reassigned', async () => {
    const deps = makeDeps({
      resolveAlternate: jest.fn().mockResolvedValue({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' }),
    });
    const b = booking();
    const result = await reassign(b, deps);

    expect(deps.getOAuthClient).toHaveBeenCalledWith({ tenantId: 'TEN1', coordinatorId: 'maya@org.com' });
    expect(deps.calendarOps.transferEvent).toHaveBeenCalledWith('AUTH', {
      eventId: 'evt-1',
      fromCalendarId: 'maya@org.com',
      toCalendarId: 'diego@org.com',
    });
    expect(deps.bookingStore.reassignBookingResource).toHaveBeenCalledWith({
      tenantId: 'TEN1',
      bookingId: 'booking#abc',
      fromResourceId: 'res-maya',
      newResourceId: 'res-diego',
      newCoordinatorEmail: 'diego@org.com',
      mutationAt: 'NOW',
    });
    expect(result).toEqual({
      outcome: OUTCOMES.REASSIGNED,
      newResourceId: 'res-diego',
      newCoordinatorEmail: 'diego@org.com',
    });
  });

  test('no alternate → no_eligible_coordinator, no calendar calls', async () => {
    const deps = makeDeps({ resolveAlternate: jest.fn().mockResolvedValue(null) });
    const result = await reassign(booking(), deps);
    expect(result).toEqual({ outcome: OUTCOMES.NO_ELIGIBLE });
    expect(deps.calendarOps.transferEvent).not.toHaveBeenCalled();
    expect(deps.bookingStore.reassignBookingResource).not.toHaveBeenCalled();
  });

  test('alternate found but booking has no calendar event → no_eligible (so cascade cancels)', async () => {
    const deps = makeDeps({
      resolveAlternate: jest.fn().mockResolvedValue({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' }),
    });
    const result = await reassign(booking({ externalEventId: null }), deps);
    expect(result).toEqual({ outcome: OUTCOMES.NO_ELIGIBLE });
    expect(deps.warn).toHaveBeenCalledWith('reassign_no_external_event', expect.any(Object));
    expect(deps.calendarOps.transferEvent).not.toHaveBeenCalled();
  });

  test('conditional-check-failed on booking repoint is swallowed (calendar move is source of truth)', async () => {
    const condErr = new Error('cond');
    const deps = makeDeps({
      resolveAlternate: jest.fn().mockResolvedValue({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' }),
      bookingStore: {
        reassignBookingResource: jest.fn().mockRejectedValue(condErr),
        isConditionalCheckFailed: jest.fn().mockReturnValue(true),
      },
    });
    const result = await reassign(booking(), deps);
    expect(result.outcome).toBe(OUTCOMES.REASSIGNED);
    expect(deps.warn).toHaveBeenCalledWith('reassign_booking_row_already_changed', expect.any(Object));
  });

  test('a non-conditional repoint error propagates', async () => {
    const boom = new Error('ddb down');
    const deps = makeDeps({
      resolveAlternate: jest.fn().mockResolvedValue({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' }),
      bookingStore: {
        reassignBookingResource: jest.fn().mockRejectedValue(boom),
        isConditionalCheckFailed: jest.fn().mockReturnValue(false),
      },
    });
    await expect(reassign(booking(), deps)).rejects.toThrow('ddb down');
  });
});

describe('cancel (handling b)', () => {
  test('deletes the calendar event and does NOT write Booking.status (§14.2 owns it)', async () => {
    const deps = makeDeps();
    const result = await cancel(booking(), deps);
    expect(deps.calendarOps.deleteEvent).toHaveBeenCalledWith('AUTH', {
      eventId: 'evt-1',
      calendarId: 'maya@org.com',
    });
    expect(deps.bookingStore.reassignBookingResource).not.toHaveBeenCalled();
    expect(result).toEqual({ outcome: OUTCOMES.CANCELED });
  });

  test('no calendar event → canceled-equivalent with note, no delete call', async () => {
    const deps = makeDeps();
    const result = await cancel(booking({ externalEventId: null }), deps);
    expect(deps.calendarOps.deleteEvent).not.toHaveBeenCalled();
    expect(result).toEqual({ outcome: OUTCOMES.CANCELED, note: 'no_calendar_event' });
    expect(deps.warn).toHaveBeenCalledWith('cancel_no_external_event', expect.any(Object));
  });
});

describe('leave (handling c)', () => {
  test('is a no-op returning left', async () => {
    const deps = makeDeps();
    const result = await leave(booking(), deps);
    expect(result).toEqual({ outcome: OUTCOMES.LEFT, bookingId: 'booking#abc' });
    expect(deps.calendarOps.deleteEvent).not.toHaveBeenCalled();
    expect(deps.calendarOps.transferEvent).not.toHaveBeenCalled();
  });
});

describe('remediate dispatch + default cascade', () => {
  test('choice=reassign routes to reassign', async () => {
    const deps = makeDeps({
      resolveAlternate: jest.fn().mockResolvedValue({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' }),
    });
    const result = await remediate(booking(), 'reassign', deps);
    expect(result.outcome).toBe(OUTCOMES.REASSIGNED);
  });

  test('choice=cancel routes to cancel', async () => {
    const deps = makeDeps();
    const result = await remediate(booking(), 'cancel', deps);
    expect(result.outcome).toBe(OUTCOMES.CANCELED);
    expect(deps.calendarOps.deleteEvent).toHaveBeenCalled();
  });

  test('choice=leave routes to leave', async () => {
    const deps = makeDeps();
    const result = await remediate(booking(), 'leave', deps);
    expect(result.outcome).toBe(OUTCOMES.LEFT);
  });

  test('no choice → cascade reassigns when an alternate exists (does NOT cancel)', async () => {
    const deps = makeDeps({
      resolveAlternate: jest.fn().mockResolvedValue({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' }),
    });
    const result = await remediate(booking(), null, deps);
    expect(result.outcome).toBe(OUTCOMES.REASSIGNED);
    expect(deps.calendarOps.deleteEvent).not.toHaveBeenCalled();
  });

  test('no choice → cascade falls back to cancel when no eligible coordinator', async () => {
    const deps = makeDeps({ resolveAlternate: jest.fn().mockResolvedValue(null) });
    const result = await remediate(booking(), undefined, deps);
    expect(result.outcome).toBe(OUTCOMES.CANCELED);
    expect(result.cascadedFrom).toBe('reassign');
    expect(deps.calendarOps.deleteEvent).toHaveBeenCalled();
  });

  test("explicit 'cascade' behaves like the default cascade", async () => {
    const deps = makeDeps({ resolveAlternate: jest.fn().mockResolvedValue(null) });
    const result = await remediate(booking(), 'cascade', deps);
    expect(result.outcome).toBe(OUTCOMES.CANCELED);
    expect(result.cascadedFrom).toBe('reassign');
  });

  test('unknown choice throws', async () => {
    await expect(remediate(booking(), 'bogus', makeDeps())).rejects.toThrow('unknown remediation choice: bogus');
  });
});
