'use strict';

/**
 * Unit tests for Attendance_Disposition_Handler/index.js — the EventBridge action router.
 * Mocks the store + the pure logic modules to assert routing + dep wiring.
 */

jest.mock('./booking-store');
jest.mock('../shared/scheduling/attendance', () => ({ runAttendanceCheck: jest.fn() }));
jest.mock('../shared/scheduling/escalation', () => ({ escalateSilence: jest.fn(), buildWeeklyDigest: jest.fn() }));
jest.mock('../shared/scheduling/zoomOutagePaging', () => ({ pageCoordinatorOnZoomOutage: jest.fn() }));
jest.mock('../shared/scheduling/tokens', () => ({ sign: jest.fn() }));

const store = require('./booking-store');
const { runAttendanceCheck } = require('../shared/scheduling/attendance');
const { escalateSilence, buildWeeklyDigest } = require('../shared/scheduling/escalation');
const { pageCoordinatorOnZoomOutage } = require('../shared/scheduling/zoomOutagePaging');
const { handler, _internal } = require('./index');

const BOOKING = { tenantId: 'T', booking_id: 'bk-1', status: 'booked' };

beforeEach(() => {
  jest.clearAllMocks();
  store.getBooking.mockResolvedValue(BOOKING);
  store.queryPendingAttendance.mockResolvedValue([]);
  runAttendanceCheck.mockResolvedValue({ outcome: 'pending_attendance_set', dispatched: {} });
  escalateSilence.mockResolvedValue({ outcome: 'resent', tier: 't24h', dispatched: {} });
  buildWeeklyDigest.mockResolvedValue({ outcome: 'digest', count: 0, recur: true, dispatched: { email: 'skipped_empty' } });
  pageCoordinatorOnZoomOutage.mockResolvedValue({ outcome: 'paged', dispatched: {} });
});

describe('bad events', () => {
  test('no action → bad_event', async () => {
    expect(await handler({ tenantId: 'T' })).toEqual({ outcome: 'bad_event' });
  });
  test('no tenant → bad_event', async () => {
    expect(await handler({ action: 'attendance_check' })).toEqual({ outcome: 'bad_event' });
  });
  test('per-booking action missing booking_id → bad_event', async () => {
    expect(await handler({ action: 'attendance_check', tenantId: 'T' })).toEqual({ outcome: 'bad_event' });
  });
  test('booking not found → booking_not_found', async () => {
    store.getBooking.mockResolvedValue(null);
    expect(await handler({ action: 'attendance_check', tenantId: 'T', booking_id: 'bk-x' })).toEqual({
      action: 'attendance_check',
      outcome: 'booking_not_found',
    });
  });
  test('unknown per-booking action → unknown_action', async () => {
    const r = await handler({ action: 'frobnicate', tenantId: 'T', booking_id: 'bk-1' });
    expect(r.outcome).toBe('unknown_action');
  });
});

describe('routing', () => {
  test('attendance_check → runAttendanceCheck with wired deps', async () => {
    const r = await handler({ action: 'attendance_check', tenantId: 'T', booking_id: 'bk-1' });
    expect(r.outcome).toBe('pending_attendance_set');
    const arg = runAttendanceCheck.mock.calls[0][0];
    expect(arg.booking).toBe(BOOKING);
    expect(arg.deps.setAttendanceState).toBe(store.setAttendanceState);
    expect(arg.deps.sendEmail).toBe(store.sendEmail);
    expect(typeof arg.deps.signToken).toBe('function');
    expect(typeof arg.deps.now).toBe('number');
  });

  test('escalate → escalateSilence with tier + portal-inbox dep', async () => {
    const r = await handler({ action: 'escalate', tenantId: 'T', booking_id: 'bk-1', tier: 't72h' });
    expect(r.outcome).toBe('resent');
    const arg = escalateSilence.mock.calls[0][0];
    expect(arg.tier).toBe('t72h');
    expect(arg.deps.writePortalInboxAlert).toBe(store.writePortalInboxAlert);
    expect(arg.deps.getAdminEmails).toBe(store.getAdminEmails);
  });

  test('weekly_digest → queryPendingAttendance + buildWeeklyDigest (no booking_id needed)', async () => {
    store.queryPendingAttendance.mockResolvedValue([{ booking_id: 'bk-old' }]);
    const r = await handler({ action: 'weekly_digest', tenantId: 'T' });
    expect(store.queryPendingAttendance).toHaveBeenCalledWith({ tenantId: 'T', now: expect.any(Number) });
    expect(buildWeeklyDigest.mock.calls[0][0].pendingBookings).toEqual([{ booking_id: 'bk-old' }]);
    expect(r.outcome).toBe('digest');
  });

  test('zoom_outage_check → pageCoordinatorOnZoomOutage with probe from event', async () => {
    const probe = jest.fn();
    store.makeZoomReachableProbe.mockReturnValue(probe);
    const r = await handler({ action: 'zoom_outage_check', tenantId: 'T', booking_id: 'bk-1', zoom_unreachable: true });
    expect(store.makeZoomReachableProbe).toHaveBeenCalledWith({ action: 'zoom_outage_check', tenantId: 'T', booking_id: 'bk-1', zoom_unreachable: true });
    const arg = pageCoordinatorOnZoomOutage.mock.calls[0][0];
    expect(arg.deps.checkZoomReachable).toBe(probe);
    expect(typeof arg.deps.selectChannels).toBe('function');
    expect(r.outcome).toBe('paged');
  });
});

describe('_internal', () => {
  test('failClosedSelectChannels → email floor only', async () => {
    await expect(_internal.failClosedSelectChannels()).resolves.toEqual({ email: true, sms: false });
  });
  test('nowSeconds returns an integer epoch', () => {
    expect(Number.isInteger(_internal.nowSeconds())).toBe(true);
  });
});
