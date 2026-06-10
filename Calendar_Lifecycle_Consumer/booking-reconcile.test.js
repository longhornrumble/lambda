'use strict';

/**
 * Unit tests for booking-reconcile.js — per-event reconciliation orchestration + the
 * §5.1 notification narrowing (TODO(Y) stubs). booking-store is mocked.
 */

const mockCancelDelete = jest.fn();
const mockCancelMove = jest.fn();
const mockReassign = jest.fn();
const mockGetNoticeContext = jest.fn();
jest.mock('./booking-store', () => ({
  cancelOnCoordinatorDelete: mockCancelDelete,
  cancelOnCoordinatorMove: mockCancelMove,
  reassignCoordinator: mockReassign,
  getNoticeContext: mockGetNoticeContext,
}));

// gap C (Y) wire: mock the dispatch primitive + the §13.4 token signer.
const mockDispatchVolunteerNotice = jest.fn();
const mockSign = jest.fn();
jest.mock('../shared/scheduling/notify', () => ({ dispatchVolunteerNotice: (...a) => mockDispatchVolunteerNotice(...a) }));
jest.mock('../shared/scheduling/tokens', () => ({ sign: (...a) => mockSign(...a) }));

// Track 1: the reminder scheduler (mocked — no real EventBridge Scheduler / DDB).
const mockDeleteReminders = jest.fn();
jest.mock('../Reminder_Scheduler/scheduler', () => ({ deleteReminders: (...a) => mockDeleteReminders(...a) }));

const reconcile = require('./booking-reconcile');

const FULL_CTX = { attendeeEmail: 'vol@example.org', attendeeName: 'Vol', appointmentTypeId: 'apt-1', startAt: '2026-06-10T15:00:00Z' };

let logSpy;
beforeEach(() => {
  mockCancelDelete.mockReset().mockResolvedValue(true);
  mockCancelMove.mockReset().mockResolvedValue(true);
  mockReassign.mockReset().mockResolvedValue(true);
  mockGetNoticeContext.mockReset().mockResolvedValue(FULL_CTX);
  mockDispatchVolunteerNotice.mockReset().mockResolvedValue({ suppressed: false, dispatched: { email: 'queued' } });
  mockSign.mockReset().mockResolvedValue('tok-resched');
  mockDeleteReminders.mockReset().mockResolvedValue({ deleted: true });
  delete process.env.SCHEDULER_TARGET_ARN; // default: scheduler not wired → reminder teardown skipped
  logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => {
  delete process.env.SCHEDULER_TARGET_ARN;
  jest.restoreAllMocks();
});

function loggedEvents() {
  return logSpy.mock.calls.map((c) => { try { return JSON.parse(c[0]).event; } catch (_) { return ''; } });
}
function loggedBlob() {
  return logSpy.mock.calls.map((c) => String(c[0])).join(' ');
}

describe('reconcileDeleted', () => {
  const env = { tenant_id: 'AUS123957', booking_id: 'b1' };

  it('cancels and dispatches a cancel_notice with a §13.4 reschedule link on a fresh cancel', async () => {
    await reconcile.reconcileDeleted(env);
    expect(mockCancelDelete).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'b1' });
    expect(mockSign).toHaveBeenCalledWith('reschedule', { tenant_id: 'AUS123957', booking_id: 'b1', start_at: FULL_CTX.startAt });
    expect(mockDispatchVolunteerNotice).toHaveBeenCalledTimes(1);
    const arg = mockDispatchVolunteerNotice.mock.calls[0][0];
    expect(arg.kind).toBe('cancel_notice');
    expect(arg.booking.attendee_email).toBe('vol@example.org');
    expect(arg.booking.reschedule_url).toBe('https://schedule.myrecruiter.ai/reschedule?t=tok-resched');
    expect(loggedEvents()).toContain('calendar_deleted_canceled');
  });

  it('does NOT dispatch a notice on an idempotent no-op', async () => {
    mockCancelDelete.mockResolvedValue(false);
    await reconcile.reconcileDeleted(env);
    expect(loggedEvents()).toContain('calendar_deleted_noop');
    expect(mockDispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  it('skips the notice (no token/dispatch) when the booking has no attendee email', async () => {
    mockGetNoticeContext.mockResolvedValue({ ...FULL_CTX, attendeeEmail: null });
    await reconcile.reconcileDeleted(env);
    expect(mockSign).not.toHaveBeenCalled();
    expect(mockDispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  it('a notice-dispatch failure does NOT throw out of reconcile (best-effort)', async () => {
    mockDispatchVolunteerNotice.mockRejectedValue(new Error('send_email invoke failed'));
    await expect(reconcile.reconcileDeleted(env)).resolves.toBeUndefined();
  });

  it('a sign() failure does NOT throw out of reconcile (best-effort)', async () => {
    mockSign.mockRejectedValue(new Error('signing_key_unavailable'));
    await expect(reconcile.reconcileDeleted(env)).resolves.toBeUndefined();
    expect(mockDispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  it('absent booking row (getNoticeContext → null) → skip (no token/dispatch)', async () => {
    mockGetNoticeContext.mockResolvedValue(null);
    await reconcile.reconcileDeleted(env);
    expect(mockSign).not.toHaveBeenCalled();
    expect(mockDispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  it('missing start_at → skip (no token/dispatch)', async () => {
    mockGetNoticeContext.mockResolvedValue({ ...FULL_CTX, startAt: null });
    await reconcile.reconcileDeleted(env);
    expect(mockSign).not.toHaveBeenCalled();
    expect(mockDispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  it('throws malformed on a missing required field', async () => {
    await expect(reconcile.reconcileDeleted({ tenant_id: 'AUS123957' }))
      .rejects.toMatchObject({ malformed: true });
    expect(mockCancelDelete).not.toHaveBeenCalled();
  });
});

describe('reconcileMoved', () => {
  const env = { tenant_id: 'AUS123957', booking_id: 'b1', new_start_at: '2026-06-05T15:00:00Z' };

  it('cancels (coordinator_moved) and dispatches the opt-in SMS notice (move_optin_sms)', async () => {
    await reconcile.reconcileMoved(env);
    expect(mockCancelMove).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'b1' });
    expect(loggedEvents()).toContain('calendar_moved_canceled');
    expect(mockDispatchVolunteerNotice).toHaveBeenCalledTimes(1);
    const arg = mockDispatchVolunteerNotice.mock.calls[0][0];
    expect(arg.kind).toBe('move_optin_sms'); // SMS path is stubbed in Y today (inert), but wired
    expect(arg.booking.new_start_at).toBe('2026-06-05T15:00:00Z');
  });

  it('does NOT auto-create a replacement booking (only the store cancel is called)', async () => {
    await reconcile.reconcileMoved(env);
    expect(mockCancelMove).toHaveBeenCalledTimes(1);
  });

  it('no-ops without a notice when already canceled', async () => {
    mockCancelMove.mockResolvedValue(false);
    await reconcile.reconcileMoved(env);
    expect(loggedEvents()).toContain('calendar_moved_noop');
    expect(mockDispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  it('a moved-notice dispatch failure does NOT throw out of reconcile (best-effort)', async () => {
    mockDispatchVolunteerNotice.mockRejectedValue(new Error('dispatch failed'));
    await expect(reconcile.reconcileMoved(env)).resolves.toBeUndefined();
  });

  it('throws malformed on a missing required field', async () => {
    await expect(reconcile.reconcileMoved({ booking_id: 'b1' })).rejects.toMatchObject({ malformed: true });
  });
});

describe('reconcileReassigned', () => {
  const env = {
    tenant_id: 'AUS123957', booking_id: 'b1',
    previous_resource_id: 'old@org.example', new_resource_id: 'new@org.example',
  };

  it('repoints the organizer and fires NO notification (§5.1) and no PII in logs', async () => {
    await reconcile.reconcileReassigned(env);
    expect(mockReassign).toHaveBeenCalledWith({
      tenantId: 'AUS123957', bookingId: 'b1',
      previousResourceId: 'old@org.example', newResourceId: 'new@org.example',
    });
    const events = loggedEvents();
    expect(events).toContain('calendar_reassigned_updated');
    expect(events).not.toContain('notify_stub_skipped'); // reassign never notifies
    // coordinator emails MUST NOT appear in any log line.
    expect(loggedBlob()).not.toContain('old@org.example');
    expect(loggedBlob()).not.toContain('new@org.example');
  });

  it('logs a no-op when the row is already repointed / stale', async () => {
    mockReassign.mockResolvedValue(false);
    await reconcile.reconcileReassigned(env);
    expect(loggedEvents()).toContain('calendar_reassigned_noop');
  });

  it('throws malformed when the reassignment fields are missing', async () => {
    await expect(reconcile.reconcileReassigned({ tenant_id: 'AUS123957', booking_id: 'b1' }))
      .rejects.toMatchObject({ malformed: true });
    expect(mockReassign).not.toHaveBeenCalled();
  });
});

describe('requireStrings helper', () => {
  it('tags the error malformed for DLQ routing', () => {
    expect(() => reconcile.requireStrings({}, ['a'])).toThrow(/missing required field/);
    try { reconcile.requireStrings({}, ['a']); } catch (e) { expect(e.malformed).toBe(true); }
  });
  it('passes when all fields are non-empty strings', () => {
    expect(() => reconcile.requireStrings({ a: 'x', b: 'y' }, ['a', 'b'])).not.toThrow();
  });
});

describe('Track 1 — reminder teardown on cancel/move', () => {
  const DEL_ENV = { tenant_id: 'AUS123957', booking_id: 'booking#1' };
  const MOVE_ENV = { tenant_id: 'AUS123957', booking_id: 'booking#1', new_start_at: '2026-07-01T15:00:00Z', new_end_at: '2026-07-01T15:30:00Z' };

  it('reconcileDeleted tears down reminders (by {tenantId, bookingId}) when the scheduler is wired', async () => {
    process.env.SCHEDULER_TARGET_ARN = 'arn:aws:scheduler:us-east-1:525409062831:schedule/picasso-scheduling/x';
    await reconcile.reconcileDeleted(DEL_ENV);
    expect(mockDeleteReminders).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'booking#1' });
    expect(loggedEvents()).toContain('reminders_deleted');
  });

  it('reconcileMoved tears down reminders (a move cancels the booking)', async () => {
    process.env.SCHEDULER_TARGET_ARN = 'arn:aws:scheduler:us-east-1:525409062831:schedule/picasso-scheduling/x';
    await reconcile.reconcileMoved(MOVE_ENV);
    expect(mockDeleteReminders).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'booking#1' });
  });

  it('skips teardown SILENTLY when the scheduler is not wired (no SCHEDULER_TARGET_ARN)', async () => {
    // env unset by beforeEach
    await reconcile.reconcileDeleted(DEL_ENV);
    expect(mockDeleteReminders).not.toHaveBeenCalled();
  });

  it('does NOT tear down reminders on a no-op (booking was not canceled)', async () => {
    process.env.SCHEDULER_TARGET_ARN = 'arn:...';
    mockCancelDelete.mockResolvedValue(false); // already canceled / not found
    await reconcile.reconcileDeleted(DEL_ENV);
    expect(mockDeleteReminders).not.toHaveBeenCalled();
  });

  it('a teardown failure is non-fatal (cancel already durable; record must not redrive)', async () => {
    process.env.SCHEDULER_TARGET_ARN = 'arn:...';
    mockDeleteReminders.mockRejectedValue(new Error('scheduler down'));
    await expect(reconcile.reconcileDeleted(DEL_ENV)).resolves.toBeUndefined();
    expect(loggedEvents()).toContain('calendar_deleted_canceled'); // the cancel still succeeded
  });
});
