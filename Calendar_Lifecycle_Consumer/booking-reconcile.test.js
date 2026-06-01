'use strict';

/**
 * Unit tests for booking-reconcile.js — per-event reconciliation orchestration + the
 * §5.1 notification narrowing (TODO(Y) stubs). booking-store is mocked.
 */

const mockCancelDelete = jest.fn();
const mockCancelMove = jest.fn();
const mockReassign = jest.fn();
jest.mock('./booking-store', () => ({
  cancelOnCoordinatorDelete: mockCancelDelete,
  cancelOnCoordinatorMove: mockCancelMove,
  reassignCoordinator: mockReassign,
}));

const reconcile = require('./booking-reconcile');

let logSpy;
beforeEach(() => {
  mockCancelDelete.mockReset().mockResolvedValue(true);
  mockCancelMove.mockReset().mockResolvedValue(true);
  mockReassign.mockReset().mockResolvedValue(true);
  logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

function loggedEvents() {
  return logSpy.mock.calls.map((c) => { try { return JSON.parse(c[0]).event; } catch (_) { return ''; } });
}
function loggedBlob() {
  return logSpy.mock.calls.map((c) => String(c[0])).join(' ');
}

describe('reconcileDeleted', () => {
  const env = { tenant_id: 'AUS123957', booking_id: 'b1' };

  it('cancels and logs a TODO(Y) reschedule-link notify stub on a fresh cancel', async () => {
    await reconcile.reconcileDeleted(env);
    expect(mockCancelDelete).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'b1' });
    const events = loggedEvents();
    expect(events).toContain('calendar_deleted_canceled');
    expect(events).toContain('notify_stub_skipped');
  });

  it('does NOT fire the notify stub on an idempotent no-op', async () => {
    mockCancelDelete.mockResolvedValue(false);
    await reconcile.reconcileDeleted(env);
    const events = loggedEvents();
    expect(events).toContain('calendar_deleted_noop');
    expect(events).not.toContain('notify_stub_skipped');
  });

  it('throws malformed on a missing required field', async () => {
    await expect(reconcile.reconcileDeleted({ tenant_id: 'AUS123957' }))
      .rejects.toMatchObject({ malformed: true });
    expect(mockCancelDelete).not.toHaveBeenCalled();
  });
});

describe('reconcileMoved', () => {
  const env = { tenant_id: 'AUS123957', booking_id: 'b1', new_start_at: '2026-06-05T15:00:00Z' };

  it('cancels (coordinator_moved) and logs the TODO(Y) reschedule path stub', async () => {
    await reconcile.reconcileMoved(env);
    expect(mockCancelMove).toHaveBeenCalledWith({ tenantId: 'AUS123957', bookingId: 'b1' });
    const events = loggedEvents();
    expect(events).toContain('calendar_moved_canceled');
    expect(events).toContain('notify_stub_skipped');
  });

  it('does NOT auto-create a replacement booking (only the store cancel is called)', async () => {
    await reconcile.reconcileMoved(env);
    expect(mockCancelMove).toHaveBeenCalledTimes(1);
    // no rebook/commit call exists on the mocked store surface — the stub is the only side effect.
  });

  it('no-ops without the notify stub when already canceled', async () => {
    mockCancelMove.mockResolvedValue(false);
    await reconcile.reconcileMoved(env);
    expect(loggedEvents()).toContain('calendar_moved_noop');
    expect(loggedEvents()).not.toContain('notify_stub_skipped');
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
