'use strict';

const mockMove = jest.fn();
const mockDelete = jest.fn();

jest.mock('@googleapis/calendar', () => ({
  calendar: () => ({
    events: {
      move: (...args) => mockMove(...args),
      delete: (...args) => mockDelete(...args),
    },
  }),
}));

const { transferEvent, deleteEvent, isAlreadyGone } = require('./calendar-ops');

beforeEach(() => {
  mockMove.mockReset();
  mockDelete.mockReset();
});

describe('transferEvent (reassign)', () => {
  test('moves the event to the new calendar with sendUpdates:all and returns data', async () => {
    mockMove.mockResolvedValue({ data: { id: 'evt-1', organizer: { email: 'diego@org.com' } } });
    const out = await transferEvent('AUTH', {
      eventId: 'evt-1', fromCalendarId: 'maya@org.com', toCalendarId: 'diego@org.com',
    });
    expect(mockMove).toHaveBeenCalledWith({
      auth: 'AUTH',
      calendarId: 'maya@org.com',
      eventId: 'evt-1',
      destination: 'diego@org.com',
      sendUpdates: 'all',
    });
    expect(out).toEqual({ id: 'evt-1', organizer: { email: 'diego@org.com' } });
  });

  test('missing args throw', async () => {
    await expect(transferEvent(null, { eventId: 'e', fromCalendarId: 'a', toCalendarId: 'b' }))
      .rejects.toThrow('required');
    await expect(transferEvent('AUTH', { eventId: 'e', fromCalendarId: 'a' }))
      .rejects.toThrow('required');
  });

  test('a 404 vanished source event throws a REDACTED error preserving code (caller cascades)', async () => {
    const gone = Object.assign(new Error('Not Found: maya@org.com calendar'), { code: 404 });
    mockMove.mockRejectedValue(gone);
    let caught;
    await transferEvent('AUTH', { eventId: 'e', fromCalendarId: 'maya@org.com', toCalendarId: 'b' })
      .catch((e) => { caught = e; });
    expect(caught.code).toBe(404); // preserved so reassign() can classify
    expect(caught.message).toBe('calendar move failed (code=404)');
    expect(caught.message).not.toContain('maya@org.com'); // PII never leaks
  });

  test('a 401 (revoked/denied) throws a REDACTED error preserving code 401 (caller propagates)', async () => {
    mockMove.mockRejectedValue({ response: { status: 401, data: { error: 'invalid_grant' } } });
    let caught;
    await transferEvent('AUTH', { eventId: 'e', fromCalendarId: 'maya@org.com', toCalendarId: 'b' })
      .catch((e) => { caught = e; });
    expect(caught.code).toBe(401);
    expect(caught.message).toBe('calendar move failed (code=401)');
  });
});

describe('deleteEvent (cancel)', () => {
  test('deletes the event with sendUpdates:none', async () => {
    mockDelete.mockResolvedValue({});
    await deleteEvent('AUTH', { eventId: 'evt-1', calendarId: 'maya@org.com' });
    expect(mockDelete).toHaveBeenCalledWith({
      auth: 'AUTH', calendarId: 'maya@org.com', eventId: 'evt-1', sendUpdates: 'none',
    });
  });

  test('404 already-gone is idempotent success', async () => {
    mockDelete.mockRejectedValue(Object.assign(new Error('gone'), { code: 404 }));
    await expect(deleteEvent('AUTH', { eventId: 'e', calendarId: 'c' })).resolves.toBeUndefined();
  });

  test('410 already-gone (via response.status) is idempotent success', async () => {
    mockDelete.mockRejectedValue({ response: { status: 410 } });
    await expect(deleteEvent('AUTH', { eventId: 'e', calendarId: 'c' })).resolves.toBeUndefined();
  });

  test('a non-gone error propagates REDACTED (code preserved, no raw message/calendarId)', async () => {
    mockDelete.mockRejectedValue(Object.assign(new Error('boom: maya@org.com'), { code: 500 }));
    let caught;
    await deleteEvent('AUTH', { eventId: 'e', calendarId: 'maya@org.com' }).catch((e) => { caught = e; });
    expect(caught.code).toBe(500);
    expect(caught.message).toBe('calendar delete failed (code=500)');
    expect(caught.message).not.toContain('maya@org.com');
  });

  test('missing args throw', async () => {
    await expect(deleteEvent('AUTH', { eventId: 'e' })).rejects.toThrow('required');
  });
});

describe('isAlreadyGone', () => {
  test('recognises 404/410 in either shape', () => {
    expect(isAlreadyGone({ code: 404 })).toBe(true);
    expect(isAlreadyGone({ response: { status: 410 } })).toBe(true);
    expect(isAlreadyGone({ code: '404' })).toBe(true);
    expect(isAlreadyGone({ code: 500 })).toBe(false);
  });
});
