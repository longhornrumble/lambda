'use strict';

const mockEventsGet = jest.fn();
const mockEventsList = jest.fn();

jest.mock('@googleapis/calendar', () => ({
  calendar: jest.fn(() => ({
    events: {
      get: (...args) => mockEventsGet(...args),
      list: (...args) => mockEventsList(...args),
    },
  })),
}));

const { getEvent, listChangedEvents } = require('./calendar-api');

const FAKE_AUTH = { _kind: 'fake-oauth2' };

beforeEach(() => {
  mockEventsGet.mockReset();
  mockEventsList.mockReset();
});

describe('getEvent', () => {
  test('returns status=found with event when API returns 200', async () => {
    const event = { id: 'evt-1', status: 'confirmed', summary: 'Hi' };
    mockEventsGet.mockResolvedValue({ data: event });
    const result = await getEvent(FAKE_AUTH, 'coord@x.com', 'evt-1');
    expect(result).toEqual({ status: 'found', event });
    expect(mockEventsGet).toHaveBeenCalledWith({
      auth: FAKE_AUTH,
      calendarId: 'coord@x.com',
      eventId: 'evt-1',
    });
  });

  test('returns status=deleted when API returns 404', async () => {
    const err = new Error('Not Found'); err.code = 404;
    mockEventsGet.mockRejectedValue(err);
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'deleted', event: null });
  });

  test('returns status=deleted when API returns 410 (Gone)', async () => {
    const err = new Error('Gone'); err.code = 410;
    mockEventsGet.mockRejectedValue(err);
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'deleted', event: null });
  });

  test('returns status=deleted when event.status === cancelled', async () => {
    mockEventsGet.mockResolvedValue({ data: { id: 'e', status: 'cancelled' } });
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'deleted', event: null });
  });

  test('returns status=private when API returns 403', async () => {
    const err = new Error('Forbidden'); err.code = 403;
    mockEventsGet.mockRejectedValue(err);
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'private', event: null });
  });

  test('returns status=private when event.visibility === private', async () => {
    mockEventsGet.mockResolvedValue({ data: { id: 'e', status: 'confirmed', visibility: 'private' } });
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'private', event: null });
  });

  test('returns status=private when event.visibility === confidential', async () => {
    mockEventsGet.mockResolvedValue({ data: { id: 'e', status: 'confirmed', visibility: 'confidential' } });
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'private', event: null });
  });

  test('reads error code from err.response.status when err.code missing', async () => {
    const err = new Error('NotFound'); err.response = { status: 404 };
    mockEventsGet.mockRejectedValue(err);
    const result = await getEvent(FAKE_AUTH, 'c@x', 'e');
    expect(result).toEqual({ status: 'deleted', event: null });
  });

  test('propagates other errors so caller can DLQ + alarm', async () => {
    const err = new Error('Internal'); err.code = 500;
    mockEventsGet.mockRejectedValue(err);
    await expect(getEvent(FAKE_AUTH, 'c@x', 'e')).rejects.toThrow('Internal');
  });

  test.each([
    [null, 'c@x', 'e'],
    [FAKE_AUTH, null, 'e'],
    [FAKE_AUTH, 'c@x', null],
  ])('throws when required arg is missing (%#)', async (auth, calId, eId) => {
    await expect(getEvent(auth, calId, eId))
      .rejects.toThrow('authClient, calendarId, and eventId are required');
  });
});

describe('listChangedEvents', () => {
  test('passes syncToken when provided', async () => {
    mockEventsList.mockResolvedValue({
      data: { items: [{ id: 'a' }], nextSyncToken: 'next-tok', nextPageToken: null },
    });
    const result = await listChangedEvents(FAKE_AUTH, 'cal', 'sync-1');
    // singleEvents:false on EVERY path (code#2) — matches the Onboarder seed mode
    expect(mockEventsList).toHaveBeenCalledWith({
      auth: FAKE_AUTH, calendarId: 'cal', syncToken: 'sync-1', singleEvents: false,
    });
    expect(result).toEqual({
      events: [{ id: 'a' }],
      nextSyncToken: 'next-tok',
      nextPageToken: null,
    });
  });

  test('initial pull (no syncToken) sets showDeleted + singleEvents:false', async () => {
    mockEventsList.mockResolvedValue({ data: { items: [], nextSyncToken: 'tok' } });
    await listChangedEvents(FAKE_AUTH, 'cal', null);
    expect(mockEventsList).toHaveBeenCalledWith({
      auth: FAKE_AUTH, calendarId: 'cal', showDeleted: true, singleEvents: false,
    });
  });

  test('returns empty arrays when API returns no items', async () => {
    mockEventsList.mockResolvedValue({ data: {} });
    const result = await listChangedEvents(FAKE_AUTH, 'cal');
    expect(result).toEqual({ events: [], nextSyncToken: null, nextPageToken: null });
  });

  test('throws when authClient missing', async () => {
    await expect(listChangedEvents(null, 'cal'))
      .rejects.toThrow('authClient and calendarId are required');
  });

  test('throws when calendarId missing', async () => {
    await expect(listChangedEvents(FAKE_AUTH, null))
      .rejects.toThrow('authClient and calendarId are required');
  });

  test('pageToken continuation: uses pageToken, sets showDeleted+singleEvents:false, omits syncToken', async () => {
    mockEventsList.mockResolvedValue({
      data: { items: [{ id: 'b' }], nextSyncToken: 'final-tok', nextPageToken: null },
    });
    const result = await listChangedEvents(FAKE_AUTH, 'cal', null, 'page-tok-2');
    // continuation MUST be the same singleEvents mode as the syncToken it pages (false) — code#2
    expect(mockEventsList).toHaveBeenCalledWith({
      auth: FAKE_AUTH, calendarId: 'cal', pageToken: 'page-tok-2',
      showDeleted: true, singleEvents: false,
    });
    expect(result).toEqual({
      events: [{ id: 'b' }],
      nextSyncToken: 'final-tok',
      nextPageToken: null,
    });
  });
});
