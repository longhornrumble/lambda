'use strict';

const mockWatch = jest.fn();
const mockList = jest.fn();
const mockStop = jest.fn();

jest.mock('@googleapis/calendar', () => ({
  calendar: () => ({
    events: {
      watch: mockWatch,
      list: mockList,
    },
    channels: {
      stop: mockStop,
    },
  }),
}));

const { registerWatch, stopWatch, seedInitialSyncToken } = require('./calendar-watch');

beforeEach(() => {
  mockWatch.mockReset();
  mockList.mockReset();
  mockStop.mockReset();
});

describe('registerWatch', () => {
  const authClient = { _isAuthClient: true };

  test.each([
    [null, 'cal-1', 'ch-1', 'tok', 'https://x.example'],
    [authClient, '', 'ch-1', 'tok', 'https://x.example'],
    [authClient, 'cal-1', '', 'tok', 'https://x.example'],
    [authClient, 'cal-1', 'ch-1', '', 'https://x.example'],
    [authClient, 'cal-1', 'ch-1', 'tok', ''],
  ])('throws when any required argument is missing', async (auth, cal, id, tok, url) => {
    await expect(registerWatch(auth, cal, id, tok, url))
      .rejects.toThrow('authClient, calendarId, channelId, channelToken, and listenerUrl are required');
    expect(mockWatch).not.toHaveBeenCalled();
  });

  test('calls events.watch with web_hook payload and returns normalized response', async () => {
    mockWatch.mockResolvedValueOnce({
      data: {
        resourceId: 'res-123',
        resourceUri: 'https://www.googleapis.com/calendar/v3/calendars/cal-1/events',
        expiration: '1735776000000',
      },
    });
    const result = await registerWatch(authClient, 'cal-1', 'ch-uuid', 'tok-hex', 'https://listener.example/');
    expect(mockWatch).toHaveBeenCalledWith({
      auth: authClient,
      calendarId: 'cal-1',
      requestBody: {
        id: 'ch-uuid',
        type: 'web_hook',
        address: 'https://listener.example/',
        token: 'tok-hex',
      },
    });
    expect(result).toEqual({
      resourceId: 'res-123',
      resourceUri: 'https://www.googleapis.com/calendar/v3/calendars/cal-1/events',
      expiration: '1735776000000',
    });
  });

  test('normalizes missing fields to null in response', async () => {
    mockWatch.mockResolvedValueOnce({ data: {} });
    const result = await registerWatch(authClient, 'cal-1', 'ch-uuid', 'tok', 'https://l.example/');
    expect(result).toEqual({ resourceId: null, resourceUri: null, expiration: null });
  });

  test('propagates API errors', async () => {
    mockWatch.mockRejectedValueOnce(new Error('quota exceeded'));
    await expect(registerWatch(authClient, 'cal-1', 'ch-uuid', 'tok', 'https://l.example/'))
      .rejects.toThrow('quota exceeded');
  });
});

describe('stopWatch', () => {
  const authClient = { _isAuthClient: true };

  test.each([
    [null, 'ch-1', 'res-1'],
    [authClient, '', 'res-1'],
    [authClient, 'ch-1', ''],
  ])('throws when any required argument is missing', async (auth, id, res) => {
    await expect(stopWatch(auth, id, res))
      .rejects.toThrow('authClient, channelId, and resourceId are required');
    expect(mockStop).not.toHaveBeenCalled();
  });

  test('calls channels.stop with id + resourceId', async () => {
    mockStop.mockResolvedValueOnce({ status: 204 });
    await stopWatch(authClient, 'ch-uuid', 'res-123');
    expect(mockStop).toHaveBeenCalledWith({
      auth: authClient,
      requestBody: { id: 'ch-uuid', resourceId: 'res-123' },
    });
  });

  test('propagates API errors', async () => {
    mockStop.mockRejectedValueOnce(new Error('channel not found'));
    await expect(stopWatch(authClient, 'ch-uuid', 'res-123'))
      .rejects.toThrow('channel not found');
  });
});

describe('seedInitialSyncToken', () => {
  const authClient = { _isAuthClient: true };

  test.each([
    [null, 'cal-1'],
    [authClient, ''],
  ])('throws when required argument missing', async (auth, cal) => {
    await expect(seedInitialSyncToken(auth, cal))
      .rejects.toThrow('authClient and calendarId are required');
  });

  test('returns syncToken on first page when no pagination needed', async () => {
    mockList.mockResolvedValueOnce({
      data: {
        items: [{ id: 'evt-a' }, { id: 'evt-b' }],
        nextSyncToken: 'tok-1',
      },
    });
    const result = await seedInitialSyncToken(authClient, 'cal-1');
    expect(result).toEqual({ syncToken: 'tok-1', pages: 1, totalSeen: 2 });
    expect(mockList).toHaveBeenCalledTimes(1);
    expect(mockList).toHaveBeenCalledWith({
      auth: authClient,
      calendarId: 'cal-1',
      showDeleted: true,
      singleEvents: false,
    });
  });

  test('paginates through multiple pages until syncToken appears', async () => {
    mockList
      .mockResolvedValueOnce({ data: { items: [{ id: '1' }], nextPageToken: 'p2' } })
      .mockResolvedValueOnce({ data: { items: [{ id: '2' }, { id: '3' }], nextPageToken: 'p3' } })
      .mockResolvedValueOnce({ data: { items: [{ id: '4' }], nextSyncToken: 'final-tok' } });

    const result = await seedInitialSyncToken(authClient, 'cal-1');
    expect(result).toEqual({ syncToken: 'final-tok', pages: 3, totalSeen: 4 });
    expect(mockList).toHaveBeenNthCalledWith(2, expect.objectContaining({ pageToken: 'p2' }));
    expect(mockList).toHaveBeenNthCalledWith(3, expect.objectContaining({ pageToken: 'p3' }));
  });

  test('returns null syncToken when Google returns neither nextPageToken nor nextSyncToken', async () => {
    mockList.mockResolvedValueOnce({ data: { items: [] } });
    const result = await seedInitialSyncToken(authClient, 'cal-1');
    expect(result).toEqual({ syncToken: null, pages: 1, totalSeen: 0 });
  });

  test('throws when maxPages is exceeded', async () => {
    mockList.mockResolvedValue({ data: { items: [{ id: 'x' }], nextPageToken: 'never-ends' } });
    await expect(seedInitialSyncToken(authClient, 'cal-1', 3))
      .rejects.toThrow('Initial sync-token seed exceeded maxPages=3');
    expect(mockList).toHaveBeenCalledTimes(3);
  });

  test('propagates API errors', async () => {
    mockList.mockRejectedValueOnce(new Error('403 Forbidden'));
    await expect(seedInitialSyncToken(authClient, 'cal-1'))
      .rejects.toThrow('403 Forbidden');
  });

  test('handles missing items array gracefully', async () => {
    mockList.mockResolvedValueOnce({ data: { nextSyncToken: 'tok-empty' } });
    const result = await seedInitialSyncToken(authClient, 'cal-1');
    expect(result).toEqual({ syncToken: 'tok-empty', pages: 1, totalSeen: 0 });
  });
});
