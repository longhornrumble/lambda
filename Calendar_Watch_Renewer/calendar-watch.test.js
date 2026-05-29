'use strict';

const mockWatch = jest.fn();
const mockStop = jest.fn();

jest.mock('@googleapis/calendar', () => ({
  calendar: () => ({
    events: {
      watch: mockWatch,
    },
    channels: {
      stop: mockStop,
    },
  }),
}));

const { registerWatch, stopWatch } = require('./calendar-watch');

beforeEach(() => {
  mockWatch.mockReset();
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
