'use strict';

const mockStop = jest.fn();

jest.mock('@googleapis/calendar', () => ({
  calendar: () => ({
    channels: {
      stop: mockStop,
    },
  }),
}));

const { stopWatch } = require('./calendar-watch');

beforeEach(() => {
  mockStop.mockReset();
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
