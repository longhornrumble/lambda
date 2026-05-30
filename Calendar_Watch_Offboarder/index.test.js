'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');

// Mock oauth-client + calendar-watch BEFORE requiring index.
const mockOauth = jest.fn();
jest.mock('./oauth-client', () => ({
  getOAuthClient: (...args) => mockOauth(...args),
}));

const mockStopWatch = jest.fn();
jest.mock('./calendar-watch', () => ({
  stopWatch: (...args) => mockStopWatch(...args),
}));

process.env.CALENDAR_WATCH_CHANNELS_TABLE = 'picasso-calendar-watch-channels-staging';

const { handler, _test } = require('./index');

const ddbMock = mockClient(DynamoDBClient);

beforeEach(() => {
  ddbMock.reset();
  mockOauth.mockReset();
  mockStopWatch.mockReset();
});

// ─── fixtures ──────────────────────────────────────────────────────────────────

function channelItem(over = {}) {
  const item = {
    channel_id:        { S: over.channel_id ?? 'ch-1' },
    tenant_id:         { S: over.tenant_id ?? 'MYR384719' },
    coordinator_id:    { S: over.coordinator_id ?? 'test-coordinator' },
    calendar_provider: { S: over.calendar_provider ?? 'google' },
  };
  if (over.resource_id !== null) {
    item.resource_id = { S: over.resource_id ?? 'res-1' };
  }
  return item;
}

function gone(status) {
  const err = new Error('channel gone');
  err.response = { status };
  return err;
}

// OAuth grant revoked — the account-suspended signal. `kind` picks the shape:
//   'status401'      → 401 from channels.stop
//   'invalid_grant'  → GaxiosError whose response.data.error is invalid_grant
//   'message'        → only the message carries invalid_grant
function authRevoked(kind = 'status401') {
  const err = new Error(kind === 'message' ? 'Token refresh failed: invalid_grant' : 'auth revoked');
  if (kind === 'status401') {
    err.response = { status: 401 };
  } else if (kind === 'invalid_grant') {
    err.response = { status: 400, data: { error: 'invalid_grant' } };
  }
  return err;
}

// ─── pure helpers ────────────────────────────────────────────────────────────────

describe('validateInput', () => {
  const { validateInput } = _test;

  test.each([null, undefined, 'str', 42])('rejects non-object input %p', (bad) => {
    expect(() => validateInput(bad)).toThrow('Input must be a JSON object');
  });

  test('requires tenant_id', () => {
    expect(() => validateInput({ coordinator_id: 'c' })).toThrow('tenant_id is required');
  });

  test.each(['has/slash', 'has space', 'a'.repeat(65), ''])('rejects bad tenant_id %p', (t) => {
    expect(() => validateInput({ tenant_id: t, coordinator_id: 'c' })).toThrow('tenant_id is required');
  });

  test('rejects neither coordinator_id nor channel_id', () => {
    expect(() => validateInput({ tenant_id: 'MYR384719' }))
      .toThrow('exactly one of coordinator_id or channel_id is required');
  });

  test('rejects both coordinator_id and channel_id', () => {
    expect(() => validateInput({ tenant_id: 'MYR384719', coordinator_id: 'c', channel_id: 'ch' }))
      .toThrow('exactly one of coordinator_id or channel_id is required');
  });

  test('rejects bad coordinator_id charset', () => {
    expect(() => validateInput({ tenant_id: 'MYR384719', coordinator_id: 'bad/slash' }))
      .toThrow('coordinator_id must match');
  });

  test('rejects bad channel_id charset', () => {
    expect(() => validateInput({ tenant_id: 'MYR384719', channel_id: 'bad/slash' }))
      .toThrow('channel_id must match');
  });

  test('accepts coordinator selector', () => {
    expect(validateInput({ tenant_id: 'MYR384719', coordinator_id: 'jane@x.org' }))
      .toEqual({ tenantId: 'MYR384719', coordinatorId: 'jane@x.org', channelId: null });
  });

  test('accepts channel selector', () => {
    expect(validateInput({ tenant_id: 'MYR384719', channel_id: 'abc-123' }))
      .toEqual({ tenantId: 'MYR384719', coordinatorId: null, channelId: 'abc-123' });
  });
});

describe('parseChannelRow', () => {
  const { parseChannelRow } = _test;

  test('parses a full row', () => {
    expect(parseChannelRow(channelItem())).toEqual({
      channelId: 'ch-1',
      tenantId: 'MYR384719',
      coordinatorId: 'test-coordinator',
      calendarProvider: 'google',
      resourceId: 'res-1',
    });
  });

  test('defaults missing optional fields', () => {
    expect(parseChannelRow({ channel_id: { S: 'ch-2' } })).toEqual({
      channelId: 'ch-2',
      tenantId: null,
      coordinatorId: null,
      calendarProvider: 'google',
      resourceId: null,
    });
  });

  test('defaults an entirely empty item (forward-compat read)', () => {
    expect(parseChannelRow({})).toEqual({
      channelId: null,
      tenantId: null,
      coordinatorId: null,
      calendarProvider: 'google',
      resourceId: null,
    });
  });
});

describe('isAlreadyGone', () => {
  const { isAlreadyGone } = _test;

  test.each([
    [gone(404), true],
    [gone(410), true],
    [gone('404'), true],
    [gone('410'), true],
    [{ code: 404 }, true],
    [{ code: '410' }, true],
    [gone(500), false],
    [new Error('network'), false],
    [null, false],
    [undefined, false],
  ])('classifies %p as %p', (err, expected) => {
    expect(isAlreadyGone(err)).toBe(expected);
  });
});

describe('isAuthRevoked', () => {
  const { isAuthRevoked } = _test;

  test.each([
    [authRevoked('status401'), true],
    [{ code: 401 }, true],
    [{ code: '401' }, true],
    [authRevoked('invalid_grant'), true],
    [{ response: { data: { error: 'unauthorized_client' } } }, true],
    // message-only "invalid_grant" (no structured 401 / response.data.error) is
    // NOT classified as revoked (phase-audit row 4) — it stays transient so an
    // ambiguous error can't wrongly delete a valid row.
    [authRevoked('message'), false],
    [gone(403), false],   // 403 = Google rate/quota overload — transient, NOT revoked
    [gone(503), false],
    [gone(404), false],
    [new Error('network'), false],
    [null, false],
    [undefined, false],
  ])('classifies %p as %p', (err, expected) => {
    expect(isAuthRevoked(err)).toBe(expected);
  });
});

// ─── handler: env guard ────────────────────────────────────────────────────────

describe('handler env guard', () => {
  test('throws when CALENDAR_WATCH_CHANNELS_TABLE is unset', async () => {
    const saved = process.env.CALENDAR_WATCH_CHANNELS_TABLE;
    delete process.env.CALENDAR_WATCH_CHANNELS_TABLE;
    let freshHandler;
    jest.isolateModules(() => {
      ({ handler: freshHandler } = require('./index'));
    });
    await expect(freshHandler({ tenant_id: 'MYR384719', channel_id: 'ch-1' }))
      .rejects.toThrow('CALENDAR_WATCH_CHANNELS_TABLE env var is required');
    process.env.CALENDAR_WATCH_CHANNELS_TABLE = saved;
  });

  test('propagates validation errors', async () => {
    await expect(handler({ tenant_id: 'MYR384719' }))
      .rejects.toThrow('exactly one of coordinator_id or channel_id is required');
  });
});

// ─── handler: channel_id selector ──────────────────────────────────────────────

describe('handler — channel_id selector', () => {
  test('stops the channel and deletes the row (happy path)', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });

    expect(mockStopWatch).toHaveBeenCalledWith({ _auth: true }, 'ch-1', 'res-1');
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(res).toEqual({ requested: 1, stopped: ['ch-1'], deleted: ['ch-1'], failed: [] });
  });

  test('no-op when the channel row does not exist', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-missing' });
    expect(mockStopWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res).toEqual({ requested: 0, stopped: [], deleted: [], failed: [] });
  });

  test('refuses to offboard a channel owned by another tenant (G6)', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem({ tenant_id: 'OTHER999' }) });
    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(mockStopWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res.requested).toBe(0);
  });

  test('already-gone (404) channel is still deleted but NOT counted as stopped', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue(gone(404));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(res.deleted).toEqual(['ch-1']);
    expect(res.stopped).toEqual([]);   // we did NOT channels.stop it (row 3)
    expect(res.failed).toEqual([]);
  });

  test('account suspended (401, from stopWatch) → row deleted, not counted as stopped', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue(authRevoked('status401'));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(res.deleted).toEqual(['ch-1']);
    expect(res.stopped).toEqual([]);
    expect(res.failed).toEqual([]);
  });

  test('account suspended (revoked grant surfaces from getOAuthClient) → row deleted', async () => {
    // Row 1: getOAuthClient is inside the try, so a revoked-grant error thrown
    // at client-build/token-refresh time is classified by isAuthRevoked too.
    mockOauth.mockRejectedValue(authRevoked('invalid_grant'));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(mockStopWatch).not.toHaveBeenCalled();   // never reached stopWatch
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(res.deleted).toEqual(['ch-1']);
    expect(res.stopped).toEqual([]);
    expect(res.failed).toEqual([]);
  });

  test('getOAuthClient transient failure (Secrets Manager down) → row left, failed', async () => {
    // Row 1: a NON-auth error from getOAuthClient stays transient (row retained).
    mockOauth.mockRejectedValue(gone(503));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res.deleted).toEqual([]);
    expect(res.failed).toHaveLength(1);
  });

  test('grant revoked (unauthorized_client) → row deleted, not counted as stopped', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue({ response: { data: { error: 'unauthorized_client' } } });
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(res.deleted).toEqual(['ch-1']);
    expect(res.stopped).toEqual([]);
    expect(res.failed).toEqual([]);
  });

  test('message-only invalid_grant is transient (not revoked) → row left, failed', async () => {
    // Row 4: removing the message-substring match means an ambiguous error whose
    // message merely contains "invalid_grant" no longer wrongly deletes the row.
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue(authRevoked('message'));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res.deleted).toEqual([]);
    expect(res.failed).toHaveLength(1);
  });

  test('rate-limit (403) is transient — row is left for retry', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue(gone(403));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res.deleted).toEqual([]);
    expect(res.failed).toHaveLength(1);
  });

  test('transient stop failure leaves the row and reports failed', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue(gone(503));
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res.deleted).toEqual([]);
    expect(res.failed).toHaveLength(1);
    expect(res.failed[0].channel_id).toBe('ch-1');
    expect(res.failed[0].error).toMatch(/transient/);
  });

  test('row without resource_id is deleted without an OAuth/stop call', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem({ resource_id: null }) });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(mockOauth).not.toHaveBeenCalled();
    expect(mockStopWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(res.deleted).toEqual(['ch-1']);
    expect(res.stopped).toEqual([]);   // no resourceId → nothing stopped (row 3)
  });

  test('delete ConditionalCheckFailed reports the channel as failed', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    const condErr = new Error('The conditional request failed');
    condErr.name = 'ConditionalCheckFailedException';
    ddbMock.on(DeleteItemCommand).rejects(condErr);

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(res.deleted).toEqual([]);
    expect(res.failed).toHaveLength(1);
  });
});

// ─── handler: coordinator_id selector ──────────────────────────────────────────

describe('handler — coordinator_id selector', () => {
  test('scheduling_tags-cleared trigger: stops + deletes every channel for the coordinator', async () => {
    // The first B6 trigger path (canonical §4.5) — coordinator no longer bookable
    // with a still-valid OAuth grant: every channel is channels.stop'd + deleted.
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock.on(QueryCommand).resolves({
      Items: [
        channelItem({ channel_id: 'ch-a', resource_id: 'res-a' }),
        channelItem({ channel_id: 'ch-b', resource_id: 'res-b' }),
      ],
    });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });
    expect(mockStopWatch).toHaveBeenCalledTimes(2);
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(2);
    expect(res.deleted.sort()).toEqual(['ch-a', 'ch-b']);
    expect(res.stopped.sort()).toEqual(['ch-a', 'ch-b']);   // both actually stopped
  });

  test('filters out rows for a different coordinator', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock.on(QueryCommand).resolves({
      Items: [
        channelItem({ channel_id: 'ch-a', coordinator_id: 'test-coordinator' }),
        channelItem({ channel_id: 'ch-b', coordinator_id: 'someone-else' }),
      ],
    });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });
    expect(res.requested).toBe(1);
    expect(res.deleted).toEqual(['ch-a']);
  });

  test('no-op when the coordinator has no channels', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'ghost' });
    expect(res).toEqual({ requested: 0, stopped: [], deleted: [], failed: [] });
  });

  test('tolerates a Query response with no Items key (forward-compat read)', async () => {
    ddbMock.on(QueryCommand).resolves({});
    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'ghost' });
    expect(res).toEqual({ requested: 0, stopped: [], deleted: [], failed: [] });
  });

  test('paginates the tenant-expiration-index query', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({ Items: [channelItem({ channel_id: 'ch-a' })], LastEvaluatedKey: { k: { S: '1' } } })
      .resolvesOnce({ Items: [channelItem({ channel_id: 'ch-b' })] });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });
    const queryCalls = ddbMock.commandCalls(QueryCommand);
    expect(queryCalls).toHaveLength(2);
    // Row 8: the second page MUST forward the first page's LastEvaluatedKey as
    // ExclusiveStartKey — else a broken cursor would silently re-query from start.
    expect(queryCalls[0].args[0].input.ExclusiveStartKey).toBeUndefined();
    expect(queryCalls[1].args[0].input.ExclusiveStartKey).toEqual({ k: { S: '1' } });
    expect(res.deleted.sort()).toEqual(['ch-a', 'ch-b']);
  });

  test('account-suspended trigger: every channel deleted despite revoked OAuth (401)', async () => {
    // The §4.5 row-4 path — coordinator's Workspace account suspended, so OAuth
    // is revoked. None of the channels can be channels.stop'd, but all rows must
    // be cleaned up (else the Renewer renews a departed coordinator forever).
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch.mockRejectedValue(authRevoked('status401'));
    ddbMock.on(QueryCommand).resolves({
      Items: [
        channelItem({ channel_id: 'ch-a', resource_id: 'res-a' }),
        channelItem({ channel_id: 'ch-b', resource_id: 'res-b' }),
      ],
    });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(2);
    expect(res.deleted.sort()).toEqual(['ch-a', 'ch-b']);
    expect(res.failed).toEqual([]);
  });

  test('one channel failing does not abort the others', async () => {
    mockOauth.mockResolvedValue({ _auth: true });
    mockStopWatch
      .mockRejectedValueOnce(gone(503))   // ch-a transient fail
      .mockResolvedValueOnce(undefined);  // ch-b ok
    ddbMock.on(QueryCommand).resolves({
      Items: [
        channelItem({ channel_id: 'ch-a', resource_id: 'res-a' }),
        channelItem({ channel_id: 'ch-b', resource_id: 'res-b' }),
      ],
    });
    ddbMock.on(DeleteItemCommand).resolves({});

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'test-coordinator' });
    expect(res.deleted).toEqual(['ch-b']);
    expect(res.failed).toHaveLength(1);
    expect(res.failed[0].channel_id).toBe('ch-a');
  });
});

// ─── offboardChannel — G3 row-identifier guards (direct) ────────────────────────

describe('offboardChannel — corrupted-row guards (G3)', () => {
  const { offboardChannel } = _test;

  test('throws on an invalid tenant_id read off the row', async () => {
    await expect(offboardChannel({ channelId: 'ch-1', tenantId: 'bad/slash', coordinatorId: 'c', resourceId: 'r' }))
      .rejects.toThrow('invalid tenant_id');
    expect(mockOauth).not.toHaveBeenCalled();
  });

  test('throws on an invalid coordinator_id read off the row', async () => {
    await expect(offboardChannel({ channelId: 'ch-1', tenantId: 'MYR384719', coordinatorId: 'bad/slash', resourceId: 'r' }))
      .rejects.toThrow('invalid coordinator_id');
    expect(mockOauth).not.toHaveBeenCalled();
  });

  test('throws on an invalid channel_id read off the row (row 2)', async () => {
    // On the coordinator path channelId comes from the DDB row, not caller input;
    // a corrupted value must be rejected before it becomes a DDB key / Google id.
    await expect(offboardChannel({ channelId: 'bad/slash', tenantId: 'MYR384719', coordinatorId: 'c', resourceId: 'r' }))
      .rejects.toThrow('invalid channel_id');
    expect(mockOauth).not.toHaveBeenCalled();
  });

  test('throws on a null channel_id read off the row (row 2)', async () => {
    await expect(offboardChannel({ channelId: null, tenantId: 'MYR384719', coordinatorId: 'c', resourceId: 'r' }))
      .rejects.toThrow('invalid channel_id');
    expect(mockOauth).not.toHaveBeenCalled();
  });
});
