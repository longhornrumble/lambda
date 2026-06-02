'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

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
// B11 offboarding trigger (gap B): set before require so the module-load const is set.
process.env.REMEDIATOR_FUNCTION_NAME = 'Stranded_Booking_Remediator';

const { handler, _test } = require('./index');

const ddbMock = mockClient(DynamoDBClient);
const lambdaMock = mockClient(LambdaClient);

beforeEach(() => {
  ddbMock.reset();
  lambdaMock.reset();
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
    expect(res).toEqual({ requested: 1, stopped: ['ch-1'], deleted: ['ch-1'], failed: [], stranded_remediation_invoked: false });
  });

  test('no-op when the channel row does not exist', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-missing' });
    expect(mockStopWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);
    expect(res).toEqual({ requested: 0, stopped: [], deleted: [], failed: [], stranded_remediation_invoked: false });
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
    // stranded_remediation_invoked=true: the coordinator path fires B11 regardless of
    // channel count (stranded bookings may exist without live channels).
    expect(res).toEqual({ requested: 0, stopped: [], deleted: [], failed: [], stranded_remediation_invoked: true });
  });

  test('tolerates a Query response with no Items key (forward-compat read)', async () => {
    ddbMock.on(QueryCommand).resolves({});
    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: 'ghost' });
    expect(res).toEqual({ requested: 0, stopped: [], deleted: [], failed: [], stranded_remediation_invoked: true });
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

// ─── error-message redaction (phase-audit row GF) ────────────────────────────────

describe('sanitizeErrorMessage (GF)', () => {
  const { sanitizeErrorMessage } = _test;

  test.each(['AccessDeniedException', 'ResourceNotFoundException', 'UnrecognizedClientException'])(
    'redacts %s (by name) to the type only',
    (name) => {
      const err = new Error('arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/MYR384719/jane@x.org-AbCdEf');
      err.name = name;
      const out = sanitizeErrorMessage(err);
      expect(out).toMatch(/redacted/);
      expect(out).not.toMatch(/arn:aws|jane@x\.org/);
    }
  );

  test('redacts when the type is on err.code (not err.name)', () => {
    const out = sanitizeErrorMessage({ code: 'AccessDeniedException', message: 'arn:aws:...:MYR384719/jane@x.org' });
    expect(out).toMatch(/redacted/);
    expect(out).not.toMatch(/jane@x\.org/);
  });

  test('passes through an ordinary error message', () => {
    expect(sanitizeErrorMessage(new Error('plain network blip'))).toBe('plain network blip');
  });

  test('stringifies a throwable with no message', () => {
    expect(sanitizeErrorMessage({ foo: 'bar' })).toBe('[object Object]');
  });
});

describe('handler does not leak the OAuth secret ARN on AccessDenied (GF)', () => {
  test('getOAuthClient AccessDenied → failed error is redacted (no ARN / email)', async () => {
    const denied = new Error(
      'not authorized to perform secretsmanager:GetSecretValue on arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/MYR384719/jane@x.org-AbCdEf'
    );
    denied.name = 'AccessDeniedException';
    mockOauth.mockRejectedValue(denied);
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });
    expect(res.failed).toHaveLength(1);
    expect(res.failed[0].error).not.toMatch(/arn:aws|jane@x\.org/);
    expect(res.failed[0].error).toMatch(/redacted/);
  });
});

// ─── B11 offboarding trigger (gap B) ──────────────────────────────────────────────
// On the coordinator-offboarding path, the Offboarder async-invokes the
// Stranded_Booking_Remediator (B11). NOT fired on the single-channel teardown path.

describe('B11 offboarding trigger', () => {
  const COORD = 'coord@myr.example.com';

  test('coordinator path → async-invokes the remediator with the offboarding payload', async () => {
    // one matching channel for the coordinator
    ddbMock.on(QueryCommand).resolves({ Items: [channelItem({ coordinator_id: COORD })] });
    mockOauth.mockResolvedValue({ _kind: 'oauth' });
    mockStopWatch.mockResolvedValue(undefined);
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: COORD });

    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);
    const input = lambdaMock.commandCalls(InvokeCommand)[0].args[0].input;
    expect(input.FunctionName).toBe('Stranded_Booking_Remediator');
    expect(input.InvocationType).toBe('Event'); // fire-and-forget
    const payload = JSON.parse(Buffer.from(input.Payload).toString());
    expect(payload.tenant_id).toBe('MYR384719');
    expect(payload.coordinator_email).toBe(COORD); // coordinator_id IS the calendar email
    expect(typeof payload.offboarding_time).toBe('string');
    expect(Number.isNaN(Date.parse(payload.offboarding_time))).toBe(false);
    expect(payload).not.toHaveProperty('choice'); // omitted ⇒ B11 default cascade
    expect(res.stranded_remediation_invoked).toBe(true);
  });

  test('coordinator path with ZERO live channels still fires remediation (stranded bookings may remain)', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: COORD });

    expect(res.requested).toBe(0);
    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 1);
    expect(res.stranded_remediation_invoked).toBe(true);
  });

  test('single-channel teardown path (channel_id) does NOT fire remediation', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: channelItem() });
    mockOauth.mockResolvedValue({ _kind: 'oauth' });
    mockStopWatch.mockResolvedValue(undefined);

    const res = await handler({ tenant_id: 'MYR384719', channel_id: 'ch-1' });

    expect(lambdaMock).toHaveReceivedCommandTimes(InvokeCommand, 0);
    expect(res.stranded_remediation_invoked).toBe(false);
  });

  test('remediation dispatch failure is best-effort: offboarding still succeeds, ARN redacted', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    const denied = new Error(
      'not authorized to perform lambda:InvokeFunction on arn:aws:lambda:us-east-1:525409062831:function:Stranded_Booking_Remediator'
    );
    denied.name = 'AccessDeniedException';
    lambdaMock.on(InvokeCommand).rejects(denied);
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    const res = await handler({ tenant_id: 'MYR384719', coordinator_id: COORD });

    // offboarding itself succeeded; the dispatch failure did not throw out of the handler
    expect(res.requested).toBe(0);
    expect(res.stranded_remediation_invoked).toBe(false);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('stranded_remediation_dispatch_failed'));
    // sanitizeErrorMessage redacts the ARN-bearing AccessDeniedException
    const logged = warnSpy.mock.calls.map((c) => c[0]).join('\n');
    expect(logged).not.toMatch(/arn:aws/);
    expect(logged).toMatch(/redacted/);
    warnSpy.mockRestore();
  });

  test('no REMEDIATOR_FUNCTION_NAME → skip + warn, no invoke (fresh module with env unset)', async () => {
    const saved = process.env.REMEDIATOR_FUNCTION_NAME;
    delete process.env.REMEDIATOR_FUNCTION_NAME;
    let fresh;
    jest.isolateModules(() => { fresh = require('./index'); });
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    const result = await fresh._test.triggerStrandedRemediation('MYR384719', COORD, '2026-06-02T00:00:00Z');

    expect(result).toBe(false);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('stranded_remediation_skipped_no_function_name'));
    warnSpy.mockRestore();
    if (saved !== undefined) process.env.REMEDIATOR_FUNCTION_NAME = saved;
  });
});
