'use strict';

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
  DynamoDBClient,
  QueryCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { CloudWatchClient, PutMetricDataCommand } = require('@aws-sdk/client-cloudwatch');

// Mock oauth-client + calendar-watch BEFORE requiring index.
const mockOauth = jest.fn();
jest.mock('./oauth-client', () => ({
  getOAuthClient: (...args) => mockOauth(...args),
}));

const mockRegisterWatch = jest.fn();
const mockStopWatch = jest.fn();
jest.mock('./calendar-watch', () => ({
  registerWatch: (...args) => mockRegisterWatch(...args),
  stopWatch: (...args) => mockStopWatch(...args),
}));

process.env.LISTENER_URL = 'https://listener.example/';
process.env.CALENDAR_WATCH_CHANNELS_TABLE = 'picasso-calendar-watch-channels-staging';
process.env.METRIC_NAMESPACE = 'Picasso/Scheduling';
process.env.SCHEDULING_TENANT_IDS = 'MYR384719';

const { handler, _test } = require('./index');

const ddbMock = mockClient(DynamoDBClient);
const cwMock = mockClient(CloudWatchClient);

beforeEach(() => {
  ddbMock.reset();
  cwMock.reset();
  mockOauth.mockReset();
  mockRegisterWatch.mockReset();
  mockStopWatch.mockReset();
});

// ─── fixtures ──────────────────────────────────────────────────────────────────

function channelItem(over = {}) {
  const item = {
    channel_id:           { S: over.channel_id ?? 'old-ch-1' },
    tenant_id:            { S: over.tenant_id ?? 'MYR384719' },
    coordinator_id:       { S: over.coordinator_id ?? 'test-coordinator' },
    calendar_id:          { S: over.calendar_id ?? 'primary' },
    calendar_provider:    { S: over.calendar_provider ?? 'google' },
    channel_token_sha256: { S: 'old-hash' },
    status:               { S: over.status ?? 'active' },
    expiration:           { N: over.expiration ?? '1700000000000' },
  };
  if (over.last_sync_token !== null) {
    item.last_sync_token = { S: over.last_sync_token ?? 'sync-tok-old' };
  }
  if (over.resource_id !== null) {
    item.resource_id = { S: over.resource_id ?? 'old-res-1' };
  }
  return item;
}

function setUpRenewHappy(items = [channelItem()]) {
  mockOauth.mockResolvedValue({ _authClient: 'mock' });
  mockRegisterWatch.mockResolvedValue({
    resourceId: 'new-res',
    resourceUri: 'https://www.googleapis.com/calendar/v3/calendars/primary/events',
    expiration: '1850000000000',
  });
  mockStopWatch.mockResolvedValue(undefined);
  ddbMock.on(QueryCommand).resolves({ Items: items });
  ddbMock.on(PutItemCommand).resolves({});
  ddbMock.on(UpdateItemCommand).resolves({});
  ddbMock.on(DeleteItemCommand).resolves({});
  cwMock.on(PutMetricDataCommand).resolves({});
}

function metricNames() {
  return cwMock.commandCalls(PutMetricDataCommand).map(
    (c) => c.args[0].input.MetricData[0].MetricName
  );
}

// ─── pure helpers ────────────────────────────────────────────────────────────────

describe('resolveTenantIds', () => {
  const savedEnv = process.env.SCHEDULING_TENANT_IDS;
  afterEach(() => { process.env.SCHEDULING_TENANT_IDS = savedEnv; });

  test('uses event.tenant_ids when provided as a non-empty array', () => {
    expect(_test.resolveTenantIds({ tenant_ids: ['T1', 'T2'] })).toEqual(['T1', 'T2']);
  });

  test('parses SCHEDULING_TENANT_IDS as a JSON array', () => {
    process.env.SCHEDULING_TENANT_IDS = '["A1","B2"]';
    expect(_test.resolveTenantIds({})).toEqual(['A1', 'B2']);
  });

  test('parses SCHEDULING_TENANT_IDS as a comma-separated list (trimmed)', () => {
    process.env.SCHEDULING_TENANT_IDS = ' MYR384719 , AUS123957 ';
    expect(_test.resolveTenantIds({})).toEqual(['MYR384719', 'AUS123957']);
  });

  test('throws when no tenant ids anywhere', () => {
    process.env.SCHEDULING_TENANT_IDS = '';
    expect(() => _test.resolveTenantIds({})).toThrow('provide event.tenant_ids or set SCHEDULING_TENANT_IDS');
  });

  test('throws on invalid JSON in SCHEDULING_TENANT_IDS', () => {
    process.env.SCHEDULING_TENANT_IDS = '[not json';
    expect(() => _test.resolveTenantIds({})).toThrow('SCHEDULING_TENANT_IDS is not valid JSON');
  });

  test('throws when the resolved list is empty after trimming', () => {
    process.env.SCHEDULING_TENANT_IDS = ' , , ';
    expect(() => _test.resolveTenantIds({})).toThrow('No valid tenant ids resolved');
  });

  test.each([
    ['../../other-tenant'],
    ['tenant/with/slash'],
    ['tenant with space'],
    ['a'.repeat(65)],
  ])('rejects path-injection / malformed tenant id: %p', (bad) => {
    expect(() => _test.resolveTenantIds({ tenant_ids: [bad] })).toThrow('Invalid tenant_id');
  });
});

describe('resolveBufferMs', () => {
  const savedEnv = process.env.RENEWAL_BUFFER_MS;
  afterEach(() => {
    if (savedEnv === undefined) delete process.env.RENEWAL_BUFFER_MS;
    else process.env.RENEWAL_BUFFER_MS = savedEnv;
  });

  test('uses event.renewal_buffer_ms when a positive integer', () => {
    expect(_test.resolveBufferMs({ renewal_buffer_ms: 12345 })).toBe(12345);
  });

  test.each([0, -5, 1.5, 'abc'])('rejects invalid event.renewal_buffer_ms: %p', (bad) => {
    expect(() => _test.resolveBufferMs({ renewal_buffer_ms: bad }))
      .toThrow('renewal_buffer_ms must be a positive integer');
  });

  test('uses RENEWAL_BUFFER_MS env when no event value', () => {
    process.env.RENEWAL_BUFFER_MS = '5000';
    expect(_test.resolveBufferMs({})).toBe(5000);
  });

  test('rejects invalid RENEWAL_BUFFER_MS env', () => {
    process.env.RENEWAL_BUFFER_MS = '-1';
    expect(() => _test.resolveBufferMs({})).toThrow('positive integer');
  });

  test('defaults to 2 days when neither provided (G2 — must be < ~7d channel lifetime)', () => {
    delete process.env.RENEWAL_BUFFER_MS;
    expect(_test.resolveBufferMs({})).toBe(2 * 24 * 60 * 60 * 1000);
  });
});

describe('generateChannelId / generateChannelToken / sha256Hex', () => {
  test('channel id is a UUID', () => {
    expect(_test.generateChannelId())
      .toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });
  test('channel token is 64 hex chars', () => {
    expect(_test.generateChannelToken()).toMatch(/^[0-9a-f]{64}$/);
  });
  test('sha256Hex is stable', () => {
    expect(_test.sha256Hex('hello'))
      .toBe('2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824');
  });
});

describe('parseChannelRow', () => {
  test('parses a full row', () => {
    const parsed = _test.parseChannelRow(channelItem());
    expect(parsed).toEqual({
      channelId: 'old-ch-1',
      tenantId: 'MYR384719',
      coordinatorId: 'test-coordinator',
      calendarId: 'primary',
      calendarProvider: 'google',
      lastSyncToken: 'sync-tok-old',
      resourceId: 'old-res-1',
      status: 'active',
      expiration: 1700000000000,
    });
  });

  test('defaults optional fields to null / google / 0', () => {
    const parsed = _test.parseChannelRow({ channel_id: { S: 'c' } });
    expect(parsed).toEqual({
      channelId: 'c',
      tenantId: null,
      coordinatorId: null,
      calendarId: null,
      calendarProvider: 'google',
      lastSyncToken: null,
      resourceId: null,
      status: null,
      expiration: 0,
    });
  });
});

describe('queryExpiringChannels', () => {
  test('paginates through LastEvaluatedKey and tolerates a page with no Items', async () => {
    ddbMock.on(QueryCommand)
      .resolvesOnce({ LastEvaluatedKey: { channel_id: { S: 'a' } } }) // no Items on page 1
      .resolves({ Items: [channelItem({ channel_id: 'b' })] });
    const rows = await _test.queryExpiringChannels('MYR384719', 123);
    expect(rows.map((r) => r.channelId)).toEqual(['b']);
    expect(ddbMock.commandCalls(QueryCommand)).toHaveLength(2);
    const q = ddbMock.commandCalls(QueryCommand)[0].args[0].input;
    expect(q.IndexName).toBe('tenant-expiration-index');
    expect(q.KeyConditionExpression).toBe('tenant_id = :t AND expiration <= :th');
    expect(q.ExpressionAttributeValues[':t'].S).toBe('MYR384719');
    expect(q.ExpressionAttributeValues[':th'].N).toBe('123');
  });
});

// ─── handler — happy path ─────────────────────────────────────────────────────────

describe('handler — renewal happy path', () => {
  test('renews an expiring channel: new row written, old channel stopped + old row deleted', async () => {
    setUpRenewHappy();

    const result = await handler({});

    expect(result.scanned).toBe(1);
    expect(result.failed).toHaveLength(0);
    expect(result.renewed).toHaveLength(1);
    expect(result.renewed[0].old_channel_id).toBe('old-ch-1');
    expect(result.renewed[0].new_channel_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(result.renewed[0].expiration).toBe('1850000000000');

    // OAuth fetched per (tenant, coordinator)
    expect(mockOauth).toHaveBeenCalledWith({ tenantId: 'MYR384719', coordinatorId: 'test-coordinator' });

    // events.watch registered a FRESH channel with a 64-hex token at the old calendar
    const watchArgs = mockRegisterWatch.mock.calls[0];
    expect(watchArgs[1]).toBe('primary');
    const newChannelId = watchArgs[2];
    expect(newChannelId).toBe(result.renewed[0].new_channel_id);
    const rawToken = watchArgs[3];
    expect(rawToken).toMatch(/^[0-9a-f]{64}$/);
    expect(watchArgs[4]).toBe('https://listener.example/');

    // New row: hash only, carries last_sync_token, records renewed_from
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(1);
    const put = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(put.ConditionExpression).toBe('attribute_not_exists(channel_id)');
    expect(put.Item.channel_id.S).toBe(newChannelId);
    expect(put.Item.status.S).toBe('active');
    expect(put.Item.expiration.N).toBe('1850000000000');
    expect(put.Item.last_sync_token.S).toBe('sync-tok-old');
    expect(put.Item.renewed_from.S).toBe('old-ch-1');
    expect(put.Item.resource_id.S).toBe('new-res');
    expect(put.Item.channel_token_sha256.S)
      .toBe(crypto.createHash('sha256').update(rawToken, 'utf8').digest('hex'));
    expect(JSON.stringify(put.Item)).not.toContain(rawToken);

    // Old channel revoked + old row deleted
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
    expect(mockStopWatch.mock.calls[0][1]).toBe('old-ch-1');
    expect(mockStopWatch.mock.calls[0][2]).toBe('old-res-1');
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
    expect(ddbMock.commandCalls(DeleteItemCommand)[0].args[0].input.Key.channel_id.S).toBe('old-ch-1');

    // No status flip on success; heartbeat emitted, no failure metric
    expect(ddbMock.commandCalls(UpdateItemCommand)).toHaveLength(0);
    expect(metricNames()).toEqual(['CalendarWatchRenewerRunCompleted']);
  });

  test('no expiring channels → scanned 0, heartbeat still emitted (invoked with no payload)', async () => {
    setUpRenewHappy([]);
    const result = await handler(); // EventBridge Scheduler may invoke with no/empty payload
    expect(result.scanned).toBe(0);
    expect(result.renewed).toHaveLength(0);
    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(metricNames()).toEqual(['CalendarWatchRenewerRunCompleted']);
  });

  test('new row omits resource_id / resource_uri when the watch response lacks them', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockResolvedValue({ resourceId: null, resourceUri: null, expiration: '1850000000000' });
    const result = await handler({});
    expect(result.renewed).toHaveLength(1);
    const put = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(put.Item.resource_id).toBeUndefined();
    expect(put.Item.resource_uri).toBeUndefined();
  });

  test('omits last_sync_token attribute when the old row has none; still renews + deletes old row', async () => {
    setUpRenewHappy([channelItem({ last_sync_token: null, resource_id: null })]);
    const result = await handler({});
    expect(result.renewed).toHaveLength(1);
    const put = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(put.Item.last_sync_token).toBeUndefined();
    // new row still records its own resource_id from the watch response
    expect(put.Item.resource_id.S).toBe('new-res');
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
  });

  test('processes multiple tenants; a per-tenant query failure does not abort the others', async () => {
    mockOauth.mockResolvedValue({ _authClient: 'mock' });
    mockRegisterWatch.mockResolvedValue({ resourceId: 'r', resourceUri: 'u', expiration: '1850000000000' });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock.on(PutItemCommand).resolves({});
    ddbMock.on(DeleteItemCommand).resolves({});
    cwMock.on(PutMetricDataCommand).resolves({});
    ddbMock.on(QueryCommand).callsFake(async (input) => {
      const t = input.ExpressionAttributeValues[':t'].S;
      if (t === 'T1') throw new Error('query boom');
      return { Items: [channelItem({ tenant_id: 'T2', channel_id: 'ch-T2', coordinator_id: 'c2' })] };
    });

    const result = await handler({ tenant_ids: ['T1', 'T2'] });

    expect(result.failed).toHaveLength(1);
    expect(result.failed[0]).toMatchObject({ tenant_id: 'T1', channel_id: null });
    expect(result.renewed).toHaveLength(1);
    expect(result.renewed[0].old_channel_id).toBe('ch-T2');
    // failure metric (tenant T1 query) + heartbeat
    expect(metricNames()).toEqual(
      expect.arrayContaining(['CalendarWatchRenewalFailed', 'CalendarWatchRenewerRunCompleted'])
    );
  });
});

// ─── handler — partial-failure recovery (self-healing) ─────────────────────────────

describe('handler — renewal failure & self-healing', () => {
  test('write-new-row fails AFTER watch → revoke new channel, mark OLD row failed, OLD row NOT deleted', async () => {
    setUpRenewHappy();
    ddbMock.on(PutItemCommand).rejects(new Error('ddb throttled'));

    const result = await handler({});

    expect(result.renewed).toHaveLength(0);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].channel_id).toBe('old-ch-1');

    // Compensation revoked the NEW channel (resource id from the live watch)
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
    expect(mockStopWatch.mock.calls[0][2]).toBe('new-res');

    // OLD row flipped to unwatched_renewal_failed, guarded by attribute_exists
    const updates = ddbMock.commandCalls(UpdateItemCommand);
    expect(updates).toHaveLength(1);
    expect(updates[0].args[0].input.Key.channel_id.S).toBe('old-ch-1');
    expect(updates[0].args[0].input.ConditionExpression).toBe('attribute_exists(channel_id) AND tenant_id = :t');
    expect(updates[0].args[0].input.ExpressionAttributeValues[':failed'].S).toBe('unwatched_renewal_failed');
    expect(updates[0].args[0].input.ExpressionAttributeValues[':t'].S).toBe('MYR384719');

    // SELF-HEALING: old row is NOT deleted, so the next run re-queries + retries it
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0);

    expect(metricNames()).toEqual(
      expect.arrayContaining(['CalendarWatchRenewalFailed', 'CalendarWatchRenewerRunCompleted'])
    );
  });

  test('self-healing across runs: a failed run leaves the old row; the NEXT run renews it', async () => {
    setUpRenewHappy();
    ddbMock.on(PutItemCommand).rejectsOnce(new Error('transient')).resolves({});

    const r1 = await handler({});
    expect(r1.failed).toHaveLength(1);
    const deletesAfterRun1 = ddbMock.commandCalls(DeleteItemCommand).length;
    expect(deletesAfterRun1).toBe(0); // old row preserved for retry

    const r2 = await handler({});
    expect(r2.renewed).toHaveLength(1);
    expect(r2.renewed[0].old_channel_id).toBe('old-ch-1');
    // run 2 succeeded → old row now deleted
    expect(ddbMock.commandCalls(DeleteItemCommand).length).toBe(1);
  });

  test('write-new-row fails AND compensation stop fails → original error recorded, run still completes', async () => {
    setUpRenewHappy();
    ddbMock.on(PutItemCommand).rejects(new Error('ddb down'));
    mockStopWatch.mockRejectedValue(new Error('stop also failed'));

    const result = await handler({});
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error).toContain('ddb down');
    expect(metricNames()).toContain('CalendarWatchRenewerRunCompleted');
  });

  test('events.watch failure → no row write, no compensation, OLD row marked failed', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockRejectedValue(new Error('Google quota exceeded'));

    const result = await handler({});
    expect(result.failed[0].error).toContain('Google quota exceeded');
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).not.toHaveBeenCalled(); // nothing created to revoke
    expect(ddbMock.commandCalls(UpdateItemCommand)).toHaveLength(1); // old row flipped
  });

  test('null expiration from watch → throws + compensates (no row write)', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockResolvedValue({ resourceId: 'res-x', resourceUri: 'u', expiration: null });

    const result = await handler({});
    expect(result.failed[0].error).toContain('non-future expiration');
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
    expect(mockStopWatch.mock.calls[0][2]).toBe('res-x');
  });

  test('undefined expiration from watch → throws + compensates', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockResolvedValue({ resourceId: 'res-u', resourceUri: 'u' }); // expiration undefined
    const result = await handler({});
    expect(result.failed[0].error).toContain('non-future expiration');
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
  });

  test('non-future expiration string from watch → throws + compensates', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockResolvedValue({ resourceId: 'res-s', resourceUri: 'u', expiration: 'soon' });
    const result = await handler({});
    expect(result.failed[0].error).toContain('non-future expiration');
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
  });

  test('renewal succeeds even if revoking the OLD channel fails (best-effort)', async () => {
    setUpRenewHappy();
    // old-channel stop rejects; write already succeeded
    mockStopWatch.mockRejectedValue(new Error('old channel already gone (404)'));

    const result = await handler({});
    expect(result.renewed).toHaveLength(1);
    // old row delete still attempted despite the stop failure
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(1);
  });

  test('renewal succeeds even if deleting the OLD row fails (best-effort)', async () => {
    setUpRenewHappy();
    ddbMock.on(DeleteItemCommand).rejects(new Error('delete throttled'));

    const result = await handler({});
    expect(result.renewed).toHaveLength(1);
    expect(result.failed).toHaveLength(0);
  });

  test('tenant query failure where the metric + heartbeat emits ALSO fail → run still completes', async () => {
    // Exercises the best-effort .catch on both the query-path failure metric
    // and the end-of-run heartbeat metric.
    ddbMock.on(QueryCommand).rejects(new Error('query boom'));
    cwMock.on(PutMetricDataCommand).rejects(new Error('cw down'));

    const result = await handler({});
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error).toContain('query_failed');
    expect(result.renewed).toHaveLength(0);
  });

  test('renewal failure where markRenewalFailed + metric + heartbeat ALL fail → run still completes', async () => {
    // Exercises the best-effort .catch on markRenewalFailed, the per-channel
    // failure metric, and the heartbeat metric simultaneously.
    setUpRenewHappy();
    mockRegisterWatch.mockRejectedValue(new Error('watch failed'));
    ddbMock.on(UpdateItemCommand).rejects(new Error('update down'));
    cwMock.on(PutMetricDataCommand).rejects(new Error('cw down'));

    const result = await handler({});
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error).toContain('watch failed');
  });
});

// ─── handler — env requirements ─────────────────────────────────────────────────

describe('handler — audit remediation (G3/G4/G5/G6/G12)', () => {
  test('G3: row with invalid coordinator_id → fails before any Google call', async () => {
    setUpRenewHappy([channelItem({ coordinator_id: 'bad/../traversal' })]);
    const result = await handler({});
    expect(result.renewed).toHaveLength(0);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error).toMatch(/invalid coordinator_id/);
    expect(mockOauth).not.toHaveBeenCalled();
    expect(mockRegisterWatch).not.toHaveBeenCalled();
    expect(ddbMock.commandCalls(UpdateItemCommand)).toHaveLength(1); // old row flipped to failed
  });

  test('G3: row with invalid tenant_id → fails before any Google call', async () => {
    setUpRenewHappy([channelItem({ tenant_id: 'bad tenant!' })]);
    const result = await handler({ tenant_ids: ['MYR384719'] });
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error).toMatch(/invalid tenant_id/);
    expect(mockRegisterWatch).not.toHaveBeenCalled();
  });

  test('G4: resourceId null + new-row write fails → compensation SKIPPED (no stopWatch), old row preserved', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockResolvedValue({ resourceId: null, resourceUri: null, expiration: '1850000000000' });
    ddbMock.on(PutItemCommand).rejects(new Error('ddb throttled'));
    const result = await handler({});
    expect(result.failed).toHaveLength(1);
    expect(mockStopWatch).not.toHaveBeenCalled(); // cannot revoke without a resourceId
    expect(ddbMock.commandCalls(DeleteItemCommand)).toHaveLength(0); // self-healing: old row stays
  });

  test('G5: past/near-zero expiration from watch → rejected + compensates (no row write)', async () => {
    setUpRenewHappy();
    mockRegisterWatch.mockResolvedValue({ resourceId: 'res-past', resourceUri: 'u', expiration: '1' });
    const result = await handler({});
    expect(result.failed[0].error).toMatch(/non-future expiration/);
    expect(ddbMock.commandCalls(PutItemCommand)).toHaveLength(0);
    expect(mockStopWatch).toHaveBeenCalledTimes(1);
    expect(mockStopWatch.mock.calls[0][2]).toBe('res-past');
  });

  test('G6: deleteOldRow + markRenewalFailed carry a tenant-ownership ConditionExpression', async () => {
    setUpRenewHappy();
    await handler({});
    const del = ddbMock.commandCalls(DeleteItemCommand)[0].args[0].input;
    expect(del.ConditionExpression).toBe('tenant_id = :t');
    expect(del.ExpressionAttributeValues[':t'].S).toBe('MYR384719');
  });

  test('G12: partial batch — channel 1 renews, channel 2 watch fails; handled independently', async () => {
    mockOauth.mockResolvedValue({ _authClient: 'mock' });
    mockStopWatch.mockResolvedValue(undefined);
    ddbMock.on(QueryCommand).resolves({ Items: [
      channelItem({ channel_id: 'ch-1', coordinator_id: 'c1' }),
      channelItem({ channel_id: 'ch-2', coordinator_id: 'c2' }),
    ] });
    ddbMock.on(PutItemCommand).resolves({});
    ddbMock.on(UpdateItemCommand).resolves({});
    ddbMock.on(DeleteItemCommand).resolves({});
    cwMock.on(PutMetricDataCommand).resolves({});
    mockRegisterWatch
      .mockResolvedValueOnce({ resourceId: 'r1', resourceUri: 'u', expiration: '1850000000000' })
      .mockRejectedValueOnce(new Error('quota exceeded on ch-2'));

    const result = await handler({});
    expect(result.scanned).toBe(2);
    expect(result.renewed).toHaveLength(1);
    expect(result.renewed[0].old_channel_id).toBe('ch-1');
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].channel_id).toBe('ch-2');
    expect(result.failed[0].error).toMatch(/quota exceeded on ch-2/);
  });
});

describe('handler — env requirements', () => {
  function withEnv(overrides, fn) {
    const saved = { ...process.env };
    Object.assign(process.env, overrides);
    for (const k of Object.keys(overrides)) {
      if (overrides[k] === undefined) delete process.env[k];
    }
    return jest.isolateModulesAsync(async () => {
      const { handler: freshHandler } = require('./index');
      await fn(freshHandler);
    }).finally(() => {
      process.env = saved;
    });
  }

  test('throws if CALENDAR_WATCH_CHANNELS_TABLE is unset', async () => {
    await withEnv({ CALENDAR_WATCH_CHANNELS_TABLE: undefined }, async (h) => {
      await expect(h({})).rejects.toThrow('CALENDAR_WATCH_CHANNELS_TABLE env var is required');
    });
  });

  test('throws if LISTENER_URL is unset', async () => {
    await withEnv({ LISTENER_URL: undefined }, async (h) => {
      await expect(h({})).rejects.toThrow('LISTENER_URL env var is required and must be https://');
    });
  });

  test('throws if LISTENER_URL is not https', async () => {
    await withEnv({ LISTENER_URL: 'http://insecure.example/' }, async (h) => {
      await expect(h({})).rejects.toThrow('must be https://');
    });
  });
});
