'use strict';

/**
 * Unit tests for conversationLock.js (contract C7) — mocked DocumentClient.
 * The handler-level race behavior is covered in
 * conversationLock.integration.test.js.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBDocumentClient, PutCommand, UpdateCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');

const {
  acquireOrCoalesce,
  claimPending,
  releaseIfIdle,
  LOCK_TTL_SECONDS,
  DRAIN_CAP,
} = require('./conversationLock');

const ddbMock = mockClient(DynamoDBDocumentClient);
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const CTX = {
  client,
  tableName: 'picasso-conversation-state',
  sessionId: 'meta:PAGE:PSID',
  owner: 'owner-a',
};
const PENDING_ITEM = { timestamp: 1, mid: 'm_x', text: 'hi' };

function ccfe() {
  const err = new Error('The conditional request failed');
  err.name = 'ConditionalCheckFailedException';
  return err;
}

beforeEach(() => ddbMock.reset());

describe('acquireOrCoalesce', () => {
  test('clean acquire → winner; condition covers fresh AND stale locks', async () => {
    ddbMock.on(PutCommand).resolves({});
    const result = await acquireOrCoalesce({ ...CTX, pendingItem: PENDING_ITEM });
    expect(result).toEqual({ role: 'winner' });
    const put = ddbMock.commandCalls(PutCommand)[0].args[0].input;
    expect(put.ConditionExpression).toBe('attribute_not_exists(sessionId) OR expires_at < :nowSec');
    expect(put.Item.stateType).toBe('lock');
    expect(put.Item.expires_at).toBeGreaterThan(Math.floor(Date.now() / 1000));
    expect(put.Item.pending).toEqual([]);
  });

  test('stale takeover inherits the crashed winner\'s pending (no-drop)', async () => {
    ddbMock.on(PutCommand).resolves({ Attributes: { pending: [{ text: 'orphaned', mid: 'm_o', timestamp: 2 }] } });
    const result = await acquireOrCoalesce({ ...CTX, pendingItem: PENDING_ITEM });
    expect(result.role).toBe('winner');
    expect(result.inheritedPending).toEqual([{ text: 'orphaned', mid: 'm_o', timestamp: 2 }]);
  });

  test('live lock → coalesce via list_append', async () => {
    ddbMock.on(PutCommand).rejects(ccfe());
    ddbMock.on(UpdateCommand).resolves({});
    const result = await acquireOrCoalesce({ ...CTX, pendingItem: PENDING_ITEM });
    expect(result).toEqual({ role: 'coalesced' });
    const upd = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
    expect(upd.UpdateExpression).toContain('list_append(if_not_exists(pending, :empty), :item)');
    expect(upd.ConditionExpression).toBe('attribute_exists(sessionId)');
    expect(upd.ExpressionAttributeValues[':item']).toEqual([PENDING_ITEM]);
  });

  test('append races the release → retries acquisition once and wins (C7 step 2)', async () => {
    ddbMock.on(PutCommand).rejectsOnce(ccfe()).resolves({});
    ddbMock.on(UpdateCommand).rejects(ccfe());
    const result = await acquireOrCoalesce({ ...CTX, pendingItem: PENDING_ITEM });
    expect(result.role).toBe('winner');
    expect(ddbMock).toHaveReceivedCommandTimes(PutCommand, 2);
  });

  test('every race lost → degraded winner (no-drop outranks serialization)', async () => {
    ddbMock.on(PutCommand).rejects(ccfe());
    ddbMock.on(UpdateCommand).rejects(ccfe());
    const result = await acquireOrCoalesce({ ...CTX, pendingItem: PENDING_ITEM });
    expect(result).toEqual({ role: 'winner', degraded: true });
  });

  test('non-conditional DDB error propagates (fail-open handled by the caller)', async () => {
    ddbMock.on(PutCommand).rejects(new Error('network'));
    await expect(acquireOrCoalesce({ ...CTX, pendingItem: PENDING_ITEM })).rejects.toThrow('network');
  });

  test('distinct sessionIds never contend — key is the sessionId', async () => {
    ddbMock.on(PutCommand).resolves({});
    await acquireOrCoalesce({ ...CTX, sessionId: 'meta:PAGE:OTHER', pendingItem: PENDING_ITEM });
    expect(ddbMock.commandCalls(PutCommand)[0].args[0].input.Item.sessionId).toBe('meta:PAGE:OTHER');
  });
});

describe('claimPending', () => {
  test('claims and clears atomically (UPDATED_OLD, owner-guarded)', async () => {
    ddbMock.on(UpdateCommand).resolves({ Attributes: { pending: [PENDING_ITEM] } });
    const items = await claimPending(CTX);
    expect(items).toEqual([PENDING_ITEM]);
    const upd = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
    expect(upd.UpdateExpression).toContain('REMOVE pending');
    expect(upd.ReturnValues).toBe('UPDATED_OLD');
    expect(upd.ConditionExpression).toBe('#owner = :owner');
  });

  test('lock stolen (owner mismatch) → [] — nothing is ours to drain', async () => {
    ddbMock.on(UpdateCommand).rejects(ccfe());
    expect(await claimPending(CTX)).toEqual([]);
  });
});

describe('releaseIfIdle', () => {
  test('releases only when pending is empty (conditional delete)', async () => {
    ddbMock.on(DeleteCommand).resolves({});
    expect(await releaseIfIdle(CTX)).toEqual({ released: true });
    const del = ddbMock.commandCalls(DeleteCommand)[0].args[0].input;
    expect(del.ConditionExpression).toBe(
      '#owner = :owner AND (attribute_not_exists(pending) OR size(pending) = :zero)'
    );
  });

  test('pending raced in → released:false (drain again, never drop-on-release)', async () => {
    ddbMock.on(DeleteCommand).rejects(ccfe());
    expect(await releaseIfIdle(CTX)).toEqual({ released: false });
  });
});

describe('C7 constants', () => {
  test('lock TTL invariant: >= 120s function timeout + 10s margin', () => {
    expect(LOCK_TTL_SECONDS).toBeGreaterThanOrEqual(130);
  });
  test('drain cap is 3 Bedrock cycles (C7 step 5)', () => {
    expect(DRAIN_CAP).toBe(3);
  });
});
