'use strict';

/**
 * Per-conversation serialization — contract C7 (docs/messenger/CONTRACTS.md).
 *
 * Single-writer lock + coalesced pending queue on the picasso-conversation-state
 * table (C4 `lock` row). The webhook async-invokes one processor per inbound
 * message; rapid-fire messages on one sessionId would otherwise run concurrent
 * turns (interleaved replies, history races, doubled Bedrock spend).
 *
 * Frozen semantics (C7):
 *  - Acquire: conditional Put — wins iff no lock row exists OR the existing
 *    lock is stale (expires_at < now; crash self-heal / takeover path).
 *  - Coalesce: the loser appends its message to the winner's `pending` list
 *    and exits without calling Bedrock. If the append races the winner's
 *    release (row deleted), the loser retries acquisition once.
 *  - Drain/claim: the winner atomically claims-and-clears `pending`
 *    (UpdateItem REMOVE, owner-guarded, UPDATED_OLD).
 *  - Release is CONDITIONAL: delete only when `pending` is empty — never
 *    drop-on-release. A failed release means new pending arrived: drain again.
 *  - LOCK TTL INVARIANT: LOCK_TTL_SECONDS >= the processor's Lambda timeout
 *    (120 s, infra/modules/lambda-meta-staging in the picasso repo) + 10 s
 *    margin. Raising the function timeout REQUIRES amending C7 and this
 *    constant together, or a live turn can have its lock stolen mid-run.
 *
 * All functions take the DocumentClient + table name so tests drive them with
 * aws-sdk-client-mock, same as the rest of this Lambda.
 */

const { PutCommand, UpdateCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');

const LOCK_TTL_SECONDS = 130; // C7 invariant — see header before changing
const DRAIN_CAP = 3; // Bedrock-calling drain cycles per lock hold (C7 step 5)
const STATE_TYPE_LOCK = 'lock';

/**
 * Try to become the single writer for this conversation.
 *
 * @returns {Promise<{role: 'winner'|'coalesced', degraded?: boolean}>}
 *   'winner'    — caller runs the turn and MUST drain + release afterwards.
 *   'coalesced' — caller's message was appended to the winner's pending list;
 *                 caller exits WITHOUT calling Bedrock or sending.
 *   degraded:true (winner) — both acquire attempts and both appends lost their
 *   races; proceeding unserialized rather than dropping input (C7 no-drop
 *   guarantee outranks serialization in this pathological corner).
 */
async function acquireOrCoalesce({ client, tableName, sessionId, owner, pendingItem }) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const nowMs = Date.now();
    try {
      const putResult = await client.send(
        new PutCommand({
          TableName: tableName,
          Item: {
            sessionId,
            stateType: STATE_TYPE_LOCK,
            owner,
            acquired_at: nowMs,
            updated_at: nowMs,
            expires_at: Math.floor(nowMs / 1000) + LOCK_TTL_SECONDS,
            schema_version: 1,
            pending: [],
          },
          // No row yet, or the previous holder's lock went stale (crash heal).
          ConditionExpression: 'attribute_not_exists(sessionId) OR expires_at < :nowSec',
          ExpressionAttributeValues: { ':nowSec': Math.floor(nowMs / 1000) },
          // Stale-takeover recovery: if we just overwrote a crashed winner's
          // row, inherit its unanswered pending items instead of dropping
          // them (C7 no-drop guarantee).
          ReturnValues: 'ALL_OLD',
        })
      );
      const inherited = Array.isArray(putResult?.Attributes?.pending)
        ? putResult.Attributes.pending
        : [];
      return inherited.length > 0 ? { role: 'winner', inheritedPending: inherited } : { role: 'winner' };
    } catch (err) {
      if (err.name !== 'ConditionalCheckFailedException') throw err;
    }

    // A live winner holds the lock — coalesce onto its pending list.
    try {
      await client.send(
        new UpdateCommand({
          TableName: tableName,
          Key: { sessionId, stateType: STATE_TYPE_LOCK },
          UpdateExpression:
            'SET pending = list_append(if_not_exists(pending, :empty), :item), updated_at = :now',
          ConditionExpression: 'attribute_exists(sessionId)',
          ExpressionAttributeValues: {
            ':empty': [],
            ':item': [pendingItem],
            ':now': Date.now(),
          },
        })
      );
      return { role: 'coalesced' };
    } catch (err) {
      if (err.name !== 'ConditionalCheckFailedException') throw err;
      // Lock vanished between our Put and our append (winner released) —
      // loop: retry acquisition once (C7 step 2).
    }
  }

  return { role: 'winner', degraded: true };
}

/**
 * Atomically claim-and-clear the pending list (winner only).
 * @returns {Promise<Array<object>>} the claimed pending items ([] if none).
 */
async function claimPending({ client, tableName, sessionId, owner }) {
  try {
    const result = await client.send(
      new UpdateCommand({
        TableName: tableName,
        Key: { sessionId, stateType: STATE_TYPE_LOCK },
        UpdateExpression: 'REMOVE pending SET updated_at = :now',
        ConditionExpression: '#owner = :owner',
        ExpressionAttributeNames: { '#owner': 'owner' },
        ExpressionAttributeValues: { ':owner': owner, ':now': Date.now() },
        ReturnValues: 'UPDATED_OLD',
      })
    );
    return Array.isArray(result?.Attributes?.pending) ? result.Attributes.pending : [];
  } catch (err) {
    if (err.name === 'ConditionalCheckFailedException') {
      // Lock stolen (TTL takeover after a stall) — nothing is ours to drain.
      return [];
    }
    throw err;
  }
}

/**
 * Release the lock iff nothing is pending (C7 step 4 — never drop-on-release).
 * @returns {Promise<{released: boolean}>} released:false ⇒ new pending raced
 *   in between the caller's last claim and this delete: drain again.
 */
async function releaseIfIdle({ client, tableName, sessionId, owner }) {
  try {
    await client.send(
      new DeleteCommand({
        TableName: tableName,
        Key: { sessionId, stateType: STATE_TYPE_LOCK },
        ConditionExpression:
          '#owner = :owner AND (attribute_not_exists(pending) OR size(pending) = :zero)',
        ExpressionAttributeNames: { '#owner': 'owner' },
        ExpressionAttributeValues: { ':owner': owner, ':zero': 0 },
      })
    );
    return { released: true };
  } catch (err) {
    if (err.name === 'ConditionalCheckFailedException') {
      return { released: false };
    }
    throw err;
  }
}

module.exports = {
  acquireOrCoalesce,
  claimPending,
  releaseIfIdle,
  LOCK_TTL_SECONDS,
  DRAIN_CAP,
};
