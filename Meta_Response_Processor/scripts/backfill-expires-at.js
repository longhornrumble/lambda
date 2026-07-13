#!/usr/bin/env node
'use strict';

/**
 * One-off backfill (M1b): recent-messages rows written before the TTL fix
 * used `ttl` as the field name, but the table's TTL attribute is
 * `expires_at` (see storeConversationContext in ../index.js) — those legacy
 * rows never expire. This script copies ttl -> expires_at (same instant) for
 * Messenger rows ONLY (sessionId begins_with 'meta:'). picasso-recent-messages
 * is SHARED with live widget chat — touching a non-meta: row is a bug, not a
 * feature, so this hard-fails rather than silently proceeding if that ever
 * happens.
 *
 * DRY-RUN BY DEFAULT — prints what it would do. Pass --execute to write.
 *
 * Usage:
 *   node scripts/backfill-expires-at.js            # dry run
 *   node scripts/backfill-expires-at.js --execute  # applies the writes
 *
 * Env:
 *   RECENT_MESSAGES_TABLE — defaults to 'picasso-recent-messages' (same
 *                            default as index.js)
 *   AWS_REGION             — defaults to 'us-east-1'
 */

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, ScanCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const RECENT_MESSAGES_TABLE = process.env.RECENT_MESSAGES_TABLE || 'picasso-recent-messages';
const EXECUTE = process.argv.includes('--execute');

const dynamodb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: AWS_REGION }));

/**
 * Scan for meta:-prefixed rows that have `ttl` but not yet `expires_at`.
 * @returns {Promise<Array<object>>}
 */
async function scanCandidates() {
  const items = [];
  let ExclusiveStartKey;

  do {
    const result = await dynamodb.send(
      new ScanCommand({
        TableName: RECENT_MESSAGES_TABLE,
        FilterExpression:
          'begins_with(sessionId, :meta) AND attribute_exists(#ttl) AND attribute_not_exists(expires_at)',
        ExpressionAttributeNames: { '#ttl': 'ttl' },
        ExpressionAttributeValues: { ':meta': 'meta:' },
        ExclusiveStartKey,
      })
    );
    items.push(...(result.Items || []));
    ExclusiveStartKey = result.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return items;
}

async function main() {
  console.log(
    `[backfill-expires-at] table=${RECENT_MESSAGES_TABLE} mode=${EXECUTE ? 'EXECUTE' : 'DRY-RUN'}`
  );

  const candidates = await scanCandidates();
  console.log(`[backfill-expires-at] found ${candidates.length} candidate row(s)`);

  let touched = 0;
  for (const item of candidates) {
    // Belt-and-suspenders: the scan filter already restricts to meta:, but a
    // shared-table bug here would be silent data loss on live widget chat —
    // hard-fail rather than proceed.
    if (!item.sessionId || !item.sessionId.startsWith('meta:')) {
      throw new Error(`Refusing to touch non-meta sessionId: ${item.sessionId}`);
    }
    if (typeof item.messageTimestamp !== 'number') {
      console.warn(
        `[backfill-expires-at] skipping row with no numeric messageTimestamp (sessionId=${item.sessionId})`
      );
      continue;
    }

    console.log(
      `${EXECUTE ? '[EXECUTE]' : '[DRY-RUN]'} sessionId=${item.sessionId} ` +
        `messageTimestamp=${item.messageTimestamp} ttl=${item.ttl} -> expires_at=${item.ttl}`
    );

    if (EXECUTE) {
      await dynamodb.send(
        new UpdateCommand({
          TableName: RECENT_MESSAGES_TABLE,
          Key: { sessionId: item.sessionId, messageTimestamp: item.messageTimestamp },
          UpdateExpression: 'SET expires_at = :e',
          ConditionExpression: 'attribute_not_exists(expires_at)',
          ExpressionAttributeValues: { ':e': item.ttl },
        })
      );
    }
    touched++;
  }

  console.log(`[backfill-expires-at] ${EXECUTE ? 'updated' : 'would update'} ${touched} row(s)`);
}

main().catch((err) => {
  console.error('[backfill-expires-at] FAILED', err);
  process.exit(1);
});
