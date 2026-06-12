/**
 * DynamoDB registry access for Attribution entry-point records (C3).
 *
 * Table: env ENTRY_POINTS_TABLE (e.g. picasso-entry-points-staging)
 * PK: tenant_id (S)
 * SK: entry_point_id (S)
 *
 * PII rule (C8.14): NO person fields ever written — no created_by, no emails.
 * Write uses a conditional put (attribute_not_exists) to prevent double-mint.
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
} from '@aws-sdk/lib-dynamodb';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';

let _docClient = null;

/**
 * Lazily initialise the DynamoDB document client.
 * Separated so tests can inject a mock client via setDocClient().
 */
function getDocClient() {
  if (!_docClient) {
    const raw = new DynamoDBClient({ region: process.env.AWS_REGION ?? 'us-east-1' });
    _docClient = DynamoDBDocumentClient.from(raw);
  }
  return _docClient;
}

/**
 * Override the DynamoDB document client (used by tests).
 * @param {import('@aws-sdk/lib-dynamodb').DynamoDBDocumentClient} client
 */
export function setDocClient(client) {
  _docClient = client;
}

/**
 * Write a new entry-point registry record with conditional put.
 * Throws ConditionalCheckFailedException if the (tenant_id, entry_point_id) key already exists.
 *
 * @param {object} record - C3 shape (no person fields)
 * @returns {Promise<void>}
 */
export async function putEntryPoint(record) {
  const tableName = process.env.ENTRY_POINTS_TABLE;
  if (!tableName) throw new Error('ENTRY_POINTS_TABLE env var is not set');

  const client = getDocClient();
  await client.send(new PutCommand({
    TableName: tableName,
    Item: record,
    ConditionExpression: 'attribute_not_exists(tenant_id) AND attribute_not_exists(entry_point_id)',
  }));
}

/**
 * Get an entry-point registry record by (tenant_id, entry_point_id).
 * Returns null if not found.
 *
 * @param {string} tenantId
 * @param {string} entryPointId
 * @returns {Promise<object|null>}
 */
export async function getEntryPoint(tenantId, entryPointId) {
  const tableName = process.env.ENTRY_POINTS_TABLE;
  if (!tableName) throw new Error('ENTRY_POINTS_TABLE env var is not set');

  const client = getDocClient();
  const result = await client.send(new GetCommand({
    TableName: tableName,
    Key: { tenant_id: tenantId, entry_point_id: entryPointId },
  }));
  return result.Item ?? null;
}

// Re-export so callers don't need a separate aws-sdk import
export { ConditionalCheckFailedException };
