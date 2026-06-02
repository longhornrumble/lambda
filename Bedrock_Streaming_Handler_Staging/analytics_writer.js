// Server-side analytics writer for picasso-session-summaries-{env}.
// Single atomic UpdateItem per call. Per-event-type ConditionExpression
// provides idempotency via last_request_id_<event>. See v7 plan
// §"PR A" and analytics_writer_contract.json for the wire-format
// invariants both this and Master_Function_Staging/analytics_writer.py
// must satisfy.

const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

const REGION = process.env.AWS_REGION || 'us-east-1';
const ddb = new DynamoDBClient({ region: REGION });

const SESSION_ID_RE = /^[a-zA-Z0-9_-]{1,128}$/;
const TENANT_HASH_RE = /^[a-zA-Z0-9]{10,20}$/;
const FIRST_QUESTION_MAX_CHARS = 50;
const TTL_DAYS = 365; // 12-month pseudonymized-summary retention (data-retention-strategy.md §2/§9; DDB-only)

const REASON_ENUM = new Set([
  'invalid_session_id_format',
  'invalid_tenant_hash_format',
  'missing_session_id',
  'missing_tenant_hash',
  'missing_event_type',
  'unknown_event_type',
  'ttl_calc_failed',
  'redact_pii_failed',
  'request_id_missing',
]);

const ERROR_ENUM = new Set([
  'ddb_throttle',
  'ddb_validation',
  'ddb_resource_not_found',
  'ddb_unknown',
  'iam_access_denied',
  'network_timeout',
  'circuit_breaker_open',
  'internal_error',
]);

const SUPPORTED_EVENT_TYPES = new Set(['MESSAGE_SENT', 'MESSAGE_RECEIVED', 'FORM_COMPLETED']);

function logStructured(state, fields) {
  // Reason/error are enum members; raw user input is never interpolated here.
  console.log(JSON.stringify({ evt: `analytics_write_${state}`, ...fields }));
}

function classifyError(err) {
  const name = err && err.name ? String(err.name) : '';
  if (name === 'ProvisionedThroughputExceededException' || name === 'ThrottlingException') return 'ddb_throttle';
  if (name === 'ValidationException') return 'ddb_validation';
  if (name === 'ResourceNotFoundException') return 'ddb_resource_not_found';
  if (name === 'AccessDeniedException') return 'iam_access_denied';
  if (name === 'TimeoutError' || name === 'NetworkingError') return 'network_timeout';
  if (name === 'ConditionalCheckFailedException') return 'ddb_validation'; // legitimate idempotency rejection
  if (name && name.startsWith('DynamoDB')) return 'ddb_unknown';
  return 'internal_error';
}

function ttlFromTimestamp(isoTimestamp) {
  // Unix-seconds, 90 days from started_at. DDB TTL convention.
  const ms = Date.parse(isoTimestamp);
  if (Number.isNaN(ms)) return null;
  return Math.floor(ms / 1000) + TTL_DAYS * 24 * 60 * 60;
}

function buildUpdateParams({ event_type, session_id, tenant_hash, tenant_id, client_timestamp, request_id, event_payload }) {
  const ttl = ttlFromTimestamp(client_timestamp);
  if (ttl === null) {
    return { error: 'ttl_calc_failed' };
  }

  // Common SET parts present on every event_type.
  const setParts = [
    'ended_at = :ended_at',
    'session_id = if_not_exists(session_id, :session_id)',
    'tenant_id = if_not_exists(tenant_id, :tenant_id)',
    'started_at = if_not_exists(started_at, :started_at)',
    '#ttl = :ttl',
  ];
  const addParts = [];
  const expressionValues = {
    ':ended_at': { S: client_timestamp },
    ':session_id': { S: session_id },
    ':tenant_id': { S: tenant_id || '' },
    ':started_at': { S: client_timestamp },
    ':ttl': { N: String(ttl) },
    ':request_id': { S: request_id },
  };
  const expressionNames = { '#ttl': 'ttl' };
  let conditionExpression;

  if (event_type === 'MESSAGE_SENT') {
    expressionValues[':one'] = { N: '1' };
    addParts.push('message_count :one', 'user_message_count :one');
    setParts.push('last_request_id_message_sent = :request_id');
    conditionExpression =
      'attribute_not_exists(last_request_id_message_sent) OR last_request_id_message_sent <> :request_id';

    const payload = event_payload || {};
    const raw = typeof payload.first_question === 'string' ? payload.first_question : '';
    if (raw.length > 0) {
      let redacted;
      try {
        const { redactPII } = require('./redactPII');
        redacted = redactPII(raw).slice(0, FIRST_QUESTION_MAX_CHARS);
      } catch (_e) {
        return { error: 'redact_pii_failed' };
      }
      setParts.push('first_question = if_not_exists(first_question, :first_question)');
      expressionValues[':first_question'] = { S: redacted };
    }
  } else if (event_type === 'MESSAGE_RECEIVED') {
    expressionValues[':one'] = { N: '1' };
    addParts.push('message_count :one', 'bot_message_count :one');
    setParts.push('last_request_id_message_received = :request_id');
    conditionExpression =
      'attribute_not_exists(last_request_id_message_received) OR last_request_id_message_received <> :request_id';

    const payload = event_payload || {};
    const rt = Number(payload.response_time_ms);
    if (Number.isFinite(rt) && rt > 0 && rt < 60000) {
      addParts.push('total_response_time_ms :response_time', 'response_count :one');
      expressionValues[':response_time'] = { N: String(Math.floor(rt)) };
    }
  } else if (event_type === 'FORM_COMPLETED') {
    setParts.push('#outcome = :outcome', 'last_request_id_form_completed = :request_id');
    expressionNames['#outcome'] = 'outcome';
    expressionValues[':outcome'] = { S: 'form_completed' };
    conditionExpression =
      'attribute_not_exists(last_request_id_form_completed) OR last_request_id_form_completed <> :request_id';

    const payload = event_payload || {};
    if (payload.form_id) {
      setParts.push('form_id = :form_id');
      expressionValues[':form_id'] = { S: String(payload.form_id) };
    }
  } else {
    return { error: 'unknown_event_type' };
  }

  let updateExpression = `SET ${setParts.join(', ')}`;
  if (addParts.length > 0) {
    updateExpression += ` ADD ${addParts.join(', ')}`;
  }

  return {
    params: {
      TableName: process.env.SESSION_SUMMARIES_TABLE,
      Key: {
        pk: { S: `TENANT#${tenant_hash}` },
        sk: { S: `SESSION#${session_id}` },
      },
      UpdateExpression: updateExpression,
      ConditionExpression: conditionExpression,
      ExpressionAttributeNames: expressionNames,
      ExpressionAttributeValues: expressionValues,
    },
  };
}

async function writeSessionSummary(input) {
  const { event_type, session_id, tenant_hash, request_id } = input || {};

  if (!event_type) {
    logStructured('invalid', { reason: 'missing_event_type' });
    return false;
  }
  if (!SUPPORTED_EVENT_TYPES.has(event_type)) {
    logStructured('invalid', { reason: 'unknown_event_type', event_type });
    return false;
  }
  if (!session_id) {
    logStructured('invalid', { reason: 'missing_session_id', event_type });
    return false;
  }
  if (!SESSION_ID_RE.test(session_id)) {
    logStructured('invalid', { reason: 'invalid_session_id_format', event_type });
    return false;
  }
  if (!tenant_hash) {
    logStructured('invalid', { reason: 'missing_tenant_hash', event_type });
    return false;
  }
  if (!TENANT_HASH_RE.test(tenant_hash)) {
    logStructured('invalid', { reason: 'invalid_tenant_hash_format', event_type });
    return false;
  }
  if (!request_id) {
    logStructured('invalid', { reason: 'request_id_missing', event_type });
    return false;
  }

  const built = buildUpdateParams(input);
  if (built.error) {
    logStructured('invalid', { reason: built.error, event_type });
    return false;
  }

  try {
    await ddb.send(new UpdateItemCommand(built.params));
    return true;
  } catch (err) {
    // ConditionalCheckFailedException = benign idempotency rejection (duplicate request_id).
    // Other errors = real failures. Both log paths fill `error` from the error enum;
    // `reason` is reserved for validation-time classifications (REASON_ENUM) and is
    // omitted on runtime-failure paths.
    if (err && err.name === 'ConditionalCheckFailedException') {
      logStructured('duplicate', { error: 'ddb_validation', event_type });
    } else {
      logStructured('failure', { error: classifyError(err), event_type });
    }
    return false;
  }
}

module.exports = {
  writeSessionSummary,
  buildUpdateParams,
  REASON_ENUM,
  ERROR_ENUM,
  SUPPORTED_EVENT_TYPES,
  FIRST_QUESTION_MAX_CHARS,
};
