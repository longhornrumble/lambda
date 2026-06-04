// Jest tests for analytics_writer + redactPII + contract fixture.
// See analytics_writer_contract.json for the wire-format invariants
// shared with Master_Function_Staging/analytics_writer.py.

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

// Mock must be installed BEFORE requiring the writer (singleton client).
const ddbMock = mockClient(DynamoDBClient);

process.env.SESSION_SUMMARIES_TABLE = 'picasso-session-summaries';

const {
  writeSessionSummary,
  buildUpdateParams,
  REASON_ENUM,
  ERROR_ENUM,
  SUPPORTED_EVENT_TYPES,
  FIRST_QUESTION_MAX_CHARS,
} = require('../analytics_writer');
const { redactPII } = require('../redactPII');
const contract = require('../analytics_writer_contract.json');

const TTL_DAYS = 365; // mirrors analytics_writer.js (12-month summary retention)
const expectedTtl = (iso) => Math.floor(Date.parse(iso) / 1000) + TTL_DAYS * 86400;

const baseInput = {
  event_type: 'MESSAGE_SENT',
  session_id: 'sess_abc123XYZ',
  tenant_hash: 'my87674d777bf9',
  tenant_id: 'MYR384719',
  client_timestamp: '2026-05-04T20:00:00.000Z',
  request_id: 'req-aaaa-1111',
  event_payload: { first_question: 'How do I apply?' },
};

beforeEach(() => {
  ddbMock.reset();
  ddbMock.on(UpdateItemCommand).resolves({});
});

// ─────────── redactPII ───────────
describe('redactPII', () => {
  test.each(contract.redact_pii_cases.filter(c => c.expected !== undefined))(
    'matches contract case: $input',
    ({ input, expected }) => {
      expect(redactPII(input)).toBe(expected);
    }
  );

  test('50-char truncation boundary (truncation is at call site, not in helper)', () => {
    const c = contract.redact_pii_cases.find(x => x.expected_truncated_50);
    expect(redactPII(c.input)).toBe(c.expected_redacted);
    expect(redactPII(c.input).slice(0, FIRST_QUESTION_MAX_CHARS)).toBe(c.expected_truncated_50);
  });

  test('non-string and empty inputs return empty string', () => {
    expect(redactPII('')).toBe('');
    expect(redactPII(null)).toBe('');
    expect(redactPII(undefined)).toBe('');
    expect(redactPII(42)).toBe('');
  });
});

// ─────────── buildUpdateParams: structural contract ───────────
describe('buildUpdateParams (wire-format contract)', () => {
  const fixturesByName = Object.fromEntries(contract.fixtures.map(f => [f.name, f]));

  test('MESSAGE_SENT_initial matches the fixture exactly', () => {
    const f = fixturesByName.MESSAGE_SENT_initial;
    const { params } = buildUpdateParams(f.input);
    expect(params.Key).toEqual(f.expected.Key);
    expect(params.UpdateExpression).toBe(f.expected.UpdateExpression);
    expect(params.ConditionExpression).toBe(f.expected.ConditionExpression);
    expect(params.ExpressionAttributeNames).toEqual(f.expected.ExpressionAttributeNames);
    // TTL value is a function of client_timestamp; verify dynamically.
    const expectedValues = {
      ...f.expected.ExpressionAttributeValues,
      ':ttl': { N: String(expectedTtl(f.input.client_timestamp)) },
    };
    expect(params.ExpressionAttributeValues).toEqual(expectedValues);
  });

  test('MESSAGE_SENT_no_first_question omits first_question SET clause', () => {
    const f = fixturesByName.MESSAGE_SENT_no_first_question;
    const { params } = buildUpdateParams(f.input);
    expect(params.UpdateExpression).toBe(f.expected.UpdateExpression);
    expect(params.ExpressionAttributeValues[':first_question']).toBeUndefined();
  });

  test('MESSAGE_RECEIVED_with_response_time appends accumulator clauses', () => {
    const f = fixturesByName.MESSAGE_RECEIVED_with_response_time;
    const { params } = buildUpdateParams(f.input);
    expect(params.UpdateExpression).toBe(f.expected.UpdateExpression);
    expect(params.ConditionExpression).toBe(f.expected.ConditionExpression);
    expect(params.ExpressionAttributeValues[':response_time']).toEqual({ N: '850' });
  });

  test('MESSAGE_RECEIVED_response_time_out_of_range omits accumulator clauses', () => {
    const f = fixturesByName.MESSAGE_RECEIVED_response_time_out_of_range;
    const { params } = buildUpdateParams(f.input);
    expect(params.UpdateExpression).toBe(f.expected.UpdateExpression);
    expect(params.ExpressionAttributeValues[':response_time']).toBeUndefined();
  });

  test('FORM_COMPLETED_with_form_id sets outcome + form_id + idempotency marker', () => {
    const f = fixturesByName.FORM_COMPLETED_with_form_id;
    const { params } = buildUpdateParams(f.input);
    expect(params.UpdateExpression).toBe(f.expected.UpdateExpression);
    expect(params.ConditionExpression).toBe(f.expected.ConditionExpression);
    expect(params.ExpressionAttributeNames).toEqual(f.expected.ExpressionAttributeNames);
    expect(params.ExpressionAttributeValues[':outcome']).toEqual({ S: 'form_completed' });
    expect(params.ExpressionAttributeValues[':form_id']).toEqual({ S: 'volunteer_signup' });
  });

  test('TableName is read from process.env.SESSION_SUMMARIES_TABLE', () => {
    const { params } = buildUpdateParams(baseInput);
    expect(params.TableName).toBe('picasso-session-summaries');
  });

  test('placeholder invariant: every :placeholder in UpdateExpression appears in ExpressionAttributeValues', () => {
    for (const fixture of contract.fixtures) {
      if (!SUPPORTED_EVENT_TYPES.has(fixture.input.event_type)) continue;
      const { params, error } = buildUpdateParams(fixture.input);
      if (error) continue;
      const placeholders = params.UpdateExpression.match(/:[A-Za-z_][A-Za-z0-9_]*/g) || [];
      const condPlaceholders = (params.ConditionExpression || '').match(/:[A-Za-z_][A-Za-z0-9_]*/g) || [];
      for (const p of [...placeholders, ...condPlaceholders]) {
        expect(params.ExpressionAttributeValues).toHaveProperty(p);
      }
    }
  });

  test('attribute-name invariant: every #name in expressions appears in ExpressionAttributeNames', () => {
    for (const fixture of contract.fixtures) {
      if (!SUPPORTED_EVENT_TYPES.has(fixture.input.event_type)) continue;
      const { params, error } = buildUpdateParams(fixture.input);
      if (error) continue;
      const names = params.UpdateExpression.match(/#[A-Za-z_][A-Za-z0-9_]*/g) || [];
      for (const n of names) {
        expect(params.ExpressionAttributeNames).toHaveProperty(n);
      }
    }
  });
});

// ─────────── writeSessionSummary: end-to-end ───────────
describe('writeSessionSummary', () => {
  test('happy path: returns true and issues exactly one UpdateItem', async () => {
    const result = await writeSessionSummary(baseInput);
    expect(result).toBe(true);
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
  });

  test('atomicity: every event_type produces exactly one UpdateItem call', async () => {
    for (const event_type of ['MESSAGE_SENT', 'MESSAGE_RECEIVED', 'FORM_COMPLETED']) {
      ddbMock.reset();
      ddbMock.on(UpdateItemCommand).resolves({});
      await writeSessionSummary({ ...baseInput, event_type, request_id: `req-${event_type}` });
      expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
    }
  });

  describe('validation rejections — log reason from REASON_ENUM, no DDB call', () => {
    const cases = [
      ['missing event_type', { ...baseInput, event_type: undefined }, 'missing_event_type'],
      ['unknown event_type', { ...baseInput, event_type: 'CTA_CLICKED' }, 'unknown_event_type'],
      ['missing session_id', { ...baseInput, session_id: '' }, 'missing_session_id'],
      ['malformed session_id (space)', { ...baseInput, session_id: 'sess abc' }, 'invalid_session_id_format'],
      ['malformed session_id (too long)', { ...baseInput, session_id: 'a'.repeat(129) }, 'invalid_session_id_format'],
      ['missing tenant_hash', { ...baseInput, tenant_hash: '' }, 'missing_tenant_hash'],
      ['malformed tenant_hash (too short)', { ...baseInput, tenant_hash: 'abc' }, 'invalid_tenant_hash_format'],
      ['malformed tenant_hash (special chars)', { ...baseInput, tenant_hash: 'tenant_with_us' }, 'invalid_tenant_hash_format'],
      ['missing request_id', { ...baseInput, request_id: '' }, 'request_id_missing'],
    ];

    test.each(cases)('%s', async (_name, input, expectedReason) => {
      const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
      const result = await writeSessionSummary(input);
      expect(result).toBe(false);
      expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 0);
      const logged = logSpy.mock.calls.map(c => c[0]).find(s => s.includes(`"analytics_write_invalid"`));
      expect(logged).toBeDefined();
      const parsed = JSON.parse(logged);
      expect(parsed.reason).toBe(expectedReason);
      expect(REASON_ENUM.has(parsed.reason)).toBe(true);
      logSpy.mockRestore();
    });
  });

  test('runtime DDB error: returns false, log error from ERROR_ENUM, reason omitted', async () => {
    const err = new Error('throttle');
    err.name = 'ThrottlingException';
    ddbMock.on(UpdateItemCommand).rejects(err);
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    const result = await writeSessionSummary(baseInput);
    expect(result).toBe(false);
    const logged = logSpy.mock.calls.map(c => c[0]).find(s => s.includes('analytics_write_failure'));
    expect(logged).toBeDefined();
    const parsed = JSON.parse(logged);
    expect(parsed.error).toBe('ddb_throttle');
    expect(ERROR_ENUM.has(parsed.error)).toBe(true);
    expect(parsed.reason).toBeUndefined();
    logSpy.mockRestore();
  });

  test('ConditionalCheckFailed: logged as duplicate, returns false (benign idempotency)', async () => {
    const err = new Error('cond');
    err.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(err);
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    const result = await writeSessionSummary(baseInput);
    expect(result).toBe(false);
    const logged = logSpy.mock.calls.map(c => c[0]).find(s => s.includes('analytics_write_duplicate'));
    expect(logged).toBeDefined();
    logSpy.mockRestore();
  });

  test('first_question is redacted + truncated to 50 chars before write', async () => {
    const longPiiInput = {
      ...baseInput,
      event_payload: { first_question: 'Email me at jane.doe@example.com — this is more than fifty characters total' },
    };
    let captured;
    ddbMock.on(UpdateItemCommand).callsFake((input) => {
      captured = input;
      return Promise.resolve({});
    });
    await writeSessionSummary(longPiiInput);
    const written = captured.ExpressionAttributeValues[':first_question'].S;
    expect(written).not.toMatch(/jane\.doe@example\.com/);
    expect(written).toMatch(/\[EMAIL\]/);
    expect(written.length).toBeLessThanOrEqual(FIRST_QUESTION_MAX_CHARS);
  });

  test('started_at is set from client_timestamp, never from Date.now()', async () => {
    const past = '2025-01-01T00:00:00.000Z';
    let captured;
    ddbMock.on(UpdateItemCommand).callsFake((input) => {
      captured = input;
      return Promise.resolve({});
    });
    await writeSessionSummary({ ...baseInput, client_timestamp: past });
    expect(captured.ExpressionAttributeValues[':started_at']).toEqual({ S: past });
    expect(captured.ExpressionAttributeValues[':ended_at']).toEqual({ S: past });
  });

  test('forward-compat: writer succeeds against an old-shape row (no last_request_id_* fields)', async () => {
    // Simulate the DDB returning success for an UpdateItem on a row that
    // never had any of the new fields set. ConditionExpression uses
    // attribute_not_exists() → succeeds when the field is absent.
    ddbMock.on(UpdateItemCommand).resolves({});
    const result = await writeSessionSummary(baseInput);
    expect(result).toBe(true);
    const sentInput = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    // Critical invariant: ConditionExpression must have an attribute_not_exists branch.
    expect(sentInput.ConditionExpression).toMatch(/attribute_not_exists/);
  });
});

// ─── Review B2: log shapes lock per contract.log_shapes ───
// Note: global.console.log is a persistent jest.fn() per setup.js — must mockClear()
// before each writer call and read mock.calls directly.
describe('log_shapes contract enforcement (Python A2 parity)', () => {
  const shapes = contract.log_shapes;

  test('analytics_write_invalid: required reason from REASON_ENUM, no error field', async () => {
    console.log.mockClear();
    await writeSessionSummary({ ...baseInput, session_id: 'bad space' });
    const logged = console.log.mock.calls.map((c) => c[0]).find((s) => typeof s === 'string' && s.includes('analytics_write_invalid'));
    const parsed = JSON.parse(logged);
    expect(parsed.evt).toBe('analytics_write_invalid');
    for (const f of shapes.analytics_write_invalid.required_fields) {
      expect(parsed[f]).toBeDefined();
    }
    for (const f of shapes.analytics_write_invalid.forbidden_fields) {
      expect(parsed[f]).toBeUndefined();
    }
    expect(REASON_ENUM.has(parsed.reason)).toBe(true);
  });

  test('analytics_write_failure: required error from ERROR_ENUM, no reason field', async () => {
    const err = new Error('boom');
    err.name = 'ThrottlingException';
    ddbMock.on(UpdateItemCommand).rejects(err);
    console.log.mockClear();
    await writeSessionSummary(baseInput);
    const logged = console.log.mock.calls.map((c) => c[0]).find((s) => typeof s === 'string' && s.includes('analytics_write_failure'));
    const parsed = JSON.parse(logged);
    expect(parsed.evt).toBe('analytics_write_failure');
    for (const f of shapes.analytics_write_failure.required_fields) {
      expect(parsed[f]).toBeDefined();
    }
    for (const f of shapes.analytics_write_failure.forbidden_fields) {
      expect(parsed[f]).toBeUndefined();
    }
    expect(ERROR_ENUM.has(parsed.error)).toBe(true);
  });

  test('analytics_write_duplicate: error must equal ddb_validation, no reason field', async () => {
    const err = new Error('cond');
    err.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(err);
    console.log.mockClear();
    await writeSessionSummary(baseInput);
    const logged = console.log.mock.calls.map((c) => c[0]).find((s) => typeof s === 'string' && s.includes('analytics_write_duplicate'));
    const parsed = JSON.parse(logged);
    for (const f of shapes.analytics_write_duplicate.forbidden_fields) {
      expect(parsed[f]).toBeUndefined();
    }
    expect(parsed.error).toBe(shapes.analytics_write_duplicate.error_must_equal);
  });
});
