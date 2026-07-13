'use strict';

/**
 * Unit tests for the M7a form engine (formEngine.js) — pure logic (flatten/
 * validate/prompt/summary/parse) plus the DDB-row CRUD and MFS-invoke helpers
 * against mocked clients. Full handler-level E2E tests (drain routing,
 * escalation/rate-limit precedence, PIC1 wiring) live in index.test.js;
 * mid-form C7 race coverage lives in conversationLock.integration.test.js.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBDocumentClient, GetCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const ddbMock = mockClient(DynamoDBDocumentClient);
const lambdaMock = mockClient(LambdaClient);
const smMock = mockClient(SecretsManagerClient);

const formEngine = require('./formEngine');

beforeEach(() => {
  ddbMock.reset();
  lambdaMock.reset();
  smMock.reset();
  formEngine.resetCfOriginSecretCacheForTests();
  delete process.env.MFS_CF_ORIGIN_SECRET_NAME;
});

const SAMPLE_FORM = {
  enabled: true,
  form_id: 'volunteer_apply',
  program: 'volunteer',
  title: 'Volunteer Application',
  description: 'Apply to volunteer',
  fields: [
    {
      id: 'full_name',
      type: 'name',
      label: 'Your name',
      prompt: 'What is your name?',
      required: true,
      subfields: [
        { id: 'first', label: 'First name', required: true, type: 'text' },
        { id: 'last', label: 'Last name', required: true, type: 'text' },
      ],
    },
    { id: 'email', type: 'email', label: 'Email', prompt: 'What is your email address?', required: true },
    {
      id: 'interest',
      type: 'select',
      label: 'Area of interest',
      prompt: 'Which program interests you?',
      required: true,
      options: [
        { value: 'mentoring', label: 'Mentoring' },
        { value: 'tutoring', label: 'Tutoring' },
      ],
    },
    { id: 'notes', type: 'textarea', label: 'Notes', prompt: 'Anything else we should know?', required: false },
  ],
};

const CONFIG = { conversational_forms: { volunteer_apply: SAMPLE_FORM } };
const SESSION_ID = 'meta:PAGE_1:PSID_1';

describe('flattenSteps', () => {
  test('expands composite subfields into sequential steps, keeps simple fields as one step', () => {
    const steps = formEngine.flattenSteps(SAMPLE_FORM);
    expect(steps.map((s) => s.key)).toEqual(['full_name.first', 'full_name.last', 'email', 'interest', 'notes']);
    expect(steps[0].parentId).toBe('full_name');
    expect(steps[2].parentId).toBeNull();
  });
});

describe('validateAnswer', () => {
  const steps = formEngine.flattenSteps(SAMPLE_FORM);
  const emailStep = steps.find((s) => s.key === 'email');
  const interestStep = steps.find((s) => s.key === 'interest');
  const notesStep = steps.find((s) => s.key === 'notes');

  test('required + empty -> invalid', () => {
    const r = formEngine.validateAnswer(emailStep, '   ');
    expect(r.valid).toBe(false);
  });

  test('optional + empty -> valid', () => {
    const r = formEngine.validateAnswer(notesStep, '');
    expect(r.valid).toBe(true);
  });

  test('email: rejects malformed, accepts + lowercases valid', () => {
    expect(formEngine.validateAnswer(emailStep, 'not-an-email').valid).toBe(false);
    const ok = formEngine.validateAnswer(emailStep, 'Jane@Example.COM');
    expect(ok.valid).toBe(true);
    expect(ok.value).toBe('jane@example.com');
  });

  test('select: accepts typed label case-insensitively (C9 free-text fallback), maps to option value', () => {
    const r = formEngine.validateAnswer(interestStep, 'MENTORING');
    expect(r.valid).toBe(true);
    expect(r.value).toBe('mentoring');
  });

  test('select: rejects a value not in the option list', () => {
    const r = formEngine.validateAnswer(interestStep, 'cooking');
    expect(r.valid).toBe(false);
  });

  test('tenant-configured validation.pattern overrides the built-in type check', () => {
    const customStep = { type: 'text', required: true, validation: { pattern: '^[A-Z]{3}$', message: 'Three capital letters only.' } };
    expect(formEngine.validateAnswer(customStep, 'abc').valid).toBe(false);
    expect(formEngine.validateAnswer(customStep, 'ABC')).toEqual({ valid: true, value: 'ABC' });
  });
});

describe('fieldPromptMessage', () => {
  test('select field renders one quick reply per option with PIC1:ffld payloads', () => {
    const steps = formEngine.flattenSteps(SAMPLE_FORM);
    const interestStep = steps.find((s) => s.key === 'interest');
    const msg = formEngine.fieldPromptMessage(interestStep, 'volunteer_apply', CONFIG, 'messenger');
    expect(msg.quickReplies).toEqual([
      { content_type: 'text', title: 'Mentoring', payload: 'PIC1:ffld:volunteer_apply:interest:mentoring' },
      { content_type: 'text', title: 'Tutoring', payload: 'PIC1:ffld:volunteer_apply:interest:tutoring' },
    ]);
  });

  test('email field on messenger channel gets the FB-only user_email quick reply', () => {
    const steps = formEngine.flattenSteps(SAMPLE_FORM);
    const emailStep = steps.find((s) => s.key === 'email');
    const msg = formEngine.fieldPromptMessage(emailStep, 'volunteer_apply', CONFIG, 'messenger');
    expect(msg.quickReplies).toContainEqual({ content_type: 'user_email' });
  });

  test('email field on instagram channel does NOT get user_email (C5: FB only)', () => {
    const steps = formEngine.flattenSteps(SAMPLE_FORM);
    const emailStep = steps.find((s) => s.key === 'email');
    const msg = formEngine.fieldPromptMessage(emailStep, 'volunteer_apply', CONFIG, 'instagram');
    expect(msg.quickReplies).toEqual([]);
  });
});

describe('beginForm + handleAnswer (full field progression)', () => {
  test('beginForm prompts the first step', () => {
    const { session, message } = formEngine.beginForm({
      sessionId: SESSION_ID,
      formId: 'volunteer_apply',
      form: SAMPLE_FORM,
      config: CONFIG,
      channelType: 'messenger',
    });
    expect(session.current_field).toBe('full_name.first');
    expect(session.answers).toEqual({});
    expect(session.schema_version).toBe(1);
    expect(typeof session.expires_at).toBe('number');
    expect(message.text).toContain('First name');
  });

  test('walks every field in order and lands on the summary with composite answers nested', () => {
    let { session } = formEngine.beginForm({
      sessionId: SESSION_ID,
      formId: 'volunteer_apply',
      form: SAMPLE_FORM,
      config: CONFIG,
      channelType: 'messenger',
    });

    let r = formEngine.handleAnswer({ session, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: 'Jane' });
    expect(r.status).toBe('next_field');
    session = r.session;

    r = formEngine.handleAnswer({ session, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: 'Doe' });
    expect(r.status).toBe('next_field');
    expect(r.session.current_field).toBe('email');
    session = r.session;

    r = formEngine.handleAnswer({ session, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: 'jane@example.com' });
    session = r.session;

    r = formEngine.handleAnswer({ session, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: 'tutoring' });
    session = r.session;

    r = formEngine.handleAnswer({ session, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: '' });
    expect(r.status).toBe('summary');
    expect(r.session.current_field).toBe(formEngine.SUMMARY_STAGE);
    expect(r.session.answers).toEqual({
      full_name: { first: 'Jane', last: 'Doe' },
      email: 'jane@example.com',
      interest: 'tutoring',
      notes: '', // optional field left blank — recorded, but summary filters empties
    });
    // D2: summary echoes only this session's own answers
    expect(r.message.text).toContain('Jane');
    expect(r.message.text).toContain('Tutoring'); // option LABEL shown, not the raw value
    expect(r.message.quickReplies).toEqual(
      expect.arrayContaining([expect.objectContaining({ payload: 'PIC1:fctl:volunteer_apply:confirm' })])
    );
  });

  test('invalid answer re-prompts the SAME field with the validation error, refreshing TTL', () => {
    const { session: s0 } = formEngine.beginForm({
      sessionId: SESSION_ID,
      formId: 'volunteer_apply',
      form: SAMPLE_FORM,
      config: CONFIG,
      channelType: 'messenger',
    });
    const r = formEngine.handleAnswer({ session: s0, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: '' });
    expect(r.status).toBe('invalid');
    expect(r.session.current_field).toBe('full_name.first'); // unchanged
    expect(r.session.attempts).toBe(1);
    expect(r.message.text).toContain('required');
  });

  test('3 consecutive invalid attempts -> gentle nudge, attempts reset to 0, field unchanged', () => {
    let { session } = formEngine.beginForm({
      sessionId: SESSION_ID,
      formId: 'volunteer_apply',
      form: SAMPLE_FORM,
      config: CONFIG,
      channelType: 'messenger',
    });
    let r;
    for (let i = 0; i < 3; i++) {
      r = formEngine.handleAnswer({ session, form: SAMPLE_FORM, config: CONFIG, channelType: 'messenger', rawText: '' });
      session = r.session;
    }
    expect(r.status).toBe('invalid');
    expect(r.session.attempts).toBe(0);
    expect(r.message.text).toMatch(/cancel/i);
    expect(r.session.current_field).toBe('full_name.first');
  });
});

describe('keyword detection (C9 free-text fallback)', () => {
  test('isCancelKeyword: exact word only, case-insensitive', () => {
    expect(formEngine.isCancelKeyword('cancel')).toBe(true);
    expect(formEngine.isCancelKeyword('CANCEL')).toBe(true);
    expect(formEngine.isCancelKeyword('  cancel  ')).toBe(true);
    expect(formEngine.isCancelKeyword('please cancel this')).toBe(false);
    expect(formEngine.isCancelKeyword('cancellation')).toBe(false);
  });

  test('isConfirmKeyword accepts confirm/yes case-insensitively only', () => {
    expect(formEngine.isConfirmKeyword('confirm')).toBe(true);
    expect(formEngine.isConfirmKeyword('Yes')).toBe(true);
    expect(formEngine.isConfirmKeyword('yes please')).toBe(false);
  });
});

describe('C3 payload parsing', () => {
  test('parseFfldPayload parses formId/fieldKey/value, tolerates dotted composite keys', () => {
    expect(formEngine.parseFfldPayload('PIC1:ffld:volunteer_apply:full_name.first:Jane')).toEqual({
      formId: 'volunteer_apply',
      fieldKey: 'full_name.first',
      value: 'Jane',
    });
  });

  test('parseFfldPayload returns null for a non-ffld payload', () => {
    expect(formEngine.parseFfldPayload('PIC1:cta:learn_more')).toBeNull();
  });

  test('parseFctlPayload parses confirm/cancel ops only', () => {
    expect(formEngine.parseFctlPayload('PIC1:fctl:volunteer_apply:confirm')).toEqual({
      formId: 'volunteer_apply',
      op: 'confirm',
    });
    expect(formEngine.parseFctlPayload('PIC1:fctl:volunteer_apply:delete')).toBeNull();
  });
});

describe('loadFormSession (T2 — expired rows are treated as absent)', () => {
  test('returns the row when not expired', async () => {
    ddbMock.on(GetCommand).resolves({ Item: { sessionId: SESSION_ID, stateType: 'form_session', expires_at: Math.floor(Date.now() / 1000) + 100 } });
    const row = await formEngine.loadFormSession({ client: ddbMock, tableName: 'picasso-conversation-state', sessionId: SESSION_ID });
    expect(row).not.toBeNull();
  });

  test('returns null + best-effort deletes a row whose expires_at has passed', async () => {
    ddbMock.on(GetCommand).resolves({ Item: { sessionId: SESSION_ID, stateType: 'form_session', expires_at: Math.floor(Date.now() / 1000) - 10 } });
    ddbMock.on(DeleteCommand).resolves({});
    const row = await formEngine.loadFormSession({ client: ddbMock, tableName: 'picasso-conversation-state', sessionId: SESSION_ID });
    expect(row).toBeNull();
    expect(ddbMock).toHaveReceivedCommandTimes(DeleteCommand, 1);
  });

  test('missing row -> null, no delete attempted', async () => {
    ddbMock.on(GetCommand).resolves({});
    const row = await formEngine.loadFormSession({ client: ddbMock, tableName: 'picasso-conversation-state', sessionId: SESSION_ID });
    expect(row).toBeNull();
    expect(ddbMock).not.toHaveReceivedCommand(DeleteCommand);
  });
});

describe('buildSubmissionEvent (S2 — pinned widget live-lane contract)', () => {
  test('matches the exact shape HTTPChatProvider.jsx submitFormToLambda sends today', () => {
    const event = formEngine.buildSubmissionEvent({
      tenantHash: 'abc123defabc123def',
      formId: 'volunteer_apply',
      answers: { email: 'jane@example.com' },
      sessionId: SESSION_ID,
    });
    expect(event.httpMethod).toBe('POST');
    expect(event.queryStringParameters).toEqual({ action: 'chat', t: 'abc123defabc123def' });
    const body = JSON.parse(event.body);
    expect(body).toMatchObject({
      tenant_hash: 'abc123defabc123def',
      form_mode: true,
      action: 'submit_form',
      form_id: 'volunteer_apply',
      form_data: { email: 'jane@example.com' },
      session_id: SESSION_ID,
      conversation_id: SESSION_ID,
    });
    // FS5 idempotency token: deterministic + matches MFS's IDEM_TOKEN_SHAPE
    expect(body.client_submission_id).toMatch(/^[A-Za-z0-9_-]{16,128}$/);
  });

  test('client_submission_id is deterministic for the same session+form (retry-safe, T3)', () => {
    const e1 = formEngine.buildSubmissionEvent({ tenantHash: 't', formId: 'f', answers: {}, sessionId: 's' });
    const e2 = formEngine.buildSubmissionEvent({ tenantHash: 't', formId: 'f', answers: { changed: true }, sessionId: 's' });
    expect(JSON.parse(e1.body).client_submission_id).toBe(JSON.parse(e2.body).client_submission_id);
  });

  test('cfOriginSecret adds the x-picasso-cf-origin header; omitted when absent', () => {
    const withSecret = formEngine.buildSubmissionEvent({
      tenantHash: 't', formId: 'f', answers: {}, sessionId: 's', cfOriginSecret: 'shh-secret',
    });
    expect(withSecret.headers[formEngine.CF_ORIGIN_HEADER_NAME]).toBe('shh-secret');

    const withoutSecret = formEngine.buildSubmissionEvent({ tenantHash: 't', formId: 'f', answers: {}, sessionId: 's' });
    expect(withoutSecret.headers[formEngine.CF_ORIGIN_HEADER_NAME]).toBeUndefined();
    expect(withoutSecret.headers['Content-Type']).toBe('application/json');
  });
});

describe('getCfOriginSecret (CF-origin header fetch for the S1 invoke)', () => {
  test('MFS_CF_ORIGIN_SECRET_NAME unset -> no header, and Secrets Manager is never called', async () => {
    const secret = await formEngine.getCfOriginSecret({ log: () => {} });
    expect(secret).toBeNull();
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(0);
  });

  test('JSON envelope {"secret": "..."} unwraps to the secret value', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ secret: 'from-secret-key' }) });
    const secret = await formEngine.getCfOriginSecret({ log: () => {} });
    expect(secret).toBe('from-secret-key');
  });

  test('JSON envelope {"value": "..."} unwraps to the secret value (BSH validator parity)', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ value: 'from-value-key' }) });
    const secret = await formEngine.getCfOriginSecret({ log: () => {} });
    expect(secret).toBe('from-value-key');
  });

  test('plain (non-JSON) SecretString is used as-is', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'plain-text-secret' });
    const secret = await formEngine.getCfOriginSecret({ log: () => {} });
    expect(secret).toBe('plain-text-secret');
  });

  test('success is cached for 5 minutes — a second call within the window makes no further SM call', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ secret: 'cached-value' }) });
    await formEngine.getCfOriginSecret({ log: () => {} });
    await formEngine.getCfOriginSecret({ log: () => {} });
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(1);
  });

  test('fetch failure -> logs WARN (never the secret name value alone is fine, but never a secret VALUE), returns null', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).rejects(new Error('AccessDenied'));
    const logged = [];
    const secret = await formEngine.getCfOriginSecret({ log: (level, msg, meta) => logged.push({ level, msg, meta }) });
    expect(secret).toBeNull();
    expect(logged.some((l) => l.level === 'WARN')).toBe(true);
    expect(JSON.stringify(logged)).not.toContain('cached-value');
  });

  test('empty/whitespace secret is treated as unusable -> WARN, null', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: '   ' });
    const logged = [];
    const secret = await formEngine.getCfOriginSecret({ log: (level, msg, meta) => logged.push({ level, msg, meta }) });
    expect(secret).toBeNull();
    expect(logged.some((l) => l.level === 'WARN')).toBe(true);
  });
});

describe('invokeMfsSubmission wires the CF-origin header into the S1 invoke', () => {
  test('header present on the invoked event when the secret resolves', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ secret: 'the-cf-secret' }) });
    lambdaMock.on(InvokeCommand).resolves({
      Payload: Buffer.from(JSON.stringify({ statusCode: 200, body: JSON.stringify({ success: true, submission_id: 'sub_1' }) })),
    });

    await formEngine.invokeMfsSubmission({
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      tenantHash: 'abc123defabc123def',
      formId: 'volunteer_apply',
      answers: { email: 'jane@example.com' },
      sessionId: SESSION_ID,
      log: () => {},
    });

    const invoked = lambdaMock.commandCalls(InvokeCommand)[0].args[0].input;
    const sentEvent = JSON.parse(Buffer.from(invoked.Payload).toString('utf-8'));
    expect(sentEvent.headers[formEngine.CF_ORIGIN_HEADER_NAME]).toBe('the-cf-secret');
  });

  test('env unset -> invoke still attempted, no header, no Secrets Manager call', async () => {
    lambdaMock.on(InvokeCommand).resolves({
      Payload: Buffer.from(JSON.stringify({ statusCode: 200, body: JSON.stringify({ success: true, submission_id: 'sub_1' }) })),
    });

    const result = await formEngine.invokeMfsSubmission({
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      tenantHash: 'abc123defabc123def',
      formId: 'volunteer_apply',
      answers: {},
      sessionId: SESSION_ID,
      log: () => {},
    });

    expect(result.success).toBe(true);
    expect(smMock.commandCalls(GetSecretValueCommand)).toHaveLength(0);
    const invoked = lambdaMock.commandCalls(InvokeCommand)[0].args[0].input;
    const sentEvent = JSON.parse(Buffer.from(invoked.Payload).toString('utf-8'));
    expect(sentEvent.headers[formEngine.CF_ORIGIN_HEADER_NAME]).toBeUndefined();
  });

  test('secret fetch failure -> the MFS invoke is still attempted, without the header (MFS 403s -> existing failure path handles it)', async () => {
    process.env.MFS_CF_ORIGIN_SECRET_NAME = 'picasso/mfs/cf-origin-secret-abc123';
    smMock.on(GetSecretValueCommand).rejects(new Error('SM outage'));
    // Simulate MFS's own fail-closed 403 when the header is missing.
    lambdaMock.on(InvokeCommand).resolves({
      Payload: Buffer.from(JSON.stringify({ statusCode: 403, body: JSON.stringify({ error: 'Forbidden' }) })),
    });

    const logged = [];
    const result = await formEngine.invokeMfsSubmission({
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      tenantHash: 'abc123defabc123def',
      formId: 'volunteer_apply',
      answers: {},
      sessionId: SESSION_ID,
      log: (level, msg, meta) => logged.push({ level, msg, meta }),
    });

    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(1); // invoke WAS attempted
    const invoked = lambdaMock.commandCalls(InvokeCommand)[0].args[0].input;
    const sentEvent = JSON.parse(Buffer.from(invoked.Payload).toString('utf-8'));
    expect(sentEvent.headers[formEngine.CF_ORIGIN_HEADER_NAME]).toBeUndefined();
    expect(result.success).toBe(false); // MFS's 403 flows into the normal failure path (T3)
    expect(logged.some((l) => l.level === 'WARN')).toBe(true);
  });
});

describe('confirmForm (S1 invoke + T3 failure semantics)', () => {
  const baseSession = {
    sessionId: SESSION_ID,
    stateType: 'form_session',
    form_id: 'volunteer_apply',
    current_field: formEngine.SUMMARY_STAGE,
    answers: { email: 'jane@example.com' },
    attempts: 0,
    schema_version: 1,
    expires_at: 1234567890,
  };

  test('success: deletes the session row, returns the submitted message', async () => {
    lambdaMock.on(InvokeCommand).resolves({
      Payload: Buffer.from(JSON.stringify({ statusCode: 200, body: JSON.stringify({ success: true, submission_id: 'sub_1' }) })),
    });
    ddbMock.on(DeleteCommand).resolves({});

    const result = await formEngine.confirmForm({
      session: baseSession,
      config: CONFIG,
      channelType: 'messenger',
      tenantHash: 'abc123defabc123def',
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      client: ddbMock,
      tableName: 'picasso-conversation-state',
      log: () => {},
    });

    expect(result.submitted).toBe(true);
    expect(ddbMock).toHaveReceivedCommandTimes(DeleteCommand, 1);
  });

  test('MFS non-200 -> failure, session row is NEVER touched (T3: no save/extend)', async () => {
    lambdaMock.on(InvokeCommand).resolves({
      Payload: Buffer.from(JSON.stringify({ statusCode: 502, body: JSON.stringify({ success: false, error: 'form_processing_failed' }) })),
    });

    const result = await formEngine.confirmForm({
      session: baseSession,
      config: CONFIG,
      channelType: 'messenger',
      tenantHash: 'abc123defabc123def',
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      client: ddbMock,
      tableName: 'picasso-conversation-state',
      log: () => {},
    });

    expect(result.submitted).toBe(false);
    expect(ddbMock.commandCalls(PutCommand)).toHaveLength(0);
    expect(ddbMock.commandCalls(DeleteCommand)).toHaveLength(0);
    expect(result.message.quickReplies).toEqual(
      expect.arrayContaining([expect.objectContaining({ payload: 'PIC1:fctl:volunteer_apply:confirm' })])
    );
  });

  test('Lambda invoke throws -> failure, never crashes the caller', async () => {
    lambdaMock.on(InvokeCommand).rejects(new Error('network blip'));

    const result = await formEngine.confirmForm({
      session: baseSession,
      config: CONFIG,
      channelType: 'messenger',
      tenantHash: 'abc123defabc123def',
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      client: ddbMock,
      tableName: 'picasso-conversation-state',
      log: () => {},
    });

    expect(result.submitted).toBe(false);
  });

  test('missing MFS_FUNCTION -> failure without attempting an invoke, session kept', async () => {
    const result = await formEngine.confirmForm({
      session: baseSession,
      config: CONFIG,
      channelType: 'messenger',
      tenantHash: 'abc123defabc123def',
      lambdaClient: lambdaMock,
      functionName: '',
      client: ddbMock,
      tableName: 'picasso-conversation-state',
      log: () => {},
    });
    expect(result.submitted).toBe(false);
    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(0);
  });

  test('D1/X3: no log call ever includes an answer value', async () => {
    lambdaMock.on(InvokeCommand).resolves({
      Payload: Buffer.from(JSON.stringify({ statusCode: 200, body: JSON.stringify({ success: true, submission_id: 'sub_1' }) })),
    });
    ddbMock.on(DeleteCommand).resolves({});
    const logged = [];
    await formEngine.confirmForm({
      session: baseSession,
      config: CONFIG,
      channelType: 'messenger',
      tenantHash: 'abc123defabc123def',
      lambdaClient: lambdaMock,
      functionName: 'Master_Function_Staging',
      client: ddbMock,
      tableName: 'picasso-conversation-state',
      log: (level, msg, meta) => logged.push(JSON.stringify(meta || {})),
    });
    const serialized = logged.join('\n');
    expect(serialized).not.toContain('jane@example.com');
  });
});
