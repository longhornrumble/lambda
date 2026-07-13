'use strict';

/**
 * C7 serialization — handler-level race tests (M1c DONE line).
 *
 * Separate file from index.test.js because CONVERSATION_STATE_TABLE must be
 * set BEFORE the handler module loads (module-scope env read). index.test.js
 * deliberately leaves it unset, which doubles as the fail-open/disabled test.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { KMSClient, DecryptCommand } = require('@aws-sdk/client-kms');
const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

jest.mock('../shared/bedrock-core', () => ({
  loadConfig: jest.fn(),
  retrieveKB: jest.fn(),
  sanitizeUserInput: jest.fn((t) => t),
}));
const { loadConfig, retrieveKB } = require('../shared/bedrock-core');

const ddbMock = mockClient(DynamoDBDocumentClient);
const kmsMock = mockClient(KMSClient);
const bedrockMock = mockClient(BedrockRuntimeClient);
const sqsMock = mockClient(SQSClient);

const STATE_TABLE = 'picasso-conversation-state';
const PAGE_ID = '112233445566778';
const PSID = '987654321012345';
const SESSION_ID = `meta:${PAGE_ID}:${PSID}`;

let handler;
beforeAll(() => {
  process.env.ENVIRONMENT = 'test';
  process.env.CHANNEL_MAPPINGS_TABLE = 'picasso-channel-mappings-test';
  process.env.RECENT_MESSAGES_TABLE = 'picasso-recent-messages-test';
  process.env.CONVERSATION_STATE_TABLE = STATE_TABLE; // the difference vs index.test.js
  process.env.KMS_KEY_ID = 'alias/test';
  process.env.BEDROCK_MODEL_ID =
    process.env.BEDROCK_MODEL_ID || 'global.anthropic.claude-haiku-4-5-20251001-v1:0';
  handler = require('./index').handler;
});

let fetchMock;
beforeEach(() => {
  ddbMock.reset();
  kmsMock.reset();
  bedrockMock.reset();
  sqsMock.reset();
  jest.clearAllMocks();

  sqsMock.on(SendMessageCommand).resolves({ MessageId: 'mock-sqs-id' });
  loadConfig.mockResolvedValue({
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'You are a helpful recruiter assistant.',
    streaming: { max_tokens: 500, temperature: 0 },
  });
  retrieveKB.mockResolvedValue('KB context.');

  // Channel mapping + token decrypt
  ddbMock.on(GetCommand).resolves({
    Item: {
      PK: `PAGE#${PAGE_ID}`,
      SK: 'CHANNEL#messenger',
      tenantId: 'TENANT_ABC',
      tenantHash: 'abc123',
      encryptedPageToken: Buffer.from('encrypted-blob').toString('base64'),
    },
  });
  kmsMock.on(DecryptCommand).resolves({ Plaintext: Buffer.from('EAAB...token') });

  // Recent-messages defaults
  ddbMock.on(QueryCommand).resolves({ Items: [] });
  ddbMock.on(PutCommand).resolves({});
  ddbMock.on(UpdateCommand).resolves({});
  ddbMock.on(DeleteCommand).resolves({});

  bedrockMock.on(InvokeModelCommand).resolves({
    body: Buffer.from(JSON.stringify({ content: [{ type: 'text', text: 'A helpful reply.' }] })),
  });

  fetchMock = jest.fn().mockResolvedValue({
    ok: true,
    status: 200,
    json: () => Promise.resolve({ recipient_id: PSID, message_id: 'mid.test' }),
  });
  global.fetch = fetchMock;
});

afterEach(() => {
  delete global.fetch;
});

function buildEvent(overrides = {}) {
  return {
    psid: PSID,
    messageText: 'Hello there',
    pageId: PAGE_ID,
    tenantId: 'TENANT_ABC',
    tenantHash: 'abc123',
    channelType: 'messenger',
    messageMid: 'm_race_1',
    isPostback: false,
    v: 2,
    eventKind: 'text',
    timestamp: Date.now(),
    quickReplyPayload: null,
    appId: null,
    attachmentTypes: [],
    targetMid: null,
    editedText: null,
    replyTo: null,
    isStandby: false,
    ...overrides,
  };
}

function ccfe() {
  const err = new Error('The conditional request failed');
  err.name = 'ConditionalCheckFailedException';
  return err;
}

/** Match state-table lock commands apart from recent-messages traffic. */
const lockPut = { TableName: STATE_TABLE };

describe('C7 handler-level serialization', () => {
  test('winner runs exactly one Bedrock turn and releases the lock', async () => {
    ddbMock.on(PutCommand, lockPut, false).resolves({}); // acquire wins
    // claimPending: state-table Update returns no pending
    ddbMock.on(UpdateCommand, { TableName: STATE_TABLE }, false).resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});

    await handler(buildEvent());

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    // Lock row released (conditional delete on the state table)
    const dels = ddbMock.commandCalls(DeleteCommand).filter(
      (c) => c.args[0].input.TableName === STATE_TABLE
    );
    expect(dels).toHaveLength(1);
    expect(dels[0].args[0].input.Key).toEqual({ sessionId: SESSION_ID, stateType: 'lock' });
  });

  test('loser coalesces: zero Bedrock calls, zero sends, one list_append', async () => {
    ddbMock.on(PutCommand, lockPut, false).rejects(ccfe()); // acquire loses
    ddbMock.on(UpdateCommand, { TableName: STATE_TABLE }, false).resolves({}); // append ok

    await handler(buildEvent({ messageMid: 'm_race_2', messageText: 'second message' }));

    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    expect(fetchMock).not.toHaveBeenCalled();
    const appends = ddbMock.commandCalls(UpdateCommand).filter(
      (c) =>
        c.args[0].input.TableName === STATE_TABLE &&
        String(c.args[0].input.UpdateExpression).includes('list_append')
    );
    expect(appends).toHaveLength(1);
    expect(appends[0].args[0].input.ExpressionAttributeValues[':item'][0].text).toBe('second message');
  });

  test('two racing invokes → exactly one Bedrock call total', async () => {
    // A wins, B loses+coalesces
    ddbMock
      .on(PutCommand, lockPut, false)
      .resolvesOnce({})
      .rejects(ccfe());
    ddbMock.on(UpdateCommand, { TableName: STATE_TABLE }, false).resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});

    await Promise.all([
      handler(buildEvent({ messageMid: 'm_a' })),
      handler(buildEvent({ messageMid: 'm_b', messageText: 'me too' })),
    ]);

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
  });

  test('drain: pending claimed after the turn is answered in a combined Bedrock cycle', async () => {
    ddbMock.on(PutCommand, lockPut, false).resolves({});
    // First claim returns two coalesced messages; later claims return empty.
    ddbMock
      .on(UpdateCommand, { TableName: STATE_TABLE }, false)
      .resolvesOnce({ Attributes: { pending: [
        { text: 'follow-up one', mid: 'm_p1', timestamp: 1 },
        { text: 'follow-up two', mid: 'm_p2', timestamp: 2 },
      ] } })
      .resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});

    await handler(buildEvent());

    // Main turn + one combined drain cycle
    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 2);
    const secondPrompt = JSON.parse(
      Buffer.from(bedrockMock.commandCalls(InvokeModelCommand)[1].args[0].input.body).toString()
    );
    const promptText = JSON.stringify(secondPrompt);
    expect(promptText).toContain('follow-up one');
    expect(promptText).toContain('follow-up two');
    // Released at the end
    const dels = ddbMock.commandCalls(DeleteCommand).filter(
      (c) => c.args[0].input.TableName === STATE_TABLE
    );
    expect(dels).toHaveLength(1);
  });

  test('stale-lock takeover inherits orphaned pending and answers it', async () => {
    ddbMock.on(PutCommand, lockPut, false).resolves({
      Attributes: { pending: [{ text: 'orphaned question', mid: 'm_o', timestamp: 1 }] },
    });
    ddbMock.on(UpdateCommand, { TableName: STATE_TABLE }, false).resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});

    await handler(buildEvent());

    // Main turn + inherited-pending cycle
    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 2);
  });

  test('release contention: delete fails once (new pending) → drained then released', async () => {
    ddbMock.on(PutCommand, lockPut, false).resolves({});
    ddbMock
      .on(UpdateCommand, { TableName: STATE_TABLE }, false)
      .resolvesOnce({ Attributes: {} }) // first claim: nothing
      .resolvesOnce({ Attributes: { pending: [{ text: 'late arrival', mid: 'm_l', timestamp: 3 }] } })
      .resolves({ Attributes: {} });
    ddbMock
      .on(DeleteCommand, { TableName: STATE_TABLE }, false)
      .rejectsOnce(ccfe()) // pending raced in — no drop-on-release
      .resolves({});

    await handler(buildEvent());

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 2);
    const dels = ddbMock.commandCalls(DeleteCommand).filter(
      (c) => c.args[0].input.TableName === STATE_TABLE
    );
    expect(dels).toHaveLength(2); // failed conditional + final release
  });

  test('lock infrastructure failure → fail-open: turn still answered', async () => {
    ddbMock.on(PutCommand, lockPut, false).rejects(new Error('state table unavailable'));

    await handler(buildEvent());

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 1);
    expect(fetchMock).toHaveBeenCalled(); // reply sent despite lock outage
  });
});

describe('M4 × C7 — coalesced PIC1 taps route through C3 in the drain', () => {
  test('drained quick-reply tap contributes the CTA query, not the tap label', async () => {
    loadConfig.mockResolvedValue({
      model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
      tone_prompt: 'T.',
      streaming: { max_tokens: 500, temperature: 0 },
      feature_flags: { MESSENGER_CHANNEL: true },
      cta_definitions: {
        learn_x: { label: 'Our Programs', action: 'send_query', query: 'tell me about programs', ai_available: true },
      },
    });
    ddbMock.on(PutCommand, { TableName: STATE_TABLE }, false).resolves({});
    // Matched on the lock row's Key specifically (not just TableName) so
    // M-Hb's rate-limit counter UpdateCommands (Key: rl_user:*/rl_day:*,
    // also issued against STATE_TABLE, now that MESSENGER_CHANNEL is on)
    // don't consume this queue out of order — aws-sdk-client-mock prefers
    // the most specific matching stub, so those fall through to the
    // module-level generic `ddbMock.on(UpdateCommand).resolves({})` default.
    ddbMock
      .on(UpdateCommand, { TableName: STATE_TABLE, Key: { sessionId: SESSION_ID, stateType: 'lock' } }, false)
      .resolvesOnce({ Attributes: { pending: [
        { text: 'Our Programs', quickReplyPayload: 'PIC1:cta:learn_x', mid: 'm_tap', timestamp: 2 },
      ] } })
      .resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});
    bedrockMock.on(InvokeModelCommand).resolves({
      body: Buffer.from(JSON.stringify({ content: [{ type: 'text', text: 'Answer.\n<<<ACTIONS []>>>' }] })),
    });

    await handler(buildEvent());

    expect(bedrockMock).toHaveReceivedCommandTimes(InvokeModelCommand, 2);
    const drainBody = JSON.parse(Buffer.from(bedrockMock.commandCalls(InvokeModelCommand)[1].args[0].input.body).toString());
    const drainTurn = drainBody.messages[drainBody.messages.length - 1].content[0].text;
    expect(drainTurn).toBe('tell me about programs'); // C7 step 3: canonical query, not "Our Programs"
  });
});

// ─── M7a × C7 — mid-form races (adversarial focus named in the plan: "two
// rapid answers — C7 must serialize"). Reuses this file's shared handler/mocks
// (CONVERSATION_STATE_TABLE already set at module load above) rather than a
// fresh isolated instance — no MFS_FUNCTION needed since these races never
// reach the confirm/submit step.
describe('M7a × C7 — mid-form races', () => {
  const FORM_CFG = {
    model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
    tone_prompt: 'Helpful.',
    streaming: { max_tokens: 500, temperature: 0 },
    feature_flags: { MESSENGER_CHANNEL: true },
    conversational_forms: {
      race_form: {
        form_id: 'race_form',
        fields: [
          { id: 'a', type: 'text', label: 'A', prompt: 'First field?', required: true },
          { id: 'b', type: 'text', label: 'B', prompt: 'Second field?', required: true },
        ],
      },
    },
  };

  function activeSessionItem(overrides = {}) {
    const now = Date.now();
    return {
      sessionId: SESSION_ID,
      stateType: 'form_session',
      form_id: 'race_form',
      current_field: 'a',
      answers: {},
      attempts: 0,
      started_at: now,
      updated_at: now,
      schema_version: 1,
      expires_at: Math.floor(now / 1000) + 3600,
      ...overrides,
    };
  }

  beforeEach(() => {
    loadConfig.mockResolvedValue(FORM_CFG);
    ddbMock.on(GetCommand, { Key: { sessionId: SESSION_ID, stateType: 'pause' } }, false).resolves({});
  });

  test('two rapid answers (winner + coalesced loser) apply SEQUENTIALLY against the evolving session — both fields land correctly, zero Bedrock calls', async () => {
    ddbMock
      .on(GetCommand, { Key: { sessionId: SESSION_ID, stateType: 'form_session' } }, false)
      .resolves({ Item: activeSessionItem() });
    // Winner acquires the lock; the second (racing) invoke coalesces its
    // answer onto pending instead of running concurrently.
    // Scoped to the LOCK row's own Put specifically (not just TableName) so
    // this doesn't also consume the form-engine's form_session Put, which
    // targets the same STATE_TABLE (mirrors the M4×C7 test's own note above
    // about UpdateCommand matching specificity).
    ddbMock.on(PutCommand, { TableName: STATE_TABLE, Item: { stateType: 'lock' } }, false).resolvesOnce({}).rejects(ccfe());
    // Winner's own answer ("first answer") advances a -> b; the coalesced
    // drain claim then delivers the loser's text ("second answer") against
    // field b.
    ddbMock
      // UpdateExpression discriminates claimPending's REMOVE from the racing
      // loser's own coalesce-append SET list_append(...) call — both target
      // the same Key, and a genuine Promise.all race means either could fire
      // first; the generic default stub (outer beforeEach) harmlessly
      // answers the append (its return value is never read).
      .on(
        UpdateCommand,
        { TableName: STATE_TABLE, Key: { sessionId: SESSION_ID, stateType: 'lock' }, UpdateExpression: 'REMOVE pending SET updated_at = :now' },
        false
      )
      .resolvesOnce({ Attributes: { pending: [{ text: 'second answer', mid: 'm_b', timestamp: 2 }] } })
      .resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});

    await Promise.all([
      handler(buildEvent({ messageMid: 'm_a', messageText: 'first answer' })),
      handler(buildEvent({ messageMid: 'm_a2', messageText: 'first answer racer' })),
    ]);

    // Neither racing message ever reached Bedrock/RAG — both were form input.
    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);

    const formPuts = ddbMock
      .commandCalls(PutCommand)
      .filter((c) => c.args[0].input.Item?.stateType === 'form_session');
    expect(formPuts.length).toBeGreaterThanOrEqual(2);
    const finalPut = formPuts[formPuts.length - 1].args[0].input.Item;
    // Both fields answered, in order, with no lost update — 'a' from the
    // winner's own turn, 'b' from the coalesced drain of the racing message.
    expect(finalPut.answers).toEqual({ a: 'first answer', b: 'second answer' });
    expect(finalPut.current_field).toBe('__summary__');
  });

  test('a coalesced ffld tap racing mid-form is applied via the form engine, not treated as free text into RAG', async () => {
    ddbMock
      .on(GetCommand, { Key: { sessionId: SESSION_ID, stateType: 'form_session' } }, false)
      .resolves({ Item: activeSessionItem() });
    // Scoped to the LOCK row's own Put specifically (not just TableName) so
    // this doesn't also consume the form-engine's form_session Put, which
    // targets the same STATE_TABLE (mirrors the M4×C7 test's own note above
    // about UpdateCommand matching specificity).
    ddbMock.on(PutCommand, { TableName: STATE_TABLE, Item: { stateType: 'lock' } }, false).resolvesOnce({}).rejects(ccfe());
    ddbMock
      .on(
        UpdateCommand,
        { TableName: STATE_TABLE, Key: { sessionId: SESSION_ID, stateType: 'lock' }, UpdateExpression: 'REMOVE pending SET updated_at = :now' },
        false
      )
      .resolvesOnce({
        Attributes: { pending: [{ text: 'Racer', quickReplyPayload: 'PIC1:ffld:race_form:b:racer-value', mid: 'm_b', timestamp: 2 }] },
      })
      .resolves({ Attributes: {} });
    ddbMock.on(DeleteCommand, { TableName: STATE_TABLE }, false).resolves({});

    await Promise.all([
      handler(buildEvent({ messageMid: 'm_a', messageText: 'first answer' })),
      handler(buildEvent({ messageMid: 'm_a2', messageText: 'tap' })),
    ]);

    expect(bedrockMock).not.toHaveReceivedCommand(InvokeModelCommand);
    const formPuts = ddbMock
      .commandCalls(PutCommand)
      .filter((c) => c.args[0].input.Item?.stateType === 'form_session');
    const finalPut = formPuts[formPuts.length - 1].args[0].input.Item;
    expect(finalPut.answers.b).toBe('racer-value'); // the ffld tap's option value, not the tap's label text
  });
});
