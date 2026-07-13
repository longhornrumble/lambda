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
