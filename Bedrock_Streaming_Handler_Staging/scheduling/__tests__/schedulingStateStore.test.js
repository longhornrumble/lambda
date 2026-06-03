'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { buildSchedulingDeps } = require('../schedulingStateStore');

const ddbMock = mockClient(DynamoDBDocumentClient);

const SESSION_TABLE = 'picasso-conversation-scheduling-session-staging';
const BOOKING_TABLE = 'picasso-booking-staging';

function deps() {
  return buildSchedulingDeps({ sessionTable: SESSION_TABLE, bookingTable: BOOKING_TABLE });
}

beforeEach(() => ddbMock.reset());

describe('loadState', () => {
  it('reads the C9 state row at the PLAIN sessionId SK (not the binding# row)', async () => {
    ddbMock.on(GetCommand).resolves({ Item: { tenantId: 'T1', session_id: 'sess-1', state: 'confirming' } });
    const out = await deps().loadState({ tenantId: 'T1', sessionId: 'sess-1' });
    expect(out.state).toBe('confirming');
    expect(ddbMock).toHaveReceivedCommandWith(GetCommand, {
      TableName: SESSION_TABLE,
      Key: { tenantId: 'T1', session_id: 'sess-1' }, // PLAIN sessionId — never binding#<id>
    });
  });

  it('returns null on a missing row (first turn)', async () => {
    ddbMock.on(GetCommand).resolves({});
    expect(await deps().loadState({ tenantId: 'T1', sessionId: 'sess-1' })).toBeNull();
  });

  it('fail-soft on missing keys (no DDB call)', async () => {
    expect(await deps().loadState({ tenantId: '', sessionId: 'x' })).toBeNull();
    expect(await deps().loadState({ tenantId: 'T1' })).toBeNull();
    expect(ddbMock).not.toHaveReceivedCommand(GetCommand);
  });
});

describe('saveState', () => {
  it('PUTs the plain-sessionId state row with state + optional slot fields + updated_at', async () => {
    ddbMock.on(PutCommand).resolves({});
    await deps().saveState({
      tenantId: 'T1', sessionId: 'sess-1', state: 'proposing',
      candidate_slots: [{ slotId: 's1' }],
    });
    const call = ddbMock.commandCalls(PutCommand)[0].args[0].input;
    expect(call.TableName).toBe(SESSION_TABLE);
    expect(call.Item.session_id).toBe('sess-1'); // plain — preserves the binding# row
    expect(call.Item.state).toBe('proposing');
    expect(call.Item.candidate_slots).toEqual([{ slotId: 's1' }]);
    expect(typeof call.Item.updated_at).toBe('string');
    expect('selected_slot' in call.Item).toBe(false); // omitted when undefined
  });

  it('fail-soft on missing keys (no DDB write)', async () => {
    await deps().saveState({ tenantId: '', sessionId: 'x', state: 'proposing' });
    expect(ddbMock).not.toHaveReceivedCommand(PutCommand);
  });
});

describe('loadBooking', () => {
  it('reads the Booking row at (tenantId, booking_id)', async () => {
    ddbMock.on(GetCommand).resolves({ Item: { tenantId: 'T1', booking_id: 'bk-9', status: 'booked' } });
    const out = await deps().loadBooking({ tenantId: 'T1', bookingId: 'bk-9' });
    expect(out.status).toBe('booked');
    expect(ddbMock).toHaveReceivedCommandWith(GetCommand, {
      TableName: BOOKING_TABLE,
      Key: { tenantId: 'T1', booking_id: 'bk-9' },
    });
  });

  it('returns null when absent / on missing keys', async () => {
    ddbMock.on(GetCommand).resolves({});
    expect(await deps().loadBooking({ tenantId: 'T1', bookingId: 'bk-9' })).toBeNull();
    expect(await deps().loadBooking({ tenantId: 'T1' })).toBeNull();
  });
});
