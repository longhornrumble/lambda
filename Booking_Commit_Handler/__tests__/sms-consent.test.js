'use strict';

const { mockClient } = require('aws-sdk-client-mock');
const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { readSmsConsent } = require('../sms-consent');

const ddbMock = mockClient(DynamoDBClient);

beforeEach(() => ddbMock.reset());

describe('readSmsConsent (G7b consent pre-filter)', () => {
  it('returns the mapped record for a live consent on the canonical key', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { consent_given: { BOOL: true } } });
    const r = await readSmsConsent('T1', '+15125551234');
    expect(r).toEqual({ consent_given: true, opted_out_at: undefined });
    const input = ddbMock.commandCalls(GetItemCommand)[0].args[0].input;
    expect(input.Key.pk.S).toBe('TENANT#T1');
    expect(input.Key.sk.S).toBe('CONSENT#transactional#+15125551234');
  });

  it('normalizes a bare 10-digit US phone to E.164 in the key', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { consent_given: { BOOL: true } } });
    await readSmsConsent('T1', '5125551234');
    const input = ddbMock.commandCalls(GetItemCommand)[0].args[0].input;
    expect(input.Key.sk.S).toBe('CONSENT#transactional#+15125551234');
  });

  it('surfaces an opt-out (opted_out_at present, consent_given mapped)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: { consent_given: { BOOL: false }, opted_out_at: { S: '2026-06-01T00:00:00Z' } },
    });
    const r = await readSmsConsent('T1', '+15125551234');
    expect(r).toEqual({ consent_given: false, opted_out_at: '2026-06-01T00:00:00Z' });
  });

  it('opted_out_at stored as a DDB NULL type is treated as not-opted-out', async () => {
    // consent.js writes opted_out_at:{NULL:true} on an initial (not-yet-revoked) record.
    ddbMock.on(GetItemCommand).resolves({
      Item: { consent_given: { BOOL: true }, opted_out_at: { NULL: true } },
    });
    const r = await readSmsConsent('T1', '+15125551234');
    expect(r).toEqual({ consent_given: true, opted_out_at: undefined });
  });

  it('a missing consent_given attribute maps to false', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: { something_else: { S: 'x' } } });
    const r = await readSmsConsent('T1', '+15125551234');
    expect(r).toEqual({ consent_given: false, opted_out_at: undefined });
  });

  it('returns null when no record exists', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    expect(await readSmsConsent('T1', '+15125551234')).toBeNull();
  });

  it('returns null (no call) for a missing tenantId', async () => {
    expect(await readSmsConsent('', '+15125551234')).toBeNull();
    expect(ddbMock.commandCalls(GetItemCommand)).toHaveLength(0);
  });

  it('returns null (no call) for an un-normalizable phone', async () => {
    expect(await readSmsConsent('T1', 'not-a-phone')).toBeNull();
    expect(ddbMock.commandCalls(GetItemCommand)).toHaveLength(0);
  });

  it('FAILS SAFE to null on a DDB error', async () => {
    ddbMock.on(GetItemCommand).rejects(new Error('throttled'));
    expect(await readSmsConsent('T1', '+15125551234')).toBeNull();
  });

  it('accepts an injected ddb client (DI seam)', async () => {
    const calls = [];
    const fakeDdb = { send: async (c) => { calls.push(c); return { Item: { consent_given: { BOOL: true } } }; } };
    const r = await readSmsConsent('T1', '+15125551234', { ddb: fakeDdb });
    expect(r.consent_given).toBe(true);
    expect(calls).toHaveLength(1);
  });
});
