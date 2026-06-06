'use strict';

/**
 * consent.test.js — recordBookingSmsConsent (WS-E-TCPA, FROZEN_CONTRACTS §E3 / SEAM-2).
 *
 * Covers: E.164-before-write (normalize + reject), the shipped key shape the SMS_Sender
 * gate reads, ttl = now + 4yr+30d, idempotent/immutable (already_exists), throw-vs-best-
 * effort split, provenance fields. Plus the AWS-touching default via aws-sdk-client-mock.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');

const {
  recordBookingSmsConsent,
  toE164,
  defaultPutConsent,
  CONSENT_TTL_SECONDS,
} = require('../consent');

// Deterministic clock: 2026-06-05T00:00:00.000Z.
const FIXED_MS = Date.UTC(2026, 5, 5, 0, 0, 0);
const fixedNow = () => FIXED_MS;
const silent = { warn: () => {}, error: () => {}, log: () => {} };

describe('toE164 — E.164-before-write', () => {
  test('prefixes +1 onto a bare 10-digit US number', () => {
    expect(toE164('5125551234')).toBe('+15125551234');
    expect(toE164('(512) 555-1234')).toBe('+15125551234');
  });
  test('keeps an already-+-prefixed international number', () => {
    expect(toE164('+447911123456')).toBe('+447911123456');
  });
  test('rejects un-normalizable input → null', () => {
    expect(toE164('')).toBeNull();
    expect(toE164('   ')).toBeNull();
    expect(toE164('12345')).toBeNull();          // too short
    expect(toE164('+1234567890123456')).toBeNull(); // too long (>15)
    expect(toE164(null)).toBeNull();
    expect(toE164(undefined)).toBeNull();
    expect(toE164(5125551234)).toBeNull();        // non-string
  });
});

describe('recordBookingSmsConsent — write path (injected putConsent)', () => {
  test('writes the shipped key shape + ttl + provenance, returns written:true', async () => {
    let captured = null;
    const putConsent = async (item) => { captured = item; return true; };

    const res = await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '5125551234', bookingId: 'b-9', consentLanguage: 'Msg & data rates may apply.' },
      { putConsent, now: fixedNow, log: silent }
    );

    expect(res).toEqual({ written: true, phone_e164: '+15125551234' });
    // The exact key the SMS_Sender gate reads (SEAM-2).
    expect(captured.pk).toBe('TENANT#T1');
    expect(captured.sk).toBe('CONSENT#transactional#+15125551234');
    expect(captured.phone_e164).toBe('+15125551234');
    expect(captured.consent_type).toBe('transactional');
    expect(captured.consent_method).toBe('scheduling_booking');
    expect(captured.consent_language).toBe('Msg & data rates may apply.');
    expect(captured.booking_id).toBe('b-9');
    expect(captured.consent_timestamp).toBe(new Date(FIXED_MS).toISOString());
    // ttl = now(s) + 4yr+30d.
    expect(captured.ttl).toBe(Math.floor(FIXED_MS / 1000) + CONSENT_TTL_SECONDS);
    expect(CONSENT_TTL_SECONDS).toBe((4 * 365 + 30) * 24 * 60 * 60);
  });

  test('defaults provenance fields when omitted', async () => {
    let captured = null;
    await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '+15125551234' },
      { putConsent: async (i) => { captured = i; return true; }, now: fixedNow, log: silent }
    );
    expect(captured.booking_id).toBe('unknown');
    expect(captured.consent_language).toBe('');
    expect(captured.consent_method).toBe('scheduling_booking');
  });

  test('uses the default clock when now is not injected', async () => {
    let captured = null;
    const before = Math.floor(Date.now() / 1000);
    const res = await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '+15125551234' },
      { putConsent: async (i) => { captured = i; return true; }, log: silent }
    );
    expect(res.written).toBe(true);
    expect(captured.ttl).toBeGreaterThanOrEqual(before + CONSENT_TTL_SECONDS);
  });

  test('source overrides consent_method', async () => {
    let captured = null;
    await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '+15125551234', source: 'scheduling_reschedule' },
      { putConsent: async (i) => { captured = i; return true; }, now: fixedNow, log: silent }
    );
    expect(captured.consent_method).toBe('scheduling_reschedule');
  });
});

describe('recordBookingSmsConsent — guards & outcomes', () => {
  test('invalid phone → not written, putConsent never called', async () => {
    const putConsent = jest.fn();
    const res = await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '12345', bookingId: 'b-1' },
      { putConsent, now: fixedNow, log: silent }
    );
    expect(res).toEqual({ written: false, reason: 'invalid_phone' });
    expect(putConsent).not.toHaveBeenCalled();
  });

  test('already-existing consent (conditional fail) → written:false reason:already_exists', async () => {
    const res = await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '+15125551234' },
      { putConsent: async () => false, now: fixedNow, log: silent }
    );
    expect(res).toEqual({ written: false, reason: 'already_exists', phone_e164: '+15125551234' });
  });

  test('transport failure → best-effort, returns written:false reason:write_failed (no throw)', async () => {
    const res = await recordBookingSmsConsent(
      { tenantId: 'T1', phone: '+15125551234' },
      { putConsent: async () => { throw new Error('ddb down'); }, now: fixedNow, log: silent }
    );
    expect(res).toEqual({ written: false, reason: 'write_failed' });
  });

  test('missing tenantId throws (caller-contract bug)', async () => {
    await expect(
      recordBookingSmsConsent({ phone: '+15125551234' }, { now: fixedNow, log: silent })
    ).rejects.toThrow(/tenantId is required/);
  });

  test('no-arg call engages defaults then throws (tenantId)', async () => {
    await expect(recordBookingSmsConsent()).rejects.toThrow(/tenantId is required/);
  });

  test('default deps + invalid phone short-circuits before AWS (no deps injected)', async () => {
    // Omitting `deps` engages the default putConsent/now/log; invalid phone returns
    // before any AWS call, so this safely exercises the default-deps branch + the
    // bookingId-absent fallback in the warn log.
    const res = await recordBookingSmsConsent({ tenantId: 'T1', phone: '12345' });
    expect(res).toEqual({ written: false, reason: 'invalid_phone' });
  });

  test('missing phone throws (caller-contract bug)', async () => {
    await expect(
      recordBookingSmsConsent({ tenantId: 'T1' }, { now: fixedNow, log: silent })
    ).rejects.toThrow(/phone is required/);
  });
});

describe('defaultPutConsent — AWS-touching default (aws-sdk-client-mock)', () => {
  const ddbMock = mockClient(DynamoDBClient);
  beforeEach(() => ddbMock.reset());

  const sampleItem = {
    pk: 'TENANT#T1',
    sk: 'CONSENT#transactional#+15125551234',
    phone_e164: '+15125551234',
    consent_timestamp: '2026-06-05T00:00:00.000Z',
    consent_method: 'scheduling_booking',
    consent_language: '',
    consent_type: 'transactional',
    booking_id: 'b-9',
    created_at: '2026-06-05T00:00:00.000Z',
    updated_at: '2026-06-05T00:00:00.000Z',
    ttl: 1877817600,
  };

  test('marshals the item + conditional put, returns true', async () => {
    ddbMock.on(PutItemCommand).resolves({});
    const ok = await defaultPutConsent(sampleItem);
    expect(ok).toBe(true);
    expect(ddbMock).toHaveReceivedCommandWith(PutItemCommand, {
      ConditionExpression: 'attribute_not_exists(pk)',
      Item: expect.objectContaining({
        pk: { S: 'TENANT#T1' },
        sk: { S: 'CONSENT#transactional#+15125551234' },
        consent_given: { BOOL: true },
        opted_out_at: { NULL: true },
        opt_out_source: { NULL: true },
        ttl: { N: '1877817600' },
      }),
    });
  });

  test('ConditionalCheckFailed → returns false (already exists)', async () => {
    ddbMock.on(PutItemCommand).rejects(
      Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' })
    );
    expect(await defaultPutConsent(sampleItem)).toBe(false);
  });

  test('other DDB error propagates', async () => {
    ddbMock.on(PutItemCommand).rejects(new Error('throttled'));
    await expect(defaultPutConsent(sampleItem)).rejects.toThrow(/throttled/);
  });
});
