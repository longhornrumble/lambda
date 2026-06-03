'use strict';

/**
 * Unit tests for sessionBinding.js (WS-BINDING, §B12 resolveBinding).
 *
 * Done-bar (work order):
 *  - valid unexpired binding → the §B12 shape
 *  - expired (now >= expires_at) → null
 *  - missing row → null
 *  - row under a different tenant → null (GetItem miss)
 *  - forward-compatible reads: tolerate missing optional coordinator_id / form_submission_id
 *
 * Two layers:
 *  1. The DI seam (deps = { ddb, now } mocked) — the contract surface.
 *  2. The default DI implementations (default ddb client + default Date.now clock)
 *     against aws-sdk-client-mock, so the module's own AWS-touching defaults are exercised.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);

const { resolveBinding } = require('../sessionBinding');

const TENANT = 'AUS123957';
const SESSION = 'sess-uuid-1234';
const BINDING_SK = `binding#${SESSION}`;

// A fixed clock for deterministic TTL assertions.
const NOW = 1_900_000_000_000;
const fixedNow = () => NOW;

// Marshalled §B10 row builder. `expires_at` defaults to 30 min ahead of NOW.
const rowItem = ({
  intent = 'rescheduling_intent',
  booking_id = 'bk-1',
  expires_at = NOW + 30 * 60 * 1000,
  session_id = BINDING_SK,
  coordinator_id,
  form_submission_id,
} = {}) => {
  const item = {
    tenantId: { S: TENANT },
    session_id: { S: session_id },
    intent: { S: intent },
    booking_id: { S: booking_id },
    expires_at: { N: String(expires_at) },
    created_at: { N: String(NOW - 1000) },
    ttl: { N: String(expires_at) },
  };
  if (coordinator_id !== undefined) item.coordinator_id = { S: coordinator_id };
  if (form_submission_id !== undefined) {
    item.form_submission_id = { S: form_submission_id };
  }
  return item;
};

// A fake ddb whose send() returns the given GetItem result, and records the input.
const fakeDdb = (getItemResult) => {
  const send = jest.fn().mockResolvedValue(getItemResult);
  return { send };
};

beforeEach(() => {
  ddbMock.reset();
});

// ─── input validation ────────────────────────────────────────────────────────────────

describe('resolveBinding — input validation', () => {
  test('throws when tenantId is missing', async () => {
    await expect(
      resolveBinding({ sessionId: SESSION, deps: { ddb: fakeDdb({}) } })
    ).rejects.toThrow('tenantId is required');
  });

  test('throws when sessionId is missing', async () => {
    await expect(
      resolveBinding({ tenantId: TENANT, deps: { ddb: fakeDdb({}) } })
    ).rejects.toThrow('sessionId is required');
  });

  test('throws when called with no args', async () => {
    await expect(resolveBinding()).rejects.toThrow('tenantId is required');
  });
});

// ─── the DI seam (deps = { ddb, now }) ───────────────────────────────────────────────

describe('resolveBinding — DI seam', () => {
  test('valid unexpired binding → the §B12 shape', async () => {
    const ddb = fakeDdb({ Item: rowItem() });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toEqual({
      intent: 'rescheduling_intent',
      booking_id: 'bk-1',
      expires_at: NOW + 30 * 60 * 1000,
      session_id: BINDING_SK,
    });
    // No optionals present on a bare row.
    expect(result).not.toHaveProperty('coordinator_id');
    expect(result).not.toHaveProperty('form_submission_id');
  });

  test('reads the binding#-prefixed SK under the tenant PK', async () => {
    const ddb = fakeDdb({ Item: rowItem() });

    await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(ddb.send).toHaveBeenCalledTimes(1);
    const cmd = ddb.send.mock.calls[0][0];
    expect(cmd).toBeInstanceOf(GetItemCommand);
    expect(cmd.input.Key).toEqual({
      tenantId: { S: TENANT },
      session_id: { S: BINDING_SK },
    });
  });

  test('expired binding (now === expires_at) → null', async () => {
    const ddb = fakeDdb({ Item: rowItem({ expires_at: NOW }) });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
  });

  test('expired binding (now > expires_at) → null', async () => {
    const ddb = fakeDdb({ Item: rowItem({ expires_at: NOW - 1 }) });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
  });

  test('missing row → null', async () => {
    const ddb = fakeDdb({}); // no Item

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
  });

  test('GetItem returns nullish response → null', async () => {
    const ddb = fakeDdb(undefined);

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
  });

  test('cross-tenant lookup misses → null (GetItem under wrong PK returns no Item)', async () => {
    // A session minted under TENANT, queried under another tenant: the real table
    // returns no Item for the (otherTenant, binding#session) key. Modeled as an empty result.
    const ddb = fakeDdb({});

    const result = await resolveBinding({
      tenantId: 'OTHER999999',
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
    const cmd = ddb.send.mock.calls[0][0];
    expect(cmd.input.Key.tenantId).toEqual({ S: 'OTHER999999' });
  });

  test('malformed row with no expires_at → null (fail closed)', async () => {
    const item = rowItem();
    delete item.expires_at;
    const ddb = fakeDdb({ Item: item });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
  });

  test('malformed row with non-numeric expires_at → null (fail closed)', async () => {
    const item = rowItem();
    item.expires_at = { S: 'not-a-number' };
    const ddb = fakeDdb({ Item: item });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toBeNull();
  });

  test('forward-compatible: includes coordinator_id and form_submission_id when present', async () => {
    const ddb = fakeDdb({
      Item: rowItem({
        intent: 'recovery_intent',
        coordinator_id: 'coord-7',
        form_submission_id: 'fs-42',
      }),
    });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result).toEqual({
      intent: 'recovery_intent',
      booking_id: 'bk-1',
      expires_at: NOW + 30 * 60 * 1000,
      session_id: BINDING_SK,
      coordinator_id: 'coord-7',
      form_submission_id: 'fs-42',
    });
  });

  test('forward-compatible: cancellation_intent with only form_submission_id absent', async () => {
    const ddb = fakeDdb({
      Item: rowItem({ intent: 'cancellation_intent', coordinator_id: 'coord-1' }),
    });

    const result = await resolveBinding({
      tenantId: TENANT,
      sessionId: SESSION,
      deps: { ddb, now: fixedNow },
    });

    expect(result.intent).toBe('cancellation_intent');
    expect(result.coordinator_id).toBe('coord-1');
    expect(result).not.toHaveProperty('form_submission_id');
  });
});

// ─── default DI implementations (real default ddb client + default Date.now) ─────────

describe('resolveBinding — default ddb client + default clock', () => {
  test('uses the default DynamoDBClient when deps.ddb is omitted', async () => {
    ddbMock.on(GetItemCommand).resolves({ Item: rowItem() });

    // No deps at all → default ddb (aws-sdk-client-mock-intercepted) + default Date.now.
    const result = await resolveBinding({ tenantId: TENANT, sessionId: SESSION });

    expect(result).toMatchObject({
      intent: 'rescheduling_intent',
      booking_id: 'bk-1',
      session_id: BINDING_SK,
    });
    expect(ddbMock).toHaveReceivedCommandWith(GetItemCommand, {
      Key: { tenantId: { S: TENANT }, session_id: { S: BINDING_SK } },
    });
  });

  test('default Date.now clock expires a past-dated binding → null', async () => {
    // expires_at well in the past relative to the real wall clock.
    ddbMock.on(GetItemCommand).resolves({ Item: rowItem({ expires_at: 1 }) });

    const result = await resolveBinding({ tenantId: TENANT, sessionId: SESSION });

    expect(result).toBeNull();
  });
});
