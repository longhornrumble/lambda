'use strict';

/**
 * Unit tests — isolate the handler's routing, the §13.9 code→status mapping, the 500
 * unhappy paths, the §B10 write-failure handling, and the D5 coordinator-contact seam,
 * with shared/scheduling/tokens.js's redeem() mocked (TokenError kept REAL so the
 * instanceof check is exercised). The real-token end-to-end matrix lives in index.test.js.
 */

const { mockClient } = require('aws-sdk-client-mock');
const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

jest.mock('../../shared/scheduling/tokens.js', () => {
  const actual = jest.requireActual('../../shared/scheduling/tokens.js');
  return { ...actual, redeem: jest.fn() };
});
const { redeem, TokenError } = require('../../shared/scheduling/tokens.js');
// Feature gate default-enabled here; these tests exercise routing/DDB-failure paths, not
// the gate (its disabled path is covered in index.test.js).
jest.mock('../../shared/scheduling/featureGate', () => ({
  isSchedulingEnabledForTenant: jest.fn().mockResolvedValue(true),
}));
const { handler, _internal } = require('../index.js');

const ddbMock = mockClient(DynamoDBClient);
const TENANT = 'TEN-U';
const BOOKING = 'bk-u-1';

function evt(path, token = 'tok') {
  return { rawPath: path, queryStringParameters: { t: token } };
}

beforeEach(() => {
  ddbMock.reset();
  ddbMock.on(GetItemCommand).resolves({
    Item: { tenantId: { S: TENANT }, booking_id: { S: BOOKING } },
  });
  ddbMock.on(PutItemCommand).resolves({});
  redeem.mockReset();
});

describe('§13.9 code → HTTP status mapping (work-order contract, not TokenError.status)', () => {
  test.each([
    ['malformed', 400],
    ['unknown_purpose', 400],
    ['invalid_signature', 401],
    ['invalid_issuer', 401],
    ['expired', 401], // intentionally 401 even though TokenError.status is 410
    ['purpose_mismatch', 403],
    ['tenant_mismatch', 403],
    ['reused', 410],
    ['signing_key_unavailable', 500],
  ])('TokenError(%s) → %i', async (code, status) => {
    redeem.mockRejectedValue(new TokenError(code, 999 /* ignored */, code));
    const res = await handler(evt('/cancel'));
    expect(res.statusCode).toBe(status);
  });

  test('unexpected non-TokenError from redeem → 500 (no detail leak)', async () => {
    redeem.mockRejectedValue(new Error('DynamoDB throttled'));
    const res = await handler(evt('/cancel'));
    expect(res.statusCode).toBe(500);
    expect(res.body).not.toMatch(/DynamoDB|throttled/i);
  });
});

describe('unhappy DDB paths after a valid redeem', () => {
  beforeEach(() => {
    redeem.mockResolvedValue({
      tenant_id: TENANT,
      booking_id: BOOKING,
      purpose: 'cancel',
      exp: Math.floor(Date.now() / 1000) + 3600,
    });
  });

  test('booking GetItem failure → 500', async () => {
    ddbMock.on(GetItemCommand).rejects(new Error('ddb down'));
    const res = await handler(evt('/cancel'));
    expect(res.statusCode).toBe(500);
  });

  test('binding PutItem failure → 500 (jti already burned)', async () => {
    ddbMock.on(PutItemCommand).rejects(new Error('ddb down'));
    const res = await handler(evt('/cancel'));
    expect(res.statusCode).toBe(500);
  });
});

describe('D5 coordinator-contact seam (failurePage)', () => {
  test('renders escaped name + work email when present', () => {
    const res = _internal.failurePage(401, {
      coordinator: { name: 'Maya <O\'Neil>', email: 'maya@org.example' },
    });
    expect(res.body).toContain('maya@org.example');
    expect(res.body).toContain('Maya &lt;O&#39;Neil&gt;'); // escaped
    expect(res.body).toContain('mailto:maya@org.example');
  });

  test('email only (no name) falls back to generic referent', () => {
    const res = _internal.failurePage(410, {
      coordinator: { email: 'x@y.example' },
    });
    expect(res.body).toContain('your coordinator');
    expect(res.body).toContain('x@y.example');
  });

  test('name only (no email) renders name without mailto', () => {
    const res = _internal.failurePage(401, { coordinator: { name: 'Sam' } });
    expect(res.body).toContain('Sam');
    expect(res.body).not.toContain('mailto:');
  });

  test('no coordinator → no contact block', () => {
    const res = _internal.failurePage(400);
    expect(res.body).not.toContain('mailto:');
    expect(res.body).not.toContain('reach out');
  });

  test('never renders a phone number field (only name + email are read)', () => {
    // The seam intentionally ignores any phone — pass one and confirm it is not surfaced.
    const res = _internal.failurePage(401, {
      coordinator: { name: 'Sam', email: 's@x.example', phone: '+15125550000' },
    });
    expect(res.body).not.toContain('5125550000');
  });
});

describe('request parsing fallbacks', () => {
  test('getPath falls back to requestContext.http.path when rawPath is absent', () => {
    expect(_internal.getPath({ requestContext: { http: { path: '/reschedule' } } })).toBe(
      '/reschedule'
    );
  });

  test('getPath defaults to / when nothing present', () => {
    expect(_internal.getPath({})).toBe('/');
  });

  test('getToken returns null when no query params', () => {
    expect(_internal.getToken({})).toBeNull();
  });

  test('unknown path → 404 without calling redeem', async () => {
    const res = await handler(evt('/bogus'));
    expect(res.statusCode).toBe(404);
    expect(redeem).not.toHaveBeenCalled();
  });
});
