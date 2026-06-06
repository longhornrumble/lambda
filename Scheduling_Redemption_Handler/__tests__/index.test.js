'use strict';

/**
 * Integration tests — exercise the FULL redemption flow through the REAL
 * shared/scheduling/tokens.js (sign + verify + one-time redeem), with only the AWS
 * clients (DynamoDB, Secrets Manager) mocked. This proves the §13.8 routing, the §13.9
 * HTTP-status contract, the §13.7 one-time-use 410, and the §B10 binding write end-to-end
 * — not just the status-code mapping (that is covered structurally in routing.test.js).
 */

const crypto = require('crypto');
const { mockClient } = require('aws-sdk-client-mock');
const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const tokens = require('../../shared/scheduling/tokens.js');
// Feature gate: default-enabled so the existing redeem/bind/redirect tests exercise the
// real flow. The disabled (403) path is covered in the dedicated describe block below.
jest.mock('../../shared/scheduling/featureGate', () => ({
  isSchedulingEnabledForTenant: jest.fn().mockResolvedValue(true),
}));
const featureGate = require('../../shared/scheduling/featureGate');
const { handler } = require('../index.js');

const ddbMock = mockClient(DynamoDBClient);
const smMock = mockClient(SecretsManagerClient);
const lambdaMock = mockClient(LambdaClient); // E6 disposition → notify.js Lambda invokes

const KEY = 'test-signing-key-0123456789abcdef'; // ≥16 chars (MIN_SIGNING_KEY_LENGTH)
const JTI_TABLE = 'picasso-token-jti-blacklist-staging';
const SESSION_TABLE = 'picasso-conversation-scheduling-session-staging';
const TENANT = 'TEN-D4-TEST';
const BOOKING = 'bk-abc123';
const CHAT_BASE = 'https://staging.chat.myrecruiter.ai';
const FAR_FUTURE_START = '2999-01-01T10:00:00Z';

function evt(path, token, method = 'GET') {
  return {
    rawPath: path,
    requestContext: { http: { method, path } },
    queryStringParameters: token != null ? { t: token } : undefined,
  };
}

// Hand-craft a HS256 JWS with an arbitrary issuer (sign() always uses our issuer, so this
// is how we forge a cross-class chat-session token to prove the iss rejection).
function craftToken(payload, key = KEY) {
  const b64 = (o) =>
    Buffer.from(JSON.stringify(o)).toString('base64url');
  const head = b64({ alg: 'HS256', typ: 'JWT' });
  const body = b64(payload);
  const sig = crypto
    .createHmac('sha256', key)
    .update(`${head}.${body}`)
    .digest('base64url');
  return `${head}.${body}.${sig}`;
}

function bookingItem() {
  return {
    Item: {
      tenantId: { S: TENANT },
      booking_id: { S: BOOKING },
      coordinator_email: { S: 'maya@org.example' },
      start_at: { S: FAR_FUTURE_START },
      status: { S: 'booked' },
    },
  };
}

beforeEach(() => {
  ddbMock.reset();
  smMock.reset();
  lambdaMock.reset();
  smMock.on(GetSecretValueCommand).resolves({ SecretString: KEY });
  // Default happy DDB: booking present, both PutItems succeed.
  ddbMock.on(GetItemCommand).resolves(bookingItem());
  ddbMock.on(PutItemCommand).resolves({});
  // E6 disposition: conditional transition returns the booked→terminal ALL_NEW row by default.
  ddbMock.on(UpdateItemCommand).resolves({
    Attributes: {
      status: { S: 'completed' },
      coordinator_email: { S: 'maya@org.example' },
      attendee_name: { S: 'Sam Patel' },
      appointment_type_name: { S: 'intake call' },
      start_at: { S: FAR_FUTURE_START },
    },
  });
  // notify.js dispatches (email + reoffer) go through the send_email / SMS_Sender Lambdas.
  lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
});

describe('feature gate: scheduling_enabled (OFF unless config opts in)', () => {
  test('disabled tenant → 403 leak-free page, NO binding write (jti still burned)', async () => {
    featureGate.isSchedulingEnabledForTenant.mockResolvedValueOnce(false);
    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel', t));
    expect(res.statusCode).toBe(403);
    expect(res.body).toMatch(/unavailable/i);
    expect(res.body).not.toMatch(/token|jwt|signature/i);
    // gate ran for the token's tenant
    expect(featureGate.isSchedulingEnabledForTenant).toHaveBeenCalledWith(TENANT);
    // no §B10 binding written to the session table (the disabled path returns first)
    const sessionPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === SESSION_TABLE);
    expect(sessionPuts.length).toBe(0);
    // SECURITY ORDERING: the gate runs AFTER redeem() so the one-time jti is STILL burned
    // (the link is single-use regardless of the gate). Assert the burn happened — this is
    // the property the test name claims; a refactor moving the gate before redeem() must fail.
    const jtiPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === JTI_TABLE);
    expect(jtiPuts.length).toBe(1);
  });
});

describe('§13.8 routing', () => {
  test('unknown path → 404', async () => {
    const res = await handler(evt('/nope', 'whatever'));
    expect(res.statusCode).toBe(404);
  });

  test('missing token → generic 400 (no detail leak)', async () => {
    const res = await handler(evt('/cancel', null));
    expect(res.statusCode).toBe(400);
    expect(res.body).not.toMatch(/token|jwt|signature/i);
  });

  test('trailing slash is tolerated', async () => {
    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel/', t));
    expect(res.statusCode).toBe(302);
  });
});

describe('volunteer-facing: valid → bind + redirect', () => {
  test('cancel → 302, writes cancellation_intent binding, redirects to chat', async () => {
    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel', t));

    expect(res.statusCode).toBe(302);
    expect(res.headers.location).toMatch(
      new RegExp(`^${CHAT_BASE}/\\?session=[0-9a-f-]{36}$`)
    );

    // §13.7: the jti was burned (PutItem to the jti table).
    const jtiPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === JTI_TABLE);
    expect(jtiPuts).toHaveLength(1);

    // §B10 binding row.
    const bindPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === SESSION_TABLE);
    expect(bindPuts).toHaveLength(1);
    const item = bindPuts[0].args[0].input.Item;
    expect(item.tenantId.S).toBe(TENANT);
    expect(item.session_id.S).toMatch(/^binding#[0-9a-f-]{36}$/); // namespaced SK value
    expect(item.intent.S).toBe('cancellation_intent');
    expect(item.booking_id.S).toBe(BOOKING);
    expect(Number(item.expires_at.N)).toBeGreaterThan(Date.now()); // epoch ms
    expect(Number(item.ttl.N)).toBeLessThan(Number(item.expires_at.N)); // ttl in seconds
    expect(item.form_submission_id).toBeUndefined();

    // The session id in the redirect matches the binding SK.
    const sid = res.headers.location.split('session=')[1];
    expect(item.session_id.S).toBe(`binding#${sid}`);
  });

  test('reschedule → 302, writes rescheduling_intent binding', async () => {
    const t = await tokens.sign(
      'reschedule',
      {
        tenant_id: TENANT,
        booking_id: BOOKING,
        start_at: FAR_FUTURE_START,
        cancellation_window_hours: 24,
      },
      { signingKey: KEY }
    );
    const res = await handler(evt('/reschedule', t));
    expect(res.statusCode).toBe(302);
    const item = ddbMock
      .commandCalls(PutItemCommand)
      .find((c) => c.args[0].input.TableName === SESSION_TABLE).args[0].input.Item;
    expect(item.intent.S).toBe('rescheduling_intent');
  });

  test('recovery (/resume) → 302, recovery_intent binding, no booking lookup, carries form_submission_id', async () => {
    const t = await tokens.sign(
      'post_application_recovery',
      { tenant_id: TENANT, form_submission_id: 'fs-789' },
      { signingKey: KEY }
    );
    const res = await handler(evt('/resume', t));
    expect(res.statusCode).toBe(302);

    // No booking_id on the token → no booking GetItem.
    expect(ddbMock.commandCalls(GetItemCommand)).toHaveLength(0);

    const item = ddbMock
      .commandCalls(PutItemCommand)
      .find((c) => c.args[0].input.TableName === SESSION_TABLE).args[0].input.Item;
    expect(item.intent.S).toBe('recovery_intent');
    expect(item.form_submission_id.S).toBe('fs-789');
    expect(item.booking_id).toBeUndefined();
    // recovery uses the token's own exp (iat+14d, seconds) → expires_at in ms.
    expect(Number(item.expires_at.N)).toBeGreaterThan(Date.now());
  });

  test('missing booking → 404 not-found page (no execution)', async () => {
    ddbMock.on(GetItemCommand).resolves({}); // no Item
    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel', t));
    expect(res.statusCode).toBe(404);
    // jti burned, but NO binding written.
    const bindPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === SESSION_TABLE);
    expect(bindPuts).toHaveLength(0);
  });

  // C-3: distinguish "token carried no booking_id" (invalid link → 400) from
  // "booking_id present but not found" (404 above). recovery legitimately has none.
  test('C-3: cancel token with no booking_id → 400 (invalid link), no binding', async () => {
    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, start_at: FAR_FUTURE_START }, // deliberately no booking_id
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel', t));
    expect(res.statusCode).toBe(400);
    expect(res.body).not.toMatch(/token|jwt|signature/i); // still no detail leak
    const bindPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === SESSION_TABLE);
    expect(bindPuts).toHaveLength(0);
  });
});

describe('SR-2: SESSION_BINDING_TTL_SECONDS validated at module load', () => {
  test.each([['abc'], ['0'], ['-5']])(
    'invalid value %p fails fast (no silent NaN/expired binding)',
    (bad) => {
      const prev = process.env.SESSION_BINDING_TTL_SECONDS;
      process.env.SESSION_BINDING_TTL_SECONDS = bad;
      try {
        expect(() =>
          jest.isolateModules(() => require('../index.js'))
        ).toThrow(/SESSION_BINDING_TTL_SECONDS/);
      } finally {
        if (prev === undefined) delete process.env.SESSION_BINDING_TTL_SECONDS;
        else process.env.SESSION_BINDING_TTL_SECONDS = prev;
      }
    }
  );
});

describe('interviewer attendance disposition (E6 — WS-E-ATTEND wires the action)', () => {
  test.each([
    ['/attended/met', 'attended_yes', 'completed'],
    ['/attended/noshow', 'no_show', 'no_show'],
    ['/attended/noconnect', 'didnt_connect', 'coordinator_no_show'],
  ])('%s valid → 200, transitions booking to %s (conditional write), no binding', async (path, purpose, target) => {
    ddbMock.on(UpdateItemCommand).resolves({
      Attributes: {
        status: { S: target },
        coordinator_email: { S: 'maya@org.example' },
        attendee_name: { S: 'Sam Patel' },
        appointment_type_name: { S: 'intake call' },
        start_at: { S: FAR_FUTURE_START },
      },
    });
    const t = await tokens.sign(
      purpose,
      { tenant_id: TENANT, booking_id: BOOKING, event_end: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt(path, t));
    expect(res.statusCode).toBe(200);
    // a real conditional booked→terminal transition ran
    const updates = ddbMock.commandCalls(UpdateItemCommand);
    expect(updates).toHaveLength(1);
    const u = updates[0].args[0].input;
    expect(u.ExpressionAttributeValues[':target'].S).toBe(target);
    expect(u.ConditionExpression).toContain('#st = :booked');
    // attendance disposition writes NO §B10 session binding (that is the volunteer path)
    const bindPuts = ddbMock
      .commandCalls(PutItemCommand)
      .filter((c) => c.args[0].input.TableName === SESSION_TABLE);
    expect(bindPuts).toHaveLength(0);
  });

  test('no_show → 200 + volunteer reoffer dispatched (Lambda invoke)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({
      Attributes: {
        status: { S: 'no_show' },
        coordinator_email: { S: 'maya@org.example' },
        attendee_email: { S: 'sam@example.com' },
        attendee_name: { S: 'Sam Patel' },
        appointment_type_name: { S: 'intake call' },
        start_at: { S: FAR_FUTURE_START },
      },
    });
    const t = await tokens.sign(
      'no_show',
      { tenant_id: TENANT, booking_id: BOOKING, event_end: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/attended/noshow', t));
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatch(/new time/i);
    // reoffer email + interviewer confirmation both route through send_email (Lambda invoke)
    expect(lambdaMock.commandCalls(InvokeCommand).length).toBeGreaterThanOrEqual(1);
  });

  test('idempotent: already-resolved (ConditionalCheckFailed) → 200 already-recorded, no dispatch', async () => {
    const condErr = new Error('cond'); condErr.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(condErr);
    const t = await tokens.sign(
      'attended_yes',
      { tenant_id: TENANT, booking_id: BOOKING, event_end: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/attended/met', t));
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatch(/already recorded/i);
    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(0);
  });

  test('attendance token without booking_id → benign 200 ack, no transition', async () => {
    const t = await tokens.sign(
      'attended_yes',
      { tenant_id: TENANT, event_end: FAR_FUTURE_START }, // no booking_id
      { signingKey: KEY }
    );
    const res = await handler(evt('/attended/met', t));
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatch(/got it/i);
    expect(ddbMock.commandCalls(UpdateItemCommand)).toHaveLength(0);
  });

  test('disposition write error (non-conditional) → 500, no detail leak', async () => {
    ddbMock.on(UpdateItemCommand).rejects(new Error('throttled'));
    const t = await tokens.sign(
      'attended_yes',
      { tenant_id: TENANT, booking_id: BOOKING, event_end: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/attended/met', t));
    expect(res.statusCode).toBe(500);
    expect(res.body).not.toMatch(/throttled|token|jwt/i);
  });
});

describe('§13.9 failure-mode HTTP contract', () => {
  test('tampered token → 400 generic (no detail leak)', async () => {
    const res = await handler(evt('/cancel', 'not-a-real-token'));
    expect(res.statusCode).toBe(400);
    expect(res.body).not.toMatch(/signature|issuer|expired|jwt/i);
  });

  test('expired token → 401', async () => {
    // Sign with a past `now`; the §B4 floor makes exp = iat+900s, still < real now.
    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: '1970-01-01T00:33:20Z' },
      { signingKey: KEY, now: 1000 }
    );
    const res = await handler(evt('/cancel', t));
    expect(res.statusCode).toBe(401);
    // No jti burned on an expired token (verify throws before the PutItem).
    expect(
      ddbMock
        .commandCalls(PutItemCommand)
        .filter((c) => c.args[0].input.TableName === JTI_TABLE)
    ).toHaveLength(0);
  });

  test('wrong purpose for slug → 403 (valid reschedule token on /cancel)', async () => {
    const t = await tokens.sign(
      'reschedule',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel', t));
    expect(res.statusCode).toBe(403);
  });

  test('cross-class chat-session token → 401 (iss rejection)', async () => {
    const forged = craftToken({
      iss: 'myrecruiter-chat', // NOT myrecruiter-scheduling
      iat: 1000,
      exp: Math.floor(Date.now() / 1000) + 3600,
      jti: crypto.randomUUID(),
      purpose: 'cancel',
      tenant_id: TENANT,
      booking_id: BOOKING,
    });
    const res = await handler(evt('/cancel', forged));
    expect(res.statusCode).toBe(401);
  });

  test('replay (already-redeemed jti) → 410 Gone', async () => {
    const condErr = new Error('conditional check failed');
    condErr.name = 'ConditionalCheckFailedException';
    ddbMock.on(PutItemCommand, { TableName: JTI_TABLE }).rejects(condErr);

    const t = await tokens.sign(
      'cancel',
      { tenant_id: TENANT, booking_id: BOOKING, start_at: FAR_FUTURE_START },
      { signingKey: KEY }
    );
    const res = await handler(evt('/cancel', t));
    expect(res.statusCode).toBe(410);
  });

  test('bad signature (right shape, wrong key) → 401', async () => {
    const forged = craftToken(
      {
        iss: 'myrecruiter-scheduling',
        iat: 1000,
        exp: Math.floor(Date.now() / 1000) + 3600,
        jti: crypto.randomUUID(),
        purpose: 'cancel',
        tenant_id: TENANT,
        booking_id: BOOKING,
      },
      'a-totally-different-signing-key-xyz'
    );
    const res = await handler(evt('/cancel', forged));
    expect(res.statusCode).toBe(401);
  });
});
