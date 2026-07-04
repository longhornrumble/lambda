'use strict';

/**
 * Scheduling_Page_Api tests — the deterministic propose/mutate gateway. Exercises the full
 * flow through the REAL shared/scheduling/sessionBinding.js resolveBinding (with DynamoDB
 * mocked): hash→tenant (registry Query), binding read (session table), booking load, and
 * the BCH invoke (Lambda mocked). Covers the §13.9-style HTTP contract + failure paths.
 */

const { mockClient } = require('aws-sdk-client-mock');
const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const { handler, _internal } = require('../index.js');

const ddbMock = mockClient(DynamoDBClient);
const lambdaMock = mockClient(LambdaClient);

const TENANT = 'TEN-GW';
const HASH = 'hsh-gw';
const BOOKING = 'booking#abc';
const SESSION = 'sess-uuid-1';
const REGISTRY_TABLE = 'picasso-tenant-registry-staging';
const SESSION_TABLE = 'picasso-conversation-scheduling-session-staging';
const BOOKING_TABLE = 'picasso-booking-staging';
const FAR_FUTURE_MS = 4102444800000; // year 2100

function evt(bodyObj, method = 'POST') {
  return {
    requestContext: { http: { method } },
    body: bodyObj == null ? undefined : JSON.stringify(bodyObj),
    isBase64Encoded: false,
  };
}

function bindingRow(intent) {
  return {
    Item: {
      tenantId: { S: TENANT },
      session_id: { S: `binding#${SESSION}` },
      intent: { S: intent },
      booking_id: { S: BOOKING },
      expires_at: { N: String(FAR_FUTURE_MS) },
      ttl: { N: String(Math.floor(FAR_FUTURE_MS / 1000)) },
    },
  };
}

function bookingRow(extra = {}) {
  return {
    Item: {
      tenantId: { S: TENANT },
      booking_id: { S: BOOKING },
      appointment_type_id: { S: 'intro-call' },
      appointment_type_name: { S: 'Intro Call' },
      coordinator_email: { S: 'maya@org.example' },
      resource_id: { S: 'emp-maya' },
      start_at: { S: '2026-06-15T15:30:00Z' },
      timezone: { S: 'America/Chicago' },
      external_event_id: { S: 'evt-1' },
      status: { S: 'booked' },
      ...extra,
    },
  };
}

function bchPayload(obj) {
  return { Payload: new TextEncoder().encode(JSON.stringify(obj)) };
}

beforeEach(() => {
  ddbMock.reset();
  lambdaMock.reset();
  // hash → tenantId (registry GSI Query)
  ddbMock.on(QueryCommand).resolves({ Items: [{ tenantId: { S: TENANT } }] });
  // binding row (rescheduling by default) + booking row — table-specific matchers
  ddbMock.on(GetItemCommand, { TableName: SESSION_TABLE }).resolves(bindingRow('rescheduling_intent'));
  ddbMock.on(GetItemCommand, { TableName: BOOKING_TABLE }).resolves(bookingRow());
  // BCH invoke default: propose ok
  lambdaMock.on(InvokeCommand).resolves(
    bchPayload({ outcome: 'ok', slots: [{ slotId: 's1', label: '1:00 PM', start: 'x', end: 'y' }], context: { duration_minutes: 30 } })
  );
});

describe('CORS + parsing', () => {
  test('OPTIONS → 204 with CORS headers', async () => {
    const res = await handler(evt(null, 'OPTIONS'));
    expect(res.statusCode).toBe(204);
    expect(res.headers['access-control-allow-origin']).toBe('https://staging.chat.myrecruiter.ai');
  });

  test('malformed JSON body → 400', async () => {
    const res = await handler({ requestContext: { http: { method: 'POST' } }, body: '{not json' });
    expect(res.statusCode).toBe(400);
  });

  test('missing params → 400', async () => {
    const res = await handler(evt({ action: 'propose' }));
    expect(res.statusCode).toBe(400);
  });

  test('unknown action → 400', async () => {
    const res = await handler(evt({ action: 'frobnicate', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(400);
  });
});

describe('tenant + binding resolution', () => {
  test('SR-2: unknown tenant hash → 401 (indistinguishable from no binding — no tenant oracle)', async () => {
    ddbMock.on(QueryCommand).resolves({ Items: [] });
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.body).error).toBe('session_expired');
  });

  test('binding missing/expired → 401', async () => {
    ddbMock.on(GetItemCommand, { TableName: SESSION_TABLE }).resolves({}); // no Item
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(401);
  });

  test('booking not found → 404', async () => {
    ddbMock.on(GetItemCommand, { TableName: BOOKING_TABLE }).resolves({});
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(404);
  });

  test('registry read error → 500', async () => {
    ddbMock.on(QueryCommand).rejects(new Error('ddb down'));
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(500);
  });
});

describe('propose', () => {
  test('happy → 200 with slots; invokes BCH scheduling_propose with appt type + day window', async () => {
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION, date: '2026-06-18' }));
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.outcome).toBe('ok');
    expect(body.slots).toHaveLength(1);
    expect(body.appointment_label).toBe('Intro Call');
    expect(body.current_start_at).toBe('2026-06-15T15:30:00Z');
    expect(body.timezone).toBe('America/Chicago');
    const call = lambdaMock.commandCalls(InvokeCommand)[0];
    const payload = JSON.parse(Buffer.from(call.args[0].input.Payload).toString('utf8'));
    expect(payload.action).toBe('scheduling_propose');
    expect(payload.appointmentTypeId).toBe('intro-call');
    expect(payload.userTimeZone).toBe('America/Chicago');
    expect(payload.date_window).toEqual({ start: '2026-06-18T00:00:00', end: '2026-06-18T23:59:59' });
  });

  test('strips candidateResourceIds (coordinator identity) from the browser response — §10.4 leak regression, 2026-07-03', async () => {
    lambdaMock.on(InvokeCommand).resolves(
      bchPayload({
        outcome: 'ok',
        slots: [
          {
            slotId: 's1',
            label: '1:00 PM',
            start: 'x',
            end: 'y',
            candidateResourceIds: ['coordinator@org.example'],
          },
        ],
        context: { duration_minutes: 30 },
      })
    );
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.slots).toEqual([{ slotId: 's1', label: '1:00 PM', start: 'x', end: 'y' }]);
    expect(res.body).not.toContain('candidateResourceIds');
    expect(res.body).not.toContain('coordinator@org.example');
  });

  test('no date → propose without date_window (whole horizon)', async () => {
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(200);
    const payload = JSON.parse(Buffer.from(lambdaMock.commandCalls(InvokeCommand)[0].args[0].input.Payload).toString('utf8'));
    expect(payload.date_window).toBeUndefined();
  });

  test('BCH invoke error → 502', async () => {
    lambdaMock.on(InvokeCommand).rejects(new Error('lambda boom'));
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION }));
    expect(res.statusCode).toBe(502);
  });

  test('BLOCK-1: propose with a cancellation-intent binding → 200 hero-only (no slots, BCH NOT invoked)', async () => {
    // The cancel page calls propose on mount purely for the hero (current appointment). It must
    // get that hero, but with NO availability + NO BCH freeBusy call (BLOCK-1's security intent).
    ddbMock.on(GetItemCommand, { TableName: SESSION_TABLE }).resolves(bindingRow('cancellation_intent'));
    const res = await handler(evt({ action: 'propose', t: HASH, session: SESSION, date: '2026-06-18' }));
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.outcome).toBe('ok');
    expect(body.slots).toEqual([]); // no coordinator availability leaked to a cancel binding
    expect(body.appointment_label).toBe('Intro Call'); // hero from the loaded booking
    expect(body.current_start_at).toBe('2026-06-15T15:30:00Z');
    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(0); // no freeBusy quota burn
  });
});

describe('mutate', () => {
  test('reschedule happy → 200; invokes BCH scheduling_mutate with newSlot + coordinatorId + booking', async () => {
    lambdaMock.on(InvokeCommand).resolves(bchPayload({ outcome: 'success', booking: {} }));
    const res = await handler(evt({
      action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule',
      newSlot: { start: '2026-06-18T18:00:00Z', end: '2026-06-18T18:30:00Z' },
    }));
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body).outcome).toBe('success');
    const payload = JSON.parse(Buffer.from(lambdaMock.commandCalls(InvokeCommand)[0].args[0].input.Payload).toString('utf8'));
    expect(payload.action).toBe('scheduling_mutate');
    expect(payload.mutation).toBe('reschedule');
    expect(payload.coordinatorId).toBe('emp-maya'); // resource_id wins
    expect(payload.newSlot.start).toBe('2026-06-18T18:00:00Z');
    expect(payload.booking.appointment_type_id).toBe('intro-call');
  });

  test('reschedule missing newSlot → 400', async () => {
    const res = await handler(evt({ action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule' }));
    expect(res.statusCode).toBe(400);
  });

  test('SR-1: reschedule with non-ISO newSlot → 400 invalid_newSlot; BCH not invoked', async () => {
    const res = await handler(evt({
      action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule',
      newSlot: { start: 'not-a-date', end: 'also-bad' },
    }));
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toBe('invalid_newSlot');
    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(0);
  });

  test('SR-1: reschedule with start >= end → 400 invalid_newSlot', async () => {
    const res = await handler(evt({
      action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule',
      newSlot: { start: '2026-06-18T18:30:00Z', end: '2026-06-18T18:00:00Z' },
    }));
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toBe('invalid_newSlot');
  });

  test('SR-1: reschedule with oversized newSlot string → 400 invalid_newSlot', async () => {
    const res = await handler(evt({
      action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule',
      newSlot: { start: '2026-06-18T18:00:00Z' + 'A'.repeat(60), end: '2026-06-18T18:30:00Z' },
    }));
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toBe('invalid_newSlot');
  });

  test('SR-1: reschedule with a non-string newSlot field → 400 invalid_newSlot', async () => {
    const res = await handler(evt({
      action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule',
      newSlot: { start: 12345, end: '2026-06-18T18:30:00Z' },
    }));
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toBe('invalid_newSlot');
  });

  test('intent mismatch (reschedule binding, cancel request) → 403', async () => {
    const res = await handler(evt({ action: 'mutate', t: HASH, session: SESSION, mutation: 'cancel' }));
    expect(res.statusCode).toBe(403);
  });

  test('cancel happy (cancellation binding) → 200', async () => {
    ddbMock.on(GetItemCommand, { TableName: SESSION_TABLE }).resolves(bindingRow('cancellation_intent'));
    lambdaMock.on(InvokeCommand).resolves(bchPayload({ outcome: 'deleted' }));
    const res = await handler(evt({ action: 'mutate', t: HASH, session: SESSION, mutation: 'cancel' }));
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body).outcome).toBe('deleted');
  });

  test('bad mutation value → 400', async () => {
    const res = await handler(evt({ action: 'mutate', t: HASH, session: SESSION, mutation: 'explode' }));
    expect(res.statusCode).toBe(400);
  });

  test('BCH outcome failed → 502', async () => {
    ddbMock.on(GetItemCommand, { TableName: SESSION_TABLE }).resolves(bindingRow('cancellation_intent'));
    lambdaMock.on(InvokeCommand).resolves(bchPayload({ outcome: 'failed' }));
    const res = await handler(evt({ action: 'mutate', t: HASH, session: SESSION, mutation: 'cancel' }));
    expect(res.statusCode).toBe(502);
  });

  test('no coordinator on booking → 409', async () => {
    // A booking row with NO coordinator fields (real DDB omits absent attrs entirely).
    ddbMock.on(GetItemCommand, { TableName: BOOKING_TABLE }).resolves({
      Item: {
        tenantId: { S: TENANT },
        booking_id: { S: BOOKING },
        appointment_type_id: { S: 'intro-call' },
        start_at: { S: '2026-06-15T15:30:00Z' },
        timezone: { S: 'America/Chicago' },
        status: { S: 'booked' },
      },
    });
    lambdaMock.on(InvokeCommand).resolves(bchPayload({ outcome: 'success' }));
    const res = await handler(evt({
      action: 'mutate', t: HASH, session: SESSION, mutation: 'reschedule',
      newSlot: { start: 'a', end: 'b' },
    }));
    expect(res.statusCode).toBe(409);
  });
});

describe('_internal helpers', () => {
  test('coordinatorIdOf precedence: binding.coordinator_id > resource_id > coordinator_email', () => {
    expect(_internal.coordinatorIdOf({ resource_id: 'r', coordinator_email: 'e' }, { coordinator_id: 'b' })).toBe('b');
    expect(_internal.coordinatorIdOf({ resource_id: 'r', coordinator_email: 'e' }, {})).toBe('r');
    expect(_internal.coordinatorIdOf({ coordinator_email: 'e' }, {})).toBe('e');
    expect(_internal.coordinatorIdOf({}, {})).toBeNull();
  });

  test('unmarshall handles S/N/BOOL', () => {
    expect(_internal.unmarshall({ a: { S: 'x' }, b: { N: '5' }, c: { BOOL: true } })).toEqual({ a: 'x', b: 5, c: true });
  });
});
