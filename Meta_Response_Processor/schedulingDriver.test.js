'use strict';

/**
 * Unit tests for schedulingDriver.js (M8a). Pure logic + row CRUD against
 * mocked DynamoDB — no real Lambda invokes (deps.invokeProposal/invokeCommit
 * are jest.fn() stubs, matching formEngine.test.js's convention). Handler-
 * level E2E (drain routing, escalation-mid-scheduling, CTA entry wiring)
 * lives in index.test.js.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBDocumentClient, GetCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');

const ddbMock = mockClient(DynamoDBDocumentClient);

// consent.js constructs its own DynamoDBClient at module load — mock that
// client class too so recordBookingSmsConsent's default writer never makes a
// real network call in tests that exercise it directly (most tests instead
// inject deps.recordConsent, but a couple exercise the real default).
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const dynamoRawMock = mockClient(DynamoDBClient);

const schedulingDriver = require('./schedulingDriver');

const SESSION_ID = 'meta:PAGE_1:PSID_1';
const TENANT_ID = 'TENANT_1';
const TABLE = 'picasso-conversation-state-test';

beforeEach(() => {
  ddbMock.reset();
  dynamoRawMock.reset();
  dynamoRawMock.on(PutItemCommand).resolves({});
});

// ─── row CRUD (T1'/T2') ──────────────────────────────────────────────────────

describe('loadSchedulingSession', () => {
  test('returns null and does not delete when no row exists', async () => {
    ddbMock.on(GetCommand).resolves({});
    const result = await schedulingDriver.loadSchedulingSession({ client: ddbMock, tableName: TABLE, sessionId: SESSION_ID });
    expect(result).toBeNull();
  });

  test('T1prime: an expired row (expires_at in the past) is treated as absent + best-effort deleted', async () => {
    const pastRow = { sessionId: SESSION_ID, stateType: 'scheduling_session', stage: 'proposing', expires_at: Math.floor(Date.now() / 1000) - 10 };
    ddbMock.on(GetCommand).resolves({ Item: pastRow });
    ddbMock.on(DeleteCommand).resolves({});
    const result = await schedulingDriver.loadSchedulingSession({ client: ddbMock, tableName: TABLE, sessionId: SESSION_ID });
    expect(result).toBeNull();
    expect(ddbMock).toHaveReceivedCommandTimes(DeleteCommand, 1);
  });

  test('returns a still-fresh row unchanged', async () => {
    const freshRow = { sessionId: SESSION_ID, stateType: 'scheduling_session', stage: 'proposing', expires_at: Math.floor(Date.now() / 1000) + 3600 };
    ddbMock.on(GetCommand).resolves({ Item: freshRow });
    const result = await schedulingDriver.loadSchedulingSession({ client: ddbMock, tableName: TABLE, sessionId: SESSION_ID });
    expect(result).toEqual(freshRow);
    expect(ddbMock).toHaveReceivedCommandTimes(DeleteCommand, 0);
  });
});

// ─── resolveAppointmentTypeId ─────────────────────────────────────────────────

describe('resolveAppointmentTypeId', () => {
  test('matches the CTA program_id against a configured appointment type', () => {
    const config = {
      scheduling: {
        appointment_types: {
          consult: { program_id: 'mentoring', name: 'Consult' },
          intake: { program_id: 'tutoring', name: 'Intake' },
        },
      },
    };
    const id = schedulingDriver.resolveAppointmentTypeId({ config, cta: { program_id: 'tutoring' } });
    expect(id).toBe('intake');
  });

  test('falls back to the sole configured type when no program_id match', () => {
    const config = { scheduling: { appointment_types: { only_one: { name: 'Only' } } } };
    const id = schedulingDriver.resolveAppointmentTypeId({ config, cta: {} });
    expect(id).toBe('only_one');
  });

  test('returns null when ambiguous (2+ types, no program_id match)', () => {
    const config = { scheduling: { appointment_types: { a: {}, b: {} } } };
    const id = schedulingDriver.resolveAppointmentTypeId({ config, cta: { program_id: 'nope' } });
    expect(id).toBeNull();
  });

  test('returns null when no appointment types configured at all', () => {
    const id = schedulingDriver.resolveAppointmentTypeId({ config: {}, cta: {} });
    expect(id).toBeNull();
  });
});

// ─── parseSchedPayload (C3) ───────────────────────────────────────────────────

describe('parseSchedPayload', () => {
  test('parses slot:{slotId}', () => {
    expect(schedulingDriver.parseSchedPayload('PIC1:sched:slot:slot#2026-01-01T10:00:00Z')).toEqual({
      op: 'slot',
      arg: 'slot#2026-01-01T10:00:00Z',
    });
  });
  test('parses confirm/cancel with empty arg', () => {
    expect(schedulingDriver.parseSchedPayload('PIC1:sched:confirm')).toEqual({ op: 'confirm', arg: '' });
    expect(schedulingDriver.parseSchedPayload('PIC1:sched:cancel')).toEqual({ op: 'cancel', arg: '' });
  });
  test('parses start:{apptTypeId}', () => {
    expect(schedulingDriver.parseSchedPayload('PIC1:sched:start:consult')).toEqual({ op: 'start', arg: 'consult' });
  });
  test('non-sched payloads return null', () => {
    expect(schedulingDriver.parseSchedPayload('PIC1:cta:foo')).toBeNull();
    expect(schedulingDriver.parseSchedPayload('GET_STARTED')).toBeNull();
    expect(schedulingDriver.parseSchedPayload(null)).toBeNull();
  });
});

// ─── beginScheduling ──────────────────────────────────────────────────────────

const APPT_TYPES_CONFIG = {
  scheduling: {
    appointment_types: {
      consult: { name: 'Consult', timezone: 'America/Los_Angeles', conference_type: 'google_meet' },
    },
  },
};

function proposeOkResult(overrides = {}) {
  return {
    outcome: 'ok',
    poolSize: 3,
    tieBreaker: 'round_robin',
    roundRobinCursor: { routingPolicyId: 'rp1' },
    slots: [
      { slotId: 'slot#1', start: '2026-08-01T17:00:00Z', end: '2026-08-01T17:30:00Z', label: 'Sat, Aug 1 · 10:00 AM', candidateResourceIds: ['r1'] },
      { slotId: 'slot#2', start: '2026-08-01T18:00:00Z', end: '2026-08-01T18:30:00Z', label: 'Sat, Aug 1 · 11:00 AM', candidateResourceIds: ['r1'] },
    ],
    context: { duration_minutes: 30, conference_type: 'google_meet', conference_label: 'Google Meet', tz_label: 'PDT' },
    ...overrides,
  };
}

describe('beginScheduling', () => {
  test('propose ok -> builds a scheduling_session row + carousel + tz-labeled text fallback (C5 <=10, never a bare time)', async () => {
    const invokeProposal = jest.fn().mockResolvedValue(proposeOkResult());
    const { session, messages, started } = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID,
      tenantId: TENANT_ID,
      appointmentTypeId: 'consult',
      config: APPT_TYPES_CONFIG,
      channelType: 'messenger',
      deps: { invokeProposal },
    });

    expect(started).toBe(true);
    expect(session.stage).toBe(schedulingDriver.STAGE_PROPOSING);
    expect(session.program_id).toBe('consult');
    expect(session.candidate_slots).toHaveLength(2);
    expect(invokeProposal).toHaveBeenCalledWith(
      expect.objectContaining({ action: 'scheduling_propose', tenantId: TENANT_ID, appointmentTypeId: 'consult', userTimeZone: 'America/Los_Angeles' })
    );

    const textMsg = messages.find((m) => m.kind === 'text');
    const carouselMsg = messages.find((m) => m.kind === 'generic_template');
    expect(textMsg.text).toContain('PDT'); // never a bare time — tz stated in the intro
    expect(textMsg.text).toContain('1. Sat, Aug 1 · 10:00 AM');
    expect(carouselMsg.elements).toHaveLength(2);
    expect(carouselMsg.elements[0].subtitle).toContain('PDT');
    expect(carouselMsg.elements[0].buttons[0].payload).toBe('PIC1:sched:slot:slot#1');
  });

  test('caps the carousel at C5 <=10 cards even if propose somehow returns more', async () => {
    const many = Array.from({ length: 15 }, (_, i) => ({
      slotId: `slot#${i}`, start: `2026-08-0${(i % 9) + 1}T17:00:00Z`, end: `2026-08-0${(i % 9) + 1}T17:30:00Z`, label: `Slot ${i}`, candidateResourceIds: ['r1'],
    }));
    const invokeProposal = jest.fn().mockResolvedValue(proposeOkResult({ slots: many }));
    const { messages } = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID, tenantId: TENANT_ID, appointmentTypeId: 'consult', config: APPT_TYPES_CONFIG, channelType: 'messenger', deps: { invokeProposal },
    });
    const carouselMsg = messages.find((m) => m.kind === 'generic_template');
    expect(carouselMsg.elements.length).toBeLessThanOrEqual(10);
  });

  test('missing tz_label still renders an explicit (non-bare) fallback phrase', async () => {
    const invokeProposal = jest.fn().mockResolvedValue(proposeOkResult({ context: { duration_minutes: 30, conference_type: 'google_meet', conference_label: 'Google Meet', tz_label: null } }));
    const { messages } = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID, tenantId: TENANT_ID, appointmentTypeId: 'consult', config: APPT_TYPES_CONFIG, channelType: 'messenger', deps: { invokeProposal },
    });
    const textMsg = messages.find((m) => m.kind === 'text');
    const carouselMsg = messages.find((m) => m.kind === 'generic_template');
    expect(textMsg.text.toLowerCase()).toContain('timezone');
    expect(carouselMsg.elements[0].subtitle.toLowerCase()).toContain('timezone');
  });

  test('no_availability -> apology, no session created, no DDB write attempted by the caller', async () => {
    const invokeProposal = jest.fn().mockResolvedValue({ outcome: 'no_availability', slots: [], poolSize: 0 });
    const { session, started, messages } = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID, tenantId: TENANT_ID, appointmentTypeId: 'consult', config: APPT_TYPES_CONFIG, channelType: 'messenger', deps: { invokeProposal },
    });
    expect(started).toBe(false);
    expect(session).toBeNull();
    expect(messages[0].text).toBe(schedulingDriver.DEFAULT_NO_SLOTS);
  });

  test('propose seam unwired -> declines gracefully, never throws', async () => {
    const { session, started } = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID, tenantId: TENANT_ID, appointmentTypeId: 'consult', config: APPT_TYPES_CONFIG, channelType: 'messenger', deps: {},
    });
    expect(started).toBe(false);
    expect(session).toBeNull();
  });

  test('propose invoke throws -> degrades to apology, never propagates', async () => {
    const invokeProposal = jest.fn().mockRejectedValue(new Error('boom'));
    const { started, messages } = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID, tenantId: TENANT_ID, appointmentTypeId: 'consult', config: APPT_TYPES_CONFIG, channelType: 'messenger', deps: { invokeProposal },
    });
    expect(started).toBe(false);
    expect(messages[0].text).toBe(schedulingDriver.DEFAULT_SCHEDULING_UNAVAILABLE);
  });
});

// ─── advanceScheduling: proposing -> contact_email (slot pick + C9 free text) ─

function baseSession(overrides = {}) {
  const now = Date.now();
  return {
    sessionId: SESSION_ID,
    stateType: 'scheduling_session',
    program_id: 'consult',
    stage: schedulingDriver.STAGE_PROPOSING,
    candidate_slots: [
      { slotId: 'slot#1', start: '2026-08-01T17:00:00Z', end: '2026-08-01T17:30:00Z', label: 'Sat, Aug 1 · 10:00 AM', candidateResourceIds: ['r1'] },
      { slotId: 'slot#2', start: '2026-08-01T18:00:00Z', end: '2026-08-01T18:30:00Z', label: 'Sat, Aug 1 · 11:00 AM', candidateResourceIds: ['r1'] },
    ],
    rejected_slot_ids: [],
    pool_size: 3,
    channel: 'messenger',
    appointment_type: { id: 'consult', name: 'Consult', timezone: 'America/Los_Angeles', conference_type: 'google_meet', cancellation_window_hours: 0 },
    started_at: now,
    updated_at: now,
    schema_version: 1,
    expires_at: Math.floor(now / 1000) + 3600,
    ...overrides,
  };
}

describe('advanceScheduling — STAGE_PROPOSING', () => {
  test('tapped slot payload advances to contact_email + refreshes TTL (T1prime)', async () => {
    const session = baseSession({ expires_at: Math.floor(Date.now() / 1000) + 10 });
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: 'Pick this time', schedPayload: { op: 'slot', arg: 'slot#2' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONTACT_EMAIL);
    expect(result.session.selected_slot.slotId).toBe('slot#2');
    expect(result.session.expires_at).toBeGreaterThan(session.expires_at);
  });

  test('C9 free-text fallback: typing "2" picks the 2nd slot', async () => {
    const session = baseSession();
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: '2', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.selected_slot.slotId).toBe('slot#2');
  });

  test('unresolvable slot pick re-prompts, stays in STAGE_PROPOSING', async () => {
    const session = baseSession();
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: '99', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_PROPOSING);
    expect(result.messages[0].text).toMatch(/tap one of the times|reply with its number/i);
  });

  test('FB (messenger) email prompt carries the user_email prefill quick reply (C5)', async () => {
    const session = baseSession({ channel: 'messenger' });
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: '1', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.messages[0].quickReplies).toEqual([{ content_type: 'user_email' }]);
  });

  test('cancel keyword at STAGE_PROPOSING ends the session', async () => {
    const session = baseSession();
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: 'cancel', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session).toBeNull();
  });
});

// ─── advanceScheduling: contact_email ─────────────────────────────────────────

describe('advanceScheduling — STAGE_CONTACT_EMAIL', () => {
  const emailSession = () => baseSession({ stage: schedulingDriver.STAGE_CONTACT_EMAIL, selected_slot: { slotId: 'slot#1', start: '2026-08-01T17:00:00Z', end: '2026-08-01T17:30:00Z', label: 'Sat, Aug 1 · 10:00 AM', candidateResourceIds: ['r1'] } });

  test('valid email -> advances to contact_phone; phone prompt carries the C1 consent language verbatim', async () => {
    const config = { messenger_behavior: { strings: { sms_consent: 'CUSTOM CONSENT TEXT.' } } };
    const result = await schedulingDriver.advanceScheduling({
      session: emailSession(), rawText: 'jane@example.com', schedPayload: null, config, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONTACT_PHONE);
    expect(result.session.contact.email).toBe('jane@example.com');
    expect(result.session.consent_language_shown).toBe('CUSTOM CONSENT TEXT.');
    expect(result.messages[0].text).toContain('CUSTOM CONSENT TEXT.');
  });

  test('default consent language used when not configured', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: emailSession(), rawText: 'jane@example.com', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.consent_language_shown).toBe(schedulingDriver.DEFAULT_SMS_CONSENT);
    expect(result.session.consent_language_shown).not.toBe('');
    expect(result.session.consent_language_shown).toBeDefined();
  });

  test('invalid email re-prompts, stays at contact_email', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: emailSession(), rawText: 'not-an-email', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONTACT_EMAIL);
    expect(result.session.contact).toBeUndefined();
  });
});

// ─── advanceScheduling: contact_phone (C5 toE164, D9 IG-mandatory) ───────────

describe('advanceScheduling — STAGE_CONTACT_PHONE', () => {
  const phoneSession = (channel) =>
    baseSession({
      stage: schedulingDriver.STAGE_CONTACT_PHONE,
      channel,
      contact: { email: 'jane@example.com' },
      consent_language_shown: schedulingDriver.DEFAULT_SMS_CONSENT,
      selected_slot: { slotId: 'slot#1', start: '2026-08-01T17:00:00Z', end: '2026-08-01T17:30:00Z', label: 'Sat, Aug 1 · 10:00 AM', candidateResourceIds: ['r1'] },
    });

  test('FB: "skip" is allowed -> advances to confirm without a phone', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: phoneSession('messenger'), rawText: 'skip', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONFIRM);
    expect(result.session.contact.phone).toBeUndefined();
    expect(result.messages[0].text).not.toContain('Phone:');
  });

  test('IG: "skip" is REJECTED — phone is mandatory (D9)', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: phoneSession('instagram'), rawText: 'skip', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONTACT_PHONE);
    expect(result.session.contact.phone).toBeUndefined();
    expect(result.messages[0].text).toMatch(/required/i);
  });

  test('IG: invalid phone re-prompts via toE164 (C5)', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: phoneSession('instagram'), rawText: '123', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONTACT_PHONE);
    expect(result.session.contact.phone).toBeUndefined();
  });

  test('IG: valid phone normalizes to E.164 and advances to confirm', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: phoneSession('instagram'), rawText: '(415) 555-0100', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONFIRM);
    expect(result.session.contact.phone).toBe('+14155550100');
  });
});

// ─── advanceScheduling: confirm -> commit (C8, T2', T3', re-propose retry) ───

describe('advanceScheduling — STAGE_CONFIRM / commit', () => {
  const confirmSession = (overrides = {}) =>
    baseSession({
      stage: schedulingDriver.STAGE_CONFIRM,
      contact: { email: 'jane@example.com' },
      consent_language_shown: schedulingDriver.DEFAULT_SMS_CONSENT,
      selected_slot: { slotId: 'slot#1', start: '2026-08-01T17:00:00Z', end: '2026-08-01T17:30:00Z', label: 'Sat, Aug 1 · 10:00 AM', candidateResourceIds: ['r1'] },
      ...overrides,
    });

  test('cancel at confirm ends the session', async () => {
    const result = await schedulingDriver.advanceScheduling({
      session: confirmSession(), rawText: 'cancel', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session).toBeNull();
    expect(result.messages[0].text).toBe(schedulingDriver.DEFAULT_SCHEDULING_CANCELLED);
  });

  test('C8: commit refuses to fire without attendee_email (defensive — row left untouched, T3\')', async () => {
    const invokeCommit = jest.fn();
    const session = confirmSession({ contact: {} }); // no email — corrupted/hand-edited row
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit },
    });
    expect(invokeCommit).not.toHaveBeenCalled();
    expect(result.session).toBeUndefined(); // T3': caller leaves the row untouched
  });

  test('happy path: BOOKED -> pinned commit payload (snake_case, incl. attendee.email), row deleted signal, consent NOT called (no phone)', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'BOOKED', bookingId: 'bk_1', resourceId: 'r1' });
    const recordConsent = jest.fn();
    const session = confirmSession(); // no phone captured
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit, recordConsent },
    });

    expect(invokeCommit).toHaveBeenCalledTimes(1);
    const payload = invokeCommit.mock.calls[0][0];
    expect(payload).toMatchObject({
      tenant_id: TENANT_ID,
      session_id: SESSION_ID,
      slot: { start: '2026-08-01T17:00:00Z', end: '2026-08-01T17:30:00Z', candidateResourceIds: ['r1'] },
      attendee: { email: 'jane@example.com', phone: undefined },
      conference_type: 'google_meet',
      pool_size: 3,
      appointment_type: expect.objectContaining({ id: 'consult', name: 'Consult' }),
    });

    expect(result.session).toBeNull(); // T2': caller deletes the row
    expect(result.committed).toBe(true);
    expect(result.bookingId).toBe('bk_1');
    expect(recordConsent).not.toHaveBeenCalled(); // no phone -> no consent write (C2/C3/C4)
    expect(result.messages[0].text).toBe(schedulingDriver.DEFAULT_BOOKED);
  });

  test('IG happy path: consent recorded with exact consent_language_shown + source messenger_booking_ig, AFTER commit (C2/C3/C4)', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'BOOKED', bookingId: 'bk_2' });
    const recordConsent = jest.fn().mockResolvedValue({ written: true });
    const session = confirmSession({ channel: 'instagram', contact: { email: 'jane@example.com', phone: '+14155550100' }, consent_language_shown: 'IG CONSENT STRING' });

    const result = await schedulingDriver.advanceScheduling({
      session, rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit, recordConsent },
    });

    expect(result.committed).toBe(true);
    expect(recordConsent).toHaveBeenCalledTimes(1);
    const consentArgs = recordConsent.mock.calls[0][0];
    expect(consentArgs.tenantId).toBe(TENANT_ID);
    expect(consentArgs.phone).toBe('+14155550100');
    expect(consentArgs.bookingId).toBe('bk_2');
    expect(consentArgs.consentLanguage).toBe('IG CONSENT STRING');
    expect(consentArgs.source).toBe('messenger_booking_ig');
    // AFTER commit: invokeCommit resolved before recordConsent was called (mock call order proxy).
    expect(invokeCommit.mock.invocationCallOrder[0]).toBeLessThan(recordConsent.mock.invocationCallOrder[0]);
  });

  test('FB source is messenger_booking_fb', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'BOOKED', bookingId: 'bk_3' });
    const recordConsent = jest.fn().mockResolvedValue({ written: true });
    const session = confirmSession({ channel: 'messenger', contact: { email: 'jane@example.com', phone: '+14155550100' } });
    await schedulingDriver.advanceScheduling({
      session, rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit, recordConsent },
    });
    expect(recordConsent.mock.calls[0][0].source).toBe('messenger_booking_fb');
  });

  test('ALREADY_CONFIRMED (idempotent BCH re-confirm) treated as success -> row deleted', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'ALREADY_CONFIRMED', bookingId: 'bk_1' });
    const result = await schedulingDriver.advanceScheduling({
      session: confirmSession(), rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit },
    });
    expect(result.session).toBeNull();
    expect(result.committed).toBe(true);
  });

  test('SLOT_UNAVAILABLE -> re-propose succeeds -> fresh slots, stage back to STAGE_PROPOSING, row updated (not deleted)', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'SLOT_UNAVAILABLE', reason: 'recheck_busy' });
    const invokeProposal = jest.fn().mockResolvedValue(proposeOkResult({ slots: [{ slotId: 'slot#9', start: '2026-08-02T17:00:00Z', end: '2026-08-02T17:30:00Z', label: 'Sun, Aug 2 · 10:00 AM', candidateResourceIds: ['r2'] }] }));
    const result = await schedulingDriver.advanceScheduling({
      session: confirmSession(), rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit, invokeProposal },
    });
    expect(result.committed).toBeFalsy();
    expect(result.session).toBeDefined();
    expect(result.session.stage).toBe(schedulingDriver.STAGE_PROPOSING);
    expect(result.session.candidate_slots[0].slotId).toBe('slot#9');
    expect(result.session.selected_slot).toBeUndefined();
  });

  test('T2prime: conflict-retry that eventually succeeds still deletes the row (re-enters same commit path)', async () => {
    // Simulate the two-turn sequence: first confirm hits SLOT_UNAVAILABLE + repropose,
    // then the user re-selects + re-confirms and THAT commit succeeds.
    const invokeProposal = jest.fn().mockResolvedValue(proposeOkResult({ slots: [{ slotId: 'slot#9', start: '2026-08-02T17:00:00Z', end: '2026-08-02T17:30:00Z', label: 'Sun, Aug 2 · 10:00 AM', candidateResourceIds: ['r2'] }] }));
    const invokeCommitFirst = jest.fn().mockResolvedValue({ status: 'SLOT_UNAVAILABLE' });
    const first = await schedulingDriver.advanceScheduling({
      session: confirmSession(), rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit: invokeCommitFirst, invokeProposal },
    });
    // user re-selects the fresh slot
    const reselected = await schedulingDriver.advanceScheduling({
      session: first.session, rawText: '1', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(reselected.session.stage).toBe(schedulingDriver.STAGE_CONTACT_EMAIL);
    // Second commit succeeds — the SAME code path deletes the row.
    const invokeCommitSecond = jest.fn().mockResolvedValue({ status: 'BOOKED', bookingId: 'bk_retry' });
    const secondConfirmSession = { ...reselected.session, stage: schedulingDriver.STAGE_CONFIRM, contact: { email: 'jane@example.com' } };
    const second = await schedulingDriver.advanceScheduling({
      session: secondConfirmSession, rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit: invokeCommitSecond },
    });
    expect(second.session).toBeNull(); // T2' — deleted on the conflict-retry's eventual success
    expect(second.committed).toBe(true);
  });

  test('T3prime: re-propose after conflict ALSO fails -> terminal, row left completely untouched', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'SLOT_UNAVAILABLE' });
    const invokeProposal = jest.fn().mockResolvedValue({ outcome: 'no_availability', slots: [] });
    const result = await schedulingDriver.advanceScheduling({
      session: confirmSession(), rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit, invokeProposal },
    });
    expect(result.session).toBeUndefined(); // caller must NOT touch the row
  });

  test('T3prime: a non-slot-unavailable commit failure (COMMIT_FAILED) leaves the row untouched, no extension', async () => {
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'COMMIT_FAILED', reason: 'oauth error' });
    const result = await schedulingDriver.advanceScheduling({
      session: confirmSession(), rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit },
    });
    expect(result.session).toBeUndefined();
    expect(result.committed).toBeFalsy();
  });

  test('typing something other than confirm/cancel at STAGE_CONFIRM re-shows the summary (not terminal, TTL refreshed)', async () => {
    const session = confirmSession({ expires_at: Math.floor(Date.now() / 1000) + 10 });
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: 'wait, what time again?', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session).toBeDefined();
    expect(result.session.stage).toBe(schedulingDriver.STAGE_CONFIRM);
    expect(result.session.expires_at).toBeGreaterThan(session.expires_at);
  });
});

// ─── C7 PII-log hygiene: no raw phone/email in any log call across the full path ─

describe('C7 — no raw contact PII in logs', () => {
  test('happy path with a captured phone never logs the phone or email value', async () => {
    const logCalls = [];
    const log = (...args) => logCalls.push(args);
    const PHONE = '+14155550100';
    const EMAIL = 'jane@example.com';

    const invokeProposal = jest.fn().mockResolvedValue(proposeOkResult());
    const begun = await schedulingDriver.beginScheduling({
      sessionId: SESSION_ID, tenantId: TENANT_ID, appointmentTypeId: 'consult', config: APPT_TYPES_CONFIG, channelType: 'instagram', deps: { invokeProposal }, log,
    });

    const picked = await schedulingDriver.advanceScheduling({
      session: begun.session, rawText: '1', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {}, log,
    });
    const emailed = await schedulingDriver.advanceScheduling({
      session: picked.session, rawText: EMAIL, schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {}, log,
    });
    const phoned = await schedulingDriver.advanceScheduling({
      session: emailed.session, rawText: PHONE, schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {}, log,
    });
    const invokeCommit = jest.fn().mockResolvedValue({ status: 'BOOKED', bookingId: 'bk_pii' });
    const recordConsent = jest.fn().mockResolvedValue({ written: true });
    await schedulingDriver.advanceScheduling({
      session: phoned.session, rawText: 'confirm', schedPayload: { op: 'confirm', arg: '' }, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: { invokeCommit, recordConsent }, log,
    });

    const serialized = JSON.stringify(logCalls);
    expect(serialized).not.toContain(PHONE);
    expect(serialized).not.toContain('4155550100');
    expect(serialized).not.toContain(EMAIL);
  });
});

// ─── T1' TTL refresh sanity on the row-level flow (schedulingDriver-only) ────

describe("T1' — idle TTL refresh", () => {
  test('every advancing step refreshes expires_at forward', async () => {
    const session = baseSession({ expires_at: Math.floor(Date.now() / 1000) + 5 });
    const before = session.expires_at;
    const result = await schedulingDriver.advanceScheduling({
      session, rawText: '1', schedPayload: null, config: {}, tenantId: TENANT_ID, sessionId: SESSION_ID, deps: {},
    });
    expect(result.session.expires_at).toBeGreaterThanOrEqual(before);
  });
});
