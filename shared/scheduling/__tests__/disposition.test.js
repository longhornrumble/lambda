'use strict';

/**
 * Unit tests for disposition.js (WS-E-ATTEND E6).
 *
 * Covers: the §11.2 purpose→Booking.status map; the conditional booked→terminal transition
 * (ReturnValues ALL_NEW); idempotent already_resolved on ConditionalCheckFailed; the no_show
 * volunteer reoffer (fresh reschedule token + notify.dispatchVolunteerNotice + §E3 channel
 * gate, incl. the fail-closed default); NO outbound for attended_yes/didnt_connect; the
 * interviewer confirmation (§11.2 action+applicant+program); and input validation.
 */

const {
  applyDisposition,
  DISPOSITION_BY_PURPOSE,
  ACTION_LABEL,
  ATTENDANCE_STATE_RESOLVED,
  failClosedSelectChannels,
  attr,
  isConditionalCheckFailed,
} = require('../disposition');

const TENANT = 'AUS123957';
const BOOKING = 'bk-1';

// ALL_NEW row the conditional UpdateItem returns on success (DDB AttributeValue shape).
function allNew(over = {}) {
  return {
    Attributes: {
      status: { S: 'no_show' },
      attendance_state: { S: 'resolved' },
      start_at: { S: '2026-06-03T14:00:00Z' },
      attendee_email: { S: 'sam@example.com' },
      attendee_phone: { S: '+15125551234' },
      attendee_name: { S: 'Sam Patel' },
      appointment_type_name: { S: 'intake call' },
      coordinator_email: { S: 'maya@org.example' },
      when_label: { S: 'Tue, Jun 3 · 2:00 PM' },
      ...over,
    },
  };
}

function condFailErr() {
  const e = new Error('conditional check failed');
  e.name = 'ConditionalCheckFailedException';
  return e;
}

function fakeSign(purpose, claims, opts) {
  return `tok.${purpose}.${claims.start_at}.${(opts && opts.now) || 'na'}`;
}

const quietLog = () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() });

function deps(over = {}) {
  return {
    ddb: { send: jest.fn().mockResolvedValue(allNew()) },
    signToken: fakeSign,
    dispatchVolunteerNotice: jest.fn().mockResolvedValue({ kind: 'reoffer', dispatched: { email: 'sent' } }),
    selectChannels: jest.fn().mockResolvedValue({ email: true, sms: true }),
    sendEmail: jest.fn().mockResolvedValue(undefined),
    baseUrl: 'https://schedule.myrecruiter.ai',
    log: quietLog(),
    now: 1000,
    ...over,
  };
}

describe('constants', () => {
  test('§11.2 disposition map is the locked three → terminal statuses', () => {
    expect(DISPOSITION_BY_PURPOSE).toEqual({
      attended_yes: 'completed',
      no_show: 'no_show',
      didnt_connect: 'coordinator_no_show',
    });
    expect(ATTENDANCE_STATE_RESOLVED).toBe('resolved');
    expect(ACTION_LABEL.no_show).toBe('no-show');
  });
});

describe('helpers', () => {
  test('attr reads .S, null when absent', () => {
    expect(attr({ x: { S: 'v' } }, 'x')).toBe('v');
    expect(attr({}, 'x')).toBeNull();
    expect(attr(null, 'x')).toBeNull();
  });
  test('isConditionalCheckFailed matches by name', () => {
    expect(isConditionalCheckFailed(condFailErr())).toBe(true);
    expect(isConditionalCheckFailed(new Error('other'))).toBe(false);
  });
  test('failClosedSelectChannels → email floor only', async () => {
    await expect(failClosedSelectChannels()).resolves.toEqual({ email: true, sms: false });
  });
});

describe('applyDisposition validation', () => {
  test('unknown purpose throws (caller bug)', async () => {
    await expect(applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'cancel', deps: deps() })).rejects.toThrow(/unknown purpose/);
  });
  test('missing tenantId/bookingId throws', async () => {
    await expect(applyDisposition({ purpose: 'no_show', deps: deps() })).rejects.toThrow(/required/);
  });
});

describe('attended_yes → completed (no outbound)', () => {
  test('transitions + interviewer confirmation, no volunteer notice', async () => {
    const d = deps({ ddb: { send: jest.fn().mockResolvedValue(allNew({ status: { S: 'completed' } })) } });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'attended_yes', deps: d });
    expect(r).toMatchObject({ outcome: 'completed', transitioned: true, status: 'completed' });
    expect(d.dispatchVolunteerNotice).not.toHaveBeenCalled();
    // confirmation email to interviewer (§11.2)
    expect(d.sendEmail).toHaveBeenCalledTimes(1);
    expect(d.sendEmail.mock.calls[0][0].to).toBe('maya@org.example');
    expect(d.sendEmail.mock.calls[0][0].subject).toContain('Sam'); // applicant
    // the conditional update guards on booked + writes attendance_state=resolved
    const cmd = d.ddb.send.mock.calls[0][0].input;
    expect(cmd.ConditionExpression).toContain('#st = :booked');
    expect(cmd.ExpressionAttributeValues[':resolved'].S).toBe('resolved');
    expect(cmd.ReturnValues).toBe('ALL_NEW');
  });
});

describe('no_show → no_show + volunteer reoffer', () => {
  test('mints reschedule token, dispatches reoffer with channels', async () => {
    const d = deps();
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show', deps: d });
    expect(r.outcome).toBe('no_show');
    expect(d.selectChannels).toHaveBeenCalledTimes(1);
    expect(d.dispatchVolunteerNotice).toHaveBeenCalledTimes(1);
    const arg = d.dispatchVolunteerNotice.mock.calls[0][0];
    expect(arg.kind).toBe('reoffer');
    expect(arg.booking.reoffer_url).toContain('/reschedule?t=tok.reschedule.');
    expect(arg.channels).toEqual({ email: true, sms: true });
    expect(r.volunteerNotice).toEqual({ kind: 'reoffer', dispatched: { email: 'sent' } });
  });

  test('reschedule-token mint failure → notice suppressed, transition still succeeds', async () => {
    const d = deps({ signToken: jest.fn().mockRejectedValue(new Error('no start_at')) });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show', deps: d });
    expect(r.outcome).toBe('no_show');
    expect(r.volunteerNotice).toEqual({ dispatched: {}, suppressed: true, reason: 'token_mint_failed' });
    expect(d.dispatchVolunteerNotice).not.toHaveBeenCalled();
  });

  test('selectChannels failure does not block the email floor', async () => {
    const d = deps({ selectChannels: jest.fn().mockRejectedValue(new Error('tcpa down')) });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show', deps: d });
    expect(r.outcome).toBe('no_show');
    // dispatch still called with the fail-closed channels {email:true, sms:false}
    expect(d.dispatchVolunteerNotice.mock.calls[0][0].channels).toEqual({ email: true, sms: false });
  });

  test('fail-closed default selectChannels used when deps omits it', async () => {
    const d = deps();
    delete d.selectChannels;
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show', deps: d });
    expect(r.outcome).toBe('no_show');
    expect(d.dispatchVolunteerNotice.mock.calls[0][0].channels).toEqual({ email: true, sms: false });
  });
});

describe('didnt_connect → coordinator_no_show (no outbound)', () => {
  test('transitions, no volunteer notice', async () => {
    const d = deps({ ddb: { send: jest.fn().mockResolvedValue(allNew({ status: { S: 'coordinator_no_show' } })) } });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'didnt_connect', deps: d });
    expect(r.outcome).toBe('coordinator_no_show');
    expect(d.dispatchVolunteerNotice).not.toHaveBeenCalled();
  });
});

describe('idempotency', () => {
  test('ConditionalCheckFailed → already_resolved, no notice, no confirmation', async () => {
    const d = deps({ ddb: { send: jest.fn().mockRejectedValue(condFailErr()) } });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show', deps: d });
    expect(r).toEqual({ outcome: 'already_resolved', transitioned: false });
    expect(d.dispatchVolunteerNotice).not.toHaveBeenCalled();
    expect(d.sendEmail).not.toHaveBeenCalled();
  });

  test('non-conditional DDB error propagates', async () => {
    const d = deps({ ddb: { send: jest.fn().mockRejectedValue(new Error('throttled')) } });
    await expect(applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show', deps: d })).rejects.toThrow(/throttled/);
  });
});

describe('interviewer confirmation edge cases', () => {
  test('no coordinator email → skipped_no_recipient', async () => {
    const d = deps({ ddb: { send: jest.fn().mockResolvedValue(allNew({ coordinator_email: undefined, status: { S: 'completed' } })) } });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'attended_yes', deps: d });
    expect(r.interviewerConfirmation).toEqual({ email: 'skipped_no_recipient' });
  });

  test('confirmation email failure → failed (best-effort)', async () => {
    const d = deps({
      ddb: { send: jest.fn().mockResolvedValue(allNew({ status: { S: 'completed' } })) },
      sendEmail: jest.fn().mockRejectedValue(new Error('ses down')),
    });
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'attended_yes', deps: d });
    expect(r.interviewerConfirmation).toEqual({ email: 'failed' });
    expect(r.transitioned).toBe(true); // transition unaffected
  });
});

// Exercises the MODULE DEFAULTS (the exact wiring the WS-D4 handler uses: no deps passed) —
// real tokens.sign + real notify.dispatchVolunteerNotice/defaultInvokeEmail + default DDB
// client, with only the AWS clients mocked. Proves the surgical handler call works end-to-end.
describe('default wiring (no deps — handler path)', () => {
  const { mockClient } = require('aws-sdk-client-mock');
  const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
  const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
  const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
  const ddbMock = mockClient(DynamoDBClient);
  const smMock = mockClient(SecretsManagerClient);
  const lambdaMock = mockClient(LambdaClient);

  beforeEach(() => {
    ddbMock.reset();
    smMock.reset();
    lambdaMock.reset();
    smMock.on(GetSecretValueCommand).resolves({ SecretString: 'test-signing-key-0123456789abcdef' });
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
  });

  test('attended_yes via defaults: default ddb update + default interviewer-confirmation email', async () => {
    ddbMock.on(UpdateItemCommand).resolves(allNew({ status: { S: 'completed' } }));
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'attended_yes' });
    expect(r).toMatchObject({ outcome: 'completed', transitioned: true });
    expect(ddbMock.commandCalls(UpdateItemCommand)).toHaveLength(1);
    // interviewer confirmation routed through notify.defaultInvokeEmail → send_email Lambda
    expect(lambdaMock.commandCalls(InvokeCommand).length).toBeGreaterThanOrEqual(1);
  });

  test('no_show via defaults: real reschedule token minted + reoffer dispatched, fail-closed sms', async () => {
    ddbMock.on(UpdateItemCommand).resolves(allNew());
    const r = await applyDisposition({ tenantId: TENANT, bookingId: BOOKING, purpose: 'no_show' });
    expect(r.outcome).toBe('no_show');
    expect(r.volunteerNotice.suppressed).toBe(false);
    // reoffer email + interviewer confirmation both invoke send_email; no SMS (fail-closed default)
    expect(lambdaMock.commandCalls(InvokeCommand).length).toBeGreaterThanOrEqual(2);
  });
});
