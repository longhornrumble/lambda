'use strict';

/**
 * Unit tests for notify.js (WS-SCHED-FOUNDATIONS, contract Y).
 *
 * Covers: per-kind email payload build; the agent-of-CoR guard (reassigned/moved
 * suppressed); the SMS TODO(SMS-E) stub; channel defaults + overrides; best-effort
 * (send failure non-fatal + PII-redacted logging); forward-compatible snake/camel
 * booking reads; and the default Lambda-invoke implementation via aws-sdk-client-mock.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const lambdaMock = mockClient(LambdaClient);

const {
  dispatchVolunteerNotice,
  buildEmailPayload,
  defaultInvokeEmail,
  defaultInvokeSms,
  render,
  escapeHtml,
} = require('../notify');

const TENANT = 'AUS123957';

const baseBooking = {
  bookingId: 'bk-1',
  attendeeEmail: 'vol@example.com',
  attendeeName: 'Sam Patel',
  organizationName: 'Austin Angels',
  appointmentTypeName: 'intake call',
  whenLabel: 'Tue, Jun 3 · 2:00 PM',
  rescheduleUrl: 'https://x/r?token=abc',
  reofferUrl: 'https://x/o?token=def',
};

const quietLog = () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() });

beforeEach(() => {
  lambdaMock.reset();
  jest.clearAllMocks();
});

// ─── buildEmailPayload (per-kind) ────────────────────────────────────────────────────

describe('buildEmailPayload', () => {
  test('reschedule_link embeds the reschedule link + STOP line', () => {
    const p = buildEmailPayload({ kind: 'reschedule_link', booking: baseBooking });
    expect(p.to).toBe('vol@example.com');
    expect(p.subject).toContain('Austin Angels');
    expect(p.text_body).toContain('https://x/r?token=abc');
    expect(p.text_body).toContain('Sam'); // first name only
    expect(p.text_body).toContain('reply STOP');
    expect(p.html_body).toContain('href="https://x/r?token=abc"');
    expect(p.html_body).toContain('reply STOP');
  });

  test('reschedule_link throws without a reschedule link', () => {
    expect(() =>
      buildEmailPayload({
        kind: 'reschedule_link',
        booking: { ...baseBooking, rescheduleUrl: undefined },
      })
    ).toThrow('reschedule_link requires booking.rescheduleUrl');
  });

  test('reoffer prefers reofferUrl', () => {
    const p = buildEmailPayload({ kind: 'reoffer', booking: baseBooking });
    expect(p.text_body).toContain('https://x/o?token=def');
  });

  test('reoffer falls back to rescheduleUrl when reofferUrl is absent', () => {
    const p = buildEmailPayload({
      kind: 'reoffer',
      booking: { ...baseBooking, reofferUrl: undefined },
    });
    expect(p.text_body).toContain('https://x/r?token=abc');
  });

  test('reoffer throws when neither link is present', () => {
    expect(() =>
      buildEmailPayload({
        kind: 'reoffer',
        booking: { ...baseBooking, reofferUrl: undefined, rescheduleUrl: undefined },
      })
    ).toThrow('reoffer requires booking.reofferUrl or booking.rescheduleUrl');
  });

  test('cancel_notice includes a rebook link when present', () => {
    const p = buildEmailPayload({ kind: 'cancel_notice', booking: baseBooking });
    expect(p.subject).toContain('canceled');
    expect(p.text_body).toContain('Want to rebook?');
    expect(p.text_body).toContain('https://x/r?token=abc');
    expect(p.html_body).toContain('href="https://x/r?token=abc"');
  });

  test('cancel_notice stands alone without a rebook link', () => {
    const p = buildEmailPayload({
      kind: 'cancel_notice',
      booking: { ...baseBooking, rescheduleUrl: undefined },
    });
    expect(p.text_body).not.toContain('Want to rebook?');
    expect(p.text_body).toContain('has been canceled');
  });

  test('reads snake_case booking rows (forward-compat) + escapes HTML', () => {
    const p = buildEmailPayload({
      kind: 'reschedule_link',
      booking: {
        booking_id: 'bk-2',
        attendee_email: 'snake@example.com',
        attendee_name: '<b>Eve</b> Stone',
        organization_name: 'Org & Co',
        appointment_type_name: 'call',
        reschedule_url: 'https://x/r',
      },
    });
    expect(p.to).toBe('snake@example.com');
    expect(p.subject).toContain('Org & Co'); // subject is plain text → raw (never escaped)
    expect(p.html_body).toContain('&lt;b&gt;Eve&lt;/b&gt;'); // first-name token escaped in html
    expect(p.text_body).toContain('<b>Eve</b>'); // raw in text body
  });

  test('falls back to generic copy when org/apptType/name absent', () => {
    const p = buildEmailPayload({
      kind: 'cancel_notice',
      booking: { attendeeEmail: 'a@b.com' },
    });
    expect(p.subject).toContain('appointment'); // default apptType
    expect(p.subject).toContain('us'); // default org
    expect(p.text_body).toContain('Hi ,'); // empty first name tolerated
  });
});

// ─── dispatchVolunteerNotice — agent-of-CoR guard ─────────────────────────────────────

describe('dispatchVolunteerNotice — agent-of-CoR guard (§5.1)', () => {
  test.each(['reassigned', 'moved'])(
    'suppresses %s (Google native email covers it) — no invoke',
    async (kind) => {
      const invokeEmail = jest.fn();
      const invokeSms = jest.fn();
      const log = quietLog();
      const res = await dispatchVolunteerNotice(
        { kind, tenantId: TENANT, booking: baseBooking },
        { invokeEmail, invokeSms, log }
      );
      expect(res).toEqual({
        kind,
        suppressed: true,
        reason: 'agent_of_cor_native_email',
        dispatched: {},
      });
      expect(invokeEmail).not.toHaveBeenCalled();
      expect(invokeSms).not.toHaveBeenCalled();
    }
  );

  test('throws on an unknown kind', async () => {
    await expect(
      dispatchVolunteerNotice(
        { kind: 'bogus', tenantId: TENANT, booking: baseBooking },
        { log: quietLog() }
      )
    ).rejects.toThrow('unknown notice kind: bogus');
  });

  test('throws without tenantId', async () => {
    await expect(
      dispatchVolunteerNotice(
        { kind: 'reoffer', booking: baseBooking },
        { log: quietLog() }
      )
    ).rejects.toThrow('tenantId is required');
  });
});

// ─── dispatchVolunteerNotice — email kinds ────────────────────────────────────────────

describe('dispatchVolunteerNotice — email dispatch', () => {
  test('reschedule_link invokes email with the built payload', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const res = await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId: TENANT, booking: baseBooking },
      { invokeEmail, log: quietLog() }
    );
    expect(res).toEqual({ kind: 'reschedule_link', suppressed: false, dispatched: { email: 'sent' } });
    expect(invokeEmail).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: TENANT, to: 'vol@example.com' })
    );
  });

  test('skips when there is no recipient email', async () => {
    const invokeEmail = jest.fn();
    const log = quietLog();
    const res = await dispatchVolunteerNotice(
      {
        kind: 'cancel_notice',
        tenantId: TENANT,
        booking: { bookingId: 'bk-x' }, // no attendee email
      },
      { invokeEmail, log }
    );
    expect(res.dispatched.email).toBe('skipped_no_recipient');
    expect(invokeEmail).not.toHaveBeenCalled();
  });

  test('send failure is non-fatal + PII-redacted (no attendee email in the log)', async () => {
    const invokeEmail = jest.fn().mockRejectedValue(new Error('lambda throttled'));
    const log = quietLog();
    const res = await dispatchVolunteerNotice(
      { kind: 'reoffer', tenantId: TENANT, booking: baseBooking },
      { invokeEmail, log }
    );
    expect(res.dispatched.email).toBe('failed'); // did not throw
    const logged = log.error.mock.calls.map((c) => c.join(' ')).join('\n');
    expect(logged).toContain('bk-1'); // booking id ok
    expect(logged).not.toContain('vol@example.com'); // attendee email never logged
  });

  test('channels.email:false suppresses the email attempt', async () => {
    const invokeEmail = jest.fn();
    const res = await dispatchVolunteerNotice(
      {
        kind: 'reschedule_link',
        tenantId: TENANT,
        booking: baseBooking,
        channels: { email: false },
      },
      { invokeEmail, log: quietLog() }
    );
    expect(invokeEmail).not.toHaveBeenCalled();
    expect(res.dispatched).toEqual({});
  });
});

// ─── dispatchVolunteerNotice — SMS stub ───────────────────────────────────────────────

describe('dispatchVolunteerNotice — SMS path (TODO SMS-E stub)', () => {
  test('move_optin_sms returns the stub marker, never sends', async () => {
    const invokeSms = jest.fn().mockResolvedValue({ stub: true });
    const res = await dispatchVolunteerNotice(
      { kind: 'move_optin_sms', tenantId: TENANT, booking: baseBooking },
      { invokeSms, log: quietLog() }
    );
    expect(res).toEqual({
      kind: 'move_optin_sms',
      suppressed: false,
      dispatched: { sms: 'stubbed_todo_sms_e' },
    });
    expect(invokeSms).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'move_optin_sms', tenantId: TENANT })
    );
  });

  test('an email kind with channels.sms:true also fires the SMS stub', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const invokeSms = jest.fn().mockResolvedValue({ stub: true });
    const res = await dispatchVolunteerNotice(
      {
        kind: 'reschedule_link',
        tenantId: TENANT,
        booking: baseBooking,
        channels: { email: true, sms: true },
      },
      { invokeEmail, invokeSms, log: quietLog() }
    );
    expect(res.dispatched).toEqual({ email: 'sent', sms: 'stubbed_todo_sms_e' });
  });

  test('a throwing SMS impl is caught (best-effort)', async () => {
    const invokeSms = jest.fn().mockRejectedValue(new Error('boom'));
    const res = await dispatchVolunteerNotice(
      { kind: 'move_optin_sms', tenantId: TENANT, booking: baseBooking },
      { invokeSms, log: quietLog() }
    );
    expect(res.dispatched.sms).toBe('failed');
  });

  test('the real stub logs the TODO and returns { stub: true } without throwing', async () => {
    const log = quietLog();
    const out = await defaultInvokeSms({ tenantId: TENANT, kind: 'move_optin_sms', log });
    expect(out).toEqual({ stub: true });
    expect(log.warn).toHaveBeenCalledWith(expect.stringContaining('TODO SMS-E'));
  });

  test('the stub tolerates an absent log (defaults to console)', async () => {
    const spy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    await defaultInvokeSms({ tenantId: TENANT, kind: 'move_optin_sms' });
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });
});

// ─── defaultInvokeEmail (aws-sdk-client-mock) ─────────────────────────────────────────

describe('defaultInvokeEmail', () => {
  test('invokes send_email async (Event) with the API-Gateway-shaped body', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    await defaultInvokeEmail({
      tenantId: TENANT,
      to: 'vol@example.com',
      subject: 'Subj',
      html_body: '<p>h</p>',
      text_body: 't',
    });
    const calls = lambdaMock.commandCalls(InvokeCommand);
    expect(calls).toHaveLength(1);
    const input = calls[0].args[0].input;
    expect(input.FunctionName).toBe('send_email');
    expect(input.InvocationType).toBe('Event');
    const event = JSON.parse(Buffer.from(input.Payload).toString());
    const inner = JSON.parse(event.body);
    expect(inner.to).toEqual(['vol@example.com']);
    expect(inner.subject).toBe('Subj');
    expect(inner.tags.tenant_id).toBe(TENANT);
    expect(inner.tags.email_type).toBe('scheduling_notice');
  });

  test('end-to-end through the default invokeEmail (no injected seam)', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    const res = await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId: TENANT, booking: baseBooking },
      { log: quietLog() }
    );
    expect(res.dispatched.email).toBe('sent');
    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(1);
  });
});

// ─── defensive defaults / edge branches ──────────────────────────────────────────────

describe('defensive defaults', () => {
  test('dispatchVolunteerNotice() with no args throws (default args/deps applied)', async () => {
    await expect(dispatchVolunteerNotice()).rejects.toThrow('tenantId is required');
  });

  test('cancel_notice with no booking object → no recipient, bookingId "unknown"', async () => {
    const invokeEmail = jest.fn();
    const log = quietLog();
    const res = await dispatchVolunteerNotice(
      { kind: 'cancel_notice', tenantId: TENANT },
      { invokeEmail, log }
    );
    expect(res.dispatched.email).toBe('skipped_no_recipient');
    expect(invokeEmail).not.toHaveBeenCalled();
    expect(log.warn.mock.calls.join(' ')).toContain('unknown'); // bookingId fallback
  });

  test('firstNameOf tolerates a whitespace-only name', () => {
    const p = buildEmailPayload({
      kind: 'cancel_notice',
      booking: { attendeeEmail: 'a@b.com', attendeeName: '   ' },
    });
    expect(p.text_body).toContain('Hi ,'); // empty first name
  });

  test('defaultInvokeEmail tags fall back to "unknown" tenant', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    await defaultInvokeEmail({ to: 'a@b.com', subject: 's', html_body: 'h', text_body: 't' });
    const input = lambdaMock.commandCalls(InvokeCommand)[0].args[0].input;
    const inner = JSON.parse(JSON.parse(Buffer.from(input.Payload).toString()).body);
    expect(inner.tags.tenant_id).toBe('unknown');
  });
});

// ─── render / escapeHtml units ────────────────────────────────────────────────────────

describe('render / escapeHtml', () => {
  test('render substitutes known vars and blanks unknown ones', () => {
    expect(render('a {{x}} b {{y}} c', { x: '1' })).toBe('a 1 b  c');
  });
  test('escapeHtml escapes the five entities and stringifies null', () => {
    expect(escapeHtml(`<&>"'`)).toBe('&lt;&amp;&gt;&quot;&#39;');
    expect(escapeHtml(null)).toBe('');
  });
});
