'use strict';

/**
 * Unit tests for notify.js (WS-SCHED-FOUNDATIONS, contract Y).
 *
 * Covers: per-kind email payload build; the agent-of-CoR guard (reassigned/moved
 * suppressed); the G7b SMS send path (buildSmsPayload + real SMS_Sender invoke,
 * sendType:'contact', STOP/HELP footer, override flow); channel defaults + overrides; best-effort
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
  buildSmsPayload,
  defaultInvokeEmail,
  defaultInvokeSms,
  render,
  escapeHtml,
  SMS_STOP_FOOTER,
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

  test('prefers explicit attendeeFirstName over deriving from attendeeName (camelCase)', () => {
    const p = buildEmailPayload({
      kind: 'cancel_notice',
      booking: { attendeeEmail: 'a@b.com', attendeeFirstName: 'Alex', attendeeName: 'Sam Patel' },
    });
    expect(p.text_body).toContain('Hi Alex,'); // explicit first name wins
    expect(p.text_body).not.toContain('Sam');
  });

  test('drops a non-https reschedule link (javascript: scheme never reaches an href)', () => {
    // reschedule_link requires a link → a hostile scheme is treated as missing → throws.
    expect(() =>
      buildEmailPayload({
        kind: 'reschedule_link',
        booking: { ...baseBooking, rescheduleUrl: 'javascript:alert(1)' },
      })
    ).toThrow('reschedule_link requires booking.rescheduleUrl');

    // cancel_notice's rebook link is optional → a hostile scheme is silently dropped,
    // and the body/href must NOT contain it.
    const p = buildEmailPayload({
      kind: 'cancel_notice',
      booking: { ...baseBooking, rescheduleUrl: 'javascript:alert(1)' },
    });
    expect(p.html_body).not.toContain('javascript:');
    expect(p.text_body).not.toContain('javascript:');
    expect(p.text_body).not.toContain('Want to rebook?');
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

  test('reengagement wraps the WS-E-COPY body with the STOP footer (no second CTA)', () => {
    const body = 'Hi Sam,\n\nWe missed you for your intake call. You can reschedule here: https://x/r?token=abc';
    const p = buildEmailPayload({
      kind: 'reengagement',
      booking: {
        attendeeEmail: 'vol@example.com',
        reengagementBody: body,
        organizationName: 'Austin Angels',
        appointmentTypeName: 'intake call',
      },
    });
    expect(p.to).toBe('vol@example.com');
    expect(p.subject).toContain('intake call');
    expect(p.subject).toContain('Austin Angels');
    expect(p.text_body).toContain('We missed you'); // COPY body verbatim
    expect(p.text_body).toContain('reply STOP'); // notify owns the footer (§E8)
    expect(p.html_body).toContain('reply STOP');
    // notify must NOT inject a SECOND reschedule CTA — COPY owns the in-body link (§E8).
    expect((p.text_body.match(/reschedule/gi) || []).length).toBeLessThanOrEqual(1);
  });

  test('reengagement throws when the body is absent (caller contract error)', () => {
    expect(() =>
      buildEmailPayload({ kind: 'reengagement', booking: { attendeeEmail: 'a@b.com' } })
    ).toThrow(/reengagement requires booking\.reengagement_body/);
    expect(() =>
      buildEmailPayload({ kind: 'reengagement', booking: { attendeeEmail: 'a@b.com', reengagementBody: '   ' } })
    ).toThrow(/reengagement requires/);
  });

  test('reengagement HTML-escapes the COPY body (XSS-safe) + reads snake_case', () => {
    const p = buildEmailPayload({
      kind: 'reengagement',
      booking: { attendee_email: 'v@e.com', reengagement_body: 'Hi <script>alert(1)</script>\n\nsee you' },
    });
    expect(p.to).toBe('v@e.com'); // snake_case forward-compat read
    expect(p.html_body).not.toContain('<script>'); // escaped, no raw markup
    expect(p.html_body).toContain('&lt;script&gt;');
    expect(p.html_body).toContain('</p><p>'); // paragraph break preserved
    expect(p.html_body).toContain('reply STOP');
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

  test('reoffer dispatches email (sent)', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const res = await dispatchVolunteerNotice(
      { kind: 'reoffer', tenantId: TENANT, booking: baseBooking },
      { invokeEmail, log: quietLog() }
    );
    expect(res).toEqual({ kind: 'reoffer', suppressed: false, dispatched: { email: 'sent' } });
    expect(invokeEmail).toHaveBeenCalledTimes(1);
  });

  test('cancel_notice dispatches email (sent)', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const res = await dispatchVolunteerNotice(
      { kind: 'cancel_notice', tenantId: TENANT, booking: baseBooking },
      { invokeEmail, log: quietLog() }
    );
    expect(res).toEqual({ kind: 'cancel_notice', suppressed: false, dispatched: { email: 'sent' } });
    expect(invokeEmail).toHaveBeenCalledTimes(1);
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

// ─── buildSmsPayload (G7b) ────────────────────────────────────────────────────────────

const smsBooking = { ...baseBooking, attendeePhone: '+15125551234' };

describe('buildSmsPayload', () => {
  test('reschedule_link renders the default body with the action link + STOP/HELP footer', () => {
    const p = buildSmsPayload({ kind: 'reschedule_link', booking: smsBooking });
    expect(p.to).toBe('+15125551234');
    expect(p.body).toContain('https://x/r?token=abc');
    expect(p.body).toContain('intake call');
    expect(p.body.endsWith(SMS_STOP_FOOTER)).toBe(true);
    expect(p.body).toMatch(/reply\s+STOP/i); // compliance marker
  });

  test('reoffer falls back to rescheduleUrl when reofferUrl absent', () => {
    const p = buildSmsPayload({
      kind: 'reoffer',
      booking: { ...smsBooking, reofferUrl: undefined },
    });
    expect(p.body).toContain('https://x/r?token=abc');
  });

  test('cancel_notice includes the optional rebook link, and omits it when absent', () => {
    const withRebook = buildSmsPayload({ kind: 'cancel_notice', booking: smsBooking });
    expect(withRebook.body).toContain('Want to rebook? https://x/r?token=abc');
    const noRebook = buildSmsPayload({
      kind: 'cancel_notice',
      booking: { ...smsBooking, rescheduleUrl: undefined },
    });
    expect(noRebook.body).not.toContain('Want to rebook?');
  });

  test('a tenant sms_text override replaces the body (footer still appended)', () => {
    const p = buildSmsPayload({
      kind: 'reschedule_link',
      booking: smsBooking,
      smsOverride: 'Yo {{firstName}} — reschedule: {{actionUrl}}',
    });
    expect(p.body).toContain('Yo Sam — reschedule: https://x/r?token=abc');
    expect(p.body.endsWith(SMS_STOP_FOOTER)).toBe(true);
  });

  test('an override already containing "reply STOP" is NOT double-footed', () => {
    const p = buildSmsPayload({
      kind: 'reschedule_link',
      booking: smsBooking,
      smsOverride: 'Pick a time {{actionUrl}}. Reply STOP to opt out.',
    });
    expect((p.body.match(/reply\s+STOP/gi) || []).length).toBe(1);
  });

  test('a kind with no SMS template and no override yields an empty body (no footer-only SMS)', () => {
    const p = buildSmsPayload({ kind: 'move_optin_sms', booking: smsBooking });
    expect(p.body).toBe('');
  });

  test('forward-compatible snake_case phone read', () => {
    const p = buildSmsPayload({
      kind: 'reschedule_link',
      booking: { attendee_phone: '+15129990000', reschedule_url: 'https://x/r?t=z' },
    });
    expect(p.to).toBe('+15129990000');
    expect(p.body).toContain('https://x/r?t=z');
  });
});

// ─── dispatchVolunteerNotice — SMS path (G7b real send) ───────────────────────────────

describe('dispatchVolunteerNotice — SMS path (G7b)', () => {
  test('email kind + channels.sms:true → email AND a real SMS invoke (sendType:contact)', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const invokeSms = jest.fn().mockResolvedValue(undefined);
    const res = await dispatchVolunteerNotice(
      {
        kind: 'reschedule_link',
        tenantId: TENANT,
        booking: smsBooking,
        channels: { email: true, sms: true },
      },
      { invokeEmail, invokeSms, log: quietLog() }
    );
    expect(res.dispatched).toEqual({ email: 'sent', sms: 'sent' });
    expect(invokeSms).toHaveBeenCalledWith(
      expect.objectContaining({
        tenantId: TENANT,
        to: '+15125551234',
        kind: 'reschedule_link',
      })
    );
    const smsBody = invokeSms.mock.calls[0][0].body;
    expect(smsBody).toMatch(/reply\s+STOP/i);       // footer present
    expect(smsBody).toContain('https://x/r?token=abc'); // the actual reschedule link, not just the footer
  });

  test('reoffer + cancel_notice also dispatch SMS via channels.sms:true', async () => {
    for (const kind of ['reoffer', 'cancel_notice']) {
      const invokeSms = jest.fn().mockResolvedValue(undefined);
      const res = await dispatchVolunteerNotice(
        { kind, tenantId: TENANT, booking: smsBooking, channels: { email: false, sms: true } },
        { invokeSms, log: quietLog() }
      );
      expect(res.dispatched.sms).toBe('sent');
      expect(invokeSms.mock.calls[0][0].body).toMatch(/reply\s+STOP/i);
    }
  });

  test('an email kind WITHOUT channels.sms does not attempt SMS (email is the floor)', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const invokeSms = jest.fn();
    const res = await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId: TENANT, booking: smsBooking },
      { invokeEmail, invokeSms, log: quietLog() }
    );
    expect(invokeSms).not.toHaveBeenCalled();
    expect(res.dispatched).toEqual({ email: 'sent' });
  });

  test('no recipient phone → skipped_no_recipient, never invokes the sender', async () => {
    const invokeSms = jest.fn();
    const res = await dispatchVolunteerNotice(
      {
        kind: 'reschedule_link',
        tenantId: TENANT,
        booking: baseBooking, // no attendeePhone
        channels: { email: false, sms: true },
      },
      { invokeSms, log: quietLog() }
    );
    expect(invokeSms).not.toHaveBeenCalled();
    expect(res.dispatched.sms).toBe('skipped_no_recipient');
  });

  test('an SMS-native kind dispatched WITHOUT explicit channels throws (TCPA gate is caller-owned)', async () => {
    const invokeSms = jest.fn();
    await expect(
      dispatchVolunteerNotice(
        { kind: 'move_optin_sms', tenantId: TENANT, booking: smsBooking },
        { invokeSms, log: quietLog() }
      )
    ).rejects.toThrow(/requires an explicit channels/);
    expect(invokeSms).not.toHaveBeenCalled();
  });

  test('SMS-native kind with no template → skipped_no_template (no footer-only spam)', async () => {
    const invokeSms = jest.fn();
    const res = await dispatchVolunteerNotice(
      // explicit channels (the caller-owned TCPA gate decided sms:true); no template for the kind.
      { kind: 'move_optin_sms', tenantId: TENANT, booking: smsBooking, channels: { sms: true } },
      { invokeSms, log: quietLog() }
    );
    expect(invokeSms).not.toHaveBeenCalled();
    expect(res.dispatched.sms).toBe('skipped_no_template');
  });

  test('channels.sms:false suppresses an SMS-native kind (mirrors the email guard)', async () => {
    const invokeSms = jest.fn();
    const res = await dispatchVolunteerNotice(
      {
        kind: 'move_optin_sms',
        tenantId: TENANT,
        booking: smsBooking,
        channels: { sms: false },
      },
      { invokeSms, log: quietLog() }
    );
    expect(invokeSms).not.toHaveBeenCalled();
    expect(res.dispatched).toEqual({});
  });

  test('the §E14 sms_text override flows into the SMS body', async () => {
    const invokeSms = jest.fn().mockResolvedValue(undefined);
    const loadTemplateOverride = jest
      .fn()
      .mockResolvedValue({ sms: 'Custom {{firstName}}: {{actionUrl}}' });
    await dispatchVolunteerNotice(
      {
        kind: 'reschedule_link',
        tenantId: TENANT,
        booking: smsBooking,
        channels: { email: false, sms: true },
      },
      { invokeSms, loadTemplateOverride, log: quietLog() }
    );
    expect(invokeSms.mock.calls[0][0].body).toContain('Custom Sam: https://x/r?token=abc');
  });

  test('a throwing SMS impl is caught (best-effort); email still sent', async () => {
    const invokeEmail = jest.fn().mockResolvedValue(undefined);
    const invokeSms = jest.fn().mockRejectedValue(new Error('telnyx 500'));
    const log = quietLog();
    const res = await dispatchVolunteerNotice(
      {
        kind: 'reschedule_link',
        tenantId: TENANT,
        booking: smsBooking,
        channels: { email: true, sms: true },
      },
      { invokeEmail, invokeSms, log }
    );
    expect(res.dispatched).toEqual({ email: 'sent', sms: 'failed' });
    // PII-redacted: the phone is never logged.
    const logged = log.error.mock.calls.map((c) => c.join(' ')).join('\n');
    expect(logged).not.toContain('+15125551234');
  });

  test('defaultInvokeSms invokes SMS_Sender async (Event) with sendType:contact', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    await defaultInvokeSms({
      tenantId: TENANT,
      to: '+15125551234',
      body: 'hello',
      kind: 'reschedule_link',
      sessionId: 'sess-1',
    });
    const calls = lambdaMock.commandCalls(InvokeCommand);
    expect(calls).toHaveLength(1);
    const input = calls[0].args[0].input;
    expect(input.FunctionName).toBe('SMS_Sender');
    expect(input.InvocationType).toBe('Event');
    const sent = JSON.parse(Buffer.from(input.Payload).toString());
    expect(sent).toMatchObject({
      to: '+15125551234',
      body: 'hello',
      tenantId: TENANT,
      type: 'reschedule_link',
      sendType: 'contact',
      sessionId: 'sess-1',
    });
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
