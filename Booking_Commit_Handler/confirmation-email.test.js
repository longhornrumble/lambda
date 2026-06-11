'use strict';

/**
 * Unit tests for confirmation-email.js — AC #7 confirmation (.ics + join link +
 * signed cancel/reschedule links, ≤60s). SES is mocked with aws-sdk-client-mock;
 * the shared D1a tokens.sign is mocked (its own suite covers signing).
 */

jest.mock('../shared/scheduling/tokens', () => ({
  sign: jest.fn(async (purpose) => `signed.${purpose}.token`),
}));

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { SESClient, SendRawEmailCommand } = require('@aws-sdk/client-ses');

const sesMock = mockClient(SESClient);
const { sign } = require('../shared/scheduling/tokens');
const email = require('./confirmation-email');

const ARGS = {
  tenantId: 'AUS123957', bookingId: 'booking#abc',
  attendeeEmail: 'sam@example.com', attendeeFirstName: 'Sam',
  appointmentTypeName: 'Volunteer intake', orgName: 'Austin Angels',
  coordinatorName: 'Maya', coordinatorEmail: 'maya@org.org',
  start: '2026-06-03T18:00:00.000Z', end: '2026-06-03T18:30:00.000Z',
  whenLabel: 'Wed, Jun 3 - 1:00 PM CT', joinUrl: 'https://zoom.us/j/12345',
  deepLink: 'https://schedule.myrecruiter.ai/b/booking#abc',
  startAt: '2026-06-03T18:00:00.000Z', cancellationWindowHours: 0,
};

beforeEach(() => {
  sesMock.reset();
  sign.mockClear();
});

describe('buildIcs', () => {
  it('emits a VCALENDAR/VEVENT with UID, DTSTART/DTEND, SUMMARY', () => {
    const ics = email.buildIcs({
      bookingId: 'booking#abc', summary: 'Volunteer intake — Sam',
      start: ARGS.start, end: ARGS.end, organizerEmail: 'maya@org.org', attendeeEmail: 'sam@example.com',
    });
    expect(ics).toContain('BEGIN:VCALENDAR');
    expect(ics).toContain('BEGIN:VEVENT');
    expect(ics).toContain('UID:booking#abc@myrecruiter.ai');
    expect(ics).toContain('DTSTART:20260603T180000Z');
    expect(ics).toContain('DTEND:20260603T183000Z');
    expect(ics).toContain('SUMMARY:Volunteer intake — Sam');
    expect(ics.split('\r\n').length).toBeGreaterThan(5); // CRLF-delimited
  });

  it('RFC5545-escapes special chars in text fields', () => {
    const ics = email.buildIcs({
      bookingId: 'b', summary: 'A; B, C\\D', start: ARGS.start, end: ARGS.end,
    });
    expect(ics).toContain('SUMMARY:A\\; B\\, C\\\\D');
  });
});

describe('buildActionLinks — §13 signed cancel/reschedule', () => {
  it('signs both purposes and builds purpose-matched URLs', async () => {
    const links = await email.buildActionLinks(
      { tenantId: 'AUS123957', bookingId: 'booking#abc', startAt: ARGS.start, cancellationWindowHours: 0 },
      undefined
    );
    expect(sign).toHaveBeenCalledWith('cancel', expect.objectContaining({ booking_id: 'booking#abc' }), undefined);
    expect(sign).toHaveBeenCalledWith('reschedule', expect.any(Object), undefined);
    expect(links.cancelUrl).toContain('/cancel?t=signed.cancel.token');
    expect(links.rescheduleUrl).toContain('/reschedule?t=signed.reschedule.token');
  });
});

describe('escaping helpers', () => {
  it('escapeHtml neutralizes a script payload', () => {
    expect(email.escapeHtml('<script>alert(1)</script>')).toBe('&lt;script&gt;alert(1)&lt;/script&gt;');
  });
  it('stripCrlf removes header-injection newlines', () => {
    expect(email.stripCrlf('Sam\r\nBcc: evil@x')).toBe('Sam Bcc: evil@x');
  });
});

describe('buildRawMime', () => {
  it('assembles multipart/mixed with the .ics attachment + alternative bodies', () => {
    const raw = email.buildRawMime({
      from: 'notify@myrecruiter.ai', to: 'sam@example.com', subject: 'Confirmed',
      textBody: 'text', htmlBody: '<p>html</p>', icsContent: 'BEGIN:VCALENDAR', icsFilename: 'invite.ics',
    });
    expect(raw).toContain('Content-Type: multipart/mixed');
    expect(raw).toContain('Content-Type: multipart/alternative');
    expect(raw).toContain('Content-Type: text/calendar');
    expect(raw).toContain('filename="invite.ics"');
    expect(raw).toContain(Buffer.from('BEGIN:VCALENDAR').toString('base64'));
  });

  it('strips CR/LF from the subject (no header injection)', () => {
    const raw = email.buildRawMime({
      from: 'f@x', to: 't@x', subject: 'Hi\r\nBcc: evil@x', textBody: 't', htmlBody: 'h',
      icsContent: 'x', icsFilename: 'invite.ics',
    });
    expect(raw).toContain('Subject: Hi Bcc: evil@x');
    expect(raw).not.toContain('Subject: Hi\r\nBcc');
  });
});

describe('sendConfirmationEmail — AC #7', () => {
  it('sends via SES SendRawEmail with .ics + signed links, within the SLA budget', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-1' });
    const startedAt = Date.now();
    const res = await email.sendConfirmationEmail(ARGS, {});
    const elapsed = Date.now() - startedAt;

    expect(res.messageId).toBe('msg-1');
    expect(res.rescheduleUrl).toContain('/reschedule?t=');
    expect(res.cancelUrl).toContain('/cancel?t=');
    expect(elapsed).toBeLessThan(60 * 1000); // 60s AC #7 budget (unit-time is ~ms)

    const sent = sesMock.commandCalls(SendRawEmailCommand)[0].args[0].input;
    const raw = Buffer.from(sent.RawMessage.Data).toString('utf8');
    expect(raw).toContain('Content-Type: text/calendar'); // the .ics attachment part
    // the .ics body is base64 — decode and confirm the calendar content rode along.
    const icsB64 = raw.split('Content-Disposition: attachment; filename="invite.ics"')[1].trim().split('\r\n\r\n')[0]
      || raw.match(/QkVHSU46[A-Za-z0-9+/=]+/)[0];
    expect(Buffer.from(icsB64, 'base64').toString('utf8')).toContain('BEGIN:VCALENDAR');
    expect(raw).toContain('Join the meeting');
    expect(raw).toContain('signed.reschedule.token');
    expect(sent.ConfigurationSetName).toBe('picasso-emails');
  });

  it('HTML-encodes the volunteer first name in the HTML body (output sanitization)', () => {
    // The HTML body is the XSS-relevant context; the text/plain body legitimately
    // carries the literal name (plain text is not executed).
    const { htmlBody } = email.buildBodies({
      firstName: '<script>alert(1)</script>', orgName: 'Org',
      whenLabel: 'soon', joinUrl: 'https://zoom.us/j/1', rescheduleUrl: 'r', cancelUrl: 'c',
    });
    expect(htmlBody).toContain('&lt;script&gt;');
    expect(htmlBody).not.toMatch(/<script>alert\(1\)<\/script>/);
  });

  it('throws when attendeeEmail is missing', async () => {
    await expect(email.sendConfirmationEmail({ ...ARGS, attendeeEmail: '' }, {})).rejects.toThrow(/attendeeEmail/);
  });
});

describe('helper edge cases (branch coverage)', () => {
  it('escapers tolerate null/undefined', () => {
    expect(email.escapeHtml(null)).toBe('');
    expect(email.escapeIcsText(undefined)).toBe('');
    expect(email.stripCrlf(null)).toBe('');
  });

  it('buildIcs omits optional lines when absent', () => {
    const ics = email.buildIcs({ bookingId: 'b', summary: 's', start: ARGS.start, end: ARGS.end });
    expect(ics).not.toContain('DESCRIPTION:');
    expect(ics).not.toContain('LOCATION:');
    expect(ics).not.toContain('ORGANIZER:');
    expect(ics).not.toContain('ATTENDEE');
  });

  it('buildBodies falls back gracefully with minimal inputs', () => {
    const { textBody, htmlBody } = email.buildBodies({
      firstName: '', orgName: '', whenLabel: '', joinUrl: '', rescheduleUrl: 'r', cancelUrl: 'c',
    });
    expect(textBody).toContain('Hi there,');
    expect(htmlBody).toContain('Hi there,');
    expect(htmlBody).not.toContain('Join the meeting'); // no joinUrl → no join link
    expect(textBody).toContain('the team'); // org fallback — S4c aligned to vars.org (was 'MyRecruiter')
  });

  it('sendConfirmationEmail uses start as startAt fallback when startAt omitted', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-3' });
    await email.sendConfirmationEmail({ ...ARGS, startAt: undefined, joinUrl: undefined, cancellationWindowHours: undefined }, {});
    expect(sign).toHaveBeenCalledWith('cancel', expect.objectContaining({ start_at: ARGS.start }), undefined);
  });
});

// ─── §E14 S4c: per-tenant confirmation template overrides ─────────────────────────────

const { mockClient: mockDdbClient } = require('aws-sdk-client-mock');
const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');

describe('buildBodies — §E14 override/default resolution', () => {
  const BASE = {
    firstName: 'Sam', orgName: 'Austin Angels', apptTypeName: 'Volunteer intake',
    whenLabel: 'Wed, Jun 3 - 1:00 PM CT', joinUrl: 'https://zoom.us/j/12345',
    rescheduleUrl: 'https://schedule.myrecruiter.ai/reschedule?t=r',
    cancelUrl: 'https://schedule.myrecruiter.ai/cancel?t=c',
  };

  it('no override → the shared §E14 defaults (byte-in-sync with the ADA editor) + action block', () => {
    const { subject, textBody, htmlBody } = email.buildBodies({ ...BASE, templateOverride: null });
    expect(subject).toBe("You're confirmed — Austin Angels");
    expect(textBody).toContain("Hi Sam,\n\nYou're confirmed for your Volunteer intake Wed, Jun 3 - 1:00 PM CT.");
    expect(textBody).toContain('Join: https://zoom.us/j/12345');
    expect(textBody).toContain('Reschedule: https://schedule.myrecruiter.ai/reschedule?t=r');
    expect(textBody).toContain('Cancel: https://schedule.myrecruiter.ai/cancel?t=c');
    // NB: the apostrophe is template-literal text (only VARS are escaped), matching ADA:
    expect(htmlBody).toContain("<p>Hi Sam,</p><p>You're confirmed for your Volunteer intake Wed, Jun 3 - 1:00 PM CT.</p>");
    expect(htmlBody).toContain('>Reschedule</a>');
    expect(htmlBody).toContain('>Cancel</a>');
  });

  it('override is rendered with {firstName,org,apptType,whenLabel}; action links survive REGARDLESS (compliance invariant)', () => {
    // Adversarial override: tries to BE the whole email with no links at all.
    const templateOverride = {
      subject: 'See you {{whenLabel}}, {{firstName}}!',
      text: '{{firstName}} — {{apptType}} at {{org}}. No links needed!',
      html: '<p>{{firstName}} — {{apptType}} at {{org}}. No links needed!</p>',
    };
    const { subject, textBody, htmlBody } = email.buildBodies({ ...BASE, templateOverride });
    expect(subject).toBe('See you Wed, Jun 3 - 1:00 PM CT, Sam!');
    expect(textBody).toContain('Sam — Volunteer intake at Austin Angels. No links needed!');
    // The signed action links are OUTSIDE the editable region — the override cannot drop them:
    expect(textBody).toContain('Reschedule: https://schedule.myrecruiter.ai/reschedule?t=r');
    expect(textBody).toContain('Cancel: https://schedule.myrecruiter.ai/cancel?t=c');
    expect(textBody).toContain('Join: https://zoom.us/j/12345');
    expect(htmlBody).toContain('>Reschedule</a>');
    expect(htmlBody).toContain('>Cancel</a>');
    expect(htmlBody).toContain('Join the meeting');
  });

  it('partial override (subject only) → body fields fall back to the defaults', () => {
    const { subject, textBody } = email.buildBodies({ ...BASE, templateOverride: { subject: 'Custom — {{org}}' } });
    expect(subject).toBe('Custom — Austin Angels');
    expect(textBody).toContain("You're confirmed for your Volunteer intake");
  });

  it('whitespace-only override field falls back; unknown {{vars}} render empty', () => {
    const { subject, textBody } = email.buildBodies({
      ...BASE,
      templateOverride: { subject: '   ', text: 'Hi {{firstName}}, ref {{actionUrl}}done.' },
    });
    expect(subject).toBe("You're confirmed — Austin Angels");
    expect(textBody).toContain('Hi Sam, ref done.');
  });

  it('html vars are HTML-escaped; text vars verbatim (injection guard)', () => {
    const { textBody, htmlBody } = email.buildBodies({
      ...BASE,
      firstName: '<img src=x onerror=alert(1)>',
      templateOverride: { text: 'Hi {{firstName}}!', html: '<p>Hi {{firstName}}!</p>' },
    });
    expect(htmlBody).toContain('&lt;img src=x onerror=alert(1)&gt;');
    expect(htmlBody).not.toContain('<img');
    expect(textBody).toContain('Hi <img src=x onerror=alert(1)>!');
  });

  it('override subject CRLF is stripped (header-injection guard)', () => {
    const { subject } = email.buildBodies({
      ...BASE,
      templateOverride: { subject: 'Hello {{firstName}}\r\nBcc: evil@x.com' },
    });
    expect(subject).toBe('Hello Sam Bcc: evil@x.com');
  });
});

describe('sendConfirmationEmail — §E14 override wiring (fail-safe on the 60s commit path)', () => {
  it('injected loader supplies the override; the .ics attachment is still present', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-1' });
    const loaderCalls = [];
    const res = await email.sendConfirmationEmail(ARGS, {
      loadTemplateOverride: async (a) => { loaderCalls.push(a); return { text: 'Custom for {{firstName}}.' }; },
    });
    expect(res.messageId).toBe('msg-1');
    expect(loaderCalls).toEqual([{ tenantId: 'AUS123957', log: undefined }]);
    const raw = Buffer.from(sesMock.commandCalls(SendRawEmailCommand)[0].args[0].input.RawMessage.Data).toString();
    expect(raw).toContain('Custom for Sam.');
    expect(raw).toContain('Content-Type: text/calendar'); // .ics survives any override
    expect(raw).toContain('filename="invite.ics"');
    expect(raw).toContain('Reschedule: '); // action links survive any override
  });

  it('throwing injected loader degrades to the default copy — the send still goes out', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-2' });
    const warns = [];
    const res = await email.sendConfirmationEmail(ARGS, {
      loadTemplateOverride: async () => { throw new Error('boom'); },
      log: { warn: (m) => warns.push(m) },
    });
    expect(res.messageId).toBe('msg-2');
    expect(warns.length).toBe(1);
    const raw = Buffer.from(sesMock.commandCalls(SendRawEmailCommand)[0].args[0].input.RawMessage.Data).toString();
    expect(raw).toContain("You're confirmed for your Volunteer intake");
  });
});

describe('loadTemplateOverride — fail-safe loader', () => {
  it('returns null with zero I/O while SCHED_NOTIF_TEMPLATE_TABLE is unset (this suite)', async () => {
    const ddbMock = mockDdbClient(DynamoDBClient);
    ddbMock.on(GetItemCommand).resolves({ Item: { subject: { S: 'x' } } });
    expect(await email.loadTemplateOverride({ tenantId: 'T1' })).toBeNull();
    expect(ddbMock.commandCalls(GetItemCommand).length).toBe(0);
    ddbMock.restore();
  });

  it('with the table env set: queries {tenantId, moment:confirmation}, maps fields, fail-safes on error', async () => {
    // The isolated registry re-instantiates @aws-sdk/client-dynamodb — mock THAT class,
    // not the outer one, or the isolated module's client escapes the mock.
    let isolated, ddbMock, GetItemCommand;
    jest.isolateModules(() => {
      process.env.SCHED_NOTIF_TEMPLATE_TABLE = 'picasso-scheduling-notif-template-test';
      try {
        const dynamo = require('@aws-sdk/client-dynamodb');
        ddbMock = mockDdbClient(dynamo.DynamoDBClient);
        GetItemCommand = dynamo.GetItemCommand;
        isolated = require('./confirmation-email');
      } finally {
        delete process.env.SCHED_NOTIF_TEMPLATE_TABLE;
      }
    });

    ddbMock.on(GetItemCommand).resolves({
      Item: { subject: { S: 'S' }, body_text: { S: 'T' }, body_html: { S: '<p>H</p>' } },
    });
    const hit = await isolated.loadTemplateOverride({ tenantId: 'AUS123957' });
    expect(hit).toEqual({ subject: 'S', text: 'T', html: '<p>H</p>' });
    const call = ddbMock.commandCalls(GetItemCommand)[0].args[0].input;
    expect(call.TableName).toBe('picasso-scheduling-notif-template-test');
    expect(call.Key).toEqual({ tenantId: { S: 'AUS123957' }, moment: { S: 'confirmation' } });

    ddbMock.reset();
    ddbMock.on(GetItemCommand).resolves({}); // miss
    expect(await isolated.loadTemplateOverride({ tenantId: 'T1' })).toBeNull();

    ddbMock.reset();
    ddbMock.on(GetItemCommand).rejects(Object.assign(new Error('denied'), { name: 'AccessDeniedException' }));
    const warns = [];
    expect(await isolated.loadTemplateOverride({ tenantId: 'T1', log: { warn: (m) => warns.push(m) } })).toBeNull();
    expect(warns[0]).toContain('AccessDeniedException');
    expect(warns[0]).not.toContain('denied'); // the raw message (ARN-bearing in real life) stays out
    ddbMock.restore();
  });

  it('non-string stored fields are dropped, not coerced (schema discipline)', async () => {
    let isolated, ddbMock, GetItemCommand;
    jest.isolateModules(() => {
      process.env.SCHED_NOTIF_TEMPLATE_TABLE = 't';
      try {
        const dynamo = require('@aws-sdk/client-dynamodb');
        ddbMock = mockDdbClient(dynamo.DynamoDBClient);
        GetItemCommand = dynamo.GetItemCommand;
        isolated = require('./confirmation-email');
      } finally {
        delete process.env.SCHED_NOTIF_TEMPLATE_TABLE;
      }
    });
    ddbMock.on(GetItemCommand).resolves({ Item: { subject: { N: '42' }, body_text: { S: 'T' } } });
    expect(await isolated.loadTemplateOverride({ tenantId: 'T1' })).toEqual({
      subject: undefined, text: 'T', html: undefined,
    });
    ddbMock.restore();
  });
});

// ─── S4c audit-fix tests ───────────────────────────────────────────────────────────────

describe('sanitizeOverrideHtml — the override cannot interfere with the action-links block', () => {
  const BASE2 = {
    firstName: 'Sam', orgName: 'Austin Angels', apptTypeName: 'Volunteer intake',
    whenLabel: 'Wed', joinUrl: 'https://zoom.us/j/12345',
    rescheduleUrl: 'https://schedule.myrecruiter.ai/reschedule?t=r',
    cancelUrl: 'https://schedule.myrecruiter.ai/cancel?t=c',
  };

  it('an UNCLOSED <!-- comment cannot swallow the appended links (B1)', () => {
    const { htmlBody } = email.buildBodies({
      ...BASE2, templateOverride: { html: '<p>See you soon!</p><!--' },
    });
    expect(htmlBody).not.toContain('<!--');
    expect(htmlBody).toContain('>Reschedule</a>');
    expect(htmlBody).toContain('>Cancel</a>');
  });

  it('closed comments and <style> blocks are stripped (SR2)', () => {
    const { htmlBody } = email.buildBodies({
      ...BASE2,
      templateOverride: { html: '<p>Hi<!-- hidden --></p><style>a{display:none}</style>' },
    });
    expect(htmlBody).not.toContain('hidden');
    expect(htmlBody).not.toContain('<style');
    expect(htmlBody).toContain('>Cancel</a>');
  });

  it('admin-authored <a> tags are stripped — a fake cancel link cannot phish (SR1)', () => {
    const { htmlBody } = email.buildBodies({
      ...BASE2,
      templateOverride: { html: '<p>Cancel here: <a href="https://evil.com/steal">Cancel</a></p>' },
    });
    expect(htmlBody).not.toContain('evil.com');
    expect(htmlBody).toContain('Cancel here: Cancel</a>'); // inner text kept, opener gone
    expect(htmlBody).toContain('href="https://schedule.myrecruiter.ai/cancel?t=c"'); // the REAL link
  });

  it('unclosed <style is also neutralized', () => {
    expect(email.sanitizeOverrideHtml('<p>x</p><style>a{display:none}')).toBe('<p>x</p>');
  });
});

describe('S4c behavioral pins + coverage', () => {
  const BASE3 = {
    firstName: 'Sam', orgName: 'Austin Angels', apptTypeName: 'Volunteer intake',
    whenLabel: 'Wed, Jun 3 - 1:00 PM CT', joinUrl: '',
    rescheduleUrl: 'r', cancelUrl: 'c',
  };

  it('coordinatorName is deliberately ABSENT from the editable copy; apptType renders (SR-2 pin)', () => {
    const { textBody, htmlBody } = email.buildBodies({ ...BASE3, coordinatorName: 'Maya', templateOverride: null });
    expect(textBody).not.toContain('Maya');
    expect(htmlBody).not.toContain('Maya');
    expect(textBody).toContain('Volunteer intake');
  });

  it('org fallback is the SAME across subject, text sign-off, and html sign-off', () => {
    const { subject, textBody, htmlBody } = email.buildBodies({
      ...BASE3, orgName: '', templateOverride: null,
    });
    expect(subject).toBe("You're confirmed — the team");
    expect(textBody).toContain('— the team');
    expect(textBody).not.toContain('MyRecruiter');
    expect(htmlBody).toContain('&mdash; the team');
  });

  it('joinUrl absent UNDER an override → no Join line, links still present', () => {
    const { textBody, htmlBody } = email.buildBodies({
      ...BASE3, templateOverride: { text: 'Custom.', html: '<p>Custom.</p>' },
    });
    expect(textBody).not.toContain('Join:');
    expect(htmlBody).not.toContain('Join the meeting');
    expect(textBody).toContain('Reschedule: r');
  });

  it('ALL html vars are escaped (org/apptType/whenLabel, not just firstName)', () => {
    const { htmlBody } = email.buildBodies({
      ...BASE3,
      orgName: '<b>org</b>', apptTypeName: '<i>type</i>', whenLabel: '"when" & <u>now</u>',
      templateOverride: { html: '<p>{{org}} {{apptType}} {{whenLabel}}</p>' },
    });
    expect(htmlBody).toContain('&lt;b&gt;org&lt;/b&gt;');
    expect(htmlBody).toContain('&lt;i&gt;type&lt;/i&gt;');
    expect(htmlBody).toContain('&quot;when&quot; &amp; &lt;u&gt;now&lt;/u&gt;');
    expect(htmlBody).not.toContain('<b>org</b>');
  });

  it('text-only override → the html part keeps the DEFAULT html copy (S3)', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-3' });
    await email.sendConfirmationEmail(ARGS, {
      loadTemplateOverride: async () => ({ text: 'Custom for {{firstName}}.' }),
    });
    const raw = Buffer.from(sesMock.commandCalls(SendRawEmailCommand)[0].args[0].input.RawMessage.Data).toString();
    expect(raw).toContain('Custom for Sam.');
    expect(raw).toContain("<p>Hi Sam,</p><p>You're confirmed for your Volunteer intake");
  });

  it('SES Tags / Destinations / 8bit CTE are pinned (S1 + SR3)', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-4' });
    await email.sendConfirmationEmail(ARGS, { loadTemplateOverride: async () => null });
    const input = sesMock.commandCalls(SendRawEmailCommand)[0].args[0].input;
    expect(input.Tags).toEqual([
      { Name: 'tenant_id', Value: 'AUS123957' },
      { Name: 'email_type', Value: 'booking_confirmation' },
    ]);
    expect(input.Destinations).toEqual(['sam@example.com']);
    const raw = Buffer.from(input.RawMessage.Data).toString();
    expect(raw).toContain('Content-Transfer-Encoding: 8bit');
    expect(raw).not.toContain('Content-Transfer-Encoding: 7bit');
  });

  it('minimal args: no appointmentTypeName/attendeeFirstName/opts → fallbacks render, send succeeds', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-5' });
    const { appointmentTypeName, attendeeFirstName, ...rest } = ARGS;
    const res = await email.sendConfirmationEmail(rest);
    expect(res.messageId).toBe('msg-5');
    const raw = Buffer.from(sesMock.commandCalls(SendRawEmailCommand)[0].args[0].input.RawMessage.Data).toString();
    expect(raw).toContain('Hi there,');
    expect(raw).toContain('for your appointment');
  });

  it('loadTemplateOverride() with no args → null (default-param branch)', async () => {
    expect(await email.loadTemplateOverride()).toBeNull();
  });

  it('a nameless error in the loader logs the generic fallback', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-6' });
    const warns = [];
    await email.sendConfirmationEmail(ARGS, {
      loadTemplateOverride: async () => { const e = new Error('x'); e.name = ''; throw e; },
      log: { warn: (m) => warns.push(m) },
    });
    expect(warns[0]).toContain(': error (using default copy)');
  });
});
