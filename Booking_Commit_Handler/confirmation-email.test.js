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
      whenLabel: 'soon', coordinatorName: 'Maya',
      joinUrl: 'https://zoom.us/j/1', rescheduleUrl: 'r', cancelUrl: 'c',
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
      firstName: '', orgName: '', whenLabel: '', coordinatorName: '', joinUrl: '', rescheduleUrl: 'r', cancelUrl: 'c',
    });
    expect(textBody).toContain('Hi there,');
    expect(htmlBody).toContain('Hi there,');
    expect(htmlBody).not.toContain('Join the meeting'); // no joinUrl → no join link
    expect(textBody).toContain('MyRecruiter'); // org fallback
  });

  it('sendConfirmationEmail uses start as startAt fallback when startAt omitted', async () => {
    sesMock.on(SendRawEmailCommand).resolves({ MessageId: 'msg-3' });
    await email.sendConfirmationEmail({ ...ARGS, startAt: undefined, joinUrl: undefined, cancellationWindowHours: undefined }, {});
    expect(sign).toHaveBeenCalledWith('cancel', expect.objectContaining({ start_at: ARGS.start }), undefined);
  });
});
