'use strict';

/**
 * Unit tests for attendance.js (WS-E-ATTEND E5).
 *
 * Covers: the three-token interviewer prompt build (slug→purpose URLs, event_end expiry
 * driver fallbacks, camel/snake reads, PII first-name-only render); runAttendanceCheck
 * idempotency (non-booked skip, already-marked skip), the non-key attendance_state write,
 * email-floor + optional staff SMS dispatch, and best-effort failure handling.
 */

const {
  runAttendanceCheck,
  buildInterviewerPrompt,
  ATTENDANCE_OPTIONS,
  ATTENDANCE_STATE_PENDING,
  escapeHtml,
  firstNameOf,
  pick,
} = require('../attendance');

const BASE = 'https://schedule.myrecruiter.ai';

// Deterministic fake signer: encodes the purpose + claims so assertions can prove the right
// token landed on the right slug, and that event_end/now flowed in.
function fakeSign(purpose, claims, opts) {
  return `tok.${purpose}.${claims.event_end}.${(opts && opts.now) || 'na'}`;
}

const snakeBooking = {
  tenant_id: 'AUS123957',
  booking_id: 'bk-1',
  status: 'booked',
  coordinator_email: 'maya@org.example',
  coordinator_name: 'Maya Lopez',
  attendee_name: 'Sam Patel',
  appointment_type_name: 'intake call',
  when_label: 'Tue, Jun 3 · 2:00 PM',
  end_at: '2026-06-03T15:00:00Z',
  start_at: '2026-06-03T14:00:00Z',
};

const quietLog = () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() });

describe('helpers', () => {
  test('escapeHtml escapes the five entities + tolerates null', () => {
    expect(escapeHtml(`<a href="x">&'`)).toBe('&lt;a href=&quot;x&quot;&gt;&amp;&#39;');
    expect(escapeHtml(null)).toBe('');
  });
  test('firstNameOf returns first token / empty for non-strings', () => {
    expect(firstNameOf('Maya Lopez')).toBe('Maya');
    expect(firstNameOf('  Sam  ')).toBe('Sam');
    expect(firstNameOf(undefined)).toBe('');
  });
  test('pick prefers camel then snake, undefined when absent', () => {
    expect(pick({ a: 1, a_b: 2 }, 'a', 'a_b')).toBe(1);
    expect(pick({ a_b: 2 }, 'a', 'a_b')).toBe(2);
    expect(pick(null, 'a', 'a_b')).toBeUndefined();
  });
});

describe('buildInterviewerPrompt', () => {
  test('mints the three §B4 tokens onto the correct /attended/* slugs', async () => {
    const p = await buildInterviewerPrompt({
      booking: snakeBooking,
      baseUrl: BASE,
      signToken: fakeSign,
      now: 1000,
    });
    expect(p.to).toBe('maya@org.example');
    expect(p.links.attended_yes).toBe(
      `${BASE}/attended/met?t=tok.attended_yes.2026-06-03T15%3A00%3A00Z.1000`
    );
    expect(p.links.no_show).toContain('/attended/noshow?t=tok.no_show.');
    expect(p.links.didnt_connect).toContain('/attended/noconnect?t=tok.didnt_connect.');
    // every option purpose is one of the three interviewer purposes
    expect(ATTENDANCE_OPTIONS.map((o) => o.purpose)).toEqual([
      'attended_yes',
      'no_show',
      'didnt_connect',
    ]);
  });

  test('renders first names only + all three links in text & html bodies', async () => {
    const p = await buildInterviewerPrompt({
      booking: snakeBooking,
      baseUrl: BASE,
      signToken: fakeSign,
      now: 1000,
    });
    expect(p.subject).toContain('Sam'); // volunteer first name
    expect(p.subject).not.toContain('Patel'); // never last name (§5.7)
    expect(p.text_body).toContain('Hi Maya,');
    expect(p.text_body).toContain(p.links.attended_yes);
    expect(p.text_body).toContain(p.links.no_show);
    expect(p.text_body).toContain(p.links.didnt_connect);
    expect(p.html_body).toContain('<a href=');
    expect(p.sms_body).toContain('Sam');
  });

  test('event_end falls back end_at → event_end → start_at', async () => {
    const onlyStart = { ...snakeBooking, end_at: undefined, event_end: undefined };
    const p1 = await buildInterviewerPrompt({ booking: onlyStart, baseUrl: BASE, signToken: fakeSign });
    expect(p1.links.attended_yes).toContain('2026-06-03T14%3A00%3A00Z'); // start_at used

    const withEventEnd = { ...snakeBooking, end_at: undefined, event_end: '2026-06-03T16:00:00Z' };
    const p2 = await buildInterviewerPrompt({ booking: withEventEnd, baseUrl: BASE, signToken: fakeSign });
    expect(p2.links.attended_yes).toContain('2026-06-03T16%3A00%3A00Z');
  });

  test('camelCase booking + missing names render safely', async () => {
    const camel = {
      tenantId: 'T',
      bookingId: 'bk-2',
      coordinatorEmail: 'c@e.x',
      attendeeName: '', // missing → "the volunteer"
      coordinatorName: '', // missing → "Hi,"
      endAt: '2026-01-01T00:00:00Z',
    };
    const p = await buildInterviewerPrompt({ booking: camel, baseUrl: BASE, signToken: fakeSign });
    expect(p.subject).toContain('the volunteer');
    expect(p.text_body.startsWith('Hi,')).toBe(true);
    expect(p.text_body).not.toContain(' on '); // no whenLabel → no " on …" suffix
  });
});

describe('runAttendanceCheck', () => {
  function deps(over = {}) {
    return {
      setAttendanceState: jest.fn().mockResolvedValue(true),
      signToken: fakeSign,
      sendEmail: jest.fn().mockResolvedValue(undefined),
      sendSms: jest.fn().mockResolvedValue(undefined),
      baseUrl: BASE,
      log: quietLog(),
      now: 2000,
      ...over,
    };
  }

  test('non-booked booking → skipped, no write, no send', async () => {
    const d = deps();
    const r = await runAttendanceCheck({
      booking: { ...snakeBooking, status: 'canceled' },
      deps: d,
    });
    expect(r.outcome).toBe('skipped_not_booked');
    expect(d.setAttendanceState).not.toHaveBeenCalled();
    expect(d.sendEmail).not.toHaveBeenCalled();
  });

  test('already-marked (conditional write returns false) → skip re-send', async () => {
    const d = deps({ setAttendanceState: jest.fn().mockResolvedValue(false) });
    const r = await runAttendanceCheck({ booking: snakeBooking, deps: d });
    expect(r.outcome).toBe('skipped_already_marked');
    expect(d.sendEmail).not.toHaveBeenCalled();
  });

  test('happy path: sets pending_attendance, sends email + staff SMS', async () => {
    const d = deps({ booking: snakeBooking });
    const r = await runAttendanceCheck({
      booking: { ...snakeBooking, coordinator_phone: '+15125550000' },
      deps: d,
    });
    expect(r.outcome).toBe('pending_attendance_set');
    expect(d.setAttendanceState).toHaveBeenCalledWith({
      tenantId: 'AUS123957',
      bookingId: 'bk-1',
      now: 2000,
    });
    expect(d.sendEmail).toHaveBeenCalledTimes(1);
    expect(d.sendEmail.mock.calls[0][0].to).toBe('maya@org.example');
    // staff SMS uses sendType:'internal' (bypasses contact consent gate)
    expect(d.sendSms).toHaveBeenCalledTimes(1);
    expect(d.sendSms.mock.calls[0][0].sendType).toBe('internal');
    expect(r.dispatched).toEqual({ email: 'sent', sms: 'sent' });
    expect(ATTENDANCE_STATE_PENDING).toBe('pending_attendance');
  });

  test('no coordinator email → email skipped_no_recipient; no phone → no SMS', async () => {
    const d = deps();
    const r = await runAttendanceCheck({
      booking: { ...snakeBooking, coordinator_email: undefined },
      deps: d,
    });
    expect(r.dispatched.email).toBe('skipped_no_recipient');
    expect(d.sendSms).not.toHaveBeenCalled();
  });

  test('best-effort: email + sms failures are caught, not thrown', async () => {
    const d = deps({
      sendEmail: jest.fn().mockRejectedValue(new Error('ses down')),
      sendSms: jest.fn().mockRejectedValue(new Error('telnyx down')),
    });
    const r = await runAttendanceCheck({
      booking: { ...snakeBooking, coordinator_phone: '+15125550000' },
      deps: d,
    });
    expect(r.outcome).toBe('pending_attendance_set');
    expect(r.dispatched).toEqual({ email: 'failed', sms: 'failed' });
  });

  test('default log (no deps.log) does not throw on happy path', async () => {
    const d = deps();
    delete d.log;
    const r = await runAttendanceCheck({ booking: snakeBooking, deps: d });
    expect(r.outcome).toBe('pending_attendance_set');
  });

  // §11.1 NO auto-completion: the attendance check sets the flow label only — it has NO way to
  // write Booking.status (setAttendanceState is its sole state mutation). A booking nobody
  // dispositions can never be auto-advanced by this path.
  test('NO auto-completion (§11.1): attendance check sets attendance_state only, never a status', async () => {
    const writes = [];
    const d = deps({
      setAttendanceState: jest.fn(async (args) => { writes.push(args); return true; }),
    });
    const r = await runAttendanceCheck({ booking: snakeBooking, deps: d });
    expect(r.outcome).toBe('pending_attendance_set');
    // the only mutation is the attendance_state write — no status field anywhere in the dep call
    expect(writes).toEqual([{ tenantId: 'AUS123957', bookingId: 'bk-1', now: 2000 }]);
    expect(JSON.stringify(writes)).not.toMatch(/status/i);
    // the module exposes no status-writing dep at all
    expect(d.setBookingStatus).toBeUndefined();
  });
});
