'use strict';

/**
 * Unit tests for calendar-events.js — the C8 write-side calendar wrapper.
 * The @googleapis/calendar surface is mocked; the event-body builder, the §5.7
 * PII-boundary content rules, the booking_id ownership tag, the CR/LF sanitizer,
 * and the OAuth-401 classifier are exercised directly.
 */

const mockInsert = jest.fn();
const mockDelete = jest.fn();

jest.mock('@googleapis/calendar', () => ({
  calendar: () => ({
    events: {
      insert: (...args) => mockInsert(...args),
      delete: (...args) => mockDelete(...args),
    },
  }),
}));

const ce = require('./calendar-events');

beforeEach(() => {
  mockInsert.mockReset();
  mockDelete.mockReset();
});

describe('clean()', () => {
  it('strips CR/LF and control chars, preserves hyphens/punctuation', () => {
    expect(ce.clean('Sam-Patel\r\nBcc: evil@x')).toBe('Sam-Patel Bcc: evil@x');
    expect(ce.clean('Mary-Jo  Watson')).toBe('Mary-Jo Watson');
    expect(ce.clean('  spaced  ')).toBe('spaced');
    expect(ce.clean(null)).toBe('');
    expect(ce.clean(undefined)).toBe('');
  });
});

describe('buildEventBody — §5.7 PII boundary + ownership tag', () => {
  const base = {
    bookingId: 'booking#abc123',
    appointmentTypeName: 'Volunteer intake',
    attendeeFirstName: 'Sam',
    attendeeLastName: 'Patel',
    attendeeEmail: 'sam@example.com',
    start: '2026-06-03T18:00:00.000Z',
    end: '2026-06-03T18:30:00.000Z',
    timezone: 'America/Chicago',
    deepLink: 'https://schedule.myrecruiter.ai/b/booking#abc123',
  };

  it('always sets extendedProperties.private.booking_id (FROZEN §A ownership tag)', () => {
    const body = ce.buildEventBody({ ...base, conference: {} });
    expect(body.extendedProperties.private.booking_id).toBe('booking#abc123');
  });

  it('title carries type + FIRST name only (no last name)', () => {
    const body = ce.buildEventBody({ ...base, conference: {} });
    expect(body.summary).toBe('Volunteer intake — Sam');
    expect(body.summary).not.toContain('Patel');
  });

  it('description carries full name + deep link, never phone/form contents', () => {
    const body = ce.buildEventBody({ ...base, conference: {} });
    expect(body.description).toContain('Sam Patel');
    expect(body.description).toContain('schedule.myrecruiter.ai');
  });

  it('attendee is the volunteer email in the native attendees[] field', () => {
    const body = ce.buildEventBody({ ...base, conference: {} });
    expect(body.attendees).toEqual([{ email: 'sam@example.com' }]);
  });

  it('throws when bookingId is missing (the tag is mandatory)', () => {
    expect(() => ce.buildEventBody({ ...base, bookingId: undefined, conference: {} })).toThrow(/bookingId/);
  });

  it('Google Meet: attaches conferenceData.createRequest, no location', () => {
    const body = ce.buildEventBody({
      ...base,
      conference: { calendarCreateRequest: { requestId: 'meet-xyz', conferenceSolutionKey: { type: 'hangoutsMeet' } } },
    });
    expect(body.conferenceData.createRequest.requestId).toBe('meet-xyz');
    expect(body.location).toBeUndefined();
  });

  it('Zoom/Null: attaches join URL as location + conferenceData entryPoint', () => {
    const body = ce.buildEventBody({
      ...base,
      conference: { provider: 'zoom', conferenceId: '99', joinUrl: 'https://zoom.us/j/99' },
    });
    expect(body.location).toBe('https://zoom.us/j/99');
    expect(body.conferenceData.entryPoints[0].uri).toBe('https://zoom.us/j/99');
    expect(body.conferenceData.conferenceSolution.name).toBe('Zoom');
  });

  it('sanitizes a CRLF/Bcc injection in the name field', () => {
    const body = ce.buildEventBody({
      ...base,
      attendeeFirstName: 'Sam\r\nBcc: evil@x',
      conference: {},
    });
    expect(body.summary).not.toMatch(/[\r\n]/);
    expect(body.summary).toContain('Bcc: evil@x'); // collapsed to one line, not a header
  });
});

describe('insertEvent', () => {
  it('sets conferenceDataVersion=1 when conferenceData present, 0 otherwise', async () => {
    mockInsert.mockResolvedValue({ data: { id: 'evt-1' } });
    await ce.insertEvent({}, 'cal@x', { summary: 's', conferenceData: { createRequest: {} } });
    expect(mockInsert.mock.calls[0][0].conferenceDataVersion).toBe(1);

    mockInsert.mockResolvedValue({ data: { id: 'evt-2' } });
    await ce.insertEvent({}, 'cal@x', { summary: 's' });
    expect(mockInsert.mock.calls[1][0].conferenceDataVersion).toBe(0);
  });

  it('returns response.data', async () => {
    mockInsert.mockResolvedValue({ data: { id: 'evt-9', htmlLink: 'x' } });
    const ev = await ce.insertEvent({}, 'cal@x', { summary: 's' });
    expect(ev.id).toBe('evt-9');
  });
});

describe('deleteEvent — idempotent compensation', () => {
  it('swallows 404/410 (already gone)', async () => {
    mockDelete.mockRejectedValueOnce({ code: 404 });
    await expect(ce.deleteEvent({}, 'cal', 'evt')).resolves.toBeUndefined();
    mockDelete.mockRejectedValueOnce({ response: { status: 410 } });
    await expect(ce.deleteEvent({}, 'cal', 'evt')).resolves.toBeUndefined();
  });

  it('rethrows other errors', async () => {
    mockDelete.mockRejectedValueOnce({ code: 500 });
    await expect(ce.deleteEvent({}, 'cal', 'evt')).rejects.toBeDefined();
  });
});

describe('extractMeetJoinUrl', () => {
  it('pulls the video entryPoint URI', () => {
    const url = ce.extractMeetJoinUrl({ conferenceData: { entryPoints: [{ entryPointType: 'video', uri: 'https://meet.google.com/abc' }] } });
    expect(url).toBe('https://meet.google.com/abc');
  });
  it('returns null when absent', () => {
    expect(ce.extractMeetJoinUrl({})).toBeNull();
    expect(ce.extractMeetJoinUrl({ conferenceData: { entryPoints: [] } })).toBeNull();
  });
});

describe('classifyAuthError — §5.5 row 4 transient vs permanent', () => {
  it('invalid_grant ⇒ permanent (degrade)', () => {
    expect(ce.classifyAuthError({ response: { data: { error: 'invalid_grant' } } })).toEqual({ isAuth: true, permanent: true });
  });
  it('"Token has been expired or revoked" message ⇒ permanent', () => {
    expect(ce.classifyAuthError({ message: 'Token has been expired or revoked.' })).toEqual({ isAuth: true, permanent: true });
  });
  it('plain 401 ⇒ transient (refresh+retry)', () => {
    expect(ce.classifyAuthError({ code: 401 })).toEqual({ isAuth: true, permanent: false });
    expect(ce.classifyAuthError({ response: { status: 401 } })).toEqual({ isAuth: true, permanent: false });
  });
  it('non-auth error ⇒ not auth', () => {
    expect(ce.classifyAuthError({ code: 500 })).toEqual({ isAuth: false, permanent: false });
  });
});
