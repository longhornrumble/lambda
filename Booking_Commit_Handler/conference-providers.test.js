'use strict';

/**
 * Unit tests for conference-providers.js — the §5.2-item-4 / §B6 ConferenceProvider
 * seam. The Zoom client is INJECTED (a double) so no network is touched.
 */

const {
  ConferenceProvider,
  GoogleMeetProvider,
  ZoomProvider,
  NullConferenceProvider,
  resolveProvider,
  meetRequestId,
} = require('./conference-providers');

const CTX = {
  tenantId: 'MYR384719', coordinatorId: 'maya@org.org', bookingId: 'booking#abc',
  topic: 'Intake', start: '2026-06-03T18:00:00Z', end: '2026-06-03T18:30:00Z', timezone: 'UTC',
  attendeeEmail: 'sam@example.com',
};

describe('ConferenceProvider base', () => {
  it('createConference is abstract', async () => {
    await expect(new ConferenceProvider().createConference({})).rejects.toThrow(/must be implemented/);
  });
});

describe('NullConferenceProvider — interface-seam verification', () => {
  it('returns synthetic ids and makes NO external call', async () => {
    const conf = await new NullConferenceProvider().createConference(CTX);
    expect(conf.provider).toBe('null');
    expect(conf.conferenceId).toBe('null-conf-booking#abc');
    expect(conf.deferToCalendarInsert).toBe(false);
    expect(conf.joinUrl).toContain('conference.invalid');
  });

  it('falls back to "unknown" bookingId when ctx is empty', async () => {
    const conf = await new NullConferenceProvider().createConference({});
    expect(conf.conferenceId).toBe('null-conf-unknown');
  });
});

describe('GoogleMeetProvider — defers to events.insert with a deterministic requestId', () => {
  it('returns a createRequest plan, no join URL yet (minted by the insert)', async () => {
    const conf = await new GoogleMeetProvider().createConference(CTX);
    expect(conf.provider).toBe('google_meet');
    expect(conf.deferToCalendarInsert).toBe(true);
    expect(conf.joinUrl).toBeNull();
    expect(conf.calendarCreateRequest.conferenceSolutionKey).toEqual({ type: 'hangoutsMeet' });
  });

  it('requestId is deterministic from bookingId (Google-native idempotency)', () => {
    expect(meetRequestId('booking#abc')).toBe(meetRequestId('booking#abc'));
    expect(meetRequestId('booking#abc')).not.toBe(meetRequestId('booking#def'));
  });

  it('throws without a bookingId', async () => {
    await expect(new GoogleMeetProvider().createConference({})).rejects.toThrow(/bookingId/);
  });
});

describe('ZoomProvider — read-before-write passthrough', () => {
  it('mints a meeting on the happy path', async () => {
    const client = { createMeeting: jest.fn().mockResolvedValue({ meetingId: '12345', joinUrl: 'https://zoom.us/j/12345' }) };
    const conf = await new ZoomProvider(client).createConference(CTX);
    expect(conf).toMatchObject({ provider: 'zoom', conferenceId: '12345', joinUrl: 'https://zoom.us/j/12345', deferToCalendarInsert: false });
    expect(client.createMeeting).toHaveBeenCalledWith(expect.objectContaining({ existingMeetingId: undefined }));
  });

  it('passes existingConferenceId through (so the client reuses, no duplicate)', async () => {
    const client = { createMeeting: jest.fn().mockResolvedValue({ meetingId: '55', joinUrl: 'https://zoom.us/j/55' }) };
    await new ZoomProvider(client).createConference({ ...CTX, existingConferenceId: '55' });
    expect(client.createMeeting).toHaveBeenCalledWith(expect.objectContaining({ existingMeetingId: '55' }));
  });

  it('requires tenantId + coordinatorId', async () => {
    await expect(new ZoomProvider({}).createConference({ bookingId: 'b' })).rejects.toThrow(/tenantId and coordinatorId/);
  });
});

describe('resolveProvider — factory + DI override', () => {
  it('resolves each concrete type', () => {
    expect(resolveProvider('google_meet')).toBeInstanceOf(GoogleMeetProvider);
    expect(resolveProvider('zoom')).toBeInstanceOf(ZoomProvider);
    expect(resolveProvider('null')).toBeInstanceOf(NullConferenceProvider);
  });
  it('honors an injected override (the NullConferenceProvider DI seam)', () => {
    const nullProvider = new NullConferenceProvider();
    expect(resolveProvider('zoom', { zoom: nullProvider })).toBe(nullProvider);
  });
  it('injects a zoom client double into ZoomProvider', async () => {
    const client = { createMeeting: jest.fn().mockResolvedValue({ meetingId: '1', joinUrl: 'u' }) };
    const provider = resolveProvider('zoom', { zoomClient: client });
    await provider.createConference(CTX);
    expect(client.createMeeting).toHaveBeenCalled();
  });
  it('throws on an unknown type', () => {
    expect(() => resolveProvider('teams')).toThrow(/unknown conference_type/);
  });
});
