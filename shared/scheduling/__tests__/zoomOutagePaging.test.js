'use strict';

/**
 * Unit tests for zoomOutagePaging.js (WS-E-ATTEND C13).
 *
 * Covers: zoom-conference detection (provider field + zoom.us join URL); the T-15
 * unreachable branch (coordinator page via internal SMS + consent-gated volunteer fallback);
 * reachable → no page; probe-throws → treated as unreachable; non-zoom skip; missing phones.
 */

const {
  pageCoordinatorOnZoomOutage,
  isZoomConference,
  failClosedSelectChannels,
} = require('../zoomOutagePaging');

const TENANT = 'AUS123957';
const quietLog = () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() });

const zoomBooking = {
  tenant_id: TENANT,
  booking_id: 'bk-1',
  conference_provider: 'zoom',
  coordinator_phone: '+15125550000',
  attendee_phone: '+15125551234',
  attendee_name: 'Sam Patel',
};

function deps(over = {}) {
  return {
    checkZoomReachable: jest.fn().mockResolvedValue(false), // unreachable
    sendSms: jest.fn().mockResolvedValue(undefined),
    selectChannels: jest.fn().mockResolvedValue({ email: true, sms: true }),
    log: quietLog(),
    now: 1000,
    ...over,
  };
}

describe('isZoomConference', () => {
  test('detects provider field', () => {
    expect(isZoomConference({ conference_provider: 'zoom' })).toBe(true);
    expect(isZoomConference({ conferenceProvider: 'ZOOM' })).toBe(true);
  });
  test('detects zoom.us join URL', () => {
    expect(isZoomConference({ join_url: 'https://us02web.zoom.us/j/123' })).toBe(true);
    expect(isZoomConference({ channel_details: 'meet at https://zoom.us/j/9' })).toBe(true);
  });
  test('false for non-zoom / empty', () => {
    expect(isZoomConference({ conference_provider: 'google_meet', join_url: 'https://meet.google.com/x' })).toBe(false);
    expect(isZoomConference({})).toBe(false);
  });
});

describe('failClosedSelectChannels', () => {
  test('email floor only', async () => {
    await expect(failClosedSelectChannels()).resolves.toEqual({ email: true, sms: false });
  });
});

describe('pageCoordinatorOnZoomOutage', () => {
  test('non-zoom booking → skipped_not_zoom', async () => {
    const d = deps();
    const r = await pageCoordinatorOnZoomOutage({ booking: { ...zoomBooking, conference_provider: 'google_meet', join_url: 'https://meet.google.com/x' }, deps: d });
    expect(r.outcome).toBe('skipped_not_zoom');
    expect(d.sendSms).not.toHaveBeenCalled();
  });

  test('zoom reachable → zoom_ok, no page', async () => {
    const d = deps({ checkZoomReachable: jest.fn().mockResolvedValue(true) });
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.outcome).toBe('zoom_ok');
    expect(d.sendSms).not.toHaveBeenCalled();
  });

  test('unreachable → pages coordinator (internal) + volunteer fallback (contact, consent)', async () => {
    const d = deps();
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.outcome).toBe('paged');
    expect(d.sendSms).toHaveBeenCalledTimes(2);
    const coordCall = d.sendSms.mock.calls.find((c) => c[0].sendType === 'internal')[0];
    expect(coordCall.to).toBe('+15125550000');
    expect(coordCall.body).toContain('Sam Patel'); // volunteer contact info to staff
    const volCall = d.sendSms.mock.calls.find((c) => c[0].sendType === 'contact')[0];
    expect(volCall.to).toBe('+15125551234');
    expect(volCall.body).toContain('STOP'); // opt-out text on contact SMS
    // urgent flag passed to selectChannels (quiet-hours bypass seam)
    expect(d.selectChannels.mock.calls[0][0].urgent).toBe(true);
    expect(r.dispatched).toEqual({ coordinator_sms: 'sent', volunteer_sms: 'sent' });
  });

  test('probe throws → treated as unreachable (fail-toward-paging)', async () => {
    const d = deps({ checkZoomReachable: jest.fn().mockRejectedValue(new Error('zoom api 500')) });
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.outcome).toBe('paged');
    expect(r.dispatched.coordinator_sms).toBe('sent');
  });

  test('volunteer has no SMS consent → suppressed (coordinator still paged)', async () => {
    const d = deps({ selectChannels: jest.fn().mockResolvedValue({ email: true, sms: false }) });
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.dispatched.coordinator_sms).toBe('sent');
    expect(r.dispatched.volunteer_sms).toBe('suppressed_no_consent');
  });

  test('selectChannels failure → volunteer fallback suppressed, coordinator paged', async () => {
    const d = deps({ selectChannels: jest.fn().mockRejectedValue(new Error('tcpa down')) });
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.dispatched.volunteer_sms).toBe('suppressed_no_consent');
  });

  test('default fail-closed selectChannels when dep omitted → volunteer suppressed', async () => {
    const d = deps();
    delete d.selectChannels;
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.dispatched.volunteer_sms).toBe('suppressed_no_consent');
  });

  test('missing phones → skipped_no_phone on each side', async () => {
    const d = deps();
    const r = await pageCoordinatorOnZoomOutage({ booking: { ...zoomBooking, coordinator_phone: undefined, attendee_phone: undefined }, deps: d });
    expect(r.dispatched).toEqual({ coordinator_sms: 'skipped_no_phone', volunteer_sms: 'skipped_no_phone' });
    expect(d.sendSms).not.toHaveBeenCalled();
  });

  test('SMS send failures are caught (best-effort)', async () => {
    const d = deps({ sendSms: jest.fn().mockRejectedValue(new Error('telnyx')) });
    const r = await pageCoordinatorOnZoomOutage({ booking: zoomBooking, deps: d });
    expect(r.dispatched.coordinator_sms).toBe('failed');
    expect(r.dispatched.volunteer_sms).toBe('failed');
  });

  test('coordinator page omits volunteer phone gracefully when absent', async () => {
    const d = deps();
    const r = await pageCoordinatorOnZoomOutage({ booking: { ...zoomBooking, attendee_phone: undefined, attendee_name: undefined }, deps: d });
    const coordCall = d.sendSms.mock.calls.find((c) => c[0].sendType === 'internal')[0];
    expect(coordCall.body).toContain('on file'); // fallback contact bits
    expect(r.dispatched.volunteer_sms).toBe('skipped_no_phone');
  });
});
