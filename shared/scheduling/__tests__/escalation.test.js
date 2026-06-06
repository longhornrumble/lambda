'use strict';

/**
 * Unit tests for escalation.js (WS-E-ATTEND E10).
 *
 * Covers: the stop-when-resolved gate (visibility-before-state-change, §11.2); t24h resend
 * + admin cc; t72h urgent admin email + Customer-Portal inbox alert; t7d weekly digest
 * (oldest-first, daysPending, empty/no-recipient branches); safeAdminEmails resilience.
 */

const {
  escalateSilence,
  buildWeeklyDigest,
  isUnresolved,
  safeAdminEmails,
  ATTENDANCE_STATE_PENDING,
} = require('../escalation');

const BASE = 'https://schedule.myrecruiter.ai';
const TENANT = 'AUS123957';

function fakeSign(purpose, claims) {
  return `tok.${purpose}.${claims.booking_id}`;
}
const quietLog = () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() });

const unresolved = {
  tenant_id: TENANT,
  booking_id: 'bk-1',
  status: 'booked',
  attendance_state: 'pending_attendance',
  coordinator_email: 'maya@org.example',
  coordinator_name: 'Maya Lopez',
  attendee_name: 'Sam Patel',
  appointment_type_name: 'intake call',
  when_label: 'Tue, Jun 3 · 2:00 PM',
  start_at: '2026-06-03T14:00:00Z',
  end_at: '2026-06-03T15:00:00Z',
};

function deps(over = {}) {
  return {
    signToken: fakeSign,
    sendEmail: jest.fn().mockResolvedValue(undefined),
    getAdminEmails: jest.fn().mockResolvedValue(['admin@org.example']),
    writePortalInboxAlert: jest.fn().mockResolvedValue(undefined),
    baseUrl: BASE,
    log: quietLog(),
    now: 1000,
    ...over,
  };
}

describe('isUnresolved', () => {
  test('true only when booked AND pending_attendance', () => {
    expect(isUnresolved(unresolved)).toBe(true);
    expect(isUnresolved({ ...unresolved, status: 'completed' })).toBe(false);
    expect(isUnresolved({ ...unresolved, attendance_state: 'resolved' })).toBe(false);
    expect(ATTENDANCE_STATE_PENDING).toBe('pending_attendance');
  });
});

describe('escalateSilence — gating', () => {
  test('unknown tier throws', async () => {
    await expect(escalateSilence({ booking: unresolved, tier: 't9000', deps: deps() })).rejects.toThrow(/unknown tier/);
  });
  test('resolved booking → stopped_resolved (no dispatch)', async () => {
    const d = deps();
    const r = await escalateSilence({ booking: { ...unresolved, status: 'completed' }, tier: 't24h', deps: d });
    expect(r.outcome).toBe('stopped_resolved');
    expect(d.sendEmail).not.toHaveBeenCalled();
  });
});

describe('escalateSilence — t24h resend + admin cc', () => {
  test('sends to coordinator, cc admin, fresh prompt', async () => {
    const d = deps();
    const r = await escalateSilence({ booking: unresolved, tier: 't24h', deps: d });
    expect(r).toMatchObject({ outcome: 'resent', tier: 't24h', adminCc: true, nextTier: 't72h' });
    const arg = d.sendEmail.mock.calls[0][0];
    expect(arg.to).toBe('maya@org.example');
    expect(arg.cc).toEqual(['admin@org.example']);
    expect(arg.subject).toMatch(/^Reminder:/);
    expect(arg.text_body).toContain('/attended/met?t=');
  });
  test('no coordinator email → skipped_no_recipient', async () => {
    const d = deps();
    const r = await escalateSilence({ booking: { ...unresolved, coordinator_email: undefined }, tier: 't24h', deps: d });
    expect(r.dispatched.email).toBe('skipped_no_recipient');
  });
  test('email failure → failed (best-effort)', async () => {
    const d = deps({ sendEmail: jest.fn().mockRejectedValue(new Error('ses')) });
    const r = await escalateSilence({ booking: unresolved, tier: 't24h', deps: d });
    expect(r.dispatched.email).toBe('failed');
  });
  test('no admin recipients → adminCc false', async () => {
    const d = deps({ getAdminEmails: jest.fn().mockResolvedValue([]) });
    const r = await escalateSilence({ booking: unresolved, tier: 't24h', deps: d });
    expect(r.adminCc).toBe(false);
  });
});

describe('escalateSilence — t72h urgent + portal inbox alert', () => {
  test('sends urgent admin email + writes inbox alert', async () => {
    const d = deps();
    const r = await escalateSilence({ booking: unresolved, tier: 't72h', deps: d });
    expect(r).toMatchObject({ outcome: 'urgent', tier: 't72h', portalInboxAlert: true, nextTier: 't7d' });
    expect(d.sendEmail.mock.calls[0][0].to).toEqual(['admin@org.example']);
    expect(d.sendEmail.mock.calls[0][0].subject).toContain('Action needed');
    expect(d.writePortalInboxAlert).toHaveBeenCalledWith({
      tenantId: TENANT,
      bookingId: 'bk-1',
      kind: 'attendance_unresolved',
      createdAt: 1000,
    });
  });
  test('no admin recipients → email skipped, alert still written', async () => {
    const d = deps({ getAdminEmails: jest.fn().mockResolvedValue([]) });
    const r = await escalateSilence({ booking: unresolved, tier: 't72h', deps: d });
    expect(r.dispatched.email).toBe('skipped_no_recipient');
    expect(r.portalInboxAlert).toBe(true);
  });
  test('email failure → failed; missing when_label + coordinator render safely', async () => {
    const d = deps({ sendEmail: jest.fn().mockRejectedValue(new Error('ses')) });
    const r = await escalateSilence({ booking: { ...unresolved, when_label: undefined, coordinator_email: undefined }, tier: 't72h', deps: d });
    expect(r.dispatched.email).toBe('failed');
  });
  test('portal-alert write failure → portalInboxAlert false (best-effort)', async () => {
    const d = deps({ writePortalInboxAlert: jest.fn().mockRejectedValue(new Error('ddb')) });
    const r = await escalateSilence({ booking: unresolved, tier: 't72h', deps: d });
    expect(r.portalInboxAlert).toBe(false);
  });
  test('no writePortalInboxAlert dep → portalInboxAlert false', async () => {
    const d = deps();
    delete d.writePortalInboxAlert;
    const r = await escalateSilence({ booking: unresolved, tier: 't72h', deps: d });
    expect(r.portalInboxAlert).toBe(false);
  });
});

describe('buildWeeklyDigest (t7d)', () => {
  const older = { ...unresolved, booking_id: 'bk-old', start_at: '2026-05-01T10:00:00Z', attendee_name: 'Aaron Older' };
  const newer = { ...unresolved, booking_id: 'bk-new', start_at: '2026-05-20T10:00:00Z', attendee_name: 'Zane Newer' };
  // now = 2026-05-25 in epoch seconds
  const NOW = Math.floor(Date.parse('2026-05-25T00:00:00Z') / 1000);

  test('empty list → skipped_empty, still recurs', async () => {
    const d = deps();
    const r = await buildWeeklyDigest({ tenantId: TENANT, pendingBookings: [], deps: d });
    expect(r).toEqual({ outcome: 'digest', count: 0, recur: true, dispatched: { email: 'skipped_empty' } });
    expect(d.sendEmail).not.toHaveBeenCalled();
  });
  test('no admin recipients → skipped_no_recipient', async () => {
    const d = deps({ getAdminEmails: jest.fn().mockResolvedValue([]) });
    const r = await buildWeeklyDigest({ tenantId: TENANT, pendingBookings: [older], deps: d });
    expect(r.dispatched.email).toBe('skipped_no_recipient');
    expect(r.count).toBe(1);
  });
  test('enumerates oldest-first with daysPending', async () => {
    const d = deps({ now: NOW });
    const r = await buildWeeklyDigest({ tenantId: TENANT, pendingBookings: [newer, older], deps: d });
    expect(r.count).toBe(2);
    const body = d.sendEmail.mock.calls[0][0].text_body;
    // oldest (bk-old, ~24d) listed before newer (bk-new, ~5d) — first name only (§5.7)
    expect(body.indexOf('Aaron')).toBeLessThan(body.indexOf('Zane'));
    expect(body).toContain('23d pending');
    expect(d.sendEmail.mock.calls[0][0].to).toEqual(['admin@org.example']);
  });
  test('row with missing start_at sinks + renders ? days', async () => {
    const noStart = { ...unresolved, booking_id: 'bk-x', start_at: undefined, attendee_name: 'Quinn NoDate' };
    const d = deps({ now: NOW });
    const r = await buildWeeklyDigest({ tenantId: TENANT, pendingBookings: [noStart, older], deps: d });
    const body = d.sendEmail.mock.calls[0][0].text_body;
    expect(body.indexOf('Aaron')).toBeLessThan(body.indexOf('Quinn')); // dated sorts first
    expect(body).toContain('?d pending');
    expect(r.count).toBe(2);
  });
  test('send failure → failed', async () => {
    const d = deps({ sendEmail: jest.fn().mockRejectedValue(new Error('ses')) });
    const r = await buildWeeklyDigest({ tenantId: TENANT, pendingBookings: [older], deps: d });
    expect(r.dispatched.email).toBe('failed');
  });
  test('non-array pendingBookings tolerated → empty', async () => {
    const d = deps();
    const r = await buildWeeklyDigest({ tenantId: TENANT, pendingBookings: undefined, deps: d });
    expect(r.count).toBe(0);
  });
});

describe('module defaults (deps omits baseUrl/log/now)', () => {
  test('escalateSilence t24h uses default baseUrl + console log + real now', async () => {
    const sendEmail = jest.fn().mockResolvedValue(undefined);
    const r = await escalateSilence({
      booking: unresolved,
      tier: 't24h',
      deps: { signToken: fakeSign, sendEmail, getAdminEmails: jest.fn().mockResolvedValue([]) },
    });
    expect(r.outcome).toBe('resent');
    // default REDEMPTION_BASE_URL reached the minted link
    expect(sendEmail.mock.calls[0][0].text_body).toContain('https://schedule.myrecruiter.ai/attended/met');
  });

  test('escalateSilence t72h with default now → createdAt is a real epoch', async () => {
    const writePortalInboxAlert = jest.fn().mockResolvedValue(undefined);
    await escalateSilence({
      booking: unresolved,
      tier: 't72h',
      deps: { sendEmail: jest.fn().mockResolvedValue(undefined), getAdminEmails: jest.fn().mockResolvedValue(['a@x']), writePortalInboxAlert },
    });
    expect(Number.isInteger(writePortalInboxAlert.mock.calls[0][0].createdAt)).toBe(true);
  });

  test('buildWeeklyDigest with default log/now still sends', async () => {
    const sendEmail = jest.fn().mockResolvedValue(undefined);
    const r = await buildWeeklyDigest({
      tenantId: TENANT,
      pendingBookings: [unresolved],
      deps: { sendEmail, getAdminEmails: jest.fn().mockResolvedValue(['a@x']) },
    });
    expect(r.count).toBe(1);
    expect(sendEmail).toHaveBeenCalled();
  });
});

describe('safeAdminEmails', () => {
  test('non-function → []', async () => {
    expect(await safeAdminEmails(undefined, TENANT, quietLog())).toEqual([]);
  });
  test('throwing resolver → []', async () => {
    expect(await safeAdminEmails(jest.fn().mockRejectedValue(new Error('s3')), TENANT, quietLog())).toEqual([]);
  });
  test('non-array result → []', async () => {
    expect(await safeAdminEmails(jest.fn().mockResolvedValue('x'), TENANT, quietLog())).toEqual([]);
  });
  test('filters falsy entries', async () => {
    expect(await safeAdminEmails(jest.fn().mockResolvedValue(['a@x', '', null, 'b@x']), TENANT, quietLog())).toEqual(['a@x', 'b@x']);
  });
});
