'use strict';

const { handleSchedulingMutate } = require('../scheduling-mutate');

// Minimal fakes for the injected seam (no real Google/Zoom/DDB).
function baseInjected(overrides = {}) {
  const calls = { reschedule: [], cancel: [], zoom: [], persist: [], facade: [], token: [], notify: [], cancelReason: [], cooldown: [], rebind: [], actionLinks: [] };
  const calendarEvents = {
    buildEventBody: (x) => ({ built: x }),
    insertEvent: (calId, body) => { calls.facade.push(['insert', calId]); return { id: 'evt-new' }; },
    deleteEvent: (calId, eid) => { calls.facade.push(['delete', calId, eid]); },
    extractMeetJoinUrl: () => 'https://meet',
  };
  return {
    calls,
    injected: {
      calendarEvents,
      getOAuthClient: async ({ tenantId, coordinatorId }) => { calls.facade.push(['auth', tenantId, coordinatorId]); return { auth: true }; },
      resolveProvider: () => ({ createConference: async () => ({ provider: 'google_meet' }) }),
      zoomClient: { updateMeeting: async (a) => { calls.zoom.push(a); } },
      bookingStore: {
        updateBookingReschedule: async (t, id, f) => { calls.persist.push([t, id, f]); },
        updateBookingCancelReason: async (t, id, f) => { calls.cancelReason.push([t, id, f]); },
        touchRescheduleLinkSentAt: async (t, id, cd) => { calls.cooldown.push([t, id, cd]); return true; },
      },
      executeReschedule: async ({ booking, newSlot }) => { calls.reschedule.push({ booking, newSlot }); return { outcome: 'success', booking: { ...booking, external_event_id: 'evt-new', start_at: newSlot.start } }; },
      executeCancel: async ({ booking }) => { calls.cancel.push({ booking }); return { outcome: 'deleted', booking }; },
      // G6 reschedule_link seams.
      signRescheduleToken: async (purpose, claims) => { calls.token.push([purpose, claims]); return 'mock.jwt.token'; },
      dispatchVolunteerNotice: async ({ kind, tenantId, booking, channels }) => { calls.notify.push({ kind, tenantId, booking, channels }); return { kind, dispatched: { email: 'sent' } }; },
      // G7b SMS-gate seams (deterministic fakes; real channels.js is covered by channels.test.js).
      // Default: no SMS unless a test opts in (consent present + a fake that honors it).
      selectChannels: ({ orgSmsEnabled, consentRecord }) => ({ email: true, sms: orgSmsEnabled === true && !!consentRecord }),
      readSmsConsent: async () => null,
      // Track 1: rebind reminders on a successful reschedule (deterministic fake; the real
      // scheduler is covered by Reminder_Scheduler's own suite).
      rebindReminders: async (args) => { calls.rebind.push(args); return { reminders: [], attendance: null, tiers: [] }; },
      // G1: mint the rebound reminders' action links for the NEW start_at (deterministic fake;
      // real buildActionLinks is covered by confirmation-email.test.js).
      buildActionLinks: async (claims) => {
        calls.actionLinks.push(claims);
        return { rescheduleUrl: 'https://schedule.myrecruiter.ai/reschedule?t=RT', cancelUrl: 'https://schedule.myrecruiter.ai/cancel?t=CT' };
      },
      logger: { warn: () => {}, error: () => {} },
      ...overrides,
    },
  };
}

const RESCH_EVENT = {
  action: 'scheduling_mutate', mutation: 'reschedule',
  tenantId: 'T1', coordinatorId: 'coord@x.com', bookingId: 'bk1',
  // S1.1: the executor projection now carries attendee_phone + organization_name (+ the
  // already-projected attendee_name / appointment_type_name / timezone) so a rebind snapshots
  // real reminder data (phone → SMS supplement; org/appt → real copy).
  booking: { booking_id: 'bk1', tenant_id: 'T1', coordinator_email: 'coord@x.com', external_event_id: 'evt-old',
    attendee_phone: '+15125550199', attendee_name: 'Sam Patel', organization_name: 'Austin Angels',
    appointment_type_name: 'Volunteer intake', timezone: 'America/Chicago', channel_details: 'https://meet.example/x' },
  newSlot: { start: '2026-07-01T15:00:00Z', end: '2026-07-01T15:30:00Z' },
};

describe('handleSchedulingMutate — validation', () => {
  it('fails on missing required fields', async () => {
    expect((await handleSchedulingMutate({ mutation: 'cancel' })).outcome).toBe('failed');
    expect((await handleSchedulingMutate({ action: 'scheduling_mutate', tenantId: 'T1' })).outcome).toBe('failed');
  });
  it('fails on unknown mutation', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingMutate({ mutation: 'frobnicate', tenantId: 'T1', coordinatorId: 'c', booking: {} }, injected);
    expect(out).toEqual({ outcome: 'failed', error: 'unknown_mutation' });
  });
  it('reschedule fails on missing newSlot', async () => {
    const { injected } = baseInjected();
    const out = await handleSchedulingMutate({ ...RESCH_EVENT, newSlot: undefined }, injected);
    expect(out.outcome).toBe('failed');
  });
  it('SR-1: refuses a cross-tenant payload (event.tenantId != booking.tenant_id)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(
      { ...RESCH_EVENT, tenantId: 'T1', booking: { ...RESCH_EVENT.booking, tenant_id: 'T2' } },
      injected
    );
    expect(out).toEqual({ outcome: 'failed', error: 'tenant_mismatch' });
    expect(calls.reschedule).toHaveLength(0); // never reached the calendar op
  });
});

describe('handleSchedulingMutate — cancel', () => {
  it('runs executeCancel with the auth-curried facade and does NOT persist status (listener owns it)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', bookingId: 'bk1', booking: { booking_id: 'bk1', tenant_id: 'T1' } },
      injected
    );
    expect(out.outcome).toBe('deleted');
    expect(calls.cancel).toHaveLength(1);
    expect(calls.persist).toHaveLength(0); // §14.2 listener flips status; executor doesn't write it
    expect(calls.cancelReason).toHaveLength(0); // no reason supplied → no audit write
  });

  it('G6: persists cancel_reason + canceled_by when a reason is supplied (audit-only, not the status flip)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', bookingId: 'bk1',
        booking: { booking_id: 'bk1', tenant_id: 'T1' }, reason: 'Volunteer requested', canceled_by: 'admin@org.com' },
      injected
    );
    expect(out.outcome).toBe('deleted');
    expect(calls.cancelReason).toHaveLength(1);
    const [t, id, fields] = calls.cancelReason[0];
    expect(t).toBe('T1');
    expect(id).toBe('bk1');
    expect(fields).toEqual({ reason: 'Volunteer requested', canceledBy: 'admin@org.com' });
  });

  it('G6: a cancel_reason persist failure is non-fatal (the calendar delete already succeeded)', async () => {
    const { injected } = baseInjected({
      bookingStore: {
        updateBookingReschedule: async () => {},
        updateBookingCancelReason: async () => { throw new Error('ddb down'); },
      },
    });
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', bookingId: 'bk1',
        booking: { booking_id: 'bk1', tenant_id: 'T1' }, reason: 'x' },
      injected
    );
    expect(out.outcome).toBe('deleted'); // swallowed; cancel still reported success
  });

  it('STRICT tenant guard: a booking row MISSING tenantId/tenant_id is refused (not silently allowed)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', bookingId: 'bk1', booking: { booking_id: 'bk1' } },
      injected
    );
    expect(out).toEqual({ outcome: 'failed', error: 'tenant_mismatch' });
    expect(calls.cancel).toHaveLength(0); // never reached the calendar op
  });
});

describe('handleSchedulingMutate — reschedule_link (G6, notify-only)', () => {
  const RL_EVENT = {
    action: 'scheduling_mutate', mutation: 'reschedule_link',
    tenantId: 'T1', coordinatorId: 'coord@x.com', bookingId: 'bk1',
    booking: { booking_id: 'bk1', tenant_id: 'T1', start_at: '2026-07-01T15:00:00Z', attendee_email: 'guest@x.com' },
  };

  it('mints a §B4 reschedule token and emails the guest the self-serve link (no calendar op)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(RL_EVENT, injected);
    expect(out).toEqual({ outcome: 'success', sent: true });
    // minted with the right purpose + claims
    expect(calls.token).toHaveLength(1);
    expect(calls.token[0][0]).toBe('reschedule');
    expect(calls.token[0][1]).toMatchObject({ tenant_id: 'T1', booking_id: 'bk1', start_at: '2026-07-01T15:00:00Z' });
    // notified with kind + a booking carrying the freshly-built rescheduleUrl
    expect(calls.notify).toHaveLength(1);
    expect(calls.notify[0].kind).toBe('reschedule_link');
    expect(calls.notify[0].booking.rescheduleUrl).toMatch(/\/reschedule\?t=mock\.jwt\.token$/);
    // NEVER builds the calendar facade for a notify-only action
    expect(calls.facade).toHaveLength(0);
    // claimed the anti-bombing cooldown slot BEFORE minting (60s window — a change to 0/3600
    // would silently weaken/strengthen the email-bombing guard, so pin it).
    expect(calls.cooldown).toHaveLength(1);
    expect(calls.cooldown[0][0]).toBe('T1');
    expect(calls.cooldown[0][1]).toBe('bk1');
    expect(calls.cooldown[0][2]).toBe(60);
  });

  it('passes the booking cancellation_window_hours into the token claims (matches disposition.js)', async () => {
    const { injected, calls } = baseInjected();
    await handleSchedulingMutate(
      { ...RL_EVENT, booking: { ...RL_EVENT.booking, cancellation_window_hours: 24 } },
      injected
    );
    expect(calls.token[0][1]).toMatchObject({ booking_id: 'bk1', cancellation_window_hours: 24 });
  });

  it('RATE-LIMITED: a repeat send within the cooldown is refused WITHOUT minting or notifying', async () => {
    const { injected, calls } = baseInjected({
      bookingStore: {
        updateBookingReschedule: async () => {},
        updateBookingCancelReason: async () => {},
        touchRescheduleLinkSentAt: async () => false, // within cooldown
      },
    });
    const out = await handleSchedulingMutate(RL_EVENT, injected);
    expect(out).toEqual({ outcome: 'rate_limited' });
    expect(calls.token).toHaveLength(0); // never minted a fresh token
    expect(calls.notify).toHaveLength(0); // never emailed
  });

  it('a cooldown-write failure folds to a clean failed (no token, no email, no FunctionError)', async () => {
    const { injected, calls } = baseInjected({
      bookingStore: {
        updateBookingReschedule: async () => {},
        updateBookingCancelReason: async () => {},
        touchRescheduleLinkSentAt: async () => { throw new Error('ddb down'); },
      },
    });
    const out = await handleSchedulingMutate(RL_EVENT, injected);
    expect(out).toEqual({ outcome: 'failed', error: 'cooldown_write_failed' });
    expect(calls.token).toHaveLength(0);
  });

  it('a token-mint failure returns token_mint_failed and never notifies', async () => {
    const { injected, calls } = baseInjected({
      signRescheduleToken: async () => { throw new Error('missing start_at'); },
    });
    const out = await handleSchedulingMutate(RL_EVENT, injected);
    expect(out).toEqual({ outcome: 'failed', error: 'token_mint_failed' });
    expect(calls.notify).toHaveLength(0);
  });

  it('reports sent:false when the notice dispatch did not send (best-effort)', async () => {
    const { injected } = baseInjected({
      dispatchVolunteerNotice: async () => ({ kind: 'reschedule_link', dispatched: { email: 'skipped_no_recipient' } }),
    });
    const out = await handleSchedulingMutate(RL_EVENT, injected);
    expect(out).toEqual({ outcome: 'success', sent: false });
  });

  it('a thrown dispatch folds to sent:false (token already minted; no FunctionError)', async () => {
    const { injected } = baseInjected({
      dispatchVolunteerNotice: async () => { throw new Error('lambda invoke 500'); },
    });
    const out = await handleSchedulingMutate(RL_EVENT, injected);
    expect(out).toEqual({ outcome: 'success', sent: false });
  });

  // ── G7b: SMS supplement channel gating (email is always the floor) ──────────────────
  const RL_SMS_EVENT = {
    ...RL_EVENT,
    org_sms_enabled: true,
    booking: { ...RL_EVENT.booking, attendee_phone: '+15125551234', timezone: 'America/Chicago' },
  };

  it('org SMS on + live consent → channels.sms:true is passed to notify', async () => {
    let consentReads = 0;
    const { injected, calls } = baseInjected({
      readSmsConsent: async (t, p) => { consentReads++; return { consent_given: true }; },
    });
    const out = await handleSchedulingMutate(RL_SMS_EVENT, injected);
    expect(out.outcome).toBe('success');
    expect(consentReads).toBe(1);
    expect(calls.notify[0].channels).toEqual({ email: true, sms: true });
  });

  it('org SMS on but NO consent record → channels.sms:false (email floor stands)', async () => {
    const { injected, calls } = baseInjected({ readSmsConsent: async () => null });
    await handleSchedulingMutate(RL_SMS_EVENT, injected);
    expect(calls.notify[0].channels).toEqual({ email: true, sms: false });
  });

  it('org SMS OFF → never reads consent, channels.sms:false', async () => {
    let consentReads = 0;
    const { injected, calls } = baseInjected({
      readSmsConsent: async () => { consentReads++; return { consent_given: true }; },
    });
    await handleSchedulingMutate({ ...RL_SMS_EVENT, org_sms_enabled: false }, injected);
    expect(consentReads).toBe(0);
    expect(calls.notify[0].channels).toEqual({ email: true, sms: false });
  });

  it('org_sms_enabled truthy-non-bool ("yes"/1) does NOT enable SMS (strict === true gate)', async () => {
    for (const truthy of ['yes', 'true', 1]) {
      const { injected, calls } = baseInjected({
        readSmsConsent: async () => ({ consent_given: true }),
      });
      await handleSchedulingMutate({ ...RL_SMS_EVENT, org_sms_enabled: truthy }, injected);
      expect(calls.notify[0].channels).toEqual({ email: true, sms: false });
    }
  });

  it('org SMS on but no phone on the booking → no consent read, channels.sms:false', async () => {
    let consentReads = 0;
    const { injected, calls } = baseInjected({
      readSmsConsent: async () => { consentReads++; return { consent_given: true }; },
    });
    await handleSchedulingMutate(
      { ...RL_SMS_EVENT, booking: { ...RL_EVENT.booking } }, // no attendee_phone
      injected
    );
    expect(consentReads).toBe(0);
    expect(calls.notify[0].channels).toEqual({ email: true, sms: false });
  });

  it('a consent-read throw FAILS CLOSED (sms:false) and still emails the link', async () => {
    const { injected, calls } = baseInjected({
      readSmsConsent: async () => { throw new Error('ddb down'); },
    });
    const out = await handleSchedulingMutate(RL_SMS_EVENT, injected);
    expect(out).toEqual({ outcome: 'success', sent: true });
    expect(calls.notify[0].channels).toEqual({ email: true, sms: false });
  });

  it('a selectChannels throw FAILS CLOSED (sms:false)', async () => {
    const { injected, calls } = baseInjected({
      readSmsConsent: async () => ({ consent_given: true }),
      selectChannels: () => { throw new Error('boom'); },
    });
    await handleSchedulingMutate(RL_SMS_EVENT, injected);
    expect(calls.notify[0].channels).toEqual({ email: true, sms: false });
  });

  it('sent:true when ONLY the SMS supplement delivered (email skipped)', async () => {
    const { injected } = baseInjected({
      readSmsConsent: async () => ({ consent_given: true }),
      dispatchVolunteerNotice: async ({ kind }) => ({ kind, dispatched: { sms: 'sent' } }),
    });
    const out = await handleSchedulingMutate(RL_SMS_EVENT, injected);
    expect(out).toEqual({ outcome: 'success', sent: true });
  });
});

describe('handleSchedulingMutate — reschedule', () => {
  it('runs executeReschedule then persists the new fields (option A)', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('success');
    expect(calls.reschedule).toHaveLength(1);
    expect(calls.persist).toHaveLength(1);
    const [t, id, fields] = calls.persist[0];
    expect(t).toBe('T1');
    expect(id).toBe('bk1');
    expect(fields.startAt).toBe(RESCH_EVENT.newSlot.start);
    expect(fields.externalEventId).toBe('evt-new');
    expect(fields.pendingCalendarSync).toBe(false);
  });

  it('does NOT call zoom.updateMeeting for a non-zoom (google_meet) booking', async () => {
    const { injected, calls } = baseInjected(); // RESCH_EVENT booking has no conference_provider
    await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(calls.zoom).toHaveLength(0);
  });

  it('PATCHes Zoom start-time only for a zoom booking', async () => {
    const { injected, calls } = baseInjected({
      executeReschedule: async ({ booking, newSlot }) => ({ outcome: 'success', booking: { ...booking, conference_provider: 'zoom', conference_id: '99887766', external_event_id: 'evt-new' } }),
    });
    await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(calls.zoom).toHaveLength(1);
    expect(calls.zoom[0].meetingId).toBe('99887766');
  });

  it('does NOT persist on a failed reschedule', async () => {
    const { injected, calls } = baseInjected({
      executeReschedule: async () => ({ outcome: 'failed', booking: RESCH_EVENT.booking }),
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('failed');
    expect(calls.persist).toHaveLength(0);
  });

  it('persist failure is non-fatal (calendar already moved; listener backstops)', async () => {
    const { injected } = baseInjected({
      bookingStore: { updateBookingReschedule: async () => { throw new Error('ddb down'); } },
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('success'); // swallowed
  });
});

describe('handleSchedulingMutate — reschedule reminder rebind (Track 1, §E1)', () => {
  it('rebinds reminders to the NEW slot on a successful reschedule', async () => {
    const { injected, calls } = baseInjected();
    const out = await handleSchedulingMutate({ ...RESCH_EVENT, org_sms_enabled: false }, injected);
    expect(out.outcome).toBe('success');
    expect(calls.rebind).toHaveLength(1);
    const { booking, tenantPrefs } = calls.rebind[0];
    expect(booking.tenant_id).toBe('T1');
    expect(booking.booking_id).toBe('bk1');
    expect(booking.start_at).toBe(RESCH_EVENT.newSlot.start); // the NEW time, not the old
    expect(booking.end_at).toBe(RESCH_EVENT.newSlot.end);
    // S1.1: real reminder data carried (phone for the SMS supplement; org/appt/name for copy)
    expect(booking.attendee_phone).toBe('+15125550199');
    expect(booking.organization_name).toBe('Austin Angels');
    expect(booking.appointment_type_name).toBe('Volunteer intake');
    expect(booking.attendee_name).toBe('Sam Patel');
    expect(booking.timezone).toBe('America/Chicago');
    expect(tenantPrefs).toEqual({ notificationPrefs: { sms: false }, sms_quiet_hours: null });
    // G1: fresh action links minted for the NEW start_at + threaded into the rebind; join link
    // (the reused conference, preserved by reschedule.js) rides the rebind view.
    expect(calls.rebind[0].rescheduleUrl).toContain('/reschedule?t=');
    expect(calls.rebind[0].cancelUrl).toContain('/cancel?t=');
    expect(booking.join_url).toBe('https://meet.example/x');
    expect(calls.actionLinks[0].startAt).toBe(RESCH_EVENT.newSlot.start);
  });

  it('truthy-non-bool org_sms_enabled ("yes"/1/"true") does NOT enable SMS on rebind (strict === true)', async () => {
    for (const truthy of ['yes', 'true', 1]) {
      const { injected, calls } = baseInjected();
      await handleSchedulingMutate({ ...RESCH_EVENT, org_sms_enabled: truthy }, injected);
      expect(calls.rebind[0].tenantPrefs.notificationPrefs.sms).toBe(false);
    }
  });

  it('passes org SMS = true + sms_quiet_hours into tenantPrefs from the event', async () => {
    const { injected, calls } = baseInjected();
    await handleSchedulingMutate(
      { ...RESCH_EVENT, org_sms_enabled: true, sms_quiet_hours: { start: 21, end: 8 } },
      injected
    );
    expect(calls.rebind[0].tenantPrefs).toEqual({
      notificationPrefs: { sms: true },
      sms_quiet_hours: { start: 21, end: 8 },
    });
  });

  it('rebinds on pending_calendar_sync too (the booking time is the new slot)', async () => {
    const { injected, calls } = baseInjected({
      executeReschedule: async ({ booking, newSlot }) => ({ outcome: 'pending_calendar_sync', booking: { ...booking, start_at: newSlot.start } }),
    });
    await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(calls.rebind).toHaveLength(1);
  });

  it('does NOT rebind on a failed reschedule', async () => {
    const { injected, calls } = baseInjected({
      executeReschedule: async () => ({ outcome: 'failed', booking: RESCH_EVENT.booking }),
    });
    await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(calls.rebind).toHaveLength(0);
  });

  it('a rebind failure is non-fatal (the move already happened; listener backstops)', async () => {
    const { injected } = baseInjected({
      rebindReminders: async () => { throw new Error('scheduler down'); },
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out.outcome).toBe('success'); // swallowed
  });
});

describe('handleSchedulingMutate — unexpected throw → clean failed (no FunctionError)', () => {
  it('getOAuthClient throwing yields { outcome: failed } (BSH fallback), not a propagated error', async () => {
    const { injected } = baseInjected({
      getOAuthClient: async () => { throw new Error('SecretsManagerAccessDenied'); },
    });
    const out = await handleSchedulingMutate(RESCH_EVENT, injected);
    expect(out).toEqual({ outcome: 'failed', error: 'executor_error' });
  });
  it('a thrown executeCancel also folds to failed (not a rejection)', async () => {
    const { injected } = baseInjected({
      executeCancel: async () => { throw new Error('google 500'); },
    });
    const out = await handleSchedulingMutate(
      { action: 'scheduling_mutate', mutation: 'cancel', tenantId: 'T1', coordinatorId: 'c@x.com', booking: { booking_id: 'bk1', tenant_id: 'T1' } },
      injected
    );
    expect(out.outcome).toBe('failed');
  });
});
