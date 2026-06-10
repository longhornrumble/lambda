/**
 * §E14 tenant notification-template overrides — unit coverage.
 *
 * Asserts: merge semantics (override wins only when non-empty), the COMPLIANCE invariant
 * (STOP is always appended even when an override supplies a full body that omits it),
 * dispatch wiring (the loaded override reaches buildEmailPayload), and the FAIL-SAFE
 * (a loader miss/throw → defaults, never blocks the send).
 */

const {
  dispatchVolunteerNotice,
  buildEmailPayload,
  mergeNoticeTemplate,
  defaultLoadTemplateOverride,
  OVERRIDABLE_MOMENTS,
} = require('../notify.js');

const STOP_TEXT = 'reply STOP';

function baseBooking(extra = {}) {
  return {
    attendee_email: 'a@example.com',
    attendee_first_name: 'Sam',
    organization_name: 'Helping Hands',
    appointment_type_name: 'intake',
    reschedule_url: 'https://example.com/r/abc',
    ...extra,
  };
}

// --------------------------------------------------------------------------- #
// mergeNoticeTemplate
// --------------------------------------------------------------------------- #

describe('mergeNoticeTemplate', () => {
  const base = { subject: 'S', text: 'T', html: 'H' };

  test('null/undefined override returns base unchanged', () => {
    expect(mergeNoticeTemplate(base, null)).toBe(base);
    expect(mergeNoticeTemplate(base, undefined)).toBe(base);
  });

  test('non-empty override fields win; others fall back to base', () => {
    expect(mergeNoticeTemplate(base, { subject: 'NEW' })).toEqual({ subject: 'NEW', text: 'T', html: 'H' });
    expect(mergeNoticeTemplate(base, { text: 'NT', html: 'NH' })).toEqual({ subject: 'S', text: 'NT', html: 'NH' });
  });

  test('empty / whitespace / non-string override fields are ignored (keep base)', () => {
    expect(mergeNoticeTemplate(base, { subject: '', text: '   ', html: 42 })).toEqual(base);
  });
});

// --------------------------------------------------------------------------- #
// buildEmailPayload — STOP compliance invariant
// --------------------------------------------------------------------------- #

describe('buildEmailPayload override + STOP invariant', () => {
  test('override subject/body is applied and rendered', () => {
    const p = buildEmailPayload({
      kind: 'reschedule_link',
      booking: baseBooking(),
      templateOverride: { subject: 'Custom for {{org}}', text: 'Hey {{firstName}} — {{actionUrl}}' },
    });
    expect(p.subject).toBe('Custom for Helping Hands');
    expect(p.text_body).toContain('Hey Sam — https://example.com/r/abc');
  });

  test('STOP is appended even when the override body omits it (cannot be removed)', () => {
    const p = buildEmailPayload({
      kind: 'cancel_notice',
      booking: baseBooking(),
      templateOverride: { text: 'No footer here', html: '<p>No footer here</p>' },
    });
    expect(p.text_body).toContain(STOP_TEXT);
    expect(p.html_body).toContain(STOP_TEXT);
  });

  test('default (no override) still carries STOP', () => {
    const p = buildEmailPayload({ kind: 'reschedule_link', booking: baseBooking() });
    expect(p.text_body).toContain(STOP_TEXT);
    expect(p.html_body).toContain(STOP_TEXT);
  });
});

// --------------------------------------------------------------------------- #
// dispatch wiring + fail-safe
// --------------------------------------------------------------------------- #

describe('dispatchVolunteerNotice §E14 wiring', () => {
  test('loaded override reaches the email payload', async () => {
    const sent = [];
    await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId: 'TEN1', booking: baseBooking() },
      {
        invokeEmail: async (p) => { sent.push(p); },
        loadTemplateOverride: async () => ({ subject: 'OVERRIDDEN {{org}}' }),
        log: { info() {}, warn() {}, error() {} },
      }
    );
    expect(sent).toHaveLength(1);
    expect(sent[0].subject).toBe('OVERRIDDEN Helping Hands');
    expect(sent[0].text_body).toContain(STOP_TEXT);
  });

  test('FAIL-SAFE: a throwing loader does not block the send (defaults used)', async () => {
    const sent = [];
    const res = await dispatchVolunteerNotice(
      { kind: 'reschedule_link', tenantId: 'TEN1', booking: baseBooking() },
      {
        invokeEmail: async (p) => { sent.push(p); },
        loadTemplateOverride: async () => { throw new Error('ddb down'); },
        log: { info() {}, warn() {}, error() {} },
      }
    );
    expect(res.dispatched.email).toBe('sent');
    expect(sent[0].subject).toContain('Need a different time?'); // default subject
    expect(sent[0].text_body).toContain(STOP_TEXT);
  });

  test('null override (loader returns null) uses defaults', async () => {
    const sent = [];
    await dispatchVolunteerNotice(
      { kind: 'cancel_notice', tenantId: 'TEN1', booking: baseBooking() },
      {
        invokeEmail: async (p) => { sent.push(p); },
        loadTemplateOverride: async () => null,
        log: { info() {}, warn() {}, error() {} },
      }
    );
    expect(sent[0].subject).toContain('was canceled');
  });
});

// --------------------------------------------------------------------------- #
// defaultLoadTemplateOverride — fail-safe guards (no DDB needed)
// --------------------------------------------------------------------------- #

describe('defaultLoadTemplateOverride guards', () => {
  test('non-overridable kind → null (no lookup)', async () => {
    expect(await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'reengagement' })).toBeNull();
    expect(await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'move_optin_sms' })).toBeNull();
  });

  test('unset table env → null (defaults used)', async () => {
    // SCHED_NOTIF_TEMPLATE_TABLE is unset in the test env → guard returns null before any DDB call.
    expect(await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'reschedule_link' })).toBeNull();
  });

  test('missing tenantId → null', async () => {
    expect(await defaultLoadTemplateOverride({ kind: 'reschedule_link' })).toBeNull();
  });

  test('OVERRIDABLE_MOMENTS is exactly the 3 dispatched notice kinds', () => {
    expect([...OVERRIDABLE_MOMENTS].sort()).toEqual(['cancel_notice', 'reoffer', 'reschedule_link']);
  });
});


// --------------------------------------------------------------------------- #
// phase-completion-audit fixes — STOP-once (B3), all-3-merge, reoffer, SMS-not-loaded
// --------------------------------------------------------------------------- #

describe('STOP appears EXACTLY once (no double-injection)', () => {
  function stopCount(s) { return (s.match(/reply\s+STOP/gi) || []).length; }

  test('override body that already contains "reply STOP" is not double-footed', () => {
    const p = buildEmailPayload({
      kind: 'reschedule_link',
      booking: baseBooking(),
      templateOverride: {
        text: 'Hi — to opt out, reply STOP.',
        html: '<p>Hi — to opt out, reply STOP.</p>',
      },
    });
    expect(stopCount(p.text_body)).toBe(1);
    expect(stopCount(p.html_body)).toBe(1);
  });

  test('default (no override) has exactly one STOP', () => {
    const p = buildEmailPayload({ kind: 'cancel_notice', booking: baseBooking() });
    expect(stopCount(p.text_body)).toBe(1);
    expect(stopCount(p.html_body)).toBe(1);
  });

  test('all three overridable moments keep exactly one STOP under a full body override', () => {
    for (const kind of ['reschedule_link', 'reoffer', 'cancel_notice']) {
      const p = buildEmailPayload({
        kind,
        booking: baseBooking({ reoffer_url: 'https://example.com/o/x' }),
        templateOverride: { text: 'custom body', html: '<p>custom body</p>' },
      });
      expect(stopCount(p.text_body)).toBe(1);
      expect(stopCount(p.html_body)).toBe(1);
      expect(p.text_body).toContain(STOP_TEXT); // still present (cannot be removed)
    }
  });
});

test('mergeNoticeTemplate: all three fields override at once', () => {
  const base = { subject: 'S', text: 'T', html: 'H' };
  expect(mergeNoticeTemplate(base, { subject: 'NS', text: 'NT', html: 'NH' }))
    .toEqual({ subject: 'NS', text: 'NT', html: 'NH' });
});

test('mergeNoticeTemplate: an override with only unknown keys keeps base', () => {
  const base = { subject: 'S', text: 'T', html: 'H' };
  expect(mergeNoticeTemplate(base, { bogus: 'x' })).toEqual(base);
});

test('override loader is loaded ONCE and shared by the email + SMS bodies (G7b)', async () => {
  let loaderCalls = 0;
  await dispatchVolunteerNotice(
    {
      kind: 'reschedule_link',
      tenantId: 'TEN1',
      booking: { ...baseBooking(), attendeePhone: '+15125551234' },
      channels: { email: true, sms: true },
    },
    {
      invokeEmail: async () => undefined,
      invokeSms: async () => undefined,
      loadTemplateOverride: async () => { loaderCalls++; return null; },
      log: { info() {}, warn() {}, error() {} },
    }
  );
  // One shared load feeds both channels — not one per channel.
  expect(loaderCalls).toBe(1);
});
