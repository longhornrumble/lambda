import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

import { dispatch } from './index.mjs';
// The REAL §E3 gate — used in the integration tests below to prove the seam is wired
// correctly end-to-end (not just that a fake's return value is forwarded).
import { selectChannels } from '../shared/scheduling/channels.js';

// ─── recording deps ─────────────────────────────────────────────────────────────────

function makeDeps({ message, selectChannels, now = Date.parse('2026-06-12T11:00:00Z') } = {}) {
  const ddbCalls = [];
  const lambdaCalls = [];
  const ddb = {
    send: async (command) => {
      const name = command.constructor.name;
      ddbCalls.push({ name, input: command.input });
      if (name === 'GetCommand') {
        // The consent-table Get (legacy path) vs the message Get are distinguished by SK.
        if (command.input.Key.sk?.startsWith('CONSENT#')) return { Item: makeDeps._consent };
        return { Item: message };
      }
      return {};
    },
  };
  const lambda = { send: async (command) => { lambdaCalls.push(command.input); } };
  const deps = {
    ddb,
    lambda,
    now: () => now,
    logger: { log() {}, warn() {}, error() {} },
    selectChannels,
  };
  return { deps, ddbCalls, lambdaCalls };
}

// NB: the base fixture has NO `tier` field — it is deliberately the LEGACY row shape
// (reminderMomentFromRow → null, §E14 overrides skipped). The `#t1h` in the sk string is
// the SK encoding only. Don't add tier here; pass it per-test, or pre-S4b coverage shifts.
const reminderRow = (o = {}) => ({
  pk: 'TENANT#AUS123957',
  sk: 'SCHEDULED#2026-06-12T12:00:00Z#booking#1#t1h',
  tenant_id: 'AUS123957',
  channel: 'email',
  recipient_email: 'vol@example.com',
  recipient_phone: '+15125551234',
  subject: 'Appointment reminder',
  body: 'Reminder: your appointment is coming up.',
  template: 'appointment_reminder',
  template_vars: { organization_name: 'Austin Angels' },
  appointment_id: 'booking#1',
  message_id: 'booking#1#t1h',
  status: 'pending',
  moment: 'reminder',
  timezone: 'America/Chicago',
  tenant_prefs: { notificationPrefs: { sms: true }, sms_quiet_hours: { start: 20, end: 8 } },
  ...o,
});

// S2 audit fix: _consent is shared mutable state reset inline by tests — an unexpected
// throw before the reset would poison later tests. Start every test clean.
beforeEach(() => { makeDeps._consent = undefined; });

const EVENT = (m) => ({ pk: m.pk, sk: m.sk, message_id: m.message_id });
const fnNames = (lambdaCalls) => lambdaCalls.map((c) => c.FunctionName);
const lastStatus = (ddbCalls) =>
  [...ddbCalls].reverse().find((c) => c.name === 'UpdateCommand')?.input.ExpressionAttributeValues[':status'];

// ─── email floor ──────────────────────────────────────────────────────────────────────

test('email-floor only when selectChannels is NOT wired (channel=email, SMS fail-closed)', async () => {
  const message = reminderRow();
  const { deps, ddbCalls, lambdaCalls } = makeDeps({ message }); // no selectChannels
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, true);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
  assert.equal(lastStatus(ddbCalls), 'sent');
});

test('email payload is API-Gateway-shaped (body is a JSON string with to/subject)', async () => {
  const message = reminderRow();
  const { deps, lambdaCalls } = makeDeps({ message });
  await dispatch(EVENT(message), deps);
  const emailInvoke = lambdaCalls.find((c) => c.FunctionName === 'send_email');
  const outer = JSON.parse(Buffer.from(emailInvoke.Payload).toString());
  const inner = JSON.parse(outer.body);
  assert.deepEqual(inner.to, ['vol@example.com']);
  assert.equal(inner.subject, 'Appointment reminder');
  assert.match(inner.text_body, /coming up/);
});

// ─── §E3 selectChannels gate at fire time ───────────────────────────────────────────────

test('selectChannels wired + sms:true → email AND SMS sent (SMS uses sendType:contact)', async () => {
  const message = reminderRow();
  const selectChannels = async () => ({ email: true, sms: true });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: true });
  assert.deepEqual(fnNames(lambdaCalls).sort(), ['SMS_Sender', 'send_email']);
  const sms = lambdaCalls.find((c) => c.FunctionName === 'SMS_Sender');
  assert.equal(JSON.parse(sms.Payload).sendType, 'contact');
});

test('selectChannels sms:false (quiet hours / opted out) → email only', async () => {
  const message = reminderRow();
  const selectChannels = async () => ({ email: true, sms: false });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('selectChannels receives the FROZEN §E3 contract: { tenantId, booking.timezone, orgSmsEnabled, consentRecord, fireTime }', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: true };
  let seen;
  const selectChannels = async (args) => { seen = args; return { email: true, sms: false }; };
  const { deps } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.equal(seen.tenantId, 'AUS123957');
  assert.deepEqual(seen.booking, { timezone: 'America/Chicago' }); // selectChannels does its own tz→local
  assert.equal(seen.orgSmsEnabled, true); // from tenant_prefs.notificationPrefs.sms
  assert.deepEqual(seen.consentRecord, { consent_given: true, opted_out_at: undefined }); // RECORD, read at fire time
  assert.ok('opted_out_at' in seen.consentRecord); // key present (not merely absent) — pins the shape
  assert.equal(seen.fireTime, Date.parse('2026-06-12T18:00:00Z')); // the REAL instant, not a wall-clock shim
  // the broken args are GONE:
  assert.equal('nowLocal' in seen, false);
  assert.equal('attendee' in seen, false);
});

test('reads the consent RECORD only when org SMS is enabled + a phone exists', async () => {
  // org SMS OFF → no consent GetItem, consentRecord null (fail-closed without I/O).
  const offMsg = reminderRow({ tenant_prefs: { notificationPrefs: { sms: false } } });
  let seenOff;
  const { deps: depsOff, ddbCalls: callsOff } = makeDeps({ message: offMsg, selectChannels: async (a) => { seenOff = a; return { email: true, sms: false }; } });
  await dispatch(EVENT(offMsg), depsOff);
  assert.equal(seenOff.orgSmsEnabled, false);
  assert.equal(seenOff.consentRecord, null);
  assert.equal(callsOff.some((c) => c.input?.Key?.sk?.startsWith('CONSENT#')), false); // no consent read
});

test('selectChannels throws → email floor still sends, SMS fails closed', async () => {
  const message = reminderRow();
  const selectChannels = async () => { throw new Error('gate down'); };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

// ─── §E3 seam FIX proof: the REAL channels.js selectChannels, not a fake ──────────────────
// Pre-S3 the dispatcher passed args channels.js ignores → orgSmsEnabled was always undefined
// → SMS could NEVER send. These exercise the real gate to prove the wiring is correct.

test('REAL gate: org on + consent + daytime (non-quiet) → SMS sent (seam fixed)', async () => {
  const message = reminderRow(); // Chicago, tenant_prefs.notificationPrefs.sms:true
  makeDeps._consent = { consent_given: true };
  // 2026-06-12T18:00Z = 13:00 America/Chicago (CDT) → outside the 8pm–8am quiet window.
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.deepEqual(res.dispatched, { email: true, sms: true });
  assert.deepEqual(fnNames(lambdaCalls).sort(), ['SMS_Sender', 'send_email']);
});

test('REAL gate: org on + consent but QUIET HOURS (volunteer-local) → email only', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: true };
  // 2026-06-12T11:00Z = 06:00 America/Chicago → inside the 8pm–8am quiet window.
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T11:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('REAL gate: org on + NO consent record → email only (fail-closed)', async () => {
  const message = reminderRow();
  makeDeps._consent = undefined; // no consent row
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('REAL gate: org on + consent but OPTED OUT → email only', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: true, opted_out_at: '2026-01-01T00:00:00Z' };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('REAL gate: org OFF (tenant_prefs.sms:false) → email only, no consent read', async () => {
  const message = reminderRow({ tenant_prefs: { notificationPrefs: { sms: false } } });
  const { deps, ddbCalls, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
  assert.equal(ddbCalls.some((c) => c.input?.Key?.sk?.startsWith('CONSENT#')), false); // org off → no consent read
});

test('REAL gate: org on + consent + daytime → SMS payload uses sendType:contact (server re-check)', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: true };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  const sms = lambdaCalls.find((c) => c.FunctionName === 'SMS_Sender');
  assert.equal(JSON.parse(sms.Payload).sendType, 'contact'); // defense-in-depth preserved on the REAL path
});

test('REAL gate: bare 10-digit recipient_phone is normalized (toE164) to hit the +1 consent key', async () => {
  const message = reminderRow({ recipient_phone: '5125551234' }); // bare, not +-prefixed
  makeDeps._consent = { consent_given: true };
  const { deps, ddbCalls, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  // the consent GetItem must key on the normalized E.164 (matches the writer) → consent found → SMS sends.
  const consentGet = ddbCalls.find((c) => c.input?.Key?.sk?.startsWith('CONSENT#'));
  assert.equal(consentGet.input.Key.sk, 'CONSENT#transactional#+15125551234');
  assert.deepEqual(res.dispatched, { email: true, sms: true });
});

test('REAL gate: consent record with explicit consent_given:false → email only', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: false };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('REAL gate: consent record MISSING the consent_given field (old shape) → email only', async () => {
  const message = reminderRow();
  makeDeps._consent = { some_other_field: 'x' }; // record exists, no consent_given
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.deepEqual(res.dispatched, { email: true, sms: false }); // === true gate → forward-compatible fail-closed
});

test('readConsentRecord DDB THROW → fail-safe (status stays sent, email floor sends, SMS suppressed)', async () => {
  const message = reminderRow();
  const { deps } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const realSend = deps.ddb.send;
  deps.ddb.send = async (command) => {
    if (command.constructor.name === 'GetCommand' && command.input.Key.sk?.startsWith('CONSENT#')) {
      throw new Error('ProvisionedThroughputExceededException');
    }
    return realSend(command);
  };
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, true); // NOT failed — the consent throw is caught in readConsentRecord
  assert.deepEqual(res.dispatched, { email: true, sms: false });
});

test('REAL gate: org on but NO phone → no consent read + SMS suppressed', async () => {
  const message = reminderRow({ recipient_phone: '' });
  const { deps, ddbCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.dispatched.sms, false);
  assert.equal(ddbCalls.some((c) => c.input?.Key?.sk?.startsWith('CONSENT#')), false); // no phone → skip read
});

test('REAL gate: quiet-hours boundary — exactly 08:00 volunteer-local → SMS allowed', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: true };
  // 2026-06-12T13:00Z = 08:00 America/Chicago (CDT) → first allowed minute (window is 20:00–08:00).
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T13:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.equal(res.dispatched.sms, true);
  assert.ok(fnNames(lambdaCalls).includes('SMS_Sender'));
});

test('REAL gate: quiet-hours boundary — 07:59 volunteer-local → SMS suppressed', async () => {
  const message = reminderRow();
  makeDeps._consent = { consent_given: true };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T12:59:00Z') }); // 07:59 Chicago
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.equal(res.dispatched.sms, false);
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('REAL gate: unresolvable timezone → fail-closed (SMS suppressed, email floor stands)', async () => {
  const message = reminderRow({ timezone: 'Not/AZone' });
  makeDeps._consent = { consent_given: true };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  makeDeps._consent = undefined;
  assert.deepEqual(res.dispatched, { email: true, sms: false });
});

// ─── legacy single-channel rows (no tenant_prefs) ───────────────────────────────────────

test('legacy SMS row with consent → SMS sent via the bare consent check', async () => {
  const message = reminderRow({ channel: 'sms', tenant_prefs: undefined, recipient_email: '' });
  makeDeps._consent = { consent_given: true };
  const { deps, lambdaCalls } = makeDeps({ message }); // no selectChannels
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: false, sms: true });
  assert.deepEqual(fnNames(lambdaCalls), ['SMS_Sender']);
  // The STOP footer rides on the legacy path too (appended in sendSms, outside any body):
  const smsCall = lambdaCalls.find((c) => c.FunctionName === 'SMS_Sender');
  assert.ok(JSON.parse(Buffer.from(smsCall.Payload).toString()).body.endsWith('\nReply STOP to opt out, HELP for help.'));
  makeDeps._consent = undefined;
});

test('legacy SMS row WITHOUT consent → suppressed (fail-closed)', async () => {
  const message = reminderRow({ channel: 'sms', tenant_prefs: undefined, recipient_email: '' });
  makeDeps._consent = undefined; // no consent record
  const { deps, ddbCalls, lambdaCalls } = makeDeps({ message });
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.suppressed, true);
  assert.equal(lambdaCalls.length, 0);
  assert.equal(lastStatus(ddbCalls), 'suppressed');
});

// ─── status-gate + guards ───────────────────────────────────────────────────────────────

test('non-pending message → skipped (defence-in-depth vs a surviving rule)', async () => {
  const message = reminderRow({ status: 'cancelled' });
  const { deps, lambdaCalls } = makeDeps({ message });
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.skipped, true);
  assert.equal(res.reason, 'cancelled');
  assert.equal(lambdaCalls.length, 0);
});

test('missing pk/sk → error', async () => {
  const { deps } = makeDeps({});
  const res = await dispatch({ message_id: 'x' }, deps);
  assert.equal(res.success, false);
  assert.equal(res.error, 'missing_keys');
});

test('message not found → not_found', async () => {
  const { deps } = makeDeps({ message: undefined });
  const res = await dispatch({ pk: 'TENANT#x', sk: 'SCHEDULED#a#b' }, deps);
  assert.equal(res.success, false);
  assert.equal(res.error, 'not_found');
});

test('no eligible channel (email row, no recipient email) → suppressed', async () => {
  const message = reminderRow({ recipient_email: '' });
  const { deps, ddbCalls } = makeDeps({ message }); // no selectChannels, channel=email
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.suppressed, true);
  assert.equal(lastStatus(ddbCalls), 'suppressed');
});

test('send failure → status failed', async () => {
  const message = reminderRow();
  const { deps, ddbCalls } = makeDeps({ message });
  deps.lambda.send = async () => { throw new Error('SES throttled'); };
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, false);
  assert.equal(res.error, 'SES throttled');
  assert.equal(lastStatus(ddbCalls), 'failed');
});

// ─── §E14 S4b: fire-time reminder template overrides ──────────────────────────────────

import {
  reminderMomentFromRow,
  buildReminderContent,
  loadTemplateOverride,
  REMINDER_TEMPLATES,
  SMS_STOP_FOOTER,
  STOP_LINE_TEXT,
  STOP_LINE_HTML,
} from './index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dir = path.dirname(fileURLToPath(import.meta.url));

// Both channels open — content tests assert WHAT is sent, not the §E3 gate (covered above).
const bothChannels = async () => ({ email: true, sms: true });

const s4bVars = {
  organization_name: 'Austin Angels',
  appointment_type: 'mentor interview',
  first_name: 'Sam',
};

const parseEmail = (lambdaCalls) => {
  const call = lambdaCalls.find((c) => c.FunctionName === 'send_email');
  return JSON.parse(JSON.parse(Buffer.from(call.Payload).toString()).body);
};
const parseSms = (lambdaCalls) => {
  const call = lambdaCalls.find((c) => c.FunctionName === 'SMS_Sender');
  return JSON.parse(call.Payload);
};

test('reminderMomentFromRow maps t24h/t1h only', () => {
  assert.equal(reminderMomentFromRow({ moment: 'reminder', tier: 't24h' }), 'reminder_24h');
  assert.equal(reminderMomentFromRow({ moment: 'reminder', tier: 't1h' }), 'reminder_1h');
  assert.equal(reminderMomentFromRow({ moment: 'reminder', tier: 't4h' }), null);
  assert.equal(reminderMomentFromRow({ moment: 'reminder', tier: 't15m' }), null);
  assert.equal(reminderMomentFromRow({ moment: 'reminder', tier: 't24h', attendance_check: true }), null);
  assert.equal(reminderMomentFromRow({ moment: 'reminder' }), null); // legacy, no tier
  assert.equal(reminderMomentFromRow({ moment: 'other', tier: 't24h' }), null);
});

test('t24h row, no override → §E14 default copy (NOT the baked row body) + SMS STOP footer', async () => {
  const message = reminderRow({ tier: 't24h', template_vars: s4bVars });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => null;
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, true);
  const email = parseEmail(lambdaCalls);
  assert.equal(email.subject, 'Reminder: your mentor interview is tomorrow — Austin Angels');
  assert.equal(email.text_body, 'Hi Sam,\n\nThis is a reminder that your mentor interview with Austin Angels is tomorrow.' + STOP_LINE_TEXT);
  assert.equal(email.html_body, '<p>Hi Sam,</p><p>This is a reminder that your mentor interview with Austin Angels is tomorrow.</p>' + STOP_LINE_HTML);
  const sms = parseSms(lambdaCalls);
  assert.equal(sms.body, 'Reminder: your mentor interview with Austin Angels is tomorrow.' + SMS_STOP_FOOTER);
});

test('t24h row with full override → override rendered on every field; STOP outside the editable body', async () => {
  const message = reminderRow({ tier: 't24h', template_vars: s4bVars });
  const loaderCalls = [];
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async (args) => {
    loaderCalls.push(args);
    return {
      subject: 'See you soon, {{firstName}}!',
      text: '{{firstName}}, your {{apptType}} at {{org}} is tomorrow!',
      html: '<p>{{firstName}}, your {{apptType}} at {{org}} is tomorrow!</p>',
      sms: '{{firstName}}: {{apptType}} tomorrow at {{org}}.',
    };
  };
  await dispatch(EVENT(message), deps);
  assert.equal(loaderCalls.length, 1);
  assert.equal(loaderCalls[0].tenantId, 'AUS123957');
  assert.equal(loaderCalls[0].moment, 'reminder_24h');
  const email = parseEmail(lambdaCalls);
  assert.equal(email.subject, 'See you soon, Sam!');
  // The email STOP line rides OUTSIDE the override body (same invariant as SMS):
  assert.equal(email.text_body, 'Sam, your mentor interview at Austin Angels is tomorrow!' + STOP_LINE_TEXT);
  assert.equal(email.html_body, '<p>Sam, your mentor interview at Austin Angels is tomorrow!</p>' + STOP_LINE_HTML);
  const sms = parseSms(lambdaCalls);
  assert.equal(sms.body, 'Sam: mentor interview tomorrow at Austin Angels.' + SMS_STOP_FOOTER);
});

test('partial override → per-field merge (sms from override, email fields from defaults)', async () => {
  const message = reminderRow({ tier: 't1h', template_vars: s4bVars });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => ({ sms: 'Almost time, {{firstName}}!' });
  await dispatch(EVENT(message), deps);
  const email = parseEmail(lambdaCalls);
  assert.equal(email.subject, 'Reminder: your mentor interview is in about an hour — Austin Angels');
  const sms = parseSms(lambdaCalls);
  assert.equal(sms.body, 'Almost time, Sam!' + SMS_STOP_FOOTER);
});

test('override carrying its own "reply STOP" line is not double-footed', async () => {
  const message = reminderRow({ tier: 't1h', template_vars: s4bVars });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => ({ sms: 'Almost time! Reply STOP to opt out.' });
  await dispatch(EVENT(message), deps);
  assert.equal(parseSms(lambdaCalls).body, 'Almost time! Reply STOP to opt out.');
});

test('html vars are escaped; text/sms vars are verbatim (HTML-injection guard)', () => {
  const content = buildReminderContent('reminder_24h', {
    first_name: '<b>Sam</b>',
    organization_name: 'A&B "Angels"',
    appointment_type: 'interview',
  }, null);
  assert.ok(content.html.includes('&lt;b&gt;Sam&lt;/b&gt;'));
  assert.ok(content.html.includes('A&amp;B &quot;Angels&quot;'));
  assert.ok(content.text.includes('<b>Sam</b>'));
  assert.ok(content.text.includes('A&B "Angels"'));
  assert.ok(content.sms.includes('A&B "Angels"'));
});

test('unknown {{vars}} in an override render as empty string (editor contract)', () => {
  const content = buildReminderContent('reminder_24h', s4bVars, {
    text: 'Hi {{firstName}}, see you {{whenLabel}}.',
  });
  assert.equal(content.text, 'Hi Sam, see you .');
});

test('whitespace-only override field falls back to the default', () => {
  const content = buildReminderContent('reminder_24h', s4bVars, { subject: '   ' });
  assert.equal(content.subject, 'Reminder: your mentor interview is tomorrow — Austin Angels');
});

test('non-overridable rows (t4h / attendance / legacy) never call the loader and keep the baked body', async () => {
  for (const row of [
    reminderRow({ tier: 't4h', template_vars: s4bVars }),
    reminderRow({ attendance_check: true, template_vars: s4bVars }),
    reminderRow({ template_vars: s4bVars }), // legacy: no tier
  ]) {
    const loaderCalls = [];
    const { deps, lambdaCalls } = makeDeps({ message: row, selectChannels: bothChannels });
    deps.loadTemplateOverride = async (args) => { loaderCalls.push(args); return null; };
    await dispatch(EVENT(row), deps);
    assert.equal(loaderCalls.length, 0);
    const email = parseEmail(lambdaCalls);
    assert.equal(email.subject, 'Appointment reminder'); // baked row subject, unchanged
    // Baked body unchanged; attendee-facing rows additionally carry the unsubscribe line
    // OUTSIDE it, while the coordinator attendance prompt does not.
    const expectedTail = row.attendance_check ? '' : STOP_LINE_TEXT;
    assert.equal(email.text_body, 'Reminder: your appointment is coming up.' + expectedTail);
  }
});

test('baked (non-overridable) SMS also carries the STOP footer', async () => {
  const message = reminderRow({ tier: 't4h', template_vars: s4bVars });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  await dispatch(EVENT(message), deps);
  assert.equal(parseSms(lambdaCalls).body, 'Reminder: your appointment is coming up.' + SMS_STOP_FOOTER);
});

test('throwing loader degrades to the default copy — never a failed send', async () => {
  const message = reminderRow({ tier: 't24h', template_vars: s4bVars });
  const { deps, lambdaCalls, ddbCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => { throw new Error('boom'); };
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, true);
  assert.equal(parseEmail(lambdaCalls).subject, 'Reminder: your mentor interview is tomorrow — Austin Angels');
  assert.equal(lastStatus(ddbCalls), 'sent');
});

test('loadTemplateOverride is a no-op (null, zero I/O) while SCHED_NOTIF_TEMPLATE_TABLE is unset', async () => {
  // This file imports index.mjs WITHOUT the env var — the IaC-not-applied-yet state.
  const calls = [];
  const ddb = { send: async (c) => { calls.push(c); return { Item: { subject: 'x' } }; } };
  const result = await loadTemplateOverride({ tenantId: 'T1', moment: 'reminder_24h', ddb, logger: console });
  assert.equal(result, null);
  assert.equal(calls.length, 0);
});

// ─── parity guards ─────────────────────────────────────────────────────────────────────
// The footer/template parity tests that lived here are GONE by construction: the Sender now
// IMPORTS the strings from shared/scheduling/notif-defaults.js (same object notify.js uses).
// ADA parity for ALL moments lives in shared/scheduling/__tests__/notif-defaults-parity.test.js.


// REMINDER_TEMPLATES must stay byte-in-sync with the ADA editor's defaults — otherwise the
// editor's reset/preview lies about what this Lambda actually sends. Reads the ADA Python
// source (same technique as notify-sms-parity.test.js); unescapes \n / \' / \\ literals.

test('overridable row with NO template_vars (old-shape) renders empty vars, never crashes', async () => {
  const message = reminderRow({ tier: 't24h' });
  delete message.template_vars;
  const { deps, lambdaCalls, ddbCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => null;
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, true);
  assert.equal(lastStatus(ddbCalls), 'sent');
  assert.equal(parseEmail(lambdaCalls).subject, 'Reminder: your  is tomorrow —');
});

// ─── audit-fix tests (S4b phase audit) ─────────────────────────────────────────────────

test('B1: dual-channel partial failure (email OK, SMS throws) → failed + dispatched truth', async () => {
  const message = reminderRow({ tier: 't24h', template_vars: s4bVars });
  const { deps, ddbCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => null;
  const sent = [];
  deps.lambda.send = async (command) => {
    sent.push(command.input.FunctionName);
    if (command.input.FunctionName === 'SMS_Sender') throw new Error('throttled');
  };
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.success, false);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(sent, ['send_email', 'SMS_Sender']); // email DID go out first
  assert.equal(lastStatus(ddbCalls), 'failed');
});

test('B2: failed status persists error_detail in the UpdateExpression', async () => {
  const message = reminderRow();
  const { deps, ddbCalls } = makeDeps({ message });
  deps.lambda.send = async () => { throw new Error('SES throttled'); };
  await dispatch(EVENT(message), deps);
  const update = ddbCalls.find((c) => c.name === 'UpdateCommand');
  assert.match(update.input.UpdateExpression, /error_detail = :error/);
  assert.equal(update.input.ExpressionAttributeValues[':error'], 'SES throttled');
});

test('B3: DDB read failure on the message Get → read_failed, nothing sent, no status write', async () => {
  const { deps, ddbCalls, lambdaCalls } = makeDeps({});
  deps.ddb.send = async () => { throw new Error('ProvisionedThroughputExceededException'); };
  const res = await dispatch({ pk: 'TENANT#x', sk: 'SCHEDULED#a#b', message_id: 'm' }, deps);
  assert.equal(res.success, false);
  assert.equal(res.error, 'read_failed');
  assert.equal(lambdaCalls.length, 0);
  assert.equal(ddbCalls.filter((c) => c.name === 'UpdateCommand').length, 0);
});

test('S3: non-pending skip writes NO status update', async () => {
  const message = reminderRow({ status: 'cancelled' });
  const { deps, ddbCalls, lambdaCalls } = makeDeps({ message });
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.skipped, true);
  assert.equal(lambdaCalls.length, 0);
  assert.equal(ddbCalls.filter((c) => c.name === 'UpdateCommand').length, 0);
});

test('S4: legacy consent-check DDB error → fail-closed suppression, no SMS', async () => {
  const message = reminderRow({ channel: 'sms', tenant_prefs: undefined, recipient_email: '' });
  const { deps, ddbCalls, lambdaCalls } = makeDeps({ message });
  const origSend = deps.ddb.send;
  deps.ddb.send = async (command) => {
    if (command.input.Key?.sk?.startsWith('CONSENT#')) throw new Error('AccessDenied');
    return origSend(command);
  };
  const res = await dispatch(EVENT(message), deps);
  assert.equal(res.suppressed, true);
  assert.equal(lambdaCalls.length, 0);
  assert.equal(lastStatus(ddbCalls), 'suppressed');
});

test('§E3: phone-but-no-email row → SMS only (the email floor cannot email a missing address)', async () => {
  const message = reminderRow({ recipient_email: '' });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: false, sms: true });
  assert.deepEqual(fnNames(lambdaCalls), ['SMS_Sender']);
});

test('REAL gate at 20:00 local (quiet-hours start) → SMS suppressed, email only', async () => {
  const message = reminderRow(); // Chicago, org sms on, quiet 20–8
  makeDeps._consent = { consent_given: true };
  // 2026-06-13T01:00:00Z = 20:00 America/Chicago (CDT) — the first quiet minute.
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-13T01:00:00Z') });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
});

test('REAL gate daytime SMS: the carrier-bound body is the §E14 copy + STOP footer (S1)', async () => {
  const message = reminderRow({ tier: 't1h', template_vars: s4bVars });
  makeDeps._consent = { consent_given: true };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels, now: Date.parse('2026-06-12T18:00:00Z') });
  deps.loadTemplateOverride = async () => null;
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: true });
  assert.equal(parseSms(lambdaCalls).body, 'Reminder: your mentor interview with Austin Angels is in about an hour.' + SMS_STOP_FOOTER);
});

test('CRLF in an override subject is stripped before send_email (header-injection guard)', async () => {
  const message = reminderRow({ tier: 't24h', template_vars: s4bVars });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  deps.loadTemplateOverride = async () => ({ subject: 'Hi {{firstName}}\r\nX-Injected: 1\nBcc: x' });
  await dispatch(EVENT(message), deps);
  assert.equal(parseEmail(lambdaCalls).subject, 'Hi Sam X-Injected: 1 Bcc: x');
});

test('bare 10-digit recipient_phone is E.164-normalized in the SMS_Sender payload `to`', async () => {
  const message = reminderRow({ recipient_phone: '5125551234' });
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels: bothChannels });
  await dispatch(EVENT(message), deps);
  assert.equal(parseSms(lambdaCalls).to, '+15125551234');
});

test('baked-path html escapes interpolated template_vars (injection guard parity with overrides)', async () => {
  const message = reminderRow({
    body: 'Hi {{first_name}}, see you soon.',
    template_vars: { first_name: '<img src=x onerror=alert(1)>' },
  });
  const { deps, lambdaCalls } = makeDeps({ message });
  await dispatch(EVENT(message), deps);
  const email = parseEmail(lambdaCalls);
  assert.ok(email.html_body.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(!email.html_body.includes('<img'));
  assert.ok(email.text_body.includes('<img src=x onerror=alert(1)>')); // text stays verbatim
});

test('attendance (coordinator) email gets NO unsubscribe line; attendee reminder email does', async () => {
  for (const [row, expectLine] of [
    [reminderRow({ attendance_check: true }), false],
    [reminderRow(), true],
  ]) {
    const { deps, lambdaCalls } = makeDeps({ message: row });
    await dispatch(EVENT(row), deps);
    const email = parseEmail(lambdaCalls);
    assert.equal(email.text_body.includes('reply STOP'), expectLine);
    assert.equal(email.html_body.includes('reply STOP'), expectLine);
  }
});

