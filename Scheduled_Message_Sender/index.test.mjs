import { test } from 'node:test';
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
