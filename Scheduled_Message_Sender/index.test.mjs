import { test } from 'node:test';
import assert from 'node:assert/strict';

import { dispatch } from './index.mjs';

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

test('selectChannels receives a fire-time nowLocal + the snapshotted tenantPrefs/moment', async () => {
  const message = reminderRow();
  let seen;
  const selectChannels = async (args) => { seen = args; return { email: true, sms: false }; };
  const { deps } = makeDeps({ message, selectChannels });
  await dispatch(EVENT(message), deps);
  assert.equal(seen.tenantId, 'AUS123957');
  assert.equal(seen.moment, 'reminder');
  assert.deepEqual(seen.attendee, { phone: '+15125551234', email: 'vol@example.com' });
  assert.deepEqual(seen.tenantPrefs, message.tenant_prefs);
  // nowLocal is a Date whose UTC hour carries the booking-timezone wall clock:
  // 2026-06-12T11:00Z is 06:00 in America/Chicago (CDT, UTC-5).
  assert.equal(seen.nowLocal.getUTCHours(), 6);
});

test('selectChannels throws → email floor still sends, SMS fails closed', async () => {
  const message = reminderRow();
  const selectChannels = async () => { throw new Error('gate down'); };
  const { deps, lambdaCalls } = makeDeps({ message, selectChannels });
  const res = await dispatch(EVENT(message), deps);
  assert.deepEqual(res.dispatched, { email: true, sms: false });
  assert.deepEqual(fnNames(lambdaCalls), ['send_email']);
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
