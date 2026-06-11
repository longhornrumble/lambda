'use strict';

/**
 * ADA <-> notif-defaults.js parity — THE §E14 merge gate (unified).
 *
 * notif-defaults.js is the single JS source every dispatcher sends from
 * (notify.js / Scheduled_Message_Sender / confirmation-email.js); the ADA
 * editor's Python dicts (`_SCHED_NOTIF_DEFAULTS` / `_SCHED_NOTIF_SMS_DEFAULTS`)
 * are what tenants SEE as the default. Byte-drift between them makes the editor
 * preview lie. This file replaces the three per-Lambda regex parity tests
 * (notify-sms-parity, the Sender's ADA test, the confirmation-only version of
 * this file) with ONE that covers EVERY dispatched moment, email + SMS.
 *
 * The extractor handles both Python quoting styles AND implicit string
 * concatenation (adjacent literals), with a non-empty guard so a reformat reads
 * as a loud failure, never a fragment false-pass.
 *
 * NOT pinned on purpose: ADA's confirmation `sms_text` default — no dispatcher
 * sends a confirmation SMS (editor-only vocabulary).
 */

const fs = require('fs');
const path = require('path');
const {
  TEMPLATES,
  SMS_TEMPLATES,
  REMINDER_TEMPLATES,
  CONFIRMATION_DEFAULTS,
} = require('../notif-defaults');

const ADA_PATH = path.resolve(__dirname, '../../../Analytics_Dashboard_API/lambda_function.py');

// One Python string literal (either quote style):
const PY_STR = `(?:'(?:[^'\\\\]|\\\\.)*'|"(?:[^"\\\\]|\\\\.)*")`;

function unescape(raw) {
  return raw.replace(/\\(n|'|"|\\)/g, (_, c) => (c === 'n' ? '\n' : c));
}

// Extract `'field': <one or more adjacent string literals>` within `block`,
// joining implicit concatenation pieces.
function extractField(block, field) {
  const m = block.match(new RegExp(`'${field}':\\s*(${PY_STR}(?:\\s*\\n?\\s*${PY_STR})*)`));
  if (!m) throw new Error(`ADA field ${field} not found`);
  const pieces = [...m[1].matchAll(new RegExp(PY_STR, 'g'))].map((p) => unescape(p[0].slice(1, -1)));
  return pieces.join('');
}

function momentBlock(dictBlock, moment) {
  const i = dictBlock.indexOf(`'${moment}'`);
  if (i === -1) throw new Error(`ADA moment ${moment} not found`);
  return dictBlock.slice(i);
}

describe('ADA <-> notif-defaults.js parity (§E14, all dispatched moments)', () => {
  const src = fs.readFileSync(ADA_PATH, 'utf8');
  const emailDict = src.slice(src.indexOf('_SCHED_NOTIF_DEFAULTS'), src.indexOf('_SCHED_NOTIF_MOMENT_VARS'));
  const smsDict = src.slice(src.indexOf('_SCHED_NOTIF_SMS_DEFAULTS'), src.indexOf('_SCHED_NOTIF_SMS_VARS'));

  // The dispatched email copy per moment, from the single shared source:
  const EMAIL = {
    reschedule_link: TEMPLATES.reschedule_link,
    reoffer: TEMPLATES.reoffer,
    cancel_notice: TEMPLATES.cancel_notice,
    reminder_24h: REMINDER_TEMPLATES.reminder_24h,
    reminder_1h: REMINDER_TEMPLATES.reminder_1h,
    confirmation: CONFIRMATION_DEFAULTS,
  };
  // The dispatched SMS copy per moment (confirmation deliberately absent):
  const SMS = {
    reschedule_link: SMS_TEMPLATES.reschedule_link,
    reoffer: SMS_TEMPLATES.reoffer,
    cancel_notice: SMS_TEMPLATES.cancel_notice,
    reminder_24h: REMINDER_TEMPLATES.reminder_24h.sms,
    reminder_1h: REMINDER_TEMPLATES.reminder_1h.sms,
  };

  for (const [moment, tpl] of Object.entries(EMAIL)) {
    for (const [jsField, adaField] of [['subject', 'subject'], ['text', 'body_text'], ['html', 'body_html']]) {
      test(`${moment}.${jsField} email default is byte-identical to ADA`, () => {
        const ada = extractField(momentBlock(emailDict, moment), adaField);
        expect(ada.length).toBeGreaterThan(10); // truncation guard: never a fragment
        expect(tpl[jsField]).toBe(ada);
      });
    }
  }

  for (const [moment, smsDefault] of Object.entries(SMS)) {
    test(`${moment} SMS default is byte-identical to ADA`, () => {
      const ada = extractField(momentBlock(smsDict, moment), moment);
      expect(ada.length).toBeGreaterThan(10);
      expect(smsDefault).toBe(ada);
    });
  }

  test('the ADA moment allowlist matches the dispatched set exactly', () => {
    // _SCHED_NOTIF_MOMENTS is the editor's allowlist; a new ADA moment without a
    // notif-defaults entry fails HERE, not in production.
    const m = src.match(/_SCHED_NOTIF_MOMENTS = \(([^)]*)\)/);
    const moments = [...m[1].matchAll(/'([a-z_0-9]+)'/g)].map((x) => x[1]);
    expect(moments.sort()).toEqual(Object.keys(EMAIL).sort());
  });
});
