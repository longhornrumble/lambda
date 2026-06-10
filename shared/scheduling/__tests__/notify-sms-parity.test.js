'use strict';

/**
 * ADA ↔ notify.js SMS-defaults parity (§E14 G7b merge gate).
 *
 * The §E14 SMS editor (Analytics_Dashboard_API `_SCHED_NOTIF_SMS_DEFAULTS`) shows the
 * tenant the DEFAULT sms_text; notify.js (`SMS_TEMPLATES`) renders it when no override
 * exists. If the two drift, the editor preview lies about what actually gets sent. This
 * test reads the ADA Python source and asserts the 3 dispatched-moment SMS defaults are
 * byte-identical to notify's — a change to EITHER file without the other fails CI.
 *
 * Follow-up (§E14 lock): extract the defaults to a shared JSON to make parity structural
 * (one source instead of two language-specific copies + this guard).
 */

const fs = require('fs');
const path = require('path');
const { SMS_TEMPLATES } = require('../notify');

// shared/scheduling/__tests__ → repo root → Analytics_Dashboard_API
const ADA_PATH = path.resolve(__dirname, '../../../Analytics_Dashboard_API/lambda_function.py');

// Slice the flat `_SCHED_NOTIF_SMS_DEFAULTS = { ... }` literal (up to the next module
// constant) and pull each `'kind': 'value'` pair. The values contain `{{var}}` braces, so
// we delimit by the next constant name rather than a naive first-`}` scan.
function extractAdaSmsDefaults(src) {
  const start = src.indexOf('_SCHED_NOTIF_SMS_DEFAULTS');
  if (start === -1) throw new Error('_SCHED_NOTIF_SMS_DEFAULTS not found in ADA source');
  const end = src.indexOf('_SCHED_NOTIF_SMS_VARS', start);
  const block = src.slice(start, end > start ? end : start + 4000);
  const out = {};
  const re = /'([a-z_]+)'\s*:\s*'((?:[^'\\]|\\.)*)'/g;
  let m;
  while ((m = re.exec(block))) out[m[1]] = m[2];
  return out;
}

describe('ADA ↔ notify.js SMS-defaults parity (§E14 G7b)', () => {
  const ada = extractAdaSmsDefaults(fs.readFileSync(ADA_PATH, 'utf8'));

  test('the ADA defaults parsed (sanity: exactly 3 dispatched moments)', () => {
    expect(Object.keys(ada).length).toBe(3);
  });

  for (const kind of Object.keys(SMS_TEMPLATES)) {
    test(`${kind}: notify SMS_TEMPLATES is byte-identical to ADA _SCHED_NOTIF_SMS_DEFAULTS`, () => {
      expect(ada[kind]).toBe(SMS_TEMPLATES[kind]);
    });
  }

  test('the moment sets match EXACTLY in both directions (no ADA-only or notify-only kind)', () => {
    // Symmetric: catches a kind added to ADA that notify can't render (editor would preview a
    // moment the sender drops) AND a notify-only kind. Both files must move together.
    expect(Object.keys(ada).sort()).toEqual(Object.keys(SMS_TEMPLATES).sort());
    expect(Object.keys(SMS_TEMPLATES).sort()).toEqual(
      ['cancel_notice', 'reoffer', 'reschedule_link'].sort()
    );
  });
});
