'use strict';

/**
 * ADA ↔ notif-defaults.js parity (§E14 S4c merge gate).
 *
 * notif-defaults.js is what the dispatchers SEND when no override exists; the ADA
 * `_SCHED_NOTIF_DEFAULTS` is what the editor SHOWS as the default. Byte-drift between
 * them means the editor preview lies. Same technique as notify-sms-parity.test.js
 * (read the Python source), extended to handle BOTH Python quoting styles — the ADA
 * confirmation strings are double-quoted (they contain apostrophes).
 */

const fs = require('fs');
const path = require('path');
const { CONFIRMATION_DEFAULTS } = require('../notif-defaults');

const ADA_PATH = path.resolve(__dirname, '../../../Analytics_Dashboard_API/lambda_function.py');

// Extract a single-quoted OR double-quoted Python string value for `'field':` within
// `block`. Truncation guard: the value must end with the given terminal token — a
// reformat into implicit string concatenation reads as a loud failure, not a false-pass.
function extractField(block, field, terminal) {
  const m = block.match(
    new RegExp(`'${field}':\\s*(?:'((?:[^'\\\\]|\\\\.)*)'|"((?:[^"\\\\]|\\\\.)*)")`)
  );
  if (!m) throw new Error(`ADA field ${field} not found`);
  const raw = m[1] != null ? m[1] : m[2];
  const value = raw.replace(/\\(n|'|"|\\)/g, (_, c) => (c === 'n' ? '\n' : c));
  if (!value.endsWith(terminal)) {
    throw new Error(`ADA ${field} extraction looks truncated (no terminal ${terminal})`);
  }
  return value;
}

describe('ADA ↔ notif-defaults.js confirmation parity (§E14 S4c)', () => {
  const src = fs.readFileSync(ADA_PATH, 'utf8');
  const start = src.indexOf('_SCHED_NOTIF_DEFAULTS');
  const end = src.indexOf('_SCHED_NOTIF_MOMENT_VARS', start);
  const block = src.slice(start, end).slice(src.slice(start, end).indexOf("'confirmation'"));

  test('subject is byte-identical to the ADA editor default', () => {
    expect(CONFIRMATION_DEFAULTS.subject).toBe(extractField(block, 'subject', '{{org}}'));
  });

  test('body_text is byte-identical to the ADA editor default', () => {
    expect(CONFIRMATION_DEFAULTS.text).toBe(extractField(block, 'body_text', '.'));
  });

  test('body_html is byte-identical to the ADA editor default', () => {
    expect(CONFIRMATION_DEFAULTS.html).toBe(extractField(block, 'body_html', '</p>'));
  });
});
