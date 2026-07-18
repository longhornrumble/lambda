/**
 * formShapeValidation.test.mjs — form-shape gate on the PUT path.
 *
 * This Lambda is the ONLY write path to the config bucket, and the widget
 * reads the stored config directly — so shapes the widget cannot render
 * (unsupported field types, composites without subfields, selects without
 * options) must be rejected HERE, not just in the Config Builder. Regression
 * net for the BRI071351 seeder incident (2026-07-18): 'boolean' fields and
 * flat name/address composites were written through this Lambda and produced
 * dead-end live forms (FormFieldPrompt renders no input for them).
 *
 * Runner: node:test (built-in), matching the rest of this Lambda's suites.
 * Fixture: __tests__/fixtures/bri071351-canonical-forms.json is the ACTUAL
 * repaired BRI071351 conversational_forms section — the canonical shape must
 * always pass.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { validateEditedSections } from './mergeStrategy.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const canonicalForms = JSON.parse(
  readFileSync(join(here, '__tests__', 'fixtures', 'bri071351-canonical-forms.json'), 'utf8')
);

const withFields = (fields) => ({
  conversational_forms: {
    test_form: { form_id: 'test_form', program: 'p1', title: 'Test', enabled: true, fields },
  },
});

// ---------------------------------------------------------------------------
// Canonical shapes pass.
// ---------------------------------------------------------------------------

test('accepts the repaired BRI071351 forms (live canonical fixture)', () => {
  const res = validateEditedSections(canonicalForms);
  assert.deepEqual(res.errors, []);
  assert.equal(res.isValid, true);
});

test('accepts every widget-renderable simple type', () => {
  const fields = ['text', 'email', 'phone', 'textarea', 'number', 'date'].map((type, i) => ({
    id: `f${i}`,
    type,
    required: false,
  }));
  assert.equal(validateEditedSections(withFields(fields)).isValid, true);
});

test('accepts composites with subfields and selects with options', () => {
  const res = validateEditedSections(
    withFields([
      {
        id: 'n',
        type: 'name',
        required: true,
        subfields: [{ id: 'n.first_name', label: 'First Name', required: true, type: 'text' }],
      },
      { id: 's', type: 'select', required: true, options: [{ value: 'yes', label: 'Yes' }] },
    ])
  );
  assert.equal(res.isValid, true);
});

// ---------------------------------------------------------------------------
// Widget-breaking shapes are blocked.
// ---------------------------------------------------------------------------

test('rejects unsupported field types (the seeder boolean incident)', () => {
  const res = validateEditedSections(
    withFields([{ id: 'consent', type: 'boolean', label: 'Consent', required: true }])
  );
  assert.equal(res.isValid, false);
  assert.ok(res.errors.some((e) => e.includes('unsupported type "boolean"')));
});

test('rejects composite fields without subfields (all three composite types)', () => {
  for (const type of ['name', 'address', 'phone_with_consent']) {
    const res = validateEditedSections(withFields([{ id: 'f1', type, required: true }]));
    assert.equal(res.isValid, false, `${type} should be rejected without subfields`);
    assert.ok(res.errors.some((e) => e.includes('requires a non-empty subfields array')));
  }
});

test('rejects select fields without options', () => {
  const res = validateEditedSections(withFields([{ id: 'pick', type: 'select', required: true }]));
  assert.equal(res.isValid, false);
  assert.ok(res.errors.some((e) => e.includes('non-empty options array')));
});

test('collects one error per bad field (multi-error reporting)', () => {
  const res = validateEditedSections(
    withFields([
      { id: 'a', type: 'boolean', required: true },
      { id: 'b', type: 'name', required: true },
      { id: 'c', type: 'select', required: true },
    ])
  );
  assert.equal(res.isValid, false);
  assert.equal(res.errors.length, 3);
});

// ---------------------------------------------------------------------------
// Scope and forward-compatibility.
// ---------------------------------------------------------------------------

test('does not shape-check writes that omit conversational_forms', () => {
  assert.equal(validateEditedSections({ programs: { p1: { program_id: 'p1' } } }).isValid, true);
});

test('tolerates malformed form containers (forward-compatible reads)', () => {
  assert.equal(validateEditedSections({ conversational_forms: null }).isValid, true);
  assert.equal(validateEditedSections({ conversational_forms: {} }).isValid, true);
  assert.equal(validateEditedSections({ conversational_forms: { f: null } }).isValid, true);
  assert.equal(
    validateEditedSections({ conversational_forms: { f: { form_id: 'f' } } }).isValid,
    true
  );
});
