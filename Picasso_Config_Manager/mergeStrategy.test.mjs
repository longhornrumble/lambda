/**
 * mergeStrategy.test.mjs — first-ever test suite for Picasso_Config_Manager
 * (Messenger Product Surface P0a).
 *
 * PURPOSE: pin the CURRENT merge/replace/drop semantics as a regression net,
 * so that T2a's `messenger_behavior` addition is provably a behavior change and
 * not an accidental one. These tests deliberately DOCUMENT known-quirky behavior
 * (wholesale-replace-per-section, silent-drop of unknown sections) as it exists
 * today — they are NOT a spec for a fixed future. Where a test pins a landmine,
 * it says so; T2a will update the specific assertion it changes.
 *
 * Runner: node:test (built-in), matching the repo's other node:test Lambdas
 * (kb_proposal_applier, notification_hub). `npm test` = `node --test *.test.mjs`.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  mergeConfigSections,
  extractEditableSections,
  validateEditedSections,
  getSectionInfo,
  isEditableSection,
  isReadOnlySection,
  mergeMultipleSectionUpdates,
  generateConfigDiff,
} from './mergeStrategy.mjs';

// ---------------------------------------------------------------------------
// EDITABLE_SECTIONS allowlist — the list every other behavior keys off.
// ---------------------------------------------------------------------------

test('getSectionInfo pins the current section categorization (19 editable, card_inventory read-only)', () => {
  const info = getSectionInfo();
  // 19 entries today. T2a adds `messenger_behavior` → this becomes 20 and is
  // the assertion T2a intentionally updates.
  assert.equal(info.editable.length, 19);
  assert.ok(info.editable.includes('feature_flags'));
  assert.ok(info.editable.includes('notification_settings'));
  assert.deepEqual(info.readOnly, ['card_inventory']);
});

test('messenger_behavior is NOT an editable section today (Landmine 2 — T2a fixes this)', () => {
  assert.equal(isEditableSection('messenger_behavior'), false);
  assert.equal(isEditableSection('feature_flags'), true);
  assert.equal(isReadOnlySection('card_inventory'), true);
  assert.equal(isReadOnlySection('feature_flags'), false);
});

// ---------------------------------------------------------------------------
// mergeConfigSections — the core write path.
// ---------------------------------------------------------------------------

test('mergeConfigSections does WHOLESALE-REPLACE per section — a partial send wipes sibling keys (Landmine 2)', () => {
  const base = {
    tenant_id: 'T1',
    feature_flags: { V5_SINGLE_PASS: true, MESSENGER_CHANNEL: true },
  };
  // Client sends only ONE key of feature_flags.
  const edited = { feature_flags: { V5_SINGLE_PASS: false } };

  const merged = mergeConfigSections(base, edited);

  // The whole section is replaced: MESSENGER_CHANNEL is GONE. This is the
  // documented hazard behind the "always send the whole section" discipline.
  assert.deepEqual(merged.feature_flags, { V5_SINGLE_PASS: false });
  assert.equal('MESSENGER_CHANNEL' in merged.feature_flags, false);
});

test('mergeConfigSections SILENTLY DROPS an unknown/unlisted section (Landmine 2 — messenger_behavior today)', () => {
  const base = { tenant_id: 'T1', tone_prompt: 'hi' };
  const edited = { messenger_behavior: { escalation_email: 'notify@myrecruiter.ai' } };

  const merged = mergeConfigSections(base, edited);

  // Not in EDITABLE_SECTIONS today → never copied into the merged config.
  // This is exactly why an edit "appears to save then vanishes" pre-T2a.
  assert.equal('messenger_behavior' in merged, false);
});

test('mergeConfigSections preserves read-only + untouched sections from base', () => {
  const base = {
    tenant_id: 'T1',
    card_inventory: { extracted: ['a'] },
    programs: [{ id: 'p1' }],
  };
  const edited = { feature_flags: { V5_SINGLE_PASS: true } };

  const merged = mergeConfigSections(base, edited);

  assert.deepEqual(merged.card_inventory, { extracted: ['a'] }); // read-only preserved
  assert.deepEqual(merged.programs, [{ id: 'p1' }]);             // untouched editable preserved
  assert.deepEqual(merged.feature_flags, { V5_SINGLE_PASS: true });
});

test('mergeConfigSections forces tenant_id from base even if the edit tries to change it', () => {
  const base = { tenant_id: 'REAL', version: '1.4.1' };
  const edited = { tenant_id: 'SPOOFED', tone_prompt: 'x' };

  const merged = mergeConfigSections(base, edited);

  assert.equal(merged.tenant_id, 'REAL');
  assert.equal(merged.tone_prompt, 'x'); // metadata field DID update
});

test('mergeConfigSections stamps a fresh last_updated ISO timestamp and defaults version', () => {
  const merged = mergeConfigSections({ tenant_id: 'T1' }, {});
  assert.match(merged.last_updated, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
  assert.equal(merged.version, '1.3'); // default when base has none
});

// ---------------------------------------------------------------------------
// validateEditedSections — WARN-only on unknown, BLOCK on read-only.
// ---------------------------------------------------------------------------

test('validateEditedSections WARNS-only on an unknown section (isValid stays true) — the drop is silent to the caller', () => {
  const res = validateEditedSections({ messenger_behavior: { escalation_email: 'x' } });
  // Critical: validation does NOT flag the section that the merge will silently
  // drop. Caller gets isValid:true, then the write loses the data.
  assert.equal(res.isValid, true);
  assert.deepEqual(res.errors, []);
});

test('validateEditedSections BLOCKS an attempt to edit a read-only section', () => {
  const res = validateEditedSections({ card_inventory: { extracted: [] } });
  assert.equal(res.isValid, false);
  assert.equal(res.errors.length, 1);
  assert.match(res.errors[0], /read-only/i);
});

test('validateEditedSections accepts a known editable section cleanly', () => {
  const res = validateEditedSections({ feature_flags: { MESSENGER_CHANNEL: true } });
  assert.equal(res.isValid, true);
  assert.deepEqual(res.errors, []);
});

// ---------------------------------------------------------------------------
// extractEditableSections — the read path back to the frontend.
// ---------------------------------------------------------------------------

test('extractEditableSections returns editable + metadata, excludes read-only and unknown', () => {
  const full = {
    tenant_id: 'T1',
    tone_prompt: 'hi',
    feature_flags: { V5_SINGLE_PASS: true },
    card_inventory: { extracted: [] }, // read-only → excluded
    messenger_behavior: { escalation_email: 'x' }, // unknown today → excluded
  };
  const editable = extractEditableSections(full);

  assert.equal(editable.tenant_id, 'T1');
  assert.deepEqual(editable.feature_flags, { V5_SINGLE_PASS: true });
  assert.equal('card_inventory' in editable, false);
  assert.equal('messenger_behavior' in editable, false); // T2a flips this
});

// ---------------------------------------------------------------------------
// Composite helpers used by the handler.
// ---------------------------------------------------------------------------

test('mergeMultipleSectionUpdates applies updates in sequence, last write wins per section', () => {
  const base = { tenant_id: 'T1' };
  const merged = mergeMultipleSectionUpdates(base, [
    { feature_flags: { V5_SINGLE_PASS: true } },
    { feature_flags: { V4_ACTION_SELECTOR: true } }, // replaces the whole section
  ]);
  assert.deepEqual(merged.feature_flags, { V4_ACTION_SELECTOR: true });
});

test('generateConfigDiff reports metadata + section changes and has_changes', () => {
  const oldC = { tone_prompt: 'a', feature_flags: { V5_SINGLE_PASS: false } };
  const newC = { tone_prompt: 'b', feature_flags: { V5_SINGLE_PASS: true } };
  const diff = generateConfigDiff(oldC, newC);
  assert.equal(diff.has_changes, true);
  assert.deepEqual(diff.metadata_changes.tone_prompt, { old: 'a', new: 'b' });
  assert.ok(diff.section_changes.feature_flags);
  assert.deepEqual(diff.section_changes.feature_flags.modified, ['V5_SINGLE_PASS']);
});

test('generateConfigDiff reports no changes for identical configs', () => {
  const c = { tone_prompt: 'a', feature_flags: { V5_SINGLE_PASS: true } };
  const diff = generateConfigDiff(c, { ...c, feature_flags: { ...c.feature_flags } });
  assert.equal(diff.has_changes, false);
});
