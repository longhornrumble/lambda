import { test } from 'node:test';
import assert from 'node:assert/strict';

import { applyAdd, applyDelete, applyAppendToArray } from './configOps.mjs';

function baseConfig() {
  return {
    tenant_id: 'MYR384719',
    content_showcase: [
      { id: 'golf_2025', enabled: true, type: 'event', name: '2025 Golf Tournament' },
    ],
    action_chips: {
      default_chips: [
        { label: 'Golf', value: 'Tell me about the 2025 Golf Tournament', showcase_id: 'golf_2025' },
      ],
    },
  };
}

test('applyAdd appends to content_showcase', () => {
  const c = baseConfig();
  applyAdd(c, 'content_showcase', { id: 'gala_2026', enabled: true, type: 'event' });
  assert.equal(c.content_showcase.length, 2);
  assert.equal(c.content_showcase[1].id, 'gala_2026');
});

test('applyAdd works with nested path action_chips.default_chips', () => {
  const c = baseConfig();
  applyAdd(c, 'action_chips.default_chips', { label: 'Gala', value: '…', showcase_id: 'gala_2026' });
  assert.equal(c.action_chips.default_chips.length, 2);
  assert.equal(c.action_chips.default_chips[1].showcase_id, 'gala_2026');
});

test('applyAdd initializes missing array (retired_showcase_ids)', () => {
  const c = baseConfig();
  applyAppendToArray(c, 'retired_showcase_ids', { id: 'golf_2024', retiredAt: '2025-05-01' });
  assert.deepEqual(c.retired_showcase_ids, [{ id: 'golf_2024', retiredAt: '2025-05-01' }]);
});

test('applyDelete removes matching showcase by id', () => {
  const c = baseConfig();
  applyDelete(c, 'content_showcase', { id: 'golf_2025' });
  assert.equal(c.content_showcase.length, 0);
});

test('applyDelete removes chip by showcase_id match', () => {
  const c = baseConfig();
  applyDelete(c, 'action_chips.default_chips', { showcase_id: 'golf_2025' });
  assert.equal(c.action_chips.default_chips.length, 0);
});

test('applyDelete throws when no items match', () => {
  const c = baseConfig();
  assert.throws(
    () => applyDelete(c, 'content_showcase', { id: 'nonexistent' }),
    /found no items matching/,
  );
});

test('applyAdd throws if target is a primitive', () => {
  const c = { tenant_id: 'MYR384719', bedrock_instructions: 'foo' };
  assert.throws(
    () => applyAdd(c, 'bedrock_instructions', 'bar'),
    /must be array or dict/,
  );
});

// ─── Dict support (the real action_chips.default_chips shape) ──────────────────────────────

function dictConfig() {
  // Mirrors production MYR384719 structure: action_chips.default_chips is a DICT keyed by
  // chip slug, not an array. This is what the Applier has to handle.
  return {
    tenant_id: 'MYR384719',
    content_showcase: [],
    action_chips: {
      enabled: true,
      default_chips: {
        how_can_i_volunteer: { label: 'Learn about Mentoring', action: 'send_query', value: '...' },
        our_programs: { label: 'Sponsor a Family', action: 'send_query', value: '...' },
      },
    },
  };
}

test('applyAdd inserts into dict path using value.showcase_id as key', () => {
  const c = dictConfig();
  applyAdd(c, 'action_chips.default_chips', {
    label: 'Golf',
    value: 'Tell me about the 2026 Golf Tournament',
    showcase_id: 'golf_2026',
  });
  assert.equal(Object.keys(c.action_chips.default_chips).length, 3);
  assert.equal(c.action_chips.default_chips.golf_2026.label, 'Golf');
});

test('applyAdd inserts into dict path using explicit op.key', () => {
  const c = dictConfig();
  applyAdd(c, 'action_chips.default_chips',
    { label: 'Gala', action: 'send_query', value: '...' },
    { key: 'spring_gala' },
  );
  assert.equal(c.action_chips.default_chips.spring_gala.label, 'Gala');
});

test('applyAdd on dict throws when no key can be derived', () => {
  const c = dictConfig();
  assert.throws(
    () => applyAdd(c, 'action_chips.default_chips', { label: 'No key', value: '...' }),
    /op.key or value.id/,
  );
});

test('applyDelete removes dict entry by matchBy criteria', () => {
  const c = dictConfig();
  applyDelete(c, 'action_chips.default_chips', { label: 'Learn about Mentoring' });
  assert.equal(Object.keys(c.action_chips.default_chips).length, 1);
  assert.ok(!c.action_chips.default_chips.how_can_i_volunteer);
  assert.ok(c.action_chips.default_chips.our_programs);
});

test('applyDelete removes dict entry by explicit key shorthand', () => {
  const c = dictConfig();
  applyDelete(c, 'action_chips.default_chips', { key: 'our_programs' });
  assert.equal(Object.keys(c.action_chips.default_chips).length, 1);
  assert.ok(!c.action_chips.default_chips.our_programs);
});

test('applyDelete on dict throws when no entry matches', () => {
  const c = dictConfig();
  assert.throws(
    () => applyDelete(c, 'action_chips.default_chips', { showcase_id: 'nonexistent' }),
    /found no entries matching/,
  );
});
