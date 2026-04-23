import { test } from 'node:test';
import assert from 'node:assert/strict';

import { applyItemAtomically, applyOperation, persistState } from './applier.mjs';

// Local ConfigETagMismatchError double — persistState identifies it via
// `instanceof deps.ConfigETagMismatchError`, so the injected deps pass this class as their
// "mismatch error" class. This mirrors the real s3Ops export without the s3Ops dependency.
class FakeETagMismatchError extends Error {
  constructor(currentETag) {
    super('Fake ETag mismatch');
    this.name = 'FakeETagMismatchError';
    this.currentETag = currentETag;
  }
}

function freshState() {
  return {
    kb: '# KB\n\n<!-- section: events -->\n\n<!-- source: existing -->\n### Existing\ncontent',
    kbDirty: false,
    config: {
      tenant_id: 'T1',
      content_showcase: [{ id: 'existing_showcase' }],
      action_chips: { default_chips: { existing_chip: { label: 'Existing' } } },
    },
    configDirty: false,
  };
}

// ════════════════════════════════════════════════════════════════════════════════════════
// applyItemAtomically — per-item atomicity contract
//
// Invariant: if any op in an item throws, ALL of that item's in-memory mutations revert to
// the snapshot taken at item entry. Documented in applier.mjs; these tests are the
// executable specification.
// ════════════════════════════════════════════════════════════════════════════════════════

test('applyItemAtomically: all ops succeed → state mutates, itemFailed false', async () => {
  const state = freshState();
  const item = {
    id: 'happy',
    operations: [
      { verb: 'kb.append', afterMarker: '<!-- section: events -->', markdown: '### New\nbody' },
      { verb: 'config.add', path: 'content_showcase', value: { id: 'new_showcase' } },
    ],
  };

  const { itemFailed, opResults } = await applyItemAtomically(item, state, {}, applyOperation);

  assert.equal(itemFailed, false);
  assert.equal(opResults.length, 2);
  assert.equal(opResults[0].status, 'applied');
  assert.equal(opResults[1].status, 'applied');
  assert.ok(state.kbDirty);
  assert.ok(state.kb.includes('### New'));
  assert.ok(state.configDirty);
  assert.equal(state.config.content_showcase.length, 2);
});

test('applyItemAtomically: op #2 throws → KB and config REVERT, op #3 never runs', async () => {
  const state = freshState();
  const preKb = state.kb;
  const preShowcaseCount = state.config.content_showcase.length;
  const preChipCount = Object.keys(state.config.action_chips.default_chips).length;

  const item = {
    id: 'will-fail',
    operations: [
      { verb: 'kb.append', afterMarker: '<!-- section: events -->', markdown: '### SHOULD_REVERT\nbody' },
      { verb: 'kb.remove', sourceMarker: '<!-- source: nonexistent -->' },
      { verb: 'config.add', path: 'content_showcase', value: { id: 'should_not_appear' } },
    ],
  };

  const { itemFailed, opResults } = await applyItemAtomically(item, state, {}, applyOperation);

  assert.equal(itemFailed, true);
  assert.equal(opResults.length, 2, 'op 3 must not have been attempted');
  assert.equal(opResults[0].status, 'applied');
  assert.equal(opResults[1].status, 'error');
  assert.match(opResults[1].error, /not found/);
  assert.equal(state.kb, preKb);
  assert.equal(state.kbDirty, false);
  assert.equal(state.config.content_showcase.length, preShowcaseCount);
  assert.equal(Object.keys(state.config.action_chips.default_chips).length, preChipCount);
  assert.ok(!state.kb.includes('SHOULD_REVERT'));
  assert.ok(!state.config.content_showcase.some(s => s.id === 'should_not_appear'));
});

test('applyItemAtomically: first op throws → state byte-identical to pre-item', async () => {
  const state = freshState();
  const originalState = JSON.parse(JSON.stringify(state));

  const item = {
    id: 'fails-immediately',
    operations: [
      { verb: 'kb.remove', sourceMarker: '<!-- source: does-not-exist -->' },
      { verb: 'kb.append', afterMarker: '<!-- section: events -->', markdown: 'never' },
    ],
  };

  const { itemFailed, opResults } = await applyItemAtomically(item, state, {}, applyOperation);

  assert.equal(itemFailed, true);
  assert.equal(opResults.length, 1);
  assert.deepEqual(state, originalState);
});

test('applyItemAtomically: deep nested mutation reverts correctly (dict under action_chips)', async () => {
  const state = freshState();

  const item = {
    id: 'deep-mutation-then-fail',
    operations: [
      { verb: 'config.add', path: 'action_chips.default_chips', value: { label: 'Temp', value: '...', showcase_id: 'temp_sc' } },
      { verb: 'kb.remove', sourceMarker: '<!-- source: does-not-exist -->' },
    ],
  };

  await applyItemAtomically(item, state, {}, applyOperation);

  assert.equal(Object.keys(state.config.action_chips.default_chips).length, 1);
  assert.ok(!('temp_sc' in state.config.action_chips.default_chips));
  assert.ok('existing_chip' in state.config.action_chips.default_chips);
});

test('applyItemAtomically: injected applyOp simulates external-call failure (e.g. dub.upsert)', async () => {
  const state = freshState();
  const attempts = [];

  const flakeyApplyOp = async (op, s, _ctx) => {
    attempts.push(op.verb);
    if (op.verb === 'dub.upsert') throw new Error('simulated Dub 500');
    return applyOperation(op, s, _ctx);
  };

  const item = {
    id: 'flakey',
    operations: [
      { verb: 'config.add', path: 'content_showcase', value: { id: 'new_1' } },
      { verb: 'dub.upsert', url: 'https://example.org/x' },
    ],
  };

  const { itemFailed, opResults } = await applyItemAtomically(item, state, {}, flakeyApplyOp);

  assert.equal(itemFailed, true);
  assert.deepEqual(attempts, ['config.add', 'dub.upsert']);
  assert.equal(state.config.content_showcase.length, 1, 'showcase count reverted');
  assert.match(opResults[1].error, /simulated Dub 500/);
});

test('applyItemAtomically: empty operations list is a no-op success', async () => {
  const state = freshState();
  const pre = JSON.parse(JSON.stringify(state));
  const { itemFailed, opResults } = await applyItemAtomically({ id: 'empty' }, state, {}, applyOperation);
  assert.equal(itemFailed, false);
  assert.deepEqual(opResults, []);
  assert.deepEqual(state, pre);
});

// ════════════════════════════════════════════════════════════════════════════════════════
// persistState — KB write + ETag-safe config save + Bedrock sync
//
// Tests use injected fakes for writeKb/saveConfig/triggerBedrockSync so we can simulate
// success, ETag 409, permission failures, and Bedrock errors without touching S3.
// ════════════════════════════════════════════════════════════════════════════════════════

function dirtyState() {
  return {
    kb: 'updated KB contents',
    kbDirty: true,
    config: { tenant_id: 'T1', content_showcase: [{ id: 'new' }] },
    configDirty: true,
    configETag: '"cafef00d"',
  };
}

function captureDeps(overrides = {}) {
  const calls = [];
  const defaults = {
    writeKb: async (key, body) => { calls.push({ fn: 'writeKb', key, bodyLen: body.length }); },
    saveConfig: async (tenantId, config, etag) => {
      calls.push({ fn: 'saveConfig', tenantId, showcaseLen: config.content_showcase.length, etag });
      return { key: `tenants/${tenantId}/..`, etag: '"newetag"' };
    },
    triggerBedrockSync: async (config) => {
      calls.push({ fn: 'triggerBedrockSync', kbId: config?.aws?.knowledge_base_id });
      return { skipped: true, reason: 'no kbDataSourceId in fake config' };
    },
    ConfigETagMismatchError: FakeETagMismatchError,
  };
  return { deps: { ...defaults, ...overrides }, calls };
}

test('persistState: happy path writes KB, saves config, triggers Bedrock, no error', async () => {
  const state = dirtyState();
  const { deps, calls } = captureDeps();

  const result = await persistState({ tenantId: 'T1', kbKey: 'tenants/T1/kb.md', state, deps });

  assert.equal(result.persistFailed, false);
  assert.equal(result.configSaveError, null);
  assert.ok(result.bedrockSync);
  assert.deepEqual(calls.map(c => c.fn), ['writeKb', 'saveConfig', 'triggerBedrockSync']);
  assert.equal(calls[1].etag, '"cafef00d"', 'saveConfig called with the state ETag');
});

test('persistState: clean KB + clean config → no writes, no Bedrock sync', async () => {
  const state = { kb: 'x', kbDirty: false, config: {}, configDirty: false, configETag: '"x"' };
  const { deps, calls } = captureDeps();

  const result = await persistState({ tenantId: 'T1', kbKey: 'k', state, deps });

  assert.equal(result.persistFailed, false);
  assert.equal(result.bedrockSync, null);
  assert.deepEqual(calls, []);
});

test('persistState: dirty KB only (no config change) → KB written, Bedrock fired, saveConfig skipped', async () => {
  const state = { ...dirtyState(), configDirty: false };
  const { deps, calls } = captureDeps();

  const result = await persistState({ tenantId: 'T1', kbKey: 'k', state, deps });

  assert.equal(result.persistFailed, false);
  assert.deepEqual(calls.map(c => c.fn), ['writeKb', 'triggerBedrockSync']);
});

test('persistState: ETag mismatch (409) → configSaveError set, persistFailed true, KB still written, Bedrock still fires', async () => {
  const state = dirtyState();
  const { deps, calls } = captureDeps({
    saveConfig: async () => {
      calls.push({ fn: 'saveConfig-threw' });
      throw new FakeETagMismatchError('"live-etag-abc123"');
    },
  });

  const result = await persistState({ tenantId: 'T1', kbKey: 'k', state, deps });

  assert.equal(result.persistFailed, true);
  assert.match(result.configSaveError, /config_changed_externally/);
  assert.match(result.configSaveError, /live-etag-abc123/);
  assert.ok(calls.some(c => c.fn === 'writeKb'));
  assert.ok(calls.some(c => c.fn === 'triggerBedrockSync'));
});

test('persistState: non-ETag config error → raw message, persistFailed true', async () => {
  const state = dirtyState();
  const { deps } = captureDeps({
    saveConfig: async () => { throw new Error('S3 AccessDenied: not allowed to PutObject'); },
  });

  const result = await persistState({ tenantId: 'T1', kbKey: 'k', state, deps });

  assert.equal(result.persistFailed, true);
  assert.match(result.configSaveError, /AccessDenied/);
});

test('persistState: KB write throws → exception propagates (environment-level, not per-item)', async () => {
  const state = dirtyState();
  const { deps } = captureDeps({
    writeKb: async () => { throw new Error('S3 throttled'); },
  });

  await assert.rejects(
    () => persistState({ tenantId: 'T1', kbKey: 'k', state, deps }),
    /S3 throttled/,
  );
});

test('persistState: Bedrock sync error is surfaced in result, not thrown', async () => {
  const state = dirtyState();
  const { deps } = captureDeps({
    triggerBedrockSync: async () => ({ triggered: false, error: 'KB not found' }),
  });

  const result = await persistState({ tenantId: 'T1', kbKey: 'k', state, deps });

  assert.equal(result.persistFailed, false, 'Bedrock failure does NOT fail persist — KB + config landed');
  assert.equal(result.bedrockSync.triggered, false);
  assert.equal(result.bedrockSync.error, 'KB not found');
});
