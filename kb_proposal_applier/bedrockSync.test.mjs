import { test } from 'node:test';
import assert from 'node:assert/strict';

import { triggerBedrockSync } from './bedrockSync.mjs';

// These tests only exercise the config-gated skip paths — they never issue a real StartIngestionJob
// because the config is missing the required fields. The "live trigger" path is covered by the
// integration test against the MYR384719 sandbox, not here.

test('triggerBedrockSync skips when knowledge_base_id is missing', async () => {
  const result = await triggerBedrockSync({ aws: {}, monitor: { kbDataSourceId: 'abc' } });
  assert.equal(result.skipped, true);
  assert.match(result.reason, /knowledge_base_id/);
});

test('triggerBedrockSync skips when kbDataSourceId is missing', async () => {
  const result = await triggerBedrockSync({
    aws: { knowledge_base_id: 'KBXYZ' },
    monitor: {},
  });
  assert.equal(result.skipped, true);
  assert.match(result.reason, /kbDataSourceId/);
});

test('triggerBedrockSync skips when both are missing', async () => {
  const result = await triggerBedrockSync({});
  assert.equal(result.skipped, true);
});
