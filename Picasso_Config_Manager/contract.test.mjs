/**
 * contract.test.mjs — Config Manager side of the shared section contract
 * (Messenger Product Surface P0b).
 *
 * Asserts the server allowlist matches the contract's `cm_accepts` tier. The
 * SAME contract file is duplicated in the Config Builder repo
 * (src/lib/contracts/config_sections_contract.json); the two repos have
 * separate CI, so each side self-validates its own copy — there is NO automated
 * check that the copies match (reconcile by manual diff). See the contract
 * file's _doc.
 *
 * Runner: node:test. Contract read via fs to avoid JSON import-attribute
 * version differences across Node 20.x.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { getSectionInfo } from './mergeStrategy.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const contract = JSON.parse(
  readFileSync(join(__dirname, 'config_sections_contract.json'), 'utf8')
);

test('EDITABLE_SECTIONS matches the contract cm_accepts tier exactly (order-independent)', () => {
  const { editable } = getSectionInfo();
  assert.deepEqual([...editable].sort(), [...contract.cm_accepts].sort());
});

test('READ_ONLY_SECTIONS matches the contract read_only_sections', () => {
  const { readOnly } = getSectionInfo();
  assert.deepEqual([...readOnly].sort(), [...contract.read_only_sections].sort());
});

test('contract is internally consistent: cb_must_emit ⊆ cm_accepts, cb_not_emitted partitions the remainder', () => {
  const accepts = new Set(contract.cm_accepts);
  for (const s of contract.cb_must_emit) {
    assert.ok(accepts.has(s), `cb_must_emit "${s}" not in cm_accepts`);
  }
  const union = new Set([...contract.cb_must_emit, ...contract.cb_not_emitted.sections]);
  assert.equal(union.size, accepts.size);
  for (const s of contract.cm_accepts) {
    assert.ok(union.has(s), `cm_accepts "${s}" is neither cb_must_emit nor cb_not_emitted`);
  }
});
