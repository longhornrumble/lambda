/**
 * Tests for authorizeVerifiedPayload — the pure payload→auth mapping.
 *
 * Signature verification (verifyClerkJwt) is exercised live against the Clerk
 * JWKS and is not re-tested here; this covers the authorization decision made
 * on an ALREADY-verified payload, including the M2M machine-caller path.
 *
 * Run: node --test
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { authorizeVerifiedPayload } from './auth.mjs';

const ALLOWED_MACHINE = 'mch_3GeaXVgEBzXRJwT52bIfi9pESy0';

test('allowlisted M2M machine → super_admin service caller', () => {
  const r = authorizeVerifiedPayload({ sub: ALLOWED_MACHINE, scopes: [ALLOWED_MACHINE] });
  assert.equal(r.success, true);
  assert.equal(r.role, 'super_admin');
  assert.equal(r.service, true);
  assert.equal(r.tenant_id, null);
  assert.deepEqual(r.tenants, []);
  assert.equal(r.email, `m2m:${ALLOWED_MACHINE}`);
});

test('a NON-allowlisted machine is rejected', () => {
  const r = authorizeVerifiedPayload({ sub: 'mch_someOtherMachineXXXXXXXXXXX' });
  assert.equal(r.success, false);
  assert.match(r.error, /Unrecognized machine/);
});

test('SECURITY: a non-allowlisted machine CANNOT self-assert super_admin via a role claim', () => {
  // An M2M token's claims are attacker-controlled at mint time. Even with a
  // forged role claim, a machine that is not on the allowlist must be rejected —
  // the machine identity is the authorization, never the claim.
  const r = authorizeVerifiedPayload({ sub: 'mch_evilMachine000000000000000', role: 'super_admin' });
  assert.equal(r.success, false, 'forged role must not grant access');
});

test('SECURITY: even the allowlisted machine ignores a downgraded role claim', () => {
  // Role claims on machine tokens are ignored entirely; the allowlisted machine
  // is always super_admin regardless of any role value present.
  const r = authorizeVerifiedPayload({ sub: ALLOWED_MACHINE, role: 'member' });
  assert.equal(r.success, true);
  assert.equal(r.role, 'super_admin');
});

test('human super_admin token unaffected', () => {
  const r = authorizeVerifiedPayload({ sub: 'user_123', email: 'chris@myrecruiter.ai', role: 'super_admin' });
  assert.equal(r.success, true);
  assert.equal(r.role, 'super_admin');
  assert.equal(r.service, undefined);
  assert.equal(r.email, 'chris@myrecruiter.ai');
});

test('human member token maps to its tenant', () => {
  const r = authorizeVerifiedPayload({ sub: 'user_456', email: 'ops@example.org', role: 'member', tenant_id: 'BRI071351' });
  assert.equal(r.success, true);
  assert.equal(r.role, 'member');
  assert.deepEqual(r.tenants, ['BRI071351']);
  assert.equal(r.tenant_hash, null);
});

test('human token with no role defaults to member', () => {
  const r = authorizeVerifiedPayload({ sub: 'user_789', email: 'x@example.org' });
  assert.equal(r.role, 'member');
});
