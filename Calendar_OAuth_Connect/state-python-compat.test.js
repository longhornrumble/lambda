'use strict';

// Cross-language wire-format proof: the integrator's init-token MINT lives in the Python
// Analytics_Dashboard_API (G3/E0), but THIS Lambda's state.verify (Node) must accept its tokens.
// The GOLDEN below was produced by the Python mint with a fixed key + fixed clock + fixed nonce
// (Analytics_Dashboard_API/test_scheduling_connection_init.py::test_golden_token_is_stable).
// Running the REAL state.verify on it proves the two implementations agree byte-for-byte. If the
// Python mint ever changes the wire format, its golden test fails AND this one fails.

const state = require('./state');

// Must match Analytics_Dashboard_API/test_scheduling_connection_init.py KEY + NOW_MS + FIXED_JTI.
// Regenerated 2026-06-11 (Track 2 / Beta-blocker §E0): payload now includes `jti` claim.
// FIXED_JTI = 'aaaabbbbccccdddd0000111122223333' (32 hex chars, pinned in the Python test).
const KEY = 'test-signing-key-deadbeef';
const deps = { getKey: () => KEY };
const GOLDEN =
  'eyJ0eXAiOiJpbml0IiwidGVuYW50X2lkIjoiVEVOMSIsImNvb3JkaW5hdG9yX2lkIjoic3RhZmZAZXhh' +
  'bXBsZS5jb20iLCJjb29yZGluYXRvcl9lbWFpbCI6InN0YWZmQGV4YW1wbGUuY29tIiwiaWF0IjoxOTAw' +
  'MDAwMDAwLCJleHAiOjE5MDAwMDAzMDAsImp0aSI6ImFhYWFiYmJiY2NjY2RkZGQwMDAwMTExMTIyMjIz' +
  'MzMzIiwibm9uY2UiOiIwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMCJ9' +
  '.lRV7ovW26MAZbaweSag_t6hPLEUrGBsP6OeEHN6IdmA';

beforeEach(() => state._resetKeyCache());

describe('state.verify accepts a Python-ADA-minted init token (G3/E0 cross-language compat)', () => {
  test('verifies and returns the minted claims', async () => {
    // nowMs is iat+100s, well inside the 300s TTL (exp = 1_900_000_300).
    const claims = await state.verify(GOLDEN, { expectedType: 'init', nowMs: 1_900_000_100_000, deps });
    expect(claims.typ).toBe('init');
    expect(claims.tenant_id).toBe('TEN1');
    expect(claims.coordinator_id).toBe('staff@example.com');
    expect(claims.coordinator_email).toBe('staff@example.com');
    expect(claims.iat).toBe(1_900_000_000);
    expect(claims.exp).toBe(1_900_000_300);
    expect(claims.nonce).toHaveLength(32);
    // jti claim (Track 2 / Beta-blocker §E0): Python mint now includes it for single-use enforcement.
    expect(claims.jti).toBe('aaaabbbbccccdddd0000111122223333');
  });

  test('rejects the same golden under the wrong key (bad_signature)', async () => {
    await expect(
      state.verify(GOLDEN, { expectedType: 'init', nowMs: 1_900_000_100_000, deps: { getKey: () => 'wrong-key' } })
    ).rejects.toMatchObject({ code: 'bad_signature' });
  });

  test('rejects the golden once past its TTL (expired)', async () => {
    await expect(
      state.verify(GOLDEN, { expectedType: 'init', nowMs: 1_900_000_301_000, deps })
    ).rejects.toMatchObject({ code: 'expired' });
  });

  test('rejects the Python-minted init token when verified as the wrong type (wrong_type)', async () => {
    // The typ-confusion defense (an 'init' token can never be replayed as a 'state' token) must
    // hold at the cross-language boundary — the Node verifier consuming a Python-minted token.
    // (G3 audit test-B3.)
    await expect(
      state.verify(GOLDEN, { expectedType: 'state', nowMs: 1_900_000_100_000, deps })
    ).rejects.toMatchObject({ code: 'wrong_type' });
  });
});
