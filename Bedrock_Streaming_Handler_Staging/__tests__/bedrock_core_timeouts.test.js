/**
 * CS1 — shared/bedrock-core.js KB-retrieve client timeout contract.
 *
 * The (non-streaming) Bedrock Retrieve call must fail fast on a hung
 * connection/response; retrieveKB() then catches and returns '' (fail-open),
 * so a KB hang stops the chat "typing" indefinitely instead of holding the
 * caller's Lambda open until its timeout.
 *
 * Asserts the exported config, not a live call — requiring the module
 * constructs the real client (no network), which is enough to pin the contract.
 */

const { KB_RETRIEVE_TIMEOUTS } = require('../../shared/bedrock-core');

describe('CS1 — bedrock-core KB-retrieve timeout contract', () => {
  test('KB_RETRIEVE_TIMEOUTS sets connect + request timeouts and enables abort', () => {
    expect(KB_RETRIEVE_TIMEOUTS).toBeDefined();
    expect(KB_RETRIEVE_TIMEOUTS.connectionTimeout).toBeGreaterThan(0);
    expect(KB_RETRIEVE_TIMEOUTS.requestTimeout).toBeGreaterThan(0);
    // Useful only if below the caller Lambda timeout (BSH is 300s).
    expect(KB_RETRIEVE_TIMEOUTS.requestTimeout).toBeLessThan(300000);
    // Without this the timeout only WARNS and never aborts — the CS1 trap.
    expect(KB_RETRIEVE_TIMEOUTS.throwOnRequestTimeout).toBe(true);
  });
});
