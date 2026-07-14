/**
 * welcomeRepush.test.mjs — server-side welcome-surface repush backstop.
 * Runner: node:test (matches mergeStrategy.test.mjs / the config_manager CI job).
 */

import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import { shouldRepushWelcome, maybeRepushWelcomeSurfaces } from './welcomeRepush.mjs';

const ORIG_FN = process.env.META_OAUTH_FUNCTION;
afterEach(() => {
  if (ORIG_FN === undefined) delete process.env.META_OAUTH_FUNCTION;
  else process.env.META_OAUTH_FUNCTION = ORIG_FN;
});

/** Mock LambdaClient capturing sent commands. */
function mockClient(onSend) {
  const calls = [];
  return {
    calls,
    send: async (cmd) => {
      calls.push(cmd);
      if (onSend) return onSend(cmd);
      return {};
    },
  };
}

// ── shouldRepushWelcome ─────────────────────────────────────────────────────

test('shouldRepushWelcome: true for ice breakers or persistent menu, false otherwise', () => {
  assert.equal(shouldRepushWelcome({ messenger_behavior: { welcome: { ice_breakers: [{ question: 'q', payload: 'p' }] } } }), true);
  assert.equal(shouldRepushWelcome({ messenger_behavior: { welcome: { persistent_menu: [{ title: 't' }] } } }), true);
  assert.equal(shouldRepushWelcome({ messenger_behavior: { welcome: {} } }), false);
  assert.equal(shouldRepushWelcome({ messenger_behavior: {} }), false);
  assert.equal(shouldRepushWelcome({}), false);
  assert.equal(shouldRepushWelcome(null), false);
});

// ── maybeRepushWelcomeSurfaces ──────────────────────────────────────────────

const WITH_SURFACES = { messenger_behavior: { welcome: { ice_breakers: [{ question: 'q', payload: 'PIC1:cta:x' }] } } };

test('invokes Meta_OAuth_Handler async with the repush-welcome event when configured + surfaces present', async () => {
  process.env.META_OAUTH_FUNCTION = 'Meta_OAuth_Handler';
  const client = mockClient();

  const res = await maybeRepushWelcomeSurfaces('MYR384719', WITH_SURFACES, { client });

  assert.deepEqual(res, { invoked: true });
  assert.equal(client.calls.length, 1);
  const input = client.calls[0].input;
  assert.equal(input.FunctionName, 'Meta_OAuth_Handler');
  assert.equal(input.InvocationType, 'Event'); // fire-and-forget
  const event = JSON.parse(Buffer.from(input.Payload).toString('utf8'));
  assert.equal(event.httpMethod, 'POST');
  assert.equal(event.path, '/meta/channels/MYR384719/repush-welcome');
});

test('no-ops (no invoke) when META_OAUTH_FUNCTION is unset', async () => {
  delete process.env.META_OAUTH_FUNCTION;
  const client = mockClient();
  const res = await maybeRepushWelcomeSurfaces('T', WITH_SURFACES, { client });
  assert.deepEqual(res, { skipped: 'META_OAUTH_FUNCTION unset' });
  assert.equal(client.calls.length, 0);
});

test('no-ops when the config has no welcome surfaces', async () => {
  process.env.META_OAUTH_FUNCTION = 'Meta_OAuth_Handler';
  const client = mockClient();
  const res = await maybeRepushWelcomeSurfaces('T', { messenger_behavior: { welcome: {} } }, { client });
  assert.deepEqual(res, { skipped: 'no welcome surfaces' });
  assert.equal(client.calls.length, 0);
});

test('best-effort: a client error is swallowed (never throws into the write path)', async () => {
  process.env.META_OAUTH_FUNCTION = 'Meta_OAuth_Handler';
  const client = mockClient(() => {
    throw new Error('AccessDenied');
  });
  const res = await maybeRepushWelcomeSurfaces('T', WITH_SURFACES, { client });
  assert.deepEqual(res, { error: 'AccessDenied' });
});
