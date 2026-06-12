import { test } from 'node:test';
import assert from 'node:assert/strict';

import { appendTokenToUrl } from './index.mjs';

// appendTokenToUrl is a pure helper — no network, no SES, no Clerk. The
// per-recipient email dispatch that uses it touches fetch + SES; that path is
// covered by the live integration test documented in the PR description
// (POST to the deployed Function URL → verify emails arrive with tokenized
// URLs). Unit tests here keep the pure logic honest.

test('appendTokenToUrl: URL without query → uses ? separator', () => {
  const out = appendTokenToUrl('https://example.com/pending-changes', 'abc123');
  assert.equal(out, 'https://example.com/pending-changes?token=abc123');
});

test('appendTokenToUrl: URL with existing query → uses & separator', () => {
  const out = appendTokenToUrl('https://example.com/pending-changes?h=hash123', 'abc');
  assert.equal(out, 'https://example.com/pending-changes?h=hash123&token=abc');
});

test('appendTokenToUrl: token is URI-encoded to avoid special-char breakage', () => {
  // Real Clerk tokens are JWTs (base64 + dot) which happen to be URL-safe, but
  // hash/equals/plus/slash COULD appear in other opaque identifiers; encoding
  // future-proofs the function against upstream token-format changes.
  const out = appendTokenToUrl('https://example.com/x', 'a+b/c=d?e&f');
  assert.equal(out, 'https://example.com/x?token=a%2Bb%2Fc%3Dd%3Fe%26f');
});

test('appendTokenToUrl: URL with fragment preserves fragment position after append', () => {
  // Not a typical case (action URLs shouldn't carry fragments) but document
  // the behaviour explicitly so no one is surprised if a fragment slips in.
  // Simple implementation pushes the fragment to the end of the query —
  // that IS a behaviour change vs a URL parser, which we're accepting as a
  // pure-string trade-off. A real fragment would need upstream fixing.
  const out = appendTokenToUrl('https://example.com/x#section', 'abc');
  assert.equal(out, 'https://example.com/x#section?token=abc');
});

test('appendTokenToUrl: falsy URL passes through (defensive)', () => {
  assert.equal(appendTokenToUrl('', 'tok'), '');
  assert.equal(appendTokenToUrl(null, 'tok'), null);
  assert.equal(appendTokenToUrl(undefined, 'tok'), undefined);
});

// DEFAULT_SENDER is a module-level env read. Query-string imports give each
// test a fresh module evaluation (separate ESM cache entry) so both env
// states can be exercised in one process.

test('DEFAULT_SENDER: env set → env used, no SENDER_ENV_MISSING warning', async () => {
  const original = process.env.DEFAULT_SENDER;
  const warnings = [];
  const originalWarn = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));
  try {
    process.env.DEFAULT_SENDER = 'notify@staging.myrecruiter.ai';
    const mod = await import('./index.mjs?sender-env-set');
    assert.equal(mod.DEFAULT_SENDER, 'notify@staging.myrecruiter.ai');
    assert.ok(!warnings.some((w) => w.includes('SENDER_ENV_MISSING')));
  } finally {
    console.warn = originalWarn;
    if (original === undefined) delete process.env.DEFAULT_SENDER;
    else process.env.DEFAULT_SENDER = original;
  }
});

test('DEFAULT_SENDER: env missing → hardcoded fallback + loud warning', async () => {
  const original = process.env.DEFAULT_SENDER;
  const warnings = [];
  const originalWarn = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));
  try {
    delete process.env.DEFAULT_SENDER;
    const mod = await import('./index.mjs?sender-env-missing');
    assert.equal(mod.DEFAULT_SENDER, 'notify@myrecruiter.ai');
    assert.ok(warnings.some((w) => w.includes('SENDER_ENV_MISSING')));
  } finally {
    console.warn = originalWarn;
    if (original === undefined) delete process.env.DEFAULT_SENDER;
    else process.env.DEFAULT_SENDER = original;
  }
});
