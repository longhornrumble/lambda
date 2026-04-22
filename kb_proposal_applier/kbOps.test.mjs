import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  appendAfterMarker,
  replaceBySourceMarker,
  removeBySourceMarker,
} from './kbOps.mjs';

const KB = `# Tenant KB

<!-- section: events -->
## Events

<!-- source: https://example.org/golf-2025 -->
### 2025 Golf Tournament
- Date: April 20, 2025
- Venue: The Clubs at Houston Oaks

<!-- source: https://example.org/gala-2026 -->
### 2026 Spring Gala
- Date: March 15, 2026

<!-- section: leadership -->
## Leadership

<!-- source: https://example.org/team#jane -->
### Jane Doe | Executive Director
Founded the org in 2019.
`;

test('appendAfterMarker inserts after the section marker', () => {
  const updated = appendAfterMarker(
    KB,
    '<!-- section: leadership -->',
    '<!-- source: https://example.org/team#sarah -->\n### Sarah Chen | Director of Programs\nJoined 2026.',
  );
  // New content should sit between the leadership marker and the next source block.
  const leadershipIdx = updated.indexOf('<!-- section: leadership -->');
  const sarahIdx = updated.indexOf('Sarah Chen');
  const janeIdx = updated.indexOf('Jane Doe');
  assert.ok(leadershipIdx < sarahIdx, 'Sarah should come after leadership marker');
  assert.ok(sarahIdx < janeIdx, 'Sarah should come before Jane (inserted at top of section)');
});

test('appendAfterMarker throws if marker not found', () => {
  assert.throws(
    () => appendAfterMarker(KB, '<!-- section: nonexistent -->', 'x'),
    /Marker not found/,
  );
});

test('replaceBySourceMarker replaces only the targeted source block', () => {
  const updated = replaceBySourceMarker(
    KB,
    '<!-- source: https://example.org/golf-2025 -->',
    '### 2025 Golf Tournament (UPDATED)\n- Date: April 20, 2025 — POSTPONED',
  );
  assert.ok(updated.includes('UPDATED'), 'updated content present');
  assert.ok(updated.includes('2026 Spring Gala'), 'sibling block preserved');
  assert.ok(updated.includes('Jane Doe'), 'leadership block preserved');
  // Source marker still present (preserved for retraceability).
  assert.ok(updated.includes('<!-- source: https://example.org/golf-2025 -->'));
  // Old body is gone.
  assert.ok(!updated.includes('Houston Oaks'));
});

test('removeBySourceMarker drops the entire block including the marker', () => {
  const updated = removeBySourceMarker(
    KB,
    '<!-- source: https://example.org/golf-2025 -->',
  );
  assert.ok(!updated.includes('2025 Golf Tournament'), 'target heading removed');
  assert.ok(!updated.includes('Houston Oaks'), 'target body removed');
  assert.ok(!updated.includes('<!-- source: https://example.org/golf-2025 -->'), 'marker removed');
  assert.ok(updated.includes('2026 Spring Gala'), 'sibling preserved');
  assert.ok(updated.includes('<!-- section: events -->'), 'parent section marker preserved');
});

test('replaceBySourceMarker works on the last source block (EOF terminator)', () => {
  const updated = replaceBySourceMarker(
    KB,
    '<!-- source: https://example.org/team#jane -->',
    '### Jane Doe | Co-Founder and CEO\nTitle change 2026.',
  );
  assert.ok(updated.includes('Co-Founder and CEO'));
  assert.ok(!updated.includes('Founded the org in 2019'));
});

test('removeBySourceMarker does NOT collapse adjacent markers when called consecutively', () => {
  // Regression test: earlier implementation stripped one leading \n per call. When two
  // adjacent blocks with the same source marker both got removed, the second call consumed
  // the newline between the section marker and the next sibling source marker, producing
  // `<!-- section: x --><!-- source: y -->` on a single line.
  const duplicated =
    '<!-- section: events -->\n\n' +
    '<!-- source: https://example.org/dupe -->\n' +
    '### Dupe Block 1\ncontent1\n\n' +
    '<!-- source: https://example.org/dupe -->\n' +
    '### Dupe Block 2\ncontent2\n\n' +
    '<!-- source: https://example.org/legit -->\n' +
    '### Legit Block\nkeep me\n';

  let after = removeBySourceMarker(duplicated, '<!-- source: https://example.org/dupe -->');
  after = removeBySourceMarker(after, '<!-- source: https://example.org/dupe -->');

  // Critical assertion: the section marker must not be directly concatenated with the legit
  // source marker. They need a newline between them at minimum.
  assert.ok(
    !after.includes('<!-- section: events --><!-- source:'),
    `adjacent markers collapsed onto one line:\n${after}`,
  );
  // And the legit block is untouched.
  assert.ok(after.includes('Legit Block'));
  assert.ok(after.includes('keep me'));
  // Both dupe blocks gone.
  assert.ok(!after.includes('Dupe Block'));
});
