/**
 * streamTail unit tests — V5 single-pass tail parser (V5.1, pure module).
 *
 * The parser's three invariants under test:
 *   1. NO LEAK — no sentinel text (open marker onward) ever reaches feed()'s
 *      return value or end().remainingText.
 *   2. NO SWALLOW — spec-compliant prose is never dropped, only delayed by
 *      at most SENTINEL_OPEN.length - 1 chars.
 *   3. CHUNKING-INVARIANCE — the result is identical no matter how the
 *      stream is split into chunks (the chunk-boundary proof: every corpus
 *      is re-run at every chunk size from 1 char up).
 *
 * Plus the V5.1 DONE-line contract: index.js must NOT import this module
 * (nothing on the request path touches it until V5.5).
 */

const {
  createTailParser,
  SENTINEL_OPEN,
  SENTINEL_CLOSE,
} = require('../streamTail');

/** Feed `input` split into `size`-char chunks; return the combined result. */
function run(input, size = Infinity) {
  const parser = createTailParser();
  let forwarded = '';
  if (input.length === 0) {
    // still exercise a zero-feed stream
  } else if (size === Infinity) {
    forwarded += parser.feed(input);
  } else {
    for (let i = 0; i < input.length; i += size) {
      forwarded += parser.feed(input.slice(i, i + size));
    }
  }
  const { remainingText, actionIds, status, trailingAfterClose } = parser.end();
  return { text: forwarded + remainingText, forwarded, remainingText, actionIds, status, trailingAfterClose };
}

/** Run `input` at every chunk size from 1..len and assert identical results. */
function assertChunkingInvariant(input, expected) {
  for (let size = 1; size <= Math.max(1, input.length); size++) {
    const result = run(input, size);
    expect({ size, text: result.text, actionIds: result.actionIds, status: result.status })
      .toEqual({ size, ...expected });
  }
}

describe('streamTail — basic parsing', () => {
  test('no sentinel at all → all text forwarded, no_sentinel', () => {
    const result = run('Hello there! How can I help you today?');
    expect(result.text).toBe('Hello there! How can I help you today?');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('no_sentinel');
  });

  test('empty stream → empty text, no_sentinel', () => {
    const result = run('');
    expect(result.text).toBe('');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('no_sentinel');
  });

  test('valid sentinel at tail → prose forwarded, ids parsed, sentinel stripped', () => {
    const result = run(
      'Want to grab a discovery-session spot?\n<<<ACTIONS ["query_discoverysession","apply_daretodream_volunteer"]>>>'
    );
    expect(result.text).toBe('Want to grab a discovery-session spot?\n');
    expect(result.actionIds).toEqual(['query_discoverysession', 'apply_daretodream_volunteer']);
    expect(result.status).toBe('actions');
  });

  test('empty array (restraint case) → actionIds [], status actions', () => {
    const result = run('Happy to help!\n<<<ACTIONS []>>>');
    expect(result.text).toBe('Happy to help!\n');
    expect(result.actionIds).toEqual([]);
    expect(result.status).toBe('actions');
  });

  test('sentinel with no leading prose → empty text, ids parsed', () => {
    const result = run('<<<ACTIONS ["a"]>>>');
    expect(result.text).toBe('');
    expect(result.actionIds).toEqual(['a']);
    expect(result.status).toBe('actions');
  });

  test('no whitespace between marker and array is accepted', () => {
    const result = run('Done.<<<ACTIONS["a"]>>>');
    expect(result.text).toBe('Done.');
    expect(result.actionIds).toEqual(['a']);
  });

  test('extra inline whitespace around the array is accepted', () => {
    const result = run('Done.<<<ACTIONS   ["a","b"]  >>>');
    expect(result.actionIds).toEqual(['a', 'b']);
    expect(result.status).toBe('actions');
  });

  test('lowercase marker is prose, not a sentinel', () => {
    const input = 'try <<<actions ["a"]>>> ok';
    const result = run(input);
    expect(result.text).toBe(input);
    expect(result.status).toBe('no_sentinel');
  });
});

describe('streamTail — chunk-boundary splits', () => {
  const prose = 'Here is what I found for you.\n';
  const sentinel = '<<<ACTIONS ["query_x","apply_y"]>>>';

  test('sentinel split across 2 chunks (inside the open marker)', () => {
    const parser = createTailParser();
    const out1 = parser.feed(prose + '<<<ACT');
    const out2 = parser.feed('IONS ["query_x","apply_y"]>>>');
    const { remainingText, actionIds, status } = parser.end();
    expect(out1 + out2 + remainingText).toBe(prose);
    expect(actionIds).toEqual(['query_x', 'apply_y']);
    expect(status).toBe('actions');
  });

  test('sentinel split across 3 chunks (marker / JSON / close each split)', () => {
    const parser = createTailParser();
    const out1 = parser.feed(prose + '<<<ACTIO');
    const out2 = parser.feed('NS ["query_x","app');
    const out3 = parser.feed('ly_y"]>>');
    const out4 = parser.feed('>');
    const { remainingText, actionIds, status } = parser.end();
    expect(out1 + out2 + out3 + out4 + remainingText).toBe(prose);
    expect(actionIds).toEqual(['query_x', 'apply_y']);
    expect(status).toBe('actions');
  });

  test('chunking-invariance: prose + sentinel, every chunk size', () => {
    assertChunkingInvariant(prose + sentinel, {
      text: prose,
      actionIds: ['query_x', 'apply_y'],
      status: 'actions',
    });
  });

  test('chunking-invariance: plain prose with sentinel-like fragments', () => {
    const input = 'Compare a<<b, then <<<ACTION items, and x << y. Done <<';
    assertChunkingInvariant(input, {
      text: input,
      actionIds: null,
      status: 'no_sentinel',
    });
  });

  test('chunking-invariance: empty-array sentinel', () => {
    assertChunkingInvariant('Thanks!<<<ACTIONS []>>>', {
      text: 'Thanks!',
      actionIds: [],
      status: 'actions',
    });
  });
});

describe('streamTail — holdback bounds (no swallow, no leak)', () => {
  test('prose is never delayed by more than SENTINEL_OPEN.length - 1 chars', () => {
    const input = 'A perfectly normal reply about programs < and volunteering <<< with angle brackets.';
    const parser = createTailParser();
    let fed = 0;
    let released = 0;
    for (const ch of input) {
      fed += 1;
      released += parser.feed(ch).length;
      expect(fed - released).toBeLessThanOrEqual(SENTINEL_OPEN.length - 1);
    }
    const { remainingText, status } = parser.end();
    released += remainingText.length;
    expect(released).toBe(fed);
    expect(status).toBe('no_sentinel');
  });

  test('sentinel-like prose that never completes is released, not swallowed', () => {
    // '<<<ACTION' is a live prefix until the next char diverges from 'S'
    const result = run('Use <<<ACTION brackets for emphasis', 4);
    expect(result.text).toBe('Use <<<ACTION brackets for emphasis');
    expect(result.status).toBe('no_sentinel');
  });

  test('live marker prefix at end of stream is released by end()', () => {
    const parser = createTailParser();
    const out = parser.feed('The answer is <<<ACTIO');
    expect(out).toBe('The answer is ');
    const { remainingText, status } = parser.end();
    expect(remainingText).toBe('<<<ACTIO');
    expect(status).toBe('no_sentinel');
  });

  test('re-arming: diverged prefix releases while a new live prefix holds', () => {
    const parser = createTailParser();
    // '<<<ACT' diverges (followed by '<'), '<<<A' is live again
    const out = parser.feed('x<<<ACT<<<A');
    expect(out).toBe('x<<<ACT');
    const { remainingText } = parser.end();
    expect(remainingText).toBe('<<<A');
  });

  test('no sentinel text ever leaks across all chunk sizes', () => {
    const input = 'Prose first.\n<<<ACTIONS ["a","b"]>>>';
    for (let size = 1; size <= input.length; size++) {
      const { text } = run(input, size);
      expect(text).not.toContain(SENTINEL_OPEN);
      expect(text).not.toContain(SENTINEL_CLOSE);
      expect(text).not.toContain('"a"');
    }
  });
});

describe('streamTail — malformed blocks (fail-soft signals)', () => {
  test('malformed JSON → malformed, block dropped, prose intact', () => {
    const result = run('Here you go.\n<<<ACTIONS [broken json>>>');
    expect(result.text).toBe('Here you go.\n');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('non-string array entries → malformed', () => {
    const result = run('Done.<<<ACTIONS [1,2]>>>');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('JSON object instead of array → malformed', () => {
    const result = run('Done.<<<ACTIONS {"a":1}>>>');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('bracketed but invalid JSON → malformed (parse-throw path)', () => {
    const result = run('Done.<<<ACTIONS [invalid]>>>');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('bare JSON string instead of array → malformed', () => {
    const result = run('Done.<<<ACTIONS "hello">>>');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('stream ends inside an unclosed block → malformed, block never leaks', () => {
    const result = run('Prose stays.\n<<<ACTIONS ["a"');
    expect(result.text).toBe('Prose stays.\n');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('stream ends right after the bare open marker → malformed', () => {
    const result = run('Prose stays.<<<ACTIONS');
    expect(result.text).toBe('Prose stays.');
    expect(result.status).toBe('malformed');
  });

  test('newline inside the block disqualifies it — marker dropped, prose after resumes', () => {
    const result = run('Before.<<<ACTIONS\nAfter the fake marker.');
    expect(result.text).toBe('Before.\nAfter the fake marker.');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('newline divergence is chunking-invariant', () => {
    assertChunkingInvariant('Before.<<<ACTIONS [half\nAfter.', {
      text: 'Before.\nAfter.',
      actionIds: null,
      status: 'malformed',
    });
  });

  test('spurious marker swallows at most one line — prose resumes after the newline', () => {
    const garbage = 'x'.repeat(800); // long single-line block, never closes
    const result = run('Start.<<<ACTIONS ' + garbage + '\nProse resumes here.', 7);
    expect(result.text).toBe('Start.\nProse resumes here.');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('unclosed single-line block running to stream end is dropped whole', () => {
    const result = run('Start.<<<ACTIONS ' + 'y'.repeat(800));
    expect(result.text).toBe('Start.');
    expect(result.status).toBe('malformed');
  });

  test('a long but valid id array still parses (caller enforces the CTA cap)', () => {
    const ids = Array.from({ length: 30 }, (unused, i) => `query_id_${i}`);
    const result = run(`Done.<<<ACTIONS ${JSON.stringify(ids)}>>>`);
    expect(result.actionIds).toEqual(ids);
    expect(result.status).toBe('actions');
  });
});

describe('streamTail — trailing content after a sentinel', () => {
  test('trailing garbage after a valid sentinel is forwarded as prose, ids kept, flagged', () => {
    const result = run('Done.<<<ACTIONS ["a"]>>> trailing words');
    expect(result.text).toBe('Done. trailing words');
    expect(result.actionIds).toEqual(['a']);
    expect(result.status).toBe('actions');
    expect(result.trailingAfterClose).toBe(true);
  });

  test('clean sentinel at the tail → trailingAfterClose false', () => {
    const result = run('Done.\n<<<ACTIONS ["a"]>>>');
    expect(result.trailingAfterClose).toBe(false);
  });

  test('whitespace-only after the close does not flag trailing prose', () => {
    const result = run('Done.<<<ACTIONS ["a"]>>>\n');
    expect(result.text).toBe('Done.\n');
    expect(result.actionIds).toEqual(['a']);
    expect(result.trailingAfterClose).toBe(false);
  });

  test('over-closed marker (>>>>) leaves the extra > as prose, flagged', () => {
    const result = run('Done.<<<ACTIONS ["a"]>>>>');
    expect(result.text).toBe('Done.>');
    expect(result.actionIds).toEqual(['a']);
    expect(result.trailingAfterClose).toBe(true);
  });

  test('close before a later newline in one chunk → block parses, newline is prose', () => {
    const result = run('X<<<ACTIONS ["a"]>>>\nmore prose');
    expect(result.text).toBe('X\nmore prose');
    expect(result.actionIds).toEqual(['a']);
    expect(result.status).toBe('actions');
    expect(result.trailingAfterClose).toBe(true);
  });

  test('newline before a later close in one chunk → divergence wins, stray >>> is prose', () => {
    const result = run('X<<<ACTIONS [junk\nprose with >>> in it');
    expect(result.text).toBe('X\nprose with >>> in it');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('two valid sentinels → last one wins, both stripped', () => {
    const result = run('A.<<<ACTIONS ["first"]>>> mid <<<ACTIONS ["second"]>>>');
    expect(result.text).toBe('A. mid ');
    expect(result.actionIds).toEqual(['second']);
    expect(result.status).toBe('actions');
  });
});

describe('streamTail — the LAST marker attempt decides (retrospective blocker #1)', () => {
  test('valid sentinel then a malformed attempt → malformed, earlier capture discarded', () => {
    // The later attempt is the model's final decision; if it is corrupted
    // (self-correction cut off by max_tokens), serving the abandoned first
    // draft while reporting success would blind the fail-soft counter.
    const result = run('A.<<<ACTIONS ["keep"]>>><<<ACTIONS [junk>>>');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('malformed attempt then a valid sentinel → the correction is served', () => {
    const result = run('A.<<<ACTIONS [junk>>><<<ACTIONS ["good"]>>>');
    expect(result.actionIds).toEqual(['good']);
    expect(result.status).toBe('actions');
  });

  test('valid sentinel then an unclosed attempt at stream end → malformed', () => {
    const result = run('A.<<<ACTIONS ["keep"]>>><<<ACTIONS ["cut');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('valid sentinel then a newline-diverged attempt → malformed', () => {
    const result = run('A.<<<ACTIONS ["keep"]>>><<<ACTIONS\nafter');
    expect(result.actionIds).toBeNull();
    expect(result.status).toBe('malformed');
  });

  test('last-attempt-decides is chunking-invariant', () => {
    assertChunkingInvariant('A.<<<ACTIONS ["keep"]>>><<<ACTIONS [junk>>>', {
      text: 'A.',
      actionIds: null,
      status: 'malformed',
    });
  });
});

describe('streamTail — unicode and emoji', () => {
  test('emoji-laden prose passes through untouched', () => {
    const input = 'Great choice! 🎉 We’d love to have you 💚 — café, naïve, 中文.';
    assertChunkingInvariant(input, {
      text: input,
      actionIds: null,
      status: 'no_sentinel',
    });
  });

  test('emoji prose with a sentinel tail, every chunk size (surrogate pairs split)', () => {
    // Chunk sizes of 1 split every surrogate pair across feeds; the combined
    // output must still be byte-identical prose.
    const prose = 'Sounds fun! 🎉🎊 Let’s do it 💪.\n';
    assertChunkingInvariant(prose + '<<<ACTIONS ["apply_y"]>>>', {
      text: prose,
      actionIds: ['apply_y'],
      status: 'actions',
    });
  });

  test('emoji inside the id array parses (validation is the caller’s job)', () => {
    const result = run('Done.<<<ACTIONS ["🎉_id"]>>>');
    expect(result.actionIds).toEqual(['🎉_id']);
  });
});

