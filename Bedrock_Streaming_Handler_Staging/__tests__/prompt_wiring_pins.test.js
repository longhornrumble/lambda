'use strict';

/**
 * Source-pin wiring contracts for the shared prompt trio (extracted from
 * prompt_v5.test.js / stream_tail.test.js when the modules moved to
 * shared/prompt in M2). These pin BSH's OWN wiring — the pure-module
 * suites live in shared/prompt/__tests__/.
 */

describe('V5.5 wired contract (1a source-pin pattern)', () => {
  // V5.5 wired the merged prompt into the request path. Pin BOTH duplicated
  // prompt call sites (streaming + buffered handler blocks): each must build
  // the V5 prompt behind the flag with the exact V4-mirroring signature, and
  // the V5 branch must sit BEFORE V4_ACTION_SELECTOR in both CTA chains
  // (tenants carry both flags — appended-after would make the V5 flip a no-op).
  test('index.js builds the V5 prompt at BOTH handler call sites', () => {
    const fs = require('fs');
    const source = fs.readFileSync(require.resolve('../index.js'), 'utf8');
    expect(source).toContain("require('../shared/prompt/prompt_v5')");
    const v5Sites = source.match(/buildV5TurnPrompt\(sanitizedInput, kbContext, tonePrompt, conversationHistory, config, body\.session_context \|\| \{\}\)/g) || [];
    expect(v5Sites).toHaveLength(2);
  });

  test('the V5 CTA branch precedes V4_ACTION_SELECTOR in the shared pipeline (both handlers delegate)', () => {
    const fs = require('fs');
    const index = fs.readFileSync(require.resolve('../index.js'), 'utf8');
    const pipeline = fs.readFileSync(require.resolve('../responsePipeline.js'), 'utf8');
    // Post-dedup the CTA ladder lives ONCE, in responsePipeline.js — a STRONGER invariant than
    // the old two-copy grep (the ordering can no longer drift between the twins). The v5Active
    // arm must precede the V4 flag check there (tenants carry both flags; appended-after would
    // make the V5 flip a no-op).
    const ordered = pipeline.match(/} else if \(v5Active\) \{[\s\S]*?} else if \(config\.feature_flags\?\.V4_ACTION_SELECTOR\) \{/g) || [];
    expect(ordered).toHaveLength(1);
    // Both handlers must route through the shared pipeline so both inherit that ordering.
    const delegations = index.match(/await runResponsePipeline\(/g) || [];
    expect(delegations).toHaveLength(2);
  });
});

describe('streamTail — V5.5 wired contract (1a source-pin pattern)', () => {
  // V5.5 wired the parser into the request path. index.js has two near-identical
  // handler blocks (streaming + buffered); pin BOTH call sites in the source —
  // each block must construct the parser behind the V5 flag.
  test('index.js imports streamTail and constructs the parser in BOTH handler blocks', () => {
    const fs = require('fs');
    const source = fs.readFileSync(require.resolve('../index.js'), 'utf8');
    expect(source).toContain("require('../shared/prompt/streamTail')");
    const parserSites = source.match(/const tailParser = v5Active \? createTailParser\(\) : null;/g) || [];
    expect(parserSites).toHaveLength(2);
  });
});
