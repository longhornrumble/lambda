/**
 * prompt_v5 unit tests — V5 single-pass turn prompt (V5.2, unwired).
 *
 * The module composes buildV4ConversationPrompt + action catalog + selector
 * rules + machine-read tail instruction. These tests pin:
 *   - the splice contract (V4 prompt carries the USER MESSAGE marker exactly
 *     once — a V4 refactor that renames it must fail here, not silently
 *     produce a promptless catalog),
 *   - section ordering (catalog before USER MESSAGE, tail instruction last),
 *   - the no-catalog degenerate case (byte-identical to the V4 prompt),
 *   - the V5.1 sentinel constants as the single source of truth,
 *   - the wired contract (V5.5: index.js builds the V5 prompt at BOTH
 *     duplicated handler call sites, V5 branch before V4_ACTION_SELECTOR),
 *   - validateActionIds (selectActionsV4's known-ids + cap-4 semantics, shared
 *     by both handler blocks).
 */

const {
  V5_TURN_PROMPT_VERSION,
  V5_TURN_INFERENCE_PARAMS,
  USER_MESSAGE_MARKER,
  buildV5TurnPrompt,
  buildActionCatalogBlock,
  buildActionTailInstruction,
  buildTurnCheckBlock,
  validateActionIds,
} = require('../prompt_v5');
const { buildV4ConversationPrompt } = require('../prompt_v4');
const { SENTINEL_OPEN, SENTINEL_CLOSE } = require('../streamTail');

const CONFIG = {
  chat_title: 'Atlanta Angels',
  cta_definitions: {
    love_box_learn: { label: 'Learn about Love Box', action: 'send_query', ai_available: true },
    apply_daretodream_volunteer: { label: 'Apply to be a Dare to Dream mentor', action: 'external_link', ai_available: true },
    contact_us: { label: 'Contact Us', action: 'start_form', ai_available: false },
    schedule_intro_call: { label: 'Schedule a Call', action: 'start_scheduling', ai_available: true },
  },
};

const KB = 'The Dare to Dream program pairs youth with mentors.';
const HISTORY = [
  { role: 'user', content: 'Tell me about mentoring.' },
  { role: 'assistant', content: 'Our program pairs youth with mentors.' },
];

describe('buildActionCatalogBlock', () => {
  test('lists only ai_available CTAs in `id — label [INTENT]` format', () => {
    const block = buildActionCatalogBlock(CONFIG);
    expect(block).toContain('love_box_learn — Learn about Love Box [LEARN]');
    expect(block).toContain('apply_daretodream_volunteer — Apply to be a Dare to Dream mentor [VISIT]');
    expect(block).toContain('schedule_intro_call — Schedule a Call [SCHEDULE]');
    expect(block).not.toContain('contact_us');
  });

  test('carries the selector rules (v5-turn.v2: sustained-interest advance)', () => {
    const block = buildActionCatalogBlock(CONFIG);
    expect(block).toContain('RESTRAINT FIRST');
    expect(block).toContain('SUSTAINED INTEREST → ADVANCE');
    expect(block).toContain('APPLY/VISIT need real intent');
    expect(block).toContain('COHERENCE');
  });

  test('returns empty string when no CTA is ai_available', () => {
    expect(buildActionCatalogBlock({ cta_definitions: { x: { label: 'X', action: 'send_query' } } })).toBe('');
    expect(buildActionCatalogBlock({})).toBe('');
    expect(buildActionCatalogBlock(undefined)).toBe('');
  });
});

describe('buildActionTailInstruction', () => {
  test('uses the streamTail sentinel constants (single source of truth)', () => {
    const tail = buildActionTailInstruction();
    expect(tail).toContain(`${SENTINEL_OPEN} ["action_id","action_id"]${SENTINEL_CLOSE}`);
    expect(tail).toContain(`${SENTINEL_OPEN} []${SENTINEL_CLOSE}`);
  });
});

describe('buildV5TurnPrompt', () => {
  test('V4 prompt carries the USER MESSAGE marker exactly once (splice contract)', () => {
    const base = buildV4ConversationPrompt('Hi', KB, '', HISTORY, CONFIG, {});
    expect(base.split(USER_MESSAGE_MARKER)).toHaveLength(2);
  });

  test('contains the V4 base sections plus catalog and tail', () => {
    const prompt = buildV5TurnPrompt('What is Love Box?', KB, '', HISTORY, CONFIG, {});
    expect(prompt).toContain('━━━ KNOWLEDGE BASE ━━━');
    expect(prompt).toContain('━━━ CONVERSATION SO FAR ━━━');
    expect(prompt).toContain('━━━ ACTIONS ━━━');
    expect(prompt).toContain('━━━ ACTION TAIL (machine-read, required) ━━━');
    expect(prompt).toContain('USER: What is Love Box?');
  });

  test('catalog sits before the USER MESSAGE section; tail instruction is last', () => {
    const prompt = buildV5TurnPrompt('Hi', KB, '', HISTORY, CONFIG, {});
    const catalogAt = prompt.indexOf('━━━ ACTIONS ━━━');
    const userAt = prompt.indexOf(USER_MESSAGE_MARKER);
    const tailAt = prompt.indexOf('━━━ ACTION TAIL');
    expect(catalogAt).toBeGreaterThan(-1);
    expect(catalogAt).toBeLessThan(userAt);
    expect(tailAt).toBeGreaterThan(userAt);
    // Tail instruction is the absolute end (recency bias).
    expect(prompt.trimEnd().endsWith(`never mention it, the IDs, or "actions" in your prose.`)).toBe(true);
  });

  test('no ai_available CTAs → byte-identical to the V4 prompt (no tail asked for)', () => {
    const bare = { chat_title: 'X', cta_definitions: {} };
    const v4 = buildV4ConversationPrompt('Hi', KB, '', HISTORY, bare, {});
    const v5 = buildV5TurnPrompt('Hi', KB, '', HISTORY, bare, {});
    expect(v5).toBe(v4);
  });

  test('sessionContext is optional (defaults to no SESSION CONTEXT block)', () => {
    const prompt = buildV5TurnPrompt('Hi', KB, '', HISTORY, CONFIG);
    expect(prompt).not.toContain('SESSION CONTEXT');
    expect(prompt).toContain('━━━ ACTIONS ━━━');
  });

  test('threads session_context through to the V4 base', () => {
    const prompt = buildV5TurnPrompt('Hi', KB, '', HISTORY, CONFIG, { accumulated_topics: ['mentoring'] });
    expect(prompt).toContain('This session so far is about: mentoring.');
  });

  test('fails loud if the V4 marker ever disappears', () => {
    jest.isolateModules(() => {
      jest.doMock('../prompt_v4', () => ({
        buildV4ConversationPrompt: () => 'a V4 prompt without the marker',
        intentLabel: (a) => a,
      }));
      const v5 = require('../prompt_v5');
      expect(() => v5.buildV5TurnPrompt('Hi', KB, '', [], CONFIG, {})).toThrow(/USER MESSAGE marker/);
    });
    jest.dontMock('../prompt_v4');
  });
});

describe('V5 constants', () => {
  test('version stamp and inference params are exported', () => {
    expect(V5_TURN_PROMPT_VERSION).toBe('v5-turn.v3');
    expect(V5_TURN_INFERENCE_PARAMS).toEqual({ temperature: 0.35, max_tokens: 700 });
  });
});

describe('buildTurnCheckBlock — server-counted funnel turn check (v5-turn.v3)', () => {
  const q = (content) => ({ role: 'assistant', content });
  const u = (content) => ({ role: 'user', content });

  test('empty below the threshold (fewer than 2 assistant questions)', () => {
    expect(buildTurnCheckBlock([])).toBe('');
    expect(buildTurnCheckBlock(undefined)).toBe('');
    expect(buildTurnCheckBlock([u('hi'), q('Welcome! How can I help?')])).toBe('');
    // Non-question assistant turns don't count.
    expect(buildTurnCheckBlock([q('Here is info.'), q('More info.'), q('Still telling, not asking.')])).toBe('');
  });

  test('emits at the threshold with the server-computed count', () => {
    const history = [u('a'), q('What draws you to mentoring?'), u('b'), q('Which area matters most?')];
    const block = buildTurnCheckBlock(history);
    expect(block).toContain('TURN CHECK');
    expect(block).toContain('asked this user 2 questions');
    expect(block).toContain('NO actions');
  });

  test('counts m.text-shaped messages and trailing whitespace question marks', () => {
    const history = [
      { role: 'assistant', text: 'What draws you to mentoring?  ' },
      { role: 'assistant', content: 'Which feels most important? ' },
    ];
    expect(buildTurnCheckBlock(history)).toContain('asked this user 2 questions');
  });

  test('the turn check sits between USER MESSAGE and ACTION TAIL in the built prompt', () => {
    const history = [u('a'), q('One?'), u('b'), q('Two?')];
    const prompt = buildV5TurnPrompt('Understanding money', KB, '', history, CONFIG, {});
    const userAt = prompt.indexOf(USER_MESSAGE_MARKER);
    const checkAt = prompt.indexOf('━━━ TURN CHECK ━━━');
    const tailAt = prompt.indexOf('━━━ ACTION TAIL');
    expect(checkAt).toBeGreaterThan(userAt);
    expect(tailAt).toBeGreaterThan(checkAt);
  });

  test('below threshold the built prompt carries NO turn check (early funnel unchanged)', () => {
    const prompt = buildV5TurnPrompt('Hi', KB, '', [u('a'), q('One question?')], CONFIG, {});
    expect(prompt).not.toContain('TURN CHECK');
  });

  test('empty catalog still returns the bare V4 prompt (no turn check either)', () => {
    const noCatalog = { cta_definitions: { x: { label: 'X', action: 'send_query' } } };
    const history = [u('a'), q('One?'), u('b'), q('Two?')];
    const prompt = buildV5TurnPrompt('Hi', KB, '', history, noCatalog, {});
    expect(prompt).not.toContain('TURN CHECK');
    expect(prompt).not.toContain('ACTION TAIL');
  });
});

describe('validateActionIds — selectActionsV4-mirroring validation (V5.5)', () => {
  test('keeps known ids (ai_available NOT required — same as selectActionsV4), drops unknown', () => {
    expect(validateActionIds(['love_box_learn', 'ghost_cta', 'contact_us'], CONFIG))
      .toEqual(['love_box_learn', 'contact_us']);
  });

  test('caps at 4', () => {
    const config = { cta_definitions: Object.fromEntries(['a', 'b', 'c', 'd', 'e'].map((id) => [id, { label: id }])) };
    expect(validateActionIds(['a', 'b', 'c', 'd', 'e'], config)).toEqual(['a', 'b', 'c', 'd']);
  });

  test('null/undefined ids and missing config degrade to []', () => {
    expect(validateActionIds(null, CONFIG)).toEqual([]);
    expect(validateActionIds(undefined, CONFIG)).toEqual([]);
    expect(validateActionIds(['love_box_learn'], {})).toEqual([]);
    expect(validateActionIds(['love_box_learn'], undefined)).toEqual([]);
  });
});

describe('V5.5 wired contract (1a source-pin pattern)', () => {
  // V5.5 wired the merged prompt into the request path. Pin BOTH duplicated
  // prompt call sites (streaming + buffered handler blocks): each must build
  // the V5 prompt behind the flag with the exact V4-mirroring signature, and
  // the V5 branch must sit BEFORE V4_ACTION_SELECTOR in both CTA chains
  // (tenants carry both flags — appended-after would make the V5 flip a no-op).
  test('index.js builds the V5 prompt at BOTH handler call sites', () => {
    const fs = require('fs');
    const source = fs.readFileSync(require.resolve('../index.js'), 'utf8');
    expect(source).toContain("require('./prompt_v5')");
    const v5Sites = source.match(/buildV5TurnPrompt\(sanitizedInput, kbContext, tonePrompt, conversationHistory, config, body\.session_context \|\| \{\}\)/g) || [];
    expect(v5Sites).toHaveLength(2);
  });

  test('the V5 CTA branch precedes V4_ACTION_SELECTOR in BOTH chains', () => {
    const fs = require('fs');
    const source = fs.readFileSync(require.resolve('../index.js'), 'utf8');
    // In each CTA chain the v5Active arm must appear before the V4 flag check:
    // `else if (v5Active) { ... } else if (config.feature_flags?.V4_ACTION_SELECTOR)`.
    const ordered = source.match(/} else if \(v5Active\) \{[\s\S]*?} else if \(config\.feature_flags\?\.V4_ACTION_SELECTOR\) \{/g) || [];
    expect(ordered).toHaveLength(2);
  });
});
