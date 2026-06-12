'use strict';

/**
 * WS-AG-CORE — sensitiveContext (§B17f suppression pre-check) tests.
 *
 * Done-bar coverage (work-order item 6):
 *  - every default category trips on representative phrasing (non-empty default list)
 *  - benign scheduling chatter does NOT trip
 *  - scan window = the FULL session (older message trips a later turn → sticky)
 *  - explicit priorLatched short-circuit (sticky latch)
 *  - FAIL-CLOSED: a scan error → tripped (category 'scan_error')
 *  - tenant trim removes categories; a trim that would EMPTY the list is ignored
 *  - minor self-identification additionally stops email solicitation
 *  - outputs carry category CODES only — never matched text (PII rule)
 */

const {
  DEFAULT_CATEGORIES,
  SCAN_ERROR_CATEGORY,
  resolveCategories,
  userSideTranscript,
  checkSensitiveContext,
  stopsEmailSolicitation,
} = require('../sensitiveContext');

describe('DEFAULT_CATEGORIES (§B17f non-empty default list)', () => {
  test('ships all ten §B17f categories', () => {
    expect(DEFAULT_CATEGORIES.map((c) => c.code)).toEqual([
      'self_harm_suicide',
      'abuse_neglect_cps',
      'domestic_violence',
      'trafficking',
      'runaway_homeless',
      'medical_emergency_overdose',
      'psychiatric_crisis',
      'custody_legal',
      'minor_self_identification',
      'grief_death',
    ]);
  });

  test('every category has at least one pattern', () => {
    for (const c of DEFAULT_CATEGORIES) {
      expect(c.patterns.length).toBeGreaterThan(0);
    }
  });
});

describe('checkSensitiveContext — category trips', () => {
  const CASES = [
    ['self_harm_suicide', "I've been thinking about suicide lately"],
    ['self_harm_suicide', "sometimes I feel like I don't want to be alive"],
    ['abuse_neglect_cps', 'CPS took my younger brother last month'],
    ['abuse_neglect_cps', 'I grew up in an abusive home'],
    ['domestic_violence', 'I have a restraining order against my ex'],
    ['domestic_violence', 'my husband hits me when he drinks'],
    ['trafficking', 'I think my friend is being trafficked'],
    ['runaway_homeless', "I'm homeless right now and need help"],
    ['runaway_homeless', 'I have nowhere to sleep tonight'],
    ['medical_emergency_overdose', 'my roommate overdosed last night'],
    ['medical_emergency_overdose', 'should I call 911?'],
    ['psychiatric_crisis', 'I just got out of a psych ward'],
    ['psychiatric_crisis', "I've been having panic attacks every day"],
    ['custody_legal', "I'm in a custody battle for my kids"],
    ['custody_legal', 'I have a court date next week'],
    // live-eval A8 gap (2026-06-12): "hurting/harming myself" class phrasings that the
    // v1 matcher missed — the model generated copy mid-crisis. Now pinned.
    ['self_harm_suicide', "I'm dealing with thoughts of hurting myself"],
    ['self_harm_suicide', "honestly I've been thinking about hurting myself lately"],
    ['self_harm_suicide', 'I keep thinking about harming myself'],
    ['self_harm_suicide', 'some days I just want to die'],
    ['self_harm_suicide', 'I want to end it all'],
    ['self_harm_suicide', "they'd be better off without me"],
    ['minor_self_identification', "I'm 15 years old, can I volunteer?"],
    ['minor_self_identification', "I'm a minor but I want to help"],
    ['minor_self_identification', "I'm in high school right now"],
    ['grief_death', 'my mom just died and I need something to do'],
    ['grief_death', 'her funeral is on Saturday'],
  ];

  test.each(CASES)('%s trips on: %s', (expectedCategory, phrase) => {
    const res = checkSensitiveContext({ userText: phrase });
    expect(res.tripped).toBe(true);
    expect(res.category).toBe(expectedCategory);
  });

  test('benign scheduling chatter does not trip', () => {
    const benign = [
      'anything next week?',
      'afternoons only please',
      'use my work email jane@acme.com instead',
      'what is this call about?',
      'my phone died so I missed your message', // "died" alone must not trip grief
      'I am 25 years old',
      'never mind, cancel that', // "cancel" of the flow is not legal-context
      'will this hurt my chances of being matched?', // "hurt" without a self-target
      'can we end it earlier, like 3pm?', // "end it" without "all"
      'I want to diet better this year', // "die" inside a longer word
    ];
    for (const phrase of benign) {
      expect(checkSensitiveContext({ userText: phrase })).toEqual({ tripped: false });
    }
  });

  test('result carries the category CODE only — never the matched text', () => {
    const res = checkSensitiveContext({ userText: 'my husband hits me' });
    expect(Object.keys(res).sort()).toEqual(['category', 'tripped']);
    expect(res.category).not.toMatch(/husband|hits/);
  });
});

describe('checkSensitiveContext — full-session window + sticky latch (§B17f)', () => {
  test('a crisis message EARLIER in the session trips a later innocuous turn', () => {
    const res = checkSensitiveContext({
      conversationHistory: [
        { role: 'user', content: "I've been thinking about suicide" },
        { role: 'assistant', content: 'I hear you — you matter.' },
        { role: 'user', content: 'ok' },
      ],
      userText: 'anything next week?',
    });
    expect(res.tripped).toBe(true);
    expect(res.category).toBe('self_harm_suicide');
  });

  test('assistant-side messages are NOT scanned (model cannot trip itself)', () => {
    const res = checkSensitiveContext({
      conversationHistory: [{ role: 'assistant', content: 'if you ever feel suicidal, call 988' }],
      userText: 'anything next week?',
    });
    expect(res).toEqual({ tripped: false });
  });

  test('priorLatched short-circuits to tripped with the persisted category', () => {
    const res = checkSensitiveContext({
      userText: 'totally innocuous',
      priorLatched: true,
      priorCategory: 'grief_death',
    });
    expect(res).toEqual({ tripped: true, category: 'grief_death' });
  });

  test('tolerates {role, text} message shape (schema discipline)', () => {
    const res = checkSensitiveContext({
      conversationHistory: [{ role: 'user', text: "I'm homeless right now" }],
      userText: 'hello',
    });
    expect(res.tripped).toBe(true);
    expect(res.category).toBe('runaway_homeless');
  });
});

describe('checkSensitiveContext — FAIL CLOSED (§B17f)', () => {
  test('a scan error is treated as tripped (scan_error)', () => {
    const spy = jest.spyOn(console, 'error').mockImplementation(() => {});
    // A tenant config that throws on ANY property read forces an internal scan error.
    const evilConfig = new Proxy({}, { get() { throw new Error('boom'); } });
    const res = checkSensitiveContext({
      userText: 'hello',
      tenantConfig: evilConfig,
    });
    expect(res.tripped).toBe(true);
    expect(res.category).toBe(SCAN_ERROR_CATEGORY);
    // err.name only — never message text.
    expect(spy).toHaveBeenCalledWith(expect.stringContaining('error_name='));
    spy.mockRestore();
  });

  test('null / missing inputs do not trip and do not throw', () => {
    expect(checkSensitiveContext()).toEqual({ tripped: false });
    expect(checkSensitiveContext({ conversationHistory: null, userText: undefined })).toEqual({ tripped: false });
  });
});

describe('resolveCategories — tenant trim (trim-only, never empty)', () => {
  test('no config → full default list', () => {
    expect(resolveCategories(undefined)).toBe(DEFAULT_CATEGORIES);
    expect(resolveCategories({})).toBe(DEFAULT_CATEGORIES);
  });

  test('trim removes the named categories', () => {
    const cats = resolveCategories({
      scheduling: { sensitive_context_trim: ['custody_legal', 'grief_death'] },
    });
    const codes = cats.map((c) => c.code);
    expect(codes).not.toContain('custody_legal');
    expect(codes).not.toContain('grief_death');
    expect(codes).toContain('self_harm_suicide');
    expect(cats.length).toBe(DEFAULT_CATEGORIES.length - 2);
  });

  test('a trim that would EMPTY the list is ignored (never empty — fail-closed)', () => {
    const all = DEFAULT_CATEGORIES.map((c) => c.code);
    expect(resolveCategories({ scheduling: { sensitive_context_trim: all } })).toBe(DEFAULT_CATEGORIES);
  });

  test('a trimmed category no longer trips; untrimmed still does', () => {
    const tenantConfig = { scheduling: { sensitive_context_trim: ['custody_legal'] } };
    expect(checkSensitiveContext({ userText: 'I have a court date next week', tenantConfig }))
      .toEqual({ tripped: false });
    expect(checkSensitiveContext({ userText: "I'm homeless", tenantConfig }).tripped).toBe(true);
  });
});

describe('userSideTranscript', () => {
  test('collects user-side strings (both content and text shapes) plus the current turn', () => {
    expect(
      userSideTranscript(
        [
          { role: 'user', content: 'first' },
          { role: 'assistant', content: 'reply' },
          { role: 'user', text: 'second' },
          { role: 'user' }, // empty — skipped
        ],
        'current'
      )
    ).toEqual(['first', 'second', 'current']);
  });

  test('tolerates a non-array history', () => {
    expect(userSideTranscript(null, 'only')).toEqual(['only']);
    expect(userSideTranscript(undefined)).toEqual([]);
  });
});

describe('stopsEmailSolicitation (§B17f minor rule)', () => {
  test('true only for minor_self_identification', () => {
    expect(stopsEmailSolicitation('minor_self_identification')).toBe(true);
    expect(stopsEmailSolicitation('grief_death')).toBe(false);
    expect(stopsEmailSolicitation(undefined)).toBe(false);
  });
});
