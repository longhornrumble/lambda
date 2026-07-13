'use strict';

/**
 * M3a unit tests — Messenger V5 prompt composition (C6 precedence, C8
 * session-scoping by construction) + sessionWindow (C8 boundary math).
 * Live-model evidence lives in evals/evidence/messenger/ (CI-generated).
 */

const {
  buildMessengerV5Prompt,
  resolveMessengerTone,
  MESSENGER_BASE_RULES,
} = require('./prompt_messenger');
const { computeSessionWindow, SESSION_GAP_MS } = require('./sessionWindow');

const CTA_CONFIG = {
  tone_prompt: 'You are Val, the Foster Village assistant.',
  cta_definitions: {
    volunteer_form: { label: 'Volunteer Sign-Up', action: 'start_form', ai_available: true },
    learn_programs: { label: 'Our Programs', action: 'send_query', ai_available: true },
    internal_only: { label: 'Hidden', action: 'send_query', ai_available: false },
  },
};

const HOUR = 60 * 60 * 1000;

function row(role, content, messageTimestamp) {
  return { role, content, messageTimestamp };
}

describe('resolveMessengerTone — C6 precedence (replace, never concatenate)', () => {
  test('base default when nothing configured', () => {
    expect(resolveMessengerTone({}, 'messenger')).toBe('You are a helpful assistant.');
  });
  test('tone_prompt beats default', () => {
    expect(resolveMessengerTone(CTA_CONFIG, 'messenger')).toBe(CTA_CONFIG.tone_prompt);
  });
  test('messenger_behavior.tone_override REPLACES tone_prompt', () => {
    const cfg = { ...CTA_CONFIG, messenger_behavior: { tone_override: 'Messenger Val.' } };
    const tone = resolveMessengerTone(cfg, 'messenger');
    expect(tone).toBe('Messenger Val.');
    expect(tone).not.toContain('Foster Village'); // replaced, not concatenated
  });
  test('channel override wins over section override, per channel', () => {
    const cfg = {
      ...CTA_CONFIG,
      messenger_behavior: {
        tone_override: 'Messenger Val.',
        channel_overrides: { instagram: { tone_override: 'IG Val.' } },
      },
    };
    expect(resolveMessengerTone(cfg, 'instagram')).toBe('IG Val.');
    expect(resolveMessengerTone(cfg, 'messenger')).toBe('Messenger Val.');
  });
});

describe('buildMessengerV5Prompt — composition', () => {
  test('with ai_available CTAs: v5Active, catalog + tail spliced, base rules locked', () => {
    const { systemContent, v5Active } = buildMessengerV5Prompt('hi', 'KB TEXT', CTA_CONFIG, [], 'messenger');
    expect(v5Active).toBe(true);
    expect(systemContent).toContain(MESSENGER_BASE_RULES);
    expect(systemContent).toContain('AVAILABLE ACTIONS');
    expect(systemContent).toContain('volunteer_form');
    expect(systemContent).not.toContain('internal_only'); // ai_available:false excluded
    expect(systemContent).toContain('ACTION TAIL');
    expect(systemContent).toContain('Relevant information from the knowledge base:\nKB TEXT');
  });

  test('no ai_available CTAs: plain short-form prompt, no tail, v5Active=false', () => {
    const { systemContent, v5Active } = buildMessengerV5Prompt('hi', '', { tone_prompt: 'T.' }, [], 'messenger');
    expect(v5Active).toBe(false);
    expect(systemContent).not.toContain('ACTION TAIL');
    expect(systemContent).not.toContain('AVAILABLE ACTIONS');
    expect(systemContent).toContain(MESSENGER_BASE_RULES);
  });

  test('turn check appears only at/after 2 session-scoped assistant questions', () => {
    const noQuestions = buildMessengerV5Prompt('hi', '', CTA_CONFIG, [row('assistant', 'Welcome!', 1)], 'messenger');
    expect(noQuestions.systemContent).not.toContain('TURN CHECK');

    const twoQuestions = buildMessengerV5Prompt('hi', '', CTA_CONFIG, [
      row('assistant', 'What brings you here?', 1),
      row('user', 'volunteering', 2),
      row('assistant', 'Which program interests you?', 3),
      row('user', 'mentoring', 4),
    ], 'messenger');
    expect(twoQuestions.systemContent).toContain('TURN CHECK');
    expect(twoQuestions.systemContent).toContain('asked this user 2 questions');
  });

  test('M7b: suppressActions:true forces v5Active=false and omits the catalog/tail even with ai_available CTAs present', () => {
    const { systemContent, v5Active } = buildMessengerV5Prompt('hi', 'KB TEXT', CTA_CONFIG, [], 'messenger', {
      suppressActions: true,
    });
    expect(v5Active).toBe(false);
    expect(systemContent).not.toContain('AVAILABLE ACTIONS');
    expect(systemContent).not.toContain('ACTION TAIL');
    expect(systemContent).not.toContain('volunteer_form');
    // Everything else (tone, base rules, KB context) is unaffected.
    expect(systemContent).toContain(MESSENGER_BASE_RULES);
    expect(systemContent).toContain('Relevant information from the knowledge base:\nKB TEXT');
  });

  test('history window honors messenger_behavior.max_history_turns', () => {
    const history = Array.from({ length: 20 }, (_, i) => row(i % 2 ? 'assistant' : 'user', `m${i}`, i));
    const cfg = { ...CTA_CONFIG, messenger_behavior: { max_history_turns: 2 } };
    const { messages } = buildMessengerV5Prompt('now', '', cfg, history, 'messenger');
    expect(messages).toHaveLength(5); // 2 pairs + current turn
    expect(messages[0].content[0].text).toBe('m16');
  });
});

describe('computeSessionWindow — C8 boundary math', () => {
  test('empty history → new session, first turn', () => {
    expect(computeSessionWindow([], 0)).toEqual({ sessionMessages: [], isSessionFirstTurn: true });
  });

  test('continuous conversation (< 24h gaps) → whole history is the session', () => {
    const now = 100 * HOUR;
    const h = [row('user', 'a', now - 3 * HOUR), row('assistant', 'b', now - 2 * HOUR)];
    const w = computeSessionWindow(h, now);
    expect(w.sessionMessages).toHaveLength(2);
    expect(w.isSessionFirstTurn).toBe(false);
  });

  test('>24h internal gap → only rows after the boundary are the session (G4: lifetime thread never trips TURN CHECK)', () => {
    const now = 1000 * HOUR;
    const h = [
      row('assistant', 'Old question one?', now - 80 * HOUR),
      row('assistant', 'Old question two?', now - 79 * HOUR),
      row('user', 'back again', now - 2 * HOUR), // 77h gap ⇒ boundary
      row('assistant', 'Welcome back!', now - 1 * HOUR),
    ];
    const w = computeSessionWindow(h, now);
    expect(w.sessionMessages).toHaveLength(2);
    expect(w.sessionMessages[0].content).toBe('back again');
    // The two old questions are outside the session — turn check stays silent
    const { systemContent } = buildMessengerV5Prompt('hi', '', CTA_CONFIG, w.sessionMessages, 'messenger');
    expect(systemContent).not.toContain('TURN CHECK');
  });

  test('exactly 24h gap ⇒ NEW session (>= semantics, C8)', () => {
    const now = 1000 * HOUR;
    const h = [row('user', 'a', now - 25 * HOUR), row('user', 'b', now - 1 * HOUR)];
    const w = computeSessionWindow(h, now);
    expect(w.sessionMessages).toHaveLength(1);
    expect(w.sessionMessages[0].content).toBe('b');
    const exact = computeSessionWindow(
      [row('user', 'a', 0), row('user', 'b', SESSION_GAP_MS)],
      SESSION_GAP_MS + 1
    );
    expect(exact.sessionMessages).toHaveLength(1);
    expect(exact.sessionMessages[0].content).toBe('b');
  });

  test('newest stored row itself >= 24h old ⇒ incoming message starts a fresh session', () => {
    const now = 1000 * HOUR;
    const h = [row('user', 'a', now - 30 * HOUR), row('assistant', 'b', now - 29 * HOUR)];
    const w = computeSessionWindow(h, now);
    expect(w.sessionMessages).toHaveLength(0);
    expect(w.isSessionFirstTurn).toBe(true);
  });

  test('rows missing messageTimestamp never fabricate a boundary (fail-open)', () => {
    const now = 100 * HOUR;
    const h = [row('user', 'a', undefined), row('assistant', 'b', now - 1 * HOUR)];
    const w = computeSessionWindow(h, now);
    expect(w.sessionMessages).toHaveLength(2);
  });
});
