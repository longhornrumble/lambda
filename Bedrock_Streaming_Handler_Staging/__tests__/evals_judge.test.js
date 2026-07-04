/**
 * Unit tests for the Tier-2 groundedness judge (sub-phase 1.4).
 *
 * Covers the pure judge module (prompt build + verdict parse) and the judge's
 * integration into the runner scoring path — all without live Bedrock (the judge
 * call is stubbed via the injected `invokeJudge` seam, mirroring `invokeResponse`).
 */

'use strict';

const {
  GROUNDEDNESS_JUDGE_PROMPT_VERSION,
  buildGroundednessJudgePrompt,
  parseJudgeVerdict,
} = require('../evals/judge');
const { scoreScenario, runScenario, compareToBaseline, buildReportItem, CURRENT_PROMPT_VERSIONS } = require('../evals/run');
const { renderReport } = require('../evals/report');

describe('judge module — buildGroundednessJudgePrompt', () => {
  test('embeds KB, user question, and reply', () => {
    const p = buildGroundednessJudgePrompt('Orientation is Saturday at 9 AM.', 'When is orientation?', 'It is Saturday at 9 AM.');
    expect(p).toContain('Orientation is Saturday at 9 AM.');
    expect(p).toContain('When is orientation?');
    expect(p).toContain('It is Saturday at 9 AM.');
    expect(p).toMatch(/GROUNDED, UNGROUNDED, or UNSURE/);
  });

  test('marks an empty KB explicitly (KB-miss turn)', () => {
    expect(buildGroundednessJudgePrompt(null, 'q', 'a')).toMatch(/knowledge base was retrieved.*empty/i);
    expect(buildGroundednessJudgePrompt('   ', 'q', 'a')).toMatch(/knowledge base was retrieved.*empty/i);
  });
});

describe('judge module — parseJudgeVerdict', () => {
  test('parses each bare verdict word', () => {
    expect(parseJudgeVerdict('GROUNDED')).toBe('grounded');
    expect(parseJudgeVerdict('UNGROUNDED')).toBe('ungrounded');
    expect(parseJudgeVerdict('UNSURE')).toBe('unsure');
  });

  test('UNGROUNDED is not misread as GROUNDED (substring order)', () => {
    expect(parseJudgeVerdict('ungrounded')).toBe('ungrounded');
    expect(parseJudgeVerdict('UNGROUNDED — the reply invents a time not in the KB')).toBe('ungrounded');
  });

  test('uses the first line, tolerating trailing explanation', () => {
    expect(parseJudgeVerdict('GROUNDED\nEverything checks out.')).toBe('grounded');
    expect(parseJudgeVerdict('  UNSURE  \nCannot tell.')).toBe('unsure');
  });

  test('scans the body when the first line is not a bare verdict', () => {
    expect(parseJudgeVerdict('Verdict: UNGROUNDED')).toBe('ungrounded');
    expect(parseJudgeVerdict('I think this is GROUNDED overall.')).toBe('grounded');
  });

  test('unparseable output routes to unsure (never a silent pass)', () => {
    expect(parseJudgeVerdict('')).toBe('unsure');
    expect(parseJudgeVerdict(null)).toBe('unsure');
    expect(parseJudgeVerdict('banana')).toBe('unsure');
  });
});

describe('scoreScenario — grounded_in_kb assertion', () => {
  const scenario = { assertions: [{ type: 'grounded_in_kb' }] };

  test('GROUNDED verdict passes, no review flag', () => {
    const r = scoreScenario(scenario, { judgeVerdict: 'grounded' });
    expect(r.pass).toBe(true);
    expect(r.review).toBe(false);
  });

  test('UNGROUNDED verdict fails', () => {
    const r = scoreScenario(scenario, { judgeVerdict: 'ungrounded' });
    expect(r.pass).toBe(false);
    expect(r.review).toBe(false);
    expect(r.assertions[0].detail).toMatch(/UNGROUNDED/);
  });

  test('UNSURE verdict is non-blocking (pass) but flags human review', () => {
    const r = scoreScenario(scenario, { judgeVerdict: 'unsure' });
    expect(r.pass).toBe(true);
    expect(r.review).toBe(true);
    expect(r.assertions[0].review).toBe(true);
  });

  test('missing verdict fails loudly (harness wiring bug)', () => {
    const r = scoreScenario(scenario, {});
    expect(r.pass).toBe(false);
    expect(r.assertions[0].detail).toMatch(/did not run/);
  });

  test('deterministic assertions still gate alongside the judge', () => {
    const s = { assertions: [
      { type: 'response_contains', value: 'Saturday' },
      { type: 'grounded_in_kb' },
    ] };
    // grounded but the deterministic assertion fails → overall fail
    expect(scoreScenario(s, { responseText: 'It is Sunday.', judgeVerdict: 'grounded' }).pass).toBe(false);
    // both pass
    expect(scoreScenario(s, { responseText: 'It is Saturday.', judgeVerdict: 'grounded' }).pass).toBe(true);
  });
});

describe('runScenario — judge seam integration (no live Bedrock)', () => {
  const groundingScenario = {
    id: 'jt_grounding',
    config: { chat_title: 'Helping Hands', tone_prompt: 'Warm coordinator.' },
    kb_context: 'Orientation is the first Saturday of each month at 9:00 AM.',
    conversation_history: [],
    user_input: 'When is orientation?',
    assertions: [
      { type: 'response_contains', value: 'Saturday' },
      { type: 'grounded_in_kb' },
    ],
  };

  test('calls invokeJudge and threads the verdict into scoring (grounded → pass)', async () => {
    const invokeResponse = jest.fn(async () => 'Orientation is the first Saturday of the month at 9:00 AM.');
    const invokeJudge = jest.fn(async () => 'GROUNDED');
    const r = await runScenario(groundingScenario, { invokeResponse, invokeJudge, bedrockClient: { send: jest.fn() } });
    expect(invokeJudge).toHaveBeenCalledTimes(1);
    // the judge prompt must carry the reply + KB
    expect(invokeJudge.mock.calls[0][0]).toContain('first Saturday');
    expect(r.ranJudge).toBe(true);
    expect(r.judgeVerdict).toBe('grounded');
    expect(r.pass).toBe(true);
    expect(r.review).toBe(false);
  });

  test('ungrounded verdict fails the scenario', async () => {
    const invokeResponse = jest.fn(async () => 'Orientation is Saturday — sign up at https://made-up.example/apply');
    const invokeJudge = jest.fn(async () => 'UNGROUNDED');
    const r = await runScenario(groundingScenario, { invokeResponse, invokeJudge, bedrockClient: { send: jest.fn() } });
    expect(r.judgeVerdict).toBe('ungrounded');
    expect(r.pass).toBe(false);
  });

  test('unsure verdict → pass with review flag', async () => {
    const invokeResponse = jest.fn(async () => 'It is Saturday.');
    const invokeJudge = jest.fn(async () => 'UNSURE');
    const r = await runScenario(groundingScenario, { invokeResponse, invokeJudge, bedrockClient: { send: jest.fn() } });
    expect(r.pass).toBe(true);
    expect(r.review).toBe(true);
  });

  test('a scenario without a judge assertion never calls invokeJudge', async () => {
    const noJudge = { ...groundingScenario, assertions: [{ type: 'response_contains', value: 'Saturday' }] };
    const invokeResponse = jest.fn(async () => 'It is Saturday.');
    const invokeJudge = jest.fn();
    const r = await runScenario(noJudge, { invokeResponse, invokeJudge, bedrockClient: { send: jest.fn() } });
    expect(invokeJudge).not.toHaveBeenCalled();
    expect(r.ranJudge).toBe(false);
    expect(r.judgeVerdict).toBeNull();
  });

  test('a missing invokeJudge seam for a judge scenario is a captured error, not a throw', async () => {
    const invokeResponse = jest.fn(async () => 'It is Saturday.');
    const r = await runScenario(groundingScenario, { invokeResponse, bedrockClient: { send: jest.fn() } });
    expect(r.error).toMatch(/invokeJudge seam/);
    expect(r.pass).toBe(false);
  });
});

describe('compareToBaseline — groundedness judge version staleness', () => {
  const cur = CURRENT_PROMPT_VERSIONS;

  test('judge-version change stales only scenarios that ran the judge', () => {
    const staleJudge = { conversation: cur.conversation, action_selector: cur.action_selector, groundedness_judge: 'JUDGE.OLD' };
    const baseline = { scenarios: {
      judged: { pass: true, prompt_versions: staleJudge },
      plain: { pass: true, prompt_versions: staleJudge },
    } };
    const results = [
      { id: 'judged', ranSelector: false, ranJudge: true, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
      { id: 'plain', ranSelector: false, ranJudge: false, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
    ];
    const byId = Object.fromEntries(compareToBaseline(results, baseline, cur).map((i) => [i.id, i.status]));
    expect(byId.judged).toBe('stale_baseline');
    expect(byId.plain).toBe('ok');
  });
});

describe('renderReport — human-review surfacing', () => {
  test('summarizes UNSURE scenarios in a dedicated section without failing the run', () => {
    const results = [{
      id: 's_unsure', ranSelector: false, ranJudge: true, judgeVerdict: 'unsure',
      pass: true, review: true, error: null,
      assertions: [{ type: 'grounded_in_kb', pass: true, review: true, detail: 'UNSURE' }],
      ctas: [], responseText: 'Maybe grounded.', prompt_versions: CURRENT_PROMPT_VERSIONS,
    }];
    const items = compareToBaseline(results, { scenarios: { s_unsure: { pass: true, prompt_versions: CURRENT_PROMPT_VERSIONS } } }, CURRENT_PROMPT_VERSIONS);
    expect(items[0].review).toBe(true);
    const md = renderReport(items, { promptVersions: CURRENT_PROMPT_VERSIONS });
    expect(md).toMatch(/need human review/);
    expect(md).toMatch(/## Needs human review/);
    expect(md).toMatch(/groundedness_judge `v1`/);
  });
});

describe('version constant', () => {
  test('judge version is stamped into CURRENT_PROMPT_VERSIONS', () => {
    expect(CURRENT_PROMPT_VERSIONS.groundedness_judge).toBe(GROUNDEDNESS_JUDGE_PROMPT_VERSION);
  });
});
