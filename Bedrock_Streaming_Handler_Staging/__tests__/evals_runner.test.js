/**
 * Unit tests for the Tier-2 eval runner harness (sub-phase 1.3).
 *
 * The runner itself is a plain-Node process, but its pure helpers (scoring,
 * baseline comparison, report rendering) and the injectable run loop are unit-
 * tested here — no live Bedrock. The live model call is stubbed via the injected
 * `invokeResponse`; selectActionsV4 runs for real against a fake bedrock client.
 */

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const {
  loadScenarios,
  scoreScenario,
  runScenario,
  runScenarioResilient,
  compareToBaseline,
  buildBaseline,
  readBaseline,
  parseArgs,
  extractUrls,
  withRetry,
  isTransientError,
  CURRENT_PROMPT_VERSIONS,
} = require('../evals/run');
const { renderReport } = require('../evals/report');

const CONFIG = {
  cta_definitions: {
    learn_volunteer: { label: 'Volunteer info', action: 'send_query', ai_available: true },
    apply_volunteer: { label: 'Apply', action: 'start_form', ai_available: true },
  },
};

describe('scoreScenario — deterministic assertions', () => {
  test('response_contains / not_contains are case-insensitive', () => {
    const s = { assertions: [
      { type: 'response_contains', value: 'saturday' },
      { type: 'response_not_contains', value: 'sunday' },
    ] };
    const r = scoreScenario(s, { responseText: 'Orientation is on SATURDAY.' });
    expect(r.pass).toBe(true);
    expect(r.assertions.map((a) => a.pass)).toEqual([true, true]);
  });

  test('response_contains fails when absent', () => {
    const r = scoreScenario({ assertions: [{ type: 'response_contains', value: 'Monday' }] }, { responseText: 'Tuesday only' });
    expect(r.pass).toBe(false);
    expect(r.assertions[0].detail).toMatch(/Monday/);
  });

  test('response_matches / not_matches honor pattern + flags', () => {
    const s = { assertions: [
      { type: 'response_matches', pattern: '\\b9:00\\s?AM\\b' },
      { type: 'response_not_matches', pattern: 'booked' },
    ] };
    expect(scoreScenario(s, { responseText: 'We start at 9:00 AM sharp.' }).pass).toBe(true);
    expect(scoreScenario(s, { responseText: 'You are booked at 9:00 AM.' }).pass).toBe(false);
  });

  test('response_urls_subset_of_kb catches a fabricated URL (KB-miss = no URLs allowed)', () => {
    const s = { assertions: [{ type: 'response_urls_subset_of_kb' }] };
    expect(scoreScenario(s, { responseText: 'See https://real.example/apply', kbContext: 'apply at https://real.example/apply' }).pass).toBe(true);
    const bad = scoreScenario(s, { responseText: 'Go to https://made-up.example', kbContext: null });
    expect(bad.pass).toBe(false);
    expect(bad.assertions[0].detail).toMatch(/made-up\.example/);
  });

  test('cta assertions: include / exclude / subset_of / equal / empty / valid', () => {
    const ctx = { ctas: ['learn_volunteer'], config: CONFIG };
    expect(scoreScenario({ assertions: [{ type: 'ctas_include', value: ['learn_volunteer'] }] }, ctx).pass).toBe(true);
    expect(scoreScenario({ assertions: [{ type: 'ctas_exclude', value: ['apply_volunteer'] }] }, ctx).pass).toBe(true);
    expect(scoreScenario({ assertions: [{ type: 'ctas_subset_of', value: ['learn_volunteer', 'apply_volunteer'] }] }, ctx).pass).toBe(true);
    expect(scoreScenario({ assertions: [{ type: 'ctas_equal', value: ['learn_volunteer'] }] }, ctx).pass).toBe(true);
    expect(scoreScenario({ assertions: [{ type: 'ctas_empty' }] }, ctx).pass).toBe(false);
    expect(scoreScenario({ assertions: [{ type: 'ctas_empty' }] }, { ctas: [], config: CONFIG }).pass).toBe(true);
    // premature APPLY: interest ≠ commitment
    expect(scoreScenario({ assertions: [{ type: 'ctas_exclude', value: ['apply_volunteer'] }] }, { ctas: ['apply_volunteer'], config: CONFIG }).pass).toBe(false);
    // invalid CTA id never returned
    expect(scoreScenario({ assertions: [{ type: 'ctas_valid' }] }, { ctas: ['ghost_cta'], config: CONFIG }).pass).toBe(false);
  });

  test('unknown assertion type fails loudly', () => {
    const r = scoreScenario({ assertions: [{ type: 'totally_made_up' }] }, {});
    expect(r.pass).toBe(false);
    expect(r.assertions[0].detail).toMatch(/unknown assertion type/);
  });
});

describe('compareToBaseline — status derivation', () => {
  const cur = CURRENT_PROMPT_VERSIONS;
  const baseEntry = (pass, pv = cur) => ({ pass, prompt_versions: pv });

  test('ok / regression / fixed / new', () => {
    const baseline = { scenarios: {
      s_ok: baseEntry(true),
      s_reg: baseEntry(true),
      s_fix: baseEntry(false),
    } };
    const results = [
      { id: 's_ok', ranSelector: false, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
      { id: 's_reg', ranSelector: false, pass: false, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
      { id: 's_fix', ranSelector: false, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
      { id: 's_new', ranSelector: false, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
    ];
    const byId = Object.fromEntries(compareToBaseline(results, baseline, cur).map((i) => [i.id, i.status]));
    expect(byId).toEqual({ s_ok: 'ok', s_reg: 'regression', s_fix: 'fixed', s_new: 'new' });
  });

  test('error result short-circuits to error status', () => {
    const results = [{ id: 's', ranSelector: false, pass: false, error: 'AccessDenied', assertions: [], ctas: [], responseText: '', prompt_versions: cur }];
    expect(compareToBaseline(results, { scenarios: { s: baseEntry(true) } }, cur)[0].status).toBe('error');
  });

  test('conversation prompt-version change → stale_baseline', () => {
    const baseline = { scenarios: { s: baseEntry(true, { conversation: 'v4-conv.OLD', action_selector: cur.action_selector }) } };
    const results = [{ id: 's', ranSelector: false, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur }];
    expect(compareToBaseline(results, baseline, cur)[0].status).toBe('stale_baseline');
  });

  test('action-selector version change only staleness a scenario that ran the selector', () => {
    const baseline = { scenarios: {
      sel: baseEntry(true, { conversation: cur.conversation, action_selector: 'v4-selector.OLD' }),
      noSel: baseEntry(true, { conversation: cur.conversation, action_selector: 'v4-selector.OLD' }),
    } };
    const results = [
      { id: 'sel', ranSelector: true, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
      { id: 'noSel', ranSelector: false, pass: true, error: null, assertions: [], ctas: [], responseText: '', prompt_versions: cur },
    ];
    const byId = Object.fromEntries(compareToBaseline(results, baseline, cur).map((i) => [i.id, i.status]));
    expect(byId.sel).toBe('stale_baseline');
    expect(byId.noSel).toBe('ok');
  });
});

describe('buildBaseline', () => {
  test('captures pass + prompt_versions and skips live errors', () => {
    const results = [
      { id: 'good', pass: true, error: null, assertions: [{ type: 'response_contains', pass: true }], prompt_versions: CURRENT_PROMPT_VERSIONS },
      { id: 'boom', pass: false, error: 'timeout', assertions: [], prompt_versions: CURRENT_PROMPT_VERSIONS },
    ];
    const bl = buildBaseline(results);
    expect(Object.keys(bl.scenarios)).toEqual(['good']);
    expect(bl.scenarios.good.pass).toBe(true);
    expect(bl.prompt_versions).toEqual(CURRENT_PROMPT_VERSIONS);
  });
});

describe('renderReport', () => {
  test('summarizes and details failing scenarios', () => {
    const results = [
      { id: 's_ok', ranSelector: false, pass: true, error: null, assertions: [{ type: 'response_contains', pass: true }], ctas: [], responseText: 'ok', prompt_versions: CURRENT_PROMPT_VERSIONS },
      { id: 's_reg', ranSelector: false, pass: false, error: null, assertions: [{ type: 'response_contains', pass: false, detail: 'missing X' }], ctas: [], responseText: 'nope', prompt_versions: CURRENT_PROMPT_VERSIONS },
    ];
    const items = compareToBaseline(results, { scenarios: { s_ok: { pass: true, prompt_versions: CURRENT_PROMPT_VERSIONS }, s_reg: { pass: true, prompt_versions: CURRENT_PROMPT_VERSIONS } } }, CURRENT_PROMPT_VERSIONS);
    const md = renderReport(items, { promptVersions: CURRENT_PROMPT_VERSIONS, generatedAt: '2026-07-04T00:00:00Z' });
    expect(md).toMatch(/# Tier-2 eval report/);
    expect(md).toMatch(/s_ok/);
    expect(md).toMatch(/Failing scenarios/);
    expect(md).toMatch(/missing X/);
  });
});

describe('runScenario — end-to-end with injected seams (no live Bedrock)', () => {
  const smoke = loadScenarios(path.join(__dirname, '..', 'evals', 'scenarios'))
    .map((x) => x.scenario)
    .find((s) => s.id === 'smoke_grounded_orientation');

  test('loads the shipped smoke scenario', () => {
    expect(smoke).toBeDefined();
    expect(smoke.assertions.length).toBeGreaterThan(0);
  });

  test('builds prompt, runs the real selector against a fake client, scores', async () => {
    const invokeResponse = jest.fn(async () => 'Volunteer orientation is the first Saturday of each month at 9:00 AM at the community center.');
    const bedrockClient = {
      send: jest.fn(async () => ({
        body: new TextEncoder().encode(JSON.stringify({ content: [{ type: 'text', text: '["learn_volunteer"]' }] })),
      })),
    };
    const r = await runScenario(smoke, { invokeResponse, bedrockClient });
    expect(r.error).toBeNull();
    expect(invokeResponse).toHaveBeenCalledTimes(1);
    expect(bedrockClient.send).toHaveBeenCalledTimes(1); // selector ran
    expect(r.ctas).toEqual(['learn_volunteer']);
    expect(r.pass).toBe(true);
  });

  test('captures a live invoke error without throwing', async () => {
    const invokeResponse = jest.fn(async () => { throw new Error('AccessDeniedException'); });
    const r = await runScenario(smoke, { invokeResponse, bedrockClient: { send: jest.fn() } });
    expect(r.error).toMatch(/AccessDenied/);
    expect(r.pass).toBe(false);
  });
});

describe('runScenario — V4.1 pool-selection path (run_pool_selection)', () => {
  // Two-program fixture mirroring the 2026-07-04 incident shape.
  const poolScenario = {
    id: 'pool_fixture',
    run_pool_selection: true,
    config: {
      tenant_id: 'EVAL_POOL',
      topic_definitions: [
        { name: 'mentoring_learn', description: 'mentoring program', tags: ['mentoring'] },
        { name: 'volunteer_general', description: 'volunteering, program unspecified', tags: ['mentoring', 'care_box', 'volunteer'] },
      ],
      cta_definitions: {
        mentor_discovery: { label: 'Discovery session', action: 'send_query', ai_available: true, selection_metadata: { topic_tags: ['mentoring', 'volunteer'], depth_level: 'action' } },
        carebox_info: { label: 'About care boxes', action: 'send_query', ai_available: true, selection_metadata: { topic_tags: ['care_box'], depth_level: 'info' } },
      },
    },
    kb_context: 'Volunteers complete an application and a background check.',
    conversation_history: [
      { role: 'user', content: 'Tell me about your mentoring program.' },
      { role: 'assistant', content: 'Our mentoring program pairs adults with youth.' },
    ],
    user_input: 'Tell me about the volunteer process',
    session_context: { accumulated_topics: ['mentoring'] },
    assertions: [
      { type: 'ctas_include', value: ['mentor_discovery'] },
      { type: 'ctas_exclude', value: ['carebox_info'] },
    ],
  };
  const classifierClient = (topicText) => ({
    send: jest.fn(async () => ({
      body: new TextEncoder().encode(JSON.stringify({ content: [{ type: 'text', text: topicText }] })),
    })),
  });

  test('classifies live, runs the real pool selection with session_context, scores ctas_* on id strings', async () => {
    const invokeResponse = jest.fn(async () => 'The volunteer process starts with an application.');
    const bedrockClient = classifierClient('volunteer_general');
    const r = await runScenario(poolScenario, { invokeResponse, bedrockClient });
    expect(r.error).toBeNull();
    expect(bedrockClient.send).toHaveBeenCalledTimes(1); // classifier ran
    expect(r.classifiedTopic).toBe('volunteer_general');
    expect(r.sessionAligned).toBe(true);
    expect(r.ctas).toEqual(['mentor_discovery']);
    expect(r.ranPool).toBe(true);
    expect(r.pass).toBe(true);
  });

  test('null classification falls through to pool fallback behavior without error', async () => {
    const invokeResponse = jest.fn(async () => 'Happy to help.');
    const r = await runScenario(poolScenario, { invokeResponse, bedrockClient: classifierClient('null') });
    expect(r.error).toBeNull();
    expect(r.classifiedTopic).toBeNull();
    expect(Array.isArray(r.ctas)).toBe(true);
  });

  test('setting both run_action_selector and run_pool_selection is an error result', async () => {
    const both = { ...poolScenario, run_action_selector: true };
    const invokeResponse = jest.fn(async () => 'text');
    const r = await runScenario(both, { invokeResponse, bedrockClient: classifierClient('volunteer_general') });
    expect(r.error).toMatch(/pick one path/);
    expect(r.pass).toBe(false);
  });
});

describe('loadScenarios + readBaseline — failure modes', () => {
  test('loadScenarios returns [] for a missing directory', () => {
    expect(loadScenarios(path.join(os.tmpdir(), 'no-such-evals-dir-xyz'))).toEqual([]);
  });

  test('loadScenarios throws on a scenario missing an id', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'evalscn-'));
    try {
      fs.writeFileSync(path.join(dir, 'bad.json'), JSON.stringify({ description: 'no id here' }));
      expect(() => loadScenarios(dir)).toThrow(/missing required "id"/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  test('readBaseline returns an empty default when the file is absent', () => {
    const bl = readBaseline(path.join(os.tmpdir(), 'no-such-baseline-xyz.json'));
    expect(bl.scenarios).toEqual({});
    expect(bl.prompt_versions).toEqual(CURRENT_PROMPT_VERSIONS);
  });
});

describe('runScenarioResilient — per-scenario retry (flaky-but-correct scenarios)', () => {
  const scenario = { id: 's' };

  test('passes on the first attempt without retrying', async () => {
    const run = jest.fn(async () => ({ id: 's', pass: true, error: null }));
    const r = await runScenarioResilient(scenario, {}, { retries: 2, run });
    expect(run).toHaveBeenCalledTimes(1);
    expect(r.pass).toBe(true);
    expect(r.attempts).toBe(1);
  });

  test('retries a stochastic flip and returns the first passing attempt', async () => {
    let n = 0;
    const run = jest.fn(async () => { n += 1; return { id: 's', pass: n >= 2, error: null }; });
    const r = await runScenarioResilient(scenario, {}, { retries: 2, run });
    expect(run).toHaveBeenCalledTimes(2);
    expect(r.pass).toBe(true);
    expect(r.attempts).toBe(2);
  });

  test('a REAL regression fails every attempt and stays failed', async () => {
    const run = jest.fn(async () => ({ id: 's', pass: false, error: null }));
    const r = await runScenarioResilient(scenario, {}, { retries: 2, run });
    expect(run).toHaveBeenCalledTimes(3); // initial + 2 retries
    expect(r.pass).toBe(false);
    expect(r.attempts).toBe(3);
  });

  test('retries a live error too', async () => {
    let n = 0;
    const run = jest.fn(async () => { n += 1; return n >= 2 ? { id: 's', pass: true, error: null } : { id: 's', pass: false, error: 'ThrottlingException' }; });
    const r = await runScenarioResilient(scenario, {}, { retries: 3, run });
    expect(r.error).toBeNull();
    expect(r.pass).toBe(true);
  });

  test('retries:0 disables retry (single attempt)', async () => {
    const run = jest.fn(async () => ({ id: 's', pass: false, error: null }));
    const r = await runScenarioResilient(scenario, {}, { retries: 0, run });
    expect(run).toHaveBeenCalledTimes(1);
    expect(r.attempts).toBe(1);
  });
});

describe('withRetry + isTransientError — live-seam resilience (CI gate reliability)', () => {
  const noSleep = async () => {};

  test('isTransientError: throttling / 5xx / timeouts are transient; real errors are not', () => {
    expect(isTransientError({ name: 'ThrottlingException' })).toBe(true);
    expect(isTransientError({ name: 'TooManyRequestsException' })).toBe(true);
    expect(isTransientError({ name: 'InternalServerException' })).toBe(true);
    expect(isTransientError({ $metadata: { httpStatusCode: 429 } })).toBe(true);
    expect(isTransientError({ $metadata: { httpStatusCode: 503 } })).toBe(true);
    expect(isTransientError({ name: 'AccessDeniedException' })).toBe(false);
    expect(isTransientError({ name: 'ValidationException' })).toBe(false);
    expect(isTransientError({ $metadata: { httpStatusCode: 400 } })).toBe(false);
    expect(isTransientError(null)).toBe(false);
  });

  test('returns on first success without retrying', async () => {
    const fn = jest.fn(async () => 'ok');
    expect(await withRetry(fn, { sleep: noSleep })).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  test('retries a transient error, then succeeds', async () => {
    let calls = 0;
    const fn = jest.fn(async () => {
      calls += 1;
      if (calls < 3) throw Object.assign(new Error('slow down'), { name: 'ThrottlingException' });
      return 'recovered';
    });
    expect(await withRetry(fn, { sleep: noSleep })).toBe('recovered');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  test('does NOT retry a non-transient error — fails fast', async () => {
    const fn = jest.fn(async () => { throw Object.assign(new Error('nope'), { name: 'AccessDeniedException' }); });
    await expect(withRetry(fn, { sleep: noSleep })).rejects.toThrow('nope');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  test('exhausts retries on a persistent transient error and throws the last one', async () => {
    const fn = jest.fn(async () => { throw Object.assign(new Error('still throttled'), { name: 'ThrottlingException' }); });
    await expect(withRetry(fn, { retries: 2, sleep: noSleep })).rejects.toThrow('still throttled');
    expect(fn).toHaveBeenCalledTimes(3); // initial + 2 retries
  });
});

describe('parseArgs + extractUrls', () => {
  test('parseArgs reads flags', () => {
    const a = parseArgs(['--filter', 'smoke', '--update-baseline', '--strict']);
    expect(a.filter).toBe('smoke');
    expect(a.update).toBe(true);
    expect(a.strict).toBe(true);
  });
  test('parseArgs rejects unknown flag', () => {
    expect(() => parseArgs(['--nope'])).toThrow(/unknown arg/);
  });
  test('extractUrls finds http(s) urls', () => {
    expect(extractUrls('go to https://a.example/x and http://b.example')).toEqual(['https://a.example/x', 'http://b.example']);
    expect(extractUrls('no urls here')).toEqual([]);
  });
});
