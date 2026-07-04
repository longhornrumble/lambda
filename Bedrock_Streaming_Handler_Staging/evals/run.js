#!/usr/bin/env node
/**
 * Tier-2 eval runner — SKELETON (chat-experience eval net, sub-phase 1.3).
 *
 * Plain Node process (NO jest). Runs scenario JSONs in-process against the REAL
 * BSH prompt modules (buildV4ConversationPrompt + selectActionsV4) with LIVE
 * Bedrock. The tenant config is a per-scenario fixture and the KB passages are
 * "recorded" in the scenario (scenario.kb_context) — only the model call is live.
 *
 * Scenario packs (1.4 grounding / 1.5 CTA+safety) and the CI wiring (1.6) come
 * later; the Haiku groundedness judge is 1.4. This slice is the harness:
 * discover → run → deterministic-score → compare-to-baseline → markdown report.
 *
 * Usage:
 *   node evals/run.js [--scenarios DIR] [--baselines FILE] [--report FILE]
 *                     [--filter SUBSTR] [--update-baseline] [--strict]
 *
 * Exit code: 1 if any scenario is a regression / stale_baseline / live error
 *            (or, with --strict, an un-baselined 'new' scenario). 0 otherwise.
 *
 * The pure helpers (scoreScenario, compareToBaseline, buildReportItem) are
 * exported for the jest unit suite; main() only runs when invoked as a script.
 */

'use strict';

const fs = require('fs');
const path = require('path');

const {
  buildV4ConversationPrompt,
  selectActionsV4,
  sanitizeTonePromptV4,
  V4_STEP2_INFERENCE_PARAMS,
  V4_CONVERSATION_PROMPT_VERSION,
  ACTION_SELECTOR_PROMPT_VERSION,
} = require('../prompt_v4');

// Haiku 4.5 — the program's default model (config.model_id / BEDROCK_MODEL_ID override).
const DEFAULT_MODEL_ID =
  process.env.BEDROCK_MODEL_ID || 'global.anthropic.claude-haiku-4-5-20251001-v1:0';
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';

const CURRENT_PROMPT_VERSIONS = Object.freeze({
  conversation: V4_CONVERSATION_PROMPT_VERSION,
  action_selector: ACTION_SELECTOR_PROMPT_VERSION,
});

const DEFAULT_SCENARIOS_DIR = path.join(__dirname, 'scenarios');
const DEFAULT_BASELINE_FILE = path.join(__dirname, 'baselines', 'tier2.json');

// ── scenario loading ───────────────────────────────────────────────────────────

/** Read + parse every *.json scenario in `dir` (sorted by id for deterministic runs). */
function loadScenarios(dir) {
  if (!fs.existsSync(dir)) return [];
  const scenarios = [];
  for (const name of fs.readdirSync(dir)) {
    if (!name.endsWith('.json')) continue;
    const full = path.join(dir, name);
    const scenario = JSON.parse(fs.readFileSync(full, 'utf8'));
    if (!scenario.id) throw new Error(`scenario ${name} is missing required "id"`);
    scenarios.push({ file: full, scenario });
  }
  return scenarios.sort((a, b) => a.scenario.id.localeCompare(b.scenario.id));
}

// ── deterministic scoring ──────────────────────────────────────────────────────

const URL_RE = /https?:\/\/[^\s)"'<>]+/gi;

function extractUrls(text) {
  return String(text || '').match(URL_RE) || [];
}

function includesCI(haystack, needle) {
  return String(haystack || '').toLowerCase().includes(String(needle || '').toLowerCase());
}

/**
 * Apply a scenario's deterministic assertions to the produced output.
 * PURE — given (scenario, { responseText, ctas, kbContext, config }).
 * Returns { pass, assertions: [{ type, pass, detail }] }.
 */
function scoreScenario(scenario, { responseText = '', ctas = [], kbContext = null, config = {} } = {}) {
  const ctaSet = new Set(ctas);
  const vocab = new Set(Object.keys(config.cta_definitions || {}));
  const assertions = (scenario.assertions || []).map((a) => {
    const val = a.value;
    switch (a.type) {
      case 'response_contains':
        return mk(a, includesCI(responseText, val), `expected response to contain "${val}"`);
      case 'response_not_contains':
        return mk(a, !includesCI(responseText, val), `expected response NOT to contain "${val}"`);
      case 'response_matches': {
        const ok = new RegExp(a.pattern, a.flags || 'i').test(responseText);
        return mk(a, ok, `expected response to match /${a.pattern}/${a.flags || 'i'}`);
      }
      case 'response_not_matches': {
        const ok = !new RegExp(a.pattern, a.flags || 'i').test(responseText);
        return mk(a, ok, `expected response NOT to match /${a.pattern}/${a.flags || 'i'}`);
      }
      case 'response_urls_subset_of_kb': {
        const urls = extractUrls(responseText);
        const kb = String(kbContext || '');
        const offenders = urls.filter((u) => !kb.includes(u));
        return mk(a, offenders.length === 0, offenders.length ? `URLs not present in KB: ${offenders.join(', ')}` : 'all response URLs present in KB');
      }
      case 'ctas_include':
        return mk(a, toArr(val).every((id) => ctaSet.has(id)), `expected ctas to include [${toArr(val).join(', ')}]; got [${ctas.join(', ')}]`);
      case 'ctas_exclude':
        return mk(a, toArr(val).every((id) => !ctaSet.has(id)), `expected ctas to exclude [${toArr(val).join(', ')}]; got [${ctas.join(', ')}]`);
      case 'ctas_subset_of': {
        const allowed = new Set(toArr(val));
        const extra = ctas.filter((id) => !allowed.has(id));
        return mk(a, extra.length === 0, extra.length ? `ctas outside allowed set: [${extra.join(', ')}]` : 'ctas within allowed set');
      }
      case 'ctas_equal': {
        const want = toArr(val);
        const ok = want.length === ctas.length && want.every((id) => ctaSet.has(id));
        return mk(a, ok, `expected ctas == [${want.join(', ')}]; got [${ctas.join(', ')}]`);
      }
      case 'ctas_empty':
        return mk(a, ctas.length === 0, `expected no ctas; got [${ctas.join(', ')}]`);
      case 'ctas_valid': {
        const invalid = ctas.filter((id) => !vocab.has(id));
        return mk(a, invalid.length === 0, invalid.length ? `ctas not in config vocabulary: [${invalid.join(', ')}]` : 'all ctas in vocabulary');
      }
      default:
        return { type: a.type, pass: false, detail: `unknown assertion type "${a.type}"` };
    }
  });
  return { pass: assertions.every((a) => a.pass), assertions };

  function mk(a, pass, detail) {
    return { type: a.type, pass, detail };
  }
  function toArr(v) {
    return Array.isArray(v) ? v : v == null ? [] : [v];
  }
}

// ── one scenario run (live seam is injected) ─────────────────────────────────────

/**
 * Run a single scenario. `deps.invokeResponse(prompt, {modelId, params})` is the
 * live Bedrock seam (injected so the jest suite can drive the loop deterministically);
 * `deps.bedrockClient` is passed straight to the real selectActionsV4.
 */
async function runScenario(scenario, deps) {
  const config = scenario.config || {};
  const kbContext = scenario.kb_context == null ? null : scenario.kb_context;
  const history = scenario.conversation_history || [];
  const ranSelector = scenario.run_action_selector === true;
  const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;

  const tonePrompt = sanitizeTonePromptV4(config.tone_prompt);
  const prompt = buildV4ConversationPrompt(scenario.user_input, kbContext, tonePrompt, history, config);

  let responseText = '';
  let ctas = [];
  let error = null;
  try {
    responseText = await deps.invokeResponse(prompt, {
      modelId,
      params: { max_tokens: V4_STEP2_INFERENCE_PARAMS.max_tokens, temperature: V4_STEP2_INFERENCE_PARAMS.temperature },
    });
    if (ranSelector) {
      // Mirrors index.js: (responseText, priorHistory, config, client).
      ctas = await selectActionsV4(responseText, history, config, deps.bedrockClient);
    }
  } catch (e) {
    error = e && e.message ? e.message : String(e);
  }

  const score = error ? { pass: false, assertions: [] } : scoreScenario(scenario, { responseText, ctas, kbContext, config });
  return {
    id: scenario.id,
    description: scenario.description || '',
    ranSelector,
    prompt_versions: { ...CURRENT_PROMPT_VERSIONS },
    responseText,
    ctas,
    error,
    pass: score.pass,
    assertions: score.assertions,
  };
}

// ── baseline comparison ──────────────────────────────────────────────────────────

/**
 * Compare live results to the committed baseline. PURE.
 * baseline shape: { prompt_versions, scenarios: { [id]: { pass, prompt_versions } } }.
 * status ∈ ok | fixed | regression | new | stale_baseline | error.
 */
function compareToBaseline(results, baseline, currentVersions = CURRENT_PROMPT_VERSIONS) {
  const scenarios = (baseline && baseline.scenarios) || {};
  return results.map((r) => {
    if (r.error) return item(r, 'error', null, `live error: ${r.error}`);
    const base = scenarios[r.id];
    if (!base) return item(r, 'new', null, 'no committed baseline — run --update-baseline');

    const bv = base.prompt_versions || {};
    const conversationChanged = bv.conversation !== currentVersions.conversation;
    const selectorChanged = r.ranSelector && bv.action_selector !== currentVersions.action_selector;
    if (conversationChanged || selectorChanged) {
      return item(r, 'stale_baseline', base.pass, 'prompt version changed — baseline no longer valid, re-baseline deliberately');
    }
    if (r.pass && !base.pass) return item(r, 'fixed', base.pass, 'now passing — update baseline to lock it in');
    if (!r.pass && base.pass) return item(r, 'regression', base.pass, 'was passing in baseline, now failing');
    return item(r, 'ok', base.pass, '');
  });

  function item(r, status, baselinePass, note) {
    return { ...buildReportItem(r), status, baseline_pass: baselinePass, note };
  }
}

/** Shape a run result for the report (drops the full response text to a preview). */
function buildReportItem(r) {
  return {
    id: r.id,
    description: r.description,
    ranSelector: r.ranSelector,
    prompt_versions: r.prompt_versions,
    pass: r.pass,
    error: r.error,
    assertions: r.assertions,
    ctas: r.ctas,
    responsePreview: String(r.responseText || '').replace(/\s+/g, ' ').trim().slice(0, 280),
  };
}

const FAIL_STATUSES = new Set(['regression', 'stale_baseline', 'error']);

// ── baseline (de)serialization ──────────────────────────────────────────────────

function readBaseline(file) {
  if (!fs.existsSync(file)) return { prompt_versions: { ...CURRENT_PROMPT_VERSIONS }, scenarios: {} };
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

/** Build the next baseline object from live results (skips live errors). */
function buildBaseline(results, currentVersions = CURRENT_PROMPT_VERSIONS) {
  const scenarios = {};
  for (const r of results) {
    if (r.error) continue; // never baseline a live failure
    scenarios[r.id] = {
      pass: r.pass,
      prompt_versions: { ...r.prompt_versions },
      assertions: r.assertions.map((a) => ({ type: a.type, pass: a.pass })),
    };
  }
  return { prompt_versions: { ...currentVersions }, scenarios };
}

// ── live Bedrock seam ─────────────────────────────────────────────────────────────

/** Real Bedrock invokers. Lazy-requires the SDK so the pure helpers import cleanly. */
function makeBedrockInvokers({ region = AWS_REGION } = {}) {
  const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
  const client = new BedrockRuntimeClient({ region });
  async function invokeResponse(prompt, { modelId, params }) {
    const command = new InvokeModelCommand({
      modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: params.max_tokens,
        temperature: params.temperature,
      }),
    });
    const res = await client.send(command);
    const payload = JSON.parse(new TextDecoder().decode(res.body));
    return (payload.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('');
  }
  return { bedrockClient: client, invokeResponse };
}

// ── CLI ────────────────────────────────────────────────────────────────────────

function parseArgs(argv) {
  const args = { scenarios: DEFAULT_SCENARIOS_DIR, baselines: DEFAULT_BASELINE_FILE, report: null, filter: null, update: false, strict: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--scenarios') args.scenarios = argv[++i];
    else if (a === '--baselines') args.baselines = argv[++i];
    else if (a === '--report') args.report = argv[++i];
    else if (a === '--filter') args.filter = argv[++i];
    else if (a === '--update-baseline') args.update = true;
    else if (a === '--strict') args.strict = true;
    else if (a === '--help' || a === '-h') args.help = true;
    else throw new Error(`unknown arg: ${a}`);
  }
  return args;
}

async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log('Usage: node evals/run.js [--scenarios DIR] [--baselines FILE] [--report FILE] [--filter SUBSTR] [--update-baseline] [--strict]');
    return 0;
  }
  // selectActionsV4 reads BEDROCK_MODEL_ID from env — default it so CTA scenarios run.
  if (!process.env.BEDROCK_MODEL_ID) process.env.BEDROCK_MODEL_ID = DEFAULT_MODEL_ID;

  let loaded = loadScenarios(args.scenarios);
  if (args.filter) loaded = loaded.filter(({ scenario }) => scenario.id.includes(args.filter));
  if (loaded.length === 0) {
    console.log(`No scenarios found in ${args.scenarios} (packs land in sub-phases 1.4/1.5).`);
    return 0;
  }

  console.log(`Running ${loaded.length} scenario(s) against live Bedrock (${AWS_REGION})…`);
  const invokers = makeBedrockInvokers({ region: AWS_REGION });
  const results = [];
  for (const { scenario } of loaded) {
    process.stdout.write(`  • ${scenario.id} … `);
    const r = await runScenario(scenario, invokers);
    process.stdout.write(r.error ? `error\n` : `${r.pass ? 'pass' : 'FAIL'}\n`);
    results.push(r);
  }

  if (args.update) {
    const next = buildBaseline(results);
    fs.mkdirSync(path.dirname(args.baselines), { recursive: true });
    fs.writeFileSync(args.baselines, `${JSON.stringify(next, null, 2)}\n`);
    console.log(`Baseline written: ${args.baselines} (${Object.keys(next.scenarios).length} scenario(s)).`);
  }

  const baseline = readBaseline(args.baselines);
  const items = compareToBaseline(results, baseline);
  const report = renderReportSafe(items, { promptVersions: CURRENT_PROMPT_VERSIONS, baselineVersions: baseline.prompt_versions });
  if (args.report) {
    fs.mkdirSync(path.dirname(args.report), { recursive: true });
    fs.writeFileSync(args.report, `${report}\n`);
    console.log(`Report written: ${args.report}`);
  } else {
    console.log(`\n${report}`);
  }

  const failed = items.filter((it) => FAIL_STATUSES.has(it.status) || (args.strict && it.status === 'new'));
  if (args.update) return 0; // an update run establishes the baseline; don't also fail on it
  return failed.length > 0 ? 1 : 0;
}

function renderReportSafe(items, meta) {
  const { renderReport } = require('./report');
  return renderReport(items, meta);
}

if (require.main === module) {
  main()
    .then((code) => process.exit(code))
    .catch((err) => {
      console.error('eval runner crashed:', err);
      process.exit(2);
    });
}

module.exports = {
  loadScenarios,
  scoreScenario,
  runScenario,
  compareToBaseline,
  buildReportItem,
  buildBaseline,
  readBaseline,
  makeBedrockInvokers,
  parseArgs,
  main,
  extractUrls,
  DEFAULT_MODEL_ID,
  CURRENT_PROMPT_VERSIONS,
  FAIL_STATUSES,
};
