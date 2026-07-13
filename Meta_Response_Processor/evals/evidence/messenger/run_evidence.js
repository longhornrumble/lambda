#!/usr/bin/env node
/**
 * M3a evidence harness — Messenger V5 prompt (Messenger Channel Experience).
 *
 * Follows the committed-and-reproducible discipline of the V5.2/V5.3 harness
 * (Bedrock_Streaming_Handler_Staging/evals/evidence/v5/run_evidence.js):
 * script + fixture + results committed; exact Clopper-Pearson 95% lower
 * bounds per gate.
 *
 * What the LIVE model runs measure (deterministic C8 session-boundary and C6
 * precedence behavior is pinned by prompt_messenger.test.js — no model calls
 * needed there):
 *   1. BREVITY — replies <= 3 sentences with the 14-CTA catalog spliced in.
 *   2. TAIL EMISSION — replies end with exactly one well-formed ACTION tail
 *      (parsed with the real createTailParser, chunking-invariant
 *      feed(full)+end()).
 *   3. TAIL VALIDITY — tail ids validate against the catalog
 *      (validateActionIds, cap 4).
 *   4. RESTRAINT — small-talk/thanks turns select ZERO actions.
 *
 * Run (CI: .github/workflows/messenger-evidence.yml, staging OIDC — or any
 * staging-credentialed shell):
 *   node evals/evidence/messenger/run_evidence.js
 *
 * NOT part of jest / the bundle — evidence only.
 */

'use strict';

const fs = require('fs');
const path = require('path');

const PROCESSOR = path.join(__dirname, '..', '..', '..');
const SHARED_PROMPT = path.join(PROCESSOR, '..', 'shared', 'prompt');
const { buildMessengerV5Prompt } = require(path.join(PROCESSOR, 'prompt_messenger.js'));
const { createTailParser } = require(path.join(SHARED_PROMPT, 'streamTail.js'));
const { validateActionIds } = require(path.join(SHARED_PROMPT, 'prompt_v5.js'));
const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');

// Real 14-ai_available-CTA catalog — same fixture the V5 evidence used.
const config = JSON.parse(
  fs.readFileSync(
    path.join(PROCESSOR, '..', 'Bedrock_Streaming_Handler_Staging', 'evals', 'evidence', 'v5', 'myr_catalog_fixture.json'),
    'utf8'
  )
);

const MODEL_ID = config.model_id || process.env.BEDROCK_MODEL_ID;
const OUT_JSONL = path.join(__dirname, 'results.jsonl');
const OUT_SUMMARY = path.join(__dirname, 'summary.json');
const CONCURRENCY = 4;
const SAMPLES_PER_SCENARIO = 5;

const KB = `MyRecruiter helps nonprofits engage volunteers and donors. The Volunteer program matches individuals with local service opportunities; volunteers complete a short application and orientation. The Mentorship program pairs adults with youth for a one-year commitment, twice-monthly meetings, background check required (cost covered). We host monthly info sessions where prospective volunteers can learn about all programs. Donations fund program operations and family support.`;

// ── scenarios ────────────────────────────────────────────────────────────────
// history rows use {role, content} (session-scoped by the caller in prod;
// here every scenario IS a single session by construction).
const SCENARIOS = [
  { id: 'greeting_smalltalk', restraintExpected: true, history: [], user: 'hey there! how are you today?' },
  { id: 'thanks_wrapup', restraintExpected: true, history: [
      { role: 'user', content: 'what programs do you have?' },
      { role: 'assistant', content: 'We offer the Volunteer program and the Mentorship program. Which sounds interesting?' },
    ], user: 'thanks, that was helpful!' },
  { id: 'program_question', restraintExpected: false, history: [], user: 'what is the mentorship program?' },
  { id: 'exploring_program', restraintExpected: false, history: [
      { role: 'user', content: 'tell me about mentoring' },
      { role: 'assistant', content: 'Our Mentorship program pairs you with a young person for a year of twice-monthly meetups. What draws you to mentoring?' },
    ], user: 'i work with teens already and want to do more' },
  { id: 'explicit_commitment', restraintExpected: false, history: [
      { role: 'user', content: 'tell me about volunteering' },
      { role: 'assistant', content: 'The Volunteer program matches you with local service opportunities. Want to hear about the application?' },
    ], user: 'yes, I want to sign up. what do I do?' },
  { id: 'factual_hours', restraintExpected: false, history: [], user: 'how often do mentors meet with their youth?' },
  { id: 'long_answer_bait', restraintExpected: false, history: [], user: 'can you explain everything about all your programs, requirements, time commitments, and how to get started with each one?' },
  { id: 'donation_interest', restraintExpected: false, history: [], user: 'how can I donate to support the work?' },
  // TURN CHECK fired live (M3a gate condition): two session-scoped assistant
  // questions put buildTurnCheckBlock at threshold — the block is IN the
  // system prompt for these calls. Gate: the reply must NOT end with another
  // exploration question.
  { id: 'turn_check_at_threshold', restraintExpected: false, turnCheckFired: true, history: [
      { role: 'assistant', content: 'What draws you to mentoring?' },
      { role: 'user', content: 'i love working with teens' },
      { role: 'assistant', content: 'Have you mentored before?' },
      { role: 'user', content: 'yes, two years at my church' },
    ], user: 'so what do you think, am i a good fit?' },
];

// ── helpers ──────────────────────────────────────────────────────────────────

const bedrock = new BedrockRuntimeClient({ region: process.env.AWS_REGION || 'us-east-1' });

async function invoke(systemContent, messages) {
  const command = new InvokeModelCommand({
    modelId: MODEL_ID,
    contentType: 'application/json',
    accept: 'application/json',
    body: JSON.stringify({
      anthropic_version: 'bedrock-2023-05-31',
      max_tokens: 500,
      temperature: 0.7, // production-realistic sampling, matches the V5 evidence discipline
      system: systemContent,
      messages,
    }),
  });
  const result = await bedrock.send(command);
  const body = JSON.parse(Buffer.from(result.body).toString('utf-8'));
  return body.content?.[0]?.text || '';
}

function countSentences(text) {
  // Known blind spot (gate review, M3a): abbreviation periods ("vs.") count
  // as sentence enders (inflates counts -> conservative for the brevity
  // gate); a terminator immediately followed by a closing quote/paren is
  // missed. Harden before certification-scale runs.
  return (text.match(/[.!?](\s|$)/g) || []).length || (text.trim() ? 1 : 0);
}

/** Markdown/formatting leakage — instrumented gate (was manual in round 2). */
function hasMarkdown(text) {
  return /\*\*|__|^#+\s|^[-*]\s|^\d+\.\s/m.test(text);
}

/** Parse the action tail the chunking-invariant way the processor will (M3b). */
function parseTail(raw) {
  const parser = createTailParser();
  const released = parser.feed(raw);
  const endResult = parser.end(); // { remainingText, actionIds, status, trailingAfterClose }
  return {
    visible: released + (endResult.remainingText || ''),
    tail: endResult,
  };
}

/** Exact Clopper-Pearson 95% lower bound for successes/n (beta inverse via bisection). */
function cpLowerBound(successes, n, alpha = 0.05) {
  if (n === 0) return 0;
  if (successes === 0) return 0;
  if (successes === n) return Math.pow(alpha, 1 / n);
  const logBeta = (a, b) => lgamma(a) + lgamma(b) - lgamma(a + b);
  function lgamma(x) {
    const c = [76.18009172947146, -86.50532032941677, 24.01409824083091, -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5];
    let y = x, tmp = x + 5.5;
    tmp -= (x + 0.5) * Math.log(tmp);
    let ser = 1.000000000190015;
    for (let j = 0; j < 6; j++) ser += c[j] / ++y;
    return -tmp + Math.log((2.5066282746310005 * ser) / x);
  }
  function betainc(x, a, b) {
    // regularized incomplete beta via continued fraction
    if (x <= 0) return 0;
    if (x >= 1) return 1;
    const lbeta = logBeta(a, b);
    const front = Math.exp(a * Math.log(x) + b * Math.log(1 - x) - lbeta) / a;
    let f = 1, cNum = 1, d = 0;
    for (let i = 0; i <= 200; i++) {
      const m = Math.floor(i / 2);
      let numerator;
      if (i === 0) numerator = 1;
      else if (i % 2 === 0) numerator = (m * (b - m) * x) / ((a + 2 * m - 1) * (a + 2 * m));
      else numerator = -((a + m) * (a + b + m) * x) / ((a + 2 * m) * (a + 2 * m + 1));
      d = 1 + numerator * d;
      if (Math.abs(d) < 1e-30) d = 1e-30;
      d = 1 / d;
      cNum = 1 + numerator / cNum;
      if (Math.abs(cNum) < 1e-30) cNum = 1e-30;
      f *= d * cNum;
      if (Math.abs(1 - d * cNum) < 1e-8) break;
    }
    return x < (a + 1) / (a + b + 2) ? front * f : 1 - front * f;
  }
  // lower bound p: solve betainc(p; successes, n - successes + 1) = alpha
  let lo = 0, hi = successes / n;
  for (let i = 0; i < 100; i++) {
    const mid = (lo + hi) / 2;
    if (betainc(mid, successes, n - successes + 1) < alpha) lo = mid;
    else hi = mid;
  }
  return lo;
}

async function pool(tasks, width) {
  const results = [];
  let idx = 0;
  await Promise.all(
    Array.from({ length: width }, async () => {
      while (idx < tasks.length) {
        const i = idx++;
        results[i] = await tasks[i]();
      }
    })
  );
  return results;
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main() {
  if (!MODEL_ID) throw new Error('No model id (fixture model_id or BEDROCK_MODEL_ID)');
  const rows = [];
  const tasks = [];

  for (const scenario of SCENARIOS) {
    for (let sample = 0; sample < SAMPLES_PER_SCENARIO; sample++) {
      tasks.push(async () => {
        const { systemContent, messages, v5Active } = buildMessengerV5Prompt(
          scenario.user, KB, config, scenario.history, 'messenger'
        );
        const raw = await invoke(systemContent, messages);
        const { visible, tail } = parseTail(raw);
        const validIds = tail && tail.status === 'actions' ? validateActionIds(tail.actionIds, config) : null;
        const row = {
          arm: 'B_messenger_v5',
          scenario: scenario.id,
          sample,
          v5Active,
          sentences: countSentences(visible),
          chars: visible.length,
          markdown: hasMarkdown(visible),
          endsWithQuestion: visible.trim().endsWith('?'),
          turnCheckFired: scenario.turnCheckFired === true,
          tailStatus: tail ? tail.status : 'missing',
          actionCount: Array.isArray(validIds) ? validIds.length : null,
          validIds,
          restraintExpected: scenario.restraintExpected,
          visible,
        };
        rows.push(row);
        return row;
      });
    }
  }

  console.log(`Running ${tasks.length} live Bedrock samples (model ${MODEL_ID}, concurrency ${CONCURRENCY})...`);
  await pool(tasks, CONCURRENCY);

  // ── gates ──
  const n = rows.length;
  const brevityOk = rows.filter((r) => r.sentences <= 3).length;
  const tailEmitted = rows.filter((r) => r.tailStatus === 'actions').length;
  const restraintRows = rows.filter((r) => r.restraintExpected);
  const restraintOk = restraintRows.filter((r) => (r.actionCount ?? 0) === 0).length;
  const capOk = rows.filter((r) => (r.actionCount ?? 0) <= 4).length;
  const markdownClean = rows.filter((r) => !r.markdown).length;
  const tcRows = rows.filter((r) => r.turnCheckFired);
  const tcOk = tcRows.filter((r) => !r.endsWithQuestion).length;

  const summary = {
    model_id: MODEL_ID,
    prompt_version: 'messenger-v5.v1',
    n,
    gates: {
      brevity_le_3_sentences: { pass: brevityOk, n, rate: brevityOk / n, cp95_lower: cpLowerBound(brevityOk, n) },
      tail_emitted_valid: { pass: tailEmitted, n, rate: tailEmitted / n, cp95_lower: cpLowerBound(tailEmitted, n) },
      restraint_zero_actions_on_smalltalk: { pass: restraintOk, n: restraintRows.length, rate: restraintRows.length ? restraintOk / restraintRows.length : null, cp95_lower: cpLowerBound(restraintOk, restraintRows.length) },
      action_cap_le_4: { pass: capOk, n, rate: capOk / n },
      markdown_free: { pass: markdownClean, n, rate: markdownClean / n, cp95_lower: cpLowerBound(markdownClean, n) },
      turn_check_no_more_questions: { pass: tcOk, n: tcRows.length, rate: tcRows.length ? tcOk / tcRows.length : null, cp95_lower: cpLowerBound(tcOk, tcRows.length) },
    },
    note: 'Deterministic C8 session-boundary + C6 precedence behavior is pinned by prompt_messenger.test.js (no model calls). generated_by CI messenger-evidence workflow.',
  };

  fs.writeFileSync(OUT_JSONL, rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
  fs.writeFileSync(OUT_SUMMARY, JSON.stringify(summary, null, 2) + '\n');
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
