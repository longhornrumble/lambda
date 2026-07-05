#!/usr/bin/env node
/**
 * V5.2/V5.3 evidence harness — COMMITTED, reproducible re-run (retrospective
 * adversarial review 2026-07-05, blockers #2/#3 + majors #4/#5/#6).
 *
 * What this hardens vs the original scratchpad run (plan §10):
 *   - Reproducible from the repo (script + fixture + results committed).
 *   - Funnel-advance uses a HARD KB fixture: both programs + events +
 *     donations, discovery session mentioned as one option among many and
 *     never framed as "the first step" — the model must judge, the KB does
 *     not hand it the answer.
 *   - Stricter proposal judge: some single sentence must contain BOTH a
 *     step term AND an invitation marker (mention alone does not count).
 *   - Restraint + first-interest measured at the real 14-ai_available-CTA
 *     MYR catalog scale (the originals used 2-4 CTA eval fixtures).
 *   - n=150 V5 format samples in one run (95% rule-of-three UCB on the
 *     failure rate = 3/150 = 2.0% — the first sample size that actually
 *     certifies the ≥98% bar at 95% confidence).
 *   - Reply word-counts recorded (V5 vs V4) — the ACTION TAIL instruction
 *     sits after the word-limit REMINDER; this measures whether that
 *     placement dilutes length compliance.
 *   - Exact Clopper-Pearson 95% lower bounds reported per behavior gate.
 *
 * Run (dev SSO, LIVE Bedrock — ~220 calls, Haiku):
 *   AWS_PROFILE=myrecruiter-dev node evals/evidence/v5/run_evidence.js
 *
 * NOT part of jest / the eval net / the bundle — evidence only.
 */

'use strict';

const fs = require('fs');
const path = require('path');

const BSH = path.join(__dirname, '..', '..', '..');
const { buildV5TurnPrompt, V5_TURN_INFERENCE_PARAMS } = require(path.join(BSH, 'prompt_v5.js'));
const { createTailParser } = require(path.join(BSH, 'streamTail.js'));
const {
  buildV4ConversationPrompt,
  selectActionsV4,
  sanitizeTonePromptV4,
  V4_STEP2_INFERENCE_PARAMS,
} = require(path.join(BSH, 'prompt_v4.js'));
const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');

const OUT_JSONL = path.join(__dirname, 'results.jsonl');
const config = JSON.parse(fs.readFileSync(path.join(__dirname, 'myr_catalog_fixture.json'), 'utf8'));
const MODEL_ID = config.model_id || process.env.BEDROCK_MODEL_ID;
const CONCURRENCY = 4;

// ── fixtures ───────────────────────────────────────────────────────────────────

// HARD KB: monolithic style, both programs + events + giving; the discovery
// session is one option among many and is NOT framed as "the first step".
const KB_HARD = `Atlanta Angels supports children, youth, and families experiencing foster care. The Love Box program matches volunteer groups with a foster family — monthly Love Boxes, intentional relationship building, and a 12-month commitment. The Dare to Dream program matches adult mentors one-on-one with youth ages 11-22 in foster care; mentors meet their youth twice a month and commit to at least one year. Both programs require an application, a background check (we cover the cost), and program training. We host discovery sessions throughout the month where prospective volunteers can learn about both programs and meet our staff — [Discovery Session registration](https://www.atlantaangels.org/discovery). Applications: [Love Box](https://www.atlantaangels.org/lovebox-apply) · [Dare to Dream mentor](https://www.atlantaangels.org/dare-to-dream-apply). Other ways to help: [Donate](https://www.atlantaangels.org/donate), corporate partnerships, and our annual golf tournament.`;

const KB_GENERAL = `Atlanta Angels walks alongside children, youth, and families experiencing foster care through intentional giving and relationship building. Our two core programs: the Love Box program surrounds foster families with holistic support — volunteers are matched with a family and provide monthly Love Boxes, mentorship, and encouragement. The Dare to Dream program pairs youth (ages 11-22) in foster care with a dedicated mentor who meets with them monthly to build life skills, confidence, and community connections. Learn more at [Get Involved](https://www.atlantaangels.org/get-involved) or read about [our mission](https://www.atlantaangels.org/mission). You can also [make a donation](https://www.atlantaangels.org/donate) to support our work.`;

const FUNNEL_HISTORY = [
  { role: 'user', content: 'Tell me about your mentoring program.' },
  { role: 'assistant', content: 'Our Dare to Dream program pairs a caring adult mentor with a youth in foster care, ages 11-22. Mentors meet twice a month and focus on life skills and consistency. What draws you to mentoring?' },
  { role: 'user', content: 'What does being a mentor actually involve week to week?' },
  { role: 'assistant', content: 'Mentors meet with their youth about twice a month — grabbing a meal, practicing life skills like budgeting or job applications, or just showing up to their game. The heart of it is consistency. Does that rhythm feel doable for you?' },
  { role: 'user', content: "I'm retired, and honestly I feel like I have a lot of life's wisdom to share with young people who need someone in their corner." },
  { role: 'assistant', content: "That's exactly the kind of heart this program is built on — youth in foster care need someone steady who believes in them, and your life experience is a real gift. Mentors like you often say they get as much out of it as the youth do. What age group would you feel most comfortable working with?" },
];

const FUNNEL_STEP_IDS = new Set(['query_discoverysession', 'apply_daretodream_volunteer']);
const APPLY_VISIT_ACTIONS = new Set(['external_link', 'start_form']);

// Strict proposal judge: one sentence must carry BOTH a step term and an
// invitation marker. "You'll eventually fill out an application" no longer
// counts; "Would you like to join a discovery session?" does.
const STEP_RE = /discovery session|apply|application/i;
const INVITE_RE = /would you|want to|ready|register|sign up|attend|join|you can|let's|here's the link|next step/i;
function proposesStep(text) {
  return text
    .split(/(?<=[.!?])\s+|\n+/)
    .some((s) => STEP_RE.test(s) && INVITE_RE.test(s));
}

function idsHaveApplyVisit(ids) {
  return (ids || []).some((id) => APPLY_VISIT_ACTIONS.has(config.cta_definitions[id]?.action));
}

const wordCount = (t) => (t.trim().match(/\S+/g) || []).length;

// ── shapes ─────────────────────────────────────────────────────────────────────
// Every shape uses the REAL 14-ai_available-CTA MYR catalog.
const SHAPES = [
  {
    id: 'R1_restraint_scale', v5n: 25, v4n: 10,
    kb: null,
    history: [
      { role: 'user', content: 'What ages are the youth in the mentoring program?' },
      { role: 'assistant', content: 'Our Dare to Dream program serves youth ages 11 to 22 who are experiencing foster care. Is there a particular age group you had in mind?' },
    ],
    input: "Thanks, that's really helpful!",
    session: { accumulated_topics: ['mentoring'] },
    judge: (ids) => Array.isArray(ids) && ids.length === 0,
    criterion: 'ids === [] (restraint at 14-CTA catalog scale)',
  },
  {
    id: 'R2_interest_scale', v5n: 25, v4n: 10,
    kb: KB_GENERAL,
    history: [
      { role: 'assistant', content: "Hi! I'm here to help you learn about Atlanta Angels. What brings you by today?" },
      { role: 'user', content: 'Just curious about what you do.' },
    ],
    input: 'What kinds of volunteer opportunities do you have?',
    session: {},
    judge: (ids) => Array.isArray(ids) && !idsHaveApplyVisit(ids),
    criterion: 'no APPLY/VISIT-class action on a first-interest turn (14-CTA scale)',
  },
  {
    id: 'R3_funnel_hard', v5n: 25, v4n: 15,
    kb: KB_HARD,
    history: FUNNEL_HISTORY,
    input: "That all makes sense. I think I'd really be good at this.",
    session: { accumulated_topics: ['mentoring', 'dare_to_dream'] },
    judge: (ids, text) => Array.isArray(ids) && ids.some((id) => FUNNEL_STEP_IDS.has(id)) && proposesStep(text),
    criterion: 'concrete next-step button + sentence-level proposal, HARD KB (soft turn-4)',
  },
  // Format-volume shapes (V5 arm only) — replicate the V5.2 gate's coverage so
  // one run yields 150 V5 samples total (25+25+25+25+25+25).
  {
    id: 'F1_cold_start', v5n: 25, v4n: 0,
    kb: KB_GENERAL, history: [], input: 'Tell me about Atlanta Angels', session: {},
    judge: () => true, criterion: 'format volume only',
  },
  {
    id: 'F2_incident_2msg', v5n: 25, v4n: 0,
    kb: KB_HARD,
    history: [
      { role: 'user', content: 'Tell me about your mentoring program.' },
      { role: 'assistant', content: 'Our Dare to Dream program pairs a caring adult mentor with a young person in foster care, ages 11-22. Mentors meet with their youth twice a month to build life skills and confidence. Would you like to hear what being a mentor looks like?' },
    ],
    input: 'Learn about the volunteer process',
    session: { accumulated_topics: ['mentoring', 'dare_to_dream'] },
    judge: () => true, criterion: 'format volume only',
  },
  {
    id: 'F3_long_emoji_links', v5n: 25, v4n: 0,
    kb: KB_GENERAL + '\n\n' + KB_HARD, history: [],
    input: 'What are all the ways I can help — volunteering, donating, everything?',
    session: {},
    configOverride: {
      bedrock_instructions: {
        ...(config.bedrock_instructions || {}),
        formatting_preferences: {
          ...((config.bedrock_instructions || {}).formatting_preferences || {}),
          detail_level: 'comprehensive', emoji_usage: 'generous', max_emojis_per_response: 6,
        },
      },
    },
    judge: () => true, criterion: 'format volume only (adversarial length/emoji/links)',
  },
];

// ── live plumbing ──────────────────────────────────────────────────────────────
const client = new BedrockRuntimeClient({ region: process.env.AWS_REGION || 'us-east-1' });

async function invoke(prompt, params, modelId) {
  const res = await client.send(new InvokeModelCommand({
    modelId, accept: 'application/json', contentType: 'application/json',
    body: JSON.stringify({
      anthropic_version: 'bedrock-2023-05-31',
      messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
      max_tokens: params.max_tokens, temperature: params.temperature,
    }),
  }));
  const payload = JSON.parse(new TextDecoder().decode(res.body));
  const text = (payload.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('');
  return { text, stopReason: payload.stop_reason };
}

async function withRetry(fn, tries = 3) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try { return await fn(); } catch (e) { lastErr = e; await new Promise((r) => setTimeout(r, 2000 * (i + 1))); }
  }
  throw lastErr;
}

async function pool(tasks, width) {
  const results = new Array(tasks.length);
  let next = 0;
  async function worker() {
    for (;;) {
      const i = next++;
      if (i >= tasks.length) return;
      results[i] = await tasks[i]();
    }
  }
  await Promise.all(Array.from({ length: Math.min(width, tasks.length) }, worker));
  return results;
}

// Exact one-sided 95% Clopper-Pearson lower bound on p, given k successes / n.
function cpLower(k, n, conf = 0.95) {
  if (k === 0) return 0;
  const alpha = 1 - conf;
  const tailAtLeastK = (p) => {
    // P(X >= k) for X ~ Bin(n, p), computed iteratively.
    let term = Math.pow(1 - p, n); // P(X=0)
    let cdf = term;
    for (let x = 1; x < k; x++) {
      term *= ((n - x + 1) / x) * (p / (1 - p));
      cdf += term;
    }
    return 1 - cdf;
  };
  let lo = 0, hi = k / n;
  for (let i = 0; i < 60; i++) {
    const mid = (lo + hi) / 2;
    if (tailAtLeastK(mid) < alpha) lo = mid; else hi = mid;
  }
  return lo;
}

// ── main ───────────────────────────────────────────────────────────────────────
async function main() {
  fs.writeFileSync(OUT_JSONL, '');
  const rows = [];
  const record = (row) => { rows.push(row); fs.appendFileSync(OUT_JSONL, JSON.stringify(row) + '\n'); };

  const tasks = [];
  for (const shape of SHAPES) {
    const effConfig = shape.configOverride ? { ...config, ...shape.configOverride } : config;
    const tone = sanitizeTonePromptV4(effConfig.tone_prompt);
    const v5prompt = buildV5TurnPrompt(shape.input, shape.kb, tone, shape.history, effConfig, shape.session);
    const v4prompt = buildV4ConversationPrompt(shape.input, shape.kb, tone, shape.history, effConfig, shape.session);

    for (let i = 0; i < shape.v5n; i++) {
      tasks.push(async () => {
        const { text, stopReason } = await withRetry(() => invoke(v5prompt, V5_TURN_INFERENCE_PARAMS, MODEL_ID));
        const parser = createTailParser();
        const fwd = parser.feed(text);
        const { remainingText, actionIds, status, trailingAfterClose } = parser.end();
        const visible = fwd + remainingText;
        record({
          arm: 'v5', shape: shape.id, i, status, actionIds, stopReason, trailingAfterClose,
          formatOk: status === 'actions',
          pass: status === 'actions' && shape.judge(actionIds, visible),
          words: wordCount(visible),
          leak: visible.includes('<<<ACTIONS'),
          fullText: text,
        });
      });
    }
    for (let i = 0; i < shape.v4n; i++) {
      tasks.push(async () => {
        const { text } = await withRetry(() => invoke(v4prompt, V4_STEP2_INFERENCE_PARAMS, MODEL_ID));
        const history = [...shape.history, { role: 'user', content: shape.input }];
        const ids = await withRetry(() => selectActionsV4(text, history, effConfig, client));
        record({
          arm: 'v4', shape: shape.id, i, actionIds: ids,
          pass: shape.judge(ids, text),
          words: wordCount(text),
          fullText: text,
        });
      });
    }
  }

  console.log(`running ${tasks.length} live samples (concurrency ${CONCURRENCY})...`);
  await pool(tasks, CONCURRENCY);

  // ── summary ──────────────────────────────────────────────────────────────────
  const v5all = rows.filter((r) => r.arm === 'v5');
  const fmtOk = v5all.filter((r) => r.formatOk).length;
  const leaks = v5all.filter((r) => r.leak).length;
  const trunc = v5all.filter((r) => r.stopReason === 'max_tokens').length;
  const trailing = v5all.filter((r) => r.trailingAfterClose).length;

  console.log('\n════ FORMAT (all V5 samples) ════');
  console.log(`sentinel present + valid JSON: ${fmtOk}/${v5all.length} (95% CP lower bound ${(cpLower(fmtOk, v5all.length) * 100).toFixed(1)}%; gate ≥98%)`);
  console.log(`leaks=${leaks} maxTokensTruncations=${trunc} trailingAfterClose=${trailing}`);

  console.log('\n════ BEHAVIOR GATES ════');
  for (const shape of SHAPES.filter((s) => s.v4n > 0)) {
    const v5 = rows.filter((r) => r.arm === 'v5' && r.shape === shape.id);
    const v4 = rows.filter((r) => r.arm === 'v4' && r.shape === shape.id);
    const v5p = v5.filter((r) => r.pass).length;
    const v4p = v4.filter((r) => r.pass).length;
    const v5w = v5.map((r) => r.words).sort((a, b) => a - b);
    const v4w = v4.map((r) => r.words).sort((a, b) => a - b);
    console.log(`${shape.id} [${shape.criterion}]`);
    console.log(`  V5 ${v5p}/${v5.length} (95% CP lower ${(cpLower(v5p, v5.length) * 100).toFixed(0)}%) | V4.0 ${v4p}/${v4.length}`);
    console.log(`  median words: V5 ${v5w[Math.floor(v5w.length / 2)]} | V4.0 ${v4w[Math.floor(v4w.length / 2)]}`);
  }
  console.log(`\nresults: ${OUT_JSONL}`);
}

main().catch((e) => { console.error(e); process.exit(2); });
