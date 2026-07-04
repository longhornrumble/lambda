/**
 * Groundedness judge (chat-experience eval net, sub-phase 1.4).
 *
 * A focused Haiku-4.5 call (temperature 0) that decides whether an assistant
 * reply stays inside the facts of the recorded KB. It runs ONLY for the
 * `grounded_in_kb` assertion — the deterministic assertions in run.js catch
 * everything a regex can (URLs, forbidden phrases, CTA ids); the judge covers
 * the fuzzy "did it invent a fact not in the KB" that a regex cannot.
 *
 * Verdicts: GROUNDED (pass), UNGROUNDED (fail), UNSURE (route to human review —
 * NOT an auto pass/fail). Unparseable output is treated as UNSURE so an
 * ambiguous judge never silently passes a scenario.
 *
 * This is EVAL-HARNESS infrastructure, not product prompt text — it lives in
 * evals/ and carries its OWN version constant so a change to the judge wording
 * is traceable and (for scenarios that ran it) invalidates their baseline the
 * same way a product-prompt-version bump does. It intentionally does NOT live in
 * prompt_v4.js and changes no deployed behavior.
 */

'use strict';

// Bump when the judge prompt wording below changes. Stamped into every result
// that ran the judge; a change makes those scenarios' baselines `stale_baseline`
// (see compareToBaseline in run.js) so they are re-confirmed deliberately.
const GROUNDEDNESS_JUDGE_PROMPT_VERSION = 'v1';

/**
 * Build the groundedness-judge prompt. PURE.
 * @param {string|null} kbContext - the recorded KB passages, or null/empty for a KB-miss turn
 * @param {string} userInput - the user's current message
 * @param {string} responseText - the assistant reply under test
 * @returns {string}
 */
function buildGroundednessJudgePrompt(kbContext, userInput, responseText) {
  const kb = String(kbContext == null ? '' : kbContext).trim();
  const kbBlock = kb || '(no knowledge base was retrieved for this turn — it is empty)';
  return `You are a strict groundedness judge for a nonprofit organization's chat assistant.

KNOWLEDGE BASE:
${kbBlock}

USER ASKED:
${String(userInput || '').trim()}

ASSISTANT REPLIED:
${String(responseText || '').trim()}

Your job: decide whether EVERY factual claim in the ASSISTANT reply about the
organization — its programs, schedule, times, locations, contacts, eligibility,
prices, availability, or links — is supported by the KNOWLEDGE BASE above.

Rules:
- If the KNOWLEDGE BASE is empty, the reply must make NO specific factual claim.
  Offering to help, offering to connect the user to someone, or asking a
  clarifying question is fine and counts as grounded.
- Inventing a fact, URL, time, date, price, or availability that is not in the
  KNOWLEDGE BASE is UNGROUNDED, even if it sounds plausible.
- General empathy, encouragement, and clarifying questions are always grounded.
- Do not penalize paraphrasing or reasonable summarization of KB facts.

Answer with EXACTLY one word on the first line — GROUNDED, UNGROUNDED, or UNSURE.
Use UNSURE only when you genuinely cannot tell from the KNOWLEDGE BASE.`;
}

/**
 * Parse the judge's raw text into a normalized verdict. PURE.
 * Order matters: UNGROUNDED is checked before GROUNDED (the latter is a
 * substring of the former). Anything unparseable → 'unsure' (never a silent pass).
 * @param {string} rawText
 * @returns {'grounded'|'ungrounded'|'unsure'}
 */
function parseJudgeVerdict(rawText) {
  const firstLine = String(rawText || '').trim().split(/\r?\n/)[0].trim().toUpperCase();
  if (/^UNGROUNDED\b/.test(firstLine)) return 'ungrounded';
  if (/^UNSURE\b/.test(firstLine)) return 'unsure';
  if (/^GROUNDED\b/.test(firstLine)) return 'grounded';
  // First line wasn't a bare verdict — scan the whole reply (UNGROUNDED first).
  const upper = String(rawText || '').toUpperCase();
  if (upper.includes('UNGROUNDED')) return 'ungrounded';
  if (upper.includes('UNSURE')) return 'unsure';
  if (upper.includes('GROUNDED')) return 'grounded';
  return 'unsure';
}

module.exports = {
  GROUNDEDNESS_JUDGE_PROMPT_VERSION,
  buildGroundednessJudgePrompt,
  parseJudgeVerdict,
};
