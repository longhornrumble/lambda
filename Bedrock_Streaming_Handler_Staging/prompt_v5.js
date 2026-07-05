/**
 * V5 single-pass turn prompt (V5.2 of docs/roadmap/V5_SINGLE_PASS_TURN_PLAN.md,
 * picasso repo).
 *
 * ONE streaming call produces the reply AND selects the actions: the V4
 * conversational prompt (persona / KB / history / session context / locked
 * rules / custom constraints) is extended with the action catalog, the
 * transferred V4.0 selector rules, and a machine-read tail instruction. The
 * model streams prose, then emits `<<<ACTIONS [...]>>>` on its own line at the
 * stream tail; streamTail.js strips it server-side (V5.1).
 *
 * Construction: buildV5TurnPrompt REUSES buildV4ConversationPrompt and splices
 * the two V5 sections in, rather than maintaining a parallel copy of the
 * response prompt — V4 prompt evolution (formatting prefs, locked rules,
 * session block) flows into V5 automatically. The splice anchors on the
 * `━━━ USER MESSAGE ━━━` section marker; a contract test pins that marker's
 * presence so a V4 refactor can't silently break the splice.
 *
 * NOT wired into the request path until V5.5 (flag `V5_SINGLE_PASS`).
 */

'use strict';

const {
  buildV4ConversationPrompt,
  intentLabel,
} = require('./prompt_v4');
const { SENTINEL_OPEN, SENTINEL_CLOSE } = require('./streamTail');

// ─── Prompt version stamp (chat-experience eval net contract) ─────────────────
// Bump whenever the V5 prompt TEXT changes (either section below, or via a
// deliberate adoption of upstream buildV4ConversationPrompt text changes —
// those bump V4_CONVERSATION_PROMPT_VERSION, which the eval runner pairs with
// this constant for single-pass scenarios).
//
// v5-turn.v1 (2026-07-05, V5.2): first draft — V4 conversation prompt + action
//   catalog (`id — label [INTENT]`, ai_available CTAs) + transferred V4.0
//   selector rules (restraint-first, commitment gate) reworded inline + a
//   reply/action coherence rule + machine-read ACTION TAIL instruction.
const V5_TURN_PROMPT_VERSION = 'v5-turn.v1';

// Like V4_STEP2_INFERENCE_PARAMS but with headroom for the action tail: at
// max_tokens the tail would be truncated mid-sentinel → malformed → fallback
// ladder. ~100 extra tokens covers the tail (~25 tokens) plus margin.
// Only these two params: Haiku 4.5 rejects temperature+top_p together
// (verified live 2026-07-05), and production sends only these anyway.
const V5_TURN_INFERENCE_PARAMS = {
  temperature: 0.35,
  max_tokens: 700,
};

const USER_MESSAGE_MARKER = '━━━ USER MESSAGE ━━━';

/**
 * Build the action-catalog + rules section from the tenant's ai_available
 * CTAs. Same vocabulary line format as selectActionsV4 (`id — label [INTENT]`).
 * Returns '' when the config has no ai_available CTAs (prompt then carries no
 * action sections and the tail instruction is omitted — callers treat that as
 * "V5 has nothing to select from").
 */
function buildActionCatalogBlock(config) {
  const vocabulary = Object.entries(config?.cta_definitions || {})
    .filter(([, cta]) => cta.ai_available)
    .map(([id, cta]) => `  ${id} — ${cta.label} [${intentLabel(cta.action)}]`)
    .join('\n');
  if (!vocabulary) return '';

  return `━━━ ACTIONS ━━━
The chat widget can show buttons under your reply. At the very end of your reply you will list which (if any) to show — the last rule below explains the exact format.

AVAILABLE ACTIONS:
${vocabulary}

ACTION RULES:
- Choose 0-4 actions. Each action is labeled LEARN, APPLY, VISIT, INFO, or SCHEDULE.
- RESTRAINT FIRST: only offer an action when it genuinely helps the user take a next step they are reaching for. Most turns need NONE — a normal answer, a thank-you, a clarification, or small talk gets no actions. Never add actions just to fill a menu under every reply.
- When the user is actively exploring a specific program, one focused, relevant LEARN action can help them go deeper. One well-chosen action beats a list.
- APPLY/VISIT ONLY WHEN COMMITTED: only offer APPLY or VISIT actions when the user has unprompted said "I want to apply", "sign me up", "I'm ready", "let's donate", or similar. Answering your question or expressing general interest is NOT commitment.
- COHERENCE: the actions must match what your reply just said. If your reply proposes a concrete next step, attach that step's action. Never attach an action your reply gives no reason for.`;
}

/**
 * The machine-read tail instruction. Placed at the absolute end of the prompt
 * (recency bias — format discipline is the thing we most need obeyed).
 */
function buildActionTailInstruction() {
  return `━━━ ACTION TAIL (machine-read, required) ━━━
After your reply, output one final line, exactly:
${SENTINEL_OPEN} ["action_id","action_id"]${SENTINEL_CLOSE}
- A JSON array of the 0-4 action IDs you chose from AVAILABLE ACTIONS, all on that ONE line, nothing after it.
- If no actions fit (most turns), output: ${SENTINEL_OPEN} []${SENTINEL_CLOSE}
- ALWAYS output this line, exactly once. It is stripped before the user sees your reply — never mention it, the IDs, or "actions" in your prose.`;
}

/**
 * Build the complete V5 single-pass turn prompt.
 *
 * Same signature as buildV4ConversationPrompt. When the config has no
 * ai_available CTAs the result is byte-identical to the V4 prompt (no action
 * sections, no tail) — the caller should then skip tail parsing.
 *
 * @returns {string}
 */
function buildV5TurnPrompt(userInput, kbContext, tonePrompt, conversationHistory, config, sessionContext = {}) {
  const base = buildV4ConversationPrompt(userInput, kbContext, tonePrompt, conversationHistory, config, sessionContext);

  const catalogBlock = buildActionCatalogBlock(config);
  if (!catalogBlock) return base;

  const markerAt = base.indexOf(USER_MESSAGE_MARKER);
  if (markerAt === -1) {
    // Contract-tested never to happen; fail loud in dev rather than silently
    // emitting a prompt without the catalog the tail instruction refers to.
    throw new Error('buildV5TurnPrompt: USER MESSAGE marker not found in V4 prompt');
  }

  return (
    base.slice(0, markerAt) +
    catalogBlock +
    '\n\n' +
    base.slice(markerAt) +
    '\n\n' +
    buildActionTailInstruction()
  );
}

module.exports = {
  V5_TURN_PROMPT_VERSION,
  V5_TURN_INFERENCE_PARAMS,
  USER_MESSAGE_MARKER,
  buildV5TurnPrompt,
  buildActionCatalogBlock,
  buildActionTailInstruction,
};
