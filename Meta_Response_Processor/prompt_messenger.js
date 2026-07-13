'use strict';

/**
 * Messenger V5 single-pass prompt — M3a (Messenger Channel Experience).
 *
 * Composes the Messenger short-form base prompt with the SHARED V5 blocks
 * (action catalog, server-counted turn check, machine-read action tail —
 * shared/prompt/prompt_v5.js; frozen in M2, never forked here).
 *
 * Precedence (contract C6, lowest → highest; each layer replaces, never
 * concatenates):
 *   1. code-owned base rules (brevity/format — locked, not tenant-overridable)
 *   2. config.tone_prompt
 *   3. config.messenger_behavior.tone_override
 *   4. config.messenger_behavior.channel_overrides.{channel}.tone_override
 *
 * History passed in MUST already be session-scoped (sessionWindow.js, C8) —
 * this module deliberately has no session logic so the turn-check block can
 * never see lifetime-thread history by construction.
 *
 * Returns the same {systemContent, messages} shape as buildMessengerPrompt
 * plus v5Active so the caller knows whether to run the tail parse (M3b).
 */

const {
  buildActionCatalogBlock,
  buildTurnCheckBlock,
  buildActionTailInstruction,
} = require('../shared/prompt/prompt_v5');

const MESSENGER_V5_PROMPT_VERSION = 'messenger-v5.v1';

const DEFAULT_TONE = 'You are a helpful assistant.';

// Code-owned, locked (C6 layer 1). Semantics carried over from the legacy
// buildMessengerPrompt STRICT RULES — short-form is the channel's nature,
// not a tenant preference.
const MESSENGER_BASE_RULES =
  'You are responding via a mobile messaging app (Facebook Messenger or Instagram DM) where the chat window is very small. STRICT RULES: Respond in 2-3 short sentences maximum. Be friendly but direct. No lists, no bullet points, no headers, no markdown, no asterisks or formatting symbols - plain conversational text only (Messenger and Instagram render formatting characters literally). Never write more than 3 sentences in a single response. If the user wants more detail, they will ask a follow-up question.';

/** Resolve the model per C6: channel override -> section -> config.model_id -> caller default. */
function resolveMessengerModelId(config, channelType, fallbackModelId) {
  const mb = config?.messenger_behavior;
  const channelKey = channelType === 'instagram' ? 'instagram' : 'messenger';
  return (
    mb?.channel_overrides?.[channelKey]?.model_id ??
    mb?.model_id ??
    config?.model_id ??
    fallbackModelId
  );
}

/** Resolve the persona layer per C6 (replace, never concatenate). */
function resolveMessengerTone(config, channelType) {
  const mb = config?.messenger_behavior;
  const channelKey = channelType === 'instagram' ? 'instagram' : 'messenger';
  return (
    mb?.channel_overrides?.[channelKey]?.tone_override ??
    mb?.tone_override ??
    config?.tone_prompt ??
    DEFAULT_TONE
  );
}

/**
 * Build the Messenger V5 prompt.
 *
 * @param {string} userInput — sanitised user message
 * @param {string} kbContext — KB retrieval result ('' when none)
 * @param {object} config — tenant config
 * @param {Array<object>} sessionHistory — SESSION-SCOPED rows (sessionWindow.js)
 * @param {'messenger'|'instagram'} channelType
 * @returns {{ systemContent: string, messages: Array<object>, v5Active: boolean, promptVersion: string }}
 */
function buildMessengerV5Prompt(userInput, kbContext, config, sessionHistory, channelType) {
  const tone = resolveMessengerTone(config, channelType);
  const catalogBlock = buildActionCatalogBlock(config);
  const v5Active = catalogBlock.length > 0;

  // Turn check counts assistant questions in the CURRENT SESSION only (C8) —
  // guaranteed by the caller passing session-scoped history.
  const turnCheckBlock = v5Active ? buildTurnCheckBlock(sessionHistory) : '';

  const systemContent = [
    tone,
    MESSENGER_BASE_RULES,
    catalogBlock,
    kbContext ? `Relevant information from the knowledge base:\n${kbContext}` : '',
    turnCheckBlock,
    v5Active ? buildActionTailInstruction() : '',
  ]
    .filter(Boolean)
    .join('\n\n');

  const maxTurns =
    typeof config?.messenger_behavior?.max_history_turns === 'number'
      ? config.messenger_behavior.max_history_turns
      : 5;
  const recentHistory = (sessionHistory || []).slice(-maxTurns * 2);
  const messages = recentHistory.map((m) => ({
    role: m.role,
    content: [{ type: 'text', text: m.content }],
  }));
  messages.push({ role: 'user', content: [{ type: 'text', text: userInput }] });

  return { systemContent, messages, v5Active, promptVersion: MESSENGER_V5_PROMPT_VERSION };
}

module.exports = {
  buildMessengerV5Prompt,
  resolveMessengerTone,
  resolveMessengerModelId,
  MESSENGER_BASE_RULES,
  MESSENGER_V5_PROMPT_VERSION,
};
