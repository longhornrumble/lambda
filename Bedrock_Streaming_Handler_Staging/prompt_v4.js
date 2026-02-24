/**
 * V4 Prompt Builders
 *
 * Two focused prompt builders for the V4 two-step pipeline:
 *   buildV4ConversationPrompt() — Step 2: streaming conversational response
 *   buildV4ActionSelectorPrompt() — Step 3: synchronous CTA selection
 *
 * Design principles:
 *   - Each function has ONE job. No vocabulary in Step 2. No persona in Step 3.
 *   - Locked rules (anti-hallucination, loop prevention, etc.) are embedded and
 *     NOT tenant-customizable. They survived the V3.5 → V4 review because each
 *     one addresses a documented, observed failure mode.
 *   - Custom constraints from the tenant config are appended AFTER locked rules
 *     but BEFORE the final instruction, so they can add constraints without
 *     overriding safety rules.
 *   - Step 3 is designed for Bedrock tool_use (structured output). A plain-text
 *     fallback is provided for environments where tool_use is unavailable.
 */

'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: Conversational Response Prompt
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build the Step 2 conversational prompt.
 *
 * @param {string} userInput - The current user message
 * @param {string|null} kbContext - Retrieved knowledge base passages, or null if no results
 * @param {string} tonePrompt - Sanitized persona/tone from tenant config
 * @param {Array} conversationHistory - [{role, content}] — all prior messages
 * @param {Object} config - Full tenant config object
 * @returns {string} Complete prompt string for the streaming Bedrock call
 */
function buildV4ConversationPrompt(userInput, kbContext, tonePrompt, conversationHistory, config) {
  console.log('[V4] Building Step 2 conversational prompt');

  const chatTitle = config?.chat_title || 'our organization';
  const turnCount = (conversationHistory || []).filter(m => m.role === 'user').length;

  // ── PERSONA ──────────────────────────────────────────────────────────────
  // Use sanitized tone prompt (removes inline-link instructions that conflict
  // with button-based CTAs). Fall back to a generic persona.
  const rawPersona = tonePrompt || config?.tone_prompt ||
    `You are a friendly, knowledgeable team member at ${chatTitle} who genuinely cares about helping people find the right program or resource for them.`;
  const persona = sanitizeTonePromptV4(rawPersona);

  // ── KNOWLEDGE BASE BLOCK ─────────────────────────────────────────────────
  let kbBlock;
  if (kbContext) {
    kbBlock = `━━━ KNOWLEDGE BASE ━━━
Use ONLY this information to answer. Never add details not found below.
Do not reproduce markdown action links from this content — action buttons are provided separately.

${kbContext}`;
  } else {
    const fallbackMessage = config?.bedrock_instructions?.fallback_message ||
      `I don't have specific information about that right now. I'm happy to help with questions about ${chatTitle}'s programs and services — or I can connect you with someone who can help directly.`;
    kbBlock = `━━━ NO KNOWLEDGE BASE RESULTS ━━━
Respond with this message (or a natural variation of it):
"${fallbackMessage}"
Do not add information not present above.`;
  }

  // ── CONVERSATION HISTORY BLOCK ────────────────────────────────────────────
  // Compression strategy: keep ALL user messages (they carry intent + PII context)
  // but only the last 2 assistant responses (they are large, and earlier ones
  // are low-signal for the current response).
  let historyBlock = '';
  if (conversationHistory && conversationHistory.length > 0) {
    const recentCutoff = conversationHistory.length - 4; // last 2 exchanges = last 4 messages
    const lines = [];

    for (let i = 0; i < conversationHistory.length; i++) {
      const msg = conversationHistory[i];
      const content = (msg.content || msg.text || '').trim();
      if (!content) continue;

      if (msg.role === 'user') {
        lines.push(`User: ${content}`);
      } else if (i >= recentCutoff) {
        lines.push(`You: ${content}`);
      }
      // Earlier assistant responses dropped — they are the bulk of prompt size
    }

    if (lines.length > 0) {
      historyBlock = `━━━ CONVERSATION SO FAR ━━━
${lines.join('\n')}`;
    }
  }

  // ── LOCKED RULES ──────────────────────────────────────────────────────────
  // These are NEVER removed or overridden by tenant config.
  // Each rule addresses a documented, observed failure mode in V3.5 testing.
  const lockedRules = buildV4LockedRules(kbContext !== null, turnCount);

  // ── TENANT CUSTOM CONSTRAINTS ─────────────────────────────────────────────
  // Tenant-provided rules (from config.bedrock_instructions.custom_constraints).
  // Filtered to remove follow-up question rules (the locked rules handle that).
  const customConstraintsBlock = buildV4CustomConstraints(config);

  // ── ASSEMBLE ─────────────────────────────────────────────────────────────
  const sections = [
    persona,
    '',
    kbBlock,
    '',
  ];

  if (historyBlock) {
    sections.push(historyBlock);
    sections.push('');
  }

  sections.push(lockedRules);

  if (customConstraintsBlock) {
    sections.push('');
    sections.push(customConstraintsBlock);
  }

  sections.push('');
  sections.push(`━━━ USER MESSAGE ━━━`);
  sections.push(`USER: ${userInput}`);
  sections.push('');
  sections.push(buildV4FinalInstruction(kbContext !== null));

  const prompt = sections.join('\n');

  console.log(`[V4] Step 2 prompt: ${prompt.length} chars, turn ${turnCount + 1}`);
  return prompt;
}

/**
 * Build the locked rules block for Step 2.
 * These rules are invariant — they do not change per tenant.
 *
 * @param {boolean} hasKb - Whether KB context is present
 * @param {number} turnCount - Number of prior user turns in this session
 * @returns {string}
 */
function buildV4LockedRules(hasKb, turnCount) {
  const rules = [];

  rules.push(`━━━ RESPONSE RULES ━━━`);

  // ── Source constraint (only meaningful when KB is present) ──────────────
  if (hasKb) {
    rules.push(`SOURCE CONSTRAINT
- Answer using only the facts in the Knowledge Base above. Do not add information that is not explicitly stated there.
- Do not change program names, eligibility rules, or numeric details. If the KB says "two programs," do not list three.
- If the KB does not contain the specific detail the user asked about, say "I don't have details on that" rather than inferring or filling in.
- Do not reproduce markdown action links (e.g., [Apply Now →](url)) from the KB in your response text. Action buttons are handled separately by the system.`);
  }

  // ── Context continuity ─────────────────────────────────────────────────
  rules.push(`CONTEXT CONTINUITY
- If the user's message is a short affirmative ("yes," "sure," "okay," "go ahead," "tell me more," "please"), treat it as a continuation of the prior topic. Answer the question you just asked or continue where you left off.
- Do not start your response with "I noticed you said yes," "Since you expressed interest," or any phrase that restates the affirmative. Just continue.
- Do not re-introduce information you already covered. Move forward.`);

  // ── Loop prevention ────────────────────────────────────────────────────
  // Abridged version of V3.5 getLockedLoopPrevention() — same behavior, fewer tokens.
  // Only inject the stage-3 escalation instruction after turn 2, where it is relevant.
  let loopRule = `LOOP PREVENTION
- Before responding, check what you covered in the prior turn. Do not repeat it.
- If you asked a follow-up question last turn and the user responded, answer that question directly — do not ask a different question in its place.`;

  if (turnCount >= 2) {
    loopRule += `
- This user has confirmed interest multiple times. Provide the direct resource, link, or next step. Do not ask again whether they want it.`;
  }

  rules.push(loopRule);

  // ── Phrasing guardrails ────────────────────────────────────────────────
  rules.push(`PHRASING
- Do not say: "Based on the knowledge base," "According to the information provided," "I found that," "As mentioned earlier," or "Based on our previous conversation."
- Do not offer to "walk the user through a form," "help them fill it out," or "guide them step by step through the application." You can provide information and links — you cannot interact with external systems.
- Do not use the phrase "Is there anything else I can help you with?" unless the conversation has reached a natural end point and there is genuinely no more to say on this topic.
- Match the user's energy. If they are warm and enthusiastic, be warm. If they are frustrated or confused, be calm and direct.`);

  // ── Formatting ─────────────────────────────────────────────────────────
  rules.push(`FORMATTING
- Write in prose paragraphs. Do not use markdown headers (lines starting with #). This is a chat conversation, not a document.
- Use bullet points only for genuine lists of 3 or more discrete, parallel items. Do not use bullets to pad a response.
- Keep responses concise: 2 to 4 short paragraphs for most answers. Include relevant KB details without padding.
- Preserve all URLs and email addresses from the KB exactly as written. Do not shorten them to "their website" or "reach out to the team."`);

  // ── Closing rule ───────────────────────────────────────────────────────
  rules.push(`CLOSING
- End every response with a specific, contextual follow-up question that invites the next natural step. The question must relate directly to the topic you just discussed.
- Good examples: "Would you like to know the eligibility requirements?" / "The application takes about 10 minutes — want me to explain what it asks for?"
- Do not use generic closers: "Is there anything else I can help you with?" / "Do you have any other questions?" / "Would you like more information?" — these are not specific.
- Exception: if the user has reached a clear end point (e.g., you just provided the direct application link and they said "thanks"), a brief warm close is appropriate.`);

  return rules.join('\n\n');
}

/**
 * Build the final instruction line at the end of the Step 2 prompt.
 * Positioned last for recency bias.
 */
function buildV4FinalInstruction(hasKb) {
  if (hasKb) {
    return `Respond conversationally using only the KB facts above. End with a specific follow-up question that moves the conversation forward.`;
  }
  return `Use the fallback message above. Offer to help with topics you do have information about.`;
}

/**
 * Build the custom constraints block from tenant config.
 * Filters out follow-up question rules (the locked rules handle that behavior).
 *
 * @param {Object} config
 * @returns {string} — empty string if no constraints
 */
function buildV4CustomConstraints(config) {
  const instructions = config?.bedrock_instructions;
  if (!instructions || !Array.isArray(instructions.custom_constraints) || instructions.custom_constraints.length === 0) {
    return '';
  }

  // Filter out constraints that would conflict with locked loop/engagement rules
  const blocked = ['follow-up question', 'follow up question', 'end with a question'];
  const filtered = instructions.custom_constraints.filter(c => {
    const lower = c.toLowerCase();
    return !blocked.some(phrase => lower.includes(phrase));
  });

  if (filtered.length === 0) return '';

  return `━━━ ADDITIONAL RULES ━━━\n` + filtered.map(c => `- ${c}`).join('\n');
}

/**
 * Sanitize tone prompt for V4.
 * Removes instructions about inline links/CTAs that conflict with the button-based UI.
 * Preserves persona, warmth, and any topic-specific tone guidance.
 *
 * @param {string} tonePrompt
 * @returns {string}
 */
function sanitizeTonePromptV4(tonePrompt) {
  if (!tonePrompt) return '';

  const blockedPhrases = [
    'inline link',
    'calls to action',
    'contact information, or calls',
    'include relevant',
    'provide links',
    'insert links',
  ];

  return tonePrompt
    .split('.')
    .filter(sentence => {
      const lower = sentence.toLowerCase();
      return !blockedPhrases.some(phrase => lower.includes(phrase));
    })
    .join('.')
    .trim();
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: Action Selector Prompt
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build the Step 3 action selector prompt.
 *
 * This is a non-streaming, synchronous call. The model has already completed
 * the Step 2 response and we now ask it which CTAs the user is ready for.
 *
 * Use with Bedrock tool_use (structured output) when available — the tool schema
 * forces a valid JSON array without requiring output format policing in the prompt.
 *
 * @param {string} step2Response - The full text of the Step 2 response
 * @param {Array} conversationHistory - Full conversation history [{role, content}]
 * @param {Object} config - Full tenant config
 * @param {Object} sessionContext - Session context including completed_forms
 * @returns {{ systemPrompt: string, conversationBlock: string }}
 */
function buildV4ActionSelectorPrompt(step2Response, conversationHistory, config, sessionContext = {}) {
  console.log('[V4] Building Step 3 action selector prompt');

  const completedForms = sessionContext?.completed_forms || [];

  // ── VOCABULARY BLOCK ──────────────────────────────────────────────────────
  const { vocabularyBlock, validIds } = buildV4VocabularyBlock(config, completedForms);

  // ── CONVERSATION BLOCK ────────────────────────────────────────────────────
  // Last 2 user messages + the Step 2 response.
  // Earlier assistant messages are dropped — they are low-signal for classification
  // and increase token cost for no benefit.
  const conversationBlock = buildV4ConversationBlock(conversationHistory, step2Response);

  // ── COMPLETED FORMS BLOCK ─────────────────────────────────────────────────
  let completedBlock = '';
  if (completedForms.length > 0) {
    completedBlock = `\nCOMPLETED FORMS (the user has already submitted these — do not select them):
${completedForms.map(f => `  - ${f}`).join('\n')}`;
  }

  // ── SYSTEM PROMPT ─────────────────────────────────────────────────────────
  const systemPrompt = `You are an action selector. Your only job is to decide which actions a user is ready to take based on a chatbot conversation.

AVAILABLE ACTIONS:
${vocabularyBlock}
${completedBlock}

SELECTION RULES:
1. Only select an action if the user has expressed clear, specific intent for that action in the conversation below. "I want to volunteer" is clear intent. Asking what programs exist is not.
2. Return an empty array in these cases:
   - The user is asking a factual or informational question and has not indicated readiness to act
   - The user is still exploring programs and has not expressed preference or intent
   - The user expressed frustration, confusion, or asked for human contact
   - The chatbot response was a fallback ("I don't have information on that")
   - The conversation has no clear action signal
3. Do not select an action just because the topic was mentioned. The user must have shown readiness.
4. Do not select more than 3 actions. If more than 3 qualify, select the most contextually relevant ones.
5. Only use IDs from the AVAILABLE ACTIONS list above. Do not invent or modify IDs.

CONVERSATION:
${conversationBlock}

Call select_actions with your selection. If no actions fit the moment, call it with an empty array. An empty array is the correct and expected answer for most informational exchanges.`;

  console.log(`[V4] Step 3 prompt: ${systemPrompt.length} chars, ${validIds.length} available CTAs`);

  return { systemPrompt, validIds };
}

/**
 * Build the vocabulary block for Step 3.
 * Groups CTAs by category with descriptions from cta_categories.
 * Falls back to ai_available for legacy tenants without categories.
 *
 * @param {Object} config
 * @param {string[]} completedForms
 * @returns {{ vocabularyBlock: string, validIds: string[] }}
 */
function buildV4VocabularyBlock(config, completedForms = []) {
  const ctaDefinitions = config.cta_definitions || {};
  const ctaCategories = config.cta_categories || {};
  const validIds = [];

  // Group CTAs by category
  const grouped = {};   // { category: [{ ctaId, cta }] }
  const uncategorized = [];

  const actionTypeMap = {
    start_form: 'form',
    external_link: 'link',
    show_info: 'info',
    send_query: 'query',
  };

  for (const [ctaId, cta] of Object.entries(ctaDefinitions)) {
    // Skip completed forms
    if (cta.action === 'start_form' && cta.formId && completedForms.includes(cta.formId)) {
      console.log(`[V4 Step3] Excluding completed form CTA: ${ctaId}`);
      continue;
    }

    // V4: category field determines AI vocabulary inclusion
    if (cta.category) {
      if (!grouped[cta.category]) grouped[cta.category] = [];
      grouped[cta.category].push({ ctaId, cta });
      validIds.push(ctaId);
    }
    // Backward compat: ai_available without category (legacy tenants)
    else if (cta.ai_available === true) {
      uncategorized.push({ ctaId, cta });
      validIds.push(ctaId);
    }
  }

  // Build vocabulary with category descriptions
  const lines = [];
  for (const [category, ctas] of Object.entries(grouped)) {
    const desc = ctaCategories[category] || '';
    lines.push(`[${category}]${desc ? ' — ' + desc : ''}`);
    for (const { ctaId, cta } of ctas) {
      const actionType = actionTypeMap[cta.action] || cta.action;
      lines.push(`  ${ctaId} — ${cta.label || ctaId} (${actionType})`);
    }
  }

  // Legacy uncategorized CTAs (ai_available but no category)
  if (uncategorized.length > 0) {
    lines.push(`[other]`);
    for (const { ctaId, cta } of uncategorized) {
      const actionType = actionTypeMap[cta.action] || cta.action;
      lines.push(`  ${ctaId} — ${cta.label || ctaId} (${actionType})`);
    }
  }

  const vocabularyBlock = lines.length > 0
    ? lines.join('\n')
    : '  (no actions configured for this tenant)';

  console.log(`[V4 Step3] Vocabulary: ${validIds.length} CTAs in ${Object.keys(grouped).length} categories`);
  return { vocabularyBlock, validIds };
}

/**
 * Build the conversation context block for Step 3.
 * Includes the last 2 user messages and the Step 2 response.
 *
 * @param {Array} conversationHistory
 * @param {string} step2Response
 * @returns {string}
 */
function buildV4ConversationBlock(conversationHistory, step2Response) {
  const lines = [];

  // Get the last 2 user messages (not all history — low signal/high token cost)
  const userMessages = (conversationHistory || [])
    .filter(m => m.role === 'user')
    .slice(-2);

  for (const msg of userMessages) {
    const content = (msg.content || msg.text || '').trim();
    if (content) lines.push(`User: ${content}`);
  }

  // The Step 2 response (the response the user just received)
  lines.push(`Assistant: ${step2Response.trim()}`);

  return lines.join('\n');
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: Tool definition for Bedrock tool_use
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Bedrock tool_use tool definition for Step 3.
 * Pass this alongside the system prompt to the Bedrock API call.
 *
 * Usage in handler:
 *   const { systemPrompt, validIds } = buildV4ActionSelectorPrompt(...);
 *   const response = await bedrock.invokeModel({
 *     modelId: HAIKU_MODEL_ID,
 *     body: JSON.stringify({
 *       system: systemPrompt,
 *       messages: [{ role: 'user', content: 'Select actions.' }],
 *       tools: [V4_SELECT_ACTIONS_TOOL],
 *       tool_choice: { type: 'tool', name: 'select_actions' },
 *       max_tokens: 50,
 *       temperature: 0.0,
 *       top_p: 1.0,
 *     })
 *   });
 */
const V4_SELECT_ACTIONS_TOOL = {
  name: 'select_actions',
  description: 'Select which actions from the available list the user is ready to take based on the conversation. Return an empty array if no actions fit.',
  input_schema: {
    type: 'object',
    properties: {
      action_ids: {
        type: 'array',
        items: { type: 'string' },
        description: 'Array of CTA IDs from the AVAILABLE ACTIONS list. Maximum 3. Empty array if no actions fit.',
        maxItems: 3,
      },
    },
    required: ['action_ids'],
  },
};


// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: Response parsing (fallback for non-tool_use path)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Parse Step 3 plain-text response to extract CTA IDs.
 * Implements a fallback chain: direct JSON parse → extract array from text → empty array.
 *
 * @param {string} rawResponse - Raw text from Bedrock (non-tool_use path)
 * @param {string[]} validIds - List of valid CTA IDs for validation
 * @returns {string[]} Array of valid CTA IDs (may be empty)
 */
function parseV4ActionResponse(rawResponse, validIds = []) {
  if (!rawResponse || typeof rawResponse !== 'string') {
    console.warn('[V4 Step3] Empty or invalid response, returning []');
    return [];
  }

  const text = rawResponse.trim();

  // Attempt 1: Direct JSON parse
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return validateAndFilterIds(parsed, validIds);
    }
  } catch (_) {
    // Not valid JSON as-is — try extraction
  }

  // Attempt 2: Extract first JSON array from text
  // Handles cases where the model prefixed or suffixed the array with text
  const arrayMatch = text.match(/\[[\s\S]*?\]/);
  if (arrayMatch) {
    try {
      const parsed = JSON.parse(arrayMatch[0]);
      if (Array.isArray(parsed)) {
        console.warn('[V4 Step3] Extracted JSON array from text response (model added surrounding text)');
        return validateAndFilterIds(parsed, validIds);
      }
    } catch (_) {
      // Extraction failed
    }
  }

  // Attempt 3: Empty array fallback — no buttons is safe
  console.warn('[V4 Step3] Failed to parse action selection response, defaulting to []');
  console.warn(`[V4 Step3] Raw response was: ${text.slice(0, 200)}`);
  return [];
}

/**
 * Validate selected IDs against the vocabulary and cap at 3.
 *
 * @param {string[]} ids - IDs returned by the model
 * @param {string[]} validIds - IDs that exist in the vocabulary
 * @returns {string[]}
 */
function validateAndFilterIds(ids, validIds) {
  if (validIds.length === 0) {
    // No vocabulary to validate against — trust the model's output but cap at 3
    console.warn('[V4 Step3] No validIds provided — skipping ID validation');
    return ids.slice(0, 3);
  }

  const valid = ids.filter(id => {
    if (!validIds.includes(id)) {
      console.warn(`[V4 Step3] Rejecting hallucinated CTA ID: "${id}"`);
      return false;
    }
    return true;
  });

  if (valid.length > 3) {
    console.warn(`[V4 Step3] Capping ${valid.length} selected IDs to 3`);
    return valid.slice(0, 3);
  }

  return valid;
}

/**
 * Parse tool_use response from Bedrock to extract selected action IDs.
 *
 * @param {Object} bedrockResponse - Raw Bedrock API response object
 * @param {string[]} validIds - Valid CTA IDs for validation
 * @returns {string[]}
 */
function parseV4ToolUseResponse(bedrockResponse, validIds = []) {
  try {
    const content = bedrockResponse?.content || [];
    const toolUseBlock = content.find(block => block.type === 'tool_use' && block.name === 'select_actions');

    if (!toolUseBlock) {
      console.warn('[V4 Step3] No tool_use block found in response');
      return [];
    }

    const ids = toolUseBlock.input?.action_ids;
    if (!Array.isArray(ids)) {
      console.warn('[V4 Step3] tool_use input.action_ids is not an array');
      return [];
    }

    return validateAndFilterIds(ids, validIds);
  } catch (err) {
    console.error('[V4 Step3] Error parsing tool_use response:', err.message);
    return [];
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 4: Assembly helper
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Assemble full action objects from selected CTA IDs.
 * Applies branch overrides if a selected CTA routes to a conversation_branch.
 * Mirrors the existing mapNextTagsToActions() and assembleActions() logic from V4 doc.
 *
 * @param {string[]} selectedIds - CTA IDs from Step 3
 * @param {Object} config - Full tenant config
 * @param {Object} sessionContext - Session context
 * @param {Function} mapNextTagsToActions - Existing V3.5 mapper function (injected to avoid duplication)
 * @returns {Object[]} Array of action objects (max 3)
 */
function assembleV4Actions(selectedIds, config, sessionContext, mapNextTagsToActions) {
  if (!selectedIds || selectedIds.length === 0) {
    return [];
  }

  // Map IDs to full action objects using the existing V3.5 mapper
  let actions = mapNextTagsToActions(selectedIds, config, sessionContext);

  // Apply branch overrides: if any selected CTA has a target_branch, and that
  // branch defines its own CTAs, those branch CTAs take priority.
  for (const action of actions) {
    if (action.target_branch && config.conversation_branches?.[action.target_branch]) {
      const branch = config.conversation_branches[action.target_branch];
      if (branch.available_ctas) {
        console.log(`[V4 Step4] Branch override applied: ${action.target_branch}`);
        actions = resolveBranchCTAsV4(branch, config, sessionContext?.completed_forms || []);
        break; // Branch takes over — use its CTA set
      }
    }
  }

  return actions.slice(0, 3);
}

/**
 * Resolve CTA objects from a branch definition.
 *
 * @param {Object} branch - Branch config object
 * @param {Object} config - Full tenant config
 * @param {string[]} completedForms
 * @returns {Object[]}
 */
function resolveBranchCTAsV4(branch, config, completedForms = []) {
  const ctaDefinitions = config.cta_definitions || {};
  const actions = [];

  const allIds = [
    branch.available_ctas?.primary,
    ...(branch.available_ctas?.secondary || []),
  ].filter(Boolean);

  for (const ctaId of allIds) {
    const cta = ctaDefinitions[ctaId];
    if (!cta) continue;
    if (cta.action === 'start_form' && cta.formId && completedForms.includes(cta.formId)) continue;

    const action = { label: cta.label || ctaId, action: cta.action };
    if (cta.action === 'start_form' && cta.formId) action.formId = cta.formId;
    if (cta.action === 'external_link' && cta.url) action.url = cta.url;
    if (cta.action === 'show_info') {
      action.prompt = cta.prompt || '';
      if (cta.target_branch) action.target_branch = cta.target_branch;
    }
    if (cta.action === 'send_query' && cta.query) action.query = cta.query;
    actions.push(action);
  }

  return actions;
}


// ─────────────────────────────────────────────────────────────────────────────
// BEDROCK CALL PARAMETERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Recommended Bedrock inference parameters for Step 2 (streaming).
 * These are defaults — the handler may override per tenant config.
 */
const V4_STEP2_INFERENCE_PARAMS = {
  temperature: 0.35,    // Slightly warm for conversational variety; do not exceed 0.4
  top_p: 0.95,
  top_k: 250,
  max_tokens: 600,      // Cap prevents runaway; typical responses are 300-400 tokens
};

/**
 * Recommended Bedrock inference parameters for Step 3 (non-streaming, classification).
 * Temperature 0.0 is greedy decoding — deterministic, testable, no randomness needed.
 */
const V4_STEP3_INFERENCE_PARAMS = {
  temperature: 0.0,     // Greedy decoding for classification
  top_p: 1.0,
  top_k: 1,
  max_tokens: 50,       // Hard cap — valid output is at most a few dozen chars
};


// ─────────────────────────────────────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────────────────────────────────────

module.exports = {
  // Step 2
  buildV4ConversationPrompt,

  // Step 3
  buildV4ActionSelectorPrompt,
  V4_SELECT_ACTIONS_TOOL,
  parseV4ActionResponse,
  parseV4ToolUseResponse,

  // Step 4
  assembleV4Actions,

  // Parameters
  V4_STEP2_INFERENCE_PARAMS,
  V4_STEP3_INFERENCE_PARAMS,

  // Utilities (exported for testing)
  sanitizeTonePromptV4,
  buildV4VocabularyBlock,
  buildV4ConversationBlock,
  validateAndFilterIds,
};
