/**
 * V4 Prompt Builders — Three-Layer Architecture
 *
 * Step 2: buildV4ConversationPrompt() — streaming conversational response
 * Step 3a: buildClassificationPrompt() + classifyIntent() — intent classification
 * Step 3b: routeFromClassification() — deterministic routing (no AI)
 *
 * The three layers are independent:
 *   - Step 2 (response generation) does not know the intent taxonomy exists
 *   - Step 3a (classification) does not know which CTAs exist
 *   - Step 3b (routing) has no AI — evaluates rules only
 *
 * Design principles:
 *   - Each function has ONE job. No vocabulary in Step 2. No persona in Step 3a.
 *   - Locked rules (anti-hallucination, loop prevention, etc.) are embedded and
 *     NOT tenant-customizable. They survived the V3.5 → V4 review because each
 *     one addresses a documented, observed failure mode.
 *   - Custom constraints from the tenant config are appended AFTER locked rules
 *     but BEFORE the final instruction, so they can add constraints without
 *     overriding safety rules.
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
// STEP 3a: Classification (LLM — non-streaming)
// PRD Amendment lines 59-143
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build the Step 3a classification prompt.
 *
 * A separate, non-streaming prompt that evaluates the user's message against
 * a described taxonomy of intents. Returns a single label or null.
 *
 * INCLUDED (amendment lines 67-70):
 *   - The user's current message (verbatim)
 *   - Recent conversation context: the last 2 complete user-turn messages
 *   - The full described taxonomy: each intent's name and description
 *
 * EXCLUDED (amendment lines 72-79 — these exclusions are not optional):
 *   - The AI's generated response from Step 2
 *   - CTA definitions, action menus, button configurations, cta_definitions records
 *   - The system prompt, persona block, or tone_prompt
 *   - KB retrieval passages
 *   - Branch structure or conversation_branches config
 *
 * @param {string} userMessage - The current user message (verbatim)
 * @param {Array} conversationHistory - [{role, content}] — all prior messages
 * @param {Object} config - Must contain config.intent_definitions (validated)
 * @returns {string} Complete prompt string for the classification Bedrock call
 */
function buildClassificationPrompt(userMessage, conversationHistory, config) {
  console.log('[V4 Step3a] Building classification prompt');

  // Extract the last 2 user-role messages from conversation history (amendment line 69)
  // Only user turns — not assistant turns, not KB passages
  const priorUserMessages = (conversationHistory || [])
    .filter(m => m.role === 'user')
    .map(m => (m.content || m.text || '').trim())
    .filter(Boolean)
    .slice(-2);

  // Build customer messages block: prior user messages + current message (most recent last)
  const allUserMessages = [...priorUserMessages, userMessage];
  const customerMessagesBlock = allUserMessages
    .map(msg => `- ${msg}`)
    .join('\n');

  // Build intent taxonomy block (amendment lines 102-106)
  // Each entry rendered as: {intent.name}: {intent.description}
  const intentDefinitions = config.intent_definitions || [];
  const intentBlock = intentDefinitions
    .map(intent => `${intent.name}: ${intent.description}`)
    .join('\n');

  // Assemble exact prompt structure from amendment lines 83-100
  const prompt = `You are a conversation classifier. Read the customer messages below and identify
which intent best matches, using only the taxonomy provided.

CUSTOMER MESSAGES (most recent last):
${customerMessagesBlock}

INTENT TAXONOMY:
${intentBlock}

Return ONLY the intent name that matches, or null if no intent matches.
Do not explain. Do not select multiple intents. Do not invent new intents.

Examples of valid output:
null
"mentoring_recipient"
"volunteer_lovebox"`;

  console.log(`[V4 Step3a] Classification prompt: ${prompt.length} chars, ${intentDefinitions.length} intents, ${allUserMessages.length} user messages`);
  return prompt;
}

/**
 * Classify user intent via a non-streaming Bedrock call.
 *
 * Makes a separate, non-streaming Bedrock InvokeModel call (amendment lines 61-63).
 * Returns a single label from the closed intent list, or null.
 *
 * AC3b: Output is either a string matching an intent_definitions[].name value,
 * or null. Any output that is not a recognized intent name is treated as null.
 * Malformed output is caught, logged, and returned as null.
 * On any error → log error, return null, never throw.
 *
 * @param {string} userMessage - The current user message
 * @param {Array} conversationHistory - [{role, content}]
 * @param {Object} config - Must contain config.intent_definitions (validated)
 * @param {Object} bedrockClient - Bedrock runtime client (for InvokeModelCommand)
 * @returns {Promise<string|null>} Matched intent name, or null
 */
async function classifyIntent(userMessage, conversationHistory, config, bedrockClient) {
  const startTime = Date.now();

  try {
    const prompt = buildClassificationPrompt(userMessage, conversationHistory, config);
    const intentDefinitions = config.intent_definitions || [];
    const knownNames = intentDefinitions.map(d => d.name);

    // Non-streaming Bedrock call — classification, not generation
    // Temperature: 0.1 (amendment line 108) — deterministic by design
    const { InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
    const modelId = config.model_id || config.aws?.model_id || 'global.anthropic.claude-haiku-4-5-20251001-v1:0';

    const command = new InvokeModelCommand({
      modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: V4_STEP3_INFERENCE_PARAMS.max_tokens,
        temperature: V4_STEP3_INFERENCE_PARAMS.temperature,
      })
    });

    const response = await bedrockClient.send(command);
    const responseBody = JSON.parse(new TextDecoder().decode(response.body));

    // Extract text from response
    const rawOutput = (responseBody?.content?.[0]?.text || '').trim();

    // Parse: trim whitespace, strip surrounding quotes
    let parsed = rawOutput;
    if (parsed.startsWith('"') && parsed.endsWith('"')) {
      parsed = parsed.slice(1, -1);
    }
    if (parsed.startsWith("'") && parsed.endsWith("'")) {
      parsed = parsed.slice(1, -1);
    }
    parsed = parsed.trim();

    const duration = Date.now() - startTime;

    // Check if the value matches any known intent name (AC3b)
    if (parsed === 'null' || parsed === '') {
      console.log(`[V4 Step3a] Classification result: null (raw: "${rawOutput}") in ${duration}ms`);
      return null;
    }

    if (knownNames.includes(parsed)) {
      console.log(`[V4 Step3a] Classification result: "${parsed}" (raw: "${rawOutput}") in ${duration}ms`);
      return parsed;
    }

    // Unknown name, explanation prose, non-string, object — treat as null (AC3b)
    console.warn(`[V4 Step3a] Unknown classification output: "${rawOutput}" — treating as null (${duration}ms)`);
    return null;

  } catch (err) {
    // On any error → log error, return null, never throw (AC3b)
    const duration = Date.now() - startTime;
    console.error(`[V4 Step3a] Classification error (${duration}ms):`, err.message);
    return null;
  }
}

/**
 * Validate intent_definitions in tenant config.
 *
 * Config validation rules (amendment lines 203-206):
 *   Rule 1: Every entry must have non-empty name and description. Reject without.
 *   Rule 2: If V4_PIPELINE is false, intent_definitions is ignored silently.
 *   Rule 3: If target_branch references missing branch, log warning but don't error.
 *
 * @param {Object} config - Full tenant config
 * @returns {{ valid: boolean, definitions: Array, warnings: string[] }}
 */
function validateIntentDefinitions(config) {
  const warnings = [];

  // Rule 2 (amendment line 205): If intent_definitions present but V4_PIPELINE false,
  // return early — ignored silently. No error.
  if (config.intent_definitions && !config.feature_flags?.V4_PIPELINE) {
    console.log('[V4] intent_definitions present but V4_PIPELINE flag is false — ignoring');
    return { valid: true, definitions: [], warnings: [] };
  }

  // If not present, not an array, or empty — valid but no definitions
  if (!config.intent_definitions || !Array.isArray(config.intent_definitions) || config.intent_definitions.length === 0) {
    return { valid: true, definitions: [], warnings: [] };
  }

  const validEntries = [];

  for (let i = 0; i < config.intent_definitions.length; i++) {
    const entry = config.intent_definitions[i];

    // Rule 1 (amendment line 204): Every entry must have non-empty name (string) and
    // non-empty description (string). Reject entries without — log a warning per rejected entry.
    if (!entry.name || typeof entry.name !== 'string' || entry.name.trim() === '') {
      const msg = `[V4] intent_definitions[${i}]: rejected — missing or empty name`;
      console.warn(msg);
      warnings.push(msg);
      continue;
    }

    if (!entry.description || typeof entry.description !== 'string' || entry.description.trim() === '') {
      const msg = `[V4] intent_definitions[${i}] ("${entry.name}"): rejected — missing or empty description`;
      console.warn(msg);
      warnings.push(msg);
      continue;
    }

    // Rule 3 (amendment line 206): If target_branch references a branch not present
    // in conversation_branches, log a warning. Do NOT error — entry stays valid,
    // falls through to fallback on match at runtime.
    if (entry.target_branch && !config.conversation_branches?.[entry.target_branch]) {
      const msg = `[V4] intent_definitions[${i}] ("${entry.name}"): target_branch "${entry.target_branch}" not found in conversation_branches — will fall through to fallback on match`;
      console.warn(msg);
      warnings.push(msg);
    }

    validEntries.push(entry);
  }

  console.log(`[V4] Validated intent_definitions: ${validEntries.length}/${config.intent_definitions.length} entries valid`);

  return {
    valid: validEntries.length > 0,
    definitions: validEntries,
    warnings
  };
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 3b: Routing (Deterministic — no AI)
// PRD Amendment lines 147-206
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Route from classification result to CTAs. Deterministic code, no AI (AC3c).
 *
 * Implements the exact function from amendment lines 155-176.
 * Given the same label and config, returns the same CTA set on every invocation.
 *
 * Step 3b routing applies only when intent_definitions is present (amendment line 179).
 * Tenants without intent_definitions continue using existing enhanceResponse() path.
 *
 * @param {string|null} label - Classified intent name from Step 3a, or null
 * @param {Object} config - Full tenant config
 * @param {string[]} completedForms - Completed form IDs for filtering
 * @returns {{ ctaButtons: Array, metadata: Object }}
 */
function routeFromClassification(label, config, completedForms = []) {
  if (!label) {
    return resolveFallbackBranch(config, completedForms);
  }

  const intent = config.intent_definitions?.find(d => d.name === label);
  if (!intent) {
    return resolveFallbackBranch(config, completedForms);
  }

  if (intent.target_branch && config.conversation_branches?.[intent.target_branch]) {
    return resolveBranchCTAs(intent.target_branch, config, completedForms, label);
  }

  // Intent matched but no branch configured — return the single CTA if specified
  if (intent.cta_id) {
    return resolveSingleCTA(intent.cta_id, config, label);
  }

  // Intent matched but no routing configured — AI response stands alone
  console.log(`[V4 Step3b] Intent "${label}" matched with no routing — response only`);
  return {
    ctaButtons: [],
    metadata: {
      routing_tier: 'v4_classification',
      classified_intent: label,
      target_branch: null,
      routing_method: 'intent_only'
    }
  };
}

/**
 * Resolve fallback branch CTAs.
 * Uses config.cta_settings.fallback_branch → buildCtasFromBranch().
 * If no fallback configured, returns empty array.
 */
function resolveFallbackBranch(config, completedForms = []) {
  const fallbackBranch = config.cta_settings?.fallback_branch;
  if (fallbackBranch && config.conversation_branches?.[fallbackBranch]) {
    console.log(`[V4 Step3b] Routing to fallback branch: ${fallbackBranch}`);
    const { buildCtasFromBranch } = require('./response_enhancer');
    const ctaButtons = buildCtasFromBranch(fallbackBranch, config, completedForms);
    return {
      ctaButtons,
      metadata: {
        routing_tier: 'v4_classification',
        classified_intent: null,
        target_branch: fallbackBranch,
        routing_method: 'fallback'
      }
    };
  }

  console.log('[V4 Step3b] No fallback branch configured — returning empty');
  return {
    ctaButtons: [],
    metadata: {
      routing_tier: 'v4_classification',
      classified_intent: null,
      target_branch: null,
      routing_method: 'no_fallback'
    }
  };
}

/**
 * Resolve CTAs for a classified branch.
 * Calls buildCtasFromBranch() from response_enhancer.js (battle-tested in production).
 */
function resolveBranchCTAs(branchName, config, completedForms = [], label = null) {
  console.log(`[V4 Step3b] Routing intent "${label}" → branch "${branchName}"`);
  const { buildCtasFromBranch } = require('./response_enhancer');
  const ctaButtons = buildCtasFromBranch(branchName, config, completedForms);
  return {
    ctaButtons,
    metadata: {
      routing_tier: 'v4_classification',
      classified_intent: label,
      target_branch: branchName,
      routing_method: 'intent_to_branch'
    }
  };
}

/**
 * Resolve a single CTA by ID.
 * Looks up config.cta_definitions[ctaId], returns as single-element array.
 */
function resolveSingleCTA(ctaId, config, label = null) {
  const ctaDef = config.cta_definitions?.[ctaId];
  if (!ctaDef) {
    console.warn(`[V4 Step3b] CTA "${ctaId}" not found in cta_definitions — falling back`);
    return resolveFallbackBranch(config);
  }

  console.log(`[V4 Step3b] Routing intent "${label}" → single CTA "${ctaId}"`);
  const { style, ...cleanCta } = ctaDef;
  return {
    ctaButtons: [{ ...cleanCta, id: ctaId }],
    metadata: {
      routing_tier: 'v4_classification',
      classified_intent: label,
      target_branch: null,
      cta_id: ctaId,
      routing_method: 'intent_to_cta'
    }
  };
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
 * Recommended Bedrock inference parameters for Step 3a (non-streaming, classification).
 * Temperature 0.1 per PRD amendment line 108 — classification, not generation.
 */
const V4_STEP3_INFERENCE_PARAMS = {
  temperature: 0.1,     // Near-deterministic for classification (amendment line 108)
  top_p: 1.0,
  top_k: 1,
  max_tokens: 50,       // Only needs room for a single intent name or null
};


// ─────────────────────────────────────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────────────────────────────────────

module.exports = {
  // Step 2: Conversational Response (streaming)
  buildV4ConversationPrompt,

  // Step 3a: Classification (non-streaming LLM call)
  buildClassificationPrompt,
  classifyIntent,

  // Step 3b: Routing (deterministic, no AI)
  routeFromClassification,

  // Config validation
  validateIntentDefinitions,

  // Parameters
  V4_STEP2_INFERENCE_PARAMS,
  V4_STEP3_INFERENCE_PARAMS,

  // Utilities (exported for testing)
  sanitizeTonePromptV4,
};
