/**
 * V4.1 Prompt Builders — Three-Layer Architecture
 *
 * Step 2: buildV4ConversationPrompt() — streaming conversational response
 * Step 3a: buildTopicClassificationPrompt() + classifyTopic() — topic classification
 * Step 3b: selectCTAsFromPool() — dynamic CTA pool selection (no AI)
 *
 * The three layers are independent:
 *   - Step 2 (response generation) does not know the topic taxonomy exists
 *   - Step 3a (classification) does not know which CTAs exist
 *   - Step 3b (pool selection) has no AI — filters CTA inventory by metadata
 *
 * V4.1 changes (Dynamic CTA Pool Selection):
 *   - topic_definitions replace intent_definitions (no target_branch, no cta_id)
 *   - selectCTAsFromPool() replaces routeFromClassification() and all branch lookup
 *   - CTA selection_metadata (topic_tags, depth_level, role_axis) drives selection
 *   - Session context (accumulated_topics, recently_shown_ctas) drives depth gate
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
  const lockedRules = buildV4LockedRules(kbContext !== null, turnCount, config);

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
  const detailLevel = config?.bedrock_instructions?.formatting_preferences?.detail_level || 'balanced';
  sections.push(buildV4FinalInstruction(kbContext !== null, detailLevel));

  const prompt = sections.join('\n');

  console.log(`[V4] Step 2 prompt: ${prompt.length} chars, turn ${turnCount + 1}`);
  return prompt;
}

/**
 * Build the locked rules block for Step 2.
 *
 * @param {boolean} hasKb - Whether KB context is present
 * @param {number} turnCount - Number of prior user turns in this session
 * @param {Object} config - Full tenant config (for formatting_preferences)
 * @returns {string}
 */
function buildV4LockedRules(hasKb, turnCount, config) {
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

  // ── Formatting (driven by tenant config formatting_preferences) ────────
  rules.push(buildV4FormattingRules(config));

  // ── Closing rule ───────────────────────────────────────────────────────
  rules.push(`CLOSING
- End every response with a specific, contextual follow-up question that invites the next natural step. The question must relate directly to the topic you just discussed.
- Good examples: "Would you like to know the eligibility requirements?" / "The application takes about 10 minutes — want me to explain what it asks for?"
- Do not use generic closers: "Is there anything else I can help you with?" / "Do you have any other questions?" / "Would you like more information?" — these are not specific.
- Exception: if the user has reached a clear end point (e.g., you just provided the direct application link and they said "thanks"), a brief warm close is appropriate.`);

  return rules.join('\n\n');
}

/**
 * Build formatting rules from tenant config formatting_preferences.
 *
 * Reads: config.bedrock_instructions.formatting_preferences
 *   - response_style: 'professional_concise' | 'warm_conversational' | 'structured_detailed'
 *   - detail_level: 'concise' | 'balanced' | 'comprehensive'
 *   - emoji_usage: 'none' | 'moderate' | 'generous'
 *   - max_emojis_per_response: number
 *
 * @param {Object} config
 * @returns {string}
 */
function buildV4FormattingRules(config) {
  const prefs = config?.bedrock_instructions?.formatting_preferences || {};
  const style = prefs.response_style || 'warm_conversational';
  const detail = prefs.detail_level || 'balanced';
  const emoji = prefs.emoji_usage || 'moderate';
  const maxEmoji = prefs.max_emojis_per_response ?? 3;
  console.log(`[V4 Formatting] Using: style=${style}, detail=${detail}, emoji=${emoji}, maxEmoji=${maxEmoji}`);

  const lines = ['FORMATTING'];

  // ── Context: This is a chat widget ──────────────────────────────────
  lines.push('- IMPORTANT: This is a small chat widget, not a document. Responses must be easy to scan on a phone screen.');
  lines.push('- Do not use markdown headers (lines starting with #).');
  lines.push('- When the KB content contains relevant links (markdown URLs), include them inline in your response so the user can click through. Present them naturally — e.g. "You can [Donate Now](https://...) or [Invest Monthly](https://...)" — do not strip links from your answer.');
  lines.push('- Preserve all URLs and email addresses from the KB exactly as written.');
  lines.push('- Never cram all KB facts into one response. Pick the most relevant facts for THIS question and leave the rest for follow-ups.');

  // ── Response style (controls FORMAT — how it's written) ─────────────
  switch (style) {
    case 'professional_concise':
      lines.push('- Use a professional, business-appropriate tone. No greetings like "Hey!" or "I\'d love to help!" — get straight to the answer.');
      lines.push('- Never open with enthusiasm or small talk. State the facts directly.');
      lines.push('- Use short bullet points when presenting 2+ items. Keep each bullet to one line.');
      break;
    case 'structured_detailed':
      lines.push('- Organize information with bold labels and short bullet points. No long flowing paragraphs.');
      lines.push('- Lead with a one-sentence summary, then break details into structured points.');
      lines.push('- Keep each bullet point to one line. Use bold text for category labels.');
      break;
    case 'warm_conversational':
    default:
      lines.push('- Write in a warm, conversational tone — like a friendly colleague.');
      lines.push('- Use short sentences and line breaks between ideas. Avoid dense walls of text.');
      lines.push('- It\'s okay to open with a brief personal touch ("Great question!" or "I\'d love to tell you about that!").');
      break;
  }

  // ── Detail level (controls LENGTH — hard word limits) ───────────────
  switch (detail) {
    case 'concise':
      lines.push('- HARD LIMIT: 2-3 sentences maximum, under 60 words total (excluding the closing question). This is non-negotiable.');
      lines.push('- Give a one-line overview. Do NOT enumerate sub-programs, tracks, age ranges, or requirements. Let the follow-up question draw out more.');
      break;
    case 'comprehensive':
      lines.push('- Provide a thorough response: 3-4 short paragraphs or an intro sentence plus organized bullet points.');
      lines.push('- Cover key distinctions (e.g. different tracks, eligibility, program details). Use line breaks between sections for readability.');
      lines.push('- LIMIT: Stay under 200 words (excluding the closing question). Thorough does not mean exhaustive.');
      break;
    case 'balanced':
    default:
      lines.push('- Keep responses to 2 short paragraphs or 1 paragraph plus a few bullet points.');
      lines.push('- Cover the main facts with enough detail to be useful. Don\'t exhaustively list everything.');
      lines.push('- LIMIT: Stay under 120 words (excluding the closing question).');
      break;
  }

  // ── Emoji usage ─────────────────────────────────────────────────────
  switch (emoji) {
    case 'none':
      lines.push('- Do not use any emojis in responses.');
      break;
    case 'generous':
      lines.push(`- Use emojis freely to add warmth (up to ${maxEmoji} per response).`);
      break;
    case 'moderate':
    default:
      lines.push(`- Use emojis sparingly (up to ${maxEmoji} per response).`);
      break;
  }

  return lines.join('\n');
}

/**
 * Build the final instruction line at the end of the Step 2 prompt.
 * Positioned last for recency bias — the model pays most attention to this.
 * Includes a hard word-limit reminder based on detail_level.
 */
function buildV4FinalInstruction(hasKb, detailLevel = 'balanced') {
  let instruction;
  if (hasKb) {
    instruction = `Respond conversationally using only the KB facts above. End with a specific follow-up question that moves the conversation forward.`;
  } else {
    instruction = `Use the fallback message above. Offer to help with topics you do have information about.`;
  }

  // Add hard word-limit reminder as the absolute last thing the model reads
  switch (detailLevel) {
    case 'concise':
      instruction += `\n\nREMINDER: Your answer (before the closing question) must be under 50 words. Two to three short sentences. Stop writing and ask your follow-up question.`;
      break;
    case 'comprehensive':
      instruction += `\n\nREMINDER: Keep your answer under 200 words. Use line breaks for readability.`;
      break;
    case 'balanced':
    default:
      instruction += `\n\nREMINDER: Keep your answer under 120 words. Use line breaks for readability.`;
      break;
  }

  return instruction;
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
// STEP 3a: Topic Classification (LLM — non-streaming)
// V4.1: topic_definitions replace intent_definitions
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build the Step 3a topic classification prompt.
 *
 * Evaluates the user's message against a described taxonomy of topics.
 * Returns a single topic name or null.
 *
 * INCLUDED:
 *   - The user's current message (verbatim)
 *   - Recent conversation context: the last 2 user-turn messages
 *   - The full described taxonomy: each topic's name and description
 *
 * EXCLUDED (these exclusions are not optional):
 *   - The AI's generated response from Step 2
 *   - CTA definitions, action menus, button configurations
 *   - The system prompt, persona block, or tone_prompt
 *   - KB retrieval passages
 *   - Branch structure or conversation_branches config
 *
 * @param {string} userMessage - The current user message (verbatim)
 * @param {Array} conversationHistory - [{role, content}] — all prior messages
 * @param {Object} config - Must contain config.topic_definitions (validated)
 * @returns {string} Complete prompt string for the classification Bedrock call
 */
function buildTopicClassificationPrompt(userMessage, conversationHistory, config) {
  console.log('[V4.1 Step3a] Building topic classification prompt');

  // Build topic taxonomy block: {topic.name}: {topic.description}
  const topicDefinitions = config.topic_definitions || [];
  const taxonomyBlock = topicDefinitions
    .map(topic => `${topic.name}: ${topic.description}`)
    .join('\n');

  // Current message only — prior messages bias the classifier more than they help.
  // Anaphora resolution (e.g. "tell me more about that") is rare enough that
  // eliminating history bias is the better tradeoff for Phase 1.
  const prompt = `You are a conversation classifier. Classify the customer message below
using only the taxonomy provided.

CUSTOMER MESSAGE:
${userMessage}

TOPIC TAXONOMY:
${taxonomyBlock}

Return ONLY the topic name that matches, or null if no topic matches.
Do not explain. Do not select multiple topics. Do not invent new topics.`;

  console.log(`[V4.1 Step3a] Classification prompt: ${prompt.length} chars, ${topicDefinitions.length} topics`);
  return prompt;
}

/**
 * Classify user topic via a non-streaming Bedrock call.
 *
 * Same mechanics as the former classifyIntent() — non-streaming InvokeModel,
 * temp 0.1, max_tokens 50, parse to known name or null.
 *
 * @param {string} userMessage - The current user message
 * @param {Array} conversationHistory - [{role, content}]
 * @param {Object} config - Must contain config.topic_definitions (validated)
 * @param {Object} bedrockClient - Bedrock runtime client (for InvokeModelCommand)
 * @returns {Promise<string|null>} Matched topic name, or null
 */
async function classifyTopic(userMessage, conversationHistory, config, bedrockClient) {
  const startTime = Date.now();

  try {
    const prompt = buildTopicClassificationPrompt(userMessage, conversationHistory, config);
    const topicDefinitions = config.topic_definitions || [];
    const knownNames = topicDefinitions.map(d => d.name);

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

    if (parsed === 'null' || parsed === '') {
      console.log(`[V4.1 Step3a] Classification result: null (raw: "${rawOutput}") in ${duration}ms`);
      return null;
    }

    if (knownNames.includes(parsed)) {
      console.log(`[V4.1 Step3a] Classification result: "${parsed}" (raw: "${rawOutput}") in ${duration}ms`);
      return parsed;
    }

    console.warn(`[V4.1 Step3a] Unknown classification output: "${rawOutput}" — treating as null (${duration}ms)`);
    return null;

  } catch (err) {
    const duration = Date.now() - startTime;
    console.error(`[V4.1 Step3a] Classification error (${duration}ms):`, err.message);
    return null;
  }
}

/**
 * Validate topic_definitions in tenant config.
 *
 * Rules:
 *   - Every entry must have non-empty name and description
 *   - tags must be an array of strings if present
 *   - role must be a known value if present
 *   - Log warnings for invalid entries, filter them out
 *
 * @param {Object} config - Full tenant config
 * @returns {{ valid: boolean, definitions: Array, warnings: string[] }}
 */
function validateTopicDefinitions(config) {
  const warnings = [];
  const KNOWN_ROLES = ['give', 'receive', 'learn', 'connect'];

  if (!config.topic_definitions || !Array.isArray(config.topic_definitions) || config.topic_definitions.length === 0) {
    return { valid: true, definitions: [], warnings: [] };
  }

  const validEntries = [];

  for (let i = 0; i < config.topic_definitions.length; i++) {
    const entry = config.topic_definitions[i];

    if (!entry.name || typeof entry.name !== 'string' || entry.name.trim() === '') {
      const msg = `[V4.1] topic_definitions[${i}]: rejected — missing or empty name`;
      console.warn(msg);
      warnings.push(msg);
      continue;
    }

    if (!entry.description || typeof entry.description !== 'string' || entry.description.trim() === '') {
      const msg = `[V4.1] topic_definitions[${i}] ("${entry.name}"): rejected — missing or empty description`;
      console.warn(msg);
      warnings.push(msg);
      continue;
    }

    if (entry.tags !== undefined && !Array.isArray(entry.tags)) {
      const msg = `[V4.1] topic_definitions[${i}] ("${entry.name}"): tags must be an array — ignoring tags`;
      console.warn(msg);
      warnings.push(msg);
      entry.tags = undefined;
    }

    if (entry.role !== undefined && !KNOWN_ROLES.includes(entry.role)) {
      const msg = `[V4.1] topic_definitions[${i}] ("${entry.name}"): unknown role "${entry.role}" — ignoring role`;
      console.warn(msg);
      warnings.push(msg);
      entry.role = undefined;
    }

    validEntries.push(entry);
  }

  console.log(`[V4.1] Validated topic_definitions: ${validEntries.length}/${config.topic_definitions.length} entries valid`);

  return {
    valid: validEntries.length > 0,
    definitions: validEntries,
    warnings
  };
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 3b: Dynamic CTA Pool Selection (Deterministic — no AI)
// V4.1: Replaces branch-based routing with metadata-driven pool filtering
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Select CTAs from the pool based on topic classification and session context.
 * Deterministic — same inputs always produce the same output. No AI.
 *
 * Replaces routeFromClassification() and all branch/CTA lookup functions.
 *
 * Algorithm:
 *   1. Resolve topic → tags + role
 *   2. Filter pool by tag intersection + role
 *   3. Determine depth preference (info vs action)
 *   4. Apply depth gate
 *   5. Sort by priority → tag overlap → insertion order
 *   6. Dedup (recently_shown_ctas, completed_forms)
 *   7. Zero-result handling (retry with fallback_tags)
 *   8. Select top 4, assign positions
 *
 * @param {string|null} topicName - Classified topic name from Step 3a, or null
 * @param {Object} config - Full tenant config (cta_definitions, topic_definitions, cta_settings)
 * @param {Object} sessionContext - { accumulated_topics, recently_shown_ctas, turns_since_click,
 *                                    completed_forms, detected_role, ctas_clicked }
 * @returns {{ ctaButtons: Array, metadata: Object }}
 */
function selectCTAsFromPool(topicName, config, sessionContext = {}) {
  const startTime = Date.now();
  const ctx = sessionContext || {};

  // ── 1. RESOLVE TOPIC ─────────────────────────────────────────────────────
  let resolvedTags = [];
  let resolvedRole = null;
  let topicDef = null;

  if (topicName) {
    topicDef = (config.topic_definitions || []).find(d => d.name === topicName);
    if (topicDef) {
      resolvedTags = topicDef.tags || [];
      resolvedRole = topicDef.role || null;
    }
  }

  // If no topic or topic has no tags, use fallback_tags.
  // Fallback_tags should be broad enough to surface good learning CTAs
  // (operator ensures key learning CTAs carry the "programs" tag).
  if (resolvedTags.length === 0) {
    resolvedTags = config.cta_settings?.fallback_tags || [];
    console.log(`[V4.1 Step3b] No topic tags — using fallback_tags: [${resolvedTags.join(', ')}]`);
  }

  console.log(`[V4.1 Step3b] Resolved: topic="${topicName || 'null'}", tags=[${resolvedTags.join(', ')}], role=${resolvedRole || 'none'}`);

  // ── 2. FILTER POOL ───────────────────────────────────────────────────────
  const ctaDefs = config.cta_definitions || {};
  let pool = [];

  for (const [ctaId, ctaDef] of Object.entries(ctaDefs)) {
    // Only CTAs available for AI selection
    if (!ctaDef.ai_available) continue;

    const ctaTags = ctaDef.selection_metadata?.topic_tags || [];
    const ctaRole = ctaDef.selection_metadata?.role_axis || null;

    // Tag intersection: CTA must share at least one tag with resolved tags
    const tagOverlap = ctaTags.filter(t => resolvedTags.includes(t));
    if (tagOverlap.length === 0) continue;

    // Role filter: if topic has a role, apply role constraint
    if (resolvedRole) {
      // Pass if: CTA role matches topic role, OR CTA role is "learn" (universal),
      // OR CTA has no role_axis (role-agnostic)
      if (ctaRole && ctaRole !== resolvedRole && ctaRole !== 'learn') continue;
    }

    // Clean CTA for output (strip style property, add id and overlap count)
    const { style, ...cleanCta } = ctaDef;
    pool.push({
      ...cleanCta,
      id: ctaId,
      _tagOverlap: tagOverlap.length,
      _priority: ctaDef.selection_metadata?.priority ?? 50,
    });
  }

  console.log(`[V4.1 Step3b] Pool after tag+role filter: ${pool.length} CTAs`);

  // ── 3. DETERMINE DEPTH (sort preference, not a filter) ──────────────────
  const depth = determineDepthPreference(resolvedTags, ctx, topicDef);

  // ── 4. FILTER ──────────────────────────────────────────────────────────
  const completedForms = ctx.completed_forms || [];

  pool = pool.filter(cta => {
    // Don't show form CTAs for forms the user already completed
    if (cta.action_type === 'start_form' && completedForms.includes(cta.form_id)) return false;
    // Filter core learning CTAs that are redundant to the question just answered.
    // "Learn about Dare to Dream" is redundant when the AI just explained D2D.
    // Sub-topic CTAs ("What's in a Love Box?", "Download D2D manual") pass through.
    if (topicName && cta.action === 'send_query' && cta.selection_metadata?.core_learning) {
      const primaryCtaTag = (cta.selection_metadata?.topic_tags || [])[0];
      const primaryTopicTag = resolvedTags[0];
      if (primaryCtaTag && primaryTopicTag && primaryCtaTag === primaryTopicTag) {
        console.log(`[V4.1 Step3b] Filtering redundant core CTA "${cta.id}" — AI just answered "${primaryTopicTag}"`);
        return false;
      }
    }
    return true;
  });

  console.log(`[V4.1 Step3b] Pool after filter: ${pool.length} CTAs`);

  // ── 5. SORT ──────────────────────────────────────────────────────────────
  // Sort by priority → tag overlap → insertion order (deterministic)
  pool.sort((a, b) => {
    if (a._priority !== b._priority) return a._priority - b._priority;
    if (a._tagOverlap !== b._tagOverlap) return b._tagOverlap - a._tagOverlap;
    return 0;
  });

  // ── 6. DIVERSE SELECT ──────────────────────────────────────────────────
  // Pick the best CTA from each depth tier to ensure the user always sees
  // a mix of options: learn more (info), take action (action), explore (lateral).
  // Depth preference controls which tier gets the primary slot.
  const tiers = { info: [], action: [], lateral: [] };
  for (const cta of pool) {
    const tier = cta.selection_metadata?.depth_level || 'info';
    if (tiers[tier]) tiers[tier].push(cta);
    else tiers.info.push(cta); // unknown depth_level → treat as info
  }

  const selected = [];
  // Order tiers by depth preference: preferred tier gets primary slot
  const tierOrder = depth === 'action'
    ? ['action', 'info', 'lateral']
    : ['info', 'action', 'lateral'];

  // Round 1: take top 1 from each non-empty tier
  for (const tier of tierOrder) {
    if (tiers[tier].length > 0 && selected.length < 4) {
      selected.push(tiers[tier].shift());
    }
  }
  // Round 2: fill remaining slots from the preferred tier, then others
  for (const tier of tierOrder) {
    while (tiers[tier].length > 0 && selected.length < 4) {
      selected.push(tiers[tier].shift());
    }
  }

  // ── 7. ZERO-RESULT HANDLING ──────────────────────────────────────────────
  if (selected.length === 0 && topicName) {
    const fallbackTags = config.cta_settings?.fallback_tags || [];
    if (fallbackTags.length > 0) {
      console.log(`[V4.1 Step3b] Zero results — retrying with fallback_tags: [${fallbackTags.join(', ')}]`);
      const fallbackResult = selectCTAsFromPool(null, config, sessionContext);
      fallbackResult.metadata.original_topic = topicName;
      fallbackResult.metadata.routing_method = 'fallback_retry';
      return fallbackResult;
    }
  }

  // Action CTAs always take the primary (first) position.
  // Learning CTAs appear alongside as secondary options for continued exploration.
  selected.sort((a, b) => {
    const aIsAction = (a.selection_metadata?.depth_level === 'action') ? 0 : 1;
    const bIsAction = (b.selection_metadata?.depth_level === 'action') ? 0 : 1;
    return aIsAction - bIsAction;
  });

  // Assign positions and clean internal fields
  const ctaButtons = selected.map((cta, idx) => {
    const { _tagOverlap, _priority, ...cleanCta } = cta;
    return {
      ...cleanCta,
      _position: idx === 0 ? 'primary' : 'secondary',
    };
  });

  const selectedIds = ctaButtons.map(c => c.id);
  const duration = Date.now() - startTime;

  console.log(`[V4.1 Step3b] Selected ${ctaButtons.length} CTAs: [${selectedIds.join(', ')}] (depth=${depth}) in ${duration}ms`);

  return {
    ctaButtons,
    metadata: {
      routing_tier: 'v4_pool',
      classified_topic: topicName || null,
      depth,
      routing_method: topicName ? 'pool_selection' : 'fallback_tags',
      pool_size: Object.keys(ctaDefs).length,
      filtered_count: pool.length,
      conversation_context: {
        matched_topics: resolvedTags,
        selected_ctas: selectedIds,
        last_classified_topic: topicName || null,
      }
    }
  };
}

/**
 * Determine depth preference based on session context and topic definition.
 *
 * Depth is a SORT preference for diverse CTA selection, not a filter.
 * It controls which tier gets the primary slot:
 *   - info → learning CTA is primary, action is secondary
 *   - action → action CTA is primary, learning is secondary
 *
 * Rules:
 *   - If topic has depth_override: "action" → action
 *   - If primary topic tag (tags[0]) overlaps with accumulated_topics → action
 *   - Otherwise → info (first encounter — learning CTAs get primary position)
 *
 * @param {string[]} resolvedTags - Tags from the topic definition
 * @param {Object} sessionContext - { accumulated_topics, ... }
 * @param {Object|null} topicDef - The matched topic definition, or null
 * @returns {'info'|'action'}
 */
function determineDepthPreference(resolvedTags, sessionContext, topicDef) {
  // Operator override: topic is always action-ready
  if (topicDef?.depth_override === 'action') {
    console.log(`[V4.1 depth] depth_override=action on topic "${topicDef.name}"`);
    return 'action';
  }

  // Check if the primary tag (tags[0]) is already in accumulated_topics
  const primaryTag = resolvedTags[0] || null;
  const accumulated = sessionContext.accumulated_topics || [];

  if (primaryTag && accumulated.includes(primaryTag)) {
    console.log(`[V4.1 depth] Primary tag "${primaryTag}" found in accumulated_topics → action`);
    return 'action';
  }

  console.log(`[V4.1 depth] Primary tag "${primaryTag || 'none'}" not in accumulated [${accumulated.join(', ')}] → info`);
  return 'info';
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

  // Step 3a: Topic Classification (non-streaming LLM call)
  buildTopicClassificationPrompt,
  classifyTopic,

  // Step 3b: Dynamic CTA Pool Selection (deterministic, no AI)
  selectCTAsFromPool,
  determineDepthPreference,

  // Config validation
  validateTopicDefinitions,

  // Parameters
  V4_STEP2_INFERENCE_PARAMS,
  V4_STEP3_INFERENCE_PARAMS,

  // Utilities (exported for testing)
  sanitizeTonePromptV4,
};
