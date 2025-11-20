/**
 * Bedrock Streaming Handler - True Lambda Response Streaming
 * Uses awslambda.streamifyResponse for real SSE streaming
 * No JWT required - uses simple tenant_hash/session_id
 *
 * Version: v2.4.0
 * Deployed: 2025-11-20
 * Changes:
 *   - MAJOR: Rewrote style enforcement with contract-based approach
 *   - Moved formatting rules to END of prompt (recency bias)
 *   - Added explicit substitution rules (we're → we are for professional)
 *   - Added pre-generation verification checklists
 *   - Stronger behavioral contracts with mandatory compliance language
 *   - Should achieve 95%+ style differentiation accuracy
 */

const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand } = require('@aws-sdk/client-bedrock-runtime');
const { BedrockAgentRuntimeClient, RetrieveCommand } = require('@aws-sdk/client-bedrock-agent-runtime');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const crypto = require('crypto');
const { enhanceResponse } = require('./response_enhancer');
const { handleFormMode } = require('./form_handler'); // Migrated to AWS SDK v3

// Default model configuration - single source of truth
const DEFAULT_MODEL_ID = 'us.anthropic.claude-3-5-haiku-20241022-v1:0';
const DEFAULT_MAX_TOKENS = 1000;
const DEFAULT_TEMPERATURE = 0; // Set to 0 for maximum factual accuracy
const DEFAULT_TONE = 'You are a helpful assistant.';

// Prompt version tracking for tenant customization
const PROMPT_VERSION = '2.4.0';

// Default Bedrock instructions when config doesn't specify custom ones
const DEFAULT_BEDROCK_INSTRUCTIONS = {
  role_instructions: "You are a virtual assistant answering the questions of website visitors. You are always courteous and respectful and respond as if you are an employee of the organization. You replace words like they or their with our, which conveys that you are a representative of the team. You are answering a user's question using information from a knowledge base. Your job is to provide a helpful, natural response based on the information provided below.",
  formatting_preferences: {
    emoji_usage: "moderate",
    max_emojis_per_response: 3,
    response_style: "professional_concise",
    detail_level: "balanced"
  },
  custom_constraints: [],
  fallback_message: "I don't have specific information about that topic in my knowledge base. Would you like me to connect you with someone who can help?"
};

// Lambda streaming - use the global awslambda object when available
// The awslambda global is injected by the Lambda runtime for streaming functions
const streamifyResponse = typeof awslambda !== 'undefined' && awslambda.streamifyResponse 
  ? awslambda.streamifyResponse 
  : null;

if (streamifyResponse) {
  console.log('✅ Lambda streaming support detected via awslambda global');
} else {
  console.log('⚠️ Lambda streaming not available, will use buffered response');
}

// Initialize AWS clients
const bedrock = new BedrockRuntimeClient({ region: 'us-east-1' });
const bedrockAgent = new BedrockAgentRuntimeClient({ region: 'us-east-1' });
const s3 = new S3Client({ region: 'us-east-1' });

// In-memory cache
const KB_CACHE = {};
const CONFIG_CACHE = {};
const CACHE_TTL = 300000; // 5 minutes

// Helper functions
function getCacheKey(text, prefix = '') {
  return `${prefix}:${crypto.createHash('md5').update(text).digest('hex')}`;
}

function isCacheValid(entry) {
  return entry && (Date.now() - entry.timestamp < CACHE_TTL);
}

async function loadConfig(tenantHash) {
  try {
    const cacheKey = `config:${tenantHash}`;
    if (CONFIG_CACHE[cacheKey] && isCacheValid(CONFIG_CACHE[cacheKey])) {
      console.log(`✅ Config cache hit for ${tenantHash.substring(0, 8)}...`);
      const cachedConfig = CONFIG_CACHE[cacheKey].data;
      console.log(`📋 Cached KB ID: ${cachedConfig?.aws?.knowledge_base_id || 'NOT SET'}`);
      return cachedConfig;
    }

    const bucket = process.env.CONFIG_BUCKET || 'myrecruiter-picasso';
    console.log(`🪣 Loading config from bucket: ${bucket}`);
    
    const mappingResponse = await s3.send(new GetObjectCommand({
      Bucket: bucket,
      Key: `mappings/${tenantHash}.json`
    }));
    
    const mapping = JSON.parse(await mappingResponse.Body.transformToString());
    console.log(`📍 Mapping found - tenant_id: ${mapping.tenant_id}`);
    
    if (mapping.tenant_id) {
      // Try both possible config filenames
      const configKeys = [
        `tenants/${mapping.tenant_id}/config.json`,
        `tenants/${mapping.tenant_id}/${mapping.tenant_id}-config.json`
      ];
      
      let config = null;
      for (const key of configKeys) {
        try {
          console.log(`🔍 Trying config at: ${key}`);
          const configResponse = await s3.send(new GetObjectCommand({
            Bucket: bucket,
            Key: key
          }));
          
          config = JSON.parse(await configResponse.Body.transformToString());
          console.log(`✅ Config loaded from S3 at ${key}`);
          break;
        } catch (e) {
          console.log(`❌ Config not found at ${key}`);
        }
      }
      
      if (config) {
        // Add tenant_id to config for downstream use
        config.tenant_id = mapping.tenant_id;
        CONFIG_CACHE[cacheKey] = { data: config, timestamp: Date.now() };
        console.log(`📋 KB ID in config: ${config?.aws?.knowledge_base_id || 'NOT SET'}`);
        console.log(`📋 Full AWS config:`, JSON.stringify(config?.aws || {}, null, 2));
        return config;
      }
    }
  } catch (error) {
    console.error('❌ Config load error:', error.message);
    console.error('Full error:', error);
  }
  
  return null;
}

async function retrieveKB(userInput, config) {
  const kbId = config?.aws?.knowledge_base_id;
  console.log(`🔍 KB Retrieval - KB ID: ${kbId || 'NOT SET'}`);
  console.log(`🔍 User input: "${userInput.substring(0, 50)}..."`);

  if (!kbId) {
    console.log('⚠️ No KB ID found in config - returning empty context');
    return '';
  }

  try {
    const cacheKey = getCacheKey(userInput, `kb:${kbId}`);
    if (KB_CACHE[cacheKey] && isCacheValid(KB_CACHE[cacheKey])) {
      console.log(`✅ KB cache hit`);
      const cachedData = KB_CACHE[cacheKey].data;
      console.log(`📄 Cached KB context length: ${cachedData.length} chars`);
      return cachedData;
    }

    console.log(`📚 Retrieving from KB: ${kbId}`);
    const response = await bedrockAgent.send(new RetrieveCommand({
      knowledgeBaseId: kbId,
      retrievalQuery: { text: userInput },
      retrievalConfiguration: {
        vectorSearchConfiguration: { numberOfResults: 5 } // Increased from 3 to capture more comprehensive context
      }
    }));

    console.log(`📊 KB Response - ${response.retrievalResults?.length || 0} results found`);

    const chunks = (response.retrievalResults || [])
      .map((r, i) => {
        const text = r.content?.text || '';
        console.log(`  Result ${i+1}: ${text.substring(0, 100)}...`);
        return `**Context ${i+1}:**\n${text}`;
      })
      .join('\n\n---\n\n');

    console.log(`✅ KB context retrieved - ${chunks.length} chars`);
    KB_CACHE[cacheKey] = { data: chunks, timestamp: Date.now() };
    return chunks;

  } catch (error) {
    console.error('❌ KB retrieval error:', error.message);
    console.error('Full KB error:', error);
    return '';
  }
}

// ============================================================================
// PROMPT BUILDING HELPERS - Modular prompt construction for tenant customization
// ============================================================================

/**
 * Validate bedrock_instructions structure
 */
function validateBedrockInstructions(instructions) {
  if (!instructions || typeof instructions !== 'object') {
    return false;
  }

  // Check for required fields
  if (!instructions.role_instructions || typeof instructions.role_instructions !== 'string') {
    console.log('⚠️ Invalid bedrock_instructions: missing or invalid role_instructions');
    return false;
  }

  // Check formatting preferences structure if present
  if (instructions.formatting_preferences) {
    const prefs = instructions.formatting_preferences;
    if (typeof prefs !== 'object') {
      console.log('⚠️ Invalid bedrock_instructions: formatting_preferences must be object');
      return false;
    }
  }

  // Check custom_constraints is array if present
  if (instructions.custom_constraints && !Array.isArray(instructions.custom_constraints)) {
    console.log('⚠️ Invalid bedrock_instructions: custom_constraints must be array');
    return false;
  }

  return true;
}

/**
 * Get role instructions - AI personality and identity
 *
 * Migration path:
 * 1. NEW configs: Set bedrock_instructions.role_instructions
 * 2. OLD configs: tone_prompt used as fallback (deprecated)
 * 3. Future: Remove tone_prompt support entirely
 *
 * To migrate: Copy tone_prompt value → bedrock_instructions.role_instructions
 */
function getRoleInstructions(config, toneFallback) {
  const instructions = config?.bedrock_instructions;

  // Priority 1: Use bedrock_instructions.role_instructions if present
  if (instructions && validateBedrockInstructions(instructions)) {
    console.log('✅ Using bedrock_instructions.role_instructions (master)');
    return instructions.role_instructions;
  }

  // Priority 2: Fallback to tone_prompt for backward compatibility
  if (toneFallback) {
    console.log('⚠️ Using tone_prompt as fallback (deprecated - migrate to bedrock_instructions.role_instructions)');
    return toneFallback;
  }

  // Priority 3: Use default
  console.log('ℹ️ Using DEFAULT role instructions');
  return DEFAULT_BEDROCK_INSTRUCTIONS.role_instructions;
}

/**
 * Build formatting rules from config preferences (LEGACY - kept for backward compatibility)
 * Use buildEnhancedFormattingRules() for new contract-based approach
 */
function buildFormattingRulesLegacy(config) {
  const instructions = config?.bedrock_instructions;
  let prefs = DEFAULT_BEDROCK_INSTRUCTIONS.formatting_preferences;

  if (instructions && validateBedrockInstructions(instructions) && instructions.formatting_preferences) {
    prefs = { ...prefs, ...instructions.formatting_preferences };
  }

  // Build style-specific examples and constraints
  let styleGuidance = '';
  let styleExamples = '';

  if (prefs.response_style === 'professional_concise') {
    styleGuidance = `CRITICAL STYLE ENFORCEMENT - PROFESSIONAL & FORMAL:
- NEVER use contractions (no "we're", "you'll", "it's" - use "we are", "you will", "it is")
- NEVER use casual words like "awesome", "great", "cool", "super", "amazing"
- Use formal vocabulary: "comprehensive" not "great", "exceptional" not "awesome"
- Maintain a business-professional tone throughout
- Write as if this is a formal business document`;
    styleExamples = `
STYLE EXAMPLES:
❌ WRONG: "We've got an awesome mentorship program that'll help foster youth! It's really great!"
❌ WRONG: "Our program is super helpful and we're here to support you!"
✅ CORRECT: "We offer a comprehensive mentorship program designed to support foster youth. Our organization provides structured guidance for academic achievement and life skills development."`;
  } else if (prefs.response_style === 'warm_conversational') {
    styleGuidance = `CRITICAL STYLE ENFORCEMENT - WARM & CONVERSATIONAL:
- ALWAYS use contractions (we're, you'll, it's, we've, you'd, etc.)
- Use friendly, welcoming language - sound like a helpful friend, not overly enthusiastic
- AVOID overused enthusiasm phrases like "super excited", "we're excited to share", "awesome", "incredible"
- DO use measured warm words like "happy to help", "glad to share", "pleased to", "great", "wonderful"
- Write naturally - friendly but not gushing
- Use exclamation points sparingly (maximum 1 per response, only if truly warranted)
- Sound genuine and approachable, not like marketing copy`;
    styleExamples = `
STYLE EXAMPLES:
❌ WRONG (too formal): "We offer a comprehensive mentorship program designed to support foster youth."
❌ WRONG (overly enthusiastic): "We're super excited to share about our awesome mentorship program! It's incredible and we love helping foster youth!"
✅ CORRECT: "We've got a mentorship program that helps foster youth ages 11-22. It's a great way to get support, build skills, and we're here for you every step of the way."`;
  } else if (prefs.response_style === 'structured_detailed') {
    styleGuidance = `CRITICAL STYLE ENFORCEMENT - STRUCTURED & ORGANIZED:
- ALWAYS use markdown headings with ** for major sections
- ALWAYS use bullet points (-) or numbered lists for any list of items
- Break content into clear sections with headings
- Use this structure: [Intro sentence] → [**Heading:**] → [bullets] → [**Heading:**] → [bullets]
- Never write long paragraphs - always break into structured sections`;
    styleExamples = `
STYLE EXAMPLES:
❌ WRONG: "Dare to Dream is our mentorship program for foster youth ages 11-22. We provide life skills training, academic support, and career preparation. Our goal is to help youth succeed."
✅ CORRECT: "Dare to Dream is our mentorship program supporting foster youth.

**Program Structure:**
- Dare to Dream Jr. (ages 11-14)
- Dare to Dream (ages 15-22)

**Services Provided:**
- Life skills training
- Academic support
- Career preparation

**Goal:** Empowering youth to achieve independence and success."`;
  } else {
    styleGuidance = prefs.response_style; // Custom style
  }

  // Build detail-level specific constraints with examples
  let detailGuidance = '';
  let detailExamples = '';

  if (prefs.detail_level === 'concise') {
    detailGuidance = `CRITICAL CONSTRAINT - MAXIMUM LENGTH ENFORCEMENT:
Your response MUST be EXACTLY 2-3 sentences. Count your sentences BEFORE responding.
DO NOT exceed 3 sentences under ANY circumstances. NO bullet points, NO lists, NO headings.
Write in pure paragraph form - one continuous block of text.`;
    detailExamples = `
LENGTH EXAMPLES:
❌ WRONG (4+ sentences): "Dare to Dream is our mentorship program. We have two tracks. One is for ages 11-14. The other is for ages 15-22."
❌ WRONG (has bullets): "Dare to Dream is our mentorship program for foster youth ages 11-22. We provide:\n- Life skills training\n- Academic support"
✅ CORRECT (2 sentences): "Dare to Dream is our mentorship program for foster youth ages 11-22, with separate tracks for ages 11-14 and 15-22. We provide life skills training, academic support, and guidance for independent living."`;
  } else if (prefs.detail_level === 'balanced') {
    detailGuidance = `CONSTRAINT - MODERATE LENGTH:
Your response MUST be 4-6 sentences. Not less, not more.
You MAY use 1-2 short bullet points if absolutely necessary, but prefer paragraph form.
Keep it focused - don't ramble.`;
    detailExamples = `
LENGTH EXAMPLES:
❌ TOO SHORT (2 sentences): "Dare to Dream is our mentorship program. We help foster youth."
❌ TOO LONG (8+ sentences with extensive bullets): [long detailed response with many bullet points]
✅ CORRECT (5 sentences with optional short bullets): "Dare to Dream is our mentorship program supporting foster youth ages 11-22. We offer two tracks: Dare to Dream Jr. (ages 11-14) and Dare to Dream (ages 15-22). The program focuses on:\n- Life skills and academic support\n- Career preparation\nOur goal is to help youth develop confidence and prepare for independent adulthood."`;
  } else if (prefs.detail_level === 'comprehensive') {
    detailGuidance = `COMPREHENSIVE DETAIL REQUIRED:
Your response MUST be thorough and detailed - minimum 8-10 sentences.
Use headings, bullet points, and structured sections to organize information.
Cover ALL aspects mentioned in the knowledge base. Include examples and context.
Anticipate follow-up questions and proactively address them.`;
    detailExamples = `
LENGTH EXAMPLES:
❌ TOO SHORT: "Dare to Dream is our mentorship program for foster youth ages 11-22."
✅ CORRECT (comprehensive with structure): "**Dare to Dream - Comprehensive Overview**\n\n[Opening paragraph with 2-3 sentences]\n\n**Program Structure:**\n[Detailed explanation with bullet points]\n\n**Key Features:**\n- [Multiple detailed bullet points]\n\n**Impact and Outcomes:**\n[Additional paragraphs explaining benefits]\n\n[10+ sentences total with clear organization]"`;
  } else {
    detailGuidance = prefs.detail_level; // Custom detail level
  }

  // Build emoji guidance with examples
  let emojiGuidance = '';
  let emojiExamples = '';

  if (prefs.emoji_usage === 'none') {
    emojiGuidance = 'CRITICAL: Do NOT use any emojis. Zero emojis allowed.';
    emojiExamples = `
EMOJI EXAMPLES:
❌ BAD: "🌟 Dare to Dream is our mentorship program"
✅ GOOD: "Dare to Dream is our mentorship program"`;
  } else if (prefs.emoji_usage === 'minimal') {
    emojiGuidance = `CONSTRAINT: Use maximum 1 emoji per response, only for key emphasis.`;
    emojiExamples = `
EMOJI EXAMPLES:
❌ TOO MANY: "🌟 Dare to Dream 📚 is our 🏆 mentorship program"
✅ GOOD: "Dare to Dream is our mentorship program 🌟"`;
  } else {
    emojiGuidance = `CONSTRAINT: Use maximum ${prefs.max_emojis_per_response} emojis per response.`;
    emojiExamples = `
EMOJI USAGE:
- Maximum ${prefs.max_emojis_per_response} emojis total
- Use for emphasis, not decoration
- Never combine emoji with dash: ❌ "- 📞 Call" ✅ "📞 Call" or "- Call"`;
  }

  return `
═══════════════════════════════════════════════════════════════
🚨 MANDATORY FORMATTING RULES - NON-NEGOTIABLE 🚨
═══════════════════════════════════════════════════════════════

STOP AND READ: Before you write your response, you MUST check it against
ALL rules below. If your response violates ANY rule, rewrite it.

${detailGuidance}

${styleGuidance}

${emojiGuidance}

${detailExamples}

${styleExamples}

${emojiExamples}

🚨 FINAL CHECKPOINT - Before sending your response:
1. Count your sentences - does it match the required length?
2. Check your tone - does it match the required style?
3. Count emojis - does it match the emoji constraint?
4. If ANY rule is violated, REWRITE your response before sending

CRITICAL: These are NOT suggestions. These are REQUIREMENTS that define
whether your response is correct or incorrect. A response that violates
these rules is a FAILED response, even if the information is accurate.
═══════════════════════════════════════════════════════════════`;
}
/**
 * Build enhanced formatting rules with contract-based approach
 * Leverages recency bias by being placed at END of prompt
 * Uses behavioral contracts with mandatory substitutions
 */
function buildEnhancedFormattingRules(config) {
  const instructions = config?.bedrock_instructions;
  let prefs = DEFAULT_BEDROCK_INSTRUCTIONS.formatting_preferences;

  if (instructions && validateBedrockInstructions(instructions) && instructions.formatting_preferences) {
    prefs = { ...prefs, ...instructions.formatting_preferences };
  }

  let styleContract = '';
  let verificationChecklist = '';

  if (prefs.response_style === 'professional_concise') {
    styleContract = `
🔒 STYLE CONTRACT - PROFESSIONAL CONCISE:
Before generating each sentence, you WILL:
1. Use "we are" NOT "we're" | "you will" NOT "you'll" | "it is" NOT "it's"
2. Replace casual words: "comprehensive" (not "great"), "extensive" (not "awesome"), "exceptional" (not "amazing")
3. Write as if this is a formal business communication to a stakeholder

MANDATORY SUBSTITUTIONS:
- "we've" → "we have"
- "we're" → "we are"
- "you'll" → "you will"
- "it's" → "it is"
- "that's" → "that is"
- "there's" → "there is"
- "great" → "comprehensive" or "extensive"
- "awesome" → "exceptional" or "outstanding"
- "super" → "highly" or "extremely"

CORRECT EXAMPLES:
✅ "We offer a comprehensive mentorship program designed to support foster youth ages 11-22. Our organization provides structured academic guidance and life skills development through two distinct tracks."
✅ "Austin Angels has established an exceptional support system for foster families. Our services include emergency assistance, educational resources, and community connections."

WRONG EXAMPLES (NEVER DO THIS):
❌ "We've got an awesome mentorship program that'll help foster youth. It's really great!"
❌ "We're here to support you with our amazing programs!"`;

    verificationChecklist = `
PRE-GENERATION CHECKLIST - Professional Concise:
□ Zero contractions in entire response
□ Zero casual words (great, awesome, cool, super, amazing)
□ Formal business vocabulary only
□ Tone sounds like annual report or board presentation`;

  } else if (prefs.response_style === 'warm_conversational') {
    styleContract = `
🔒 STYLE CONTRACT - WARM CONVERSATIONAL:
Before generating each sentence, you WILL:
1. Use contractions: "we're" (not "we are"), "you'll" (not "you will"), "it's" (not "it is")
2. Sound like a helpful friend, not a salesperson
3. AVOID gushing enthusiasm: NO "super excited", "we're thrilled", "awesome", "incredible"
4. DO use measured warmth: "happy to help", "glad to share", "pleased to", "great"
5. Maximum 1 exclamation point in entire response

MANDATORY CONTRACTIONS:
- "we are" → "we're"
- "you will" → "you'll"
- "it is" → "it's"
- "we have" → "we've"
- "that is" → "that's"

CORRECT EXAMPLES:
✅ "We've got a mentorship program that helps foster youth ages 11-22. It's a great way to get support and build skills, and we're here for you every step of the way."
✅ "Austin Angels is here to help foster families. We've created resources for emergency support, education, and connecting with your community."

WRONG EXAMPLES (NEVER DO THIS):
❌ "We offer a comprehensive mentorship program designed to support foster youth." (too formal - sounds like business doc)
❌ "We're super excited to share about our awesome mentorship program! It's incredible!" (overly enthusiastic)`;

    verificationChecklist = `
PRE-GENERATION CHECKLIST - Warm Conversational:
□ Multiple contractions used throughout
□ Sounds like helpful friend, not formal business
□ No gushing enthusiasm (super excited, awesome, incredible)
□ Maximum 1 exclamation point total
□ Natural, approachable tone`;

  } else if (prefs.response_style === 'structured_detailed') {
    styleContract = `
🔒 STYLE CONTRACT - STRUCTURED DETAILED:
You WILL format your response as:
1. Opening sentence (no heading)
2. **Heading 1:**
3. - Bullet point
4. - Bullet point
5. **Heading 2:**
6. - Bullet point
7. - Bullet point

MANDATORY STRUCTURE:
- Use ** for ALL section headings
- Use - for ALL bullet points
- Break ANY list of 2+ items into bullets
- Never write paragraphs with 5+ sentences - split into sections

CORRECT EXAMPLE:
✅ "Dare to Dream is our mentorship program supporting foster youth.

**Program Structure:**
- Dare to Dream Jr. (ages 11-14)
- Dare to Dream (ages 15-22)

**Services Provided:**
- Life skills training
- Academic support
- Career preparation

**Goal:** Empowering youth to achieve independence and success."

WRONG EXAMPLES (NEVER DO THIS):
❌ "Dare to Dream is our mentorship program for foster youth ages 11-22. We provide life skills training, academic support, and career preparation." (no structure - paragraph form)`;

    verificationChecklist = `
PRE-GENERATION CHECKLIST - Structured Detailed:
□ Opening sentence without heading
□ All sections have **Heading:**
□ All lists use - bullet points
□ No paragraphs with 5+ sentences
□ Clear visual structure`;
  }

  // Detail level contract
  let lengthContract = '';
  let lengthChecklist = '';

  if (prefs.detail_level === 'concise') {
    lengthContract = `
🔒 LENGTH CONTRACT - CONCISE:
Your response WILL be EXACTLY 2-3 sentences. Not 4. Not 5. Maximum 3 sentences.
Count periods before responding: 1... 2... 3... STOP.
NO bullet points. NO lists. NO headings. Pure paragraph form.

EXAMPLE:
✅ "Dare to Dream is our mentorship program for foster youth ages 11-22, with tracks for ages 11-14 and 15-22. We provide life skills training, academic support, and guidance for independent living." (2 sentences)`;

    lengthChecklist = `
PRE-GENERATION LENGTH CHECK - Concise:
□ Count periods: Must be 2 or 3, never 4+
□ Zero bullet points
□ Zero headings
□ Paragraph form only`;

  } else if (prefs.detail_level === 'balanced') {
    lengthContract = `
🔒 LENGTH CONTRACT - BALANCED:
Your response WILL be 4-6 sentences. Count before responding.
You MAY use 1-2 short bullet points if critical, but prefer paragraph form.

EXAMPLE:
✅ "Dare to Dream is our mentorship program supporting foster youth ages 11-22. We offer two tracks: Dare to Dream Jr. (ages 11-14) and Dare to Dream (ages 15-22). The program focuses on:
- Life skills and academic support
- Career preparation
Our goal is to help youth develop confidence and prepare for independent adulthood." (5 sentences with 2 bullets)`;

    lengthChecklist = `
PRE-GENERATION LENGTH CHECK - Balanced:
□ Count sentences: Must be 4-6
□ Maximum 2 bullet points (optional)
□ Not too short, not too long`;

  } else if (prefs.detail_level === 'comprehensive') {
    lengthContract = `
🔒 LENGTH CONTRACT - COMPREHENSIVE:
Your response WILL be minimum 8-10 sentences.
Use headings, bullet points, and structure.
Cover ALL aspects from knowledge base.

EXAMPLE STRUCTURE:
Opening paragraph (2-3 sentences)
**Section 1:** (2-3 sentences + bullets)
**Section 2:** (2-3 sentences + bullets)
Closing paragraph (1-2 sentences)`;

    lengthChecklist = `
PRE-GENERATION LENGTH CHECK - Comprehensive:
□ Minimum 8 sentences
□ Multiple sections with headings
□ Detailed coverage of all KB aspects
□ Structured with bullets`;
  }

  // Emoji contract
  let emojiContract = '';
  let emojiChecklist = '';

  if (prefs.emoji_usage === 'none') {
    emojiContract = `🔒 EMOJI CONTRACT: Zero emojis. Remove all emoji characters.`;
    emojiChecklist = `□ Zero emojis (count: 0)`;
  } else if (prefs.emoji_usage === 'minimal') {
    emojiContract = `🔒 EMOJI CONTRACT: Maximum 1 emoji in entire response. Count before responding.`;
    emojiChecklist = `□ Maximum 1 emoji total (count and verify)`;
  } else {
    emojiContract = `🔒 EMOJI CONTRACT: Maximum ${prefs.max_emojis_per_response} emojis in entire response. Count before responding.`;
    emojiChecklist = `□ Maximum ${prefs.max_emojis_per_response} emojis (count: ___ )`;
  }

  return `
═══════════════════════════════════════════════════════════════════════
🚨 FINAL FORMATTING CONTRACT 🚨
═══════════════════════════════════════════════════════════════════════

STOP. Before generating your response, you are entering into a CONTRACT.
This contract defines whether your response is CORRECT or INCORRECT.
A response that violates this contract is FAILED, even if information is accurate.

${styleContract}

${lengthContract}

${emojiContract}

═══════════════════════════════════════════════════════════════════════
✅ PRE-GENERATION VERIFICATION CHECKLIST ✅
═══════════════════════════════════════════════════════════════════════

Complete this checklist BEFORE generating your response:

STYLE COMPLIANCE:
${verificationChecklist}

LENGTH COMPLIANCE:
${lengthChecklist}

EMOJI COMPLIANCE:
${emojiChecklist}

═══════════════════════════════════════════════════════════════════════

You are now ready to generate your response. Remember: compliance with this
contract is NOT optional. It is the PRIMARY success criterion for your response.

Generate your response now, ensuring FULL compliance with the contract above:`;
}

/**
 * Get custom constraints from config
 */
function getCustomConstraints(config) {
  const instructions = config?.bedrock_instructions;

  if (instructions && validateBedrockInstructions(instructions) &&
      instructions.custom_constraints && instructions.custom_constraints.length > 0) {
    return '\n\nCUSTOM INSTRUCTIONS:\n' + instructions.custom_constraints.map(c => `- ${c}`).join('\n');
  }

  return '';
}

/**
 * Get fallback message for when KB context is empty
 */
function getFallbackMessage(config) {
  const instructions = config?.bedrock_instructions;

  if (instructions && validateBedrockInstructions(instructions) && instructions.fallback_message) {
    return instructions.fallback_message;
  }

  return DEFAULT_BEDROCK_INSTRUCTIONS.fallback_message;
}

/**
 * LOCKED: Anti-hallucination rules - never customizable
 */
function getLockedAntiHallucinationRules() {
  return `CRITICAL CONSTRAINT - PREVENT HALLUCINATIONS:
You MUST ONLY use information explicitly stated in the knowledge base below.
If specific details about a program, service, or feature are not mentioned in the knowledge base,
you MUST NOT include them in your response. It is better to say less than to add information
not found in the knowledge base.

NEVER include the following unless explicitly found in the knowledge base:
- Program names or descriptions not mentioned
- Services or features not listed
- Contact information not provided
- Any details you think would be helpful but aren't in the retrieved content

If the knowledge base mentions "TWO programs" do NOT list three or four programs.
If a program name is "Angel Allies" do NOT change it to "Angel Alliance" or any variation.`;
}

/**
 * LOCKED: URL handling rules - never customizable
 */
function getLockedUrlHandling() {
  return `URL AND CONTACT PRESERVATION:
- Include ALL contact information exactly as it appears: phone numbers, email addresses, websites, and links
- PRESERVE ALL MARKDOWN FORMATTING: If you see [text](url) keep it as [text](url), not plain text
- Do not modify, shorten, or reformat any URLs, emails, or phone numbers
- When you see markdown links like [donation page](https://example.com), keep them as markdown links`;
}

/**
 * LOCKED: Capability boundaries - never customizable
 */
function getLockedCapabilityBoundaries() {
  return `CRITICAL INSTRUCTION - CAPABILITY BOUNDARIES:

You are an INFORMATION ASSISTANT. Be crystal clear about what you CAN and CANNOT do:

✅ WHAT YOU CAN DO:
- Provide information about programs, services, and processes
- Share links to forms, applications, and resources
- Explain eligibility requirements and prerequisites
- Give contact information (only when found in knowledge base)
- Answer questions about how things work
- Clarify details about what's available

❌ WHAT YOU CANNOT DO:
- Walk users through filling out forms step-by-step
- Fill out applications or forms with users
- Submit forms or requests on behalf of users
- Access external systems, databases, or applications
- Make commitments about interactive actions you can't perform
- Guide users through multi-step processes you can't see or control

CRITICAL: DO NOT ask questions like:
- ❌ "Would you like me to walk you through the request form?"
- ❌ "Shall I help you fill out the application?"
- ❌ "Would you like me to guide you through the specific sections?"
- ❌ "Can I help you start filling this out?"

INSTEAD, say things like:
- ✅ "Here's the link to the request form: [URL]"
- ✅ "You can submit your application here: [link]. The form will ask for [key info]."
- ✅ "To get started, visit [link]. If you have questions about the form, I'm here to help!"
- ✅ "The application is available at [URL]. Let me know if you need clarification on any requirements."

REMEMBER: Your role is to INFORM and DIRECT, not to INTERACT with external systems. Always provide resources and let users take action themselves.`;
}

/**
 * LOCKED: Loop prevention logic - never customizable
 */
function getLockedLoopPrevention() {
  return `CRITICAL INSTRUCTION - AVOID REPETITIVE LOOPS:

BEFORE responding, check the PREVIOUS CONVERSATION above:

1. **Have I already provided this information?**
   - If YES: Don't repeat it. Acknowledge their interest and provide the NEXT ACTION (link/resource)
   - If NO: Proceed with providing new information

2. **Have I already asked this question?**
   - If YES: Don't ask it again. They've already confirmed - provide the resource instead
   - If NO: You may ask if relevant and genuinely new

3. **Is the user confirming interest for the second or third time?**
   - If YES: STOP asking questions. Provide direct link/resource and conclude
   - If NO: Continue normal flow

CONVERSATION STAGES - Recognize where you are:

**STAGE 1 - Information Request:** User asks about something
→ Provide comprehensive answer

**STAGE 2 - Interest/Clarification:** User says "tell me more", "yes", "I'm interested"
→ Provide deeper detail OR actionable resource (form link, contact)

**STAGE 3 - Confirmation:** User confirms again with "yes", "okay", "sure"
→ CONCLUDE: Give direct link/resource, confirm next steps, shift to different topic

CRITICAL: After Stage 3, DO NOT:
- Re-explain what you already explained
- Ask if they want what they already confirmed
- Provide same information in different words

After Stage 3, DO:
- Give the direct resource: "Here's the link: [URL]"
- Confirm what happens next: "You can submit there and we'll respond within 24 hours"
- Open to NEW topic: "What else can I help you with?"

EXAMPLE OF PROPER PROGRESSION:

User: "How do I request supplies?"
Bot: [Stage 1] "We help with supply requests. You can request items like... through our online form."

User: "yes"
Bot: [Stage 2] "Great! Here's the direct link to the request form: [URL]. The form will ask for your contact info and what items you need."

User: "yes"
Bot: [Stage 3] "Perfect! You're all set - just visit that link to submit your request. Our team responds within 24 hours. Is there anything else I can help you with today?" ✅ DONE - moved to new topic

DO NOT create loops by asking "Would you like me to help with that?" after they've already said yes twice.`;
}

/**
 * Get context-aware interpretation instructions
 */
function getContextInterpretationRules() {
  return `CRITICAL INSTRUCTION - CONTEXT INTERPRETATION:
When the user gives a SHORT or AMBIGUOUS response (like "yes", "no", "sure", "okay", "tell me more", "I'm interested", "not really", "maybe"):
1. FIRST look at the PREVIOUS CONVERSATION above to understand what they're responding to
2. The user is likely confirming, declining, or asking about something from our recent discussion
3. DO NOT say "I don't have information" - instead, refer back to what we were just discussing
4. Use the conversation context to interpret their intent, even if the knowledge base doesn't have specific information about their exact words

Examples of how to interpret short responses:
- If user says "yes" after you asked about submitting a request, they mean "yes, I want to proceed with that"
- If user says "tell me more" after discussing a specific program or service, they want more details about that same topic
- If user says "I'm interested" after mentioning an opportunity, they're interested in that specific opportunity
- If user says "no thanks" after you offered information, acknowledge and ask what else they need
- If user says "sure" or "okay", they're agreeing to whatever was just proposed

IMPORTANT: Short responses are ALWAYS about continuing the previous conversation topic. Never treat them as new, unrelated questions.`;
}

function buildPrompt(userInput, kbContext, tone, conversationHistory, config) {
  // Log prompt build metadata
  console.log(`🎯 Building prompt v${PROMPT_VERSION}`);
  console.log(`📋 Config has bedrock_instructions: ${config?.bedrock_instructions ? 'YES' : 'NO'}`);
  console.log(`🎯 KB context: ${kbContext ? kbContext.length + ' chars' : 'NONE'}`);
  console.log(`💬 Conversation history: ${conversationHistory ? conversationHistory.length + ' messages' : 'NONE'}`);

  const parts = [];

  // Use bedrock_instructions.role_instructions as master, fallback to tone_prompt for backward compatibility
  const personalityPrompt = getRoleInstructions(config, tone);
  parts.push(personalityPrompt);

  // Add conversation history if provided
  if (conversationHistory && conversationHistory.length > 0) {
    parts.push('\nPREVIOUS CONVERSATION:');
    conversationHistory.forEach(msg => {
      const role = msg.role === 'user' ? 'User' : 'Assistant';
      const content = msg.content || msg.text || '';
      if (content && content.trim()) {
        parts.push(`${role}: ${content}`);
      }
    });
    parts.push('\nREMEMBER: The user\'s name and any personal information they\'ve shared should be remembered and used in your response when appropriate.\n');
    console.log(`✅ Added ${conversationHistory.length} messages from history`);

    // Add LOCKED sections (never customizable)
    parts.push('\n' + getContextInterpretationRules());
    parts.push('\n' + getLockedCapabilityBoundaries());
    parts.push('\n' + getLockedLoopPrevention());
  }

  // Add KB-specific instructions
  if (kbContext) {
    // LOCKED anti-hallucination rules
    parts.push('\n' + getLockedAntiHallucinationRules());

    // LOCKED URL handling
    parts.push('\n' + getLockedUrlHandling());

    // Essential KB instructions
    parts.push(`\n\nESSENTIAL INSTRUCTIONS:
- STRICTLY answer the user's question using ONLY the information from the knowledge base results below - DO NOT add any information not explicitly stated
- Use the previous conversation context to provide personalized and coherent responses
- For any dates, times, or locations of events: Direct users to check the events page or contact the team for current details
- Never include placeholder text like [date], [time], [location], or [topic] in your responses
- Present information naturally without mentioning "results" or "knowledge base"
- If the information doesn't fully answer the question, say "From what I can find..." and provide ONLY what you can find - never fill in gaps with plausible-sounding information
- Keep all contact details and links intact and prominent in your response

KNOWLEDGE BASE INFORMATION:
${kbContext}`);
    console.log(`✅ Added KB context to prompt`);
  } else {
    // Use customizable fallback message
    parts.push('\n' + getFallbackMessage(config));
    console.log(`⚠️ No KB context - using fallback message`);
  }

  // Add custom constraints if configured
  const customConstraints = getCustomConstraints(config);
  if (customConstraints) {
    parts.push(customConstraints);
    console.log(`✅ Added custom constraints`);
  }

  // Add current question
  parts.push(`\n\nCURRENT USER QUESTION: ${userInput}`);

  // Add final instructions if we have KB context
  if (kbContext) {
    parts.push(`\n\nCRITICAL INSTRUCTIONS:
1. Do NOT include phone numbers or email addresses in your response unless the user specifically asks "how do I contact you" or similar contact-focused questions
2. NEVER make up or invent ANY details including program names, services, or contact information - if not explicitly in the knowledge base, don't include it
3. You MAY include informational resource URLs that provide additional context (like program pages or resource links)
4. When you see a URL like https://example.com/page, include the FULL URL, not just "their website"
5. If the URL appears as a markdown link [text](url), preserve the markdown format

ABSOLUTELY CRITICAL - NO ACTION CTAs IN TEXT:
6. DO NOT EVER include action-oriented call-to-action links or phrases in your response
7. NEVER write things like "Join our [program] →", "Apply here →", "Check out...", "Want to learn more?", "Ready to get started?", "Sign up for..."
8. If the knowledge base contains action links (like "Join our Love Box training program →"), DO NOT INCLUDE THEM in your response
9. Remove ANY action-oriented links from your response - they will be provided as separate buttons
10. Your response should end with a warm conversational statement ONLY - no action prompts, no program enrollment links`);

    // ═══════════════════════════════════════════════════════════════
    // FORMATTING RULES - POSITIONED AT END FOR RECENCY BIAS
    // ═══════════════════════════════════════════════════════════════
    // The last thing the AI sees before generating - highest priority
    parts.push(buildEnhancedFormattingRules(config));
    console.log(`✅ Applied enhanced formatting contract with recency bias`);
  }

  const finalPrompt = parts.join('\n');
  console.log(`📝 Final prompt length: ${finalPrompt.length} chars`);
  console.log(`📝 Prompt version: ${PROMPT_VERSION}`);

  return finalPrompt;
}

/**
 * Preview prompt handler - returns the constructed prompt without calling Bedrock
 * This allows the Config Builder UI to preview how prompts will be built
 */
async function handlePromptPreview(event) {
  console.log('🔍 Prompt preview handler invoked');

  try {
    // Parse request
    const body = event.body ? JSON.parse(event.body) : event;
    const tenantHash = body.tenant_hash || '';
    const userInput = body.user_input || 'Hello, how can you help me?';
    const conversationHistory = body.conversation_history || [];
    const kbContext = body.kb_context || 'Sample knowledge base context about our services...';

    if (!tenantHash) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ error: 'Missing tenant_hash' })
      };
    }

    // Load config
    const config = await loadConfig(tenantHash);
    if (!config) {
      return {
        statusCode: 404,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ error: 'Config not found for tenant' })
      };
    }

    // Build the prompt
    const prompt = buildPrompt(
      userInput,
      kbContext,
      config.tone_prompt,
      conversationHistory,
      config
    );

    // Return preview data
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
      },
      body: JSON.stringify({
        prompt_version: PROMPT_VERSION,
        tenant_hash: tenantHash,
        tenant_id: config.tenant_id,
        has_custom_instructions: !!config.bedrock_instructions,
        bedrock_instructions: config.bedrock_instructions || null,
        prompt_length: prompt.length,
        prompt: prompt,
        metadata: {
          role_instructions_source: config.bedrock_instructions ? 'custom' : 'default',
          fallback_message_source: config.bedrock_instructions?.fallback_message ? 'custom' : 'default',
          has_custom_constraints: config.bedrock_instructions?.custom_constraints?.length > 0,
          formatting_preferences: config.bedrock_instructions?.formatting_preferences || DEFAULT_BEDROCK_INSTRUCTIONS.formatting_preferences
        }
      }, null, 2)
    };

  } catch (error) {
    console.error('❌ Preview error:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({
        error: error.message,
        stack: error.stack
      })
    };
  }
}

/**
 * Main streaming handler - uses true streaming if available, falls back to buffered
 */
const streamingHandler = async (event, responseStream, context) => {
  console.log('🌊 True streaming handler invoked');
  
  // Handle OPTIONS requests - Function URLs handle CORS automatically when configured
  if (event.httpMethod === 'OPTIONS' || event.requestContext?.http?.method === 'OPTIONS') {
    // Don't write empty string, just end the stream
    responseStream.end();
    return;
  }
  
  // Track if stream has ended to prevent write-after-end errors
  let streamEnded = false;
  
  // Buffer for complete Q&A logging - builds in parallel without blocking
  let responseBuffer = '';
  let questionBuffer = '';
  
  // For Lambda Function URL streaming, we write the SSE response directly
  const write = (data) => {
    if (!streamEnded) {
      responseStream.write(data);
    }
  };
  
  // Send prelude to open the pipe immediately
  write(':ok\n\n');
  // Send a tiny data frame to force early paint in some UAs/proxies
  write('data: {"type":"start"}\n\n');
  
  const startTime = Date.now();
  let heartbeatInterval;
  
  try {
    // Parse request - handle both direct invocation and Function URL
    console.log('📥 Event type:', typeof event);
    console.log('📥 Event keys:', Object.keys(event));
    
    // For direct invocation, event IS the body. For Function URL, event.body contains the JSON string
    const body = event.body ? JSON.parse(event.body) : event;
    console.log('📥 Parsed body:', JSON.stringify(body).substring(0, 200));
    
    const tenantHash = body.tenant_hash || '';
    const sessionId = body.session_id || 'default';
    const userInput = body.user_input || '';
    
    if (!tenantHash || !userInput) {
      const error = !tenantHash ? 'Missing tenant_hash' : 'Missing user_input';
      write(`data: {"type": "error", "error": "${error}"}\n\n`);
      write('data: [DONE]\n\n');
      streamEnded = true;
      responseStream.end();
      return;
    }
    
    // Capture the question for logging
    questionBuffer = userInput;
    
    // Extract conversation history from the request
    const conversationHistory = body.conversation_history || 
                               body.conversation_context?.recentMessages || 
                               [];
    
    console.log(`📝 Processing: ${tenantHash.substring(0,8)}... / ${sessionId.substring(0,12)}...`);
    console.log(`💬 Conversation history: ${conversationHistory.length} messages`);
    
    // Start heartbeat to keep connection alive
    heartbeatInterval = setInterval(() => {
      // Use a data frame rather than a comment; comments can be buffered by some intermediaries
      write('data: {"type":"heartbeat"}\n\n');
      console.log('💓 Heartbeat sent');
    }, 2000);

    // Load config
    let config = await loadConfig(tenantHash);
    if (!config) {
      config = {
        model_id: DEFAULT_MODEL_ID,
        streaming: { max_tokens: DEFAULT_MAX_TOKENS, temperature: DEFAULT_TEMPERATURE },
        tone_prompt: DEFAULT_TONE
      };
    }

    // Support bedrock_instructions_override for testing
    if (body.bedrock_instructions_override) {
      console.log('🔧 Applying bedrock_instructions_override from request');
      config.bedrock_instructions = body.bedrock_instructions_override;
    }

    // Check for form mode - bypass Bedrock for form field collection
    if (body.form_mode === true) {
      console.log('📝 Form mode detected - handling locally without Bedrock');
      try {
        const formResponse = await handleFormMode(body, config);

        // Send the form response as a single SSE event
        write(`data: ${JSON.stringify(formResponse)}\n\n`);
        write('data: [DONE]\n\n');

        // Clear heartbeat and end stream
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        streamEnded = true;
        responseStream.end();
        return;
      } catch (error) {
        console.error('Form mode error:', error);
        write(`data: {"type": "error", "error": "Form processing failed: ${error.message}"}\n\n`);
        write('data: [DONE]\n\n');
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // Get KB context
    const kbContext = await retrieveKB(userInput, config);
    const prompt = buildPrompt(userInput, kbContext, config.tone_prompt, conversationHistory, config);

    // Prepare Bedrock request - use config model or default
    const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    const maxTokens = config.streaming?.max_tokens || DEFAULT_MAX_TOKENS;
    const temperature = config.streaming?.temperature || DEFAULT_TEMPERATURE;
    
    console.log(`🚀 Invoking Bedrock with model: ${modelId}`);
    
    const command = new InvokeModelWithResponseStreamCommand({
      modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: maxTokens,
        temperature: temperature
      })
    });
    
    const response = await bedrock.send(command);
    
    let firstTokenTime = null;
    let tokenCount = 0;
    
    // Stream the response - NO BUFFERING!
    for await (const event of response.body) {
      if (event.chunk?.bytes) {
        const chunkData = JSON.parse(new TextDecoder().decode(event.chunk.bytes));
        
        if (chunkData.type === 'content_block_start') {
          // Nudge client: ensure at least one data frame precedes first text delta
          write('data: {"type":"stream_start"}\n\n');
        } else if (chunkData.type === 'content_block_delta') {
          const delta = chunkData.delta;
          if (delta?.type === 'text_delta' && delta.text) {
            tokenCount++;
            if (!firstTokenTime) {
              firstTokenTime = Date.now() - startTime;
              write(`: x-first-token-ms=${firstTokenTime}\n\n`);
              console.log(`⚡ First token in ${firstTokenTime}ms`);
            }
            
            // Stream to client immediately - NO DELAY
            const sseData = JSON.stringify({
              type: 'text',
              content: delta.text,
              session_id: sessionId
            });
            write(`data: ${sseData}\n\n`);
            
            // Also append to buffer in parallel (microseconds, no blocking)
            responseBuffer += delta.text;
          }
        } else if (chunkData.type === 'message_stop') {
          console.log('✅ Bedrock stream complete');
          break;
        }
      }
    }
    
    // Send completion metadata
    const totalTime = Date.now() - startTime;
    write(`: x-total-tokens=${tokenCount}\n`);
    write(`: x-total-time-ms=${totalTime}\n`);
    console.log(`✅ Complete - ${tokenCount} tokens in ${totalTime}ms`);
    
    // Log complete Q&A pair AFTER streaming is done (no impact on user experience)
    if (questionBuffer && responseBuffer) {
      console.log('📝 Q&A Pair Captured:');
      console.log(`  Session: ${sessionId}`);
      console.log(`  Tenant: ${tenantHash.substring(0, 8)}...`);
      console.log(`  Question: "${questionBuffer.substring(0, 100)}${questionBuffer.length > 100 ? '...' : ''}"`);
      console.log(`  Answer: "${responseBuffer.substring(0, 200)}${responseBuffer.length > 200 ? '...' : ''}"`);
      console.log(`  Full Q Length: ${questionBuffer.length} chars`);
      console.log(`  Full A Length: ${responseBuffer.length} chars`);
      
      // Log full Q&A in structured format for analytics
      console.log(JSON.stringify({
        type: 'QA_COMPLETE',
        timestamp: new Date().toISOString(),
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || null,  // Add tenant_id from config
        conversation_id: body.conversation_id || sessionId,  // Add conversation_id
        question: questionBuffer,
        answer: responseBuffer,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: responseBuffer.length
        }
      }));
    }

    // Enhance response with CTAs after streaming is complete
    try {
      const { enhanceResponse } = require('./response_enhancer');

      // Extract routing metadata for 3-tier explicit routing (PRD: Action Chips)
      const routingMetadata = body.routing_metadata || {};

      const enhancedData = await enhanceResponse(
        responseBuffer,  // The complete Bedrock response
        userInput,       // The user's message
        tenantHash,      // Tenant identifier
        body.session_context || {}, // Session context for form tracking
        routingMetadata  // Routing metadata for explicit routing (action chips, CTAs, fallback)
      );

      if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
        // Send CTAs as a separate SSE event
        const ctaData = JSON.stringify({
          type: 'cta_buttons',
          ctaButtons: enhancedData.ctaButtons,
          metadata: enhancedData.metadata,
          session_id: sessionId
        });
        write(`data: ${ctaData}\n\n`);
        console.log(`🎯 Sent ${enhancedData.ctaButtons.length} CTA buttons for branch: ${enhancedData.metadata?.branch_detected || 'form'}`);
      }
    } catch (enhanceError) {
      console.error('❌ CTA enhancement error:', enhanceError);
      // Don't fail the response if CTA enhancement fails
    }

  } catch (error) {
    console.error('❌ Stream error:', error);
    write(`data: {"type": "error", "error": "${error.message}"}\n\n`);
  } finally {
    // Clean up
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
    }
    
    // Send completion marker
    write('data: [DONE]\n\n');
    
    // End the stream
    streamEnded = true;
    responseStream.end();
  }

};

/**
 * Buffered handler for when streaming is not available
 */
const bufferedHandler = async (event, context) => {
  console.log('📡 Handler invoked');

  // Check for preview endpoint
  const queryParams = event.queryStringParameters || {};
  const body = event.body ? JSON.parse(event.body) : event;

  if (queryParams.action === 'preview' || body.action === 'preview') {
    console.log('🔍 Routing to preview handler');
    return await handlePromptPreview(event);
  }

  console.log('📡 Using buffered SSE handler for streaming');

  // Handle OPTIONS
  if (event.httpMethod === 'OPTIONS' || event.requestContext?.http?.method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
      },
      body: ''
    };
  }
  
  const startTime = Date.now();
  const chunks = [];
  let responseBuffer = '';
  let questionBuffer = '';
  
  // Add prelude
  chunks.push(':ok\n\n');
  
  try {
    // Parse request
    const body = event.body ? JSON.parse(event.body) : {};
    const tenantHash = body.tenant_hash || '';
    const sessionId = body.session_id || 'default';
    const userInput = body.user_input || '';
    
    // Capture the question
    questionBuffer = userInput;
    
    // Extract conversation history from the request
    const conversationHistory = body.conversation_history || 
                               body.conversation_context?.recentMessages || 
                               [];
    
    console.log(`💬 Conversation history: ${conversationHistory.length} messages`);
    
    if (!tenantHash || !userInput) {
      const error = !tenantHash ? 'Missing tenant_hash' : 'Missing user_input';
      chunks.push(`data: {"type": "error", "error": "${error}"}\n\n`);
      chunks.push('data: [DONE]\n\n');
      
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache, no-transform',
          'Access-Control-Allow-Origin': '*',
          'X-Accel-Buffering': 'no'
        },
        body: chunks.join('')
      };
    }
    
    console.log(`📝 Processing: ${tenantHash.substring(0,8)}... / ${sessionId.substring(0,12)}...`);
    
    // Load config
    let config = await loadConfig(tenantHash);
    if (!config) {
      config = {
        model_id: DEFAULT_MODEL_ID,
        streaming: { max_tokens: DEFAULT_MAX_TOKENS, temperature: DEFAULT_TEMPERATURE },
        tone_prompt: DEFAULT_TONE
      };
    }

    // Support bedrock_instructions_override for testing
    if (body.bedrock_instructions_override) {
      console.log('🔧 Applying bedrock_instructions_override from request');
      config.bedrock_instructions = body.bedrock_instructions_override;
    }

    // Get KB context
    const kbContext = await retrieveKB(userInput, config);
    const prompt = buildPrompt(userInput, kbContext, config.tone_prompt, conversationHistory, config);
    
    // Prepare Bedrock request - use config model or default
    const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    const maxTokens = config.streaming?.max_tokens || DEFAULT_MAX_TOKENS;
    const temperature = config.streaming?.temperature || DEFAULT_TEMPERATURE;
    
    // Invoke Bedrock
    const response = await bedrock.send(new InvokeModelWithResponseStreamCommand({
      modelId: modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: maxTokens,
        temperature: temperature
      })
    }));
    
    let firstTokenTime = null;
    let tokenCount = 0;
    
    // Process stream (buffered)
    for await (const event of response.body) {
      if (event.chunk?.bytes) {
        const chunkData = JSON.parse(new TextDecoder().decode(event.chunk.bytes));
        
        if (chunkData.type === 'content_block_delta') {
          const text = chunkData.delta?.text;
          if (text) {
            tokenCount++;
            
            if (!firstTokenTime) {
              firstTokenTime = Date.now() - startTime;
              chunks.push(`: x-first-token-ms=${firstTokenTime}\n\n`);
            }
            
            chunks.push(`data: {"type": "text", "content": ${JSON.stringify(text)}, "session_id": "${sessionId}"}\n\n`);
            responseBuffer += text;
          }
        } else if (chunkData.type === 'message_stop') {
          break;
        }
      }
    }
    
    // Add completion
    const totalTime = Date.now() - startTime;
    chunks.push(`: x-total-tokens=${tokenCount}\n`);
    chunks.push(`: x-total-time-ms=${totalTime}\n`);
    chunks.push('data: [DONE]\n\n');
    
    console.log(`✅ Complete - ${tokenCount} tokens in ${totalTime}ms`);
    
    // Log complete Q&A pair for analytics
    if (questionBuffer && responseBuffer) {
      console.log(JSON.stringify({
        type: 'QA_COMPLETE',
        timestamp: new Date().toISOString(),
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || null,  // Add tenant_id from config
        conversation_id: body.conversation_id || sessionId,  // Add conversation_id
        question: questionBuffer,
        answer: responseBuffer,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: responseBuffer.length
        }
      }));
    }

    // Enhance response with CTAs after generation is complete
    try {
      const { enhanceResponse } = require('./response_enhancer');

      // Extract routing metadata for 3-tier explicit routing (PRD: Action Chips)
      const routingMetadata = body.routing_metadata || {};

      const enhancedData = await enhanceResponse(
        responseBuffer,  // The complete Bedrock response
        userInput,       // The user's message
        tenantHash,      // Tenant identifier
        body.session_context || {}, // Session context for form tracking
        routingMetadata  // Routing metadata for explicit routing (action chips, CTAs, fallback)
      );

      if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
        // Add CTAs to the chunks array before completion
        const ctaData = JSON.stringify({
          type: 'cta_buttons',
          ctaButtons: enhancedData.ctaButtons,
          metadata: enhancedData.metadata,
          session_id: sessionId
        });
        // Insert CTAs before the [DONE] marker
        chunks.splice(chunks.length - 1, 0, `data: ${ctaData}\n\n`);
        console.log(`🎯 Added ${enhancedData.ctaButtons.length} CTA buttons for branch: ${enhancedData.metadata?.branch_detected || 'form'}`);
      }
    } catch (enhanceError) {
      console.error('❌ CTA enhancement error:', enhanceError);
      // Don't fail the response if CTA enhancement fails
    }

    // For Lambda Function URLs, we need to return the raw SSE content
    // The Function URL will handle setting the appropriate headers
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
        'X-Accel-Buffering': 'no'
      },
      body: chunks.join(''),
      isBase64Encoded: false
    };
    
  } catch (error) {
    console.error('Handler error:', error);
    
    chunks.push(`data: {"type": "error", "error": "${error.message}"}\n\n`);
    chunks.push('data: [DONE]\n\n');
    
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Access-Control-Allow-Origin': '*'
      },
      body: chunks.join('')
    };
  }
};

// Export the appropriate handler based on streaming support
exports.handler = streamifyResponse ? streamifyResponse(streamingHandler) : bufferedHandler;