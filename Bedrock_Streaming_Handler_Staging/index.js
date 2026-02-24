/**
 * Bedrock Streaming Handler - True Lambda Response Streaming
 * Uses awslambda.streamifyResponse for real SSE streaming
 * No JWT required - uses simple tenant_hash/session_id
 *
 * Version: v2.9.0
 * Deployed: 2025-12-05
 * Changes:
 *   - NEW: Engagement question feature - responses end with contextual follow-up questions
 *   - Prompts users to explore related topics (e.g., "Would you like to know the requirements?")
 *   - Works with existing loop prevention (Stage 1 only, skips Stage 2/3)
 *   - ENHANCED: Links and contact info now ALWAYS included regardless of formatting preferences
 *
 * Version: v2.7.0
 * Deployed: 2025-11-26
 * Changes:
 *   - NEW: Tier 4 AI-suggested branch routing for CTAs
 *   - Model suggests conversation branch based on response content
 *   - Branch hint injected via prompt, extracted via <!-- BRANCH: xxx --> tag
 *   - Enables CTAs for free-flow conversations without explicit routing
 *   - Falls back to fallback_branch if invalid branch suggested
 *
 * Version: v2.6.0
 * Deployed: 2025-11-26
 * Changes:
 *   - NEW: Intelligent follow-up question detection
 *   - Detects when user says "yes" to a question the bot asked
 *   - Extracts the topic from questions like "Would you like to learn more about X?"
 *   - Adds explicit directive telling model exactly what to answer
 *   - Should eliminate "I noticed you said yes..." responses
 *
 * Version: v2.5.1
 * Deployed: 2025-11-26
 * Changes:
 *   - FIXED: Formatting preference conflicts between response_style and detail_level
 *   - Added conflict resolution for structured_detailed + concise (was contradicting)
 *   - Added conflict resolution for structured_detailed + balanced (was tension)
 *   - All 9 combinations now have consistent, non-conflicting instructions
 *
 * Version: v2.5.0
 * Deployed: 2025-11-26
 * Changes:
 *   - UPGRADED MODEL: Claude 3.5 Haiku → Claude Haiku 4.5
 *   - Better instruction following for follow-up questions
 *   - 8x larger max output (64K vs 8K tokens)
 *   - Vision/image support added
 *
 * Version: v2.4.2
 * Deployed: 2025-11-26
 * Changes:
 *   - STRONGER follow-up question fix: moved instruction to CRITICAL section near end of prompt
 *   - Added explicit WRONG/RIGHT examples for the model to follow
 *   - Instructions now have higher priority via recency bias positioning
 *
 * Version: v2.4.1
 * Deployed: 2025-11-26
 * Changes:
 *   - Fixed follow-up question handling: bot now answers its own questions when user affirms
 *   - Added explicit instructions to prevent "I noticed you said yes" preambles
 *   - Improved context interpretation for affirmative responses
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

const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
const { BedrockAgentRuntimeClient, RetrieveCommand } = require('@aws-sdk/client-bedrock-agent-runtime');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand, SendMessageBatchCommand } = require('@aws-sdk/client-sqs');
const crypto = require('crypto');
const { enhanceResponse } = require('./response_enhancer');
const { handleFormMode } = require('./form_handler'); // Migrated to AWS SDK v3

// Default model configuration - single source of truth
// Upgraded to Haiku 4.5 for better instruction following (2025-11-26)
const DEFAULT_MODEL_ID = 'us.anthropic.claude-haiku-4-5-20251001-v1:0';
const DEFAULT_MAX_TOKENS = 1000;
const DEFAULT_TEMPERATURE = 0.2; // Slight variation for natural responses (v3.5: simpler prompt reduces confusion risk)
const DEFAULT_TONE = 'You are a helpful assistant.';

// Prompt version tracking for tenant customization
const PROMPT_VERSION = '3.5.0';

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

// Initialize AWS clients with configurable region
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const bedrock = new BedrockRuntimeClient({ region: AWS_REGION });
const bedrockAgent = new BedrockAgentRuntimeClient({ region: AWS_REGION });
const s3 = new S3Client({ region: AWS_REGION });
const sqs = new SQSClient({ region: AWS_REGION });

// Analytics SQS queue URL
const ANALYTICS_QUEUE_URL = process.env.ANALYTICS_QUEUE_URL || 'https://sqs.us-east-1.amazonaws.com/614056832592/picasso-analytics-events';

// In-memory cache with size limits to prevent memory exhaustion
const KB_CACHE = {};
const CONFIG_CACHE = {};
const CACHE_TTL = 300000; // 5 minutes
const MAX_CACHE_SIZE = 100; // Maximum entries per cache

// ═══════════════════════════════════════════════════════════════
// EVOLUTION v3.0: Streaming tag stripper for <thought> tags
// ═══════════════════════════════════════════════════════════════

/**
 * Creates a stateful thought-tag stripper for streaming contexts.
 * Handles partial tags across chunk boundaries.
 *
 * Usage:
 *   const stripper = createThoughtTagStripper();
 *   const visibleText = stripper.process(chunk);  // returns text with <thought>...</thought> removed
 *   const fullText = stripper.getFullBuffer();     // returns unprocessed original text
 *
 * States: NORMAL → MAYBE_OPEN → INSIDE → MAYBE_CLOSE → NORMAL
 */
function createThoughtTagStripper() {
  let state = 'NORMAL'; // NORMAL | MAYBE_OPEN | INSIDE | MAYBE_CLOSE
  let pendingBuffer = ''; // holds partial tag content while we determine if it's a tag
  let fullBuffer = ''; // complete unprocessed text for post-processing

  return {
    /**
     * Process a chunk of streaming text, stripping <thought>...</thought> content.
     * @param {string} chunk - Raw text chunk from Bedrock stream
     * @returns {string} - Text safe to send to the client (thought content removed)
     */
    process(chunk) {
      fullBuffer += chunk;
      let output = '';

      for (let i = 0; i < chunk.length; i++) {
        const char = chunk[i];

        switch (state) {
          case 'NORMAL':
            if (char === '<') {
              // Might be start of <thought>
              pendingBuffer = '<';
              state = 'MAYBE_OPEN';
            } else {
              output += char;
            }
            break;

          case 'MAYBE_OPEN':
            pendingBuffer += char;
            // Check if we're building toward "<thought>"
            if ('<thought>'.startsWith(pendingBuffer.toLowerCase())) {
              if (pendingBuffer.toLowerCase() === '<thought>') {
                // Full opening tag matched — enter INSIDE state, discard tag
                pendingBuffer = '';
                state = 'INSIDE';
              }
              // else: still accumulating, stay in MAYBE_OPEN
            } else {
              // Not a thought tag — flush pending buffer as normal text
              output += pendingBuffer;
              pendingBuffer = '';
              state = 'NORMAL';
            }
            break;

          case 'INSIDE':
            // Inside thought content — don't output anything
            if (char === '<') {
              // Might be start of </thought>
              pendingBuffer = '<';
              state = 'MAYBE_CLOSE';
            }
            // else: discard character (it's thought content)
            break;

          case 'MAYBE_CLOSE':
            pendingBuffer += char;
            if ('</thought>'.startsWith(pendingBuffer.toLowerCase())) {
              if (pendingBuffer.toLowerCase() === '</thought>') {
                // Full closing tag matched — return to NORMAL
                pendingBuffer = '';
                state = 'NORMAL';
              }
              // else: still accumulating, stay in MAYBE_CLOSE
            } else {
              // Not a closing tag — discard (still inside thought)
              pendingBuffer = '';
              state = 'INSIDE';
            }
            break;
        }
      }

      return output;
    },

    /**
     * Get the full unprocessed buffer (includes thought tags).
     * Used for QA_COMPLETE logging and post-processing.
     */
    getFullBuffer() {
      return fullBuffer;
    },

    /**
     * Check if currently inside a thought tag.
     * If true at end of stream, there's an unclosed thought tag.
     */
    isInsideThought() {
      return state === 'INSIDE' || state === 'MAYBE_CLOSE';
    },

    /**
     * Flush any pending buffer as output (call at end of stream).
     * Returns any incomplete tag text that wasn't a valid thought tag.
     */
    flush() {
      if (state === 'MAYBE_OPEN') {
        // Partial opening tag that never completed — it's just regular text
        const flushed = pendingBuffer;
        pendingBuffer = '';
        state = 'NORMAL';
        return flushed;
      }
      return '';
    }
  };
}

// ═══════════════════════════════════════════════════════════════
// SECURITY: Input sanitization to prevent prompt injection
// ═══════════════════════════════════════════════════════════════
const MAX_USER_INPUT_LENGTH = 4000; // Reasonable limit for chat messages

/**
 * Sanitize user input to prevent prompt injection attacks
 * Removes control characters and potential injection patterns while preserving normal text
 * @param {string} input - Raw user input
 * @returns {string} - Sanitized input safe for prompt inclusion
 */
function sanitizeUserInput(input) {
  if (!input || typeof input !== 'string') {
    return '';
  }

  // Truncate to max length
  let sanitized = input.slice(0, MAX_USER_INPUT_LENGTH);

  // Remove null bytes and other control characters (except newlines and tabs)
  sanitized = sanitized.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');

  // Neutralize potential prompt injection patterns by escaping them
  // These patterns could be used to inject new system instructions
  const injectionPatterns = [
    /\n\s*(SYSTEM|ASSISTANT|HUMAN|USER)\s*:/gi,
    /\n\s*<\|?(system|assistant|human|user|im_start|im_end)\|?>/gi,
    /\[\s*(INST|\/INST|SYS|\/SYS)\s*\]/gi
  ];

  for (const pattern of injectionPatterns) {
    sanitized = sanitized.replace(pattern, (match) => `[FILTERED: ${match.trim()}]`);
  }

  return sanitized.trim();
}

/**
 * Sanitize text for SMS messages - remove special characters that could cause issues
 * @param {string} text - Raw text
 * @returns {string} - SMS-safe text
 */
function sanitizeForSMS(text) {
  if (!text || typeof text !== 'string') {
    return '';
  }
  // Keep only alphanumeric, spaces, and basic punctuation
  return text.replace(/[^\w\s@.-]/g, '').slice(0, 50);
}

// Helper functions
function getCacheKey(text, prefix = '') {
  return `${prefix}:${crypto.createHash('sha256').update(text).digest('hex').slice(0, 32)}`;
}

function isCacheValid(entry) {
  return entry && (Date.now() - entry.timestamp < CACHE_TTL);
}

/**
 * Evict oldest entries if cache exceeds max size (LRU-style)
 * @param {Object} cache - Cache object to check
 * @param {number} maxSize - Maximum allowed entries
 */
function evictOldestCacheEntries(cache, maxSize = MAX_CACHE_SIZE) {
  const keys = Object.keys(cache);
  if (keys.length <= maxSize) return;

  // Sort by timestamp (oldest first) and remove oldest entries
  const sortedKeys = keys.sort((a, b) => (cache[a]?.timestamp || 0) - (cache[b]?.timestamp || 0));
  const toRemove = sortedKeys.slice(0, keys.length - maxSize);

  for (const key of toRemove) {
    delete cache[key];
  }

  if (toRemove.length > 0) {
    console.log(`🧹 Evicted ${toRemove.length} old cache entries`);
  }
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
        // Add tenant_id and tenant_hash to config for downstream use
        config.tenant_id = mapping.tenant_id;
        config.tenant_hash = tenantHash;
        CONFIG_CACHE[cacheKey] = { data: config, timestamp: Date.now() };
        evictOldestCacheEntries(CONFIG_CACHE); // Prevent memory exhaustion
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

/**
 * Build an enriched KB search query using conversation context.
 * For short/ambiguous inputs like "yes", "sure", "tell me more", "1",
 * we look at the last assistant message to understand what the user
 * is actually asking about and build a better search query.
 *
 * @param {string} userInput - Raw user input
 * @param {Array} conversationHistory - Recent conversation messages
 * @returns {string} - Enriched search query for KB retrieval
 */
function buildKBSearchQuery(userInput, conversationHistory) {
  const trimmed = userInput.trim().toLowerCase();
  const wordCount = userInput.trim().split(/\s+/).length;

  // Short/ambiguous patterns that need enrichment (affirmations, single words)
  const isAmbiguous = (
    wordCount <= 3 &&
    /^(yes|yeah|yep|sure|okay|ok|no|nah|1|2|3|4|tell me more|go on|please|thanks|thank you|absolutely|definitely|of course|why not|sounds good|let's do it|i'm interested|interested)$/i.test(trimmed)
  );

  // Topic-continuation queries: generic questions that implicitly refer to the active topic
  // e.g., "What are the requirements?" after discussing Love Box → "Love Box requirements"
  const topicContinuationWords = /\b(requirements?|cost|how much|time commitment|how long|process|steps|schedule|training|qualifications?|eligibility|apply|sign up|get started|involved)\b/i;
  const isTopicContinuation = !isAmbiguous && wordCount <= 8 && topicContinuationWords.test(trimmed);

  const needsEnrichment = isAmbiguous || isTopicContinuation;

  if (!needsEnrichment || !conversationHistory || conversationHistory.length === 0) {
    return userInput; // Normal query — use as-is
  }

  // Find the last assistant message to extract active topic
  const lastAssistant = [...conversationHistory]
    .reverse()
    .find(m => m.role === 'assistant' || m.role === 'bot');

  if (!lastAssistant) {
    return userInput;
  }

  const assistantText = (lastAssistant.content || lastAssistant.text || '').trim();
  if (!assistantText) {
    return userInput;
  }

  const sentences = assistantText.split(/[.!?]+/).map(s => s.trim()).filter(Boolean);

  // For topic-continuation queries, prepend the active topic to the user's query
  if (isTopicContinuation) {
    // Extract the main topic/program name from the last assistant response
    // Look for program names, proper nouns, or key phrases in the first 2 sentences
    const topicContext = sentences.slice(0, 2).join('. ');
    // Common program name patterns
    const programMatch = topicContext.match(/\b(Love Box|Dare to Dream|Angel Alliance|Foster Care|Discovery Session)\b/i);
    if (programMatch) {
      const enriched = `${programMatch[1]} ${userInput.trim()}`;
      console.log(`🔄 KB query enriched (topic continuation): "${userInput}" → "${enriched}"`);
      return enriched;
    }
    // Fallback: prepend first sentence as context
    if (sentences[0] && sentences[0].length > 10) {
      const enriched = `${sentences[0].slice(0, 100)} - ${userInput.trim()}`;
      console.log(`🔄 KB query enriched (context prefix): "${userInput}" → "${enriched.substring(0, 80)}..."`);
      return enriched;
    }
  }

  // For ambiguous inputs: Strategy 1 — extract topic from the assistant's last question
  const lastQuestion = [...sentences].reverse().find(s =>
    assistantText.includes(s + '?')
  );

  if (lastQuestion) {
    const enriched = lastQuestion
      .replace(/^(would you like to|do you want to|shall i|can i|should i|want to)\s*/i, '')
      .replace(/^(know|learn|hear|find out)\s*(more\s*)?(about\s*)?/i, '')
      .replace(/\?$/, '')
      .trim();

    if (enriched.length > 10) {
      console.log(`🔄 KB query enriched from question: "${userInput}" → "${enriched}"`);
      return enriched;
    }
  }

  // Strategy 2: Use the main topic from the last assistant message
  const topicSentences = sentences.slice(0, 2).join('. ');
  if (topicSentences.length > 20) {
    const enriched = topicSentences.slice(0, 200);
    console.log(`🔄 KB query enriched from topic: "${userInput}" → "${enriched.substring(0, 60)}..."`);
    return enriched;
  }

  return userInput;
}

async function retrieveKB(userInput, config, conversationHistory) {
  const kbId = config?.aws?.knowledge_base_id;
  console.log(`🔍 KB Retrieval - KB ID: ${kbId || 'NOT SET'}`);
  console.log(`🔍 User input: "${userInput.substring(0, 50)}..."`);

  if (!kbId) {
    console.log('⚠️ No KB ID found in config - returning empty context');
    return '';
  }

  // Enrich short/ambiguous queries with conversation context
  const searchQuery = buildKBSearchQuery(userInput, conversationHistory);

  try {
    const cacheKey = getCacheKey(searchQuery, `kb:${kbId}`);
    if (KB_CACHE[cacheKey] && isCacheValid(KB_CACHE[cacheKey])) {
      console.log(`✅ KB cache hit`);
      const cachedData = KB_CACHE[cacheKey].data;
      console.log(`📄 Cached KB context length: ${cachedData.length} chars`);
      return cachedData;
    }

    console.log(`📚 Retrieving from KB: ${kbId} (query: "${searchQuery.substring(0, 80)}...")`);
    const response = await bedrockAgent.send(new RetrieveCommand({
      knowledgeBaseId: kbId,
      retrievalQuery: { text: searchQuery },
      retrievalConfiguration: {
        vectorSearchConfiguration: { numberOfResults: 5 }
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
    evictOldestCacheEntries(KB_CACHE); // Prevent memory exhaustion
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

  // Detail level contract - with conflict resolution for structured_detailed style
  let lengthContract = '';
  let lengthChecklist = '';
  const isStructuredStyle = prefs.response_style === 'structured_detailed';

  if (prefs.detail_level === 'concise') {
    if (isStructuredStyle) {
      // CONFLICT RESOLUTION: structured_detailed + concise
      // Keep it short but allow minimal structure
      lengthContract = `
🔒 LENGTH CONTRACT - CONCISE STRUCTURED:
Your response WILL be brief (3-4 sentences worth of content) but WITH structure.
Use ONE heading and 2-4 bullet points maximum.

EXAMPLE:
✅ "Dare to Dream supports foster youth ages 11-22.

**Key Features:**
- Two age tracks: Jr. (11-14) and Senior (15-22)
- Life skills training and academic support
- Career preparation guidance"

This format keeps content brief while maintaining structure.`;

      lengthChecklist = `
PRE-GENERATION LENGTH CHECK - Concise Structured:
□ Brief total content (equivalent to 3-4 sentences)
□ Maximum 1 heading
□ Maximum 4 bullet points
□ No long paragraphs`;
    } else {
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
    }

  } else if (prefs.detail_level === 'balanced') {
    if (isStructuredStyle) {
      // CONFLICT RESOLUTION: structured_detailed + balanced
      // Medium length WITH required structure
      lengthContract = `
🔒 LENGTH CONTRACT - BALANCED STRUCTURED:
Your response WILL be medium length (4-6 sentences worth of content) WITH structure.
Use 1-2 headings and organized bullet points.

EXAMPLE:
✅ "Dare to Dream is our mentorship program supporting foster youth.

**Program Tracks:**
- Dare to Dream Jr. (ages 11-14): Focus on confidence and academic skills
- Dare to Dream (ages 15-22): Career preparation and independent living

**What We Provide:**
- Life skills training
- Academic support and tutoring
- Career guidance

Our goal is helping youth build independence and achieve their potential."`;

      lengthChecklist = `
PRE-GENERATION LENGTH CHECK - Balanced Structured:
□ Medium content length (4-6 sentences equivalent)
□ 1-2 section headings
□ Organized bullet points
□ Clear visual structure`;
    } else {
      lengthContract = `
🔒 LENGTH CONTRACT - BALANCED:
Your response WILL be 4-6 sentences. Count before responding.
You MAY use 1-2 short bullet points if helpful, but paragraph form is fine.

EXAMPLE:
✅ "Dare to Dream is our mentorship program supporting foster youth ages 11-22. We offer two tracks: Dare to Dream Jr. (ages 11-14) and Dare to Dream (ages 15-22). The program provides life skills and academic support, along with career preparation. Our goal is to help youth develop confidence and prepare for independent adulthood." (4 sentences)`;

      lengthChecklist = `
PRE-GENERATION LENGTH CHECK - Balanced:
□ Count sentences: Must be 4-6
□ Bullet points optional (0-2)
□ Not too short, not too long`;
    }

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
    let result = '\n\nCUSTOM INSTRUCTIONS:\n' + instructions.custom_constraints.map(c => `- ${c}`).join('\n');

    // When DYNAMIC_ACTIONS is on, override the "always ask a follow-up" pattern
    // to allow progression toward action after deep exploration
    if (isFeatureEnabled('DYNAMIC_ACTIONS', config)) {
      result += '\n- IMPORTANT OVERRIDE: After 3+ exchanges on the same topic, your follow-up should invite the user to TAKE ACTION (apply, sign up, get started) rather than asking another informational question. The system will provide action buttons — your job is to naturally close the loop.';
    }

    return result;
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
 * CRITICAL: Links and contact info ALWAYS included regardless of response style/detail level
 */
function getLockedUrlHandling() {
  return `🔒 MANDATORY: LINKS AND CONTACT INFORMATION (OVERRIDES ALL OTHER FORMATTING RULES)

This rule OVERRIDES response style and detail level settings. Even in concise mode, you MUST include:

ALWAYS INCLUDE (regardless of response length or style):
- ALL URLs and links from the knowledge base - use markdown format [text](url)
- ALL email addresses (e.g., erika@nationalangels.org)
- ALL phone numbers (e.g., (512) 521-3165)
- ALL contact names and titles (e.g., "Contact Erika, Partnership Director")
- ALL call-to-action links for forms, applications, or next steps

FORMAT REQUIREMENTS:
- PRESERVE ALL MARKDOWN FORMATTING: If you see [text](url) keep it as [text](url), not plain text
- Do not modify, shorten, or reformat any URLs, emails, or phone numbers
- When the knowledge base mentions "contact us at..." or "apply at..." include the FULL contact method

EXAMPLES:
❌ WRONG (even in concise mode): "For partnerships, reach out to our team."
✅ CORRECT (even in concise mode): "For partnerships, contact Erika at erika@nationalangels.org."

❌ WRONG (even in concise mode): "Visit our website to learn more."
✅ CORRECT (even in concise mode): "Learn more at [nationalangels.org](https://www.nationalangels.org)."

❌ WRONG: Omitting a relevant link to shorten the response
✅ CORRECT: Include the link even if it makes the response slightly longer

NOTE: If including all relevant links/contacts pushes you past the sentence count limit, that is ACCEPTABLE.
Links and contact info are MORE important than strict adherence to length constraints.`;
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
 * LOCKED: Engagement question - end responses with contextual follow-up
 */
function getLockedEngagementQuestion() {
  return `ENGAGEMENT QUESTION - END EACH RESPONSE WITH A CONTEXTUAL FOLLOW-UP:

At the END of your response, include a brief follow-up question that:
1. Relates directly to the topic you just discussed
2. Offers a natural next step or deeper exploration
3. Is specific, not generic

EXAMPLES OF GOOD ENGAGEMENT QUESTIONS:
- After explaining partnerships: "Would you like to know more about the different partnership levels?"
- After describing Love Box: "Would you like to know the requirements to become a Love Box volunteer?"
- After discussing mentorship: "Would you like to learn about the application process for mentors?"
- After explaining donations: "Would you like to know about our monthly giving program?"
- After describing a chapter: "Would you like to know how to get involved with your local chapter?"

EXAMPLES OF BAD ENGAGEMENT QUESTIONS (avoid these):
- ❌ "Is there anything else I can help you with?" (too generic - save for Stage 3)
- ❌ "Do you have any other questions?" (too generic)
- ❌ "Would you like more information?" (too vague - about what?)
- ❌ "Can I help you with something else?" (off-topic)

WHEN TO SKIP THE ENGAGEMENT QUESTION:
- Stage 2/3: When user has already confirmed interest (e.g., said "yes" to your previous question)
- Contact requests: When user asked "how do I contact you" - just provide contact info
- Simple confirmations: When you're providing a direct resource link as the final step

The engagement question should feel like a natural extension of the conversation, inviting the user to learn more about a specific aspect of what you just discussed.`;
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

CRITICAL - ANSWERING YOUR OWN FOLLOW-UP QUESTIONS:
If your previous message ended with a question (like "Would you like to learn more about X?" or "Shall I explain the requirements?") and the user responds affirmatively:
1. ANSWER THAT QUESTION DIRECTLY - provide the information you offered
2. DO NOT say "I noticed you said yes" or "Since you're interested" - just provide the answer
3. DO NOT repeat information you already gave - provide NEW details about what you asked
4. Treat their "yes" as if they had asked the question themselves

Example of CORRECT behavior:
- You asked: "Would you like to learn about the requirements to become a mentor?"
- User says: "yes"
- Your response: "To become a mentor with Dare to Dream, you'll need to be at least 21 years old, pass a background check, and commit to meeting with your mentee at least twice per month..." ✅

Example of WRONG behavior:
- You asked: "Would you like to learn about the requirements to become a mentor?"
- User says: "yes"
- Your response: "I noticed you said yes. Since we were discussing Dare to Dream, here's some more context about our mentorship program..." ❌

Examples of how to interpret short responses:
- If user says "yes" after you asked about submitting a request, they mean "yes, I want to proceed with that"
- If user says "tell me more" after discussing a specific program or service, they want more details about that same topic
- If user says "I'm interested" after mentioning an opportunity, they're interested in that specific opportunity
- If user says "no thanks" after you offered information, acknowledge and ask what else they need
- If user says "sure" or "okay", they're agreeing to whatever was just proposed

IMPORTANT: Short responses are ALWAYS about continuing the previous conversation topic. Never treat them as new, unrelated questions.`;
}

// ═══════════════════════════════════════════════════════════════
// FEATURE FLAGS - Per-tenant and environment variable resolution
// ═══════════════════════════════════════════════════════════════

/**
 * Check if a feature flag is enabled
 * Resolution order: tenant config → environment variable → default (off)
 */
function isFeatureEnabled(flagName, config) {
  // 1. Tenant config override (highest priority)
  if (config?.feature_flags?.[flagName] !== undefined) {
    return config.feature_flags[flagName];
  }
  // 2. Environment variable (PICASSO_DYNAMIC_ACTIONS, etc.)
  const envVar = process.env[`PICASSO_${flagName}`];
  if (envVar !== undefined) {
    return envVar === 'true';
  }
  // 3. Default: off
  return false;
}

// ═══════════════════════════════════════════════════════════════
// EVOLUTION v3.0: Internal Monologue + Anti-Robot Guardrails
// ═══════════════════════════════════════════════════════════════

/**
 * Build the internal monologue (thought layer) prompt section.
 * Forces the model to reason about intent, context, and transitions
 * BEFORE generating its response. <thought> tags are stripped server-side.
 */
function buildThoughtLayerSection() {
  return `
INTERNAL REASONING (required before every response):
Before writing your response, you MUST include a brief reasoning step inside <thought> tags.
This is hidden from the user and helps you plan a natural, human-like response.

In your <thought> tags, work through these 5 steps:
1. WHAT DID THE USER ASK? What is the user actually asking or feeling?
2. WHAT DID I ALREADY TELL THEM? List the key facts I've already covered in this conversation (time, cost, requirements, etc.)
3. WHAT'S NEW TO SHARE? What information would actually be NEW and useful — not a repeat?
4. HOW DEEP ARE WE? How many turns on this topic? If 3+, I should guide toward action, not more facts.
5. CHIP PLAN: What question will I end with? The chips must be the 2-3 most likely answers the user would give to that question.

If the user's input is very short ("yes", "sure", "tell me more"):
- Do NOT recap or acknowledge the "yes"
- Continue directly as if you're finishing a thought you started

CONVERSATION PROGRESSION:
- After 2-3 exchanges on the same topic, shift from informing to inviting action.
- Instead of "Would you like to know more about X?", say "Ready to take the next step?" or "Want me to help you get started?"
- NEVER repeat information you already provided.

Example:
<thought>1. User asked about Love Box requirements. 2. I already told them: one-year commitment, 2-3 hours monthly, $75/$25 budget, background check. 3. NEW info I can share: what the training covers, how matching works. 4. This is turn 4 on Love Box — time to nudge toward action. 5. CHIP PLAN: My response will cover training briefly then invite them to apply. Chips should NOT be about time/cost/requirements (already covered). Good chips: "How do I apply?" or "Tell me about Dare to Dream".</thought>`;
}

/**
 * Build anti-robot guardrails prompt section.
 * Explicit rules for human-like interaction patterns.
 */
function buildAntiRobotSection() {
  return `
HUMAN INTERACTION RULES:
- NEVER reference source materials, databases, or files. Treat all knowledge as your own experience.
- NEVER use phrases like "Based on our previous conversation", "According to the information", "I found that", or "As mentioned earlier"
- Use natural conversational transitions: "Actually, there's another part to this...", "You know, one of the things people love most is...", "I'm glad you asked about that..."
- When the user shows excitement or interest, match their energy. Don't flatten emotion with clinical language.
- Treat every user input as a continuation of the conversation, not a new search query.
- If you just shared a fact, your next response should give the "why" or "how" behind it — not repeat it.
- End with engagement that invites deeper exploration, NOT generic "Is there anything else I can help with?"`;
}

// ═══════════════════════════════════════════════════════════════
// EVOLUTION v3.0: Dynamic Action Context + Chips
// ═══════════════════════════════════════════════════════════════

/**
 * Build the dynamic action context section for the prompt.
 * Tells the model what forms and links are available so it can generate
 * contextual CTAs via <!-- ACTIONS: [...] --> tags.
 *
 * @param {Object} config - Tenant configuration
 * @param {Array} completedForms - List of completed form program IDs
 * @returns {string} - Prompt section for action generation, or empty string
 */
function buildActionContextSection(config, completedForms = []) {
  if (!isFeatureEnabled('DYNAMIC_ACTIONS', config)) {
    return '';
  }

  const parts = [];
  parts.push(`
DYNAMIC RESPONSE ACTIONS:
After your response, if contextual next steps would help the user, append an HTML comment
with 1-3 action buttons. These are HIDDEN from the user and parsed by the system.`);

  // Build available actions from cta_definitions (ai_available CTAs)
  const ctaDefinitions = config.cta_definitions || {};
  const formEntries = [];
  const linkEntries = [];
  const infoCtas = [];

  for (const [ctaId, cta] of Object.entries(ctaDefinitions)) {
    if (cta.ai_available !== true) continue;

    if (cta.action === 'start_form' && cta.formId) {
      if (!completedForms.includes(cta.formId)) {
        formEntries.push(`- "${cta.label || ctaId}" (formId: "${cta.formId}")`);
      }
    } else if (cta.action === 'external_link' && cta.url) {
      linkEntries.push(`- "${cta.label || ctaId}": ${cta.url}`);
    } else if (cta.action === 'show_info') {
      infoCtas.push(`- "${cta.label || ctaId}": Guided exploration with branch CTAs`);
    }
  }

  // Legacy fallback: if no ai_available CTAs, try available_actions
  if (formEntries.length === 0 && linkEntries.length === 0 && infoCtas.length === 0 && config.available_actions) {
    console.log('[v3.5] buildActionContextSection: falling back to available_actions');
    const availableActions = config.available_actions;
    if (availableActions.forms) {
      for (const [formId, formInfo] of Object.entries(availableActions.forms)) {
        formEntries.push(`- formId "${formId}": ${formInfo.description || formInfo.label || formId}`);
      }
    }
    if (availableActions.links) {
      for (const [linkId, linkInfo] of Object.entries(availableActions.links)) {
        linkEntries.push(`- "${linkInfo.label || linkId}": ${linkInfo.url}`);
      }
    }
  }

  if (formEntries.length > 0) {
    parts.push(`
AVAILABLE FORMS (use action "start_form"):
${formEntries.join('\n')}`);
  }

  if (linkEntries.length > 0) {
    parts.push(`
AVAILABLE LINKS (use action "external_link"):
${linkEntries.join('\n')}`);
  }

  if (infoCtas.length > 0) {
    parts.push(`
AVAILABLE INFO PAGES (use action "show_info"):
${infoCtas.join('\n')}`);
  }

  // Completed forms
  if (completedForms.length > 0) {
    parts.push(`
USER HAS COMPLETED: [${completedForms.join(', ')}] — do NOT suggest these forms.`);
  }

  // Format instructions
  parts.push(`
FORMAT (append at the very end of your response, after all visible text):
<!-- ACTIONS: [{"label":"Apply Now","action":"start_form","formId":"lb_apply"},{"label":"Learn About Love Box","action":"show_info","prompt":"...","target_branch":"lovebox_info"}] -->

RULES:
- Only suggest actions that are contextually relevant to THIS response
- Do NOT suggest forms the user has already completed (listed above)
- Include 0-3 actions (0 is fine — don't force actions when none are relevant)
- Always include formId for "start_form" actions (must match available forms above)
- Always include url for "external_link" actions
- For "show_info" actions, include prompt text and target_branch if applicable

PROGRESSION RULES:
- After 3+ turns on the same topic, prioritize ACTION buttons over info queries
- If the user has been exploring a program in depth, suggest "Get Started" or "Schedule a Call" type actions, not more "Learn About..." buttons
- Vary your action labels — don't repeat the same button across multiple responses`);

  const totalEntries = formEntries.length + linkEntries.length + infoCtas.length;
  console.log(`✅ Built dynamic action context section (${formEntries.length} forms, ${linkEntries.length} links, ${infoCtas.length} info)`);
  return parts.join('\n');
}

/**
 * Build the suggested chips section for the prompt.
 * Instructs the model to generate follow-up question chips.
 *
 * @param {Object} config - Tenant configuration
 * @returns {string} - Prompt section for chip generation, or empty string
 */
function buildChipSection(config, turnCount = 0) {
  if (!isFeatureEnabled('DYNAMIC_CHIPS', config)) {
    return '';
  }

  const maxChips = config?.suggested_chips?.max_chips || 3;

  return `
CHIPS — suggest up to ${maxChips} things the user would most likely say next.
Format: <!-- CHIPS: ["option 1", "option 2"] -->
- Each under 50 chars, written from the user's perspective
- Never repeat or rephrase what the user just asked — chips move forward, not backward
- If your response covers multiple topics, spread chips across them — don't favor just one
- Fewer is better than filler. Only include chips that are genuinely useful.`;
}

/**
 * Build guidance module section based on conversation context.
 * Injects topic-specific tone/behavior instructions when relevant.
 *
 * @param {Object} config - Tenant configuration
 * @param {Array} conversationHistory - Recent conversation messages
 * @returns {string} - Guidance section, or empty string
 */
function buildGuidanceSection(config, conversationHistory) {
  if (!isFeatureEnabled('GUIDANCE_MODULES', config)) {
    return '';
  }

  const guidanceModules = config.guidance_modules;
  if (!guidanceModules || Object.keys(guidanceModules).length === 0) {
    return '';
  }

  // Simple topic detection from recent messages (last 3)
  const recentText = (conversationHistory || [])
    .slice(-3)
    .map(m => (m.content || m.text || '').toLowerCase())
    .join(' ');

  const matchedModules = [];
  for (const [key, module] of Object.entries(guidanceModules)) {
    if (module.enabled === false) continue;
    // Check if the topic keyword appears in recent conversation
    if (recentText.includes(key.toLowerCase())) {
      matchedModules.push(module);
    }
  }

  if (matchedModules.length === 0) {
    return '';
  }

  const sections = matchedModules.map(m =>
    `- ${m.title || 'Guidance'}: ${m.content}`
  ).join('\n');

  console.log(`✅ Injected ${matchedModules.length} guidance module(s)`);
  return `\nTOPIC-SPECIFIC GUIDANCE (apply to your response):\n${sections}`;
}

/**
 * Build branch prompt section for Tier 4 AI-suggested routing
 * DEPRECATED: Replaced by buildActionContextSection() in v3.0
 * Kept for backward compatibility when DYNAMIC_ACTIONS feature flag is off.
 *
 * @param {Object} config - Tenant configuration
 * @returns {string} - Prompt section for branch suggestions, or empty string if no branches
 */
function buildBranchPromptSection(config) {
  const ctaSettings = config?.cta_settings || {};

  // If no fallback_branch is configured, user wants explicit routing only (Tier 1-3)
  // This respects the Config Builder's "None (no CTAs shown when no match)" setting
  if (!ctaSettings.fallback_branch) {
    console.log('ℹ️ No fallback_branch configured - Tier 4 AI routing disabled (explicit routing only)');
    return '';
  }

  const branches = config?.conversation_branches || {};
  const ctaDefinitions = config?.cta_definitions || {};

  // Filter out branches that shouldn't be suggested
  const suggestibleBranches = Object.entries(branches).filter(([name, branch]) => {
    // Exclude 'fallback' branch - it's for when no branch matches
    if (name === 'fallback') return false;
    // Only include branches that have CTAs defined
    if (!branch.available_ctas) return false;
    return true;
  });

  if (suggestibleBranches.length === 0) {
    console.log('ℹ️ No suggestible branches found - skipping Tier 4 prompt injection');
    return '';
  }

  // Build branch descriptions - prefer explicit description, fallback to CTA labels
  const branchDescriptions = suggestibleBranches.map(([branchName, branch]) => {
    // Use explicit description if provided
    if (branch.description && branch.description.trim()) {
      return `- "${branchName}": ${branch.description.trim()}`;
    }

    // Fallback: build description from CTA labels
    const ctaLabels = [];

    // Get primary CTA label
    const primaryId = branch.available_ctas?.primary;
    if (primaryId && ctaDefinitions[primaryId]) {
      const primaryCta = ctaDefinitions[primaryId];
      ctaLabels.push(primaryCta.label || primaryCta.text || primaryId);
    }

    // Get secondary CTA labels
    const secondaryIds = branch.available_ctas?.secondary || [];
    for (const ctaId of secondaryIds) {
      if (ctaDefinitions[ctaId]) {
        const cta = ctaDefinitions[ctaId];
        ctaLabels.push(cta.label || cta.text || ctaId);
      }
    }

    // Format: "branch_name": CTA1, CTA2, CTA3
    const fallbackDescription = ctaLabels.slice(0, 3).join(', ');
    return `- "${branchName}": ${fallbackDescription || 'General actions'}`;
  });

  console.log(`✅ Built branch prompt section with ${suggestibleBranches.length} branches`);

  return `

CONVERSATION TOPIC ROUTING (Tier 4):
When your response clearly relates to one of these topics, append a branch tag at the very end:

${branchDescriptions.join('\n')}

INSTRUCTIONS:
- If your response discusses a specific program or topic that matches a branch, append the tag
- Format: <!-- BRANCH: branch_name -->
- Example: If discussing Love Box volunteering, end with <!-- BRANCH: volunteer_lovebox -->
- Only suggest ONE branch per response
- If no branch clearly applies, do NOT add any tag
- The tag will be stripped from the visible response - it's for internal routing only
`;
}

/**
 * Detects if the user is responding affirmatively to a question the assistant asked
 * Returns the extracted question if detected, null otherwise
 */
function detectFollowUpQuestionResponse(userInput, conversationHistory) {
  // Check if user input is a short affirmative response
  const affirmativePatterns = /^(yes|yeah|yep|sure|okay|ok|please|definitely|absolutely|yea|yup|go ahead|tell me|i'd like that|sounds good|please do|yes please)\.?!?$/i;
  const isAffirmative = affirmativePatterns.test(userInput.trim());

  if (!isAffirmative || !conversationHistory || conversationHistory.length === 0) {
    return null;
  }

  // Find the last assistant message
  let lastAssistantMessage = null;
  for (let i = conversationHistory.length - 1; i >= 0; i--) {
    const msg = conversationHistory[i];
    if (msg.role === 'assistant') {
      lastAssistantMessage = msg.content || msg.text || '';
      break;
    }
  }

  if (!lastAssistantMessage) {
    return null;
  }

  // Check if the assistant's last message ended with a question
  // Look for question patterns at the end of the message
  const questionPatterns = [
    /Would you like (?:to |me to )?(?:learn |know |hear )?(more )?about ([^?]+)\?/i,
    /Would you like (?:to |me to )?([^?]+)\?/i,
    /Shall I (?:tell you |explain |share )?(more )?about ([^?]+)\?/i,
    /Do you want (?:to |me to )?(?:learn |know |hear )?(more )?about ([^?]+)\?/i,
    /Can I (?:tell you |share |explain )?(more )?about ([^?]+)\?/i,
    /Want (?:to |me to )?(?:learn |know |hear )?(more )?about ([^?]+)\?/i,
    /Interested in (?:learning |knowing |hearing )?(more )?about ([^?]+)\?/i,
  ];

  for (const pattern of questionPatterns) {
    const match = lastAssistantMessage.match(pattern);
    if (match) {
      // Extract the topic from the question
      const topic = match[match.length - 1] || match[1];
      if (topic) {
        console.log(`🔍 Detected follow-up question response. Topic: "${topic.trim()}"`);
        return {
          originalQuestion: match[0],
          topic: topic.trim(),
          fullAssistantMessage: lastAssistantMessage
        };
      }
    }
  }

  // Check for general question ending
  if (lastAssistantMessage.trim().endsWith('?')) {
    // Extract the last sentence that ends with ?
    const sentences = lastAssistantMessage.split(/[.!]\s+/);
    const lastSentence = sentences[sentences.length - 1];
    if (lastSentence && lastSentence.includes('?')) {
      console.log(`🔍 Detected affirmative to question: "${lastSentence.trim()}"`);
      return {
        originalQuestion: lastSentence.trim(),
        topic: null,
        fullAssistantMessage: lastAssistantMessage
      };
    }
  }

  return null;
}

// ═══════════════════════════════════════════════════════════════
// EVOLUTION v3.5: W5 Framework + Tag & Map Hybrid
//
// DESIGN PRINCIPLES:
// 1. W5 thought framework (Who, What, When, Where, Why) replaces rigid rules
// 2. Tag & Map: Haiku picks simple IDs, code maps to full action JSON
// 3. Sanitized persona: keeps tenant personality, strips link/CTA instructions
// 4. All action IDs are predefined in config — no freeform generation
// 5. CHIPS = AI-creative conversation flow, NEXT = deterministic hard actions
// ═══════════════════════════════════════════════════════════════

/**
 * Sanitize tone_prompt to remove sentences that conflict with button-based CTAs.
 * Keeps the tenant's personality and warmth, strips instructions about inline links/CTAs.
 */
function sanitizeTonePrompt(tonePrompt) {
  if (!tonePrompt) return '';
  const blocked = ['inline link', 'calls to action', 'contact information, or calls', 'include relevant'];
  return tonePrompt
    .split('.')
    .filter(sentence => {
      const lower = sentence.toLowerCase();
      return !blocked.some(phrase => lower.includes(phrase));
    })
    .join('.')
    .trim();
}

/**
 * Parse <!-- NEXT: id | id | id --> tags from Bedrock response.
 * Returns parsed IDs and cleaned response with tag stripped.
 */
function parseNextTags(response) {
  if (!response || typeof response !== 'string') {
    return { nextIds: [], cleanedResponse: response || '' };
  }

  const pattern = /<!--\s*NEXT:\s*(.*?)\s*-->/i;
  const match = response.match(pattern);

  if (!match) {
    return { nextIds: [], cleanedResponse: response };
  }

  const ids = match[1].split('|').map(id => id.trim()).filter(Boolean);
  const cleanedResponse = response.replace(pattern, '').trim();

  console.log(`[v3.5] Parsed ${ids.length} NEXT tags: ${ids.join(', ')}`);
  return { nextIds: ids, cleanedResponse };
}

/**
 * Map NEXT tag IDs to full action objects using tenant config.
 * All IDs are deterministic — mapped from available_actions in config.
 *
 * Supported prefixes:
 *   learn:formId  → send_query "Tell me more about..."
 *   apply:formId  → start_form (filtered if already completed)
 *   query:queryId → send_query from config.available_actions.queries
 *   link:linkId   → external_link from config.available_actions.links
 */
function mapNextTagsToActions(nextIds, config, sessionContext) {
  const ctaDefinitions = config.cta_definitions || {};
  const completedForms = sessionContext?.completed_forms || [];
  const actions = [];

  for (const id of nextIds) {
    const ctaId = id.trim();
    const cta = ctaDefinitions[ctaId];

    if (!cta) {
      // Legacy fallback: try prefix-based lookup against available_actions
      const legacyAction = mapLegacyPrefixTag(ctaId, config, completedForms);
      if (legacyAction) {
        actions.push(legacyAction);
      } else {
        console.log(`[v3.5] Unknown CTA ID in NEXT tag: "${ctaId}"`);
      }
      continue;
    }

    // Skip completed forms
    if (cta.action === 'start_form' && cta.formId) {
      if (completedForms.includes(cta.formId)) {
        console.log(`[v3.5] Skipping completed form CTA: ${ctaId}`);
        continue;
      }
    }

    // Build action payload directly from CTA definition
    const action = {
      label: cta.label || ctaId,
      action: cta.action,
    };

    if (cta.action === 'start_form' && cta.formId) {
      action.formId = cta.formId;
    }
    if (cta.action === 'external_link' && cta.url) {
      action.url = cta.url;
    }
    if (cta.action === 'show_info') {
      action.prompt = cta.prompt || '';
      if (cta.target_branch) {
        action.target_branch = cta.target_branch;
      }
    }
    if (cta.action === 'send_query' && cta.query) {
      action.query = cta.query;
    }

    actions.push(action);
  }

  console.log(`[v3.5] Mapped ${actions.length}/${nextIds.length} NEXT tags to actions`);
  return actions.slice(0, 3);
}

/**
 * Legacy fallback: parse prefix-based tags (learn:/apply:/link:) against available_actions.
 * Supports tenants not yet migrated to ai_available CTAs. Remove after full migration.
 */
function mapLegacyPrefixTag(tag, config, completedForms) {
  const availableActions = config.available_actions || {};

  if (tag.startsWith('apply:')) {
    const formId = tag.slice(6);
    if (completedForms.includes(formId)) return null;
    const formInfo = availableActions.forms?.[formId];
    if (!formInfo) return null;
    if (formInfo.show_info === true) {
      return {
        label: formInfo.label || 'Learn More',
        action: 'show_info',
        prompt: formInfo.prompt || formInfo.description || '',
        target_branch: formInfo.target_branch || null
      };
    }
    return { label: formInfo.label || 'Apply', action: 'start_form', formId };
  }

  if (tag.startsWith('link:')) {
    const linkId = tag.slice(5);
    const linkInfo = availableActions.links?.[linkId];
    if (!linkInfo) return null;
    return { label: linkInfo.label || linkId, action: 'external_link', url: linkInfo.url };
  }

  // learn: dropped — CHIPS handle follow-up suggestions
  if (tag.startsWith('learn:')) {
    console.log(`[v3.5] Ignoring deprecated learn: tag — CHIPS handle follow-ups`);
    return null;
  }

  return null;
}

function buildV3Prompt(userInput, kbContext, tone, conversationHistory, config, sessionContext = {}) {
  console.log(`🎯 Building V3.5 prompt (W5 + Tag & Map)`);

  const chatTitle = config?.chat_title || 'our organization';
  const completedForms = sessionContext?.completed_forms || [];
  const turnCount = (conversationHistory || []).filter(m => m.role === 'user').length;

  // ── SANITIZED PERSONA (keep tenant personality, strip link/CTA instructions) ──
  const rawPersona = config?.tone_prompt || tone ||
    `You are a friendly team member at ${chatTitle} who genuinely cares about helping people.`;
  const persona = sanitizeTonePrompt(rawPersona);

  // ── CONVERSATION HISTORY (smart compression: all user messages + last 2 assistant responses) ──
  let historyBlock = '';
  if (conversationHistory && conversationHistory.length > 0) {
    const recentCutoff = conversationHistory.length - 4; // Last 2 exchanges kept in full
    const lines = [];

    for (let i = 0; i < conversationHistory.length; i++) {
      const msg = conversationHistory[i];
      const content = (msg.content || msg.text || '').trim();
      if (!content) continue;

      if (msg.role === 'user') {
        // Always keep user messages — they contain personal details and context
        lines.push(`User: ${content}`);
      } else if (i >= recentCutoff) {
        // Recent assistant responses — keep in full for immediate context
        lines.push(`You: ${content}`);
      }
      // Earlier assistant responses are dropped — they're the bulk of prompt size
    }

    historyBlock = `\n━━━ CONVERSATION HISTORY ━━━\n${lines.join('\n')}\n`;
  }

  // ── KNOWLEDGE BASE ──
  let kbBlock = '';
  if (kbContext) {
    kbBlock = `\n━━━ KNOWLEDGE BASE ━━━\nUse ONLY this information to answer. Never invent details.\n\n${kbContext}\n`;
  } else {
    const fallback = config?.bedrock_instructions?.fallback_message ||
      "I don't have specific information about that. Let me help you with something else, or I can connect you with someone who can help.";
    kbBlock = `\n━━━ NO KNOWLEDGE BASE RESULTS ━━━\nRespond with: "${fallback}"\n`;
  }

  // ── BUILD VOCABULARY from ai_available CTAs ──
  const ctaDefinitions = config.cta_definitions || {};

  // Filter to AI-available CTAs, group by action type
  const formCtas = [];
  const infoCtas = [];
  const linkCtas = [];

  for (const [ctaId, cta] of Object.entries(ctaDefinitions)) {
    if (cta.ai_available !== true) continue;

    // Skip completed forms
    if (cta.action === 'start_form' && cta.formId && completedForms.includes(cta.formId)) {
      continue;
    }

    switch (cta.action) {
      case 'start_form':
        formCtas.push({ ctaId, label: cta.label });
        break;
      case 'show_info':
        infoCtas.push({ ctaId, label: cta.label });
        break;
      case 'external_link':
        linkCtas.push({ ctaId, label: cta.label });
        break;
      // send_query excluded — CHIPS handle follow-up suggestions
    }
  }

  // Legacy fallback: if no ai_available CTAs, try available_actions
  const hasAiCtas = formCtas.length > 0 || infoCtas.length > 0 || linkCtas.length > 0;
  if (!hasAiCtas && config.available_actions) {
    console.log('[v3.5] No ai_available CTAs found, falling back to available_actions vocabulary');
    const availableActions = config.available_actions;
    if (availableActions.forms) {
      for (const [formId, info] of Object.entries(availableActions.forms)) {
        if (!completedForms.includes(formId)) {
          if (info.direct_cta === true) formCtas.push({ ctaId: `apply:${formId}`, label: info.label || formId });
          infoCtas.push({ ctaId: `learn:${formId}`, label: `Learn about ${(info.label || formId).replace(/^Apply to /i, '')}` });
        }
      }
    }
    if (availableActions.links) {
      for (const [linkId, info] of Object.entries(availableActions.links)) {
        linkCtas.push({ ctaId: `link:${linkId}`, label: info.label || linkId });
      }
    }
  }

  // ── BUILD VOCABULARY BLOCK ──
  let vocabBlock = '';
  const hasCtas = formCtas.length > 0 || infoCtas.length > 0 || linkCtas.length > 0;

  if (hasCtas) {
    vocabBlock = '\n━━━ NEXT STEPS YOU CAN OFFER (pick 2-3) ━━━\n';
    if (infoCtas.length > 0) {
      vocabBlock += 'Explore:\n';
      infoCtas.forEach(c => {
        vocabBlock += `  ${c.ctaId} — ${c.label}\n`;
      });
    }
    if (formCtas.length > 0) {
      vocabBlock += 'Apply:\n';
      formCtas.forEach(c => {
        vocabBlock += `  ${c.ctaId} — ${c.label}\n`;
      });
    }
    if (linkCtas.length > 0) {
      vocabBlock += 'Links:\n';
      linkCtas.forEach(c => {
        vocabBlock += `  ${c.ctaId} — ${c.label}\n`;
      });
    }
    vocabBlock += '\nRULES:';
    vocabBlock += '\n• Use exact CTA IDs from the list above. Do not invent IDs.';
    if (formCtas.length > 0) {
      vocabBlock += '\n• Offer apply CTAs after you\'ve explained a program — that\'s the natural next step.';
    }
    if (infoCtas.length > 0) {
      vocabBlock += '\n• Offer explore CTAs for programs the user hasn\'t asked about yet.';
    }
    vocabBlock += '\n• Only offer a CTA when the user is ready for that action. Timing matters — don\'t push actions before the user has shown intent.';
    vocabBlock += '\n• Zero buttons is fine if none fit the moment. Don\'t force CTAs.';
  }

  // ── GUIDANCE MODULES ──
  const guidanceBlock = buildGuidanceSection(config, conversationHistory);

  // ── FILTERED CUSTOM CONSTRAINTS ──
  // Remove "follow-up question" rules — W5 WHERE handles conversation flow
  let constraintsBlock = '';
  const instructions = config?.bedrock_instructions;
  if (instructions && instructions.custom_constraints && instructions.custom_constraints.length > 0) {
    const filtered = instructions.custom_constraints.filter(c => {
      const lower = c.toLowerCase();
      return !lower.includes('follow-up question') && !lower.includes('follow up question');
    });
    if (filtered.length > 0) {
      constraintsBlock = '\n━━━ ADDITIONAL RULES ━━━\n' +
        filtered.map(c => `- ${c}`).join('\n') + '\n';
    }
  }

  // ── BUILD THE PROMPT ──
  const chipSection = buildChipSection(config, turnCount);

  const prompt = `${persona}
${kbBlock}
${historyBlock}
${vocabBlock}
${guidanceBlock}
${constraintsBlock}

━━━ YOUR TASK ━━━
USER: ${userInput}

Before responding, consider (silently — do NOT write your thinking):
  WHO — Is this person exploring, learning, or ready to act?
  WHAT — What specifically did they ask? Stay on that thread.
  WHEN — Turn ${turnCount + 1}. What have I already covered? Don't repeat it.
  WHERE — What's the natural next step from here — more info, or action?
  WHY — For each button I pick: would they actually click it right now?

Then write a concise, human response using only KB facts. End by guiding them forward.

If the user is ready for an action, pick next steps from the vocabulary (use exact IDs, pipe-separated):
<!-- NEXT: id | id -->
Omit the NEXT tag entirely if no CTA fits the moment.
${chipSection}

OUTPUT:
[your response]
<!-- NEXT: ... -->
${chipSection ? '<!-- CHIPS: [...] -->' : ''}`;

  console.log(`📝 V3.5 prompt length: ${prompt.length} chars`);
  console.log(`   Turn: ${turnCount + 1}, Explore: ${infoCtas.length}, Apply: ${formCtas.length}, Links: ${linkCtas.length}`);
  return prompt;
}

function buildPrompt(userInput, kbContext, tone, conversationHistory, config, sessionContext = {}) {
  // V3.0: Use completely new prompt when DYNAMIC_ACTIONS is enabled
  if (isFeatureEnabled('DYNAMIC_ACTIONS', config)) {
    return buildV3Prompt(userInput, kbContext, tone, conversationHistory, config, sessionContext);
  }

  // Legacy prompt for tenants without DYNAMIC_ACTIONS
  // Log prompt build metadata
  console.log(`🎯 Building prompt v${PROMPT_VERSION}`);
  console.log(`📋 Config has bedrock_instructions: ${config?.bedrock_instructions ? 'YES' : 'NO'}`);
  console.log(`🎯 KB context: ${kbContext ? kbContext.length + ' chars' : 'NONE'}`);
  console.log(`💬 Conversation history: ${conversationHistory ? conversationHistory.length + ' messages' : 'NONE'}`);
  console.log(`🚩 Feature flags: DYNAMIC_ACTIONS=${isFeatureEnabled('DYNAMIC_ACTIONS', config)}, DYNAMIC_CHIPS=${isFeatureEnabled('DYNAMIC_CHIPS', config)}, GUIDANCE_MODULES=${isFeatureEnabled('GUIDANCE_MODULES', config)}`);

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

    // ═══════════════════════════════════════════════════════════════
    // EVOLUTION v3.0: Internal Monologue + Anti-Robot Guardrails
    // ═══════════════════════════════════════════════════════════════
    parts.push('\n' + buildThoughtLayerSection());
    parts.push('\n' + buildAntiRobotSection());
    console.log(`✅ Added thought layer and anti-robot guardrails`);

    // Add LOCKED sections (never customizable)
    parts.push('\n' + getContextInterpretationRules());
    parts.push('\n' + getLockedCapabilityBoundaries());
    parts.push('\n' + getLockedLoopPrevention());
    parts.push('\n' + getLockedEngagementQuestion());
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

  // ═══════════════════════════════════════════════════════════════
  // EVOLUTION v3.0: Guidance Modules (topic-specific tone)
  // ═══════════════════════════════════════════════════════════════
  const guidanceSection = buildGuidanceSection(config, conversationHistory);
  if (guidanceSection) {
    parts.push(guidanceSection);
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

ABSOLUTELY CRITICAL - NO ACTION LINKS IN TEXT:
6. DO NOT include clickable action links like "Join our [program] →", "Apply here →" or markdown action links in your response text
7. If the knowledge base contains action links (like "Join our Love Box training program →"), DO NOT INCLUDE THEM — action buttons are provided separately by the system
8. Remove ANY action-oriented links from your response

CLOSING YOUR RESPONSE:
9. For the FIRST 2-3 exchanges on a topic: end with a follow-up question that invites deeper exploration (e.g., "Would you like to know about the training process?")
10. After 3+ exchanges on the SAME topic: shift to an action invitation instead (e.g., "Ready to take the next step?", "Want me to help you get started?", "Would you like to apply?")
11. The goal is to MOVE THE CONVERSATION FORWARD — don't keep offering more info when the user has enough to take action

🚨 CRITICAL - ANSWERING FOLLOW-UP QUESTIONS 🚨
11. If your PREVIOUS message asked a question (like "Would you like to learn more about X?") and the user says "yes", "sure", "okay", etc.:
    - ANSWER THAT SPECIFIC QUESTION - provide the information about X
    - DO NOT start with "Since the previous conversation..." or "I noticed you said yes..."
    - DO NOT repeat information you already gave - provide NEW details about what you asked
    - Just answer directly as if the user had asked the question themselves

    WRONG: "Since the previous conversation was about Dare to Dream and you responded with 'yes', I'll provide more details..."
    RIGHT: [Directly answer the question you asked, e.g., "Our mentorship approach supports foster youth through..."]`);

    // ═══════════════════════════════════════════════════════════════
    // EVOLUTION v3.0: DYNAMIC ACTIONS + CHIPS (replaces Tier 4 branch routing)
    // ═══════════════════════════════════════════════════════════════
    const completedForms = sessionContext?.completed_forms || [];
    const actionContextSection = buildActionContextSection(config, completedForms);
    if (actionContextSection) {
      parts.push(actionContextSection);
      console.log(`✅ Injected dynamic action context`);
    } else {
      // Fallback to legacy branch routing if DYNAMIC_ACTIONS is off
      const branchPromptSection = buildBranchPromptSection(config);
      if (branchPromptSection) {
        parts.push(branchPromptSection);
        console.log(`✅ Injected legacy Tier 4 branch routing prompt (DYNAMIC_ACTIONS off)`);
      }
    }

    const legacyTurnCount = (conversationHistory || []).filter(m => m.role === 'user').length;
    const chipSection = buildChipSection(config, legacyTurnCount);
    if (chipSection) {
      parts.push(chipSection);
      console.log(`✅ Injected dynamic chip generation prompt`);
    }

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
 * Analytics event handler - receives events from widget and sends to SQS
 * Supports both single events and batched events for efficiency
 *
 * Request format:
 * Single event: { schema_version, session_id, tenant_id, timestamp, step_number, event: { type, payload } }
 * Batch: { batch: true, events: [...] }
 */
async function handleAnalyticsEvent(event) {
  console.log('📊 Analytics event handler invoked');

  try {
    // Parse request body
    const body = event.body ? JSON.parse(event.body) : event;

    // Handle batch events
    if (body.batch && Array.isArray(body.events)) {
      const events = body.events;
      console.log(`📊 Processing batch of ${events.length} analytics events`);

      if (events.length === 0) {
        return {
          statusCode: 200,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
          },
          body: JSON.stringify({ status: 'success', processed: 0 })
        };
      }

      // For batches up to 10, use SQS batch send
      if (events.length <= 10) {
        const entries = events.map((evt, idx) => ({
          Id: `msg-${idx}`,
          MessageBody: JSON.stringify(evt)
        }));

        await sqs.send(new SendMessageBatchCommand({
          QueueUrl: ANALYTICS_QUEUE_URL,
          Entries: entries
        }));

        console.log(`✅ Sent ${events.length} events to SQS (batch)`);
      } else {
        // For larger batches, send as single message with batch flag
        await sqs.send(new SendMessageCommand({
          QueueUrl: ANALYTICS_QUEUE_URL,
          MessageBody: JSON.stringify({ batch: true, events })
        }));

        console.log(`✅ Sent ${events.length} events to SQS (single batch message)`);
      }

      return {
        statusCode: 200,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ status: 'success', processed: events.length })
      };
    }

    // Handle single event
    if (!body.session_id || !body.event) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ error: 'Missing required fields: session_id, event' })
      };
    }

    // Send single event to SQS
    await sqs.send(new SendMessageCommand({
      QueueUrl: ANALYTICS_QUEUE_URL,
      MessageBody: JSON.stringify(body)
    }));

    console.log(`✅ Sent single event to SQS: ${body.event.type}`);

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({ status: 'success', processed: 1 })
    };

  } catch (error) {
    console.error('❌ Analytics handler error:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({ error: error.message })
    };
  }
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

  // Route analytics requests (non-streaming) - write JSON response to stream
  const queryParams = event.queryStringParameters || {};
  const parsedBody = event.body ? JSON.parse(event.body) : event;
  if (queryParams.action === 'analytics' || parsedBody.action === 'analytics') {
    console.log('📊 Routing to analytics handler (via streaming handler)');
    const result = await handleAnalyticsEvent(event);
    responseStream.write(JSON.stringify(JSON.parse(result.body)));
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
    const isFormMode = body.form_mode === true;

    // Form mode requests don't require user_input - they have form_data instead
    if (!tenantHash || (!userInput && !isFormMode)) {
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

    // Support bedrock_instructions_override for testing (GATED - requires ENABLE_INSTRUCTION_OVERRIDE=true)
    if (body.bedrock_instructions_override && process.env.ENABLE_INSTRUCTION_OVERRIDE === 'true') {
      console.log('🔧 Applying bedrock_instructions_override from request (override enabled via env var)');
      config.bedrock_instructions = body.bedrock_instructions_override;
    } else if (body.bedrock_instructions_override) {
      console.log('⚠️ bedrock_instructions_override ignored - ENABLE_INSTRUCTION_OVERRIDE not set');
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

    // Check for show_showcase action - bypass Bedrock and return showcase card directly
    const routingMetadata = body.routing_metadata || {};
    if (routingMetadata.action === 'show_showcase' && routingMetadata.target_showcase_id) {
      console.log(`🎨 Show showcase mode detected - bypassing Bedrock for showcase: ${routingMetadata.target_showcase_id}`);
      try {
        const { getShowcaseById, loadTenantConfig } = require('./response_enhancer');
        const fullConfig = await loadTenantConfig(tenantHash);
        const showcaseCard = getShowcaseById(routingMetadata.target_showcase_id, fullConfig);

        if (showcaseCard) {
          // Send showcase card as SSE event
          const showcaseResponse = JSON.stringify({
            type: 'showcase_card',
            showcaseCard: showcaseCard,
            session_id: sessionId,
            metadata: {
              routing_tier: 'action_chip_direct',
              routing_method: 'show_showcase',
              showcase_id: showcaseCard.id
            }
          });
          write(`data: ${showcaseResponse}\n\n`);
          console.log(`✅ Sent showcase card: ${showcaseCard.id}`);
        } else {
          // Showcase not found - send error
          write(`data: {"type": "error", "error": "Showcase item not found: ${routingMetadata.target_showcase_id}"}\n\n`);
        }

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
        console.error('Showcase mode error:', error);
        write(`data: {"type": "error", "error": "Showcase processing failed: ${error.message}"}\n\n`);
        write('data: [DONE]\n\n');
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // Sanitize user input to prevent prompt injection
    const sanitizedInput = sanitizeUserInput(userInput);

    // Extract session context for form tracking and dynamic actions
    const sessionContext = body.session_context || {};

    // Get KB context
    const kbContext = await retrieveKB(sanitizedInput, config, conversationHistory);

    // V4 Pipeline: Use focused conversational prompt (no CTA instructions)
    const isV4 = isFeatureEnabled('V4_PIPELINE', config);
    let prompt;
    let modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    let maxTokens, temperature;

    if (isV4) {
      const { buildV4ConversationPrompt, V4_STEP2_INFERENCE_PARAMS } = require('./prompt_v4');
      prompt = buildV4ConversationPrompt(sanitizedInput, kbContext, config.tone_prompt, conversationHistory, config);
      maxTokens = V4_STEP2_INFERENCE_PARAMS.max_tokens;
      temperature = V4_STEP2_INFERENCE_PARAMS.temperature;
      console.log(`[V4] Step 2: Conversational prompt (${prompt.length} chars)`);
    } else {
      prompt = buildPrompt(sanitizedInput, kbContext, config.tone_prompt, conversationHistory, config, sessionContext);
      maxTokens = config.streaming?.max_tokens || DEFAULT_MAX_TOKENS;
      temperature = config.streaming?.temperature || DEFAULT_TEMPERATURE;
    }

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

    // Initialize thought-tag stripper for real-time <thought> removal
    const thoughtStripper = createThoughtTagStripper();

    // Stream the response - strip <thought> tags before sending to client
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

            // Strip <thought> tags from streamed text before sending to client
            const visibleText = thoughtStripper.process(delta.text);

            if (visibleText) {
              // Stream visible text to client immediately
              const sseData = JSON.stringify({
                type: 'text',
                content: visibleText,
                session_id: sessionId
              });
              write(`data: ${sseData}\n\n`);
            }

            // Append raw text to buffer for post-processing (includes thought tags)
            responseBuffer += delta.text;
          }
        } else if (chunkData.type === 'message_stop') {
          // Flush any remaining pending buffer from the stripper
          const remaining = thoughtStripper.flush();
          if (remaining) {
            const sseData = JSON.stringify({
              type: 'text',
              content: remaining,
              session_id: sessionId
            });
            write(`data: ${sseData}\n\n`);
          }
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

    // ═══════════════════════════════════════════════════════════════
    // POST-STREAMING: Parse response and select actions
    // V4: Separate Step 3 call for action selection
    // V3.5/Legacy: Parse hidden NEXT/CHIPS tags from response buffer
    // ═══════════════════════════════════════════════════════════════
    const { parseAiActions, parseChips } = require('./response_enhancer');

    // Extract thought content for logging (before stripping)
    const thoughtMatch = responseBuffer.match(/<thought>([\s\S]*?)<\/thought>/i);
    const thoughtContent = thoughtMatch ? thoughtMatch[1].trim() : null;

    // Strip thought tags from buffer
    let cleanBuffer = responseBuffer.replace(/<thought>[\s\S]*?<\/thought>/gi, '').trim();

    let parsedActions = [];
    let parsedChips = [];
    let cleanResponse = cleanBuffer;
    let selectedIds = []; // V4: track for logging

    if (isV4) {
      // ═══════════════════════════════════════════════════════════════
      // V4 PIPELINE: Step 3 — Synchronous Action Selection
      // ═══════════════════════════════════════════════════════════════
      const {
        buildV4ActionSelectorPrompt, V4_SELECT_ACTIONS_TOOL,
        parseV4ToolUseResponse, assembleV4Actions, V4_STEP3_INFERENCE_PARAMS,
      } = require('./prompt_v4');

      const { systemPrompt, validIds } = buildV4ActionSelectorPrompt(
        cleanBuffer, conversationHistory, config, sessionContext
      );

      if (validIds.length > 0) {
        try {
          const step3Start = Date.now();
          const step3Command = new InvokeModelCommand({
            modelId,
            accept: 'application/json',
            contentType: 'application/json',
            body: JSON.stringify({
              anthropic_version: 'bedrock-2023-05-31',
              system: systemPrompt,
              messages: [{ role: 'user', content: [{ type: 'text', text: 'Select actions.' }] }],
              tools: [V4_SELECT_ACTIONS_TOOL],
              tool_choice: { type: 'tool', name: 'select_actions' },
              max_tokens: V4_STEP3_INFERENCE_PARAMS.max_tokens,
              temperature: V4_STEP3_INFERENCE_PARAMS.temperature,
            })
          });

          const step3Response = await bedrock.send(step3Command);
          const step3Body = JSON.parse(new TextDecoder().decode(step3Response.body));
          selectedIds = parseV4ToolUseResponse(step3Body, validIds);

          const step3Time = Date.now() - step3Start;
          console.log(`[V4 Step3] Selected ${selectedIds.length} actions in ${step3Time}ms: ${selectedIds.join(', ') || '(none)'}`);

          // Step 4: Assemble full action objects from selected IDs
          parsedActions = assembleV4Actions(selectedIds, config, sessionContext, mapNextTagsToActions);
        } catch (step3Error) {
          console.error('[V4 Step3] Error:', step3Error.message);
          // Fail open — no CTAs is safe
          parsedActions = [];
        }
      } else {
        console.log('[V4 Step3] No CTAs in vocabulary — skipping action selection');
      }

      // V4: No CHIPS
      parsedChips = [];
      cleanResponse = cleanBuffer;

    } else {
      // ═══════════════════════════════════════════════════════════════
      // V3.5 / Legacy: Parse hidden tags from response buffer
      // ═══════════════════════════════════════════════════════════════
      let afterActions = cleanBuffer;

      const { nextIds, cleanedResponse: afterNext } = parseNextTags(cleanBuffer);
      if (nextIds.length > 0) {
        // v3.5 Tag & Map: map IDs to full action objects
        parsedActions = mapNextTagsToActions(nextIds, config, sessionContext);
        // Cap link CTAs to max 1 — prioritize learn/query actions over links
        const nonLinks = parsedActions.filter(a => a.action !== 'external_link');
        const links = parsedActions.filter(a => a.action === 'external_link');
        parsedActions = [...nonLinks, ...links.slice(0, 1)];
        afterActions = afterNext;
        console.log(`[v3.5] Mapped NEXT tags → ${parsedActions.length} actions: ${parsedActions.map(a => a.label).join(', ')}`);
      } else {
        // v3.0 fallback: parse full JSON actions
        const legacy = parseAiActions(cleanBuffer);
        parsedActions = legacy.actions;
        afterActions = legacy.cleanedResponse;
        if (parsedActions.length > 0) {
          console.log(`[v3.0 fallback] Parsed ${parsedActions.length} legacy ACTIONS`);
        }
      }

      const chipsResult = parseChips(afterActions);
      parsedChips = chipsResult.chips;
      cleanResponse = chipsResult.cleanedResponse;
    }

    // Use clean response (no hidden tags) for logging
    const logBuffer = cleanResponse;

    // Log complete Q&A pair AFTER streaming is done (no impact on user experience)
    if (questionBuffer && logBuffer) {
      console.log('📝 Q&A Pair Captured:');
      console.log(`  Session: ${sessionId}`);
      console.log(`  Tenant: ${tenantHash.substring(0, 8)}...`);
      console.log(`  Question: "${questionBuffer.substring(0, 100)}${questionBuffer.length > 100 ? '...' : ''}"`);
      console.log(`  Answer: "${logBuffer.substring(0, 200)}${logBuffer.length > 200 ? '...' : ''}"`);
      console.log(`  Full Q Length: ${questionBuffer.length} chars`);
      console.log(`  Full A Length: ${logBuffer.length} chars`);

      // Log full Q&A in structured format for analytics
      console.log(JSON.stringify({
        type: 'QA_COMPLETE',
        timestamp: new Date().toISOString(),
        pipeline: isV4 ? 'v4' : (isFeatureEnabled('DYNAMIC_ACTIONS', config) ? 'v3.5' : 'legacy'),
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || null,
        conversation_id: body.conversation_id || sessionId,
        question: questionBuffer,
        answer: logBuffer,
        thought: thoughtContent,
        step3_selected_ids: isV4 && selectedIds.length > 0 ? selectedIds : undefined,
        ai_actions: parsedActions.length > 0 ? parsedActions : undefined,
        suggested_chips: parsedChips.length > 0 ? parsedChips : undefined,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: logBuffer.length
        }
      }));

      // NOTE: MESSAGE_SENT and MESSAGE_RECEIVED events are now emitted by the frontend
      // (StreamingChatProvider.jsx and HTTPChatProvider.jsx) via the analytics pipeline.
      // This ensures reliable delivery since the frontend knows exactly when messages are sent/received.
    }

    // Enhance response with CTAs after streaming is complete
    try {
      const { enhanceResponse } = require('./response_enhancer');

      // Extract routing metadata for 3-tier explicit routing (PRD: Action Chips)
      const routingMetadata = body.routing_metadata || {};

      const enhancedData = await enhanceResponse(
        cleanResponse,   // The clean Bedrock response (hidden tags stripped)
        userInput,        // The user's message
        tenantHash,       // Tenant identifier
        sessionContext,   // Session context for form tracking
        routingMetadata,  // Routing metadata for explicit routing (action chips, CTAs, fallback)
        parsedActions     // AI-generated actions (from NEXT tags or legacy ACTIONS tag)
      );

      if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
        // Sort CTAs: form actions first, then queries, then links
        const ctaPriority = { 'start_form': 0, 'send_query': 1, 'external_link': 2 };
        enhancedData.ctaButtons.sort((a, b) => (ctaPriority[a.action] ?? 9) - (ctaPriority[b.action] ?? 9));
        // Mark first CTA as primary, rest as secondary (drives frontend styling)
        enhancedData.ctaButtons.forEach((cta, i) => {
          cta._position = i === 0 ? 'primary' : 'secondary';
        });
        // Send CTAs as a separate SSE event
        const ctaData = JSON.stringify({
          type: 'cta_buttons',
          ctaButtons: enhancedData.ctaButtons,
          metadata: enhancedData.metadata,
          session_id: sessionId
        });
        write(`data: ${ctaData}\n\n`);
        console.log(`🎯 Sent ${enhancedData.ctaButtons.length} CTA buttons (${enhancedData.metadata?.routing_tier || 'dynamic'})`);
      }

      // Send showcase card if present
      if (enhancedData.showcaseCard) {
        const showcaseData = JSON.stringify({
          type: 'showcase_card',
          showcaseCard: enhancedData.showcaseCard,
          session_id: sessionId,
          metadata: enhancedData.metadata
        });
        write(`data: ${showcaseData}\n\n`);
        console.log(`🎯 Sent showcase card: ${enhancedData.showcaseCard.id}`);
      }
    } catch (enhanceError) {
      console.error('❌ CTA enhancement error:', enhanceError);
      // Don't fail the response if CTA enhancement fails
    }

    // ═══════════════════════════════════════════════════════════════
    // EVOLUTION v3.0: Send suggested chips as separate SSE event
    // ═══════════════════════════════════════════════════════════════
    if (parsedChips.length > 0 && isFeatureEnabled('DYNAMIC_CHIPS', config)) {
      const chipData = JSON.stringify({
        type: 'suggested_chips',
        chips: parsedChips,
        session_id: sessionId
      });
      write(`data: ${chipData}\n\n`);
      console.log(`💡 Sent ${parsedChips.length} suggested chips`);
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

  // Route to analytics handler
  if (queryParams.action === 'analytics' || body.action === 'analytics') {
    console.log('📊 Routing to analytics handler');
    return await handleAnalyticsEvent(event);
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

    // Support bedrock_instructions_override for testing (GATED - requires ENABLE_INSTRUCTION_OVERRIDE=true)
    if (body.bedrock_instructions_override && process.env.ENABLE_INSTRUCTION_OVERRIDE === 'true') {
      console.log('🔧 Applying bedrock_instructions_override from request (override enabled via env var)');
      config.bedrock_instructions = body.bedrock_instructions_override;
    } else if (body.bedrock_instructions_override) {
      console.log('⚠️ bedrock_instructions_override ignored - ENABLE_INSTRUCTION_OVERRIDE not set');
    }

    // Check for show_showcase action - bypass Bedrock and return showcase card directly
    const routingMetadata = body.routing_metadata || {};
    if (routingMetadata.action === 'show_showcase' && routingMetadata.target_showcase_id) {
      console.log(`🎨 Show showcase mode detected - bypassing Bedrock for showcase: ${routingMetadata.target_showcase_id}`);
      try {
        const { getShowcaseById, loadTenantConfig } = require('./response_enhancer');
        const fullConfig = await loadTenantConfig(tenantHash);
        const showcaseCard = getShowcaseById(routingMetadata.target_showcase_id, fullConfig);

        if (showcaseCard) {
          // Send showcase card as SSE event
          const showcaseResponse = JSON.stringify({
            type: 'showcase_card',
            showcaseCard: showcaseCard,
            session_id: sessionId,
            metadata: {
              routing_tier: 'action_chip_direct',
              routing_method: 'show_showcase',
              showcase_id: showcaseCard.id
            }
          });
          chunks.push(`data: ${showcaseResponse}\n\n`);
          console.log(`✅ Sent showcase card: ${showcaseCard.id}`);
        } else {
          // Showcase not found - send error
          chunks.push(`data: {"type": "error", "error": "Showcase item not found: ${routingMetadata.target_showcase_id}"}\n\n`);
        }

        chunks.push('data: [DONE]\n\n');

        return {
          statusCode: 200,
          headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            'Access-Control-Allow-Origin': '*',
            'X-Accel-Buffering': 'no'
          },
          body: chunks.join(''),
          isBase64Encoded: false
        };
      } catch (error) {
        console.error('Showcase mode error:', error);
        chunks.push(`data: {"type": "error", "error": "Showcase processing failed: ${error.message}"}\n\n`);
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
    }

    // Sanitize user input to prevent prompt injection
    const sanitizedInput = sanitizeUserInput(userInput);

    // Extract session context for form tracking and dynamic actions
    const sessionContext = body.session_context || {};

    // Get KB context
    const kbContext = await retrieveKB(sanitizedInput, config, conversationHistory);

    // V4 Pipeline: Use focused conversational prompt (no CTA instructions)
    const isV4 = isFeatureEnabled('V4_PIPELINE', config);
    let prompt;
    let modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    let maxTokens, temperature;

    if (isV4) {
      const { buildV4ConversationPrompt, V4_STEP2_INFERENCE_PARAMS } = require('./prompt_v4');
      prompt = buildV4ConversationPrompt(sanitizedInput, kbContext, config.tone_prompt, conversationHistory, config);
      maxTokens = V4_STEP2_INFERENCE_PARAMS.max_tokens;
      temperature = V4_STEP2_INFERENCE_PARAMS.temperature;
      console.log(`[V4] Step 2: Conversational prompt (${prompt.length} chars)`);
    } else {
      prompt = buildPrompt(sanitizedInput, kbContext, config.tone_prompt, conversationHistory, config, sessionContext);
      maxTokens = config.streaming?.max_tokens || DEFAULT_MAX_TOKENS;
      temperature = config.streaming?.temperature || DEFAULT_TEMPERATURE;
    }

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

      // NOTE: MESSAGE_SENT and MESSAGE_RECEIVED events are now emitted by the frontend
      // (StreamingChatProvider.jsx and HTTPChatProvider.jsx) via the analytics pipeline.
      // This ensures reliable delivery since the frontend knows exactly when messages are sent/received.
    }

    // ═══════════════════════════════════════════════════════════════
    // POST-GENERATION: Select actions and enhance response
    // ═══════════════════════════════════════════════════════════════
    let v4Actions = [];

    if (isV4) {
      // V4 Step 3: Synchronous action selection
      try {
        const {
          buildV4ActionSelectorPrompt, V4_SELECT_ACTIONS_TOOL,
          parseV4ToolUseResponse, assembleV4Actions, V4_STEP3_INFERENCE_PARAMS,
        } = require('./prompt_v4');

        const { systemPrompt, validIds } = buildV4ActionSelectorPrompt(
          responseBuffer, conversationHistory, config, sessionContext
        );

        if (validIds.length > 0) {
          const step3Start = Date.now();
          const step3Command = new InvokeModelCommand({
            modelId,
            accept: 'application/json',
            contentType: 'application/json',
            body: JSON.stringify({
              anthropic_version: 'bedrock-2023-05-31',
              system: systemPrompt,
              messages: [{ role: 'user', content: [{ type: 'text', text: 'Select actions.' }] }],
              tools: [V4_SELECT_ACTIONS_TOOL],
              tool_choice: { type: 'tool', name: 'select_actions' },
              max_tokens: V4_STEP3_INFERENCE_PARAMS.max_tokens,
              temperature: V4_STEP3_INFERENCE_PARAMS.temperature,
            })
          });

          const step3Response = await bedrock.send(step3Command);
          const step3Body = JSON.parse(new TextDecoder().decode(step3Response.body));
          const selectedIds = parseV4ToolUseResponse(step3Body, validIds);

          console.log(`[V4 Step3] Selected ${selectedIds.length} actions in ${Date.now() - step3Start}ms: ${selectedIds.join(', ') || '(none)'}`);
          v4Actions = assembleV4Actions(selectedIds, config, sessionContext, mapNextTagsToActions);
        }
      } catch (step3Error) {
        console.error('[V4 Step3] Error:', step3Error.message);
      }
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
        routingMetadata, // Routing metadata for explicit routing (action chips, CTAs, fallback)
        isV4 ? v4Actions : undefined // V4: pass pre-selected actions
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

// Test-only exports (not used by Lambda runtime)
if (process.env.NODE_ENV === 'test' || process.env.PICASSO_TEST_MODE === 'true') {
  exports._test = {
    buildPrompt,
    buildV3Prompt,
    buildKBSearchQuery,
    isFeatureEnabled,
    createThoughtTagStripper,
    buildActionContextSection,
    buildChipSection,
    buildThoughtLayerSection,
    buildAntiRobotSection,
    buildGuidanceSection,
  };
}