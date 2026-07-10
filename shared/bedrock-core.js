/**
 * Shared Bedrock Core Module
 *
 * Extracted from Bedrock_Streaming_Handler_Staging/index.js to enable reuse
 * across multiple Lambda functions (streaming handler, Meta response processor, etc.)
 *
 * Exports:
 *   - loadConfig(tenantHash, options) — Resolve tenant hash → config via DynamoDB registry + S3 fallback
 *   - retrieveKB(userInput, config) — Query Bedrock Knowledge Base with caching
 *   - sanitizeUserInput(input) — Strip injection patterns from user input
 *
 * Each Lambda that bundles this module gets its own in-memory cache instance.
 * This is correct — do not attempt to share cache state across Lambda functions.
 */

const { BedrockAgentRuntimeClient, RetrieveCommand } = require('@aws-sdk/client-bedrock-agent-runtime');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, QueryCommand } = require('@aws-sdk/client-dynamodb');
const crypto = require('crypto');

// Initialize AWS clients
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
/**
 * Cross-account KB access: when KB_RETRIEVER_ROLE_ARN is set (staging account
 * reaching prod-account KBs), assume that role for Bedrock-Agent-Runtime calls.
 * fromTemporaryCredentials caches and auto-refreshes via STS, so AssumeRole
 * fires only on first use + before credential expiry. Unset env var → SDK
 * default credential chain (legacy / prod-account behavior).
 *
 * Lazy-require keeps prod-account consumers from needing credential-providers.
 *
 * MODULE-LOAD-TIME CAPTURE: this block runs once when bedrock-core is required.
 * The bedrockAgent singleton retains whatever credentials it got at load.
 * Tests that toggle KB_RETRIEVER_ROLE_ARN MUST use jest.isolateModules to force
 * a fresh require — otherwise the singleton from a prior test contaminates the
 * next one. See __tests__/bedrock_core_assume_role.test.js for the pattern.
 */
const KB_RETRIEVER_ROLE_ARN = process.env.KB_RETRIEVER_ROLE_ARN;
// CS1: bound the (non-streaming) Bedrock Retrieve call so a hung connection or
// stalled response fails fast instead of hanging until the caller's Lambda
// timeout — a KB-retrieve hang otherwise leaves the chat "typing" indefinitely.
// throwOnRequestTimeout:true is REQUIRED (without it the timeout only warns).
// On timeout the reject is caught by retrieveKB() → returns '' (fail-open: the
// answer ships without KB grounding, same as the existing AccessDenied path).
const KB_RETRIEVE_TIMEOUTS = {
  connectionTimeout: 6000,
  requestTimeout: 15000,
  throwOnRequestTimeout: true,
};
const bedrockAgentClientConfig = { region: AWS_REGION, requestHandler: KB_RETRIEVE_TIMEOUTS };
let kbCredsInitFailed = false;
if (KB_RETRIEVER_ROLE_ARN) {
  try {
    const { fromTemporaryCredentials } = require('@aws-sdk/credential-providers');
    bedrockAgentClientConfig.credentials = fromTemporaryCredentials({
      params: {
        RoleArn: KB_RETRIEVER_ROLE_ARN,
        RoleSessionName: 'bedrock-kb-retriever',
        DurationSeconds: 3600,
      },
    });
  } catch (e) {
    // Silent fallback to default creds → cross-account KB Retrieve fails with
    // AccessDenied → retrieveKB() catches and returns ''  → chat answer ships
    // with NO KB grounding. This is fail-open for chat. Set a sticky flag so
    // every subsequent retrieveKB() call emits a structured signal that a
    // CloudWatch metric filter can alert on.
    kbCredsInitFailed = true;
    console.error('KB_RETRIEVER_ROLE_ARN is set but @aws-sdk/credential-providers is not installed; falling back to default credentials. Bedrock Retrieve will fail with cross-account KB.', e.message);
  }
}
const bedrockAgent = new BedrockAgentRuntimeClient(bedrockAgentClientConfig);
const s3 = new S3Client({ region: AWS_REGION });
const dynamodb = new DynamoDBClient({ region: AWS_REGION });

// Tenant registry feature flag
const USE_REGISTRY = (process.env.USE_REGISTRY_FOR_RESOLUTION || '').toLowerCase() === 'true';
const TENANT_REGISTRY_TABLE = process.env.TENANT_REGISTRY_TABLE || `picasso-tenant-registry-${process.env.ENVIRONMENT || 'staging'}`;

// In-memory cache with size limits to prevent memory exhaustion
const KB_CACHE = {};
const CONFIG_CACHE = {};
const CACHE_TTL = 300000; // 5 minutes
const MAX_CACHE_SIZE = 100; // Maximum entries per cache

// ═══════════════════════════════════════════════════════════════
// SECURITY: Input sanitization to prevent prompt injection
// ═══════════════════════════════════════════════════════════════
const MAX_USER_INPUT_LENGTH = 4000;

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
  const injectionPatterns = [
    /\n\s*(SYSTEM|ASSISTANT|HUMAN|USER)\s*:/gi,
    /\n\s*<\|?(system|assistant|human|user|im_start|im_end)\|?>/gi,
    /[\s*[](INST|\/INST|SYS|\/SYS)\s*\]/gi
  ];

  for (const pattern of injectionPatterns) {
    sanitized = sanitized.replace(pattern, (match) => `[FILTERED: ${match.trim()}]`);
  }

  return sanitized.trim();
}

// ═══════════════════════════════════════════════════════════════
// CACHE UTILITIES
// ═══════════════════════════════════════════════════════════════

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

  const sortedKeys = keys.sort((a, b) => (cache[a]?.timestamp || 0) - (cache[b]?.timestamp || 0));
  const toRemove = sortedKeys.slice(0, keys.length - maxSize);

  for (const key of toRemove) {
    delete cache[key];
  }

  if (toRemove.length > 0) {
    console.log(`🧹 Evicted ${toRemove.length} old cache entries`);
  }
}

// ═══════════════════════════════════════════════════════════════
// TENANT CONFIG LOADING
// ═══════════════════════════════════════════════════════════════

async function loadConfig(tenantHash, { skipCache = false } = {}) {
  try {
    const cacheKey = `config:${tenantHash}`;
    if (!skipCache && CONFIG_CACHE[cacheKey] && isCacheValid(CONFIG_CACHE[cacheKey])) {
      console.log(`✅ Config cache hit for ${tenantHash.substring(0, 8)}...`);
      const cachedConfig = CONFIG_CACHE[cacheKey].data;
      console.log(`📋 Cached KB ID: ${cachedConfig?.aws?.knowledge_base_id || 'NOT SET'}`);
      return cachedConfig;
    }

    const bucket = process.env.CONFIG_BUCKET || 'myrecruiter-picasso';
    console.log(`🪣 Loading config from bucket: ${bucket}`);

    // Resolve tenant hash → tenant_id (DynamoDB first, S3 fallback)
    let mapping = null;

    if (USE_REGISTRY) {
      try {
        const registryResult = await dynamodb.send(new QueryCommand({
          TableName: TENANT_REGISTRY_TABLE,
          IndexName: 'TenantHashIndex',
          KeyConditionExpression: 'tenantHash = :hash',
          ExpressionAttributeValues: { ':hash': { S: tenantHash } },
          Limit: 1,
        }));
        const items = registryResult.Items || [];
        if (items.length > 0 && items[0].status?.S === 'active') {
          mapping = { tenant_id: items[0].tenantId.S };
          console.log(`📍 Resolved via DynamoDB registry: ${mapping.tenant_id}`);
        } else if (items.length > 0) {
          console.warn(`⚠️ Registry record found but status=${items[0].status?.S}, falling back to S3`);
        }
      } catch (registryErr) {
        console.warn(`⚠️ Registry lookup failed, falling back to S3: ${registryErr.message}`);
      }
    }

    if (!mapping) {
      const mappingResponse = await s3.send(new GetObjectCommand({
        Bucket: bucket,
        Key: `mappings/${tenantHash}.json`
      }));
      mapping = JSON.parse(await mappingResponse.Body.transformToString());
      console.log(`📍 Resolved via S3 mapping: ${mapping.tenant_id}`);
    }

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
        evictOldestCacheEntries(CONFIG_CACHE);
        console.log(`📋 KB ID in config: ${config?.aws?.knowledge_base_id || 'NOT SET'}`);
        console.log(`📋 Full AWS config:`, JSON.stringify(config?.aws || {}, null, 2));

        // V4.1 Pipeline: validate topic_definitions at load time
        if (config.topic_definitions) {
          // validateTopicDefinitions is caller-provided (prompt_v4.js)
          // Only validate if the function is available in the caller's context
          try {
            const { validateTopicDefinitions } = require('./prompt_v4');
            const validation = validateTopicDefinitions(config);
            if (validation.warnings.length > 0) {
              console.warn(`[V4.1] topic_definitions validation warnings:`, validation.warnings);
            }
            if (validation.definitions.length > 0) {
              console.log(`[V4.1] ${validation.definitions.length} valid topic definitions loaded`);
            }
          } catch (e) {
            // prompt_v4 not available in this Lambda context — skip validation
          }
        }

        return config;
      }
    }
  } catch (error) {
    console.error('❌ Config load error:', error.message);
    console.error('Full error:', error);
  }

  return null;
}

// ═══════════════════════════════════════════════════════════════
// KNOWLEDGE BASE RETRIEVAL
// ═══════════════════════════════════════════════════════════════

async function retrieveKB(userInput, config) {
  const kbId = config?.aws?.knowledge_base_id;
  console.log(`🔍 KB Retrieval - KB ID: ${kbId || 'NOT SET'}`);
  console.log(`🔍 User input: "${userInput.substring(0, 50)}..."`);

  // Sticky alert signal: KB cross-account creds failed at module load.
  // Emitted on EVERY retrieve so a CloudWatch metric filter on
  // `kb_creds_init_failed` will fire reliably (not just at cold start).
  if (kbCredsInitFailed) {
    console.log(JSON.stringify({
      evt: 'kb_creds_init_failed',
      kb_id: kbId || null,
      role_arn: KB_RETRIEVER_ROLE_ARN || null,
    }));
  }

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
    evictOldestCacheEntries(KB_CACHE);
    return chunks;

  } catch (error) {
    console.error('❌ KB retrieval error:', error.message);
    console.error('Full KB error:', error);
    return '';
  }
}

// ═══════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════

module.exports = {
  loadConfig,
  retrieveKB,
  sanitizeUserInput,
  // Expose cache utilities for consumers that need custom caching
  getCacheKey,
  isCacheValid,
  evictOldestCacheEntries,
  // Expose constants for consumers that need them
  CACHE_TTL,
  MAX_CACHE_SIZE,
  KB_RETRIEVE_TIMEOUTS,
};
