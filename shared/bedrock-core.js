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
const bedrockAgent = new BedrockAgentRuntimeClient({ region: AWS_REGION });
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
};
