/**
 * S3 Operations Module
 * Handles all S3 interactions for tenant configuration management
 */

import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectCommand,
} from '@aws-sdk/client-s3';

const BUCKET = process.env.S3_BUCKET || 'myrecruiter-picasso';
const REGION = process.env.AWS_REGION || 'us-east-1';

// Initialize S3 client
const s3Client = new S3Client({ region: REGION });

/**
 * Stream to string helper
 */
async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

/**
 * List all tenant configurations in the bucket
 * @returns {Promise<Array>} Array of tenant metadata
 */
export async function listTenantConfigs() {
  try {
    const command = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: 'tenants/',
      Delimiter: '/',
    });

    const response = await s3Client.send(command);

    if (!response.CommonPrefixes) {
      return [];
    }

    // List all tenant folders
    const tenantIds = response.CommonPrefixes.map(prefix => {
      const match = prefix.Prefix.match(/^tenants\/([^/]+)\/$/);
      return match ? match[1] : null;
    }).filter(Boolean);

    // Get metadata for each tenant
    const tenants = [];
    for (const tenantId of tenantIds) {
      try {
        const metadata = await getTenantMetadata(tenantId);
        tenants.push({
          tenantId,
          ...metadata,
        });
      } catch (error) {
        console.warn(`Could not load metadata for ${tenantId}:`, error.message);
      }
    }

    return tenants;
  } catch (error) {
    console.error('Error listing tenant configs:', error);
    throw new Error(`Failed to list tenant configs: ${error.message}`);
  }
}

/**
 * Load a tenant configuration from S3
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<Object>} The tenant configuration object
 */
export async function loadConfig(tenantId) {
  const { config } = await loadConfigWithETag(tenantId);
  return config;
}

/**
 * Load a tenant configuration along with its S3 ETag, so callers can
 * implement optimistic concurrency control on subsequent writes.
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<{config: Object, etag: string}>}
 */
export async function loadConfigWithETag(tenantId) {
  try {
    const key = `tenants/${tenantId}/${tenantId}-config.json`;
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });

    const response = await s3Client.send(command);
    const configString = await streamToString(response.Body);
    const config = JSON.parse(configString);

    return { config, etag: response.ETag };
  } catch (error) {
    if (error.name === 'NoSuchKey') {
      throw new Error(`Config not found for tenant: ${tenantId}`);
    }
    console.error(`Error loading config for ${tenantId}:`, error);
    throw new Error(`Failed to load config: ${error.message}`);
  }
}

/**
 * Get tenant metadata without loading full config
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<Object>} Tenant metadata
 */
export async function getTenantMetadata(tenantId) {
  try {
    const config = await loadConfig(tenantId);

    return {
      tenant_id: config.tenant_id,
      // Exposed so the frontend can resolve hash-based deep links (e.g.
      // /pending-changes?h=HASH from Slack/email). Per security policy, tenant
      // IDs are never in open URLs — only hashes. The frontend looks up the
      // matching entry in this already-loaded list rather than a new endpoint.
      tenant_hash: config.tenant_hash || null,
      version: config.version,
      chat_title: config.chat_title,
      company_name: config.company_name || config.chat_title,
      last_updated: config.last_updated || null,
      program_count: Object.keys(config.programs || {}).length,
      form_count: Object.keys(config.conversational_forms || {}).length,
      cta_count: Object.keys(config.cta_definitions || {}).length,
      branch_count: Object.keys(config.conversation_branches || {}).length,
    };
  } catch (error) {
    console.error(`Error getting metadata for ${tenantId}:`, error);
    throw error;
  }
}

/**
 * Error thrown when an optimistic-concurrency check fails on save.
 * Carries the current config + etag so callers can show the user the
 * latest server state.
 */
export class ConfigETagMismatchError extends Error {
  constructor(currentConfig, currentETag) {
    super('Config was modified since it was loaded');
    this.name = 'ConfigETagMismatchError';
    this.currentConfig = currentConfig;
    this.currentETag = currentETag;
  }
}

/**
 * Save a tenant configuration to S3
 * @param {string} tenantId - The tenant ID
 * @param {Object} config - The configuration object
 * @param {boolean} createBackup - Whether to create a backup before saving
 * @param {string} [ifMatch] - Optional S3 ETag. When provided, the save
 *   proceeds only if the current object's ETag matches. Mismatch throws
 *   ConfigETagMismatchError.
 * @returns {Promise<Object>} Save result (includes the new ETag)
 */
export async function saveConfig(tenantId, config, createBackup = true, ifMatch) {
  try {
    // Optimistic concurrency: verify the current object hasn't changed
    // since the caller loaded it. Uses a HEAD-before-PUT check because
    // S3's native IfMatch on PutObject isn't universally supported
    // across SDK/region combos. This is a narrow TOCTOU window in
    // exchange for portability — acceptable for the low-concurrency
    // config editor workflow.
    if (ifMatch) {
      try {
        const { etag: currentETag, config: currentConfig } = await loadConfigWithETag(tenantId);
        if (currentETag !== ifMatch) {
          throw new ConfigETagMismatchError(currentConfig, currentETag);
        }
      } catch (error) {
        if (error instanceof ConfigETagMismatchError) throw error;
        // If the current object doesn't exist yet, the caller shouldn't
        // have an ETag to match against — treat as mismatch.
        if (error.message.includes('not found')) {
          throw new ConfigETagMismatchError(null, null);
        }
        throw error;
      }
    }

    // Create backup if requested and config exists
    if (createBackup) {
      try {
        const existingConfig = await loadConfig(tenantId);
        await createConfigBackup(tenantId, existingConfig);
      } catch (error) {
        // If config doesn't exist, no backup needed
        if (!error.message.includes('not found')) {
          console.warn(`Failed to create backup: ${error.message}`);
        }
      }
    }

    // Add metadata
    config.last_updated = new Date().toISOString();
    config.tenant_id = tenantId;

    // Save to S3
    const key = `tenants/${tenantId}/${tenantId}-config.json`;
    const command = new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: JSON.stringify(config, null, 2),
      ContentType: 'application/json',
    });

    const putResponse = await s3Client.send(command);

    return {
      success: true,
      tenantId,
      key,
      timestamp: config.last_updated,
      etag: putResponse.ETag,
    };
  } catch (error) {
    if (error instanceof ConfigETagMismatchError) throw error;
    console.error(`Error saving config for ${tenantId}:`, error);
    throw new Error(`Failed to save config: ${error.message}`);
  }
}

/**
 * Create a backup of a tenant configuration
 * Backups are now stored in the tenant's folder for better organization
 * @param {string} tenantId - The tenant ID
 * @param {Object} config - The configuration object
 * @returns {Promise<string>} The backup key
 */
export async function createConfigBackup(tenantId, config) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    // Store backup in tenant's folder instead of centralized backups/ folder
    const backupKey = `tenants/${tenantId}/${tenantId}-${timestamp}.json`;

    const command = new PutObjectCommand({
      Bucket: BUCKET,
      Key: backupKey,
      Body: JSON.stringify(config, null, 2),
      ContentType: 'application/json',
    });

    await s3Client.send(command);

    return backupKey;
  } catch (error) {
    console.error(`Error creating backup for ${tenantId}:`, error);
    throw new Error(`Failed to create backup: ${error.message}`);
  }
}

/**
 * Delete a tenant configuration from S3
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<Object>} Delete result
 */
export async function deleteConfig(tenantId) {
  try {
    // Create backup before deleting
    const config = await loadConfig(tenantId);
    const backupKey = await createConfigBackup(tenantId, config);

    // Delete the config
    const key = `tenants/${tenantId}/${tenantId}-config.json`;
    const command = new DeleteObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });

    await s3Client.send(command);

    return {
      success: true,
      tenantId,
      deletedKey: key,
      backupKey,
    };
  } catch (error) {
    console.error(`Error deleting config for ${tenantId}:`, error);
    throw new Error(`Failed to delete config: ${error.message}`);
  }
}

/**
 * List backups for a tenant
 * Reads backups from tenant's folder (new location)
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<Array>} Array of backup metadata
 */
export async function listBackups(tenantId) {
  try {
    const command = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: `tenants/${tenantId}/`,
    });

    const response = await s3Client.send(command);

    if (!response.Contents) {
      return [];
    }

    // Filter to only backup files (exclude the main config and draft files)
    // Backup files have format: tenantId-YYYY-MM-DDTHH-MM-SS-MMMZ.json
    // Main config has format: tenantId-config.json
    // Draft has format: tenantId-draft.json
    return response.Contents
      .filter(item => {
        const key = item.Key;
        return key.endsWith('.json') && !key.endsWith('-config.json') && !key.endsWith('-draft.json');
      })
      .map(item => ({
        key: item.Key,
        lastModified: item.LastModified,
        size: item.Size,
      }))
      .sort((a, b) => b.lastModified - a.lastModified);
  } catch (error) {
    console.error(`Error listing backups for ${tenantId}:`, error);
    throw new Error(`Failed to list backups: ${error.message}`);
  }
}

/**
 * Save a draft configuration for a tenant
 * Draft is stored as a sibling to the live config; no backup is created.
 * @param {string} tenantId - The tenant ID
 * @param {Object} config - The configuration object
 * @returns {Promise<Object>} Save result
 */
export async function saveDraft(tenantId, config) {
  try {
    const draftSavedAt = new Date().toISOString();
    const draftConfig = { ...config, draft_saved_at: draftSavedAt };

    const key = `tenants/${tenantId}/${tenantId}-draft.json`;
    const command = new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: JSON.stringify(draftConfig, null, 2),
      ContentType: 'application/json',
    });

    await s3Client.send(command);
    console.log(`Draft saved for tenant ${tenantId} at ${draftSavedAt}`);

    return {
      success: true,
      tenantId,
      key,
      timestamp: draftSavedAt,
    };
  } catch (error) {
    console.error(`Error saving draft for ${tenantId}:`, error);
    throw new Error(`Failed to save draft: ${error.message}`);
  }
}

/**
 * Load a draft configuration for a tenant
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<{ config: Object|null, hasDraft: boolean }>}
 */
export async function loadDraft(tenantId) {
  try {
    const key = `tenants/${tenantId}/${tenantId}-draft.json`;
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });

    const response = await s3Client.send(command);
    const configString = await streamToString(response.Body);
    const config = JSON.parse(configString);

    return { config, hasDraft: true };
  } catch (error) {
    if (error.name === 'NoSuchKey') {
      return { config: null, hasDraft: false };
    }
    console.error(`Error loading draft for ${tenantId}:`, error);
    throw new Error(`Failed to load draft: ${error.message}`);
  }
}

/**
 * Delete a draft configuration for a tenant
 * @param {string} tenantId - The tenant ID
 * @returns {Promise<Object>} Delete result
 */
export async function deleteDraft(tenantId) {
  try {
    const key = `tenants/${tenantId}/${tenantId}-draft.json`;
    const command = new DeleteObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });

    await s3Client.send(command);
    console.log(`Draft deleted for tenant ${tenantId}`);

    return {
      success: true,
      tenantId,
      deletedKey: key,
    };
  } catch (error) {
    console.error(`Error deleting draft for ${tenantId}:`, error);
    throw new Error(`Failed to delete draft: ${error.message}`);
  }
}

/**
 * List pending KB-freshness proposals for a tenant.
 * Proposals are written by the scanner to
 * `pending-proposals/{tenantId}/{proposalId}.json`.
 * Returns an array of parsed proposal objects, newest first.
 * @param {string} tenantId
 * @returns {Promise<Array<Object>>}
 */
export async function listProposals(tenantId) {
  const prefix = `pending-proposals/${tenantId}/`;
  try {
    const list = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
    }));

    const keys = (list.Contents || [])
      .map((o) => o.Key)
      .filter((k) => k && k.endsWith('.json'));

    const proposals = await Promise.all(
      keys.map(async (key) => {
        try {
          const res = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
          const body = await streamToString(res.Body);
          return JSON.parse(body);
        } catch (err) {
          console.warn(`Failed to read proposal ${key}: ${err.message}`);
          return null;
        }
      })
    );

    return proposals
      .filter(Boolean)
      .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  } catch (error) {
    console.error(`Error listing proposals for ${tenantId}:`, error);
    throw new Error(`Failed to list proposals: ${error.message}`);
  }
}

/**
 * Store tenant ID to hash mapping
 * @param {string} tenantId - The tenant ID
 * @param {string} tenantHash - The generated tenant hash
 * @returns {Promise<Object>} The mapping object
 */
export async function storeTenantMapping(tenantId, tenantHash) {
  const mapping = {
    tenant_id: tenantId,
    tenant_hash: tenantHash,
    created_at: new Date().toISOString(),
  };

  const params = {
    Bucket: BUCKET,
    Key: `mappings/${tenantHash}.json`,
    Body: JSON.stringify(mapping, null, 2),
    ContentType: 'application/json',
  };

  await s3Client.send(new PutObjectCommand(params));
  return mapping;
}
