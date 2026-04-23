/**
 * S3 operations for the KB proposal applier.
 *
 * Three S3 surfaces:
 *   - Proposals bucket (myrecruiter-picasso) — read pending proposals, write applied.json audit trails
 *   - Config bucket (myrecruiter-picasso, same bucket, different prefix) — read/write tenant configs
 *     with ETag-based optimistic concurrency
 *   - KB bucket (kbragdocs) — read/write the tenant's knowledge base markdown
 *
 * Config I/O mirrors Picasso_Config_Manager/s3Operations.mjs deliberately — same ETag check,
 * same backup convention, same key layout — so the Applier preserves concurrency guarantees
 * even though it doesn't route through Config Manager's HTTP surface. If Config Manager ever
 * gains server-side validation hooks, switch to Lambda-to-Lambda invocation.
 */

import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';

const REGION = process.env.AWS_REGION || 'us-east-1';
const CONFIG_BUCKET = process.env.CONFIG_BUCKET || 'myrecruiter-picasso';
const KB_BUCKET = process.env.KB_BUCKET || 'kbragdocs';

const s3 = new S3Client({ region: REGION });

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf-8');
}

// ─── Proposals ──────────────────────────────────────────────────────────────

export async function loadProposal(tenantId, proposalId) {
  const key = `pending-proposals/${tenantId}/${proposalId}.json`;
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: CONFIG_BUCKET, Key: key }));
    return JSON.parse(await streamToString(res.Body));
  } catch (error) {
    if (error.name === 'NoSuchKey') {
      throw new Error(`Proposal not found: ${key}`);
    }
    throw error;
  }
}

export async function writeAppliedAudit(tenantId, proposalId, auditBody) {
  const key = `pending-proposals/${tenantId}/${proposalId}/applied.json`;
  await s3.send(new PutObjectCommand({
    Bucket: CONFIG_BUCKET,
    Key: key,
    Body: JSON.stringify(auditBody, null, 2),
    ContentType: 'application/json',
  }));
  return key;
}

// ─── Tenant config (with ETag concurrency) ──────────────────────────────────

export class ConfigETagMismatchError extends Error {
  constructor(currentETag) {
    super('Config was modified since the Applier loaded it');
    this.name = 'ConfigETagMismatchError';
    this.currentETag = currentETag;
  }
}

export async function loadConfigWithETag(tenantId) {
  const key = `tenants/${tenantId}/${tenantId}-config.json`;
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: CONFIG_BUCKET, Key: key }));
    const body = await streamToString(res.Body);
    return { config: JSON.parse(body), etag: res.ETag };
  } catch (error) {
    if (error.name === 'NoSuchKey') throw new Error(`Config not found for tenant: ${tenantId}`);
    throw error;
  }
}

export async function saveConfig(tenantId, config, ifMatchETag) {
  // HEAD-before-PUT ETag check: same TOCTOU window as Picasso_Config_Manager, same tradeoff.
  // S3 PutObject IfMatch isn't universally reliable across SDK/region combos; this pattern is
  // what's in production today and the Applier matches it for consistency.
  //
  // We also capture the CURRENT config for backup during the same HEAD call — backing up the
  // pre-write state, not the post-write state, so the backup is a rollback target.
  let backupSource = null;
  if (ifMatchETag) {
    const { etag: currentETag, config: currentConfig } = await loadConfigWithETag(tenantId);
    if (currentETag !== ifMatchETag) {
      throw new ConfigETagMismatchError(currentETag);
    }
    backupSource = currentConfig;
  } else {
    // No ETag provided — still back up current state before overwriting.
    try {
      const { config: currentConfig } = await loadConfigWithETag(tenantId);
      backupSource = currentConfig;
    } catch (error) {
      if (!error.message.includes('not found')) throw error;
      // New tenant, nothing to back up.
    }
  }

  if (backupSource) {
    await createConfigBackup(tenantId, backupSource);
  }

  config.last_updated = new Date().toISOString();
  config.tenant_id = tenantId;

  const key = `tenants/${tenantId}/${tenantId}-config.json`;
  const res = await s3.send(new PutObjectCommand({
    Bucket: CONFIG_BUCKET,
    Key: key,
    Body: JSON.stringify(config, null, 2),
    ContentType: 'application/json',
  }));

  return { key, etag: res.ETag, timestamp: config.last_updated };
}

async function createConfigBackup(tenantId, config) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupKey = `tenants/${tenantId}/${tenantId}-${timestamp}.json`;
  await s3.send(new PutObjectCommand({
    Bucket: CONFIG_BUCKET,
    Key: backupKey,
    Body: JSON.stringify(config, null, 2),
    ContentType: 'application/json',
  }));
  return backupKey;
}

// ─── Knowledge base markdown ────────────────────────────────────────────────

/**
 * Discover the tenant's KB file under `s3://{KB_BUCKET}/tenants/{tenantId}/`.
 * v1 assumes exactly one `.md` file. If zero or multiple exist, the caller must surface
 * a clear error — the scanner/onboarding convention expects one canonical KB per tenant.
 */
export async function discoverKbKey(tenantId) {
  const prefix = `tenants/${tenantId}/`;
  const res = await s3.send(new ListObjectsV2Command({
    Bucket: KB_BUCKET,
    Prefix: prefix,
  }));
  const mdKeys = (res.Contents || [])
    .map(o => o.Key)
    .filter(k => k && k.endsWith('.md'));

  if (mdKeys.length === 0) {
    throw new Error(`No KB markdown found at s3://${KB_BUCKET}/${prefix}`);
  }
  if (mdKeys.length > 1) {
    throw new Error(`Multiple KB markdowns at s3://${KB_BUCKET}/${prefix}: ${mdKeys.join(', ')}. v1 expects exactly one.`);
  }
  return mdKeys[0];
}

export async function readKb(key) {
  const res = await s3.send(new GetObjectCommand({ Bucket: KB_BUCKET, Key: key }));
  return streamToString(res.Body);
}

export async function writeKb(key, content) {
  await s3.send(new PutObjectCommand({
    Bucket: KB_BUCKET,
    Key: key,
    Body: content,
    ContentType: 'text/markdown',
  }));
}
