/**
 * Consumer PII Remediation Path A, Phase 1 — stable subject identifier (BSH port).
 *
 * Node.js port of Master_Function_Staging/pii_subject.py for the BSH form_handler
 * writer (M1.G6 / F-DSAR18 closure). Mints an opaque, non-reversible
 * `pii_subject_id` at the first identifying input (a form submission) and
 * maintains a per-tenant `(tenant_id, normalized_email) → pii_subject_id` index
 * for later DSAR/delete lookup.
 *
 * Contract: `pii_subject_id` is **additive**. Scheduling continues to key on
 * `submission_id`; DSAR walker reads `pii_subject_id`. Index access is
 * **best-effort** — a form submission must never fail because the index is
 * unavailable (mirrors the existing non-fatal analytics-write pattern).
 *
 * Behavioral parity targets (Master_Function_Staging/pii_subject.py):
 *   - mint_pii_subject_id   → mintPiiSubjectId
 *   - normalize_email       → normalizeEmail (Gmail dot+plus rules ONLY)
 *   - extract_email         → extractEmail
 *   - get_or_create         → getOrCreatePiiSubjectId
 *
 * Caller (form_handler.js:saveFormSubmission) supplies the canonical email
 * from extractCanonicalContact when available, falling back to extractEmail
 * for the raw responses bag.
 */

const crypto = require('crypto');
const { GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');

// Account=env: env var set in Terraform module wiring (M1.G6 picasso PR).
// Default below is STAGING-ONLY. A prod promotion MUST set this env var —
// silent fallback to the staging name would write to the wrong account and
// orphan every subject (mirrors the Python module's documented hazard).
const PII_SUBJECT_INDEX_TABLE = process.env.PII_SUBJECT_INDEX_TABLE
  || 'picasso-pii-subject-index-staging';

// Bounded retry for the get → conditional-put race. 3 attempts is ample:
// each lost race means a winner committed, so the next iteration's strongly-
// consistent read resolves it (matches pii_subject.py:_MAX_INDEX_ATTEMPTS).
const MAX_INDEX_ATTEMPTS = 3;

const GMAIL_DOMAINS = new Set(['gmail.com', 'googlemail.com']);
const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
// Response keys that conventionally hold the submitter's email.
const EMAIL_KEY_HINTS = ['email', 'e-mail', 'email_address', 'emailaddress'];

/**
 * A fresh opaque subject id. Carries zero information about the person.
 * Matches pii_subject.py:mint_pii_subject_id format ("psub_" + 32 hex chars).
 */
function mintPiiSubjectId() {
  // crypto.randomUUID() returns 36-char canonical form with hyphens; strip
  // them to match the Python uuid.uuid4().hex 32-char format exactly.
  return 'psub_' + crypto.randomUUID().replace(/-/g, '');
}

/**
 * Deterministic email normalization (PII Identity Contract §4).
 *
 * Pure function: same input → same output. Returns null for anything that is
 * not a syntactically usable address (caller still mints a subject id). Only
 * Gmail dot/plus aliasing is provider-guaranteed to deliver every variant to
 * one inbox, so only Gmail is safe to collapse — do NOT alter non-Gmail local
 * parts beyond lowercase/trim (audit 2026-05-18 #6, option A).
 */
function normalizeEmail(email) {
  if (email === null || email === undefined) return null;
  const e = String(email).trim();
  if (!e || /\s/.test(e)) return null;  // internal whitespace ⇒ not usable
  if (!e.includes('@')) return null;
  const atIdx = e.lastIndexOf('@');
  let local = e.slice(0, atIdx);
  let domain = e.slice(atIdx + 1);
  if (!local || !domain || local.includes('@')) return null;  // multi-@ malformed
  domain = domain.toLowerCase();
  local = local.toLowerCase();
  if (GMAIL_DOMAINS.has(domain)) {
    domain = 'gmail.com';
    if (local.includes('+')) {
      local = local.split('+', 1)[0];
    }
    local = local.replace(/\./g, '');
  }
  if (!local) return null;
  return `${local}@${domain}`;
}

/**
 * Best-effort: find the submitter's email in arbitrary form responses.
 *
 * First an email-named key (case-insensitive), then the first value that looks
 * like an address. Returns the raw string (caller normalizes).
 */
function extractEmail(responses) {
  if (!responses || typeof responses !== 'object') return null;
  for (const [key, value] of Object.entries(responses)) {
    if (typeof key === 'string'
        && EMAIL_KEY_HINTS.some(h => key.toLowerCase().includes(h))) {
      if (value && EMAIL_RE.test(String(value).trim())) {
        return String(value).trim();
      }
    }
  }
  for (const value of Object.values(responses)) {
    if (typeof value === 'string' && EMAIL_RE.test(value.trim())) {
      return value.trim();
    }
  }
  return null;
}

/**
 * Return the subject id for this submission, minting/indexing as needed.
 *
 * Always returns a usable id. A subject exists even when the submission has
 * no email (it just is not email-indexed). Never raises — index failures fall
 * back to the freshly-minted candidate so the submission still records a
 * stable id.
 *
 * `docClient` is required (the BSH module passes its own DynamoDBDocumentClient
 * instance so this module doesn't have to construct one and so unit tests can
 * inject a mock).
 *
 * @param {string} tenantId
 * @param {object} responses - the form responses bag
 * @param {object} options
 * @param {object} options.docClient - DynamoDBDocumentClient
 * @param {string} [options.knownEmail] - caller-supplied email (avoids re-extract
 *   when the caller already has it from extractCanonicalContact)
 * @returns {Promise<string>} pii_subject_id
 */
async function getOrCreatePiiSubjectId(tenantId, responses, options) {
  const { docClient, knownEmail } = options || {};
  const candidate = mintPiiSubjectId();

  // Sprint E1 / audit blocker B1 (cross-tenant collision): tenant_id missing
  // or literal 'unknown' MUST NOT be indexed. If two unrelated submissions
  // from differently-misconfigured tenants both fall through to
  // `tenant_id='unknown'`, they would collide on the index key
  // (tenant_id, normalized_email) and either reuse a single subject id across
  // distinct subjects or mint divergent ids depending on which got there
  // first. Mint UNINDEXED instead — the Phase-2 orphan-sweep gate covers
  // UNINDEXED rows by design. Mirror in pii_subject.py.
  if (!tenantId || tenantId === 'unknown') {
    console.warn(
      '[pii_subject] tenant_id missing/unknown — minting UNINDEXED '
      + 'pii_subject_id to avoid cross-tenant index collision'
    );
    return candidate;
  }

  if (!docClient) {
    // Caller misuse — but stay best-effort and never throw.
    console.warn('[pii_subject] docClient not provided; row will be UNINDEXED');
    return candidate;
  }

  try {
    const raw = knownEmail || extractEmail(responses);
    const normalized = normalizeEmail(raw);
    if (!normalized) {
      return candidate;
    }

    const key = { tenant_id: tenantId, normalized_email: normalized };
    const nowIso = new Date().toISOString();

    // Bounded GET → conditional-PUT loop. On a lost race, the winner's PUT is
    // already committed, so the next iteration's strongly-consistent GET
    // returns the winner's id — never mint a divergent id for a person who
    // already has an index entry (matches pii_subject.py gate blocker B1).
    for (let attempt = 0; attempt < MAX_INDEX_ATTEMPTS; attempt++) {
      const existing = await docClient.send(new GetCommand({
        TableName: PII_SUBJECT_INDEX_TABLE,
        Key: key,
        ConsistentRead: attempt > 0,
      }));
      const existingSid = existing.Item && existing.Item.pii_subject_id;
      // Require non-empty string: a corrupted/empty index value must not be
      // reused (silent divergence) nor spin the loop forever.
      if (typeof existingSid === 'string' && existingSid) {
        return existingSid;
      }
      try {
        await docClient.send(new PutCommand({
          TableName: PII_SUBJECT_INDEX_TABLE,
          Item: {
            tenant_id: tenantId,
            normalized_email: normalized,
            pii_subject_id: candidate,
            created_at: nowIso,
          },
          ConditionExpression: 'attribute_not_exists(normalized_email)',
        }));
        return candidate;
      } catch (err) {
        if (err && err.name === 'ConditionalCheckFailedException') {
          continue;  // someone won the race; loop re-reads consistently
        }
        throw err;  // bubbles to outer best-effort catch
      }
    }

    // Unresolved race: the submission still gets a usable id, but it is
    // UNINDEXED. Across multiple submissions this orphans the row — the
    // Phase-2 orphan-sweep gate covers it (see PII_IDENTITY_CONTRACT §7/§8
    // and the Python module's docstring for the full reasoning).
    console.warn(
      `[pii_subject] index race unresolved after ${MAX_INDEX_ATTEMPTS} attempts `
      + `(tenant=${tenantId}); row is UNINDEXED — incomplete-deletion risk, `
      + `requires Phase-2 orphan-sweep gate`
    );
    return candidate;
  } catch (err) {
    console.warn(
      `[pii_subject] index unavailable (non-fatal): ${err && err.name || err} `
      + `— row is UNINDEXED, requires Phase-2 orphan-sweep gate `
      + `(incomplete-deletion risk)`
    );
    return candidate;
  }
}

module.exports = {
  mintPiiSubjectId,
  normalizeEmail,
  extractEmail,
  getOrCreatePiiSubjectId,
  // Exposed for tests + integrations that need the table name.
  PII_SUBJECT_INDEX_TABLE,
};
