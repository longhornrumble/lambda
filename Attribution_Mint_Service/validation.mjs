/**
 * Input validation for the mint action.
 * Returns null if valid, or a { code, message } error object.
 *
 * Enforces C8 PII taxonomy guardrails:
 * - Length caps on label/campaign/placement
 * - Reject '@' in label/campaign/placement (re-identification control)
 * - Destination URL: https only, no userinfo, query params limited to utm_*, ep appended by service
 * - Channel must be "standalone" | "campaign"
 */

const VALID_CHANNELS = new Set(['standalone', 'campaign']);
const MAX_LABEL_LEN = 128;
const MAX_CAMPAIGN_LEN = 128;
const MAX_PLACEMENT_LEN = 128;
const MAX_SUFFIX_LEN = 190;
// tenant_id becomes a DDB partition key and Dub tag — cap it defensively.
const MAX_TENANT_ID_LEN = 128;

// Only utm_* query params are allowed in the destination URL (ep is added by the service).
const ALLOWED_QUERY_PARAM_RE = /^utm_/;

// Destination URL validation
const FORBIDDEN_SCHEMES = ['mailto:', 'javascript:'];

/**
 * Validate a destination URL per C4/C8.15:
 * - Must be https:
 * - No userinfo (user:pass@host)
 * - Query params limited to utm_* (ep will be appended by the service)
 * - No mailto: / javascript:
 * @param {string} urlStr
 * @returns {string|null} error message, or null if valid
 */
export function validateDestinationUrl(urlStr) {
  if (!urlStr || typeof urlStr !== 'string') {
    return 'target.url is required';
  }

  for (const scheme of FORBIDDEN_SCHEMES) {
    if (urlStr.toLowerCase().startsWith(scheme)) {
      return `Forbidden URL scheme: ${scheme}`;
    }
  }

  let parsed;
  try {
    parsed = new URL(urlStr);
  } catch {
    return 'target.url is not a valid URL';
  }

  if (parsed.protocol !== 'https:') {
    return 'target.url must use https:';
  }

  if (parsed.username || parsed.password) {
    return 'target.url must not contain userinfo (user:pass@host)';
  }

  for (const [key] of parsed.searchParams.entries()) {
    if (!ALLOWED_QUERY_PARAM_RE.test(key)) {
      return `Disallowed query param in target.url: "${key}" (only utm_* allowed; ep is appended by the service)`;
    }
  }

  return null;
}

/**
 * Validate a taxonomy field (label, campaign, placement) per C8.13.
 * NFKC-normalises the value before the @ check so that full-width variants
 * such as U+FF20 (＠) are caught alongside the ASCII @ (U+0040).
 * @param {string} value
 * @param {string} fieldName
 * @param {number} maxLen
 * @returns {string|null} error message, or null
 */
function validateTaxonomyField(value, fieldName, maxLen) {
  if (!value || typeof value !== 'string') {
    return `${fieldName} is required`;
  }
  if (value.length > maxLen) {
    return `${fieldName} must be ${maxLen} characters or fewer (got ${value.length})`;
  }
  // Normalize to NFC-compatible form before checking for @ variants (C8.13 unicode-bypass guard).
  if (value.normalize('NFKC').includes('@')) {
    return `${fieldName} must not contain '@' — never name an individual in attribution taxonomy`;
  }
  return null;
}

/**
 * Validate the full mint request body.
 * @param {object} body
 * @returns {{ code: string, message: string }|null} null = valid
 */
export function validateMintRequest(body) {
  if (!body || typeof body !== 'object') {
    return { code: 'VALIDATION', message: 'Request body must be an object' };
  }

  const { tenant_id, label, channel, campaign, placement, target, suffix } = body;

  if (!tenant_id || typeof tenant_id !== 'string' || !tenant_id.trim()) {
    return { code: 'VALIDATION', message: 'tenant_id is required' };
  }
  if (tenant_id.length > MAX_TENANT_ID_LEN) {
    return { code: 'VALIDATION', message: `tenant_id must be ${MAX_TENANT_ID_LEN} characters or fewer (got ${tenant_id.length})` };
  }

  const labelErr = validateTaxonomyField(label, 'label', MAX_LABEL_LEN);
  if (labelErr) return { code: 'VALIDATION', message: labelErr };

  if (!VALID_CHANNELS.has(channel)) {
    return { code: 'VALIDATION', message: `channel must be "standalone" or "campaign" (got: ${JSON.stringify(channel)})` };
  }

  const campaignErr = validateTaxonomyField(campaign, 'campaign', MAX_CAMPAIGN_LEN);
  if (campaignErr) return { code: 'VALIDATION', message: campaignErr };

  const placementErr = validateTaxonomyField(placement, 'placement', MAX_PLACEMENT_LEN);
  if (placementErr) return { code: 'VALIDATION', message: placementErr };

  if (!target || typeof target !== 'object') {
    return { code: 'VALIDATION', message: 'target is required' };
  }

  const validTargetTypes = new Set(['standalone_chat', 'site_url']);
  if (!validTargetTypes.has(target.type)) {
    return { code: 'VALIDATION', message: `target.type must be "standalone_chat" or "site_url"` };
  }

  const urlErr = validateDestinationUrl(target.url);
  if (urlErr) return { code: 'VALIDATION', message: urlErr };

  if (suffix !== undefined && suffix !== null) {
    if (typeof suffix !== 'string') {
      return { code: 'VALIDATION', message: 'suffix must be a string when provided' };
    }
    // Treat empty string as absent — a zero-length key would silently drop through
    // validation and produce unexpected Dub API behavior.
    if (suffix.length === 0) {
      return { code: 'VALIDATION', message: 'suffix must not be an empty string; omit the field to use an auto-generated key' };
    }
    if (suffix.length > MAX_SUFFIX_LEN) {
      return { code: 'VALIDATION', message: `suffix must be ${MAX_SUFFIX_LEN} characters or fewer` };
    }
  }

  // Item 10 (C4 obligation): when no tenant site-domain list is available to verify
  // the destination URL's domain, emit a warn so a CloudWatch metric filter can count it.
  // This is a best-effort observability hook — it does NOT block the mint.
  console.warn('[Attribution/mint] destination-domain-unverified', {
    tenant_id,
    metric: 'attribution.destination.unverified',
  });

  return null;
}
