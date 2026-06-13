/**
 * Dub.co API client for the Attribution Mint Service.
 *
 * Auth/error/429 patterns follow the house conventions in
 * picasso-webscraping/rag-scraper/lib/dub.mjs — but this client uses
 * the attribution-specific externalId namespace (ep_ ULIDs, not KB slugs)
 * and POST /links (not /links/upsert) so mint fails loudly on suffix collision.
 *
 * KEY SECURITY RULES (C8/C4):
 * - API key is NEVER logged, NEVER stored in env vars.
 * - Only IDs and counts appear in logs; never full request/response bodies.
 * - No consumer identifiers ever flow to Dub.
 */

export const DUB_API_BASE = 'https://api.dub.co';

/**
 * Maximum milliseconds to wait on a Dub 429 Retry-After before capping.
 * 30 s is the house convention (matches the existing sleep cap in index.mjs).
 */
export const MAX_RETRY_WAIT_MS = 30_000;

/**
 * Dub domain used for all minted shortlinks.
 * Defaults to 'myrctr.link' so glue code needs no env var unless overriding.
 */
export const DUB_DOMAIN = process.env.DUB_DOMAIN ?? 'myrctr.link';

/**
 * Optional workspace scoping (C4 amendment 2026-06-12): the operator's Dub keys
 * are personal-type, which Dub only resolves to a workspace when requests carry
 * ?workspaceId=. Read at call time (not module load) so tests can toggle it.
 */
function wsQuery() {
  const ws = process.env.DUB_WORKSPACE_ID ?? '';
  return ws ? `?workspaceId=${encodeURIComponent(ws)}` : '';
}

/**
 * Build the QR URL for a Dub shortlink (public GET, no API call needed).
 * Spec: C4 — size 1000, level H (error correction high for print).
 * @param {string} shortLink
 * @returns {string}
 */
export function buildQrUrl(shortLink) {
  return `${DUB_API_BASE}/qr?url=${encodeURIComponent(shortLink)}&size=1000&level=H`;
}

/**
 * Mint a new Dub shortlink via POST /links.
 * Deliberately NOT /links/upsert — mint must fail loudly on suffix collision (C4).
 *
 * @param {string} apiKey - Dub API key (from Secrets Manager, never logged)
 * @param {object} params
 * @param {string} params.destinationUrl - Full destination URL including ?ep=...
 * @param {string} params.entryPointId  - ep_ ULID used as Dub externalId
 * @param {string} params.tenantId
 * @param {string|undefined} params.suffix - Optional custom key (<= 190 chars)
 * @returns {Promise<{ id: string, shortLink: string, key: string }>}
 * @throws {DubConflictError|DubRateLimitError|DubError}
 */
export async function dubMintLink(apiKey, { destinationUrl, entryPointId, tenantId, suffix }) {
  const payload = {
    url: destinationUrl,
    domain: DUB_DOMAIN,
    externalId: entryPointId,
    tenantId,
    // tagNames deliberately omitted (C4 amendment 2026-06-12): Dub 404s on
    // tags that don't pre-exist in the workspace, and per-tenant filtering
    // uses tenantId/externalId — Dub-side tags add nothing to the pipeline.
    comments: 'Attribution entry point — minted by MyRecruiter',
  };
  if (suffix) {
    payload.key = suffix;
  }

  const response = await fetch(`${DUB_API_BASE}/links${wsQuery()}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (response.status === 429) {
    // Parse Retry-After — the header may be a delta-seconds integer or an HTTP-date.
    // parseInt of an HTTP-date string produces NaN; guard so we never setTimeout(NaN).
    // Cap at MAX_RETRY_WAIT_MS / 1000 (30 s house convention).
    const raw = parseInt(response.headers.get('Retry-After') ?? '', 10);
    const retryAfter = Number.isFinite(raw) && raw > 0
      ? Math.min(raw, MAX_RETRY_WAIT_MS / 1000)
      : 1;
    throw new DubRateLimitError(retryAfter);
  }

  if (response.status === 409) {
    throw new DubConflictError(entryPointId);
  }

  if (!response.ok) {
    // Surface Dub's structured error code/message (no PII, no credentials in
    // Dub error bodies) — debugging blind cost three deploy cycles 2026-06-12.
    // Never log the raw body verbatim; extract the two known fields only.
    let dubCode = '';
    let dubMessage = '';
    try {
      const errBody = await response.json();
      dubCode = errBody?.error?.code ?? '';
      dubMessage = (errBody?.error?.message ?? '').slice(0, 200);
    } catch { /* non-JSON body — status alone */ }
    throw new DubError(
      `Dub POST /links returned HTTP ${response.status}` +
      (dubCode ? ` (${dubCode}: ${dubMessage})` : ''),
    );
  }

  const data = await response.json();
  // Log id only (no full body — C8.10)
  return { id: data.id, shortLink: data.shortLink, key: data.key };
}

/**
 * Repoint an existing Dub link to a new destination URL via PATCH /links/ext_{entryPointId}.
 *
 * @param {string} apiKey - Dub API key (from Secrets Manager, never logged)
 * @param {string} entryPointId - ep_ id used as Dub externalId (ext_ prefix added here)
 * @param {string} newUrl - New destination URL (already has ?ep= appended by caller)
 * @returns {Promise<void>}
 * @throws {DubRateLimitError|DubError}
 */
export async function dubRepointLink(apiKey, entryPointId, newUrl) {
  const response = await fetch(`${DUB_API_BASE}/links/ext_${entryPointId}${wsQuery()}`, {
    method: 'PATCH',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ url: newUrl }),
  });

  if (response.status === 429) {
    const raw = parseInt(response.headers.get('Retry-After') ?? '', 10);
    const retryAfter = Number.isFinite(raw) && raw > 0
      ? Math.min(raw, MAX_RETRY_WAIT_MS / 1000)
      : 1;
    throw new DubRateLimitError(retryAfter);
  }

  if (response.status === 404) {
    let dubCode = '';
    let dubMessage = '';
    try {
      const errBody = await response.json();
      dubCode = errBody?.error?.code ?? '';
      dubMessage = (errBody?.error?.message ?? '').slice(0, 200);
    } catch { /* non-JSON body — status alone */ }
    throw new DubError(
      `Dub PATCH /links/ext_${entryPointId} returned HTTP 404` +
      (dubCode ? ` (${dubCode}: ${dubMessage})` : ''),
    );
  }

  if (!response.ok) {
    let dubCode = '';
    let dubMessage = '';
    try {
      const errBody = await response.json();
      dubCode = errBody?.error?.code ?? '';
      dubMessage = (errBody?.error?.message ?? '').slice(0, 200);
    } catch { /* non-JSON body — status alone */ }
    throw new DubError(
      `Dub PATCH /links/ext_${entryPointId} returned HTTP ${response.status}` +
      (dubCode ? ` (${dubCode}: ${dubMessage})` : ''),
    );
  }
}

/**
 * Delete a Dub link by its internal id (best-effort cleanup after registry failure).
 * Errors are swallowed; the caller logs them as warnings.
 *
 * @param {string} apiKey
 * @param {string} dubLinkId
 * @returns {Promise<void>}
 */
export async function dubDeleteLink(apiKey, dubLinkId) {
  const response = await fetch(`${DUB_API_BASE}/links/${dubLinkId}${wsQuery()}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!response.ok) {
    throw new DubError(`Dub DELETE /links/${dubLinkId} returned HTTP ${response.status}`);
  }
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

export class DubError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DubError';
  }
}

export class DubConflictError extends DubError {
  constructor(entryPointId) {
    super(`Dub 409: externalId already exists for ${entryPointId}`);
    this.name = 'DubConflictError';
    this.entryPointId = entryPointId;
  }
}

export class DubRateLimitError extends DubError {
  constructor(retryAfterSeconds) {
    super(`Dub 429: rate limited, Retry-After ${retryAfterSeconds}s`);
    this.name = 'DubRateLimitError';
    this.retryAfterSeconds = retryAfterSeconds;
  }
}
