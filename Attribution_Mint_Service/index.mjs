/**
 * Attribution_Mint_Service — Lambda handler
 *
 * Runtime: Node.js 20.x ESM
 * Handler: index.handler
 *
 * Implements the C4b mint action and C4b repoint extension:
 *   mint:
 *     1. Validate request
 *     2. Fetch Dub API key from Secrets Manager
 *     3. Generate entry_point_id (ep_ + ULID)
 *     4. Build destination URL (appends ?ep={id})
 *     5. POST to Dub /links
 *     6. Conditional-put registry record to DynamoDB
 *     7. On registry failure: best-effort DELETE the Dub link, surface error
 *   repoint:
 *     1. Validate entry_point_id format + target.url
 *     2. Fetch API key
 *     3. getEntryPoint (cross-tenant guard — absent = NOT_FOUND)
 *     4. Build new destination URL (re-appends ?ep=)
 *     5. PATCH Dub /links/ext_{id} with one 429 retry
 *     6. updateDestination in registry
 *     7. Return updated entry_point (short_link/qr_url unchanged)
 *
 * Error codes (C4b):
 *   VALIDATION   — bad input
 *   DUB_ERROR    — Dub API failure or key not configured
 *   SUFFIX_TAKEN — Dub 409 on custom key collision (mint only)
 *   CONFLICT     — Dub 409 on externalId without existing registry row (mint only)
 *   NOT_FOUND    — entry point not found or cross-tenant mismatch (repoint only)
 *
 * PII rules (C8):
 *   - Never log request/response payloads; log ids and counts only.
 *   - Never log the API key.
 *   - Registry holds NO person fields (no created_by, no emails).
 */

import { validateMintRequest, validateRepointRequest } from './validation.mjs';
import { generateULID } from './ulid.mjs';
import {
  dubMintLink,
  dubDeleteLink,
  dubRepointLink,
  buildQrUrl,
  DubConflictError,
  DubRateLimitError,
  DubError,
} from './dubClient.mjs';
import { putEntryPoint, getEntryPoint, updateDestination, ConditionalCheckFailedException } from './registry.mjs';
import { getDubApiKey } from './secrets.mjs';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Append ?ep={id} to the destination URL using the URL API so that
 * fragment-bearing URLs (e.g. https://host/path#anchor) are handled
 * correctly — searchParams are placed before the fragment, not inside it.
 */
function buildDestinationUrl(baseUrl, entryPointId) {
  const u = new URL(baseUrl);
  u.searchParams.set('ep', entryPointId);
  return u.href;
}

/**
 * Sleep for ms milliseconds (used for 429 Retry-After).
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Core mint logic (exported for testing)
// ---------------------------------------------------------------------------

/**
 * Execute the mint operation.
 *
 * @param {object} body - validated request body
 * @returns {Promise<{ ok: boolean, entry_point?: object, error?: object }>}
 */
export async function mintEntryPoint(body) {
  const { tenant_id, label, channel, campaign, placement, target, suffix } = body;

  // Step 1: Fetch API key (graceful absent)
  const apiKey = await getDubApiKey();
  if (!apiKey) {
    console.warn('[Attribution/mint] DUB_SECRET_NAME not configured or secret is empty', {
      tenant_id,
    });
    return {
      ok: false,
      error: { code: 'DUB_ERROR', message: 'Dub API key is not configured' },
    };
  }

  // Step 2: Generate entry_point_id
  const entryPointId = `ep_${generateULID()}`;
  const createdAt = new Date().toISOString();

  // Step 3: Build destination URL (append ?ep=)
  const destinationUrl = buildDestinationUrl(target.url, entryPointId);

  // Step 4: Mint Dub link (with one 429 retry)
  let dubResult;
  try {
    dubResult = await dubMintLink(apiKey, {
      destinationUrl,
      entryPointId,
      tenantId: tenant_id,
      suffix: suffix || undefined,
    });
  } catch (err) {
    if (err instanceof DubRateLimitError) {
      // Honor Retry-After once, then retry
      const waitMs = Math.min(err.retryAfterSeconds * 1000, 30_000);
      console.warn('[Attribution/mint] Dub 429 — waiting before retry', {
        tenant_id,
        retryAfterSeconds: err.retryAfterSeconds,
      });
      await sleep(waitMs);
      try {
        dubResult = await dubMintLink(apiKey, {
          destinationUrl,
          entryPointId,
          tenantId: tenant_id,
          suffix: suffix || undefined,
        });
      } catch (retryErr) {
        return await _handleDubError(retryErr, tenant_id, entryPointId, suffix);
      }
    } else {
      return await _handleDubError(err, tenant_id, entryPointId, suffix);
    }
  }

  // Step 5: Conditional-put registry record (C3 — no person fields)
  const registryRecord = {
    tenant_id,
    entry_point_id: entryPointId,
    label,
    channel,
    campaign,
    placement,
    target_type: target.type,
    destination_url: destinationUrl,
    dub_link_id: dubResult.id,
    dub_short_link: dubResult.shortLink,
    dub_key: dubResult.key,
    status: 'active',
    created_at: createdAt,
  };

  try {
    await putEntryPoint(registryRecord);
  } catch (err) {
    if (err instanceof ConditionalCheckFailedException || err.name === 'ConditionalCheckFailedException') {
      // Double-mint: conditional put failed — shouldn't happen with fresh ULIDs, but log and surface
      console.error('[Attribution/mint] Registry conditional-put failed (duplicate key — unexpected)', {
        tenant_id,
        entry_point_id: entryPointId,
        dub_link_id: dubResult.id,
      });
      // Best-effort cleanup of the Dub link
      await _tryCleanupDubLink(apiKey, dubResult.id, entryPointId, tenant_id);
      return {
        ok: false,
        error: { code: 'CONFLICT', message: 'Entry point id collision — please retry' },
      };
    }

    // Other DynamoDB error: best-effort Dub cleanup, then surface
    console.error('[Attribution/mint] Registry put failed after successful Dub mint', {
      tenant_id,
      entry_point_id: entryPointId,
      dub_link_id: dubResult.id,
      errorName: err.name,
    });
    await _tryCleanupDubLink(apiKey, dubResult.id, entryPointId, tenant_id);
    return {
      ok: false,
      error: {
        code: 'DUB_ERROR',
        message: 'Registry write failed after Dub link was minted; link has been cleaned up',
      },
    };
  }

  // Step 6: Build success response
  const shortLink = dubResult.shortLink;
  const qrUrl = buildQrUrl(shortLink);

  console.info('[Attribution/mint] Entry point minted', {
    tenant_id,
    entry_point_id: entryPointId,
    channel,
  });

  return {
    ok: true,
    entry_point: {
      entry_point_id: entryPointId,
      short_link: shortLink,
      qr_url: qrUrl,
      destination_url: destinationUrl,
      dub_link_id: dubResult.id,
      created_at: createdAt,
    },
  };
}

// ---------------------------------------------------------------------------
// Core repoint logic (exported for testing)
// ---------------------------------------------------------------------------

/**
 * Execute the repoint operation — change the destination URL of an existing
 * entry point. The printed QR code and short_link are unchanged.
 *
 * @param {object} body - validated request body
 * @returns {Promise<{ ok: boolean, entry_point?: object, error?: object }>}
 */
export async function repointEntryPoint(body) {
  const { tenant_id, entry_point_id, target } = body;

  // Step 1: Fetch API key (graceful absent)
  const apiKey = await getDubApiKey();
  if (!apiKey) {
    console.warn('[Attribution/repoint] DUB_SECRET_NAME not configured or secret is empty', {
      tenant_id,
      entry_point_id,
    });
    return {
      ok: false,
      error: { code: 'DUB_ERROR', message: 'Dub API key is not configured' },
    };
  }

  // Step 2: Look up existing registry row (cross-tenant guard)
  let existingRow;
  try {
    existingRow = await getEntryPoint(tenant_id, entry_point_id);
  } catch (err) {
    console.warn('[Attribution/repoint] Registry lookup failed', {
      tenant_id,
      entry_point_id,
      errorName: err.name,
    });
    return {
      ok: false,
      error: { code: 'DUB_ERROR', message: 'Registry lookup failed; please retry' },
    };
  }

  if (!existingRow) {
    return {
      ok: false,
      error: { code: 'NOT_FOUND', message: 'entry point not found' },
    };
  }

  // Step 3: Build new destination URL (re-append ?ep= — never trust caller-supplied ep)
  const newDestination = buildDestinationUrl(target.url, entry_point_id);
  const updatedAt = new Date().toISOString();

  // Step 4: PATCH Dub link (with one 429 retry)
  try {
    await dubRepointLink(apiKey, entry_point_id, newDestination);
  } catch (err) {
    if (err instanceof DubRateLimitError) {
      const waitMs = Math.min(err.retryAfterSeconds * 1000, 30_000);
      console.warn('[Attribution/repoint] Dub 429 — waiting before retry', {
        tenant_id,
        entry_point_id,
        retryAfterSeconds: err.retryAfterSeconds,
      });
      await sleep(waitMs);
      try {
        await dubRepointLink(apiKey, entry_point_id, newDestination);
      } catch (retryErr) {
        return _handleRepointDubError(retryErr, tenant_id, entry_point_id);
      }
    } else {
      return _handleRepointDubError(err, tenant_id, entry_point_id);
    }
  }

  // Step 5: Update registry destination_url
  try {
    await updateDestination(tenant_id, entry_point_id, newDestination);
  } catch (err) {
    console.error('[Attribution/repoint] Registry update failed after successful Dub PATCH', {
      tenant_id,
      entry_point_id,
      errorName: err.name,
    });
    // Dub link is already updated; registry is stale but the printed QR still works.
    // Surface as DUB_ERROR so WS-D can present a retriable error.
    return {
      ok: false,
      error: { code: 'DUB_ERROR', message: 'Registry update failed after Dub link was repointed; retry to sync' },
    };
  }

  // Step 6: Build success response (short_link and qr_url are UNCHANGED)
  const shortLink = existingRow.dub_short_link;
  const qrUrl = buildQrUrl(shortLink);

  console.info('[Attribution/repoint] Entry point repointed', {
    tenant_id,
    entry_point_id,
  });

  return {
    ok: true,
    entry_point: {
      entry_point_id,
      short_link: shortLink,
      qr_url: qrUrl,
      destination_url: newDestination,
      dub_link_id: existingRow.dub_link_id,
      updated_at: updatedAt,
    },
  };
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

function _handleRepointDubError(err, tenant_id, entry_point_id) {
  if (err instanceof DubRateLimitError) {
    return {
      ok: false,
      error: { code: 'DUB_ERROR', message: `Dub rate limit exceeded; retry after ${err.retryAfterSeconds}s` },
    };
  }
  // Generic DubError (includes 404 from Dub when link not found there)
  console.warn('[Attribution/repoint] Dub API error', { tenant_id, entry_point_id, errorName: err.name });
  return {
    ok: false,
    error: { code: 'DUB_ERROR', message: err.message ?? 'Dub API error' },
  };
}

async function _handleDubError(err, tenant_id, entryPointId, suffix) {
  if (err instanceof DubConflictError) {
    if (suffix) {
      // A custom suffix was provided — this is a key collision (SUFFIX_TAKEN).
      return {
        ok: false,
        error: { code: 'SUFFIX_TAKEN', message: `Custom suffix is already taken` },
      };
    }
    // No custom suffix — externalId collision means Dub already has this ep_ id.
    // Per C4: check the registry. If the row exists, this is an idempotent retry
    // (previous mint succeeded but the caller did not receive the response).
    // Return ok:true with stored fields. If the row is absent, the Dub link is an
    // orphan (registry write failed after a prior Dub mint) — return CONFLICT.
    let existingRow = null;
    try {
      existingRow = await getEntryPoint(tenant_id, entryPointId);
    } catch (lookupErr) {
      console.warn('[Attribution/mint] Registry lookup failed on Dub 409', {
        tenant_id,
        entry_point_id: entryPointId,
        errorName: lookupErr.name,
      });
    }
    if (existingRow) {
      // Idempotent: row exists — return the stored fields as success.
      return {
        ok: true,
        entry_point: {
          entry_point_id: existingRow.entry_point_id,
          short_link: existingRow.dub_short_link,
          qr_url: buildQrUrl(existingRow.dub_short_link),
          destination_url: existingRow.destination_url,
          dub_link_id: existingRow.dub_link_id,
          created_at: existingRow.created_at,
        },
      };
    }
    return {
      ok: false,
      error: { code: 'CONFLICT', message: 'Dub link already exists for this entry point id; registry inconsistency' },
    };
  }
  if (err instanceof DubRateLimitError) {
    return {
      ok: false,
      error: { code: 'DUB_ERROR', message: `Dub rate limit exceeded; retry after ${err.retryAfterSeconds}s` },
    };
  }
  // Generic Dub error
  console.warn('[Attribution/mint] Dub API error', { tenant_id, errorName: err.name });
  return {
    ok: false,
    error: { code: 'DUB_ERROR', message: err.message ?? 'Dub API error' },
  };
}

async function _tryCleanupDubLink(apiKey, dubLinkId, entryPointId, tenant_id) {
  try {
    await dubDeleteLink(apiKey, dubLinkId);
    console.info('[Attribution/mint] Dub link cleaned up after registry failure', {
      dub_link_id: dubLinkId,
      entry_point_id: entryPointId,
    });
  } catch (cleanupErr) {
    // Orphan is tolerable: unregistered ep_ ids resolve to "website" downstream (C4 atomicity note)
    console.warn('[Attribution/mint] Dub link cleanup FAILED — orphan link exists', {
      dub_link_id: dubLinkId,
      entry_point_id: entryPointId,
      tenant_id,
      cleanupErrorName: cleanupErr.name,
    });
  }
}

// ---------------------------------------------------------------------------
// Lambda handler
// ---------------------------------------------------------------------------

export async function handler(event) {
  // IAM-invoke-only: the event IS the body (plain JSON object).
  // The API Gateway string-body path has been removed — this Lambda is invoked
  // directly via IAM; a string body surface is unnecessary and widens attack surface.
  const body = event;

  if (body.action === 'mint') {
    const validationError = validateMintRequest(body);
    if (validationError) {
      return {
        statusCode: 400,
        body: JSON.stringify({ ok: false, error: validationError }),
      };
    }

    const result = await mintEntryPoint(body);
    const statusCode = result.ok ? 201 : (
      result.error?.code === 'VALIDATION' ? 400 :
      result.error?.code === 'SUFFIX_TAKEN' ? 409 :
      result.error?.code === 'CONFLICT' ? 409 :
      502
    );
    return { statusCode, body: JSON.stringify(result) };
  }

  if (body.action === 'repoint') {
    const validationError = validateRepointRequest(body);
    if (validationError) {
      return {
        statusCode: 400,
        body: JSON.stringify({ ok: false, error: validationError }),
      };
    }

    const result = await repointEntryPoint(body);
    const statusCode = result.ok ? 200 : (
      result.error?.code === 'VALIDATION' ? 400 :
      result.error?.code === 'NOT_FOUND' ? 404 :
      502
    );
    return { statusCode, body: JSON.stringify(result) };
  }

  return {
    statusCode: 400,
    body: JSON.stringify({ ok: false, error: { code: 'VALIDATION', message: 'Unknown action' } }),
  };
}
