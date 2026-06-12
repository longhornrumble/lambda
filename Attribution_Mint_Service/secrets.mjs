/**
 * Secrets Manager access for the Dub API key.
 *
 * C4/C8 rules:
 * - Secret name comes from env DUB_SECRET_NAME (never a plaintext env var for the key itself).
 * - The key is NEVER logged at any level.
 * - If the secret is absent, empty, or Secrets Manager is unavailable, return null;
 *   the caller degrades gracefully to DUB_ERROR (never crashes).
 *
 * The resolved key is cached in module scope after the first cold-start fetch
 * so subsequent invocations within the same Lambda container don't re-fetch.
 */

import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';

let _cachedKey = undefined; // undefined = not yet fetched; null = fetch attempted, absent/empty

let _smClient = null;

function getSmClient() {
  if (!_smClient) {
    _smClient = new SecretsManagerClient({ region: process.env.AWS_REGION ?? 'us-east-1' });
  }
  return _smClient;
}

/**
 * Override the Secrets Manager client (used by tests).
 * Also resets the cache so fresh tests start clean.
 * @param {import('@aws-sdk/client-secrets-manager').SecretsManagerClient|null} client
 */
export function setSmClient(client) {
  _smClient = client;
  _cachedKey = undefined; // reset cache so the new client is actually used
}

/**
 * Fetch the Dub API key from Secrets Manager.
 * Returns null (gracefully) if:
 *   - DUB_SECRET_NAME env var is not set or empty
 *   - Secrets Manager returns no secret / empty string
 *   - Secrets Manager throws (network error, permission denied, etc.)
 *
 * @returns {Promise<string|null>}
 */
export async function getDubApiKey() {
  if (_cachedKey !== undefined) return _cachedKey;

  const secretName = process.env.DUB_SECRET_NAME;
  if (!secretName) {
    // Missing env var — degrade gracefully (C4)
    _cachedKey = null;
    return null;
  }

  try {
    const client = getSmClient();
    const resp = await client.send(new GetSecretValueCommand({ SecretId: secretName }));
    const value = resp.SecretString ?? '';
    if (!value.trim()) {
      // Secret is present but genuinely empty — cache null so we don't re-fetch.
      _cachedKey = null;
      return null;
    }
    _cachedKey = value.trim();
    return _cachedKey;
  } catch (err) {
    // Never log the secret name value that was attempted — only that fetch failed.
    // Do NOT cache null on transient errors: leave _cachedKey as undefined so the
    // next invocation retries. Caching null here would poison the container for
    // its lifetime after a single network blip.
    console.warn('[Attribution/secrets] Failed to fetch DUB_SECRET_NAME; mint will degrade to DUB_ERROR', {
      errorName: err.name,
    });
    // _cachedKey stays undefined — retry on next call.
    return null;
  }
}
