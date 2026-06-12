/**
 * Attribution_Mint_Service unit tests.
 *
 * All external surfaces (fetch, DynamoDB, Secrets Manager) are mocked.
 * No live AWS or Dub calls.
 *
 * Coverage targets:
 *  - Happy mint (no suffix, with suffix)
 *  - SUFFIX_TAKEN (Dub 409 on custom key)
 *  - CONFLICT (Dub 409 on externalId, no registry row)
 *  - Dub 5xx → DUB_ERROR
 *  - Dub 409 on externalId with existing registry row → (not tested here — WS-B
 *    produces CONFLICT in both cases since fresh ULID means externalId collision
 *    is indistinguishable; the 409+suffix=SUFFIX_TAKEN path covers the suffix case)
 *  - Registry conditional-put failure after Dub success → cleanup + error
 *  - Validation failures: bad channel, missing campaign/placement, @ in labels,
 *    non-https destination, disallowed query params, label >128
 *  - Secret absent/empty → DUB_ERROR "not configured" (no crash)
 *  - 429 with Retry-After honored once then surfaced
 */

import { jest, describe, it, expect, beforeEach, afterEach } from '@jest/globals';

// ---- Module-level mocks ----
// We mock the low-level modules so the handler under test uses fake clients.

// Mock fetch globally
globalThis.fetch = jest.fn();

// We'll use the setDocClient / setSmClient injection points.
// Import them after setting up our fakes.
import { setDocClient } from '../registry.mjs';
import { setSmClient } from '../secrets.mjs';

// Import the units under test
import { mintEntryPoint, handler } from '../index.mjs';
import { validateMintRequest, validateDestinationUrl } from '../validation.mjs';
import { generateULID } from '../ulid.mjs';
import { buildQrUrl } from '../dubClient.mjs';

// ---- Helpers ----

function makeDubSuccessResponse(overrides = {}) {
  return {
    id: 'dub_link_001',
    shortLink: 'https://myrctr.link/gala-tents',
    key: overrides.key ?? 'gala-tents',
    ...overrides,
  };
}

function makeValidBody(overrides = {}) {
  return {
    action: 'mint',
    tenant_id: 'TENANT123',
    label: 'Gala Tents',
    channel: 'campaign',
    campaign: 'Gala 2026',
    placement: 'Homepage Hero',
    target: { type: 'site_url', url: 'https://example.org/gala' },
    ...overrides,
  };
}

/** Create a mock DynamoDB DocumentClient that succeeds on both put and get */
function makeDdbMock({ putThrows = null, getResult = null } = {}) {
  return {
    send: jest.fn(async (command) => {
      const name = command.constructor?.name ?? '';
      if (name === 'PutCommand' || command instanceof Object && command.input?.ConditionExpression) {
        if (putThrows) throw putThrows;
        return {};
      }
      if (name === 'GetCommand') {
        return { Item: getResult };
      }
      return {};
    }),
  };
}

/** Create a mock SecretsManager client that returns the given secret string */
function makeSmMock(secretString) {
  return {
    send: jest.fn(async () => {
      if (secretString === null) throw new Error('ResourceNotFoundException');
      return { SecretString: secretString };
    }),
  };
}

// ---- Test setup ----

beforeEach(() => {
  jest.clearAllMocks();
  jest.useRealTimers(); // always restore real timers before each test
  process.env.ENTRY_POINTS_TABLE = 'picasso-entry-points-test';
  process.env.DUB_SECRET_NAME = 'picasso-dub-api-key-test';
  process.env.AWS_REGION = 'us-east-1';

  // Reset secrets cache by replacing the client (setSmClient resets _cachedKey)
  setSmClient(makeSmMock('test-dub-key'));
  setDocClient(makeDdbMock());

  // Default fetch: Dub mint succeeds
  globalThis.fetch.mockResolvedValue({
    ok: true,
    status: 200,
    headers: { get: () => null },
    json: async () => makeDubSuccessResponse(),
  });
});

afterEach(() => {
  jest.useRealTimers();
  delete process.env.ENTRY_POINTS_TABLE;
  delete process.env.DUB_SECRET_NAME;
});

// ============================================================
// ULID
// ============================================================

describe('generateULID', () => {
  it('returns a 26-character uppercase Crockford base32 string', () => {
    const id = generateULID();
    expect(id).toHaveLength(26);
    expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
  });

  it('generates unique values', () => {
    const a = generateULID();
    const b = generateULID();
    expect(a).not.toBe(b);
  });

  it('entry_point_id has ep_ prefix', () => {
    const id = `ep_${generateULID()}`;
    expect(id).toMatch(/^ep_[0-9A-HJKMNP-TV-Z]{26}$/);
  });
});

// ============================================================
// buildQrUrl
// ============================================================

describe('buildQrUrl', () => {
  it('builds correct QR URL with size=1000 and level=H', () => {
    const url = buildQrUrl('https://myrctr.link/abc');
    expect(url).toBe(
      'https://api.dub.co/qr?url=https%3A%2F%2Fmyrctr.link%2Fabc&size=1000&level=H'
    );
  });
});

// ============================================================
// validateDestinationUrl
// ============================================================

describe('validateDestinationUrl', () => {
  it('accepts a plain https URL', () => {
    expect(validateDestinationUrl('https://example.org/path')).toBeNull();
  });

  it('accepts https URL with utm_ params', () => {
    expect(validateDestinationUrl('https://example.org/?utm_source=chat&utm_campaign=gala')).toBeNull();
  });

  it('rejects http://', () => {
    expect(validateDestinationUrl('http://example.org')).toMatch(/https/);
  });

  it('rejects mailto:', () => {
    expect(validateDestinationUrl('mailto:user@example.org')).toMatch(/Forbidden/);
  });

  it('rejects javascript:', () => {
    expect(validateDestinationUrl('javascript:alert(1)')).toMatch(/Forbidden/);
  });

  it('rejects URL with userinfo', () => {
    expect(validateDestinationUrl('https://user:pass@example.org')).toMatch(/userinfo/);
  });

  it('rejects URL with disallowed query params', () => {
    expect(validateDestinationUrl('https://example.org/?ep=whatever')).toMatch(/Disallowed/);
    expect(validateDestinationUrl('https://example.org/?tracking=123')).toMatch(/Disallowed/);
  });

  it('rejects missing/null url', () => {
    expect(validateDestinationUrl(null)).toMatch(/required/);
    expect(validateDestinationUrl('')).toMatch(/required/);
  });

  it('rejects invalid URL string', () => {
    expect(validateDestinationUrl('not-a-url')).toMatch(/valid URL/);
  });
});

// ============================================================
// validateMintRequest
// ============================================================

describe('validateMintRequest', () => {
  it('returns null for a valid request', () => {
    expect(validateMintRequest(makeValidBody())).toBeNull();
  });

  it('rejects missing tenant_id', () => {
    const err = validateMintRequest(makeValidBody({ tenant_id: '' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/tenant_id/);
  });

  it('rejects bad channel', () => {
    const err = validateMintRequest(makeValidBody({ channel: 'email' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/channel/);
  });

  it('rejects missing campaign', () => {
    const err = validateMintRequest(makeValidBody({ campaign: '' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/campaign/);
  });

  it('rejects missing placement', () => {
    const err = validateMintRequest(makeValidBody({ placement: '' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/placement/);
  });

  it('rejects @ in label', () => {
    const err = validateMintRequest(makeValidBody({ label: 'jane@example.com' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/@/);
  });

  it('rejects @ in campaign', () => {
    const err = validateMintRequest(makeValidBody({ campaign: 'user@org' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/@/);
  });

  it('rejects @ in placement', () => {
    const err = validateMintRequest(makeValidBody({ placement: 'lead@campaign' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/@/);
  });

  it('rejects label > 128 chars', () => {
    const err = validateMintRequest(makeValidBody({ label: 'x'.repeat(129) }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/128/);
  });

  it('rejects non-https destination', () => {
    const err = validateMintRequest(
      makeValidBody({ target: { type: 'site_url', url: 'http://example.org' } })
    );
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/https/);
  });

  it('rejects disallowed query params in destination', () => {
    const err = validateMintRequest(
      makeValidBody({ target: { type: 'site_url', url: 'https://example.org/?tracking=abc' } })
    );
    expect(err?.code).toBe('VALIDATION');
  });

  it('rejects suffix > 190 chars', () => {
    const err = validateMintRequest(makeValidBody({ suffix: 'a'.repeat(191) }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/190/);
  });

  it('accepts suffix exactly 190 chars', () => {
    expect(validateMintRequest(makeValidBody({ suffix: 'a'.repeat(190) }))).toBeNull();
  });

  it('accepts request without suffix', () => {
    const body = makeValidBody();
    delete body.suffix;
    expect(validateMintRequest(body)).toBeNull();
  });
});

// ============================================================
// Happy path mint
// ============================================================

describe('mintEntryPoint — happy path', () => {
  it('returns ok:true with expected shape on success', async () => {
    const result = await mintEntryPoint(makeValidBody());

    expect(result.ok).toBe(true);
    expect(result.entry_point.entry_point_id).toMatch(/^ep_[0-9A-HJKMNP-TV-Z]{26}$/);
    expect(result.entry_point.short_link).toBe('https://myrctr.link/gala-tents');
    expect(result.entry_point.qr_url).toMatch(/api\.dub\.co\/qr/);
    expect(result.entry_point.destination_url).toMatch(/\?ep=ep_/);
    expect(result.entry_point.dub_link_id).toBe('dub_link_001');
    expect(result.entry_point.created_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it('passes custom suffix to Dub', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      headers: { get: () => null },
      json: async () => makeDubSuccessResponse({ key: 'my-suffix' }),
    });

    const result = await mintEntryPoint(makeValidBody({ suffix: 'my-suffix' }));
    expect(result.ok).toBe(true);

    const fetchCall = globalThis.fetch.mock.calls[0];
    const body = JSON.parse(fetchCall[1].body);
    expect(body.key).toBe('my-suffix');
  });

  it('appends ?ep= to destination URL without existing params', async () => {
    const result = await mintEntryPoint(makeValidBody({
      target: { type: 'site_url', url: 'https://example.org/gala' },
    }));
    expect(result.entry_point.destination_url).toMatch(/^https:\/\/example\.org\/gala\?ep=ep_/);
  });

  it('appends &ep= to destination URL with existing utm_ params', async () => {
    const result = await mintEntryPoint(makeValidBody({
      target: { type: 'site_url', url: 'https://example.org/gala?utm_source=chat' },
    }));
    expect(result.entry_point.destination_url).toMatch(/&ep=ep_/);
  });
});

// ============================================================
// SUFFIX_TAKEN
// ============================================================

describe('mintEntryPoint — SUFFIX_TAKEN', () => {
  it('returns SUFFIX_TAKEN when Dub returns 409 and a custom suffix was given', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 409,
      headers: { get: () => null },
      json: async () => ({}),
    });

    const result = await mintEntryPoint(makeValidBody({ suffix: 'taken-key' }));
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('SUFFIX_TAKEN');
  });
});

// ============================================================
// CONFLICT (no suffix — externalId collision)
// ============================================================

describe('mintEntryPoint — CONFLICT', () => {
  it('returns CONFLICT when Dub returns 409 and no custom suffix', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 409,
      headers: { get: () => null },
      json: async () => ({}),
    });

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('CONFLICT');
  });
});

// ============================================================
// Dub 5xx → DUB_ERROR
// ============================================================

describe('mintEntryPoint — Dub 5xx', () => {
  it('returns DUB_ERROR on Dub 500', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      headers: { get: () => null },
    });

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/500/);
  });

  it('returns DUB_ERROR on Dub 503', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 503,
      headers: { get: () => null },
    });

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
  });
});

// ============================================================
// Registry conditional-put failure after Dub success
// ============================================================

describe('mintEntryPoint — registry failure after Dub success', () => {
  it('attempts Dub cleanup and returns DUB_ERROR when DynamoDB put fails', async () => {
    // Dub mint succeeds
    // Registry put throws generic error
    const ddbErr = new Error('ProvisionedThroughputExceededException');
    ddbErr.name = 'ProvisionedThroughputExceededException';
    setDocClient(makeDdbMock({ putThrows: ddbErr }));

    // Dub DELETE (cleanup) call also mocked
    globalThis.fetch
      .mockResolvedValueOnce({
        // POST /links — success
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => makeDubSuccessResponse(),
      })
      .mockResolvedValueOnce({
        // DELETE /links/dub_link_001 — success
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => ({}),
      });

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/Registry/);

    // Verify DELETE was called
    const deleteCalls = globalThis.fetch.mock.calls.filter(
      call => call[0]?.includes('links/dub_link_001') && call[1]?.method === 'DELETE'
    );
    expect(deleteCalls).toHaveLength(1);
  });

  it('logs warning and continues if Dub cleanup also fails (orphan)', async () => {
    const ddbErr = new Error('NetworkError');
    ddbErr.name = 'NetworkError';
    setDocClient(makeDdbMock({ putThrows: ddbErr }));

    globalThis.fetch
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => makeDubSuccessResponse(),
      })
      .mockResolvedValueOnce({
        // DELETE fails
        ok: false,
        status: 500,
        headers: { get: () => null },
      });

    const result = await mintEntryPoint(makeValidBody());
    // Error is still surfaced — orphan link is tolerable
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
  });
});

// ============================================================
// Secret absent / empty
// ============================================================

describe('mintEntryPoint — secret absent or empty', () => {
  it('returns DUB_ERROR gracefully when DUB_SECRET_NAME env var is not set', async () => {
    delete process.env.DUB_SECRET_NAME;
    setSmClient(null); // will not be called — env var check exits early

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/not configured/);
  });

  it('returns DUB_ERROR gracefully when secret value is empty string', async () => {
    setSmClient(makeSmMock(''));

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/not configured/);
  });

  it('returns DUB_ERROR gracefully when Secrets Manager throws', async () => {
    const smMock = {
      send: jest.fn(async () => { throw new Error('AccessDeniedException'); }),
    };
    setSmClient(smMock);

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/not configured/);
  });

  it('does not crash the Lambda — returns error shape', async () => {
    delete process.env.DUB_SECRET_NAME;
    setSmClient(null);

    await expect(mintEntryPoint(makeValidBody())).resolves.toMatchObject({
      ok: false,
      error: { code: 'DUB_ERROR' },
    });
  });
});

// ============================================================
// 429 with Retry-After honored once then surfaced
// ============================================================

describe('mintEntryPoint — 429 rate limit', () => {
  it('retries once after Retry-After delay and succeeds', async () => {
    jest.useFakeTimers();

    globalThis.fetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        headers: { get: (h) => h === 'Retry-After' ? '1' : null },
        json: async () => ({}),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => makeDubSuccessResponse(),
      });

    // Start the mint, then run all pending timers so sleep() resolves.
    const promise = mintEntryPoint(makeValidBody());
    await jest.runAllTimersAsync();
    const result = await promise;

    expect(result.ok).toBe(true);
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
  }, 10000);

  it('returns DUB_ERROR after retry also fails with 429', async () => {
    jest.useFakeTimers();

    globalThis.fetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        headers: { get: (h) => h === 'Retry-After' ? '1' : null },
        json: async () => ({}),
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        headers: { get: (h) => h === 'Retry-After' ? '1' : null },
        json: async () => ({}),
      });

    const promise = mintEntryPoint(makeValidBody());
    await jest.runAllTimersAsync();
    const result = await promise;

    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/rate limit/i);
  }, 10000);
});

// ============================================================
// handler() — top-level Lambda dispatch
// ============================================================

describe('handler', () => {
  it('returns 201 on success', async () => {
    const response = await handler(makeValidBody());
    expect(response.statusCode).toBe(201);
    const body = JSON.parse(response.body);
    expect(body.ok).toBe(true);
  });

  it('returns 400 for unknown action', async () => {
    const response = await handler({ action: 'list' });
    expect(response.statusCode).toBe(400);
  });

  it('returns 400 for VALIDATION error', async () => {
    const response = await handler(makeValidBody({ channel: 'bad' }));
    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.error.code).toBe('VALIDATION');
  });

  it('parses JSON string body (API Gateway proxy format)', async () => {
    const response = await handler({ body: JSON.stringify(makeValidBody()) });
    expect(response.statusCode).toBe(201);
  });

  it('returns 400 for invalid JSON body', async () => {
    const response = await handler({ body: '{ bad json' });
    expect(response.statusCode).toBe(400);
  });

  it('returns 409 for SUFFIX_TAKEN', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 409,
      headers: { get: () => null },
      json: async () => ({}),
    });
    const response = await handler(makeValidBody({ suffix: 'taken' }));
    expect(response.statusCode).toBe(409);
    const body = JSON.parse(response.body);
    expect(body.error.code).toBe('SUFFIX_TAKEN');
  });

  it('returns 502 for DUB_ERROR', async () => {
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      headers: { get: () => null },
    });
    const response = await handler(makeValidBody());
    expect(response.statusCode).toBe(502);
    const body = JSON.parse(response.body);
    expect(body.error.code).toBe('DUB_ERROR');
  });
});
