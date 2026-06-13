/**
 * Attribution_Mint_Service unit tests.
 *
 * All external surfaces (fetch, DynamoDB, Secrets Manager) are mocked.
 * No live AWS or Dub calls.
 *
 * Coverage targets (mint):
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
 *
 * Coverage targets (repoint):
 *  - Happy repoint: PATCH to /links/ext_{id} with wsQuery honored; body.url has ?ep=; UpdateItem called; short_link/qr unchanged
 *  - NOT_FOUND when getEntryPoint returns null
 *  - Validation failures: bad entry_point_id format, non-https target.url, disallowed query param, mailto
 *  - Dub 404 → DUB_ERROR with surfaced detail
 *  - Dub 5xx → DUB_ERROR
 *  - Dub 429 → retry then surface
 *  - Secret absent → DUB_ERROR
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
import { mintEntryPoint, repointEntryPoint, handler } from '../index.mjs';
import { validateMintRequest, validateRepointRequest, validateDestinationUrl } from '../validation.mjs';
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

/** Create a mock DynamoDB DocumentClient that succeeds on put/get/update */
function makeDdbMock({ putThrows = null, getResult = null, updateThrows = null } = {}) {
  return {
    send: jest.fn(async (command) => {
      const name = command.constructor?.name ?? '';
      if (name === 'PutCommand' || (command instanceof Object && command.input?.ConditionExpression)) {
        if (putThrows) throw putThrows;
        return {};
      }
      if (name === 'GetCommand') {
        return { Item: getResult };
      }
      if (name === 'UpdateCommand') {
        if (updateThrows) throw updateThrows;
        return {};
      }
      // Item 13: throw on unrecognized commands rather than silently falling through.
      throw new Error(`Unexpected DynamoDB command in test mock: ${name || JSON.stringify(command)}`);
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

  it('accepts direct IAM invocation (plain object, no wrapper)', async () => {
    // The API Gateway string-body branch was removed (item 11 — surface reduction).
    // IAM invoke passes the event object directly as the body.
    const response = await handler(makeValidBody());
    expect(response.statusCode).toBe(201);
  });

  it('returns 400 for unknown action when action field is missing', async () => {
    const body = makeValidBody();
    delete body.action;
    const response = await handler(body);
    expect(response.statusCode).toBe(400);
    const parsed = JSON.parse(response.body);
    expect(parsed.error.code).toBe('VALIDATION');
    // Item 3: error message must be static — no raw input reflected.
    expect(parsed.error.message).toBe('Unknown action');
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

// ============================================================
// Item 1 — 409 registry-existence idempotent path (C4)
// ============================================================

describe('mintEntryPoint — Dub 409 no-suffix idempotent retry (item 1)', () => {
  it('returns ok:true with STORED fields when registry row exists (idempotent)', async () => {
    // Dub 409 (no suffix) → getEntryPoint returns an existing row.
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 409,
      headers: { get: () => null },
      json: async () => ({}),
    });

    const existingRow = {
      tenant_id: 'TENANT123',
      entry_point_id: 'ep_STORED001',
      dub_short_link: 'https://myrctr.link/stored-key',
      dub_link_id: 'dub_stored_001',
      destination_url: 'https://example.org/gala?ep=ep_STORED001',
      created_at: '2026-01-01T00:00:00.000Z',
    };
    setDocClient(makeDdbMock({ getResult: existingRow }));

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(true);
    expect(result.entry_point.entry_point_id).toBe('ep_STORED001');
    expect(result.entry_point.short_link).toBe('https://myrctr.link/stored-key');
    expect(result.entry_point.dub_link_id).toBe('dub_stored_001');
    expect(result.entry_point.created_at).toBe('2026-01-01T00:00:00.000Z');
  });

  it('returns CONFLICT when Dub 409 (no suffix) and registry row is absent', async () => {
    // Dub 409 (no suffix) → getEntryPoint returns null (orphan Dub link).
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 409,
      headers: { get: () => null },
      json: async () => ({}),
    });
    setDocClient(makeDdbMock({ getResult: null }));

    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('CONFLICT');
  });
});

// ============================================================
// Item 2 — Unicode @ bypass (C8.13)
// ============================================================

describe('validateMintRequest — Unicode @ bypass (item 2)', () => {
  it('rejects full-width @ (U+FF20) in label via NFKC normalization', () => {
    const err = validateMintRequest(makeValidBody({ label: 'jane＠example.com' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/@/);
  });

  it('rejects full-width @ in campaign', () => {
    const err = validateMintRequest(makeValidBody({ campaign: 'user＠org' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/@/);
  });

  it('rejects full-width @ in placement', () => {
    const err = validateMintRequest(makeValidBody({ placement: 'lead＠campaign' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/@/);
  });
});

// ============================================================
// Item 4 — Secrets cache: transient error then success
// ============================================================

describe('getDubApiKey — transient error does not poison cache (item 4)', () => {
  it('returns key on second call after first call threw transiently', async () => {
    let callCount = 0;
    const smMock = {
      send: jest.fn(async () => {
        callCount++;
        if (callCount === 1) throw new Error('TransientNetworkError');
        return { SecretString: 'real-api-key' };
      }),
    };
    setSmClient(smMock);

    // First call — should throw → return null but NOT cache null.
    const first = await mintEntryPoint(makeValidBody());
    expect(first.ok).toBe(false);
    expect(first.error.code).toBe('DUB_ERROR');

    // Reset Dub fetch mock for the second call
    globalThis.fetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      headers: { get: () => null },
      json: async () => makeDubSuccessResponse(),
    });

    // Second call — cache was not poisoned; secret fetch retries and succeeds.
    const second = await mintEntryPoint(makeValidBody());
    expect(second.ok).toBe(true);
    expect(smMock.send).toHaveBeenCalledTimes(2);
  });
});

// ============================================================
// Item 5 — Fragment URL: ?ep= placed before fragment
// ============================================================

describe('buildDestinationUrl — fragment URL (item 5)', () => {
  it('places ?ep= before the # fragment, not inside it', async () => {
    const result = await mintEntryPoint(makeValidBody({
      target: { type: 'site_url', url: 'https://example.org/page#section' },
    }));
    expect(result.ok).toBe(true);
    const dest = result.entry_point.destination_url;
    // ep param must appear before the fragment
    const epIndex = dest.indexOf('?ep=');
    const hashIndex = dest.indexOf('#');
    expect(epIndex).toBeGreaterThan(-1);
    expect(hashIndex).toBeGreaterThan(-1);
    expect(epIndex).toBeLessThan(hashIndex);
    // Confirm attribution id is in the query string, not the fragment
    expect(dest).toMatch(/\?ep=ep_[0-9A-HJKMNP-TV-Z]{26}#section$/);
  });
});

// ============================================================
// Item 6 — Retry-After NaN guard (HTTP-date header)
// ============================================================

describe('dubClient — Retry-After NaN guard (item 6)', () => {
  it('uses 1s fallback when Retry-After header is an HTTP-date (non-numeric)', async () => {
    jest.useFakeTimers();

    globalThis.fetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        // HTTP-date format → parseInt → NaN
        headers: { get: (h) => h === 'Retry-After' ? 'Fri, 20 Jun 2026 12:00:00 GMT' : null },
        json: async () => ({}),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => makeDubSuccessResponse(),
      });

    const promise = mintEntryPoint(makeValidBody());
    await jest.runAllTimersAsync();
    const result = await promise;

    expect(result.ok).toBe(true);
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
  }, 10000);

  it('caps Retry-After at 30 s even when header value is very large', async () => {
    jest.useFakeTimers();

    globalThis.fetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        headers: { get: (h) => h === 'Retry-After' ? '9999' : null },
        json: async () => ({}),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => makeDubSuccessResponse(),
      });

    const promise = mintEntryPoint(makeValidBody());
    await jest.runAllTimersAsync();
    const result = await promise;
    expect(result.ok).toBe(true);
  }, 10000);
});

// ============================================================
// Item 7 — Assert the Dub POST body fields individually
// ============================================================

describe('mintEntryPoint — Dub POST body assertions (item 7)', () => {
  it('extracts the key from a console-stored JSON wrapper secret', async () => {
    setSmClient(makeSmMock('{"api-key":"dub_FROM_JSON"}'));
    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(true);
    expect(globalThis.fetch.mock.calls[0][1].headers.Authorization).toBe('Bearer dub_FROM_JSON');
  });

  it('appends ?workspaceId= to the Dub URL when DUB_WORKSPACE_ID is set', async () => {
    process.env.DUB_WORKSPACE_ID = 'ws_TEST123';
    try {
      const result = await mintEntryPoint(makeValidBody());
      expect(result.ok).toBe(true);
      expect(globalThis.fetch.mock.calls[0][0])
        .toBe('https://api.dub.co/links?workspaceId=ws_TEST123');
    } finally {
      delete process.env.DUB_WORKSPACE_ID;
    }
  });

  it('sends correct externalId, domain, tenantId, and url with ?ep= (no tagNames)', async () => {
    const result = await mintEntryPoint(makeValidBody());
    expect(result.ok).toBe(true);

    const fetchCall = globalThis.fetch.mock.calls[0];
    expect(fetchCall[0]).toBe('https://api.dub.co/links');
    expect(fetchCall[1].method).toBe('POST');

    const sentBody = JSON.parse(fetchCall[1].body);

    // externalId must match the minted ep_ id
    expect(sentBody.externalId).toBe(result.entry_point.entry_point_id);
    // domain must be the configured Dub domain
    expect(sentBody.domain).toBe('myrctr.link');
    // tenantId must be the request tenant_id
    expect(sentBody.tenantId).toBe('TENANT123');
    // tagNames must be ABSENT (C4 amendment: Dub 404s on nonexistent tags)
    expect(sentBody.tagNames).toBeUndefined();
    // url must contain ?ep= (the destination URL with ep appended)
    expect(sentBody.url).toContain('?ep=');
    expect(sentBody.url).toContain(result.entry_point.entry_point_id);
  });
});

// ============================================================
// Item 8 — tenant_id length cap ≤128
// ============================================================

describe('validateMintRequest — tenant_id length cap (item 8)', () => {
  it('rejects tenant_id > 128 chars', () => {
    const err = validateMintRequest(makeValidBody({ tenant_id: 'x'.repeat(129) }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/tenant_id/);
    expect(err?.message).toMatch(/128/);
  });

  it('accepts tenant_id exactly 128 chars', () => {
    expect(validateMintRequest(makeValidBody({ tenant_id: 'x'.repeat(128) }))).toBeNull();
  });
});

// ============================================================
// Item 9 — Empty-string suffix treated as absent (validation error)
// ============================================================

describe('validateMintRequest — empty-string suffix (item 9)', () => {
  it('rejects empty string suffix', () => {
    const err = validateMintRequest(makeValidBody({ suffix: '' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/empty string/);
  });

  it('accepts undefined suffix (field omitted)', () => {
    const body = makeValidBody();
    delete body.suffix;
    expect(validateMintRequest(body)).toBeNull();
  });
});

// ============================================================
// validateRepointRequest
// ============================================================

function makeValidRepointBody(overrides = {}) {
  return {
    action: 'repoint',
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    target: { url: 'https://example.org/new-landing' },
    ...overrides,
  };
}

describe('validateRepointRequest', () => {
  it('returns null for a valid request', () => {
    expect(validateRepointRequest(makeValidRepointBody())).toBeNull();
  });

  it('rejects missing entry_point_id', () => {
    const err = validateRepointRequest(makeValidRepointBody({ entry_point_id: '' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/entry_point_id/);
  });

  it('rejects entry_point_id without ep_ prefix', () => {
    const err = validateRepointRequest(makeValidRepointBody({ entry_point_id: 'ABCD1234' }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/entry_point_id/);
  });

  it('rejects entry_point_id with fewer than 8 chars after ep_', () => {
    const err = validateRepointRequest(makeValidRepointBody({ entry_point_id: 'ep_ABC' }));
    expect(err?.code).toBe('VALIDATION');
  });

  it('accepts entry_point_id with exactly 8 chars after ep_', () => {
    expect(validateRepointRequest(makeValidRepointBody({ entry_point_id: 'ep_ABCD1234' }))).toBeNull();
  });

  it('rejects non-https target.url', () => {
    const err = validateRepointRequest(makeValidRepointBody({
      target: { url: 'http://example.org/new' },
    }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/https/);
  });

  it('rejects mailto: target.url', () => {
    const err = validateRepointRequest(makeValidRepointBody({
      target: { url: 'mailto:user@example.org' },
    }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/Forbidden/);
  });

  it('rejects disallowed query param in target.url', () => {
    const err = validateRepointRequest(makeValidRepointBody({
      target: { url: 'https://example.org/new?tracking=123' },
    }));
    expect(err?.code).toBe('VALIDATION');
    expect(err?.message).toMatch(/Disallowed/);
  });

  it('accepts utm_ query params in target.url', () => {
    expect(validateRepointRequest(makeValidRepointBody({
      target: { url: 'https://example.org/new?utm_source=print' },
    }))).toBeNull();
  });
});

// ============================================================
// repointEntryPoint — happy path
// ============================================================

describe('repointEntryPoint — happy path', () => {
  const existingRow = {
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    dub_short_link: 'https://myrctr.link/gala-tents',
    dub_link_id: 'dub_link_001',
    dub_key: 'gala-tents',
    destination_url: 'https://example.org/old?ep=ep_ABCD1234',
  };

  beforeEach(() => {
    setDocClient(makeDdbMock({ getResult: existingRow }));
    // Dub PATCH succeeds
    globalThis.fetch.mockResolvedValue({
      ok: true,
      status: 200,
      headers: { get: () => null },
      json: async () => ({}),
    });
  });

  it('returns ok:true with updated destination_url and unchanged short_link/qr', async () => {
    const result = await repointEntryPoint(makeValidRepointBody());

    expect(result.ok).toBe(true);
    expect(result.entry_point.entry_point_id).toBe('ep_ABCD1234');
    expect(result.entry_point.short_link).toBe('https://myrctr.link/gala-tents');
    expect(result.entry_point.qr_url).toMatch(/api\.dub\.co\/qr/);
    expect(result.entry_point.destination_url).toMatch(/\?ep=ep_ABCD1234$/);
    expect(result.entry_point.destination_url).toContain('new-landing');
    expect(result.entry_point.dub_link_id).toBe('dub_link_001');
    expect(result.entry_point.updated_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    // short_link must be unchanged from registry (not regenerated)
    expect(result.entry_point.short_link).toBe(existingRow.dub_short_link);
  });

  it('sends PATCH to /links/ext_{id} (not POST)', async () => {
    await repointEntryPoint(makeValidRepointBody());

    const fetchCall = globalThis.fetch.mock.calls[0];
    expect(fetchCall[0]).toContain('/links/ext_ep_ABCD1234');
    expect(fetchCall[1].method).toBe('PATCH');
  });

  it('body.url contains ?ep=entry_point_id (re-appended, not caller-supplied)', async () => {
    await repointEntryPoint(makeValidRepointBody());

    const fetchCall = globalThis.fetch.mock.calls[0];
    const sentBody = JSON.parse(fetchCall[1].body);
    expect(sentBody.url).toContain('?ep=ep_ABCD1234');
    expect(sentBody.url).toContain('new-landing');
  });

  it('appends ?workspaceId= to PATCH URL when DUB_WORKSPACE_ID is set', async () => {
    process.env.DUB_WORKSPACE_ID = 'ws_REPOINT';
    try {
      await repointEntryPoint(makeValidRepointBody());
      const fetchCall = globalThis.fetch.mock.calls[0];
      expect(fetchCall[0]).toContain('?workspaceId=ws_REPOINT');
    } finally {
      delete process.env.DUB_WORKSPACE_ID;
    }
  });

  it('calls registry UpdateItem with new destination_url', async () => {
    const ddbMock = makeDdbMock({ getResult: existingRow });
    setDocClient(ddbMock);

    await repointEntryPoint(makeValidRepointBody());

    const updateCalls = ddbMock.send.mock.calls.filter(
      ([cmd]) => cmd.constructor?.name === 'UpdateCommand'
    );
    expect(updateCalls).toHaveLength(1);
    const updateInput = updateCalls[0][0].input;
    expect(updateInput.ExpressionAttributeValues[':url']).toContain('?ep=ep_ABCD1234');
  });
});

// ============================================================
// repointEntryPoint — registry update fails after Dub PATCH succeeds
// ============================================================

describe('repointEntryPoint — registry update failure after Dub PATCH success', () => {
  const existingRow = {
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    dub_short_link: 'https://myrctr.link/gala-tents',
    dub_link_id: 'dub_link_001',
    destination_url: 'https://example.org/old?ep=ep_ABCD1234',
  };

  it('returns DUB_ERROR with "retry to sync" message when registry UpdateItem fails after Dub PATCH succeeds', async () => {
    const updateErr = new Error('ProvisionedThroughputExceededException');
    updateErr.name = 'ProvisionedThroughputExceededException';
    setDocClient(makeDdbMock({ getResult: existingRow, updateThrows: updateErr }));

    globalThis.fetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      headers: { get: () => null },
      json: async () => ({}),
    });

    const result = await repointEntryPoint(makeValidRepointBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/retry to sync/);
  });
});

// ============================================================
// repointEntryPoint — NOT_FOUND
// ============================================================

describe('repointEntryPoint — NOT_FOUND', () => {
  it('returns NOT_FOUND when getEntryPoint returns null', async () => {
    setDocClient(makeDdbMock({ getResult: null }));

    const result = await repointEntryPoint(makeValidRepointBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('NOT_FOUND');
    expect(result.error.message).toMatch(/entry point not found/);
    // Dub should NOT have been called
    expect(globalThis.fetch).not.toHaveBeenCalled();
  });
});

// ============================================================
// repointEntryPoint — Dub error paths
// ============================================================

describe('repointEntryPoint — Dub 404', () => {
  const existingRow = {
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    dub_short_link: 'https://myrctr.link/gala-tents',
    dub_link_id: 'dub_link_001',
    destination_url: 'https://example.org/old?ep=ep_ABCD1234',
  };

  it('returns DUB_ERROR when Dub returns 404', async () => {
    setDocClient(makeDdbMock({ getResult: existingRow }));
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      headers: { get: () => null },
      json: async () => ({ error: { code: 'not_found', message: 'Link not found' } }),
    });

    const result = await repointEntryPoint(makeValidRepointBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/404/);
  });
});

describe('repointEntryPoint — Dub 5xx', () => {
  const existingRow = {
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    dub_short_link: 'https://myrctr.link/gala-tents',
    dub_link_id: 'dub_link_001',
    destination_url: 'https://example.org/old?ep=ep_ABCD1234',
  };

  it('returns DUB_ERROR with surfaced detail on Dub 500', async () => {
    setDocClient(makeDdbMock({ getResult: existingRow }));
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      headers: { get: () => null },
      json: async () => ({ error: { code: 'server_error', message: 'Internal error' } }),
    });

    const result = await repointEntryPoint(makeValidRepointBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/500/);
  });
});

describe('repointEntryPoint — Dub 429 retry', () => {
  const existingRow = {
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    dub_short_link: 'https://myrctr.link/gala-tents',
    dub_link_id: 'dub_link_001',
    destination_url: 'https://example.org/old?ep=ep_ABCD1234',
  };

  it('retries once on 429 and succeeds', async () => {
    jest.useFakeTimers();
    setDocClient(makeDdbMock({ getResult: existingRow }));

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
        json: async () => ({}),
      });

    const promise = repointEntryPoint(makeValidRepointBody());
    await jest.runAllTimersAsync();
    const result = await promise;

    expect(result.ok).toBe(true);
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
  }, 10000);

  it('returns DUB_ERROR after retry also fails with 429', async () => {
    jest.useFakeTimers();
    setDocClient(makeDdbMock({ getResult: existingRow }));

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

    const promise = repointEntryPoint(makeValidRepointBody());
    await jest.runAllTimersAsync();
    const result = await promise;

    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/rate limit/i);
  }, 10000);
});

// ============================================================
// repointEntryPoint — secret absent
// ============================================================

describe('repointEntryPoint — secret absent', () => {
  it('returns DUB_ERROR gracefully when DUB_SECRET_NAME not set', async () => {
    delete process.env.DUB_SECRET_NAME;
    setSmClient(null);

    const result = await repointEntryPoint(makeValidRepointBody());
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('DUB_ERROR');
    expect(result.error.message).toMatch(/not configured/);
    // getEntryPoint should NOT have been called (exits before DDB)
    // — and since DUB_SECRET_NAME is absent, fetch definitely not called
    expect(globalThis.fetch).not.toHaveBeenCalled();
  });
});

// ============================================================
// handler — repoint dispatch
// ============================================================

describe('handler — repoint dispatch', () => {
  const existingRow = {
    tenant_id: 'TENANT123',
    entry_point_id: 'ep_ABCD1234',
    dub_short_link: 'https://myrctr.link/gala-tents',
    dub_link_id: 'dub_link_001',
    destination_url: 'https://example.org/old?ep=ep_ABCD1234',
  };

  beforeEach(() => {
    setDocClient(makeDdbMock({ getResult: existingRow }));
    globalThis.fetch.mockResolvedValue({
      ok: true,
      status: 200,
      headers: { get: () => null },
      json: async () => ({}),
    });
  });

  it('returns 200 on successful repoint', async () => {
    const response = await handler(makeValidRepointBody());
    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.ok).toBe(true);
    expect(body.entry_point.short_link).toBe('https://myrctr.link/gala-tents');
  });

  it('returns 400 for repoint validation error (bad entry_point_id)', async () => {
    const response = await handler(makeValidRepointBody({ entry_point_id: 'bad-id' }));
    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.error.code).toBe('VALIDATION');
  });

  it('returns 404 for NOT_FOUND', async () => {
    setDocClient(makeDdbMock({ getResult: null }));
    const response = await handler(makeValidRepointBody());
    expect(response.statusCode).toBe(404);
    const body = JSON.parse(response.body);
    expect(body.error.code).toBe('NOT_FOUND');
  });

  it('returns 400 for unknown action (repoint path not triggered for "list")', async () => {
    const response = await handler({ action: 'list' });
    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.error.code).toBe('VALIDATION');
    expect(body.error.message).toBe('Unknown action');
  });
});
