'use strict';

const { runRevocationObserve } = require('./revocation-observer');

function makeDeps(overrides = {}) {
  return {
    emitCycleResult: jest.fn().mockResolvedValue(),
    alert: jest.fn().mockResolvedValue(),
    baseUrl: 'https://schedule.example.test',
    ...overrides,
  };
}

describe('revocation-observer (operator-triggered token cycle, §13.7)', () => {
  test('success: 302 first then 410 replay → success metric, token never logged', async () => {
    const fetch = jest
      .fn()
      .mockResolvedValueOnce({ status: 302 })
      .mockResolvedValueOnce({ status: 410 });
    const deps = makeDeps({ fetch });
    const res = await runRevocationObserve({ slug: '/cancel', token: 'SECRET.tok.en' }, deps);

    expect(res).toMatchObject({ cycle: 'revocation', success: true, firstStatus: 302, replayStatus: 410 });
    expect(deps.emitCycleResult).toHaveBeenCalledWith('revocation', true);
    // URL carries the token but the cycle return value never echoes it.
    expect(fetch.mock.calls[0][0]).toContain('t=SECRET.tok.en');
    expect(JSON.stringify(res)).not.toContain('SECRET.tok.en');
  });

  test('success: 200 (attendance page) first then 410 replay', async () => {
    const fetch = jest
      .fn()
      .mockResolvedValueOnce({ status: 200 })
      .mockResolvedValueOnce({ status: 410 });
    const res = await runRevocationObserve({ slug: '/attended/met', token: 't' }, makeDeps({ fetch }));
    expect(res.success).toBe(true);
  });

  test('fails when the first redemption is not 200/302', async () => {
    const fetch = jest.fn().mockResolvedValueOnce({ status: 401 });
    const deps = makeDeps({ fetch });
    const res = await runRevocationObserve({ slug: '/cancel', token: 't' }, deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/first redemption/);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails when the replay is not 410 (one-time-use NOT enforced)', async () => {
    const fetch = jest
      .fn()
      .mockResolvedValueOnce({ status: 302 })
      .mockResolvedValueOnce({ status: 302 });
    const res = await runRevocationObserve({ slug: '/cancel', token: 't' }, makeDeps({ fetch }));
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/replay expected 410/);
  });

  test('rejects an invalid slug without calling fetch', async () => {
    const fetch = jest.fn();
    const res = await runRevocationObserve({ slug: '/bogus', token: 't' }, makeDeps({ fetch }));
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/invalid or missing slug/);
    expect(fetch).not.toHaveBeenCalled();
  });

  test('rejects a missing token without calling fetch', async () => {
    const fetch = jest.fn();
    const res = await runRevocationObserve({ slug: '/cancel' }, makeDeps({ fetch }));
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/missing token/);
    expect(fetch).not.toHaveBeenCalled();
  });

  test('fails + alerts when fetch rejects (endpoint unreachable)', async () => {
    const fetch = jest.fn().mockRejectedValue(new Error('connect ETIMEDOUT'));
    const deps = makeDeps({ fetch });
    const res = await runRevocationObserve({ slug: '/cancel', token: 't' }, deps);
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/ETIMEDOUT/);
    expect(deps.alert).toHaveBeenCalled();
  });

  test('fails cleanly when no fetch implementation is available (older runtime)', async () => {
    const orig = globalThis.fetch;
    globalThis.fetch = undefined;
    try {
      const res = await runRevocationObserve(
        { slug: '/cancel', token: 't' },
        { fetch: undefined, emitCycleResult: jest.fn(), alert: jest.fn(), baseUrl: 'https://x.test' }
      );
      expect(res.success).toBe(false);
      expect(res.error).toMatch(/no fetch/);
    } finally {
      globalThis.fetch = orig;
    }
  });
});
