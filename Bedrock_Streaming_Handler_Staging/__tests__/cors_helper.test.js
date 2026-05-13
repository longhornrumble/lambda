const { corsHeaders, pickOrigin, ALLOWED_ORIGINS, DEFAULT_ORIGIN } = require('../cors-helper');

describe('cors-helper / pickOrigin', () => {
  test('returns DEFAULT_ORIGIN when event is undefined', () => {
    expect(pickOrigin(undefined)).toBe(DEFAULT_ORIGIN);
  });

  test('returns DEFAULT_ORIGIN when headers missing', () => {
    expect(pickOrigin({})).toBe(DEFAULT_ORIGIN);
  });

  test('returns DEFAULT_ORIGIN when origin header missing', () => {
    expect(pickOrigin({ headers: { 'content-type': 'application/json' } })).toBe(DEFAULT_ORIGIN);
  });

  test.each([
    ['https://chat.myrecruiter.ai'],
    ['https://staging.chat.myrecruiter.ai'],
    ['https://picassocode.s3.amazonaws.com'],
    ['https://picassostaging.s3.amazonaws.com'],
    ['http://localhost:3000'],
    ['http://localhost:5173'],
    ['http://localhost:8000'],
  ])('reflects allowlisted origin %s', (origin) => {
    expect(pickOrigin({ headers: { origin } })).toBe(origin);
  });

  test('reflects any localhost port via prefix match (dev workflow)', () => {
    expect(pickOrigin({ headers: { origin: 'http://localhost:9999' } })).toBe('http://localhost:9999');
    expect(pickOrigin({ headers: { origin: 'https://localhost:4443' } })).toBe('https://localhost:4443');
  });

  test('falls back to DEFAULT_ORIGIN for unknown origin', () => {
    expect(pickOrigin({ headers: { origin: 'https://evil.example.com' } })).toBe(DEFAULT_ORIGIN);
  });

  test('handles mixed-case Origin header', () => {
    expect(pickOrigin({ headers: { Origin: 'https://chat.myrecruiter.ai' } })).toBe('https://chat.myrecruiter.ai');
    expect(pickOrigin({ headers: { ORIGIN: 'https://chat.myrecruiter.ai' } })).toBe('https://chat.myrecruiter.ai');
  });

  test('lowercase origin takes precedence over mixed-case (Lambda URL invocation shape)', () => {
    expect(pickOrigin({ headers: { origin: 'https://chat.myrecruiter.ai', Origin: 'https://evil.test' } })).toBe('https://chat.myrecruiter.ai');
  });

  test('does not match origin substrings (no prefix bypass)', () => {
    expect(pickOrigin({ headers: { origin: 'https://chat.myrecruiter.ai.evil.test' } })).toBe(DEFAULT_ORIGIN);
    expect(pickOrigin({ headers: { origin: 'https://evil.test/chat.myrecruiter.ai' } })).toBe(DEFAULT_ORIGIN);
  });
});

describe('cors-helper / corsHeaders', () => {
  test('returns the full 4-header set with reflected origin', () => {
    const headers = corsHeaders({ headers: { origin: 'https://chat.myrecruiter.ai' } });
    expect(headers).toEqual({
      'Access-Control-Allow-Origin': 'https://chat.myrecruiter.ai',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
      'Access-Control-Allow-Credentials': 'true',
    });
  });

  test('returns DEFAULT_ORIGIN reflected when no origin header', () => {
    const headers = corsHeaders({});
    expect(headers['Access-Control-Allow-Origin']).toBe(DEFAULT_ORIGIN);
  });

  test('merges extras without overwriting CORS keys by default', () => {
    const headers = corsHeaders(
      { headers: { origin: 'https://chat.myrecruiter.ai' } },
      { 'Content-Type': 'application/json', 'X-Accel-Buffering': 'no' }
    );
    expect(headers['Content-Type']).toBe('application/json');
    expect(headers['X-Accel-Buffering']).toBe('no');
    expect(headers['Access-Control-Allow-Origin']).toBe('https://chat.myrecruiter.ai');
  });

  test('NEVER emits wildcard origin', () => {
    const cases = [
      { headers: { origin: 'https://evil.example.com' } },
      { headers: { origin: '*' } },
      {},
      undefined,
    ];
    for (const ev of cases) {
      expect(corsHeaders(ev)['Access-Control-Allow-Origin']).not.toBe('*');
    }
  });
});

describe('cors-helper / ALLOWED_ORIGINS', () => {
  test('mirrors the MFS Python allowlist (drift guard)', () => {
    expect([...ALLOWED_ORIGINS].sort()).toEqual([
      'http://localhost:3000',
      'http://localhost:5173',
      'http://localhost:8000',
      'https://chat.myrecruiter.ai',
      'https://picassocode.s3.amazonaws.com',
      'https://picassostaging.s3.amazonaws.com',
      'https://staging.chat.myrecruiter.ai',
    ]);
  });
});
