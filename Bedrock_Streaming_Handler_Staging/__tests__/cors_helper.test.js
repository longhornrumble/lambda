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

  test('port-appended allowed origin falls back to DEFAULT_ORIGIN (Set lookup is exact)', () => {
    expect(pickOrigin({ headers: { origin: 'https://chat.myrecruiter.ai:1234' } })).toBe(DEFAULT_ORIGIN);
    expect(pickOrigin({ headers: { origin: 'https://staging.chat.myrecruiter.ai:443' } })).toBe(DEFAULT_ORIGIN);
  });

  test('IPv6 localhost ([::1]:port) falls back to DEFAULT_ORIGIN (not in prefix match)', () => {
    expect(pickOrigin({ headers: { origin: 'http://[::1]:3000' } })).toBe(DEFAULT_ORIGIN);
    expect(pickOrigin({ headers: { origin: 'https://[::1]:5173' } })).toBe(DEFAULT_ORIGIN);
  });
});

describe('cors-helper / corsHeaders', () => {
  test('returns the full 4-header set with reflected origin', () => {
    const headers = corsHeaders({ headers: { origin: 'https://chat.myrecruiter.ai' } });
    expect(headers).toEqual({
      'Access-Control-Allow-Origin': 'https://chat.myrecruiter.ai',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept',
      'Access-Control-Allow-Credentials': 'true',
    });
  });

  test('includes Accept in Allow-Headers (widget sends Accept on streaming requests)', () => {
    const headers = corsHeaders({ headers: { origin: 'https://chat.myrecruiter.ai' } });
    expect(headers['Access-Control-Allow-Headers']).toContain('Accept');
  });

  test('returns DEFAULT_ORIGIN reflected when no origin header', () => {
    const headers = corsHeaders({});
    expect(headers['Access-Control-Allow-Origin']).toBe(DEFAULT_ORIGIN);
  });

  test('extras can add supplemental headers alongside CORS', () => {
    const headers = corsHeaders(
      { headers: { origin: 'https://chat.myrecruiter.ai' } },
      { 'Content-Type': 'application/json', 'X-Accel-Buffering': 'no' }
    );
    expect(headers['Content-Type']).toBe('application/json');
    expect(headers['X-Accel-Buffering']).toBe('no');
    expect(headers['Access-Control-Allow-Origin']).toBe('https://chat.myrecruiter.ai');
  });

  test('CORS keys are NOT overridable by extras (security guarantee)', () => {
    // Caller attempts to override every security-critical CORS key
    const headers = corsHeaders(
      { headers: { origin: 'https://chat.myrecruiter.ai' } },
      {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'DELETE, TRACE',
        'Access-Control-Allow-Headers': '*',
        'Access-Control-Allow-Credentials': 'false',
      }
    );
    // Each CORS key MUST remain at its helper-controlled value
    expect(headers['Access-Control-Allow-Origin']).toBe('https://chat.myrecruiter.ai');
    expect(headers['Access-Control-Allow-Methods']).toBe('GET, POST, OPTIONS');
    expect(headers['Access-Control-Allow-Headers']).toBe('Content-Type, Authorization, X-Requested-With, Accept');
    expect(headers['Access-Control-Allow-Credentials']).toBe('true');
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
  // This is a snapshot test of BSH's own allowlist — NOT a cross-file
  // consistency check against MFS. If MFS's _CORS_ALLOWED_ORIGINS_DEFAULT
  // in lambda_function.py changes, this test still passes. There is no
  // automated cross-repo enforcement; the canonical source-of-truth pair
  // (MFS Python + this file) must be updated together by human discipline.
  // Path to MFS list: Master_Function_Staging/lambda_function.py
  // constant: _CORS_ALLOWED_ORIGINS_DEFAULT
  test('BSH allowlist snapshot (manual cross-check required against MFS lambda_function.py _CORS_ALLOWED_ORIGINS_DEFAULT)', () => {
    expect([...ALLOWED_ORIGINS].sort()).toEqual([
      'http://localhost:3000',
      'http://localhost:5173',
      'http://localhost:8000',
      'https://chat.myrecruiter.ai',
      'https://picassocode.s3.amazonaws.com',
      'https://staging.chat.myrecruiter.ai',
    ]);
  });
});
