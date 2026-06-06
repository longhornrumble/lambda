'use strict';

const { classifyTokenError, PERMANENT_MARKERS, PLATFORM_MARKERS } = require('./revocation');

describe('classifyTokenError — per-coordinator revocation vs platform vs transient', () => {
  // Work-order core: invalid_grant → disconnect; 5xx → stale-connected.
  // Integrator directive #3: invalid_client is PLATFORM, NOT a per-coordinator revocation.

  test('STRING shape invalid_grant → permanent (→ disconnect), not platform', () => {
    const err = { code: 400, response: { status: 400, data: { error: 'invalid_grant' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: true, platform: false, httpStatus: 400 });
  });

  test('Calendar-API OBJECT shape with marker → permanent (no .includes crash)', () => {
    const err = {
      response: { status: 401, data: { error: { code: 401, message: 'Token has been expired or revoked' } } },
    };
    expect(classifyTokenError(err)).toMatchObject({ permanent: true, platform: false });
  });

  test('unauthorized_client in err.message → permanent', () => {
    expect(classifyTokenError(new Error('unauthorized_client: bad app')).permanent).toBe(true);
  });

  test('invalid_client → PLATFORM (NOT permanent) — must not mass-revoke coordinators', () => {
    const err = { code: 401, response: { status: 401, data: { error: 'invalid_client' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, platform: true, httpStatus: 401 });
  });

  test('5xx server error → neither permanent nor platform (stale-connected)', () => {
    const err = { code: 503, response: { status: 503, data: { error: 'backendError' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, platform: false, httpStatus: 503 });
  });

  test('plain 401 with no marker → neither (conservative)', () => {
    const err = { response: { status: 401, data: { error: 'temporarily_unavailable' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, platform: false, httpStatus: 401 });
  });

  test('network error (no response) → neither, httpStatus null', () => {
    expect(classifyTokenError(new Error('ETIMEDOUT'))).toEqual({ permanent: false, platform: false, httpStatus: null });
  });

  test('object error without message/status coerces to "" without throwing', () => {
    const err = { response: { status: 500, data: { error: { errors: [] } } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, platform: false, httpStatus: 500 });
  });

  test('null/undefined error is handled', () => {
    expect(classifyTokenError(null)).toEqual({ permanent: false, platform: false, httpStatus: null });
    expect(classifyTokenError(undefined)).toEqual({ permanent: false, platform: false, httpStatus: null });
  });

  test('every PERMANENT marker classifies permanent; invalid_client is NOT among them', () => {
    for (const m of PERMANENT_MARKERS) {
      expect(classifyTokenError(new Error(`prefix ${m} suffix`)).permanent).toBe(true);
    }
    expect(PERMANENT_MARKERS).not.toContain('invalid_client');
  });

  test('every PLATFORM marker classifies platform', () => {
    for (const m of PLATFORM_MARKERS) {
      expect(classifyTokenError(new Error(`x ${m} y`)).platform).toBe(true);
    }
  });
});
