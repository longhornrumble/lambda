'use strict';

const { classifyTokenError, PERMANENT_MARKERS } = require('./revocation');

describe('classifyTokenError — revocation (permanent) vs stale-connected (transient/5xx)', () => {
  // The work-order's core requirement: invalid_grant → disconnect; 5xx → stale-connected.

  test('OAuth token-endpoint STRING shape: invalid_grant → permanent (→ disconnect)', () => {
    const err = { code: 400, response: { status: 400, data: { error: 'invalid_grant' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: true, httpStatus: 400 });
  });

  test('Calendar-API OBJECT shape: { message } with marker → permanent (no .includes crash)', () => {
    const err = {
      response: { status: 401, data: { error: { code: 401, message: 'Token has been expired or revoked' } } },
    };
    expect(classifyTokenError(err).permanent).toBe(true);
  });

  test('marker only in err.message → permanent', () => {
    expect(classifyTokenError(new Error('unauthorized_client: bad app')).permanent).toBe(true);
  });

  test('5xx server error → NOT permanent (stale-connected, secret left intact)', () => {
    const err = { code: 503, response: { status: 503, data: { error: 'backendError' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, httpStatus: 503 });
  });

  test('plain 401 with no permanent marker → NOT permanent (conservative)', () => {
    const err = { response: { status: 401, data: { error: 'temporarily_unavailable' } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, httpStatus: 401 });
  });

  test('network error (no response) → NOT permanent, httpStatus null', () => {
    expect(classifyTokenError(new Error('ETIMEDOUT'))).toEqual({ permanent: false, httpStatus: null });
  });

  test('object error without message/status coerces to "" without throwing', () => {
    const err = { response: { status: 500, data: { error: { errors: [] } } } };
    expect(classifyTokenError(err)).toEqual({ permanent: false, httpStatus: 500 });
  });

  test('null/undefined error is handled', () => {
    expect(classifyTokenError(null)).toEqual({ permanent: false, httpStatus: null });
    expect(classifyTokenError(undefined)).toEqual({ permanent: false, httpStatus: null });
  });

  test('all four shipped markers are covered', () => {
    for (const m of PERMANENT_MARKERS) {
      expect(classifyTokenError(new Error(`prefix ${m} suffix`)).permanent).toBe(true);
    }
  });
});
