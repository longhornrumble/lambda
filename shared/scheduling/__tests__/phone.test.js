'use strict';

const { toE164 } = require('../phone');

describe('phone.toE164 — E.164 normalization (single source of truth)', () => {
  it('prepends +1 to a bare 10-digit US number', () => {
    expect(toE164('5125551234')).toBe('+15125551234');
    expect(toE164('(512) 555-1234')).toBe('+15125551234');
  });
  it('keeps an 11-digit number with the leading US country code (no double prefix)', () => {
    expect(toE164('15125551234')).toBe('+15125551234');
    expect(toE164('1 (512) 555-1234')).toBe('+15125551234');
  });
  it('keeps an already-+-prefixed international number', () => {
    expect(toE164('+447911123456')).toBe('+447911123456');
    expect(toE164('+15125551234')).toBe('+15125551234');
  });
  it('returns null for empty / whitespace / non-string', () => {
    expect(toE164('')).toBeNull();
    expect(toE164('   ')).toBeNull();
    expect(toE164(null)).toBeNull();
    expect(toE164(undefined)).toBeNull();
    expect(toE164(5125551234)).toBeNull();
  });
  it('returns null when normalization cannot reach a valid +<10..15 digits>', () => {
    expect(toE164('123')).toBeNull(); // +1123 → 4 digits, fails the regex
    expect(toE164('+1234567890123456')).toBeNull(); // 16 digits, too long
    expect(toE164('abc')).toBeNull();
  });
});

describe('phone.toE164 — consent.js re-export parity', () => {
  it('consent.js re-exports the SAME toE164 (key-match invariant with the writer)', () => {
    const { toE164: consentToE164 } = require('../consent');
    expect(consentToE164).toBe(toE164); // same function reference, not a copy
  });
});
