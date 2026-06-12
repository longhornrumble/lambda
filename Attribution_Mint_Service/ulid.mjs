/**
 * Minimal crypto-based ULID generator (Crockford base32, uppercase).
 * No external dependencies — uses Node.js crypto.
 *
 * Format: 10-char timestamp prefix + 16-char random suffix = 26 chars total.
 * Specification: https://github.com/ulid/spec
 */

import { randomBytes } from 'crypto';

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'; // Crockford base32

/**
 * Encode a number into n Crockford base32 characters (most-significant first).
 */
function encodeBase32(value, numChars) {
  let result = '';
  for (let i = numChars - 1; i >= 0; i--) {
    result = ENCODING[value & 0x1f] + result;
    value = Math.floor(value / 32);
  }
  return result;
}

/**
 * Generate a ULID string: ep_ prefix is applied by the caller.
 * Returns a 26-character uppercase Crockford base32 string.
 */
export function generateULID() {
  const now = Date.now(); // 48-bit ms timestamp → 10 chars

  // Timestamp: 48 bits → 10 Crockford chars
  const timePart = encodeBase32(now, 10);

  // Random: 80 bits (10 bytes) → 16 Crockford chars
  const randBytes = randomBytes(10);
  let randPart = '';
  // Pack 10 bytes into 80 bits and encode as 16 base32 chars.
  // We extract 5-bit groups from the byte stream.
  let bits = 0;
  let bitsAvailable = 0;
  for (let i = 0; i < randBytes.length && randPart.length < 16; i++) {
    bits = (bits << 8) | randBytes[i];
    bitsAvailable += 8;
    while (bitsAvailable >= 5 && randPart.length < 16) {
      bitsAvailable -= 5;
      randPart += ENCODING[(bits >> bitsAvailable) & 0x1f];
    }
  }

  return timePart + randPart;
}
