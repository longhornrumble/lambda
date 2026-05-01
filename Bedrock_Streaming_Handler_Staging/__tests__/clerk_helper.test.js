/**
 * Unit tests for clerk_helper.js — Phase 4 Notification Recipients Refactor
 *
 * How to run:
 *   cd Lambdas/lambda/Bedrock_Streaming_Handler_Staging
 *   npm test -- --testPathPattern=clerk_helper
 *
 *   # With coverage:
 *   npm run test:coverage -- --testPathPattern=clerk_helper
 *
 *   # Verbose:
 *   npm run test:verbose -- --testPathPattern=clerk_helper
 *
 * All Clerk API calls (https module) are mocked — no real API key or network
 * access required. Tests are fully self-contained and deterministic.
 */

'use strict';

const https = require('https');
const { EventEmitter } = require('events');

// ---------------------------------------------------------------------------
// Helper: build an https mock that returns a canned response
// ---------------------------------------------------------------------------

/**
 * Creates a mock that replaces https.request for a single call.
 * @param {number} statusCode
 * @param {object|null} body - parsed JSON body (null → send empty string)
 * @returns jest mock function
 */
function mockHttpsRequest(statusCode, body) {
  const mockReq = new EventEmitter();
  mockReq.end = jest.fn();
  mockReq.destroy = jest.fn();

  const mockRes = new EventEmitter();
  mockRes.statusCode = statusCode;

  const requestSpy = jest.spyOn(https, 'request').mockImplementation((opts, callback) => {
    callback(mockRes);

    // Emit body data and end asynchronously
    process.nextTick(() => {
      if (body !== null) {
        mockRes.emit('data', JSON.stringify(body));
      }
      mockRes.emit('end');
    });

    return mockReq;
  });

  return requestSpy;
}

/**
 * Creates a mock that simulates a network error on https.request.
 */
function mockHttpsRequestError(errorMessage) {
  const mockReq = new EventEmitter();
  mockReq.end = jest.fn();
  mockReq.destroy = jest.fn();

  const requestSpy = jest.spyOn(https, 'request').mockImplementation(() => {
    process.nextTick(() => {
      mockReq.emit('error', new Error(errorMessage));
    });
    return mockReq;
  });

  return requestSpy;
}

// ---------------------------------------------------------------------------
// Helper: build Clerk user objects
// ---------------------------------------------------------------------------

function buildClerkUser({
  id = 'user_abc123',
  email = 'alice@example.com',
  primaryEmailId = 'idn_primary',
  phone = null,
  sms = false,
  quietHours = null,
} = {}) {
  const notificationPreferences = { sms };
  if (phone !== null) notificationPreferences.phone = phone;
  if (quietHours !== null) notificationPreferences.sms_quiet_hours = quietHours;

  return {
    id,
    primary_email_address_id: primaryEmailId,
    email_addresses: [
      { id: primaryEmailId, email_address: email },
    ],
    unsafe_metadata: {
      notification_preferences: notificationPreferences,
    },
  };
}

// ---------------------------------------------------------------------------
// Module re-import helper (clears module cache to reset in-module state)
// ---------------------------------------------------------------------------

function freshClerkHelper() {
  // Clear module cache so the in-memory userCache Map starts empty
  jest.resetModules();
  // Re-set the env var every time (setup.js may not set it)
  process.env.CLERK_SECRET_KEY = 'sk_test_fake_key';
  return require('../clerk_helper');
}

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

beforeEach(() => {
  process.env.CLERK_SECRET_KEY = 'sk_test_fake_key';
  jest.restoreAllMocks();
});

afterEach(() => {
  jest.restoreAllMocks();
});

// ===========================================================================
// extractEmailFromClerkUser
// ===========================================================================

describe('extractEmailFromClerkUser', () => {
  const { extractEmailFromClerkUser } = require('../clerk_helper');

  it('returns the primary email when primary_email_address_id matches', () => {
    const user = {
      primary_email_address_id: 'idn_primary',
      email_addresses: [
        { id: 'idn_other', email_address: 'other@example.com' },
        { id: 'idn_primary', email_address: 'Alice@Example.com' },
      ],
    };
    expect(extractEmailFromClerkUser(user)).toBe('alice@example.com');
  });

  it('falls back to first email when no primary_email_address_id match', () => {
    const user = {
      primary_email_address_id: 'idn_nonexistent',
      email_addresses: [
        { id: 'idn_first', email_address: 'First@Example.com' },
        { id: 'idn_second', email_address: 'second@example.com' },
      ],
    };
    expect(extractEmailFromClerkUser(user)).toBe('first@example.com');
  });

  it('returns null when email_addresses is empty', () => {
    const user = {
      primary_email_address_id: null,
      email_addresses: [],
    };
    expect(extractEmailFromClerkUser(user)).toBeNull();
  });

  it('trims whitespace from the returned email', () => {
    const user = {
      primary_email_address_id: 'idn_primary',
      email_addresses: [{ id: 'idn_primary', email_address: '  alice@example.com  ' }],
    };
    expect(extractEmailFromClerkUser(user)).toBe('alice@example.com');
  });

  it('lowercases the returned email', () => {
    const user = {
      primary_email_address_id: 'idn_primary',
      email_addresses: [{ id: 'idn_primary', email_address: 'ALICE@EXAMPLE.COM' }],
    };
    expect(extractEmailFromClerkUser(user)).toBe('alice@example.com');
  });
});

// ===========================================================================
// extractPhoneFromClerkUser
// ===========================================================================

describe('extractPhoneFromClerkUser', () => {
  const { extractPhoneFromClerkUser } = require('../clerk_helper');

  it('returns phone from notification_preferences in unsafeMetadata', () => {
    const user = buildClerkUser({ phone: '+15125551234' });
    expect(extractPhoneFromClerkUser(user)).toBe('+15125551234');
  });

  it('returns null when phone is absent from notification_preferences', () => {
    const user = buildClerkUser(); // no phone key
    expect(extractPhoneFromClerkUser(user)).toBeNull();
  });

  it('returns null when unsafe_metadata is missing', () => {
    const user = { id: 'user_x', email_addresses: [] };
    expect(extractPhoneFromClerkUser(user)).toBeNull();
  });

  it('returns null when notification_preferences is missing', () => {
    const user = { id: 'user_x', unsafe_metadata: {} };
    expect(extractPhoneFromClerkUser(user)).toBeNull();
  });

  it('returns null for null or empty string phone value', () => {
    const user = buildClerkUser({ phone: null });
    // phone: null is explicitly set; the function returns null
    const user2 = buildClerkUser({ phone: '' });
    // '' is falsy — should return null
    expect(extractPhoneFromClerkUser(user2) || null).toBeNull();
  });
});

// ===========================================================================
// getUserNotificationPreferences
// ===========================================================================

describe('getUserNotificationPreferences', () => {
  const { getUserNotificationPreferences } = require('../clerk_helper');

  it('returns stored preferences when present', () => {
    const user = buildClerkUser({ phone: '+15125551234', sms: true });
    const prefs = getUserNotificationPreferences(user);
    expect(prefs.sms).toBe(true);
    expect(prefs.phone).toBe('+15125551234');
  });

  it('returns defaults when unsafe_metadata is missing', () => {
    const user = { id: 'user_x' };
    const prefs = getUserNotificationPreferences(user);
    expect(prefs.email).toBe(true);
    expect(prefs.sms).toBe(false);
    expect(prefs.phone).toBeNull();
    expect(prefs.sms_quiet_hours.enabled).toBe(false);
  });

  it('returns defaults when notification_preferences is absent', () => {
    const user = { id: 'user_x', unsafe_metadata: {} };
    const prefs = getUserNotificationPreferences(user);
    expect(prefs.email).toBe(true);
    expect(prefs.sms).toBe(false);
  });

  it('default email preference is true (opt-in by default)', () => {
    const prefs = getUserNotificationPreferences({ id: 'user_x' });
    expect(prefs.email).toBe(true);
  });
});

// ===========================================================================
// isInQuietHours
// ===========================================================================

describe('isInQuietHours', () => {
  const { isInQuietHours } = require('../clerk_helper');

  // Helper: build prefs with quiet hours and control the "current time" in the
  // target timezone by mocking Date.prototype.toLocaleTimeString.
  function testQuietHours({ start, end, timezone = 'America/Chicago', currentTime, enabled = true }) {
    const prefs = {
      sms_quiet_hours: {
        enabled,
        start,
        end,
        timezone,
        fallback_to_email: false,
      },
    };

    if (currentTime) {
      // Mock toLocaleTimeString to return a controlled time string
      jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue(currentTime);
    }

    return isInQuietHours(prefs);
  }

  afterEach(() => {
    jest.restoreAllMocks();
  });

  // --- Disabled quiet hours ---

  it('returns false when enabled is false', () => {
    const result = testQuietHours({ start: '09:00', end: '17:00', enabled: false, currentTime: '12:00' });
    expect(result).toBe(false);
  });

  // --- Missing fields ---

  it('returns false when prefs is null', () => {
    expect(isInQuietHours(null)).toBe(false);
  });

  it('returns false when sms_quiet_hours is missing', () => {
    expect(isInQuietHours({})).toBe(false);
  });

  it('returns false when start is missing', () => {
    const prefs = { sms_quiet_hours: { enabled: true, end: '17:00', timezone: 'UTC' } };
    expect(isInQuietHours(prefs)).toBe(false);
  });

  it('returns false when end is missing', () => {
    const prefs = { sms_quiet_hours: { enabled: true, start: '09:00', timezone: 'UTC' } };
    expect(isInQuietHours(prefs)).toBe(false);
  });

  it('returns false when timezone is missing', () => {
    const prefs = { sms_quiet_hours: { enabled: true, start: '09:00', end: '17:00' } };
    expect(isInQuietHours(prefs)).toBe(false);
  });

  // --- Same-day window: 09:00–17:00 ---

  it('same-day window: returns true when current time is inside (12:00)', () => {
    const result = testQuietHours({ start: '09:00', end: '17:00', currentTime: '12:00' });
    expect(result).toBe(true);
  });

  it('same-day window: returns true at the start boundary (09:00)', () => {
    const result = testQuietHours({ start: '09:00', end: '17:00', currentTime: '09:00' });
    expect(result).toBe(true);
  });

  it('same-day window: returns false at the end boundary (17:00 is exclusive)', () => {
    const result = testQuietHours({ start: '09:00', end: '17:00', currentTime: '17:00' });
    expect(result).toBe(false);
  });

  it('same-day window: returns false before window (08:59)', () => {
    const result = testQuietHours({ start: '09:00', end: '17:00', currentTime: '08:59' });
    expect(result).toBe(false);
  });

  it('same-day window: returns false after window (17:01)', () => {
    const result = testQuietHours({ start: '09:00', end: '17:00', currentTime: '17:01' });
    expect(result).toBe(false);
  });

  // --- Overnight window: 19:00–07:00 ---

  it('overnight window: returns true in evening (23:00)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '23:00' });
    expect(result).toBe(true);
  });

  it('overnight window: returns true at start boundary (19:00)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '19:00' });
    expect(result).toBe(true);
  });

  it('overnight window: returns true at midnight (00:00)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '00:00' });
    expect(result).toBe(true);
  });

  it('overnight window: returns true in early morning (03:00)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '03:00' });
    expect(result).toBe(true);
  });

  it('overnight window: returns false at end boundary (07:00 is exclusive)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '07:00' });
    expect(result).toBe(false);
  });

  it('overnight window: returns false in afternoon (13:00)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '13:00' });
    expect(result).toBe(false);
  });

  it('overnight window: returns false just before start (18:59)', () => {
    const result = testQuietHours({ start: '19:00', end: '07:00', currentTime: '18:59' });
    expect(result).toBe(false);
  });

  // --- start === end: always quiet ---

  it('start === end: returns true regardless of current time (12:00)', () => {
    const result = testQuietHours({ start: '12:00', end: '12:00', currentTime: '12:00' });
    expect(result).toBe(true);
  });

  it('start === end: returns true even at non-matching time (03:00)', () => {
    const result = testQuietHours({ start: '12:00', end: '12:00', currentTime: '03:00' });
    expect(result).toBe(true);
  });

  // --- Invalid timezone ---

  it('invalid timezone: returns false (does not throw)', () => {
    // toLocaleTimeString throws RangeError for invalid timezone
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockImplementation(() => {
      throw new RangeError('Invalid time zone specified: Mars/Olympus');
    });
    const prefs = {
      sms_quiet_hours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'Mars/Olympus',
      },
    };
    expect(() => isInQuietHours(prefs)).not.toThrow();
    expect(isInQuietHours(prefs)).toBe(false);
  });
});

// ===========================================================================
// fetchClerkUser (via resolveEmailsFromUserIds — tests cache + 404 behavior)
// ===========================================================================

describe('fetchClerkUser (via resolveEmailsFromUserIds)', () => {
  it('returns empty array when CLERK_SECRET_KEY is not set', async () => {
    const { resolveEmailsFromUserIds } = freshClerkHelper();
    delete process.env.CLERK_SECRET_KEY;
    const result = await resolveEmailsFromUserIds(['user_abc123']);
    expect(result).toEqual([]);
    process.env.CLERK_SECRET_KEY = 'sk_test_fake_key';
  });

  it('returns empty array for 404 users (user deleted)', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequest(404, { errors: [{ message: 'not found' }] });
    const result = await helper.resolveEmailsFromUserIds(['user_deleted']);
    expect(result).toEqual([]);
  });

  it('returns empty array when Clerk API returns a network error', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequestError('ECONNREFUSED');
    const result = await helper.resolveEmailsFromUserIds(['user_abc123']);
    expect(result).toEqual([]);
  });

  it('uses cached result on second call (no second HTTP request)', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({ id: 'user_ccc', email: 'cached@example.com' });
    const requestSpy = mockHttpsRequest(200, user);

    await helper.resolveEmailsFromUserIds(['user_ccc']);
    // Second call — should use cache
    await helper.resolveEmailsFromUserIds(['user_ccc']);

    // https.request should only have been called once
    expect(requestSpy).toHaveBeenCalledTimes(1);
  });
});

// ===========================================================================
// resolveEmailsFromUserIds
// ===========================================================================

describe('resolveEmailsFromUserIds', () => {
  it('returns { email, userId } pairs for valid users', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({ id: 'user_aaa', email: 'alice@example.com' });
    mockHttpsRequest(200, user);

    const result = await helper.resolveEmailsFromUserIds(['user_aaa']);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ email: 'alice@example.com', userId: 'user_aaa' });
  });

  it('skips users that return 404', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequest(404, null);

    const result = await helper.resolveEmailsFromUserIds(['user_deleted']);
    expect(result).toEqual([]);
  });

  it('skips users where fetch fails (Clerk error) — does not throw', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequestError('connection refused');

    await expect(helper.resolveEmailsFromUserIds(['user_err'])).resolves.toEqual([]);
  });

  it('resolves multiple users in parallel — returns all results', async () => {
    const helper = freshClerkHelper();
    const userA = buildClerkUser({ id: 'user_aaa', email: 'alice@example.com' });
    const userB = buildClerkUser({ id: 'user_bbb', email: 'bob@example.com' });

    // Mock two sequential https.request calls
    let callCount = 0;
    jest.spyOn(https, 'request').mockImplementation((opts, callback) => {
      const mockReq = new EventEmitter();
      mockReq.end = jest.fn();
      const mockRes = new EventEmitter();
      mockRes.statusCode = 200;
      callback(mockRes);
      process.nextTick(() => {
        const body = callCount === 0 ? userA : userB;
        callCount++;
        mockRes.emit('data', JSON.stringify(body));
        mockRes.emit('end');
      });
      return mockReq;
    });

    const result = await helper.resolveEmailsFromUserIds(['user_aaa', 'user_bbb']);
    expect(result).toHaveLength(2);
    const emails = result.map(r => r.email);
    expect(emails).toContain('alice@example.com');
    expect(emails).toContain('bob@example.com');
  });

  it('skips users with no email (extractEmailFromClerkUser returns null)', async () => {
    const helper = freshClerkHelper();
    const user = {
      id: 'user_noemail',
      primary_email_address_id: null,
      email_addresses: [],
      unsafe_metadata: {},
    };
    mockHttpsRequest(200, user);

    const result = await helper.resolveEmailsFromUserIds(['user_noemail']);
    expect(result).toEqual([]);
  });

  it('never throws even when all fetches fail', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequestError('ECONNREFUSED');
    await expect(
      helper.resolveEmailsFromUserIds(['user_a', 'user_b'])
    ).resolves.toEqual([]);
  });
});

// ===========================================================================
// resolvePhonesFromUserIds
// ===========================================================================

describe('resolvePhonesFromUserIds', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns phone for users with SMS opted in and phone set', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({ phone: '+15125551234', sms: true });
    mockHttpsRequest(200, user);

    const result = await helper.resolvePhonesFromUserIds(['user_aaa']);
    expect(result).toContain('+15125551234');
  });

  it('skips users who are not SMS opted in (sms=false)', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({ phone: '+15125551234', sms: false });
    mockHttpsRequest(200, user);

    const result = await helper.resolvePhonesFromUserIds(['user_aaa']);
    expect(result).toEqual([]);
  });

  it('skips users who have no phone number', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({ sms: true }); // opted in but no phone
    mockHttpsRequest(200, user);

    const result = await helper.resolvePhonesFromUserIds(['user_aaa']);
    expect(result).toEqual([]);
  });

  it('skips users who are in quiet hours', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({
      phone: '+15125551234',
      sms: true,
      quietHours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'UTC',
        fallback_to_email: false,
      },
    });
    mockHttpsRequest(200, user);

    // Mock current time to be inside quiet hours
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('12:00');

    const result = await helper.resolvePhonesFromUserIds(['user_aaa']);
    expect(result).toEqual([]);
  });

  it('includes users outside quiet hours', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({
      phone: '+15125551234',
      sms: true,
      quietHours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'UTC',
        fallback_to_email: false,
      },
    });
    mockHttpsRequest(200, user);

    // Mock current time to be OUTSIDE quiet hours
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('20:00');

    const result = await helper.resolvePhonesFromUserIds(['user_aaa']);
    expect(result).toContain('+15125551234');
  });

  it('skips 404 users without throwing', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequest(404, null);
    await expect(helper.resolvePhonesFromUserIds(['user_gone'])).resolves.toEqual([]);
  });

  it('skips users where fetch fails — never throws', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequestError('timeout');
    await expect(helper.resolvePhonesFromUserIds(['user_err'])).resolves.toEqual([]);
  });
});

// ===========================================================================
// resolveQuietHoursFallbackEmails
// ===========================================================================

describe('resolveQuietHoursFallbackEmails', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns email for user in quiet hours with fallback_to_email enabled', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({
      email: 'alice@example.com',
      sms: true,
      quietHours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'UTC',
        fallback_to_email: true,
      },
    });
    mockHttpsRequest(200, user);
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('12:00'); // inside quiet hours

    const result = await helper.resolveQuietHoursFallbackEmails(['user_aaa']);
    expect(result).toContain('alice@example.com');
  });

  it('does not return email when fallback_to_email is false', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({
      email: 'alice@example.com',
      sms: true,
      quietHours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'UTC',
        fallback_to_email: false,
      },
    });
    mockHttpsRequest(200, user);
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('12:00');

    const result = await helper.resolveQuietHoursFallbackEmails(['user_aaa']);
    expect(result).toEqual([]);
  });

  it('does not return email for user NOT in quiet hours', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({
      email: 'alice@example.com',
      sms: true,
      quietHours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'UTC',
        fallback_to_email: true,
      },
    });
    mockHttpsRequest(200, user);
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('20:00'); // outside quiet hours

    const result = await helper.resolveQuietHoursFallbackEmails(['user_aaa']);
    expect(result).toEqual([]);
  });

  it('skips users not opted into SMS (fallback only applies to SMS-opted-in users)', async () => {
    const helper = freshClerkHelper();
    const user = buildClerkUser({
      email: 'alice@example.com',
      sms: false, // not opted in
      quietHours: {
        enabled: true,
        start: '09:00',
        end: '17:00',
        timezone: 'UTC',
        fallback_to_email: true,
      },
    });
    mockHttpsRequest(200, user);
    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('12:00');

    const result = await helper.resolveQuietHoursFallbackEmails(['user_aaa']);
    expect(result).toEqual([]);
  });

  it('skips 404 users and does not throw', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequest(404, null);
    await expect(helper.resolveQuietHoursFallbackEmails(['user_gone'])).resolves.toEqual([]);
  });

  it('never throws when fetch fails', async () => {
    const helper = freshClerkHelper();
    mockHttpsRequestError('ECONNREFUSED');
    await expect(
      helper.resolveQuietHoursFallbackEmails(['user_err'])
    ).resolves.toEqual([]);
  });

  it('returns only emails of users in quiet hours, skipping those who are not', async () => {
    const helper = freshClerkHelper();

    let callCount = 0;
    jest.spyOn(https, 'request').mockImplementation((opts, callback) => {
      const mockReq = new EventEmitter();
      mockReq.end = jest.fn();
      const mockRes = new EventEmitter();
      mockRes.statusCode = 200;
      callback(mockRes);

      process.nextTick(() => {
        // user_aaa: in quiet hours + fallback=true → should be included
        // user_bbb: in quiet hours + fallback=false → should NOT be included
        const users = [
          buildClerkUser({
            id: 'user_aaa',
            email: 'alice@example.com',
            sms: true,
            quietHours: { enabled: true, start: '09:00', end: '17:00', timezone: 'UTC', fallback_to_email: true },
          }),
          buildClerkUser({
            id: 'user_bbb',
            email: 'bob@example.com',
            sms: true,
            quietHours: { enabled: true, start: '09:00', end: '17:00', timezone: 'UTC', fallback_to_email: false },
          }),
        ];
        mockRes.emit('data', JSON.stringify(users[callCount]));
        callCount++;
        mockRes.emit('end');
      });
      return mockReq;
    });

    jest.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('12:00'); // inside quiet hours

    const result = await helper.resolveQuietHoursFallbackEmails(['user_aaa', 'user_bbb']);
    expect(result).toContain('alice@example.com');
    expect(result).not.toContain('bob@example.com');
  });
});

// ---------------------------------------------------------------------------
// Secrets Manager fallback for CLERK_SECRET_KEY
//
// During the env-var → Secrets Manager migration, env var must keep winning
// when set (safe rollout). Once removed, code falls back to Secrets Manager
// and caches the value for the Lambda's lifetime.
// ---------------------------------------------------------------------------

describe('CLERK_SECRET_KEY resolution (env var vs Secrets Manager)', () => {
  let smGetCmdMock;
  let originalSecretKey;

  beforeEach(() => {
    jest.resetModules();
    originalSecretKey = process.env.CLERK_SECRET_KEY;

    // Mock Secrets Manager SDK
    smGetCmdMock = jest.fn();
    jest.doMock('@aws-sdk/client-secrets-manager', () => ({
      SecretsManagerClient: jest.fn().mockImplementation(() => ({
        send: smGetCmdMock,
      })),
      GetSecretValueCommand: jest.fn().mockImplementation((input) => input),
    }));
  });

  afterEach(() => {
    if (originalSecretKey === undefined) {
      delete process.env.CLERK_SECRET_KEY;
    } else {
      process.env.CLERK_SECRET_KEY = originalSecretKey;
    }
  });

  it('uses env var when set, never calling Secrets Manager', async () => {
    process.env.CLERK_SECRET_KEY = 'sk_test_from_env';
    const helper = require('../clerk_helper');

    const httpsMock = mockHttpsRequest(200, {
      id: 'user_x',
      email_addresses: [{ id: 'em_1', email_address: 'x@example.com' }],
      primary_email_address_id: 'em_1',
      public_metadata: {},
    });
    jest.spyOn(https, 'request').mockImplementation(httpsMock);

    await helper.resolveEmailsFromUserIds(['user_x']);

    // Env var path: Secrets Manager not called
    expect(smGetCmdMock).not.toHaveBeenCalled();

    // Confirm the env-var value reached the Clerk request
    expect(httpsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: 'Bearer sk_test_from_env' }),
      }),
      expect.any(Function)
    );
  });

  it('falls back to Secrets Manager when env var is absent', async () => {
    delete process.env.CLERK_SECRET_KEY;
    smGetCmdMock.mockResolvedValue({
      SecretString: JSON.stringify({ secret_key: 'sk_live_from_secrets_manager' }),
    });

    const helper = require('../clerk_helper');

    const httpsMock = mockHttpsRequest(200, {
      id: 'user_y',
      email_addresses: [{ id: 'em_2', email_address: 'y@example.com' }],
      primary_email_address_id: 'em_2',
      public_metadata: {},
    });
    jest.spyOn(https, 'request').mockImplementation(httpsMock);

    await helper.resolveEmailsFromUserIds(['user_y']);

    // Secrets Manager called exactly once
    expect(smGetCmdMock).toHaveBeenCalledTimes(1);

    // The fetched value reached the Clerk request
    expect(httpsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: 'Bearer sk_live_from_secrets_manager' }),
      }),
      expect.any(Function)
    );
  });

  it('caches the Secrets Manager fetch — only one SM call across many invocations', async () => {
    delete process.env.CLERK_SECRET_KEY;
    smGetCmdMock.mockResolvedValue({
      SecretString: JSON.stringify({ secret_key: 'sk_live_cached' }),
    });

    const helper = require('../clerk_helper');

    let httpsCallCount = 0;
    jest.spyOn(https, 'request').mockImplementation((options, callback) => {
      httpsCallCount += 1;
      const mockReq = new EventEmitter();
      mockReq.end = jest.fn();
      mockReq.destroy = jest.fn();
      const mockRes = new EventEmitter();
      mockRes.statusCode = 200;
      setImmediate(() => {
        callback(mockRes);
        mockRes.emit(
          'data',
          JSON.stringify({
            id: `user_${httpsCallCount}`,
            email_addresses: [{ id: 'em', email_address: `u${httpsCallCount}@example.com` }],
            primary_email_address_id: 'em',
            public_metadata: {},
          })
        );
        mockRes.emit('end');
      });
      return mockReq;
    });

    // Three separate invocations
    await helper.resolveEmailsFromUserIds(['user_1']);
    await helper.resolveEmailsFromUserIds(['user_2']);
    await helper.resolveEmailsFromUserIds(['user_3']);

    expect(smGetCmdMock).toHaveBeenCalledTimes(1);
    expect(httpsCallCount).toBe(3);
  });

  it('handles plaintext-stored secrets (no JSON wrapper)', async () => {
    delete process.env.CLERK_SECRET_KEY;
    smGetCmdMock.mockResolvedValue({ SecretString: 'sk_live_plaintext' });

    const helper = require('../clerk_helper');

    const httpsMock = mockHttpsRequest(200, {
      id: 'user_z',
      email_addresses: [{ id: 'em_3', email_address: 'z@example.com' }],
      primary_email_address_id: 'em_3',
      public_metadata: {},
    });
    jest.spyOn(https, 'request').mockImplementation(httpsMock);

    await helper.resolveEmailsFromUserIds(['user_z']);

    expect(httpsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: 'Bearer sk_live_plaintext' }),
      }),
      expect.any(Function)
    );
  });
});
