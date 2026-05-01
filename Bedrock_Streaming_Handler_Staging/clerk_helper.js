/**
 * Clerk Helper for Bedrock Streaming Handler
 * Phase 4 — Resolves recipient_user_ids to email/phone at send time
 * Uses Clerk Backend API with in-memory 5-minute cache
 */

const https = require('https');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const CLERK_API_BASE = 'api.clerk.com';
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const CLERK_SECRET_ID = process.env.CLERK_SECRET_KEY_SECRET_ID || 'prod/clerk/picasso/secret_key';

// In-memory cache: userId → { data, fetchedAt }
const userCache = new Map();

// Cached Clerk secret key. Lambda lifetime — fetched once per cold start.
const smClient = new SecretsManagerClient({});
let cachedSecretKey = null;
let secretLoadingPromise = null;

/**
 * Resolve the Clerk secret key. Prefers process.env.CLERK_SECRET_KEY when set
 * (backwards compat during the env-var → Secrets Manager migration). Once the
 * env var is removed, falls back to Secrets Manager and caches for the
 * Lambda's lifetime.
 */
async function getClerkSecretKey() {
  if (process.env.CLERK_SECRET_KEY) {
    return process.env.CLERK_SECRET_KEY;
  }
  if (cachedSecretKey) {
    return cachedSecretKey;
  }
  if (!secretLoadingPromise) {
    secretLoadingPromise = (async () => {
      const result = await smClient.send(new GetSecretValueCommand({ SecretId: CLERK_SECRET_ID }));
      const raw = result.SecretString || '';
      // Console-created secrets store JSON like {"secret_key": "sk_live_..."}.
      // Plaintext-stored secrets are just the raw key string. Handle both.
      let value = raw;
      try {
        const parsed = JSON.parse(raw);
        value = parsed.secret_key || parsed.CLERK_SECRET_KEY || parsed.value || raw;
      } catch (_) { /* not JSON — use raw */ }
      cachedSecretKey = value;
      return value;
    })();
  }
  return secretLoadingPromise;
}

/**
 * Fetch a Clerk user by ID with in-memory caching.
 * Returns null for 404 (user deleted). Throws Error on other failures.
 */
async function fetchClerkUser(userId) {
  const secretKey = await getClerkSecretKey();
  if (!secretKey) {
    throw new Error('CLERK_SECRET_KEY not configured (env var unset and Secrets Manager fetch returned empty)');
  }

  // Check cache
  const cached = userCache.get(userId);
  if (cached && (Date.now() - cached.fetchedAt) < CACHE_TTL_MS) {
    return cached.data;
  }

  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: CLERK_API_BASE,
      path: `/v1/users/${userId}`,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${secretKey}`,
        'User-Agent': 'Picasso-BedrockHandler/1.0',
        'Accept': 'application/json',
      },
      timeout: 5000,
    }, (res) => {
      let body = '';
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => {
        if (res.statusCode === 404) {
          console.warn(`[clerk_helper] User ${userId} not found (404) — skipping`);
          resolve(null);
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`Clerk API returned ${res.statusCode} for user ${userId}: ${body.slice(0, 200)}`));
          return;
        }
        try {
          const data = JSON.parse(body);
          userCache.set(userId, { data, fetchedAt: Date.now() });
          resolve(data);
        } catch (e) {
          reject(new Error(`Failed to parse Clerk API response for ${userId}`));
        }
      });
    });
    req.on('error', (e) => reject(new Error(`Clerk API request failed for ${userId}: ${e.message}`)));
    req.on('timeout', () => { req.destroy(); reject(new Error(`Clerk API timeout for ${userId}`)); });
    req.end();
  });
}

/**
 * Extract primary email from Clerk user object.
 */
function extractEmailFromClerkUser(userData) {
  const emails = userData.email_addresses || [];
  const primary = emails.find(e => e.id === userData.primary_email_address_id);
  const addr = primary || emails[0];
  if (!addr) return null;
  return addr.email_address.toLowerCase().trim();
}

/**
 * Extract phone from Clerk user unsafeMetadata.notification_preferences.phone
 * NOT Clerk's native phone_numbers (requires paid SMS add-on)
 */
function extractPhoneFromClerkUser(userData) {
  return userData?.unsafe_metadata?.notification_preferences?.phone || null;
}

/**
 * Get user's notification preferences from unsafeMetadata.
 */
function getUserNotificationPreferences(userData) {
  return userData?.unsafe_metadata?.notification_preferences || {
    email: true,
    sms: false,
    phone: null,
    sms_quiet_hours: { enabled: false },
  };
}

/**
 * Check if current time falls within user's configured quiet hours.
 *
 * Cases:
 * 1. start < end (e.g. 09:00–17:00): quiet if current >= start AND current < end
 * 2. start > end (overnight, e.g. 19:00–07:00): quiet if current >= start OR current < end
 * 3. start === end: always quiet (full 24-hour window)
 * 4. Invalid timezone: log warning, treat as NOT in quiet hours
 */
function isInQuietHours(prefs) {
  const qh = prefs?.sms_quiet_hours;
  if (!qh?.enabled || !qh.start || !qh.end || !qh.timezone) return false;

  let nowInTz;
  try {
    nowInTz = new Date().toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      timeZone: qh.timezone,
    }); // "HH:MM"
  } catch (e) {
    console.warn(`[clerk_helper] Invalid timezone "${qh.timezone}" for quiet hours — allowing send`);
    return false;
  }

  const current = nowInTz.slice(0, 5);
  const { start, end } = qh;

  if (start === end) return true; // 24-hour quiet window

  if (start < end) {
    return current >= start && current < end;
  } else {
    // Overnight (e.g. 19:00–07:00)
    return current >= start || current < end;
  }
}

/**
 * Resolve user IDs to { email, userId } pairs.
 * Skips users that 404 or fail to fetch. Never throws.
 */
async function resolveEmailsFromUserIds(userIds) {
  const results = [];
  await Promise.allSettled(userIds.map(async (userId) => {
    try {
      const userData = await fetchClerkUser(userId);
      if (!userData) return; // 404
      const email = extractEmailFromClerkUser(userData);
      if (email) results.push({ email, userId });
    } catch (e) {
      console.warn(`[clerk_helper] Failed to resolve email for ${userId}: ${e.message}`);
    }
  }));
  return results;
}

/**
 * Resolve user IDs to phone numbers.
 * Skips: 404 users, users without phone, users not SMS opted in, users in quiet hours.
 * Never throws.
 */
async function resolvePhonesFromUserIds(userIds) {
  const results = [];
  await Promise.allSettled(userIds.map(async (userId) => {
    try {
      const userData = await fetchClerkUser(userId);
      if (!userData) return;
      const prefs = getUserNotificationPreferences(userData);
      if (!prefs.sms) return; // not SMS opted in
      const phone = prefs.phone;
      if (!phone) return; // no phone number
      if (isInQuietHours(prefs)) {
        console.log(`[clerk_helper] User ${userId} in quiet hours — skipping SMS`);
        return;
      }
      results.push(phone);
    } catch (e) {
      console.warn(`[clerk_helper] Failed to resolve phone for ${userId}: ${e.message}`);
    }
  }));
  return results;
}

/**
 * Returns emails for users in quiet hours who have fallback_to_email enabled.
 * These users should receive email instead of SMS during quiet hours.
 */
async function resolveQuietHoursFallbackEmails(userIds) {
  const fallbackEmails = [];
  await Promise.allSettled(userIds.map(async (userId) => {
    try {
      const userData = await fetchClerkUser(userId);
      if (!userData) return;
      const prefs = getUserNotificationPreferences(userData);
      if (!prefs.sms) return; // not SMS opted in, not relevant for fallback
      if (isInQuietHours(prefs) && prefs.sms_quiet_hours?.fallback_to_email) {
        const email = extractEmailFromClerkUser(userData);
        if (email) fallbackEmails.push(email);
      }
    } catch (e) {
      console.warn(`[clerk_helper] quiet-hours fallback failed for ${userId}: ${e.message}`);
    }
  }));
  return fallbackEmails;
}

module.exports = {
  fetchClerkUser,
  extractEmailFromClerkUser,
  extractPhoneFromClerkUser,
  getUserNotificationPreferences,
  isInQuietHours,
  resolveEmailsFromUserIds,
  resolvePhonesFromUserIds,
  resolveQuietHoursFallbackEmails,
};
