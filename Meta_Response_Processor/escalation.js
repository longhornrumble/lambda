'use strict';

/**
 * M6a — Escalation: transfer + notify (docs/messenger/CONTRACTS.md C2/C4).
 *
 * "Talk to a human" intent detection + the three-step handoff:
 *   1. pass_thread_control to the tenant's inbox app (Business Suite inbox)
 *   2. a 24h pause row in the C4 conversation-state table (bot stands down)
 *   3. a best-effort SES email to staff (deep link only — no conversation
 *      content, no psid; G-P2 PII minimization)
 *
 * Every function here is defensive on the escalation *side effects*
 * (thread-control handoff, email) — a tenant that hasn't configured
 * Conversation Routing, or has no escalation_email set, must never block the
 * user-visible confirmation or the pause. `detectEscalationIntent` and
 * `writePauseRow` are the two exceptions: intent detection is a pure
 * predicate (nothing to fail), and a pause-row write failure is surfaced to
 * the caller (index.js) so it can log at ERROR — silently pretending the
 * bot is paused when the row never landed would be worse than a loud error.
 *
 * Env vars read at CALL TIME (not module load) so tests can set
 * process.env.* freely without jest.resetModules() gymnastics:
 *   FB_INBOX_APP_ID — target_app_id for Facebook Messenger (default: Meta's
 *     published Page Inbox app id, 263902037430900)
 *   IG_INBOX_APP_ID — target_app_id for Instagram DM (default: 1217981644879628)
 *   SES_FROM_EMAIL  — verified SES sender; '' (default) disables staff email
 */

const { SendEmailCommand } = require('@aws-sdk/client-ses');
const { PutCommand } = require('@aws-sdk/lib-dynamodb');

const DEFAULT_FB_INBOX_APP_ID = '263902037430900';
const DEFAULT_IG_INBOX_APP_ID = '1217981644879628';

const META_GRAPH_VERSION = 'v21.0';
const META_GRAPH_BASE = `https://graph.facebook.com/${META_GRAPH_VERSION}`;

const PAUSE_TTL_SECONDS = 24 * 60 * 60; // C4: ~24h thread-control idle expiry

// ─── Intent detection ─────────────────────────────────────────────────────────

// Tight, word-boundary-anchored, first-person "connect me to a human" phrasing.
// Deliberately narrow: general nouns like "human"/"person"/"agent" alone must
// NEVER trip this (false-positive risk called out in the plan: "how do humans
// apply?" must not escalate). Every phrase below REQUIRES a human-noun object
// (code review finding: bare "connect me to/with" and "transfer me" matched
// "connect me to the volunteer page" / "transfer me some documents" — object
// gating closes that).
const LEAD =
  "(?:can\\s+i|could\\s+i|i\\s+want\\s+to|i\\s+need\\s+to|i'?d\\s+like\\s+to|let\\s+me|please)?";
const VERB = '(?:talk|speak|chat)';
const PREP = '(?:to|with)';
const HUMAN_NOUN =
  '(?:a\\s+|an\\s+|the\\s+)?(?:real\\s+human|real\\s+person|human(?:\\s+being)?|person|agent|representative|someone|staff)';

const ESCALATION_PATTERNS = [
  new RegExp(`\\b${LEAD}\\s*${VERB}\\s+${PREP}\\s+${HUMAN_NOUN}\\b`, 'i'),
  /\bhuman agent\b/i,
  /\breal human\b/i,
  new RegExp(`\\bconnect me (?:to|with)\\s+${HUMAN_NOUN}\\b`, 'i'),
  new RegExp(`\\btransfer me\\s+(?:to|over to)\\s+${HUMAN_NOUN}\\b`, 'i'),
  /\bspeak to staff\b/i,
];

// Negation guard (code review finding): "I do not want to talk to a human",
// "rather not talk to an agent" etc. previously matched, because the regexes
// above search for a substring match anywhere in the text — a negation word
// several tokens EARLIER in the sentence is invisible to a plain .test(). We
// require an .exec() (to get the match position) and reject the match if a
// negation cue appears in a short window immediately before it.
const NEGATION_WINDOW_CHARS = 30; // ~5-6 tokens — covers "I do not want to ", "rather not ", "no thanks I do not need to " with margin
const NEGATION_PATTERN =
  /\b(?:don'?t|do not|doesn'?t|does not|didn'?t|did not|won'?t|will not|wouldn'?t|would not|can'?t|cannot|couldn'?t|could not|never|no need|not need|rather not|no thanks?)\b/i;

/**
 * Detect a tight, first-person "connect me to a human" request. Negated
 * phrasing ("I do not want to talk to a human") is deliberately excluded.
 * @param {string} text — sanitized user input
 * @returns {boolean}
 */
function detectEscalationIntent(text) {
  if (typeof text !== 'string' || text.trim().length === 0) return false;
  for (const pattern of ESCALATION_PATTERNS) {
    const match = pattern.exec(text);
    if (!match) continue;
    const precedingWindow = text.slice(Math.max(0, match.index - NEGATION_WINDOW_CHARS), match.index);
    if (NEGATION_PATTERN.test(precedingWindow)) continue; // negated — not a real request
    return true;
  }
  return false;
}

// ─── Thread-control handoff ───────────────────────────────────────────────────

/**
 * Hand off Messenger thread control to the tenant's configured inbox app
 * (Business Suite / Page Inbox) so a human staff member can reply.
 *
 * Defensive: a tenant that hasn't set Conversation Routing / doesn't have us
 * as the default app returns a non-2xx here — that is NOT fatal. Logs a WARN
 * and returns {ok:false} so the caller's email + pause still proceed.
 * NEVER throws (network errors are also caught and reported as {ok:false}).
 *
 * @param {{pageId: string, psid: string, channelType: string, accessToken: string, metadata: string}} params
 * @returns {Promise<{ok: boolean}>}
 */
async function passThreadControl({ pageId, psid, channelType, accessToken, metadata }) {
  const targetAppId =
    channelType === 'instagram'
      ? process.env.IG_INBOX_APP_ID || DEFAULT_IG_INBOX_APP_ID
      : process.env.FB_INBOX_APP_ID || DEFAULT_FB_INBOX_APP_ID;

  // access_token rides the JSON body (code review: match this file's existing
  // convention — callMetaSendApi/sendStructuredMessage in index.js both put
  // access_token in the body, never the URL query string).
  const url = `${META_GRAPH_BASE}/me/pass_thread_control`;
  const body = {
    recipient: { id: psid },
    target_app_id: targetAppId,
    // Short, content-free metadata — NEVER conversation text (G-P2).
    metadata: metadata || 'picasso-escalation',
    access_token: accessToken,
  };

  let response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (networkErr) {
    console.warn(
      JSON.stringify({
        level: 'WARN',
        message: 'pass_thread_control network error — continuing (email + pause still proceed)',
        service: 'MetaResponseProcessor',
        pageId,
        channelType,
        error: networkErr.message,
      })
    );
    return { ok: false };
  }

  if (!response.ok) {
    const errorBody = await response.json().catch(() => ({}));
    console.warn(
      JSON.stringify({
        level: 'WARN',
        message: 'pass_thread_control failed — tenant may not have Conversation Routing configured',
        service: 'MetaResponseProcessor',
        pageId,
        channelType,
        status: response.status,
        errorBody,
      })
    );
    return { ok: false };
  }

  return { ok: true };
}

// ─── Pause row (C4) ───────────────────────────────────────────────────────────

/**
 * Write the C4 `pause` row: bot stands down for ~24h while staff owns the
 * thread (M6b consumes this to resume). Plain PutItem — an escalation retry
 * overwriting an existing pause row is fine (idempotent).
 *
 * Does NOT catch errors itself — the caller decides how loud to be about a
 * failed pause write (index.js logs ERROR and continues; a missing pause row
 * just means the next inbound gets a normal turn instead of standing down).
 *
 * @param {{client: object, tableName: string, sessionId: string}} params
 * @returns {Promise<void>}
 */
async function writePauseRow({ client, tableName, sessionId }) {
  const nowMs = Date.now();
  await client.send(
    new PutCommand({
      TableName: tableName,
      Item: {
        sessionId,
        stateType: 'pause',
        reason: 'escalation',
        paused_at: nowMs,
        updated_at: nowMs,
        schema_version: 1,
        expires_at: Math.floor(nowMs / 1000) + PAUSE_TTL_SECONDS,
      },
    })
  );
}

// ─── Staff email (SES) ────────────────────────────────────────────────────────

/**
 * Best-effort SES email to staff when a Messenger user asks for a human.
 *
 * PII-minimal by design (G-P2 advisory): the body carries ONLY channel,
 * tenantId, pageId, timestamp, and a deep link into the Business Suite inbox
 * — the inbox thread itself is where staff read the actual conversation.
 * NO psid, NO session id, NO message content ever goes into this email or
 * into any log line this function emits (only a boolean emailSent + tenantId
 * are logged — never the recipient address).
 *
 * Never throws: absent recipient, unset SES_FROM_EMAIL, or an SES failure
 * all resolve to a logged outcome, never an exception.
 *
 * @param {{sesClient: object, config: object, tenantId: string, channelType: string, sessionId: string, pageId?: string}} params
 * @returns {Promise<{skipped: true, reason: string}|{sent: true}|{failed: true}>}
 */
async function sendEscalationEmail({ sesClient, config, tenantId, channelType, sessionId, pageId }) {
  const recipient = config?.messenger_behavior?.escalation_email;
  const fromEmail = process.env.SES_FROM_EMAIL || '';

  if (!recipient) {
    console.log(
      JSON.stringify({
        level: 'INFO',
        message: 'Escalation email skipped — no messenger_behavior.escalation_email configured',
        service: 'MetaResponseProcessor',
        tenantId,
        emailSent: false,
      })
    );
    return { skipped: true, reason: 'no_recipient' };
  }
  if (!fromEmail) {
    console.log(
      JSON.stringify({
        level: 'INFO',
        message: 'Escalation email skipped — SES_FROM_EMAIL not configured',
        service: 'MetaResponseProcessor',
        tenantId,
        emailSent: false,
      })
    );
    return { skipped: true, reason: 'ses_disabled' };
  }

  const timestamp = new Date().toISOString();
  const subject = `A Messenger user asked for a human — ${tenantId}`;
  // Content-free body (G-P2): channel/tenant/page/time + a deep link only.
  // Deliberately NO psid, NO sessionId, NO message text — staff read the
  // actual conversation in the Business Suite inbox thread itself.
  const bodyLines = [
    `Channel: ${channelType}`,
    `Tenant: ${tenantId}`,
    ...(pageId ? [`Page: ${pageId}`] : []),
    `Time: ${timestamp}`,
    '',
    'Open the conversation: https://business.facebook.com/latest/inbox',
  ];
  const textBody = bodyLines.join('\n');

  const params = {
    Source: fromEmail,
    Destination: { ToAddresses: [recipient] },
    Message: {
      Subject: { Data: subject },
      Body: { Text: { Data: textBody } },
    },
  };

  try {
    await sesClient.send(new SendEmailCommand(params));
    console.log(
      JSON.stringify({
        level: 'INFO',
        message: 'Escalation email sent',
        service: 'MetaResponseProcessor',
        tenantId,
        emailSent: true,
      })
    );
    return { sent: true };
  } catch (sesErr) {
    console.error(
      JSON.stringify({
        level: 'ERROR',
        message: 'Escalation email failed to send',
        service: 'MetaResponseProcessor',
        tenantId,
        emailSent: false,
        error: sesErr.message,
      })
    );
    return { failed: true };
  }
}

module.exports = {
  detectEscalationIntent,
  passThreadControl,
  writePauseRow,
  sendEscalationEmail,
  DEFAULT_FB_INBOX_APP_ID,
  DEFAULT_IG_INBOX_APP_ID,
};
