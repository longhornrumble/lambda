/**
 * Notification Hub Lambda (Function URL)
 *
 * POST /notify → fan out one notification to the channels configured for
 * `source` in s3://{ROUTES_BUCKET}/{ROUTES_KEY}.
 *
 * Request (JSON):
 *   {
 *     "source":        "kb-freshness-scanner",   // required — key into routes
 *     "severity":      "info|warning|critical",  // optional, default "info"
 *     "title":         "Short headline",         // required
 *     "summary":       "One-line summary",       // required
 *     "body_markdown": "Optional long form",     // optional
 *     "action":        { "label": "Review", "url": "https://..." }  // optional
 *   }
 *
 * Auth: `x-notify-key` header must equal NOTIFY_SHARED_SECRET env var.
 *
 * Routes file shape (s3://myrecruiter-picasso/notification-routes.json):
 *   {
 *     "kb-freshness-scanner": {
 *       "channels": ["slack", "email"],
 *       "slack":    { "webhook_env": "SLACK_WEBHOOK_OPS" },
 *       "email":    { "to": ["chris@myrecruiter.ai"], "from": "notify@myrecruiter.ai" }
 *     }
 *   }
 *
 * Fire-and-forget: valid payload → 200. Per-channel failures are logged
 * but never propagated. Unknown source → 200 with empty dispatched list.
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SESv2Client, SendEmailCommand } from '@aws-sdk/client-sesv2';

const REGION = process.env.AWS_REGION || 'us-east-1';
const NOTIFY_SHARED_SECRET = process.env.NOTIFY_SHARED_SECRET;
const ROUTES_BUCKET = process.env.ROUTES_BUCKET || 'myrecruiter-picasso';
const ROUTES_KEY = process.env.ROUTES_KEY || 'notification-routes.json';
const DEFAULT_SENDER = process.env.DEFAULT_SENDER || 'notify@myrecruiter.ai';
const CONFIGURATION_SET = process.env.CONFIGURATION_SET || 'picasso-emails';
const CACHE_TTL_MS = 5 * 60 * 1000;
// Optional: when set, the hub can generate Clerk sign-in tokens per email recipient
// and embed them in the action.url so clicking from the email auto-signs the user in.
// Slack stays tokenless because it's a multi-user channel and broadcasting a token
// would let any channel member consume someone else's Clerk identity.
const CLERK_SECRET_KEY = process.env.CLERK_SECRET_KEY;
const CLERK_SIGN_IN_TOKEN_TTL_SEC = Number(process.env.CLERK_SIGN_IN_TOKEN_TTL_SEC || '3600');

const s3 = new S3Client({ region: REGION });
const ses = new SESv2Client({ region: REGION });

const SEVERITY_COLORS = {
  info: '#0F766E',
  warning: '#F59E0B',
  critical: '#DC2626',
};

let routesCache = { data: null, fetchedAt: 0 };

async function getRoutes() {
  const now = Date.now();
  if (routesCache.data && now - routesCache.fetchedAt < CACHE_TTL_MS) {
    return routesCache.data;
  }
  const res = await s3.send(new GetObjectCommand({ Bucket: ROUTES_BUCKET, Key: ROUTES_KEY }));
  const body = await res.Body.transformToString();
  routesCache = { data: JSON.parse(body), fetchedAt: now };
  return routesCache.data;
}

function buildSlackPayload({ title, summary, severity, action, body_markdown }) {
  const color = SEVERITY_COLORS[severity] || SEVERITY_COLORS.info;
  const blocks = [
    { type: 'header', text: { type: 'plain_text', text: title } },
    { type: 'section', text: { type: 'mrkdwn', text: summary } },
  ];
  if (body_markdown) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: body_markdown } });
  }
  if (action?.url) {
    blocks.push({
      type: 'actions',
      elements: [{
        type: 'button',
        text: { type: 'plain_text', text: action.label || 'Open' },
        url: action.url,
        style: severity === 'critical' ? 'danger' : 'primary',
      }],
    });
  }
  return { attachments: [{ color, blocks }] };
}

async function sendSlack(webhookUrl, payload) {
  const res = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Slack ${res.status}: ${await res.text()}`);
  }
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function buildEmailHtml({ title, summary, body_markdown, action, severity, source }) {
  const color = SEVERITY_COLORS[severity] || SEVERITY_COLORS.info;
  const bodyHtml = body_markdown
    ? `<div style="margin: 16px 0; white-space: pre-wrap;">${escapeHtml(body_markdown)}</div>`
    : '';
  const actionHtml = action?.url
    ? `<p style="margin-top: 24px;"><a href="${escapeHtml(action.url)}" style="display:inline-block; background:${color}; color:#fff; padding:10px 18px; border-radius:6px; text-decoration:none; font-weight:600;">${escapeHtml(action.label || 'Review')}</a></p>`
    : '';
  return `<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; line-height:1.6; color:#333; max-width:640px; margin:0 auto; padding:20px; background:#f5f5f5;">
<div style="background:#fff; border-radius:8px; padding:28px; box-shadow:0 2px 4px rgba(0,0,0,0.08); border-top:4px solid ${color};">
<h2 style="margin:0 0 8px; color:${color};">${escapeHtml(title)}</h2>
<p style="margin:0; color:#475569;">${escapeHtml(summary)}</p>
${bodyHtml}
${actionHtml}
<div style="margin-top:28px; padding-top:16px; border-top:1px solid #E2E8F0; font-size:12px; color:#64748B;">Notification source: ${escapeHtml(source)}</div>
</div></body></html>`;
}

/**
 * Generate a single-use Clerk sign-in token for `userId`.
 * Returns the opaque token string, or throws on API error.
 *
 * Tokens are single-use and expire per CLERK_SIGN_IN_TOKEN_TTL_SEC (default 1h).
 * They're safe to embed in 1:1 email but NOT in multi-user channels — the Slack
 * path deliberately skips this. See https://clerk.com/docs/guides/development/custom-flows/embeddable-email-links
 */
async function createClerkSignInToken(userId) {
  if (!CLERK_SECRET_KEY) {
    throw new Error('CLERK_SECRET_KEY not set on notification_hub Lambda env');
  }
  const res = await fetch('https://api.clerk.com/v1/sign_in_tokens', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${CLERK_SECRET_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      user_id: userId,
      expires_in_seconds: CLERK_SIGN_IN_TOKEN_TTL_SEC,
    }),
  });
  if (!res.ok) {
    throw new Error(`Clerk sign_in_tokens ${res.status}: ${await res.text()}`);
  }
  const body = await res.json();
  if (!body.token) {
    throw new Error(`Clerk sign_in_tokens returned no token: ${JSON.stringify(body)}`);
  }
  return body.token;
}

/**
 * Append `?token=<TOKEN>` (or `&token=<TOKEN>`) to a URL, URI-encoding the token.
 * Pure — doesn't parse the URL, just appends correctly. Testable in isolation.
 */
export function appendTokenToUrl(url, token) {
  if (!url) return url;
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}token=${encodeURIComponent(token)}`;
}

async function sendEmail({ to, from, subject, html }) {
  await ses.send(new SendEmailCommand({
    Destination: { ToAddresses: to },
    FromEmailAddress: from,
    Content: {
      Simple: {
        Subject: { Data: subject, Charset: 'UTF-8' },
        Body: { Html: { Data: html, Charset: 'UTF-8' } },
      },
    },
    ConfigurationSetName: CONFIGURATION_SET,
  }));
}

function response(status, body) {
  return {
    statusCode: status,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  };
}

export const handler = async (event) => {
  const headers = event.headers || {};
  const key = headers['x-notify-key'] || headers['X-Notify-Key'];
  if (!NOTIFY_SHARED_SECRET || key !== NOTIFY_SHARED_SECRET) {
    return response(401, { error: 'Unauthorized' });
  }

  let payload;
  try {
    payload = JSON.parse(event.body || '{}');
  } catch {
    return response(400, { error: 'Invalid JSON' });
  }

  const { source, severity = 'info', title, summary, body_markdown, action } = payload;
  if (!source || !title || !summary) {
    return response(400, { error: 'Missing required fields: source, title, summary' });
  }

  let routes;
  try {
    routes = await getRoutes();
  } catch (e) {
    console.error('failed to load routes', e);
    return response(500, { error: 'Route config unavailable' });
  }

  const route = routes[source];
  if (!route) {
    console.warn(`no route configured for source: ${source}`);
    return response(200, { ok: true, dispatched: [] });
  }

  const channels = route.channels || [];
  const dispatched = [];

  if (channels.includes('slack') && route.slack) {
    try {
      const webhookUrl = process.env[route.slack.webhook_env];
      if (!webhookUrl) throw new Error(`env var ${route.slack.webhook_env} not set`);
      await sendSlack(webhookUrl, buildSlackPayload({ title, summary, severity, action, body_markdown }));
      dispatched.push('slack');
    } catch (e) {
      console.error(`slack dispatch failed for source ${source}:`, e.message);
    }
  }

  if (channels.includes('email') && route.email) {
    const emailSent = await dispatchEmail({ route, title, summary, body_markdown, action, severity, source });
    if (emailSent > 0) dispatched.push('email');
  }

  return response(200, { ok: true, dispatched });
};

/**
 * Send the email notification. Two modes:
 *
 *   1. `route.email.clerk_user_ids` is set (and same length as `route.email.to`):
 *      Send ONE email per recipient, each with a unique Clerk sign-in token
 *      appended to the action.url. This enables passwordless deep-link access —
 *      the recipient clicks "Review Changes" and lands signed-in without the
 *      manual Clerk sign-in gate.
 *
 *   2. No `clerk_user_ids`: send ONE email to all recipients with the action.url
 *      as-is (no token). Users sign in manually on arrival.
 *
 * Per-recipient failures are logged but don't fail the whole dispatch. Returns
 * the count of emails successfully sent.
 */
async function dispatchEmail({ route, title, summary, body_markdown, action, severity, source }) {
  const to = Array.isArray(route.email.to) ? route.email.to : [route.email.to];
  const from = route.email.from || DEFAULT_SENDER;
  const subject = `[${severity.toUpperCase()}] ${title}`;
  const clerkUserIds = Array.isArray(route.email.clerk_user_ids) ? route.email.clerk_user_ids : null;

  // Mode 1: passwordless per-recipient (requires pairing + action.url to embed into).
  const tokensEnabled =
    clerkUserIds &&
    clerkUserIds.length === to.length &&
    action?.url &&
    CLERK_SECRET_KEY;

  if (tokensEnabled) {
    let sent = 0;
    for (let i = 0; i < to.length; i++) {
      const recipient = to[i];
      const userId = clerkUserIds[i];
      try {
        const token = await createClerkSignInToken(userId);
        const tokenizedAction = { ...action, url: appendTokenToUrl(action.url, token) };
        const html = buildEmailHtml({ title, summary, body_markdown, action: tokenizedAction, severity, source });
        await sendEmail({ to: [recipient], from, subject, html });
        sent++;
      } catch (e) {
        console.error(`email dispatch failed for ${recipient} (source ${source}):`, e.message);
      }
    }
    return sent;
  }

  // Mode 2: fan-out to all recipients in one email (tokenless).
  try {
    const html = buildEmailHtml({ title, summary, body_markdown, action, severity, source });
    await sendEmail({ to, from, subject, html });
    return to.length;
  } catch (e) {
    console.error(`email dispatch failed for source ${source}:`, e.message);
    return 0;
  }
}
