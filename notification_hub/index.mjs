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
    try {
      const html = buildEmailHtml({ title, summary, body_markdown, action, severity, source });
      await sendEmail({
        to: Array.isArray(route.email.to) ? route.email.to : [route.email.to],
        from: route.email.from || DEFAULT_SENDER,
        subject: `[${severity.toUpperCase()}] ${title}`,
        html,
      });
      dispatched.push('email');
    } catch (e) {
      console.error(`email dispatch failed for source ${source}:`, e.message);
    }
  }

  return response(200, { ok: true, dispatched });
};
