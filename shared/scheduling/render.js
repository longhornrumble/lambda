'use strict';

/**
 * render.js — the single {{var}} substitution for scheduling notice / confirmation /
 * reminder copy.
 *
 * Unknown vars render '' (the §E14 editor contract — a var used in the wrong moment
 * renders empty, never a literal {{...}} in a recipient's inbox). Values are inserted
 * verbatim; the CALLER pre-escapes html-bound vars (escapeHtml) before rendering into an
 * html body — this function does no escaping, matching the three byte-identical copies it
 * replaces (notify.js `render`, confirmation-email.js `renderVars`,
 * Scheduled_Message_Sender/index.mjs `render`).
 *
 * NB: Scheduled_Message_Sender ALSO has `renderTemplate` (per-key, leaves unknown literal)
 * for BAKED row bodies — that is a deliberately different behaviour and is NOT this function.
 */
function render(template, vars) {
  return template.replace(/\{\{(\w+)\}\}/g, (_, key) =>
    vars[key] != null ? String(vars[key]) : ''
  );
}

function escapeHtmlEntities(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[c]));
}

/**
 * linkHtml(url) — render a link token as a clickable anchor for the HTML body.
 *
 * A bare URL is NOT auto-linked by HTML email clients, so the link tokens
 * ({{joinUrl}}/{{rescheduleUrl}}/{{cancelUrl}}) are passed to the html render as an
 * <a> element while the text/SMS render gets the raw URL. https-only (matches the
 * senders' safeUrl scheme guard) — a non-https / empty url renders '' so a hostile or
 * missing link can never become an executable href, and an absent link leaves no dangling
 * anchor. The href + visible text are both entity-escaped.
 */
function linkHtml(url) {
  const u = typeof url === 'string' ? url.trim() : '';
  if (!/^https:\/\//i.test(u)) return '';
  const e = escapeHtmlEntities(u);
  return `<a href="${e}">${e}</a>`;
}

module.exports = { render, linkHtml };
