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

module.exports = { render };
