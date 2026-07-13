'use strict';

/**
 * CTA rendering map — contract C9 (M4). Turns the turn's validated actionIds
 * into Messenger/IG send structures. The rendering DECISION is frozen in C9 —
 * this module implements it, it does not re-derive it:
 *
 *   send_query / show_info (suggestion class) → quick replies (transient)
 *   external_link / start_form (commitment)   → button-template web_url
 *                                                buttons (persistent)
 *
 * start_form interim (pre-M7): rendered as a URL button only when the CTA
 * carries a `url` (link-out per C9); without one there is nothing to link —
 * skipped with a log (never silently).
 *
 * Caps from C5 (capabilities.js): QR ≤13, titles truncated to 20 chars,
 * buttons ≤3 (overflow logged, V5 order wins).
 */

const { QUICK_REPLY_MAX, QUICK_REPLY_TITLE_MAX, BUTTON_TEMPLATE_MAX } = require('./capabilities');

function truncateTitle(label) {
  const t = String(label || '').trim();
  return t.length <= QUICK_REPLY_TITLE_MAX ? t : t.slice(0, QUICK_REPLY_TITLE_MAX - 1) + '…';
}

/**
 * @param {string[]} actionIds — validated ids (validateActionIds output)
 * @param {object} config — tenant config (cta_definitions)
 * @param {(level: string, msg: string, meta?: object) => void} log
 * @returns {{ quickReplies: Array<{content_type: 'text', title: string, payload: string}>,
 *             buttons: Array<{type: 'web_url', url: string, title: string}> }}
 */
function renderMessengerActions(actionIds, config, log) {
  const quickReplies = [];
  const buttons = [];

  for (const id of actionIds || []) {
    const cta = config?.cta_definitions?.[id];
    if (!cta) continue; // validateActionIds already filters; belt-and-suspenders

    switch (cta.action) {
      case 'send_query':
      case 'show_info':
        quickReplies.push({
          content_type: 'text',
          title: truncateTitle(cta.label || cta.text || id),
          payload: `PIC1:cta:${id}`,
        });
        break;
      case 'external_link':
      case 'start_form':
        if (cta.url) {
          buttons.push({
            type: 'web_url',
            url: cta.url,
            title: truncateTitle(cta.label || cta.text || id),
          });
        } else {
          // start_form without a link-out URL: nothing to render until M7.
          log('INFO', 'CTA skipped — no url for commitment rendering (pre-M7 interim)', {
            ctaId: id,
            action: cta.action,
          });
        }
        break;
      default:
        log('INFO', 'CTA skipped — unmapped action type for Messenger rendering', {
          ctaId: id,
          action: cta.action,
        });
    }
  }

  if (quickReplies.length > QUICK_REPLY_MAX) {
    log('WARN', 'Quick replies over C5 cap — truncating (V5 order wins)', {
      requested: quickReplies.length,
      cap: QUICK_REPLY_MAX,
    });
    quickReplies.length = QUICK_REPLY_MAX;
  }
  if (buttons.length > BUTTON_TEMPLATE_MAX) {
    log('WARN', 'Buttons over C5 cap — truncating (V5 order wins)', {
      requested: buttons.length,
      cap: BUTTON_TEMPLATE_MAX,
    });
    buttons.length = BUTTON_TEMPLATE_MAX;
  }

  return { quickReplies, buttons };
}

/**
 * Resolve a PIC1:cta:{id} payload to the free-text turn it stands for
 * (contract C3 routing). Returns null when the payload is not a resolvable
 * cta route — the caller then treats the payload as free text into RAG
 * (C3's unknown-payload rule, which also covers stale buttons forever).
 *
 * @returns {{ turnText: string, ctaId: string } | null}
 */
function resolveCtaPayload(payload, config) {
  if (typeof payload !== 'string' || !payload.startsWith('PIC1:cta:')) return null;
  const ctaId = payload.slice('PIC1:cta:'.length);
  const cta = config?.cta_definitions?.[ctaId];
  if (!cta) return null;

  switch (cta.action) {
    case 'send_query':
      return { turnText: cta.query || cta.label || ctaId, ctaId };
    case 'show_info':
      return { turnText: cta.prompt || cta.label || ctaId, ctaId };
    default:
      // Commitment CTAs render as URL buttons (no postback round-trip); a
      // PIC1:cta payload for one is unexpected — answer about it via RAG.
      return { turnText: cta.label || ctaId, ctaId };
  }
}

module.exports = { renderMessengerActions, resolveCtaPayload, truncateTitle };
