'use strict';

/**
 * CTA rendering map — contract C9 (M4). Turns the turn's validated actionIds
 * into Messenger/IG send structures. The rendering DECISION is frozen in C9 —
 * this module implements it, it does not re-derive it:
 *
 *   send_query / show_info (suggestion class) → quick replies (transient)
 *   external_link (commitment)                → button-template web_url
 *                                                buttons (persistent)
 *   start_form (M7a replacement of the pre-M7 interim named in C9):
 *     - carries a `url` → still a button (tenant explicitly wants a link-out
 *       instead of the in-Messenger form engine — override preserved)
 *     - no `url` → quick reply (`PIC1:cta:{id}`); tapping it resolves via
 *       `resolveCtaPayload` below into `formEngine.beginForm` when the CTA's
 *       formId matches a `conversational_forms` entry (index.js wiring)
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
        if (cta.url) {
          buttons.push({
            type: 'web_url',
            url: cta.url,
            title: truncateTitle(cta.label || cta.text || id),
          });
        } else {
          log('INFO', 'CTA skipped — external_link missing url', { ctaId: id, action: cta.action });
        }
        break;
      case 'start_form':
        if (cta.url) {
          // Explicit link-out override — tenant wants an external page
          // instead of the in-Messenger form engine.
          buttons.push({
            type: 'web_url',
            url: cta.url,
            title: truncateTitle(cta.label || cta.text || id),
          });
        } else {
          // M7a: replaces the pre-M7 logged-skip interim (C9) — start_form
          // without a url now renders as a quick reply. A tap resolves via
          // resolveCtaPayload (below) into formEngine.beginForm when the
          // CTA's formId matches a configured conversational_forms entry.
          quickReplies.push({
            content_type: 'text',
            title: truncateTitle(cta.label || cta.text || id),
            payload: `PIC1:cta:${id}`,
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
 * M7a addition: a `start_form` CTA whose formId resolves in the tenant's
 * `conversational_forms` returns `startFormId` in addition to `turnText` —
 * the caller (index.js) begins the form engine instead of a RAG turn when
 * MESSENGER_CHANNEL + the state table are both available; an unresolvable
 * formId (mistyped, or the form was removed from config) falls back to the
 * pre-M7 behavior below (RAG on the CTA label) exactly like any other
 * unmapped case.
 *
 * @returns {{ turnText: string, ctaId: string, startFormId?: string } | null}
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
    case 'start_form': {
      const formId = cta.formId || cta.form_id;
      if (formId && config?.conversational_forms?.[formId]) {
        return { turnText: cta.label || ctaId, ctaId, startFormId: formId };
      }
      return { turnText: cta.label || ctaId, ctaId };
    }
    default:
      // Commitment CTAs render as URL buttons (no postback round-trip); a
      // PIC1:cta payload for one is unexpected — answer about it via RAG.
      return { turnText: cta.label || ctaId, ctaId };
  }
}

module.exports = { renderMessengerActions, resolveCtaPayload, truncateTitle };
