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
 *   start_scheduling (M8a — the CB config.ts `CTAActionType`/`CTAType`
 *     marker: `action:'start_scheduling'`, `type:'scheduling_trigger'`,
 *     config.ts:193/200) → quick reply (`PIC1:cta:{id}`); tapping it resolves
 *     via `resolveCtaPayload` below into `schedulingDriver.beginScheduling`
 *     (index.js wiring), mirroring the start_form precedent above.
 *   resume_scheduling (M8b — scheduling: manage; config.ts:194) → quick
 *     reply (`PIC1:cta:{id}`), same shape as start_scheduling. Tapping it
 *     resolves via `resolveCtaPayload` below into an ambiguous manage-menu
 *     prompt (schedulingDriver.resolveManageTrigger's `ask_menu` — the CTA
 *     itself doesn't say cancel vs reschedule, so the bot asks) when a
 *     last_booking (C4) is found; index.js wiring, mirrors start_scheduling.
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
      case 'start_scheduling':
        // M8a: always a quick reply — scheduling has no link-out override
        // (unlike start_form, there is no `url` escape hatch on this CTA
        // shape; config.ts's CTADefinition has no `url` semantics for
        // start_scheduling). A tap resolves via resolveCtaPayload (below)
        // into schedulingDriver.beginScheduling (index.js wiring) when
        // scheduling is enabled for the tenant; otherwise it falls back to
        // RAG on the CTA label like any other unresolvable PIC1 payload.
        quickReplies.push({
          content_type: 'text',
          title: truncateTitle(cta.label || cta.text || id),
          payload: `PIC1:cta:${id}`,
        });
        break;
      case 'resume_scheduling':
        // M8b: same posture as start_scheduling — always a quick reply, no
        // link-out override. A tap resolves via resolveCtaPayload (below)
        // into the manage-menu entry (index.js wiring) when scheduling is
        // enabled; otherwise falls back to RAG on the CTA label.
        quickReplies.push({
          content_type: 'text',
          title: truncateTitle(cta.label || cta.text || id),
          payload: `PIC1:cta:${id}`,
        });
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
 * M8a addition: a `start_scheduling` CTA returns `startScheduling: true` in
 * addition to `turnText` — the caller (index.js) resolves the appointment
 * type (schedulingDriver.resolveAppointmentTypeId, using the CTA's own
 * `program_id` field) and begins the scheduling driver instead of a RAG turn
 * when MESSENGER_CHANNEL + scheduling_enabled + the state table + the BCH
 * invoke function are all available; otherwise this falls back to the
 * pre-M8a behavior (RAG on the CTA label), same posture as an unresolvable
 * start_form.
 *
 * M8b addition: a `resume_scheduling` CTA returns `resumeScheduling: true` —
 * the caller (index.js) checks for a last_booking (C4) and, when found,
 * shows the manage menu (schedulingDriver's `ask_menu` trigger) instead of a
 * RAG turn; otherwise falls back to the pre-M8b behavior (RAG on the CTA
 * label), same posture as an unresolvable start_form/start_scheduling.
 *
 * @returns {{ turnText: string, ctaId: string, startFormId?: string, startScheduling?: boolean, resumeScheduling?: boolean } | null}
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
    case 'start_scheduling':
      return { turnText: cta.label || ctaId, ctaId, startScheduling: true };
    case 'resume_scheduling':
      return { turnText: cta.label || ctaId, ctaId, resumeScheduling: true };
    default:
      // Commitment CTAs render as URL buttons (no postback round-trip); a
      // PIC1:cta payload for one is unexpected — answer about it via RAG.
      return { turnText: cta.label || ctaId, ctaId };
  }
}

module.exports = { renderMessengerActions, resolveCtaPayload, truncateTitle };
