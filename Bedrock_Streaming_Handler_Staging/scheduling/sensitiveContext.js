'use strict';

/**
 * sensitiveContext.js — §B17f sensitive-context suppression pre-check (WS-AG-CORE).
 *
 * Canonical: FROZEN_CONTRACTS.md §B17f (governance + PII advisory pass 2026-06-12);
 * design doc AGENTIC_SCHEDULING_SLICE_DESIGN.md §3.4.
 *
 * Runs on EVERY agent turn (increments 1 AND 2), BEFORE the model call:
 *   - scan window = the FULL session transcript (user-side), not just this turn —
 *     stickiness for the session derives from full-window scanning (the triggering
 *     text stays in the transcript), plus an explicit `priorLatched` short-circuit
 *     the caller may thread from a persisted latch field when one exists.
 *   - FAILS CLOSED: any scan error → treated as tripped (category 'scan_error').
 *   - ships with the NON-EMPTY DEFAULT category list below. Tenants may TRIM
 *     (config `scheduling.sensitive_context_trim`: array of category CODES to
 *     disable) but may NOT start empty — a trim that would empty the list is
 *     IGNORED and the full default list applies (fail-closed).
 *   - keyword lists are language-bound English (known gap; multi-language is v2).
 *
 * Category CODES (the only thing that may appear in audit events — §B17g:
 * `suppression_category` is a category code, NEVER raw matched text):
 *   self_harm_suicide · abuse_neglect_cps · domestic_violence · trafficking ·
 *   runaway_homeless · medical_emergency_overdose · psychiatric_crisis ·
 *   custody_legal · minor_self_identification · grief_death  (+ 'scan_error'
 *   for the fail-closed path).
 *
 * minor_self_identification ADDITIONALLY stops agent email solicitation (§B17f) —
 * under increment 1 the trip suppresses the whole agent turn (no model call), so
 * no solicitation can occur; `stopsEmailSolicitation(category)` is exported for
 * the increment-2 wiring.
 *
 * PII rule: this module never logs message text, matched text, or match offsets —
 * its only outputs are booleans + category codes.
 */

// ─── default category list (§B17f — trim-only, never empty) ─────────────────────────

// Patterns are deliberately conservative-English v1. Broad-but-bounded: a false
// trip degrades to warm human-contact copy (safe); a miss degrades to the model's
// own §B17e behavior. Order = first match wins.
const DEFAULT_CATEGORIES = Object.freeze([
  {
    code: 'self_harm_suicide',
    patterns: [
      /suicid/i,
      /self[\s-]?harm/i,
      /kill (myself|himself|herself|themselves)/i,
      /hurt (myself|themselves)/i,
      /end (my|his|her|their) (own )?life/i,
      /don'?t want to (live|be alive|go on)/i,
      /no reason to live/i,
    ],
  },
  {
    code: 'abuse_neglect_cps',
    patterns: [
      /\babus(e|ed|ing|ive)\b/i,
      /\bneglect(ed|ing)?\b/i,
      /\bCPS\b/,
      /child protective/i,
      /molest/i,
    ],
  },
  {
    code: 'domestic_violence',
    patterns: [
      /domestic violence/i,
      /restraining order/i,
      /(hits|beats|hurts|hit|beat) me\b/i,
      /afraid of my (husband|wife|partner|boyfriend|girlfriend|ex)\b/i,
      /(violent|abusive) (husband|wife|partner|boyfriend|girlfriend|ex|relationship|home)/i,
    ],
  },
  {
    code: 'trafficking',
    patterns: [/traffick/i, /being (sold|forced to work)/i],
  },
  {
    code: 'runaway_homeless',
    patterns: [
      /\brunaway\b/i,
      /\bran away from home\b/i,
      /running away from home/i,
      /\bhomeless\b/i,
      /nowhere to (go|stay|sleep|live)/i,
      /kicked (me )?out of (the house|home)/i,
      /living (in my car|on the street)/i,
    ],
  },
  {
    code: 'medical_emergency_overdose',
    patterns: [
      /overdos/i,
      /\b911\b/,
      /emergency room\b/i,
      /not breathing/i,
      /\bunconscious\b/i,
      /\bpoison(ed|ing)?\b/i,
    ],
  },
  {
    code: 'psychiatric_crisis',
    patterns: [
      /psychiatric/i,
      /psych (ward|hold|hospital)/i,
      /(mental|nervous) breakdown/i,
      /hearing voices/i,
      /crisis (line|hotline)/i,
      /panic attacks?/i,
    ],
  },
  {
    code: 'custody_legal',
    patterns: [
      /\bcustody\b/i,
      /court (date|case|hearing|order)/i,
      /legal proceeding/i,
      /\blawsuit\b/i,
      /\b(my|a|need a|talk to a) (lawyer|attorney)\b/i,
      /\bprobation officer\b/i,
    ],
  },
  {
    code: 'minor_self_identification',
    patterns: [
      /\bI('?| a)?m (only )?(1[0-7]|[1-9]) ?(years? old|y\/?o)\b/i,
      /\bI am (only )?(1[0-7]|[1-9]) ?(years? old|y\/?o)\b/i,
      /\bI'?m a minor\b/i,
      /\bI'?m under (18|eighteen)\b/i,
      /\bI'?m (in|still in) (middle school|high school|8th grade|9th grade|10th grade|11th grade|12th grade)\b/i,
    ],
  },
  {
    code: 'grief_death',
    patterns: [
      /passed away/i,
      /\bfuneral\b/i,
      /\bgriev(e|ing)\b/i,
      /\bgrief\b/i,
      /\bmiscarriage\b/i,
      /\bin hospice\b/i,
      /\b(mom|dad|mother|father|husband|wife|son|daughter|brother|sister|grandma|grandmother|grandpa|grandfather|best friend|child) (just )?(died|passed)\b/i,
      /just lost my (mom|dad|mother|father|husband|wife|son|daughter|brother|sister|grandma|grandmother|grandpa|grandfather|best friend|child|job and my home)\b/i,
    ],
  },
]);

// The fail-closed pseudo-category (scan error → treated as tripped).
const SCAN_ERROR_CATEGORY = 'scan_error';

// ─── tenant trim (trim-only; never empty) ────────────────────────────────────────────

/**
 * Resolve the effective category list for a tenant. Tenants may TRIM via
 * `tenantConfig.scheduling.sensitive_context_trim` (array of category codes to
 * disable). A trim that would empty the list is ignored — the full default list
 * applies (§B17f: "may NOT start empty"; fail-closed). Schema-discipline: any
 * missing/malformed config → full default list.
 *
 * @param {object|null|undefined} tenantConfig
 * @returns {Array<{code:string, patterns:RegExp[]}>}
 */
function resolveCategories(tenantConfig) {
  const trim = tenantConfig?.scheduling?.sensitive_context_trim;
  if (!Array.isArray(trim) || trim.length === 0) return DEFAULT_CATEGORIES;
  const trimmed = DEFAULT_CATEGORIES.filter((c) => !trim.includes(c.code));
  return trimmed.length > 0 ? trimmed : DEFAULT_CATEGORIES;
}

// ─── transcript assembly ─────────────────────────────────────────────────────────────

/**
 * Extract the USER-SIDE transcript strings from a conversation history (the §B17f
 * scan window is the full session). Tolerates the codebase's mixed message shapes
 * ({role, content} or {role, text}). Assistant turns are NOT scanned — the model
 * must not be able to trip (or un-trip) its own suppression.
 *
 * @param {Array} conversationHistory
 * @param {string} [currentUserText] - this turn's typed message
 * @returns {string[]}
 */
function userSideTranscript(conversationHistory, currentUserText) {
  const out = [];
  for (const m of Array.isArray(conversationHistory) ? conversationHistory : []) {
    if (m && m.role === 'user') {
      const text = typeof m.content === 'string' ? m.content : typeof m.text === 'string' ? m.text : '';
      if (text) out.push(text);
    }
  }
  if (typeof currentUserText === 'string' && currentUserText) out.push(currentUserText);
  return out;
}

// ─── the pre-check ───────────────────────────────────────────────────────────────────

/**
 * §B17f suppression pre-check. Scan the full user-side session transcript for
 * sensitive-context categories. FAILS CLOSED: any error → { tripped: true,
 * category: 'scan_error' }.
 *
 * @param {object} params
 * @param {Array}  [params.conversationHistory] - full session messages
 * @param {string} [params.userText]            - this turn's typed message
 * @param {object} [params.tenantConfig]        - for the trim-only category config
 * @param {boolean}[params.priorLatched]        - a persisted session latch (sticky)
 * @param {string} [params.priorCategory]       - the persisted latch's category code
 * @returns {{tripped: boolean, category?: string}}
 */
function checkSensitiveContext({ conversationHistory, userText, tenantConfig, priorLatched, priorCategory } = {}) {
  try {
    // Sticky latch: once tripped for the session, stays tripped (§B17f).
    if (priorLatched === true) {
      return { tripped: true, category: priorCategory || SCAN_ERROR_CATEGORY };
    }
    const categories = resolveCategories(tenantConfig);
    const transcript = userSideTranscript(conversationHistory, userText);
    for (const message of transcript) {
      for (const category of categories) {
        for (const pattern of category.patterns) {
          if (pattern.test(message)) {
            return { tripped: true, category: category.code };
          }
        }
      }
    }
    return { tripped: false };
  } catch (err) {
    // FAIL CLOSED (§B17f): a scan error is treated as tripped. err.name only — never text.
    console.error(`[WS-AG-CORE] sensitive-context scan failed (fail-closed → tripped): error_name=${(err && err.name) || 'unknown'}`);
    return { tripped: true, category: SCAN_ERROR_CATEGORY };
  }
}

/**
 * §B17f: minor self-identification additionally stops agent email solicitation
 * (tenant opt-in to change — increment-2 wiring; under increment 1 the whole agent
 * turn is suppressed, so no solicitation can occur).
 * @param {string} category
 * @returns {boolean}
 */
function stopsEmailSolicitation(category) {
  return category === 'minor_self_identification';
}

module.exports = {
  DEFAULT_CATEGORIES,
  SCAN_ERROR_CATEGORY,
  resolveCategories,
  userSideTranscript,
  checkSensitiveContext,
  stopsEmailSolicitation,
};
