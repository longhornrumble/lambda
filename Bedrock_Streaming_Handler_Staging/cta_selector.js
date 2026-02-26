/**
 * Dynamic CTA Selection Module
 *
 * Selects 2-3 contextually relevant CTA buttons after each AI response
 * using code-based topic extraction and a 3-slot filling algorithm.
 *
 * Slots:
 *   - action: commit-type CTA (apply, donate, enroll)
 *   - info:   go-deeper CTA (learn more about current topic)
 *   - lateral: escape-route CTA (explore a different topic)
 */

// --- Intent Detection ---

const INTENT_PHRASES = [
  'i want to apply', 'i would like to apply', 'i want to volunteer',
  'i want to donate', 'how do i sign up', 'how do i apply',
  'i\'d like to', 'i am interested in applying', 'i want to sign up',
  'i want to enroll', 'how do i enroll', 'i want to register',
  'i\'d like to volunteer', 'i\'d like to donate', 'i want to mentor',
  'how can i apply', 'where do i sign up', 'i\'m ready to',
  'sign me up', 'i want to help', 'how do i get involved',
  'i want to sponsor', 'i\'d like to sponsor'
];

/**
 * Detects whether the user's message expresses commitment intent.
 * @param {string} userMessage
 * @returns {"exploring" | "committed"}
 */
function detectUserIntent(userMessage) {
  if (!userMessage) return 'exploring';
  const lower = userMessage.toLowerCase();
  return INTENT_PHRASES.some(phrase => lower.includes(phrase))
    ? 'committed'
    : 'exploring';
}

// --- Topic Extraction ---

/**
 * Extracts canonical topic IDs from the AI response by matching
 * against the tenant's topic vocabulary.
 *
 * @param {string} responseText - The AI's response
 * @param {Object} topicVocabulary - Map of topic_id → keyword variants[]
 * @returns {string[]} Array of matched canonical topic IDs
 */
function extractTopics(responseText, topicVocabulary) {
  if (!responseText || !topicVocabulary) return [];
  const lower = responseText.toLowerCase();
  const matched = [];

  for (const [topicId, variants] of Object.entries(topicVocabulary)) {
    if (!Array.isArray(variants)) continue;
    for (const variant of variants) {
      if (lower.includes(variant.toLowerCase())) {
        matched.push(topicId);
        break; // one match is enough for this topic
      }
    }
  }

  return matched;
}

// --- CTA Scoring ---

/**
 * Scores all eligible CTAs based on topic overlap, user intent,
 * and recency exclusion.
 *
 * @param {Object} ctaDefinitions - All CTA definitions from config
 * @param {string[]} matchedTopics - Topics found in the AI response
 * @param {"exploring"|"committed"} userIntent
 * @param {string[]} recentlyShownIds - CTA IDs shown in last N turns
 * @param {string[]} completedForms - Form IDs already completed
 * @returns {Array<{id: string, score: number, cta: Object}>}
 */
function scoreCTAs(ctaDefinitions, matchedTopics, userIntent, recentlyShownIds = [], completedForms = []) {
  if (!ctaDefinitions) return [];

  const recentSet = new Set(recentlyShownIds);
  const completedSet = new Set(completedForms);
  const topicSet = new Set(matchedTopics);
  const scored = [];

  for (const [id, cta] of Object.entries(ctaDefinitions)) {
    // Gate: must be AI-available
    if (!cta.ai_available) continue;

    // Gate: must have selection_metadata
    const meta = cta.selection_metadata;
    if (!meta) continue;

    // Exclusion: recently shown (hard exclude)
    if (recentSet.has(id)) continue;

    // Exclusion: completed form
    if (cta.action === 'start_form' && cta.formId && completedSet.has(cta.formId)) continue;

    // Score: topic overlap
    // Uses both absolute overlap count and normalized ratio to prevent
    // single-tag CTAs from outscoring multi-tag CTAs with more matches.
    // Example: 2 of 3 tags matching (0.67 ratio, 2 absolute) should beat
    //          1 of 1 tag matching (1.0 ratio, 1 absolute)
    const ctaTopics = meta.topic_tags || [];
    let topicOverlap = 0;
    for (const tag of ctaTopics) {
      if (topicSet.has(tag)) topicOverlap++;
    }
    const normalizedScore = ctaTopics.length > 0
      ? topicOverlap / ctaTopics.length
      : 0;
    // Blend: 60% absolute overlap (rewards breadth), 40% normalized (rewards specificity)
    const topicScore = (topicOverlap * 0.6) + (normalizedScore * 0.4);

    // Boost: committed intent + action depth
    let intentBoost = 0;
    if (userIntent === 'committed' && meta.depth_level === 'action') {
      intentBoost = 0.3;
    }

    const totalScore = topicScore + intentBoost;

    scored.push({ id, score: totalScore, cta });
  }

  // Sort descending by score
  scored.sort((a, b) => b.score - a.score);

  return scored;
}

// --- Slot Filling ---

/**
 * Fills 3 slots (action, info, lateral) from scored CTAs.
 * Guarantees spread by picking one CTA per slot type.
 *
 * @param {Array<{id: string, score: number, cta: Object}>} scoredCTAs
 * @returns {{action: Object|null, info: Object|null, lateral: Object|null}}
 */
function fillSlots(scoredCTAs) {
  const slots = { action: null, info: null, lateral: null };
  const usedIds = new Set();

  // Fill action slot: highest-scoring CTA eligible for "action"
  for (const item of scoredCTAs) {
    if (usedIds.has(item.id)) continue;
    const meta = item.cta.selection_metadata;
    const eligibility = meta.slot_eligibility || [];
    if (eligibility.includes('action')) {
      slots.action = formatSlotCTA(item);
      usedIds.add(item.id);
      break;
    }
  }

  // Fill info slot: highest-scoring CTA eligible for "info"
  for (const item of scoredCTAs) {
    if (usedIds.has(item.id)) continue;
    const meta = item.cta.selection_metadata;
    const eligibility = meta.slot_eligibility || [];
    if (eligibility.includes('info')) {
      slots.info = formatSlotCTA(item);
      usedIds.add(item.id);
      break;
    }
  }

  // Fill lateral slot: prefer LOWEST-scoring lateral-eligible CTA.
  // Lateral = escape route, so we want CTAs with LESS topic overlap (different topic).
  // Candidates are sorted ascending by score — pick the first eligible one.
  const lateralCandidates = scoredCTAs
    .filter(item => {
      if (usedIds.has(item.id)) return false;
      const meta = item.cta.selection_metadata;
      const eligibility = meta.slot_eligibility || [];
      return eligibility.includes('lateral') || meta.lateral_eligible;
    })
    .sort((a, b) => a.score - b.score); // ascending — lowest topic overlap first

  if (lateralCandidates.length > 0) {
    slots.lateral = formatSlotCTA(lateralCandidates[0]);
    usedIds.add(lateralCandidates[0].id);
  }

  return slots;
}

/**
 * Formats a scored CTA item into the CTA button object expected by the frontend.
 */
function formatSlotCTA(item) {
  const cta = item.cta;
  return {
    id: item.id,
    label: cta.label || cta.text || item.id,
    action: cta.action,
    type: cta.action, // frontend expects 'type' field
    ...(cta.formId && { formId: cta.formId }),
    ...(cta.url && { url: cta.url }),
    ...(cta.target_branch && { target_branch: cta.target_branch }),
    ...(cta.query && { query: cta.query }),
    ...(cta.value && { value: cta.value }),
    _position: item.cta.selection_metadata?.depth_level === 'action' ? 'primary' : 'secondary',
    _slot: item.cta.selection_metadata?.depth_level,
    _score: item.score
  };
}

// --- Main Entry Point ---

/**
 * Selects 2-3 contextually relevant CTAs for the current turn.
 *
 * @param {string} responseText - The AI's buffered response
 * @param {string} userMessage - The user's input message
 * @param {Object} config - Tenant configuration
 * @param {Object} sessionState - Session state
 * @param {string[]} sessionState.recentlyShownCTAIds - CTA IDs shown in last 2 turns
 * @param {string[]} sessionState.completedForms - Completed form IDs
 * @param {number} sessionState.turnsSinceClick - Turns since user last clicked a CTA
 * @returns {Object} { ctaButtons: CTA[], selectionLog: Object }
 */
function selectCTAs(responseText, userMessage, config, sessionState = {}) {
  const ctaDefinitions = config.cta_definitions || {};
  const topicVocabulary = config.topic_vocabulary || {};
  const {
    recentlyShownCTAIds = [],
    completedForms = [],
    turnsSinceClick = 0
  } = sessionState;

  // Recency reset: after 5+ turns without a CTA click, clear recency exclusion
  const effectiveRecency = turnsSinceClick >= 5 ? [] : recentlyShownCTAIds;

  // Step 1: Extract topics from AI response
  const matchedTopics = extractTopics(responseText, topicVocabulary);

  // Step 2: Detect user intent
  const userIntent = detectUserIntent(userMessage);

  // Step 3: Score all eligible CTAs
  const scoredCTAs = scoreCTAs(
    ctaDefinitions,
    matchedTopics,
    userIntent,
    effectiveRecency,
    completedForms
  );

  // Step 4: Fill slots
  const slots = fillSlots(scoredCTAs);

  // Collect non-null slots into array (preserve order: action, info, lateral)
  const ctaButtons = [];
  if (slots.action) ctaButtons.push(slots.action);
  if (slots.info) ctaButtons.push(slots.info);
  if (slots.lateral) ctaButtons.push(slots.lateral);

  // Build selection log for observability
  const selectionLog = {
    event: 'cta_selection',
    matched_topics: matchedTopics,
    user_intent: userIntent,
    recency_reset: turnsSinceClick >= 5,
    scores_top5: scoredCTAs.slice(0, 5).map(c => ({ id: c.id, score: c.score })),
    selected: ctaButtons.map(c => c.id),
    slots: {
      action: slots.action?.id || null,
      info: slots.info?.id || null,
      lateral: slots.lateral?.id || null
    }
  };

  return { ctaButtons, selectionLog };
}

module.exports = {
  selectCTAs,
  extractTopics,
  detectUserIntent,
  scoreCTAs,
  fillSlots,
  // Exported for testing
  INTENT_PHRASES
};
