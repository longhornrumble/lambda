/**
 * Dynamic CTA Selection Module
 *
 * Selects 2-4 contextually relevant CTA buttons after each AI response
 * using code-based topic extraction, role detection, and a 4-slot
 * filling algorithm with topic cluster allocation.
 *
 * Slots:
 *   - action:  commit-type CTA matching user's role (apply, donate, enroll)
 *   - info1:   go-deeper CTA (learn more about program A)
 *   - info2:   go-deeper CTA (learn more about program B, or second angle on A)
 *   - lateral: escape-route CTA (explore a different topic)
 */

// --- Role Detection ---

const ROLE_SIGNALS = {
  give: [
    'i want to volunteer', 'i want to donate', 'i want to mentor',
    'i want to help', 'i want to give', 'how can i volunteer',
    'how can i help', 'how can i donate', 'how do i volunteer',
    'how do i donate', 'i\'d like to volunteer', 'i\'d like to donate',
    'i\'d like to mentor', 'i\'d like to help', 'i want to sponsor',
    'i\'d like to sponsor', 'i want to get involved',
    'how do i get involved', 'give back', 'i want to contribute',
    'sign me up to volunteer', 'sign me up to mentor',
    'where can i volunteer', 'where can i donate'
  ],
  receive: [
    'my family needs', 'i need help', 'we need help', 'my child needs',
    'i need support', 'we need support', 'looking for support',
    'looking for help', 'need assistance', 'can you help my family',
    'i\'m a foster', 'we\'re a foster', 'i\'m fostering',
    'how do i enroll', 'how do i sign up my', 'enroll my',
    'register my', 'apply for services', 'apply for help',
    'request a mentor', 'get a mentor for', 'need a mentor',
    'my kid', 'my kids', 'our family', 'struggling',
    'how can i receive', 'how do i get help', 'services for families',
    'support for families', 'i need a love box', 'need care packages'
  ]
};

/**
 * Detects the user's role relative to the organization.
 * Returns { role, confidence } where confidence gates filtering behavior.
 *
 * @param {string} userMessage
 * @returns {{ role: "give"|"receive"|"neutral", confidence: "high"|"low" }}
 */
function detectUserRole(userMessage) {
  if (!userMessage) return { role: 'neutral', confidence: 'low' };
  const lower = userMessage.toLowerCase();

  let giveScore = 0;
  let receiveScore = 0;

  for (const phrase of ROLE_SIGNALS.give) {
    if (lower.includes(phrase)) giveScore++;
  }
  for (const phrase of ROLE_SIGNALS.receive) {
    if (lower.includes(phrase)) receiveScore++;
  }

  if (giveScore > 0 && receiveScore === 0) {
    return { role: 'give', confidence: 'high' };
  }
  if (receiveScore > 0 && giveScore === 0) {
    return { role: 'receive', confidence: 'high' };
  }
  if (giveScore > 0 && receiveScore > 0) {
    // Ambiguous — both signals present. Pick stronger, mark low confidence.
    return {
      role: giveScore > receiveScore ? 'give' : 'receive',
      confidence: 'low'
    };
  }

  return { role: 'neutral', confidence: 'low' };
}

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
 * @param {Object} topicVocabulary - Map of topic_id -> keyword variants[]
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
        break;
      }
    }
  }

  return matched;
}

// --- Topic Clustering ---

/**
 * Groups matched topics into program clusters using config-defined clusters.
 * The topic_vocabulary can define a `clusters` map: { cluster_name: [topic_ids] }.
 * If no cluster config exists, falls back to treating each program-level topic
 * (topics that appear as primary tags on action CTAs) as its own cluster.
 *
 * @param {string[]} matchedTopics - Topics found in the AI response
 * @param {Object} config - Full tenant config (uses topic_vocabulary.clusters or cta_definitions)
 * @returns {string[][]} Array of topic clusters (each cluster is a string[])
 */
function buildTopicClusters(matchedTopics, config) {
  if (matchedTopics.length <= 1) return [matchedTopics];

  const topicSet = new Set(matchedTopics);

  // Use config-defined clusters if available
  const clusterDefs = config.topic_clusters;
  if (clusterDefs && typeof clusterDefs === 'object') {
    const clusters = [];
    const assigned = new Set();

    for (const [, topicIds] of Object.entries(clusterDefs)) {
      if (!Array.isArray(topicIds)) continue;
      const clusterTopics = topicIds.filter(t => topicSet.has(t) && !assigned.has(t));
      if (clusterTopics.length > 0) {
        clusters.push(clusterTopics);
        clusterTopics.forEach(t => assigned.add(t));
      }
    }

    // Any unassigned matched topics form their own cluster
    const unassigned = matchedTopics.filter(t => !assigned.has(t));
    if (unassigned.length > 0) {
      clusters.push(unassigned);
    }

    return clusters;
  }

  // Fallback: each distinct program-level topic is its own cluster.
  // Program-level topics are those that appear as the first tag on action CTAs.
  const programTopics = new Set();
  const ctaDefs = config.cta_definitions || {};
  for (const cta of Object.values(ctaDefs)) {
    if (!cta.ai_available || !cta.selection_metadata) continue;
    const tags = cta.selection_metadata.topic_tags || [];
    const eligibility = cta.selection_metadata.slot_eligibility || [];
    if (eligibility.includes('action') && tags.length > 0) {
      programTopics.add(tags[0]);
    }
  }

  // Group matched topics: program topics each get their own cluster,
  // non-program topics (generic ones like "volunteer", "foster") are shared
  const clusters = [];
  const shared = [];

  for (const topic of matchedTopics) {
    if (programTopics.has(topic)) {
      clusters.push([topic]);
    } else {
      shared.push(topic);
    }
  }

  // Attach shared topics to all clusters (they're cross-cutting)
  if (clusters.length > 0 && shared.length > 0) {
    for (const cluster of clusters) {
      cluster.push(...shared);
    }
  } else if (clusters.length === 0) {
    clusters.push(matchedTopics);
  }

  return clusters;
}

// --- CTA Scoring ---

/**
 * Scores all eligible CTAs based on topic overlap, user intent,
 * user role, and recency exclusion.
 *
 * @param {Object} ctaDefinitions - All CTA definitions from config
 * @param {string[]} matchedTopics - Topics found in the AI response
 * @param {"exploring"|"committed"} userIntent
 * @param {{ role: string, confidence: string }} userRole
 * @param {string[]} recentlyShownIds - CTA IDs shown in last N turns
 * @param {string[]} completedForms - Form IDs already completed
 * @returns {Array<{id: string, score: number, cta: Object, cluster: string|null}>}
 */
function scoreCTAs(ctaDefinitions, matchedTopics, userIntent, userRole, recentlyShownIds = [], completedForms = []) {
  if (!ctaDefinitions) return [];

  const recentSet = new Set(recentlyShownIds);
  const completedSet = new Set(completedForms);
  const topicSet = new Set(matchedTopics);
  const scored = [];

  for (const [id, cta] of Object.entries(ctaDefinitions)) {
    if (!cta.ai_available) continue;

    const meta = cta.selection_metadata;
    if (!meta) continue;

    // Exclusion: recently shown (hard exclude)
    if (recentSet.has(id)) continue;

    // Exclusion: completed form
    if (cta.action === 'start_form' && cta.formId && completedSet.has(cta.formId)) continue;

    // Role filtering: when role is detected with high confidence,
    // suppress CTAs from the opposing role_axis.
    const ctaRole = meta.role_axis;
    if (userRole.confidence === 'high' && ctaRole) {
      if (userRole.role === 'give' && ctaRole === 'receive') continue;
      if (userRole.role === 'receive' && ctaRole === 'give') continue;
    }

    // Score: topic overlap
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

    // Boost: committed intent + action depth (role-aware)
    let intentBoost = 0;
    if (userIntent === 'committed' && meta.depth_level === 'action') {
      intentBoost = 0.3;
    }

    // Boost: role alignment on low-confidence (soft boost, not hard filter)
    let roleBoost = 0;
    if (userRole.confidence === 'low' && userRole.role !== 'neutral' && ctaRole) {
      if (ctaRole === userRole.role) roleBoost = 0.2;
    }

    const totalScore = topicScore + intentBoost + roleBoost;

    // Determine which cluster this CTA primarily belongs to
    const primaryCluster = (ctaTopics.find(t => topicSet.has(t))) || null;

    scored.push({ id, score: totalScore, cta, cluster: primaryCluster });
  }

  scored.sort((a, b) => b.score - a.score);
  return scored;
}

// --- Slot Filling ---

/**
 * Fills 4 slots (action, info1, info2, lateral) from scored CTAs.
 * When multiple topic clusters exist, allocates info slots across clusters.
 * Returns 2-4 CTAs depending on availability.
 *
 * @param {Array<{id: string, score: number, cta: Object, cluster: string|null}>} scoredCTAs
 * @param {string[][]} topicClusters - Topic clusters from buildTopicClusters
 * @returns {{action: Object|null, info1: Object|null, info2: Object|null, lateral: Object|null}}
 */
function fillSlots(scoredCTAs, topicClusters) {
  const slots = { action: null, info1: null, info2: null, lateral: null };
  const usedIds = new Set();

  // Fill action slot: highest-scoring action-eligible CTA
  for (const item of scoredCTAs) {
    if (usedIds.has(item.id)) continue;
    const eligibility = item.cta.selection_metadata.slot_eligibility || [];
    if (eligibility.includes('action')) {
      slots.action = formatSlotCTA(item, 'action');
      usedIds.add(item.id);
      break;
    }
  }

  // Determine cluster allocation for info slots.
  // If 2+ distinct clusters exist, try to give each cluster one info slot.
  const distinctClusters = topicClusters.filter(c => c.length > 0);
  const multiCluster = distinctClusters.length >= 2;

  if (multiCluster) {
    // Build cluster sets for matching
    const clusterSets = distinctClusters.map(c => new Set(c));

    // Fill info1 from cluster A, info2 from cluster B
    for (let ci = 0; ci < Math.min(clusterSets.length, 2); ci++) {
      const clusterSet = clusterSets[ci];
      const slotKey = ci === 0 ? 'info1' : 'info2';

      for (const item of scoredCTAs) {
        if (usedIds.has(item.id)) continue;
        const eligibility = item.cta.selection_metadata.slot_eligibility || [];
        if (!eligibility.includes('info')) continue;
        // CTA belongs to this cluster if any of its tags are in the cluster
        const ctaTags = item.cta.selection_metadata.topic_tags || [];
        const belongsToCluster = ctaTags.some(t => clusterSet.has(t));
        if (belongsToCluster) {
          slots[slotKey] = formatSlotCTA(item, slotKey);
          usedIds.add(item.id);
          break;
        }
      }
    }
  } else {
    // Single cluster or no clusters: fill info1 and info2 by score
    let infoCount = 0;
    for (const item of scoredCTAs) {
      if (infoCount >= 2) break;
      if (usedIds.has(item.id)) continue;
      const eligibility = item.cta.selection_metadata.slot_eligibility || [];
      if (eligibility.includes('info')) {
        const slotKey = infoCount === 0 ? 'info1' : 'info2';
        slots[slotKey] = formatSlotCTA(item, slotKey);
        usedIds.add(item.id);
        infoCount++;
      }
    }
  }

  // Fill lateral slot: prefer LOWEST-scoring lateral-eligible CTA
  const lateralCandidates = scoredCTAs
    .filter(item => {
      if (usedIds.has(item.id)) return false;
      const meta = item.cta.selection_metadata;
      const eligibility = meta.slot_eligibility || [];
      return eligibility.includes('lateral') || meta.lateral_eligible;
    })
    .sort((a, b) => a.score - b.score);

  if (lateralCandidates.length > 0) {
    slots.lateral = formatSlotCTA(lateralCandidates[0], 'lateral');
    usedIds.add(lateralCandidates[0].id);
  }

  return slots;
}

/**
 * Formats a scored CTA item into the CTA button object expected by the frontend.
 */
function formatSlotCTA(item, slotName) {
  const cta = item.cta;
  return {
    id: item.id,
    label: cta.label || cta.text || item.id,
    action: cta.action,
    type: cta.action,
    ...(cta.formId && { formId: cta.formId }),
    ...(cta.url && { url: cta.url }),
    ...(cta.target_branch && { target_branch: cta.target_branch }),
    ...(cta.query && { query: cta.query }),
    ...(cta.value && { value: cta.value }),
    _position: item.cta.selection_metadata?.depth_level === 'action' ? 'primary' : 'secondary',
    _slot: slotName,
    _score: item.score
  };
}

// --- Main Entry Point ---

/**
 * Selects 2-4 contextually relevant CTAs for the current turn.
 *
 * @param {string} responseText - The AI's buffered response
 * @param {string} userMessage - The user's input message
 * @param {Object} config - Tenant configuration
 * @param {Object} sessionState - Session state
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

  // Step 2: Detect user intent and role
  const userIntent = detectUserIntent(userMessage);
  const userRole = detectUserRole(userMessage);

  // Step 3: Build topic clusters for slot allocation
  const topicClusters = buildTopicClusters(matchedTopics, config);

  // Step 4: Score all eligible CTAs (role-filtered)
  const scoredCTAs = scoreCTAs(
    ctaDefinitions,
    matchedTopics,
    userIntent,
    userRole,
    effectiveRecency,
    completedForms
  );

  // Step 5: Fill slots with cluster-aware allocation
  const slots = fillSlots(scoredCTAs, topicClusters);

  // Collect non-null slots into array (preserve order: action, info1, info2, lateral)
  const ctaButtons = [];
  if (slots.action) ctaButtons.push(slots.action);
  if (slots.info1) ctaButtons.push(slots.info1);
  if (slots.info2) ctaButtons.push(slots.info2);
  if (slots.lateral) ctaButtons.push(slots.lateral);

  // Build selection log for observability
  const selectionLog = {
    event: 'cta_selection',
    matched_topics: matchedTopics,
    topic_clusters: topicClusters,
    user_intent: userIntent,
    user_role: userRole,
    recency_reset: turnsSinceClick >= 5,
    scores_top5: scoredCTAs.slice(0, 5).map(c => ({ id: c.id, score: c.score, role_axis: c.cta.selection_metadata?.role_axis })),
    selected: ctaButtons.map(c => c.id),
    slots: {
      action: slots.action?.id || null,
      info1: slots.info1?.id || null,
      info2: slots.info2?.id || null,
      lateral: slots.lateral?.id || null
    }
  };

  return { ctaButtons, selectionLog };
}

module.exports = {
  selectCTAs,
  extractTopics,
  detectUserIntent,
  detectUserRole,
  buildTopicClusters,
  scoreCTAs,
  fillSlots,
  INTENT_PHRASES,
  ROLE_SIGNALS
};
