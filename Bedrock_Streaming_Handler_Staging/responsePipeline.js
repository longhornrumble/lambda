'use strict';

/**
 * Shared post-response pipeline for the streaming + buffered handlers (dedup #5, Phase 1).
 *
 * Both handler twins ran ~identical logic after the model reply is assembled: the §B14
 * scheduling action boundary (runSchedulingTurn / runNewBookingEntry), then the CTA-selection
 * tier ladder (explicit-click → V5 → V4 → topic-pool → enhanceResponse fallback), each emitting
 * `cta_buttons` / `showcase_card` SSE frames. The ONLY intended difference was the emit
 * mechanism: the streaming handler `write(...)`s frames live; the buffered handler splices them
 * into its `chunks` array before the terminal `[DONE]`. Hand-mirroring the two drifted (e.g. the
 * buffered twin silently dropped the `enhanceResponse.showcaseCard` emit that streaming has in
 * the explicit-click + fallback tiers — an F-DSAR25-class divergence). This module is the single
 * source of truth; each handler passes its own `emit`.
 *
 * `emit(sseFrameString)` writes ONE ready-to-send SSE frame. Streaming passes its live `write`;
 * the buffered path passes `(data) => chunks.splice(chunks.length - 1, 0, data)` so frames land
 * before the already-pushed `[DONE]`. The pipeline never touches the terminal marker itself.
 *
 * Behaviour-preserving vs the pre-dedup STREAMING (prod) path — verified against the dual-handler
 * characterization net (handler_characterization.test.js). The buffered path GAINS the showcase
 * frame it was missing (drift fix); that is the deliberate acceptance signal for this change.
 */

const { enhanceResponse } = require('./response_enhancer');
const {
  classifyTopic,
  selectCTAsFromPool,
  selectActionsV4,
  validateTopicDefinitions,
} = require('./prompt_v4');
const { validateActionIds } = require('./prompt_v5');
const { redactPII } = require('./redactPII');
const { runSchedulingTurn } = require('./scheduling/schedulingFlow');
const { runNewBookingEntry } = require('./scheduling/newBookingEntry');

/**
 * Build the CTA buttons for a selected-id list (V4 / V5 tiers share this shape).
 * Strips the internal `style` field and stamps primary/secondary position.
 */
function ctaButtonsFromIds(selectedIds, config) {
  return selectedIds.map((id, idx) => {
    const { style, ...cleanCta } = config.cta_definitions[id] || {};
    return { ...cleanCta, id, _position: idx === 0 ? 'primary' : 'secondary' };
  });
}

/**
 * Run the shared post-response pipeline. Emits scheduling + CTA/showcase SSE frames via `emit`.
 * @returns {Promise<{schedulingResult: object|null, newBookingResult: object|null}>}
 */
async function runResponsePipeline({
  emit,
  responseBuffer,
  userInput,
  conversationHistory,
  config,
  tenantHash,
  sessionId,
  bindingSessionId,
  routingMetadata,
  sessionContext,
  schedulingEnabled,
  bedrock,
  v5Active,
  v5Tail,
  v5CatalogEmpty,
  schedulingDeps,
  schedulingExecDep,
  newBookingDep,
}) {
  const send = (frame) => emit(`data: ${JSON.stringify(frame)}\n\n`);

  let schedulingResult = null;
  let newBookingResult = null;

  // The whole boundary + CTA ladder is best-effort: a failure here must NOT fail the
  // already-streamed response (matches both handlers' `catch (enhanceError)`).
  try {
    const validation = validateTopicDefinitions(config);

    // WS-CONVO (B3 keystone): post-stream §B14 action boundary. Resolves the §B10 binding
    // (no-op when absent → the CTA ladder below runs unchanged) and, when a reschedule/cancel
    // binding governs this turn, detects a STRUCTURED action and executes via the shipped §B9
    // modules — NEVER on free text. Feature-gated: OFF → null → CTA ladder unchanged.
    schedulingResult = schedulingEnabled
      ? await runSchedulingTurn({
          responseText: responseBuffer, conversationHistory,
          tenantId: config?.tenant_id, sessionId, bindingSessionId, config, bedrock, write: emit,
          deps: { ...schedulingDeps, ...schedulingExecDep },
        })
      : null;

    // WS-NEWBOOK (§B16d): if the recovery loop didn't own this turn, try the NEW-booking
    // entry-hook (engages on routing_metadata.scheduling_intent:'new_booking' or an in-flight
    // new-booking session row). No-op for normal chat.
    newBookingResult = (schedulingEnabled && !schedulingResult?.handled)
      ? await runNewBookingEntry({
          responseText: responseBuffer, conversationHistory,
          tenantId: config?.tenant_id, sessionId, config, bedrock, write: emit,
          routingMetadata, deps: { ...schedulingDeps, ...newBookingDep },
        })
      : null;

    if (schedulingResult?.handled || newBookingResult?.handled) {
      // S-3: the scheduling turn owns the post-stream surface — skip CTA selection so a
      // scheduling turn doesn't also append CTAs.
      console.log(`[WS-CONVO] scheduling turn handled (action=${schedulingResult?.action || newBookingResult?.action || 'n/a'}) — skipping CTA selection`);
    } else if (routingMetadata.action_chip_triggered || routingMetadata.cta_triggered) {
      // Tiers 1-2: Explicit clicks — use enhanceResponse()
      console.log('[Tier 1-2] Explicit click routing — using enhanceResponse()');
      const enhancedData = await enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata);
      if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
        send({ type: 'cta_buttons', ctaButtons: enhancedData.ctaButtons, metadata: enhancedData.metadata, session_id: sessionId });
        console.log(`🎯 [Tier 1-2] sent ${enhancedData.ctaButtons.length} CTAs | tier: ${enhancedData.metadata?.routing_tier || 'explicit'}`);
      }
      if (enhancedData.showcaseCard) {
        send({ type: 'showcase_card', showcaseCard: enhancedData.showcaseCard, metadata: enhancedData.metadata, session_id: sessionId });
      }
    } else if (v5Active) {
      // V5 single-pass: actions came from the SAME call that wrote the prose (v5Tail). Sits
      // BEFORE V4_ACTION_SELECTOR — a tenant may carry both flags and V5 must win. Validation
      // = selectActionsV4's semantics (known-ids filter + cap 4, shared via validateActionIds).
      let selectedIds;
      if (v5Tail && v5Tail.actionIds !== null) {
        selectedIds = validateActionIds(v5Tail.actionIds, config);
      } else if (v5CatalogEmpty) {
        // No ai_available CTAs ⇒ the prompt asked for no tail; nothing to select, no fallback.
        selectedIds = [];
      } else {
        // Fail-soft ladder: no/bad tail → ONE selectActionsV4 call → else no buttons.
        console.log(`[V5 SinglePass] tail ${v5Tail ? v5Tail.status : 'missing'} — fail-soft selectActionsV4`);
        selectedIds = await selectActionsV4(responseBuffer, conversationHistory, config, bedrock);
      }
      if (selectedIds.length > 0) {
        const ctaButtons = ctaButtonsFromIds(selectedIds, config);
        send({ type: 'cta_buttons', ctaButtons, metadata: { routing_tier: 'v5_single_pass', selected_ids: selectedIds, conversation_context: { selected_ctas: selectedIds } }, session_id: sessionId });
        console.log(`🎯 [V5 SinglePass] sent ${ctaButtons.length} CTAs: [${selectedIds.join(', ')}]`);
      } else {
        console.log('[V5 SinglePass] No CTAs selected');
      }
    } else if (config.feature_flags?.V4_ACTION_SELECTOR) {
      // V4.0 Action Selector: AI picks CTAs from the full vocabulary.
      console.log('[V4 ActionSelector] Using LLM-based CTA selection');
      const selectedIds = await selectActionsV4(responseBuffer, conversationHistory, config, bedrock);
      if (selectedIds.length > 0) {
        const ctaButtons = ctaButtonsFromIds(selectedIds, config);
        send({ type: 'cta_buttons', ctaButtons, metadata: { routing_tier: 'v4_action_selector', selected_ids: selectedIds, conversation_context: { selected_ctas: selectedIds } }, session_id: sessionId });
        console.log(`🎯 [V4 ActionSelector] sent ${ctaButtons.length} CTAs: [${selectedIds.join(', ')}]`);
      } else {
        console.log('[V4 ActionSelector] No CTAs selected');
      }
    } else if (validation.definitions.length > 0) {
      // Step 3a: Topic classification (non-streaming LLM call).
      console.log(`[Step 3a] Classifying topic (${validation.definitions.length} definitions)`);
      let topicName = await classifyTopic(
        userInput, conversationHistory,
        { ...config, topic_definitions: validation.definitions }, bedrock
      );
      // Continuation detection: short/ambiguous messages carry forward the previous topic.
      const isShortMessage = userInput.trim().length < 20;
      const isNullOrGeneral = !topicName || topicName === 'general_inquiry';
      const previousTopic = sessionContext.last_classified_topic;
      if (isShortMessage && isNullOrGeneral && previousTopic) {
        console.log(`[Step 3a] Continuation detected: "${redactPII(userInput)}" → carrying forward topic "${previousTopic}"`);
        topicName = previousTopic;
      }
      // Step 3b: Dynamic CTA pool selection (deterministic, no AI).
      const result = selectCTAsFromPool(topicName, config, sessionContext);
      if (result.ctaButtons && result.ctaButtons.length > 0) {
        send({ type: 'cta_buttons', ctaButtons: result.ctaButtons, metadata: result.metadata, session_id: sessionId });
        console.log(`🎯 [Step3] sent ${result.ctaButtons.length} CTAs | topic: ${result.metadata?.classified_topic || 'null'} | depth: ${result.metadata?.depth} | method: ${result.metadata?.routing_method}`);
      } else {
        console.log(`[Step3] No CTAs to send | topic: ${topicName || 'null'} | method: ${result.metadata?.routing_method}`);
      }
    } else {
      // No topic_definitions — fallback to enhanceResponse().
      console.log('No topic_definitions configured — using enhanceResponse()');
      const enhancedData = await enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata);
      if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
        send({ type: 'cta_buttons', ctaButtons: enhancedData.ctaButtons, metadata: enhancedData.metadata, session_id: sessionId });
        console.log(`🎯 [fallback] sent ${enhancedData.ctaButtons.length} CTAs | tier: ${enhancedData.metadata?.routing_tier || 'unknown'}`);
      }
      if (enhancedData.showcaseCard) {
        send({ type: 'showcase_card', showcaseCard: enhancedData.showcaseCard, metadata: enhancedData.metadata, session_id: sessionId });
      }
    }
  } catch (enhanceError) {
    console.error('❌ CTA enhancement error:', enhanceError);
    // Don't fail the response if the scheduling boundary / CTA enhancement fails.
  }

  return { schedulingResult, newBookingResult };
}

module.exports = { runResponsePipeline, ctaButtonsFromIds };
