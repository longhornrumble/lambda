'use strict';

/**
 * agentTurn.js — the §B17b bounded agent tool loop (WS-AG-CORE keystone).
 *
 * The platform's FIRST agent execution surface (design doc
 * AGENTIC_SCHEDULING_SLICE_DESIGN.md §3.2/§13; FROZEN_CONTRACTS.md §B17 — LOCKED
 * 2026-06-12 incl. the governance + PII advisory amendments).
 *
 * ── What this is ──
 * When a scheduling-enabled tenant has `feature_flags.AGENTIC_SCHEDULING` on and a
 * typed-text turn arrives with an in-flight scheduling session (§B17a — the INTEGRATOR
 * wires that routing branch in index.js; this module is only the turn itself), the
 * turn runs a bounded tool loop: stream the model with the two §B17c tools attached,
 * execute any tool call server-side via agentTools.js (emitting the tool's UI SSE
 * event mid-turn), append assistant + tool_result blocks, and continue — at most
 * MAX_TOOL_ITERATIONS model calls. Clicks stay deterministic (§B16b); the agent owns
 * SENTENCES. No tool books: the §B16c commit seam is unreachable from this module
 * (no reference exists; jest statically asserts it).
 *
 * ── Interface (consumed by the integrator + WS-AG-EVAL) ──
 *   agentTurn({ event, context, sessionRow, tenantConfig, deps, streamWriter }) → void
 *     event:        { userText (or user_input), conversationHistory (or
 *                     conversation_history), sessionId?, systemPrompt? } — systemPrompt
 *                     is the persona/KB prompt the non-agent path already builds;
 *                     falls back to sanitizeTonePromptV4(tenantConfig.tone_prompt).
 *     context:      Lambda context (reserved; unused in Phase-0).
 *     sessionRow:   the LIVE ConversationSchedulingSession row read before entry
 *                     (§B17b: session state derives from server state, never model output).
 *     tenantConfig: the authenticated S3 tenant config.
 *     deps:         { bedrock (required), invokeProposal, saveState, loadState?,
 *                     qualifyingContext?, auditLog?, logger?, env? } — auditLog
 *                     overrides the default console-JSON audit writer; env overrides
 *                     process.env (tests).
 *     streamWriter: the BSH SSE write(string) function.
 *
 *   isAgentTurnEnabled({ env, tenantConfig }) → boolean — the §B17h guard the
 *     integrator checks before calling agentTurn (also re-checked inside, fail-closed).
 *
 * ── Kill switches (§B17h) ──
 *   env AGENTIC_SCHEDULING_DISABLED='true' → both agent branches off unconditionally
 *   (checked FIRST). Per-tenant feature_flags.AGENTIC_SCHEDULING must be exactly true.
 *   feature_flags.scheduling_enabled must be true (the §B17a routing premise — the
 *   platform's master scheduling gate; every in-chat scheduling entry point checks it).
 *   When blocked: return WITHOUT entering the loop — no model call, no SSE, no audit
 *   (flag-off tenants must be byte-identical to the pre-agent baseline).
 *
 * ── Suppression pre-check (§B17f) ──
 *   Runs on EVERY agent turn BEFORE the model call (sensitiveContext.js; full-session
 *   scan window; sticky via full-window rescan + a tolerated `suppression_latched`
 *   session-row field if the integrator later persists one; FAILS CLOSED). On trip:
 *   warm human-contact copy + tenant-configured crisis resources
 *   (tenantConfig.scheduling.crisis_resources), NO model call, no unprompted resume.
 *   Minor self-ID additionally stops email solicitation — trivially satisfied here
 *   because the whole agent turn is suppressed. The increment-2 suggestion-offer gate
 *   (§B17f holds #1–#3/#5) is OUT of this slice — integrator wires it later.
 *
 * ── Audit (§B17g — EXHAUSTIVE field allowlist) ──
 *   agent_tool_call + agent_turn_summary (+ suggestion_gate_decision on suppression
 *   trips) are emitted via deps.auditLog or the default structured-JSON console line
 *   (the same writer pattern as the other scheduling audit events). FORBIDDEN
 *   everywhere: raw attendee_email, ANY email hash, message/narration text,
 *   tool_result bodies. Error logging = err.name only. Jest asserts serialized lines
 *   for an email-bearing turn never contain '@'.
 *
 * ── Overflow note (§B17b implemented verbatim) ──
 *   The loop body executes tool calls on every iteration the model requests one,
 *   per the §B17b pseudocode. Overflow = the model STILL wants tools after the final
 *   iteration; recorded via agent_turn_summary.overflow=true + the scheduling_notice
 *   (reason 'agent_overflow') + templated warm-honest copy. No 4th model call is ever
 *   made. The agent_tool_call outcome value 'overflow' is reserved for a refused tool
 *   execution; under the verbatim loop every requested tool within budget executes,
 *   so the canonical overflow record is the turn summary (flagged to the integrator
 *   in the WS-AG-CORE PR as a contract-ambiguity note, not a fork).
 */

const { InvokeModelWithResponseStreamCommand } = require('@aws-sdk/client-bedrock-runtime');
const { sanitizeTonePromptV4, V4_STEP2_INFERENCE_PARAMS } = require('../prompt_v4');
const {
  AGENT_TOOL_DEFINITIONS,
  executeGetAvailableTimes,
  executeRequestBookingConfirmation,
} = require('./agentTools');
const { checkSensitiveContext, userSideTranscript } = require('./sensitiveContext');

// §B17b LOOP INVARIANT.
const MAX_TOOL_ITERATIONS = 3;

// agent_turn_summary.prompt_version (§B17g) — bump when AGENT_NARRATION_RULES changes.
const PROMPT_VERSION = 'b17e.v1';

// §B17b overflow: templated warm-honest copy (verbatim) + the shipped async-escape SSE.
const OVERFLOW_COPY = 'I ran into a snag — let me get someone to help.';

// Honest-failure copy for a mid-turn model/stream error (acceptance §9.4: no dead air).
const AGENT_ERROR_COPY =
  "I hit a technical snag just now — give me a moment, or a real person can follow up with you by email.";

// §B17f trip: warm human-contact copy. Tenant crisis resources are appended when
// configured. Never pushes the booking flow (no unprompted resume).
const SUPPRESSION_COPY =
  "I want to pause the scheduling side of this for a moment — it sounds like there's " +
  'something going on that a caring person should help with directly, and I don\'t want ' +
  'to route you through a booking flow right now.';
const SUPPRESSION_CLOSE_COPY =
  'A real person here can help you directly whenever you\'re ready.';

// ─── §B17e — agent narration rules (scheduling instruction block; LOCKED text) ────────

const AGENT_NARRATION_RULES = [
  'SCHEDULING AGENT RULES (locked):',
  '1. You have live scheduling tools: you can look up REAL times and stage NEW bookings. ' +
    'You cannot reschedule, cancel, or see existing bookings — offer the email/human ' +
    'fallback for those. If a tool fails, say the lookup failed right now; never say you ' +
    'lack scheduling access, never invent times.',
  '2. Never state or imply a booking exists. Confirmed bookings are announced by the ' +
    'system, not you.',
  '3. Only mention times returned by get_available_times this conversation.',
  "4. Before staging you must have the user's email — ask naturally; one question at a time.",
  '5. Mid-booking, no KB tangents; answer side-questions briefly and return to the flow.',
  '6. After staging, state plainly that nothing is booked until they press Confirm.',
  "7. Never repeat the user's email back in your text — the confirmation card displays it.",
  '8. When asking for the email, say why (to send the calendar invite).',
  '9. Never imply a human has already been involved; the MEETING is with a human, the ' +
    'scheduler is an AI assistant.',
  '10. Avoid guarantee language about offered times (a slot can be taken until confirmed).',
  '11. (increment 2 only) Offer booking only when the suggestion gate passes; ' +
    '"just exploring" gets learning content, never a booking pitch.',
].join('\n');

// ─── §B17h kill-switch guard ───────────────────────────────────────────────────────────

/**
 * The guard the integrator checks before calling agentTurn (§B17h; also re-checked
 * inside agentTurn, fail-closed). Order: global env override FIRST, then the platform
 * scheduling gate, then the per-tenant agent flag. All reads tolerate absence (false).
 *
 * @param {object} params - { env, tenantConfig }
 * @returns {boolean}
 */
function isAgentTurnEnabled({ env, tenantConfig } = {}) {
  const e = env || process.env;
  if (e && e.AGENTIC_SCHEDULING_DISABLED === 'true') return false; // global emergency override
  if (tenantConfig?.feature_flags?.scheduling_enabled !== true) return false; // §B17a premise
  if (tenantConfig?.feature_flags?.AGENTIC_SCHEDULING !== true) return false; // increment-1 flag
  return true;
}

// ─── §B17d session-state line ──────────────────────────────────────────────────────────

/**
 * Build the §B17d state line from the LIVE session row (never model output).
 *   "[scheduling state: <state> | staged slot: <label> (<slotId>) | email: <known|unknown>]"
 * PII RULE (pinned): the email segment is EXACTLY 'email: known' or 'email: unknown' —
 * the raw address NEVER appears (jest-asserted).
 *
 * @param {object|null} sessionRow
 * @returns {string}
 */
function buildStateLine(sessionRow) {
  const state = (sessionRow && sessionRow.state) || 'none';
  let staged = 'none';
  const sel = sessionRow && sessionRow.selected_slot;
  if (sel && sel.slotId) {
    const fromCandidates = ((sessionRow && sessionRow.candidate_slots) || []).find(
      (s) => s && s.slotId === sel.slotId
    );
    const label = (fromCandidates && fromCandidates.label) || sel.label;
    staged = label ? `${label} (${sel.slotId})` : `(${sel.slotId})`;
  }
  const emailKnown = !!(
    sessionRow &&
    typeof sessionRow.attendee_email === 'string' &&
    sessionRow.attendee_email.trim()
  );
  return `[scheduling state: ${state} | staged slot: ${staged} | email: ${emailKnown ? 'known' : 'unknown'}]`;
}

// ─── §B17g audit emitters (EXHAUSTIVE allowlists — emit nothing not listed) ────────────

function emitAudit(deps, event) {
  const logger = (deps && deps.logger) || console;
  try {
    if (deps && typeof deps.auditLog === 'function') {
      deps.auditLog(event);
    } else {
      logger.info(JSON.stringify(event));
    }
  } catch (err) {
    logger.error(`[WS-AG-CORE] audit emit failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
  }
}

// agent_tool_call — built field-by-field from the §B17g allowlist; tool args are
// reduced to: slot_id (opaque id), date (the YYYY-MM-DD arg), exclude_slot_ids
// (opaque ids), email_present (boolean — NEVER the email).
function emitAgentToolCall(deps, { tenantId, sessionId, tool, outcome, latencyMs, iteration, input }) {
  const evt = {
    event_type: 'agent_tool_call',
    tenant_id: tenantId,
    session_id: sessionId,
    tool,
    outcome,
    latency_ms: latencyMs,
    iteration,
    email_present: !!(input && typeof input.attendee_email === 'string' && input.attendee_email),
  };
  if (input && typeof input.slot_id === 'string') evt.slot_id = input.slot_id;
  if (input && typeof input.date === 'string') evt.date = input.date;
  if (input && Array.isArray(input.exclude_slot_ids)) {
    evt.exclude_slot_ids = input.exclude_slot_ids.filter((v) => typeof v === 'string');
  }
  emitAudit(deps, evt);
}

function emitAgentTurnSummary(deps, { tenantId, sessionId, iterations, stopReasonSequence, overflow, modelId, flagsActive }) {
  emitAudit(deps, {
    event_type: 'agent_turn_summary',
    tenant_id: tenantId,
    session_id: sessionId,
    iterations,
    stop_reason_sequence: stopReasonSequence,
    overflow,
    prompt_version: PROMPT_VERSION,
    model_id: modelId,
    flags_active: flagsActive,
  });
}

function emitSuggestionGateDecision(deps, { tenantId, sessionId, offered, reasonCodes, suppressionCategory }) {
  const evt = {
    event_type: 'suggestion_gate_decision',
    tenant_id: tenantId,
    session_id: sessionId,
    offered,
    reason_codes: reasonCodes,
  };
  if (suppressionCategory) evt.suppression_category = suppressionCategory; // category CODE only
  emitAudit(deps, evt);
}

// ─── streaming model call (one §B17b loop iteration) ───────────────────────────────────

/**
 * One InvokeModelWithResponseStreamCommand call. Forwards text deltas as SSE
 * (IDENTICAL frame shape to the non-agent path: {type:'text', content, session_id}),
 * accumulates assistant content blocks (text + tool_use), and returns the stop_reason.
 */
async function streamModelCall({ bedrock, modelId, system, messages, write, sessionId, turnState }) {
  const command = new InvokeModelWithResponseStreamCommand({
    modelId,
    accept: 'application/json',
    contentType: 'application/json',
    body: JSON.stringify({
      anthropic_version: 'bedrock-2023-05-31',
      system,
      messages,
      tools: AGENT_TOOL_DEFINITIONS,
      max_tokens: V4_STEP2_INFERENCE_PARAMS.max_tokens,
      temperature: V4_STEP2_INFERENCE_PARAMS.temperature,
    }),
  });

  const response = await bedrock.send(command);

  let stopReason = null;
  const blocks = [];
  let current = null;

  const flushCurrent = () => {
    if (!current) return;
    if (current.type === 'tool_use') {
      let input = {};
      try {
        input = current.inputJson ? JSON.parse(current.inputJson) : {};
      } catch {
        input = {}; // malformed tool input JSON → empty args (executors fail it closed)
      }
      blocks.push({ type: 'tool_use', id: current.id, name: current.name, input });
    } else if (current.type === 'text' && current.text) {
      blocks.push({ type: 'text', text: current.text });
    }
    current = null;
  };

  for await (const evt of response.body) {
    if (!evt || !evt.chunk || !evt.chunk.bytes) continue;
    const data = JSON.parse(new TextDecoder().decode(evt.chunk.bytes));

    if (data.type === 'content_block_start') {
      flushCurrent(); // defensive: a start without a prior stop
      const cb = data.content_block || {};
      if (cb.type === 'tool_use') {
        current = { type: 'tool_use', id: cb.id, name: cb.name, inputJson: '' };
      } else {
        current = { type: 'text', text: typeof cb.text === 'string' ? cb.text : '' };
        if (!turnState.streamStarted && typeof write === 'function') {
          // Nudge frame — same as the non-agent path; once per agent turn.
          write('data: {"type":"stream_start"}\n\n');
          turnState.streamStarted = true;
        }
      }
    } else if (data.type === 'content_block_delta') {
      const delta = data.delta || {};
      if (delta.type === 'text_delta' && delta.text) {
        if (typeof write === 'function') {
          write(`data: ${JSON.stringify({ type: 'text', content: delta.text, session_id: sessionId })}\n\n`);
        }
        if (current && current.type === 'text') current.text += delta.text;
        else if (!current) current = { type: 'text', text: delta.text }; // defensive
      } else if (delta.type === 'input_json_delta') {
        if (current && current.type === 'tool_use') current.inputJson += delta.partial_json || '';
      }
    } else if (data.type === 'content_block_stop') {
      flushCurrent();
    } else if (data.type === 'message_delta') {
      if (data.delta && data.delta.stop_reason) stopReason = data.delta.stop_reason;
    }
    // message_start / message_stop need no handling; the iterator ends the loop.
  }
  flushCurrent(); // defensive: stream ended without a final content_block_stop

  return { stopReason, blocks };
}

// ─── tool execution wrapper (audit + result envelope) ──────────────────────────────────

async function runOneTool(toolUse, { iteration, tenantId, sessionId, tenantConfig, deps, write, getSession, setSession, userTranscript }) {
  const logger = (deps && deps.logger) || console;
  const started = Date.now();
  let result;
  try {
    if (toolUse.name === 'get_available_times') {
      result = await executeGetAvailableTimes({
        input: toolUse.input,
        session: getSession(),
        tenantId,
        sessionId,
        tenantConfig,
        deps,
        write,
        setSession,
      });
    } else if (toolUse.name === 'request_booking_confirmation') {
      result = await executeRequestBookingConfirmation({
        input: toolUse.input,
        session: getSession(),
        tenantId,
        sessionId,
        tenantConfig,
        deps,
        write,
        userTranscript,
        setSession,
      });
    } else {
      // The API constrains tool names to the provided catalog; treat drift as an
      // honest transient failure (closed §B17c error vocabulary).
      logger.warn('[WS-AG-CORE] unknown tool requested by model — refused');
      result = { error: 'lookup_failed', note: 'That tool is not available.' };
    }
  } catch (err) {
    logger.error(`[WS-AG-CORE] tool executor failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    result = { error: 'lookup_failed', note: 'The tool failed just now. Offer the email/human fallback.' };
  }

  const outcome = result && result.error
    ? result.error
    : toolUse.name === 'request_booking_confirmation' ? 'staged' : 'ok';
  emitAgentToolCall(deps, {
    tenantId,
    sessionId,
    tool: toolUse.name,
    outcome,
    latencyMs: Date.now() - started,
    iteration,
    input: toolUse.input,
  });

  return result;
}

// ─── the agent turn (§B17b) ────────────────────────────────────────────────────────────

/**
 * Run one agent turn. → void (streams SSE text deltas + tool UI events; never returns
 * a booking row; never throws — all failures degrade to honest copy on the stream).
 *
 * @param {object} params - { event, context, sessionRow, tenantConfig, deps, streamWriter }
 */
async function agentTurn({ event, context, sessionRow, tenantConfig, deps = {}, streamWriter } = {}) {
  void context; // reserved (Lambda context; unused in Phase-0)
  const logger = deps.logger || console;
  const write = typeof streamWriter === 'function' ? streamWriter : null;

  // §B17h kill switches — re-checked here fail-closed (the integrator also gates).
  // Blocked → return with NO side effects (flag-off behavior must be byte-identical
  // to the pre-agent baseline: no model call, no SSE, no audit).
  if (!isAgentTurnEnabled({ env: deps.env || process.env, tenantConfig })) return;

  const userText = (event && (event.userText ?? event.user_input)) || '';
  const conversationHistory =
    (event && (event.conversationHistory || event.conversation_history)) || [];
  const sessionId =
    (sessionRow && sessionRow.session_id) ||
    (event && (event.sessionId || event.session_id)) ||
    '';
  const tenantId =
    (tenantConfig && tenantConfig.tenant_id) || (sessionRow && sessionRow.tenantId) || '';

  if (!userText || !deps.bedrock || typeof deps.bedrock.send !== 'function') {
    logger.warn('[WS-AG-CORE] agentTurn skipped: missing userText or bedrock client');
    return;
  }

  const flagsActive = [];
  if (tenantConfig?.feature_flags?.scheduling_enabled === true) flagsActive.push('scheduling_enabled');
  if (tenantConfig?.feature_flags?.AGENTIC_SCHEDULING === true) flagsActive.push('AGENTIC_SCHEDULING');
  if (tenantConfig?.feature_flags?.AGENTIC_SCHEDULING_SUGGEST === true) flagsActive.push('AGENTIC_SCHEDULING_SUGGEST');
  const modelId =
    (tenantConfig && (tenantConfig.model_id || (tenantConfig.aws && tenantConfig.aws.model_id))) ||
    process.env.BEDROCK_MODEL_ID;

  // §B17f suppression pre-check — EVERY agent turn, BEFORE the model call. Full-session
  // scan window; sticky (full-window rescan + tolerated persisted latch); fails closed.
  const suppression = checkSensitiveContext({
    conversationHistory,
    userText,
    tenantConfig,
    priorLatched: !!(sessionRow && sessionRow.suppression_latched === true),
    priorCategory: sessionRow && sessionRow.suppression_category,
  });
  if (suppression.tripped) {
    // Pause the flow with warm human-contact copy + tenant-configured crisis resources.
    // NO model call; no unprompted resume; minor self-ID email-solicitation stop is
    // trivially satisfied (the whole agent turn is suppressed). The deterministic click
    // path stays available if the user ASKS to book (§B17f documented asymmetry).
    const crisisResources = tenantConfig?.scheduling?.crisis_resources;
    const copy = [
      SUPPRESSION_COPY,
      typeof crisisResources === 'string' && crisisResources.trim() ? crisisResources.trim() : null,
      SUPPRESSION_CLOSE_COPY,
    ]
      .filter(Boolean)
      .join('\n\n');
    if (write) {
      write(`data: ${JSON.stringify({ type: 'text', content: copy, session_id: sessionId })}\n\n`);
    }
    emitSuggestionGateDecision(deps, {
      tenantId,
      sessionId,
      offered: false,
      reasonCodes: ['suppression_tripped'],
      suppressionCategory: suppression.category, // category CODE — never raw matched text
    });
    emitAgentTurnSummary(deps, {
      tenantId,
      sessionId,
      iterations: 0,
      stopReasonSequence: [],
      overflow: false,
      modelId,
      flagsActive,
    });
    return;
  }

  // §B17b system prompt: persona/KB prompt + §B17e block + §B17d state line.
  const personaPrompt =
    (event && typeof event.systemPrompt === 'string' && event.systemPrompt) ||
    sanitizeTonePromptV4(tenantConfig && tenantConfig.tone_prompt) ||
    'You are a helpful assistant.';
  const system = `${personaPrompt}\n\n${AGENT_NARRATION_RULES}\n\n${buildStateLine(sessionRow)}`;

  // §B17c guard #3 input: the session's USER-SIDE transcript (incl. this turn).
  const userTranscript = userSideTranscript(conversationHistory, userText);

  // Live session view: starts from the pre-entry row; tool executors thread updates
  // via setSession so a later tool call in the SAME turn sees this turn's staging.
  let liveSession = sessionRow || null;
  const getSession = () => liveSession;
  const setSession = (s) => {
    liveSession = s;
  };

  let messages = [{ role: 'user', content: userText }];
  const stopReasonSequence = [];
  let toolCallCount = 0;
  let lastStopReason = null;
  const turnState = { streamStarted: false };

  try {
    // ── the §B17b bounded loop (verbatim) ──
    for (let i = 0; i < MAX_TOOL_ITERATIONS; i++) {
      const { stopReason, blocks } = await streamModelCall({
        bedrock: deps.bedrock,
        modelId,
        system,
        messages,
        write,
        sessionId,
        turnState,
      });
      lastStopReason = stopReason;
      stopReasonSequence.push(stopReason || 'unknown');

      if (stopReason !== 'tool_use') break;

      const toolUses = blocks.filter((b) => b.type === 'tool_use');
      if (toolUses.length === 0) {
        // Defensive: a tool_use stop with no tool_use block — nothing to execute and
        // nothing valid to append; stop here (treated as a non-tool stop, not overflow).
        lastStopReason = stopReason ? 'tool_use_empty' : stopReason;
        break;
      }

      // Execute tool call(s) server-side; the executor emits the tool's UI SSE event.
      // (Each tool_use block in the response gets its tool_result — the API requires it.)
      const toolResults = [];
      for (const toolUse of toolUses) {
        toolCallCount += 1;
        const result = await runOneTool(toolUse, {
          iteration: i + 1, // §B17g: 1-based loop index
          tenantId,
          sessionId,
          tenantConfig,
          deps,
          write,
          getSession,
          setSession,
          userTranscript,
        });
        toolResults.push({
          type: 'tool_result',
          tool_use_id: toolUse.id,
          content: JSON.stringify(result),
        });
      }

      messages = [
        ...messages,
        { role: 'assistant', content: blocks },
        { role: 'user', content: toolResults },
      ];
      // LOOP CONTINUES with updated messages.
    }

    // ── §B17b OVERFLOW: the model still wants tools after the final iteration ──
    if (lastStopReason === 'tool_use') {
      if (write) {
        write(`data: ${JSON.stringify({ type: 'text', content: OVERFLOW_COPY, session_id: sessionId })}\n\n`);
        write(`data: ${JSON.stringify({ type: 'scheduling_notice', reason: 'agent_overflow', session_id: sessionId })}\n\n`);
      }
      emitAgentTurnSummary(deps, {
        tenantId,
        sessionId,
        iterations: toolCallCount,
        stopReasonSequence,
        overflow: true,
        modelId,
        flagsActive,
      });
      return;
    }

    emitAgentTurnSummary(deps, {
      tenantId,
      sessionId,
      iterations: toolCallCount,
      stopReasonSequence,
      overflow: false,
      modelId,
      flagsActive,
    });
  } catch (err) {
    // Never dead air, never a thrown error to the handler (acceptance §9.4). err.name only.
    logger.error(`[WS-AG-CORE] agent turn failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
    if (write) {
      write(`data: ${JSON.stringify({ type: 'text', content: AGENT_ERROR_COPY, session_id: sessionId })}\n\n`);
      write(`data: ${JSON.stringify({ type: 'scheduling_notice', reason: 'agent_error', session_id: sessionId })}\n\n`);
    }
    stopReasonSequence.push('error');
    emitAgentTurnSummary(deps, {
      tenantId,
      sessionId,
      iterations: toolCallCount,
      stopReasonSequence,
      overflow: false,
      modelId,
      flagsActive,
    });
  }
}

module.exports = {
  agentTurn,
  isAgentTurnEnabled,
  MAX_TOOL_ITERATIONS,
  PROMPT_VERSION,
  // exported for tests + WS-AG-EVAL
  buildStateLine,
  AGENT_NARRATION_RULES,
  OVERFLOW_COPY,
  SUPPRESSION_COPY,
};
