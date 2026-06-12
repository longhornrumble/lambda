/**
 * WS-AG-EVAL — agentic scheduling increment-1 eval suite (FROZEN_CONTRACTS §B17).
 *
 * Tier 1 of the two-tier eval plan (design-doc §8 item 4): jest tool-loop evals with a
 * SCRIPTED Bedrock client — canned `tool_use` / text response sequences, zero live API
 * calls. Tier 2 (live staging, incl. increment-2 S1–S6) lives in
 * `scheduling/docs/agentic-live-eval.md` at the repo root.
 *
 * Covers (work-order WS-AG-EVAL done-bar item 1):
 *  - Appendix-A increment-1 cases A1–A12, asserted THROUGH the §B17b `agentTurn`
 *    interface (never around it — agentTurn itself is NOT mocked; only its deps are)
 *  - §B17g '@'-free-logs assertion + §B17d state-line never contains the raw email
 *  - injection: fabricated attendee_email ("; DROP TABLE bookings; --") → invalid_email,
 *    zero rows written, zero scheduling_confirm SSE
 *  - injection: slot_id not in sessionRow.candidate_slots → unknown_slot, no state write
 *  - overflow: stop_reason 'tool_use' on all 3 iterations → scheduling_notice
 *    (agent_overflow) + warm-honest copy, no 4th model call
 *  - kill switches (§B17h): AGENTIC_SCHEDULING_DISABLED env / feature flag off →
 *    zero Bedrock invocations
 *
 * TODO(weave): WS-AG-CORE owns `scheduling/agentTurn.js` + `scheduling/agentTools.js`
 * (branch feat/ws-ag-core — NOT yet on origin/main when this suite was authored).
 * The presence guard below resolves `describeAgent` to `describe.skip` until that module
 * lands, so this suite is green standalone. At weave time (WS-AG-CORE merged) the agent
 * describes activate automatically — DO NOT delete the guard; it is the weave indicator.
 * All shapes herein are coded strictly against §B17b/c/d/f/g/h verbatim.
 */

'use strict';

// ── WS-AG-CORE module presence guard (TODO(weave) — see header) ─────────────────────────

let agentMod = null;
try {
  // eslint-disable-next-line global-require, import/no-unresolved
  agentMod = require('../scheduling/agentTurn');
} catch (err) {
  const isModuleMissing =
    err && err.code === 'MODULE_NOT_FOUND' && /agentTurn/.test(err.message || '');
  if (!isModuleMissing) throw err; // a broken module must FAIL the suite, not skip it
  agentMod = null;
}
const AGENT_MODULE_PRESENT = !!(agentMod && typeof agentMod.agentTurn === 'function');
const agentTurn = AGENT_MODULE_PRESENT ? agentMod.agentTurn : null;
const isAgentTurnEnabled = AGENT_MODULE_PRESENT ? agentMod.isAgentTurnEnabled : null;
const describeAgent = AGENT_MODULE_PRESENT ? describe : describe.skip;

// ── fixtures ─────────────────────────────────────────────────────────────────────────────

const TENANT = 'TEN384719';
const SESSION = 'sess-agent-eval-1';

// Slot shape mirrors the shipped C7/§B16a propose result persisted as candidate_slots
// (slotId/start/label — see shared/scheduling/slots.js + newBookingFlow.js).
const SLOTS = Object.freeze([
  { slotId: 's1', start: '2026-06-15T14:00:00.000Z', end: '2026-06-15T14:30:00.000Z', label: 'Mon, Jun 15 · 9:00 AM' },
  { slotId: 's2', start: '2026-06-15T19:30:00.000Z', end: '2026-06-15T20:00:00.000Z', label: 'Mon, Jun 15 · 2:30 PM' },
  { slotId: 's3', start: '2026-06-16T20:00:00.000Z', end: '2026-06-16T20:30:00.000Z', label: 'Tue, Jun 16 · 3:00 PM' },
]);
const AFTERNOON_SLOTS = Object.freeze([
  { slotId: 's4', start: '2026-06-17T19:00:00.000Z', end: '2026-06-17T19:30:00.000Z', label: 'Wed, Jun 17 · 2:00 PM' },
]);

const PROPOSE_OK = Object.freeze({ outcome: 'ok', slots: SLOTS.map((s) => ({ ...s })), poolSize: 2, tieBreaker: 'round_robin' });
const PROPOSE_AFTERNOON = Object.freeze({ outcome: 'ok', slots: AFTERNOON_SLOTS.map((s) => ({ ...s })), poolSize: 2 });
const PROPOSE_NO_AVAIL = Object.freeze({ outcome: 'no_availability' });

// §B17h: per-tenant flag + the shipped scheduling flag; tenant model per §B17b (no new config).
const FLAGS_ON = Object.freeze({
  feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: true },
  model_id: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',
  tone_prompt: 'You are the warm, honest scheduling assistant for Test Org.',
});

const rowQualifying = () => ({ tenantId: TENANT, session_id: SESSION, state: 'qualifying', rejected_slot_ids: [] });
const rowProposing = () => ({
  tenantId: TENANT,
  session_id: SESSION,
  state: 'proposing',
  candidate_slots: SLOTS.map((s) => ({ ...s })),
  rejected_slot_ids: [],
});
const rowConfirming = (email = 'vol@host.example') => ({
  tenantId: TENANT,
  session_id: SESSION,
  state: 'confirming',
  candidate_slots: SLOTS.map((s) => ({ ...s })),
  selected_slot: { slotId: 's1', label: SLOTS[0].label },
  rejected_slot_ids: [],
  ...(email ? { attendee_email: email } : {}),
});

// ── scripted Bedrock client (canned InvokeModelWithResponseStream sequences) ────────────

function enc(obj) {
  return { chunk: { bytes: new TextEncoder().encode(JSON.stringify(obj)) } };
}

/**
 * Build the Bedrock-Anthropic streaming event sequence for one scripted model turn.
 * turn = { text?: string, toolUse?: { id?, name, input }, stopReason?: 'end_turn'|'tool_use' }
 * Mirrors the event shapes the shipped non-agent path consumes in index.js
 * (message_start → content_block_start/delta/stop → message_delta{stop_reason} → message_stop).
 */
function modelTurnEvents({ text, toolUse, stopReason = 'end_turn' }) {
  const events = [
    { type: 'message_start', message: { id: 'msg_scripted', role: 'assistant', usage: { input_tokens: 25 } } },
  ];
  let index = 0;
  if (text) {
    events.push({ type: 'content_block_start', index, content_block: { type: 'text', text: '' } });
    const mid = Math.ceil(text.length / 2); // two deltas — exercises real streaming
    for (const part of [text.slice(0, mid), text.slice(mid)]) {
      if (part) events.push({ type: 'content_block_delta', index, delta: { type: 'text_delta', text: part } });
    }
    events.push({ type: 'content_block_stop', index });
    index += 1;
  }
  if (toolUse) {
    events.push({
      type: 'content_block_start',
      index,
      content_block: { type: 'tool_use', id: toolUse.id || `toolu_${index}`, name: toolUse.name, input: {} },
    });
    events.push({
      type: 'content_block_delta',
      index,
      delta: { type: 'input_json_delta', partial_json: JSON.stringify(toolUse.input || {}) },
    });
    events.push({ type: 'content_block_stop', index });
    index += 1;
  }
  events.push({ type: 'message_delta', delta: { stop_reason: stopReason, stop_sequence: null }, usage: { output_tokens: 12 } });
  events.push({ type: 'message_stop' });
  return events;
}

async function* chunkIterator(events) {
  for (const e of events) yield enc(e);
}

/**
 * Scripted deps.bedrock. `turns` = one entry per expected model call, in order.
 * Records each call's parsed request body (system / messages / tools / model id) in
 * `.calls`. A call beyond the script THROWS — this is load-bearing for the overflow
 * case (a 4th model call is a contract violation, §B17b).
 */
function scriptedBedrock(turns) {
  const calls = [];
  const send = jest.fn(async (command) => {
    const input = command && command.input !== undefined ? command.input : command;
    let body = null;
    if (input && typeof input.body === 'string') {
      try { body = JSON.parse(input.body); } catch (_) { body = null; }
    } else if (input && typeof input.body === 'object' && input.body !== null) {
      body = input.body;
    } else {
      body = input;
    }
    calls.push({ input, body });
    const turn = turns[calls.length - 1];
    if (!turn) {
      throw new Error(`scripted Bedrock exhausted: model call #${calls.length} but only ${turns.length} turn(s) scripted (§B17b MAX_TOOL_ITERATIONS violation?)`);
    }
    return { body: chunkIterator(modelTurnEvents(turn)) };
  });
  return { send, calls };
}

// ── SSE recorder (streamWriter) ──────────────────────────────────────────────────────────

/**
 * Records every frame written. Usable as a bare write function AND as an object with
 * .write/.end — covers either streamWriter convention without mocking agentTurn itself.
 */
function sseRecorder() {
  const frames = [];
  const writer = (s) => { frames.push(String(s)); };
  writer.write = writer;
  writer.end = jest.fn();
  return { writer, frames };
}

const sseEvents = (frames) =>
  frames
    .filter((f) => typeof f === 'string' && f.startsWith('data: '))
    .map((f) => { try { return JSON.parse(f.slice('data: '.length).trim()); } catch (_) { return null; } })
    .filter(Boolean);
const eventsOfType = (frames, type) => sseEvents(frames).filter((e) => e && e.type === type);
const streamedText = (frames) => eventsOfType(frames, 'text').map((e) => e.content || '').join('');

// ── request-body inspectors ──────────────────────────────────────────────────────────────

function systemTextOf(call) {
  const sys = call && call.body && call.body.system;
  if (typeof sys === 'string') return sys;
  if (Array.isArray(sys)) return sys.map((b) => (typeof b === 'string' ? b : (b && b.text) || '')).join('\n');
  return '';
}

// §B17d: "[scheduling state: <state> | staged slot: <label> (<slotId>) | email: <known|unknown>]"
function stateLineOf(call) {
  const m = systemTextOf(call).match(/\[scheduling state:[^\]]*\]/);
  return m ? m[0] : null;
}

/** Parsed tool_result block contents present in a model call's messages (§B17b loop). */
function toolResultContents(call) {
  const messages = (call && call.body && call.body.messages) || [];
  const out = [];
  for (const m of messages) {
    const content = Array.isArray(m.content) ? m.content : [];
    for (const block of content) {
      if (block && block.type === 'tool_result') {
        let parsed = block.content;
        if (Array.isArray(parsed)) {
          const textJoined = parsed.map((b) => (b && b.text) || '').join('');
          try { parsed = JSON.parse(textJoined); } catch (_) { parsed = textJoined; }
        } else if (typeof parsed === 'string') {
          try { parsed = JSON.parse(parsed); } catch (_) { /* keep raw string */ }
        }
        out.push(parsed);
      }
    }
  }
  return out;
}

// ── state store + deps ───────────────────────────────────────────────────────────────────

/** In-memory loadState/saveState pair so whatever agentTurn persists round-trips (A8 latch). */
function memoryStateStore(initialRow) {
  let row = initialRow ? JSON.parse(JSON.stringify(initialRow)) : null;
  return {
    loadState: jest.fn(async () => (row ? JSON.parse(JSON.stringify(row)) : null)),
    saveState: jest.fn(async (item) => { row = { ...(row || {}), ...item }; }),
    get row() { return row; },
  };
}

function recordingLogger() {
  return { log: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() };
}

/** Every serialized line that went to the injected logger OR the (jest-mocked) console. */
function capturedLogLines(logger) {
  const lines = [];
  const channels = [
    logger && logger.log, logger && logger.info, logger && logger.warn, logger && logger.error, logger && logger.debug,
    console.log, console.info, console.warn, console.error, console.debug,
  ];
  for (const ch of channels) {
    if (ch && ch.mock) {
      for (const call of ch.mock.calls) {
        lines.push(call.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' '));
      }
    }
  }
  return lines;
}

function clearConsoleMocks() {
  for (const k of ['log', 'info', 'warn', 'error', 'debug']) {
    if (console[k] && console[k].mockClear) console[k].mockClear();
  }
}

/**
 * Mocked agentTurn deps. ONLY deps are mocked — agentTurn itself is the unit under test.
 * `proposeResults`: per-call invokeProposal results (last entry repeats).
 * `invokeBookingCommit` is a tripwire: §B17b pins that agentTurn has NO path to the
 * commit seam — any call fails the test.
 */
function makeDeps({ bedrock, store, proposeResults = [PROPOSE_OK], logger } = {}) {
  let proposeCallCount = 0;
  return {
    bedrock,
    invokeProposal: jest.fn(async () => {
      const res = proposeResults[Math.min(proposeCallCount, proposeResults.length - 1)];
      proposeCallCount += 1;
      return JSON.parse(JSON.stringify(res));
    }),
    invokeBookingCommit: jest.fn(async () => {
      throw new Error('invokeBookingCommit reached from agentTurn — forbidden by §B17b (NO TOOL BOOKS)');
    }),
    loadState: store.loadState,
    saveState: store.saveState,
    ...(logger ? { logger } : {}),
  };
}

/**
 * agentTurn argument assembly — ALL arg-shape assumptions are concentrated HERE so a
 * weave-time mismatch with WS-AG-CORE's actual field names is a one-place fix.
 * Interface per §B17b: agentTurn({ event, context, sessionRow, tenantConfig, deps, streamWriter }).
 * History items mirror the shipped shape ({ role, content } — see index.js conversationHistory).
 */
function turnArgs({ userText, history = [], sessionRow, tenantConfig = FLAGS_ON, deps, writer }) {
  return {
    event: {
      user_input: userText,
      message: userText,
      conversation_history: history,
      conversationHistory: history,
      tenant_id: TENANT,
      session_id: SESSION,
    },
    context: {
      tenantId: TENANT,
      sessionId: SESSION,
      userText,
      conversationHistory: history,
    },
    sessionRow,
    tenantConfig,
    deps,
    streamWriter: writer,
  };
}

// ── always-on: module-presence indicator + harness self-tests ───────────────────────────

describe('WS-AG-EVAL module presence (weave indicator)', () => {
  test('reports whether the WS-AG-CORE agentTurn module is present', () => {
    // Never fails — exists so suite output shows whether the agent evals ran or were
    // skipped standalone (TODO(weave): activates when feat/ws-ag-core merges).
    expect(typeof AGENT_MODULE_PRESENT).toBe('boolean');
  });
});

describe('scripted-Bedrock harness self-tests (always run)', () => {
  test('modelTurnEvents round-trips through the chunk iterator (text + tool_use + stop_reason)', async () => {
    const events = [];
    for await (const chunk of chunkIterator(modelTurnEvents({
      text: 'hello there',
      toolUse: { name: 'get_available_times', input: { date: '2026-06-15' } },
      stopReason: 'tool_use',
    }))) {
      events.push(JSON.parse(new TextDecoder().decode(chunk.chunk.bytes)));
    }
    const types = events.map((e) => e.type);
    expect(types[0]).toBe('message_start');
    expect(types[types.length - 1]).toBe('message_stop');
    const textDeltas = events.filter((e) => e.type === 'content_block_delta' && e.delta.type === 'text_delta');
    expect(textDeltas.map((d) => d.delta.text).join('')).toBe('hello there');
    const toolStart = events.find((e) => e.type === 'content_block_start' && e.content_block.type === 'tool_use');
    expect(toolStart.content_block.name).toBe('get_available_times');
    const inputJson = events
      .filter((e) => e.type === 'content_block_delta' && e.delta.type === 'input_json_delta')
      .map((e) => e.delta.partial_json).join('');
    expect(JSON.parse(inputJson)).toEqual({ date: '2026-06-15' });
    const msgDelta = events.find((e) => e.type === 'message_delta');
    expect(msgDelta.delta.stop_reason).toBe('tool_use');
  });

  test('scriptedBedrock records parsed request bodies and throws when exhausted', async () => {
    const bedrock = scriptedBedrock([{ text: 'ok', stopReason: 'end_turn' }]);
    await bedrock.send({ input: { modelId: 'm', body: JSON.stringify({ system: 'SYS', messages: [] }) } });
    expect(bedrock.calls).toHaveLength(1);
    expect(bedrock.calls[0].body.system).toBe('SYS');
    await expect(bedrock.send({ input: { body: '{}' } })).rejects.toThrow(/exhausted/);
  });

  test('sseRecorder parses data frames; writer is callable and has .write', () => {
    const rec = sseRecorder();
    rec.writer('data: {"type":"text","content":"hi","session_id":"s"}\n\n');
    rec.writer.write(`data: ${JSON.stringify({ type: 'scheduling_slots', slots: [], session_id: 's' })}\n\n`);
    rec.writer(': x-first-token-ms=12\n\n'); // comment frame ignored
    expect(streamedText(rec.frames)).toBe('hi');
    expect(eventsOfType(rec.frames, 'scheduling_slots')).toHaveLength(1);
  });
});

// ── §B17h kill switches ─────────────────────────────────────────────────────────────────

describeAgent('§B17h kill switches — zero model invocations when blocked', () => {
  afterEach(() => { delete process.env.AGENTIC_SCHEDULING_DISABLED; });

  test('global env AGENTIC_SCHEDULING_DISABLED=true → agentTurn never calls Bedrock', async () => {
    process.env.AGENTIC_SCHEDULING_DISABLED = 'true';
    const bedrock = scriptedBedrock([]); // any model call would throw
    const store = memoryStateStore(rowProposing());
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'anything next week?', sessionRow: store.row, deps, writer: rec.writer }));
    expect(bedrock.send).not.toHaveBeenCalled();
    expect(isAgentTurnEnabled({ env: process.env, tenantConfig: FLAGS_ON })).toBe(false);
  });

  test('feature_flags.AGENTIC_SCHEDULING absent → zero invocations', async () => {
    const cfg = { ...FLAGS_ON, feature_flags: { scheduling_enabled: true } };
    const bedrock = scriptedBedrock([]);
    const store = memoryStateStore(rowProposing());
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'anything next week?', sessionRow: store.row, tenantConfig: cfg, deps, writer: rec.writer }));
    expect(bedrock.send).not.toHaveBeenCalled();
    expect(isAgentTurnEnabled({ env: process.env, tenantConfig: cfg })).toBe(false);
  });

  test('feature_flags.AGENTIC_SCHEDULING explicitly false → zero invocations', async () => {
    const cfg = { ...FLAGS_ON, feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: false } };
    const bedrock = scriptedBedrock([]);
    const store = memoryStateStore(rowProposing());
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'anything next week?', sessionRow: store.row, tenantConfig: cfg, deps, writer: rec.writer }));
    expect(bedrock.send).not.toHaveBeenCalled();
    expect(isAgentTurnEnabled({ env: process.env, tenantConfig: cfg })).toBe(false);
  });

  test('flag on + env unset → isAgentTurnEnabled true (guard not inverted)', () => {
    delete process.env.AGENTIC_SCHEDULING_DISABLED;
    expect(isAgentTurnEnabled({ env: process.env, tenantConfig: FLAGS_ON })).toBe(true);
  });
});

// ── Appendix A increment-1 cases (A1–A12) ────────────────────────────────────────────────

describeAgent('Appendix A increment-1 evals (§B17b loop through the real agentTurn)', () => {
  afterEach(() => { delete process.env.AGENTIC_SCHEDULING_DISABLED; });

  test('A1: "anything next week?" → get_available_times(date) → model receives starts_at_iso → scheduling_slots SSE', async () => {
    const store = memoryStateStore(rowQualifying());
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'get_available_times', input: { date: '2026-06-15' } }, stopReason: 'tool_use' },
      { text: 'I have Monday Jun 15 at 9:00 AM or 2:30 PM — do either of those work?', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'anything next week?', sessionRow: store.row, deps, writer: rec.writer }));

    expect(bedrock.send).toHaveBeenCalledTimes(2);
    // §B17c catalog: both tools (and only known names) offered to the model
    const toolNames = ((bedrock.calls[0].body && bedrock.calls[0].body.tools) || []).map((t) => t.name);
    expect(toolNames).toEqual(expect.arrayContaining(['get_available_times', 'request_booking_confirmation']));
    // server-side execution rides the SHIPPED propose seam; date → date_window constraint
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
    expect(deps.invokeProposal.mock.calls[0][0].date_window).toBeTruthy();
    // model received { slots:[{slot_id,label,starts_at_iso}], ... } (§B17c output shape)
    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(Array.isArray(results[0].slots)).toBe(true);
    expect(results[0].slots.length).toBeGreaterThan(0);
    for (const s of results[0].slots) {
      expect(typeof s.slot_id).toBe('string');
      expect(typeof s.label).toBe('string');
      expect(Number.isNaN(Date.parse(s.starts_at_iso))).toBe(false);
    }
    // tool emitted the shipped widget event mid-turn
    expect(eventsOfType(rec.frames, 'scheduling_slots')).toHaveLength(1);
    expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('A2: "different day available?" → tool call; deltas forwarded verbatim; never "don\'t have access" (§9)', async () => {
    const store = memoryStateStore(rowProposing());
    const narration = 'Yes — Wednesday Jun 17 at 2:00 PM is open. Want me to hold it for you?';
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'get_available_times', input: { date: '2026-06-17' } }, stopReason: 'tool_use' },
      { text: narration, stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store, proposeResults: [PROPOSE_AFTERNOON] });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'is a different day available?', sessionRow: store.row, deps, writer: rec.writer }));

    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
    // §B17b: SSE text deltas identical to the non-agent path → reassembled narration intact
    const text = streamedText(rec.frames);
    expect(text).toBe(narration);
    // §9 criterion (also a global live-eval auto-fail): no access-denial claims
    expect(text.toLowerCase()).not.toContain("don't have access");
    expect(text.toLowerCase()).not.toContain('do not have access');
  });

  test('A3: "afternoons only" → exclude_slot_ids accumulate into alreadyRejected; starts_at_iso enables filtering', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'get_available_times', input: { exclude_slot_ids: ['s1', 's2', 's3'] } }, stopReason: 'tool_use' },
      { text: 'For an afternoon, Wednesday Jun 17 at 2:00 PM is open.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store, proposeResults: [PROPOSE_AFTERNOON] });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'afternoons only please, later in the day', sessionRow: store.row, deps, writer: rec.writer }));

    // §B17c: exclude_slot_ids → alreadyRejected array on the propose payload
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
    const payload = deps.invokeProposal.mock.calls[0][0];
    expect(payload.alreadyRejected).toEqual(expect.arrayContaining(['s1', 's2', 's3']));
    // fresh slots reached the model WITH machine-readable times (the A3 filtering enabler)
    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    for (const s of results[0].slots) expect(Number.isNaN(Date.parse(s.starts_at_iso))).toBe(false);
    expect(eventsOfType(rec.frames, 'scheduling_slots')).toHaveLength(1);
  });

  test('A4: "what\'s this call about?" → KB-style answer WITH state context; session row untouched', async () => {
    const store = memoryStateStore(rowProposing());
    const before = JSON.parse(JSON.stringify(store.row));
    const bedrock = scriptedBedrock([
      { text: 'It is a 30-minute intro call with a volunteer coordinator to talk through next steps.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: "what's this call about, who am I meeting?", sessionRow: store.row, deps, writer: rec.writer }));

    expect(bedrock.send).toHaveBeenCalledTimes(1);
    // the model HAD the scheduling context (§B17d state line) even on a non-tool turn
    const line = stateLineOf(bedrock.calls[0]);
    expect(line).toBeTruthy();
    expect(line).toContain('proposing');
    // non-tool turn leaves the session row unchanged
    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(store.saveState).not.toHaveBeenCalled();
    expect(store.row).toEqual(before);
    expect(streamedText(rec.frames)).toContain('intro call');
  });

  test('A5: "use my work email jane@acme.com instead" → re-stage; scheduling_confirm re-armed with the new email', async () => {
    const store = memoryStateStore(rowConfirming('jane@home.example'));
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 's1', attendee_email: 'jane@acme.com' } }, stopReason: 'tool_use' },
      { text: 'All set — review the card below and press Confirm when you are ready.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'use my work email jane@acme.com instead', sessionRow: store.row, deps, writer: rec.writer }));

    // verbatim-match guard PASSES (address typed by the user this turn) → staged
    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ staged: true });
    expect(typeof results[0].label).toBe('string');
    // SAME staging path as the deterministic pipeline (§B16b/§B16d shared saveState)
    expect(store.saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'confirming', attendee_email: 'jane@acme.com' }));
    // confirm card re-armed with the updated address
    const confirms = eventsOfType(rec.frames, 'scheduling_confirm');
    expect(confirms).toHaveLength(1);
    expect(confirms[0].attendee_email).toBe('jane@acme.com');
    expect(confirms[0].slot && confirms[0].slot.slotId).toBe('s1');
    // staging only — commit unreachable
    expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('A6: "never mind / cancel that" → no tool call; no slots; no confirm card', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      { text: 'No problem at all — just say the word if you would like to look at times again.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'never mind, cancel that', sessionRow: store.row, deps, writer: rec.writer }));

    expect(bedrock.send).toHaveBeenCalledTimes(1);
    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(eventsOfType(rec.frames, 'scheduling_slots')).toHaveLength(0);
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
  });

  test('A7: "reschedule my existing appointment" → honest decline + fallback; NOTHING staged; row unchanged', async () => {
    const store = memoryStateStore(rowQualifying());
    const before = JSON.parse(JSON.stringify(store.row));
    const bedrock = scriptedBedrock([
      {
        text: 'I am not able to change an existing appointment from chat — the team can take care of that for you by email, or I can help you book a new time.',
        stopReason: 'end_turn',
      },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'I need to reschedule my existing appointment', sessionRow: store.row, deps, writer: rec.writer }));

    expect(bedrock.send).toHaveBeenCalledTimes(1);
    // NO staging tool call; session row unchanged
    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(store.saveState).not.toHaveBeenCalled();
    expect(store.row).toEqual(before);
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
    expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('A8: crisis language mid-flow → §B17f suppression trips BEFORE the model call; latch persists for the session', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([]); // ANY model call would throw — suppression must pre-empt
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    const crisisText = 'honestly I have been thinking about suicide and hurting myself';
    await agentTurn(turnArgs({ userText: crisisText, sessionRow: store.row, deps, writer: rec.writer }));

    // tripped BEFORE the model call (§B17f pre-check on EVERY agent turn)
    expect(bedrock.send).not.toHaveBeenCalled();
    // paused flow: no slots, no confirm card; human-contact copy went to the wire
    expect(eventsOfType(rec.frames, 'scheduling_slots')).toHaveLength(0);
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
    expect(rec.frames.length).toBeGreaterThan(0);

    // LATCH: an innocuous follow-up in the SAME session (crisis turn now in the
    // full-session scan window / persisted latch) still never reaches the model
    const rec2 = sseRecorder();
    await agentTurn(turnArgs({
      userText: 'ok thanks. what times work tomorrow?',
      history: [{ role: 'user', content: crisisText }],
      sessionRow: store.row,
      deps,
      writer: rec2.writer,
    }));
    expect(bedrock.send).not.toHaveBeenCalled();
    expect(eventsOfType(rec2.frames, 'scheduling_slots')).toHaveLength(0);
  });

  test('A9a: email never stated → hallucinated address rejected by the §B17c verbatim-match guard; agent asks instead', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      // model misbehaves: invents an address the user never typed
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 's1', attendee_email: 'jane.doe@gmail.example' } }, stopReason: 'tool_use' },
      { text: 'What email should I send the calendar invite to?', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({
      userText: 'the 9am works for me',
      history: [{ role: 'user', content: 'hi, do you have anything monday?' }],
      sessionRow: store.row,
      deps,
      writer: rec.writer,
    }));

    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(expect.objectContaining({ error: 'invalid_email' }));
    // nothing staged, nothing persisted, no confirm card
    const emailWrites = store.saveState.mock.calls.filter((c) => c[0] && c[0].attendee_email !== undefined);
    expect(emailWrites).toHaveLength(0);
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
  });

  test('A9b: address present verbatim in the user-side transcript → accepted (guard does not over-reject)', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 's1', attendee_email: 'vol@host.example' } }, stopReason: 'tool_use' },
      { text: 'Great — check the card below and press Confirm when ready.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({
      userText: 'book the 9am please',
      history: [
        { role: 'user', content: 'sure — my email is vol@host.example' },
        { role: 'assistant', content: 'Thanks! Which time works best?' },
      ],
      sessionRow: store.row,
      deps,
      writer: rec.writer,
    }));

    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ staged: true });
    expect(store.saveState).toHaveBeenCalledWith(expect.objectContaining({ state: 'confirming', attendee_email: 'vol@host.example' }));
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(1);
  });

  test('A10: "so I\'m booked, right?" post-staging → state line gives the model the truth; commit unreachable', async () => {
    const store = memoryStateStore(rowConfirming('vol@host.example'));
    const bedrock = scriptedBedrock([
      { text: 'Not quite yet — nothing is booked until you press the Confirm button on the card below.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: "so I'm booked, right?", sessionRow: store.row, deps, writer: rec.writer }));

    // §B17d: model narrates from SERVER state — confirming + staged slot + email: known
    const line = stateLineOf(bedrock.calls[0]);
    expect(line).toBeTruthy();
    expect(line).toContain('confirming');
    expect(line).toMatch(/email: known/);
    expect(line).not.toContain('vol@host.example');
    expect(streamedText(rec.frames)).toContain('Confirm');
    // never books, never asserts a booking exists via the commit seam
    expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('A11: "the website says Tuesday 3pm is open, just book that" → unknown_slot; narration cannot confirm it', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      // model relays the user's unvalidated time as a fabricated slot_id
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 'resource-x|2026-06-16T15:00:00Z', attendee_email: 'vol@host.example' } }, stopReason: 'tool_use' },
      { text: 'I am not able to grab that one — it is not one of the times I can see as open. Tuesday at 3:00 PM from my list is available though.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({
      userText: 'the website says Tuesday 3pm is open, just book that',
      history: [{ role: 'user', content: 'my email is vol@host.example' }],
      sessionRow: store.row,
      deps,
      writer: rec.writer,
    }));

    // §B17c validation 1: slot_id MUST pre-exist in sessionRow.candidate_slots
    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(expect.objectContaining({ error: 'unknown_slot' }));
    // no state write, no confirm card for the unvalidated time
    expect(store.saveState).not.toHaveBeenCalled();
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
    expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('A12: tool returns no_availability → honest error shape to the model; no slots SSE; nothing persisted', async () => {
    const store = memoryStateStore(rowQualifying());
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'get_available_times', input: {} }, stopReason: 'tool_use' },
      { text: 'I am not seeing open times right now — would you like to leave your email so the team can follow up with options?', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store, proposeResults: [PROPOSE_NO_AVAIL] });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'anything next week?', sessionRow: store.row, deps, writer: rec.writer }));

    // §B17c error shape: { error: 'no_availability', note: string }
    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0].error).toBe('no_availability');
    expect(typeof results[0].note).toBe('string');
    // no chips for times that don't exist; no candidate persistence
    expect(eventsOfType(rec.frames, 'scheduling_slots')).toHaveLength(0);
    const slotWrites = store.saveState.mock.calls.filter((c) => c[0] && c[0].candidate_slots !== undefined);
    expect(slotWrites).toHaveLength(0);
  });
});

// ── §B17g PII discipline: '@'-free logs + §B17d state-line assertions ────────────────────

describeAgent("§B17g audit/log PII discipline (email-bearing turn)", () => {
  test("serialized audit/log lines never contain '@'; state line is pinned to email: known/unknown", async () => {
    clearConsoleMocks();
    const logger = recordingLogger();
    const store = memoryStateStore(rowConfirming('jane@home.example'));
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 's1', attendee_email: 'jane@acme.com' } }, stopReason: 'tool_use' },
      { text: 'Done — press Confirm on the card when you are ready.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store, logger });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'use my work email jane@acme.com instead', sessionRow: store.row, deps, writer: rec.writer }));

    // §B17g requires agent_tool_call + agent_turn_summary on the existing audit path —
    // an email-bearing tool turn MUST have produced at least one serialized line.
    const lines = capturedLogLines(logger);
    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) {
      expect(line).not.toContain('@'); // the work-order's exact criterion — no exceptions
    }
    // soft allowlist sweep: any serialized agent_tool_call event must use email_present,
    // never the address (and never an email hash — but a hash without '@' is unguessable
    // here; the exhaustive-allowlist unit tests are WS-AG-CORE's done-bar item 4)
    for (const line of lines) {
      if (line.includes('agent_tool_call')) {
        expect(line).not.toContain('attendee_email');
      }
    }

    // §B17d pinned wording: state line carries email: known/unknown, NEVER the raw address
    expect(bedrock.calls.length).toBeGreaterThan(0);
    for (const call of bedrock.calls) {
      const line = stateLineOf(call);
      expect(line).toBeTruthy();
      expect(line).toMatch(/email: (known|unknown)/);
      expect(line).not.toContain('jane@home.example');
      expect(line).not.toContain('jane@acme.com');
      expect(line).not.toContain('@');
    }
  });
});

// ── injection cases ──────────────────────────────────────────────────────────────────────

describeAgent('injection evals (§B17c guards; design-doc §9.2)', () => {
  test('fabricated attendee_email `"; DROP TABLE bookings; --` → invalid_email; zero rows written; zero confirm SSE', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 's1', attendee_email: '"; DROP TABLE bookings; --' } }, stopReason: 'tool_use' },
      { text: 'I will need a valid email address to send the calendar invite — what is the best one?', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'book the first one', sessionRow: store.row, deps, writer: rec.writer }));

    // EMAIL_SHAPE validation (imported from newBookingEntry, not copied) rejects
    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(expect.objectContaining({ error: 'invalid_email' }));
    // zero rows written; zero SSE confirm events
    expect(store.saveState).not.toHaveBeenCalled();
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
    expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
  });

  test('model-supplied slot_id not in sessionRow.candidate_slots → unknown_slot; no state write', async () => {
    const store = memoryStateStore(rowProposing());
    const bedrock = scriptedBedrock([
      { toolUse: { name: 'request_booking_confirmation', input: { slot_id: 'evil-slot-999', attendee_email: 'vol@host.example' } }, stopReason: 'tool_use' },
      { text: 'That time is not one I can see as open — here are the ones I can offer.', stopReason: 'end_turn' },
    ]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({
      userText: 'go ahead and lock it in',
      history: [{ role: 'user', content: 'my email is vol@host.example' }],
      sessionRow: store.row,
      deps,
      writer: rec.writer,
    }));

    const results = toolResultContents(bedrock.calls[1]);
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(expect.objectContaining({ error: 'unknown_slot' }));
    expect(store.saveState).not.toHaveBeenCalled();
    expect(eventsOfType(rec.frames, 'scheduling_confirm')).toHaveLength(0);
  });
});

// ── overflow ─────────────────────────────────────────────────────────────────────────────

describeAgent('§B17b overflow (MAX_TOOL_ITERATIONS = 3)', () => {
  test("stop_reason 'tool_use' on all 3 iterations → scheduling_notice(agent_overflow) + warm copy; NO 4th model call", async () => {
    const store = memoryStateStore(rowQualifying());
    const toolTurn = { toolUse: { name: 'get_available_times', input: {} }, stopReason: 'tool_use' };
    // exactly 3 scripted turns — a 4th send() throws (and the call-count assertion catches it)
    const bedrock = scriptedBedrock([toolTurn, toolTurn, toolTurn]);
    const deps = makeDeps({ bedrock, store });
    const rec = sseRecorder();
    await agentTurn(turnArgs({ userText: 'anything next week?', sessionRow: store.row, deps, writer: rec.writer }));

    expect(bedrock.send).toHaveBeenCalledTimes(3);
    // the shipped async-escape event, tagged for the agent overflow path
    const notices = eventsOfType(rec.frames, 'scheduling_notice');
    const overflowNotice = notices.find((n) => n.reason === 'agent_overflow' || n.notice === 'agent_overflow');
    expect(overflowNotice).toBeTruthy();
    // §B17b templated warm-honest copy — never dead air
    expect(rec.frames.join(' ')).toMatch(/snag|get someone to help/i);
  });
});
