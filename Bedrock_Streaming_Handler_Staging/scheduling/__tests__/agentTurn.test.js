'use strict';

/**
 * WS-AG-CORE — agentTurn (§B17b bounded tool loop) tests.
 *
 * Done-bar coverage (work-order item 7):
 *  - loop terminates on end_turn after 0 tool calls
 *  - loop executes 1 tool call + continues (tool_result re-enters the loop)
 *  - overflow at iteration 3 emits scheduling_notice (+ warm copy; no 4th model call)
 *  - kill-switch env off suppresses entry; kill-switch flag off suppresses entry
 *  - SSE text deltas are forwarded (identical frame shape to the non-agent path)
 *  - get_available_times result re-enters the loop (slots visible to call 2)
 *  - §B17g: serialized log/audit lines for an email-bearing turn never contain '@'
 *  - §B17d: the state line never contains the raw email (pinned 'email: known/unknown')
 *  - §B17f: suppression pre-check runs BEFORE the model call (trip → no model call)
 */

const {
  agentTurn,
  isAgentTurnEnabled,
  MAX_TOOL_ITERATIONS,
  PROMPT_VERSION,
  buildStateLine,
  buildTodayLine,
  resolveAgentTimeZone,
  DEFAULT_AGENT_TIME_ZONE,
  buildAgentMessages,
  MAX_HISTORY_MESSAGES,
  AGENT_NARRATION_RULES,
  OVERFLOW_COPY,
  SUPPRESSION_COPY,
} = require('../agentTurn');

// ── mock Bedrock streaming plumbing ───────────────────────────────────────────────────

function chunk(obj) {
  return { chunk: { bytes: new TextEncoder().encode(JSON.stringify(obj)) } };
}

function streamFrom(events) {
  return {
    body: (async function* gen() {
      for (const e of events) yield e;
    })(),
  };
}

// A plain end-of-turn text response.
function textResponse(text, stopReason = 'end_turn') {
  const words = text.split(' ');
  return streamFrom([
    chunk({ type: 'message_start' }),
    chunk({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }),
    ...words.map((w, i) =>
      chunk({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: (i ? ' ' : '') + w } })
    ),
    chunk({ type: 'content_block_stop', index: 0 }),
    chunk({ type: 'message_delta', delta: { stop_reason: stopReason } }),
    chunk({ type: 'message_stop' }),
  ]);
}

// A response that narrates briefly then requests a tool (input streamed as split JSON).
function toolUseResponse({ text = 'One moment…', id = 'tu_1', name, input = {} }) {
  const json = JSON.stringify(input);
  const splitAt = Math.min(5, json.length);
  return streamFrom([
    chunk({ type: 'message_start' }),
    chunk({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }),
    chunk({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text } }),
    chunk({ type: 'content_block_stop', index: 0 }),
    chunk({ type: 'content_block_start', index: 1, content_block: { type: 'tool_use', id, name, input: {} } }),
    chunk({ type: 'content_block_delta', index: 1, delta: { type: 'input_json_delta', partial_json: json.slice(0, splitAt) } }),
    chunk({ type: 'content_block_delta', index: 1, delta: { type: 'input_json_delta', partial_json: json.slice(splitAt) } }),
    chunk({ type: 'content_block_stop', index: 1 }),
    chunk({ type: 'message_delta', delta: { stop_reason: 'tool_use' } }),
    chunk({ type: 'message_stop' }),
  ]);
}

function fakeBedrock(responses) {
  const send = jest.fn();
  for (const r of responses) send.mockResolvedValueOnce(r);
  return { send };
}

// ── fixtures ──────────────────────────────────────────────────────────────────────────

const SLOT = Object.freeze({
  slotId: 's1',
  start: '2026-06-19T19:00:00Z',
  end: '2026-06-19T19:30:00Z',
  label: 'Fri, Jun 19 · 2:00 PM',
  candidateResourceIds: ['maya@org.example'],
});

const PROPOSE_OK = Object.freeze({ outcome: 'ok', slots: [SLOT], poolSize: 1 });

const CONFIG = Object.freeze({
  tenant_id: 'TEN',
  model_id: 'anthropic.claude-haiku-4-5',
  tone_prompt: 'You are the Org assistant.',
  feature_flags: {
    scheduling_enabled: true,
    AGENTIC_SCHEDULING: true,
  },
  scheduling: { appointment_types: { apt_intro: { timezone: 'America/Chicago' } } },
});

const QCTX = Object.freeze({ appointmentTypeId: 'apt_intro', userTimeZone: 'America/Chicago' });

function frames(write) {
  return write.mock.calls
    .map(([s]) => s)
    .filter((s) => typeof s === 'string' && s.startsWith('data: {'))
    .map((s) => JSON.parse(s.slice('data: '.length)));
}

function makeDeps({ bedrock, audit = [], logLines = [] } = {}) {
  const log = (line) => logLines.push(String(line));
  return {
    bedrock,
    env: {}, // isolate from the real process.env
    invokeProposal: jest.fn().mockResolvedValue(PROPOSE_OK),
    saveState: jest.fn().mockResolvedValue(undefined),
    qualifyingContext: QCTX,
    auditLog: (evt) => audit.push(evt),
    logger: { info: log, warn: log, error: log },
  };
}

function baseTurn({ deps, write, userText = 'anything next week?', sessionRow, config = CONFIG, history = [] } = {}) {
  return {
    event: { userText, conversationHistory: history },
    context: {},
    sessionRow: sessionRow === undefined
      ? { tenantId: 'TEN', session_id: 'sess-1', state: 'qualifying' }
      : sessionRow,
    tenantConfig: config,
    deps,
    streamWriter: write,
  };
}

// ── isAgentTurnEnabled (§B17h) ────────────────────────────────────────────────────────

describe('isAgentTurnEnabled (§B17h kill switches)', () => {
  const ON = { feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: true } };

  test('true when scheduling_enabled + AGENTIC_SCHEDULING and no env override', () => {
    expect(isAgentTurnEnabled({ env: {}, tenantConfig: ON })).toBe(true);
  });

  test('global env override beats everything (checked first)', () => {
    expect(isAgentTurnEnabled({ env: { AGENTIC_SCHEDULING_DISABLED: 'true' }, tenantConfig: ON })).toBe(false);
  });

  test('AGENTIC_SCHEDULING absent / false / truthy-but-not-true → false (fail-closed)', () => {
    expect(isAgentTurnEnabled({ env: {}, tenantConfig: { feature_flags: { scheduling_enabled: true } } })).toBe(false);
    expect(isAgentTurnEnabled({ env: {}, tenantConfig: { feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: false } } })).toBe(false);
    expect(isAgentTurnEnabled({ env: {}, tenantConfig: { feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: 'true' } } })).toBe(false);
  });

  test('scheduling_enabled off → false (the §B17a premise)', () => {
    expect(isAgentTurnEnabled({ env: {}, tenantConfig: { feature_flags: { AGENTIC_SCHEDULING: true } } })).toBe(false);
  });

  test('missing config → false', () => {
    expect(isAgentTurnEnabled({ env: {} })).toBe(false);
  });
});

// ── §B17d state line ──────────────────────────────────────────────────────────────────

describe('buildStateLine (§B17d — pinned wording)', () => {
  test('no session → none/none/unknown', () => {
    expect(buildStateLine(null)).toBe('[scheduling state: none | staged slot: none | email: unknown]');
  });

  test('proposing, nothing staged, no email', () => {
    expect(buildStateLine({ state: 'proposing' })).toBe(
      '[scheduling state: proposing | staged slot: none | email: unknown]'
    );
  });

  test('confirming with staged slot (label resolved from candidates) and a known email', () => {
    const line = buildStateLine({
      state: 'confirming',
      candidate_slots: [SLOT],
      selected_slot: { slotId: 's1', start: SLOT.start, end: SLOT.end },
      attendee_email: 'vol@example.com',
    });
    expect(line).toBe(`[scheduling state: confirming | staged slot: ${SLOT.label} (s1) | email: known]`);
  });

  test('PII RULE: the raw email NEVER appears — segment is exactly known/unknown', () => {
    const line = buildStateLine({ state: 'confirming', attendee_email: 'vol@example.com' });
    expect(line).not.toContain('@');
    expect(line).not.toContain('vol@example.com');
    expect(line).toContain('email: known');
    expect(buildStateLine({ state: 'confirming' })).toContain('email: unknown');
  });

  test('staged slot with no resolvable label falls back to the bare slotId', () => {
    const line = buildStateLine({
      state: 'confirming',
      selected_slot: { slotId: 's9' }, // not in candidates; no label persisted
    });
    expect(line).toContain('staged slot: (s9)');
  });
});

// ── the bounded loop (§B17b) ──────────────────────────────────────────────────────────

describe('agentTurn — §B17b loop', () => {
  test('MAX_TOOL_ITERATIONS is the §B17b constant', () => {
    expect(MAX_TOOL_ITERATIONS).toBe(3);
  });

  test('terminates on end_turn after 0 tool calls; SSE text deltas identical to the non-agent path', async () => {
    const bedrock = fakeBedrock([textResponse('Happy to help with that')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).toHaveBeenCalledTimes(1);
    const f = frames(write);
    // nudge frame precedes the first delta (same as the non-agent path)
    expect(f[0]).toEqual({ type: 'stream_start' });
    expect(f[1]).toEqual({ type: 'text', content: 'Happy', session_id: 'sess-1' });
    expect(f.filter((x) => x.type === 'text').map((x) => x.content).join('')).toBe('Happy to help with that');

    const summary = audit.find((e) => e.event_type === 'agent_turn_summary');
    expect(summary).toEqual({
      event_type: 'agent_turn_summary',
      tenant_id: 'TEN',
      session_id: 'sess-1',
      iterations: 0,
      stop_reason_sequence: ['end_turn'],
      overflow: false,
      prompt_version: expect.any(String),
      model_id: 'anthropic.claude-haiku-4-5',
      flags_active: ['scheduling_enabled', 'AGENTIC_SCHEDULING'],
    });
    expect(audit.filter((e) => e.event_type === 'agent_tool_call')).toHaveLength(0);
  });

  test('executes 1 tool call and continues — get_available_times result re-enters the loop', async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ name: 'get_available_times', input: { date: '2026-06-19' } }),
      textResponse('Here are the real openings'),
    ]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).toHaveBeenCalledTimes(2);
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);

    // the tool's UI SSE event was emitted mid-turn
    const slotEvents = frames(write).filter((f) => f.type === 'scheduling_slots');
    expect(slotEvents).toEqual([{ type: 'scheduling_slots', slots: [SLOT], session_id: 'sess-1' }]);

    // call 2 received assistant tool_use + user tool_result (the loop re-entered)
    const body2 = JSON.parse(bedrock.send.mock.calls[1][0].input.body);
    expect(body2.messages).toHaveLength(3);
    expect(body2.messages[1].role).toBe('assistant');
    expect(body2.messages[1].content.find((b) => b.type === 'tool_use')).toEqual({
      type: 'tool_use',
      id: 'tu_1',
      name: 'get_available_times',
      input: { date: '2026-06-19' },
    });
    expect(body2.messages[2].role).toBe('user');
    const toolResult = body2.messages[2].content[0];
    expect(toolResult.type).toBe('tool_result');
    expect(toolResult.tool_use_id).toBe('tu_1');
    const parsed = JSON.parse(toolResult.content);
    expect(parsed.slots).toEqual([{ slot_id: 's1', label: SLOT.label, starts_at_iso: SLOT.start }]);
    expect(parsed.user_time_zone).toBe('America/Chicago');

    // §B17g agent_tool_call — EXACT shape (allowlist is exhaustive; no extra fields)
    const toolCalls = audit.filter((e) => e.event_type === 'agent_tool_call');
    expect(toolCalls).toEqual([
      {
        event_type: 'agent_tool_call',
        tenant_id: 'TEN',
        session_id: 'sess-1',
        tool: 'get_available_times',
        outcome: 'ok',
        latency_ms: expect.any(Number),
        iteration: 1,
        email_present: false,
        date: '2026-06-19',
      },
    ]);

    const summary = audit.find((e) => e.event_type === 'agent_turn_summary');
    expect(summary.iterations).toBe(1);
    expect(summary.stop_reason_sequence).toEqual(['tool_use', 'end_turn']);
    expect(summary.overflow).toBe(false);
  });

  test('system prompt = persona + §B17e block + §B17d state line; tools attached; user text is the message', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      sessionRow: { tenantId: 'TEN', session_id: 'sess-1', state: 'proposing', attendee_email: 'vol@example.com' },
    }));

    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).toContain('You are the Org assistant.');
    expect(body.system).toContain('SCHEDULING AGENT RULES');
    expect(body.system).toContain('never say you lack scheduling access');
    expect(body.system).toContain('[scheduling state: proposing | staged slot: none | email: known]');
    expect(body.system).not.toContain('vol@example.com'); // §B17d PII rule
    expect(body.tools.map((t) => t.name)).toEqual(['get_available_times', 'request_booking_confirmation']);
    expect(body.messages).toEqual([{ role: 'user', content: 'anything next week?' }]);
    expect(body.anthropic_version).toBe('bedrock-2023-05-31');
  });

  test('overflow: tool_use on all 3 iterations → warm copy + scheduling_notice(agent_overflow); NO 4th model call', async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ id: 'tu_1', name: 'get_available_times', input: {} }),
      toolUseResponse({ id: 'tu_2', name: 'get_available_times', input: { exclude_slot_ids: ['s1'] } }),
      toolUseResponse({ id: 'tu_3', name: 'get_available_times', input: {} }),
    ]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).toHaveBeenCalledTimes(3); // never a 4th call

    const f = frames(write);
    expect(f).toContainEqual({ type: 'text', content: OVERFLOW_COPY, session_id: 'sess-1' });
    expect(f).toContainEqual({ type: 'scheduling_notice', notice: 'agent_overflow', session_id: 'sess-1' });

    const summary = audit.find((e) => e.event_type === 'agent_turn_summary');
    expect(summary.overflow).toBe(true);
    expect(summary.stop_reason_sequence).toEqual(['tool_use', 'tool_use', 'tool_use']);
    expect(summary.iterations).toBe(3); // §B17b verbatim loop: each requested tool within budget executes
  });

  test('caps tool_use blocks executed per iteration to 2 (3 parallel requests → only first 2 run)', async () => {
    // One model response carrying THREE parallel tool_use blocks.
    const bedrock = fakeBedrock([
      streamFrom([
        chunk({ type: 'message_start' }),
        chunk({ type: 'content_block_start', index: 0, content_block: { type: 'tool_use', id: 'tu_1', name: 'get_available_times', input: {} } }),
        chunk({ type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: '{}' } }),
        chunk({ type: 'content_block_stop', index: 0 }),
        chunk({ type: 'content_block_start', index: 1, content_block: { type: 'tool_use', id: 'tu_2', name: 'get_available_times', input: {} } }),
        chunk({ type: 'content_block_delta', index: 1, delta: { type: 'input_json_delta', partial_json: '{}' } }),
        chunk({ type: 'content_block_stop', index: 1 }),
        chunk({ type: 'content_block_start', index: 2, content_block: { type: 'tool_use', id: 'tu_3', name: 'get_available_times', input: {} } }),
        chunk({ type: 'content_block_delta', index: 2, delta: { type: 'input_json_delta', partial_json: '{}' } }),
        chunk({ type: 'content_block_stop', index: 2 }),
        chunk({ type: 'message_delta', delta: { stop_reason: 'tool_use' } }),
        chunk({ type: 'message_stop' }),
      ]),
      textResponse('Here are the openings'),
    ]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    // only the first 2 tool_use blocks were executed (cap = catalog size)
    expect(deps.invokeProposal).toHaveBeenCalledTimes(2);
    expect(audit.filter((e) => e.event_type === 'agent_tool_call')).toHaveLength(2);
    const body2 = JSON.parse(bedrock.send.mock.calls[1][0].input.body);
    expect(body2.messages[2].content.map((r) => r.tool_use_id)).toEqual(['tu_1', 'tu_2']);
    expect(audit.find((e) => e.event_type === 'agent_turn_summary').iterations).toBe(2);
  });

  test('kill-switch: env AGENTIC_SCHEDULING_DISABLED suppresses entry (no model call, no SSE, no audit)', async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    deps.env = { AGENTIC_SCHEDULING_DISABLED: 'true' };
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).not.toHaveBeenCalled();
    expect(write).not.toHaveBeenCalled();
    expect(audit).toHaveLength(0);
  });

  test('kill-switch: per-tenant AGENTIC_SCHEDULING off suppresses entry', async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();
    const config = { ...CONFIG, feature_flags: { scheduling_enabled: true } }; // flag absent

    await agentTurn(baseTurn({ deps, write, config }));

    expect(bedrock.send).not.toHaveBeenCalled();
    expect(write).not.toHaveBeenCalled();
    expect(audit).toHaveLength(0);
  });

  test('model/stream error → honest copy + scheduling_notice (no dead air, nothing thrown)', async () => {
    const bedrock = { send: jest.fn().mockRejectedValue(new Error('stream blew up')) };
    const audit = [];
    const logLines = [];
    const deps = makeDeps({ bedrock, audit, logLines });
    const write = jest.fn();

    await expect(agentTurn(baseTurn({ deps, write }))).resolves.toBeUndefined();

    const f = frames(write);
    expect(f.some((x) => x.type === 'text')).toBe(true);
    expect(f).toContainEqual({ type: 'scheduling_notice', notice: 'agent_error', session_id: 'sess-1' });
    const summary = audit.find((e) => e.event_type === 'agent_turn_summary');
    expect(summary.stop_reason_sequence).toContain('error');
    // err.name-only logging — the error MESSAGE never reaches the logs
    expect(logLines.join('\n')).not.toContain('stream blew up');
  });
});

// ── §B17f suppression pre-check ───────────────────────────────────────────────────────

describe('agentTurn — §B17f suppression pre-check (before the model call)', () => {
  test('crisis language trips: NO model call; warm human-contact copy + crisis resources; gate audit', async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();
    const config = {
      ...CONFIG,
      scheduling: { ...CONFIG.scheduling, crisis_resources: 'You can reach the 988 Lifeline any time.' },
    };

    await agentTurn(baseTurn({ deps, write, config, userText: "I've been thinking about suicide" }));

    expect(bedrock.send).not.toHaveBeenCalled(); // pre-check runs BEFORE the model call
    expect(deps.invokeProposal).not.toHaveBeenCalled();

    const f = frames(write);
    const text = f.filter((x) => x.type === 'text').map((x) => x.content).join('');
    expect(text).toContain(SUPPRESSION_COPY);
    expect(text).toContain('988 Lifeline'); // tenant-configured crisis resources
    expect(f.some((x) => x.type === 'scheduling_slots')).toBe(false); // no slots on a tripped turn

    expect(audit.find((e) => e.event_type === 'suggestion_gate_decision')).toEqual({
      event_type: 'suggestion_gate_decision',
      tenant_id: 'TEN',
      session_id: 'sess-1',
      offered: false,
      reason_codes: ['suppression_tripped'],
      suppression_category: 'self_harm_suicide', // category CODE — never raw matched text
    });
    const summary = audit.find((e) => e.event_type === 'agent_turn_summary');
    expect(summary.iterations).toBe(0);
  });

  test('full-session window: crisis EARLIER in the session trips a later innocuous turn (sticky)', async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const deps = makeDeps({ bedrock });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      userText: 'anything next week?',
      history: [
        { role: 'user', content: 'my mom just died last month' },
        { role: 'assistant', content: 'I am so sorry.' },
      ],
    }));

    expect(bedrock.send).not.toHaveBeenCalled();
  });

  test("live-eval A8 phrasing ('thinking about hurting myself') trips BEFORE the model — matcher-gap regression", async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      userText: "honestly I've been thinking about hurting myself lately",
      sessionRow: { tenantId: 'TEN', session_id: 'sess-1', state: 'proposing', candidate_slots: [SLOT] },
    }));

    expect(bedrock.send).not.toHaveBeenCalled(); // pre-check fires before ANY model call
    const text = frames(write).filter((x) => x.type === 'text').map((x) => x.content).join('');
    expect(text).toContain(SUPPRESSION_COPY);
    expect(audit.find((e) => e.event_type === 'suggestion_gate_decision')).toMatchObject({
      offered: false,
      reason_codes: ['suppression_tripped'],
      suppression_category: 'self_harm_suicide', // category CODE — never the matched text
    });
    expect(audit.find((e) => e.event_type === 'agent_turn_summary').iterations).toBe(0);
  });

  test('a persisted session latch is honored (suppression_latched on the row)', async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      sessionRow: {
        tenantId: 'TEN',
        session_id: 'sess-1',
        state: 'proposing',
        suppression_latched: true,
        suppression_category: 'grief_death',
      },
    }));

    expect(bedrock.send).not.toHaveBeenCalled();
    expect(audit.find((e) => e.event_type === 'suggestion_gate_decision').suppression_category).toBe('grief_death');
  });
});

// ── §B17g PII discipline — the '@'-free-logs assertion ────────────────────────────────

describe('agentTurn — §B17g logs never carry the email', () => {
  test("an email-bearing staging turn: every serialized audit/log line is '@'-free; audit shape is the exact allowlist", async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({
        name: 'request_booking_confirmation',
        input: { slot_id: 's1', attendee_email: 'vol@example.com' },
      }),
      textResponse('Staged — nothing is booked until you press Confirm.'),
    ]);
    const audit = [];
    const logLines = [];
    const deps = makeDeps({ bedrock, audit, logLines });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      userText: 'use vol@example.com please',
      history: [{ role: 'user', content: 'hi' }],
      sessionRow: {
        tenantId: 'TEN',
        session_id: 'sess-1',
        state: 'proposing',
        candidate_slots: [SLOT],
        proposal: { poolSize: 1 },
        rejected_slot_ids: [],
      },
    }));

    // staging actually happened (the SSE confirm card is UI, not a log — it carries the email)
    expect(frames(write).some((f) => f.type === 'scheduling_confirm')).toBe(true);

    // §B17g: serialized audit events never contain '@' (no raw email, no email hash)
    for (const evt of audit) {
      expect(JSON.stringify(evt)).not.toContain('@');
    }
    // and the module's own log lines never contain '@'
    for (const line of logLines) {
      expect(line).not.toContain('@');
    }

    // exact allowlist shape for the staging call (email_present boolean — NEVER the email)
    expect(audit.filter((e) => e.event_type === 'agent_tool_call')).toEqual([
      {
        event_type: 'agent_tool_call',
        tenant_id: 'TEN',
        session_id: 'sess-1',
        tool: 'request_booking_confirmation',
        outcome: 'staged',
        latency_ms: expect.any(Number),
        iteration: 1,
        email_present: true,
        slot_id: 's1',
      },
    ]);
  });

  test('a rejected hallucinated email logs outcome invalid_email — still no @ anywhere', async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({
        name: 'request_booking_confirmation',
        input: { slot_id: 's1', attendee_email: 'invented@model.example' },
      }),
      textResponse('Could you share your email?'),
    ]);
    const audit = [];
    const logLines = [];
    const deps = makeDeps({ bedrock, audit, logLines });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      userText: 'book the 2pm', // user never typed an email
      sessionRow: {
        tenantId: 'TEN',
        session_id: 'sess-1',
        state: 'proposing',
        candidate_slots: [SLOT],
      },
    }));

    const toolCall = audit.find((e) => e.event_type === 'agent_tool_call');
    expect(toolCall.outcome).toBe('invalid_email');
    expect(toolCall.email_present).toBe(true);
    expect(JSON.stringify(audit)).not.toContain('@');
    expect(logLines.join('\n')).not.toContain('@');
    // no staging happened
    expect(frames(write).some((f) => f.type === 'scheduling_confirm')).toBe(false);
    expect(deps.saveState).not.toHaveBeenCalled();
  });
});

// ── failure modes of the tool-execution wrapper ───────────────────────────────────────

describe('agentTurn — tool-execution failure modes', () => {
  test('an executor throw (e.g. saveState blows up) → lookup_failed tool_result; the loop continues; err.name-only logging', async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ name: 'get_available_times', input: {} }),
      textResponse('Sorry — the lookup failed just now.'),
    ]);
    const audit = [];
    const logLines = [];
    const deps = makeDeps({ bedrock, audit, logLines });
    deps.saveState = jest.fn().mockRejectedValue(new Error('DDB exploded with vol@example.com in the message'));
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).toHaveBeenCalledTimes(2); // loop continued past the failure
    const body2 = JSON.parse(bedrock.send.mock.calls[1][0].input.body);
    const toolResult = JSON.parse(body2.messages[2].content[0].content);
    expect(toolResult.error).toBe('lookup_failed');

    const toolCall = audit.find((e) => e.event_type === 'agent_tool_call');
    expect(toolCall.outcome).toBe('lookup_failed');
    // err.name only — the throw's message (which carried an email) never reaches logs
    expect(logLines.join('\n')).not.toContain('@');
    expect(logLines.join('\n')).not.toContain('DDB exploded');
  });

  test('an unknown tool name from the model is refused with lookup_failed (no executor runs)', async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ name: 'delete_all_bookings', input: { really: true } }),
      textResponse('I cannot do that.'),
    ]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(deps.saveState).not.toHaveBeenCalled();
    const toolCall = audit.find((e) => e.event_type === 'agent_tool_call');
    expect(toolCall.tool).toBe('unknown'); // §B17g: audit tool field clamped to the catalog
    expect(toolCall.outcome).toBe('lookup_failed');
  });

  test("a pathological 5000-char tool name is clamped: audit tool === 'unknown'; raw name never serialized", async () => {
    const hugeName = 'x'.repeat(5000);
    const bedrock = fakeBedrock([
      toolUseResponse({ name: hugeName, input: {} }),
      textResponse('I cannot do that.'),
    ]);
    const audit = [];
    const logLines = [];
    const deps = makeDeps({ bedrock, audit, logLines });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    const toolCall = audit.find((e) => e.event_type === 'agent_tool_call');
    expect(toolCall.tool).toBe('unknown');
    expect(toolCall.outcome).toBe('lookup_failed');
    // the raw model-supplied name is absent from EVERY serialized audit event and log line
    expect(JSON.stringify(audit)).not.toContain(hugeName);
    expect(logLines.join('\n')).not.toContain(hugeName);
  });

  test('malformed (unparseable) streamed tool-input JSON degrades to {} → executors fail it closed', async () => {
    // Hand-build a stream whose input_json_delta is broken JSON.
    const bedrock = fakeBedrock([
      streamFrom([
        chunk({ type: 'message_start' }),
        chunk({ type: 'content_block_start', index: 0, content_block: { type: 'tool_use', id: 'tu_x', name: 'request_booking_confirmation', input: {} } }),
        chunk({ type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: '{"slot_id": "s1", "attendee_em' } }),
        chunk({ type: 'content_block_stop', index: 0 }),
        chunk({ type: 'message_delta', delta: { stop_reason: 'tool_use' } }),
        chunk({ type: 'message_stop' }),
      ]),
      textResponse('Hmm, let me try again differently.'),
    ]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      sessionRow: { tenantId: 'TEN', session_id: 'sess-1', state: 'proposing', candidate_slots: [SLOT] },
    }));

    // {} args → no slot_id → unknown_slot; nothing staged, nothing written
    const toolCall = audit.find((e) => e.event_type === 'agent_tool_call');
    expect(toolCall.outcome).toBe('unknown_slot');
    expect(deps.saveState).not.toHaveBeenCalled();
    expect(frames(write).some((f) => f.type === 'scheduling_confirm')).toBe(false);
  });

  test('defensive: stop_reason tool_use with NO tool_use block → stops without overflow copy', async () => {
    const bedrock = fakeBedrock([textResponse('just text', 'tool_use')]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).toHaveBeenCalledTimes(1);
    const f = frames(write);
    expect(f.some((x) => x.type === 'scheduling_notice')).toBe(false);
    expect(f.some((x) => x.content === OVERFLOW_COPY)).toBe(false);
    expect(audit.find((e) => e.event_type === 'agent_turn_summary').overflow).toBe(false);
  });

  test('a throwing deps.auditLog is contained (audit emit is non-fatal; err.name only)', async () => {
    const bedrock = fakeBedrock([textResponse('fine')]);
    const logLines = [];
    const log = (line) => logLines.push(String(line));
    const deps = makeDeps({ bedrock });
    deps.auditLog = () => { throw new Error('audit sink down — vol@example.com'); };
    deps.logger = { info: log, warn: log, error: log };
    const write = jest.fn();

    await expect(agentTurn(baseTurn({ deps, write }))).resolves.toBeUndefined();

    expect(bedrock.send).toHaveBeenCalledTimes(1); // turn completed despite the audit failure
    expect(logLines.join('\n')).toContain('audit emit failed');
    expect(logLines.join('\n')).not.toContain('@'); // err.name only — the message never logs
  });

  test("default audit writer (no deps.auditLog): structured JSON console lines, still '@'-free", async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ name: 'request_booking_confirmation', input: { slot_id: 's1', attendee_email: 'vol@example.com' } }),
      textResponse('Staged.'),
    ]);
    const logLines = [];
    const log = (line) => logLines.push(String(line));
    const deps = makeDeps({ bedrock });
    delete deps.auditLog; // exercise the default console-JSON path (the prod path)
    deps.logger = { info: log, warn: log, error: log };
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      userText: 'use vol@example.com please',
      sessionRow: { tenantId: 'TEN', session_id: 'sess-1', state: 'proposing', candidate_slots: [SLOT] },
    }));

    const auditLines = logLines.filter((l) => l.startsWith('{'));
    expect(auditLines.length).toBeGreaterThanOrEqual(2); // agent_tool_call + agent_turn_summary
    const events = auditLines.map((l) => JSON.parse(l));
    expect(events.some((e) => e.event_type === 'agent_tool_call' && e.outcome === 'staged')).toBe(true);
    expect(events.some((e) => e.event_type === 'agent_turn_summary')).toBe(true);
    for (const line of logLines) {
      expect(line).not.toContain('@'); // §B17g on the DEFAULT path too
    }
  });
});

// ── live-eval fix-loop (2026-06-12): F1 history · F2 KB · F4 separator ─────────────────

describe('agentTurn — F1 (eval A9): conversation history threads into the model call', () => {
  test('bare-email turn at proposing: prior turns ride along; current text is the last user message', async () => {
    const bedrock = fakeBedrock([textResponse('Got it')]);
    const deps = makeDeps({ bedrock });
    const write = jest.fn();

    await agentTurn(baseTurn({
      deps,
      write,
      userText: 'chris+x@myrecruiter.ai',
      history: [
        { role: 'user', content: 'the 10am works for me' },
        { role: 'assistant', content: 'Great — what email should the calendar invite go to?' },
      ],
      sessionRow: { tenantId: 'TEN', session_id: 'sess-1', state: 'proposing', candidate_slots: [SLOT] },
    }));

    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.messages).toEqual([
      { role: 'user', content: 'the 10am works for me' },
      { role: 'assistant', content: 'Great — what email should the calendar invite go to?' },
      { role: 'user', content: 'chris+x@myrecruiter.ai' },
    ]);
  });

  test('buildAgentMessages: caps at MAX_HISTORY_MESSAGES (newest win); strict alternation; opens with user', () => {
    const many = Array.from({ length: 30 }, (_, i) => ({
      role: i % 2 ? 'assistant' : 'user',
      content: `m${i}`,
    }));
    const capped = buildAgentMessages(many, 'now');
    const joined = JSON.stringify(capped);
    expect(joined).not.toContain('"m17"'); // older than the cap window (30 - 12 = m18+ kept)
    expect(joined).toContain('m18');
    expect(capped[0].role).toBe('user');
    expect(capped[capped.length - 1]).toEqual({ role: 'user', content: 'now' });
    for (let i = 1; i < capped.length; i++) {
      expect(capped[i].role).not.toBe(capped[i - 1].role); // the API rejects non-alternation
    }
    expect(MAX_HISTORY_MESSAGES).toBe(12);
  });

  test('buildAgentMessages: merges consecutive same-role turns; tolerates the {text} shape; drops a leading assistant turn', () => {
    expect(
      buildAgentMessages(
        [
          { role: 'assistant', content: 'welcome!' }, // leading assistant → dropped
          { role: 'user', content: 'first' },
          { role: 'user', text: 'second' }, // {text} shape + same-role merge
          { role: 'assistant', content: 'reply' },
        ],
        'third'
      )
    ).toEqual([
      { role: 'user', content: 'first\nsecond' },
      { role: 'assistant', content: 'reply' },
      { role: 'user', content: 'third' },
    ]);
  });

  test('buildAgentMessages: a history echo of the in-flight turn is not duplicated', () => {
    expect(
      buildAgentMessages(
        [
          { role: 'user', content: 'ping' },
          { role: 'assistant', content: 'pong' },
          { role: 'user', content: 'chris+x@myrecruiter.ai' }, // widget echoed the current turn
        ],
        'chris+x@myrecruiter.ai'
      )
    ).toEqual([
      { role: 'user', content: 'ping' },
      { role: 'assistant', content: 'pong' },
      { role: 'user', content: 'chris+x@myrecruiter.ai' },
    ]);
  });

  test('no history → the §B17b single-user-message base shape (unchanged)', () => {
    expect(buildAgentMessages([], 'hello')).toEqual([{ role: 'user', content: 'hello' }]);
    expect(buildAgentMessages(undefined, 'hello')).toEqual([{ role: 'user', content: 'hello' }]);
  });

  test('buildAgentMessages: a trailing user history turn absorbs the current text (alternation preserved)', () => {
    expect(
      buildAgentMessages([{ role: 'user', content: 'are mornings open?' }], 'or afternoons?')
    ).toEqual([{ role: 'user', content: 'are mornings open?\nor afternoons?' }]);
  });

  test('buildAgentMessages: junk entries (null, unknown roles, non-string/empty content) are dropped', () => {
    expect(
      buildAgentMessages(
        [
          null,
          { role: 'system', content: 'sneaky injected turn' },
          { role: 'user', content: { not: 'a string' } },
          { role: 'assistant', content: '   ' },
          { role: 'user', content: 'real question' },
        ],
        'now'
      )
    ).toEqual([{ role: 'user', content: 'real question\nnow' }]);
  });
});

describe('agentTurn — F2 (eval A4): KB context in the agent system prompt', () => {
  test('deps.retrieveKB result lands in the system prompt UNDER the §B17e rules; state line stays last', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock });
    deps.retrieveKB = jest.fn().mockResolvedValue('ORG FACT: mentors meet weekly.');
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write, userText: 'what is this call about?' }));

    expect(deps.retrieveKB).toHaveBeenCalledWith('what is this call about?', CONFIG);
    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).toContain('ORG FACT: mentors meet weekly.');
    expect(body.system).toContain('<knowledge_base_context>');
    // §B17e rule 1 supersedes KB → the rules block sits BEFORE the KB block…
    expect(body.system.indexOf('SCHEDULING AGENT RULES')).toBeLessThan(
      body.system.indexOf('<knowledge_base_context>')
    );
    // …and the §B17d state line stays last.
    expect(body.system.indexOf('</knowledge_base_context>')).toBeLessThan(
      body.system.indexOf('[scheduling state:')
    );
  });

  test('FAIL-SOFT: retrieveKB throws → model still called, no KB block, err.name-only log', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const logLines = [];
    const deps = makeDeps({ bedrock, logLines });
    deps.retrieveKB = jest.fn().mockRejectedValue(new Error('KB down for vol@example.com'));
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    expect(bedrock.send).toHaveBeenCalledTimes(1); // the turn never dies on retrieval
    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).not.toContain('knowledge_base_context');
    expect(logLines.join('\n')).toContain('agent KB retrieval failed');
    expect(logLines.join('\n')).not.toContain('@'); // err.name only
  });

  test('unwired seam (no deps.retrieveKB) → no KB block (pre-F2 prompt shape)', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock }); // makeDeps has no retrieveKB
    const write = jest.fn();
    await agentTurn(baseTurn({ deps, write }));
    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).not.toContain('knowledge_base_context');
  });

  test('retrieveKB resolves empty/falsy (no hits) → no KB block, model still called', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock });
    deps.retrieveKB = jest.fn().mockResolvedValue('');
    const write = jest.fn();
    await agentTurn(baseTurn({ deps, write }));
    expect(bedrock.send).toHaveBeenCalledTimes(1);
    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).not.toContain('knowledge_base_context');
  });
});

describe('agentTurn — F4: separator between iteration text segments', () => {
  test("text in iteration 1 then text in iteration 2 → one '\\n\\n' separator frame between (never concatenated)", async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ text: 'Let me check that for you.', name: 'get_available_times', input: {} }),
      textResponse('Here are the real openings'),
    ]);
    const deps = makeDeps({ bedrock });
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    const texts = frames(write).filter((f) => f.type === 'text').map((f) => f.content);
    expect(texts.join('')).toContain('for you.\n\nHere are');
    expect(texts.filter((t) => t === '\n\n')).toHaveLength(1); // exactly one separator
  });

  test('single-iteration turn → no separator frame', async () => {
    const bedrock = fakeBedrock([textResponse('plain answer')]);
    const deps = makeDeps({ bedrock });
    const write = jest.fn();
    await agentTurn(baseTurn({ deps, write }));
    const texts = frames(write).filter((f) => f.type === 'text').map((f) => f.content);
    expect(texts).not.toContain('\n\n');
  });
});

describe('agentTurn — F3/F5 prompt rules (live-eval G1/A7 + A2/A3)', () => {
  test("F3: the 'I don't have access' construction is banned; approved existing-appointment phrasing present", () => {
    expect(AGENT_NARRATION_RULES).toContain('BANNED');
    expect(AGENT_NARRATION_RULES).toContain(
      'I can\'t see or change existing appointments — but our team can; want me to get you their contact, or set up a NEW time?'
    );
  });

  test('F5: alternatives rule (date + exclude_slot_ids; never affirm unreceived availability) + day-part from starts_at_iso', () => {
    expect(AGENT_NARRATION_RULES).toContain('exclude_slot_ids');
    expect(AGENT_NARRATION_RULES).toContain('never affirm availability the tool did not return');
    expect(AGENT_NARRATION_RULES).toContain("starts_at_iso");
    expect(AGENT_NARRATION_RULES).toContain('never describe a morning time as afternoon');
  });

  test('rule 15 (chips carry the times): never enumerate individual times; summarize + ONE closing question', () => {
    expect(AGENT_NARRATION_RULES).toContain('render as tappable buttons');
    expect(AGENT_NARRATION_RULES).toContain('NEVER enumerate individual times in your text');
    expect(AGENT_NARRATION_RULES).toContain('ask ONE closing question');
  });

  test('PROMPT_VERSION bumped for the rules change (§B17g)', () => {
    expect(PROMPT_VERSION).toBe('b17e.v4');
  });
});

// ── date awareness (live-eval A1/A13): today-line + the rule-14 date-resolution rule ───

describe('agentTurn — date awareness (today-line in the appointment timezone)', () => {
  // Friday, June 12, 2026 12:00 PM in America/Chicago (17:00 UTC).
  const NOON_CDT_FRI_JUN_12 = Date.parse('2026-06-12T17:00:00Z');

  test('system prompt carries the today-line, formatted in the appointment TZ via the injectable clock (deps.nowMs)', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock }); // qualifyingContext: America/Chicago
    deps.nowMs = NOON_CDT_FRI_JUN_12;
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).toContain('[today: Friday, June 12, 2026 — timezone: America/Chicago]');
    // placed with the §B17d state line (immediately above it)
    const todayIdx = body.system.indexOf('[today:');
    const stateIdx = body.system.indexOf('[scheduling state:');
    expect(todayIdx).toBeGreaterThan(-1);
    expect(todayIdx).toBeLessThan(stateIdx);
    expect(body.system.slice(todayIdx, stateIdx)).toBe(
      '[today: Friday, June 12, 2026 — timezone: America/Chicago]\n'
    );
  });

  test('appointment-TZ date wins at a UTC date boundary (late Chicago evening = next day UTC)', async () => {
    // 2026-06-13T03:30:00Z is still Friday, June 12, 10:30 PM in America/Chicago.
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock });
    deps.nowMs = Date.parse('2026-06-13T03:30:00Z');
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write }));

    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).toContain('[today: Friday, June 12, 2026 — timezone: America/Chicago]');
    expect(body.system).not.toContain('June 13');
  });

  test('resolveAgentTimeZone: appt-type tz > qctx user_time_zone > America/Chicago (resolver UTC default treated as unresolved)', () => {
    // 1. appointment-type timezone wins (incl. snake_case)
    expect(resolveAgentTimeZone({
      deps: { qualifyingContext: { appointment_type: { timezone: 'America/New_York' }, userTimeZone: 'America/Denver' } },
    })).toBe('America/New_York');
    expect(resolveAgentTimeZone({
      deps: { qualifyingContext: { appointment_type: { time_zone: 'America/New_York' } } },
    })).toBe('America/New_York');
    // 2. qctx user_time_zone next (camel or snake)
    expect(resolveAgentTimeZone({ deps: { qualifyingContext: { userTimeZone: 'America/Denver' } } })).toBe('America/Denver');
    expect(resolveAgentTimeZone({ deps: { qualifyingContext: { user_time_zone: 'America/Denver' } } })).toBe('America/Denver');
    // 3. unwired qctx seam → shipped resolver over the tenant config (sole appt type)
    expect(resolveAgentTimeZone({ tenantConfig: CONFIG, deps: {} })).toBe('America/Chicago');
    expect(resolveAgentTimeZone({
      tenantConfig: { scheduling: { appointment_types: { a: { timezone: 'America/Los_Angeles' } } } },
      deps: {},
    })).toBe('America/Los_Angeles');
    // 4. nothing configured: the resolver's 'UTC' DEFAULT is unresolved → platform home zone
    expect(resolveAgentTimeZone({ tenantConfig: {}, deps: {} })).toBe(DEFAULT_AGENT_TIME_ZONE);
    expect(resolveAgentTimeZone({})).toBe(DEFAULT_AGENT_TIME_ZONE);
    // 5. an appointment type EXPLICITLY configured to UTC is honored
    expect(resolveAgentTimeZone({
      deps: { qualifyingContext: { appointment_type: { timezone: 'UTC' } } },
    })).toBe('UTC');
    // 6. a THROWING resolver path degrades to the default — never out of the turn
    expect(resolveAgentTimeZone({
      tenantConfig: { get scheduling() { throw new Error('hostile config'); } },
      deps: {},
    })).toBe(DEFAULT_AGENT_TIME_ZONE);
    expect(DEFAULT_AGENT_TIME_ZONE).toBe('America/Chicago');
  });

  test('buildTodayLine: pinned format; an invalid configured tz falls back to the default instead of throwing', () => {
    expect(buildTodayLine(NOON_CDT_FRI_JUN_12, 'America/Chicago')).toBe(
      '[today: Friday, June 12, 2026 — timezone: America/Chicago]'
    );
    // 03:30 UTC Sat Jun 13 renders as Fri Jun 12 in Chicago but Sat Jun 13 in UTC
    expect(buildTodayLine(Date.parse('2026-06-13T03:30:00Z'), 'UTC')).toBe(
      '[today: Saturday, June 13, 2026 — timezone: UTC]'
    );
    expect(buildTodayLine(NOON_CDT_FRI_JUN_12, 'Not/AZone')).toBe(
      '[today: Friday, June 12, 2026 — timezone: America/Chicago]'
    );
    // falsy tz → default (resolveAgentTimeZone never returns falsy; defensive)
    expect(buildTodayLine(NOON_CDT_FRI_JUN_12, undefined)).toBe(
      '[today: Friday, June 12, 2026 — timezone: America/Chicago]'
    );
  });

  test('garbage injected clock (non-numeric deps.nowMs) falls back to the real clock — the turn never throws', async () => {
    const bedrock = fakeBedrock([textResponse('ok')]);
    const deps = makeDeps({ bedrock });
    deps.nowMs = 'not-a-clock'; // Invalid-Date bait — runs OUTSIDE the loop's try
    const write = jest.fn();

    await expect(agentTurn(baseTurn({ deps, write }))).resolves.toBeUndefined();

    expect(bedrock.send).toHaveBeenCalledTimes(1); // turn completed
    const body = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body.system).toMatch(/\[today: [A-Z][a-z]+, [A-Z][a-z]+ \d{1,2}, \d{4} — timezone: America\/Chicago\]/);
  });

  test('rule 14 (date resolution) carries the locked wording; banned undated-day-conclusion phrasing present', () => {
    expect(AGENT_NARRATION_RULES).toContain("resolve it to YYYY-MM-DD using today's date and PASS the `date` argument");
    expect(AGENT_NARRATION_RULES).toContain('check each day with separate tool calls (max 2 per turn)');
    expect(AGENT_NARRATION_RULES).toContain(
      'Without a date argument the tool only returns the earliest few openings'
    );
    expect(AGENT_NARRATION_RULES).toContain(
      'never conclude a specific future day is unavailable unless you queried THAT day'
    );
  });

  test('scripted-Bedrock: named-day ask → model passes date; dated tool result flows back through the loop', async () => {
    const bedrock = fakeBedrock([
      toolUseResponse({ name: 'get_available_times', input: { date: '2026-06-15' } }),
      textResponse('Monday June 15 has a 2:00 PM open — want it?'),
    ]);
    const audit = [];
    const deps = makeDeps({ bedrock, audit });
    deps.nowMs = NOON_CDT_FRI_JUN_12;
    const write = jest.fn();

    await agentTurn(baseTurn({ deps, write, userText: 'do you have something Monday of next week?' }));

    // the model HAD the anchor (today-line) on the call that produced the dated tool use
    const body1 = JSON.parse(bedrock.send.mock.calls[0][0].input.body);
    expect(body1.system).toContain('[today: Friday, June 12, 2026 — timezone: America/Chicago]');
    // the date arg became the propose route's date_window for THAT day
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
    expect(deps.invokeProposal.mock.calls[0][0].date_window).toEqual({
      start: '2026-06-15T00:00:00.000Z',
      end: '2026-06-16T00:00:00.000Z',
    });
    // the dated tool result re-entered the loop
    const body2 = JSON.parse(bedrock.send.mock.calls[1][0].input.body);
    const toolResult = JSON.parse(body2.messages[2].content[0].content);
    expect(toolResult.slots).toEqual([{ slot_id: 's1', label: SLOT.label, starts_at_iso: SLOT.start }]);
    // §B17g audit carries the date arg
    expect(audit.find((e) => e.event_type === 'agent_tool_call').date).toBe('2026-06-15');
  });
});

// ── misc interface guards ─────────────────────────────────────────────────────────────

describe('agentTurn — interface guards', () => {
  test('missing userText → no-op (no model call)', async () => {
    const bedrock = fakeBedrock([textResponse('never')]);
    const deps = makeDeps({ bedrock });
    const write = jest.fn();
    await agentTurn(baseTurn({ deps, write, userText: '' }));
    expect(bedrock.send).not.toHaveBeenCalled();
  });

  test('missing bedrock client → no-op, nothing thrown', async () => {
    const deps = makeDeps({ bedrock: undefined });
    delete deps.bedrock;
    const write = jest.fn();
    await expect(agentTurn(baseTurn({ deps, write }))).resolves.toBeUndefined();
  });

  test('§B17e narration rules carry the locked wording', () => {
    expect(AGENT_NARRATION_RULES).toContain('never say you lack scheduling access, never invent times');
    expect(AGENT_NARRATION_RULES).toContain('nothing is booked until they press Confirm');
    expect(AGENT_NARRATION_RULES).toContain('Never repeat the user\'s email back');
    expect(AGENT_NARRATION_RULES).toContain('the MEETING is with a human, the scheduler is an AI assistant');
  });
});
