/**
 * V5.5 wiring tests — flag-gated single-pass turn through BOTH handler blocks.
 *
 * Drives the real handlers (streaming via the index.test.js mocked-runtime
 * pattern; buffered via the cf_origin_wiring.test.js pattern: unset
 * global.awslambda + jest.resetModules) against a scripted Bedrock stream and
 * pins every V5.5 amendment from the plan's adversarial review:
 *
 *   1. The flag gates the prompt-swap AND the stream-loop parser regardless of
 *      which downstream branch owns CTAs — scheduling-handled and click-routed
 *      turns on a V5 tenant must never leak the sentinel to the widget.
 *   2. responseBuffer holds parser-forwarded (stripped) text — its consumers
 *      (QA_COMPLETE, runSchedulingTurn, enhanceResponse, the fail-soft
 *      selectActionsV4) never see the sentinel.
 *   3. The V5 branch beats V4_ACTION_SELECTOR when a tenant carries both flags.
 *   4. Buffered handler drives the same behavior (its CTA chain had ~zero
 *      coverage — index.test.js's module-scope awslambda pins everything to the
 *      streaming handler).
 *   5. firstTokenTime fires on the first NON-EMPTY parser-forwarded text; the
 *      holdback never emits empty text frames.
 *   6. Empty catalog (no ai_available CTAs) skips the fail-soft ladder AND the
 *      V5_TAIL_STATUS counter (no tail was asked for).
 *   7. Structured logs: V5_TAIL_STATUS {status, trailing_after_close,
 *      tenant_hash, session_id}; QA_COMPLETE stamps single_pass on V5 turns.
 *   8. Flag off ⇒ byte-identical pass-through (sentinel-shaped prose is NOT
 *      stripped; V4 params; no V5 logs; no single_pass stamp).
 */

'use strict';

const { mockClient } = require('aws-sdk-client-mock');
const {
  BedrockRuntimeClient,
  InvokeModelWithResponseStreamCommand,
  InvokeModelCommand,
} = require('@aws-sdk/client-bedrock-runtime');

jest.mock('../../shared/bedrock-core', () => ({
  loadConfig: jest.fn(),
  retrieveKB: jest.fn(),
  sanitizeUserInput: jest.fn((input) => input),
  getCacheKey: jest.fn(),
  isCacheValid: jest.fn(),
  evictOldestCacheEntries: jest.fn(),
  CACHE_TTL: 300000,
  MAX_CACHE_SIZE: 100,
}));
jest.mock('../form_handler', () => ({ handleFormMode: jest.fn() }));
jest.mock('../response_enhancer', () => ({ enhanceResponse: jest.fn() }));
jest.mock('../analytics_writer', () => ({ writeSessionSummary: jest.fn(async () => {}) }));
// Keep isSchedulingEnabled REAL (the gate predicate); stub the I/O-touching calls.
jest.mock('../scheduling/bindingContext', () => {
  const actual = jest.requireActual('../scheduling/bindingContext');
  return { ...actual, injectSchedulingContext: jest.fn(async (p) => p) };
});
jest.mock('../scheduling/schedulingFlow', () => ({
  runSchedulingTurn: jest.fn(async () => ({ handled: false })),
}));

const bedrockMock = mockClient(BedrockRuntimeClient);

// Mock the Lambda streaming global BEFORE requiring index.js — exports.handler
// then resolves to streamifyResponse(streamingHandler).
global.awslambda = {
  streamifyResponse: jest.fn((handler) => async (event, responseStream, context) =>
    handler(event, responseStream, context)),
};

const { loadConfig, retrieveKB } = require('../../shared/bedrock-core');
const { enhanceResponse } = require('../response_enhancer');
const { runSchedulingTurn } = require('../scheduling/schedulingFlow');
const { V5_TURN_PROMPT_VERSION, V5_TURN_INFERENCE_PARAMS } = require('../prompt_v5');
const { V4_STEP2_INFERENCE_PARAMS } = require('../prompt_v4');
const { SENTINEL_OPEN } = require('../streamTail');

const V5_CONFIG = {
  tenant_id: 'TEST123',
  aws: { model_id: 'us.anthropic.claude-haiku-4-5-20251001-v1:0' },
  tone_prompt: 'You are a helpful assistant.',
  feature_flags: { V5_SINGLE_PASS: true },
  cta_definitions: {
    learn_volunteer: { label: 'Volunteer info', action: 'send_query', ai_available: true },
    apply_volunteer: { label: 'Apply', action: 'start_form', ai_available: true },
  },
};

// ── helpers ────────────────────────────────────────────────────────────────────

function bedrockStream(texts) {
  const events = [
    { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'content_block_start' })) } },
    ...texts.map((text) => ({
      chunk: {
        bytes: Buffer.from(JSON.stringify({ type: 'content_block_delta', delta: { type: 'text_delta', text } })),
      },
    })),
    { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'message_stop' })) } },
  ];
  return {
    body: {
      [Symbol.asyncIterator]: async function* () {
        for (const e of events) yield e;
      },
    },
  };
}

function mockResponseStream() {
  const chunks = [];
  return {
    write: jest.fn((data) => chunks.push(data)),
    end: jest.fn(),
    getChunks: () => chunks,
  };
}

/** All SSE JSON events of a given type from written chunks / a body string. */
function sseEvents(raw, type) {
  // Split on single newlines: SSE comment frames (`: x-total-tokens=N\n`) end
  // with ONE newline, so a double-newline split would glue the following
  // `data:` line onto the comment and hide it.
  const text = Array.isArray(raw) ? raw.join('') : raw;
  return text
    .split('\n')
    .filter((l) => l.startsWith('data: '))
    .map((l) => {
      try { return JSON.parse(l.slice(6)); } catch { return null; }
    })
    .filter((e) => e && e.type === type);
}

function joinedText(raw) {
  return sseEvents(raw, 'text').map((e) => e.content).join('');
}

/** First structured console.log line of the given type. */
function findJsonLog(spy, type) {
  for (const call of spy.mock.calls) {
    const arg = call[0];
    if (typeof arg === 'string' && arg.startsWith('{')) {
      try {
        const obj = JSON.parse(arg);
        if (obj.type === type) return obj;
      } catch { /* not JSON */ }
    }
  }
  return null;
}

/** The turn-call request body (prompt + inference params) sent to Bedrock. */
function turnRequest(mock) {
  const calls = mock.commandCalls(InvokeModelWithResponseStreamCommand);
  expect(calls.length).toBeGreaterThanOrEqual(1);
  const body = JSON.parse(calls[0].args[0].input.body);
  return { prompt: body.messages[0].content[0].text, max_tokens: body.max_tokens, temperature: body.temperature };
}

const selectorResponse = (idsJson) => ({
  body: new TextEncoder().encode(JSON.stringify({ content: [{ type: 'text', text: idsJson }] })),
});

// ── streaming handler ──────────────────────────────────────────────────────────

describe('V5.5 wiring — streamingHandler', () => {
  let indexModule;
  let logSpy;

  beforeAll(() => {
    indexModule = require('../index.js');
  });

  beforeEach(() => {
    bedrockMock.reset();
    loadConfig.mockReset();
    retrieveKB.mockReset();
    enhanceResponse.mockReset();
    runSchedulingTurn.mockReset();
    runSchedulingTurn.mockResolvedValue({ handled: false });
    loadConfig.mockResolvedValue(V5_CONFIG);
    retrieveKB.mockResolvedValue('Volunteer orientation is the first Saturday of each month.');
    logSpy = jest.spyOn(console, 'log');
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  async function invokeChat(bodyOverrides = {}) {
    const stream = mockResponseStream();
    const event = {
      body: JSON.stringify({
        tenant_hash: 'abc123',
        session_id: 'sess-v5-test',
        user_input: 'How do I learn about volunteering?',
        ...bodyOverrides,
      }),
    };
    await indexModule.handler(event, stream, { awsRequestId: 'test-req' });
    return stream.getChunks();
  }

  test('strips a chunk-split sentinel, validates ids, emits cta_buttons before [DONE], stamps both logs', async () => {
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Great question! Here is the info.\n', '<<<ACT', 'IONS ["learn_volunteer","ghost_id"', ']>>>'])
    );

    const chunks = await invokeChat();
    const all = chunks.join('');

    // 1. No sentinel text (nor any fragment of it) ever reaches the client.
    expect(all).not.toContain(SENTINEL_OPEN);
    expect(joinedText(chunks)).toBe('Great question! Here is the info.\n');

    // 2. cta_buttons: validated ids only (ghost dropped), V5 routing tier, before [DONE].
    const ctas = sseEvents(chunks, 'cta_buttons');
    expect(ctas).toHaveLength(1);
    expect(ctas[0].ctaButtons.map((c) => c.id)).toEqual(['learn_volunteer']);
    expect(ctas[0].metadata.routing_tier).toBe('v5_single_pass');
    const ctaIdx = chunks.findIndex((c) => c.includes('cta_buttons'));
    const doneIdx = chunks.findIndex((c) => c.includes('data: [DONE]'));
    expect(ctaIdx).toBeGreaterThan(-1);
    expect(ctaIdx).toBeLessThan(doneIdx);

    // 3. The turn call used the V5 prompt + V5 inference params.
    const req = turnRequest(bedrockMock);
    expect(req.prompt).toContain('ACTION TAIL');
    expect(req.prompt).toContain('learn_volunteer — Volunteer info');
    expect(req.max_tokens).toBe(V5_TURN_INFERENCE_PARAMS.max_tokens);
    expect(req.temperature).toBe(V5_TURN_INFERENCE_PARAMS.temperature);

    // 4. Single pass: no selector call.
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(0);

    // 5. Structured logs: tail status + QA_COMPLETE with stripped answer + V5 stamp.
    const tail = findJsonLog(logSpy, 'V5_TAIL_STATUS');
    expect(tail).toMatchObject({
      status: 'actions',
      trailing_after_close: false,
      tenant_hash: 'abc123',
      session_id: 'sess-v5-test',
    });
    const qa = findJsonLog(logSpy, 'QA_COMPLETE');
    expect(qa.answer).not.toContain(SENTINEL_OPEN);
    expect(qa.prompt_versions.single_pass).toBe(V5_TURN_PROMPT_VERSION);
  });

  test('BOTH flags true: the V5 branch wins over V4_ACTION_SELECTOR (no second model call)', async () => {
    loadConfig.mockResolvedValue({
      ...V5_CONFIG,
      feature_flags: { V5_SINGLE_PASS: true, V4_ACTION_SELECTOR: true },
    });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Sure thing.\n<<<ACTIONS ["apply_volunteer"]>>>'])
    );

    const chunks = await invokeChat();
    const ctas = sseEvents(chunks, 'cta_buttons');
    expect(ctas[0].ctaButtons.map((c) => c.id)).toEqual(['apply_volunteer']);
    expect(ctas[0].metadata.routing_tier).toBe('v5_single_pass');
    // V4's selectActionsV4 (an InvokeModelCommand) never ran.
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(0);
  });

  test('restraint tail [] emits NO cta_buttons and NO fallback call', async () => {
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Happy to help!\n<<<ACTIONS []>>>'])
    );

    const chunks = await invokeChat();
    expect(sseEvents(chunks, 'cta_buttons')).toHaveLength(0);
    expect(joinedText(chunks)).toBe('Happy to help!\n');
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(0);
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS').status).toBe('actions');
  });

  test('malformed tail → fail-soft: ONE selectActionsV4 call fed STRIPPED text', async () => {
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Some reply.\n<<<ACTIONS [broken>>>'])
    );
    bedrockMock.on(InvokeModelCommand).resolves(selectorResponse('["learn_volunteer"]'));

    const chunks = await invokeChat();

    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS').status).toBe('malformed');
    // Exactly one rescue call; the malformed marker never reached the client.
    const selectorCalls = bedrockMock.commandCalls(InvokeModelCommand);
    expect(selectorCalls).toHaveLength(1);
    expect(chunks.join('')).not.toContain(SENTINEL_OPEN);
    // The selector prompt embeds responseBuffer — it must be sentinel-free too.
    const selectorPrompt = JSON.parse(selectorCalls[0].args[0].input.body).messages[0].content[0].text;
    expect(selectorPrompt).not.toContain(SENTINEL_OPEN);
    // The rescue's ids are served.
    expect(sseEvents(chunks, 'cta_buttons')[0].ctaButtons.map((c) => c.id)).toEqual(['learn_volunteer']);
  });

  test('missing tail → fail-soft ladder fires with status no_sentinel', async () => {
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['A plain reply with no tail at all.'])
    );
    bedrockMock.on(InvokeModelCommand).resolves(selectorResponse('[]'));

    const chunks = await invokeChat();
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS').status).toBe('no_sentinel');
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(1);
    expect(sseEvents(chunks, 'cta_buttons')).toHaveLength(0);
    expect(joinedText(chunks)).toBe('A plain reply with no tail at all.');
  });

  test('empty catalog: V4-identical prompt, NO tail-status log, NO fallback call (ladder skipped)', async () => {
    loadConfig.mockResolvedValue({
      ...V5_CONFIG,
      cta_definitions: { hidden: { label: 'Hidden', action: 'send_query', ai_available: false } },
    });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Just a normal answer.'])
    );

    const chunks = await invokeChat();

    const req = turnRequest(bedrockMock);
    expect(req.prompt).not.toContain('ACTION TAIL'); // buildV5TurnPrompt degenerates to the V4 prompt
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS')).toBeNull(); // no tail asked ⇒ not a countable failure
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(0); // no guaranteed-empty rescue call
    expect(sseEvents(chunks, 'cta_buttons')).toHaveLength(0);
    expect(joinedText(chunks)).toBe('Just a normal answer.');
  });

  test('scheduling-handled turn on a V5 tenant: sentinel never leaks; runSchedulingTurn sees stripped text', async () => {
    loadConfig.mockResolvedValue({
      ...V5_CONFIG,
      feature_flags: { V5_SINGLE_PASS: true, scheduling_enabled: true },
    });
    runSchedulingTurn.mockResolvedValue({ handled: true, action: 'present_slots' });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Here are your options.\n<<<ACTIONS ["learn_volunteer"]>>>'])
    );

    const chunks = await invokeChat();

    expect(chunks.join('')).not.toContain(SENTINEL_OPEN);
    // The scheduling flow owned the turn — V5 emitted no buttons.
    expect(sseEvents(chunks, 'cta_buttons')).toHaveLength(0);
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(0);
    // responseBuffer consumer contract: the flow received STRIPPED text.
    expect(runSchedulingTurn).toHaveBeenCalledTimes(1);
    expect(runSchedulingTurn.mock.calls[0][0].responseText).toBe('Here are your options.\n');
  });

  test('click-routed turn on a V5 tenant: enhanceResponse sees stripped text; V5 branch skipped', async () => {
    enhanceResponse.mockResolvedValue({
      ctaButtons: [{ id: 'clicked_cta', label: 'Clicked' }],
      metadata: { routing_tier: 'explicit' },
    });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Routed reply.\n<<<ACTIONS ["learn_volunteer"]>>>'])
    );

    const chunks = await invokeChat({ routing_metadata: { action_chip_triggered: true } });

    expect(chunks.join('')).not.toContain(SENTINEL_OPEN);
    // responseBuffer consumer contract: enhanceResponse received STRIPPED text.
    expect(enhanceResponse).toHaveBeenCalledTimes(1);
    expect(enhanceResponse.mock.calls[0][0]).toBe('Routed reply.\n');
    // The click router owned CTAs; the V5 branch (and any selector call) never ran.
    const ctas = sseEvents(chunks, 'cta_buttons');
    expect(ctas[0].metadata.routing_tier).toBe('explicit');
    expect(bedrockMock.commandCalls(InvokeModelCommand)).toHaveLength(0);
  });

  test('flag OFF: byte-identical pass-through — sentinel-shaped prose is NOT stripped, V4 params, no V5 logs', async () => {
    loadConfig.mockResolvedValue({
      ...V5_CONFIG,
      feature_flags: { V4_ACTION_SELECTOR: true }, // V5 absent
    });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['Literal text: <<<ACTIONS ["learn_volunteer"]>>> stays visible.'])
    );
    bedrockMock.on(InvokeModelCommand).resolves(selectorResponse('[]'));

    const chunks = await invokeChat();

    // The parser never engaged: the sentinel-shaped prose reaches the client verbatim.
    expect(joinedText(chunks)).toBe('Literal text: <<<ACTIONS ["learn_volunteer"]>>> stays visible.');
    // V4 prompt + V4 params.
    const req = turnRequest(bedrockMock);
    expect(req.prompt).not.toContain('ACTION TAIL');
    expect(req.max_tokens).toBe(V4_STEP2_INFERENCE_PARAMS.max_tokens);
    expect(req.temperature).toBe(V4_STEP2_INFERENCE_PARAMS.temperature);
    // V4 selector ran (with the RAW buffer — pre-V5 behavior untouched).
    const selectorCalls = bedrockMock.commandCalls(InvokeModelCommand);
    expect(selectorCalls).toHaveLength(1);
    const selectorPrompt = JSON.parse(selectorCalls[0].args[0].input.body).messages[0].content[0].text;
    expect(selectorPrompt).toContain(SENTINEL_OPEN);
    // No V5 logs, no single_pass stamp.
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS')).toBeNull();
    const qa = findJsonLog(logSpy, 'QA_COMPLETE');
    expect(qa.prompt_versions).not.toHaveProperty('single_pass');
  });

  test('end-flush forwards held prose: a stream ending in a live sentinel prefix loses NOTHING', async () => {
    // The final delta ends mid-prefix ('<<' could still become '<<<ACTIONS'),
    // so feed() holds it; end() must release it as prose (NO SWALLOW), then the
    // ladder fires (no_sentinel).
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['The answer is A ', '<<'])
    );
    bedrockMock.on(InvokeModelCommand).resolves(selectorResponse('[]'));

    const chunks = await invokeChat();
    expect(joinedText(chunks)).toBe('The answer is A <<');
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS').status).toBe('no_sentinel');
    // The held-then-released prose also reached responseBuffer (QA sees it).
    expect(findJsonLog(logSpy, 'QA_COMPLETE').answer).toBe('The answer is A <<');
  });

  test('holdback never emits empty text frames; x-first-token-ms fires once, on first NON-EMPTY forward', async () => {
    // First delta is a live sentinel prefix — feed() returns '' (held). The
    // second delta diverges, releasing everything. No empty frames allowed.
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(
      bedrockStream(['<', '<Hello world.\n<<<ACTIONS []>>>'])
    );

    const chunks = await invokeChat();

    expect(joinedText(chunks)).toBe('<<Hello world.\n');
    const emptyFrames = sseEvents(chunks, 'text').filter((e) => e.content === '');
    expect(emptyFrames).toHaveLength(0);
    const ftm = chunks.filter((c) => typeof c === 'string' && c.startsWith(': x-first-token-ms='));
    expect(ftm).toHaveLength(1);
    // The timing comment precedes the first text frame.
    const ftmIdx = chunks.findIndex((c) => c.startsWith(': x-first-token-ms='));
    const textIdx = chunks.findIndex((c) => c.includes('"type":"text"'));
    expect(ftmIdx).toBeLessThan(textIdx);
  });
});

// ── buffered handler (cf_origin_wiring pattern) ────────────────────────────────

describe('V5.5 wiring — bufferedHandler (unset awslambda + resetModules)', () => {
  let mod;
  let bedrockMockB;
  let sdkB;
  let loadConfigB;
  let retrieveKBB;
  let logSpy;
  let prevAwsLambda;

  beforeAll(() => {
    jest.resetModules();
    prevAwsLambda = global.awslambda;
    global.awslambda = undefined;
    // Fresh module registry ⇒ fresh SDK class ⇒ stub the fresh prototype.
    sdkB = require('@aws-sdk/client-bedrock-runtime');
    bedrockMockB = mockClient(sdkB.BedrockRuntimeClient);
    ({ loadConfig: loadConfigB, retrieveKB: retrieveKBB } = require('../../shared/bedrock-core'));
    mod = require('../index.js'); // exports.handler === bufferedHandler
  });

  afterAll(() => {
    global.awslambda = prevAwsLambda;
  });

  beforeEach(() => {
    bedrockMockB.reset();
    loadConfigB.mockReset();
    retrieveKBB.mockReset();
    loadConfigB.mockResolvedValue(V5_CONFIG);
    retrieveKBB.mockResolvedValue('Volunteer orientation is the first Saturday of each month.');
    logSpy = jest.spyOn(console, 'log');
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  function bufferedStream(texts) {
    const events = [
      ...texts.map((text) => ({
        chunk: {
          bytes: Buffer.from(JSON.stringify({ type: 'content_block_delta', delta: { type: 'text_delta', text } })),
        },
      })),
      { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'message_stop' })) } },
    ];
    return {
      body: {
        [Symbol.asyncIterator]: async function* () {
          for (const e of events) yield e;
        },
      },
    };
  }

  async function invokeBuffered(bodyOverrides = {}) {
    const event = {
      body: JSON.stringify({
        tenant_hash: 'abc123',
        session_id: 'sess-v5-buffered',
        user_input: 'How do I learn about volunteering?',
        ...bodyOverrides,
      }),
    };
    return mod.handler(event, { awsRequestId: 'test-req' });
  }

  test('V5 on: chunk-split sentinel stripped, validated cta_buttons spliced BEFORE [DONE], both logs stamped', async () => {
    bedrockMockB.on(sdkB.InvokeModelWithResponseStreamCommand).resolves(
      bufferedStream(['Buffered answer.\n', '<<<ACTIO', 'NS ["apply_volunteer","ghost"]>>>'])
    );

    const result = await invokeBuffered();

    expect(result.statusCode).toBe(200);
    expect(result.body).not.toContain(SENTINEL_OPEN);
    expect(joinedText(result.body)).toBe('Buffered answer.\n');

    const ctas = sseEvents(result.body, 'cta_buttons');
    expect(ctas).toHaveLength(1);
    expect(ctas[0].ctaButtons.map((c) => c.id)).toEqual(['apply_volunteer']);
    expect(ctas[0].metadata.routing_tier).toBe('v5_single_pass');
    // SSE ordering: the spliced cta event precedes the [DONE] marker.
    expect(result.body.indexOf('cta_buttons')).toBeLessThan(result.body.indexOf('data: [DONE]'));

    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS')).toMatchObject({
      status: 'actions',
      trailing_after_close: false,
      tenant_hash: 'abc123',
      session_id: 'sess-v5-buffered',
    });
    const qa = findJsonLog(logSpy, 'QA_COMPLETE');
    expect(qa.answer).not.toContain(SENTINEL_OPEN);
    expect(qa.prompt_versions.single_pass).toBe(V5_TURN_PROMPT_VERSION);

    // V5 inference params on the turn call; no selector call.
    const body = JSON.parse(bedrockMockB.commandCalls(sdkB.InvokeModelWithResponseStreamCommand)[0].args[0].input.body);
    expect(body.max_tokens).toBe(V5_TURN_INFERENCE_PARAMS.max_tokens);
    expect(body.temperature).toBe(V5_TURN_INFERENCE_PARAMS.temperature);
    expect(bedrockMockB.commandCalls(sdkB.InvokeModelCommand)).toHaveLength(0);
  });

  test('BOTH flags true: V5 wins in the buffered CTA chain too', async () => {
    loadConfigB.mockResolvedValue({
      ...V5_CONFIG,
      feature_flags: { V5_SINGLE_PASS: true, V4_ACTION_SELECTOR: true },
    });
    bedrockMockB.on(sdkB.InvokeModelWithResponseStreamCommand).resolves(
      bufferedStream(['Reply.\n<<<ACTIONS ["learn_volunteer"]>>>'])
    );

    const result = await invokeBuffered();
    const ctas = sseEvents(result.body, 'cta_buttons');
    expect(ctas[0].metadata.routing_tier).toBe('v5_single_pass');
    expect(bedrockMockB.commandCalls(sdkB.InvokeModelCommand)).toHaveLength(0);
  });

  test('end-flush (buffered): held prose is pushed before [DONE]; nothing is swallowed', async () => {
    bedrockMockB.on(sdkB.InvokeModelWithResponseStreamCommand).resolves(
      bufferedStream(['The answer is B ', '<<'])
    );
    bedrockMockB.on(sdkB.InvokeModelCommand).resolves(selectorResponse('[]'));

    const result = await invokeBuffered();
    expect(joinedText(result.body)).toBe('The answer is B <<');
    // Held prose was flushed BEFORE the [DONE] marker (SSE ordering).
    expect(result.body.indexOf('The answer is B')).toBeLessThan(result.body.indexOf('data: [DONE]'));
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS').status).toBe('no_sentinel');
  });

  test('flag OFF: sentinel-shaped prose passes through verbatim; no V5 logs; no single_pass stamp', async () => {
    loadConfigB.mockResolvedValue({ ...V5_CONFIG, feature_flags: {} });
    bedrockMockB.on(sdkB.InvokeModelWithResponseStreamCommand).resolves(
      bufferedStream(['Raw: <<<ACTIONS ["learn_volunteer"]>>> visible.'])
    );

    const result = await invokeBuffered();

    expect(joinedText(result.body)).toBe('Raw: <<<ACTIONS ["learn_volunteer"]>>> visible.');
    const body = JSON.parse(bedrockMockB.commandCalls(sdkB.InvokeModelWithResponseStreamCommand)[0].args[0].input.body);
    expect(body.max_tokens).toBe(V4_STEP2_INFERENCE_PARAMS.max_tokens);
    expect(findJsonLog(logSpy, 'V5_TAIL_STATUS')).toBeNull();
    const qa = findJsonLog(logSpy, 'QA_COMPLETE');
    expect(qa.prompt_versions).not.toHaveProperty('single_pass');
  });
});
