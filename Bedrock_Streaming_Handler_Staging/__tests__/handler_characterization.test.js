/**
 * Handler characterization net (Phase 0 of the streaming/buffered dedup, #5).
 *
 * GOAL: freeze the observable SSE behaviour of BOTH `streamingHandler` (prod) and
 * `bufferedHandler` (test/fallback) so the Phase-1 extraction of the shared
 * post-response pipeline can be proven behaviour-preserving. Two assertion kinds:
 *
 *   1. GOLDEN MASTER (per handler) — for a given input, each handler emits a known
 *      sequence of semantic SSE events. The regression tripwire for Phase 1: after
 *      the refactor the same inputs MUST yield the same sequence.
 *   2. CROSS-HANDLER PARITY — for the same input the two handlers emit the SAME
 *      semantic event sequence, modulo the intended I/O-only differences (the
 *      streaming-only `start` frame, heartbeat/metric comment frames, and the
 *      terminal mechanism). This is the invariant the dedup guarantees, and the
 *      assertion that would have caught the F-DSAR25 "fix landed in one twin" drift.
 *
 * Pure characterization — adds NO production code. Both handlers are loaded via
 * jest.isolateModules (the ONLY way to get the buffered export: awslambda absent).
 * Per the analytics_integration.test.js note, aws-sdk-client-mock does NOT reach
 * inside an isolated registry, so every seam — including the Bedrock client — is
 * jest.doMock'd against module-scope spies that each test configures.
 */

'use strict';

// ── Module-scope controllable spies (referenced by the per-isolate doMock factories;
//    jest.doMock is NOT hoisted, so these closures capture the live jest.fn refs). ──
const spies = {
  loadConfig: jest.fn(),
  retrieveKB: jest.fn(),
  enhanceResponse: jest.fn(),
  selectActionsV4: jest.fn(),
  classifyTopic: jest.fn(),
  selectCTAsFromPool: jest.fn(),
  runSchedulingTurn: jest.fn(),
  runNewBookingEntry: jest.fn(),
  writeSessionSummary: jest.fn(),
  bedrockSend: jest.fn(),
};

// Bedrock streaming mock: an async-iterable body of content_block_delta frames.
function createBedrockStream(textChunks) {
  const frames = [
    { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'content_block_start' })) } },
    ...textChunks.map((text) => ({
      chunk: { bytes: Buffer.from(JSON.stringify({ type: 'content_block_delta', delta: { type: 'text_delta', text } })) },
    })),
    { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'message_stop' })) } },
  ];
  return { body: { [Symbol.asyncIterator]: async function* () { for (const f of frames) yield f; } } };
}

function createMockResponseStream() {
  const chunks = [];
  return { write: jest.fn((d) => chunks.push(d)), end: jest.fn(), getChunks: () => chunks };
}

// Load one handler flavour in an isolated registry with all seams doMock'd.
function loadHandler(streaming) {
  let handler;
  jest.isolateModules(() => {
    if (streaming) global.awslambda = { streamifyResponse: (h) => h };
    else delete global.awslambda;

    jest.doMock('@aws-sdk/client-bedrock-runtime', () => ({
      BedrockRuntimeClient: jest.fn(function () { this.send = (...a) => spies.bedrockSend(...a); }),
      InvokeModelWithResponseStreamCommand: jest.fn(function (input) { this.input = input; }),
    }));
    jest.doMock('@aws-sdk/client-sqs', () => ({
      SQSClient: jest.fn(function () { this.send = jest.fn().mockResolvedValue({ MessageId: 't' }); }),
      SendMessageCommand: jest.fn(),
      SendMessageBatchCommand: jest.fn(),
    }));
    jest.doMock('../../shared/bedrock-core', () => ({
      loadConfig: (...a) => spies.loadConfig(...a),
      retrieveKB: (...a) => spies.retrieveKB(...a),
      sanitizeUserInput: (x) => x,
      getCacheKey: jest.fn(), isCacheValid: jest.fn(), evictOldestCacheEntries: jest.fn(),
      CACHE_TTL: 300000, MAX_CACHE_SIZE: 100,
    }));
    jest.doMock('../form_handler', () => ({ handleFormMode: jest.fn() }));
    jest.doMock('../response_enhancer', () => ({
      enhanceResponse: (...a) => spies.enhanceResponse(...a),
      getShowcaseById: jest.fn(), loadTenantConfig: jest.fn(),
    }));
    jest.doMock('../analytics_writer', () => ({ writeSessionSummary: (...a) => spies.writeSessionSummary(...a) }));
    jest.doMock('../prompt_v4', () => {
      const actual = jest.requireActual('../prompt_v4');
      return {
        ...actual,
        classifyTopic: (...a) => spies.classifyTopic(...a),
        selectActionsV4: (...a) => spies.selectActionsV4(...a),
        selectCTAsFromPool: (...a) => spies.selectCTAsFromPool(...a),
      };
    });
    jest.doMock('../scheduling/schedulingFlow', () => ({ runSchedulingTurn: (...a) => spies.runSchedulingTurn(...a) }));
    jest.doMock('../scheduling/newBookingEntry', () => ({
      runNewBookingEntry: (...a) => spies.runNewBookingEntry(...a),
      captureAttendeeEmail: jest.fn(async () => ({ captured: false })),
      // Real-shaped regex: the handler calls EMAIL_SHAPE.test(userInput) to gate the
      // email-capture branch. A non-email input must return false so both paths skip it.
      EMAIL_SHAPE: /^[^\s@<>]+@[^\s@]+\.[^\s@]+$/,
    }));
    jest.doMock('../scheduling/postBookingPrepNote', () => ({
      capturePrepNote: jest.fn(async () => ({ captured: false })),
      tenantHasPostBookingQuestion: jest.fn(() => false),
    }));
    jest.doMock('../scheduling/agentTurn', () => ({ agentTurn: jest.fn(), isAgentTurnEnabled: jest.fn(() => false) }));
    jest.doMock('../scheduling/bindingContext', () => {
      const actual = jest.requireActual('../scheduling/bindingContext');
      return { ...actual, injectSchedulingContext: jest.fn(async (p) => p) };
    });

    handler = require('../index.js').handler;
  });
  return handler;
}

let streamingHandler;
let bufferedHandler;
beforeAll(() => {
  const saved = global.awslambda;
  streamingHandler = loadHandler(true);
  bufferedHandler = loadHandler(false);
  global.awslambda = saved;
});
afterAll(() => { jest.resetModules(); });

// ── SSE → normalized semantic sequence ───────────────────────────────────────────
// LINE-based (not \n\n-block-based): the buffered path pushes its terminal metric
// comments + [DONE] joined with single \n, and formats `data:` JSON with spaces
// ("type": "text") vs streaming's compact JSON. A line parser normalizes past both
// so only the semantic `data:` frames remain; `:`-comment lines (heartbeat, metrics)
// are dropped as intended I/O-only noise.
function parseSSE(raw) {
  if (!raw) return [];
  const out = [];
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t || t.startsWith(':')) continue;            // blank or comment (heartbeat/metrics)
    if (!t.startsWith('data:')) continue;
    const payload = t.slice(5).trim();
    if (payload === '[DONE]') { out.push({ type: 'DONE' }); continue; }
    try { out.push(JSON.parse(payload)); } catch { out.push({ type: '__raw__', raw: payload }); }
  }
  return out;
}
// Comparable shape: type + load-bearing payload (session_id dropped — echoes the request).
function semantic(events) {
  return events.map((e) => {
    if (e.type === 'text') return { type: 'text', content: e.content };
    if (e.type === 'cta_buttons') return { type: 'cta_buttons', ids: (e.ctaButtons || []).map((c) => c.id), routing_tier: e.metadata?.routing_tier };
    if (e.type === 'showcase_card') return { type: 'showcase_card', showcaseCard: e.showcaseCard };
    if (e.type === 'error') return { type: 'error' };
    return { type: e.type };
  });
}
// `start` + `stream_start` are intended streaming-only connection markers (buffered
// emits neither); drop both when comparing the two paths for semantic parity.
const STREAM_ONLY = new Set(['start', 'stream_start']);
const noStreamMarkers = (seq) => seq.filter((e) => !STREAM_ONLY.has(e.type));

async function runStreaming(event, ctx = {}) {
  const rs = createMockResponseStream();
  await streamingHandler(event, rs, ctx);
  return parseSSE(rs.getChunks().join(''));
}
async function runBuffered(event, ctx = {}) {
  const res = await bufferedHandler(event, ctx);
  return { events: parseSSE(res && res.body), statusCode: res && res.statusCode };
}

const chatEvent = (extra = {}) => ({ body: JSON.stringify({ tenant_hash: 'abc123', user_input: 'Tell me about volunteering', ...extra }) });
const baseConfig = {
  tenant_id: 'TEST123',
  aws: { knowledge_base_id: 'KB123', model_id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0' },
  streaming: { max_tokens: 1000, temperature: 0 },
  tone_prompt: 'You are a helpful assistant.',
  cta_definitions: {
    apply_now: { label: 'Apply Now', action: 'start_form', formId: 'volunteer_apply', ai_available: true },
    learn_more: { label: 'Learn More', action: 'show_info', ai_available: true },
  },
};

beforeEach(() => {
  Object.values(spies).forEach((s) => s.mockReset());
  spies.loadConfig.mockResolvedValue(baseConfig);
  spies.retrieveKB.mockResolvedValue({ context: '', citations: [] });
  spies.enhanceResponse.mockResolvedValue({ message: '', ctaButtons: [], metadata: {} });
  spies.selectActionsV4.mockResolvedValue([]);
  spies.classifyTopic.mockResolvedValue(null);
  spies.selectCTAsFromPool.mockReturnValue({ ctaButtons: [], metadata: {} });
  spies.runSchedulingTurn.mockResolvedValue({ handled: false });
  spies.runNewBookingEntry.mockResolvedValue({ handled: false });
  spies.writeSessionSummary.mockResolvedValue(true);
  // Fresh stream per invocation — a test drives BOTH handlers, and a Bedrock async-iterable
  // body must not be shared/re-consumed across the two calls.
  spies.bedrockSend.mockImplementation(async () => createBedrockStream(['Hello ', 'world']));
  process.env.CONFIG_BUCKET = 'test-bucket';
});

// ═══════════════════════════════════════════════════════════════════════════════
describe('characterization — normal chat (fallback tier, no CTAs)', () => {
  it('streaming golden master: start, stream_start, text×2, DONE', async () => {
    expect(semantic(await runStreaming(chatEvent()))).toEqual([
      { type: 'start' }, { type: 'stream_start' },
      { type: 'text', content: 'Hello ' }, { type: 'text', content: 'world' }, { type: 'DONE' },
    ]);
  });
  it('buffered golden master: text×2, DONE (status 200)', async () => {
    const { events, statusCode } = await runBuffered(chatEvent());
    expect(statusCode).toBe(200);
    expect(semantic(events)).toEqual([
      { type: 'text', content: 'Hello ' }, { type: 'text', content: 'world' }, { type: 'DONE' },
    ]);
  });
  it('parity: identical semantic sequence (streaming-only start frame dropped)', async () => {
    expect(noStreamMarkers(semantic(await runStreaming(chatEvent())))).toEqual(semantic((await runBuffered(chatEvent())).events));
  });
});

describe('characterization — fallback tier emits CTAs (enhanceResponse)', () => {
  beforeEach(() => {
    spies.enhanceResponse.mockResolvedValue({
      message: '', metadata: { routing_tier: 'explicit' },
      ctaButtons: [{ id: 'apply_now', label: 'Apply Now', _position: 'primary' }],
    });
  });
  it('streaming: a cta_buttons frame precedes DONE', async () => {
    const seq = noStreamMarkers(semantic(await runStreaming(chatEvent())));
    expect(seq).toContainEqual({ type: 'cta_buttons', ids: ['apply_now'], routing_tier: 'explicit' });
    expect(seq[seq.length - 1]).toEqual({ type: 'DONE' });
  });
  it('parity: buffered emits the same cta_buttons frame in the same order', async () => {
    expect(noStreamMarkers(semantic(await runStreaming(chatEvent())))).toEqual(semantic((await runBuffered(chatEvent())).events));
  });
});

describe('characterization — V4 action selector tier', () => {
  beforeEach(() => {
    spies.loadConfig.mockResolvedValue({ ...baseConfig, feature_flags: { V4_ACTION_SELECTOR: true } });
    spies.selectActionsV4.mockResolvedValue(['apply_now', 'learn_more']);
  });
  it('parity: both emit v4_action_selector CTAs built from the selected ids', async () => {
    const s = noStreamMarkers(semantic(await runStreaming(chatEvent())));
    const b = semantic((await runBuffered(chatEvent())).events);
    const cta = s.find((e) => e.type === 'cta_buttons');
    expect(cta).toEqual({ type: 'cta_buttons', ids: ['apply_now', 'learn_more'], routing_tier: 'v4_action_selector' });
    expect(s).toEqual(b);
  });
});

describe('characterization — showcase card (enhanceResponse.showcaseCard)', () => {
  // ✅ DRIFT RESOLVED by Phase 1. Pre-dedup, streaming emitted enhanceResponse.showcaseCard in
  // the CTA tiers but the buffered twin dropped it (an F-DSAR25-class divergence the net caught
  // and froze). The shared responsePipeline.js now unifies the emit path, so BOTH handlers emit
  // the frame. The former known-drift pair is retired for this parity assertion — the deliberate
  // acceptance signal for the extraction.
  beforeEach(() => {
    spies.enhanceResponse.mockResolvedValue({
      message: '', metadata: { routing_tier: 'explicit' }, ctaButtons: [],
      showcaseCard: { id: 'sc1', title: 'Our Programs' },
    });
  });
  it('parity: both handlers emit a showcase_card frame', async () => {
    const s = noStreamMarkers(semantic(await runStreaming(chatEvent())));
    const b = semantic((await runBuffered(chatEvent())).events);
    expect(s).toContainEqual({ type: 'showcase_card', showcaseCard: { id: 'sc1', title: 'Our Programs' } });
    expect(s).toEqual(b);
  });
});

describe('characterization — scheduling turn owns the surface (CTA selection skipped)', () => {
  beforeEach(() => {
    // scheduling must be ENABLED for runSchedulingTurn to be invoked at all (gate:
    // config.feature_flags.scheduling_enabled === true, bindingContext.isSchedulingEnabled).
    spies.loadConfig.mockResolvedValue({ ...baseConfig, feature_flags: { scheduling_enabled: true } });
    spies.runSchedulingTurn.mockResolvedValue({ handled: true, action: 'confirm_reschedule' });
    spies.enhanceResponse.mockResolvedValue({ message: '', ctaButtons: [{ id: 'apply_now' }], metadata: {} });
  });
  it('parity: neither handler emits cta_buttons when the scheduling turn is handled', async () => {
    const s = noStreamMarkers(semantic(await runStreaming(chatEvent())));
    const b = semantic((await runBuffered(chatEvent())).events);
    expect(s.some((e) => e.type === 'cta_buttons')).toBe(false);
    expect(b.some((e) => e.type === 'cta_buttons')).toBe(false);
    expect(s).toEqual(b);
  });
});

describe('characterization — Bedrock error path', () => {
  beforeEach(() => { spies.bedrockSend.mockRejectedValue(new Error('bedrock exploded')); });
  it('streaming: emits an error frame then DONE (never leaves the stream open)', async () => {
    const seq = semantic(await runStreaming(chatEvent()));
    expect(seq.some((e) => e.type === 'error')).toBe(true);
    expect(seq[seq.length - 1]).toEqual({ type: 'DONE' });
  });
  it('buffered: returns 500 with an error frame + DONE', async () => {
    const { events, statusCode } = await runBuffered(chatEvent());
    expect(statusCode).toBe(500);
    expect(events.some((e) => e.type === 'error')).toBe(true);
    expect(events[events.length - 1]).toEqual({ type: 'DONE' });
  });
});
