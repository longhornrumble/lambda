/**
 * §B16d hardening — start_scheduling deterministic entry bypass.
 *
 * The start_scheduling CTA click must be a deterministic route, NOT a chat turn:
 * no Bedrock invocation of any kind (no KB answer, no detector), one templated
 * line, then the entry hook (qualifying session + propose → scheduling_slots).
 * Pinned by QA 2026-06-12 (P0-2: KB co-mingling + fictional narration on the
 * entry turn).
 *
 * Harness mirrors index.test.js (bedrock-core fully mocked; Bedrock runtime via
 * aws-sdk-client-mock; awslambda.streamifyResponse pass-through).
 */

const { mockClient } = require('aws-sdk-client-mock');
const {
  BedrockRuntimeClient,
  InvokeModelWithResponseStreamCommand,
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

const bedrockMock = mockClient(BedrockRuntimeClient);

jest.mock('../form_handler', () => ({ handleFormMode: jest.fn() }));
jest.mock('../response_enhancer', () => ({ enhanceResponse: jest.fn() }));

// Keep isSchedulingEnabled REAL (the gate under test); stub the context injector.
jest.mock('../scheduling/bindingContext', () => {
  const actual = jest.requireActual('../scheduling/bindingContext');
  return { ...actual, injectSchedulingContext: jest.fn(async (p) => p) };
});
jest.mock('../scheduling/schedulingFlow', () => ({
  runSchedulingTurn: jest.fn(async () => ({ handled: false })),
}));
// The seam under test: the bypass must call this with bedrock:null and end the stream.
// EMAIL_SHAPE stays REAL — index.js gates the email-capture path on it.
jest.mock('../scheduling/newBookingEntry', () => {
  const actual = jest.requireActual('../scheduling/newBookingEntry');
  return {
    runNewBookingEntry: jest.fn(),
    captureAttendeeEmail: jest.fn(),
    EMAIL_SHAPE: actual.EMAIL_SHAPE,
  };
});

global.awslambda = {
  streamifyResponse: jest.fn((handler) => async (event, responseStream, context) =>
    handler(event, responseStream, context)
  ),
};

const { loadConfig, retrieveKB } = require('../../shared/bedrock-core');
const { enhanceResponse } = require('../response_enhancer');
const { runNewBookingEntry, captureAttendeeEmail } = require('../scheduling/newBookingEntry');

const indexModule = require('../index');

const ENTRY_COPY = 'Happy to set that up';
const FALLBACK_COPY = "couldn't pull up available times";

const schedulingConfig = {
  tenant_id: 'TEST123',
  feature_flags: { scheduling_enabled: true },
  aws: { knowledge_base_id: 'KB123', model_id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0' },
  tone_prompt: 'You are a helpful assistant.',
};

const noSchedulingConfig = {
  ...schedulingConfig,
  feature_flags: {},
};

function bedrockStream(texts) {
  const events = [
    { chunk: { bytes: Buffer.from(JSON.stringify({ type: 'content_block_start' })) } },
    ...texts.map((text) => ({
      chunk: {
        bytes: Buffer.from(
          JSON.stringify({ type: 'content_block_delta', delta: { type: 'text_delta', text } })
        ),
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
    write: jest.fn((d) => chunks.push(d)),
    end: jest.fn(),
    getChunks: () => chunks,
  };
}

function clickEvent(overrides = {}) {
  return {
    body: JSON.stringify({
      tenant_hash: 'abc123',
      user_input: '📅 Schedule a Call',
      session_id: 'sess-bypass-1',
      conversation_history: [],
      routing_metadata: {
        scheduling_intent: 'new_booking',
        cta_triggered: true,
        cta_id: 'schedule_intro_call',
        cta_action: 'start_scheduling',
      },
      ...overrides,
    }),
  };
}

// File-scope resets: BOTH describe blocks below share these mocks — a describe-scoped
// beforeEach left the second block with accumulated state (found the hard way).
beforeEach(() => {
  bedrockMock.reset();
  loadConfig.mockReset();
  retrieveKB.mockReset();
  enhanceResponse.mockReset();
  runNewBookingEntry.mockReset();
  captureAttendeeEmail.mockReset();
  captureAttendeeEmail.mockResolvedValue({ captured: false, reason: 'no_confirming_session' });

  loadConfig.mockResolvedValue(schedulingConfig);
  retrieveKB.mockResolvedValue('kb context');
  enhanceResponse.mockResolvedValue({ message: '', ctaButtons: [], metadata: {} });
  runNewBookingEntry.mockResolvedValue({ handled: true });

  process.env.CONFIG_BUCKET = 'test-bucket';
});

describe('start_scheduling deterministic entry bypass', () => {

  it('bypasses Bedrock entirely and runs the entry hook with bedrock:null', async () => {
    const responseStream = mockResponseStream();
    await indexModule.handler(clickEvent(), responseStream, {});

    // No model call of ANY kind — neither the KB answer nor the detector.
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
    expect(retrieveKB).not.toHaveBeenCalled();

    expect(runNewBookingEntry).toHaveBeenCalledTimes(1);
    expect(runNewBookingEntry).toHaveBeenCalledWith(
      expect.objectContaining({
        bedrock: null,
        sessionId: 'sess-bypass-1',
        tenantId: 'TEST123',
        routingMetadata: expect.objectContaining({ scheduling_intent: 'new_booking' }),
      })
    );

    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain(ENTRY_COPY);
    expect(chunks).not.toContain(FALLBACK_COPY);
    expect(chunks).toContain('[DONE]');
    expect(responseStream.end).toHaveBeenCalled();
  });

  it('falls through to normal chat when scheduling is disabled', async () => {
    loadConfig.mockResolvedValue(noSchedulingConfig);
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Hello!']));

    const responseStream = mockResponseStream();
    await indexModule.handler(clickEvent(), responseStream, {});

    // Normal path ran: Bedrock invoked, bypass copy absent, entry hook NOT engaged
    // (post-stream hook is gated on schedulingEnabled).
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    const chunks = responseStream.getChunks().join('');
    expect(chunks).not.toContain(ENTRY_COPY);
    expect(runNewBookingEntry).not.toHaveBeenCalled();
  });

  it('does not engage on a normal turn without the signal', async () => {
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Hi there']));

    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: { cta_triggered: false } }),
      responseStream,
      {}
    );

    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    expect(responseStream.getChunks().join('')).not.toContain(ENTRY_COPY);
  });

  it('writes the honest fallback line when the entry hook reports unhandled', async () => {
    runNewBookingEntry.mockResolvedValue({ handled: false });

    const responseStream = mockResponseStream();
    await indexModule.handler(clickEvent(), responseStream, {});

    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain(ENTRY_COPY);
    expect(chunks).toContain(FALLBACK_COPY);
    expect(chunks).toContain('[DONE]');
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
  });

  it('writes the honest fallback line when the flow reports handled:true with propose_failed_outcome (dead-air regression, 2026-07-03)', async () => {
    // newBookingFlow returns handled:true + reason:'propose_failed_outcome' when the
    // propose seam reports outcome:'failed' (its "non-fatal, no picker" branch). On a
    // click turn there is no streamed chat text to fall back on, so without the reason
    // check the user got the entry copy and then silence — observed live on staging
    // when every appointment type lacked availability_windows.
    runNewBookingEntry.mockResolvedValue({
      handled: true,
      executed: false,
      state: 'qualifying',
      reason: 'propose_failed_outcome',
    });

    const responseStream = mockResponseStream();
    await indexModule.handler(clickEvent(), responseStream, {});

    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain(ENTRY_COPY);
    expect(chunks).toContain(FALLBACK_COPY);
    expect(chunks).toContain('[DONE]');
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
  });

  it('survives an entry-hook throw with the fallback line (no crash, stream ends)', async () => {
    runNewBookingEntry.mockRejectedValue(new Error('DDB exploded'));

    const responseStream = mockResponseStream();
    await indexModule.handler(clickEvent(), responseStream, {});

    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain(FALLBACK_COPY);
    expect(chunks).toContain('[DONE]');
    expect(responseStream.end).toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
  });
});

describe('deterministic click router — select / confirm / email-capture turns', () => {
  it('select_slot click routes deterministically; identity known → "lock it in" copy', async () => {
    runNewBookingEntry.mockResolvedValue({ handled: true, state: 'confirming', identity: true });
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({
        routing_metadata: { scheduling_action: 'select_slot', scheduling_slot_id: 's1', cta_triggered: false },
        user_input: 'Fri, Jun 12 · 9:00 AM',
      }),
      responseStream,
      {}
    );
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
    expect(runNewBookingEntry).toHaveBeenCalledWith(
      expect.objectContaining({
        bedrock: null,
        routingMetadata: expect.objectContaining({ scheduling_action: 'select_slot', scheduling_slot_id: 's1' }),
      })
    );
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('lock it in');
    expect(chunks).not.toContain(ENTRY_COPY); // entry copy is the CTA turn's line, not select's
    expect(chunks).toContain('[DONE]');
  });

  it('select_slot click without identity → email-ask copy', async () => {
    runNewBookingEntry.mockResolvedValue({ handled: true, state: 'confirming', identity: false });
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: { scheduling_action: 'select_slot', scheduling_slot_id: 's1' } }),
      responseStream,
      {}
    );
    expect(responseStream.getChunks().join('')).toContain('best email');
  });

  it('select_slot rejected (stale/spoofed slot) → slot-gone copy', async () => {
    runNewBookingEntry.mockResolvedValue({ handled: true, rejected: true, reason: 'unknown_slot' });
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: { scheduling_action: 'select_slot', scheduling_slot_id: 'zz' } }),
      responseStream,
      {}
    );
    expect(responseStream.getChunks().join('')).toContain("isn't available anymore");
  });

  it('confirm_book click executed → booked copy; no Bedrock', async () => {
    runNewBookingEntry.mockResolvedValue({ handled: true, action: 'confirm_book', executed: true, state: 'booked' });
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: { scheduling_action: 'confirm_book' } }),
      responseStream,
      {}
    );
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
    expect(responseStream.getChunks().join('')).toContain('Booked!');
  });

  it('confirm_book click held at identity_required → email-ask copy, no crash', async () => {
    runNewBookingEntry.mockResolvedValue({ handled: true, executed: false, rejected: true, reason: 'identity_required' });
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: { scheduling_action: 'confirm_book' } }),
      responseStream,
      {}
    );
    expect(responseStream.getChunks().join('')).toContain('best email');
  });

  it('bare email turn with a confirming session → captured, "Got it" copy, no Bedrock', async () => {
    captureAttendeeEmail.mockResolvedValue({ captured: true, email: 'vol@example.com' });
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: {}, user_input: 'vol@example.com' }),
      responseStream,
      {}
    );
    expect(captureAttendeeEmail).toHaveBeenCalledWith(
      expect.objectContaining({ email: 'vol@example.com', sessionId: 'sess-bypass-1' })
    );
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('Got it — vol@example.com');
    expect(chunks).toContain('[DONE]');
  });

  it('bare email with NO in-flight session → falls through to normal chat (Bedrock runs)', async () => {
    captureAttendeeEmail.mockResolvedValue({ captured: false, reason: 'no_confirming_session' });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Sure!']));
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: {}, user_input: 'someone@example.com' }),
      responseStream,
      {}
    );
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    expect(responseStream.getChunks().join('')).not.toContain('Got it —');
  });

  it('scheduling disabled: select_slot click falls through to normal chat', async () => {
    loadConfig.mockResolvedValue(noSchedulingConfig);
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Hi']));
    // Pollution guard: a prior test's post-stream tail can land a late send on the
    // shared client mock — count from a clean history so this asserts THIS turn only.
    bedrockMock.resetHistory();
    const responseStream = mockResponseStream();
    await indexModule.handler(
      clickEvent({ routing_metadata: { scheduling_action: 'select_slot', scheduling_slot_id: 's1' } }),
      responseStream,
      {}
    );
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    expect(runNewBookingEntry).not.toHaveBeenCalled();
  });
});
