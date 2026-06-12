/**
 * Integrator glue — agentic scheduling slice wiring (§B17a/§B17d + Track-D D3).
 *
 * Owns the THREE index.js wirings (the modules themselves are owned by
 * WS-AG-CORE / WS-TRACKD-BE and tested in their own suites):
 *   1. §B17a agent-turn routing branch: typed text + AGENTIC_SCHEDULING on +
 *      in-flight new-booking session row → agentTurn, stream ended like the
 *      click router; flag off / no row → legacy path byte-identical (§B17h).
 *   2. §B17d state-line deps: the streaming injectSchedulingContext call site
 *      passes deps.loadState so the flag-off path gets the state line.
 *   3. D3 postFormOffer call site: successful final form submission with an
 *      email → offer, gated by the STRICT layer-1 guard (loadState must return
 *      NO row at all); offer text + scheduling_slots emitted as SSE; the
 *      internal applicant_contact seam field never reaches the wire.
 *
 * Harness mirrors schedulingEntryBypass.test.js (bedrock-core fully mocked;
 * Bedrock runtime via aws-sdk-client-mock; awslambda.streamifyResponse
 * pass-through; schedulingStateStore mocked so no real DDB client exists).
 */

// Set BEFORE index.js loads so newBookingDep (invokeProposal/invokeBookingCommit)
// is populated and the glue's deps-threading is assertable. Cleaned up in afterAll
// so later test files in the same worker see the default (unset) state.
process.env.SCHEDULING_EXECUTOR_FUNCTION_NAME = 'test-executor-fn';

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

// The DDB seam: index.js builds schedulingDeps from this at module load. Mocked so
// the glue's loadState reads hit controllable fns (and no real DDB client exists).
jest.mock('../scheduling/schedulingStateStore', () => {
  const loadState = jest.fn();
  const saveState = jest.fn();
  const loadBooking = jest.fn();
  return {
    buildSchedulingDeps: () => ({ loadState, saveState, loadBooking }),
    __mockStore: { loadState, saveState, loadBooking },
  };
});

// Keep isSchedulingEnabled / resolveNewBookingSessionRow / NEW_BOOKING_IN_FLIGHT_STATES
// REAL (the routing gates under test); stub only the prompt injector so its call args
// (wiring 2: deps.loadState) are assertable without a real binding read.
jest.mock('../scheduling/bindingContext', () => {
  const actual = jest.requireActual('../scheduling/bindingContext');
  return { ...actual, injectSchedulingContext: jest.fn(async (p) => p) };
});
jest.mock('../scheduling/schedulingFlow', () => ({
  runSchedulingTurn: jest.fn(async () => ({ handled: false })),
}));
jest.mock('../scheduling/newBookingEntry', () => {
  const actual = jest.requireActual('../scheduling/newBookingEntry');
  return {
    runNewBookingEntry: jest.fn(),
    captureAttendeeEmail: jest.fn(),
    EMAIL_SHAPE: actual.EMAIL_SHAPE,
  };
});

// The modules under WIRING test — mocked; their internals are owned by
// WS-AG-CORE / WS-TRACKD-BE suites. isAgentTurnEnabled stays REAL (§B17h gate).
jest.mock('../scheduling/agentTurn', () => {
  const actual = jest.requireActual('../scheduling/agentTurn');
  return { ...actual, agentTurn: jest.fn() };
});
jest.mock('../scheduling/postFormOffer', () => ({ postFormOffer: jest.fn() }));

global.awslambda = {
  streamifyResponse: jest.fn((handler) => async (event, responseStream, context) =>
    handler(event, responseStream, context)
  ),
};

const { loadConfig, retrieveKB } = require('../../shared/bedrock-core');
const { enhanceResponse } = require('../response_enhancer');
const { handleFormMode } = require('../form_handler');
const { runNewBookingEntry, captureAttendeeEmail } = require('../scheduling/newBookingEntry');
const { injectSchedulingContext } = require('../scheduling/bindingContext');
const { agentTurn } = require('../scheduling/agentTurn');
const { postFormOffer } = require('../scheduling/postFormOffer');
const { __mockStore } = require('../scheduling/schedulingStateStore');

const indexModule = require('../index');

const AGENT_NARRATION = 'agent narration from the tool loop';
const OFFER_TEXT = 'Would you like to book a quick call? Here are some times that work:';

const agentConfig = {
  tenant_id: 'TEST123',
  feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: true },
  aws: { knowledge_base_id: 'KB123', model_id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0' },
  tone_prompt: 'You are a helpful assistant.',
};

// AGENTIC_SCHEDULING absent → §B17h flag-off (scheduling itself stays on).
const flagOffConfig = {
  ...agentConfig,
  feature_flags: { scheduling_enabled: true },
};

const inFlightRow = {
  state: 'proposing',
  session_id: 'sess-glue-1',
  candidate_slots: [{ slotId: 's1', label: 'Fri, Jun 12 · 9:00 AM' }],
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

function chatEvent(overrides = {}) {
  return {
    body: JSON.stringify({
      tenant_hash: 'abc123',
      user_input: 'can you find me a time on Friday?',
      session_id: 'sess-glue-1',
      conversation_history: [],
      ...overrides,
    }),
  };
}

function formSubmitEvent(overrides = {}) {
  return {
    body: JSON.stringify({
      tenant_hash: 'abc123',
      form_mode: true,
      action: 'submit_form',
      form_id: 'volunteer_apply',
      form_data: { field_1: 'Jane', field_2: 'jane@example.org' },
      session_id: 'sess-glue-form-1',
      ...overrides,
    }),
  };
}

afterAll(() => {
  delete process.env.SCHEDULING_EXECUTOR_FUNCTION_NAME;
});

beforeEach(() => {
  bedrockMock.reset();
  loadConfig.mockReset();
  retrieveKB.mockReset();
  enhanceResponse.mockReset();
  handleFormMode.mockReset();
  runNewBookingEntry.mockReset();
  captureAttendeeEmail.mockReset();
  injectSchedulingContext.mockClear();
  agentTurn.mockReset();
  postFormOffer.mockReset();
  __mockStore.loadState.mockReset();
  __mockStore.saveState.mockReset();
  __mockStore.loadBooking.mockReset();

  delete process.env.AGENTIC_SCHEDULING_DISABLED;

  loadConfig.mockResolvedValue(agentConfig);
  retrieveKB.mockResolvedValue('kb context');
  enhanceResponse.mockResolvedValue({ message: '', ctaButtons: [], metadata: {} });
  runNewBookingEntry.mockResolvedValue({ handled: true });
  captureAttendeeEmail.mockResolvedValue({ captured: false, reason: 'no_confirming_session' });
  injectSchedulingContext.mockImplementation(async (p) => p);
  __mockStore.loadState.mockResolvedValue(null);
  agentTurn.mockImplementation(async ({ event, streamWriter }) => {
    streamWriter(
      `data: ${JSON.stringify({ type: 'text', content: AGENT_NARRATION, session_id: event.sessionId })}\n\n`
    );
  });
  postFormOffer.mockResolvedValue({ offerText: null, slotsResult: null });
});

describe('§B17a agent-turn routing branch (wiring 1)', () => {
  it('(a) flag on + in-flight row + typed text → agentTurn runs, stream ends, no main-path model call', async () => {
    __mockStore.loadState.mockResolvedValue(inFlightRow);

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    // The agent turn ran once, with the live row + the full deps bag threaded.
    expect(agentTurn).toHaveBeenCalledTimes(1);
    expect(agentTurn).toHaveBeenCalledWith(
      expect.objectContaining({
        event: expect.objectContaining({
          userText: 'can you find me a time on Friday?',
          sessionId: 'sess-glue-1',
          conversationHistory: [],
        }),
        sessionRow: inFlightRow,
        tenantConfig: agentConfig,
        streamWriter: expect.any(Function),
        deps: expect.objectContaining({
          bedrock: expect.anything(),
          loadState: expect.any(Function),
          saveState: expect.any(Function),
          invokeProposal: expect.any(Function),
        }),
      })
    );

    // ONE loadState read gated the branch.
    expect(__mockStore.loadState).toHaveBeenCalledTimes(1);
    expect(__mockStore.loadState).toHaveBeenCalledWith({
      tenantId: 'TEST123',
      sessionId: 'sess-glue-1',
    });

    // No model call from the MAIN path (agentTurn owns its own model calls; mocked here).
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
    expect(retrieveKB).not.toHaveBeenCalled();

    // Stream ended exactly like the click router: narration → [DONE] → end().
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain(AGENT_NARRATION);
    expect(chunks).toContain('[DONE]');
    expect(chunks.indexOf(AGENT_NARRATION)).toBeLessThan(chunks.indexOf('[DONE]'));
    expect(responseStream.end).toHaveBeenCalled();
  });

  it('(b) flag off → legacy path byte-identical (Bedrock called, agent never consulted, no agent-branch state read)', async () => {
    loadConfig.mockResolvedValue(flagOffConfig);
    // Even WITH an in-flight row present, flag off must never reach the agent.
    __mockStore.loadState.mockResolvedValue(inFlightRow);
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Hello!']));

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    expect(agentTurn).not.toHaveBeenCalled();
    // The §B17h flag check GATES the state read — flag-off adds zero I/O
    // (injectSchedulingContext is stubbed here; its deps threading is wiring-2's test).
    expect(__mockStore.loadState).not.toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('Hello!');
    expect(chunks).not.toContain(AGENT_NARRATION);
    expect(chunks).toContain('[DONE]');
  });

  it('(b2) global env kill switch AGENTIC_SCHEDULING_DISABLED=true overrides the per-tenant flag', async () => {
    process.env.AGENTIC_SCHEDULING_DISABLED = 'true';
    __mockStore.loadState.mockResolvedValue(inFlightRow);
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Hello!']));

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    expect(agentTurn).not.toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
  });

  it('(c) flag on + NO session row → normal chat (Bedrock called, agentTurn not called)', async () => {
    __mockStore.loadState.mockResolvedValue(null);
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Normal answer']));

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    expect(agentTurn).not.toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('Normal answer');
    expect(chunks).toContain('[DONE]');
  });

  it('(c2) flag on + row in a non-in-flight state (booked) → normal chat', async () => {
    __mockStore.loadState.mockResolvedValue({ state: 'booked', session_id: 'sess-glue-1' });
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Normal answer']));

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    expect(agentTurn).not.toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
  });

  it('(c3) flag on + state read THROWS → non-fatal, normal chat (resolveNewBookingSessionRow fail-soft)', async () => {
    __mockStore.loadState.mockRejectedValue(new Error('DDB exploded'));
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Normal answer']));

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    expect(agentTurn).not.toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(1);
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('Normal answer');
    expect(chunks).toContain('[DONE]');
    expect(responseStream.end).toHaveBeenCalled();
  });

  it('(d) click turns still route deterministically — agent never sees a click (regression)', async () => {
    __mockStore.loadState.mockResolvedValue(inFlightRow);
    runNewBookingEntry.mockResolvedValue({ handled: true, state: 'confirming', identity: true });

    const responseStream = mockResponseStream();
    await indexModule.handler(
      chatEvent({
        user_input: 'Fri, Jun 12 · 9:00 AM',
        routing_metadata: { scheduling_action: 'select_slot', scheduling_slot_id: 's1' },
      }),
      responseStream,
      {}
    );

    // Deterministic router handled it: entry hook ran with bedrock:null, agent untouched,
    // no model call of any kind.
    expect(runNewBookingEntry).toHaveBeenCalledTimes(1);
    expect(runNewBookingEntry).toHaveBeenCalledWith(
      expect.objectContaining({ bedrock: null })
    );
    expect(agentTurn).not.toHaveBeenCalled();
    expect(bedrockMock.commandCalls(InvokeModelWithResponseStreamCommand)).toHaveLength(0);
    expect(responseStream.getChunks().join('')).toContain('[DONE]');
  });
});

describe('§B17d state-line deps at the injectSchedulingContext call site (wiring 2)', () => {
  it('passes deps.loadState so the flag-off path can build the state line', async () => {
    loadConfig.mockResolvedValue(flagOffConfig);
    bedrockMock.on(InvokeModelWithResponseStreamCommand).resolves(bedrockStream(['Hello!']));

    const responseStream = mockResponseStream();
    await indexModule.handler(chatEvent(), responseStream, {});

    expect(injectSchedulingContext).toHaveBeenCalledTimes(1);
    expect(injectSchedulingContext).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        tenantId: 'TEST123',
        sessionId: 'sess-glue-1',
        deps: expect.objectContaining({ loadState: expect.any(Function) }),
      })
    );
    // The threaded seam IS the store's loadState (one I/O implementation, not a copy).
    const { deps } = injectSchedulingContext.mock.calls[0][1];
    await deps.loadState({ tenantId: 'TEST123', sessionId: 'sess-glue-1' });
    expect(__mockStore.loadState).toHaveBeenCalledWith({
      tenantId: 'TEST123',
      sessionId: 'sess-glue-1',
    });
  });
});

describe('D3 postFormOffer call site (wiring 3)', () => {
  const completeResponse = {
    type: 'form_complete',
    status: 'success',
    message: 'Thank you!',
    submissionId: 'sub_1',
    priority: 'normal',
    fulfillment: { type: 'email', status: 'sent' },
    applicant_contact: { email: 'jane@example.org', first_name: 'Jane' },
  };

  it('(e) form completion with email + NO session row → offer made, SSE written, internal field stripped', async () => {
    handleFormMode.mockResolvedValue(completeResponse);
    __mockStore.loadState.mockResolvedValue(null); // STRICT layer-1 guard: null row only
    postFormOffer.mockImplementation(async ({ sessionId, deps }) => {
      deps.emitSse({
        type: 'scheduling_slots',
        slots: [{ slotId: 's1', label: 'Fri, Jun 12 · 9:00 AM' }],
        session_id: sessionId,
      });
      return { offerText: OFFER_TEXT, slotsResult: { outcome: 'ok' } };
    });

    const responseStream = mockResponseStream();
    await indexModule.handler(formSubmitEvent(), responseStream, {});

    // Strict guard read happened at the call site (layer 1), then the offer ran with
    // the full deps bag (emitSse closure + state seams + propose seam).
    expect(__mockStore.loadState).toHaveBeenCalledWith({
      tenantId: 'TEST123',
      sessionId: 'sess-glue-form-1',
    });
    expect(postFormOffer).toHaveBeenCalledTimes(1);
    expect(postFormOffer).toHaveBeenCalledWith(
      expect.objectContaining({
        tenantConfig: agentConfig,
        sessionId: 'sess-glue-form-1',
        attendee: expect.objectContaining({ email: 'jane@example.org' }),
        deps: expect.objectContaining({
          emitSse: expect.any(Function),
          loadState: expect.any(Function),
          saveState: expect.any(Function),
          invokeProposal: expect.any(Function),
        }),
      })
    );

    const chunks = responseStream.getChunks().join('');
    // Form response frame → slots SSE → offer text → [DONE], in order.
    expect(chunks).toContain('form_complete');
    expect(chunks).toContain('scheduling_slots');
    expect(chunks).toContain(OFFER_TEXT);
    expect(chunks).toContain('[DONE]');
    expect(chunks.indexOf('form_complete')).toBeLessThan(chunks.indexOf('scheduling_slots'));
    expect(chunks.indexOf('scheduling_slots')).toBeLessThan(chunks.indexOf(OFFER_TEXT));
    expect(chunks.indexOf(OFFER_TEXT)).toBeLessThan(chunks.indexOf('[DONE]'));
    // The internal seam field never reaches the wire; the email is never echoed.
    expect(chunks).not.toContain('applicant_contact');
    expect(chunks).not.toContain('jane@example.org');
  });

  it('(f) form completion with an in-flight session → postFormOffer NOT called (strict layer-1 guard)', async () => {
    handleFormMode.mockResolvedValue(completeResponse);
    // A 'qualifying' row — the MODULE's layer-2 guard would allow this; the call-site
    // layer-1 guard is stricter (ANY row suppresses) and must win.
    __mockStore.loadState.mockResolvedValue({ state: 'qualifying' });

    const responseStream = mockResponseStream();
    await indexModule.handler(formSubmitEvent(), responseStream, {});

    expect(postFormOffer).not.toHaveBeenCalled();
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('form_complete');
    expect(chunks).toContain('[DONE]');
  });

  it('(f2) scheduling disabled tenant → no state read, no offer', async () => {
    loadConfig.mockResolvedValue({ ...agentConfig, feature_flags: {} });
    handleFormMode.mockResolvedValue(completeResponse);

    const responseStream = mockResponseStream();
    await indexModule.handler(formSubmitEvent(), responseStream, {});

    expect(__mockStore.loadState).not.toHaveBeenCalled();
    expect(postFormOffer).not.toHaveBeenCalled();
    expect(responseStream.getChunks().join('')).toContain('[DONE]');
  });

  it('(f3) completion without a usable email → no offer attempted', async () => {
    handleFormMode.mockResolvedValue({ ...completeResponse, applicant_contact: null });

    const responseStream = mockResponseStream();
    await indexModule.handler(formSubmitEvent(), responseStream, {});

    expect(postFormOffer).not.toHaveBeenCalled();
    expect(responseStream.getChunks().join('')).toContain('[DONE]');
  });

  it('(f4) strict-guard state read THROWS → caught at the call site, no offer, [DONE] still written', async () => {
    handleFormMode.mockResolvedValue(completeResponse);
    __mockStore.loadState.mockRejectedValue(new Error('DDB exploded'));

    const responseStream = mockResponseStream();
    await indexModule.handler(formSubmitEvent(), responseStream, {});

    expect(postFormOffer).not.toHaveBeenCalled();
    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('form_complete');
    expect(chunks).toContain('[DONE]');
    expect(responseStream.end).toHaveBeenCalled();
  });

  it('(e2) an offer-path throw is non-fatal: form response + [DONE] still complete the turn', async () => {
    handleFormMode.mockResolvedValue(completeResponse);
    __mockStore.loadState.mockResolvedValue(null);
    postFormOffer.mockRejectedValue(new Error('offer exploded'));

    const responseStream = mockResponseStream();
    await indexModule.handler(formSubmitEvent(), responseStream, {});

    const chunks = responseStream.getChunks().join('');
    expect(chunks).toContain('form_complete');
    expect(chunks).toContain('[DONE]');
    expect(chunks).not.toContain('offer exploded');
    expect(responseStream.end).toHaveBeenCalled();
  });

  it('non-final form responses (validation) never trigger the offer', async () => {
    handleFormMode.mockResolvedValue({ type: 'form_response', status: 'success', continue: true });

    const responseStream = mockResponseStream();
    await indexModule.handler(
      formSubmitEvent({ action: 'validate_field', field_id: 'f1', field_value: 'x' }),
      responseStream,
      {}
    );

    expect(postFormOffer).not.toHaveBeenCalled();
    expect(__mockStore.loadState).not.toHaveBeenCalled();
  });
});
