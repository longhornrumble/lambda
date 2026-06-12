'use strict';

/**
 * WS-AG-CORE — agentTools (§B17c executors) tests.
 *
 * Done-bar coverage (work-order item 8):
 *  - executeGetAvailableTimes success path + no_availability + lookup_failed
 *  - date → date_window; exclude_slot_ids → alreadyRejected; §B16a payload shape
 *  - executeRequestBookingConfirmation success path + unknown_slot guard +
 *    invalid_email guard + the §B17c VERBATIM-MATCH email guard (hallucinated
 *    address not in transcript → rejected; transcript-present address → accepted;
 *    session-row captured address → accepted)
 *  - no call to invokeBookingCommit in ANY code path (spy + static source assertion)
 */

const fs = require('fs');
const path = require('path');

const {
  AGENT_TOOL_DEFINITIONS,
  executeGetAvailableTimes,
  executeRequestBookingConfirmation,
} = require('../agentTools');

// ── fixtures (mirror the shipped newBookingFlow.test.js shapes) ───────────────────────

const SLOT = Object.freeze({
  slotId: 's1',
  start: '2026-06-19T19:00:00Z',
  end: '2026-06-19T19:30:00Z',
  label: 'Fri, Jun 19 · 2:00 PM',
  candidateResourceIds: ['maya@org.example'],
});
const SLOT2 = Object.freeze({
  slotId: 's2',
  start: '2026-06-19T21:00:00Z',
  end: '2026-06-19T21:30:00Z',
  label: 'Fri, Jun 19 · 4:00 PM',
  candidateResourceIds: ['maya@org.example'],
});

const PROPOSE_OK = Object.freeze({
  outcome: 'ok',
  slots: [SLOT, SLOT2],
  poolSize: 2,
  tieBreaker: 'round_robin',
  roundRobinCursor: 'cur-7',
});

const QCTX = Object.freeze({
  appointmentTypeId: 'apt_intro',
  userTimeZone: 'America/Chicago',
  conference_type: 'google_meet',
  appointment_type: { id: 'apt_intro', name: 'Intro call', duration_minutes: 30 },
});

const CONFIG = Object.freeze({
  tenant_id: 'TEN',
  feature_flags: { scheduling_enabled: true, AGENTIC_SCHEDULING: true },
  scheduling: { appointment_types: { apt_intro: { timezone: 'America/Chicago' } } },
});

// Parse SSE frames captured by a jest.fn() write.
function frames(write) {
  return write.mock.calls
    .map(([s]) => s)
    .filter((s) => typeof s === 'string' && s.startsWith('data: {'))
    .map((s) => JSON.parse(s.slice('data: '.length)));
}

const quietLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };

function tool1Args(overrides = {}) {
  return {
    input: {},
    session: { state: 'qualifying', session_id: 'sess-1' },
    tenantId: 'TEN',
    sessionId: 'sess-1',
    tenantConfig: CONFIG,
    deps: {
      invokeProposal: jest.fn().mockResolvedValue(PROPOSE_OK),
      saveState: jest.fn().mockResolvedValue(undefined),
      qualifyingContext: QCTX,
      logger: quietLogger,
    },
    write: jest.fn(),
    setSession: jest.fn(),
    ...overrides,
  };
}

function tool2Args(overrides = {}) {
  return {
    input: { slot_id: 's1', attendee_email: 'vol@example.com' },
    session: {
      state: 'proposing',
      session_id: 'sess-1',
      candidate_slots: [SLOT, SLOT2],
      proposal: { poolSize: 2 },
      rejected_slot_ids: [],
    },
    tenantId: 'TEN',
    sessionId: 'sess-1',
    tenantConfig: CONFIG,
    deps: { saveState: jest.fn().mockResolvedValue(undefined), logger: quietLogger },
    write: jest.fn(),
    userTranscript: ['hi', 'sure — vol@example.com works'],
    setSession: jest.fn(),
    ...overrides,
  };
}

// ── tool definitions (the catalog the model sees) ─────────────────────────────────────

describe('AGENT_TOOL_DEFINITIONS (§B17c)', () => {
  test('exactly the two v1 tools, names + required args pinned', () => {
    expect(AGENT_TOOL_DEFINITIONS.map((t) => t.name)).toEqual([
      'get_available_times',
      'request_booking_confirmation',
    ]);
    expect(AGENT_TOOL_DEFINITIONS[0].input_schema.required).toEqual([]);
    expect(AGENT_TOOL_DEFINITIONS[1].input_schema.required).toEqual(['slot_id', 'attendee_email']);
  });

  test('descriptions carry the §B17c anti-hallucination instructions', () => {
    expect(AGENT_TOOL_DEFINITIONS[0].description).toContain('Never invent times');
    expect(AGENT_TOOL_DEFINITIONS[1].description).toContain('does NOT book');
  });
});

// ── TOOL 1: executeGetAvailableTimes ──────────────────────────────────────────────────

describe('executeGetAvailableTimes', () => {
  test('success: wraps §B16a invokeProposal, persists candidates, emits scheduling_slots, returns §B17c shape', async () => {
    const args = tool1Args();
    const result = await executeGetAvailableTimes(args);

    // §B16a payload (server-derived fields — never model args)
    expect(args.deps.invokeProposal).toHaveBeenCalledWith({
      action: 'scheduling_propose',
      tenantId: 'TEN',
      sessionId: 'sess-1',
      appointmentTypeId: 'apt_intro',
      userTimeZone: 'America/Chicago',
      alreadyRejected: [],
    });

    // persists candidate_slots (state 'proposing', same shape as the deterministic _propose)
    expect(args.deps.saveState).toHaveBeenCalledWith({
      tenantId: 'TEN',
      sessionId: 'sess-1',
      state: 'proposing',
      candidate_slots: [SLOT, SLOT2],
      proposal: { poolSize: 2, tieBreaker: 'round_robin', roundRobinCursor: 'cur-7' },
      rejected_slot_ids: [],
    });

    // emits the SHIPPED scheduling_slots SSE (unchanged widget contract)
    const slotEvents = frames(args.write).filter((f) => f.type === 'scheduling_slots');
    expect(slotEvents).toHaveLength(1);
    expect(slotEvents[0]).toEqual({ type: 'scheduling_slots', slots: [SLOT, SLOT2], session_id: 'sess-1' });

    // §B17c output to model: slot_id + label + starts_at_iso; GENERIC (no coordinator identity)
    expect(result).toEqual({
      slots: [
        { slot_id: 's1', label: SLOT.label, starts_at_iso: SLOT.start },
        { slot_id: 's2', label: SLOT2.label, starts_at_iso: SLOT2.start },
      ],
      user_time_zone: 'America/Chicago',
      note: expect.any(String),
    });
    expect(JSON.stringify(result)).not.toContain('candidateResourceIds');
    expect(JSON.stringify(result)).not.toContain('maya@org.example');

    // live-session threading for the same-turn staging call
    expect(args.setSession).toHaveBeenCalledWith(
      expect.objectContaining({ state: 'proposing', candidate_slots: [SLOT, SLOT2] })
    );
  });

  test('date arg → §B16e date_window passthrough', async () => {
    const args = tool1Args({ input: { date: '2026-06-19' } });
    await executeGetAvailableTimes(args);
    expect(args.deps.invokeProposal).toHaveBeenCalledWith(
      expect.objectContaining({
        date_window: { start: '2026-06-19T00:00:00.000Z', end: '2026-06-20T00:00:00.000Z' },
      })
    );
  });

  test('invalid model-supplied date → lookup_failed, no propose invoke', async () => {
    const args = tool1Args({ input: { date: '2026-02-31' } });
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('lookup_failed');
    expect(args.deps.invokeProposal).not.toHaveBeenCalled();
  });

  test('exclude_slot_ids → alreadyRejected (sanitized to strings)', async () => {
    const args = tool1Args({ input: { exclude_slot_ids: ['s1', 42, null, 's2'] } });
    await executeGetAvailableTimes(args);
    expect(args.deps.invokeProposal).toHaveBeenCalledWith(
      expect.objectContaining({ alreadyRejected: ['s1', 's2'] })
    );
  });

  test('exclude_slot_ids bounds: oversized ids dropped, list capped at 100 (payload hygiene)', async () => {
    const huge = Array.from({ length: 150 }, (_, i) => `s${i}`);
    const args = tool1Args({ input: { exclude_slot_ids: ['x'.repeat(201), ...huge] } });
    await executeGetAvailableTimes(args);
    const sent = args.deps.invokeProposal.mock.calls[0][0].alreadyRejected;
    expect(sent).toHaveLength(100);
    expect(sent[0]).toBe('s0'); // the 201-char id was dropped before the cap
  });

  test('no_availability outcome → { error: no_availability, note }', async () => {
    const args = tool1Args();
    args.deps.invokeProposal.mockResolvedValue({ outcome: 'no_availability', slots: [] });
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('no_availability');
    expect(result.note).toEqual(expect.any(String));
    expect(args.deps.saveState).not.toHaveBeenCalled();
    expect(frames(args.write)).toHaveLength(0);
  });

  test("outcome 'failed' → lookup_failed", async () => {
    const args = tool1Args();
    args.deps.invokeProposal.mockResolvedValue({ outcome: 'failed', error: 'boom' });
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('lookup_failed');
  });

  test('propose invoke throws → lookup_failed (non-fatal; err.name-only logging)', async () => {
    const args = tool1Args();
    args.deps.invokeProposal.mockRejectedValue(new Error('socket hang up'));
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('lookup_failed');
    // the §B17e wording hook: the note steers the model away from "no access" claims
    expect(result.note).toContain('never that you lack scheduling access');
  });

  test('propose seam unwired → lookup_failed', async () => {
    const args = tool1Args();
    delete args.deps.invokeProposal;
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('lookup_failed');
  });

  test('a previously captured attendee_email survives the re-propose saveState (§B16d amendment)', async () => {
    const args = tool1Args({
      session: { state: 'proposing', session_id: 'sess-1', attendee_email: 'vol@example.com' },
    });
    await executeGetAvailableTimes(args);
    expect(args.deps.saveState).toHaveBeenCalledWith(
      expect.objectContaining({ attendee_email: 'vol@example.com' })
    );
  });

  test("illegal live state (booked) → lookup_failed; the model cannot re-open a finished arc", async () => {
    const args = tool1Args({ session: { state: 'booked', session_id: 'sess-1' } });
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('lookup_failed');
    expect(args.deps.invokeProposal).not.toHaveBeenCalled();
  });

  test('no prior session row → initialization in proposing is allowed', async () => {
    const args = tool1Args({ session: null });
    const result = await executeGetAvailableTimes(args);
    expect(result.slots).toHaveLength(2);
  });

  test('falls back to resolveQualifyingContext over the tenant config when deps.qualifyingContext is absent', async () => {
    const args = tool1Args();
    delete args.deps.qualifyingContext;
    await executeGetAvailableTimes(args);
    expect(args.deps.invokeProposal).toHaveBeenCalledWith(
      expect.objectContaining({ appointmentTypeId: 'apt_intro', userTimeZone: 'America/Chicago' })
    );
  });

  test('forwards the configured availability window (§B16a parity with _propose; omit when absent)', async () => {
    const args = tool1Args({
      deps: {
        invokeProposal: jest.fn().mockResolvedValue(PROPOSE_OK),
        saveState: jest.fn(),
        qualifyingContext: { ...QCTX, windowStart: '2026-06-15T00:00:00Z', window_end: '2026-07-15T00:00:00Z' },
        logger: quietLogger,
      },
    });
    await executeGetAvailableTimes(args);
    expect(args.deps.invokeProposal).toHaveBeenCalledWith(
      expect.objectContaining({ windowStart: '2026-06-15T00:00:00Z', windowEnd: '2026-07-15T00:00:00Z' })
    );
    // and the base success case (tool1Args default) sends NO window keys
    const bare = tool1Args();
    await executeGetAvailableTimes(bare);
    const payload = bare.deps.invokeProposal.mock.calls[0][0];
    expect(payload).not.toHaveProperty('windowStart');
    expect(payload).not.toHaveProperty('windowEnd');
  });

  test("outcome 'ok' with an empty slots array → lookup_failed (never an empty chip strip)", async () => {
    const args = tool1Args();
    args.deps.invokeProposal.mockResolvedValue({ outcome: 'ok', slots: [], poolSize: 0 });
    const result = await executeGetAvailableTimes(args);
    expect(result.error).toBe('lookup_failed');
    expect(args.deps.saveState).not.toHaveBeenCalled();
    expect(frames(args.write)).toHaveLength(0);
  });
});

// ── TOOL 2: executeRequestBookingConfirmation ─────────────────────────────────────────

describe('executeRequestBookingConfirmation', () => {
  test('success: validates, stages via the SAME saveState path, emits scheduling_confirm, returns { staged, label }', async () => {
    const args = tool2Args();
    const result = await executeRequestBookingConfirmation(args);

    expect(result).toEqual({ staged: true, label: SLOT.label });

    // the SAME staging path the deterministic pipeline uses (state→confirming,
    // selected_slot, attendee_email; propose metadata carried forward)
    expect(args.deps.saveState).toHaveBeenCalledWith({
      tenantId: 'TEN',
      sessionId: 'sess-1',
      state: 'confirming',
      selected_slot: {
        slotId: 's1',
        start: SLOT.start,
        end: SLOT.end,
        candidateResourceIds: SLOT.candidateResourceIds,
      },
      candidate_slots: [SLOT, SLOT2],
      proposal: { poolSize: 2 },
      rejected_slot_ids: [],
      attendee_email: 'vol@example.com',
    });

    // the SHIPPED scheduling_confirm SSE (server-driven confirm card, picasso#538)
    const confirmEvents = frames(args.write).filter((f) => f.type === 'scheduling_confirm');
    expect(confirmEvents).toEqual([
      {
        type: 'scheduling_confirm',
        session_id: 'sess-1',
        slot: { slotId: 's1', label: SLOT.label },
        attendee_email: 'vol@example.com',
      },
    ]);

    expect(args.setSession).toHaveBeenCalledWith(
      expect.objectContaining({ state: 'confirming', attendee_email: 'vol@example.com' })
    );
  });

  test('unknown_slot: slot_id not in persisted candidate_slots → rejected, no write, no SSE', async () => {
    const args = tool2Args({ input: { slot_id: 'fabricated', attendee_email: 'vol@example.com' } });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ error: 'unknown_slot' });
    expect(args.deps.saveState).not.toHaveBeenCalled();
    expect(frames(args.write)).toHaveLength(0);
  });

  test('unknown_slot: no candidates at all (no session)', async () => {
    const args = tool2Args({ session: null });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ error: 'unknown_slot' });
  });

  test('invalid_email: shape failures rejected by the imported EMAIL_SHAPE', async () => {
    for (const bad of ['not-an-email', '"; DROP TABLE bookings; --', 'a@b', '<vol@example.com>', '']) {
      const args = tool2Args({ input: { slot_id: 's1', attendee_email: bad } });
      const result = await executeRequestBookingConfirmation(args);
      expect(result).toEqual({ error: 'invalid_email' });
      expect(args.deps.saveState).not.toHaveBeenCalled();
      expect(frames(args.write)).toHaveLength(0);
    }
  });

  test('§B17c VERBATIM-MATCH guard: a hallucinated (well-formed) address not in the transcript → invalid_email', async () => {
    const args = tool2Args({
      input: { slot_id: 's1', attendee_email: 'invented@model.example' },
      userTranscript: ['hi', 'book me for friday'], // user never typed an email
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ error: 'invalid_email' });
    expect(args.deps.saveState).not.toHaveBeenCalled();
  });

  test('§B17c VERBATIM-MATCH guard: a transcript-present address → accepted', async () => {
    const args = tool2Args({
      input: { slot_id: 's1', attendee_email: 'jane@acme.com' },
      userTranscript: ['use my work email jane@acme.com instead'],
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ staged: true, label: SLOT.label });
  });

  test('§B17c VERBATIM-MATCH guard: equals the session-row captured attendee_email → accepted', async () => {
    const args = tool2Args({
      input: { slot_id: 's1', attendee_email: 'captured@example.com' },
      session: {
        state: 'confirming',
        session_id: 'sess-1',
        candidate_slots: [SLOT, SLOT2],
        attendee_email: 'captured@example.com', // captured by the deterministic pipeline
      },
      userTranscript: ['hello'], // not in this transcript window
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ staged: true, label: SLOT.label });
  });

  test('re-stage from confirming (A5 — "use my work email instead") is allowed', async () => {
    const args = tool2Args({
      input: { slot_id: 's2', attendee_email: 'jane@acme.com' },
      session: {
        state: 'confirming',
        session_id: 'sess-1',
        candidate_slots: [SLOT, SLOT2],
        selected_slot: { slotId: 's1', start: SLOT.start, end: SLOT.end },
        attendee_email: 'vol@example.com',
      },
      userTranscript: ['use my work email jane@acme.com instead'],
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ staged: true, label: SLOT2.label });
    expect(args.deps.saveState).toHaveBeenCalledWith(
      expect.objectContaining({ selected_slot: expect.objectContaining({ slotId: 's2' }), attendee_email: 'jane@acme.com' })
    );
  });

  test('fail-closed state gate: staging from a booked session is rejected (unknown_slot)', async () => {
    const args = tool2Args({
      session: { state: 'booked', session_id: 'sess-1', candidate_slots: [SLOT, SLOT2] },
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ error: 'unknown_slot' });
    expect(args.deps.saveState).not.toHaveBeenCalled();
  });
});

// ── §B17b NO TOOL BOOKS — the commit seam is unreachable ──────────────────────────────

describe('NO TOOL BOOKS (§B17b/§B17c)', () => {
  test('invokeBookingCommit is never referenced in agentTools.js or agentTurn.js (static)', () => {
    for (const file of ['agentTools.js', 'agentTurn.js', 'sensitiveContext.js']) {
      const src = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
      expect(src).not.toMatch(/invokeBookingCommit/);
    }
  });

  test('a wired invokeBookingCommit dep is never called on any executor path (spy)', async () => {
    const commitSpy = jest.fn();

    const okArgs = tool1Args();
    okArgs.deps.invokeBookingCommit = commitSpy;
    await executeGetAvailableTimes(okArgs);

    const stageArgs = tool2Args();
    stageArgs.deps.invokeBookingCommit = commitSpy;
    await executeRequestBookingConfirmation(stageArgs);

    const failArgs = tool2Args({ input: { slot_id: 'nope', attendee_email: 'vol@example.com' } });
    failArgs.deps.invokeBookingCommit = commitSpy;
    await executeRequestBookingConfirmation(failArgs);

    expect(commitSpy).not.toHaveBeenCalled();
  });
});
