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

  test('alreadyRejected ACCUMULATES the persisted rejected_slot_ids with the model arg (§B16b rule)', async () => {
    const args = tool1Args({
      input: { exclude_slot_ids: ['s2', 's3'] },
      session: { state: 'proposing', session_id: 'sess-1', rejected_slot_ids: ['s1', 's2'] },
    });
    await executeGetAvailableTimes(args);
    expect(args.deps.invokeProposal.mock.calls[0][0].alreadyRejected.sort()).toEqual(['s1', 's2', 's3']);
  });

  test('no resolvable appointment type → the seam is STILL invoked (resolution lives behind §B16a), key omitted', async () => {
    // Mirrors the §B17 eval fixtures (agentEvals A1–A3/A12): bare flags-on config, no
    // scheduling block, no deps.qualifyingContext — the executor must not pre-empt the seam.
    const args = tool1Args({ tenantConfig: { tenant_id: 'TEN', feature_flags: { scheduling_enabled: true } } });
    delete args.deps.qualifyingContext;
    const result = await executeGetAvailableTimes(args);
    expect(args.deps.invokeProposal).toHaveBeenCalledTimes(1);
    expect(args.deps.invokeProposal.mock.calls[0][0]).not.toHaveProperty('appointmentTypeId');
    expect(result.slots).toHaveLength(2);
  });

  test('exclude_slot_ids bounds: oversized ids dropped, list capped at 100 (payload hygiene)', async () => {
    const huge = Array.from({ length: 150 }, (_, i) => `s${i}`);
    const args = tool1Args({ input: { exclude_slot_ids: ['x'.repeat(201), ...huge] } });
    await executeGetAvailableTimes(args);
    const sent = args.deps.invokeProposal.mock.calls[0][0].alreadyRejected;
    expect(sent).toHaveLength(100);
    expect(sent[0]).toBe('s0'); // the 201-char id was dropped before the cap
  });

  test("F5 (eval A2/A3): re-lookup returning the SAME slots → note says SAME results, never 'new availability'", async () => {
    const args = tool1Args({
      session: { state: 'proposing', session_id: 'sess-1', candidate_slots: [SLOT, SLOT2] },
    });
    const result = await executeGetAvailableTimes(args);
    expect(result.note).toContain('SAME results as before');
    expect(result.note).toContain('nothing else is open');
  });

  test('F5: a lookup returning DIFFERENT slots keeps the normal note', async () => {
    const args = tool1Args({
      session: { state: 'proposing', session_id: 'sess-1', candidate_slots: [SLOT] },
    });
    const result = await executeGetAvailableTimes(args); // PROPOSE_OK returns [SLOT, SLOT2]
    expect(result.note).not.toContain('SAME results as before');
    expect(result.note).toContain('only real, bookable times');
  });

  test('F5: first lookup of the session (no prior candidates) keeps the normal note', async () => {
    const args = tool1Args(); // session has no candidate_slots
    const result = await executeGetAvailableTimes(args);
    expect(result.note).not.toContain('SAME results as before');
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

  // ── F6: same-turn multi-day union (live defect 2026-06-11 — second dated call's
  // PutItem replaced the first day's candidate_slots → unknown_slot on a first-day chip)
  describe('F6 same-turn candidate union (turnCandidates accumulator)', () => {
    const MON_SLOT = Object.freeze({
      slotId: 'm1',
      start: '2026-06-15T14:00:00Z',
      end: '2026-06-15T14:30:00Z',
      label: 'Mon, Jun 15 · 9:00 AM',
      candidateResourceIds: ['maya@org.example'],
    });

    test('second same-turn call UNIONs with the accumulator: prior slots first (§B16b order), new appended; accumulator updated', async () => {
      const turnCandidates = { slots: [MON_SLOT] };
      const args = tool1Args({
        session: { state: 'proposing', session_id: 'sess-1', candidate_slots: [MON_SLOT] },
        turnCandidates,
      });
      const result = await executeGetAvailableTimes(args); // PROPOSE_OK → [SLOT, SLOT2]

      // persisted row = the UNION (both days stageable after the turn)
      expect(args.deps.saveState).toHaveBeenCalledWith(
        expect.objectContaining({ candidate_slots: [MON_SLOT, SLOT, SLOT2] })
      );
      // live-session threading sees the union too (same-turn staging of EITHER day)
      expect(args.setSession).toHaveBeenCalledWith(
        expect.objectContaining({ candidate_slots: [MON_SLOT, SLOT, SLOT2] })
      );
      // accumulator carries the union forward for a later same-turn call
      expect(turnCandidates.slots).toEqual([MON_SLOT, SLOT, SLOT2]);
      // SSE + model result stay PER-CALL (the widget merges; the model asked about THIS day)
      const slotEvents = frames(args.write).filter((f) => f.type === 'scheduling_slots');
      expect(slotEvents).toHaveLength(1);
      expect(slotEvents[0].slots).toEqual([SLOT, SLOT2]);
      expect(result.slots.map((s) => s.slot_id)).toEqual(['s1', 's2']);
    });

    test('union dedupes by slotId (first occurrence wins)', async () => {
      const turnCandidates = { slots: [SLOT] };
      const args = tool1Args({ turnCandidates });
      await executeGetAvailableTimes(args); // PROPOSE_OK → [SLOT, SLOT2]; SLOT overlaps
      expect(args.deps.saveState).toHaveBeenCalledWith(
        expect.objectContaining({ candidate_slots: [SLOT, SLOT2] })
      );
      expect(turnCandidates.slots).toEqual([SLOT, SLOT2]);
    });

    test('union caps the persisted candidates at 10', async () => {
      const prior = Array.from({ length: 9 }, (_, i) => ({
        slotId: `p${i}`,
        start: `2026-06-15T1${i}:00:00Z`,
        end: `2026-06-15T1${i}:30:00Z`,
        label: `Mon · slot ${i}`,
      }));
      const turnCandidates = { slots: prior };
      const args = tool1Args({ turnCandidates });
      await executeGetAvailableTimes(args); // PROPOSE_OK → [SLOT, SLOT2]; union would be 11
      const saved = args.deps.saveState.mock.calls[0][0].candidate_slots;
      expect(saved).toHaveLength(10);
      expect(saved.slice(0, 9)).toEqual(prior); // earlier-persisted slots win the cap
      expect(saved[9]).toEqual(SLOT);
      expect(turnCandidates.slots).toHaveLength(10);
    });

    test('prior-TURN candidates (empty accumulator) are still REPLACED — the union is same-turn only', async () => {
      const args = tool1Args({
        // MON_SLOT was persisted by an EARLIER turn → this lookup replaces it (§B16b
        // re-propose semantics unchanged).
        session: { state: 'proposing', session_id: 'sess-1', candidate_slots: [MON_SLOT] },
        turnCandidates: { slots: null },
      });
      await executeGetAvailableTimes(args);
      expect(args.deps.saveState).toHaveBeenCalledWith(
        expect.objectContaining({ candidate_slots: [SLOT, SLOT2] })
      );
    });

    test('malformed accumulator entries (no slotId / null) are skipped, never persisted', async () => {
      const turnCandidates = { slots: [null, { label: 'orphan, no slotId' }, MON_SLOT] };
      const args = tool1Args({ turnCandidates });
      await executeGetAvailableTimes(args); // PROPOSE_OK → [SLOT, SLOT2]
      expect(args.deps.saveState).toHaveBeenCalledWith(
        expect.objectContaining({ candidate_slots: [MON_SLOT, SLOT, SLOT2] })
      );
    });

    test('no turnCandidates param at all (direct caller) → unchanged replace behavior', async () => {
      const args = tool1Args({
        session: { state: 'proposing', session_id: 'sess-1', candidate_slots: [MON_SLOT] },
      });
      await executeGetAvailableTimes(args);
      expect(args.deps.saveState).toHaveBeenCalledWith(
        expect.objectContaining({ candidate_slots: [SLOT, SLOT2] })
      );
    });
  });

  // ── day-part bounds (after_time/before_time — live defect 2026-06-12) ────────────────
  describe('day-part bounds (after_time/before_time → tz-resolved date_window instants)', () => {
    test("CT afternoon: date + after 12:00 + before 17:00 → 17:00Z–22:00Z (June, CDT = UTC-5)", async () => {
      const args = tool1Args({ input: { date: '2026-06-19', after_time: '12:00', before_time: '17:00' } });
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T17:00:00.000Z', end: '2026-06-19T22:00:00.000Z' },
        })
      );
    });

    test('appointment-type timezone wins over the qctx user timezone (today-line precedence)', async () => {
      const args = tool1Args({
        input: { date: '2026-06-19', after_time: '12:00' },
        deps: {
          invokeProposal: jest.fn().mockResolvedValue(PROPOSE_OK),
          saveState: jest.fn().mockResolvedValue(undefined),
          qualifyingContext: {
            ...QCTX,
            appointment_type: { ...QCTX.appointment_type, timezone: 'America/New_York' },
          },
          logger: quietLogger,
        },
      });
      await executeGetAvailableTimes(args);
      // EDT = UTC-4; implicit end widens to the civil-day end IN THE BOUNDS TZ
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T16:00:00.000Z', end: '2026-06-20T04:00:00.000Z' },
        })
      );
    });

    test('after-only: implicit end = next-day civil midnight in the bounds tz (evenings not clipped by the UTC-day end)', async () => {
      const args = tool1Args({ input: { date: '2026-06-19', after_time: '17:00' } });
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T22:00:00.000Z', end: '2026-06-20T05:00:00.000Z' },
        })
      );
    });

    test('before-only: implicit start = the civil-day start in the bounds tz', async () => {
      const args = tool1Args({ input: { date: '2026-06-19', before_time: '12:00' } });
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T05:00:00.000Z', end: '2026-06-19T17:00:00.000Z' },
        })
      );
    });

    test('DST fall-back day (2026-11-01, Chicago): afternoon 12:00–17:00 = 18:00Z–23:00Z (CST = UTC-6 after the 2am change)', async () => {
      const args = tool1Args({ input: { date: '2026-11-01', after_time: '12:00', before_time: '17:00' } });
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-11-01T18:00:00.000Z', end: '2026-11-01T23:00:00.000Z' },
        })
      );
    });

    test('DST spring-forward day (2027-03-14, Chicago): after 12:00 = 17:00Z (CDT); the nonexistent 02:30 → invalid_time', async () => {
      const ok = tool1Args({ input: { date: '2027-03-14', after_time: '12:00' } });
      await executeGetAvailableTimes(ok);
      expect(ok.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2027-03-14T17:00:00.000Z', end: '2027-03-15T05:00:00.000Z' },
        })
      );
      // 02:30 does not exist on the spring-forward day → structured invalid_time, no lookup
      const gap = tool1Args({ input: { date: '2027-03-14', after_time: '02:30' } });
      const result = await executeGetAvailableTimes(gap);
      expect(result.error).toBe('invalid_time');
      expect(typeof result.note).toBe('string');
      expect(gap.deps.invokeProposal).not.toHaveBeenCalled();
    });

    test('DST-at-midnight zone (America/Santiago, DST starts 2026-09-06 00:00): implicit start edge keeps the conservative UTC edge', async () => {
      // Chile's spring-forward happens AT midnight — 2026-09-06 00:00 does not exist
      // (clocks jump 00:00 → 01:00). The implicit start edge must fall back to the
      // shipped UTC-day edge instead of failing; the explicit 12:00 bound resolves
      // post-transition (CLST = UTC-3) → 15:00Z.
      const args = tool1Args({
        input: { date: '2026-09-06', before_time: '12:00' },
        deps: {
          invokeProposal: jest.fn().mockResolvedValue(PROPOSE_OK),
          saveState: jest.fn().mockResolvedValue(undefined),
          qualifyingContext: {
            ...QCTX,
            appointment_type: { ...QCTX.appointment_type, timezone: 'America/Santiago' },
          },
          logger: quietLogger,
        },
      });
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-09-06T00:00:00.000Z', end: '2026-09-06T15:00:00.000Z' },
        })
      );
    });

    test('invalid HH:MM shapes → invalid_time, no propose invoke, nothing persisted', async () => {
      for (const bad of ['noon', '25:00', '24:00', '12:60', '12:5', '1230', '12:00pm', '', 9]) {
        const args = tool1Args({ input: { date: '2026-06-19', after_time: bad } });
        const result = await executeGetAvailableTimes(args);
        expect(result).toEqual(expect.objectContaining({ error: 'invalid_time' }));
        expect(args.deps.invokeProposal).not.toHaveBeenCalled();
        expect(args.deps.saveState).not.toHaveBeenCalled();
        const before = tool1Args({ input: { date: '2026-06-19', before_time: bad } });
        expect((await executeGetAvailableTimes(before)).error).toBe('invalid_time');
        expect(before.deps.invokeProposal).not.toHaveBeenCalled();
      }
    });

    test('inverted bounds (after ≥ before) → invalid_time, never a guaranteed-empty lookup', async () => {
      for (const input of [
        { date: '2026-06-19', after_time: '17:00', before_time: '12:00' },
        { date: '2026-06-19', after_time: '12:00', before_time: '12:00' },
      ]) {
        const args = tool1Args({ input });
        const result = await executeGetAvailableTimes(args);
        expect(result.error).toBe('invalid_time');
        expect(args.deps.invokeProposal).not.toHaveBeenCalled();
      }
    });

    test('bounds without a date apply to TODAY in the bounds tz (deps.nowMs clock)', async () => {
      // 2026-06-19T15:00:00Z = 10:00 AM CDT, Friday June 19 — today is 2026-06-19
      const args = tool1Args({ input: { after_time: '12:00' } });
      args.deps.nowMs = Date.parse('2026-06-19T15:00:00Z');
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T17:00:00.000Z', end: '2026-06-20T05:00:00.000Z' },
        })
      );
    });

    test("TODAY resolves as the CIVIL date in the bounds tz, not UTC (late Chicago evening = next day UTC)", async () => {
      // 2026-06-20T03:00:00Z is still Friday June 19, 10:00 PM in America/Chicago
      const args = tool1Args({ input: { before_time: '12:00' } });
      args.deps.nowMs = Date.parse('2026-06-20T03:00:00Z');
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T05:00:00.000Z', end: '2026-06-19T17:00:00.000Z' },
        })
      );
    });

    test('garbage deps.nowMs (non-finite) falls back to the real clock — never throws (mirrors the agentTurn guard)', async () => {
      const args = tool1Args({ input: { after_time: '12:00' } });
      args.deps.nowMs = 'not-a-clock';
      const result = await executeGetAvailableTimes(args);
      expect(result.error).toBeUndefined(); // lookup proceeded on the real clock
      const dw = args.deps.invokeProposal.mock.calls[0][0].date_window;
      expect(Number.isNaN(Date.parse(dw.start))).toBe(false);
      expect(Number.isNaN(Date.parse(dw.end))).toBe(false);
      expect(Date.parse(dw.start)).toBeLessThan(Date.parse(dw.end));
    });

    test('date WITHOUT bounds keeps the shipped §B16e full-UTC-day mapping (byte-identical)', async () => {
      const args = tool1Args({ input: { date: '2026-06-19' } });
      await executeGetAvailableTimes(args);
      expect(args.deps.invokeProposal).toHaveBeenCalledWith(
        expect.objectContaining({
          date_window: { start: '2026-06-19T00:00:00.000Z', end: '2026-06-20T00:00:00.000Z' },
        })
      );
    });

    test('F6 union across a morning call + bounded afternoon call: earliest-first, deduped, capped at 10', async () => {
      // 8 morning slots persisted by THIS turn's earlier (unbounded) call…
      const morning = Array.from({ length: 8 }, (_, i) => ({
        slotId: `am${i}`,
        start: `2026-06-19T1${i}:00:00Z`,
        end: `2026-06-19T1${i}:30:00Z`,
        label: `Fri · morning ${i}`,
      }));
      // …then the bounded afternoon call returns 7 slots, one duplicating am7 → 15 raw,
      // 14 after dedupe, capped at 10.
      const afternoon = [
        { ...morning[7] }, // duplicate slotId — first occurrence must win
        ...Array.from({ length: 6 }, (_, i) => ({
          slotId: `pm${i}`,
          start: `2026-06-19T${18 + i}:00:00Z`,
          end: `2026-06-19T${18 + i}:30:00Z`,
          label: `Fri · afternoon ${i}`,
        })),
      ];
      const turnCandidates = { slots: morning };
      const args = tool1Args({
        input: { date: '2026-06-19', after_time: '12:00' },
        session: { state: 'proposing', session_id: 'sess-1', candidate_slots: morning },
        turnCandidates,
      });
      args.deps.invokeProposal = jest.fn().mockResolvedValue({ outcome: 'ok', slots: afternoon, poolSize: 2 });
      await executeGetAvailableTimes(args);

      const saved = args.deps.saveState.mock.calls[0][0].candidate_slots;
      expect(saved).toHaveLength(10);
      expect(saved.slice(0, 8)).toEqual(morning); // earliest-first: prior call's slots lead
      expect(saved.map((s) => s.slotId).filter((id) => id === 'am7')).toHaveLength(1); // deduped
      expect(saved.slice(8).map((s) => s.slotId)).toEqual(['pm0', 'pm1']); // first fresh afternoon slots fill the cap
      expect(turnCandidates.slots).toHaveLength(10);
    });
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

  test('§B17c VERBATIM-MATCH guard is case-insensitive: user typed vol@example.com, model supplies VOL@EXAMPLE.COM → accepted', async () => {
    const args = tool2Args({
      input: { slot_id: 's1', attendee_email: 'VOL@EXAMPLE.COM' },
      userTranscript: ['hi', 'sure — vol@example.com works'],
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ staged: true, label: SLOT.label });
    // staged lowercase-normalized
    expect(args.deps.saveState).toHaveBeenCalledWith(
      expect.objectContaining({ attendee_email: 'vol@example.com' })
    );
  });

  test('§B17c VERBATIM-MATCH guard: an address the user never typed in ANY case → invalid_email', async () => {
    const args = tool2Args({
      input: { slot_id: 's1', attendee_email: 'Invented@Model.Example' },
      userTranscript: ['hi', 'book me for friday'], // no email in any casing
    });
    const result = await executeRequestBookingConfirmation(args);
    expect(result).toEqual({ error: 'invalid_email' });
    expect(args.deps.saveState).not.toHaveBeenCalled();
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

// ─── §B18b context forwarding: executeGetAvailableTimes old-shape fixture ────────────

describe('executeGetAvailableTimes — §B18b context forwarding (old-shape fixture)', () => {
  test('propose result WITH context → scheduling_slots SSE carries context', async () => {
    const proposeWithCtx = jest.fn().mockResolvedValue({
      ...PROPOSE_OK,
      context: { duration_minutes: 30, conference_type: 'google_meet', conference_label: 'Google Meet', tz_label: 'Central Time' },
    });
    const args = tool1Args({ deps: { invokeProposal: proposeWithCtx } });
    await executeGetAvailableTimes(args);
    const slotEvents = frames(args.write).filter((f) => f.type === 'scheduling_slots');
    expect(slotEvents.length).toBeGreaterThan(0);
    expect(slotEvents[0].context).toEqual({ duration_minutes: 30, conference_type: 'google_meet', conference_label: 'Google Meet', tz_label: 'Central Time' });
  });

  test('propose result WITHOUT context (old shape) → no context key on SSE, no crash', async () => {
    // PROPOSE_OK has no context field — old shape
    const args = tool1Args();
    await executeGetAvailableTimes(args);
    const slotEvents = frames(args.write).filter((f) => f.type === 'scheduling_slots');
    expect(slotEvents.length).toBeGreaterThan(0);
    expect(slotEvents[0]).not.toHaveProperty('context');
  });
});
