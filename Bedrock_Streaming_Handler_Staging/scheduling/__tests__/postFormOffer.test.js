'use strict';

/**
 * postFormOffer — Track-D fix 3 (WS-TRACKD-BE) tests. Work-order item 7:
 *  - success: outcome:'ok' with 3 slots → offerText non-null + scheduling_slots SSE
 *    emitted + attendee_email persisted on the session row
 *  - no availability → offerText non-null warm copy + NO SSE
 *  - failed → offerText null (offer suppressed)
 *  - attendee_email is NOT logged (no email in any emitted audit event)
 * Plus the §B14 boundary: never commits, never stages 'confirming'/'booked'.
 */

const { postFormOffer, OFFER_TEXT_OK, OFFER_TEXT_NO_AVAILABILITY, IN_FLIGHT_GUARD_STATES } = require('../postFormOffer');

const EMAIL = 'jane.doe+app@acme-volunteers.org';

const TENANT_CONFIG = {
  tenant_id: 'TEN123',
  feature_flags: { scheduling_enabled: true },
  scheduling: {
    appointment_types: {
      intro_call: { name: 'Intro Call', timezone: 'America/Chicago' },
    },
  },
};

const SLOTS_3 = [
  { slotId: 's1', start: '2026-06-15T14:30:00Z', end: '2026-06-15T15:00:00Z', label: 'Mon Jun 15 9:30', candidateResourceIds: ['r1'] },
  { slotId: 's2', start: '2026-06-15T16:00:00Z', end: '2026-06-15T16:30:00Z', label: 'Mon Jun 15 11:00', candidateResourceIds: ['r1'] },
  { slotId: 's3', start: '2026-06-16T14:30:00Z', end: '2026-06-16T15:00:00Z', label: 'Tue Jun 16 9:30', candidateResourceIds: ['r2'] },
];

const OK_RESULT = { outcome: 'ok', slots: SLOTS_3, poolSize: 2, tieBreaker: 'tb', roundRobinCursor: 1 };

function makeDeps(overrides = {}) {
  return {
    invokeProposal: jest.fn().mockResolvedValue(OK_RESULT),
    emitSse: jest.fn(),
    saveState: jest.fn().mockResolvedValue(undefined),
    invokeBookingCommit: jest.fn(), // §B14 spy — must NEVER be called
    logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
    ...overrides,
  };
}

function call(deps, attendee = { email: EMAIL }, tenantConfig = TENANT_CONFIG) {
  return postFormOffer({ tenantConfig, sessionId: 'sess-1', attendee, deps });
}

describe('postFormOffer — success (outcome:ok)', () => {
  test('returns warm offerText + raw slotsResult', async () => {
    const deps = makeDeps();
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_OK);
    expect(out.slotsResult).toBe(OK_RESULT);
  });

  test('invokes §B16a propose with the shipped payload shape (fresh session → alreadyRejected [])', async () => {
    const deps = makeDeps();
    await call(deps);
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
    expect(deps.invokeProposal).toHaveBeenCalledWith({
      action: 'scheduling_propose',
      tenantId: 'TEN123',
      sessionId: 'sess-1',
      appointmentTypeId: 'intro_call',
      userTimeZone: 'America/Chicago',
      alreadyRejected: [],
    });
  });

  test('emits the scheduling_slots SSE event (3 slots)', async () => {
    const deps = makeDeps();
    await call(deps);
    expect(deps.emitSse).toHaveBeenCalledTimes(1);
    expect(deps.emitSse).toHaveBeenCalledWith({
      type: 'scheduling_slots',
      slots: SLOTS_3,
      session_id: 'sess-1',
    });
  });

  test('persists attendee_email on the session row in the SAME saveState that stages proposing (§B16b ordering + D3 pre-fill)', async () => {
    const deps = makeDeps();
    await call(deps);
    expect(deps.saveState).toHaveBeenCalledTimes(1);
    expect(deps.saveState).toHaveBeenCalledWith({
      tenantId: 'TEN123',
      sessionId: 'sess-1',
      state: 'proposing',
      candidate_slots: SLOTS_3,
      proposal: { poolSize: 2, tieBreaker: 'tb', roundRobinCursor: 1 },
      rejected_slot_ids: [],
      attendee_email: EMAIL,
    });
  });

  test('saveState uses ONLY the deterministic pipeline whitelist keys (§B16b shared staging path)', async () => {
    const deps = makeDeps();
    await call(deps);
    const WHITELIST = ['tenantId', 'sessionId', 'state', 'candidate_slots', 'selected_slot', 'proposal', 'rejected_slot_ids', 'attendee_email'];
    for (const callArgs of deps.saveState.mock.calls) {
      for (const key of Object.keys(callArgs[0])) {
        expect(WHITELIST).toContain(key);
      }
    }
  });

  test('omits optional propose carry-forwards when absent (no explicit undefined persisted)', async () => {
    const deps = makeDeps({
      invokeProposal: jest.fn().mockResolvedValue({ outcome: 'ok', slots: SLOTS_3, poolSize: 1 }),
    });
    await call(deps);
    const saved = deps.saveState.mock.calls[0][0];
    expect(saved.proposal).toEqual({ poolSize: 1 });
    expect('tieBreaker' in saved.proposal).toBe(false);
    expect('roundRobinCursor' in saved.proposal).toBe(false);
  });

  test('saveState seam unwired → offer still returned, SSE still emitted (fail-soft)', async () => {
    const deps = makeDeps({ saveState: undefined });
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_OK);
    expect(deps.emitSse).toHaveBeenCalledTimes(1);
  });

  test('emitSse seam unwired → offer still returned, state still staged (fail-soft)', async () => {
    const deps = makeDeps({ emitSse: undefined });
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_OK);
    expect(deps.saveState).toHaveBeenCalledTimes(1);
  });

  test('a throwing saveState is swallowed → offer suppressed, never breaks the form turn', async () => {
    const deps = makeDeps({ saveState: jest.fn().mockRejectedValue(new Error('ddb down')) });
    const out = await call(deps);
    expect(out).toEqual({ offerText: null, slotsResult: null });
  });
});

describe('postFormOffer — no availability', () => {
  test('warm no-times copy + NO SSE emitted', async () => {
    const deps = makeDeps({
      invokeProposal: jest.fn().mockResolvedValue({ outcome: 'no_availability', slots: [] }),
    });
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_NO_AVAILABILITY);
    expect(out.slotsResult).toEqual({ outcome: 'no_availability', slots: [] });
    expect(deps.emitSse).not.toHaveBeenCalled();
  });

  test('stays in qualifying (§B16b strand-prevention) with the email pre-filled — never a slot-less proposing session', async () => {
    const deps = makeDeps({
      invokeProposal: jest.fn().mockResolvedValue({ outcome: 'no_availability' }),
    });
    await call(deps);
    expect(deps.saveState).toHaveBeenCalledTimes(1);
    expect(deps.saveState).toHaveBeenCalledWith({
      tenantId: 'TEN123',
      sessionId: 'sess-1',
      state: 'qualifying',
      attendee_email: EMAIL,
    });
  });
});

describe('postFormOffer — failed (offer suppressed silently)', () => {
  test('outcome:failed → offerText null, slotsResult passed through, no SSE, no state write', async () => {
    const failed = { outcome: 'failed', error: 'boom' };
    const deps = makeDeps({ invokeProposal: jest.fn().mockResolvedValue(failed) });
    const out = await call(deps);
    expect(out.offerText).toBeNull();
    expect(out.slotsResult).toBe(failed);
    expect(deps.emitSse).not.toHaveBeenCalled();
    expect(deps.saveState).not.toHaveBeenCalled();
  });

  test('invokeProposal throws → offerText null + synthesized failed outcome (never breaks the form turn)', async () => {
    const deps = makeDeps({ invokeProposal: jest.fn().mockRejectedValue(new Error('lambda down')) });
    const out = await call(deps);
    expect(out.offerText).toBeNull();
    expect(out.slotsResult).toEqual({ outcome: 'failed' });
    expect(deps.emitSse).not.toHaveBeenCalled();
    expect(deps.saveState).not.toHaveBeenCalled();
  });

  test('unknown outcome value → fail-closed (suppressed like failed)', async () => {
    const weird = { outcome: 'maybe?' };
    const deps = makeDeps({ invokeProposal: jest.fn().mockResolvedValue(weird) });
    const out = await call(deps);
    expect(out.offerText).toBeNull();
    expect(out.slotsResult).toBe(weird);
  });
});

describe('postFormOffer — guards (fail-closed, no propose attempted)', () => {
  test('scheduling disabled → suppressed, propose never invoked', async () => {
    const deps = makeDeps();
    const out = await call(deps, { email: EMAIL }, { ...TENANT_CONFIG, feature_flags: {} });
    expect(out).toEqual({ offerText: null, slotsResult: null });
    expect(deps.invokeProposal).not.toHaveBeenCalled();
  });

  test('missing / malformed / over-long email → suppressed (EMAIL_SHAPE imported, not copied)', async () => {
    // null (not undefined) so the call() helper's default attendee is NOT triggered.
    for (const attendee of [null, {}, { email: '' }, { email: 'not-an-email' }, { email: `<${EMAIL}>` }, { email: 'a@' + 'b'.repeat(250) + '.com' }]) {
      const deps = makeDeps();
      const out = await postFormOffer({ tenantConfig: TENANT_CONFIG, sessionId: 'sess-1', attendee, deps });
      expect(out).toEqual({ offerText: null, slotsResult: null });
      expect(deps.invokeProposal).not.toHaveBeenCalled();
    }
  });

  test('no resolvable appointment type (multi-type tenant, no sole default) → suppressed', async () => {
    const multiType = {
      ...TENANT_CONFIG,
      scheduling: { appointment_types: { a: { name: 'A' }, b: { name: 'B' } } },
    };
    const deps = makeDeps();
    const out = await call(deps, { email: EMAIL }, multiType);
    expect(out).toEqual({ offerText: null, slotsResult: null });
    expect(deps.invokeProposal).not.toHaveBeenCalled();
  });

  test('invokeProposal seam unwired → suppressed', async () => {
    const deps = makeDeps({ invokeProposal: undefined });
    const out = await call(deps);
    expect(out).toEqual({ offerText: null, slotsResult: null });
  });

  test('missing tenant_id / sessionId → suppressed', async () => {
    const deps = makeDeps();
    expect(await postFormOffer({ tenantConfig: { feature_flags: { scheduling_enabled: true } }, sessionId: 's', attendee: { email: EMAIL }, deps }))
      .toEqual({ offerText: null, slotsResult: null });
    expect(await postFormOffer({ tenantConfig: TENANT_CONFIG, sessionId: '', attendee: { email: EMAIL }, deps }))
      .toEqual({ offerText: null, slotsResult: null });
    expect(deps.invokeProposal).not.toHaveBeenCalled();
  });
});

describe('postFormOffer — self-defense in-flight clobber guard (audit fix 3, deps.loadState)', () => {
  test('the guard set is exactly proposing/confirming (qualifying deliberately proceeds)', () => {
    expect([...IN_FLIGHT_GUARD_STATES].sort()).toEqual(['confirming', 'proposing']);
  });

  test('in-flight proposing session → suppressed (session_in_flight); NO propose, NO saveState, NO SSE', async () => {
    const deps = makeDeps({ loadState: jest.fn().mockResolvedValue({ state: 'proposing', candidate_slots: SLOTS_3 }) });
    const out = await call(deps);
    expect(out.suppressed).toBe(true);
    expect(out.reason).toBe('session_in_flight');
    expect(out.offerText).toBeNull();
    expect(out.slotsResult).toBeNull();
    expect(deps.loadState).toHaveBeenCalledWith({ tenantId: 'TEN123', sessionId: 'sess-1' });
    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(deps.saveState).not.toHaveBeenCalled();
    expect(deps.emitSse).not.toHaveBeenCalled();
  });

  test('in-flight confirming session (staged pick) → suppressed the same way', async () => {
    const deps = makeDeps({ loadState: jest.fn().mockResolvedValue({ state: 'confirming', selected_slot: { slotId: 's1' } }) });
    const out = await call(deps);
    expect(out.suppressed).toBe(true);
    expect(out.reason).toBe('session_in_flight');
    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(deps.saveState).not.toHaveBeenCalled();
    expect(deps.emitSse).not.toHaveBeenCalled();
  });

  test('qualifying session → proceeds (nothing staged to clobber; offer is the natural upgrade)', async () => {
    const deps = makeDeps({ loadState: jest.fn().mockResolvedValue({ state: 'qualifying' }) });
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_OK);
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
    expect(deps.saveState).toHaveBeenCalledTimes(1);
  });

  test('no existing session (loadState → null) → proceeds normally', async () => {
    const deps = makeDeps({ loadState: jest.fn().mockResolvedValue(null) });
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_OK);
    expect(deps.invokeProposal).toHaveBeenCalledTimes(1);
  });

  test('loadState throws → SUPPRESS (a write you can\'t verify is safe to skip); NO propose, NO saveState, NO SSE', async () => {
    const deps = makeDeps({ loadState: jest.fn().mockRejectedValue(new Error('ddb down')) });
    const out = await call(deps);
    expect(out.suppressed).toBe(true);
    expect(out.reason).toBe('session_state_unverifiable');
    expect(out.offerText).toBeNull();
    expect(out.slotsResult).toBeNull();
    expect(deps.invokeProposal).not.toHaveBeenCalled();
    expect(deps.saveState).not.toHaveBeenCalled();
    expect(deps.emitSse).not.toHaveBeenCalled();
  });

  test('deps.loadState absent → current behavior unchanged (the integrator guard alone governs)', async () => {
    const deps = makeDeps(); // makeDeps wires no loadState
    expect(deps.loadState).toBeUndefined();
    const out = await call(deps);
    expect(out.offerText).toBe(OFFER_TEXT_OK);
    expect('suppressed' in out).toBe(false);
  });

  test('suppression audit events are PII-safe (reason + email_present boolean; never the email / any "@")', async () => {
    for (const loadState of [
      jest.fn().mockResolvedValue({ state: 'proposing' }),
      jest.fn().mockRejectedValue(new Error(`boom ${EMAIL}`)),
    ]) {
      const deps = makeDeps({ loadState });
      await call(deps);
      const lines = [];
      for (const fn of [deps.logger.info, deps.logger.warn, deps.logger.error]) {
        for (const args of fn.mock.calls) lines.push(args.join(' '));
      }
      expect(lines.length).toBeGreaterThan(0);
      for (const line of lines) {
        expect(line).not.toContain(EMAIL);
        expect(line).not.toContain('@');
      }
      const auditLines = lines.filter((l) => l.includes('post_form_offer'));
      expect(auditLines.length).toBe(1);
      expect(auditLines[0]).toContain('"outcome":"skipped"');
      expect(auditLines[0]).toContain('"email_present":true');
    }
  });
});

describe('postFormOffer — §B14 boundary (NEVER commits, NEVER stages confirming)', () => {
  test('invokeBookingCommit is never called, on any outcome', async () => {
    for (const result of [OK_RESULT, { outcome: 'no_availability' }, { outcome: 'failed' }]) {
      const deps = makeDeps({ invokeProposal: jest.fn().mockResolvedValue(result) });
      await call(deps);
      expect(deps.invokeBookingCommit).not.toHaveBeenCalled();
    }
  });

  test('no saveState ever advances past proposing (no confirming/booked)', async () => {
    for (const result of [OK_RESULT, { outcome: 'no_availability' }, { outcome: 'failed' }]) {
      const deps = makeDeps({ invokeProposal: jest.fn().mockResolvedValue(result) });
      await call(deps);
      for (const callArgs of deps.saveState.mock.calls) {
        expect(['qualifying', 'proposing']).toContain(callArgs[0].state);
      }
    }
  });
});

describe('postFormOffer — PII: the attendee email is NEVER logged', () => {
  test('no emitted audit/log line contains the email (or any "@") across all outcomes', async () => {
    const consoleSpies = [
      jest.spyOn(console, 'info').mockImplementation(() => {}),
      jest.spyOn(console, 'warn').mockImplementation(() => {}),
      jest.spyOn(console, 'error').mockImplementation(() => {}),
    ];
    try {
      const loggers = [];
      const outcomes = [OK_RESULT, { outcome: 'no_availability' }, { outcome: 'failed' }];
      for (const result of outcomes) {
        const deps = makeDeps({ invokeProposal: jest.fn().mockResolvedValue(result) });
        await call(deps);
        loggers.push(deps.logger);
      }
      // throwing propose too (the error path logs)
      const throwingDeps = makeDeps({ invokeProposal: jest.fn().mockRejectedValue(new Error(`boom ${EMAIL}`)) });
      await call(throwingDeps);
      loggers.push(throwingDeps.logger);

      const allLines = [];
      for (const logger of loggers) {
        for (const fn of [logger.info, logger.warn, logger.error]) {
          for (const args of fn.mock.calls) allLines.push(args.join(' '));
        }
      }
      for (const spy of consoleSpies) {
        for (const args of spy.mock.calls) allLines.push(args.join(' '));
      }
      expect(allLines.length).toBeGreaterThan(0);
      for (const line of allLines) {
        expect(line).not.toContain(EMAIL);
        expect(line).not.toContain('@');
      }
      // and the audit events carry only the boolean
      const auditLines = allLines.filter((l) => l.includes('post_form_offer'));
      expect(auditLines.length).toBeGreaterThanOrEqual(3);
      for (const line of auditLines) {
        expect(line).toContain('"email_present":true');
      }
    } finally {
      consoleSpies.forEach((s) => s.mockRestore());
    }
  });
});
