/**
 * WS-CONVO — bindingContext (pre-turn §B10 binding hook) tests.
 *
 * Done-bar coverage:
 *  - binding.intent → initial C9 state per intent (rescheduling/cancellation/recovery)
 *  - injectSchedulingContext: NO binding → prompt returned UNCHANGED (no-regression)
 *  - injectSchedulingContext: binding present → <scheduling_context> block prepended
 *  - non-fatal: a throwing resolveBinding never breaks chat (returns null/base prompt)
 *  - the injected block frames data-not-instructions + escapes ids
 */

const {
  injectSchedulingContext,
  resolveSchedulingBinding,
  buildSchedulingContextBlock,
  initStateFromIntent,
  STATE_FOR_INTENT,
  CONTEXT_INSTRUCTION,
} = require('../bindingContext');

const RESCHEDULE_BINDING = {
  intent: 'rescheduling_intent',
  booking_id: 'bk_123',
  coordinator_id: 'maya@org.example',
  expires_at: Date.now() + 60000,
  session_id: 'binding#sess-1',
};

describe('initStateFromIntent', () => {
  test('rescheduling_intent → rescheduling', () => {
    expect(initStateFromIntent('rescheduling_intent')).toBe('rescheduling');
  });
  test('cancellation_intent → canceling', () => {
    expect(initStateFromIntent('cancellation_intent')).toBe('canceling');
  });
  test('recovery_intent → null (B-remainder, not driven by the minimal loop)', () => {
    expect(initStateFromIntent('recovery_intent')).toBeNull();
  });
  test('unknown intent → null', () => {
    expect(initStateFromIntent('nonsense')).toBeNull();
    expect(initStateFromIntent(undefined)).toBeNull();
  });
  test('the mapping only covers the three known intents', () => {
    expect(Object.keys(STATE_FOR_INTENT).sort()).toEqual(
      ['cancellation_intent', 'recovery_intent', 'rescheduling_intent']
    );
  });
});

describe('buildSchedulingContextBlock', () => {
  test('no binding / no state → empty string', () => {
    expect(buildSchedulingContextBlock(null, 'rescheduling')).toBe('');
    expect(buildSchedulingContextBlock(RESCHEDULE_BINDING, null)).toBe('');
  });

  test('includes the data-not-instructions framing + intent/booking_id/state', () => {
    const block = buildSchedulingContextBlock(RESCHEDULE_BINDING, 'rescheduling');
    expect(block).toContain(CONTEXT_INSTRUCTION);
    expect(block).toContain('<scheduling_context>');
    expect(block).toContain('</scheduling_context>');
    expect(block).toContain('"intent": "rescheduling_intent"');
    expect(block).toContain('"booking_id": "bk_123"');
    expect(block).toContain('"state": "rescheduling"');
    expect(block).toContain('"coordinator_id": "maya@org.example"');
  });

  test('escapes HTML-significant chars in ids (defensive)', () => {
    const block = buildSchedulingContextBlock(
      { intent: 'rescheduling_intent', booking_id: '<b>"x"</b>' },
      'rescheduling'
    );
    expect(block).toContain('&lt;b&gt;');
    expect(block).not.toContain('<b>');
  });
});

describe('resolveSchedulingBinding (non-fatal)', () => {
  test('returns null on missing keys without calling resolveBinding', async () => {
    const resolveBinding = jest.fn();
    expect(await resolveSchedulingBinding({ tenantId: '', sessionId: 's', deps: { resolveBinding } })).toBeNull();
    expect(await resolveSchedulingBinding({ tenantId: 't', sessionId: '', deps: { resolveBinding } })).toBeNull();
    expect(resolveBinding).not.toHaveBeenCalled();
  });

  test('ignores placeholder session ids', async () => {
    const resolveBinding = jest.fn();
    expect(await resolveSchedulingBinding({ tenantId: 't', sessionId: 'unknown', deps: { resolveBinding } })).toBeNull();
    expect(await resolveSchedulingBinding({ tenantId: 't', sessionId: 'default', deps: { resolveBinding } })).toBeNull();
    expect(resolveBinding).not.toHaveBeenCalled();
  });

  test('a throwing resolveBinding is swallowed → null (never breaks chat)', async () => {
    const resolveBinding = jest.fn().mockRejectedValue(new Error('ddb down'));
    expect(await resolveSchedulingBinding({ tenantId: 't', sessionId: 's', deps: { resolveBinding } })).toBeNull();
  });

  test('passes through a resolved binding', async () => {
    const resolveBinding = jest.fn().mockResolvedValue(RESCHEDULE_BINDING);
    const out = await resolveSchedulingBinding({ tenantId: 't', sessionId: 's', deps: { resolveBinding } });
    expect(out).toBe(RESCHEDULE_BINDING);
    expect(resolveBinding).toHaveBeenCalledWith(expect.objectContaining({ tenantId: 't', sessionId: 's' }));
  });
});

describe('injectSchedulingContext (the index.js call-site wrapper)', () => {
  const BASE = 'BASE PROMPT';

  test('NO binding → prompt returned UNCHANGED (no-regression done-bar)', async () => {
    const resolveBinding = jest.fn().mockResolvedValue(null);
    const out = await injectSchedulingContext(BASE, { tenantId: 't', sessionId: 's', deps: { resolveBinding } });
    expect(out).toBe(BASE);
  });

  test('binding present → block prepended, base prompt preserved', async () => {
    const resolveBinding = jest.fn().mockResolvedValue(RESCHEDULE_BINDING);
    const out = await injectSchedulingContext(BASE, { tenantId: 't', sessionId: 's', deps: { resolveBinding } });
    expect(out).toContain('<scheduling_context>');
    expect(out.endsWith(BASE)).toBe(true);
    expect(out.indexOf('<scheduling_context>')).toBeLessThan(out.indexOf(BASE));
  });

  test('recovery_intent binding (no minimal-loop state) → prompt unchanged', async () => {
    const resolveBinding = jest.fn().mockResolvedValue({ ...RESCHEDULE_BINDING, intent: 'recovery_intent' });
    const out = await injectSchedulingContext(BASE, { tenantId: 't', sessionId: 's', deps: { resolveBinding } });
    expect(out).toBe(BASE);
  });
});
