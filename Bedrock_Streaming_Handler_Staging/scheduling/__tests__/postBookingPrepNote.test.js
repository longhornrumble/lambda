'use strict';

/**
 * postBookingPrepNote — capturePrepNote tests (§B post-booking amendment).
 *  - happy path: a booked row with awaiting_prep_note + booking_id → attach to the booking,
 *    clear the one-shot flag, stream the ack, return { captured:true }
 *  - non-awaiting / no booking_id / no session → { captured:false } (fall through to chat)
 *  - fail-soft: loadState throws → { captured:false }; attach throws → STILL clears + acks
 *  - the answer text is the verbatim user turn (trimmed); never a model call
 */

const { capturePrepNote, tenantHasPostBookingQuestion, PREP_NOTE_ACK } = require('../postBookingPrepNote');

const BASE = { tenantId: 'TEN', sessionId: 'sess-1' };
const awaitingRow = { state: 'booked', awaiting_prep_note: true, booking_id: 'booking#abc' };

function deps(row, overrides = {}) {
  return {
    loadState: jest.fn().mockResolvedValue(row),
    saveState: jest.fn().mockResolvedValue(undefined),
    invokeAttachPrepNote: jest.fn().mockResolvedValue({ outcome: 'ok' }),
    ...overrides,
  };
}

describe('capturePrepNote', () => {
  test('booked + awaiting → attaches answer, clears flag, acks, captured:true', async () => {
    const d = deps(awaitingRow);
    const write = jest.fn();
    const res = await capturePrepNote({ ...BASE, userInput: '  Respite care options.  ', deps: d, write });

    expect(res).toEqual({ captured: true });
    expect(d.invokeAttachPrepNote).toHaveBeenCalledWith({
      action: 'attach_prep_note', tenantId: 'TEN', bookingId: 'booking#abc', prepNote: 'Respite care options.',
    });
    // one-shot: the flag is cleared by re-saving 'booked' WITHOUT the prep fields
    expect(d.saveState).toHaveBeenCalledWith({ tenantId: 'TEN', sessionId: 'sess-1', state: 'booked' });
    expect(write.mock.calls[0][0]).toContain('"type":"text"');
    expect(write.mock.calls[0][0]).toContain(PREP_NOTE_ACK);
  });

  test('row not awaiting → captured:false, no attach, no clear, no ack', async () => {
    const d = deps({ state: 'booked' });
    const write = jest.fn();
    const res = await capturePrepNote({ ...BASE, userInput: 'hello', deps: d, write });
    expect(res).toEqual({ captured: false });
    expect(d.invokeAttachPrepNote).not.toHaveBeenCalled();
    expect(d.saveState).not.toHaveBeenCalled();
    expect(write).not.toHaveBeenCalled();
  });

  test('awaiting but no booking_id → captured:false (cannot target a row)', async () => {
    const d = deps({ state: 'booked', awaiting_prep_note: true });
    const res = await capturePrepNote({ ...BASE, userInput: 'hello', deps: d, write: jest.fn() });
    expect(res).toEqual({ captured: false });
    expect(d.invokeAttachPrepNote).not.toHaveBeenCalled();
  });

  test('no session row → captured:false', async () => {
    const d = deps(null);
    const res = await capturePrepNote({ ...BASE, userInput: 'hello', deps: d, write: jest.fn() });
    expect(res).toEqual({ captured: false });
  });

  test('blank / non-string input → captured:false, never reads state', async () => {
    const d = deps(awaitingRow);
    expect(await capturePrepNote({ ...BASE, userInput: '   ', deps: d, write: jest.fn() })).toEqual({ captured: false });
    expect(await capturePrepNote({ ...BASE, userInput: undefined, deps: d, write: jest.fn() })).toEqual({ captured: false });
    expect(d.loadState).not.toHaveBeenCalled();
  });

  test('loadState throws → captured:false (fail-soft, fall through to chat)', async () => {
    const d = deps(awaitingRow, { loadState: jest.fn().mockRejectedValue(new Error('ddb')) });
    const res = await capturePrepNote({ ...BASE, userInput: 'hello', deps: d, write: jest.fn() });
    expect(res).toEqual({ captured: false });
    expect(d.invokeAttachPrepNote).not.toHaveBeenCalled();
  });

  test('attach throws → STILL clears flag + acks + captured:true (one-shot, never re-ask)', async () => {
    const d = deps(awaitingRow, { invokeAttachPrepNote: jest.fn().mockRejectedValue(new Error('throttle')) });
    const write = jest.fn();
    const res = await capturePrepNote({ ...BASE, userInput: 'hello', deps: d, write });
    expect(res).toEqual({ captured: true });
    expect(d.saveState).toHaveBeenCalledWith({ tenantId: 'TEN', sessionId: 'sess-1', state: 'booked' });
    expect(write.mock.calls[0][0]).toContain(PREP_NOTE_ACK);
  });

  test('executor seam unwired (no invokeAttachPrepNote) → still clears + acks + captured:true', async () => {
    const d = deps(awaitingRow, { invokeAttachPrepNote: undefined });
    const res = await capturePrepNote({ ...BASE, userInput: 'hello', deps: d, write: jest.fn() });
    expect(res).toEqual({ captured: true });
    expect(d.saveState).toHaveBeenCalled();
  });
});

describe('tenantHasPostBookingQuestion — per-turn read gate (byte-identical for non-feature tenants)', () => {
  test('true when ANY form configures a non-blank post_booking_question', () => {
    expect(tenantHasPostBookingQuestion({
      conversational_forms: {
        a: { post_submission: { confirmation_message: 'Thanks' } },
        b: { post_submission: { post_booking_question: 'What would you like to talk about?' } },
      },
    })).toBe(true);
  });

  test('false when no form configures one (blank / absent / no forms / bad shape)', () => {
    expect(tenantHasPostBookingQuestion({ conversational_forms: { a: { post_submission: { post_booking_question: '   ' } } } })).toBe(false);
    expect(tenantHasPostBookingQuestion({ conversational_forms: { a: { post_submission: {} } } })).toBe(false);
    expect(tenantHasPostBookingQuestion({ conversational_forms: {} })).toBe(false);
    expect(tenantHasPostBookingQuestion({})).toBe(false);
    expect(tenantHasPostBookingQuestion(null)).toBe(false);
  });
});
