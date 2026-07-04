'use strict';

/**
 * slotWire.test.js — client-boundary allowlist for slot chips.
 *
 * §10.4 / §5.7: candidateResourceIds (coordinator identity — their email in v1)
 * is SERVER-INTERNAL; it was found leaking in the live scheduling_slots SSE on
 * staging 2026-07-03. Every SSE emit site passes chips through slotsForClient.
 */

const { slotsForClient } = require('../slotWire');

describe('slotsForClient', () => {
  const FULL_SLOT = {
    slotId: 'slot#2026-07-06T14:00:00.000Z',
    start: '2026-07-06T14:00:00.000Z',
    end: '2026-07-06T14:45:00.000Z',
    label: 'Mon, Jul 6 · 9:00 AM',
    candidateResourceIds: ['info@myrecruiter.ai'],
  };

  test('strips candidateResourceIds, keeps the four documented chip fields', () => {
    const out = slotsForClient([FULL_SLOT]);
    expect(out).toEqual([
      {
        slotId: FULL_SLOT.slotId,
        start: FULL_SLOT.start,
        end: FULL_SLOT.end,
        label: FULL_SLOT.label,
      },
    ]);
    expect(JSON.stringify(out)).not.toContain('candidateResourceIds');
    expect(JSON.stringify(out)).not.toContain('info@myrecruiter.ai');
  });

  test('strips ANY undocumented field, not just the known leak', () => {
    const out = slotsForClient([{ ...FULL_SLOT, coordinatorEmail: 'x@y.z', internal: true }]);
    expect(Object.keys(out[0]).sort()).toEqual(['end', 'label', 'slotId', 'start']);
  });

  test('does not mutate the input chips (state persistence keeps the full shape)', () => {
    const input = [{ ...FULL_SLOT }];
    slotsForClient(input);
    expect(input[0].candidateResourceIds).toEqual(['info@myrecruiter.ai']);
  });

  test('tolerates junk: non-array → [], non-object entries dropped', () => {
    expect(slotsForClient(undefined)).toEqual([]);
    expect(slotsForClient(null)).toEqual([]);
    expect(slotsForClient('nope')).toEqual([]);
    expect(slotsForClient([null, 'x', FULL_SLOT])).toHaveLength(1);
  });
});
