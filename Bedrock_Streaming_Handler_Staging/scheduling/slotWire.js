'use strict';

/**
 * slotWire.js — client-boundary allowlist for slot chips.
 *
 * §10.4 / §5.7 (pool.js): a chip's `candidateResourceIds` is the tie-broken
 * coordinator pool that can serve that time — SERVER-INTERNAL only; coordinator
 * identity (their email, in v1) is revealed at confirmation, never at proposal.
 * The flows persist the FULL chips (candidate_slots on the session row — the
 * commit's lock walk needs the pool), but every `scheduling_slots` SSE write
 * must pass through this allowlist so the browser only ever sees the four
 * documented §B3 chip fields. Found leaking live on staging 2026-07-03.
 */

/**
 * Strip slot chips to the client-safe shape: { slotId, start, end, label }.
 * Tolerant of junk input (non-array → [], non-object entries dropped).
 * @param {Array<object>} slots
 * @returns {Array<{slotId:string,start:string,end:string,label:string}>}
 */
function slotsForClient(slots) {
  if (!Array.isArray(slots)) return [];
  return slots
    .filter((s) => s && typeof s === 'object')
    .map(({ slotId, start, end, label }) => ({ slotId, start, end, label }));
}

module.exports = { slotsForClient };
