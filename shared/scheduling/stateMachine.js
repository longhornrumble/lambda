'use strict';

/**
 * stateMachine.js — ConversationSchedulingSession state machine (WS-C9).
 *
 * Canonical §9.2 (the eight states + strict "no skips" sequencing). Pure-logic
 * library module consumed by the BSH conversational flow; transitions persist on
 * the `picasso-conversation-scheduling-session-{env}` row (frozen §A: PK
 * `tenantId` · SK `session_id`; the state-machine state is a non-key attribute).
 *
 * ── SESSION state vs Booking.status (the distinction §9.2 + the work-order stress) ──
 * The EIGHT values here are the CONVERSATION SESSION state — a different vocabulary
 * from `Booking.status` (the 5 canonical values in `shared/booking-status.js`,
 * frozen §A). `completed` and `no_show` are deliberately NOT session states
 * (§9.2 "States intentionally NOT first-class": they are terminal on
 * `Booking.status` only). This module never writes Booking rows (work-order OUT
 * OF SCOPE — that is C8); it only moves the session row's `state` attribute and,
 * for traceability, documents which `Booking.status` each terminal session
 * disposition resolves to (DISPOSITION_BOOKING_STATUS below), validated against
 * the frozen vocabulary so the two cannot drift.
 *
 * ── Interpretations layered on canonical §9.2 (flagged in the PR for integrator
 *    confirmation; none redefine a frozen §B interface) ──
 *   • `booked → rescheduling` / `booked → canceling`: §9.2 names rescheduling /
 *     canceling as re-engagement states but does not draw the entry arrow. A
 *     re-engagement acts on an ACTIVE booking, so the natural source is `booked`.
 *     (A token-redemption reschedule/cancel that lands a FRESH session lands via
 *     INITIALIZATION directly into that state — see next bullet — not a move.)
 *   • Initialization vs transition: a session may be CREATED in any state (a new
 *     booking starts in `qualifying`; a token-redeemed reschedule/cancel starts
 *     directly in `rescheduling`/`canceling`). `transition()` governs MOVES
 *     between states, not initial placement; the illegal-transition matrix is
 *     about moves only.
 *   • `pending_attendance` exits: only `→ coordinator_no_show` is an intra-
 *     session edge (the "We didn't connect" disposition, §11.2). The
 *     `attended_yes → completed` and `no_show` dispositions resolve
 *     `Booking.status` and END the session arc with no dedicated session-state
 *     node (completed/no_show are not session states).
 *   • `canceling → booked` ("cancel the cancel", §9.2) is the only edge out of
 *     `canceling`; the "otherwise terminal (Booking.status = canceled)" path
 *     ends the session arc with no session-state target.
 *   • State attribute name = `state` (only PK/SK are frozen §A; this non-key
 *     attribute name is a code choice).
 */

const {
  DynamoDBClient,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { isBookingStatus } = require('../booking-status');

// Created once at module load; reused across warm invocations (Calendar_Watch_* style).
const ddb = new DynamoDBClient({});

const ENV = process.env.ENVIRONMENT || 'staging';
const SCHEDULING_SESSION_TABLE =
  process.env.SCHEDULING_SESSION_TABLE ||
  `picasso-conversation-scheduling-session-${ENV}`;

// ─── The eight session states (canonical §9.2) ─────────────────────────────────────

// Frozen so a consumer can't accidentally mutate the shared vocabulary.
const SESSION_STATES = Object.freeze([
  'qualifying', // checks routing/identity context; asks only for what's missing
  'proposing', // slot generation; 3–5 candidate chips; self-loop on "more times"
  'confirming', // user has selected a slot; echo-back + commit
  'booked', // sub-flow exit; conversation hands back to the LLM
  'rescheduling', // re-engagement to change a booking; loops back through proposing
  'canceling', // re-engagement to cancel; one tap
  'pending_attendance', // entered at event_end + 30min grace; awaits disposition
  'coordinator_no_show', // terminal; "We didn't connect" — no outbound to volunteer
]);

// Legal next-states. The strict happy path is qualifying → proposing →
// confirming → booked (no skips). All other edges are the canonical §9.2
// non-obvious transitions / re-engagement entries (see interpretations above).
const TRANSITIONS = Object.freeze({
  qualifying: Object.freeze(['proposing']),
  proposing: Object.freeze(['proposing', 'confirming']), // self-loop + advance
  confirming: Object.freeze(['proposing', 'booked']), // reoffer-on-race + commit
  booked: Object.freeze(['pending_attendance', 'rescheduling', 'canceling']),
  rescheduling: Object.freeze(['proposing']),
  canceling: Object.freeze(['booked']), // "cancel the cancel"
  pending_attendance: Object.freeze(['coordinator_no_show']),
  coordinator_no_show: Object.freeze([]), // terminal
});

// Which Booking.status each TERMINAL session disposition resolves to. This wires
// the session vocabulary to the frozen Booking.status SoT WITHOUT duplicating
// literals or editing the frozen file; validated at load (below). It is
// documentation + a drift guard, not a transition edge (the targets are
// Booking.status values, not session states).
const DISPOSITION_BOOKING_STATUS = Object.freeze({
  coordinator_no_show: 'coordinator_no_show', // §11.2 "We didn't connect"
  attended_yes: 'completed', // §11.2 disposition (resolves Booking.status only)
  no_show: 'no_show', // §11.2 disposition (resolves Booking.status only)
  cancel: 'canceled', // canceling terminal (§9.2 / §9.4)
});

// Load-time guard: every mapped value MUST be a legal Booking.status. If the
// frozen vocabulary ever changes shape, this throws on import (fail fast) rather
// than letting a stale literal drift in.
for (const status of Object.values(DISPOSITION_BOOKING_STATUS)) {
  if (!isBookingStatus(status)) {
    throw new Error(
      `stateMachine: '${status}' is not a legal Booking.status (booking-status.js drift)`
    );
  }
}

// ─── Error types ───────────────────────────────────────────────────────────────────

class IllegalStateTransition extends Error {
  constructor(from, to) {
    super(`IllegalStateTransition: ${from} → ${to} is not allowed`);
    this.name = 'IllegalStateTransition';
    this.from = from;
    this.to = to;
  }
}

// Thrown when the PERSISTED state was not the expected `fromState` at write time
// (a concurrent transition moved it). The transition is rejected; the row is
// left exactly as the concurrent writer left it.
class StateTransitionConflict extends Error {
  constructor(from, to) {
    super(`StateTransitionConflict: session was not in '${from}' (expected for → ${to})`);
    this.name = 'StateTransitionConflict';
    this.from = from;
    this.to = to;
  }
}

// ─── Pure transition logic ──────────────────────────────────────────────────────────

function isSessionState(value) {
  return typeof value === 'string' && SESSION_STATES.includes(value);
}

// Legal next-states for a given state ([] for terminal / unknown).
function legalTargets(from) {
  return TRANSITIONS[from] || [];
}

function canTransition(from, to) {
  return legalTargets(from).includes(to);
}

/**
 * Pure transition. Returns a NEW session object with the advanced `state`; the
 * input is never mutated (so the caller's reference "stays" in its prior state
 * on rejection AND on success — the new state lives only on the returned copy).
 * Throws IllegalStateTransition for any move not in the §9.2 table.
 *
 * @param {{state: string}} session - a ConversationSchedulingSession-shaped object
 * @param {string} toState
 * @returns {object} new session with state = toState
 */
function transition(session, toState) {
  const fromState = session && session.state;
  if (!canTransition(fromState, toState)) {
    throw new IllegalStateTransition(fromState, toState);
  }
  return { ...session, state: toState };
}

/**
 * The full illegal-transition matrix, DERIVED from TRANSITIONS over every
 * ordered pair of the eight states. Computed (never hand-maintained) so it
 * regenerates automatically whenever the state machine evolves — the
 * "regenerable fixture" the C9 done-bar asks for, as a single-source-of-truth
 * computation that cannot drift from TRANSITIONS.
 *
 * @returns {Array<{from: string, to: string}>}
 */
function illegalTransitionMatrix() {
  const pairs = [];
  for (const from of SESSION_STATES) {
    for (const to of SESSION_STATES) {
      if (!canTransition(from, to)) {
        pairs.push({ from, to });
      }
    }
  }
  return pairs;
}

// ─── Persistence (frozen §A ConversationSchedulingSession row) ──────────────────────

const SESSION_KEY = (tenantId, sessionId) => ({
  tenantId: { S: tenantId },
  session_id: { S: sessionId },
});

/**
 * Validate the move (§9.2), then persist it with an atomic conditional write
 * guarded on the current state. The condition guarantees the row only advances
 * from exactly `fromState` — an illegal move never reaches DDB (rejected by
 * transition() first), and a concurrent move is caught by the condition, so the
 * persisted state is never silently clobbered.
 *
 * @param {{tenantId: string, sessionId: string, fromState: string, toState: string}}
 * @returns {Promise<object>} the new session attributes (ALL_NEW)
 * @throws {IllegalStateTransition} move not allowed by §9.2 (no write attempted)
 * @throws {StateTransitionConflict} persisted state was not `fromState`
 */
async function applyTransition({ tenantId, sessionId, fromState, toState }) {
  // Pure-validate first — an illegal move never touches the table.
  transition({ state: fromState }, toState);

  try {
    const res = await ddb.send(
      new UpdateItemCommand({
        TableName: SCHEDULING_SESSION_TABLE,
        Key: SESSION_KEY(tenantId, sessionId),
        UpdateExpression: 'SET #s = :to, updated_at = :at',
        ConditionExpression: '#s = :from',
        ExpressionAttributeNames: { '#s': 'state' },
        ExpressionAttributeValues: {
          ':to': { S: toState },
          ':from': { S: fromState },
          ':at': { N: String(Date.now()) },
        },
        ReturnValues: 'ALL_NEW',
      })
    );
    return res.Attributes;
  } catch (err) {
    if (err && err.name === 'ConditionalCheckFailedException') {
      throw new StateTransitionConflict(fromState, toState);
    }
    throw err;
  }
}

module.exports = {
  SESSION_STATES,
  TRANSITIONS,
  DISPOSITION_BOOKING_STATUS,
  IllegalStateTransition,
  StateTransitionConflict,
  isSessionState,
  legalTargets,
  canTransition,
  transition,
  illegalTransitionMatrix,
  applyTransition,
};
