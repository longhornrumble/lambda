'use strict';

/**
 * Unit tests for stateMachine.js (WS-C9) — canonical §9.2, CI-3c.
 *
 * Covers: the eight-state vocabulary; every LEGAL transition; the headline
 * "no skips" done-bar — the FULL illegal-transition matrix (derived, so it
 * regenerates with the machine) asserts each illegal pair throws
 * IllegalStateTransition AND leaves the synthesized session in its prior state;
 * the booking-status.js consumption guard; and persistence (conditional write
 * on the frozen §A ConversationSchedulingSession row, incl. the concurrent-move
 * conflict path).
 *
 * DDB is mocked with aws-sdk-client-mock (Calendar_Watch_* test convention).
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);

const {
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
} = require('../stateMachine');

const { isBookingStatus } = require('../../booking-status');

const TENANT = 'AUS123957';
const SESSION = 'sess-abc';

// The legal edges, written out independently of TRANSITIONS so the test is a
// real cross-check of the canonical §9.2 table (not a tautology against the SoT).
const LEGAL_EDGES = [
  ['qualifying', 'proposing'],
  ['proposing', 'proposing'], // self-loop (rejected slots accumulate)
  ['proposing', 'confirming'],
  ['confirming', 'proposing'], // reoffer when live re-check fails
  ['confirming', 'booked'],
  ['booked', 'pending_attendance'], // auto at event_end + 30min
  ['booked', 'rescheduling'], // re-engagement
  ['booked', 'canceling'], // re-engagement
  ['rescheduling', 'proposing'], // loops back through proposing → confirming
  ['canceling', 'booked'], // "cancel the cancel"
  ['pending_attendance', 'coordinator_no_show'], // "We didn't connect"
];

beforeEach(() => {
  ddbMock.reset();
});

// ─── Vocabulary ─────────────────────────────────────────────────────────────────────

describe('SESSION_STATES vocabulary', () => {
  it('is exactly the eight canonical §9.2 states', () => {
    expect([...SESSION_STATES].sort()).toEqual(
      [
        'booked',
        'canceling',
        'confirming',
        'coordinator_no_show',
        'pending_attendance',
        'proposing',
        'qualifying',
        'rescheduling',
      ].sort()
    );
  });

  it('is frozen (consumers cannot mutate the shared vocabulary)', () => {
    expect(Object.isFrozen(SESSION_STATES)).toBe(true);
    expect(() => {
      SESSION_STATES.push('hacked');
    }).toThrow();
  });

  it('isSessionState recognizes members and rejects non-members', () => {
    expect(isSessionState('qualifying')).toBe(true);
    expect(isSessionState('completed')).toBe(false); // Booking.status, not a session state
    expect(isSessionState('no_show')).toBe(false); // Booking.status, not a session state
    expect(isSessionState(undefined)).toBe(false);
    expect(isSessionState(42)).toBe(false);
  });

  it('TRANSITIONS is frozen and every target is itself a session state', () => {
    expect(Object.isFrozen(TRANSITIONS)).toBe(true);
    for (const targets of Object.values(TRANSITIONS)) {
      for (const t of targets) {
        expect(isSessionState(t)).toBe(true);
      }
    }
  });
});

// ─── Legal transitions ───────────────────────────────────────────────────────────────

describe('legal transitions (canonical §9.2)', () => {
  it.each(LEGAL_EDGES)('%s → %s is allowed and advances state', (from, to) => {
    expect(canTransition(from, to)).toBe(true);
    const next = transition({ tenantId: TENANT, session_id: SESSION, state: from }, to);
    expect(next.state).toBe(to);
  });

  it('transition returns a NEW object and never mutates the input', () => {
    const session = { tenantId: TENANT, session_id: SESSION, state: 'qualifying', extra: 1 };
    const next = transition(session, 'proposing');
    expect(session.state).toBe('qualifying'); // input untouched
    expect(next).not.toBe(session);
    expect(next.extra).toBe(1); // other fields preserved (forward-compat read)
  });

  it('legalTargets returns the configured next-states; [] for terminal/unknown', () => {
    expect(legalTargets('qualifying')).toEqual(['proposing']);
    expect(legalTargets('coordinator_no_show')).toEqual([]); // terminal
    expect(legalTargets('not_a_state')).toEqual([]); // unknown
  });

  it('the strict happy path runs end-to-end with no skips', () => {
    let s = { tenantId: TENANT, session_id: SESSION, state: 'qualifying' };
    for (const to of ['proposing', 'confirming', 'booked']) {
      s = transition(s, to);
    }
    expect(s.state).toBe('booked');
  });
});

// ─── Headline done-bar: the full illegal-transition matrix ("no skips") ──────────────

describe('illegal-transition matrix — "no skips" (done-bar)', () => {
  const matrix = illegalTransitionMatrix();

  it('the canonical skip qualifying → confirming is illegal AND leaves the session in qualifying', () => {
    const session = { tenantId: TENANT, session_id: SESSION, state: 'qualifying' };
    expect(() => transition(session, 'confirming')).toThrow(IllegalStateTransition);
    expect(session.state).toBe('qualifying'); // unchanged
  });

  it('the matrix is the exact complement of the legal edges (64 ordered pairs total)', () => {
    expect(matrix.length).toBe(
      SESSION_STATES.length * SESSION_STATES.length - LEGAL_EDGES.length
    );
    // No legal edge leaked into the illegal matrix.
    const legalKey = new Set(LEGAL_EDGES.map(([f, t]) => `${f}->${t}`));
    for (const { from, to } of matrix) {
      expect(legalKey.has(`${from}->${to}`)).toBe(false);
    }
  });

  it('EVERY illegal pair throws IllegalStateTransition AND leaves the session unchanged', () => {
    for (const { from, to } of matrix) {
      const session = { tenantId: TENANT, session_id: SESSION, state: from };
      let thrown;
      try {
        transition(session, to);
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toBeInstanceOf(IllegalStateTransition);
      expect(thrown.from).toBe(from);
      expect(thrown.to).toBe(to);
      expect(session.state).toBe(from); // never mutated on rejection
    }
  });

  it('spot-checks the named skips from the plan C9 done-bar', () => {
    // qualifying → booked, proposing → booked (skipping confirming), etc.
    for (const [from, to] of [
      ['qualifying', 'booked'],
      ['qualifying', 'canceling'],
      ['proposing', 'booked'],
      ['confirming', 'pending_attendance'],
      ['booked', 'confirming'],
    ]) {
      expect(canTransition(from, to)).toBe(false);
    }
  });

  it('terminal coordinator_no_show has no legal exit', () => {
    for (const to of SESSION_STATES) {
      expect(canTransition('coordinator_no_show', to)).toBe(false);
    }
  });
});

// ─── booking-status.js consumption (session-terminal → Booking.status) ───────────────

describe('DISPOSITION_BOOKING_STATUS — wires session terminals to the frozen Booking.status SoT', () => {
  it('every disposition maps to a legal Booking.status value', () => {
    for (const status of Object.values(DISPOSITION_BOOKING_STATUS)) {
      expect(isBookingStatus(status)).toBe(true);
    }
  });

  it('maps the §11.2 dispositions to the correct Booking.status', () => {
    expect(DISPOSITION_BOOKING_STATUS.attended_yes).toBe('completed');
    expect(DISPOSITION_BOOKING_STATUS.no_show).toBe('no_show');
    expect(DISPOSITION_BOOKING_STATUS.coordinator_no_show).toBe('coordinator_no_show');
    expect(DISPOSITION_BOOKING_STATUS.cancel).toBe('canceled');
  });
});

// ─── Persistence (frozen §A ConversationSchedulingSession row) ───────────────────────

describe('applyTransition — conditional persistence', () => {
  it('persists a legal move with a state-guarded conditional UpdateItem', async () => {
    ddbMock.on(UpdateItemCommand).resolves({
      Attributes: { state: { S: 'proposing' } },
    });

    const attrs = await applyTransition({
      tenantId: TENANT,
      sessionId: SESSION,
      fromState: 'qualifying',
      toState: 'proposing',
    });

    expect(attrs).toEqual({ state: { S: 'proposing' } });
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
    const call = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(call.Key).toEqual({
      tenantId: { S: TENANT },
      session_id: { S: SESSION },
    });
    expect(call.ConditionExpression).toBe('#s = :from');
    expect(call.ExpressionAttributeNames).toEqual({ '#s': 'state' });
    expect(call.ExpressionAttributeValues[':from']).toEqual({ S: 'qualifying' });
    expect(call.ExpressionAttributeValues[':to']).toEqual({ S: 'proposing' });
  });

  it('rejects an illegal move WITHOUT touching DDB', async () => {
    await expect(
      applyTransition({
        tenantId: TENANT,
        sessionId: SESSION,
        fromState: 'qualifying',
        toState: 'confirming', // the canonical forbidden skip
      })
    ).rejects.toBeInstanceOf(IllegalStateTransition);
    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 0);
  });

  it('maps a ConditionalCheckFailed (concurrent move) to StateTransitionConflict', async () => {
    const err = new Error('conditional');
    err.name = 'ConditionalCheckFailedException';
    ddbMock.on(UpdateItemCommand).rejects(err);

    await expect(
      applyTransition({
        tenantId: TENANT,
        sessionId: SESSION,
        fromState: 'qualifying',
        toState: 'proposing',
      })
    ).rejects.toBeInstanceOf(StateTransitionConflict);
  });

  it('propagates unexpected DDB errors unchanged', async () => {
    const err = new Error('throttled');
    err.name = 'ProvisionedThroughputExceededException';
    ddbMock.on(UpdateItemCommand).rejects(err);

    await expect(
      applyTransition({
        tenantId: TENANT,
        sessionId: SESSION,
        fromState: 'qualifying',
        toState: 'proposing',
      })
    ).rejects.toThrow('throttled');
  });
});
