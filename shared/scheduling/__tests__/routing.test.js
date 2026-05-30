'use strict';

/**
 * Unit tests for routing.js (WS-C5) — canonical §10.1/§10.2, frozen §B2.
 *
 * Covers: tag-condition eligibility (equals / in_any / AND / empty); freeBusy
 * intersection (failed query excluded); both tie-breakers (round_robin rotation
 * + first_available ordering); round_robin cold-start → first_available
 * fallback; the returned shape (ordered / tieBreaker / roundRobinCursor); and
 * the headline done-bar: round-robin state under partial failure (advance then
 * commit fails → revert restores the cursor so the advanced coordinator isn't
 * skipped).
 *
 * DDB is mocked with aws-sdk-client-mock (Calendar_Watch_* test convention).
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);

const { evaluatePool, advanceRoundRobin, revertRoundRobin } = require('../routing');

const TENANT = 'AUS123957';
const POLICY_ID = 'rp-mentoring';

// Convenience: candidate with flat scheduling_tags.
const cand = (resourceId, scheduling_tags = []) => ({ resourceId, scheduling_tags });
// Convenience: a successful (fully-free) freeBusy entry.
const free = () => ({ busy: [], cachedAt: '2026-06-01T00:00:00Z', source: 'google_freebusy' });
const busy = (intervals) => ({ busy: intervals, cachedAt: '2026-06-01T00:00:00Z', source: 'google_freebusy' });

beforeEach(() => {
  ddbMock.reset();
});

// ─── evaluatePool: tag-condition eligibility ───────────────────────────────────────

describe('evaluatePool — tag-condition eligibility', () => {
  const fb = { a: free(), b: free(), c: free() };

  it('empty tag_conditions → every candidate eligible (solo/no-filter, §10.3)', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'first_available' },
      candidates: [cand('a'), cand('b'), cand('c')],
      freeBusyByResource: fb,
    });
    expect(res.ordered.sort()).toEqual(['a', 'b', 'c']);
  });

  it("operator 'equals' → resource must carry the value", async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [{ tag: 'program', operator: 'equals', values: ['mentoring'] }],
        tie_breaker: 'first_available',
      },
      candidates: [cand('a', ['mentoring']), cand('b', ['tutoring']), cand('c', ['mentoring', 'es'])],
      freeBusyByResource: fb,
    });
    expect(res.ordered).toEqual(['a', 'c']); // b excluded (no 'mentoring')
  });

  it("operator 'equals' with multiple values → resource must carry ALL", async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [{ tag: 'skills', operator: 'equals', values: ['mentoring', 'es'] }],
        tie_breaker: 'first_available',
      },
      candidates: [cand('a', ['mentoring', 'es']), cand('b', ['mentoring'])],
      freeBusyByResource: fb,
    });
    expect(res.ordered).toEqual(['a']); // b lacks 'es'
  });

  it("operator 'in_any' → resource must carry at least one value", async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [{ tag: 'language', operator: 'in_any', values: ['es', 'fr'] }],
        tie_breaker: 'first_available',
      },
      candidates: [cand('a', ['es']), cand('b', ['de']), cand('c', ['fr', 'de'])],
      freeBusyByResource: fb,
    });
    expect(res.ordered).toEqual(['a', 'c']); // b carries neither es nor fr
  });

  it('multiple conditions AND together', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [
          { tag: 'program', operator: 'equals', values: ['mentoring'] },
          { tag: 'language', operator: 'in_any', values: ['es'] },
        ],
        tie_breaker: 'first_available',
      },
      candidates: [cand('a', ['mentoring', 'es']), cand('b', ['mentoring']), cand('c', ['es'])],
      freeBusyByResource: fb,
    });
    expect(res.ordered).toEqual(['a']); // only a satisfies both
  });

  it('candidate with no scheduling_tags is excluded when a condition exists', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [{ tag: 'program', operator: 'equals', values: ['mentoring'] }],
        tie_breaker: 'first_available',
      },
      candidates: [cand('a', ['mentoring']), { resourceId: 'b' }],
      freeBusyByResource: fb,
    });
    expect(res.ordered).toEqual(['a']);
  });
});

// ─── evaluatePool: freeBusy intersection ───────────────────────────────────────────

describe('evaluatePool — freeBusy intersection (§10.2 step 2)', () => {
  it('excludes eligible resources whose freeBusy query failed (null/absent entry)', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'first_available' },
      candidates: [cand('a'), cand('b'), cand('c')],
      freeBusyByResource: { a: free(), b: null /* query failed */ /* c absent */ },
    });
    expect(res.ordered).toEqual(['a']); // b null, c absent → both excluded
  });

  it('empty intersection → ordered is empty (C6 maps to SLOT_UNAVAILABLE)', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'round_robin' },
      candidates: [cand('a'), cand('b')],
      freeBusyByResource: { a: null, b: null },
    });
    expect(res.ordered).toEqual([]);
    expect(res.roundRobinCursor).toEqual({
      routingPolicyId: POLICY_ID,
      previousResourceId: null,
      previousAt: null,
    });
  });
});

// ─── evaluatePool: first_available tie-breaker ─────────────────────────────────────

describe('evaluatePool — first_available tie-breaker (§10.1)', () => {
  it('fully-free resources rank first, then soonest-freeing, ties by resourceId', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'first_available' },
      candidates: [cand('busy-late'), cand('free2'), cand('busy-early'), cand('free1')],
      freeBusyByResource: {
        'busy-late': busy([{ start: '2026-06-01T09:00:00Z', end: '2026-06-01T12:00:00Z' }]),
        free2: free(),
        'busy-early': busy([{ start: '2026-06-01T09:00:00Z', end: '2026-06-01T10:00:00Z' }]),
        free1: free(),
      },
    });
    // free1/free2 first (fully free, tie by id), then busy-early (frees 10:00), then busy-late (frees 12:00).
    expect(res.ordered).toEqual(['free1', 'free2', 'busy-early', 'busy-late']);
    expect(res.tieBreaker).toBe('first_available');
    expect(res.roundRobinCursor).toBeNull();
  });

  it('breaks ties by resourceId when two resources free up at the same time', async () => {
    const sameEnd = { start: '2026-06-01T09:00:00Z', end: '2026-06-01T10:00:00Z' };
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'first_available' },
      candidates: [cand('zoe'), cand('amy')],
      freeBusyByResource: { zoe: busy([sameEnd]), amy: busy([sameEnd]) },
    });
    expect(res.ordered).toEqual(['amy', 'zoe']); // equal free time → id order
  });

  it('orders by earliest free moment across multiple busy intervals', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'first_available' },
      candidates: [cand('x'), cand('y')],
      freeBusyByResource: {
        // x frees up at 10:00 (earliest interval end), y at 11:00.
        x: busy([
          { start: '2026-06-01T09:00:00Z', end: '2026-06-01T10:00:00Z' },
          { start: '2026-06-01T13:00:00Z', end: '2026-06-01T14:00:00Z' },
        ]),
        y: busy([{ start: '2026-06-01T09:00:00Z', end: '2026-06-01T11:00:00Z' }]),
      },
    });
    expect(res.ordered).toEqual(['x', 'y']);
  });
});

// ─── evaluatePool: round_robin tie-breaker ─────────────────────────────────────────

describe('evaluatePool — round_robin tie-breaker (§10.1/§10.2)', () => {
  const candidates = [cand('alice'), cand('bob'), cand('carol')];
  const fb = { alice: free(), bob: free(), carol: free() };

  it('rotates so the resource after last_assigned is first', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [],
        tie_breaker: 'round_robin',
        last_assigned_resource_id: 'alice',
        last_assigned_at: 1700000000000,
      },
      candidates,
      freeBusyByResource: fb,
    });
    // canonical order [alice, bob, carol], last=alice → [bob, carol, alice]
    expect(res.ordered).toEqual(['bob', 'carol', 'alice']);
    expect(res.tieBreaker).toBe('round_robin');
    expect(res.roundRobinCursor).toEqual({
      routingPolicyId: POLICY_ID,
      previousResourceId: 'alice',
      previousAt: 1700000000000,
    });
  });

  it('wraps around when last_assigned is last in canonical order', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [],
        tie_breaker: 'round_robin',
        last_assigned_resource_id: 'carol',
        last_assigned_at: 1700000000000,
      },
      candidates,
      freeBusyByResource: fb,
    });
    expect(res.ordered).toEqual(['alice', 'bob', 'carol']);
  });

  it('cold start (no last_assigned) → first_available fallback, cursor still present', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'round_robin' },
      candidates,
      freeBusyByResource: fb,
    });
    expect(res.tieBreaker).toBe('first_available');
    expect(res.ordered).toEqual(['alice', 'bob', 'carol']); // first_available, all free → id order
    expect(res.roundRobinCursor).toEqual({
      routingPolicyId: POLICY_ID,
      previousResourceId: null,
      previousAt: null,
    });
  });

  it('last_assigned no longer in free pool → first_available fallback', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [],
        tie_breaker: 'round_robin',
        last_assigned_resource_id: 'dave', // not among candidates / not free
        last_assigned_at: 1700000000000,
      },
      candidates,
      freeBusyByResource: fb,
    });
    expect(res.tieBreaker).toBe('first_available');
    expect(res.ordered).toEqual(['alice', 'bob', 'carol']);
    // cursor preserves dave so a successful commit can still advance from it / revert
    expect(res.roundRobinCursor).toEqual({
      routingPolicyId: POLICY_ID,
      previousResourceId: 'dave',
      previousAt: 1700000000000,
    });
  });

  it('defaults tie_breaker to round_robin when unset (schema default)', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [],
        last_assigned_resource_id: 'alice',
        last_assigned_at: 1700000000000,
      },
      candidates,
      freeBusyByResource: fb,
    });
    expect(res.tieBreaker).toBe('round_robin');
    expect(res.ordered).toEqual(['bob', 'carol', 'alice']);
  });

  it('single eligible resource → ordered has one, no rotation needed', async () => {
    const res = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: { id: POLICY_ID, tag_conditions: [], tie_breaker: 'round_robin' },
      candidates: [cand('alice')],
      freeBusyByResource: { alice: free() },
    });
    expect(res.ordered).toEqual(['alice']);
  });
});

// ─── evaluatePool: defensive defaults ──────────────────────────────────────────────

describe('evaluatePool — defensive defaults', () => {
  it('tolerates missing routingPolicy / candidates / freeBusyByResource', async () => {
    const res = await evaluatePool({ tenantId: TENANT });
    expect(res.ordered).toEqual([]);
    // default tie_breaker is round_robin → cursor present with null id
    expect(res.roundRobinCursor).toEqual({
      routingPolicyId: undefined,
      previousResourceId: null,
      previousAt: null,
    });
  });
});

// ─── advanceRoundRobin ─────────────────────────────────────────────────────────────

describe('advanceRoundRobin — atomic UpdateItem (only on commit success)', () => {
  it('SETs last_assigned_resource_id + last_assigned_at and returns new state', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(1700000000000);

    const result = await advanceRoundRobin({
      tenantId: TENANT,
      routingPolicyId: POLICY_ID,
      assignedResourceId: 'bob',
    });

    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 1);
    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.TableName).toBe('picasso-routing-policy-staging');
    expect(input.Key).toEqual({
      tenantId: { S: TENANT },
      routing_policy_id: { S: POLICY_ID },
    });
    expect(input.UpdateExpression).toBe(
      'SET last_assigned_resource_id = :rid, last_assigned_at = :at'
    );
    expect(input.ExpressionAttributeValues).toEqual({
      ':rid': { S: 'bob' },
      ':at': { N: '1700000000000' },
    });
    expect(result).toEqual({ last_assigned_resource_id: 'bob', last_assigned_at: 1700000000000 });

    nowSpy.mockRestore();
  });
});

// ─── revertRoundRobin ──────────────────────────────────────────────────────────────

describe('revertRoundRobin — compensating UpdateItem', () => {
  it('restores a non-null previous cursor with SET', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});

    const result = await revertRoundRobin({
      tenantId: TENANT,
      routingPolicyId: POLICY_ID,
      previousResourceId: 'alice',
      previousAt: 1699999999000,
    });

    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.UpdateExpression).toBe(
      'SET last_assigned_resource_id = :rid, last_assigned_at = :at'
    );
    expect(input.ExpressionAttributeValues).toEqual({
      ':rid': { S: 'alice' },
      ':at': { N: '1699999999000' },
    });
    expect(result).toEqual({ last_assigned_resource_id: 'alice', last_assigned_at: 1699999999000 });
  });

  it('clears state with REMOVE when there was no prior cursor (cold-start advance)', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});

    const result = await revertRoundRobin({
      tenantId: TENANT,
      routingPolicyId: POLICY_ID,
      previousResourceId: null,
      previousAt: null,
    });

    const input = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(input.UpdateExpression).toBe('REMOVE last_assigned_resource_id, last_assigned_at');
    expect(input.ExpressionAttributeValues).toBeUndefined();
    expect(result).toEqual({ last_assigned_resource_id: null, last_assigned_at: null });
  });
});

// ─── Headline done-bar: round-robin under partial failure ──────────────────────────

describe('round-robin state under partial failure (advance → commit fails → revert)', () => {
  it('revert restores the cursor so the advanced coordinator is NOT skipped next attempt', async () => {
    ddbMock.on(UpdateItemCommand).resolves({});
    const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(1700000005000);

    // Prior committed state: last_assigned = alice @ t0.
    const t0 = 1700000000000;

    // 1. evaluatePool picks the rotation; bob is first up.
    const evald = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [],
        tie_breaker: 'round_robin',
        last_assigned_resource_id: 'alice',
        last_assigned_at: t0,
      },
      candidates: [cand('alice'), cand('bob'), cand('carol')],
      freeBusyByResource: { alice: free(), bob: free(), carol: free() },
    });
    expect(evald.ordered[0]).toBe('bob');

    // 2. C8 advances on (optimistic) assignment to bob.
    await advanceRoundRobin({
      tenantId: TENANT,
      routingPolicyId: POLICY_ID,
      assignedResourceId: 'bob',
    });
    const advanceInput = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(advanceInput.ExpressionAttributeValues[':rid']).toEqual({ S: 'bob' });

    // 3. Booking commit FAILS after advance → C8 reverts using the cursor.
    await revertRoundRobin({
      tenantId: TENANT,
      routingPolicyId: POLICY_ID,
      previousResourceId: evald.roundRobinCursor.previousResourceId,
      previousAt: evald.roundRobinCursor.previousAt,
    });
    const revertInput = ddbMock.commandCalls(UpdateItemCommand)[1].args[0].input;
    expect(revertInput.UpdateExpression).toBe(
      'SET last_assigned_resource_id = :rid, last_assigned_at = :at'
    );
    expect(revertInput.ExpressionAttributeValues).toEqual({
      ':rid': { S: 'alice' },
      ':at': { N: String(t0) },
    });

    // 4. Net effect: state is back to alice@t0, so the NEXT evaluatePool again
    //    puts bob first — the advanced coordinator is not skipped.
    const nextEval = await evaluatePool({
      tenantId: TENANT,
      routingPolicy: {
        id: POLICY_ID,
        tag_conditions: [],
        tie_breaker: 'round_robin',
        last_assigned_resource_id: 'alice',
        last_assigned_at: t0,
      },
      candidates: [cand('alice'), cand('bob'), cand('carol')],
      freeBusyByResource: { alice: free(), bob: free(), carol: free() },
    });
    expect(nextEval.ordered[0]).toBe('bob');

    expect(ddbMock).toHaveReceivedCommandTimes(UpdateItemCommand, 2);
    nowSpy.mockRestore();
  });
});
