'use strict';

/**
 * routing.js — RoutingPolicy evaluation + round-robin state (WS-C5).
 *
 * Canonical §10.1/§10.2; frozen contract FROZEN_CONTRACTS.md §B2. Pure-logic
 * library module consumed by the C6 pool-at-commit Lambda. This module owns
 * three exports and NOTHING else:
 *
 *   - evaluatePool(...)      pure: tag-condition eligibility → freeBusy
 *                            intersection → tie-breaker ordering. Returns the
 *                            ORDERED list of viable resources; the C6 caller
 *                            interprets length (0 → SLOT_UNAVAILABLE, 1 →
 *                            assign with no tie-break, N → tie-break order).
 *   - advanceRoundRobin(...) atomic UpdateItem; called by C8 ONLY on a
 *                            successful booking commit (§10.2 advancement
 *                            timing — state advances only after event creation).
 *   - revertRoundRobin(...)  compensating UpdateItem; called by C8 when a
 *                            commit fails AFTER advance, so the advanced
 *                            coordinator is not skipped on the next attempt.
 *
 * The advance/revert are SEPARATE calls the commit step (C8) drives — this
 * module never performs the booking commit itself (work-order OUT OF SCOPE).
 *
 * ── Interpretations layered on the frozen §B2 signatures (flagged in the PR
 *    for integrator/C6 confirmation; none redefine a frozen contract) ──
 *   • candidate shape: { resourceId: string, scheduling_tags: string[] }
 *     (scheduling_tags is the flat, vocabulary-constrained tag list on the
 *      AdminEmployee registry record — canonical §7.2 / config schema §6).
 *   • tag-condition match (config schema §6 tagConditionSchema):
 *       operator 'equals'  → resource carries EVERY value in `values`;
 *       operator 'in_any'  → resource carries AT LEAST ONE value.
 *       `tag` is the operator-facing category label (e.g. 'program') and is
 *       NOT itself matched against scheduling_tags. Conditions AND together;
 *       empty tag_conditions → every candidate eligible (solo policy, §10.3).
 *   • freeBusyByResource: { [resourceId]: { busy:[{start,end}], ... } | null }.
 *     A null/absent entry = that coordinator's freeBusy query failed → excluded
 *     from this attempt (§10.2 step 2: one coordinator's failure never breaks
 *     the pool). Per-slot overlap exclusion + the live re-check are C8's job;
 *     evaluatePool treats a successful freeBusy entry as "viable".
 *   • first_available ordering is a window-level availability heuristic
 *     (fully-free first, then soonest-freeing); slot-exact computation lives in
 *     C7 slot generation, not here.
 *   • "round_robin first, first_available fallback" (work-order/§B2 done-bar):
 *     a round_robin policy with no usable prior cursor (cold start, or the
 *     last-assigned resource is no longer in the free pool) falls back to
 *     first_available ordering for THIS pick; the returned tieBreaker reflects
 *     what actually fired.
 */

const {
  DynamoDBClient,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');

// Created once at module load; reused across warm invocations (Calendar_Watch_* style).
const ddb = new DynamoDBClient({});

const ENV = process.env.ENVIRONMENT || 'staging';
const ROUTING_POLICY_TABLE =
  process.env.ROUTING_POLICY_TABLE || `picasso-routing-policy-${ENV}`;

// ─── Eligibility (tag_conditions) ────────────────────────────────────────────────

function matchesCondition(tags, condition) {
  const values = condition.values || [];
  if (condition.operator === 'in_any') {
    return values.some((v) => tags.includes(v));
  }
  // 'equals' (schema default): resource must carry every required value.
  return values.every((v) => tags.includes(v));
}

function isEligible(candidate, tagConditions) {
  const tags = candidate.scheduling_tags || [];
  return tagConditions.every((c) => matchesCondition(tags, c));
}

// ─── Ordering helpers ────────────────────────────────────────────────────────────

// Stable canonical order (resourceId asc) — deterministic tie-break base.
function byResourceId(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

// Earliest moment a resource frees up within the queried window. A fully-free
// resource (no busy intervals) returns null and sorts first.
function earliestFreeAt(freeBusy) {
  const busy = (freeBusy && freeBusy.busy) || [];
  if (busy.length === 0) return null;
  return busy.reduce((min, iv) => (iv.end < min ? iv.end : min), busy[0].end);
}

function orderByFirstAvailable(resourceIds, freeBusyByResource) {
  return [...resourceIds].sort((a, b) => {
    const ea = earliestFreeAt(freeBusyByResource[a]);
    const eb = earliestFreeAt(freeBusyByResource[b]);
    if (ea === null && eb === null) return byResourceId(a, b);
    if (ea === null) return -1; // a fully free → first
    if (eb === null) return 1; // b fully free → first
    if (ea < eb) return -1;
    if (ea > eb) return 1;
    return byResourceId(a, b);
  });
}

// Round-robin: rotate the stable canonical order so the resource immediately
// AFTER the last-assigned one is first ("longest without a booking", §10.1).
function orderByRoundRobin(resourceIds, lastAssignedResourceId) {
  const base = [...resourceIds].sort(byResourceId);
  const idx = base.indexOf(lastAssignedResourceId);
  if (idx === -1) return base; // last-assigned no longer in pool → no rotation
  return [...base.slice(idx + 1), ...base.slice(0, idx + 1)];
}

// ─── evaluatePool (frozen §B2) ────────────────────────────────────────────────────

async function evaluatePool({
  tenantId, // eslint-disable-line no-unused-vars -- part of frozen §B2 signature
  appointmentType, // eslint-disable-line no-unused-vars -- frozen §B2; consumed by C6/C7, not by ordering
  routingPolicy,
  candidates,
  freeBusyByResource,
}) {
  const policy = routingPolicy || {};
  const tagConditions = policy.tag_conditions || [];
  const fb = freeBusyByResource || {};
  const pool = candidates || [];

  // 1. tag-condition eligibility filter.
  const eligible = pool.filter((c) => isEligible(c, tagConditions));

  // 2. freeBusy intersection: drop resources whose freeBusy query failed
  //    (null/absent entry → excluded per §10.2 step 2).
  const freeIds = eligible
    .map((c) => c.resourceId)
    .filter((id) => fb[id] != null);

  const requested = policy.tie_breaker || 'round_robin';
  const lastAssigned = policy.last_assigned_resource_id;
  const cursorUsable =
    lastAssigned != null && freeIds.includes(lastAssigned);

  let ordered;
  let tieBreaker;
  if (requested === 'round_robin' && cursorUsable) {
    ordered = orderByRoundRobin(freeIds, lastAssigned);
    tieBreaker = 'round_robin';
  } else {
    // first_available policy, OR round_robin cold-start fallback.
    ordered = orderByFirstAvailable(freeIds, fb);
    tieBreaker = 'first_available';
  }

  // Round-robin policies always carry a cursor so the commit step can advance
  // (even cold-start: the first commit sets state) and revert on failure.
  // first_available is stateless → no cursor.
  const roundRobinCursor =
    requested === 'round_robin'
      ? {
          routingPolicyId: policy.id,
          previousResourceId: lastAssigned != null ? lastAssigned : null,
          previousAt:
            policy.last_assigned_at != null ? policy.last_assigned_at : null,
        }
      : null;

  return { ordered, tieBreaker, roundRobinCursor };
}

// ─── Round-robin state writes (driven by C8 commit) ────────────────────────────────

const RP_KEY = (tenantId, routingPolicyId) => ({
  tenantId: { S: tenantId },
  routing_policy_id: { S: routingPolicyId },
});

// Atomic advance — called ONLY after a successful booking commit (§10.2).
async function advanceRoundRobin({ tenantId, routingPolicyId, assignedResourceId }) {
  const now = Date.now();
  await ddb.send(
    new UpdateItemCommand({
      TableName: ROUTING_POLICY_TABLE,
      Key: RP_KEY(tenantId, routingPolicyId),
      UpdateExpression:
        'SET last_assigned_resource_id = :rid, last_assigned_at = :at',
      ExpressionAttributeValues: {
        ':rid': { S: assignedResourceId },
        ':at': { N: String(now) },
      },
    })
  );
  return { last_assigned_resource_id: assignedResourceId, last_assigned_at: now };
}

// Compensating revert — restores the cursor the matching advance moved off of,
// so the advanced coordinator is not skipped on the next attempt. A null
// previousResourceId means there was no prior state (cold-start advance) → clear it.
async function revertRoundRobin({
  tenantId,
  routingPolicyId,
  previousResourceId,
  previousAt,
}) {
  const Key = RP_KEY(tenantId, routingPolicyId);
  if (previousResourceId == null) {
    await ddb.send(
      new UpdateItemCommand({
        TableName: ROUTING_POLICY_TABLE,
        Key,
        UpdateExpression: 'REMOVE last_assigned_resource_id, last_assigned_at',
      })
    );
    return { last_assigned_resource_id: null, last_assigned_at: null };
  }
  await ddb.send(
    new UpdateItemCommand({
      TableName: ROUTING_POLICY_TABLE,
      Key,
      UpdateExpression:
        'SET last_assigned_resource_id = :rid, last_assigned_at = :at',
      ExpressionAttributeValues: {
        ':rid': { S: previousResourceId },
        ':at': { N: String(previousAt) },
      },
    })
  );
  return {
    last_assigned_resource_id: previousResourceId,
    last_assigned_at: previousAt,
  };
}

module.exports = {
  evaluatePool,
  advanceRoundRobin,
  revertRoundRobin,
};
