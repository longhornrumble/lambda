'use strict';

/**
 * routing-context.js — the default "find an alternate coordinator" resolver for
 * handling (a) reassign (canonical §7.3 / §10.2).
 *
 * Re-runs the booking's routing to answer ONE question: is there a DIFFERENT eligible
 * coordinator who is free at this booking's exact existing slot? It composes the frozen
 * Wave-1 contracts — it never redefines them (FROZEN §C):
 *
 *   1. booking.appointment_type_id → AppointmentType row (FROZEN §A) → routing_policy_id
 *      (canonical §10.1 "AppointmentType.routing_policy_id always points at one").
 *   2. routing_policy_id → RoutingPolicy row (FROZEN §A: PK tenantId · SK routing_policy_id).
 *   3. candidate roster (the flagged seam — see loadCandidates below), minus the departed
 *      coordinator.
 *   4. per-candidate freeBusy at [start_at, end_at] via C4 availability.getBusyIntervals
 *      (FROZEN §B1); a candidate whose calendar OVERLAPS the slot, or whose freeBusy
 *      query fails, is excluded — exactly the C8 liveFreeBusyRecheck posture.
 *   5. C5 routing.evaluatePool (FROZEN §B2) over the free candidates → ordered pick.
 *
 * Returns { resourceId, coordinatorEmail } for the best alternate, or null when none
 * exists (no eligible coordinator, none free at the slot, or no roster wired). null is
 * the signal the cascade default (a)→(b) falls back to cancel on — exactly the behavior
 * the plan B11 done-bar verifies ("default cascade when no eligible coordinator exists").
 *
 * ⚑ Flagged for the integrator (FROZEN §C — not forked): the candidate ROSTER has no
 *   frozen contract in FROZEN_CONTRACTS §A (which freezes Booking / AppointmentType /
 *   RoutingPolicy / ConversationSchedulingSession / form_submissions-GSI — not the
 *   AdminEmployee registry). C8 receives its candidates pre-assembled from upstream
 *   conversation context; B11 has no such upstream. So loadCandidates is an INJECTED
 *   seam: the default returns [] (reassign safely degrades to cancel) and logs a warn,
 *   rather than guessing a registry table name/schema. The integrator wires the real
 *   roster loader (employee-registry query, scheduling_tags projection) at the same
 *   time it wires the offboarding trigger + IaC. The reassign machinery itself is fully
 *   exercised by injecting a fixture loadCandidates (routing-context.test.js).
 */

const availability = require('../shared/scheduling/availability'); // C4 §B1
const routing = require('../shared/scheduling/routing'); // C5 §B2

const {
  DynamoDBClient,
  GetItemCommand,
} = require('@aws-sdk/client-dynamodb');

const { sdkConfig } = require('./aws-client-config');

const ENV = process.env.ENVIRONMENT || 'staging';
const APPOINTMENT_TYPE_TABLE =
  process.env.APPOINTMENT_TYPE_TABLE || `picasso-appointment-type-${ENV}`;
const ROUTING_POLICY_TABLE =
  process.env.ROUTING_POLICY_TABLE || `picasso-routing-policy-${ENV}`;

const ddb = new DynamoDBClient(sdkConfig());

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// Two intervals overlap iff one starts before the other ends and vice-versa. Mirrors
// the C8 intervalsOverlap check so "free at the slot" means the same thing both sides.
function overlapsSlot(busy, startMs, endMs) {
  return (busy || []).some(
    (iv) => Date.parse(iv.start) < endMs && Date.parse(iv.end) > startMs
  );
}

async function loadAppointmentType(tenantId, appointmentTypeId) {
  if (!appointmentTypeId) return null;
  const resp = await ddb.send(
    new GetItemCommand({
      TableName: APPOINTMENT_TYPE_TABLE,
      Key: {
        tenantId: { S: tenantId },
        appointment_type_id: { S: appointmentTypeId },
      },
    })
  );
  if (!resp.Item) return null;
  // Forward-compatible reads.
  return {
    appointmentTypeId,
    routingPolicyId: resp.Item.routing_policy_id?.S ?? null,
    raw: resp.Item,
  };
}

async function loadRoutingPolicy(tenantId, routingPolicyId) {
  if (!routingPolicyId) return null;
  const resp = await ddb.send(
    new GetItemCommand({
      TableName: ROUTING_POLICY_TABLE,
      Key: {
        tenantId: { S: tenantId },
        routing_policy_id: { S: routingPolicyId },
      },
    })
  );
  if (!resp.Item) return null;
  const it = resp.Item;
  // Shape per routing.evaluatePool's expected `routingPolicy` (FROZEN §B2 / canonical
  // §10.1). tag_conditions is stored as JSON in v1 hand-edited configs; tolerate both a
  // DynamoDB List and a JSON string.
  let tagConditions = [];
  if (it.tag_conditions?.L) {
    tagConditions = it.tag_conditions.L.map((c) => JSON.parse(c.S));
  } else if (it.tag_conditions?.S) {
    try {
      tagConditions = JSON.parse(it.tag_conditions.S);
    } catch (_) {
      tagConditions = [];
    }
  }
  return {
    id: routingPolicyId,
    tag_conditions: tagConditions,
    tie_breaker: it.tie_breaker?.S ?? 'round_robin',
    last_assigned_resource_id: it.last_assigned_resource_id?.S ?? undefined,
    last_assigned_at: it.last_assigned_at?.N ? Number(it.last_assigned_at.N) : undefined,
  };
}

// ⚑ Flagged seam (see header). Default: no roster wired → no alternate → cascade cancels.
async function defaultLoadCandidates(/* tenantId, appointmentType, routingPolicy */) {
  warn('reassign_roster_not_wired', {
    note: 'no candidate roster source configured; reassign degrades to cancel. '
      + 'Integrator wires loadCandidates (employee-registry → {resourceId, coordinatorEmail, scheduling_tags}).',
  });
  return [];
}

/**
 * Build the resolveAlternate(booking) function. loadCandidates is injectable so the
 * reassign path is fully testable with fixtures; production wiring is the integrator's.
 *   loadCandidates(tenantId, appointmentType, routingPolicy)
 *     → [ { resourceId, coordinatorEmail, scheduling_tags: string[] } ]
 */
function buildResolveAlternate({ loadCandidates = defaultLoadCandidates } = {}) {
  return async function resolveAlternate(booking) {
    const { tenantId, appointmentTypeId, resourceId: departedResourceId,
            coordinatorEmail: departedEmail, startAt, endAt } = booking;

    const appointmentType = await loadAppointmentType(tenantId, appointmentTypeId);
    if (!appointmentType || !appointmentType.routingPolicyId) {
      log('reassign_no_routing_policy', { booking_id: booking.bookingId });
      return null;
    }
    const routingPolicy = await loadRoutingPolicy(tenantId, appointmentType.routingPolicyId);
    if (!routingPolicy) {
      log('reassign_routing_policy_missing', { booking_id: booking.bookingId });
      return null;
    }

    const roster = await loadCandidates(tenantId, appointmentType, routingPolicy);
    // Exclude the departed coordinator (by resource id OR calendar email).
    const candidates = (roster || []).filter(
      (c) => c.resourceId !== departedResourceId && c.coordinatorEmail !== departedEmail
    );
    if (candidates.length === 0) return null;

    const startMs = Date.parse(startAt);
    const endMs = Date.parse(endAt);
    const emailByResource = {};
    const freeBusyByResource = {};
    for (const c of candidates) {
      emailByResource[c.resourceId] = c.coordinatorEmail;
      try {
        const fb = await availability.getBusyIntervals({
          tenantId,
          resourceId: c.resourceId,
          coordinatorId: c.coordinatorEmail,
          windowStart: startAt,
          windowEnd: endAt,
        });
        // Free at the slot → eligible for evaluatePool; busy or failed → excluded
        // (null entry; evaluatePool drops nulls per §10.2 step 2).
        freeBusyByResource[c.resourceId] = overlapsSlot(fb && fb.busy, startMs, endMs)
          ? null
          : fb;
      } catch (_) {
        freeBusyByResource[c.resourceId] = null; // freeBusy failure → exclude this one
      }
    }

    const { ordered } = await routing.evaluatePool({
      tenantId,
      appointmentType: appointmentType.raw,
      routingPolicy,
      candidates: candidates.map((c) => ({
        resourceId: c.resourceId,
        scheduling_tags: c.scheduling_tags || [],
      })),
      freeBusyByResource,
    });

    if (!ordered || ordered.length === 0) return null;
    const pick = ordered[0];
    return { resourceId: pick, coordinatorEmail: emailByResource[pick] };
  };
}

module.exports = {
  buildResolveAlternate,
  defaultLoadCandidates,
  loadAppointmentType,
  loadRoutingPolicy,
  overlapsSlot,
};
