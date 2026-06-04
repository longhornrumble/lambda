'use strict';

/**
 * scheduling_propose — new-booking availability route (§B16a; FROZEN_CONTRACTS).
 *
 * A READ-ONLY sibling of the scheduling_mutate executor. Invoked by the Bedrock
 * Streaming Handler (new-booking FLOW) once an appointment type is resolved, to obtain
 * 3-5 GENERIC candidate slots (label only — NO coordinator name, §10.4) for the user's
 * timezone. It writes NO Booking row and does NOT advance round-robin — the commit route
 * (C8 default action) owns all of that. The coordinator is bound at commit by
 * pool.lockSlot(); propose only reveals generic times.
 *
 * From appointmentTypeId it resolves THREE things and hands them to the SHIPPED C6
 * orchestrator pool.select (freeBusy-per-resource → routing.evaluatePool → C7
 * generateSlots → merge into generic chips):
 *   (a) the AppointmentType row            → pool.select's `appointmentType`
 *   (b) its RoutingPolicy object           → pool.select's `routingPolicy`
 *   (c) the candidate pool (§B7 resolveCandidates) → pool.select's `candidates`
 * §B7 resolveCandidates returns the candidates list but NOT the RoutingPolicy object, so
 * (a) and (b) are read separately via candidate-resolver's exported default readers
 * (consumed, never modified). Then pool.select's return is MAPPED into the §B16a route
 * response (status → outcome; orderedPool.length → TOP-LEVEL poolSize; chips passed
 * through unchanged; tieBreaker/roundRobinCursor carried for the commit).
 *
 * PII: the propose payload carries NO attendee identity (it stays in the BSH flow until
 * commit) — this route logs none (only tenant/appointment ids + counts).
 */

const pool = require('../shared/scheduling/pool'); // C6 — the shipped orchestrator
const candidateResolver = require('../shared/scheduling/candidate-resolver'); // §B7

// Structured log via the injected logger (PII-clean — only ids + counts, never identity).
function logLine(logger, name, fields) {
  const line = JSON.stringify({ event: name, ...fields });
  if (typeof logger.log === 'function') logger.log(line);
  else if (typeof logger.info === 'function') logger.info(line);
}

// Map pool.select's status → the §B16a outcome vocabulary. pool.select only returns
// 'SLOTS_PROPOSED' when chips.length > 0, so 'ok' iff slots.length > 0 (per the contract).
function outcomeForStatus(status) {
  return status === 'SLOTS_PROPOSED' ? 'ok' : 'no_availability';
}

async function handleSchedulingPropose(event = {}, injected = {}) {
  const _select = injected.poolSelect || pool.select;
  const _resolveCandidates = injected.resolveCandidates || candidateResolver.resolveCandidates;
  const _getAppointmentType = injected.getAppointmentType || candidateResolver.defaultGetAppointmentType;
  const _getRoutingPolicy = injected.getRoutingPolicy || candidateResolver.defaultGetRoutingPolicy;
  const logger = injected.logger || console;

  const { tenantId, appointmentTypeId, userTimeZone, alreadyRejected, windowStart, windowEnd } = event;
  if (!tenantId || !appointmentTypeId || !userTimeZone) {
    return { outcome: 'failed', slots: [], poolSize: 0, error: 'missing_required_fields' };
  }

  // An UNEXPECTED throw (bad DDB read, freeBusy/Google error inside pool.select, etc.)
  // becomes a clean { outcome:'failed' } the BSH flow already handles — NOT a Lambda
  // FunctionError (which would trip the Errors alarm).
  try {
    // (a) AppointmentType row → pool.select's `appointmentType` (+ its routing_policy_id).
    const appointmentType = await _getAppointmentType({ tenantId, appointmentTypeId });
    if (!appointmentType) {
      return { outcome: 'failed', slots: [], poolSize: 0, error: 'appointment_type_not_found' };
    }
    const routingPolicyId = appointmentType.routing_policy_id;
    if (!routingPolicyId) {
      return { outcome: 'failed', slots: [], poolSize: 0, error: 'no_routing_policy' };
    }
    // (b) RoutingPolicy object — read SEPARATELY (§B7 resolveCandidates does not return it).
    const routingPolicy = await _getRoutingPolicy({ tenantId, routingPolicyId });
    if (!routingPolicy) {
      return { outcome: 'failed', slots: [], poolSize: 0, error: 'routing_policy_not_found' };
    }
    // (c) the eligible candidate pool — §B7 shape VERIFIED: [{ resourceId, scheduling_tags,
    //     coordinatorEmail }], exactly what pool.select consumes (it reads candidate.resourceId
    //     + candidate.coordinatorId || resourceId for the calendar query; coordinatorEmail ===
    //     resourceId in v1 so the `||` fallback addresses the right calendar). Fits as-is.
    const candidates = await _resolveCandidates({ tenantId, appointmentTypeId });

    // The SHIPPED C6 orchestrator — do NOT re-implement its freeBusy/eval/slot passes.
    const result = await _select({
      tenantId,
      appointmentType,
      routingPolicy,
      candidates,
      userTimeZone,
      alreadyRejected, // forwarded UNCHANGED → pool.select's `slot#${start}` re-offer dedup
      windowStart,
      windowEnd,
    });

    const orderedPool = (result && result.orderedPool) || [];
    const response = {
      outcome: outcomeForStatus(result && result.status),
      slots: (result && result.slots) || [], // §B3 chips, passed through UNCHANGED (generic)
      poolSize: orderedPool.length, // TOP-LEVEL routing pool size — NOT per-slot, NOT
      //                               candidateResourceIds.length (commit's §5.5 solo-vs-pool
      //                               branch depends on this exact value).
    };
    if (result && result.tieBreaker != null) response.tieBreaker = result.tieBreaker;
    if (result && result.roundRobinCursor != null) response.roundRobinCursor = result.roundRobinCursor;

    logLine(logger, 'scheduling_propose_result', {
      tenant_id: tenantId, appointment_type_id: appointmentTypeId,
      outcome: response.outcome, pool_size: response.poolSize, slot_count: response.slots.length,
    });
    return response;
  } catch (err) {
    logLine(logger, 'scheduling_propose_failed', {
      tenant_id: tenantId, appointment_type_id: appointmentTypeId,
      error_name: (err && err.name) || 'unknown',
    });
    return { outcome: 'failed', slots: [], poolSize: 0, error: 'propose_error' };
  }
}

module.exports = { handleSchedulingPropose };
