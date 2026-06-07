'use strict';

/**
 * candidate-resolver.js — (X) candidate-pool resolver (WS-SCHED-FOUNDATIONS).
 *
 * Canonical §10.1/§10.2/§10.3; FROZEN_CONTRACTS §A (RoutingPolicy / AppointmentType /
 * employee-registry-v2 keys). Turns a booking/tenant context into the candidate pool
 * the frozen `routing.evaluatePool` / `pool.select` (§B2) already consume.
 *
 *   resolveCandidates({ tenantId, routingPolicyId | appointmentTypeId }, deps)
 *     → [ { resourceId, scheduling_tags, coordinatorEmail } ]
 *
 * This is the single loader behind B11's `loadCandidates` seam and B9's reoffer
 * re-pooling. Neither path stores a `routing_policy_id` on the Booking row (§A:
 * "No routing_policy_id on the row — re-pooling needs the (X) resolver") — they hold
 * an `appointment_type_id`, so the appointment-type → routing-policy hop is the
 * PRIMARY entry; the direct `routingPolicyId` entry is for callers that already hold it.
 *
 * ── resourceId ↔ employee mapping (RESOLVED, not guessed — see PR report-back) ──
 *   The employee-registry-v2 record is keyed PK `tenantId` · SK `employeeId` (a UUID,
 *   supports non-Clerk users) and carries `email` + `scheduling_tags`. The routing /
 *   booking / freeBusy layers do NOT key on the UUID — they key on the coordinator's
 *   CALENDAR id, which in v1 is the coordinator's own email (their primary calendar):
 *     • `availability.getBusyIntervals` queries Google freeBusy against the calendar id;
 *     • `pool.select` builds `freeBusyByResource` keyed by `candidate.resourceId` and
 *       falls back `coordinatorId || resourceId` for the calendar query — so for that
 *       fallback to address the right calendar, `resourceId` MUST be the calendar id;
 *     • `routing.advanceRoundRobin` stores `last_assigned_resource_id` = this same id.
 *   Therefore: **`resourceId` = the coordinator's email (= v1 calendar id)**, and
 *   `coordinatorEmail` = the same email (it lands on `Booking.coordinator_email` and
 *   the `tenantId-coordinator_email-index`, and drives the "with Maya" display name).
 *   In v1 the two fields coincide; they are carried separately because they are
 *   semantically distinct (routing-resource identity vs. coordinator address) and may
 *   diverge in v2 (shared / resource calendars whose id is not a person's email). The
 *   registry `employeeId` UUID is the registry's own identity and is intentionally NOT
 *   the routing `resourceId`. Email is lower-cased + trimmed so the round-robin cursor
 *   and the Booking row stay byte-stable across reads (calendar ids are case-insensitive).
 *
 * ── Eligibility (tag_conditions) ──
 *   The resolver returns the ELIGIBLE pool: scheduling-tagged employees that satisfy
 *   the policy's `tag_conditions` (AND across conditions; empty conditions → every
 *   scheduling-tagged employee, the §10.3 solo policy). The match semantics MIRROR
 *   `routing.isEligible` ('in_any' → at least one value; default/'equals' → every
 *   value). `routing.js` does not export that helper, so the ~6 lines are reproduced
 *   here; `routing.evaluatePool` re-applies the same filter downstream, so feeding the
 *   resolver's output straight into evaluatePool is a safe no-op re-check (defense in
 *   depth), not a double-exclusion. **Flagged for the integrator** (FROZEN_CONTRACTS
 *   §C): if the duplicated eligibility ever needs to change, export it from routing.js
 *   and have both consume it — do not let the two copies drift.
 *
 * ── DI seam ──
 *   Every live read (routing-policy GetItem, appointment-type GetItem, registry Query)
 *   is injectable via `deps`, so the resolver is fully unit-testable without DynamoDB.
 *   The defaults below are the only thing that touches AWS.
 */

const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

// Created once at module load; reused across warm invocations (Calendar_Watch_* style).
const ddb = new DynamoDBClient({});
// The §B7 revoked-exclusion runs on the booking-propose path; reads are best-effort, parallel,
// and fail-open (see resolveCandidates), and maxAttempts is capped so a flaky Secrets Manager
// can't multiply retries on a hot path.
const sm = new SecretsManagerClient({ maxAttempts: 2 });

const ENV = process.env.ENVIRONMENT || 'staging';
const ROUTING_POLICY_TABLE =
  process.env.ROUTING_POLICY_TABLE || `picasso-routing-policy-${ENV}`;
const APPOINTMENT_TYPE_TABLE =
  process.env.APPOINTMENT_TYPE_TABLE || `picasso-appointment-type-${ENV}`;
const OAUTH_SECRET_PATH_PREFIX =
  process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';
const EMPLOYEE_REGISTRY_TABLE =
  process.env.EMPLOYEE_REGISTRY_TABLE || `picasso-employee-registry-v2-${ENV}`;

// ─── Minimal attribute-value unmarshalling ──────────────────────────────────────────
// Covers exactly the shapes these rows use (S/N/BOOL/NULL/L/M/SS/NS). Avoids pulling in
// @aws-sdk/util-dynamodb for three reads; tag_conditions (L of M of {S, L<S>}) and
// scheduling_tags (L<S> or SS) both round-trip correctly.

function fromAttr(av) {
  if (av == null) return undefined;
  if (av.S !== undefined) return av.S;
  if (av.N !== undefined) return Number(av.N);
  if (av.BOOL !== undefined) return av.BOOL;
  if (av.NULL !== undefined) return null;
  if (av.L !== undefined) return av.L.map(fromAttr);
  if (av.M !== undefined) return unmarshalItem(av.M);
  if (av.SS !== undefined) return av.SS;
  if (av.NS !== undefined) return av.NS.map(Number);
  return undefined;
}

function unmarshalItem(item) {
  const out = {};
  for (const key of Object.keys(item)) {
    out[key] = fromAttr(item[key]);
  }
  return out;
}

// ─── Eligibility (mirrors routing.isEligible — see header note) ──────────────────────

function matchesCondition(tags, condition) {
  const values = (condition && condition.values) || [];
  if (condition && condition.operator === 'in_any') {
    return values.some((v) => tags.includes(v));
  }
  // 'equals' (schema default): resource must carry every required value.
  return values.every((v) => tags.includes(v));
}

function isEligible(tags, tagConditions) {
  return tagConditions.every((c) => matchesCondition(tags, c));
}

// ─── Default DI implementations (the only AWS-touching code) ─────────────────────────

async function defaultGetRoutingPolicy({ tenantId, routingPolicyId }) {
  const res = await ddb.send(
    new GetItemCommand({
      TableName: ROUTING_POLICY_TABLE,
      Key: {
        tenantId: { S: tenantId },
        routing_policy_id: { S: routingPolicyId },
      },
    })
  );
  return res.Item ? unmarshalItem(res.Item) : null;
}

async function defaultGetAppointmentType({ tenantId, appointmentTypeId }) {
  const res = await ddb.send(
    new GetItemCommand({
      TableName: APPOINTMENT_TYPE_TABLE,
      Key: {
        tenantId: { S: tenantId },
        appointment_type_id: { S: appointmentTypeId },
      },
    })
  );
  return res.Item ? unmarshalItem(res.Item) : null;
}

// Query the full tenant partition, following pagination to completion. v1 pilot scale
// is small; the loop bounds correctness over micro-optimisation (one page is typical).
async function defaultQueryEmployees({ tenantId }) {
  const employees = [];
  let ExclusiveStartKey;
  do {
    const res = await ddb.send(
      new QueryCommand({
        TableName: EMPLOYEE_REGISTRY_TABLE,
        KeyConditionExpression: 'tenantId = :t',
        ExpressionAttributeValues: { ':t': { S: tenantId } },
        // Pull ONLY the three fields the resolver reads — not the whole registry row
        // (cost + avoids dragging unrelated PII into this query). `email` is a DDB
        // reserved word, so it is aliased via ExpressionAttributeNames.
        ProjectionExpression: 'employeeId, #email, scheduling_tags',
        ExpressionAttributeNames: { '#email': 'email' },
        ExclusiveStartKey,
      })
    );
    for (const item of res.Items || []) {
      employees.push(unmarshalItem(item));
    }
    ExclusiveStartKey = res.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return employees;
}

// Read a coordinator's per-coordinator OAuth secret `status` field (§B7). Returns the status
// string, or null if the secret is missing / unreadable / shapeless. The secret path mirrors
// secrets.buildSecretPath: `${OAUTH_SECRET_PATH_PREFIX}/{tenantId}/{coordinatorId}` where
// coordinatorId is the v1 calendar id (= the lower-cased email = resourceId).
async function defaultGetCoordinatorStatus({ tenantId, coordinatorId }) {
  const res = await sm.send(
    new GetSecretValueCommand({
      SecretId: `${OAUTH_SECRET_PATH_PREFIX}/${tenantId}/${coordinatorId}`,
    })
  );
  if (!res || typeof res.SecretString !== 'string' || !res.SecretString) return null;
  let parsed;
  try {
    parsed = JSON.parse(res.SecretString);
  } catch {
    return null;
  }
  return parsed && typeof parsed.status === 'string' ? parsed.status : null;
}

// ─── resolveCandidates ───────────────────────────────────────────────────────────────

/**
 * @param {{ tenantId: string, routingPolicyId?: string, appointmentTypeId?: string }} args
 * @param {object} [deps] - { getRoutingPolicy, getAppointmentType, queryEmployees, log }
 * @returns {Promise<Array<{ resourceId: string, scheduling_tags: string[], coordinatorEmail: string }>>}
 */
async function resolveCandidates(
  { tenantId, routingPolicyId, appointmentTypeId } = {},
  deps = {}
) {
  if (!tenantId) {
    throw new Error('tenantId is required');
  }
  if (!routingPolicyId && !appointmentTypeId) {
    throw new Error('routingPolicyId or appointmentTypeId is required');
  }

  const {
    getRoutingPolicy = defaultGetRoutingPolicy,
    getAppointmentType = defaultGetAppointmentType,
    queryEmployees = defaultQueryEmployees,
    getCoordinatorStatus = defaultGetCoordinatorStatus,
    log = console,
  } = deps;

  // 1. Resolve the routing policy id (directly, or via the appointment type → §A hop).
  let policyId = routingPolicyId;
  if (!policyId) {
    const appt = await getAppointmentType({ tenantId, appointmentTypeId });
    policyId = appt && appt.routing_policy_id;
    if (!policyId) {
      throw new Error(
        `appointment type ${appointmentTypeId} not found or has no routing_policy_id`
      );
    }
  }

  // 2. Read the policy's tag_conditions (forward-compatible: a missing field → []).
  const policy = await getRoutingPolicy({ tenantId, routingPolicyId: policyId });
  if (!policy) {
    throw new Error(`routing policy ${policyId} not found`);
  }
  const tagConditions = Array.isArray(policy.tag_conditions)
    ? policy.tag_conditions
    : [];

  // 3. Query every employee for the tenant; tolerate malformed rows (never crash).
  const employees = (await queryEmployees({ tenantId })) || [];

  // 4. scheduling-tagged + bookable (has an email) + tag-condition-eligible → mapped.
  const candidates = [];
  for (const emp of employees) {
    const tags =
      emp && Array.isArray(emp.scheduling_tags) ? emp.scheduling_tags : [];
    if (tags.length === 0) {
      continue; // not a schedulable resource (also covers null / malformed rows)
    }
    const email =
      emp && typeof emp.email === 'string' ? emp.email.trim().toLowerCase() : '';
    if (!email) {
      // resourceId === calendar id === email; without it the resource is unbookable
      // and unnotifiable. Skip, don't crash. Log the UUID only (not the email — none).
      log.warn(
        `[candidate-resolver] employee ${
          (emp && emp.employeeId) || 'unknown'
        } skipped: no email`
      );
      continue;
    }
    if (!isEligible(tags, tagConditions)) {
      continue;
    }
    candidates.push({
      resourceId: email,
      scheduling_tags: tags,
      coordinatorEmail: email,
    });
  }

  // 5. §B7 revoked→pool-exclusion: never offer a calendar whose per-coordinator OAuth secret is
  //    explicitly status:'revoked' (set by Calendar_OAuth_Connect /connection/status when a
  //    refresh probe returns invalid_grant). FAIL-OPEN: a missing secret / read error / any other
  //    status keeps the candidate — only an explicit 'revoked' excludes. This matches the §E11
  //    caution that a transient/platform failure (e.g. invalid_client) must NOT mass-exclude the
  //    whole pool; the cost of a stray offer to a just-revoked coordinator is a failed commit, not
  //    a silent outage. Reads run in parallel over the (small, v1-pilot) eligible pool.
  if (candidates.length === 0) {
    return candidates;
  }
  const statuses = await Promise.all(
    candidates.map(async (c) => {
      try {
        return await getCoordinatorStatus({ tenantId, coordinatorId: c.resourceId });
      } catch (err) {
        log.warn(
          `[candidate-resolver] status read failed for a candidate (fail-open, kept): ${
            (err && err.name) || 'error'
          }`
        );
        return null; // fail-open
      }
    })
  );
  const bookable = candidates.filter((_, i) => statuses[i] !== 'revoked');
  return bookable;
}

module.exports = {
  resolveCandidates,
  // exported for unit coverage + reuse:
  defaultGetRoutingPolicy,
  defaultGetAppointmentType,
  defaultQueryEmployees,
  defaultGetCoordinatorStatus,
  isEligible,
  unmarshalItem,
  _ROUTING_POLICY_TABLE: ROUTING_POLICY_TABLE,
  _APPOINTMENT_TYPE_TABLE: APPOINTMENT_TYPE_TABLE,
  _EMPLOYEE_REGISTRY_TABLE: EMPLOYEE_REGISTRY_TABLE,
};
