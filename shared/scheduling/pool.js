'use strict';

/**
 * pool.js — Pool-at-commit selection + slot-lock primitive (WS-C6).
 *
 * Canonical §10.2 (Pool-at-Commit Algorithm) + §5.4 layer 5 (format-scoped
 * `(resource_id, start_at, end_at)` uniqueness) + §5.5 row "DDB conditional-write
 * contention". Pure-orchestration library module: it CONSUMES the three frozen
 * Wave-1 contracts and owns the §10.2 selection pass + the conditional-write lock.
 *
 *   - C4 `availability.getBusyIntervals` (§B1) — per-resource freeBusy.
 *   - C5 `routing.evaluatePool`            (§B2) — tag eligibility + tie-breaker order.
 *   - C7 `slots.generateSlots`             (§B3) — per-resource candidate chips.
 *
 * This module NEVER modifies those — it calls them. It exposes exactly two
 * orchestration entry points the C8 keystone wires:
 *
 *   select(...)   → §10.2 steps 1-5: assemble candidates → tag eligibility →
 *                   freeBusy intersection → empty/single/multiple branch →
 *                   tie-breaker → per-resource slot generation, merged into the
 *                   3-5 GENERIC chips presented to the volunteer (§10.4). Each
 *                   chip carries `candidateResourceIds` (the tie-broken pool order
 *                   that can serve that exact time) — SERVER-INTERNAL only; never
 *                   send it to the client (coordinator identity is revealed at
 *                   confirmation, §10.4 / §5.7 write-side PII boundary).
 *
 *   lockSlot(...) → §5.4 layer 5 / §10.2 conditional-write slot lock. Walks the
 *                   chosen chip's `candidateResourceIds` doing an
 *                   `attribute_not_exists` conditional PutItem per resource; the
 *                   FIRST that succeeds wins the (resource, slot, format) lock. On
 *                   `ConditionalCheckFailed` the slot is already taken for that
 *                   resource → try the next pool member silently (§10.2 "the pool
 *                   absorbs the contention"). All taken → reoffer (never silent-drop).
 *
 * ── C8 wiring (OUT OF SCOPE here; documented so C8 can compose) ──
 *   C6.select → present slots → C8 live freeBusy re-check (§5.4 layer 2) →
 *   C6.lockSlot → C8 conference + calendar `events.insert` + Booking record write →
 *   C5.advanceRoundRobin on success / C5.revertRoundRobin on failure (§10.2
 *   advancement timing — round-robin advances ONLY after event creation succeeds,
 *   so C6 deliberately does NOT call advance/revert).
 *
 * ── Slot-lock storage decision (within the §B mandate; flagged for the integrator) ──
 *   The work-order mandates "a conditional write on the EXISTING Booking table" —
 *   NOT a new table/GSI. So the lock is a discriminated `item_type='slot_lock'`
 *   item in `picasso-booking-{env}`: PK `tenantId`, SK `booking_id` = the
 *   deterministic lock key `slot_lock#{format}#{resource_id}#{start}#{end}`.
 *   `attribute_not_exists(tenantId)` (the PK attribute) enforces uniqueness: first
 *   writer creates it, a concurrent writer gets `ConditionalCheckFailed`. The lock
 *   item stores the times under `lock_start_at`/`lock_end_at` and the coordinator
 *   under `resource_id` — NOT under the Booking GSI key names (`start_at`,
 *   `coordinator_email`) — so lock items stay invisible to the
 *   `tenantId-start_at-index` / `tenantId-coordinator_email-index` that B9/B11/E9
 *   query, and can never be mistaken for real bookings. Lock release /
 *   reconciliation on commit success/failure is C8's compensating-transaction job.
 *
 * ── Interpretations layered on the frozen contracts (flagged in the PR per
 *    FROZEN_CONTRACTS §C; none redefine a contract) ──
 *   • candidate shape: { resourceId, scheduling_tags[], coordinatorId? }. C4's
 *     `getBusyIntervals` needs `coordinatorId` (the calendar identity); in v1 the
 *     routing resource IS the coordinator, so `coordinatorId ?? resourceId`.
 *   • field-shim (this module OWNS it, §B3 note): the config `appointmentType`
 *     schema field names are mapped to the normalized names C7 reads —
 *     `buffer_before_minutes`/`buffer_after_minutes` → `buffer_minutes`
 *     (= max of the two: C7 pads symmetrically, so max guarantees AT LEAST the
 *     configured gap on each side), `lead_time_minutes` → `min_lead_minutes`,
 *     `max_advance_days` → the C7 `searchDays`/freeBusy-window horizon;
 *     `slot_granularity_minutes`/`duration_minutes`/`timezone`/`availability_windows`
 *     keep their names (pass-through). `timezone`+`availability_windows` are NOT in
 *     the config `appointmentTypeSchema`; they are assumed already present on the
 *     normalized object the caller supplies (sourced upstream from the coordinator's
 *     working hours) — the work-order's shim scope is only the four scalar renames.
 *   • circuit-breaker (§10.2): in-memory, per `(tenantId, resourceId)`. 3 freeBusy
 *     failures within 5 min → degraded → excluded from the pool. Entries age out of
 *     the 5-min window (self-healing / half-open); a successful freeBusy clears the
 *     count. The durable `degraded`-state transition + admin alert (§5.5 row 4) is
 *     the caller's side effect, not this pure module's.
 */

const {
  DynamoDBClient,
  PutItemCommand,
} = require('@aws-sdk/client-dynamodb');

const availability = require('./availability'); // §B1 (C4)
const routing = require('./routing'); // §B2 (C5)
const slots = require('./slots'); // §B3 (C7)

// Created once at module load; reused across warm invocations (Calendar_Watch_* style).
const ddb = new DynamoDBClient({});

const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;

const DEFAULT_FORMAT = 'one_to_one';
const DEFAULT_MAX_ADVANCE_DAYS = 30; // config schema §5 default
const DEFAULT_MAX_SLOTS = 5; // §B3 "3-5 chips"

// ─── Circuit-breaker (§10.2 "repeated coordinator failures") ───────────────────────

const CB_WINDOW_MS = 5 * 60 * 1000; // 5-minute window (§10.2)
const CB_THRESHOLD = 3; // 3 failures → degraded
const _breaker = new Map(); // `${tenantId}:${resourceId}` → [failureTsMs, ...]

function cbKey(tenantId, resourceId) {
  return `${tenantId}:${resourceId}`;
}

// Drop failures older than the rolling 5-min window; persist the pruned list.
function liveFailures(tenantId, resourceId) {
  const key = cbKey(tenantId, resourceId);
  const now = Date.now();
  const all = _breaker.get(key) || [];
  const live = all.filter((t) => now - t < CB_WINDOW_MS);
  if (live.length !== all.length) {
    if (live.length === 0) _breaker.delete(key);
    else _breaker.set(key, live);
  }
  return live;
}

// Record one freeBusy failure; returns true if the breaker is now tripped (degraded).
function recordFreeBusyFailure(tenantId, resourceId) {
  const live = liveFailures(tenantId, resourceId);
  live.push(Date.now());
  _breaker.set(cbKey(tenantId, resourceId), live);
  return live.length >= CB_THRESHOLD;
}

// A successful freeBusy clears the failure count (health restored for this attempt).
function recordFreeBusySuccess(tenantId, resourceId) {
  _breaker.delete(cbKey(tenantId, resourceId));
}

function isResourceDegraded(tenantId, resourceId) {
  return liveFailures(tenantId, resourceId).length >= CB_THRESHOLD;
}

function _resetCircuitBreaker() {
  _breaker.clear();
}

// ─── Field-shim: config appointmentType → the normalized shape C7 reads ─────────────

function normalizeAppointmentType(config) {
  if (!config || typeof config !== 'object') {
    throw new Error('appointmentType is required');
  }
  const bufferBefore = Number(config.buffer_before_minutes) || 0;
  const bufferAfter = Number(config.buffer_after_minutes) || 0;
  return {
    // pass-through fields C7 reads under the same name
    ...config,
    duration_minutes: config.duration_minutes,
    timezone: config.timezone,
    availability_windows: config.availability_windows,
    slot_granularity_minutes: config.slot_granularity_minutes,
    // renamed fields (the shim this module owns, §B3 note)
    buffer_minutes: Math.max(bufferBefore, bufferAfter),
    min_lead_minutes: Number(config.lead_time_minutes) || 0,
  };
}

// ─── Slot-lock key (§5.4 layer 5 — format-scoped uniqueness) ────────────────────────

// `start`/`end` are ISO8601 (contain ':' not '#'); `resource_id`/`format` are
// controlled-vocabulary platform ids (no '#'). The '#' delimiter is therefore
// unambiguous. Same (resource, start, end) + same format → identical key → the
// SECOND conditional write is rejected. DIFFERENT format → different key → accepted
// (v2 Group readiness, §5.4 layer 5).
function buildLockKey({ format, resourceId, start, end }) {
  return `slot_lock#${format}#${resourceId}#${start}#${end}`;
}

function isConditionalCheckFailed(err) {
  return Boolean(err) && err.name === 'ConditionalCheckFailedException';
}

// ─── select (§10.2 steps 1-5 + slot generation) ─────────────────────────────────────

/**
 * select({ tenantId, appointmentType, routingPolicy, candidates, userTimeZone,
 *          alreadyRejected?, now?, windowStart?, windowEnd?, maxSlots? })
 *   → { status, poolBranch, orderedPool, tieBreaker, roundRobinCursor, slots }
 *
 *   status:    'SLOTS_PROPOSED' | 'SLOT_UNAVAILABLE'
 *   poolBranch:'empty' | 'single' | 'multiple'   (§10.2 branch that fired)
 *   orderedPool: tie-broken resourceIds still viable (the pool C8 will lock against)
 *   slots: [ { slotId, start, end, label, candidateResourceIds } ]  (3-5 generic chips)
 */
async function select({
  tenantId,
  appointmentType,
  routingPolicy,
  candidates,
  userTimeZone,
  alreadyRejected,
  now,
  windowStart,
  windowEnd,
  maxSlots = DEFAULT_MAX_SLOTS,
}) {
  if (!tenantId) throw new Error('tenantId is required');
  if (!userTimeZone) throw new Error('userTimeZone is required');

  const normalized = normalizeAppointmentType(appointmentType);
  const searchDays =
    Number(appointmentType && appointmentType.max_advance_days) > 0
      ? Number(appointmentType.max_advance_days)
      : DEFAULT_MAX_ADVANCE_DAYS;

  // freeBusy query window: now → now + searchDays (matches the slot horizon).
  const nowMs = now != null ? Date.parse(now) : Date.now();
  const winStart = windowStart || new Date(nowMs).toISOString();
  const winEnd =
    windowEnd || new Date(nowMs + searchDays * 24 * 60 * 60 * 1000).toISOString();

  const pool = Array.isArray(candidates) ? candidates : [];

  // §10.2 step 2: per-resource freeBusy. Each query is independent — one failure
  // never breaks the pool; a degraded coordinator is excluded up front.
  const freeBusyByResource = {};
  for (const candidate of pool) {
    const resourceId = candidate.resourceId;
    if (isResourceDegraded(tenantId, resourceId)) {
      continue; // excluded from this attempt (breaker open)
    }
    const coordinatorId = candidate.coordinatorId || resourceId;
    try {
      const fb = await availability.getBusyIntervals({
        tenantId,
        resourceId,
        coordinatorId,
        windowStart: winStart,
        windowEnd: winEnd,
      });
      freeBusyByResource[resourceId] = fb;
      recordFreeBusySuccess(tenantId, resourceId);
    } catch (err) {
      // §10.2 step 2: exclude this coordinator, feed the circuit-breaker, continue.
      recordFreeBusyFailure(tenantId, resourceId);
    }
  }

  // §10.2 steps 1+2+5: tag eligibility → freeBusy intersection → tie-breaker order.
  const { ordered, tieBreaker, roundRobinCursor } = await routing.evaluatePool({
    tenantId,
    appointmentType: normalized,
    routingPolicy,
    candidates: pool,
    freeBusyByResource,
  });

  // §10.2 steps 3+4+5: empty / single / multiple branch.
  if (ordered.length === 0) {
    return {
      status: 'SLOT_UNAVAILABLE', // §10.2 step 3: empty intersection → reoffer
      poolBranch: 'empty',
      orderedPool: [],
      tieBreaker,
      roundRobinCursor,
      slots: [],
    };
  }
  const poolBranch = ordered.length === 1 ? 'single' : 'multiple';

  // Per-resource slot generation (§B3: call generateSlots once per candidate
  // resource, supplying resourceId), merged into generic time chips.
  const byStart = new Map(); // startISO → { start, end, label, resources: Set }
  for (const resourceId of ordered) {
    const fb = freeBusyByResource[resourceId];
    const resourceSlots = slots.generateSlots({
      busyIntervals: (fb && fb.busy) || [],
      appointmentType: normalized,
      userTimeZone,
      alreadyRejected,
      resourceId,
      now,
      searchDays,
      maxSlots,
    });
    for (const s of resourceSlots) {
      const existing = byStart.get(s.start);
      if (existing) {
        existing.resources.add(resourceId);
      } else {
        byStart.set(s.start, {
          start: s.start,
          end: s.end,
          label: s.label,
          resources: new Set([resourceId]),
        });
      }
    }
  }

  // Earliest-first, capped at maxSlots. candidateResourceIds preserves the
  // tie-broken `ordered` priority (who C8 locks against first for that time).
  const merged = [...byStart.values()].sort((a, b) =>
    a.start < b.start ? -1 : a.start > b.start ? 1 : 0
  );
  const chips = merged.slice(0, maxSlots).map((m) => ({
    slotId: `slot#${m.start}`,
    start: m.start,
    end: m.end,
    label: m.label,
    candidateResourceIds: ordered.filter((r) => m.resources.has(r)),
  }));

  return {
    status: chips.length > 0 ? 'SLOTS_PROPOSED' : 'SLOT_UNAVAILABLE',
    poolBranch,
    orderedPool: ordered,
    tieBreaker,
    roundRobinCursor,
    slots: chips,
  };
}

// ─── lockSlot (§5.4 layer 5 / §10.2 conditional-write slot lock + backoff) ───────────

/**
 * lockSlot({ tenantId, format?, start, end, candidateResourceIds, poolSize?,
 *            attempt?, maxSoloAttempts? })
 *   → LOCKED:        { status:'LOCKED', resourceId, lockKey, format, start, end, lockedAt }
 *   → all taken:     { status:'SLOT_UNAVAILABLE', action:'reoffer', ... }
 *
 * Walks `candidateResourceIds` (tie-broken order) doing one `attribute_not_exists`
 * conditional PutItem per resource. The first success wins; `ConditionalCheckFailed`
 * means the slot is taken for that resource → try the next pool member silently
 * (§10.2). All taken:
 *   - pool (poolSize > 1)  → reoffer fresh slots (poolExhausted).
 *   - solo (poolSize == 1) → reoffer; after `maxSoloAttempts` (3) failed attempts
 *                            return to `proposing` (§5.5 row, §10.2). Never silent-drop.
 * Non-`ConditionalCheckFailed` errors (throttle, network) propagate — a transient
 * write error must NOT be mistaken for "slot taken".
 */
async function lockSlot({
  tenantId,
  format = DEFAULT_FORMAT,
  start,
  end,
  candidateResourceIds,
  poolSize,
  attempt = 1,
  maxSoloAttempts = 3,
}) {
  if (!tenantId) throw new Error('tenantId is required');
  if (!start || !end) throw new Error('start and end are required');
  if (!Array.isArray(candidateResourceIds) || candidateResourceIds.length === 0) {
    throw new Error('candidateResourceIds must be a non-empty array');
  }

  for (const resourceId of candidateResourceIds) {
    const lockKey = buildLockKey({ format, resourceId, start, end });
    const lockedAt = Date.now();
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: BOOKING_TABLE,
          Item: {
            tenantId: { S: tenantId },
            booking_id: { S: lockKey },
            item_type: { S: 'slot_lock' },
            resource_id: { S: resourceId },
            // deliberately NOT `start_at`/`coordinator_email` — keeps lock items out
            // of the Booking GSIs that B9/B11/E9 query.
            lock_start_at: { S: start },
            lock_end_at: { S: end },
            appointment_format: { S: format },
            locked_at: { N: String(lockedAt) },
          },
          ConditionExpression: 'attribute_not_exists(tenantId)',
        })
      );
      return { status: 'LOCKED', resourceId, lockKey, format, start, end, lockedAt };
    } catch (err) {
      if (isConditionalCheckFailed(err)) {
        continue; // taken for this resource → next pool member (§10.2)
      }
      throw err; // real error surfaces (DLQ + alarm upstream)
    }
  }

  // Every candidate's lock is taken.
  const solo = (poolSize != null ? poolSize : candidateResourceIds.length) === 1;
  if (!solo) {
    return { status: 'SLOT_UNAVAILABLE', action: 'reoffer', poolExhausted: true };
  }
  if (attempt >= maxSoloAttempts) {
    return {
      status: 'SLOT_UNAVAILABLE',
      action: 'reoffer',
      state: 'proposing', // §5.5: solo, 3 failed attempts → reoffer from proposing
      soloExhausted: true,
      attempt,
    };
  }
  return {
    status: 'SLOT_UNAVAILABLE',
    action: 'reoffer',
    soloExhausted: false,
    attempt,
  };
}

module.exports = {
  select,
  lockSlot,
  // circuit-breaker (exported for the caller's degraded-state side effects + tests):
  recordFreeBusyFailure,
  recordFreeBusySuccess,
  isResourceDegraded,
  // field-shim + lock-key (exported for unit coverage + C8 reuse):
  normalizeAppointmentType,
  buildLockKey,
  _resetCircuitBreaker,
  _BOOKING_TABLE: BOOKING_TABLE,
  _CB_THRESHOLD: CB_THRESHOLD,
  _CB_WINDOW_MS: CB_WINDOW_MS,
};
