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
 *     routing resource IS the coordinator, so `coordinatorId || resourceId` (the
 *     `||` falls back on an empty-string coordinatorId too, which is meaningless).
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
  QueryCommand,
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

// A successful freeBusy only AGES OUT stale failures (prune the rolling window); it
// does NOT clear in-window failures. Otherwise a flapping resource
// (fail,fail,success,fail,fail,…) would never reach 3-in-5-min and the breaker
// would never trip — defeating the §10.2 "repeated coordinator failures" intent.
function recordFreeBusySuccess(tenantId, resourceId) {
  liveFailures(tenantId, resourceId);
}

function isResourceDegraded(tenantId, resourceId) {
  return liveFailures(tenantId, resourceId).length >= CB_THRESHOLD;
}

function _resetCircuitBreaker() {
  _breaker.clear();
}

// ─── G4: per-staff availability intersection ─────────────────────────────────────────
//
// intersectAvailabilityWindows(staff, type)
//   - `staff` null/undefined → return `type` UNCHANGED (identity, no copy). This is the
//     byte-identical guarantee: a candidate without availability_windows must not even
//     shallow-copy the normalized object, so downstream callers can assert strict
//     reference equality.
//   - else per day key: intersect staff intervals with type intervals. For each pair of
//     overlapping [s,e] ranges (compared as "HH:MM" strings, converted to minutes):
//     emit {start,end} for overlap [max(starts), min(ends)] when min(ends) > max(starts).
//   - A day absent in staff → empty (staff unavailable that day); a day absent in type
//     → empty (type doesn't offer it); days with 0 intervals after intersection are
//     omitted from the result.
//   - Malformed entries (non-string start/end, un-parseable, NaN) are silently skipped.
//   - Times re-emitted as "HH:MM" zero-padded to match input format.
//   - Never throws.

function _parseMinutes(hhmm) {
  if (typeof hhmm !== 'string') return NaN;
  const colon = hhmm.indexOf(':');
  if (colon < 0) return NaN;
  const h = parseInt(hhmm.slice(0, colon), 10);
  const m = parseInt(hhmm.slice(colon + 1), 10);
  if (Number.isNaN(h) || Number.isNaN(m)) return NaN;
  return h * 60 + m;
}

function _minutesToHHMM(mins) {
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
}

function intersectAvailabilityWindows(staff, type) {
  // Identity branch: no staff windows → use type unchanged (byte-identical guarantee).
  if (staff == null) return type;

  const DAY_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
  const result = {};

  for (const day of DAY_KEYS) {
    const staffIntervals = Array.isArray(staff[day]) ? staff[day] : [];
    const typeIntervals = Array.isArray(type.availability_windows && type.availability_windows[day])
      ? type.availability_windows[day]
      : [];

    // If either side has no intervals for this day, the intersection for that day is empty.
    if (staffIntervals.length === 0 || typeIntervals.length === 0) continue;

    const dayResult = [];
    for (const si of staffIntervals) {
      const sStart = _parseMinutes(si && si.start);
      const sEnd = _parseMinutes(si && si.end);
      if (Number.isNaN(sStart) || Number.isNaN(sEnd) || sEnd <= sStart) continue; // malformed

      for (const ti of typeIntervals) {
        const tStart = _parseMinutes(ti && ti.start);
        const tEnd = _parseMinutes(ti && ti.end);
        if (Number.isNaN(tStart) || Number.isNaN(tEnd) || tEnd <= tStart) continue; // malformed

        const overlapStart = Math.max(sStart, tStart);
        const overlapEnd = Math.min(sEnd, tEnd);
        if (overlapEnd > overlapStart) {
          dayResult.push({ start: _minutesToHHMM(overlapStart), end: _minutesToHHMM(overlapEnd) });
        }
      }
    }

    if (dayResult.length > 0) {
      result[day] = dayResult;
    }
  }

  // Return a per-resource appointmentType with the intersected windows substituted.
  return { ...type, availability_windows: result };
}

// ─── G5: per-staff max-bookings-per-day booking count helper ─────────────────────────
//
// countBookingsByDay({ tenantId, coordinatorEmail, winStartISO, winEndISO, businessTz })
//   → Map(dayKey → count) where dayKey is the business-tz civil date "YYYY-MM-DD".
//
// Queries GSI `tenantId-coordinator_email-index` for real bookings (item_type='booking',
// status='booked') with start_at inside the freeBusy window. Follows pagination to
// completion (pilot scale is small). Uses Intl.DateTimeFormat for tz-correct day bucketing
// — the same helper is used for slot-day lookup in select(), keeping the two keys byte-identical.
//
// Injectable: the `select` function accepts an optional `countBookingsByDay` DI param
// (default = this function) so tests don't touch DDB.

function _civilDay(isoInstant, businessTz) {
  // Returns the "YYYY-MM-DD" civil date of isoInstant in businessTz.
  // Uses en-CA locale for guaranteed ISO-format date output from DateTimeFormat.
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: businessTz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date(isoInstant));
  } catch {
    // Fallback: UTC date (should only happen with a bad tz, and is consistent with
    // the slot-key fallback in select()).
    return isoInstant.slice(0, 10);
  }
}

async function countBookingsByDay({ tenantId, coordinatorEmail, winStartISO, winEndISO, businessTz }) {
  const counts = new Map();
  let ExclusiveStartKey;
  do {
    const res = await ddb.send(
      new QueryCommand({
        TableName: BOOKING_TABLE,
        IndexName: 'tenantId-coordinator_email-index',
        KeyConditionExpression: 'tenantId = :t AND coordinator_email = :c',
        FilterExpression:
          'item_type = :booking AND #st = :booked AND start_at BETWEEN :ws AND :we',
        ExpressionAttributeNames: { '#st': 'status' },
        ExpressionAttributeValues: {
          ':t':       { S: tenantId },
          ':c':       { S: coordinatorEmail },
          ':booking': { S: 'booking' },
          ':booked':  { S: 'booked' },
          ':ws':      { S: winStartISO },
          ':we':      { S: winEndISO },
        },
        ExclusiveStartKey,
      })
    );
    for (const item of res.Items || []) {
      const startAt = item.start_at && item.start_at.S;
      if (!startAt) continue;
      const dayKey = _civilDay(startAt, businessTz);
      counts.set(dayKey, (counts.get(dayKey) || 0) + 1);
    }
    ExclusiveStartKey = res.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return counts;
}

// ─── Field-shim: config appointmentType → the normalized shape C7 reads ─────────────

function normalizeAppointmentType(config) {
  if (!config || typeof config !== 'object') {
    throw new Error('appointmentType is required');
  }
  const bufferBefore = Number(config.buffer_before_minutes) || 0;
  const bufferAfter = Number(config.buffer_after_minutes) || 0;
  // Explicit allowlist — NO `...config` spread. Only the fields C7 reads are handed
  // on, so unrelated caller fields (id, location_mode, raw before/after, etc.) can
  // never leak into the object C5/C7 receive.
  return {
    // pass-through fields C7 reads under the same name
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

// §B18a: CANDIDATE_CAP for diverse-3 sampling — passed to generateSlots as maxSlots
// when sampling mode is active. slots.js is NOT modified; pool just passes the wider cap.
const CANDIDATE_CAP = 48;

// §B18a daypart boundaries in wall-clock minutes from midnight.
// morning < 720 (12:00), midday 720–899 (12:00–14:59), afternoon >= 900 (15:00).
function daypartOf(isoStart, userTimeZone) {
  // Parse the hour+minute in the user's timezone from an ISO8601 UTC instant.
  // We use Intl to avoid timezone math. Falls back to UTC on any failure (never throws).
  try {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: userTimeZone,
      hour: 'numeric',
      minute: 'numeric',
      hour12: false,
    }).formatToParts(new Date(isoStart));
    const h = parseInt(parts.find((p) => p.type === 'hour').value, 10);
    const m = parseInt(parts.find((p) => p.type === 'minute').value, 10);
    const wallMins = h * 60 + m;
    if (wallMins < 720) return 'morning';
    if (wallMins < 900) return 'midday';
    return 'afternoon';
  } catch {
    return 'morning'; // safe fallback — only affects presentation diversity
  }
}

// §B18a pick rules:
//   pick-1: earliest overall.
//   pick-2: different daypart from pick-1, preferring same day; fall back to next day.
//   pick-3: third daypart (distinct from both picks 1+2), same-day pref then day-spread.
//   Output sorted chronologically.
//   ≤count candidates → return all.
function sampleDaypartDiverse(merged, count, userTimeZone) {
  if (merged.length <= count) return [...merged];

  const withDaypart = merged.map((m) => ({ ...m, _daypart: daypartOf(m.start, userTimeZone) }));

  const picks = [];
  const usedDayparts = new Set();
  const usedStarts = new Set();

  // pick-1: earliest overall
  const p1 = withDaypart[0];
  picks.push(p1);
  usedDayparts.add(p1._daypart);
  usedStarts.add(p1.start);
  const p1Day = p1.start.slice(0, 10);

  // pick-2: different daypart, same day first then any day
  let p2 = null;
  // same-day preference
  for (const c of withDaypart) {
    if (usedStarts.has(c.start)) continue;
    if (!usedDayparts.has(c._daypart) && c.start.slice(0, 10) === p1Day) {
      p2 = c; break;
    }
  }
  // day-spread fallback
  if (!p2) {
    for (const c of withDaypart) {
      if (usedStarts.has(c.start)) continue;
      if (!usedDayparts.has(c._daypart)) { p2 = c; break; }
    }
  }
  // different day than pick-1, regardless of daypart
  if (!p2) {
    for (const c of withDaypart) {
      if (usedStarts.has(c.start)) continue;
      if (c.start.slice(0, 10) !== p1Day) { p2 = c; break; }
    }
  }
  // last resort: any different slot (daypart exhausted, same day only)
  if (!p2) {
    for (const c of withDaypart) {
      if (!usedStarts.has(c.start)) { p2 = c; break; }
    }
  }
  // p2 is always found here: merged has unique starts (byStart Map dedup), and
  // merged.length > count >= 1 guarantees at least one other unique start remains.
  picks.push(p2);
  usedDayparts.add(p2._daypart);
  usedStarts.add(p2.start);

  if (count < 3) return picks.map(({ _daypart: _, ...rest }) => rest).sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));

  // pick-3: third daypart distinct from picks 1+2; same-day pref then day-spread
  let p3 = null;
  for (const c of withDaypart) {
    if (usedStarts.has(c.start)) continue;
    if (!usedDayparts.has(c._daypart) && c.start.slice(0, 10) === p1Day) {
      p3 = c; break;
    }
  }
  if (!p3) {
    for (const c of withDaypart) {
      if (usedStarts.has(c.start)) continue;
      if (!usedDayparts.has(c._daypart)) { p3 = c; break; }
    }
  }
  // earliest on a day not yet represented
  if (!p3) {
    const p2Day = p2.start.slice(0, 10);
    for (const c of withDaypart) {
      if (usedStarts.has(c.start)) continue;
      const cDay = c.start.slice(0, 10);
      if (cDay !== p1Day && cDay !== p2Day) { p3 = c; break; }
    }
  }
  if (!p3) {
    for (const c of withDaypart) {
      if (!usedStarts.has(c.start)) { p3 = c; break; }
    }
  }
  // p3 is always found here: merged.length > 3 and only 2 starts are used, so at
  // least 2 unique starts remain after p1+p2 (final fallback is always-found).
  picks.push(p3);

  // Sort chronologically; strip the _daypart annotation.
  return picks
    .map(({ _daypart: _, ...rest }) => rest)
    .sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
}

/**
 * select({ tenantId, appointmentType, routingPolicy, candidates, userTimeZone,
 *          alreadyRejected?, now?, windowStart?, windowEnd?, dateWindow?, maxSlots?,
 *          sampling?, countBookingsByDay? })
 *   → { status, poolBranch, orderedPool, tieBreaker, roundRobinCursor, slots }
 *
 *   status:    'SLOTS_PROPOSED' | 'SLOT_UNAVAILABLE'
 *   poolBranch:'empty' | 'single' | 'multiple'   (§10.2 branch that fired)
 *   orderedPool: tie-broken resourceIds still viable (the pool C8 will lock against)
 *   slots: [ { slotId, start, end, label, candidateResourceIds } ]  (3-5 generic chips)
 *
 *   §B16e: optional dateWindow: { startISO, endISO } constrains slot generation to a
 *   picked day. Absent → unchanged behavior. Forwarded to each generateSlots call so
 *   the day-filter is not silently discarded (fix: was in the destructure but not
 *   passed to C7, making the whole day-filter a production no-op).
 *
 *   §B18a: optional sampling: { mode: 'daypart-diverse', count: 3 }
 *   Absent → behavior byte-identical to today (earliest-first slice at maxSlots).
 *   Present → generateSlots called with CANDIDATE_CAP=48; after merge+alreadyRejected
 *   filter, diverse-3 sampling applied. Per-chip shape unchanged.
 *
 *   G4: When a candidate has availability_windows, the effective windows passed to
 *   generateSlots are the intersection of the staff windows with the appointment-type
 *   windows. When the candidate has NO availability_windows (undefined/null), the
 *   exact `normalized` object is passed through UNCHANGED — zero copies, strict identity.
 *
 *   G5: When a candidate has max_bookings_per_day, days at/over the cap are excluded
 *   from that resource's generated slots. When the cap is absent, countBookingsByDay is
 *   NOT called and slot generation is byte-identical to the pre-G5 path.
 *   RESIDUAL RACE (documented, not fixed): slot-gen exclusion is not a hard commit-time
 *   lock; a rare propose→commit race can let the (cap+1)th booking through. Acceptable
 *   v1 guardrail — Booking_Commit_Handler owns the hard idempotency fence.
 *
 *   countBookingsByDay: optional DI parameter (default = module function) so tests don't
 *   need to hit DynamoDB.
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
  dateWindow,
  maxSlots = DEFAULT_MAX_SLOTS,
  sampling,
  countBookingsByDay: _countBookingsByDay = countBookingsByDay,
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

  // G4: build resourceId → candidate map for per-resource window intersection lookup.
  const candidatesById = new Map();
  for (const cand of pool) {
    if (cand && cand.resourceId) candidatesById.set(cand.resourceId, cand);
  }

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

  // §B18a: when sampling is active, generate a wider candidate set so diversity
  // sampling has enough material. slots.js is NOT modified — pool passes the wider cap.
  const isDiverse = sampling && sampling.mode === 'daypart-diverse';
  const generationCap = isDiverse ? CANDIDATE_CAP : maxSlots;

  // Per-resource slot generation (§B3: call generateSlots once per candidate
  // resource, supplying resourceId), merged into generic time chips.
  //
  // alreadyRejected is NOT forwarded to C7: pool chips live in the `slot#${start}`
  // namespace, whereas C7 dedups on its own `${resourceId}|${startISO}` slotIds —
  // forwarding would silently never match. Re-offer dedup happens at the pool layer
  // below (against `slot#${start}`), which is the namespace the volunteer rejected in.
  const byStart = new Map(); // startISO → { start, end, label, resources: Set }
  for (const resourceId of ordered) {
    const fb = freeBusyByResource[resourceId];
    const cand = candidatesById.get(resourceId);

    // G4: intersect per-staff availability_windows with the appointment-type windows.
    // When the staff member has NO availability_windows, pass normalized UNCHANGED
    // (strict identity — no copy, no mutation; byte-identical guarantee).
    const effectiveApptType = intersectAvailabilityWindows(
      cand && cand.availability_windows,
      normalized
    );

    // G5: when the candidate has a max_bookings_per_day cap, query existing bookings
    // over the freeBusy window and build a day → count map for exclusion below.
    // Cap absent → NO query, NO filtering (byte-identical default path).
    const cap = cand && cand.max_bookings_per_day;
    let bookedDayCounts = null;
    if (cap != null) {
      try {
        bookedDayCounts = await _countBookingsByDay({
          tenantId,
          coordinatorEmail: cand.coordinatorEmail || resourceId,
          winStartISO: winStart,
          winEndISO: winEnd,
          businessTz: (appointmentType && appointmentType.timezone) || 'UTC',
        });
      } catch {
        // Fail-open: if the count query errors, proceed without cap enforcement
        // rather than dropping the resource from slot generation entirely.
        bookedDayCounts = null;
      }
    }

    let resourceSlots = slots.generateSlots({
      busyIntervals: (fb && fb.busy) || [],
      appointmentType: effectiveApptType,
      userTimeZone,
      resourceId,
      now,
      searchDays,
      maxSlots: generationCap,
      // §B16e: forward the day-picker constraint so generateSlots filters to the
      // selected day. When absent (undefined) generateSlots ignores it (no-op).
      dateWindow,
    });

    // G5: exclude slots on days at/over the cap for this resource. When cap is absent
    // (bookedDayCounts === null), this block is entirely skipped (byte-identical).
    if (bookedDayCounts !== null && cap != null) {
      const businessTz = (appointmentType && appointmentType.timezone) || 'UTC';
      resourceSlots = resourceSlots.filter((s) => {
        const dayKey = _civilDay(s.start, businessTz);
        return (bookedDayCounts.get(dayKey) || 0) < cap;
      });
    }

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

  // Re-offer dedup (§10.2): drop times the volunteer already rejected, by the pool's
  // own `slot#${start}` chip id. Then earliest-first.
  // candidateResourceIds preserves the tie-broken `ordered` priority (who C8 locks
  // against first for that time).
  const rejected = new Set(Array.isArray(alreadyRejected) ? alreadyRejected : []);
  const merged = [...byStart.values()]
    .filter((m) => !rejected.has(`slot#${m.start}`))
    .sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));

  // §B18a: apply diverse-3 sampling AFTER merge+alreadyRejected filter.
  // Default path (no sampling): earliest-first slice at maxSlots — byte-identical regression.
  const selected = isDiverse
    ? sampleDaypartDiverse(merged, sampling.count, userTimeZone)
    : merged.slice(0, maxSlots);

  const chips = selected.map((m) => ({
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
 * lockSlot({ tenantId, format?, start, end, candidateResourceIds, poolSize,
 *            attempt?, maxSoloAttempts? })
 *   → LOCKED:        { status:'LOCKED', resourceId, lockKey, format, start, end, lockedAt }
 *   → all taken:     { status:'SLOT_UNAVAILABLE', action:'reoffer', ... }
 *
 * `poolSize` is the ROUTING pool size — `select`'s `orderedPool.length`, NOT the
 * per-slot `candidateResourceIds.length` (a single coordinator being the only one
 * free for THIS time inside a multi-member pool is NOT a solo program). REQUIRED so
 * the §5.5 solo-vs-pool branch can't be mis-flagged.
 *
 * Walks `candidateResourceIds` (tie-broken order) doing one `attribute_not_exists`
 * conditional PutItem per resource. The first success wins; `ConditionalCheckFailed`
 * means the slot is taken for that resource → try the next pool member silently
 * (§10.2). All taken:
 *   - pool (poolSize > 1)  → reoffer fresh slots (poolExhausted).
 *   - solo (poolSize == 1) → reoffer; the result carries `nextAttempt` (= attempt+1)
 *                            so C8 feeds it back; at `maxSoloAttempts` (3) failed
 *                            attempts → return to `proposing` (§5.5 row, §10.2).
 *                            Never silent-drop.
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
  // Required (no candidate-count fallback): the routing pool size decides solo vs pool.
  if (typeof poolSize !== 'number' || !Number.isInteger(poolSize) || poolSize < 1) {
    throw new Error('poolSize (the routing pool size, >= 1) is required');
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
          // Condition on the SK (booking_id = the deterministic lock key) — idiomatic
          // "this exact item does not yet exist" and refactor-proof if the PK changes.
          ConditionExpression: 'attribute_not_exists(booking_id)',
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

  // Every candidate's lock is taken. solo vs pool is the ROUTING pool size.
  if (poolSize > 1) {
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
  // C8 feeds nextAttempt back on the solo re-pick so the 3-attempt cap actually counts.
  return {
    status: 'SLOT_UNAVAILABLE',
    action: 'reoffer',
    soloExhausted: false,
    attempt,
    nextAttempt: attempt + 1,
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
  // G4+G5 helpers (exported for unit coverage):
  intersectAvailabilityWindows,
  countBookingsByDay,
  _civilDay,
};
