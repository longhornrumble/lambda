'use strict';

/**
 * Stranded_Booking_Remediator — scheduling sub-phase B Task B11.
 *
 * Coordinator-offboarding stranded-booking remediation (canonical §7.3). When a
 * coordinator leaves — admin clears `scheduling_tags` from the AdminEmployee record, or
 * §5.5 row 4 detects a suspended Workspace account — the calendar admin usually
 * reassigns/cancels that coordinator's meetings calendar-side, and the §14.2 listener
 * follows those changes automatically. The bookings the admin did NOT address are
 * "stranded"; this is the only platform-side intervention.
 *
 * This Lambda detects the stranded set and applies one of the three §7.3 handlings to
 * each, OR the default cascade when no admin choice is given. v1 ships these as backend
 * callable operations; the admin UI ("N bookings need attention" + the three buttons)
 * is deferred (E-phase). The offboarding trigger wiring + IaC are the integrator's.
 *
 * Input (direct-invoke / integrator-wired offboarding trigger):
 *   {
 *     tenant_id:         string (required),
 *     coordinator_email: string (required) — the departed coordinator's calendar email
 *                                            (the Booking `coordinator_email` GSI key),
 *     offboarding_time:  ISO8601 (required) — bookings whose last calendar mutation
 *                                             predates this are stranded,
 *     choice?:           'reassign' | 'cancel' | 'leave' — applied to ALL stranded
 *                                             bookings; omitted ⇒ default cascade (a)→(b).
 *   }
 *
 * Return:
 *   {
 *     tenant_id, stranded: <count>, applied: 'reassign'|'cancel'|'leave'|'cascade',
 *     results: [ { booking_id, outcome, ... } ],
 *     failed:  [ { booking_id, error } ],
 *   }
 *
 * Idempotent / at-least-once safe: each handling is conditional or already-gone-tolerant
 * (reassign's UpdateItem is guarded; cancel's delete treats 404/410 as success), and a
 * re-invoke after success finds nothing still `booked` for the departed coordinator.
 *
 * PII-log hygiene (§5.7 / sub-phase B audit SR-2): coordinator/attendee emails are
 * never logged raw — only a short stable hash fingerprint.
 */

const crypto = require('crypto');

const bookingStore = require('./booking-store');
const calendarOps = require('./calendar-ops');
const { getOAuthClient } = require('./oauth-client');
const { buildResolveAlternate } = require('./routing-context');
const { remediate, OUTCOMES } = require('./remediation');
const { resolveCandidates } = require('../shared/scheduling/candidate-resolver'); // (X) §B7

// ─── structured logging ─────────────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}
function hashId(value) {
  return crypto.createHash('sha256').update(String(value), 'utf8').digest('hex').slice(0, 12);
}

// ─── input validation ───────────────────────────────────────────────────────────────

const TENANT_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
// coordinator_email flows into the OAuth secret path + DDB GSI key; same charset the
// Calendar_Watch_* coordinator_id allowlist enforces (email-shaped).
const COORDINATOR_EMAIL_RE = /^[A-Za-z0-9._@+-]{1,128}$/;
const VALID_CHOICES = ['reassign', 'cancel', 'leave'];

function validateInput(input) {
  if (!input || typeof input !== 'object') {
    throw new Error('Input must be a JSON object');
  }
  const tenantId = input.tenant_id;
  if (typeof tenantId !== 'string' || !TENANT_ID_RE.test(tenantId)) {
    throw new Error('tenant_id is required and must match /^[A-Za-z0-9_-]{1,64}$/');
  }
  const coordinatorEmail = input.coordinator_email;
  if (typeof coordinatorEmail !== 'string' || !COORDINATOR_EMAIL_RE.test(coordinatorEmail)) {
    throw new Error('coordinator_email is required and must match /^[A-Za-z0-9._@+-]{1,128}$/');
  }
  const offboardingTime = input.offboarding_time;
  if (typeof offboardingTime !== 'string' || Number.isNaN(Date.parse(offboardingTime))) {
    throw new Error('offboarding_time is required and must be a parseable ISO8601 timestamp');
  }
  let choice = input.choice;
  if (choice === undefined || choice === null) {
    choice = null; // default cascade
  } else if (!VALID_CHOICES.includes(choice)) {
    throw new Error(`choice must be one of ${VALID_CHOICES.join('/')} (or omitted for the default cascade)`);
  }
  return { tenantId, coordinatorEmail, offboardingTime, choice };
}

// ─── dependency wiring ──────────────────────────────────────────────────────────────
// Built once at module load and reused. resolveAlternate's roster loader is the
// integrator-wired seam (routing-context.js header); now wired to the (X) candidate
// resolver so reassign produces a real alternate instead of degrading to cancel.

// (X) gap-C wire: adapt resolveCandidates to the loadCandidates seam contract
// (tenantId, appointmentType, routingPolicy) → [{resourceId, scheduling_tags, coordinatorEmail}].
// routing-context's loadRoutingPolicy returns { id, tag_conditions, ... }, so the
// routing_policy_id is routingPolicy.id. resolveCandidates reads its own tables via its
// default deps (routing_policy + appointment_type GetItem already granted to B11;
// employee-registry-v2 Query added in the coupled IaC PR).
function loadCandidatesViaResolver(tenantId, appointmentType, routingPolicy) {
  return resolveCandidates({ tenantId, routingPolicyId: routingPolicy.id });
}

function buildDeps(overrides = {}) {
  return {
    resolveAlternate:
      overrides.resolveAlternate || buildResolveAlternate({ loadCandidates: loadCandidatesViaResolver }),
    getOAuthClient: overrides.getOAuthClient || getOAuthClient,
    calendarOps: overrides.calendarOps || calendarOps,
    bookingStore: overrides.bookingStore || bookingStore,
    now: overrides.now || (() => new Date().toISOString()),
    log,
    warn,
  };
}

// ─── main handler ───────────────────────────────────────────────────────────────────

exports.handler = async function handler(event) {
  const { tenantId, coordinatorEmail, offboardingTime, choice } = validateInput(event);
  const applied = choice || 'cascade';

  log('remediator_invoked', {
    tenant_id: tenantId,
    coordinator_email_hash: hashId(coordinatorEmail),
    offboarding_time: offboardingTime,
    applied,
  });

  const stranded = await bookingStore.findStrandedBookings({
    tenantId,
    coordinatorEmail,
    offboardingTime,
  });

  log('remediator_stranded_set', {
    tenant_id: tenantId,
    coordinator_email_hash: hashId(coordinatorEmail),
    stranded_count: stranded.length,
  });

  const deps = buildDeps();
  const results = [];
  const failed = [];

  for (const booking of stranded) {
    try {
      const result = await remediate(booking, choice, deps);
      // Redact PII before it enters the return payload (which may be logged by the
      // caller): a 'reassigned' result carries the new coordinator's email — hash it.
      // The DDB write (booking-store) keeps the real value; only this summary is hashed.
      const safeResult = { ...result };
      if (safeResult.newCoordinatorEmail) {
        safeResult.newCoordinatorEmailHash = hashId(safeResult.newCoordinatorEmail);
        delete safeResult.newCoordinatorEmail;
      }
      results.push({ booking_id: booking.bookingId, ...safeResult });
      log('remediator_booking_handled', {
        booking_id: booking.bookingId,
        outcome: result.outcome,
      });
    } catch (err) {
      warn('remediator_booking_failed', {
        booking_id: booking.bookingId,
        error: err.message,
      });
      failed.push({ booking_id: booking.bookingId, error: err.message });
    }
  }

  log('remediator_run_complete', {
    tenant_id: tenantId,
    stranded: stranded.length,
    handled: results.length,
    failed: failed.length,
  });

  return {
    tenant_id: tenantId,
    stranded: stranded.length,
    applied,
    results,
    failed,
  };
};

// ─── test-only exports ──────────────────────────────────────────────────────────────

exports._test = {
  validateInput,
  buildDeps,
  hashId,
  loadCandidatesViaResolver,
  OUTCOMES,
};
