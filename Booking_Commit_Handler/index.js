'use strict';

/**
 * Booking_Commit_Handler — scheduling sub-phase C Task C8 (the booking-commit keystone).
 *
 * The single transactional commit path. Invoked at the volunteer's "Confirm" with a
 * chosen slot (already presented by C6 pool.select earlier in the conversation). It
 * performs, IN ORDER, with compensating transactions on every failure (§4.5):
 *
 *   1. Live freeBusy re-check (C4 getBusyIntervals fresh) — §5.4 layer 2.
 *   2. C6 pool.lockSlot — §5.4 layer 4 conditional-write slot lock.
 *   3. ConferenceProvider.createConference (GoogleMeet | Zoom | Null) — §5.2 item 4.
 *   4. Google Calendar events.insert with extendedProperties.private.booking_id — FROZEN §A.
 *   5a. C5 routing.advanceRoundRobin — §10.2 (advanced here; REVERTED if the Booking
 *       write then fails, so the net effect honors "advance only on full success").
 *   5b. Booking record write (status=booked) — conditional, idempotent.
 *   6. Confirmation email with .ics + join link + signed cancel/reschedule links — AC #7 (≤60s).
 *
 * Compensating transactions: unconditional slot-lock release on EVERY path; calendar
 * event + conference rollback on post-insert failure; round-robin revert if a
 * post-advance step fails; OAuth-401 transient-refresh-retry / permanent-degrade +
 * re-pool against remaining candidates (§5.5 row 4).
 *
 * Idempotency (AC #6): booking_id is deterministic over (tenantId, sessionId, start);
 * a step-0 gate returns an already-booked row ("already confirmed", C11); the lock is
 * the concurrency mutex; the Booking write is conditional.
 *
 * Consumes the frozen shared/scheduling/* modules + shared/booking-status — never
 * modifies them (FROZEN §C: a wrong contract is FLAGGED to the integrator, not forked).
 */

const crypto = require('crypto');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const { sdkConfig } = require('./aws-client-config');
const pool = require('../shared/scheduling/pool'); // C6
const routing = require('../shared/scheduling/routing'); // C5
const availability = require('../shared/scheduling/availability'); // C4
const featureGate = require('../shared/scheduling/featureGate'); // backend scheduling_enabled gate

const { getOAuthClient, clearCacheEntry } = require('./oauth-client');
const calendarEvents = require('./calendar-events');
const { resolveProvider } = require('./conference-providers');
const bookingStore = require('./booking-store');
const { sendConfirmationEmail } = require('./confirmation-email');

const CONFIRMATION_SLA_MS = 60 * 1000; // AC #7
const OPS_ALERTS_TOPIC_ARN = process.env.OPS_ALERTS_TOPIC_ARN || '';
const sns = new SNSClient(sdkConfig());

// ─── structured logging ──────────────────────────────────────────────────────────────

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── typed control-flow signal ──────────────────────────────────────────────────────

// Raised when a coordinator's OAuth grant is permanently unusable (revoked /
// invalid_grant) — index.js degrades them and re-pools against the rest.
class DegradeCoordinator extends Error {
  constructor(coordinatorId, cause) {
    super(`coordinator OAuth permanently failed: ${coordinatorId}`);
    this.name = 'DegradeCoordinator';
    this.coordinatorId = coordinatorId;
    this.cause = cause;
  }
}

// ─── small helpers ────────────────────────────────────────────────────────────────────

function resolveCoordinatorId(resourceId, coordinatorEmails) {
  return (coordinatorEmails && coordinatorEmails[resourceId]) || resourceId;
}

// §5.7 PII-log hygiene: coordinatorId can be an email. Never write it raw to a log
// or an ops alert — hash to a short stable fingerprint (matches the Onboarder's
// coordinator_id_hash discipline, sub-phase B audit SR-2).
function hashId(value) {
  return crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 12);
}

function intervalsOverlap(busy, start, end) {
  const s = Date.parse(start);
  const e = Date.parse(end);
  return busy.some((iv) => Date.parse(iv.start) < e && Date.parse(iv.end) > s);
}

// Locale-aware "when" label (§9.3: never hand-format times).
function formatWhen(start, timeZone) {
  try {
    return new Intl.DateTimeFormat('en-US', {
      weekday: 'short', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit', timeZoneName: 'short',
      timeZone: timeZone || 'UTC',
    }).format(new Date(start));
  } catch (_) {
    return start;
  }
}

async function alertAdmin(subject, detail) {
  if (!OPS_ALERTS_TOPIC_ARN) {
    warn('admin_alert_skipped_no_topic', { subject });
    return;
  }
  try {
    await sns.send(new PublishCommand({
      TopicArn: OPS_ALERTS_TOPIC_ARN,
      Subject: String(subject).slice(0, 100),
      Message: JSON.stringify(detail),
    }));
  } catch (err) {
    warn('admin_alert_failed', { subject, error: err.message });
  }
}

// ─── step 1: live freeBusy re-check ───────────────────────────────────────────────────

// Re-query each candidate's freeBusy for the narrow [start,end] window; drop any whose
// calendar shows the slot now busy, and any whose freeBusy errors (excluded + breaker
// fed, §10.2 step 2). Returns survivors in the SAME tie-broken order.
async function liveFreeBusyRecheck({ tenantId, candidateResourceIds, coordinatorEmails, start, end }) {
  const survivors = [];
  for (const resourceId of candidateResourceIds) {
    const coordinatorId = resolveCoordinatorId(resourceId, coordinatorEmails);
    try {
      const fb = await availability.getBusyIntervals({
        tenantId, resourceId, coordinatorId, windowStart: start, windowEnd: end,
      });
      pool.recordFreeBusySuccess(tenantId, resourceId);
      if (!intervalsOverlap((fb && fb.busy) || [], start, end)) {
        survivors.push(resourceId);
      }
    } catch (err) {
      pool.recordFreeBusyFailure(tenantId, resourceId);
      warn('recheck_freebusy_failed', { tenant_id: tenantId, resource_id: resourceId });
    }
  }
  return survivors;
}

// ─── step 4: events.insert with the §5.5-row-4 OAuth-401 thread ───────────────────────

async function insertWithOAuthRetry({ tenantId, coordinatorId, calendarId, requestBody }) {
  let client = await getOAuthClient({ tenantId, coordinatorId });
  try {
    return await calendarEvents.insertEvent(client, calendarId, requestBody);
  } catch (err) {
    const cls = calendarEvents.classifyAuthError(err);
    if (!cls.isAuth) throw err; // not an auth problem → surface (caller compensates)
    if (cls.permanent) throw new DegradeCoordinator(coordinatorId, err); // revoked grant
    // transient: force a fresh secret fetch + client, retry once (§6.2).
    clearCacheEntry({ tenantId, coordinatorId });
    client = await getOAuthClient({ tenantId, coordinatorId });
    try {
      return await calendarEvents.insertEvent(client, calendarId, requestBody);
    } catch (err2) {
      const cls2 = calendarEvents.classifyAuthError(err2);
      if (cls2.isAuth) throw new DegradeCoordinator(coordinatorId, err2); // still 401 → degrade
      throw err2;
    }
  }
}

// ─── one full commit attempt against ONE locked coordinator ───────────────────────────
//
// Returns a success result, OR throws DegradeCoordinator (caller re-pools), OR throws
// any other error after compensating (caller surfaces a graceful failure). Always
// releases the lock on its own exit paths.
async function commitAgainstResource({
  tenantId, resourceId, lockKey, bookingId, ctx,
}) {
  const coordinatorId = resolveCoordinatorId(resourceId, ctx.coordinatorEmails);
  const provider = resolveProvider(ctx.conferenceType, ctx.providerOverrides);

  // Stamp a TTL on the just-acquired lock (best-effort) so a crash before the
  // explicit release can't strand the slot — DynamoDB TTL garbage-collects it.
  try {
    await bookingStore.setLockTtl(tenantId, lockKey);
  } catch (ttlErr) {
    warn('lock_ttl_set_failed', { lock_key: lockKey, error: ttlErr.message });
  }

  // Recover a conference id a prior partial attempt recorded on the lock (Zoom
  // read-before-write → no duplicate meeting on retry).
  let existingConferenceId;
  try {
    const priorLock = await bookingStore.readLock(tenantId, lockKey);
    existingConferenceId = priorLock && priorLock.conference_id && priorLock.conference_id.S;
  } catch (_) { /* a missing/unreadable lock just means no prior conference */ }

  let conference = null;
  let event = null;
  let rrAdvanced = false;

  try {
    // step 3: conference (Meet defers to the insert; Zoom/Null mint up front).
    conference = await provider.createConference({
      tenantId, coordinatorId, bookingId,
      topic: ctx.appointmentTypeName,
      start: ctx.start, end: ctx.end, timezone: ctx.timezone,
      attendeeEmail: ctx.attendee.email,
      existingConferenceId,
    });
    // Persist a freshly-minted (non-Meet) conference id on the lock BEFORE insert,
    // so a retry reuses it instead of creating a duplicate.
    if (conference.conferenceId && !conference.deferToCalendarInsert) {
      await bookingStore.recordConferenceOnLock(tenantId, lockKey, {
        conferenceId: conference.conferenceId, provider: conference.provider,
      });
    }

    // step 4: calendar events.insert (with the booking_id ownership tag).
    const deepLink = ctx.deepLinkBase ? `${ctx.deepLinkBase}/${encodeURIComponent(bookingId)}` : '';
    const requestBody = calendarEvents.buildEventBody({
      bookingId,
      appointmentTypeName: ctx.appointmentTypeName,
      attendeeFirstName: ctx.attendee.first_name,
      attendeeLastName: ctx.attendee.last_name,
      attendeeEmail: ctx.attendee.email,
      start: ctx.start, end: ctx.end, timezone: ctx.timezone,
      deepLink,
      conference,
    });
    event = await insertWithOAuthRetry({
      tenantId, coordinatorId, calendarId: coordinatorId, requestBody,
    });

    // Resolve the join URL + conference id (Meet's are known only post-insert).
    const joinUrl = conference.deferToCalendarInsert
      ? calendarEvents.extractMeetJoinUrl(event)
      : conference.joinUrl;
    const conferenceId = conference.deferToCalendarInsert
      ? (event.conferenceData && event.conferenceData.conferenceId) || null
      : conference.conferenceId;

    // step 5a: advance round-robin (reverted below if the Booking write fails).
    if (ctx.tieBreaker === 'round_robin' && ctx.roundRobinCursor) {
      await routing.advanceRoundRobin({
        tenantId,
        routingPolicyId: ctx.roundRobinCursor.routingPolicyId,
        assignedResourceId: resourceId,
      });
      rrAdvanced = true;
    }

    // step 5b: Booking record write (conditional, idempotent).
    const createdAt = new Date().toISOString();
    let bookingItem;
    try {
      bookingItem = await bookingStore.writeBooking({
        tenantId, bookingId, sessionId: ctx.sessionId,
        status: 'booked',
        start: ctx.start, end: ctx.end, timezone: ctx.timezone,
        coordinatorEmail: coordinatorId, resourceId,
        appointmentTypeId: ctx.appointmentTypeId,
        attendeeEmail: ctx.attendee.email,
        attendeeName: [ctx.attendee.first_name, ctx.attendee.last_name].filter(Boolean).join(' '),
        attendeePhone: ctx.attendee.phone,
        externalEventId: event.id,
        conferenceProvider: conference.provider,
        conferenceId,
        joinUrl,
        createdAt,
        lastCalendarMutationAt: event.updated || createdAt,
      });
    } catch (writeErr) {
      // A concurrent confirm already wrote the booking → we lost the race.
      if (bookingStore.isConditionalCheckFailed(writeErr)) {
        if (rrAdvanced) await safeRevertRR(tenantId, resourceId, ctx);
        await rollbackCalendarAndConference({ tenantId, coordinatorId, event, conference });
        await safeReleaseLock(tenantId, lockKey);
        const existing = await bookingStore.getBookingById(tenantId, bookingId);
        return { status: 'ALREADY_CONFIRMED', bookingId, booking: existing };
      }
      // Real write failure AFTER advance → revert RR, roll back event+conference.
      if (rrAdvanced) await safeRevertRR(tenantId, resourceId, ctx);
      await rollbackCalendarAndConference({ tenantId, coordinatorId, event, conference });
      await safeReleaseLock(tenantId, lockKey);
      throw writeErr;
    }

    // The Booking row now IS the source of truth — release the slot lock immediately
    // (BEFORE the email step), so a slow/failing email never holds the lock and the
    // slot frees up the instant the booking is durable.
    await safeReleaseLock(tenantId, lockKey);

    // step 6: confirmation email within the 60s SLA (AC #7). Email failure does NOT
    // roll back a committed booking (coordinator already sees the event; "never
    // half-book / fail forward", §5.5) — it alerts for async retry.
    const startedAt = ctx.startedAtMs;
    try {
      const sent = await sendConfirmationEmail({
        tenantId, bookingId,
        attendeeEmail: ctx.attendee.email,
        attendeeFirstName: ctx.attendee.first_name,
        appointmentTypeName: ctx.appointmentTypeName,
        orgName: ctx.orgName,
        coordinatorName: ctx.coordinatorName,
        coordinatorEmail: coordinatorId,
        start: ctx.start, end: ctx.end,
        whenLabel: formatWhen(ctx.start, ctx.timezone),
        joinUrl, deepLink,
        startAt: ctx.start,
        cancellationWindowHours: ctx.cancellationWindowHours,
      }, { signOpts: ctx.signOpts });
      const elapsed = Date.now() - startedAt;
      if (elapsed > CONFIRMATION_SLA_MS) {
        warn('confirmation_sla_exceeded', { booking_id: bookingId, elapsed_ms: elapsed });
        await alertAdmin('Scheduling: confirmation SLA exceeded', { bookingId, elapsedMs: elapsed });
      }
      log('confirmation_sent', { booking_id: bookingId, message_id: sent.messageId, elapsed_ms: elapsed });
    } catch (emailErr) {
      warn('confirmation_email_failed', { booking_id: bookingId, error: emailErr.message });
      await alertAdmin('Scheduling: confirmation email failed (booking valid)', {
        bookingId, error: emailErr.message,
      });
    }

    // (lock already released above, right after the Booking write.)
    return {
      // No coordinatorEmail in the response (§5.7 PII boundary): the caller has
      // resourceId; the coordinator identity is revealed in-conversation (C12), and
      // the email/identity live on the durable Booking row, not in this payload.
      status: 'BOOKED', bookingId, resourceId,
      externalEventId: event.id, joinUrl,
      conferenceProvider: conference.provider,
      booking: bookingItem,
    };
  } catch (err) {
    if (err instanceof DegradeCoordinator) {
      // Permanent OAuth revoke: durable marker + admin alert, then roll back THIS
      // attempt and let the caller re-pool against remaining candidates.
      try {
        await bookingStore.writeDegradedMarker(tenantId, coordinatorId, 'oauth_revoked');
      } catch (markErr) {
        warn('degraded_marker_failed', { coordinator_id_hash: hashId(coordinatorId), error: markErr.message });
      }
      await alertAdmin('Scheduling: coordinator OAuth revoked (degraded)', {
        tenantId, resourceId, coordinator_id_hash: hashId(coordinatorId),
      });
      if (rrAdvanced) await safeRevertRR(tenantId, resourceId, ctx);
      await rollbackCalendarAndConference({ tenantId, coordinatorId, event, conference });
      await safeReleaseLock(tenantId, lockKey);
      throw err; // caller's loop catches DegradeCoordinator and re-pools
    }
    // Any other failure (conference create, calendar timeout, etc.): compensate +
    // surface. revert RR if it advanced before the throw.
    if (rrAdvanced) await safeRevertRR(tenantId, resourceId, ctx);
    await rollbackCalendarAndConference({ tenantId, coordinatorId, event, conference });
    await safeReleaseLock(tenantId, lockKey);
    throw err;
  }
}

// ─── compensation primitives (best-effort; never throw past here) ─────────────────────

async function safeReleaseLock(tenantId, lockKey) {
  try {
    await bookingStore.releaseLock(tenantId, lockKey);
  } catch (err) {
    // Could not release → leave a queryable reconciliation flag for the ops sweep
    // (runbook: orphan slot_lock# items), and alarm.
    warn('lock_release_failed', { lock_key: lockKey, error: err.message });
    try {
      await bookingStore.flagLockForReconciliation(tenantId, lockKey, `release_failed:${err.message}`);
    } catch (_) { /* the alarm below is the backstop */ }
    await alertAdmin('Scheduling: slot-lock release failed (orphan lock)', { tenantId, lockKey });
  }
}

async function safeRevertRR(tenantId, resourceId, ctx) {
  try {
    await routing.revertRoundRobin({
      tenantId,
      routingPolicyId: ctx.roundRobinCursor.routingPolicyId,
      previousResourceId: ctx.roundRobinCursor.previousResourceId,
      previousAt: ctx.roundRobinCursor.previousAt,
    });
  } catch (err) {
    warn('round_robin_revert_failed', { tenant_id: tenantId, resource_id: resourceId, error: err.message });
    await alertAdmin('Scheduling: round-robin revert failed', { tenantId, resourceId });
  }
}

async function rollbackCalendarAndConference({ tenantId, coordinatorId, event, conference }) {
  // Delete the calendar event first (it carries the booking_id tag the listener acts on).
  if (event && event.id) {
    try {
      const client = await getOAuthClient({ tenantId, coordinatorId });
      await calendarEvents.deleteEvent(client, coordinatorId, event.id);
    } catch (err) {
      warn('event_rollback_failed', { event_id: event.id, error: err.message });
      await alertAdmin('Scheduling: calendar event rollback failed', { tenantId, eventId: event.id });
    }
  }
  // Delete an externally-minted Zoom meeting (Meet rides the event, already deleted).
  if (conference && conference.provider === 'zoom' && conference.conferenceId) {
    try {
      const zoom = require('./zoom-client');
      await zoom.deleteMeeting(tenantId, conference.conferenceId);
    } catch (err) {
      warn('zoom_rollback_failed', { conference_id: conference.conferenceId, error: err.message });
      await alertAdmin('Scheduling: Zoom meeting rollback failed (orphan meeting)', {
        tenantId, conferenceId: conference.conferenceId,
      });
    }
  }
}

// ─── input validation ─────────────────────────────────────────────────────────────────

const TENANT_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

function validate(event) {
  if (!event || typeof event !== 'object') throw new Error('event must be a JSON object');
  const tenantId = event.tenant_id;
  if (!tenantId || !TENANT_ID_RE.test(tenantId)) {
    throw new Error('tenant_id is required and must match /^[A-Za-z0-9_-]{1,64}$/');
  }
  if (!event.session_id) throw new Error('session_id is required');
  const slot = event.slot;
  if (!slot || !slot.start || !slot.end || !Array.isArray(slot.candidateResourceIds) || slot.candidateResourceIds.length === 0) {
    throw new Error('slot.{start,end,candidateResourceIds[]} are required');
  }
  if (!event.attendee || !event.attendee.email) {
    throw new Error('attendee.email is required');
  }
  const conferenceType = event.conference_type || 'google_meet';
  if (!['google_meet', 'zoom', 'null'].includes(conferenceType)) {
    throw new Error(`unknown conference_type: ${conferenceType}`);
  }
  if (typeof event.pool_size !== 'number' || event.pool_size < 1) {
    throw new Error('pool_size (the routing pool size, >= 1) is required');
  }
  return { tenantId, slot, conferenceType };
}

// ─── handler ──────────────────────────────────────────────────────────────────────────

exports.handler = async function handler(event, _lambdaCtx, injected = {}) {
  // Tier-2 calendar-mutation executor (option d): BSH invokes BCH for an
  // already-§B14-authorized reschedule/cancel. Routed before the commit flow; it
  // shares BCH's Google-auth + calendar/conference/zoom modules but NOT the commit
  // path. The state machine is NOT re-run here — BSH validated the transition.
  if (event && event.action === 'scheduling_mutate') {
    // Feature gate (defense-in-depth — BSH already gates before invoking): refuse a
    // calendar mutation for a tenant without scheduling_enabled. Fail-closed.
    const enabled = await (injected.isSchedulingEnabledForTenant || featureGate.isSchedulingEnabledForTenant)(event.tenantId, injected);
    if (!enabled) {
      log('scheduling_mutate_disabled', { mutation: event.mutation });
      return { outcome: 'failed', error: 'scheduling_disabled' };
    }
    const { handleSchedulingMutate } = require('./scheduling-mutate');
    return await handleSchedulingMutate(event, injected);
  }

  const startedAtMs = Date.now();
  const { tenantId, slot, conferenceType } = validate(event);

  // Feature gate: scheduling is OFF unless the tenant config sets feature_flags.
  // scheduling_enabled (like Forms). Fail-closed — a config we cannot read → refuse.
  const schedulingEnabled = await (injected.isSchedulingEnabledForTenant || featureGate.isSchedulingEnabledForTenant)(tenantId, injected);
  if (!schedulingEnabled) {
    log('commit_scheduling_disabled', { tenant_id: tenantId });
    return { status: 'SCHEDULING_DISABLED', reason: 'feature_not_enabled' };
  }
  const appt = event.appointment_type || {};

  const bookingId = bookingStore.buildBookingId(tenantId, event.session_id, slot.start);
  log('commit_invoked', {
    tenant_id: tenantId, booking_id: bookingId, conference_type: conferenceType,
    pool_size: event.pool_size,
  });

  // step 0: idempotency gate (AC #6 / C11).
  const existing = await bookingStore.getBookingById(tenantId, bookingId);
  if (existing && existing.status && existing.status.S === 'booked') {
    log('already_confirmed', { tenant_id: tenantId, booking_id: bookingId });
    return { status: 'ALREADY_CONFIRMED', bookingId, booking: existing };
  }

  // shared context threaded through the attempt loop.
  const ctx = {
    sessionId: event.session_id,
    start: slot.start, end: slot.end,
    timezone: appt.timezone || event.user_time_zone || 'UTC',
    appointmentTypeId: appt.id,
    appointmentTypeName: appt.name,
    cancellationWindowHours: appt.cancellation_window_hours || 0,
    attendee: event.attendee,
    conferenceType,
    coordinatorEmails: event.coordinator_emails || {},
    coordinatorName: event.coordinator_name || '',
    orgName: event.org_name || '',
    deepLinkBase: event.deep_link_base || '',
    tieBreaker: event.tie_breaker,
    roundRobinCursor: event.round_robin_cursor || null,
    startedAtMs,
    // DI seams for tests (NullConferenceProvider / Zoom client double / token key).
    providerOverrides: injected.providerOverrides || {},
    signOpts: injected.signOpts,
  };

  // step 1: live freeBusy re-check → surviving candidates (tie-broken order).
  const survivors = await liveFreeBusyRecheck({
    tenantId, candidateResourceIds: slot.candidateResourceIds,
    coordinatorEmails: ctx.coordinatorEmails, start: slot.start, end: slot.end,
  });
  if (survivors.length === 0) {
    log('reoffer_recheck_busy', { tenant_id: tenantId, booking_id: bookingId });
    return { status: 'SLOT_UNAVAILABLE', action: 'reoffer', reason: 'recheck_busy' };
  }

  // attempt loop: lock → commit; on DegradeCoordinator drop that resource + re-pool.
  let remaining = [...survivors];
  let attempt = Number(event.attempt) || 1;
  while (remaining.length > 0) {
    // step 2: C6 conditional-write slot lock over the remaining candidates.
    const lock = await pool.lockSlot({
      tenantId, format: appt.format || 'one_to_one',
      start: slot.start, end: slot.end,
      candidateResourceIds: remaining,
      poolSize: event.pool_size,
      attempt,
    });
    if (lock.status !== 'LOCKED') {
      log('reoffer_lock_unavailable', { tenant_id: tenantId, booking_id: bookingId, ...lock });
      return lock; // SLOT_UNAVAILABLE (carries nextAttempt / soloExhausted per C6)
    }

    try {
      const result = await commitAgainstResource({
        tenantId, resourceId: lock.resourceId, lockKey: lock.lockKey, bookingId, ctx,
      });
      log('commit_result', { tenant_id: tenantId, booking_id: bookingId, status: result.status, resource_id: result.resourceId });
      return result;
    } catch (err) {
      if (err instanceof DegradeCoordinator) {
        // re-pool: exclude the degraded coordinator, lock the next candidate.
        remaining = remaining.filter((r) => r !== lock.resourceId);
        attempt += 1;
        warn('coordinator_degraded_repool', { tenant_id: tenantId, resource_id: lock.resourceId, remaining: remaining.length });
        continue;
      }
      // graceful failure surfaced to the conversation (lock already released).
      warn('commit_failed', { tenant_id: tenantId, booking_id: bookingId, error: err.message });
      return { status: 'COMMIT_FAILED', action: 'graceful_error', reason: err.message };
    }
  }

  // every candidate degraded out → reoffer (admin already alerted per degrade).
  log('reoffer_all_degraded', { tenant_id: tenantId, booking_id: bookingId });
  return { status: 'SLOT_UNAVAILABLE', action: 'reoffer', reason: 'all_candidates_degraded' };
};

// ─── test-only exports ────────────────────────────────────────────────────────────────

exports._test = {
  DegradeCoordinator,
  resolveCoordinatorId,
  intervalsOverlap,
  formatWhen,
  liveFreeBusyRecheck,
  insertWithOAuthRetry,
  commitAgainstResource,
  rollbackCalendarAndConference,
  safeReleaseLock,
  safeRevertRR,
  validate,
};
