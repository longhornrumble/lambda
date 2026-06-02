'use strict';

/**
 * reschedule.js — §B9 reschedule execution module (WS-D6).
 *
 * Canonical §9.4 (reschedule = cancel + rebook under the hood); plan D6 (the four
 * outcomes verbatim); FROZEN_CONTRACTS.md §B9 (the locked `executeReschedule`
 * signature + four-outcome contract) and §B6 (`ConferenceProvider`).
 *
 * The calendar MOVE that runs IN-CHAT after the volunteer confirms the new slot —
 * NOT at the redemption endpoint (§13.4: the token authenticates ENTRY only; the
 * state-changing op happens later, in the live session, validated against the §B10
 * session binding). WS-D4 owns token validation + the jti one-time write + the
 * binding; this module performs NEITHER and persists NOTHING — it returns the
 * updated in-memory `booking` for the caller to persist.
 *
 * ── "Atomic" is aspirational — Google has no atomic-move API ──
 *   Locked ordering (§D6): events.insert(new) FIRST, events.delete(old) SECOND. The
 *   four real outcomes are the (insert, delete) truth table:
 *     (i)   insert ✓ + delete ✓ → 'success'            (clean move)
 *     (ii)  insert ✓ + delete ✗ → 'pending_calendar_sync'  (new live; old lingers;
 *             set pending_calendar_sync + store rescheduled_old_event_id; the E9
 *             nightly reconciler retries the delete — the volunteer sees only the
 *             new invite)
 *     (iii) insert ✗ + delete ✓ → 'canceled_insert_failed' — UNREACHABLE from
 *             executeReschedule (see the B-1 guard below); kept only as the pure
 *             classifyOutcome truth-table label.
 *     (iv)  insert ✗ (delete skipped) → 'failed'        (no state change; retry)
 *   Insert-first is locked because, of the partial-failure cells, it makes the COMMON
 *   one (ii) "two invites, recoverable by the reconciler" rather than zero-invites.
 *
 *   ✓ Integrator-resolved (FROZEN §C, operator 2026-06-02 — was a flagged interpretation,
 *   NOT a fork): the delete is GUARDED on insertOk (B-1). If the insert fails, the old
 *   event is NEVER deleted → a transient insert hiccup (Zoom secret, Google 500) can never
 *   strand the volunteer with zero invites; insert✗ lands in (iv) 'failed' (retryable).
 *   Outcome (iii) 'canceled_insert_failed' is therefore unreachable here; §B9 + canonical
 *   plan D6 were corrected to match (the "delete✓+insert✗" framing was a delete-first vestige).
 *
 * ── Zoom join-URL preservation (§B6) ──
 *   The new event's conference is resolved through the injected §B6 ConferenceProvider
 *   with `existingConferenceId` = the booking's current conference id. ZoomProvider's
 *   read-before-write reuses the SAME meeting → the join URL is PRESERVED across the
 *   move. GoogleMeetProvider defers to events.insert (a fresh Meet link rides the new
 *   event — Meet links are calendar-event-bound, so preservation is a documented
 *   no-op). NullConferenceProvider yields a synthetic id (no-op).
 *
 * ── DI seam (matches the C8 / cal-lifecycle pattern) ──
 *   All I/O is injected via `deps = { calendar, conference, ddb, alertAdmin, logger }`
 *   — no module-level AWS/Google clients. `deps.calendar` is the integrator's
 *   AUTH-BOUND wrapper over the shipped C8 `Booking_Commit_Handler/calendar-events.js`
 *   (auth lives there because §B9 froze `deps` without an authClient slot):
 *     buildEventBody(params) → requestBody          // §5.7-compliant body + ownership tag
 *     insertEvent(calendarId, requestBody) → event  // event.id, event.conferenceData
 *     deleteEvent(calendarId, eventId) → void       // 404/410 idempotent inside
 *     extractMeetJoinUrl(event) → string | null
 *   `deps.conference` is the resolved §B6 provider (createConference(ctx)). `deps.ddb`
 *   and `deps.alertAdmin` are part of the shared §B9 deps shape but UNUSED here — this
 *   module never persists (the caller does) and, post-B-1, never destructively cancels
 *   (so there is nothing to alert on; both stay reserved for shape symmetry). `deps.logger`
 *   logs PII-redacted (booking_id + outcome only — never attendee email/name/phone), and
 *   SR-2: error fields log err.code/err.name, never err.message (Google errors embed PII).
 *   MUTATES the booking IN PLACE and returns the same object for the caller to persist
 *   (SR-3 resolved this way — consistent with the test suite + reviewer's "test is authority";
 *   D7 cancel.js + §B9 aligned to mutate-in-place; a caller wanting isolation defends-copies).
 */

// ─── booking field reads (schema discipline — tolerate camel OR snake, missing → undefined) ──

function pick(booking, camel, snake) {
  if (!booking) return undefined;
  return booking[camel] != null ? booking[camel] : booking[snake];
}

function splitName(booking) {
  let first = pick(booking, 'attendeeFirstName', 'attendee_first_name');
  let last = pick(booking, 'attendeeLastName', 'attendee_last_name');
  if (first == null && last == null) {
    const full = pick(booking, 'attendeeName', 'attendee_name');
    if (typeof full === 'string') {
      const parts = full.trim().split(/\s+/).filter(Boolean);
      first = parts.shift() || '';
      last = parts.join(' ');
    }
  }
  return { first: first || '', last: last || '' };
}

// ─── outcome classification — the four §D6 cells ───────────────────────────────────────

const OUTCOME = {
  SUCCESS: 'success',
  PENDING_CALENDAR_SYNC: 'pending_calendar_sync',
  CANCELED_INSERT_FAILED: 'canceled_insert_failed',
  FAILED: 'failed',
};

function classifyOutcome(insertOk, deleteOk) {
  if (insertOk && deleteOk) return OUTCOME.SUCCESS; // (i)
  if (insertOk && !deleteOk) return OUTCOME.PENDING_CALENDAR_SYNC; // (ii)
  if (!insertOk && deleteOk) return OUTCOME.CANCELED_INSERT_FAILED; // (iii)
  return OUTCOME.FAILED; // (iv)
}

// ─── conference shape adapter (§B6 createConference result → calendar-events conference) ──

// buildEventBody wants { provider, joinUrl?, conferenceId?, calendarCreateRequest? }.
function confToEventConference(conf) {
  if (conf.deferToCalendarInsert) {
    // Google Meet: the link is minted by the same events.insert (createRequest).
    return { provider: conf.provider, calendarCreateRequest: conf.calendarCreateRequest };
  }
  // Zoom / Null: an externally-minted (or preserved) join URL attached to the event.
  return { provider: conf.provider, joinUrl: conf.joinUrl, conferenceId: conf.conferenceId };
}

// ─── PII-redacted structured log ───────────────────────────────────────────────────────

function logEvent(logger, level, event, fields) {
  const line = JSON.stringify({ event, level: level.toUpperCase(), ...fields });
  const sink = logger || console;
  if (level === 'error' && sink.error) return sink.error(line);
  if (level === 'warn' && sink.warn) return sink.warn(line);
  return (sink.info || sink.log || console.log).call(sink, line);
}

// ─── executeReschedule (frozen §B9) ─────────────────────────────────────────────────────

async function executeReschedule({ booking, newSlot, deps } = {}) {
  if (!booking) throw new Error('executeReschedule requires booking');
  if (!newSlot || !newSlot.start || !newSlot.end) {
    throw new Error('executeReschedule requires newSlot.start and newSlot.end');
  }
  if (!deps || !deps.calendar || !deps.conference) {
    throw new Error('executeReschedule requires deps.calendar and deps.conference');
  }

  const logger = deps.logger;
  const bookingId = pick(booking, 'bookingId', 'booking_id');
  const tenantId = pick(booking, 'tenantId', 'tenant_id');
  const oldEventId = pick(booking, 'externalEventId', 'external_event_id');
  // v1: resourceId == coordinatorEmail == the coordinator's calendar id (§B7).
  const calendarId =
    pick(booking, 'coordinatorEmail', 'coordinator_email') ||
    pick(booking, 'resourceId', 'resource_id');
  const resourceId = pick(booking, 'resourceId', 'resource_id') || calendarId;

  // Caller-contract preconditions (distinct from the calendar-op outcomes below).
  if (!oldEventId) {
    throw new Error('executeReschedule requires booking.external_event_id (the old event to move)');
  }
  if (!calendarId) {
    throw new Error('executeReschedule requires booking.coordinator_email (the calendar id)');
  }
  // SR-4: guard the identity fields too — without them the logs (and any alert) carry undefined.
  if (!bookingId) {
    throw new Error('executeReschedule requires booking.booking_id');
  }
  if (!tenantId) {
    throw new Error('executeReschedule requires booking.tenant_id');
  }

  const now = (deps.now || (() => new Date().toISOString()))();

  // ── step 1: insert the NEW event FIRST (locked §D6 ordering) ───────────────────────────
  let insertOk = false;
  let newEventId;
  let newJoinUrl;
  let newConferenceId;
  try {
    // 1a. resolve the conference for the new event — preserves the Zoom join URL by
    //     reusing the existing meeting (§B6 read-before-write). Meet/Null no-op.
    const conf = await deps.conference.createConference({
      tenantId,
      coordinatorId: resourceId,
      bookingId,
      topic: pick(booking, 'appointmentTypeName', 'appointment_type_name'),
      start: newSlot.start,
      end: newSlot.end,
      timezone: pick(booking, 'timeZone', 'timezone'),
      attendeeEmail: pick(booking, 'attendeeEmail', 'attendee_email'),
      existingConferenceId: pick(booking, 'conferenceId', 'conference_id') || undefined,
    });

    // 1b. build the §5.7-compliant body (ownership tag = the UNCHANGED booking_id, so
    //     the B2 listener still resolves this same booking) via the C8 builder.
    const { first, last } = splitName(booking);
    const requestBody = deps.calendar.buildEventBody({
      bookingId,
      appointmentTypeName: pick(booking, 'appointmentTypeName', 'appointment_type_name'),
      attendeeFirstName: first,
      attendeeLastName: last,
      attendeeEmail: pick(booking, 'attendeeEmail', 'attendee_email'),
      start: newSlot.start,
      end: newSlot.end,
      timezone: pick(booking, 'timeZone', 'timezone'),
      deepLink: pick(booking, 'deepLink', 'deep_link'),
      conference: confToEventConference(conf),
    });

    // 1c. insert.
    const inserted = await deps.calendar.insertEvent(calendarId, requestBody);
    newEventId = inserted && inserted.id;
    insertOk = !!newEventId;

    // 1d. resolve the new join URL: Meet rides the insert; Zoom/Null are minted up front.
    newJoinUrl = conf.deferToCalendarInsert
      ? deps.calendar.extractMeetJoinUrl(inserted)
      : conf.joinUrl;
    newConferenceId = conf.deferToCalendarInsert
      ? (inserted && inserted.conferenceData && inserted.conferenceData.conferenceId) || null
      : conf.conferenceId;
  } catch (err) {
    insertOk = false;
    // SR-2: log a non-PII discriminator, NOT err.message — Google errors embed the
    // calendar id (coordinator email): "Calendar 'maya@org' not found".
    logEvent(logger, 'error', 'reschedule_insert_failed', {
      booking_id: bookingId,
      error: (err && (err.code || err.name)) || 'error',
    });
  }

  // ── step 2: delete the OLD event SECOND — ONLY if the new one is live (B-1 guard) ──────
  // Insert-first exists to PREFER "two invites (recoverable)" over "zero invites
  // (unrecoverable)". So if the insert failed, we must NOT delete the old event — a
  // transient insert hiccup (Zoom secret, Google 500) would otherwise strand the volunteer
  // with no booking. Insert✗ therefore lands in outcome (iv) 'failed' (no state change,
  // retryable). This makes outcome (iii) 'canceled_insert_failed' UNREACHABLE from
  // executeReschedule (classifyOutcome keeps it only as the pure-truth-table label).
  let deleteOk = false;
  if (insertOk) {
    try {
      await deps.calendar.deleteEvent(calendarId, oldEventId);
      deleteOk = true;
    } catch (err) {
      deleteOk = false;
      logEvent(logger, 'warn', 'reschedule_delete_failed', {
        booking_id: bookingId,
        error: (err && (err.code || err.name)) || 'error',
      });
    }
  }

  // ── step 3: classify + mutate the booking IN MEMORY (caller persists; same object back) ─
  const outcome = classifyOutcome(insertOk, deleteOk);
  // B-2: write back in the SAME casing the caller used (camel OR snake) so a later pick()
  // read (incl. a retry) never sees a stale camelCase field shadowing the new snake value.
  const isCamel = booking.externalEventId !== undefined || booking.bookingId !== undefined;
  const set = (snakeKey, camelKey, val) => {
    booking[isCamel ? camelKey : snakeKey] = val;
  };

  if (outcome === OUTCOME.SUCCESS || outcome === OUTCOME.PENDING_CALENDAR_SYNC) {
    // The new event owns the booking now — point the row at it and stamp the move.
    set('external_event_id', 'externalEventId', newEventId);
    set('start_at', 'startAt', newSlot.start);
    set('end_at', 'endAt', newSlot.end);
    set('last_calendar_mutation_at', 'lastCalendarMutationAt', now);
    if (newJoinUrl != null) set('channel_details', 'channelDetails', newJoinUrl);
    if (newConferenceId != null) set('conference_id', 'conferenceId', newConferenceId);
  }

  if (outcome === OUTCOME.PENDING_CALENDAR_SYNC) {
    // (ii) old delete failed → the E9 reconciler retries it; flag + remember the orphan.
    set('pending_calendar_sync', 'pendingCalendarSync', true);
    set('rescheduled_old_event_id', 'rescheduledOldEventId', oldEventId);
  } else if (outcome === OUTCOME.SUCCESS) {
    // Clean move — clear any stale sync bookkeeping a prior attempt may have left.
    const psKey = isCamel ? 'pendingCalendarSync' : 'pending_calendar_sync';
    if (psKey in booking) booking[psKey] = false;
    delete booking[isCamel ? 'rescheduledOldEventId' : 'rescheduled_old_event_id'];
  }
  // (iv) FAILED → no state change (the B-1 guard guarantees the old event is untouched on
  // insert✗, so the booking is intact and the volunteer can retry). No admin alert: a
  // failed move is transient/retryable, not a destructive cancel.

  logEvent(logger, 'info', 'reschedule_outcome', { booking_id: bookingId, outcome });

  return { outcome, booking, newEventId, oldEventId };
}

module.exports = {
  executeReschedule,
  classifyOutcome,
  confToEventConference,
  OUTCOME,
};
