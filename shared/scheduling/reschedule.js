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
 *     (iii) insert ✗ + delete ✓ → 'canceled_insert_failed' (old gone, no usable new;
 *             status=canceled + alertAdmin → treat as cancel + manual rebook; chat
 *             surfaces "your booking was canceled — please pick a new time")
 *     (iv)  insert ✗ + delete ✗ → 'failed'             (no state change; retry)
 *   Insert-first is locked because, of the two partial-failure cells, it makes the
 *   COMMON one (ii) "two invites, recoverable by the reconciler" rather than (iii)
 *   "zero invites, unrecoverable" — better two than zero. Outcome (iii) is the rare
 *   genuinely-bad cell the state machine still handles gracefully.
 *
 *   ⚑ Flagged for the integrator (FROZEN §C — interpretation, NOT a fork): this
 *   module attempts BOTH ops (insert first) and classifies on the truth table, so
 *   outcome (iii) (insert✗ + delete✓) is a reachable path — required because the
 *   §B9 done-bar mandates an outcome-(iii) test asserting status=canceled +
 *   alertAdmin. If the canonical intent is instead to SHORT-CIRCUIT on insert
 *   failure (skip the delete so a transient insert hiccup can never strand the user
 *   → (iii) becomes unreachable, (iv) absorbs it), that is a one-line guard on the
 *   delete step — confirm the intended (iii) reachability.
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
 *   is part of the shared §B9 deps shape but UNUSED here — this module never persists
 *   (the caller does). `deps.alertAdmin(info)` fires on outcome (iii). `deps.logger`
 *   logs PII-redacted (booking_id + outcome only — never attendee email/name/phone).
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
    logEvent(logger, 'error', 'reschedule_insert_failed', {
      booking_id: bookingId,
      error: err && err.message,
    });
  }

  // ── step 2: delete the OLD event SECOND ───────────────────────────────────────────────
  let deleteOk = false;
  try {
    await deps.calendar.deleteEvent(calendarId, oldEventId);
    deleteOk = true;
  } catch (err) {
    deleteOk = false;
    logEvent(logger, 'warn', 'reschedule_delete_failed', {
      booking_id: bookingId,
      error: err && err.message,
    });
  }

  // ── step 3: classify + mutate the booking IN MEMORY (caller persists) ─────────────────
  const outcome = classifyOutcome(insertOk, deleteOk);

  if (outcome === OUTCOME.SUCCESS || outcome === OUTCOME.PENDING_CALENDAR_SYNC) {
    // The new event owns the booking now — point the row at it and stamp the move.
    booking.external_event_id = newEventId;
    booking.start_at = newSlot.start;
    booking.end_at = newSlot.end;
    booking.last_calendar_mutation_at = now;
    if (newJoinUrl != null) booking.channel_details = newJoinUrl;
    if (newConferenceId != null) booking.conference_id = newConferenceId;
  }

  if (outcome === OUTCOME.PENDING_CALENDAR_SYNC) {
    // (ii) old delete failed → the E9 reconciler retries it; flag + remember the orphan.
    booking.pending_calendar_sync = true;
    booking.rescheduled_old_event_id = oldEventId;
  } else if (outcome === OUTCOME.SUCCESS) {
    // Clean move — clear any stale sync bookkeeping a prior attempt may have left.
    if ('pending_calendar_sync' in booking) booking.pending_calendar_sync = false;
    delete booking.rescheduled_old_event_id;
  } else if (outcome === OUTCOME.CANCELED_INSERT_FAILED) {
    // (iii) old gone, no usable new event → treat as cancel + manual rebook.
    booking.status = 'canceled';
    booking.last_calendar_mutation_at = now;
    if (typeof deps.alertAdmin === 'function') {
      await deps.alertAdmin({
        kind: 'reschedule_insert_failed',
        tenantId,
        booking_id: bookingId,
        old_event_id: oldEventId,
      });
    } else {
      logEvent(logger, 'error', 'reschedule_alert_admin_missing', { booking_id: bookingId });
    }
  }
  // (iv) FAILED → no state change.

  logEvent(logger, 'info', 'reschedule_outcome', { booking_id: bookingId, outcome });

  return { outcome, booking, newEventId, oldEventId };
}

module.exports = {
  executeReschedule,
  classifyOutcome,
  confToEventConference,
  OUTCOME,
};
