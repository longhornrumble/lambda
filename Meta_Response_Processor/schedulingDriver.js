'use strict';

/**
 * M8a — Messenger in-chat scheduling ("book"). docs/messenger/CONTRACTS.md C3
 * (`PIC1:sched:{op}:{arg}`), C4 (`scheduling_session` row), C5 (carousel ≤10
 * cards), C7 (index.js's existing per-conversation lock owns serialization —
 * this module contributes no locking of its own), C9 (carousel + free-text
 * fallback).
 *
 * Flow: a `start_scheduling` CTA tap (config.ts CTAActionType/CTAType marker
 * — see renderMessengerActions.js) resolves an appointment type → BCH
 * `scheduling_propose` → slot carousel + numbered text fallback → slot pick
 * (tap or typed number, C9) → email (M7a-style, reusing formEngine's
 * validator) → phone (mandatory on Instagram — D9, IG has no post-window SMS
 * lane; skippable on Facebook via the literal word "skip") → confirm → BCH's
 * default commit route → confirmation + best-effort SMS-consent record.
 *
 * G-P4 (light gate) bindings implemented here:
 *   T1' — expires_at is epoch SECONDS, refreshed on every step
 *         (SCHEDULING_SESSION_TTL_SECONDS, 1 h idle).
 *   T2' — DELETE on every successful commit, INCLUDING a conflict-retry's
 *         eventual success (the retry re-enters the same stage machine, so
 *         the one delete-on-success code path covers both).
 *   T3' — a terminal failure (re-propose-also-failed after a slot conflict,
 *         or a non-slot-unavailable commit failure) leaves the row
 *         COMPLETELY untouched — no save, no TTL bump — mirroring
 *         formEngine's confirmForm T3.
 *   C1  — the phone-capture prompt carries the consent language VERBATIM
 *         (config `messenger_behavior.strings.sms_consent`, else
 *         DEFAULT_SMS_CONSENT). The exact rendered string is persisted on the
 *         row (`consent_language_shown`) the moment it is shown, so the
 *         commit-time consent write uses precisely what the user read.
 *   C2/C3/C4 — recordBookingSmsConsent is invoked ONLY when a phone was
 *         captured, ONLY after a successful commit (best-effort — never
 *         blocks or reverts the booking), with the exact
 *         `consent_language_shown` + `source: 'messenger_booking_fb'` /
 *         `'messenger_booking_ig'`.
 *   C5  — phone is normalized via shared/scheduling/phone.js `toE164` at
 *         CAPTURE time with re-prompt on failure; an un-normalizable value is
 *         NEVER stored and can therefore never reach commit — "invalid_phone
 *         at commit" is structurally impossible, not just checked.
 *   C7  — every log call below passes only stage / booking-id / boolean
 *         field-presence — never `session.contact.email` or `.phone`.
 *   C8  — `advanceScheduling`'s confirm branch refuses to invoke the commit
 *         (logs, does not throw) when `session.contact?.email` is missing.
 *         Unreachable given the stage machine (email precedes confirm) — the
 *         guard exists so a corrupted/hand-edited row can never book a dead
 *         inbox.
 *
 * DI seam: `deps.invokeProposal` / `deps.invokeCommit` — both plain async
 * functions index.js binds to a single RequestResponse Lambda invoke of
 * Booking_Commit_Handler (BOOKING_COMMIT_FUNCTION), distinguished by the
 * payload's own `action` field — mirrors BSH's schedulingFlow.js /
 * newBookingFlow.js, which invoke the identical function for both routes.
 * `deps.recordConsent` defaults to the real shared/scheduling/consent.js
 * writer. Absent invoke deps ⇒ the flow degrades to an apologetic message,
 * never a crash (same posture as formEngine.confirmForm's MFS_FUNCTION-unset
 * guard).
 *
 * KNOWN v1 DEVIATION (documented, not hidden): Messenger has no client-side
 * JS, so there is no way to read the user's IANA timezone the way the widget
 * does. v1 renders slot times in the tenant's CONFIGURED appointment-type
 * business timezone and labels every card + the intro line with its explicit
 * abbreviation (tz_label from BCH's scheduling_propose `context`) — never a
 * bare time, but not auto-localized to the user's device.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * M8b — Messenger scheduling "manage" (reschedule/cancel of a PAST booking).
 * docs/messenger/CONTRACTS.md C3 (new `sched` ops `mcancel`/`mresched`/
 * `mabort` — additive under the existing `PIC1:sched:{op}:{arg}` route,
 * which C3 documents as extensible: "ops defined in M8a, e.g. `slot:{arg}`"),
 * C9 (free-text fallback), CB config.ts `CTAActionType` `resume_scheduling`
 * (already reserved for M8b by the M8a doc comment in
 * renderMessengerActions.js).
 *
 * BOOKING LOOKUP MECHANISM (verified from source, not assumed):
 *   Booking_Commit_Handler's `scheduling_mutate` action
 *   (Booking_Commit_Handler/scheduling-mutate.js:109 `handleSchedulingMutate`)
 *   is a PURE EXECUTOR — it does NOT look up the booking itself. Every real
 *   caller (BSH's schedulingFlow.js `_executeViaExecutor`, Scheduling_Page_
 *   Api/index.js's `mutate` action) already holds a loaded Booking row and a
 *   `coordinatorId`, and BOTH derive it via their OWN direct DynamoDB read of
 *   the Booking table (`picasso-booking-{env}`) — a per-Lambda IAM grant this
 *   Meta processor does NOT have (and this subphase does not add one; a new
 *   IAM grant is a Terraform change, out of scope for OWN: schedulingDriver.js/
 *   index.js). Instead: M8a's own `commitAndRecordConsent` ALREADY receives
 *   the freshly-committed booking on `res.booking` (BCH's commit-success
 *   response, Booking_Commit_Handler/index.js:473 `booking: bookingItem`) —
 *   data we already possess at that exact moment, no extra read needed. M8b
 *   extends that one commit path to persist a C4-additive `last_booking` row
 *   (PK/SK unchanged, new stateType — no C4 amendment) carrying a PII-
 *   minimized projection of that SAME booking (MUTATE_BOOKING_FIELDS below,
 *   copied verbatim from BSH's already-audited `_EXEC_BOOKING_FIELDS`,
 *   Bedrock_Streaming_Handler_Staging/scheduling/schedulingFlow.js:~225) plus
 *   the derived `coordinator_id`. M8b's manage flow reads ONLY this row —
 *   zero Booking-table access, ever.
 *
 *   `res.booking` is the RAW DynamoDB item Booking_Commit_Handler/booking-
 *   store.js's `buildBookingItem` writes (every attribute wrapped via its
 *   local `s()` string-AttributeValue helper — `{ coordinator_email: {S:
 *   '...'}, ... }`), because `writeBooking`/`getBookingById` use the
 *   low-level `DynamoDBClient` (not the Document client) — confirmed by
 *   reading booking-store.js directly. `unmarshallBookingItem` below is a
 *   minimal S-only unmarshal, the same shape as Scheduling_Page_Api/
 *   index.js's local `unmarshall()` helper (never re-derived, independently
 *   reconstructed here only because it's a few lines and pulling in
 *   `@aws-sdk/util-dynamodb` for one caller wasn't worth a new dependency).
 *
 * PINNED scheduling_mutate INVOKE SHAPE (verbatim from
 * Bedrock_Streaming_Handler_Staging/scheduling/schedulingFlow.js
 * `_executeViaExecutor`, validated server-side by Booking_Commit_Handler/
 * scheduling-mutate.js:127-136):
 *   { action: 'scheduling_mutate', mutation: 'cancel'|'reschedule', tenantId,
 *     coordinatorId, booking: <projected fields>, newSlot?: {start, end} }
 *   Response: { outcome: 'success'|'deleted'|'pending_calendar_sync'|'failed'
 *     |'rate_limited'|'slot_unavailable', booking?, error? } — cancel emits
 *     'deleted'|'pending_calendar_sync'; reschedule emits 'success'|
 *     'pending_calendar_sync'|'slot_unavailable' (FS7 live re-check)|'failed'.
 *
 * FLOW:
 *   Entry (no active scheduling_session — checked by index.js's existing
 *   precedence order): a manage-intent keyword ("reschedule" / "cancel my
 *   appointment", tight word-boundary regex — KNOWN v1 DEVIATION: this is
 *   keyword matching, not NLU, so an off-topic mention like "what's your
 *   reschedule policy?" can false-positive into a manage prompt; disclosed,
 *   not hidden), OR a `resume_scheduling` CTA tap (ambiguous — shows a
 *   two-option menu), OR a typed/tapped confirm|abort of a PREVIOUSLY shown
 *   manage-confirm prompt (`last_booking.pending_manage_action`).
 *     -> no last_booking (absent or T2-expired) => graceful decline, ZERO
 *        invokes (`resolveManageTrigger`/`handleManageTrigger` never call
 *        deps.invokeProposal/invokeMutate on this path — structurally, not
 *        just by convention).
 *     -> found => build an explicit-confirmation message naming the target
 *        ("Your {slot_label} appointment — cancel it?") with QRs
 *        `PIC1:sched:mcancel:{bookingId}` / `PIC1:sched:mresched:{bookingId}`
 *        (C9 free-text fallback: typed "yes"/"confirm" resolves identically
 *        via `pending_manage_action`, typed "cancel"/"never mind" aborts).
 *        NEVER mutates on this turn — the row write is ONLY
 *        `pending_manage_action`, never a BCH invoke.
 *   Cancel (`execute_cancel`): re-verifies the payload's bookingId against
 *     the CURRENT last_booking (defense against a stale tap after a NEW
 *     booking replaced the row) -> BCH `scheduling_mutate` cancel -> success:
 *     delete last_booking + confirmation string; failure: apologize, row
 *     kept UNTOUCHED (retry by saying "confirm" again).
 *   Reschedule (`execute_reschedule`): re-verify -> BCH `scheduling_propose`
 *     (reusing the exact M8a action, keyed off the booking's stored
 *     `appointment_type_id`) -> a NEW `scheduling_session` row
 *     (`mode:'manage_reschedule'`, carrying the last_booking's
 *     `coordinator_id`/`booking` snapshot) -> carousel (reuses
 *     `buildSlotMessages`) -> slot pick (reuses `findChosenSlot`, dispatched
 *     from `advanceScheduling`'s STAGE_PROPOSING case) -> BCH
 *     `scheduling_mutate` reschedule with `newSlot` -> success: patch
 *     last_booking's `slot_label` (booking_id is unchanged — a reschedule
 *     moves the SAME booking); `slot_unavailable`/failure: apologize, END the
 *     manage attempt (last_booking untouched — a fresh "reschedule" re-
 *     proposes). Scope note: unlike M8a's book flow, M8b does NOT
 *     auto-retry a slot conflict inline — not required by this subphase's
 *     DONE line, and re-triggering "reschedule" already re-proposes fresh
 *     slots against the SAME still-valid last_booking.
 *   Universal in-flow cancel ("cancel"/"exit"/"quit"/"stop"/"never mind",
 *   any stage) during an ACTIVE manage_reschedule session abandons the
 *   RESCHEDULE ATTEMPT only — the underlying booking is untouched, and the
 *   message says so explicitly (`DEFAULT_MANAGE_RESCHEDULE_ABORTED`), never
 *   the book-flow's `DEFAULT_SCHEDULING_CANCELLED` text (which would
 *   misleadingly imply the original appointment was cancelled).
 *   Ambiguity rule (plan §6 M8b): manage-intent keywords/CTA are only
 *   evaluated when NO scheduling_session is already active — an in-flight
 *   book OR manage flow owns the turn first (index.js's existing
 *   precedence), so "cancel" mid-book still means "abandon this booking
 *   attempt", never "cancel my past appointment".
 *   Escalation/rate-limit gates are unconditionally ahead of ALL scheduling
 *   routing (index.js's existing pause-check + M-Hb block run before the
 *   scheduling_session/manage checks) — unchanged by M8b, not re-derived.
 *
 * Scope boundary (disclosed): the C7 drain loop (coalesced rapid-fire
 * messages during a lock hold) does not run manage-trigger DETECTION on
 * drained free-text members — they still flow into the combined RAG turn as
 * before M8b. Only the PRIMARY (non-drained) turn path detects a manage
 * trigger. This subphase's DONE line ("complete E2E from a staging DM") does
 * not require drain-loop integration; flagged here for a future follow-up.
 */

const { GetCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { toE164 } = require('../shared/scheduling/phone');
const { recordBookingSmsConsent } = require('../shared/scheduling/consent');
const formEngine = require('./formEngine');
const { CAROUSEL_MAX } = require('./capabilities');

const STATE_TYPE_SCHEDULING_SESSION = 'scheduling_session';
/** G-P4 T1': idle TTL = 1 hour from last update, refreshed on each step. */
const SCHEDULING_SESSION_TTL_SECONDS = 60 * 60;

// M8b: the C4-additive `last_booking` row (see the M8b doc block above).
const STATE_TYPE_LAST_BOOKING = 'last_booking';
/** Bounded, matches M1b's 7-day history TTL (plan §3 fact 6) — not the 1h
 * in-flow idle TTL above, which governs an ACTIVE book/manage session, not
 * the durable link to a completed booking. Refreshed on every write. */
const LAST_BOOKING_TTL_SECONDS = 7 * 24 * 60 * 60;

/**
 * The exact booking fields Booking_Commit_Handler's scheduling_mutate /
 * shared/scheduling/reschedule.js / cancel.js read via their `pick()` helper
 * — copied VERBATIM from BSH's already-audited (NTH1) allowlist
 * (Bedrock_Streaming_Handler_Staging/scheduling/schedulingFlow.js
 * `_EXEC_BOOKING_FIELDS`), not re-derived. PII-minimized: only what the
 * executor actually reads reaches the stored last_booking row.
 */
const MUTATE_BOOKING_FIELDS = [
  'booking_id', 'bookingId', 'tenant_id', 'tenantId',
  'coordinator_email', 'coordinatorEmail', 'resource_id', 'resourceId',
  'external_event_id', 'externalEventId', 'conference_id', 'conferenceId',
  'conference_provider', 'conferenceProvider', 'appointment_type_name', 'appointmentTypeName',
  'attendee_email', 'attendeeEmail', 'attendee_first_name', 'attendeeFirstName',
  'attendee_last_name', 'attendeeLastName', 'attendee_name', 'attendeeName',
  'attendee_phone', 'attendeePhone', 'organization_name', 'organizationName',
  'timezone', 'timeZone', 'deep_link', 'deepLink',
];

const STAGE_PROPOSING = 'proposing';
const STAGE_CONTACT_EMAIL = 'contact_email';
const STAGE_CONTACT_PHONE = 'contact_phone';
const STAGE_CONFIRM = 'confirm';

const DEFAULT_SMS_CONSENT =
  "We'll text your appointment confirmation and reminders for this booking to this number. " +
  'Msg & data rates may apply. Reply STOP to opt out, HELP for help.';
const DEFAULT_NO_SLOTS = "I couldn't find any open times right now — please try again in a bit.";
const DEFAULT_SCHEDULING_UNAVAILABLE = "Sorry, I'm not able to pull up scheduling right now — please try again shortly.";
const DEFAULT_SLOT_INVALID = 'Please tap one of the times above, or reply with its number.';
const DEFAULT_EMAIL_PROMPT = "What's the best email for your confirmation?";
const DEFAULT_PHONE_PROMPT_FB = "What's the best phone number for text reminders? (or reply \"skip\" to skip)";
const DEFAULT_PHONE_PROMPT_IG = "What's the best phone number for text reminders? A phone number is required so we can text your confirmation and reminders.";
const DEFAULT_PHONE_REQUIRED_IG = 'A phone number is required for Instagram booking reminders — please share a number.';
const DEFAULT_PHONE_INVALID = "That doesn't look like a valid phone number — please include an area code.";
const DEFAULT_CONFIRM_READY = 'Reply "confirm" to book it, or "cancel" to stop.';
const DEFAULT_SCHEDULING_CANCELLED = "No problem — I've cancelled that. Let me know if you'd like to book again.";
const DEFAULT_BOOKED = "You're booked! We'll send a confirmation shortly.";
const DEFAULT_SLOT_GONE = "That time was just taken — here are some fresh options.";
const DEFAULT_COMMIT_FAILED =
  "Sorry — something went wrong booking that time. You can reply \"confirm\" to try again, or \"cancel\" to stop.";

// M8b — manage (reschedule/cancel) default strings. All overridable via C2's
// additive MessengerStrings index signature (messenger_behavior.strings.*) —
// no CB config.ts change needed (schema already permits additive keys).
const DEFAULT_MANAGE_NOT_FOUND =
  "I couldn't find a recent booking for you — please contact us directly, or say \"book an appointment\" to schedule a new one.";
const DEFAULT_MANAGE_ABORTED = "No problem — I've left your appointment as it was.";
const DEFAULT_MANAGE_RESCHEDULE_ABORTED = "No problem — I've left your appointment as it was.";
const DEFAULT_MANAGE_MUTATE_FAILED =
  "Sorry — something went wrong with that. Please try again in a bit, or contact us directly.";
const DEFAULT_MANAGE_RESCHEDULED = "Done — you're rescheduled!";
const DEFAULT_SLOT_GONE_MANAGE = 'That time was just taken. Say "reschedule" to see fresh options.';

// ─── C2-style string precedence (small local copy — avoids a circular
// require with index.js, which defines its own; mirrors formEngine.js) ─────
function getMessengerString(config, channelType, key, fallback) {
  const behavior = config?.messenger_behavior || {};
  const channelOverride = behavior.channel_overrides?.[channelType]?.strings?.[key];
  if (channelOverride !== undefined) return channelOverride;
  const topLevel = behavior.strings?.[key];
  if (topLevel !== undefined) return topLevel;
  return fallback;
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function refreshedExpiry(nowMs) {
  return Math.floor(nowMs / 1000) + SCHEDULING_SESSION_TTL_SECONDS;
}

function refreshedLastBookingExpiry(nowMs) {
  return Math.floor(nowMs / 1000) + LAST_BOOKING_TTL_SECONDS;
}

// ─── M8b: booking-item unmarshal / projection (see the M8b doc block) ───────

/**
 * Minimal DynamoDB unmarshal (S only — Booking_Commit_Handler/booking-
 * store.js's buildBookingItem writes every attribute via its local `s()`
 * string wrapper, so a full @aws-sdk/util-dynamodb unmarshall is unneeded).
 * Mirrors Scheduling_Page_Api/index.js's identical helper. Tolerates an
 * already-plain object (defensive — a future non-string attribute, or a test
 * fixture that passes plain values, is copied through unchanged).
 */
function unmarshallBookingItem(item) {
  const out = {};
  if (!item) return out;
  for (const k of Object.keys(item)) {
    const v = item[k];
    if (v == null) continue;
    if (typeof v === 'object' && v.S !== undefined) out[k] = v.S;
    else if (typeof v !== 'object') out[k] = v;
  }
  return out;
}

/** Project a (possibly raw-DDB-wrapped) booking item down to MUTATE_BOOKING_FIELDS. */
function projectBookingForMutate(rawBooking) {
  const plain = unmarshallBookingItem(rawBooking);
  const out = {};
  for (const k of MUTATE_BOOKING_FIELDS) if (plain[k] !== undefined) out[k] = plain[k];
  return out;
}

/** Mirrors BSH's schedulingFlow.js calendarIdOf/_executeViaExecutor precedence
 * (minus the §B10 `binding.coordinator_id` fallback, which has no Messenger
 * analogue — this processor has no session-binding row). */
function coordinatorIdOfProjection(booking) {
  return (
    (booking && (booking.resource_id || booking.resourceId)) ||
    (booking && (booking.coordinator_email || booking.coordinatorEmail)) ||
    null
  );
}

/**
 * Build the last_booking row payload from a just-committed BCH response.
 * Called from M8a's commitAndRecordConsent on BOOKED/ALREADY_CONFIRMED.
 * @returns {{bookingId, slotLabel, appointmentTypeId, coordinatorId, booking, channel}}
 */
function buildLastBookingSnapshot({ bookingId, slotLabel, appointmentTypeId, rawBooking, channel }) {
  const booking = projectBookingForMutate(rawBooking);
  return {
    bookingId,
    slotLabel: slotLabel || null,
    appointmentTypeId: appointmentTypeId || null,
    coordinatorId: coordinatorIdOfProjection(booking),
    booking,
    channel,
  };
}

/** Build the PINNED scheduling_mutate invoke payload (see the M8b doc block
 * for the verbatim shape + source citations). `bookingCtx` is either a
 * last_booking row or a manage_reschedule scheduling_session row — both
 * carry `coordinator_id` + `booking` in the same shape. */
function buildMutatePayload({ tenantId, mutation, bookingCtx, newSlot }) {
  const payload = {
    action: 'scheduling_mutate',
    mutation,
    tenantId,
    coordinatorId: bookingCtx.coordinator_id,
    booking: bookingCtx.booking,
  };
  if (mutation === 'reschedule' && newSlot) payload.newSlot = newSlot;
  return payload;
}

// ─── C4 row CRUD (T1'/T2' pattern — mirrors formEngine's form_session CRUD) ─

/**
 * Load the active scheduling_session row. T1'/T2' analogue: a row whose
 * expires_at has already passed is treated as ABSENT (DynamoDB's own TTL
 * sweep can lag) — filtered out here and best-effort deleted.
 * @returns {Promise<object|null>}
 */
async function loadSchedulingSession({ client, tableName, sessionId, log }) {
  const result = await client.send(
    new GetCommand({
      TableName: tableName,
      Key: { sessionId, stateType: STATE_TYPE_SCHEDULING_SESSION },
    })
  );
  if (!result.Item) return null;

  const cutoff = nowSec();
  if (typeof result.Item.expires_at === 'number' && result.Item.expires_at <= cutoff) {
    try {
      await client.send(
        new DeleteCommand({
          TableName: tableName,
          Key: { sessionId, stateType: STATE_TYPE_SCHEDULING_SESSION },
          ConditionExpression: 'expires_at <= :cutoff',
          ExpressionAttributeValues: { ':cutoff': cutoff },
        })
      );
    } catch (cleanupErr) {
      if (cleanupErr.name !== 'ConditionalCheckFailedException') {
        log && log('WARN', 'Stale scheduling-session cleanup failed (non-fatal — TTL sweep will catch it)', {
          sessionId,
          error: cleanupErr.message,
        });
      }
    }
    return null; // T1'/T2': expired ⇒ absent, regardless of cleanup outcome
  }
  return result.Item;
}

async function saveSchedulingSession({ client, tableName, session }) {
  await client.send(new PutCommand({ TableName: tableName, Item: session }));
}

async function deleteSchedulingSession({ client, tableName, sessionId }) {
  await client.send(
    new DeleteCommand({ TableName: tableName, Key: { sessionId, stateType: STATE_TYPE_SCHEDULING_SESSION } })
  );
}

// ─── M8b: last_booking row CRUD (T1'/T2' pattern, mirrors above) ────────────

/**
 * Load the last_booking row. An expired row (T2-style) is treated as ABSENT
 * — filtered out here and best-effort deleted, same posture as
 * loadSchedulingSession.
 * @returns {Promise<object|null>}
 */
async function loadLastBooking({ client, tableName, sessionId, log }) {
  const result = await client.send(
    new GetCommand({ TableName: tableName, Key: { sessionId, stateType: STATE_TYPE_LAST_BOOKING } })
  );
  if (!result.Item) return null;

  const cutoff = nowSec();
  if (typeof result.Item.expires_at === 'number' && result.Item.expires_at <= cutoff) {
    try {
      await client.send(
        new DeleteCommand({
          TableName: tableName,
          Key: { sessionId, stateType: STATE_TYPE_LAST_BOOKING },
          ConditionExpression: 'expires_at <= :cutoff',
          ExpressionAttributeValues: { ':cutoff': cutoff },
        })
      );
    } catch (cleanupErr) {
      if (cleanupErr.name !== 'ConditionalCheckFailedException') {
        log && log('WARN', 'Stale last_booking cleanup failed (non-fatal — TTL sweep will catch it)', {
          sessionId,
          error: cleanupErr.message,
        });
      }
    }
    return null;
  }
  return result.Item;
}

/** Write/refresh the last_booking row from a buildLastBookingSnapshot() result. */
async function saveLastBooking({ client, tableName, sessionId, bookingId, slotLabel, appointmentTypeId, coordinatorId, booking, channel }) {
  const now = Date.now();
  const item = {
    sessionId,
    stateType: STATE_TYPE_LAST_BOOKING,
    booking_id: bookingId,
    slot_label: slotLabel || null,
    appointment_type_id: appointmentTypeId || null,
    coordinator_id: coordinatorId || null,
    booking: booking || {},
    channel,
    updated_at: now,
    expires_at: refreshedLastBookingExpiry(now),
    schema_version: 1,
  };
  await client.send(new PutCommand({ TableName: tableName, Item: item }));
  return item;
}

async function deleteLastBooking({ client, tableName, sessionId }) {
  await client.send(
    new DeleteCommand({ TableName: tableName, Key: { sessionId, stateType: STATE_TYPE_LAST_BOOKING } })
  );
}

/**
 * Read-modify-write patch of the last_booking row (pending_manage_action
 * set/clear, or the post-reschedule slot_label refresh). A key set to
 * `undefined` in `patch` DELETES that attribute (DDB rejects literal
 * `undefined` values on Put). No-op (logged) when the row is already gone —
 * a patch must never resurrect a deleted/expired row.
 */
async function patchLastBooking({ client, tableName, sessionId, patch, log }) {
  const current = await client.send(
    new GetCommand({ TableName: tableName, Key: { sessionId, stateType: STATE_TYPE_LAST_BOOKING } })
  );
  if (!current.Item) {
    log && log('WARN', 'patchLastBooking: no row to patch (already deleted/expired) — skipping', { sessionId });
    return null;
  }
  const now = Date.now();
  const next = { ...current.Item, ...patch, updated_at: now, expires_at: refreshedLastBookingExpiry(now) };
  for (const k of Object.keys(patch)) {
    if (patch[k] === undefined) delete next[k];
  }
  await client.send(new PutCommand({ TableName: tableName, Item: next }));
  return next;
}

// ─── Appointment-type / program resolution ───────────────────────────────────

/**
 * Resolve the appointment type a `start_scheduling` CTA tap should propose.
 * v1 SIMPLE resolution (documented, not hidden): the CTA's own `program_id`
 * (config.ts CTADefinition field, "Program ID to associate this CTA with a
 * specific program") is matched against `config.scheduling.appointment_types`
 * entries carrying the SAME `program_id`; if none match and there is EXACTLY
 * ONE configured appointment type overall, that sole type is used (mirrors
 * BSH's newBookingEntry.js `resolveQualifyingContext` "sole configured type"
 * fallback for the widget). Anything more ambiguous (no program_id match, 2+
 * configured types) returns null — the caller declines gracefully rather
 * than guessing which type the tenant meant.
 *
 * @returns {string|null} appointmentTypeId
 */
function resolveAppointmentTypeId({ config, cta }) {
  const types = config?.scheduling?.appointment_types || {};
  const ids = Object.keys(types);
  const wantedProgramId = cta?.program_id;

  if (wantedProgramId) {
    const matched = ids.find((id) => types[id]?.program_id === wantedProgramId);
    if (matched) return matched;
  }
  if (ids.length === 1) return ids[0];
  return null;
}

// ─── Payload parsing (C3 `sched` route) ──────────────────────────────────────

/**
 * Parse a `PIC1:sched:{op}:{arg}` payload. `arg` may be '' (confirm/cancel
 * carry no argument).
 * @returns {{op: string, arg: string}|null}
 */
function parseSchedPayload(payload) {
  if (typeof payload !== 'string' || !payload.startsWith('PIC1:sched:')) return null;
  const rest = payload.slice('PIC1:sched:'.length);
  const idx = rest.indexOf(':');
  const op = idx === -1 ? rest : rest.slice(0, idx);
  const arg = idx === -1 ? '' : rest.slice(idx + 1);
  if (!op) return null;
  return { op, arg };
}

// ─── Message building ─────────────────────────────────────────────────────────

function tzSuffix(tzLabel) {
  return tzLabel ? ` (times shown in ${tzLabel})` : ' (times shown in the organization’s local timezone)';
}

/**
 * Build the propose-success messages: ONE text message (tz-labeled intro +
 * numbered list — C9 free-text fallback) + ONE generic-template carousel
 * message (C5 ≤10 cards). The adversarial focus (never a bare time): the tz
 * label is stated in BOTH the text intro AND every card's subtitle.
 */
function buildSlotMessages(slots, tzLabel) {
  const capped = slots.slice(0, CAROUSEL_MAX);
  const listLines = capped.map((s, i) => `${i + 1}. ${s.label}`);
  const text = [`Here are some times that work${tzSuffix(tzLabel)}:`, ...listLines, '', 'Tap a card below, or reply with its number.'].join('\n');

  const subtitle = tzLabel ? `Times shown in ${tzLabel}` : 'Times shown in your organization’s local timezone';
  const elements = capped.map((s) => ({
    title: s.label,
    subtitle,
    buttons: [{ type: 'postback', title: 'Pick this time', payload: `PIC1:sched:slot:${s.slotId}` }],
  }));

  return [
    { kind: 'text', text, quickReplies: [] },
    { kind: 'generic_template', elements },
  ];
}

function confirmSummaryText(session) {
  const lines = ["Here's what I have:", `Time: ${session.selected_slot?.label || 'selected time'}`, `Email: ${session.contact?.email || ''}`];
  if (session.contact?.phone) lines.push('Phone: on file for text reminders');
  lines.push('', DEFAULT_CONFIRM_READY);
  return lines.join('\n');
}

function confirmQuickReplies() {
  return [
    { content_type: 'text', title: 'Confirm', payload: 'PIC1:sched:confirm' },
    { content_type: 'text', title: 'Cancel', payload: 'PIC1:sched:cancel' },
  ];
}

// ─── Begin (CTA tap / PIC1:sched:start entry) ────────────────────────────────

/**
 * Start a new scheduling session: invoke BCH `scheduling_propose` and, on
 * `outcome:'ok'`, build the C4 row + the slot-carousel messages. Overwrites
 * any existing scheduling_session row for this conversation (C4 documents
 * exactly one active flow-of-this-kind per conversation, mirroring forms).
 *
 * @returns {Promise<{session: object|null, messages: Array<object>, started: boolean}>}
 */
async function beginScheduling({ sessionId, tenantId, appointmentTypeId, config, channelType, deps = {}, log }) {
  const types = config?.scheduling?.appointment_types || {};
  const apptType = types[appointmentTypeId] || {};
  const userTimeZone = apptType.timezone || apptType.time_zone || 'UTC';

  if (!deps.invokeProposal) {
    log && log('WARN', 'Scheduling propose seam not wired — declining scheduling start', { sessionId });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_UNAVAILABLE, quickReplies: [] }], started: false };
  }

  const proposePayload = {
    action: 'scheduling_propose',
    tenantId,
    appointmentTypeId,
    userTimeZone,
  };

  let res;
  try {
    res = await deps.invokeProposal(proposePayload);
  } catch (err) {
    log && log('ERROR', 'Scheduling propose invoke failed', { sessionId, error_name: err?.name || 'unknown' });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_UNAVAILABLE, quickReplies: [] }], started: false };
  }

  if (!res || res.outcome !== 'ok' || !Array.isArray(res.slots) || res.slots.length === 0) {
    log && log('INFO', 'Scheduling propose returned no slots', { sessionId, outcome: res && res.outcome });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_NO_SLOTS, quickReplies: [] }], started: false };
  }

  const now = Date.now();
  const tzLabel = res.context?.tz_label || null;
  const session = {
    sessionId,
    stateType: STATE_TYPE_SCHEDULING_SESSION,
    program_id: appointmentTypeId,
    stage: STAGE_PROPOSING,
    candidate_slots: res.slots,
    rejected_slot_ids: [],
    pool_size: res.poolSize,
    tie_breaker: res.tieBreaker,
    round_robin_cursor: res.roundRobinCursor,
    tz_label: tzLabel,
    channel: channelType,
    appointment_type: {
      id: appointmentTypeId,
      name: apptType.name,
      timezone: userTimeZone,
      conference_type: apptType.conference_type || apptType.conferenceType || 'google_meet',
      cancellation_window_hours: apptType.cancellation_window_hours || 0,
      format: apptType.format,
      ...(apptType.program_id ? { program_id: apptType.program_id } : {}),
    },
    started_at: now,
    updated_at: now,
    schema_version: 1,
    expires_at: refreshedExpiry(now),
  };

  return { session, messages: buildSlotMessages(res.slots, tzLabel), started: true };
}

// ─── Per-stage turn handling ──────────────────────────────────────────────────

function findChosenSlot(session, schedPayload, rawText) {
  const tappedSlotId = schedPayload && schedPayload.op === 'slot' ? schedPayload.arg : null;
  const candidates = session.candidate_slots || [];
  if (tappedSlotId) {
    return candidates.find((s) => s.slotId === tappedSlotId) || null;
  }
  const trimmed = typeof rawText === 'string' ? rawText.trim() : '';
  if (/^\d+$/.test(trimmed)) {
    const idx = parseInt(trimmed, 10) - 1;
    return candidates[idx] || null;
  }
  return null;
}

async function handleProposing({ session, schedPayload, rawText, log }) {
  const chosen = findChosenSlot(session, schedPayload, rawText);
  const now = Date.now();
  if (!chosen) {
    return {
      session: { ...session, updated_at: now, expires_at: refreshedExpiry(now) },
      messages: [{ kind: 'text', text: DEFAULT_SLOT_INVALID, quickReplies: [] }],
    };
  }
  const nextSession = {
    ...session,
    stage: STAGE_CONTACT_EMAIL,
    selected_slot: {
      slotId: chosen.slotId,
      start: chosen.start,
      end: chosen.end,
      label: chosen.label,
      candidateResourceIds: chosen.candidateResourceIds || [],
    },
    updated_at: now,
    expires_at: refreshedExpiry(now),
  };
  // C5: the FB-only user_email prefill quick reply — same convention as
  // formEngine.fieldPromptMessage (E1/E2: rendered blind, never read until tapped/typed).
  const quickReplies = session.channel === 'messenger' ? [{ content_type: 'user_email' }] : [];
  return {
    session: nextSession,
    messages: [{ kind: 'text', text: DEFAULT_EMAIL_PROMPT, quickReplies }],
  };
}

function handleContactEmail({ session, rawText, consentLanguage }) {
  const now = Date.now();
  const { valid, value, error } = formEngine.validateAnswer({ type: 'email', required: true }, rawText);
  if (!valid) {
    return {
      session: { ...session, updated_at: now, expires_at: refreshedExpiry(now) },
      messages: [{ kind: 'text', text: error, quickReplies: session.channel === 'messenger' ? [{ content_type: 'user_email' }] : [] }],
    };
  }
  const phonePrompt = session.channel === 'instagram' ? DEFAULT_PHONE_PROMPT_IG : DEFAULT_PHONE_PROMPT_FB;
  return {
    session: {
      ...session,
      stage: STAGE_CONTACT_PHONE,
      contact: { ...(session.contact || {}), email: value },
      // C1/C2: capture the EXACT rendered consent string at the moment the
      // phone prompt (which carries it) is built, so commit-time uses
      // precisely what the user read.
      consent_language_shown: consentLanguage,
      updated_at: now,
      expires_at: refreshedExpiry(now),
    },
    messages: [{ kind: 'text', text: `${consentLanguage}\n\n${phonePrompt}`, quickReplies: [] }],
  };
}

function handleContactPhone({ session, rawText }) {
  const now = Date.now();
  const trimmed = typeof rawText === 'string' ? rawText.trim() : '';
  const isSkip = /^skip$/i.test(trimmed);

  if (session.channel === 'instagram' && isSkip) {
    // D9: IG has no post-window SMS-free reminder lane — phone is mandatory.
    return {
      session: { ...session, updated_at: now, expires_at: refreshedExpiry(now) },
      messages: [{ kind: 'text', text: `${session.consent_language_shown}\n\n${DEFAULT_PHONE_REQUIRED_IG}`, quickReplies: [] }],
    };
  }

  if (session.channel === 'messenger' && isSkip) {
    const nextSession = { ...session, stage: STAGE_CONFIRM, updated_at: now, expires_at: refreshedExpiry(now) };
    return { session: nextSession, messages: [{ kind: 'text', text: confirmSummaryText(nextSession), quickReplies: confirmQuickReplies() }] };
  }

  const e164 = toE164(trimmed);
  if (!e164) {
    return {
      session: { ...session, updated_at: now, expires_at: refreshedExpiry(now) },
      messages: [{ kind: 'text', text: `${session.consent_language_shown}\n\n${DEFAULT_PHONE_INVALID}`, quickReplies: [] }],
    };
  }

  const nextSession = {
    ...session,
    stage: STAGE_CONFIRM,
    contact: { ...(session.contact || {}), phone: e164 },
    updated_at: now,
    expires_at: refreshedExpiry(now),
  };
  return { session: nextSession, messages: [{ kind: 'text', text: confirmSummaryText(nextSession), quickReplies: confirmQuickReplies() }] };
}

/** Build the pinned Booking_Commit_Handler default-commit payload (snake_case — validate() in Booking_Commit_Handler/index.js). */
function buildCommitPayload({ session, tenantId, sessionId }) {
  const appt = session.appointment_type || {};
  return {
    tenant_id: tenantId,
    session_id: sessionId,
    slot: {
      start: session.selected_slot.start,
      end: session.selected_slot.end,
      candidateResourceIds: session.selected_slot.candidateResourceIds || [],
    },
    attendee: { email: session.contact.email, phone: session.contact.phone },
    conference_type: appt.conference_type || 'google_meet',
    pool_size: session.pool_size,
    appointment_type: {
      id: appt.id,
      name: appt.name,
      timezone: appt.timezone,
      cancellation_window_hours: appt.cancellation_window_hours,
      format: appt.format,
      ...(appt.program_id ? { program_id: appt.program_id } : {}),
    },
    user_time_zone: appt.timezone,
    ...(session.tie_breaker != null ? { tie_breaker: session.tie_breaker } : {}),
    ...(session.round_robin_cursor != null ? { round_robin_cursor: session.round_robin_cursor } : {}),
  };
}

async function commitAndRecordConsent({ session, tenantId, sessionId, deps, log }) {
  const res = await deps.invokeCommit(buildCommitPayload({ session, tenantId, sessionId }));
  const status = res && res.status;

  if (status === 'BOOKED' || status === 'ALREADY_CONFIRMED') {
    // T2': delete on EVERY successful commit, including a conflict-retry's
    // eventual success (this code path is reached identically either way).
    if (session.contact.phone) {
      // C2/C3/C4: best-effort, AFTER commit, exact consent_language_shown +
      // channel-scoped source. Never blocks/reverts the booking.
      try {
        await (deps.recordConsent || recordBookingSmsConsent)({
          tenantId,
          phone: session.contact.phone,
          bookingId: res.bookingId,
          consentLanguage: session.consent_language_shown,
          source: session.channel === 'instagram' ? 'messenger_booking_ig' : 'messenger_booking_fb',
        });
      } catch (consentErr) {
        log && log('WARN', 'SMS consent record failed (best-effort, booking stands)', {
          sessionId,
          error_name: consentErr?.name || 'unknown',
        });
      }
    }
    log && log('INFO', 'Scheduling committed', { sessionId, status, bookingId: res.bookingId });
    // M8b: capture the just-committed booking (already in-hand on this same
    // response — no extra read) so the manage flow can find it later. Never
    // blocks/affects the booking outcome — a snapshot-build issue just means
    // the manage flow won't find this booking (caller best-effort-writes it).
    const bookingSnapshot = buildLastBookingSnapshot({
      bookingId: res.bookingId,
      slotLabel: session.selected_slot?.label,
      appointmentTypeId: session.appointment_type?.id,
      rawBooking: res.booking,
      channel: session.channel,
    });
    return {
      committed: true, session: null, bookingId: res.bookingId, bookingSnapshot,
      messages: [{ kind: 'text', text: DEFAULT_BOOKED, quickReplies: [] }],
    };
  }

  if (status === 'SLOT_UNAVAILABLE') {
    // Conflict retry: re-propose fresh slots, stage back to STAGE_PROPOSING.
    if (!deps.invokeProposal) {
      log && log('WARN', 'Slot conflict but propose seam unwired — cannot retry', { sessionId });
      // T3': terminal — leave the row COMPLETELY untouched (no TTL bump).
      return { committed: false, session: undefined, messages: [{ kind: 'text', text: DEFAULT_COMMIT_FAILED, quickReplies: [] }] };
    }
    const rejected = [...(session.rejected_slot_ids || []), session.selected_slot?.slotId].filter(Boolean);
    let reproposed;
    try {
      reproposed = await deps.invokeProposal({
        action: 'scheduling_propose',
        tenantId,
        appointmentTypeId: session.program_id,
        userTimeZone: session.appointment_type?.timezone || 'UTC',
        alreadyRejected: rejected,
      });
    } catch (err) {
      log && log('ERROR', 'Re-propose after slot conflict failed', { sessionId, error_name: err?.name || 'unknown' });
      // T3': terminal — leave the row COMPLETELY untouched.
      return { committed: false, session: undefined, messages: [{ kind: 'text', text: DEFAULT_COMMIT_FAILED, quickReplies: [] }] };
    }
    if (!reproposed || reproposed.outcome !== 'ok' || !Array.isArray(reproposed.slots) || reproposed.slots.length === 0) {
      // T3': re-propose also failed — terminal, leave the row untouched.
      return { committed: false, session: undefined, messages: [{ kind: 'text', text: DEFAULT_COMMIT_FAILED, quickReplies: [] }] };
    }
    const now = Date.now();
    const tzLabel = reproposed.context?.tz_label || session.tz_label;
    const nextSession = {
      ...session,
      stage: STAGE_PROPOSING,
      candidate_slots: reproposed.slots,
      rejected_slot_ids: rejected,
      pool_size: reproposed.poolSize,
      tie_breaker: reproposed.tieBreaker,
      round_robin_cursor: reproposed.roundRobinCursor,
      tz_label: tzLabel,
      selected_slot: undefined,
      updated_at: now,
      expires_at: refreshedExpiry(now),
    };
    return {
      committed: false,
      session: nextSession,
      messages: [{ kind: 'text', text: DEFAULT_SLOT_GONE, quickReplies: [] }, ...buildSlotMessages(reproposed.slots, tzLabel)],
    };
  }

  // COMMIT_FAILED / SCHEDULING_DISABLED / unknown → T3': terminal, row untouched.
  log && log('WARN', 'Scheduling commit failed', { sessionId, status });
  return { committed: false, session: undefined, messages: [{ kind: 'text', text: DEFAULT_COMMIT_FAILED, quickReplies: confirmQuickReplies() }] };
}

async function handleConfirm({ session, schedPayload, rawText, tenantId, sessionId, deps, log }) {
  const isCancel = (schedPayload && schedPayload.op === 'cancel') || (!schedPayload && formEngine.isCancelKeyword(rawText));
  if (isCancel) {
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_CANCELLED, quickReplies: [] }] };
  }
  const isConfirm = (schedPayload && schedPayload.op === 'confirm') || (!schedPayload && formEngine.isConfirmKeyword(rawText));
  if (!isConfirm) {
    const now = Date.now();
    return {
      session: { ...session, updated_at: now, expires_at: refreshedExpiry(now) },
      messages: [{ kind: 'text', text: confirmSummaryText(session), quickReplies: confirmQuickReplies() }],
    };
  }

  // C8: commit CANNOT fire without attendee_email. Defensive — unreachable
  // given the stage machine (email precedes confirm) but never trust a
  // hand-edited/corrupted row over the contract.
  if (!session.contact || !session.contact.email) {
    log && log('ERROR', 'Confirm reached without attendee email — refusing to commit (row kept untouched, T3\')', { sessionId });
    // T3': terminal — leave the row COMPLETELY untouched (no TTL bump).
    return { session: undefined, messages: [{ kind: 'text', text: DEFAULT_COMMIT_FAILED, quickReplies: [] }] };
  }
  if (!deps.invokeCommit) {
    log && log('WARN', 'Commit seam not wired — cannot book', { sessionId });
    // T3': terminal — leave the row COMPLETELY untouched (no TTL bump).
    return { session: undefined, messages: [{ kind: 'text', text: DEFAULT_COMMIT_FAILED, quickReplies: [] }] };
  }

  const result = await commitAndRecordConsent({ session, tenantId, sessionId, deps, log });
  return {
    session: result.session, messages: result.messages, committed: result.committed,
    bookingId: result.bookingId, bookingSnapshot: result.bookingSnapshot,
  };
}

// ─── M8b: manage (reschedule/cancel) ──────────────────────────────────────

/**
 * Resolve the fresh candidate slots for a reschedule, re-invoking BCH
 * `scheduling_propose` for the ORIGINAL booking's appointment type (stored on
 * last_booking at M8a commit time — no CTA/program_id resolution needed, the
 * booking already names its own type). On success, returns a NEW
 * `scheduling_session` row (`mode:'manage_reschedule'`) carrying the
 * last_booking's `coordinator_id`/`booking` snapshot forward, so the eventual
 * `scheduling_mutate` call (at slot-pick time) needs no further lookup.
 *
 * @returns {Promise<{session: object|null, messages: Array<object>, started: boolean}>}
 */
async function beginManageReschedule({ lastBooking, tenantId, sessionId, config, channelType, deps = {}, log }) {
  const apptTypeId = lastBooking.appointment_type_id;
  if (!deps.invokeProposal) {
    log && log('WARN', 'Reschedule propose seam not wired — declining', { sessionId });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_UNAVAILABLE, quickReplies: [] }], started: false };
  }
  if (!apptTypeId) {
    log && log('WARN', 'last_booking missing appointment_type_id — cannot re-propose', { sessionId });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }], started: false };
  }

  const types = config?.scheduling?.appointment_types || {};
  const apptType = types[apptTypeId] || {};
  const userTimeZone =
    apptType.timezone || apptType.time_zone || lastBooking.booking?.timezone || lastBooking.booking?.timeZone || 'UTC';

  let res;
  try {
    res = await deps.invokeProposal({ action: 'scheduling_propose', tenantId, appointmentTypeId: apptTypeId, userTimeZone });
  } catch (err) {
    log && log('ERROR', 'Reschedule propose invoke failed', { sessionId, error_name: err?.name || 'unknown' });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_UNAVAILABLE, quickReplies: [] }], started: false };
  }
  if (!res || res.outcome !== 'ok' || !Array.isArray(res.slots) || res.slots.length === 0) {
    log && log('INFO', 'Reschedule propose returned no slots', { sessionId, outcome: res && res.outcome });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_NO_SLOTS, quickReplies: [] }], started: false };
  }

  const now = Date.now();
  const tzLabel = res.context?.tz_label || null;
  const session = {
    sessionId,
    stateType: STATE_TYPE_SCHEDULING_SESSION,
    mode: 'manage_reschedule',
    booking_id: lastBooking.booking_id,
    coordinator_id: lastBooking.coordinator_id,
    booking: lastBooking.booking,
    appointment_type_id: apptTypeId,
    stage: STAGE_PROPOSING,
    candidate_slots: res.slots,
    tz_label: tzLabel,
    channel: channelType,
    started_at: now,
    updated_at: now,
    schema_version: 1,
    expires_at: refreshedExpiry(now),
  };
  return { session, messages: buildSlotMessages(res.slots, tzLabel), started: true };
}

/** STAGE_PROPOSING dispatch for a `mode:'manage_reschedule'` session — a slot
 * pick invokes BCH scheduling_mutate reschedule DIRECTLY (no email/phone/
 * confirm stages: contact info is already on file from the original
 * booking). Returns `lastBookingWrite` on success so the caller (index.js)
 * refreshes last_booking's slot_label — the booking_id is unchanged, a
 * reschedule moves the SAME booking, it never deletes the link. */
async function handleManageReschedulePick({ session, schedPayload, rawText, tenantId, sessionId, deps = {}, log }) {
  const chosen = findChosenSlot(session, schedPayload, rawText);
  const now = Date.now();
  if (!chosen) {
    return {
      session: { ...session, updated_at: now, expires_at: refreshedExpiry(now) },
      messages: [{ kind: 'text', text: DEFAULT_SLOT_INVALID, quickReplies: [] }],
    };
  }
  if (!deps.invokeMutate) {
    log && log('WARN', 'Mutate seam not wired — cannot reschedule', { sessionId });
    // T3'-style: leave the row untouched so a retry (once wired) can re-pick.
    return { session: undefined, messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }] };
  }

  const newSlot = { start: chosen.start, end: chosen.end };
  let res;
  try {
    res = await deps.invokeMutate(buildMutatePayload({ tenantId, mutation: 'reschedule', bookingCtx: session, newSlot }));
  } catch (err) {
    log && log('ERROR', 'Reschedule mutate invoke failed', { sessionId, error_name: err?.name || 'unknown' });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }] };
  }

  const outcome = res && res.outcome;
  if (outcome === 'success' || outcome === 'pending_calendar_sync') {
    log && log('INFO', 'Reschedule committed', { sessionId, bookingId: session.booking_id, outcome });
    return {
      session: null,
      messages: [{ kind: 'text', text: DEFAULT_MANAGE_RESCHEDULED, quickReplies: [] }],
      rescheduled: true,
      lastBookingWrite: { patch: { slot_label: chosen.label } },
    };
  }
  if (outcome === 'slot_unavailable') {
    log && log('INFO', 'Reschedule slot no longer available', { sessionId, bookingId: session.booking_id });
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SLOT_GONE_MANAGE, quickReplies: [] }] };
  }
  log && log('WARN', 'Reschedule mutate failed', { sessionId, bookingId: session.booking_id, outcome });
  return { session: null, messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }] };
}

/** Cancel the booking directly (single-shot — no session row involved). */
async function executeManageCancel({ lastBooking, tenantId, sessionId, deps = {}, log }) {
  if (!deps.invokeMutate) {
    log && log('WARN', 'Mutate seam not wired — cannot cancel', { sessionId });
    return { outcome: 'unwired', messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }] };
  }
  let res;
  try {
    res = await deps.invokeMutate(buildMutatePayload({ tenantId, mutation: 'cancel', bookingCtx: lastBooking }));
  } catch (err) {
    log && log('ERROR', 'Cancel mutate invoke failed', { sessionId, error_name: err?.name || 'unknown' });
    return { outcome: 'failed', messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }] };
  }
  const outcome = res && res.outcome;
  if (outcome === 'deleted' || outcome === 'pending_calendar_sync') {
    log && log('INFO', 'Booking cancelled', { sessionId, bookingId: lastBooking.booking_id, outcome });
    const label = lastBooking.slot_label || 'your';
    return { outcome, messages: [{ kind: 'text', text: `Done — I've cancelled your ${label} appointment.`, quickReplies: [] }] };
  }
  log && log('WARN', 'Cancel mutate failed', { sessionId, bookingId: lastBooking.booking_id, outcome });
  return { outcome: outcome || 'failed', messages: [{ kind: 'text', text: DEFAULT_MANAGE_MUTATE_FAILED, quickReplies: [] }] };
}

// KNOWN v1 DEVIATION (documented in the M8b doc block): keyword matching, not
// NLU. Whole-message-independent (unlike formEngine's isCancelKeyword) — these
// scan anywhere in a free-form sentence, since "reschedule my Tuesday thing"
// is a realistic phrasing (unlike the anytime-cancel keyword, matched only
// mid-flow against a short deliberate reply).
const RESCHEDULE_INTENT_REGEX = /\breschedul\w*\b/i;
const CANCEL_INTENT_REGEX = /\bcancel\b[^.!?]{0,30}\b(appointment|booking)\b/i;

/** @returns {'cancel'|'reschedule'|null} */
function detectManageIntent(text) {
  if (typeof text !== 'string') return null;
  const trimmed = text.trim();
  if (!trimmed) return null;
  if (RESCHEDULE_INTENT_REGEX.test(trimmed)) return 'reschedule';
  if (CANCEL_INTENT_REGEX.test(trimmed)) return 'cancel';
  return null;
}

function manageConfirmMessage(config, channelType, action, lastBooking) {
  const label = lastBooking.slot_label || 'upcoming';
  const bookingId = lastBooking.booking_id;
  if (action === 'cancel') {
    const text = getMessengerString(config, channelType, 'manage_cancel_confirm', null) || `Your ${label} appointment — cancel it?`;
    return {
      kind: 'text', text,
      quickReplies: [
        { content_type: 'text', title: 'Yes, cancel it', payload: `PIC1:sched:mcancel:${bookingId}` },
        { content_type: 'text', title: 'Never mind', payload: `PIC1:sched:mabort:${bookingId}` },
      ],
    };
  }
  const text = getMessengerString(config, channelType, 'manage_reschedule_confirm', null) || `Your ${label} appointment — want to reschedule it?`;
  return {
    kind: 'text', text,
    quickReplies: [
      { content_type: 'text', title: 'Yes, reschedule', payload: `PIC1:sched:mresched:${bookingId}` },
      { content_type: 'text', title: 'Never mind', payload: `PIC1:sched:mabort:${bookingId}` },
    ],
  };
}

function manageMenuMessage(config, channelType, lastBooking) {
  const label = lastBooking.slot_label || 'upcoming';
  const bookingId = lastBooking.booking_id;
  const text = getMessengerString(config, channelType, 'manage_menu', null) || `What would you like to do with your ${label} appointment?`;
  return {
    kind: 'text', text,
    quickReplies: [
      { content_type: 'text', title: 'Reschedule', payload: `PIC1:sched:mresched:${bookingId}` },
      { content_type: 'text', title: 'Cancel', payload: `PIC1:sched:mcancel:${bookingId}` },
    ],
  };
}

/**
 * Decide whether this turn is a manage-flow trigger, and which one. Called
 * ONLY when no scheduling_session is already active (index.js's precedence).
 * Never itself invokes anything — pure decision, zero side effects.
 *
 * @returns {{kind:'ask_cancel'|'ask_reschedule'|'ask_menu'|'abort_pending'
 *            |'execute_cancel'|'execute_reschedule', bookingId?:string}|null}
 */
function resolveManageTrigger({ schedPayload, resumeSchedulingCta, rawText, lastBooking }) {
  if (schedPayload) {
    if (schedPayload.op === 'mcancel') return { kind: 'execute_cancel', bookingId: schedPayload.arg };
    if (schedPayload.op === 'mresched') return { kind: 'execute_reschedule', bookingId: schedPayload.arg };
    if (schedPayload.op === 'mabort') return { kind: 'abort_pending' };
  }

  if (lastBooking && lastBooking.pending_manage_action) {
    if (formEngine.isConfirmKeyword(rawText)) {
      return lastBooking.pending_manage_action === 'cancel'
        ? { kind: 'execute_cancel', bookingId: lastBooking.booking_id }
        : { kind: 'execute_reschedule', bookingId: lastBooking.booking_id };
    }
    if (formEngine.isCancelKeyword(rawText)) {
      return { kind: 'abort_pending' };
    }
  }

  if (resumeSchedulingCta) return { kind: 'ask_menu' };

  const intent = detectManageIntent(rawText);
  if (intent === 'cancel') return { kind: 'ask_cancel' };
  if (intent === 'reschedule') return { kind: 'ask_reschedule' };

  return null;
}

/**
 * Execute the decision from resolveManageTrigger. Pure w.r.t. DDB (index.js
 * performs the actual last_booking/scheduling_session writes per the
 * returned `lastBookingWrite`/`startSession`) — mirrors the
 * advance*SessionTurn wrapper split used for forms/book-scheduling.
 *
 * @returns {Promise<{messages: Array<object>, lastBookingWrite: ('delete'|{patch:object}|null),
 *                     startSession: object|null}>}
 */
async function handleManageTrigger({ trigger, lastBooking, config, tenantId, sessionId, channelType, deps = {}, log }) {
  const notFound = () => ({
    messages: [{ kind: 'text', text: getMessengerString(config, channelType, 'manage_not_found', DEFAULT_MANAGE_NOT_FOUND), quickReplies: [] }],
    lastBookingWrite: null,
    startSession: null,
  });

  if (trigger.kind === 'ask_cancel' || trigger.kind === 'ask_reschedule') {
    if (!lastBooking) return notFound();
    const action = trigger.kind === 'ask_cancel' ? 'cancel' : 'reschedule';
    return {
      messages: [manageConfirmMessage(config, channelType, action, lastBooking)],
      lastBookingWrite: { patch: { pending_manage_action: action } },
      startSession: null,
    };
  }

  if (trigger.kind === 'ask_menu') {
    if (!lastBooking) return notFound();
    return { messages: [manageMenuMessage(config, channelType, lastBooking)], lastBookingWrite: null, startSession: null };
  }

  if (trigger.kind === 'abort_pending') {
    return {
      messages: [{ kind: 'text', text: getMessengerString(config, channelType, 'manage_aborted', DEFAULT_MANAGE_ABORTED), quickReplies: [] }],
      lastBookingWrite: { patch: { pending_manage_action: undefined } },
      startSession: null,
    };
  }

  // execute_cancel / execute_reschedule — re-verify the bookingId matches the
  // CURRENTLY-tracked last_booking (defense: a stale tap/typed-confirm after
  // a NEW booking replaced the row must never act on the wrong booking).
  if (!lastBooking || lastBooking.booking_id !== trigger.bookingId) return notFound();

  if (trigger.kind === 'execute_cancel') {
    const result = await executeManageCancel({ lastBooking, tenantId, sessionId, deps, log });
    const success = result.outcome === 'deleted' || result.outcome === 'pending_calendar_sync';
    return { messages: result.messages, lastBookingWrite: success ? 'delete' : null, startSession: null };
  }

  // execute_reschedule: start the propose sub-flow — the mutate itself
  // happens later, at slot-pick (handleManageReschedulePick above).
  const begun = await beginManageReschedule({ lastBooking, tenantId, sessionId, config, channelType, deps, log });
  return { messages: begun.messages, lastBookingWrite: null, startSession: begun.started ? begun.session : null };
}

/**
 * Dispatch one turn to the session's current stage. Returns
 * `{ session, messages, committed?, bookingId? }`. `session: null` ⇒ caller
 * deletes the row (cancel / successful commit). `session: undefined` ⇒ T3' —
 * caller does NOT touch the row at all (leaves it exactly as loaded).
 *
 * @param {{ session: object, rawText: string, schedPayload: {op,arg}|null,
 *           config: object, tenantId: string, sessionId: string, deps: object,
 *           log?: Function }} params
 */
async function advanceScheduling({ session, rawText, schedPayload, config, tenantId, sessionId, deps = {}, log }) {
  // Universal cancel keyword/tap (any stage, mirrors formEngine's anytime-cancel).
  const isCancelTap = schedPayload && schedPayload.op === 'cancel';
  if (isCancelTap || (!schedPayload && formEngine.isCancelKeyword(rawText) && session.stage !== STAGE_CONFIRM)) {
    // M8b: a manage_reschedule session abandons only the RESCHEDULE ATTEMPT —
    // the underlying booking is untouched; say so explicitly (never the
    // book-flow text, which would misleadingly imply the booking itself was
    // cancelled).
    const abortText = session.mode === 'manage_reschedule' ? DEFAULT_MANAGE_RESCHEDULE_ABORTED : DEFAULT_SCHEDULING_CANCELLED;
    return { session: null, messages: [{ kind: 'text', text: abortText, quickReplies: [] }] };
  }

  switch (session.stage) {
    case STAGE_PROPOSING:
      return session.mode === 'manage_reschedule'
        ? handleManageReschedulePick({ session, schedPayload, rawText, tenantId, sessionId, deps, log })
        : handleProposing({ session, schedPayload, rawText, log });
    case STAGE_CONTACT_EMAIL: {
      const consentLanguage = getMessengerString(config, session.channel, 'sms_consent', DEFAULT_SMS_CONSENT);
      return handleContactEmail({ session, rawText, consentLanguage });
    }
    case STAGE_CONTACT_PHONE:
      return handleContactPhone({ session, rawText });
    case STAGE_CONFIRM:
      return handleConfirm({ session, schedPayload, rawText, tenantId, sessionId, deps, log });
    default:
      // Defensive: unknown/corrupt stage — end the session rather than loop forever.
      log && log('WARN', 'Unknown scheduling stage — ending session', { sessionId, stage: session.stage });
      return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_UNAVAILABLE, quickReplies: [] }] };
  }
}

module.exports = {
  // row CRUD
  loadSchedulingSession,
  saveSchedulingSession,
  deleteSchedulingSession,
  // entry
  resolveAppointmentTypeId,
  beginScheduling,
  // per-turn dispatch
  advanceScheduling,
  parseSchedPayload,
  getMessengerString,
  // constants
  STATE_TYPE_SCHEDULING_SESSION,
  SCHEDULING_SESSION_TTL_SECONDS,
  STAGE_PROPOSING,
  STAGE_CONTACT_EMAIL,
  STAGE_CONTACT_PHONE,
  STAGE_CONFIRM,
  DEFAULT_SMS_CONSENT,
  DEFAULT_NO_SLOTS,
  DEFAULT_SCHEDULING_UNAVAILABLE,
  DEFAULT_BOOKED,
  DEFAULT_SCHEDULING_CANCELLED,
  DEFAULT_COMMIT_FAILED,
  // M8b: last_booking row CRUD
  loadLastBooking,
  saveLastBooking,
  deleteLastBooking,
  patchLastBooking,
  // M8b: manage (reschedule/cancel) entry + dispatch
  resolveManageTrigger,
  handleManageTrigger,
  detectManageIntent,
  // M8b: constants
  STATE_TYPE_LAST_BOOKING,
  LAST_BOOKING_TTL_SECONDS,
  DEFAULT_MANAGE_NOT_FOUND,
  DEFAULT_MANAGE_ABORTED,
  DEFAULT_MANAGE_RESCHEDULE_ABORTED,
  DEFAULT_MANAGE_MUTATE_FAILED,
  DEFAULT_MANAGE_RESCHEDULED,
  DEFAULT_SLOT_GONE_MANAGE,
  // internals exposed for unit tests
  buildSlotMessages,
  buildCommitPayload,
  confirmSummaryText,
  findChosenSlot,
  buildLastBookingSnapshot,
  buildMutatePayload,
  projectBookingForMutate,
  unmarshallBookingItem,
  coordinatorIdOfProjection,
  beginManageReschedule,
  executeManageCancel,
  handleManageReschedulePick,
};
