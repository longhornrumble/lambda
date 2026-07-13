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
 */

const { GetCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { toE164 } = require('../shared/scheduling/phone');
const { recordBookingSmsConsent } = require('../shared/scheduling/consent');
const formEngine = require('./formEngine');
const { CAROUSEL_MAX } = require('./capabilities');

const STATE_TYPE_SCHEDULING_SESSION = 'scheduling_session';
/** G-P4 T1': idle TTL = 1 hour from last update, refreshed on each step. */
const SCHEDULING_SESSION_TTL_SECONDS = 60 * 60;

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
    return { committed: true, session: null, bookingId: res.bookingId, messages: [{ kind: 'text', text: DEFAULT_BOOKED, quickReplies: [] }] };
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
  return { session: result.session, messages: result.messages, committed: result.committed, bookingId: result.bookingId };
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
    return { session: null, messages: [{ kind: 'text', text: DEFAULT_SCHEDULING_CANCELLED, quickReplies: [] }] };
  }

  switch (session.stage) {
    case STAGE_PROPOSING:
      return handleProposing({ session, schedPayload, rawText, log });
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
  // internals exposed for unit tests
  buildSlotMessages,
  buildCommitPayload,
  confirmSummaryText,
  findChosenSlot,
};
