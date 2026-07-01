'use strict';

/**
 * scheduler.js — per-booking EventBridge Scheduler rule lifecycle (FROZEN_CONTRACTS §E1).
 *
 * The ONLY new backend surface in sub-phase E: nobody creates per-booking schedules
 * today. The dispatch CONSUMER already exists and is frozen by its shipped shape —
 * `Scheduled_Message_Sender.handler({ pk, sk, message_id })` reads the row, status-gates
 * 'pending', and dispatches. This module CREATES the rows it reads + the EventBridge
 * one-time schedules that fire it.
 *
 * Lifecycle (§E1):
 *   • At commit (E2): write N picasso-scheduled-messages rows (status:'pending') + create
 *     N EventBridge schedules (one per reminder tier) + the `sched-attendance-{id}` rule.
 *   • RE-BIND (token-reschedule ONLY — same booking_id, start_at updated in place via §B9
 *     executeReschedule): DELETE old schedules+rows, recompute tiers vs the NEW start_at,
 *     CREATE fresh. ◀ the named WS-E-REMIND exit-criterion (see scheduler.test.js).
 *   • DELETE (any cancel, INCLUDING booking.calendar_moved): the cal-lifecycle consumer
 *     CANCELS on a coordinator move (cancel_reason=coordinator_moved — it does NOT move in
 *     place), so calendar_moved → DELETE all schedules+rows. A rebook is a NEW booking.
 *
 * Deterministic, idempotent names (§E1):
 *   reminder    → `sched-reminder-{tier}-{booking_id}`   (tier ∈ t24h|t4h|t1h|t15m)
 *   attendance  → `sched-attendance-{booking_id}`
 * (booking_id is sanitised for the EventBridge name charset; the exact names + row SKs are
 *  persisted on the Booking as `reminder_schedule_state` (E6) so delete/rebind are exact.)
 *
 * Rule target = Scheduled_Message_Sender; rule input = { pk, sk, message_id } (exact shape).
 * The EventBridge Scheduler IAM execution role (trust scheduler.amazonaws.com,
 * lambda:InvokeFunction on Scheduled_Message_Sender only) is integrator glue — passed as
 * RoleArn on every CreateSchedule. See DEPLOY_NOTE.md.
 *
 * All AWS access is injected (deps) — no module-level clients in the testable path.
 */

const { computeReminderTiers } = require('./cadence');

// ─── field access (forward-compatible reads — snake_case OR camelCase) ─────────────────

function pick(obj, ...keys) {
  if (!obj) return undefined;
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null && obj[k] !== '') return obj[k];
  }
  return undefined;
}

// G1: locale-aware "when" label (mirrors index.js formatWhen — shared discipline §9.3).
function formatWhenLabel(startAt, timeZone) {
  try {
    return new Intl.DateTimeFormat('en-US', {
      weekday: 'short', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit', timeZoneName: 'short',
      timeZone: timeZone || 'UTC',
    }).format(new Date(startAt));
  } catch (_) {
    return startAt || '';
  }
}

// ─── name / id derivation (deterministic + idempotent) ────────────────────────────────

// EventBridge Scheduler names allow [0-9a-zA-Z-_.] only — sanitise the booking_id
// (`booking#<hex>`) deterministically. The exact name is also persisted in
// reminder_schedule_state, so a non-reversible transform is fine.
function safeName(bookingId) {
  return String(bookingId).replace(/[^0-9a-zA-Z]/g, '-');
}

function reminderScheduleName(tier, bookingId) {
  return `sched-reminder-${tier}-${safeName(bookingId)}`;
}

function attendanceScheduleName(bookingId) {
  return `sched-attendance-${safeName(bookingId)}`;
}

function reminderMessageId(bookingId, tier) {
  return `${bookingId}#${tier}`;
}

function attendanceMessageId(bookingId) {
  return `${bookingId}#attendance`;
}

function messagePk(tenantId) {
  return `TENANT#${tenantId}`;
}

function messageSk(startAtIso, messageId) {
  return `SCHEDULED#${startAtIso}#${messageId}`;
}

// EventBridge Scheduler one-time `at()` expression: NO timezone offset / 'Z' / millis.
function atExpression(fireAtMs) {
  return `at(${new Date(fireAtMs).toISOString().slice(0, 19)})`;
}

// ─── booking projection ───────────────────────────────────────────────────────────────

function readBooking(booking) {
  return {
    tenantId: pick(booking, 'tenant_id', 'tenantId'),
    bookingId: pick(booking, 'booking_id', 'bookingId'),
    startAt: pick(booking, 'start_at', 'startAt'),
    endAt: pick(booking, 'end_at', 'endAt'),
    timezone: pick(booking, 'timezone', 'timeZone') || 'UTC',
    attendeeEmail: pick(booking, 'attendee_email', 'attendeeEmail'),
    attendeePhone: pick(booking, 'attendee_phone', 'attendeePhone'),
    attendeeName: pick(booking, 'attendee_name', 'attendeeName'),
    coordinatorEmail: pick(booking, 'coordinator_email', 'coordinatorEmail'),
    appointmentTypeName:
      pick(booking, 'appointment_type_name', 'appointmentTypeName') || 'appointment',
    organizationName:
      pick(booking, 'organization_name', 'organizationName', 'org_name') || 'us',
    // Additive, forward-compatible: absent on old rows → '' (the {{programName}} token
    // then renders empty, per the §E14 unknown-var contract).
    programName: pick(booking, 'program_name', 'programName') || '',
    isSynthetic: pick(booking, 'is_synthetic', 'isSynthetic') === true,
    existingState: pick(booking, 'reminder_schedule_state', 'reminderScheduleState'),
    // G1 additive: forward-compatible (undefined on old rows → join link skipped).
    joinUrl: pick(booking, 'join_url', 'joinUrl'),
  };
}

// ─── row builders (§E1 write shape) ───────────────────────────────────────────────────

// The minimal tenant SMS-prefs snapshot the fire-time selectChannels gate (§E3) needs.
// Snapshotting (vs a config fetch at dispatch) keeps the consumer self-contained; the
// TCPA-critical signal — live consent — is still read at fire time inside selectChannels.
function snapshotTenantPrefs(tenantPrefs) {
  const np = (tenantPrefs && tenantPrefs.notificationPrefs) || {};
  return {
    notificationPrefs: { sms: np.sms === true },
    sms_quiet_hours: pick(tenantPrefs, 'sms_quiet_hours') || (np && np.sms_quiet_hours) || null,
  };
}

function reminderBody({ b, tier }) {
  // v1 reminder copy. `start_at` is read at FIRE TIME by the consumer for the live
  // appointment instant; this body carries the org + type only (no snapshotted time).
  const lead =
    tier === 't24h' ? 'tomorrow' : tier === 't4h' ? 'in a few hours' : tier === 't1h' ? 'in about an hour' : 'shortly';
  return `Reminder: your ${b.appointmentTypeName} with ${b.organizationName} is coming up ${lead}.`;
}

function buildReminderRow({ b, tier, fireAtMs, tenantPrefsSnap, config, rescheduleUrl, cancelUrl, joinUrl, whenLabel }) {
  const messageId = reminderMessageId(b.bookingId, tier);
  const body = reminderBody({ b, tier });
  // G1: bake action-link fields onto the row (only when non-empty — forward-compatible).
  // The consumer (Scheduled_Message_Sender) appends the rendered action block OUTSIDE
  // the editable body at fire time; old-shape rows lacking these fields → no block emitted.
  const row = {
    pk: messagePk(b.tenantId),
    sk: messageSk(b.startAt, messageId),
    tenant_id: b.tenantId,
    // Email is the §E3 floor; the fire-time selectChannels gate adds the SMS supplement.
    channel: 'email',
    recipient_email: b.attendeeEmail || '',
    recipient_phone: b.attendeePhone || '',
    subject: `Appointment reminder — ${b.organizationName}`,
    body,
    template: 'appointment_reminder',
    template_vars: {
      organization_name: b.organizationName,
      appointment_type: b.appointmentTypeName,
      first_name: (b.attendeeName || '').split(' ')[0] || '',
      program_name: b.programName || '',
    },
    appointment_id: b.bookingId,
    message_id: messageId,
    from_number: config.fromNumber || '',
    status: 'pending',
    // additive fire-time-gate context (consumed by the §E3 selectChannels call):
    moment: 'reminder',
    timezone: b.timezone,
    tenant_prefs: tenantPrefsSnap,
    tier,
    fire_at: new Date(fireAtMs).toISOString(),
    // best-effort self-clean (requires TTL enabled on the table — see DEPLOY_NOTE);
    // harmless when TTL is off. 7d after fire.
    ttl: Math.floor(fireAtMs / 1000) + 7 * 24 * 60 * 60,
  };
  // G1: bake action-link fields only when non-empty (forward-compatible — consumer
  // skips the action block when these are absent, no empty lines emitted).
  if (rescheduleUrl) row.reschedule_url = rescheduleUrl;
  if (cancelUrl) row.cancel_url = cancelUrl;
  if (joinUrl) row.join_url = joinUrl;
  if (whenLabel) row.when_label = whenLabel;
  return row;
}

function buildAttendanceRow({ b, fireAtMs, tenantPrefsSnap, config }) {
  const messageId = attendanceMessageId(b.bookingId);
  return {
    pk: messagePk(b.tenantId),
    sk: messageSk(b.startAt, messageId),
    tenant_id: b.tenantId,
    channel: 'email',
    // The 3-option interviewer prompt is addressed to the coordinator. WS-E-ATTEND owns
    // the fire-time disposition semantics (sets attendance_state, renders the 3-option) —
    // this row + schedule satisfy the §E1 obligation to CREATE the check at commit.
    recipient_email: b.coordinatorEmail || '',
    recipient_phone: '',
    subject: `Did your ${b.appointmentTypeName} happen?`,
    body: `Please confirm whether your ${b.appointmentTypeName} with ${b.organizationName} took place.`,
    template: 'appointment_attendance_check',
    template_vars: {
      organization_name: b.organizationName,
      appointment_type: b.appointmentTypeName,
    },
    appointment_id: b.bookingId,
    message_id: messageId,
    from_number: config.fromNumber || '',
    status: 'pending',
    moment: 'reminder',
    attendance_check: true,
    timezone: b.timezone,
    tenant_prefs: tenantPrefsSnap,
    fire_at: new Date(fireAtMs).toISOString(),
    ttl: Math.floor(fireAtMs / 1000) + 7 * 24 * 60 * 60,
  };
}

// ─── default deps (the only AWS-touching code) ────────────────────────────────────────

function defaultConfig() {
  const ENV = process.env.ENVIRONMENT || 'staging';
  return {
    scheduledMessagesTable: process.env.SCHEDULED_MESSAGES_TABLE || 'picasso-scheduled-messages',
    bookingTable: process.env.BOOKING_TABLE || `picasso-booking-${ENV}`,
    targetArn: process.env.SCHEDULER_TARGET_ARN || '',
    roleArn: process.env.SCHEDULER_ROLE_ARN || '',
    groupName: process.env.SCHEDULER_GROUP_NAME || 'default',
    fromNumber: process.env.SCHEDULING_FROM_NUMBER || '',
    stagingTestMode: process.env.STAGING_TEST_MODE === 'true',
  };
}

function buildDefaultDeps() {
  // Lazy require so unit tests (which inject deps) never load the AWS SDK.
  const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
  const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
  const {
    SchedulerClient,
    CreateScheduleCommand,
    DeleteScheduleCommand,
  } = require('@aws-sdk/client-scheduler');

  const region = process.env.AWS_REGION || 'us-east-1';
  const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
  const schedulerClient = new SchedulerClient({ region });

  return {
    ddb,
    now: () => Date.now(),
    logger: console,
    config: defaultConfig(),
    scheduler: {
      async createSchedule(params) {
        try {
          await schedulerClient.send(new CreateScheduleCommand(params));
        } catch (err) {
          // Idempotent create: a same-named schedule already exists → treat as success.
          if (err && err.name === 'ConflictException') return;
          throw err;
        }
      },
      async deleteSchedule({ Name, GroupName }) {
        try {
          await schedulerClient.send(new DeleteScheduleCommand({ Name, GroupName }));
        } catch (err) {
          // Idempotent delete: already gone → success.
          if (err && err.name === 'ResourceNotFoundException') return;
          throw err;
        }
      },
    },
  };
}

// ─── DDB helpers (injected ddb) ───────────────────────────────────────────────────────

async function putRow(deps, table, item) {
  const { PutCommand } = require('@aws-sdk/lib-dynamodb');
  await deps.ddb.send(new PutCommand({ TableName: table, Item: item }));
}

async function deleteRow(deps, table, pk, sk) {
  const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
  await deps.ddb.send(new DeleteCommand({ TableName: table, Key: { pk, sk } }));
}

// Persist reminder_schedule_state onto the Booking (E6 additive bookkeeping) so
// delete/rebind operate on the EXACT names + SKs that were created.
async function writeScheduleState(deps, table, tenantId, bookingId, state) {
  const { UpdateCommand } = require('@aws-sdk/lib-dynamodb');
  await deps.ddb.send(
    new UpdateCommand({
      TableName: table,
      Key: { tenantId, booking_id: bookingId },
      UpdateExpression: 'SET reminder_schedule_state = :s',
      ExpressionAttributeValues: { ':s': state },
    })
  );
}

// ─── public API ───────────────────────────────────────────────────────────────────────

/**
 * Create the reminder + attendance schedules and their picasso-scheduled-messages rows
 * for a freshly-committed (or re-derived) booking.
 *
 * @param {object} args
 * @param {object} args.booking       - the persisted Booking (snake_case or camelCase)
 * @param {object} [args.tenantPrefs] - tenant config slice ({ notificationPrefs, sms_quiet_hours })
 *        used to snapshot the fire-time §E3 gate inputs into each row.
 * @param {string} [args.rescheduleUrl] - G1: pre-minted one-tap reschedule link. Minted by the
 *        COMMIT path (BCH buildActionLinks, which owns the tokens.js + Secrets Manager dep) and
 *        passed in — so scheduler.js imports NO signing SDK and stays loadable in the
 *        Reminder_Scheduler Lambda. Absent ('') → the row omits the field (fail-soft).
 * @param {string} [args.cancelUrl]     - G1: pre-minted one-tap cancel link (same rationale).
 * @param {object} [deps]             - injected { ddb, scheduler, now, logger, config }
 * @returns {Promise<{ tenantId, bookingId, reminders: {tier,scheduleName,sk}[],
 *          attendance: {scheduleName,sk}, tiers: string[] }>} the persisted reminder_schedule_state.
 */
async function scheduleReminders(
  { booking, tenantPrefs, rescheduleUrl = '', cancelUrl = '' } = {},
  deps = buildDefaultDeps()
) {
  const b = readBooking(booking);
  if (!b.tenantId) throw new Error('scheduleReminders requires booking.tenant_id');
  if (!b.bookingId) throw new Error('scheduleReminders requires booking.booking_id');
  if (!b.startAt) throw new Error('scheduleReminders requires booking.start_at');

  const cfg = deps.config || defaultConfig();
  const log = deps.logger || console;
  const nowMs = (deps.now || Date.now)();

  // is_synthetic time-compression — DOUBLE-gated (STAGING_TEST_MODE && is_synthetic).
  const synthetic = cfg.stagingTestMode === true && b.isSynthetic === true;

  const tenantPrefsSnap = snapshotTenantPrefs(tenantPrefs);
  const tiers = computeReminderTiers({ startAt: b.startAt, nowMs, synthetic });

  // G1: whenLabel is pure (Intl) — always computed so reminders show the appointment time
  // even when no action links were minted. The reschedule/cancel URLs arrive pre-minted from
  // the commit path (no signing dependency here). join link rides the booking row (b.joinUrl).
  const whenLabel = formatWhenLabel(b.startAt, b.timezone);

  const state = {
    tenantId: b.tenantId,
    bookingId: b.bookingId,
    reminders: [],
    attendance: null,
    tiers: tiers.map((t) => t.tier),
    synthetic,
    created_at: new Date(nowMs).toISOString(),
  };

  // 1. Reminder rows + schedules.
  for (const { tier, fireAtMs } of tiers) {
    const row = buildReminderRow({
      b, tier, fireAtMs, tenantPrefsSnap, config: cfg,
      // G1: pass minted urls ('' when minting failed → fields omitted from row).
      rescheduleUrl, cancelUrl, joinUrl: b.joinUrl || '', whenLabel,
    });
    await putRow(deps, cfg.scheduledMessagesTable, row);
    const name = reminderScheduleName(tier, b.bookingId);
    await deps.scheduler.createSchedule(
      buildCreateParams({ name, fireAtMs, cfg, pk: row.pk, sk: row.sk, messageId: row.message_id })
    );
    state.reminders.push({ tier, scheduleName: name, sk: row.sk });
  }

  // 2. Attendance check (event_end + 30min). §E1 requires it created at commit; the
  //    fire-time disposition (attendance_state + 3-option) is WS-E-ATTEND's surface.
  if (b.endAt) {
    const attendanceFireMs = synthetic
      ? nowMs + 5 * 60 * 1000
      : Date.parse(b.endAt) + 30 * 60 * 1000;
    if (attendanceFireMs > nowMs) {
      const row = buildAttendanceRow({ b, fireAtMs: attendanceFireMs, tenantPrefsSnap, config: cfg });
      await putRow(deps, cfg.scheduledMessagesTable, row);
      const name = attendanceScheduleName(b.bookingId);
      await deps.scheduler.createSchedule(
        buildCreateParams({
          name,
          fireAtMs: attendanceFireMs,
          cfg,
          pk: row.pk,
          sk: row.sk,
          messageId: row.message_id,
        })
      );
      state.attendance = { scheduleName: name, sk: row.sk };
    }
  }

  // 3. Persist the exact names + SKs onto the Booking (best-effort — the schedules+rows
  //    are authoritative; state is convenience bookkeeping for exact delete/rebind).
  try {
    await writeScheduleState(deps, cfg.bookingTable, b.tenantId, b.bookingId, state);
  } catch (err) {
    log.warn(
      JSON.stringify({
        event: 'reminder_schedule_state_write_failed',
        booking_id: b.bookingId,
        error: err.message,
      })
    );
  }

  log.info(
    JSON.stringify({
      event: 'reminders_scheduled',
      booking_id: b.bookingId,
      tiers: state.tiers,
      attendance: !!state.attendance,
      synthetic,
    })
  );
  return state;
}

function buildCreateParams({ name, fireAtMs, cfg, pk, sk, messageId }) {
  return {
    Name: name,
    GroupName: cfg.groupName,
    ScheduleExpression: atExpression(fireAtMs),
    ScheduleExpressionTimezone: 'UTC',
    FlexibleTimeWindow: { Mode: 'OFF' },
    ActionAfterCompletion: 'DELETE',
    Target: {
      Arn: cfg.targetArn,
      RoleArn: cfg.roleArn,
      Input: JSON.stringify({ pk, sk, message_id: messageId }),
    },
  };
}

/**
 * Delete every reminder + attendance schedule and row for a booking. Used by ANY cancel
 * — including booking.calendar_moved (move = cancel per the shipped reconcileMoved).
 *
 * Operates from reminder_schedule_state when present (exact SKs); otherwise falls back to
 * the deterministic schedule names (schedules) — rows then rely on the consumer's
 * status-gate defence-in-depth. Pass either a full booking or { tenantId, bookingId }.
 */
async function deleteReminders({ booking, tenantId, bookingId } = {}, deps = buildDefaultDeps()) {
  const cfg = deps.config || defaultConfig();
  const log = deps.logger || console;

  let b = booking ? readBooking(booking) : null;
  const tid = (b && b.tenantId) || tenantId;
  const bid = (b && b.bookingId) || bookingId;
  if (!tid || !bid) throw new Error('deleteReminders requires a booking or { tenantId, bookingId }');

  // If we were given only ids, try to load the booking so we can use its exact state.
  if (!b || !b.existingState) {
    const loaded = await loadBooking(deps, cfg.bookingTable, tid, bid).catch(() => null);
    if (loaded) b = readBooking(loaded);
  }
  const state = b && b.existingState;

  if (state) {
    for (const r of state.reminders || []) {
      await deps.scheduler.deleteSchedule({ Name: r.scheduleName, GroupName: cfg.groupName });
      if (r.sk) await deleteRow(deps, cfg.scheduledMessagesTable, messagePk(tid), r.sk);
    }
    if (state.attendance) {
      await deps.scheduler.deleteSchedule({
        Name: state.attendance.scheduleName,
        GroupName: cfg.groupName,
      });
      if (state.attendance.sk) {
        await deleteRow(deps, cfg.scheduledMessagesTable, messagePk(tid), state.attendance.sk);
      }
    }
  } else {
    // No bookkeeping — delete schedules by deterministic name (rows are status-gated
    // by the consumer as defence in depth).
    for (const tier of ['t24h', 't4h', 't1h', 't15m']) {
      await deps.scheduler.deleteSchedule({
        Name: reminderScheduleName(tier, bid),
        GroupName: cfg.groupName,
      });
    }
    await deps.scheduler.deleteSchedule({
      Name: attendanceScheduleName(bid),
      GroupName: cfg.groupName,
    });
  }

  // Clear the bookkeeping (best-effort).
  try {
    await clearScheduleState(deps, cfg.bookingTable, tid, bid);
  } catch (_) {
    /* best-effort */
  }

  log.info(JSON.stringify({ event: 'reminders_deleted', booking_id: bid, had_state: !!state }));
  return { tenantId: tid, bookingId: bid, deleted: true };
}

/**
 * Re-bind reminders on a TOKEN-RESCHEDULE (§B9 executeReschedule mutated start_at in place,
 * same booking_id). Deletes the old schedules+rows, recomputes tiers vs the NEW start_at,
 * and creates fresh. This is the ONLY re-bind trigger — calendar_moved is a DELETE.
 */
async function rebindReminders(
  { booking, tenantPrefs, rescheduleUrl = '', cancelUrl = '' } = {},
  deps = buildDefaultDeps()
) {
  const b = readBooking(booking);
  if (!b.tenantId || !b.bookingId) {
    throw new Error('rebindReminders requires booking.tenant_id and booking.booking_id');
  }
  await deleteReminders({ booking }, deps);
  // Re-read nothing — `booking` already carries the new start_at (executeReschedule
  // mutated it in place). Drop the stale state so scheduleReminders starts clean.
  // G1: pass through the freshly-minted (for the NEW start_at) action links so the rebound
  // reminders carry working reschedule/cancel links too.
  const fresh = { ...booking };
  delete fresh.reminder_schedule_state;
  delete fresh.reminderScheduleState;
  return scheduleReminders({ booking: fresh, tenantPrefs, rescheduleUrl, cancelUrl }, deps);
}

// ─── booking-row helpers ──────────────────────────────────────────────────────────────

async function loadBooking(deps, table, tenantId, bookingId) {
  const { GetCommand } = require('@aws-sdk/lib-dynamodb');
  const res = await deps.ddb.send(
    new GetCommand({ TableName: table, Key: { tenantId, booking_id: bookingId } })
  );
  return res && res.Item;
}

async function clearScheduleState(deps, table, tenantId, bookingId) {
  const { UpdateCommand } = require('@aws-sdk/lib-dynamodb');
  await deps.ddb.send(
    new UpdateCommand({
      TableName: table,
      Key: { tenantId, booking_id: bookingId },
      UpdateExpression: 'REMOVE reminder_schedule_state',
    })
  );
}

module.exports = {
  scheduleReminders,
  rebindReminders,
  deleteReminders,
  // exported for WS-E-ATTEND reuse + tests:
  reminderScheduleName,
  attendanceScheduleName,
  reminderMessageId,
  attendanceMessageId,
  messagePk,
  messageSk,
  atExpression,
  buildReminderRow,
  buildAttendanceRow,
  readBooking,
  snapshotTenantPrefs,
  buildDefaultDeps,
  defaultConfig,
};
