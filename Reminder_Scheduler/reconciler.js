'use strict';

/**
 * reconciler.js — nightly reminder/attendance reconciler (FROZEN_CONTRACTS §E1/§E4; plan E9).
 *
 * A BOUNDED safety-net scan — NEVER a full-table scan. Per tenant it queries the
 * `tenantId-start_at-index` GSI over a bounded start_at window and performs three
 * corrections on what it finds.
 *
 * WINDOW NOTE (implementation reconciliation, flagged for the integrator — NOT a contract
 * fork): plan E9 / the work-order describe a "prior-7d window" for the attendance backstop
 * AND a ">7d-old" terminal-schedule cleanup. A literal 7-day window cannot contain a
 * >7-day-old booking, so the bounded scan uses a configurable LOOKBACK (default 14d) that
 * covers BOTH: the attendance backstop fires on recently-ended bookings, the terminal
 * cleanup fires on terminal bookings whose start_at is older than the 7d threshold. The
 * scan stays bounded (RECONCILE_LOOKBACK_DAYS) — never a full-table scan.
 *
 * The three corrections:
 *
 *   (a) ATTENDANCE BACKSTOP — a booking whose appointment ended (end_at + 35min) is in the
 *       past, status still 'booked', and has NO attendance_state, means the attendance
 *       check did not fire (missed push / failed schedule). Set attendance_state =
 *       'pending_attendance' via a CONDITIONAL write (attribute_not_exists) so it is a
 *       no-op if WS-E-ATTEND's fire-time path already set it. NO auto-completion (§E4.1) —
 *       only the pending flag; human disposition still required.
 *   (b) TERMINAL CLEANUP — a booking in a terminal status (canceled / completed / no_show /
 *       coordinator_no_show) whose start_at is >7d old: delete any lingering EventBridge
 *       schedules + rows (defence against a delete that was missed at cancel time).
 *   (c) D6-OUTCOME-(ii) ORPHAN RECOVERY — a booking carrying pending_calendar_sync +
 *       rescheduled_old_event_id (executeReschedule landed insert✓/delete✗) — retry the
 *       Google delete of the lingering OLD event; on success clear both flags.
 *
 * Tenant enumeration mirrors Calendar_Watch_Renewer: event.tenant_ids OR env
 * SCHEDULING_TENANT_IDS (no tenant-registry scan). All AWS access is injected (deps).
 */

const { deleteReminders } = require('./scheduler');

const DAY_MS = 24 * 60 * 60 * 1000;

const TERMINAL_STATUSES = new Set(['canceled', 'completed', 'no_show', 'coordinator_no_show']);
const TENANT_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

function s(item, key) {
  return item && item[key] !== undefined ? item[key] : undefined;
}

function resolveTenantIds(event, env) {
  let raw = [];
  if (event && Array.isArray(event.tenant_ids) && event.tenant_ids.length > 0) {
    raw = event.tenant_ids;
  } else if (env.SCHEDULING_TENANT_IDS) {
    raw = String(env.SCHEDULING_TENANT_IDS)
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean);
  }
  if (raw.length === 0) {
    throw new Error('No tenant ids: provide event.tenant_ids or set SCHEDULING_TENANT_IDS');
  }
  for (const id of raw) {
    if (!TENANT_ID_RE.test(id)) throw new Error(`Invalid tenant_id "${id}"`);
  }
  return raw;
}

// Bounded GSI query: a LOOKBACK-day start_at window for one tenant (paginated).
async function queryRecentBookings(deps, cfg, tenantId, nowMs) {
  const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
  const lowerIso = new Date(nowMs - cfg.lookbackDays * DAY_MS).toISOString();
  const upperIso = new Date(nowMs).toISOString();
  const items = [];
  let ExclusiveStartKey;
  let pages = 0;
  do {
    const res = await deps.ddb.send(
      new QueryCommand({
        TableName: cfg.bookingTable,
        IndexName: cfg.startAtIndex,
        KeyConditionExpression: 'tenantId = :t AND start_at BETWEEN :lo AND :hi',
        ExpressionAttributeValues: { ':t': tenantId, ':lo': lowerIso, ':hi': upperIso },
        ExclusiveStartKey,
      })
    );
    for (const it of res.Items || []) {
      if (s(it, 'item_type') && s(it, 'item_type') !== 'booking') continue; // skip slot_lock etc.
      items.push(it);
    }
    ExclusiveStartKey = res.LastEvaluatedKey;
    pages += 1;
  } while (ExclusiveStartKey && pages < cfg.maxPagesPerTenant);
  return items;
}

async function setPendingAttendance(deps, cfg, tenantId, bookingId, nowIso) {
  const { UpdateCommand } = require('@aws-sdk/lib-dynamodb');
  try {
    await deps.ddb.send(
      new UpdateCommand({
        TableName: cfg.bookingTable,
        Key: { tenantId, booking_id: bookingId },
        UpdateExpression: 'SET attendance_state = :p, attendance_flagged_at = :n',
        ConditionExpression: 'attribute_not_exists(attendance_state)',
        ExpressionAttributeValues: { ':p': 'pending_attendance', ':n': nowIso },
      })
    );
    return true;
  } catch (err) {
    // Already set by WS-E-ATTEND's fire-time path → idempotent no-op.
    if (err && err.name === 'ConditionalCheckFailedException') return false;
    throw err;
  }
}

async function clearOrphanFlags(deps, cfg, tenantId, bookingId) {
  const { UpdateCommand } = require('@aws-sdk/lib-dynamodb');
  await deps.ddb.send(
    new UpdateCommand({
      TableName: cfg.bookingTable,
      Key: { tenantId, booking_id: bookingId },
      UpdateExpression: 'REMOVE pending_calendar_sync, rescheduled_old_event_id',
    })
  );
}

// D6-outcome-(ii): retry the Google delete of the lingering OLD event. The actual
// per-coordinator OAuth + calendar.delete is injected (deps.deleteCalendarEvent) — same
// DI seam reschedule.js/cancel.js use — so this module stays free of googleapis in tests.
async function recoverOrphan(deps, cfg, item, log) {
  const tenantId = s(item, 'tenantId');
  const bookingId = s(item, 'booking_id');
  const oldEventId = s(item, 'rescheduled_old_event_id');
  const calendarId = s(item, 'coordinator_email');
  if (!oldEventId || !calendarId) return { recovered: false, reason: 'missing_ids' };
  if (typeof deps.deleteCalendarEvent !== 'function') {
    log.warn(
      JSON.stringify({ event: 'orphan_recovery_skipped_no_dep', booking_id: bookingId })
    );
    return { recovered: false, reason: 'no_dep' };
  }
  try {
    await deps.deleteCalendarEvent({ tenantId, coordinatorEmail: calendarId, eventId: oldEventId });
    await clearOrphanFlags(deps, cfg, tenantId, bookingId);
    return { recovered: true };
  } catch (err) {
    log.warn(
      JSON.stringify({ event: 'orphan_recovery_failed', booking_id: bookingId, error: err.message })
    );
    return { recovered: false, reason: 'delete_failed' };
  }
}

function defaultConfig(env) {
  const ENV = env.ENVIRONMENT || 'staging';
  return {
    bookingTable: env.BOOKING_TABLE || `picasso-booking-${ENV}`,
    startAtIndex: env.BOOKING_START_AT_INDEX || 'tenantId-start_at-index',
    scheduledMessagesTable: env.SCHEDULED_MESSAGES_TABLE || 'picasso-scheduled-messages',
    maxPagesPerTenant: Number(env.RECONCILE_MAX_PAGES || 20),
    lookbackDays: Number(env.RECONCILE_LOOKBACK_DAYS || 14),
    terminalCleanupAgeMs: 7 * DAY_MS, // schedules for terminal bookings >7d old are swept
    attendanceGraceMs: 35 * 60 * 1000, // end_at + 35min
  };
}

/**
 * Run the reconciler over a set of tenants.
 *
 * @param {object} args
 * @param {string[]} args.tenantIds
 * @param {number}   [args.nowMs]
 * @param {object}   deps - injected { ddb, scheduler, deleteCalendarEvent?, logger, config }
 * @returns {Promise<object>} per-correction counts.
 */
async function runReconcile({ tenantIds, nowMs } = {}, deps = {}) {
  const log = deps.logger || console;
  const cfg = deps.config || defaultConfig(process.env);
  const now = nowMs || (deps.now ? deps.now() : Date.now());
  const nowIso = new Date(now).toISOString();

  const summary = {
    tenants: 0,
    bookings_scanned: 0,
    attendance_flagged: 0,
    terminal_cleaned: 0,
    orphans_recovered: 0,
    errors: 0,
  };

  for (const tenantId of tenantIds) {
    summary.tenants += 1;
    let items;
    try {
      items = await queryRecentBookings(deps, cfg, tenantId, now);
    } catch (err) {
      summary.errors += 1;
      log.error(JSON.stringify({ event: 'reconcile_query_failed', tenant_id: tenantId, error: err.message }));
      continue;
    }

    for (const item of items) {
      summary.bookings_scanned += 1;
      const bookingId = s(item, 'booking_id');
      const status = s(item, 'status');
      const startAtMs = Date.parse(s(item, 'start_at') || '');
      const endAtMs = Date.parse(s(item, 'end_at') || s(item, 'start_at') || '');

      try {
        // (c) orphan recovery — independent of status (a pending_calendar_sync booking
        //     may be active or terminal).
        if (s(item, 'pending_calendar_sync') === true && s(item, 'rescheduled_old_event_id')) {
          const r = await recoverOrphan(deps, cfg, item, log);
          if (r.recovered) summary.orphans_recovered += 1;
        }

        // (b) terminal cleanup — >7d-old terminal bookings: sweep stray schedules+rows.
        if (TERMINAL_STATUSES.has(status)) {
          if (!Number.isNaN(startAtMs) && now - startAtMs > cfg.terminalCleanupAgeMs) {
            await deleteReminders({ booking: item, tenantId, bookingId }, deps);
            summary.terminal_cleaned += 1;
          }
          continue; // terminal bookings never need the attendance backstop
        }

        // (a) attendance backstop — booked + ended >35min ago + no attendance_state.
        if (
          status === 'booked' &&
          !Number.isNaN(endAtMs) &&
          now - endAtMs > cfg.attendanceGraceMs &&
          !s(item, 'attendance_state')
        ) {
          const flagged = await setPendingAttendance(deps, cfg, tenantId, bookingId, nowIso);
          if (flagged) summary.attendance_flagged += 1;
        }
      } catch (err) {
        summary.errors += 1;
        log.error(
          JSON.stringify({ event: 'reconcile_item_failed', booking_id: bookingId, error: err.message })
        );
      }
    }
  }

  log.info(JSON.stringify({ event: 'reconcile_complete', ...summary }));
  return summary;
}

module.exports = {
  runReconcile,
  resolveTenantIds,
  defaultConfig,
  TERMINAL_STATUSES,
};
