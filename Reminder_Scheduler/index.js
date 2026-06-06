'use strict';

/**
 * Reminder_Scheduler — entrypoint (FROZEN_CONTRACTS §E1; plan E2/E4/E9).
 *
 * Two roles:
 *   1. LIBRARY (re-exported below) — the per-booking EventBridge Scheduler rule lifecycle
 *      (scheduleReminders / rebindReminders / deleteReminders). The integrator wires these
 *      into the commit (BCH), the in-chat reschedule flow (after §B9 executeReschedule), and
 *      the cal-lifecycle cancel/move path. See DEPLOY_NOTE.md.
 *   2. HANDLER (`handler`) — the nightly E9 reconciler Lambda (EventBridge Scheduler /
 *      cron trigger). Event: { tenant_ids?: string[] }.
 *
 * PROD GUARD (§E1, SR-3): the handler refuses to start if STAGING_TEST_MODE=true AND
 * ENVIRONMENT=production — the synthetic time-compression branch must never run in prod.
 */

const scheduler = require('./scheduler');
const { runReconcile, resolveTenantIds, defaultConfig } = require('./reconciler');

// ─── prod-synthetic guard (§E1) ───────────────────────────────────────────────────────

function assertNotProdSynthetic(env = process.env) {
  if (env.STAGING_TEST_MODE === 'true' && env.ENVIRONMENT === 'production') {
    throw new Error(
      'Reminder_Scheduler refusing to start: STAGING_TEST_MODE=true with ENVIRONMENT=production ' +
        '(synthetic time-compression must never run in prod — §E1 SR-3 prod guard).'
    );
  }
}

// ─── orphan-recovery seam (D6-outcome-(ii)) ───────────────────────────────────────────

// The orphan-recovery LOGIC (find pending_calendar_sync + retry the old-event delete +
// clear the flags) lives in reconciler.js and is fully tested via the injected
// `deps.deleteCalendarEvent` seam. The actual delete needs the SHIPPED §B13 calendar
// facade curried with per-(tenant,coordinator) OAuth — which requires a per-secret IAM
// grant on the reconciler role + the coordinator-identity mapping (email vs resource_id).
// That is INTEGRATOR GLUE (parallel to the EventBridge Scheduler IAM role + the §E3
// selectChannels wiring), so it is NOT statically imported here — that would force the
// googleapis/OAuth stack into this otherwise-lean reconciler bundle. The integrator wires
// `deps.deleteCalendarEvent` at deploy (DEPLOY_NOTE.md "Orphan recovery wiring"). Until
// then recoverOrphan logs `orphan_recovery_skipped_no_dep` and skips (no crash).
function buildCalendarDeleter(wiring) {
  // wiring = { buildCalendarFacade, getOAuthClient, calendarEvents } (integrator-supplied).
  if (!wiring || !wiring.buildCalendarFacade) return undefined;
  const { buildCalendarFacade, getOAuthClient, calendarEvents } = wiring;
  return async function deleteCalendarEvent({ tenantId, coordinatorEmail, eventId }) {
    const facade = buildCalendarFacade({
      tenantId,
      coordinatorId: coordinatorEmail,
      deps: { getOAuthClient, calendarEvents },
    });
    await facade.deleteEvent(coordinatorEmail, eventId);
  };
}

// ─── reconciler handler ───────────────────────────────────────────────────────────────

async function handler(event = {}) {
  assertNotProdSynthetic();

  const tenantIds = resolveTenantIds(event, process.env);
  const deps = {
    ...scheduler.buildDefaultDeps(),
    config: { ...scheduler.defaultConfig(), ...defaultConfig(process.env) },
    // Integrator wires orphan recovery (per-secret OAuth/IAM) — see DEPLOY_NOTE.
    deleteCalendarEvent: undefined,
    logger: console,
  };
  // The reconciler shares the scheduler's ddb + scheduler clients (delete sweeps reuse
  // deleteReminders) — defaultConfig above already carries both tables + the GSI name.
  return runReconcile({ tenantIds }, deps);
}

module.exports = {
  handler,
  assertNotProdSynthetic,
  buildCalendarDeleter,
  // library re-exports (integrator wiring points):
  scheduleReminders: scheduler.scheduleReminders,
  rebindReminders: scheduler.rebindReminders,
  deleteReminders: scheduler.deleteReminders,
  runReconcile,
};
