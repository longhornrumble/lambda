'use strict';

/**
 * cleanup.js — nightly synthetic test-data hygiene (§5.1).
 *
 * Deletes synthetic bookings older than the retention window (default 7 days). Bounded to
 * the synthetic tenant's partition (Query on the PK) — never a full-table scan, never
 * cross-tenant — and filtered to is_synthetic=true + item_type='booking', so a real
 * booking can never be deleted. Refuses to run if SYNTHETIC_TENANT_ID is unset.
 */

const bookingTable = require('./booking-table');
const alerts = require('./alerts');

const SYNTHETIC_TENANT_ID = process.env.SYNTHETIC_TENANT_ID || '';
const RETENTION_DAYS = Number(process.env.SYNTHETIC_RETENTION_DAYS || 7);

function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

async function runCleanup(deps = {}) {
  const query = deps.querySyntheticOlderThan || bookingTable.querySyntheticOlderThan;
  const del = deps.deleteBooking || bookingTable.deleteBooking;
  const emit = deps.emitCycleResult || alerts.emitCycleResult;
  const alert = deps.alert || alerts.alert;
  const tenantId = deps.tenantId || SYNTHETIC_TENANT_ID;
  const nowMs = deps.nowMs || Date.now();
  const retentionDays = deps.retentionDays || RETENTION_DAYS;

  try {
    if (!tenantId) {
      throw new Error('SYNTHETIC_TENANT_ID is required for cleanup');
    }
    const cutoffIso = new Date(nowMs - retentionDays * 24 * 60 * 60 * 1000).toISOString();
    const stale = await query(tenantId, cutoffIso);
    let deleted = 0;
    for (const row of stale) {
      await del(row.tenantId, row.booking_id);
      deleted += 1;
    }
    log('cleanup_ok', { tenant_id: tenantId, cutoff: cutoffIso, candidates: stale.length, deleted });
    await emit('cleanup', true);
    return { cycle: 'cleanup', success: true, deleted, cutoff: cutoffIso };
  } catch (err) {
    warn('cleanup_failed', { tenant_id: tenantId, error: err.message });
    await emit('cleanup', false);
    await alert('Scheduling synthetic: cleanup FAILED', { tenantId, error: err.message });
    return { cycle: 'cleanup', success: false, error: err.message };
  }
}

module.exports = { runCleanup };
