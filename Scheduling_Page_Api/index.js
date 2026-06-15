'use strict';

/**
 * Scheduling_Page_Api — deterministic propose/mutate gateway for the branded /schedule/
 * page (M1, Calendly-style picker). The page (browser) calls this directly via a CORS-
 * enabled Function URL. It is the DETERMINISTIC path: pick a day → propose (that day's
 * times) → pick a time + Confirm → mutate (reschedule/cancel). The conversational agent
 * path (the companion chat → streaming handler) is SEPARATE and unchanged.
 *
 * ── Auth model (no token here) ──
 *   The §B10 session binding minted at token redemption IS the auth (30-min TTL). The page
 *   carries ?session=<bindingSessionId> + ?t=<tenantHash>. We resolve tenantHash → tenantId
 *   (registry), then resolveBinding({tenantId, sessionId}) — a sessionId minted under tenant
 *   A misses the GetItem under tenant B (unforgeable cross-tenant, §B12). The binding's
 *   `intent` must match the requested mutation (rescheduling_intent → reschedule, etc.).
 *
 * ── What it reuses (never re-implements) ──
 *   shared/scheduling/sessionBinding.js `resolveBinding` (§B12); the SHIPPED BCH
 *   `scheduling_propose` / `scheduling_mutate` actions (the SAME deterministic seam the
 *   streaming agent invokes — same Lambda, same payloads, mirrored from schedulingFlow.js).
 *
 * ── PII hygiene ──
 *   Never logs token/email/booking PII — only opaque ids + outcome. CORS is locked to the
 *   page origin. The full booking is loaded server-side and handed to BCH over the same-
 *   account encrypted Lambda invoke (not the security boundary; transport only).
 *
 * Env: ENVIRONMENT, BOOKING_TABLE, TENANT_REGISTRY_TABLE, SCHEDULING_EXECUTOR_FUNCTION_NAME
 *   (= Booking_Commit_Handler), PAGE_ALLOWED_ORIGIN, DEFAULT_TIMEZONE,
 *   SCHEDULING_SESSION_TABLE (read by sessionBinding.js), AWS_* timeouts.
 */

const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
  DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

// §B12 binding resolver (SoT) — never re-implemented.
const { resolveBinding } = require('../shared/scheduling/sessionBinding.js');

// ─── config ───────────────────────────────────────────────────────────────────────
const ENV = process.env.ENVIRONMENT || 'staging';
const BOOKING_TABLE = process.env.BOOKING_TABLE || `picasso-booking-${ENV}`;
const TENANT_REGISTRY_TABLE =
  process.env.TENANT_REGISTRY_TABLE || `picasso-tenant-registry-${ENV}`;
const BCH_FUNCTION =
  process.env.SCHEDULING_EXECUTOR_FUNCTION_NAME || 'Booking_Commit_Handler';
const ALLOWED_ORIGIN =
  process.env.PAGE_ALLOWED_ORIGIN || 'https://staging.chat.myrecruiter.ai';
const DEFAULT_TZ = process.env.DEFAULT_TIMEZONE || 'America/Chicago';
// The §B10 binding row lives in the conversation-scheduling-session table at SK
// `binding#<sessionId>` (same default sessionBinding.js uses). We delete it after a
// successful mutate (one-time-use — no replay of the real calendar op).
const SCHEDULING_SESSION_TABLE =
  process.env.SCHEDULING_SESSION_TABLE ||
  `picasso-conversation-scheduling-session-${ENV}`;

// newSlot timestamps must be RFC3339/ISO-8601 with an explicit offset/Z — arbitrary
// strings would reach the Google Calendar API. (SEC-S3.)
const ISO_DATETIME_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
const MAX_HASH_LEN = 128;
const MAX_SESSION_LEN = 64;

const REQUEST_TIMEOUT_MS = Number(process.env.AWS_REQUEST_TIMEOUT_MS || 5000);
const CONNECTION_TIMEOUT_MS = Number(process.env.AWS_CONNECTION_TIMEOUT_MS || 3000);
const ddb = new DynamoDBClient({
  maxAttempts: Number(process.env.AWS_MAX_ATTEMPTS || 2),
  requestHandler: new NodeHttpHandler({
    connectionTimeout: CONNECTION_TIMEOUT_MS,
    requestTimeout: REQUEST_TIMEOUT_MS,
  }),
});
// BCH calendar ops can take longer than a DDB op — give the invoke more headroom.
const lambda = new LambdaClient({
  maxAttempts: 1,
  requestHandler: new NodeHttpHandler({
    connectionTimeout: CONNECTION_TIMEOUT_MS,
    requestTimeout: Number(process.env.BCH_INVOKE_TIMEOUT_MS || 25000),
  }),
});

// The binding intent that authorizes each mutation (defense-in-depth — a reschedule
// binding must not be usable to cancel, and vice-versa).
const INTENT_FOR = Object.freeze({
  reschedule: 'rescheduling_intent',
  cancel: 'cancellation_intent',
});

// ─── logging (no PII) ───────────────────────────────────────────────────────────────
function log(event, fields) {
  console.log(JSON.stringify({ event, ...fields }));
}
function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

// ─── responses ──────────────────────────────────────────────────────────────────────
// NOTE: CORS is a BROWSER-enforced convention, NOT a server security boundary — a non-
// browser caller (curl/SSR) ignores it entirely. Auth is the §B10 binding, never CORS.
// `Referrer-Policy: no-referrer` keeps the ?session=/?t= from leaking via Referer (SEC-S2).
function corsHeaders() {
  return {
    'access-control-allow-origin': ALLOWED_ORIGIN,
    'access-control-allow-methods': 'POST, OPTIONS',
    'access-control-allow-headers': 'content-type',
    'access-control-max-age': '600',
    vary: 'origin',
    'referrer-policy': 'no-referrer',
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  };
}
function json(statusCode, body) {
  return { statusCode, headers: corsHeaders(), body: JSON.stringify(body) };
}

// ─── request parsing (Function URL, payload format 2.0) ─────────────────────────────
function getMethod(event) {
  return (
    (event &&
      event.requestContext &&
      event.requestContext.http &&
      event.requestContext.http.method) ||
    'POST'
  );
}
function parseBody(event) {
  try {
    if (!event || event.body == null) return {};
    const raw = event.isBase64Encoded
      ? Buffer.from(event.body, 'base64').toString('utf8')
      : event.body;
    const parsed = JSON.parse(raw || '{}');
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (_) {
    return null;
  }
}

// ─── DynamoDB reads ─────────────────────────────────────────────────────────────────
// tenantHash → tenantId via the registry GSI (same index Master_Function / bedrock-core use).
async function getTenantIdByHash(tenantHash) {
  const res = await ddb.send(
    new QueryCommand({
      TableName: TENANT_REGISTRY_TABLE,
      IndexName: 'TenantHashIndex',
      KeyConditionExpression: 'tenantHash = :h',
      ExpressionAttributeValues: { ':h': { S: tenantHash } },
      Limit: 1,
    })
  );
  const item = res && res.Items && res.Items[0];
  return (item && item.tenantId && item.tenantId.S) || null;
}

// Minimal DynamoDB unmarshal (S / N / BOOL only — the booking row's attribute types).
function unmarshall(item) {
  const out = {};
  if (!item) return out;
  for (const k of Object.keys(item)) {
    const v = item[k];
    if (!v) continue; // defensive: a malformed attr value never crashes the read
    if (v.S !== undefined) out[k] = v.S;
    else if (v.N !== undefined) out[k] = Number(v.N);
    else if (v.BOOL !== undefined) out[k] = v.BOOL;
  }
  return out;
}

async function getBooking(tenantId, bookingId) {
  const res = await ddb.send(
    new GetItemCommand({
      TableName: BOOKING_TABLE,
      Key: { tenantId: { S: tenantId }, booking_id: { S: bookingId } },
    })
  );
  return res && res.Item ? unmarshall(res.Item) : null;
}

// booking → coordinatorId. coordinatorId is the OAuth secret-path key
// (picasso/scheduling/oauth/{tenantId}/{coordinatorId}) = the routing resource_id, NOT the
// coordinator EMAIL. Falling back to the email routes BCH's getOAuthClient to a non-existent
// secret → silent failure (OAUTH-1). So: binding.coordinator_id → booking.resource_id only.
function coordinatorIdOf(booking, binding) {
  return (
    (binding && binding.coordinator_id) ||
    booking.resource_id ||
    booking.resourceId ||
    null
  );
}

// Full-day UTC window for a calendar day `dateIso` (YYYY-MM-DD) in tenant tz `tz`.
// The naive `${date}T00:00:00` (no offset) is parsed as UTC in the Lambda → a user west of
// the coordinator loses that day's later slots (TZ-1). We resolve the tz's UTC offset for
// the date (sampled at local noon to dodge the midnight DST edge) and shift the bounds.
function zonedDayWindowUTC(dateIso, tz) {
  const [y, m, d] = dateIso.split('-').map(Number);
  const noonUTC = Date.UTC(y, m - 1, d, 12, 0, 0);
  let offsetMs = 0;
  try {
    const dtf = new Intl.DateTimeFormat('en-US', {
      timeZone: tz, hour12: false,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
    const p = Object.fromEntries(
      dtf.formatToParts(new Date(noonUTC)).filter((x) => x.type !== 'literal').map((x) => [x.type, +x.value])
    );
    // hour can come back as 24 for midnight in some engines — normalize.
    const hh = p.hour === 24 ? 0 : p.hour;
    const wallAsUTC = Date.UTC(p.year, p.month - 1, p.day, hh, p.minute, p.second);
    offsetMs = wallAsUTC - noonUTC; // tz is ahead of UTC by offsetMs
  } catch (_) {
    offsetMs = 0; // bad tz → fall back to UTC bounds (no worse than before)
  }
  const startUTC = Date.UTC(y, m - 1, d, 0, 0, 0) - offsetMs;
  const endUTC = Date.UTC(y, m - 1, d, 23, 59, 59) - offsetMs;
  return { start: new Date(startUTC).toISOString(), end: new Date(endUTC).toISOString() };
}

// One-time-use: burn the §B10 binding after a successful mutate so the real calendar op
// cannot be replayed for the rest of the 30-min TTL (REPLAY-1). Best-effort.
async function deleteBinding(tenantId, sessionId) {
  await ddb.send(
    new DeleteItemCommand({
      TableName: SCHEDULING_SESSION_TABLE,
      Key: { tenantId: { S: tenantId }, session_id: { S: `binding#${sessionId}` } },
    })
  );
}

// ─── BCH invoke (the SHIPPED deterministic seam) ────────────────────────────────────
async function invokeBch(payload) {
  const out = await lambda.send(
    new InvokeCommand({
      FunctionName: BCH_FUNCTION,
      InvocationType: 'RequestResponse',
      Payload: Buffer.from(JSON.stringify(payload)),
    })
  );
  if (out.FunctionError) {
    throw new Error(`BCH FunctionError: ${out.FunctionError}`);
  }
  return out.Payload
    ? JSON.parse(Buffer.from(out.Payload).toString('utf8'))
    : null;
}

// ─── handler ────────────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  if (getMethod(event) === 'OPTIONS') {
    return { statusCode: 204, headers: corsHeaders(), body: '' };
  }

  const body = parseBody(event);
  if (!body) return json(400, { error: 'bad_request' });

  const action = body.action;
  const tenantHash =
    typeof body.t === 'string' && body.t.length <= MAX_HASH_LEN ? body.t : null;
  const sessionId =
    typeof body.session === 'string' && body.session.length <= MAX_SESSION_LEN
      ? body.session
      : null;
  if (!tenantHash || !sessionId || (action !== 'propose' && action !== 'mutate')) {
    return json(400, { error: 'missing_params' });
  }

  // hash → tenantId. ENUM-1: every PRE-auth failure (bad hash, missing/expired binding,
  // booking gone) returns an IDENTICAL 401 — no differential oracle to enumerate tenants /
  // live sessions / booking state. Server logs still distinguish for ops.
  let tenantId;
  try {
    tenantId = await getTenantIdByHash(tenantHash);
  } catch (err) {
    warn('gw_registry_error', { name: err && err.name });
    return json(500, { error: 'server_error' });
  }
  if (!tenantId) {
    log('gw_tenant_not_found', {});
    return json(401, { error: 'unauthorized' });
  }

  // resolve the §B10 binding (the auth).
  let binding;
  try {
    binding = await resolveBinding({ tenantId, sessionId });
  } catch (err) {
    warn('gw_binding_error', { tenant_id: tenantId, name: err && err.name });
    return json(500, { error: 'server_error' });
  }
  if (!binding) {
    log('gw_binding_missing_or_expired', { tenant_id: tenantId });
    return json(401, { error: 'unauthorized' });
  }
  if (!binding.booking_id) {
    log('gw_binding_no_booking', { tenant_id: tenantId });
    return json(401, { error: 'unauthorized' });
  }

  // Intent gate (SEC-S1): only a reschedule/cancel binding may use the page API. A
  // recovery_intent binding must not enumerate availability. (cancel mode calls propose
  // once for the booking summary, so both reschedule + cancel intents are allowed here.)
  if (binding.intent !== 'rescheduling_intent' && binding.intent !== 'cancellation_intent') {
    log('gw_intent_unsupported', { tenant_id: tenantId, intent: binding.intent });
    return json(403, { error: 'forbidden' });
  }

  // load the booking
  let booking;
  try {
    booking = await getBooking(tenantId, binding.booking_id);
  } catch (err) {
    warn('gw_booking_error', {
      tenant_id: tenantId,
      booking_id: binding.booking_id,
      name: err && err.name,
    });
    return json(500, { error: 'server_error' });
  }
  if (!booking) {
    log('gw_booking_not_found', { tenant_id: tenantId });
    return json(401, { error: 'unauthorized' });
  }

  // ── propose: that day's available times (deterministic, read-only) ──
  if (action === 'propose') {
    const apptId = booking.appointment_type_id || booking.appointmentTypeId;
    if (!apptId) {
      log('gw_no_appointment_type', { tenant_id: tenantId });
      return json(409, { error: 'no_appointment_type' });
    }
    const userTimeZone = booking.timezone || booking.timeZone || DEFAULT_TZ;
    const payload = {
      action: 'scheduling_propose',
      tenantId,
      sessionId,
      appointmentTypeId: apptId,
      userTimeZone,
    };
    // Optional single-day window (the calendar / quick-day buttons pass ?date=YYYY-MM-DD).
    // TZ-1: bound the day in the tenant timezone, emitted as UTC ISO (not naive local).
    if (typeof body.date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(body.date)) {
      payload.date_window = zonedDayWindowUTC(body.date, userTimeZone);
    }
    let res;
    try {
      res = await invokeBch(payload);
    } catch (err) {
      warn('gw_propose_error', { tenant_id: tenantId, name: err && err.name });
      return json(502, { error: 'propose_failed' });
    }
    log('gw_proposed', {
      tenant_id: tenantId,
      outcome: (res && res.outcome) || 'failed',
      slot_count: (res && res.slots && res.slots.length) || 0,
    });
    return json(200, {
      outcome: (res && res.outcome) || 'failed',
      slots: (res && res.slots) || [],
      context: (res && res.context) || null,
      // Hero context (no extra read — the booking is already loaded). Lets the page render
      // "Current appointment: <formatted current_start_at in timezone>" below the headline.
      appointment_label: booking.appointment_type_name || booking.appointmentTypeName || null,
      current_start_at: booking.start_at || booking.startAt || null,
      timezone: userTimeZone,
    });
  }

  // ── mutate: reschedule / cancel (deterministic commit) ──
  const mutation =
    body.mutation === 'cancel'
      ? 'cancel'
      : body.mutation === 'reschedule'
        ? 'reschedule'
        : null;
  if (!mutation) return json(400, { error: 'bad_mutation' });

  // The binding's intent must authorize THIS mutation (a reschedule binding can't cancel).
  if (binding.intent !== INTENT_FOR[mutation]) {
    log('gw_intent_mismatch', { mutation, intent: binding.intent });
    return json(403, { error: 'intent_mismatch' });
  }

  const coordinatorId = coordinatorIdOf(booking, binding);
  if (!coordinatorId) {
    log('gw_no_coordinator', { tenant_id: tenantId });
    return json(409, { error: 'no_coordinator' });
  }

  const payload = {
    action: 'scheduling_mutate',
    mutation,
    tenantId,
    coordinatorId,
    booking,
  };
  if (mutation === 'reschedule') {
    const ns = body.newSlot;
    if (!ns || !ns.start || !ns.end) return json(400, { error: 'missing_newSlot' });
    // VAL-1: only ISO-8601 timestamps with an explicit offset, start strictly before end —
    // arbitrary strings would reach the Google Calendar API.
    if (
      !ISO_DATETIME_RE.test(ns.start) ||
      !ISO_DATETIME_RE.test(ns.end) ||
      !(new Date(ns.start) < new Date(ns.end))
    ) {
      return json(400, { error: 'invalid_slot' });
    }
    payload.newSlot = { start: ns.start, end: ns.end };
  }

  let res;
  try {
    res = await invokeBch(payload);
  } catch (err) {
    warn('gw_mutate_error', { mutation, tenant_id: tenantId, name: err && err.name });
    return json(502, { error: 'mutate_failed' });
  }
  const outcome = (res && res.outcome) || 'failed';
  const ok =
    outcome === 'success' || outcome === 'deleted' || outcome === 'pending_calendar_sync';
  // REPLAY-1: one-time-use — burn the binding after a successful real calendar op so it
  // can't be replayed for the rest of the 30-min TTL. Best-effort (the mutate already
  // succeeded; a failed delete just leaves it redeemable until TTL — log, don't fail).
  if (ok) {
    try {
      await deleteBinding(tenantId, sessionId);
    } catch (err) {
      warn('gw_binding_burn_failed', { mutation, tenant_id: tenantId, name: err && err.name });
    }
  }
  log('gw_mutated', { mutation, tenant_id: tenantId, outcome });
  return json(ok ? 200 : 502, { outcome });
};

exports._internal = {
  getMethod,
  parseBody,
  getTenantIdByHash,
  getBooking,
  unmarshall,
  coordinatorIdOf,
  corsHeaders,
  INTENT_FOR,
};
