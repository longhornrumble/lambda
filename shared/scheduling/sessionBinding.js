'use strict';

/**
 * sessionBinding.js — §B12 `resolveBinding` (WS-BINDING, B1-backend).
 *
 * The in-chat reschedule/cancel/recovery flow has no per-action token: a token
 * authenticates ENTRY only (canonical §13.4). WS-D4, at redemption, writes ONE
 * §B10 session-binding row and lands the volunteer in chat; subsequent
 * slot-pick/confirm turns are authorized against that binding instead of a second
 * token. This module is the read+validate half WS-CONVO calls as its pre-turn hook.
 *
 * ── The binding row (§B10) ──
 *   Lives in the EXISTING picasso-conversation-scheduling-session-{env} table
 *   (C3 — PK `tenantId` · SK `session_id`; same table stateMachine.js uses for the
 *   C9 state row). The binding is a DISTINCT row keyed by SK `binding#<session_id>`,
 *   so it coexists with the plain-`session_id` state row for the same chat session.
 *   WS-D4 writes: { tenantId, session_id:`binding#<id>`, intent, booking_id,
 *   form_submission_id? (recovery only), expires_at (epoch ms), created_at, ttl }.
 *
 * ── Security model ──
 *   - Tenant isolation: `tenantId` is the PK and comes from the AUTHENTICATED
 *     request context (never the URL). A `sessionId` minted under tenant A simply
 *     misses the GetItem under tenant B → unforgeable cross-tenant. This module only
 *     does the tenant-scoped point-read; it never accepts a tenant from untrusted input.
 *   - TTL enforced IN CODE (§B12, architect): `now >= expires_at` → null. We do NOT
 *     trust DDB-TTL deletion timing for the gate (deletion lags expiry by minutes).
 *   - Fail closed: a malformed/expiry-less row returns null, not a throw — a binding
 *     we cannot trust must not authorize an action.
 *
 * ── DI seam ──
 *   `deps = { ddb, now }` (§B12). `ddb` is a DynamoDBClient (`.send`-able); `now` is a
 *   () => epochMs clock. Both default to real implementations so production callers may
 *   omit them; tests inject fakes.
 *
 * ── Schema discipline (CLAUDE.md) ──
 *   Optional fields (`coordinator_id`, `form_submission_id`) are read defensively and
 *   only included on the result when present on the row — old rows without them do not
 *   crash this reader. `coordinator_id` is not written by today's WS-D4 (§B10); it is a
 *   forward-compatible passthrough for a future writer.
 */

const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');

// Created once at module load; reused across warm invocations. Only touched when a
// caller omits deps.ddb (production default).
const defaultDdb = new DynamoDBClient({});

const ENV = process.env.ENVIRONMENT || 'staging';
const SCHEDULING_SESSION_TABLE =
  process.env.SCHEDULING_SESSION_TABLE ||
  `picasso-conversation-scheduling-session-${ENV}`;

// The SK discriminator that separates a binding row from the C9 state row of the
// same chat session (§B10: SK `binding#<session_id>`).
const BINDING_SK_PREFIX = 'binding#';

/**
 * Resolve the §B10 session-binding for the current chat session.
 *
 * @param {object}   args
 * @param {string}   args.tenantId  - from the authenticated request context (PK).
 * @param {string}   args.sessionId - the chat session id WS-D4 redirected with; the
 *                                     binding row lives at SK `binding#${sessionId}`.
 * @param {object}   [args.deps]    - { ddb, now } DI seam.
 * @returns {Promise<{
 *   intent: 'rescheduling_intent'|'cancellation_intent'|'recovery_intent',
 *   booking_id: string,
 *   coordinator_id?: string,
 *   form_submission_id?: string,
 *   expires_at: number,
 *   session_id: string,
 * } | null>} the binding, or null when missing / expired / malformed.
 */
async function resolveBinding({ tenantId, sessionId, deps = {} } = {}) {
  if (!tenantId) {
    throw new Error('tenantId is required');
  }
  if (!sessionId) {
    throw new Error('sessionId is required');
  }

  const { ddb = defaultDdb, now = Date.now } = deps;

  const res = await ddb.send(
    new GetItemCommand({
      TableName: SCHEDULING_SESSION_TABLE,
      Key: {
        tenantId: { S: tenantId },
        session_id: { S: `${BINDING_SK_PREFIX}${sessionId}` },
      },
    })
  );

  const item = res && res.Item;
  if (!item) {
    return null; // missing row, or cross-tenant miss → no binding.
  }

  // TTL gate (in code, not DDB-TTL). A row without a finite expires_at is untrusted.
  const expiresAt =
    item.expires_at && item.expires_at.N !== undefined
      ? Number(item.expires_at.N)
      : NaN;
  if (!Number.isFinite(expiresAt) || now() >= expiresAt) {
    return null;
  }

  const result = {
    intent: item.intent && item.intent.S,
    booking_id: item.booking_id && item.booking_id.S,
    expires_at: expiresAt,
    session_id: item.session_id && item.session_id.S,
  };

  // Forward-compatible optionals: present-only (§B10 + schema discipline).
  if (item.coordinator_id && item.coordinator_id.S !== undefined) {
    result.coordinator_id = item.coordinator_id.S;
  }
  if (item.form_submission_id && item.form_submission_id.S !== undefined) {
    result.form_submission_id = item.form_submission_id.S;
  }

  return result;
}

module.exports = { resolveBinding };
