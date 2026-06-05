"""
picasso-pii-tenant-purge-staging
---------------------------------
Operator-invocable per-TENANT offboarding purge Lambda (P1 — Class A surfaces).

Design: docs/roadmap/PII-Project/tenant-offboarding-purge-design.md (sign-off
complete 2026-06-03; picasso#361). Routes to data-retention-strategy.md §9
"per-tenant offboarding purge" — the one genuine build.

WHY A NEW LAMBDA (not a DSAR request_type): the DSAR Lambda is *subject*-scoped
(resolve one pii_subject_id, filter every surface by it). A tenant purge is
*partition*-scoped with no subject filter. The surfaces split into reachability
classes (design §3). This Lambda implements **Class A only** (P1): the cleanly
tenant-partitioned DynamoDB surfaces, plus the notification-events chain.

CLASS A SURFACES PURGED (this Lambda):
    - form-submissions       Query PK=tenant_id            → delete whole partition
    - notification-sends     Query PK=TENANT#{tenant_id}   → delete; yields message_ids
    - notification-events    chained via message_ids (ByMessageId GSI) → delete
    - pii-subject-index      Query PK=tenant_id            → delete (re-id key)
    - sms-usage              Query PK=tenant_id            → delete (also 30d TTL)

NOT in this Lambda (design §3/§8 decisions, RESOLVED 2026-06-03):
    - Class B (recent-messages 24h, session-events 90d, archive): TTL age-out —
      NOT force-deleted here (Q-B decided: v1 makes no sub-TTL erasure promise).
    - Class C (session-summaries, tenant_hash-keyed): P2, operator passes
      tenant_hash (Q-C). conversation-summaries (7d) ages out.
    - Class D (Glacier log archive, by log group not tenant): 365d age-out.

CARVE-OUTS THAT SURVIVE THE PURGE (design §5, counsel-gated — NEVER deleted):
    - picasso-sms-consent (opt-in proof + STOP/opt-out rows): legal floor (4–5yr).
    - SES account-level suppression list.
    - audit tables (this Lambda WRITES its own audit; it never deletes audit).
  The response + audit explicitly list these so an auditor sees they were
  intentionally retained, not missed.

CONTRACT (operator invokes via `aws lambda invoke`):
    {
      "tenant_id":       "<tenant_id>",
      "operator":        "<email of operator>",
      "purge_id":        "<uuid>",          # caller-supplied ledger ref
      "grace_confirmed": true,              # attests the 30-day grace elapsed
      "dry_run":         true               # DEFAULT true; explicit false to delete
    }

DUAL GATE (design §6): deletion happens ONLY when `dry_run == false` AND
`grace_confirmed == true`. Either one alone → dry-run count (counts what WOULD
be deleted, deletes nothing) + a manual_followup explaining why. This makes an
accidental whole-tenant deletion structurally impossible from a single typo.

RESPONSE (JSON):
    {
      "purge_id":             "<echo>",
      "tenant_id":            "<echo>",
      "status":               "completed" | "partial_error" | "failed",
      "deleted":              true|false,   # whether DeleteItem actually ran
      "rows_touched":         {"<surface>": <count>, ...},
      "carve_outs_retained":  ["sms-consent (+STOP)", "SES suppression", "audit"],
      "manual_followups":     ["<human-readable note>", ...],
      "audit_row_pks":        ["<purge_id|event_timestamp>", ...]
    }

STATUS SEMANTICS:
    - "completed":     all walkers ran cleanly (dry-run or real).
    - "partial_error": at least one walker errored mid-batch (query/delete fail).
    - "failed":        env-guard / input validation / audit collision — never
                       reached the walker dispatch.
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CLAUDE.md account→env map. Refuse to run unless the caller account matches
# EXPECTED_ACCOUNT — same env-guard posture as the DSAR Lambda. Per prod-cutover
# decision D1 (2026-06-04) this is an IaC-set env var (staging module → 525…,
# prod module → 614…), replacing the former hardcoded constant; the env-var
# guard is the prod-promotion gate. A whole-tenant purge is far more destructive
# than a per-subject DSAR, so FAIL-CLOSED is non-negotiable: unset ⇒ refuse.
EXPECTED_ACCOUNT = os.environ.get("EXPECTED_ACCOUNT")

# Staging table names — single source of truth (matches infra/modules/* locals
# and the DSAR Lambda's constants).
TABLE_FORM_SUBMISSIONS = "picasso-form-submissions-staging"
TABLE_NOTIFICATION_SENDS = "picasso-notification-sends"
TABLE_NOTIFICATION_EVENTS = "picasso-notification-events"
TABLE_SUBJECT_INDEX = "picasso-pii-subject-index-staging"
TABLE_SMS_USAGE = "picasso-sms-usage"
# Class C (F-DSAR31): pseudonymized session summaries, partitioned by
# pk=TENANT#{tenant_hash} (tenant_hash, NOT tenant_id). Reached only when the
# caller (the dashboard UI, or a CLI operator) supplies tenant_hash.
TABLE_SESSION_SUMMARIES = "picasso-session-summaries"
TABLE_PURGE_AUDIT = "picasso-pii-tenant-purge-audit-staging"
GSI_NOTIFICATION_EVENTS_BY_MESSAGE_ID = "ByMessageId"

# Carve-outs surfaced in every response/audit so retention is explicit, not
# accidental (design §5).
CARVE_OUTS_RETAINED = [
    "sms-consent (incl. STOP/opt-out proof) — legal floor 4-5yr",
    "SES account-level suppression list",
    "audit tables (purge writes audit; never deletes it)",
]

# Bound the chained notification-events GSI walk so a single purge cannot
# exhaust the Lambda timeout (mirrors DSAR MAX_MESSAGE_IDS_PER_INVOCATION).
# 500 message_ids x ~50ms GSI Query = ~25s, under the 120s timeout budget
# after the partition walks. Overflow surfaces as a manual_followup; the
# operator re-invokes (idempotent) to drain the remainder.
MAX_MESSAGE_IDS_PER_INVOCATION = 500

# Module-level clients (warm boto3 pool on cold-start).
ddb = boto3.resource("dynamodb")
sts = boto3.client("sts")


# ───────────────────────────────────────────────────────────────────────────
# Cold-start guard
# ───────────────────────────────────────────────────────────────────────────
def _assert_account():
    """Refuse to run unless the caller account matches EXPECTED_ACCOUNT; return
    caller ARN.

    Raises RuntimeError when EXPECTED_ACCOUNT is unset (fail-closed) or on
    account mismatch — handler returns status=failed, no DDB ops, no audit row.
    Returns the caller ARN (the Lambda execution role AWS actually sees) so the
    audit row records the real identity alongside the self-reported `operator`
    field. Mirrors DSAR `_assert_account`.
    """
    if not EXPECTED_ACCOUNT:
        raise RuntimeError(
            "tenant_purge_account_guard: EXPECTED_ACCOUNT env var is unset; "
            "refusing to run (fail-closed). IaC must set it to the account this "
            "Lambda is deployed in."
        )
    identity = sts.get_caller_identity()
    actual = identity["Account"]
    if actual != EXPECTED_ACCOUNT:
        raise RuntimeError(
            f"tenant_purge_account_guard: refusing to run in account {actual}; "
            f"expected account {EXPECTED_ACCOUNT} (set via EXPECTED_ACCOUNT env var)."
        )
    return identity.get("Arn")


# ───────────────────────────────────────────────────────────────────────────
# Input validation
# ───────────────────────────────────────────────────────────────────────────
class InvalidInput(ValueError):
    pass


class AuditCollision(RuntimeError):
    """Raised when audit PutItem hits ConditionalCheckFailedException.

    Idempotency invariant violated — the (purge_id, event_timestamp) pair
    already exists. Realistic cause is operator replay of the same purge_id.
    Handler returns status=failed (loudest signal) rather than silently
    overwriting a prior audit row. Mirrors DSAR AuditCollision.
    """
    pass


def _validate(event):
    """Strict required-field check. Operator-invoked → fail loud on bad input.

    Returns a normalized dict. `dry_run` defaults True and deletion requires
    BOTH `dry_run=false` AND `grace_confirmed=true` (the dual gate) so a typo
    or a missing field can never produce an unintended whole-tenant deletion.
    Booleans are validated as true booleans — string 'true'/'false' from a CLI
    caller would otherwise be truthy in Python and silently flip the gate.
    """
    if not isinstance(event, dict):
        raise InvalidInput("event must be a JSON object")

    required = {"tenant_id", "operator", "purge_id", "grace_confirmed"}
    missing = required - event.keys()
    if missing:
        raise InvalidInput(f"missing required fields: {sorted(missing)}")

    tenant_id = event["tenant_id"]
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise InvalidInput("tenant_id must be a non-empty string")
    tenant_id = tenant_id.strip()

    purge_id = event["purge_id"]
    if not isinstance(purge_id, str) or not purge_id.strip():
        raise InvalidInput("purge_id must be a non-empty string")
    purge_id = purge_id.strip()

    operator = event["operator"]
    if not isinstance(operator, str) or not operator.strip():
        raise InvalidInput("operator must be a non-empty string")

    grace_confirmed = event["grace_confirmed"]
    if not isinstance(grace_confirmed, bool):
        raise InvalidInput(
            f"grace_confirmed must be boolean true/false; got "
            f"{type(grace_confirmed).__name__}={grace_confirmed!r}"
        )

    # dry_run defaults True; must be a true boolean if provided.
    dry_run = event.get("dry_run", True)
    if dry_run is None:
        dry_run = True
    if not isinstance(dry_run, bool):
        raise InvalidInput(
            f"dry_run must be boolean true/false; got "
            f"{type(dry_run).__name__}={dry_run!r}"
        )

    return {
        "tenant_id": tenant_id,
        "operator": operator,
        "purge_id": purge_id,
        "grace_confirmed": grace_confirmed,
        "dry_run": dry_run,
        # Optional: tenant_hash unlocks the Class-C session-summaries surface
        # (pk=TENANT#{tenant_hash}). Absent → that surface is skipped with a
        # manual_followup; all Class-A surfaces still purge.
        "tenant_hash": (event.get("tenant_hash") or "").strip() or None,
    }


# ───────────────────────────────────────────────────────────────────────────
# Audit write (append-only event log to picasso-pii-tenant-purge-audit-staging)
# ───────────────────────────────────────────────────────────────────────────
def _now_iso():
    # Microsecond timespec for stable lexicographic ordering of audit rows
    # (same rationale as DSAR `_now_iso`).
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _write_audit_event(purge_id, event_type, status, payload):
    """PutItem append-only event row. PK=purge_id, SK=event_timestamp.

    `payload` is JSON-serialized into `details`. `status` is duplicated to the
    top level. Idempotency: ConditionExpression refuses to overwrite an
    existing (purge_id, event_timestamp); collision raises AuditCollision so
    the handler surfaces the replay instead of silently mutating audit state.
    Mirrors DSAR `_write_audit_event`.
    """
    table = ddb.Table(TABLE_PURGE_AUDIT)
    event_timestamp = _now_iso()
    item = {
        "purge_id": purge_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type,
        "status": status,
        "details": json.dumps(payload, default=str),
        # Year-month partition for a future counsel-determined audit purge to
        # Query a partition instead of full-table Scan (DSAR H4 pattern).
        "created_at_partition": event_timestamp[:7],
    }
    try:
        table.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(purge_id) AND "
                "attribute_not_exists(event_timestamp)"
            ),
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            logger.error(
                "purge_audit_collision: purge_id=%s event_type=%s ts=%s "
                "— idempotency invariant violated (caller replayed?)",
                purge_id, event_type, event_timestamp,
            )
            raise AuditCollision(
                f"audit row already exists for purge_id={purge_id} "
                f"event_timestamp={event_timestamp}"
            ) from exc
        raise
    return event_timestamp


# ───────────────────────────────────────────────────────────────────────────
# Per-surface partition purgers (Class A)
#
# Each: Query the tenant partition (paginated) → on real-delete, DeleteItem per
# row; on dry-run, count only. Corrupted rows (missing key parts) are logged +
# skipped, never crashed-on (schema discipline). Per-surface delete-failure
# counts distinguish "matched but not deleted" from "matched and deleted".
# Logs NEVER carry row PII (email/content) — key fields only.
# ───────────────────────────────────────────────────────────────────────────
def _delete_partition_rows(table, matched, key_fields, surface, dry_run):
    """Shared delete loop for a list of matched rows.

    `key_fields` is the ordered list of attribute names forming the table's
    primary key (e.g. ["tenant_id", "submission_id"]). A row missing any key
    field is skipped as corrupted. Returns a result dict.
    """
    rows_found = len(matched)
    if dry_run:
        return {"rows_found": rows_found, "action": "dry_run_count"}

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    for row in matched:
        key = {k: row.get(k) for k in key_fields}
        if any(v is None for v in key.values()):
            skipped_corrupted += 1
            # Redaction: log only the key-field NAMES that were present/absent,
            # never values (which may be PII like normalized_email).
            present = {k: (row.get(k) is not None) for k in key_fields}
            logger.error(
                "%s_delete_skipped_corrupted: key_present=%s — row missing key",
                surface, present,
            )
            continue
        try:
            table.delete_item(Key=key)
            deleted += 1
        except ClientError as exc:
            delete_failed += 1
            # Log only that a delete failed + the error code; never the key
            # values (PII). Surface in the count so the operator sees the gap.
            logger.error(
                "%s_delete_failed: code=%s",
                surface, exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
    }


def _query_partition(table, key_condition):
    """Paginated Query collecting all items for one partition. Raises on error
    (caller wraps to mark the surface errored)."""
    matched = []
    last_evaluated_key = None
    while True:
        kwargs = {"KeyConditionExpression": key_condition}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        resp = table.query(**kwargs)
        matched.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    return matched


def _purge_form_submissions(tenant_id, dry_run):
    """form-submissions: Query PK=tenant_id → delete whole tenant partition."""
    table = ddb.Table(TABLE_FORM_SUBMISSIONS)
    try:
        matched = _query_partition(table, Key("tenant_id").eq(tenant_id))
    except ClientError as exc:
        logger.error("form_submissions_query_failed: code=%s",
                     exc.response.get("Error", {}).get("Code"))
        return {"rows_found": 0, "action": "error", "error": "query_failed"}
    return _delete_partition_rows(
        table, matched, ["tenant_id", "submission_id"], "form_submissions", dry_run)


def _purge_notification_sends(tenant_id, dry_run):
    """notification-sends: Query PK=TENANT#{tenant_id} → delete; collect
    message_ids for the chained notification-events purge.

    message_ids are collected from the MATCHED rows regardless of dry_run so
    the events walker can dry-run-count the chain too. Empty message_id values
    (failed-send rows record '') are skipped.
    """
    table = ddb.Table(TABLE_NOTIFICATION_SENDS)
    pk_value = f"TENANT#{tenant_id}"
    try:
        matched = _query_partition(table, Key("pk").eq(pk_value))
    except ClientError as exc:
        logger.error("notification_sends_query_failed: code=%s",
                     exc.response.get("Error", {}).get("Code"))
        return {"rows_found": 0, "action": "error", "error": "query_failed",
                "message_ids": []}
    message_ids = [mid for mid in (r.get("message_id") for r in matched) if mid]
    result = _delete_partition_rows(
        table, matched, ["pk", "sk"], "notification_sends", dry_run)
    result["message_ids"] = message_ids
    return result


def _purge_notification_events(message_ids, dry_run):
    """notification-events: chained walk via the sends' message_ids.

    For each message_id, Query the ByMessageId GSI (projection ALL → base
    PK/SK present for DeleteItem). Bounded fan-out: overflow above
    MAX_MESSAGE_IDS_PER_INVOCATION is truncated and surfaced (idempotent
    re-invoke drains the rest). Continue-on-error: a failed message_id Query
    is recorded, not fatal. message_id is cryptographically unique (SES/Telnyx
    IDs) so the GSI walk cannot cross tenants.
    """
    if not message_ids:
        return {"rows_found": 0, "action": "no_messages"}

    truncated = 0
    if len(message_ids) > MAX_MESSAGE_IDS_PER_INVOCATION:
        truncated = len(message_ids) - MAX_MESSAGE_IDS_PER_INVOCATION
        message_ids = message_ids[:MAX_MESSAGE_IDS_PER_INVOCATION]
        logger.warning("notification_events_message_ids_truncated: cap=%d overflow=%d",
                       MAX_MESSAGE_IDS_PER_INVOCATION, truncated)

    table = ddb.Table(TABLE_NOTIFICATION_EVENTS)
    matched = []
    failed_message_ids = []
    for message_id in message_ids:
        last_evaluated_key = None
        while True:
            kwargs = {
                "IndexName": GSI_NOTIFICATION_EVENTS_BY_MESSAGE_ID,
                "KeyConditionExpression": Key("message_id").eq(message_id),
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key
            try:
                resp = table.query(**kwargs)
            except ClientError as exc:
                logger.error("notification_events_query_failed: code=%s",
                             exc.response.get("Error", {}).get("Code"))
                failed_message_ids.append(message_id)
                break
            matched.extend(resp.get("Items", []))
            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    result = _delete_partition_rows(
        table, matched, ["pk", "sk"], "notification_events", dry_run)
    if failed_message_ids:
        result["failed_message_ids"] = failed_message_ids
    if truncated:
        result["truncated_message_id_count"] = truncated
    return result


def _purge_subject_index(tenant_id, dry_run):
    """pii-subject-index: Query PK=tenant_id → delete the tenant's
    re-identification key rows (no subject survives the tenant)."""
    table = ddb.Table(TABLE_SUBJECT_INDEX)
    try:
        matched = _query_partition(table, Key("tenant_id").eq(tenant_id))
    except ClientError as exc:
        logger.error("subject_index_query_failed: code=%s",
                     exc.response.get("Error", {}).get("Code"))
        return {"rows_found": 0, "action": "error", "error": "query_failed"}
    return _delete_partition_rows(
        table, matched, ["tenant_id", "normalized_email"], "subject_index", dry_run)


def _purge_sms_usage(tenant_id, dry_run):
    """sms-usage: Query PK=tenant_id → delete monthly counters. (Also self-
    expires at 30d TTL; this purge is belt-and-suspenders.)"""
    table = ddb.Table(TABLE_SMS_USAGE)
    try:
        matched = _query_partition(table, Key("tenant_id").eq(tenant_id))
    except ClientError as exc:
        logger.error("sms_usage_query_failed: code=%s",
                     exc.response.get("Error", {}).get("Code"))
        return {"rows_found": 0, "action": "error", "error": "query_failed"}
    return _delete_partition_rows(
        table, matched, ["tenant_id", "month"], "sms_usage", dry_run)


def _purge_session_summaries(tenant_hash, dry_run):
    """session-summaries (Class C): Query PK=TENANT#{tenant_hash} → delete the
    whole tenant partition. tenant_hash-keyed (not tenant_id) — caller must
    supply tenant_hash; the handler only calls this when it's present."""
    table = ddb.Table(TABLE_SESSION_SUMMARIES)
    try:
        matched = _query_partition(table, Key("pk").eq(f"TENANT#{tenant_hash}"))
    except ClientError as exc:
        logger.error("session_summaries_query_failed: code=%s",
                     exc.response.get("Error", {}).get("Code"))
        return {"rows_found": 0, "action": "error", "error": "query_failed"}
    return _delete_partition_rows(
        table, matched, ["pk", "sk"], "session_summaries", dry_run)


# ───────────────────────────────────────────────────────────────────────────
# Handler
# ───────────────────────────────────────────────────────────────────────────
def _surface_rows_touched(result):
    """Pull the count to report per surface (rows_deleted on real delete,
    rows_found on dry-run / no-op)."""
    if result.get("action") == "deleted":
        return result.get("rows_deleted", 0)
    return result.get("rows_found", 0)


def lambda_handler(event, context):
    # 1. Account guard — fail closed before any DDB op or audit write.
    try:
        caller_arn = _assert_account()
    except RuntimeError as exc:
        logger.error("account_guard_failed: %s", exc)
        return {"status": "failed", "error": str(exc)}

    # 2. Validate.
    try:
        params = _validate(event)
    except InvalidInput as exc:
        logger.error("input_validation_failed: %s", exc)
        return {"status": "failed", "error": str(exc)}

    tenant_id = params["tenant_id"]
    purge_id = params["purge_id"]
    # Dual gate: deletion requires BOTH dry_run=false AND grace_confirmed=true.
    delete_authorized = (params["dry_run"] is False) and (params["grace_confirmed"] is True)
    walker_dry_run = not delete_authorized

    manual_followups = []
    if (params["dry_run"] is False) and (params["grace_confirmed"] is not True):
        manual_followups.append(
            "dry_run=false but grace_confirmed!=true → ran as DRY-RUN (no "
            "deletes). Set BOTH dry_run=false and grace_confirmed=true to "
            "delete (dual gate, design §6)."
        )

    audit_row_pks = []

    # 3. Opening audit row.
    try:
        ts = _write_audit_event(
            purge_id, "purge_requested",
            "in_progress" if delete_authorized else "dry_run",
            {
                "tenant_id": tenant_id,
                "operator": params["operator"],
                "caller_arn": caller_arn,
                "dry_run": params["dry_run"],
                "grace_confirmed": params["grace_confirmed"],
                "delete_authorized": delete_authorized,
            },
        )
        audit_row_pks.append(f"{purge_id}|{ts}")
    except AuditCollision as exc:
        return {"status": "failed", "error": str(exc), "purge_id": purge_id}
    except ClientError as exc:
        logger.error("opening_audit_write_failed: code=%s",
                     exc.response.get("Error", {}).get("Code"))
        return {"status": "failed", "error": "audit_write_failed", "purge_id": purge_id}

    # 4. Run Class A purgers in order (sends BEFORE events to chain message_ids).
    rows_touched = {}
    any_error = False

    form_res = _purge_form_submissions(tenant_id, walker_dry_run)
    rows_touched["form-submissions"] = _surface_rows_touched(form_res)

    sends_res = _purge_notification_sends(tenant_id, walker_dry_run)
    rows_touched["notification-sends"] = _surface_rows_touched(sends_res)

    events_res = _purge_notification_events(sends_res.get("message_ids", []), walker_dry_run)
    rows_touched["notification-events"] = _surface_rows_touched(events_res)

    subject_res = _purge_subject_index(tenant_id, walker_dry_run)
    rows_touched["subject-index"] = _surface_rows_touched(subject_res)

    sms_res = _purge_sms_usage(tenant_id, walker_dry_run)
    rows_touched["sms-usage"] = _surface_rows_touched(sms_res)

    # Class C (F-DSAR31): session-summaries — only if tenant_hash supplied.
    tenant_hash = params["tenant_hash"]
    if tenant_hash:
        ss_res = _purge_session_summaries(tenant_hash, walker_dry_run)
    else:
        ss_res = {"rows_found": 0, "action": "skipped_no_tenant_hash"}
        manual_followups.append(
            "session-summaries: skipped — tenant_hash not provided. This "
            "tenant_hash-keyed surface (pk=TENANT#{tenant_hash}) is only purged "
            "when tenant_hash is supplied (the dashboard passes it automatically; "
            "a CLI caller must include it). All other surfaces purged normally."
        )
    rows_touched["session-summaries"] = _surface_rows_touched(ss_res)

    surface_results = {
        "form-submissions": form_res,
        "notification-sends": sends_res,
        "notification-events": events_res,
        "subject-index": subject_res,
        "sms-usage": sms_res,
        "session-summaries": ss_res,
    }

    # 5. Per-surface audit + error/followup aggregation.
    for surface, result in surface_results.items():
        if result.get("action") == "error":
            any_error = True
            manual_followups.append(f"{surface}: query failed — re-invoke to retry.")
        if result.get("rows_delete_failed"):
            any_error = True
            manual_followups.append(
                f"{surface}: {result['rows_delete_failed']} row(s) failed to "
                f"delete — re-invoke (idempotent) to retry.")
        if result.get("rows_skipped_corrupted"):
            manual_followups.append(
                f"{surface}: {result['rows_skipped_corrupted']} corrupted row(s) "
                f"skipped (missing key) — operator inspection.")
        if result.get("failed_message_ids"):
            any_error = True
            manual_followups.append(
                f"notification-events: {len(result['failed_message_ids'])} "
                f"message_id(s) failed GSI query — re-invoke to retry.")
        if result.get("truncated_message_id_count"):
            manual_followups.append(
                f"notification-events: {result['truncated_message_id_count']} "
                f"message_id(s) over the {MAX_MESSAGE_IDS_PER_INVOCATION} cap — "
                f"re-invoke (idempotent) to drain the remainder.")
        # Surface-level audit row. Best-effort: an audit failure here does not
        # abort the purge (the deletes already happened) but is logged.
        try:
            ts = _write_audit_event(
                purge_id, f"surface_purged:{surface}",
                "error" if result.get("action") == "error" else "ok",
                {k: v for k, v in result.items() if k != "message_ids"},
            )
            audit_row_pks.append(f"{purge_id}|{ts}")
        except (AuditCollision, ClientError) as exc:
            logger.error("surface_audit_write_failed: surface=%s err=%s", surface, exc)

    status = "partial_error" if any_error else "completed"

    # 6. Closing audit row.
    try:
        ts = _write_audit_event(
            purge_id, "closed", status,
            {"rows_touched": rows_touched, "deleted": delete_authorized,
             "carve_outs_retained": CARVE_OUTS_RETAINED},
        )
        audit_row_pks.append(f"{purge_id}|{ts}")
    except (AuditCollision, ClientError) as exc:
        logger.error("closing_audit_write_failed: err=%s", exc)

    # 7. Response.
    return {
        "purge_id": purge_id,
        "tenant_id": tenant_id,
        "status": status,
        "deleted": delete_authorized,
        "rows_touched": rows_touched,
        "carve_outs_retained": CARVE_OUTS_RETAINED,
        "manual_followups": manual_followups,
        "audit_row_pks": audit_row_pks,
    }
