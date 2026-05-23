"""
picasso-pii-dsar-staging
------------------------
Operator-invocable DSAR fulfillment Lambda for MFS-scoped surfaces.

Capability-bundle item 1a per
docs/roadmap/PII-Project/CONSUMER_PII_REMEDIATION.md §"Path A Re-baseline v3"
(picasso PR #155, merged 2026-05-20).

CONTRACT (operator invokes via `aws lambda invoke` with this payload):
    {
      "subject_identifier":  "<email | psid | phone>",
      "identifier_type":     "email",            # milestone 1: email only
      "request_type":        "access" | "delete",# milestone 1: access + delete only
      "tenant_id":           "<tenant_id>",
      "operator":            "<email of operator>",
      "dsar_id":             "<uuid>",           # caller-supplied (DSAR ledger ref)
      "dry_run":             true                # DEFAULT true; explicit false to delete
    }

RESPONSE (JSON):
    {
      "dsar_id":           "<echo>",
      "status":            "completed" | "partial" | "partial_error" | "failed",
      "pii_subject_id":    "<resolved-opaque-id | null>",
      "rows_touched":      {"<surface>": <count>, ...},
      "exported_rows":     {"<surface>": [<row>, ...], ...},   # access only
      "manual_followups":  ["<human-readable-todo>", ...],
      "audit_row_pks":     ["<dsar_id|event_timestamp>", ...]
    }

STATUS SEMANTICS (audit fix-now #5):
    - "completed":     all walkers ran cleanly; no errors; no deferred surfaces
    - "partial":       walker(s) ran cleanly but at least one surface is still
                       deferred (today: all DSARs return "partial" because 5
                       of 6 surfaces are still deferred)
    - "partial_error": at least one walker errored mid-batch (query failure,
                       corrupted-row skip, surface audit collision)
    - "failed":        env-guard / input validation / audit-write collision
                       failure — never reached the walker dispatch

WHAT THIS LAMBDA DOES TODAY:
    - Cold-start env-guard (refuse to run outside account 525)
    - Input validation (required fields, supported types, dry_run default)
    - Subject resolution: identifier → pii_subject_id via picasso-pii-subject-index-staging
    - Audit writes — append-only, idempotent (ConditionExpression refuses
      replay on identical (dsar_id, event_timestamp)). Per-DSAR events:
      request_received → surface_walked:<surface> (one per non-deferred
      surface) → closed. All to picasso-pii-dsar-audit-staging.
    - form-submissions walker — tenant-scoped Query + FilterExpression on
      pii_subject_id; access returns rows in exported_rows; delete dry-runs by
      default; explicit dry_run=false performs DeleteItem per matched row;
      corrupted rows (missing PK/SK) are logged + skipped, not crashed-on
    - notification-sends walker — tenant-scoped Query (PK=`TENANT#<id>`) +
      FilterExpression on `recipient == normalized_email`. Catches direct-to-
      consumer notifications. Staff-recipient rows (notifications ABOUT the
      consumer to staff) are operator/staff PII under a different controller
      relationship (D5 G-H + F9) and flagged in manual_followup with an
      operator-actionable CLI snippet — NOT auto-deleted.
    - notification-events walker — chained walk via the `message_id`s captured
      by notification-sends. Queries the ByMessageId GSI per message_id.
      If notification-sends produces no message_ids (today's common case),
      records `action=no_messages_to_walk` with rows_touched=0.
    - Remaining surfaces (recent-messages, conversation-summaries, audit)
      return manual_followup + walker_results status=deferred until each
      surface's subject-linking attribute is verified against MFS writer code

WHAT IT DOES NOT YET DO (follow-up PRs):
    - Per-surface walkers for the 5 remaining MFS surfaces (one PR per surface)
    - picasso-audit-staging read for access-type DSARs (Art 17(3)(b) read-only)
    - Meta channel-mappings PSID-keyed walk (milestone 2 / item 1b)
    - S3 / ARCHIVE_BUCKET walks (milestone 2 / item 1b)
    - Pre-Phase-1 form-submission backfill walk (deferred per Apply-2)

The Lambda is intentionally deployable and invocable now. Calls succeed end-to-end
(env-guard → validate → resolve subject → audit-write → response) and produce a
complete audit trail. The substantive deletion/export work is the deliberate
discovery-then-implement loop documented in CONSUMER_PII_REMEDIATION.md v3.
"""
import json
import logging
import os
import re
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CLAUDE.md account→env map. The Lambda must refuse to run outside staging
# until the prod-promotion gate fires (see CONSUMER_PII_REMEDIATION.md
# v2 §"Locked decisions" / Apply-1 module's env-guard pattern).
EXPECTED_ACCOUNT = "525409062831"

# Staging table names — single source of truth (matches infra/modules/* locals).
TABLE_SUBJECT_INDEX = "picasso-pii-subject-index-staging"
TABLE_DSAR_AUDIT = "picasso-pii-dsar-audit-staging"
TABLE_FORM_SUBMISSIONS = "picasso-form-submissions-staging"
TABLE_NOTIFICATION_SENDS = "picasso-notification-sends-staging"
TABLE_NOTIFICATION_EVENTS = "picasso-notification-events-staging"
TABLE_RECENT_MESSAGES = "staging-recent-messages"
GSI_NOTIFICATION_EVENTS_BY_MESSAGE_ID = "ByMessageId"

# Bound the chained notification-events GSI walk so a single DSAR cannot
# exhaust the 60s Lambda timeout (Security advisor Finding 3, 2026-05-21).
# 200 message_ids × ~50ms avg GSI Query = ~10s, well under the timeout
# budget after the form-submissions walk + audit writes. Overflow surfaces
# as a manual_followup with the excess count; operator can re-invoke with
# narrower scope or wait for item 6's batching.
MAX_MESSAGE_IDS_PER_INVOCATION = 200

# Bound the chained recent-messages walk symmetrically — same rationale as
# MAX_MESSAGE_IDS_PER_INVOCATION; one Query per session_id, ~50ms typical,
# 200 sessions × 50ms = ~10s well under the 60s Lambda timeout.
MAX_SESSION_IDS_PER_INVOCATION = 200

# Soft cap on the access-export response payload. A chatty subject can
# accumulate dozens of messages per session × multiple sessions. Lambda's
# response limit is 6 MB; an unbounded export risks exceeding it. Overflow
# surfaces in manual_followups; the walker returns the first
# MAX_EXPORTED_MESSAGES rows.
MAX_EXPORTED_MESSAGES = 1000

# Surfaces explicitly deferred from M1 (re-scoped 2026-05-23 per
# phase-completion-audit row 5 / tech-lead B1). M1 outcome scope: form-
# submissions + notification-sends + notification-events + recent-messages
# walkers, plus subject resolution + audit-write + dispatcher. The two
# entries below were originally listed in M1 outcome statement (master
# plan §2) but never had walkers implemented; M1 v0.3 re-scopes them out
# explicitly with named routing — see MASTER_PROJECT_PLAN.md M1 done-bar
# revision history v0.3.
DEFERRED_SURFACES = {
    "conversation-summaries": (
        "Walker pending: sessionId-keyed (no subject linkage on row); "
        "chained walk via form-submissions session_ids — pattern mirrors "
        "recent-messages once verified against writer. M1 scope-excluded "
        "(v0.3 2026-05-23); routed to a follow-on milestone."
    ),
    "audit-read-only": (
        "Walker pending: picasso-audit-staging is read-only per Art 17(3)(b) "
        "carve-out (D5 G-C). Access-type DSAR exports rows; never delete. "
        "M1 scope-excluded (v0.3 2026-05-23); routed to a follow-on milestone."
    ),
}

# Backward-compat alias (callers in _walk_mfs_surfaces). The rename is
# documentation, not behavior — same dict, clearer name.
MFS_SCOPED_SURFACES = DEFERRED_SURFACES

SUPPORTED_REQUEST_TYPES = {"access", "delete"}
SUPPORTED_IDENTIFIER_TYPES = {"email"}  # milestone 1; psid arrives in 1b

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Module-level clients (boto3 connection pool warm on cold-start).
ddb = boto3.resource("dynamodb")
sts = boto3.client("sts")


# ───────────────────────────────────────────────────────────────────────────
# Cold-start guard
# ───────────────────────────────────────────────────────────────────────────
def _assert_account():
    """Refuse to run in any account other than staging; return caller ARN.

    Raises RuntimeError on mismatch — Lambda returns 500, no DDB ops happen,
    no audit row written. The Lambda execution role grants sts:GetCallerIdentity
    explicitly (lambda-pii-dsar-staging module).

    Audit row 12 (Security SR3): returns the caller ARN so the handler can
    log it into the `request_received` audit row. The `operator` payload
    field is self-reported; the caller ARN is the actual identity AWS sees
    (the Lambda's execution role, since invocation hops through Lambda's
    own service principal). This preserves accountability when the operator
    payload value can't be trusted.
    """
    identity = sts.get_caller_identity()
    actual = identity["Account"]
    if actual != EXPECTED_ACCOUNT:
        raise RuntimeError(
            f"dsar_account_guard: refusing to run in account {actual}; "
            f"expected staging account {EXPECTED_ACCOUNT}. "
            f"Prod promotion requires explicit code change."
        )
    return identity.get("Arn")


# ───────────────────────────────────────────────────────────────────────────
# Input validation
# ───────────────────────────────────────────────────────────────────────────
class InvalidInput(ValueError):
    pass


class AuditCollision(RuntimeError):
    """Raised when audit PutItem hits ConditionalCheckFailedException.

    Idempotency invariant violated — the (dsar_id, event_timestamp) pair
    already exists. Realistic cause is operator replay of the same dsar_id;
    microsecond-precision timestamp collisions on sequential calls are
    effectively impossible. Handler returns status=failed (loudest signal)
    rather than silently overwriting a prior audit row.
    """
    pass


def _normalize_email(email):
    """Lower + strip — matches the Phase-1 subject-index writer normalization."""
    return email.strip().lower()


def _validate(event):
    """Strict required-field check. Operator-invoked → fail loud on bad input.

    Returns a normalized dict. dry_run defaults True so a typo or missing field
    can never produce an unintended deletion.
    """
    if not isinstance(event, dict):
        raise InvalidInput("event must be a JSON object")

    required = {"subject_identifier", "identifier_type", "request_type",
                "tenant_id", "operator", "dsar_id"}
    missing = required - event.keys()
    if missing:
        raise InvalidInput(f"missing required fields: {sorted(missing)}")

    identifier_type = event["identifier_type"]
    if identifier_type not in SUPPORTED_IDENTIFIER_TYPES:
        raise InvalidInput(
            f"identifier_type {identifier_type!r} not supported in milestone 1; "
            f"supported: {sorted(SUPPORTED_IDENTIFIER_TYPES)}"
        )

    request_type = event["request_type"]
    if request_type not in SUPPORTED_REQUEST_TYPES:
        raise InvalidInput(
            f"request_type {request_type!r} not supported in milestone 1; "
            f"supported: {sorted(SUPPORTED_REQUEST_TYPES)}"
        )

    subject_identifier = event["subject_identifier"]
    if identifier_type == "email":
        normalized = _normalize_email(subject_identifier)
        if not EMAIL_RE.match(normalized):
            raise InvalidInput(f"subject_identifier does not look like an email")
        subject_identifier = normalized

    return {
        "subject_identifier": subject_identifier,
        "identifier_type": identifier_type,
        "request_type": request_type,
        "tenant_id": event["tenant_id"],
        "operator": event["operator"],
        "dsar_id": event["dsar_id"],
        "dry_run": bool(event.get("dry_run", True)),
    }


# ───────────────────────────────────────────────────────────────────────────
# Subject resolution
# ───────────────────────────────────────────────────────────────────────────
def _resolve_subject(tenant_id, normalized_email):
    """Look up pii_subject_id via picasso-pii-subject-index-staging.

    Returns the pii_subject_id string, or None if no index entry exists.
    The subject-index Query is keyed on (tenant_id, normalized_email).
    """
    table = ddb.Table(TABLE_SUBJECT_INDEX)
    try:
        resp = table.get_item(Key={
            "tenant_id": tenant_id,
            "normalized_email": normalized_email,
        })
    except ClientError as exc:
        logger.error(
            "subject_resolution_failed: tenant=%s err=%s",
            tenant_id, exc.response.get("Error", {}).get("Code"),
        )
        raise
    item = resp.get("Item")
    if not item:
        return None
    return item.get("pii_subject_id")


# ───────────────────────────────────────────────────────────────────────────
# Audit write (append-only event log to picasso-pii-dsar-audit-staging)
# ───────────────────────────────────────────────────────────────────────────
def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _write_audit_event(dsar_id, event_type, status, payload):
    """PutItem append-only event row. PK=dsar_id, SK=event_timestamp.

    `payload` is serialized to JSON in the `details` attribute. `status` is
    duplicated to the top level (StatusIndex GSI hash key) so the future
    EventBridge SLA alarm Lambda can Query by status.

    Idempotency: ConditionExpression refuses to overwrite an existing
    (dsar_id, event_timestamp) row. Collision raises AuditCollision so the
    handler can surface the replay condition to the operator instead of
    silently mutating prior audit state. See class docstring for rationale.
    """
    table = ddb.Table(TABLE_DSAR_AUDIT)
    event_timestamp = _now_iso()
    item = {
        "dsar_id": dsar_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type,
        "status": status,
        "details": json.dumps(payload, default=str),
        # H4 (PR1 fix-now-4 / 🟡 N-2): ByCreatedAt GSI hash key. Format =
        # ISO YYYY-MM (event_timestamp[:7]). Enables future counsel-determined
        # purge to Query a year-month partition instead of full-table Scan.
        # See docs/roadmap/PII-Project/audit-table-retention-runbook.md §3.
        "created_at_partition": event_timestamp[:7],
    }
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(dsar_id) AND attribute_not_exists(event_timestamp)",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            logger.error(
                "audit_event_collision: dsar_id=%s event_type=%s event_timestamp=%s "
                "— idempotency invariant violated (caller replayed?)",
                dsar_id, event_type, event_timestamp,
            )
            raise AuditCollision(
                f"audit row already exists for dsar_id={dsar_id} "
                f"event_timestamp={event_timestamp}"
            ) from exc
        raise
    return event_timestamp


# ───────────────────────────────────────────────────────────────────────────
# Per-surface walkers
# ───────────────────────────────────────────────────────────────────────────
def _walk_form_submissions(pii_subject_id, tenant_id, request_type, dry_run):
    """Walk picasso-form-submissions-staging for one subject under one tenant.

    Access pattern: tenant-scoped Query (PK=tenant_id) + FilterExpression on
    pii_subject_id. The PiiSubjectIdIndex GSI is forward-referenced in IAM but
    not yet created (Apply-3 deferred per D5). Tenant-scoped Query bounds the
    read to one partition — far cheaper than a full-table Scan, and matches
    the v3 §F12 procedure-mitigated reachability pattern.

    request_type:
      - "access":            return matched rows in `exported_rows`
      - "delete" + dry_run=True:  count only; no DeleteItem calls
      - "delete" + dry_run=False: DeleteItem per matched row

    Coverage gap (explicit, documented in manual_followup): only rows that
    carry the pii_subject_id attribute are matched. Pre-Phase-1 rows
    (submissions before lambda #130, 2026-05-18) lack the attribute. The
    durable fix is Apply-2 backfill (deferred); the interim is a manual
    email-keyed walk on suspected pre-Phase-1 subjects.

    `session_ids` is collected from matched rows for the chained recent-
    messages + (future) conversation-summaries walkers. Empty/None values
    are skipped (mirrors the notification-sends message_ids extraction).
    """
    table = ddb.Table(TABLE_FORM_SUBMISSIONS)

    matched = []
    last_evaluated_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("tenant_id").eq(tenant_id),
            "FilterExpression": Attr("pii_subject_id").eq(pii_subject_id),
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        try:
            resp = table.query(**kwargs)
        except ClientError as exc:
            logger.error(
                "form_submissions_query_failed: tenant=%s subject=%s code=%s",
                tenant_id, pii_subject_id,
                exc.response.get("Error", {}).get("Code"),
            )
            return {"rows_found": 0, "session_ids": [], "error": "query_failed"}
        matched.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    rows_found = len(matched)
    # Schema discipline (CLAUDE.md): use .get() exclusively on optional fields.
    # Inline guard rejects None / empty strings.
    session_ids = [
        sid for sid in (row.get("session_id") for row in matched) if sid
    ]

    if request_type == "access":
        return {
            "rows_found": rows_found,
            "session_ids": session_ids,
            "action": "exported",
            "exported_rows": matched,
        }

    # request_type == "delete"
    if dry_run:
        return {
            "rows_found": rows_found,
            "session_ids": session_ids,
            "action": "dry_run_count",
        }

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    for row in matched:
        # Schema discipline (CLAUDE.md §"Schema Discipline"): the walker MUST
        # tolerate corrupted rows — a missing PK/SK indicates writer drift,
        # not an operator-actionable failure. Log + continue so one bad row
        # never breaks the whole batch.
        row_tenant_id = row.get("tenant_id")
        row_submission_id = row.get("submission_id")
        if row_tenant_id is None or row_submission_id is None:
            skipped_corrupted += 1
            # D1 (PR1 fix-now-4): pii_subject_id REDACTED — opaque PSID is
            # still PII per current classification (D5 G-H). tenant_id + the
            # corrupted PK/SK marker are sufficient for operator triage.
            logger.error(
                "form_submissions_delete_skipped_corrupted: "
                "tenant_id=%s submission_id=%s "
                "— row missing PK/SK; cannot delete safely",
                row_tenant_id, row_submission_id,
            )
            continue
        try:
            table.delete_item(Key={
                "tenant_id": row_tenant_id,
                "submission_id": row_submission_id,
            })
            deleted += 1
        except ClientError as exc:
            # Audit row 8 (code-reviewer SR1): count delete failures so the
            # response can distinguish "matched but not deleted" from
            # "matched and deleted". Without this counter, rows_deleted
            # silently undercounts and the operator believes the delete
            # completed when it didn't.
            delete_failed += 1
            logger.error(
                "form_submissions_delete_failed: submission=%s code=%s",
                row_submission_id,
                exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "session_ids": session_ids,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
    }


def _walk_notification_sends(tenant_id, normalized_email, request_type, dry_run):
    """Walk picasso-notification-sends-staging for direct-to-consumer messages.

    Access pattern: tenant-scoped Query (PK=`TENANT#<tenant_id>`) +
    FilterExpression on `recipient == normalized_email`. Catches notification
    rows where the consumer themselves received the message (email-channel
    auto-replies, confirmations, etc.).

    Does NOT walk by `submission_id` (which would catch staff-recipient
    notifications about the consumer's submission). Staff-recipient rows are
    operator/staff PII under a different controller relationship (D5 G-H +
    F9 + Step 10 v3 §F9 three-part mitigation). The walker flags this gap in
    the dispatcher's manual_followup with an operator-actionable CLI snippet
    so the operator has a clear next step when staff-side inspection is
    required.

    Returns:
      {
        "rows_found":    int,
        "rows_matched":  [<full row>, ...],     # for chained event walk
        "message_ids":   [<message_id>, ...],   # for chained event walk
        "action":        "exported" | "dry_run_count" | "deleted" | "error",
        "exported_rows": [<row>, ...]            # access only
        "rows_deleted":  int                     # delete-real only
        "rows_skipped_corrupted": int            # delete-real only
        "error":         <code>                  # error only
      }

    Today's expected outcome: 0 rows in most cases — the MFS email path
    writes notifications addressed to operator/staff `recipients` (config-
    driven); the consumer's email rarely appears as recipient. Returning 0
    rows here is a true negative for the consumer-direct delete scope,
    NOT a coverage failure. The follow-up makes the scope explicit.

    Normalization symmetry (D5 row F-DSAR3, 2026-05-21 advisor audit):
    Writers (`form_handler.py:802`, `SMS_Sender/index.mjs:127`) store
    `recipient` verbatim without lowercasing or stripping. The walker
    therefore CANNOT use a DynamoDB FilterExpression on `recipient` —
    `Attr.eq` is case-sensitive and would miss `Person@Example.COM`
    when the operator submits `person@example.com`. Instead, the walker
    queries by tenant PK only and applies a case-insensitive Python
    post-filter using the same `.strip().lower()` normalization that
    produced `normalized_email`. RCU cost is unchanged (DynamoDB
    FilterExpression evaluates after RCU consumption); the only added
    cost is bandwidth + Lambda Python compare time, which is negligible
    at tenant scale (~hundreds to low-thousands of rows). Durable fix is
    writer-side normalization — tracked in F-DSAR3.
    """
    table = ddb.Table(TABLE_NOTIFICATION_SENDS)
    pk_value = f"TENANT#{tenant_id}"

    all_tenant_rows = []
    last_evaluated_key = None
    while True:
        kwargs = {"KeyConditionExpression": Key("pk").eq(pk_value)}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        try:
            resp = table.query(**kwargs)
        except ClientError as exc:
            logger.error(
                "notification_sends_query_failed: tenant=%s code=%s",
                tenant_id, exc.response.get("Error", {}).get("Code"),
            )
            return {
                "rows_found": 0,
                "rows_matched": [],
                "message_ids": [],
                "error": "query_failed",
            }
        all_tenant_rows.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    # Case-insensitive Python post-filter on `recipient` (see docstring +
    # F-DSAR3 for why this is in Python and not in FilterExpression).
    matched = [
        row for row in all_tenant_rows
        if isinstance(row.get("recipient"), str)
        and row["recipient"].strip().lower() == normalized_email
    ]
    rows_found = len(matched)
    # message_ids feed _walk_notification_events. Some send rows have empty
    # message_id (failed-send rows record `message_id: ''`); skip those for
    # the chained event lookup. Schema discipline: .get() only.
    message_ids = [
        mid for mid in (row.get("message_id") for row in matched) if mid
    ]

    if request_type == "access":
        return {
            "rows_found": rows_found,
            "rows_matched": matched,
            "message_ids": message_ids,
            "action": "exported",
            "exported_rows": matched,
        }

    # request_type == "delete"
    if dry_run:
        return {
            "rows_found": rows_found,
            "rows_matched": matched,
            "message_ids": message_ids,
            "action": "dry_run_count",
        }

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    for row in matched:
        row_pk = row.get("pk")
        row_sk = row.get("sk")
        if row_pk is None or row_sk is None:
            skipped_corrupted += 1
            # D1 (PR1 fix-now-4): recipient REDACTED — direct email PII.
            # pk/sk markers are sufficient for operator triage.
            logger.error(
                "notification_sends_delete_skipped_corrupted: "
                "pk=%s sk=%s — row missing PK/SK",
                row_pk, row_sk,
            )
            continue
        try:
            table.delete_item(Key={"pk": row_pk, "sk": row_sk})
            deleted += 1
        except ClientError as exc:
            # Audit row 8 (code-reviewer SR1): count delete failures.
            delete_failed += 1
            logger.error(
                "notification_sends_delete_failed: sk=%s code=%s",
                row_sk,
                exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "rows_matched": matched,
        "message_ids": message_ids,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
    }


def _walk_notification_events(message_ids, request_type, dry_run):
    """Walk picasso-notification-events-staging via ByMessageId GSI.

    Chained walker: input is `message_ids` produced by
    `_walk_notification_sends`. For each message_id, Query the ByMessageId
    GSI (hash=message_id, range=event_type_timestamp). GSI projection is ALL,
    so each returned item carries the base table's PK/SK for DeleteItem.

    Access pattern: NOT tenant-scoped. The GSI is keyed on message_id only,
    which is acceptable because message_id has cryptographic uniqueness
    (SES UUIDs, Telnyx message IDs) — different tenants cannot collide.

    Returns:
      {
        "rows_found":             int,
        "action":                 "exported" | "dry_run_count" | "deleted" | "error" | "no_messages",
        "exported_rows":          [<event row>, ...]   # access only
        "rows_deleted":           int                  # delete-real only
        "rows_skipped_corrupted": int                  # delete-real only
        "error":                  <code>               # error only
        "failed_message_ids":     [<message_id>, ...]  # set when partial errors
        "truncated_message_id_count": int              # set when cap hit
      }

    If `message_ids` is empty, returns `{"action": "no_messages",
    "rows_found": 0}` immediately — no GSI queries are issued. This is the
    common case when the consumer has no direct-recipient notifications.

    Bounded fan-out (Security advisor Finding 3, 2026-05-21):
    `len(message_ids) > MAX_MESSAGE_IDS_PER_INVOCATION` is truncated to the
    cap; the overflow count is returned so the dispatcher can surface a
    manual_followup. This prevents a high-volume subject from exhausting
    the 60s Lambda timeout before the `closed` audit row is written.

    Continue-on-error (Security advisor Finding 2, 2026-05-21): per-
    message_id GSI failures are logged + recorded in `failed_message_ids`;
    the walker continues to the next message_id rather than short-circuiting.
    Operator gets a complete picture of which IDs succeeded vs failed
    instead of a binary "query_failed" with hidden progress.

    GSI eventual consistency note: DynamoDB GSI reads are eventually
    consistent by default. A row deleted on the base table during or just
    before a GSI query window may still appear in the GSI's response. The
    walker's subsequent `DeleteItem` is idempotent (DDB silently succeeds
    on non-existent items), so `rows_deleted` may overcount by the phantom
    window. Operationally harmless; not a correctness risk.
    """
    if not message_ids:
        return {"rows_found": 0, "action": "no_messages"}

    # Bounded fan-out per Security advisor Finding 3.
    truncated_message_id_count = 0
    if len(message_ids) > MAX_MESSAGE_IDS_PER_INVOCATION:
        truncated_message_id_count = len(message_ids) - MAX_MESSAGE_IDS_PER_INVOCATION
        message_ids = message_ids[:MAX_MESSAGE_IDS_PER_INVOCATION]
        logger.warning(
            "notification_events_message_ids_truncated: cap=%d overflow=%d",
            MAX_MESSAGE_IDS_PER_INVOCATION, truncated_message_id_count,
        )

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
                logger.error(
                    "notification_events_query_failed: message_id=%s code=%s",
                    message_id, exc.response.get("Error", {}).get("Code"),
                )
                # Continue-on-error per Security Finding 2: record this
                # message_id as failed and proceed to the next; do not
                # short-circuit the whole walk.
                failed_message_ids.append(message_id)
                break
            matched.extend(resp.get("Items", []))
            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    rows_found = len(matched)

    # Build a base result dict with the progress-tracking fields always
    # included so the dispatcher can emit accurate followups regardless of
    # request_type or outcome.
    progress_fields = {}
    if failed_message_ids:
        progress_fields["failed_message_ids"] = failed_message_ids
    if truncated_message_id_count:
        progress_fields["truncated_message_id_count"] = truncated_message_id_count

    if request_type == "access":
        return {
            "rows_found": rows_found,
            "action": "exported",
            "exported_rows": matched,
            **progress_fields,
        }

    if dry_run:
        return {
            "rows_found": rows_found,
            "action": "dry_run_count",
            **progress_fields,
        }

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    for row in matched:
        row_pk = row.get("pk")
        row_sk = row.get("sk")
        if row_pk is None or row_sk is None:
            skipped_corrupted += 1
            logger.error(
                "notification_events_delete_skipped_corrupted: "
                "pk=%s sk=%s message_id=%s — row missing PK/SK",
                row_pk, row_sk, row.get("message_id"),
            )
            continue
        try:
            table.delete_item(Key={"pk": row_pk, "sk": row_sk})
            deleted += 1
        except ClientError as exc:
            # Audit row 8 (code-reviewer SR1): count delete failures.
            delete_failed += 1
            logger.error(
                "notification_events_delete_failed: sk=%s code=%s",
                row_sk,
                exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
        **progress_fields,
    }


def _project_recent_messages_row(row):
    """Project a recent-messages row to the minimum subject-meaningful fields.

    Article 15 / data minimization (pii-lifecycle advisor 2026-05-21):
    `staging-recent-messages` rows include internal join keys (`sessionId`,
    `messageId`) and lifecycle metadata (`expires_at`) that the subject
    neither created nor has any operational use for. Returning the full row
    would over-disclose internal identifiers and could enable session
    correlation against other rows in the export. This projection limits
    the export to fields the subject can reason about — what they said
    (`content`), who said it (`role`), and when (`messageTimestamp`).

    Divergent from form-submissions and notification-sends walkers (which
    return full rows). The divergence is justified by `content`'s free-text
    nature — those other surfaces are field-constrained.
    """
    return {
        "role": row.get("role"),
        "content": row.get("content"),
        "messageTimestamp": row.get("messageTimestamp"),
    }


def _walk_recent_messages(tenant_id, session_ids, request_type, dry_run):
    """Walk staging-recent-messages for one subject via chained session_ids.

    Chained walker: input is `session_ids` produced by `_walk_form_submissions`.
    The recent-messages table has no subject-linking attribute (no email, no
    pii_subject_id, no userId) — the only available linkage is via
    form-submissions `session_id`. For each session_id, Query(PK=sessionId).

    `tenant_id` is unused by the query (the table has no tenantId on row)
    but is a required argument for defense-in-depth — assert non-empty on
    entry so a future caller drift cannot silently span tenants. The
    upstream form-submissions walker IS tenant-keyed; this argument keeps
    the contract explicit.

    Coverage gaps (flagged in dispatcher manual_followups):
      - Chat-only subjects (never submitted a form) are NOT in
        form-submissions, so their session_ids are unknown to this walker.
        Compensating control = 24h TTL on the table; structural fix tracked
        in D5 F-DSAR4.
      - Pre-Phase-1 form submissions lack pii_subject_id (F-DSAR1), so
        their session_ids are also unreachable through the upstream
        walker — gap inherits.

    request_type:
      - "access":            return projected rows (role/content/timestamp)
      - "delete" + dry_run:  count only; no DeleteItem calls
      - "delete" + real:     DeleteItem per (sessionId, messageTimestamp)

    Returns:
      {
        "rows_found":                int,
        "action":                    "exported" | "dry_run_count" | "deleted" | "no_sessions",
        "exported_rows":             [<projected>, ...]   # access only
        "rows_deleted":              int                  # delete-real only
        "rows_skipped_corrupted":    int                  # delete-real only
        "failed_session_ids":        [<sid>, ...]         # set on partial errors
        "truncated_session_id_count":int                  # set when cap hit
        "exported_messages_truncated_count": int          # set when soft cap hit
      }

    If `session_ids` is empty, returns `{"action": "no_sessions",
    "rows_found": 0}` immediately — no Query calls are issued.

    Bounded fan-out: `len(session_ids) > MAX_SESSION_IDS_PER_INVOCATION` is
    truncated to the cap; overflow surfaces as a manual_followup with the
    excess count.

    Continue-on-error: per-session_id Query failures are logged + recorded
    in `failed_session_ids`; the walker continues to the next session_id.
    Mirrors the notification-events fix-now-2 pattern.

    Logging discipline: per-PII (audit log leakage) — the walker NEVER logs
    `content`. On errors, logs `sessionId` + `messageTimestamp` only.

    Exported-messages soft cap: at request_type=access, the projected
    output is capped at MAX_EXPORTED_MESSAGES to keep the Lambda response
    payload under 6 MB. Overflow surfaces in
    `exported_messages_truncated_count`.
    """
    # Defense-in-depth: tenant_id MUST be non-empty even though the walker
    # doesn't use it. Upstream form-submissions walker enforces tenant
    # scoping; if a future caller bypasses that and calls this walker
    # directly, fail loud.
    if not tenant_id:
        raise ValueError(
            "_walk_recent_messages requires non-empty tenant_id "
            "(defense-in-depth — table has no tenantId; upstream walker "
            "must enforce tenant scoping)"
        )

    if not session_ids:
        return {"rows_found": 0, "action": "no_sessions"}

    # Bounded fan-out.
    truncated_session_id_count = 0
    if len(session_ids) > MAX_SESSION_IDS_PER_INVOCATION:
        truncated_session_id_count = len(session_ids) - MAX_SESSION_IDS_PER_INVOCATION
        session_ids = session_ids[:MAX_SESSION_IDS_PER_INVOCATION]
        logger.warning(
            "recent_messages_session_ids_truncated: cap=%d overflow=%d",
            MAX_SESSION_IDS_PER_INVOCATION, truncated_session_id_count,
        )

    table = ddb.Table(TABLE_RECENT_MESSAGES)

    matched = []
    failed_session_ids = []
    for session_id in session_ids:
        last_evaluated_key = None
        while True:
            kwargs = {
                "KeyConditionExpression": Key("sessionId").eq(session_id),
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key
            try:
                resp = table.query(**kwargs)
            except ClientError as exc:
                logger.error(
                    "recent_messages_query_failed: sessionId=%s code=%s",
                    session_id, exc.response.get("Error", {}).get("Code"),
                )
                # Continue-on-error: record this session_id as failed and
                # proceed to the next; do not short-circuit the whole walk.
                # NB: NEVER log `content` from the row.
                failed_session_ids.append(session_id)
                break
            matched.extend(resp.get("Items", []))
            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    rows_found = len(matched)

    progress_fields = {}
    if failed_session_ids:
        progress_fields["failed_session_ids"] = failed_session_ids
    if truncated_session_id_count:
        progress_fields["truncated_session_id_count"] = truncated_session_id_count

    if request_type == "access":
        # Soft cap on exported messages to stay under Lambda 6 MB response.
        exported_messages_truncated_count = 0
        export_rows = matched
        if len(export_rows) > MAX_EXPORTED_MESSAGES:
            exported_messages_truncated_count = len(export_rows) - MAX_EXPORTED_MESSAGES
            export_rows = export_rows[:MAX_EXPORTED_MESSAGES]
            logger.warning(
                "recent_messages_exported_truncated: cap=%d overflow=%d",
                MAX_EXPORTED_MESSAGES, exported_messages_truncated_count,
            )
        projected = [_project_recent_messages_row(r) for r in export_rows]
        result = {
            "rows_found": rows_found,
            "action": "exported",
            "exported_rows": projected,
            **progress_fields,
        }
        if exported_messages_truncated_count:
            result["exported_messages_truncated_count"] = exported_messages_truncated_count
        return result

    if dry_run:
        return {
            "rows_found": rows_found,
            "action": "dry_run_count",
            **progress_fields,
        }

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    for row in matched:
        row_session_id = row.get("sessionId")
        row_timestamp = row.get("messageTimestamp")
        if row_session_id is None or row_timestamp is None:
            skipped_corrupted += 1
            # NB: do NOT log `content`. Log only the key fields we already
            # know are present (or None).
            logger.error(
                "recent_messages_delete_skipped_corrupted: "
                "sessionId=%s messageTimestamp=%s — row missing PK/SK",
                row_session_id, row_timestamp,
            )
            continue
        try:
            table.delete_item(Key={
                "sessionId": row_session_id,
                "messageTimestamp": row_timestamp,
            })
            deleted += 1
        except ClientError as exc:
            # Audit row 8 (code-reviewer SR1): count delete failures.
            delete_failed += 1
            logger.error(
                "recent_messages_delete_failed: sessionId=%s messageTimestamp=%s code=%s",
                row_session_id, row_timestamp,
                exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
        **progress_fields,
    }


def _recent_messages_chat_only_cli_snippet(tenant_id):
    """Operator-actionable CLI snippet for the chat-only F-DSAR4 gap.

    Used in the manual_followup block to give the operator a concrete next
    step when the walker reaches zero session_ids (or the subject is
    chat-only / pre-Phase-1). The walker cannot enumerate sessions for a
    subject who never submitted a form — but the operator may have a
    session_id from out-of-band sources (support transcript, browser
    history, etc.) and can query directly.

    Audit row 11 (Security SR2): `<SUBJECT_EMAIL>` placeholder substituted
    for the operator's normalized email — the operator fills it in at run
    time from the DSAR ledger. Prevents consumer email from leaking into
    operator-side response storage (CLI snippets get pasted into tickets,
    log files, etc.).
    """
    return (
        f"  # Operator-known session_id direct query (preferred):\n"
        f"  aws dynamodb query --table-name staging-recent-messages \\\n"
        f"    --profile myrecruiter-staging \\\n"
        f"    --key-condition-expression 'sessionId = :s' \\\n"
        f"    --expression-attribute-values "
        f"'{{\":s\":{{\"S\":\"<SESSION_ID-from-out-of-band-source>\"}}}}'\n"
        f"  # Last-resort content-substring scan (CASE-SENSITIVE; false positives likely):\n"
        f"  aws dynamodb scan --table-name staging-recent-messages \\\n"
        f"    --profile myrecruiter-staging \\\n"
        f"    --filter-expression 'contains(#c, :e)' \\\n"
        f"    --expression-attribute-names '{{\"#c\":\"content\"}}' \\\n"
        f"    --expression-attribute-values "
        f"'{{\":e\":{{\"S\":\"<SUBJECT_EMAIL>\"}}}}'"
    )


def _staff_notification_cli_snippet(tenant_id):
    """Operator-actionable CLI snippet for staff-recipient notification inspection.

    Audit row 11: tenant_id kept (not consumer PII); submission_id is also
    a placeholder. Pattern matches the other snippets — operator fills in
    placeholders at run time from the DSAR ledger.
    """
    return (
        f"  aws dynamodb query --table-name picasso-notification-sends-staging \\\n"
        f"    --profile myrecruiter-staging \\\n"
        f"    --key-condition-expression 'pk = :pk' \\\n"
        f"    --filter-expression 'submission_id = :sid' \\\n"
        f"    --expression-attribute-values "
        f"'{{\":pk\":{{\"S\":\"TENANT#{tenant_id}\"}},\":sid\":{{\"S\":\"<SUBMISSION_ID-from-form-submissions-walker>\"}}}}'"
    )


def _pre_phase1_cli_snippet(tenant_id):
    """Operator-actionable CLI snippet for manual email-keyed Scan.

    Used when the pii-subject-index does not yield a hit — likely cause is a
    pre-Phase-1 row (submitted before lambda #130 wrote pii_subject_id on
    2026-05-18).

    Audit row 11 (Security SR2): `<SUBJECT_EMAIL>` placeholder substituted
    for the operator's normalized email. The operator fills it in at run
    time from the DSAR ledger; prevents PII leak into operator response
    storage.
    """
    return (
        f"  aws dynamodb scan --table-name picasso-form-submissions-staging \\\n"
        f"    --profile myrecruiter-staging \\\n"
        f"    --filter-expression 'submitter_email = :e AND tenant_id = :t' \\\n"
        f"    --expression-attribute-values "
        f"'{{\":e\":{{\"S\":\"<SUBJECT_EMAIL>\"}},\":t\":{{\"S\":\"{tenant_id}\"}}}}'"
    )


def _walk_mfs_surfaces(pii_subject_id, tenant_id, normalized_email, request_type, dry_run):
    """Dispatch per-surface walkers.

    Returns (rows_touched: Dict[str,int], manual_followups: List[str],
             exported_rows: Dict[str, List[dict]], walker_results: Dict[str, dict]).

    `walker_results[surface]` is one of:
      - {"status": "completed", "action": ..., "rows_touched": int, ...}
      - {"status": "errored", "error": str, "rows_touched": int}
      - {"status": "deferred", "reason": str}
      - {"status": "skipped_no_subject"}

    The handler consumes `walker_results` to (a) emit per-surface audit events
    and (b) compute close-event status (completed / partial / partial_error).

    form-submissions has a real walker (`_walk_form_submissions`); the surfaces
    in MFS_SCOPED_SURFACES still return `deferred` until their schemas are
    verified against MFS writer code.
    """
    rows_touched = {
        "form-submissions": 0,
        "notification-sends": 0,
        "notification-events": 0,
        "recent-messages": 0,
    }
    rows_touched.update({s: 0 for s in MFS_SCOPED_SURFACES})
    manual_followups = []
    exported_rows = {}
    walker_results = {}

    if pii_subject_id is None:
        # Stronger followup (audit fix-now #3): include a copy-pasteable CLI
        # snippet for the manual email-keyed walk so the operator has a
        # zero-templating fallback path. Pre-Phase-1 rows are the dominant
        # cause of subject-not-found and the prior soft message offered no
        # concrete next step.
        cli = _pre_phase1_cli_snippet(tenant_id)
        manual_followups.append(
            f"Subject {normalized_email!r} not found in pii-subject-index "
            f"(tenant {tenant_id!r}). Possible reasons: (a) subject has no "
            f"recorded interactions; (b) interactions are pre-Phase-1 "
            f"(before 2026-05-18) so the writer never created the index "
            f"entry. For (b), run a manual email-keyed Scan on each MFS "
            f"surface, starting with form-submissions:\n"
            f"{cli}\n"
            f"Repeat per surface; row-by-row DeleteItem if matches found."
        )
        # No walker can run without a pii_subject_id; record skipped + deferred.
        # Note: notification_sends + notification_events walkers depend on
        # `normalized_email` (recipient match) not `pii_subject_id`, but the
        # broader DSAR contract treats subject-resolution failure as a halt
        # signal — we don't process any surface for an unresolved subject so
        # the operator confronts the resolution failure before any action.
        manual_followups.append(
            "form-submissions: skipped (no pii_subject_id resolved)"
        )
        walker_results["form-submissions"] = {"status": "skipped_no_subject"}
        manual_followups.append(
            "notification-sends: skipped (no pii_subject_id resolved — "
            "DSAR halts on subject-resolution failure)"
        )
        walker_results["notification-sends"] = {"status": "skipped_no_subject"}
        manual_followups.append(
            "notification-events: skipped (chained walker requires "
            "notification-sends to run first)"
        )
        walker_results["notification-events"] = {"status": "skipped_no_subject"}
        manual_followups.append(
            "recent-messages: skipped (chained walker requires "
            "form-submissions to run first)"
        )
        walker_results["recent-messages"] = {"status": "skipped_no_subject"}
        for surface, reason in MFS_SCOPED_SURFACES.items():
            manual_followups.append(f"{surface}: {reason}")
            walker_results[surface] = {"status": "deferred", "reason": reason}
        return rows_touched, manual_followups, exported_rows, walker_results

    # form-submissions: real walker.
    fs = _walk_form_submissions(pii_subject_id, tenant_id, request_type, dry_run)
    rows_touched["form-submissions"] = fs["rows_found"]
    if fs.get("error"):
        manual_followups.append(
            f"form-submissions: query failed ({fs['error']}); retry advised"
        )
        walker_results["form-submissions"] = {
            "status": "errored",
            "error": fs["error"],
            "rows_touched": fs["rows_found"],
        }
    elif fs.get("action") == "exported":
        exported_rows["form-submissions"] = fs["exported_rows"]
        walker_results["form-submissions"] = {
            "status": "completed",
            "action": "exported",
            "rows_touched": fs["rows_found"],
        }
    elif fs.get("action") == "dry_run_count":
        manual_followups.append(
            f"form-submissions: dry_run=true; {fs['rows_found']} row(s) would "
            f"be deleted; re-invoke with dry_run=false to delete"
        )
        walker_results["form-submissions"] = {
            "status": "completed",
            "action": "dry_run_count",
            "rows_touched": fs["rows_found"],
        }
    else:  # action == "deleted"
        result = {
            "status": "completed",
            "action": "deleted",
            "rows_touched": fs["rows_found"],
            "rows_deleted": fs.get("rows_deleted", 0),
            "rows_skipped_corrupted": fs.get("rows_skipped_corrupted", 0),
        }
        if fs.get("rows_skipped_corrupted", 0) > 0:
            manual_followups.append(
                f"form-submissions: {fs['rows_skipped_corrupted']} row(s) "
                f"skipped due to corrupted PK/SK schema — see CloudWatch logs "
                f"for tenant_id+submission_id values; manual inspection required."
            )
            # Treat corrupted-row skip as errored — operator needs to know
            # the delete batch was not exhaustive.
            result["status"] = "errored"
            result["error"] = "rows_skipped_corrupted"
        walker_results["form-submissions"] = result

    # Coverage gap noted whenever the walker ran (even with 0 rows): operator
    # may need to manually walk pre-Phase-1 rows that lack pii_subject_id.
    manual_followups.append(
        "form-submissions: walker filters by pii_subject_id (Phase-1 attribute "
        "from lambda #130). Submissions written before 2026-05-18 do not carry "
        "this attribute; durable fix = Apply-2 backfill (deferred); interim = "
        "manual email-keyed walk if a pre-Phase-1 subject is suspected:\n"
        f"{_pre_phase1_cli_snippet(tenant_id)}"
    )

    # notification-sends: tenant-Query + FilterExpression on
    # recipient==normalized_email. Catches direct-to-consumer notifications.
    # Captured message_ids feed the chained notification-events walker.
    ns = _walk_notification_sends(
        tenant_id, normalized_email, request_type, dry_run,
    )
    rows_touched["notification-sends"] = ns["rows_found"]
    captured_message_ids = ns.get("message_ids", [])
    if ns.get("error"):
        manual_followups.append(
            f"notification-sends: query failed ({ns['error']}); retry advised"
        )
        walker_results["notification-sends"] = {
            "status": "errored",
            "error": ns["error"],
            "rows_touched": ns["rows_found"],
        }
    elif ns.get("action") == "exported":
        exported_rows["notification-sends"] = ns["exported_rows"]
        walker_results["notification-sends"] = {
            "status": "completed",
            "action": "exported",
            "rows_touched": ns["rows_found"],
        }
    elif ns.get("action") == "dry_run_count":
        manual_followups.append(
            f"notification-sends: dry_run=true; {ns['rows_found']} direct-to-"
            f"consumer row(s) would be deleted; re-invoke with dry_run=false "
            f"to delete (notification-events chained walk will follow)"
        )
        walker_results["notification-sends"] = {
            "status": "completed",
            "action": "dry_run_count",
            "rows_touched": ns["rows_found"],
        }
    else:  # action == "deleted"
        ns_result = {
            "status": "completed",
            "action": "deleted",
            "rows_touched": ns["rows_found"],
            "rows_deleted": ns.get("rows_deleted", 0),
            "rows_skipped_corrupted": ns.get("rows_skipped_corrupted", 0),
        }
        if ns.get("rows_skipped_corrupted", 0) > 0:
            manual_followups.append(
                f"notification-sends: {ns['rows_skipped_corrupted']} row(s) "
                f"skipped due to corrupted PK/SK schema — see CloudWatch logs "
                f"for pk+sk+recipient; manual inspection required."
            )
            ns_result["status"] = "errored"
            ns_result["error"] = "rows_skipped_corrupted"
        walker_results["notification-sends"] = ns_result

    # Always note the scope-limit: walker catches consumer-recipient
    # notifications only. Staff-recipient rows about this consumer's submission
    # are operator/staff PII (different controller relationship per D5 G-H +
    # F9) and require manual operator inspection.
    manual_followups.append(
        "notification-sends: walker matches `recipient == normalized_email` "
        "(direct consumer messages only). Staff-recipient rows where staff "
        "were notified ABOUT this consumer's submission are operator PII "
        "under a different controller relationship (D5 G-H + F9) and are "
        "NOT auto-deleted. For each submission_id from the form-submissions "
        "walker, the operator may inspect with:\n"
        f"{_staff_notification_cli_snippet(tenant_id)}"
    )

    # notification-events: chained walk via captured message_ids. If
    # notification-sends produced zero message_ids, returns no_messages
    # (the common case today). Errors only on real GSI failure.
    ne = _walk_notification_events(
        captured_message_ids, request_type, dry_run,
    )
    rows_touched["notification-events"] = ne["rows_found"]
    if ne.get("error"):
        manual_followups.append(
            f"notification-events: query failed ({ne['error']}); retry advised"
        )
        walker_results["notification-events"] = {
            "status": "errored",
            "error": ne["error"],
            "rows_touched": ne["rows_found"],
        }
    elif ne.get("action") == "no_messages":
        # Not an error — notification-sends had no consumer-recipient rows
        # so there's nothing to chain into. Record as completed with 0.
        walker_results["notification-events"] = {
            "status": "completed",
            "action": "no_messages_to_walk",
            "rows_touched": 0,
        }
    elif ne.get("action") == "exported":
        exported_rows["notification-events"] = ne["exported_rows"]
        walker_results["notification-events"] = {
            "status": "completed",
            "action": "exported",
            "rows_touched": ne["rows_found"],
        }
    elif ne.get("action") == "dry_run_count":
        manual_followups.append(
            f"notification-events: dry_run=true; {ne['rows_found']} event "
            f"row(s) would be deleted (chained from notification-sends "
            f"message_ids); re-invoke with dry_run=false to delete"
        )
        walker_results["notification-events"] = {
            "status": "completed",
            "action": "dry_run_count",
            "rows_touched": ne["rows_found"],
        }
    else:  # action == "deleted"
        ne_result = {
            "status": "completed",
            "action": "deleted",
            "rows_touched": ne["rows_found"],
            "rows_deleted": ne.get("rows_deleted", 0),
            "rows_skipped_corrupted": ne.get("rows_skipped_corrupted", 0),
        }
        if ne.get("rows_skipped_corrupted", 0) > 0:
            manual_followups.append(
                f"notification-events: {ne['rows_skipped_corrupted']} row(s) "
                f"skipped due to corrupted PK/SK schema — see CloudWatch logs "
                f"for pk+sk+message_id; manual inspection required."
            )
            ne_result["status"] = "errored"
            ne_result["error"] = "rows_skipped_corrupted"
        walker_results["notification-events"] = ne_result

    # Surface advisor-audit fix-now items #2 + #4: per-message_id failures
    # and message_id cap overflow. Both apply across any walker action; emit
    # followups + taint walker status to errored when failures occurred so
    # close_status reflects partial_error per audit fix-now #5 semantics.
    failed_ids = ne.get("failed_message_ids", []) if ne else []
    truncated_count = ne.get("truncated_message_id_count", 0) if ne else 0
    if failed_ids:
        manual_followups.append(
            f"notification-events: {len(failed_ids)} message_id(s) failed "
            f"per-id GSI query and were skipped (walker continued on remaining "
            f"IDs). Failed message_ids: {failed_ids[:5]}"
            f"{'...' if len(failed_ids) > 5 else ''}. "
            f"Re-invoke with the same dsar_id (new event_timestamp) to retry, "
            f"or inspect CloudWatch logs for the specific GSI error codes."
        )
        existing = walker_results.get("notification-events", {})
        walker_results["notification-events"] = {
            **existing,
            "status": "errored",
            "error": "partial_query_failures",
            "failed_message_ids_count": len(failed_ids),
        }
    if truncated_count:
        manual_followups.append(
            f"notification-events: chained walker capped at "
            f"{MAX_MESSAGE_IDS_PER_INVOCATION} message_ids per invocation "
            f"(Lambda timeout protection). {truncated_count} message_id(s) "
            f"were NOT walked. Re-invoke with a narrower scope or wait for "
            f"item 6's integration-test batching."
        )
        # Security advisor fix-now-3 (2026-05-21): taint unconditionally —
        # truncation is always an error regardless of whether
        # partial_query_failures was already set on this walker. Previous
        # status-gated taint silently dropped `truncated_count` from
        # walker_results when failed_message_ids fired first.
        existing = walker_results.get("notification-events", {})
        walker_results["notification-events"] = {
            **existing,
            "status": "errored",
            "error": existing.get("error") or "message_ids_truncated",
            "truncated_count": truncated_count,
        }

    # recent-messages: chained walk via form-submissions session_ids.
    # Defense-in-depth: if form-submissions errored, the session_ids list
    # is empty and the walker correctly short-circuits to no_sessions.
    captured_session_ids = fs.get("session_ids", []) if fs else []
    rm = _walk_recent_messages(
        tenant_id, captured_session_ids, request_type, dry_run,
    )
    rows_touched["recent-messages"] = rm["rows_found"]
    if rm.get("action") == "no_sessions":
        # Not an error — form-submissions had no rows (or no rows carried
        # session_id) so there's nothing to chain into. Record as completed.
        walker_results["recent-messages"] = {
            "status": "completed",
            "action": "no_sessions_to_walk",
            "rows_touched": 0,
        }
    elif rm.get("action") == "exported":
        exported_rows["recent-messages"] = rm["exported_rows"]
        walker_results["recent-messages"] = {
            "status": "completed",
            "action": "exported",
            "rows_touched": rm["rows_found"],
        }
    elif rm.get("action") == "dry_run_count":
        manual_followups.append(
            f"recent-messages: dry_run=true; {rm['rows_found']} message "
            f"row(s) would be deleted (chained from form-submissions "
            f"session_ids); re-invoke with dry_run=false to delete"
        )
        walker_results["recent-messages"] = {
            "status": "completed",
            "action": "dry_run_count",
            "rows_touched": rm["rows_found"],
        }
    else:  # action == "deleted"
        rm_result = {
            "status": "completed",
            "action": "deleted",
            "rows_touched": rm["rows_found"],
            "rows_deleted": rm.get("rows_deleted", 0),
            "rows_skipped_corrupted": rm.get("rows_skipped_corrupted", 0),
        }
        if rm.get("rows_skipped_corrupted", 0) > 0:
            manual_followups.append(
                f"recent-messages: {rm['rows_skipped_corrupted']} row(s) "
                f"skipped due to corrupted PK/SK schema — see CloudWatch logs "
                f"for sessionId+messageTimestamp; manual inspection required."
            )
            rm_result["status"] = "errored"
            rm_result["error"] = "rows_skipped_corrupted"
        walker_results["recent-messages"] = rm_result

    # F-DSAR4 chat-only gap + F-DSAR1 inheritance + TTL note + Article 15
    # third-party-disclosure caveat. Surfaced regardless of outcome so the
    # operator is reminded of the structural limits each time.
    failed_session_ids = rm.get("failed_session_ids", []) if rm else []
    truncated_session_count = rm.get("truncated_session_id_count", 0) if rm else 0
    exported_truncated = rm.get("exported_messages_truncated_count", 0) if rm else 0
    if failed_session_ids:
        manual_followups.append(
            f"recent-messages: {len(failed_session_ids)} session_id(s) "
            f"failed Query and were skipped (walker continued on remaining "
            f"sessions). Failed session_ids: {failed_session_ids[:5]}"
            f"{'...' if len(failed_session_ids) > 5 else ''}. "
            f"Re-invoke with the same dsar_id (new event_timestamp) to retry, "
            f"or inspect CloudWatch logs for the specific error codes."
        )
        existing = walker_results.get("recent-messages", {})
        walker_results["recent-messages"] = {
            **existing,
            "status": "errored",
            "error": "partial_query_failures",
            "failed_session_ids_count": len(failed_session_ids),
        }
    if truncated_session_count:
        manual_followups.append(
            f"recent-messages: chained walker capped at "
            f"{MAX_SESSION_IDS_PER_INVOCATION} session_ids per invocation "
            f"(Lambda timeout protection). {truncated_session_count} "
            f"session_id(s) were NOT walked. Re-invoke with a narrower scope."
        )
        # Security advisor fix-now-3 (2026-05-21): taint unconditionally
        # (see notification-events comment above for rationale). Preserve
        # the original `error` if one was already set so the operator's
        # walker_results carries the FIRST failure mode + truncation count.
        existing = walker_results.get("recent-messages", {})
        walker_results["recent-messages"] = {
            **existing,
            "status": "errored",
            "error": existing.get("error") or "session_ids_truncated",
            "truncated_count": truncated_session_count,
        }
    if exported_truncated:
        manual_followups.append(
            f"recent-messages: exported message payload capped at "
            f"{MAX_EXPORTED_MESSAGES} rows (Lambda 6 MB response limit). "
            f"{exported_truncated} additional row(s) were truncated from "
            f"the export. Re-invoke with a narrower scope to retrieve."
        )
        # Security advisor fix-now-3: taint unconditionally; preserve
        # original error code if set. Reuse `truncated_count` key for
        # whichever truncation fired first (callsite is mutually exclusive
        # with session_ids_truncated in practice — exported truncation
        # only fires on access path; session-id truncation fires on all
        # paths — but the unconditional merge is defensive).
        existing = walker_results.get("recent-messages", {})
        walker_results["recent-messages"] = {
            **existing,
            "status": "errored",
            "error": existing.get("error") or "exported_messages_truncated",
            "exported_messages_truncated_count": exported_truncated,
        }
    # F-DSAR4 — chat-only-no-form coverage gap. Surfaced any time the
    # walker ran (incl. no_sessions outcomes) so the operator never assumes
    # zero rows means clean. Also surfaces F-DSAR1 inheritance per advisor
    # fix-now-3 (pii-data-lifecycle 2026-05-21): pre-Phase-1 form rows
    # lack pii_subject_id, so their session_ids are unreachable through
    # this walker via the chained path either — the operator-known
    # session_id query below is the primary fallback for that case too.
    manual_followups.append(
        "recent-messages: walker reaches only sessions linked via form-"
        "submissions (chained walk). Subjects who chatted without ever "
        "submitting a form have no durable subject linkage on the row; "
        "their messages age out via the 24h TTL (best-effort within 48h "
        "per DynamoDB TTL semantics). Compensating control = TTL; "
        "structural fix tracked as D5 F-DSAR4. **F-DSAR1 inheritance**: "
        "if a pre-Phase-1 subject is suspected (see form-submissions "
        "followup above), their session_ids will also be unreachable "
        "through this walker — the operator-known session_id query "
        "below is the primary fallback. Operator-actionable fallback if "
        "an out-of-band session_id is known, OR for content-substring "
        "scan (case-sensitive — false positives likely):\n"
        f"{_recent_messages_chat_only_cli_snippet(tenant_id)}"
    )
    # Article 15 — third-party disclosure caveat for access exports only.
    if request_type == "access" and rm.get("action") == "exported":
        manual_followups.append(
            "recent-messages: access export includes free-text `content` that "
            "may reference third parties (family members, others mentioned in "
            "chat). Operator should review prior to delivery to subject; "
            "consider per-message redaction if any message references a "
            "non-subject individual. Export is projected to "
            "{role, content, messageTimestamp} — internal identifiers "
            "(sessionId, messageId, expires_at) intentionally omitted per "
            "Art 15 data-minimization (advisor 2026-05-21)."
        )

    # Other surfaces: still scaffolded.
    for surface, reason in MFS_SCOPED_SURFACES.items():
        manual_followups.append(f"{surface}: {reason}")
        walker_results[surface] = {"status": "deferred", "reason": reason}

    return rows_touched, manual_followups, exported_rows, walker_results


# ───────────────────────────────────────────────────────────────────────────
# Handler
# ───────────────────────────────────────────────────────────────────────────
def _compute_close_status(walker_results):
    """Compute the closed-event status from per-surface walker outcomes.

    Status semantics (audit fix-now #5):
      - "completed":     all walkers ran cleanly (no errors, no deferrals)
      - "partial":       at least one surface still deferred OR walker skipped
                         due to no-subject; no walker errors
      - "partial_error": at least one walker errored mid-batch
                         (e.g., query failure, corrupted-row skip)
      - "failed":        env-guard / validation / audit-write failure
                         (handled directly in lambda_handler, not here)

    Today, 5 of 6 surfaces are always `deferred` → close status will always
    be at least "partial" until the remaining walkers ship. This function
    makes the distinction visible from day 1 so the operator signal is
    correct as soon as the 5 remaining walkers land.
    """
    errored = any(r.get("status") == "errored" for r in walker_results.values())
    deferred_or_skipped = any(
        r.get("status") in ("deferred", "skipped_no_subject")
        for r in walker_results.values()
    )
    if errored:
        return "partial_error"
    if deferred_or_skipped:
        return "partial"
    return "completed"


def lambda_handler(event, context):
    """Operator-invocable DSAR entry point.

    See module docstring for contract.
    """
    caller_arn = _assert_account()

    try:
        inputs = _validate(event)
    except InvalidInput as exc:
        logger.error("dsar_input_invalid: %s", exc)
        return {"status": "failed", "error": "invalid_input", "message": str(exc)}

    dsar_id = inputs["dsar_id"]

    # request_received audit event. AuditCollision here means dsar_id replay —
    # fail loud (Q2 decision: loudest signal beats silent overwrite).
    # Audit row 12 (Security SR3): caller_arn = STS-derived identity, not the
    # operator self-report. Distinguishes attribution-claim (operator) from
    # actual-identity (caller_arn) in audit trail.
    try:
        received_ts = _write_audit_event(
            dsar_id=dsar_id,
            event_type="request_received",
            status="in_progress",
            payload={
                "operator": inputs["operator"],
                "caller_arn": caller_arn,
                "tenant_id": inputs["tenant_id"],
                "identifier_type": inputs["identifier_type"],
                "request_type": inputs["request_type"],
                "dry_run": inputs["dry_run"],
            },
        )
    except AuditCollision as exc:
        return {
            "dsar_id": dsar_id,
            "status": "failed",
            "error": "audit_collision",
            "message": str(exc),
        }

    # E2 (PR1 fix-now-4): subject-index lookup is a DDB call — ClientError
    # (throttle, AccessDenied, network) previously propagated uncaught and
    # crashed the Lambda with no audit row. Now: audit-write the failure and
    # return failed cleanly. normalized_email NOT logged/returned (consumer PII).
    try:
        pii_subject_id = _resolve_subject(
            tenant_id=inputs["tenant_id"],
            normalized_email=inputs["subject_identifier"],
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        try:
            _write_audit_event(
                dsar_id=dsar_id,
                event_type="subject_resolution_failed",
                status="failed",
                payload={
                    "tenant_id": inputs["tenant_id"],
                    "error_code": error_code,
                },
            )
        except AuditCollision as audit_exc:
            logger.error(
                "subject_resolution_failure_audit_collision: dsar_id=%s err=%s",
                dsar_id, audit_exc,
            )
        return {
            "dsar_id": dsar_id,
            "status": "failed",
            "error": "subject_resolution_failed",
            "message": f"DDB ClientError on subject-index lookup: {error_code}",
        }
    logger.info(
        "dsar_subject_resolved: dsar_id=%s tenant=%s found=%s",
        dsar_id, inputs["tenant_id"], pii_subject_id is not None,
    )

    rows_touched, manual_followups, exported_rows, walker_results = (
        _walk_mfs_surfaces(
            pii_subject_id=pii_subject_id,
            tenant_id=inputs["tenant_id"],
            normalized_email=inputs["subject_identifier"],
            request_type=inputs["request_type"],
            dry_run=inputs["dry_run"],
        )
    )

    # Per-surface audit events (audit fix-now #5). Skip deferred surfaces —
    # writing an audit row for "we did nothing because the walker doesn't
    # exist yet" is noise. Only audit walker outcomes that actually ran.
    surface_audit_ts = []
    for surface, result in walker_results.items():
        if result.get("status") == "deferred":
            continue
        try:
            ts = _write_audit_event(
                dsar_id=dsar_id,
                event_type=f"surface_walked:{surface}",
                status=result["status"],
                payload={
                    "rows_touched": result.get("rows_touched", 0),
                    "action": result.get("action"),
                    "error": result.get("error"),
                    "rows_deleted": result.get("rows_deleted"),
                    "rows_skipped_corrupted": result.get("rows_skipped_corrupted"),
                },
            )
            surface_audit_ts.append(ts)
        except AuditCollision as exc:
            # Surface-walked audit collision is recoverable: log loudly,
            # taint walker_results so close_status reflects the audit failure,
            # and continue to close-event write.
            logger.error(
                "surface_audit_collision: surface=%s dsar_id=%s err=%s",
                surface, dsar_id, exc,
            )
            walker_results[surface] = {
                **result,
                "status": "errored",
                "error": f"audit_collision:{exc}",
            }

    close_status = _compute_close_status(walker_results)

    # closed audit event. AuditCollision here is a worst-case: walks happened,
    # audit log can't be closed. Surface to operator as failed (their best
    # signal that the audit trail is inconsistent and needs manual review).
    try:
        closed_ts = _write_audit_event(
            dsar_id=dsar_id,
            event_type="closed",
            status=close_status,
            payload={
                "pii_subject_id_found": pii_subject_id is not None,
                "rows_touched": rows_touched,
                "manual_followups_count": len(manual_followups),
                "exported_surfaces": list(exported_rows.keys()),
                "walker_results": {
                    s: r.get("status") for s, r in walker_results.items()
                },
            },
        )
    except AuditCollision as exc:
        return {
            "dsar_id": dsar_id,
            "status": "failed",
            "error": "closed_audit_collision",
            "message": str(exc),
            "pii_subject_id": pii_subject_id,
            "rows_touched": rows_touched,
            "exported_rows": exported_rows,
            "manual_followups": manual_followups,
            "audit_row_pks": [
                f"{dsar_id}|{received_ts}",
                *(f"{dsar_id}|{t}" for t in surface_audit_ts),
            ],
        }

    return {
        "dsar_id": dsar_id,
        "status": close_status,
        "pii_subject_id": pii_subject_id,
        "rows_touched": rows_touched,
        "exported_rows": exported_rows,
        "manual_followups": manual_followups,
        "audit_row_pks": [
            f"{dsar_id}|{received_ts}",
            *(f"{dsar_id}|{t}" for t in surface_audit_ts),
            f"{dsar_id}|{closed_ts}",
        ],
    }
