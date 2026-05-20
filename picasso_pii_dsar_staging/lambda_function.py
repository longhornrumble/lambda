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
      "status":            "completed" | "partial" | "failed",
      "pii_subject_id":    "<resolved-opaque-id | null>",
      "rows_touched":      {"<surface>": <count>, ...},
      "exported_rows":     {"<surface>": [<row>, ...], ...},   # access only
      "manual_followups":  ["<human-readable-todo>", ...],
      "audit_row_pks":     ["<dsar_id|event_timestamp>", ...]
    }

WHAT THIS LAMBDA DOES TODAY:
    - Cold-start env-guard (refuse to run outside account 525)
    - Input validation (required fields, supported types, dry_run default)
    - Subject resolution: identifier → pii_subject_id via picasso-pii-subject-index-staging
    - Audit writes (request_received + closed events to picasso-pii-dsar-audit-staging)
    - form-submissions walker — tenant-scoped Query + FilterExpression on
      pii_subject_id; access returns rows in exported_rows; delete dry-runs by
      default; explicit dry_run=false performs DeleteItem per matched row
    - Remaining surfaces (notification-sends/events, recent-messages,
      conversation-summaries, audit) return manual_followup until each surface's
      subject-linking attribute is verified against MFS writer code

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

# Surfaces still scaffolded with deferred walkers. form-submissions has its
# walker implemented (`_walk_form_submissions`); the rest return human-readable
# manual_followup until each surface's subject-linking attribute is verified
# against the MFS writer code.
MFS_SCOPED_SURFACES = {
    "notification-sends": (
        "Walker pending: subject-linking attribute on pk/sk schema not yet "
        "documented; requires reading MFS notification_hub writer code first."
    ),
    "notification-events": (
        "Walker pending: same as notification-sends (generic pk/sk schema)."
    ),
    "recent-messages": (
        "Walker pending: table keyed by sessionId/messageTimestamp; subject "
        "linkage requires Scan + FilterExpression on a user-id-like attribute "
        "yet to be verified against MFS writer."
    ),
    "conversation-summaries": (
        "Walker pending: same as recent-messages (sessionId-keyed)."
    ),
    "audit-read-only": (
        "Walker pending: picasso-audit-staging is read-only per Art 17(3)(b) "
        "carve-out (D5 G-C). Access-type DSAR exports rows; never delete."
    ),
}

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
    """Refuse to run in any account other than staging.

    Raises RuntimeError on mismatch — Lambda returns 500, no DDB ops happen,
    no audit row written. The Lambda execution role grants sts:GetCallerIdentity
    explicitly (lambda-pii-dsar-staging module).
    """
    actual = sts.get_caller_identity()["Account"]
    if actual != EXPECTED_ACCOUNT:
        raise RuntimeError(
            f"dsar_account_guard: refusing to run in account {actual}; "
            f"expected staging account {EXPECTED_ACCOUNT}. "
            f"Prod promotion requires explicit code change."
        )


# ───────────────────────────────────────────────────────────────────────────
# Input validation
# ───────────────────────────────────────────────────────────────────────────
class InvalidInput(ValueError):
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
    """
    table = ddb.Table(TABLE_DSAR_AUDIT)
    event_timestamp = _now_iso()
    item = {
        "dsar_id": dsar_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type,
        "status": status,
        "details": json.dumps(payload, default=str),
    }
    table.put_item(Item=item)
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
            return {"rows_found": 0, "error": "query_failed"}
        matched.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    rows_found = len(matched)

    if request_type == "access":
        return {
            "rows_found": rows_found,
            "action": "exported",
            "exported_rows": matched,
        }

    # request_type == "delete"
    if dry_run:
        return {"rows_found": rows_found, "action": "dry_run_count"}

    deleted = 0
    for row in matched:
        try:
            table.delete_item(Key={
                "tenant_id": row["tenant_id"],
                "submission_id": row["submission_id"],
            })
            deleted += 1
        except ClientError as exc:
            logger.error(
                "form_submissions_delete_failed: submission=%s code=%s",
                row.get("submission_id"),
                exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "action": "deleted",
        "rows_deleted": deleted,
    }


def _walk_mfs_surfaces(pii_subject_id, tenant_id, request_type, dry_run):
    """Dispatch per-surface walkers.

    Returns (rows_touched: Dict[str,int], manual_followups: List[str],
             exported_rows: Dict[str, List[dict]]).

    form-submissions has a real walker (`_walk_form_submissions`); the surfaces
    in MFS_SCOPED_SURFACES still return manual_followup until their schemas are
    verified against MFS writer code.
    """
    rows_touched = {"form-submissions": 0}
    rows_touched.update({s: 0 for s in MFS_SCOPED_SURFACES})
    manual_followups = []
    exported_rows = {}

    if pii_subject_id is None:
        manual_followups.append(
            "Subject not found in pii-subject-index. If subject has known "
            "interactions, MFS may not have written the index entry — "
            "investigate via picasso-audit-staging Query on tenant."
        )
        # No walker can run without a pii_subject_id; emit followups for all.
        manual_followups.append(
            "form-submissions: skipped (no pii_subject_id resolved)"
        )
        for surface, reason in MFS_SCOPED_SURFACES.items():
            manual_followups.append(f"{surface}: {reason}")
        return rows_touched, manual_followups, exported_rows

    # form-submissions: real walker.
    fs = _walk_form_submissions(pii_subject_id, tenant_id, request_type, dry_run)
    rows_touched["form-submissions"] = fs["rows_found"]
    if fs.get("error"):
        manual_followups.append(
            f"form-submissions: query failed ({fs['error']}); retry advised"
        )
    elif fs.get("action") == "exported":
        exported_rows["form-submissions"] = fs["exported_rows"]
    elif fs.get("action") == "dry_run_count":
        manual_followups.append(
            f"form-submissions: dry_run=true; {fs['rows_found']} row(s) would "
            f"be deleted; re-invoke with dry_run=false to delete"
        )
    # action == "deleted" → successful real delete; no followup needed.

    # Coverage gap noted whenever the walker ran (even with 0 rows): operator
    # may need to manually walk pre-Phase-1 rows that lack pii_subject_id.
    manual_followups.append(
        "form-submissions: walker filters by pii_subject_id (Phase-1 attribute "
        "from lambda #130). Submissions written before 2026-05-18 do not carry "
        "this attribute; durable fix = Apply-2 backfill (deferred); interim = "
        "manual email-keyed walk if a pre-Phase-1 subject is suspected."
    )

    # Other surfaces: still scaffolded.
    for surface, reason in MFS_SCOPED_SURFACES.items():
        manual_followups.append(f"{surface}: {reason}")

    return rows_touched, manual_followups, exported_rows


# ───────────────────────────────────────────────────────────────────────────
# Handler
# ───────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """Operator-invocable DSAR entry point.

    See module docstring for contract.
    """
    _assert_account()

    try:
        inputs = _validate(event)
    except InvalidInput as exc:
        logger.error("dsar_input_invalid: %s", exc)
        return {"status": "failed", "error": "invalid_input", "message": str(exc)}

    dsar_id = inputs["dsar_id"]

    received_ts = _write_audit_event(
        dsar_id=dsar_id,
        event_type="request_received",
        status="in_progress",
        payload={
            "operator": inputs["operator"],
            "tenant_id": inputs["tenant_id"],
            "identifier_type": inputs["identifier_type"],
            "request_type": inputs["request_type"],
            "dry_run": inputs["dry_run"],
        },
    )

    pii_subject_id = _resolve_subject(
        tenant_id=inputs["tenant_id"],
        normalized_email=inputs["subject_identifier"],
    )
    logger.info(
        "dsar_subject_resolved: dsar_id=%s tenant=%s found=%s",
        dsar_id, inputs["tenant_id"], pii_subject_id is not None,
    )

    rows_touched, manual_followups, exported_rows = _walk_mfs_surfaces(
        pii_subject_id=pii_subject_id,
        tenant_id=inputs["tenant_id"],
        request_type=inputs["request_type"],
        dry_run=inputs["dry_run"],
    )

    # Status: "partial" while any walker is still deferred OR any walker
    # returned a manual_followup. When all walkers ship, the rule becomes
    # "completed" iff len(manual_followups) == 0; "failed" if any walker raised.
    closed_ts = _write_audit_event(
        dsar_id=dsar_id,
        event_type="closed",
        status="partial",
        payload={
            "pii_subject_id_found": pii_subject_id is not None,
            "rows_touched": rows_touched,
            "manual_followups_count": len(manual_followups),
            "exported_surfaces": list(exported_rows.keys()),
        },
    )

    return {
        "dsar_id": dsar_id,
        "status": "partial",
        "pii_subject_id": pii_subject_id,
        "rows_touched": rows_touched,
        "exported_rows": exported_rows,
        "manual_followups": manual_followups,
        "audit_row_pks": [
            f"{dsar_id}|{received_ts}",
            f"{dsar_id}|{closed_ts}",
        ],
    }
