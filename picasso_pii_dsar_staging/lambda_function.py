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
      "manual_followups":  ["<human-readable-todo>", ...],
      "audit_row_pks":     ["<dsar_id|event_timestamp>", ...]
    }

WHAT THIS SCAFFOLD DOES (milestone 1 / PR C):
    - Cold-start env-guard (refuse to run outside account 525)
    - Input validation (required fields, supported types, dry_run default)
    - Subject resolution: identifier → pii_subject_id via picasso-pii-subject-index-staging
    - Audit writes (request_received + closed events to picasso-pii-dsar-audit-staging)
    - Per-surface walkers return manual_followup until their attribute schemas are
      verified against MFS writer code (separate follow-up PR per surface)

WHAT IT DOES NOT YET DO (follow-up PRs):
    - form-submissions / notification-* / recent-messages / conversation-summaries
      actual row enumeration + deletion (need MFS writer attribute schemas)
    - picasso-audit-staging read for access-type DSARs (same — Art 17(3)(b) read-only)
    - Meta channel-mappings PSID-keyed walk (milestone 2 / item 1b)
    - S3 / ARCHIVE_BUCKET walks (milestone 2 / item 1b)

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

# Surfaces walked by milestone 1. Each entry's value is the human-readable
# "implementation deferred" reason returned in manual_followups until the
# per-surface walker lands.
MFS_SCOPED_SURFACES = {
    "form-submissions": (
        "Walker pending: form-submissions has no PiiSubjectIdIndex GSI yet "
        "(Apply-3 deferred per D5). Implementation requires Scan + "
        "FilterExpression on pii_subject_id; lands in follow-up PR."
    ),
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
# Per-surface walkers (scaffold — return manual_followup until verified)
# ───────────────────────────────────────────────────────────────────────────
def _walk_mfs_surfaces(pii_subject_id, tenant_id, request_type, dry_run):
    """Returns (rows_touched, manual_followups).

    Per-surface walker implementation is deferred (see MFS_SCOPED_SURFACES).
    This scaffold returns zero rows_touched + a manual_followup per surface so
    the response shape matches the contract and the operator gets honest
    "implementation pending" signal rather than a silent zero.
    """
    rows_touched = {surface: 0 for surface in MFS_SCOPED_SURFACES}
    manual_followups = [
        f"{surface}: {reason}" for surface, reason in MFS_SCOPED_SURFACES.items()
    ]
    if pii_subject_id is None:
        manual_followups.insert(
            0,
            "Subject not found in pii-subject-index. If subject has known "
            "interactions, MFS may not have written the index entry — "
            "investigate via picasso-audit-staging Query on tenant.",
        )
    return rows_touched, manual_followups


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

    rows_touched, manual_followups = _walk_mfs_surfaces(
        pii_subject_id=pii_subject_id,
        tenant_id=inputs["tenant_id"],
        request_type=inputs["request_type"],
        dry_run=inputs["dry_run"],
    )

    # Status: "partial" because all per-surface walkers are deferred. When
    # walkers ship, the rule becomes: "completed" if all walks succeeded;
    # "partial" if any returned a manual_followup; "failed" if any raised.
    closed_ts = _write_audit_event(
        dsar_id=dsar_id,
        event_type="closed",
        status="partial",
        payload={
            "pii_subject_id_found": pii_subject_id is not None,
            "rows_touched": rows_touched,
            "manual_followups_count": len(manual_followups),
        },
    )

    return {
        "dsar_id": dsar_id,
        "status": "partial",
        "pii_subject_id": pii_subject_id,
        "rows_touched": rows_touched,
        "manual_followups": manual_followups,
        "audit_row_pks": [
            f"{dsar_id}|{received_ts}",
            f"{dsar_id}|{closed_ts}",
        ],
    }
