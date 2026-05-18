"""Consumer PII Remediation Path A, Phase 1 — stable subject identifier.

Mints an opaque, non-reversible ``pii_subject_id`` at the first identifying input
(a form submission) and maintains a per-tenant ``normalized_email -> pii_subject_id``
index for later DSAR/delete lookup.

Contract: ``pii_subject_id`` is **additive**. Scheduling continues to key on
``form_submission_id``; nothing reads ``pii_subject_id`` until Phase 2. Index access is
**best-effort** — a form submission must never fail because the index is unavailable
(mirrors the existing non-fatal analytics-write pattern in ``form_handler``).

Design: ``docs/roadmap/PII_IDENTITY_CONTRACT.md`` §3-§5.
"""

import logging
import os
import re
import uuid
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Account = env: code default targets the staging table; the Lambda env var
# (PII_SUBJECT_INDEX_TABLE) + IAM grant are wired at the Phase 1->2 boundary.
PII_SUBJECT_INDEX_TABLE = os.environ.get(
    "PII_SUBJECT_INDEX_TABLE", "picasso-pii-subject-index-staging"
)

# Bounded retry for the get -> conditional-put race (gate blocker B1). 3 is ample:
# each lost race means a winner committed, so attempt 2's consistent read resolves it.
_MAX_INDEX_ATTEMPTS = 3

_GMAIL_DOMAINS = {"gmail.com", "googlemail.com"}
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# Response keys that conventionally hold the submitter's email.
_EMAIL_KEY_HINTS = ("email", "e-mail", "email_address", "emailaddress")

_dynamodb = None


def _table():
    """Lazily construct the index table handle (no import-time AWS dependency)."""
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb.Table(PII_SUBJECT_INDEX_TABLE)


def mint_pii_subject_id() -> str:
    """A fresh opaque subject id. Carries zero information about the person."""
    return "psub_" + uuid.uuid4().hex


def normalize_email(email: Any) -> Optional[str]:
    """Deterministic email normalization (PII Identity Contract §4).

    Pure function: same input -> same output. Returns ``None`` for anything that
    is not a syntactically usable address (caller still mints a subject id).
    """
    if email is None:
        return None
    e = str(email).strip()
    if "@" not in e:
        return None
    local, _, domain = e.rpartition("@")
    if not local or not domain or "@" in local:  # multi-@ == malformed
        return None
    domain = domain.lower()
    local = local.lower()
    if "+" in local:
        local = local.split("+", 1)[0]
    if domain in _GMAIL_DOMAINS:
        domain = "gmail.com"
        local = local.replace(".", "")
    if not local:
        return None
    return f"{local}@{domain}"


def extract_email(responses: Dict[str, Any]) -> Optional[str]:
    """Best-effort: find the submitter's email in arbitrary form responses.

    First an email-named key (case-insensitive), then the first value that looks
    like an address. Returns the raw string (caller normalizes).
    """
    if not isinstance(responses, dict):
        return None
    for key, value in responses.items():
        if isinstance(key, str) and any(h in key.lower() for h in _EMAIL_KEY_HINTS):
            if value and _EMAIL_RE.match(str(value).strip()):
                return str(value).strip()
    for value in responses.values():
        if isinstance(value, str) and _EMAIL_RE.match(value.strip()):
            return value.strip()
    return None


def get_or_create_pii_subject_id(
    tenant_id: str,
    responses: Dict[str, Any],
    *,
    table=None,
) -> str:
    """Return the subject id for this submission, minting/indexing as needed.

    Always returns a usable id. A subject exists even when the submission has no
    email (it just is not email-indexed). Never raises — index failures fall back
    to the freshly-minted candidate so the submission still records a stable id.
    """
    candidate = mint_pii_subject_id()
    try:
        raw = extract_email(responses)
        normalized = normalize_email(raw)
        if not normalized:
            return candidate

        tbl = table if table is not None else _table()
        key = {"tenant_id": tenant_id, "normalized_email": normalized}

        # Bounded get -> conditional-put loop. On a lost race, the winner's put
        # is already committed, so the next iteration's STRONGLY-CONSISTENT read
        # is guaranteed to return the winner's id — we never mint a divergent id
        # for a person who already has an index entry (gate blocker B1).
        for attempt in range(_MAX_INDEX_ATTEMPTS):
            existing = tbl.get_item(
                Key=key, ConsistentRead=(attempt > 0)
            ).get("Item")
            if existing and existing.get("pii_subject_id"):
                return existing["pii_subject_id"]
            try:
                tbl.put_item(
                    Item={
                        "tenant_id": tenant_id,
                        "normalized_email": normalized,
                        "pii_subject_id": candidate,
                        "created_at": _now_iso(),
                    },
                    ConditionExpression="attribute_not_exists(normalized_email)",
                )
                return candidate
            except ClientError as e:
                if (
                    e.response.get("Error", {}).get("Code")
                    == "ConditionalCheckFailedException"
                ):
                    continue  # someone won the race; loop re-reads consistently
                raise

        # Persistent unresolved race (winner put-then-deleted repeatedly — degenerate).
        # NOT a silent divergent mint: the row is recorded UNINDEXED, which is exactly
        # the pre-Phase-1 shape that locked decision #5 (backfill/TTL) already covers.
        logger.warning(
            "pii_subject index race unresolved after %d attempts (tenant=%s); "
            "row recorded UNINDEXED — legacy-equivalent, Phase-2 backfill covers it",
            _MAX_INDEX_ATTEMPTS,
            tenant_id,
        )
        return candidate
    except Exception as err:  # noqa: BLE001 - index access is best-effort, never fatal
        logger.warning(
            "pii_subject index unavailable (non-fatal): %s", type(err).__name__
        )
        return candidate


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
