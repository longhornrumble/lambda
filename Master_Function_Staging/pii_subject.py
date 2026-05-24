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
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Account=env: the Lambda env var PII_SUBJECT_INDEX_TABLE + the MFS IAM grant are
# wired in Terraform in Phase 1, atomic with this code (gate blocker B2). The
# default below is STAGING-ONLY. A prod promotion MUST set this env var — a prod
# Lambda silently falling back to this staging name would write to the wrong
# account and orphan every subject. Recorded as a Phase-2/promotion gate item.
PII_SUBJECT_INDEX_TABLE = os.environ.get(
    "PII_SUBJECT_INDEX_TABLE", "picasso-pii-subject-index-staging"
)

# Bounded retry for the get -> conditional-put race (gate blocker B1). 3 is ample:
# each lost race means a winner committed, so the next iteration's strongly-
# consistent read resolves it.
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


def read_subject_id(record: Any) -> Optional[str]:
    """Canonical forward-compatible reader of ``pii_subject_id`` off a stored
    record. Pre-Phase-1 rows have no such field and yield ``None`` — never
    raises. Phase-2+ readers MUST call this rather than bracket-access the
    field, so old-shape prod rows never crash a reader (CLAUDE.md schema
    discipline). This is the reader the forward-compat fixture exercises.
    """
    if not isinstance(record, dict):
        return None
    sid = record.get("pii_subject_id")
    return sid if isinstance(sid, str) and sid else None


def normalize_email(email: Any) -> Optional[str]:
    """Deterministic email normalization (PII Identity Contract §4).

    Pure function: same input -> same output. Returns ``None`` for anything that
    is not a syntactically usable address (caller still mints a subject id).
    """
    if email is None:
        return None
    e = str(email).strip()
    if not e or any(ch.isspace() for ch in e):
        return None  # internal whitespace ⇒ not a usable address (R1)
    if "@" not in e:
        return None
    local, _, domain = e.rpartition("@")
    if not local or not domain or "@" in local:  # multi-@ == malformed
        return None
    domain = domain.lower()
    local = local.lower()
    # Only Gmail's dot/plus aliasing is provider-guaranteed to deliver every
    # variant to one inbox, so only Gmail is safe to collapse. Stripping +tag
    # for other providers was an unverified assumption that created an
    # imposter-deletion vector (audit 2026-05-18 #6, option A) — do NOT alter
    # non-Gmail local parts beyond lowercase/trim.
    if domain in _GMAIL_DOMAINS:
        domain = "gmail.com"
        if "+" in local:
            local = local.split("+", 1)[0]
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

    # Sprint E1 / audit blocker B1 (cross-tenant collision): tenant_id missing
    # or literal 'unknown' MUST NOT be indexed. Two unrelated submissions from
    # differently-misconfigured tenants would otherwise collide on the index
    # key (tenant_id, normalized_email) — either reusing a single subject id
    # across distinct subjects or minting divergent ids depending on ordering.
    # Mint UNINDEXED instead; the Phase-2 orphan-sweep gate covers UNINDEXED
    # rows by design. Mirror in pii_subject.js.
    if not tenant_id or tenant_id == "unknown":
        logger.warning(
            "pii_subject tenant_id missing/unknown — minting UNINDEXED "
            "pii_subject_id to avoid cross-tenant index collision"
        )
        return candidate

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
            ).get("Item") or {}
            existing_sid = existing.get("pii_subject_id")
            # Require a non-empty string: a corrupted/empty index value must not
            # be reused (silent divergence) nor spin the loop forever (#7).
            if isinstance(existing_sid, str) and existing_sid:
                return existing_sid
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

        # Unresolved race (and the non-CCF error path below): the submission still
        # gets a usable id, but it is UNINDEXED. Across multiple submissions this
        # ORPHANS the row — the next submission mints a fresh indexed id, and the
        # Phase-2 delete pipeline walks FROM the index, so it would silently miss
        # this row (incomplete deletion = compliance failure). This is ONLY closed
        # by the Phase-2 orphan-sweep gate (sweep form-submissions for
        # pii_subject_id absent from the index — see PII_IDENTITY_CONTRACT §7/§8).
        # A submission must never fail for this, so we stay best-effort + log loud.
        logger.warning(
            "pii_subject index race unresolved after %d attempts (tenant=%s); "
            "row is UNINDEXED — incomplete-deletion risk, requires Phase-2 "
            "orphan-sweep gate",
            _MAX_INDEX_ATTEMPTS,
            tenant_id,
        )
        return candidate
    except Exception as err:  # noqa: BLE001 - index access is best-effort, never fatal
        logger.warning(
            "pii_subject index unavailable (non-fatal): %s — row is UNINDEXED, "
            "requires the Phase-2 orphan-sweep gate (incomplete-deletion risk)",
            type(err).__name__,
        )
        return candidate


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
