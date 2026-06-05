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
                       deferred (today: email-path DSARs typically return
                       "partial" because the 2 surfaces in DEFERRED_SURFACES —
                       conversation-summaries (session-summaries; F-DSAR31)
                       and audit-read-only — remain deferred. The 5 walked
                       surfaces today: form-submissions, notification-sends,
                       notification-events, recent-messages, archive. psid-path
                       DSARs walk 3: recent-messages, session-events, archive)
    - "partial_error": at least one walker errored mid-batch (query failure,
                       corrupted-row skip, surface audit collision)
    - "failed":        env-guard / input validation / audit-write collision
                       failure — never reached the walker dispatch

WHAT THIS LAMBDA DOES TODAY:
    - Cold-start env-guard (refuse to run outside account 525)
    - Input validation (required fields, supported types, dry_run default)
    - Subject resolution: identifier → pii_subject_id via picasso-pii-subject-index
    - Audit writes — append-only, idempotent (ConditionExpression refuses
      replay on identical (dsar_id, event_timestamp)). Per-DSAR events:
      request_received → surface_walked:<surface> (one per non-deferred
      surface) → closed. All to picasso-pii-dsar-audit.
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
    - recent-messages walker — chained walk via `session_id`s captured by
      form-submissions (email path) OR composed `meta:{pageId}:{psid}` session
      ids (psid path). Queries `recent-messages` per sessionId.
      Per-message export projected to {role, content, messageTimestamp};
      delete-real iterates DeleteItem per (sessionId, messageTimestamp).
    - session-events walker (M2 Sprint B) — chained walk via the same
      sessionId list as recent-messages. Queries
      `picasso-session-events` by `pk=SESSION#{sessionId}`. Access
      exports full STEP rows; delete real deletes per (pk, sk).
    - archive walker (M2 Sprint C) — version-aware S3 walk over
      `picasso-archive-staging/sessions/{sessionId}/` prefix. Uses
      `list_object_versions` + per-version `DeleteObject(VersionId)` +
      delete-marker enumeration (versioning=ENABLED per
      archive-reachability-decision.md; single-shot DeleteObject would
      leave prior versions). Access returns object keys (not bodies —
      operator pulls bodies with `aws s3 cp` under own SSO role).
    - psid resolver (M2 Sprint B) — for `identifier_type=psid`, the
      handler branches: tenant → list of pageIds via channel-mappings
      `TenantIndex` GSI Query; composes `sessionId=meta:{pageId}:{psid}`
      per page; routes through `_walk_psid_surfaces` dispatcher
      (recent-messages + session-events + archive).
    - Remaining surfaces (conversation-summaries / session-summaries,
      audit-read-only) return manual_followup + walker_results
      status=deferred. session-summaries needs `tenant_hash` discovery
      (pk=TENANT#{tenant_hash}) — deferred per F-DSAR31 + Sprint B
      descope; audit-read-only stays deferred per Art 17(3)(b) carve-out.

WHAT IT DOES NOT YET DO (follow-up PRs):
    - session-summaries walker — F-DSAR31 (Sprint B descope; needs
      tenant_hash discovery via either operator-passed field on event OR
      tenant_id → tenant_hash lookup via tenant-registry; defer-with-
      trigger recommended, calendar backstop 2026-08-22)
    - picasso-audit-staging read for access-type DSARs (Art 17(3)(b)
      carve-out, D5 G-C — counsel-pending; UpdateItem deliberately omitted)
    - Per-tenant S3 fulfillment writer extension SHIPPED 2026-05-26 in
      lambda#166 (both Master_Function_Staging and BSH form_handlers).
      KNOWN-ORPHAN race remains (audit closure 2026-05-26 row #7): the
      writer issues `s3.put_object` BEFORE the DDB UpdateItem that records
      `fulfillment_path`. If a DSAR walker runs between those two operations
      (or if UpdateItem fails after a successful put), the S3 object is
      stored but no row references it. The walker will surface this case
      as `rows_without_path` (writer-extension-pending wording), but the
      manual-followup is identical to the pre-extension wording — operators
      cannot distinguish a real writer-pending row from a race-lost orphan.
      Promotion trigger: first DSAR where `rows_without_path > 0` AND the
      tenant has S3 fulfillment configured AND the submission post-dates
      lambda#166 deploy (2026-05-26T15:37Z). Sprint E follow-up: tighten
      the manual-followup wording to call out this race explicitly.
    - phone / name+address identifier_types — walker-NOT-supported per
      F-DSAR30 (Sprint A §3.3 decision); M3 playbook documents manual
      fallback procedures (Sprint E adds the entries)
    - Pre-Phase-1 form-submission backfill walk (deferred per Apply-2)

The Lambda is intentionally deployable and invocable now. Calls succeed end-to-end
(env-guard → validate → resolve subject → audit-write → response) and produce a
complete audit trail. The substantive deletion/export work is the deliberate
discovery-then-implement loop documented in CONSUMER_PII_REMEDIATION.md v3.
"""
import hashlib
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

# CLAUDE.md account→env map. The Lambda refuses to run unless its caller
# account matches EXPECTED_ACCOUNT. Per prod-cutover decision D1 (2026-06-04)
# this is now an IaC-set env var (staging module → 525…, prod module → 614…),
# replacing the former hardcoded constant ("Decision A — FLIP"): one codebase
# serves both accounts and the env-var guard is the prod-promotion gate.
# FAIL-CLOSED: unset ⇒ refuse (never default to an account). See _assert_account.
EXPECTED_ACCOUNT = os.environ.get("EXPECTED_ACCOUNT")

# Staging table names — single source of truth (matches infra/modules/* locals).
TABLE_SUBJECT_INDEX = "picasso-pii-subject-index"
TABLE_DSAR_AUDIT = "picasso-pii-dsar-audit"
# D2: form-submissions is the table-rename program's held carve-out — its NAME
# diverges across accounts (staging picasso-form-submissions-staging vs prod
# picasso_form_submissions), so the name is account-resolved via an IaC-set env
# var (default = the staging name), mirroring the EXPECTED_ACCOUNT pattern. The
# KEY SCHEMA also diverges; that is handled at access time by
# _form_submissions_key_schema (no account branch).
TABLE_FORM_SUBMISSIONS = os.environ.get(
    "FORM_SUBMISSIONS_TABLE", "picasso-form-submissions-staging")
TABLE_NOTIFICATION_SENDS = "picasso-notification-sends"
TABLE_NOTIFICATION_EVENTS = "picasso-notification-events"
TABLE_RECENT_MESSAGES = "picasso-recent-messages"
TABLE_CHANNEL_MAPPINGS = "picasso-channel-mappings"
TABLE_SESSION_EVENTS = "picasso-session-events"
# F-DSAR31 (closed): session-summaries surface. pk=TENANT#{tenant_hash},
# sk=SESSION#{sessionId}; rows carry a redacted first_question + counts/outcome
# linkable by pii_subject_id. Reached via the operator-passed tenant_hash on the
# DSAR event (the partition is tenant_hash-keyed, not tenant_id-keyed).
TABLE_SESSION_SUMMARIES = "picasso-session-summaries"
GSI_NOTIFICATION_EVENTS_BY_MESSAGE_ID = "ByMessageId"
GSI_CHANNEL_MAPPINGS_TENANT_INDEX = "TenantIndex"
# M2 Sprint C: ARCHIVE_BUCKET. Per archive-reachability-decision.md
# (2026-05-23): bucket exists in staging acct 525 only (prod has no archive
# surface — F-DSAR17 routing). Key shape: sessions/{sessionId}/...
# Versioning ENABLED with 7-day NoncurrentVersionExpiration compensating
# control; walker must enumerate versions on delete to fully erase.
S3_ARCHIVE_BUCKET = "picasso-archive-staging"
S3_ARCHIVE_KEY_PREFIX = "sessions/"

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

# Audit fix #3 — bound the channel-mappings TenantIndex GSI Query so the
# psid resolver cannot exhaust the 60s timeout on a multi-page tenant. 100
# pages × 1 psid = 100 sessionIds, which then enters the downstream
# MAX_SESSION_IDS_PER_INVOCATION=200 walker cap with headroom. Overflow
# surfaces in manual_followup with the truncated page count.
MAX_PAGE_IDS_PER_INVOCATION = 100

# Soft cap on the access-export response payload. A chatty subject can
# accumulate dozens of messages per session × multiple sessions. Lambda's
# response limit is 6 MB; an unbounded export risks exceeding it. Overflow
# surfaces in manual_followups; the walker returns the first
# MAX_EXPORTED_MESSAGES rows.
MAX_EXPORTED_MESSAGES = 1000

# Audit fix #9 — session-events access path soft cap. STEP rows are
# workflow state (not free-text content) but a session with many steps × 200
# sessions can still approach the 6 MB Lambda response cap when combined
# with form-submissions + notification-sends/events + recent-messages +
# archive exported_rows. Overflow surfaces in exported_steps_truncated_count.
MAX_EXPORTED_STEPS = 1000

# Surfaces still deferred after M2 Sprint B + Sprint C. Original M1 deferral
# context: re-scoped 2026-05-23 per phase-completion-audit row 5 / tech-lead
# B1; both surfaces were listed in M1 outcome statement (master plan §2)
# but never had walkers implemented. Post-M2 Sprint B/C, the deferral
# rationale for `conversation-summaries` (session-summaries surface) has
# evolved — F-DSAR31 names tenant_hash discovery as the new prerequisite.
# `audit-read-only` deferral rationale is unchanged (Art 17(3)(b) carve-out).
#
# F-DSAR31 CLOSED 2026-06-03: the `conversation-summaries` (session-summaries)
# surface now has a real walker (`_walk_session_summaries`), reached via the
# operator-passed `tenant_hash` on the DSAR event (resolution option (a)). It is
# no longer deferred; `audit-read-only` remains the only deferred surface.
DEFERRED_SURFACES = {
    "audit-read-only": (
        "Walker pending: picasso-audit-staging is read-only per Art 17(3)(b) "
        "carve-out (D5 G-C; counsel-pending). Access-type DSAR exports rows; "
        "never delete. M1 scope-excluded (v0.3 2026-05-23) and remains "
        "deferred under the carve-out until counsel determination changes."
    ),
}

# Backward-compat alias (callers in _walk_mfs_surfaces). The rename is
# documentation, not behavior — same dict, clearer name.
MFS_SCOPED_SURFACES = DEFERRED_SURFACES

SUPPORTED_REQUEST_TYPES = {"access", "delete"}
# Milestone 1 shipped email; M2 Sprint B adds psid (Meta Messenger subjects).
# phone + name+address remain walker-NOT-supported per M2 Sprint A design §3.3
# (D5 row F-DSAR30); manual M3 playbook procedures cover those gaps.
SUPPORTED_IDENTIFIER_TYPES = {"email", "psid"}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Module-level clients (boto3 connection pool warm on cold-start).
ddb = boto3.resource("dynamodb")
sts = boto3.client("sts")
# M2 Sprint C: S3 client for the ARCHIVE_BUCKET walker. Eager region from the
# Lambda runtime (AWS_DEFAULT_REGION); no explicit region kwarg.
s3 = boto3.client("s3")

# Audit fix #10: MFA-Delete posture cache. The archive walker assumes
# MFA-Delete is disabled — under MFA-Delete=enabled, every
# delete_object(VersionId=...) returns 403 with an uninformative AccessDenied.
# Cold-start check (lazy; cached) lets the walker fail loud rather than
# silently rack up versions_delete_failed without diagnosis. None = not yet
# checked; True = enabled (block deletes); False = disabled (proceed).
_archive_mfa_delete_enabled = None


# ───────────────────────────────────────────────────────────────────────────
# Cold-start guard
# ───────────────────────────────────────────────────────────────────────────
def _assert_account():
    """Refuse to run unless the caller account matches EXPECTED_ACCOUNT; return
    caller ARN.

    Raises RuntimeError when EXPECTED_ACCOUNT is unset (fail-closed) or on
    account mismatch — Lambda returns 500, no DDB ops happen, no audit row
    written. The Lambda execution role grants sts:GetCallerIdentity explicitly
    (lambda-pii-dsar module).

    Audit row 12 (Security SR3): returns the caller ARN so the handler can
    log it into the `request_received` audit row. The `operator` payload
    field is self-reported; the caller ARN is the actual identity AWS sees
    (the Lambda's execution role, since invocation hops through Lambda's
    own service principal). This preserves accountability when the operator
    payload value can't be trusted.
    """
    if not EXPECTED_ACCOUNT:
        raise RuntimeError(
            "dsar_account_guard: EXPECTED_ACCOUNT env var is unset; refusing to "
            "run (fail-closed). IaC must set it to the account this Lambda is "
            "deployed in."
        )
    identity = sts.get_caller_identity()
    actual = identity["Account"]
    if actual != EXPECTED_ACCOUNT:
        raise RuntimeError(
            f"dsar_account_guard: refusing to run in account {actual}; "
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

    Idempotency invariant violated — the (dsar_id, event_timestamp) pair
    already exists. Realistic cause is operator replay of the same dsar_id;
    microsecond-precision timestamp collisions on sequential calls are
    effectively impossible. Handler returns status=failed (loudest signal)
    rather than silently overwriting a prior audit row.
    """
    pass


def _normalize_email(email):
    """Lower + strip ONLY — used for the recipient-equality walker filters
    (`recipient == normalized_email`), which match against stored notification
    recipients that the walkers also lower()+strip() before comparing.

    NOTE: this is NOT the subject-index key normalization. The index is keyed by
    the writer's Gmail-aware `normalize_email` (pii_subject.py) — use
    `_normalize_email_for_index` for the index lookup. (G1, phase-audit
    2026-06-05: the prior docstring wrongly claimed this matched the writer; a
    Gmail subject like Foo.Bar@Gmail.com strip+lowers to foo.bar@gmail.com but
    the index key is foobar@gmail.com → the lookup missed every Gmail-with-dots
    subject.)
    """
    return email.strip().lower()


# Gmail dot/plus collapsing — VERBATIM port of the subject-index writer's
# normalize_email (Master_Function_Staging/pii_subject.py / pii_subject.js), so
# the DSAR index lookup reproduces the exact key the writer stored. Only Gmail
# is collapsed (provider-guaranteed alias delivery); non-Gmail locals get
# lower/strip only. A parity test replays the writer's vectors.
_GMAIL_DOMAINS = {"gmail.com", "googlemail.com"}


def _normalize_email_for_index(email):
    """Deterministic, Gmail-aware normalization for the subject-index KEY lookup
    (G1). Mirrors pii_subject.py:normalize_email exactly. Returns None for a
    non-usable address (→ no possible index entry → subject resolves to None)."""
    if email is None:
        return None
    e = str(email).strip()
    if not e or any(ch.isspace() for ch in e):
        return None
    if "@" not in e:
        return None
    local, _, domain = e.rpartition("@")
    if not local or not domain or "@" in local:
        return None
    domain = domain.lower()
    local = local.lower()
    if domain in _GMAIL_DOMAINS:
        domain = "gmail.com"
        if "+" in local:
            local = local.split("+", 1)[0]
        local = local.replace(".", "")
    if not local:
        return None
    return f"{local}@{domain}"


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
            f"identifier_type {identifier_type!r} not supported; "
            f"supported: {sorted(SUPPORTED_IDENTIFIER_TYPES)}. "
            f"phone / name+address are walker-NOT-supported per F-DSAR30 — "
            f"see dsar-operator-playbook.md §Per-surface manual fallback."
        )

    request_type = event["request_type"]
    if request_type not in SUPPORTED_REQUEST_TYPES:
        raise InvalidInput(
            f"request_type {request_type!r} not supported; "
            f"supported: {sorted(SUPPORTED_REQUEST_TYPES)}"
        )

    subject_identifier = event["subject_identifier"]
    if identifier_type == "email":
        normalized = _normalize_email(subject_identifier)
        if not EMAIL_RE.match(normalized):
            raise InvalidInput(f"subject_identifier does not look like an email")
        subject_identifier = normalized
    elif identifier_type == "psid":
        # Facebook PSIDs are opaque numeric strings (typically 15-17 digits)
        # scoped to the (page, user) pair. No normalization beyond strip;
        # cross-tenant isolation is enforced upstream by _resolve_psid_subject
        # via the channel-mappings TenantIndex GSI Query.
        if not isinstance(subject_identifier, str) or not subject_identifier.strip():
            raise InvalidInput("subject_identifier must be a non-empty string for identifier_type=psid")
        subject_identifier = subject_identifier.strip()

    # Sprint E1 / audit blocker B2 (smoke-prefix is UX, not security boundary):
    # the operator playbook §8 JMESPath at-risk filter excludes
    # dsar_id starting with 'smoke-' as hygiene. Without a write-side gate, a
    # mistyped operator (or malicious actor with operator role) could create
    # `dsar_id='smoke-real-001'` for a real DSAR — that DSAR would then be
    # permanently hidden from the operator view AND undeletable per the C2
    # 4-action Deny resource policy. The marker below ELIMINATES the
    # accident/typo failure mode by requiring an explicit smoke_test_marker
    # field; it is NOT a security boundary against an attacker with legitimate
    # operator privileges (no code-level boundary is possible there) but it
    # makes the existence of any smoke-prefixed row in production explicit and
    # security-logged for the forensic trail.
    dsar_id = event["dsar_id"]

    # Sprint F1 / audit-of-audit finding 6: prefix check is case-insensitive
    # so 'Smoke-real-001' / 'SMOKE-001' don't bypass the guard. Reviewer
    # (test-engineer) observed the original lowercase-only check let
    # capital-S variants through silently.
    is_smoke_prefix = dsar_id.lower().startswith("smoke-")

    # Sprint F1 / audit-of-audit finding 15: smoke_test_marker MUST be a true
    # boolean — string 'true'/'false' would otherwise be truthy in Python and
    # silently activate the marker. Reviewer (test-engineer) noted CLI callers
    # passing JSON-deserialized strings are the realistic footgun. None is
    # treated as "absent" (same as the .get() default) and falls through to
    # the smoke-prefix-without-marker rejection.
    smoke_marker = event.get("smoke_test_marker", False)
    if smoke_marker is None:
        smoke_marker = False
    if not isinstance(smoke_marker, bool):
        raise InvalidInput(
            f"smoke_test_marker must be boolean true/false; got "
            f"{type(smoke_marker).__name__}={smoke_marker!r}"
        )

    # Sprint F1 / audit-of-audit finding 13: if/elif (not two parallel ifs).
    # Original sequential ifs were functionally correct but a future
    # early-return refactor could silently break the contract.
    if is_smoke_prefix and not smoke_marker:
        raise InvalidInput(
            "dsar_id starts with reserved 'smoke-' prefix (case-insensitive); "
            "re-invoke with smoke_test_marker=true if this is intentional "
            "(will be security-logged), or change the dsar_id."
        )
    elif is_smoke_prefix and smoke_marker:
        logger.warning(
            "SECURITY: smoke-prefix dsar_id accepted via explicit "
            "smoke_test_marker=true: dsar_id=%s operator=%s tenant_id=%s",
            dsar_id, event.get("operator"), event.get("tenant_id"),
        )
    # Audit closure 2026-05-26 row #21 (Security-Reviewer 🟡): the marker is a
    # two-way gate. Setting smoke_test_marker=true on a NON-smoke-prefixed
    # dsar_id would tag a real DSAR with is_smoke_test=true in the audit
    # row, causing it to be filtered out of the SLA monitor scan
    # (`!is_smoke_test` filter in `dsar-operator-playbook.md` §8). Reject
    # the inconsistent combination so a real DSAR can never be silently
    # hidden by a stale marker setting.
    elif smoke_marker and not is_smoke_prefix:
        raise InvalidInput(
            "smoke_test_marker=true requires a 'smoke-' prefixed dsar_id "
            "(audit closure 2026-05-26 row #21); the marker drives the "
            "is_smoke_test audit attribute which SLA-monitor scans use to "
            "filter out test rows — using it on a real dsar_id would hide "
            "the DSAR from SLA tracking. Either prefix the dsar_id with "
            "'smoke-' (for tests) or set smoke_test_marker=false (for real "
            "DSARs)."
        )

    # G2 (phase-audit 2026-06-05): dry_run must be a true boolean. The prior
    # bool(event.get("dry_run", True)) coerced a numeric 0/0.0 to a REAL delete
    # (bool(0) is False) despite this function's "a typo can never produce an
    # unintended deletion" intent. Mirror the purge Lambda's strict isinstance
    # guard: default True, reject any non-bool.
    dry_run = event.get("dry_run", True)
    if dry_run is None:
        dry_run = True
    if not isinstance(dry_run, bool):
        raise InvalidInput(
            f"dry_run must be boolean true/false; got "
            f"{type(dry_run).__name__}={dry_run!r}"
        )

    return {
        "subject_identifier": subject_identifier,
        "identifier_type": identifier_type,
        "request_type": request_type,
        "tenant_id": event["tenant_id"],
        "operator": event["operator"],
        "dsar_id": dsar_id,
        # F-DSAR31: optional operator-passed tenant_hash. Required ONLY to reach
        # the session-summaries surface (pk=TENANT#{tenant_hash}). Absent → that
        # surface is skipped with a manual_followup (all other surfaces still
        # walk). Normalize empty/whitespace to None so the dispatcher's
        # presence-check is unambiguous.
        "tenant_hash": (event.get("tenant_hash") or "").strip() or None,
        "dry_run": dry_run,
        # Closeout-audit row #15: propagate the validated marker so the
        # audit writer can stamp a top-level `is_smoke_test` attribute on
        # every row this invocation writes. Operator scans can then
        # FilterExpression `is_smoke_test = :false` to exclude synthetic
        # rows without relying on `begins_with(dsar_id, 'smoke-')` (the
        # prefix is a UX convention, not a hard contract).
        "smoke_test_marker": smoke_marker,
    }


# ───────────────────────────────────────────────────────────────────────────
# Subject resolution
# ───────────────────────────────────────────────────────────────────────────
def _resolve_subject(tenant_id, normalized_email):
    """Look up pii_subject_id via picasso-pii-subject-index.

    Returns the pii_subject_id string, or None if no index entry exists.
    The subject-index Query is keyed on (tenant_id, normalized_email).

    G1 (phase-audit 2026-06-05): the index key was written by the writer's
    Gmail-aware normalize_email, so the lookup MUST apply the same Gmail dot/plus
    collapsing — otherwise a Gmail-with-dots subject (foo.bar@gmail.com) never
    matches its stored key (foobar@gmail.com). The incoming `normalized_email` is
    only strip+lower'd (it's also reused for recipient-equality filters), so
    re-normalize here for the index key without affecting that other use.
    """
    index_key = _normalize_email_for_index(normalized_email)
    if not index_key:
        return None  # non-usable address → never indexed (writer mints UNINDEXED)
    table = ddb.Table(TABLE_SUBJECT_INDEX)
    try:
        resp = table.get_item(Key={
            "tenant_id": tenant_id,
            "normalized_email": index_key,
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


def _resolve_psid_subject(tenant_id, psid):
    """Resolve psid → list of Meta sessionIds for the tenant.

    M2 Sprint B subject resolver for identifier_type=psid. Two-step lookup:
      1. tenant → list of pageIds via channel-mappings TenantIndex GSI
         (KEY: tenantId=HASH, channelType=RANGE; channelType="messenger").
         The row's PK ("PAGE#{pageId}") yields the pageId.
      2. For each pageId: construct sessionId = "meta:{pageId}:{psid}".
         (Per Meta_Response_Processor/index.js:230-263, the session key is
         derived from the (pageId, psid) pair at write time.)

    Returns a list of sessionId strings. Empty list = no Meta pages for the
    tenant; downstream walkers treat this as "no Meta surface to walk."

    Cross-tenant isolation: the GSI Query is bounded by tenantId; only pages
    belonging to the requested tenant are enumerated. PSIDs themselves are
    NOT tenant-scoped attributes (the same PSID could appear on different
    tenants' pages), so the (pageId, psid) composition is what scopes the
    sessionId list to the tenant.

    Why this returns sessionIds (not pii_subject_id): Meta-only subjects
    have no entry in picasso-pii-subject-index (the index is built
    from form-submission writes per Phase-1; PSID-only subjects never
    submit forms). The sessionId list IS the subject context for the
    psid-path walkers — _walk_recent_messages + _walk_session_events
    consume it the same way the email-path walkers consume session_ids
    chained from _walk_form_submissions.

    On ClientError: raises (handler audit-writes the failure and returns
    failed cleanly — same contract as _resolve_subject's error path).
    """
    table = ddb.Table(TABLE_CHANNEL_MAPPINGS)
    page_ids = []
    last_evaluated_key = None
    truncated = False
    while True:
        kwargs = {
            "IndexName": GSI_CHANNEL_MAPPINGS_TENANT_INDEX,
            "KeyConditionExpression": (
                Key("tenantId").eq(tenant_id) & Key("channelType").eq("messenger")
            ),
            "ProjectionExpression": "PK",
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        try:
            resp = table.query(**kwargs)
        except ClientError as exc:
            logger.error(
                "psid_subject_resolution_failed: tenant=%s err=%s",
                tenant_id, exc.response.get("Error", {}).get("Code"),
            )
            raise
        for item in resp.get("Items", []):
            pk = item.get("PK", "")
            if pk.startswith("PAGE#"):
                page_ids.append(pk.split("#", 1)[1])
        # Audit fix #3: bounded fan-out — stop paginating after the cap to
        # protect the 60s Lambda timeout on a multi-page tenant. The cap
        # itself is large enough to cover any realistic single-tenant
        # Messenger fan-out at current scale; overflow surfaces via the
        # truncated flag returned in the result list metadata.
        if len(page_ids) >= MAX_PAGE_IDS_PER_INVOCATION:
            if len(page_ids) > MAX_PAGE_IDS_PER_INVOCATION or resp.get("LastEvaluatedKey"):
                truncated = True
                page_ids = page_ids[:MAX_PAGE_IDS_PER_INVOCATION]
            break
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    if truncated:
        logger.warning(
            "psid_resolver_page_ids_truncated: tenant=%s cap=%d",
            tenant_id, MAX_PAGE_IDS_PER_INVOCATION,
        )

    return [f"meta:{page_id}:{psid}" for page_id in page_ids]


# ───────────────────────────────────────────────────────────────────────────
# Audit write (append-only event log to picasso-pii-dsar-audit)
# ───────────────────────────────────────────────────────────────────────────
def _now_iso():
    # M9.G7 / F-DSAR27: PINNED format. The SLA monitor Lambda
    # (`picasso_pii_dsar_sla_monitor_staging/lambda_function.py`) reads this
    # value via the StatusIndex GSI range key and does DDB lexicographic
    # string comparison against a threshold built with the SAME timespec
    # ('microseconds'). Don't change to timespec='auto' or `.isoformat()`
    # default — auto drops the microseconds field when it's exactly zero,
    # which makes lex comparison silently mis-order at zero-microsecond
    # boundaries. The reader has a regression test
    # (`test_event_timestamp_iso_format_contract`) that fires if this
    # writer format drifts.
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _write_audit_event(dsar_id, event_type, status, payload, is_smoke_test=False):
    """PutItem append-only event row. PK=dsar_id, SK=event_timestamp.

    `payload` is serialized to JSON in the `details` attribute. `status` is
    duplicated to the top level (StatusIndex GSI hash key) so the future
    EventBridge SLA alarm Lambda can Query by status.

    Idempotency: ConditionExpression refuses to overwrite an existing
    (dsar_id, event_timestamp) row. Collision raises AuditCollision so the
    handler can surface the replay condition to the operator instead of
    silently mutating prior audit state. See class docstring for rationale.

    is_smoke_test (closeout-audit row #15): when True, stamps a top-level
    `is_smoke_test=True` attribute on the audit row so operator scans of
    `picasso-pii-dsar-audit` can FilterExpression them out without
    depending on the `dsar_id` prefix (which is a UX convention, not a
    schema guarantee). When False (default), no attribute is written
    (forward-compatible with pre-fix rows; readers MUST use .get()).
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
    if is_smoke_test:
        item["is_smoke_test"] = True
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
# ── D2: form-submissions schema-adaptive access ────────────────────────────
# The form-submissions table key schema DIVERGES across accounts (the schema +
# naming carve-out held out of the table-rename program):
#   - staging picasso-form-submissions-staging — composite key (tenant_id HASH,
#     submission_id RANGE). A subject's rows are found by a base-table Query on
#     tenant_id.
#   - prod picasso_form_submissions — single key (submission_id HASH); tenant_id
#     is a NON-key attribute, so a base-table Query on tenant_id raises
#     ValidationException. The tenant's rows are reachable via the
#     tenant-timestamp-index GSI (PK=tenant_id), present on BOTH tables with
#     ProjectionType=ALL and full row coverage (verified 2026-06-05: prod 47/47,
#     staging 5/5 rows present in the GSI).
# One account-agnostic path adapts to whichever shape it runs against by
# discovering the live table key schema once per container (DescribeTable is
# cached — the schema is immutable for the table's life). No account-id branch,
# no prod schema migration.
TENANT_TIMESTAMP_INDEX = "tenant-timestamp-index"
_form_key_schema_cache = {}


def _form_submissions_key_schema(table_name):
    """Return (hash_key, range_key|None) for the form-submissions table, cached
    per container. Raises ClientError if DescribeTable is denied/unavailable —
    callers wrap it the same as a query failure."""
    cached = _form_key_schema_cache.get(table_name)
    if cached is None:
        desc = ddb.meta.client.describe_table(TableName=table_name)["Table"]
        keys = {k["KeyType"]: k["AttributeName"] for k in desc["KeySchema"]}
        cached = (keys["HASH"], keys.get("RANGE"))
        _form_key_schema_cache[table_name] = cached
    return cached


def _walk_form_submissions(pii_subject_id, tenant_id, request_type, dry_run):
    """Walk the form-submissions table for one subject under one tenant.

    Access pattern: tenant-scoped Query + FilterExpression on pii_subject_id.
    Schema-adaptive (D2): when tenant_id is the base-table HASH (staging) the
    base table is queried directly; when the table is single-submission_id-keyed
    (prod) the tenant-timestamp-index GSI is queried on tenant_id instead. Both
    bound the read to one tenant — far cheaper than a full-table Scan — and yield
    identical row coverage (GSI ProjectionType=ALL). See
    _form_submissions_key_schema.

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

    try:
        hash_key, range_key = _form_submissions_key_schema(TABLE_FORM_SUBMISSIONS)
    except ClientError as exc:
        logger.error(
            "form_submissions_describe_failed: tenant=%s code=%s",
            tenant_id, exc.response.get("Error", {}).get("Code"),
        )
        return {"rows_found": 0, "session_ids": [], "error": "query_failed"}
    index_name = None if hash_key == "tenant_id" else TENANT_TIMESTAMP_INDEX

    matched = []
    last_evaluated_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("tenant_id").eq(tenant_id),
            "FilterExpression": Attr("pii_subject_id").eq(pii_subject_id),
        }
        if index_name:
            kwargs["IndexName"] = index_name
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
            "matched_rows": matched,
            "action": "exported",
            "exported_rows": matched,
        }

    # request_type == "delete"
    if dry_run:
        return {
            "rows_found": rows_found,
            "session_ids": session_ids,
            "matched_rows": matched,
            "action": "dry_run_count",
        }

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    # D2: the DeleteItem Key is built from the discovered table key schema —
    # composite {tenant_id, submission_id} on staging, single {submission_id}
    # on prod. Passing the staging composite key against the prod single-key
    # table (or vice versa) would itself raise ValidationException.
    key_fields = [hash_key] + ([range_key] if range_key else [])
    for row in matched:
        # Schema discipline (CLAUDE.md §"Schema Discipline"): the walker MUST
        # tolerate corrupted rows — a missing key attr indicates writer drift,
        # not an operator-actionable failure. Log + continue so one bad row
        # never breaks the whole batch.
        key = {k: row.get(k) for k in key_fields}
        if any(v is None for v in key.values()):
            skipped_corrupted += 1
            # D1 (PR1 fix-now-4): pii_subject_id REDACTED (opaque PSID is still
            # PII per D5 G-H), but tenant_id + the present-key marker are
            # operator-actionable and non-PII. tenant_id is read off the row
            # attribute (present even on the prod single-submission_id-key shape
            # where it is not part of the key).
            present = {k: (row.get(k) is not None) for k in key_fields}
            logger.error(
                "form_submissions_delete_skipped_corrupted: tenant_id=%s "
                "key_present=%s — row missing key; cannot delete safely",
                row.get("tenant_id"), present,
            )
            continue
        try:
            table.delete_item(Key=key)
            deleted += 1
        except ClientError as exc:
            # Audit row 8 (code-reviewer SR1): count delete failures so the
            # response can distinguish "matched but not deleted" from
            # "matched and deleted". Without this counter, rows_deleted
            # silently undercounts and the operator believes the delete
            # completed when it didn't.
            delete_failed += 1
            # Redaction: log the error code only — never the key values.
            logger.error(
                "form_submissions_delete_failed: code=%s",
                exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "session_ids": session_ids,
        "matched_rows": matched,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
    }


def _walk_session_summaries(pii_subject_id, tenant_hash, request_type, dry_run):
    """Walk picasso-session-summaries for one subject (F-DSAR31, closed).

    The partition is keyed pk=TENANT#{tenant_hash}, sk=SESSION#{sessionId}; rows
    carry a redacted first_question + counts/outcome linkable by pii_subject_id.
    Mirrors _walk_form_submissions: tenant-partition Query + FilterExpression on
    pii_subject_id.

    `tenant_hash` is operator-passed on the DSAR event (the partition is
    tenant_hash-keyed, not tenant_id-keyed — that asymmetry was why this surface
    was deferred). The dispatcher only calls this walker when tenant_hash is
    non-empty; otherwise it skips with a manual_followup.

    request_type: "access" → export matched rows; "delete"+dry_run → count;
    "delete"+real → DeleteItem per (pk, sk). Logs NEVER carry pii_subject_id
    (opaque PSID is PII per D5 G-H) or row content.
    """
    table = ddb.Table(TABLE_SESSION_SUMMARIES)
    pk_value = f"TENANT#{tenant_hash}"

    matched = []
    last_evaluated_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("pk").eq(pk_value),
            "FilterExpression": Attr("pii_subject_id").eq(pii_subject_id),
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        try:
            resp = table.query(**kwargs)
        except ClientError as exc:
            logger.error(
                "session_summaries_query_failed: code=%s",
                exc.response.get("Error", {}).get("Code"),
            )
            return {"rows_found": 0, "session_ids": [], "error": "query_failed"}
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

    if dry_run:
        return {"rows_found": rows_found, "action": "dry_run_count"}

    deleted = 0
    delete_failed = 0
    skipped_corrupted = 0
    for row in matched:
        row_pk = row.get("pk")
        row_sk = row.get("sk")
        if row_pk is None or row_sk is None:
            skipped_corrupted += 1
            logger.error(
                "session_summaries_delete_skipped_corrupted: pk=%s sk=%s — row missing PK/SK",
                row_pk, row_sk,
            )
            continue
        try:
            table.delete_item(Key={"pk": row_pk, "sk": row_sk})
            deleted += 1
        except ClientError as exc:
            delete_failed += 1
            logger.error(
                "session_summaries_delete_failed: sk=%s code=%s",
                row_sk, exc.response.get("Error", {}).get("Code"),
            )
    return {
        "rows_found": rows_found,
        "action": "deleted",
        "rows_deleted": deleted,
        "rows_delete_failed": delete_failed,
        "rows_skipped_corrupted": skipped_corrupted,
    }


def _walk_notification_sends(tenant_id, normalized_email, request_type, dry_run):
    """Walk picasso-notification-sends for direct-to-consumer messages.

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
    """Walk picasso-notification-events via ByMessageId GSI.

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
    `recent-messages` rows include internal join keys (`sessionId`,
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
    """Walk recent-messages for one subject via chained session_ids.

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


def _walk_session_events(tenant_id, session_ids, request_type, dry_run):
    """Walk picasso-session-events for one subject via session_ids.

    M2 Sprint B walker. Chained input: session_ids derived from either
    _walk_form_submissions (email path, via the subject's submitted forms)
    OR _resolve_psid_subject (psid path, via tenant→pageIds composition).

    Table schema (live as of 2026-05-25): pk=SESSION#{sessionId},
    sk=STEP#{n}. No tenantId on row (cross-tenant isolation enforced
    upstream — tenant_id arg is defense-in-depth only).

    The walker mirrors _walk_recent_messages's contract precisely:
    pagination, continue-on-error per session_id, bounded fan-out,
    skip-corrupted-on-delete with audit-visible counter. Differs only
    in (a) the PK shape (SESSION# prefix on pk vs sessionId column on
    recent-messages) and (b) the projection — STEP rows carry workflow
    state, not free-text consumer content, so the access export returns
    the full row (no _project_* function needed).

    Returns shape mirrors _walk_recent_messages.
    """
    # Defense-in-depth: tenant_id MUST be non-empty even though the walker
    # doesn't use it in the Query. Upstream resolver (form-submissions for
    # email path; channel-mappings TenantIndex GSI for psid path) enforces
    # tenant scoping. If a future caller bypasses both, fail loud.
    if not tenant_id:
        raise ValueError(
            "_walk_session_events requires non-empty tenant_id "
            "(defense-in-depth — table has no tenantId on row; upstream "
            "resolver must enforce tenant scoping)"
        )

    if not session_ids:
        return {"rows_found": 0, "action": "no_sessions"}

    # Bounded fan-out — same cap as _walk_recent_messages.
    truncated_session_id_count = 0
    if len(session_ids) > MAX_SESSION_IDS_PER_INVOCATION:
        truncated_session_id_count = len(session_ids) - MAX_SESSION_IDS_PER_INVOCATION
        session_ids = session_ids[:MAX_SESSION_IDS_PER_INVOCATION]
        logger.warning(
            "session_events_session_ids_truncated: cap=%d overflow=%d",
            MAX_SESSION_IDS_PER_INVOCATION, truncated_session_id_count,
        )

    table = ddb.Table(TABLE_SESSION_EVENTS)

    matched = []
    failed_session_ids = []
    for session_id in session_ids:
        pk_value = f"SESSION#{session_id}"
        last_evaluated_key = None
        while True:
            kwargs = {
                "KeyConditionExpression": Key("pk").eq(pk_value),
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key
            try:
                resp = table.query(**kwargs)
            except ClientError as exc:
                logger.error(
                    "session_events_query_failed: sessionId=%s code=%s",
                    session_id, exc.response.get("Error", {}).get("Code"),
                )
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
        # STEP rows are workflow state (not free-text consumer content);
        # return full rows.
        # Audit fix #9: soft cap on combined export. The upstream
        # MAX_SESSION_IDS_PER_INVOCATION=200 plus ~dozens of STEP rows/session
        # can approach the 6 MB Lambda response cap when summed with the
        # other walkers' exports. MAX_EXPORTED_STEPS caps it; overflow
        # surfaces in exported_steps_truncated_count + an operator followup.
        exported_steps_truncated_count = 0
        export_rows = matched
        if len(export_rows) > MAX_EXPORTED_STEPS:
            exported_steps_truncated_count = len(export_rows) - MAX_EXPORTED_STEPS
            export_rows = export_rows[:MAX_EXPORTED_STEPS]
            logger.warning(
                "session_events_exported_truncated: cap=%d overflow=%d",
                MAX_EXPORTED_STEPS, exported_steps_truncated_count,
            )
        result = {
            "rows_found": rows_found,
            "action": "exported",
            "exported_rows": export_rows,
            **progress_fields,
        }
        if exported_steps_truncated_count:
            result["exported_steps_truncated_count"] = exported_steps_truncated_count
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
        row_pk = row.get("pk")
        row_sk = row.get("sk")
        if row_pk is None or row_sk is None:
            skipped_corrupted += 1
            logger.error(
                "session_events_delete_skipped_corrupted: "
                "pk=%s sk=%s — row missing PK/SK",
                row_pk, row_sk,
            )
            continue
        try:
            table.delete_item(Key={"pk": row_pk, "sk": row_sk})
            deleted += 1
        except ClientError as exc:
            delete_failed += 1
            logger.error(
                "session_events_delete_failed: pk=%s sk=%s code=%s",
                row_pk, row_sk,
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


def _check_archive_mfa_delete_posture():
    """Audit fix #10: one-shot cold-start check of the archive bucket's
    MFA-Delete status. Cached in `_archive_mfa_delete_enabled` for the
    Lambda instance lifetime; refreshed only at the next cold start.

    Returns True if MFA-Delete is enabled (archive deletes will fail),
    False if disabled (proceed). On API error, returns None (proceed but
    log — operator should investigate posture separately).
    """
    global _archive_mfa_delete_enabled
    if _archive_mfa_delete_enabled is not None:
        return _archive_mfa_delete_enabled
    try:
        resp = s3.get_bucket_versioning(Bucket=S3_ARCHIVE_BUCKET)
    except ClientError as exc:
        logger.warning(
            "archive_mfa_delete_check_failed: code=%s — proceeding without "
            "posture verification (operator should run "
            "`aws s3api get-bucket-versioning --bucket %s` to confirm)",
            exc.response.get("Error", {}).get("Code"), S3_ARCHIVE_BUCKET,
        )
        return None
    enabled = resp.get("MFADelete") == "Enabled"
    _archive_mfa_delete_enabled = enabled
    if enabled:
        logger.error(
            "SECURITY: archive bucket %s has MFA-Delete=Enabled — DSAR "
            "delete-real operations WILL FAIL with 403 AccessDenied on "
            "every DeleteObjectVersion call. Operator must disable "
            "MFA-Delete or perform deletes via privileged session with MFA.",
            S3_ARCHIVE_BUCKET,
        )
    return enabled


def _walk_archive_bucket(tenant_id, session_ids, request_type, dry_run):
    """Walk picasso-archive-staging for one subject via session_ids.

    M2 Sprint C walker for the S7 surface. Chained input: session_ids from
    upstream resolver (email path: _walk_form_submissions output; psid path:
    _resolve_psid_subject output).

    Bucket posture (per archive-reachability-decision.md, 2026-05-23):
      - Region: us-east-1 (matches Lambda region)
      - Encryption: SSE-S3 (no CMK)
      - Versioning: ENABLED (7-day NoncurrentVersionExpiration compensating
        control). Walker MUST enumerate versions on delete to fully erase —
        a plain delete_object leaves a delete-marker, not actual erasure.
      - Key shape: sessions/{sessionId}/...

    request_type:
      - "access":            list objects under sessions/{sid}/; return key list
                             (object content NOT fetched — operator-actionable
                             via aws s3 cp on the returned keys; keeps Lambda
                             response under 6 MB)
      - "delete" + dry_run:  list objects + version count; no delete calls
      - "delete" + real:     list_object_versions → delete_object(key, version_id)
                             per (key, version) tuple. The single-shot
                             delete_object (no version_id) is INSUFFICIENT
                             under versioning=ENABLED (leaves prior versions).

    tenant_id is unused by the walk but required for defense-in-depth
    (mirrors _walk_recent_messages / _walk_session_events pattern).
    session_ids are tenant-scoped upstream.

    Continue-on-error per session_id and per (key, version) delete failure
    (mirrors M1 DDB walker patterns).

    Returns:
      {
        "objects_found":              int,   # number of distinct keys
        "versions_found":             int,   # total (key, version) tuples
        "action":                     "exported" | "dry_run_count" | "deleted" | "no_sessions",
        "exported_keys":              [str, ...]                # access only
        "versions_deleted":           int                       # delete-real only
        "versions_delete_failed":     int                       # delete-real only
        "failed_session_ids":         [str, ...]                # set on partial errors
        "truncated_session_id_count": int                       # set when cap hit
      }
    """
    if not tenant_id:
        raise ValueError(
            "_walk_archive_bucket requires non-empty tenant_id "
            "(defense-in-depth — S3 walk has no tenant ARN scoping; upstream "
            "resolver must enforce tenant scoping via the session_id list)"
        )

    if not session_ids:
        return {"objects_found": 0, "versions_found": 0, "action": "no_sessions"}

    truncated_session_id_count = 0
    if len(session_ids) > MAX_SESSION_IDS_PER_INVOCATION:
        truncated_session_id_count = len(session_ids) - MAX_SESSION_IDS_PER_INVOCATION
        session_ids = session_ids[:MAX_SESSION_IDS_PER_INVOCATION]
        logger.warning(
            "archive_session_ids_truncated: cap=%d overflow=%d",
            MAX_SESSION_IDS_PER_INVOCATION, truncated_session_id_count,
        )

    # Collect (key, version_id) tuples across all sessions. Two distinct
    # structures: `keys` (unique object keys for access export) and
    # `versions` (every (key, version_id) tuple for delete enumeration).
    keys = []
    versions = []
    failed_session_ids = []

    for session_id in session_ids:
        prefix = f"{S3_ARCHIVE_KEY_PREFIX}{session_id}/"
        # Use list_object_versions — returns BOTH current + non-current
        # versions in one paginated stream. (list_objects_v2 only returns
        # current versions, missing non-current copies that the 7-day
        # NoncurrentVersionExpiration window allows.)
        continuation_kwargs = {}
        session_had_error = False
        while True:
            try:
                resp = s3.list_object_versions(
                    Bucket=S3_ARCHIVE_BUCKET,
                    Prefix=prefix,
                    **continuation_kwargs,
                )
            except ClientError as exc:
                logger.error(
                    "archive_list_failed: sessionId=%s code=%s",
                    session_id, exc.response.get("Error", {}).get("Code"),
                )
                failed_session_ids.append(session_id)
                session_had_error = True
                break
            for v in resp.get("Versions", []):
                # Audit fix #2: missing VersionId surfaces as skipped+logged,
                # NOT as literal "null" string. delete_object(VersionId="null")
                # would return 400 InvalidArgument silently caught by the
                # continue-on-error loop — operator would see uninformative
                # NoSuchVersion log. None makes boto3 raise pre-call.
                if v.get("VersionId") is None:
                    logger.error("archive_skipped_version_missing_id: key_prefix=sessions/<redacted>")
                    continue
                versions.append((v["Key"], v["VersionId"]))
                keys.append(v["Key"])
            for dm in resp.get("DeleteMarkers", []):
                # Delete-markers themselves must be removed to fully erase
                # the object history (otherwise the marker persists as a row
                # in the version list even after all real versions go).
                if dm.get("VersionId") is None:
                    logger.error("archive_skipped_delete_marker_missing_id: key_prefix=sessions/<redacted>")
                    continue
                versions.append((dm["Key"], dm["VersionId"]))
                # Don't add to `keys` — a delete-marker is not an object
                # the subject can access; only count it for the version
                # erase path.
            if not resp.get("IsTruncated"):
                break
            # list_object_versions pagination uses KeyMarker + VersionIdMarker
            continuation_kwargs = {}
            if resp.get("NextKeyMarker"):
                continuation_kwargs["KeyMarker"] = resp["NextKeyMarker"]
            if resp.get("NextVersionIdMarker"):
                continuation_kwargs["VersionIdMarker"] = resp["NextVersionIdMarker"]
            if not continuation_kwargs:
                # Defensive: no markers but IsTruncated=true → break to avoid
                # infinite loop (boto3 shouldn't return this, but belt-and-
                # suspenders given S3 API surface complexity).
                break
        if session_had_error:
            continue

    # Distinct keys for access export
    unique_keys = sorted(set(keys))
    objects_found = len(unique_keys)
    versions_found = len(versions)

    progress_fields = {}
    if failed_session_ids:
        progress_fields["failed_session_ids"] = failed_session_ids
    if truncated_session_id_count:
        progress_fields["truncated_session_id_count"] = truncated_session_id_count

    if request_type == "access":
        # Return key list only (not object bodies) — operator runs
        # `aws s3 cp` on returned keys if subject requests the content.
        # Keeps Lambda response payload under 6 MB even on chatty archives.
        return {
            "objects_found": objects_found,
            "versions_found": versions_found,
            "action": "exported",
            "exported_keys": unique_keys,
            **progress_fields,
        }

    if dry_run:
        return {
            "objects_found": objects_found,
            "versions_found": versions_found,
            "action": "dry_run_count",
            **progress_fields,
        }

    # Delete-real: enumerate every (key, version_id) tuple.
    # Audit fix #10: cold-start MFA-Delete posture check — fail loud if
    # the bucket has MFA-Delete enabled (every delete would 403 silently).
    if _check_archive_mfa_delete_posture() is True:
        return {
            "objects_found": objects_found,
            "versions_found": versions_found,
            "action": "errored",
            "error": "mfa_delete_enabled",
            "versions_deleted": 0,
            "versions_delete_failed": versions_found,
            **progress_fields,
        }
    versions_deleted = 0
    versions_delete_failed = 0
    for key, version_id in versions:
        try:
            s3.delete_object(
                Bucket=S3_ARCHIVE_BUCKET,
                Key=key,
                VersionId=version_id,
            )
            versions_deleted += 1
        except ClientError as exc:
            versions_delete_failed += 1
            # Audit fix #8: S3 keys under sessions/{sessionId}/* include
            # `sessionId=meta:{pageId}:{psid}` for Meta subjects — PSID is
            # PII per D5 G-H. Mask the suffix in CW Logs; emit only the
            # static path prefix + a hash of the full key for cross-
            # reference if operator needs the exact key from audit row.
            key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
            logger.error(
                "archive_delete_failed: key_prefix=sessions/<redacted> "
                "key_sha256_12=%s version_id=%s code=%s",
                key_hash, version_id,
                exc.response.get("Error", {}).get("Code"),
            )

    return {
        "objects_found": objects_found,
        "versions_found": versions_found,
        "action": "deleted",
        "versions_deleted": versions_deleted,
        "versions_delete_failed": versions_delete_failed,
        **progress_fields,
    }


def _walk_fulfillment_s3(tenant_id, form_submissions_rows, request_type, dry_run):
    """Walk per-tenant S3 fulfillment objects for one subject.

    M2 Sprint D walker per `PII_DELETE_PIPELINE_DESIGN.md` Arm 3. Chained
    input: form-submissions rows from `_walk_form_submissions`. The walker
    reads each row's `fulfillment_path` attribute (when present), parses
    the `s3://bucket/key` URI, and lists/deletes the object.

    Resolution source (Sprint D scope):
      - Per-row `fulfillment_path` attribute on the form-submissions row.
        Form_handler writer does NOT yet stamp this attribute (separate
        follow-up PR). When writer ships, rows written post-extension carry
        a stamped path; pre-extension rows fall back to manual_followup.

    Resolution sources NOT in Sprint D (defer-with-trigger):
      - Tenant config `fulfillment.bucket` synthesis (reading tenant config
        from S3 requires a new IAM scope addition + tenant-config schema
        coupling; out of scope for code-only Sprint D). When writer
        extension ships, the per-row path supersedes the synthesized
        prediction anyway.

    request_type:
      - "access":            list the object via HeadObject; return key list
                             (object body NOT fetched — operator-actionable
                             via aws s3 cp on returned keys; keeps Lambda
                             response under 6 MB)
      - "delete" + dry_run:  count enumerable rows; no delete calls
      - "delete" + real:     s3.delete_object per (bucket, key) tuple

    Design contract (Arm 3):
      - No fulfillment declared anywhere ⇒ no-op (logged + manual_followup)
      - `fulfillment_path` parse failure ⇒ counted in failed_paths; row's
        object un-located but the subject's other surfaces still proceed
      - DeleteObject failure ⇒ hard partial-failure (versions_delete_failed
        > 0 in result; status="errored"); never reported complete
      - IAM grant is resource-ARN-scoped
        `arn:aws:s3:::{bucket}/submissions/{tenant_id}/*` per known
        (bucket, tenant_id) pair (cannot use s3:prefix — that's
        ListBucket-only). Unknown bucket ⇒ DeleteObject returns
        AccessDenied ⇒ hard partial-failure until IAM policy updated.
        "Fail-closed, never silent" is the design intent — the walker does
        NOT attempt to mask AccessDenied as success.

    tenant_id is defense-in-depth (mirrors _walk_archive_bucket pattern).
    Per-row `fulfillment_path` should always begin with
    `s3://<bucket>/submissions/{tenant_id}/...` per the form_handler writer
    contract (form_handler.py:992-1007). Walker validates the tenant_id
    segment matches the requested tenant; any mismatch is rejected as
    suspected cross-tenant pointer (hard skip + log; never delete).

    Returns:
      {
        "objects_found":          int,   # number of (bucket, key) pairs
        "action":                 "exported" | "dry_run_count" | "deleted" | "no_fulfillment_paths",
        "exported_keys":          [str, ...]                 # access only — "s3://bucket/key" URIs
        "objects_deleted":        int                        # delete-real only
        "objects_delete_failed":  int                        # delete-real only
        "rows_with_path":         int                        # forward-compat metric
        "rows_without_path":      int                        # pre-extension rows
        "failed_paths":           int                        # parse-failure count
        "skipped_cross_tenant":   int                        # tenant_id mismatch
      }
    """
    if not tenant_id:
        raise ValueError(
            "_walk_fulfillment_s3 requires non-empty tenant_id "
            "(defense-in-depth — per-row fulfillment_path is validated "
            "against the requested tenant_id segment; any mismatch is "
            "rejected as suspected cross-tenant pointer)"
        )

    rows_with_path = 0
    rows_without_path = 0
    failed_paths = 0
    skipped_cross_tenant = 0
    enumerated = []  # [(bucket, key)]

    for row in form_submissions_rows or []:
        # Schema discipline: writer extension is a future PR; tolerate
        # absence on every row without crashing the walker.
        path = row.get("fulfillment_path")
        if not path:
            rows_without_path += 1
            continue
        # Expected shape: s3://<bucket>/submissions/<tenant_id>/<form_type>/<submission_id>.json
        # Per form_handler.py:992-1007 writer. Reject anything that
        # doesn't match the s3:// scheme + tenant segment.
        if not isinstance(path, str) or not path.startswith("s3://"):
            failed_paths += 1
            logger.error(
                "fulfillment_path_parse_failed: path_type=%s starts_with_s3=%s",
                type(path).__name__,
                isinstance(path, str) and path.startswith("s3://"),
            )
            continue
        # Strip s3:// and split bucket from key
        without_scheme = path[len("s3://"):]
        slash_idx = without_scheme.find("/")
        if slash_idx <= 0:
            failed_paths += 1
            logger.error("fulfillment_path_parse_failed: no_key_in_uri")
            continue
        bucket = without_scheme[:slash_idx]
        key = without_scheme[slash_idx + 1:]
        # Tenant-segment validation: key shape is
        # submissions/{tenant_id}/{form_type}/{submission_id}.json per
        # form_handler writer. Reject any key whose tenant segment doesn't
        # match the requested tenant_id — defense against a stale or
        # tampered row pointing at another tenant's bucket prefix.
        #
        # Audit closure 2026-05-26 row #5 (Security-Reviewer 🔴): the prefix
        # check `startswith` is a literal-string match — S3 does NOT
        # canonicalize `..` in object keys, so a key like
        # `submissions/{tenant_id}/../OTHER/x.json` passes the prefix check
        # AND is the literal key S3 stores/deletes. Reject any key whose
        # path segments contain `..` (or other obviously-malformed segments)
        # before the prefix check. Also reject any non-ASCII-printable
        # characters which could indicate URL-encoded bypass attempts
        # (e.g., `%54EN-SMOKE-FULFILL` — note: S3 percent-DECODES the key on
        # the wire but the Python string contains the literal `%XX` form
        # which won't match the plain-ASCII expected_prefix; the literal
        # match correctly rejects encoded variants. The `..` rejection is
        # the load-bearing fix).
        key_segments = key.split("/")
        if ".." in key_segments or "" in key_segments[:-1]:
            failed_paths += 1
            logger.error(
                "fulfillment_path_parse_failed: path_traversal_or_empty_segment "
                "bucket=%s",
                bucket,
            )
            continue
        expected_prefix = f"submissions/{tenant_id}/"
        if not key.startswith(expected_prefix):
            skipped_cross_tenant += 1
            # Mask the actual path in logs (the suffix may contain
            # submission_id which is operator-only — see D1 redaction rule).
            logger.error(
                "fulfillment_path_cross_tenant_rejected: bucket=%s "
                "expected_prefix=%s",
                bucket, expected_prefix,
            )
            continue
        rows_with_path += 1
        enumerated.append((bucket, key))

    objects_found = len(enumerated)

    progress_fields = {
        "rows_with_path": rows_with_path,
        "rows_without_path": rows_without_path,
    }
    if failed_paths:
        progress_fields["failed_paths"] = failed_paths
    if skipped_cross_tenant:
        progress_fields["skipped_cross_tenant"] = skipped_cross_tenant

    # Audit closure 2026-05-26 row #18 (Security-Reviewer 🟡): record a
    # per-object sha256[:12] hash so the audit trail can attest per-object
    # deletion without exposing the raw submission_id (D1 redaction rule).
    # Hash is computed over the full `bucket/key` tuple so cross-bucket
    # collisions are not possible.
    key_hashes = [
        hashlib.sha256(f"{b}/{k}".encode("utf-8")).hexdigest()[:12]
        for b, k in enumerated
    ]

    if objects_found == 0:
        return {
            "objects_found": 0,
            "action": "no_fulfillment_paths",
            **progress_fields,
        }

    if request_type == "access":
        # Return URIs as `s3://bucket/key` — operator runs `aws s3 cp`
        # on each to retrieve content. Keeps Lambda response under 6 MB.
        return {
            "objects_found": objects_found,
            "action": "exported",
            "exported_keys": [f"s3://{b}/{k}" for b, k in enumerated],
            "key_sha256_12": key_hashes,
            **progress_fields,
        }

    if dry_run:
        return {
            "objects_found": objects_found,
            "action": "dry_run_count",
            "key_sha256_12": key_hashes,
            **progress_fields,
        }

    # Delete-real: per-object DeleteObject. Fulfillment buckets do not
    # have versioning by contract (form_handler writer does single
    # put_object; not versioning-aware). A single DeleteObject is
    # sufficient — unlike `_walk_archive_bucket` which must enumerate
    # versions because picasso-archive-staging has versioning=ENABLED.
    objects_deleted = 0
    objects_delete_failed = 0
    deleted_hashes = []
    failed_hashes = []
    for (bucket, key), key_hash in zip(enumerated, key_hashes):
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            objects_deleted += 1
            deleted_hashes.append(key_hash)
        except ClientError as exc:
            objects_delete_failed += 1
            failed_hashes.append(key_hash)
            # Mask the key suffix per D1 redaction rule (submission_id is
            # operator-only); emit bucket + 12-char hash of the full key
            # for cross-reference in audit_row + manual_followup.
            logger.error(
                "fulfillment_delete_failed: bucket=%s "
                "key_prefix=submissions/<redacted> key_sha256_12=%s code=%s",
                bucket, key_hash,
                exc.response.get("Error", {}).get("Code"),
            )

    return {
        "objects_found": objects_found,
        "action": "deleted",
        "objects_deleted": objects_deleted,
        "objects_delete_failed": objects_delete_failed,
        "deleted_key_sha256_12": deleted_hashes,
        "failed_key_sha256_12": failed_hashes,
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
        f"  aws dynamodb query --table-name picasso-recent-messages \\\n"
        f"    --profile myrecruiter-staging \\\n"
        f"    --key-condition-expression 'sessionId = :s' \\\n"
        f"    --expression-attribute-values "
        f"'{{\":s\":{{\"S\":\"<SESSION_ID-from-out-of-band-source>\"}}}}'\n"
        f"  # Last-resort content-substring scan (CASE-SENSITIVE; false positives likely):\n"
        f"  aws dynamodb scan --table-name picasso-recent-messages \\\n"
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
        f"  aws dynamodb query --table-name picasso-notification-sends \\\n"
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


def _walk_mfs_surfaces(pii_subject_id, tenant_id, normalized_email, request_type, dry_run,
                       tenant_hash=None):
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
        # Audit fix #1: session-events surface added to email-path dispatcher
        # (Sprint A walker matrix §4 row S6 required this; Sprint B missed it).
        "session-events": 0,
        "archive": 0,
        # M2 Sprint D: per-tenant S3 fulfillment walker chained off the
        # form-submissions matched rows (reads each row's fulfillment_path).
        "fulfillment": 0,
        # F-DSAR31 (closed): session-summaries, reached via operator-passed
        # tenant_hash. Stays 0 when tenant_hash is absent (surface skipped).
        "session-summaries": 0,
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
        manual_followups.append(
            "session-events: skipped (chained walker requires form-submissions "
            "session_ids; no subject resolved → no chain)"
        )
        walker_results["session-events"] = {"status": "skipped_no_subject"}
        manual_followups.append(
            "archive: skipped (chained walker requires form-submissions "
            "session_ids; no subject resolved → no chain)"
        )
        walker_results["archive"] = {"status": "skipped_no_subject"}
        manual_followups.append(
            "fulfillment: skipped (chained walker requires form-submissions "
            "matched rows; no subject resolved → no chain)"
        )
        walker_results["fulfillment"] = {"status": "skipped_no_subject"}
        manual_followups.append(
            "session-summaries: skipped (filters on pii_subject_id; no subject "
            "resolved)"
        )
        walker_results["session-summaries"] = {"status": "skipped_no_subject"}
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
    # M2 Sprint D: capture matched rows (PRE-delete; the Python list still
    # references the original dicts even after DDB rows are deleted) so the
    # fulfillment walker can extract per-row `fulfillment_path` attributes.
    captured_matched_rows = fs.get("matched_rows", []) if fs else []
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

    # session-events: audit fix #1 — chained walk via the same form-
    # submissions session_ids. Sprint A walker matrix §4 row S6 required
    # session-events on BOTH email + psid paths; Sprint B missed the email
    # path. session_ids are tenant-scoped upstream by _walk_form_submissions
    # (Query bounded by pii_subject_id + tenant_id).
    _apply_session_events_walker_result(
        tenant_id, captured_session_ids, request_type, dry_run,
        rows_touched, manual_followups, exported_rows, walker_results,
    )

    # archive: M2 Sprint C — chained walk via the same form-submissions
    # session_ids. session_ids are tenant-scoped upstream by
    # _walk_form_submissions (Query bounded by pii_subject_id + tenant_id).
    _apply_archive_walker_result(
        tenant_id, captured_session_ids, request_type, dry_run,
        rows_touched, manual_followups, exported_rows, walker_results,
    )

    # fulfillment: M2 Sprint D — chained walk via the form-submissions
    # matched rows (read each row's `fulfillment_path` attribute). Email
    # path only — psid subjects don't submit forms, so there is no
    # fulfillment surface in `_walk_psid_surfaces`.
    _apply_fulfillment_walker_result(
        tenant_id, captured_matched_rows, request_type, dry_run,
        rows_touched, manual_followups, exported_rows, walker_results,
    )

    # session-summaries (F-DSAR31, closed): reachable only with the operator-
    # passed tenant_hash (partition pk=TENANT#{tenant_hash}). When absent, skip
    # the surface gracefully with a manual_followup rather than guessing the
    # hash — all other surfaces have already walked.
    if tenant_hash:
        ss = _walk_session_summaries(pii_subject_id, tenant_hash, request_type, dry_run)
        rows_touched["session-summaries"] = ss["rows_found"]
        if ss.get("error"):
            manual_followups.append(
                f"session-summaries: query failed ({ss['error']}); retry advised"
            )
            walker_results["session-summaries"] = {
                "status": "errored", "error": ss["error"],
                "rows_touched": ss["rows_found"],
            }
        elif ss.get("action") == "exported":
            exported_rows["session-summaries"] = ss["exported_rows"]
            walker_results["session-summaries"] = {
                "status": "completed", "action": "exported",
                "rows_touched": ss["rows_found"],
            }
        elif ss.get("action") == "dry_run_count":
            manual_followups.append(
                f"session-summaries: {ss['rows_found']} row(s) match (dry-run; "
                f"none deleted)"
            )
            walker_results["session-summaries"] = {
                "status": "completed", "action": "dry_run_count",
                "rows_touched": ss["rows_found"],
            }
        else:  # deleted
            if ss.get("rows_delete_failed"):
                manual_followups.append(
                    f"session-summaries: {ss['rows_delete_failed']} row(s) failed "
                    f"to delete — retry advised"
                )
            if ss.get("rows_skipped_corrupted"):
                manual_followups.append(
                    f"session-summaries: {ss['rows_skipped_corrupted']} corrupted "
                    f"row(s) skipped"
                )
            walker_results["session-summaries"] = {
                "status": "completed", "action": "deleted",
                "rows_touched": ss.get("rows_deleted", 0),
            }
    else:
        manual_followups.append(
            "session-summaries: skipped — tenant_hash not provided on the DSAR "
            "event. This surface (pk=TENANT#{tenant_hash}) requires the tenant's "
            "hash; re-invoke with a `tenant_hash` field to include it. All other "
            "surfaces walked normally."
        )
        walker_results["session-summaries"] = {
            "status": "deferred",
            "reason": "tenant_hash not provided on the DSAR event",
        }

    # Other surfaces: still scaffolded.
    for surface, reason in MFS_SCOPED_SURFACES.items():
        manual_followups.append(f"{surface}: {reason}")
        walker_results[surface] = {"status": "deferred", "reason": reason}

    return rows_touched, manual_followups, exported_rows, walker_results


def _walk_psid_surfaces(tenant_id, psid, session_ids, request_type, dry_run):
    """Dispatch per-surface walkers for the psid path (M2 Sprint B).

    Parallel to _walk_mfs_surfaces but for identifier_type=psid. The
    surfaces walked are a SUBSET of the email-path surfaces — Meta-only
    subjects appear in conversation surfaces (recent-messages,
    session-events) but NOT in form-keyed surfaces (form-submissions,
    notification-sends/events were never written for a PSID-only subject;
    those are email-keyed by definition).

    session-summaries deferred to a follow-on sprint per D5 row F-DSAR31
    (requires tenant_hash resolution — session-summaries pk uses
    TENANT#{tenant_hash} not tenant_id; Sprint B narrows to surfaces
    reachable without tenant_hash discovery).

    Returns the same shape as _walk_mfs_surfaces.

    Cross-tenant isolation: session_ids are pre-composed by
    _resolve_psid_subject as "meta:{pageId}:{psid}" where pageId belongs
    to the requested tenant per the TenantIndex GSI Query. The walkers
    here accept session_ids opaquely.
    """
    rows_touched = {
        "recent-messages": 0,
        "session-events": 0,
        "archive": 0,
    }
    manual_followups = []
    exported_rows = {}
    walker_results = {}

    if not session_ids:
        # tenant has no Meta pages OR pages had no matching sessions for
        # this psid. Surface as completed-with-no-data; operator may need
        # to verify the PSID is correct, or the subject's TTL may have
        # purged the rows already (recent-messages has a 7-day
        # TTL per Meta_Response_Processor).
        manual_followups.append(
            f"psid path: 0 sessionIds resolved for tenant {tenant_id!r} + "
            f"psid {psid!r}. Verify: (a) tenant has Messenger channel "
            f"configured (channel-mappings TenantIndex GSI Query); (b) "
            f"PSID belongs to a page in the tenant's channel mappings; "
            f"(c) subject's messages may have aged out via 7-day TTL on "
            f"recent-messages."
        )
        walker_results["recent-messages"] = {
            "status": "completed",
            "action": "no_sessions",
            "rows_touched": 0,
        }
        walker_results["session-events"] = {
            "status": "completed",
            "action": "no_sessions",
            "rows_touched": 0,
        }
        walker_results["archive"] = {
            "status": "completed",
            "action": "no_sessions",
            "rows_touched": 0,
        }
        # Note: session-summaries is intentionally NOT in this dict — it's
        # deferred to F-DSAR31. Including it here would noise up the audit
        # log and the operator-facing response.
        return rows_touched, manual_followups, exported_rows, walker_results

    # recent-messages: reuse the M1 walker. session_ids are tenant-scoped
    # upstream via channel-mappings TenantIndex GSI.
    rm = _walk_recent_messages(tenant_id, session_ids, request_type, dry_run)
    rows_touched["recent-messages"] = rm["rows_found"]
    if rm.get("error"):
        manual_followups.append(
            f"recent-messages: query failed ({rm['error']}); retry advised"
        )
        walker_results["recent-messages"] = {
            "status": "errored",
            "error": rm["error"],
            "rows_touched": rm["rows_found"],
        }
    elif rm.get("action") == "no_sessions":
        walker_results["recent-messages"] = {
            "status": "completed",
            "action": "no_sessions",
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
            f"recent-messages: dry_run=true; {rm['rows_found']} row(s) would "
            f"be deleted; re-invoke with dry_run=false to delete"
        )
        walker_results["recent-messages"] = {
            "status": "completed",
            "action": "dry_run_count",
            "rows_touched": rm["rows_found"],
        }
    else:  # action == "deleted"
        result = {
            "status": "completed",
            "action": "deleted",
            "rows_touched": rm["rows_found"],
            "rows_deleted": rm.get("rows_deleted", 0),
            "rows_skipped_corrupted": rm.get("rows_skipped_corrupted", 0),
        }
        if rm.get("rows_skipped_corrupted", 0) > 0:
            manual_followups.append(
                f"recent-messages: {rm['rows_skipped_corrupted']} row(s) "
                f"skipped due to corrupted PK/SK schema — see CloudWatch "
                f"logs for sessionId+messageTimestamp values; manual "
                f"inspection required."
            )
            result["status"] = "errored"
            result["error"] = "rows_skipped_corrupted"
        walker_results["recent-messages"] = result

    # session-events: M2 Sprint B walker. Audit fix #1: now extracted into
    # _apply_session_events_walker_result helper for reuse on the email path
    # (was Sprint B oversight — email-path dispatcher missed this surface).
    _apply_session_events_walker_result(
        tenant_id, session_ids, request_type, dry_run,
        rows_touched, manual_followups, exported_rows, walker_results,
    )

    # archive: M2 Sprint C — list versions under sessions/{sessionId}/ for
    # each Meta sessionId. Version-aware delete required (versioning ENABLED
    # per archive-reachability-decision.md). session_ids same as upstream
    # walkers; tenant-scoped via the channel-mappings GSI Query.
    _apply_archive_walker_result(
        tenant_id, session_ids, request_type, dry_run,
        rows_touched, manual_followups, exported_rows, walker_results,
    )

    return rows_touched, manual_followups, exported_rows, walker_results


def _apply_archive_walker_result(
    tenant_id, session_ids, request_type, dry_run,
    rows_touched, manual_followups, exported_rows, walker_results,
):
    """Invoke _walk_archive_bucket and fold its output into the dispatcher's
    accumulators. Extracted to a helper because both _walk_psid_surfaces
    (M2 Sprint C path) and _walk_mfs_surfaces (M2 Sprint C path) share the
    exact same archive walker invocation + result-handling shape.

    Mutates rows_touched, manual_followups, exported_rows, walker_results
    in place. No return value.

    Surface key in walker_results: "archive".
    """
    arc = _walk_archive_bucket(tenant_id, session_ids, request_type, dry_run)
    # Archive walker returns objects_found (distinct keys) + versions_found
    # (total tuples). Use objects_found for rows_touched display — that's
    # what the operator-facing audit row shows; versions_found is also
    # propagated into walker_results for the audit payload (audit fix #20 —
    # regulator-defense: erasure work was N versions, not just 1 object).
    rows_touched["archive"] = arc["objects_found"]

    # Audit fix #18: removed dead `arc.get("error")` branch — the archive
    # walker never returns a top-level "error" key; errors surface via
    # `failed_session_ids` in progress_fields. Per-session failures are
    # now propagated as walker-error status below (was previously dropped,
    # leaving status="completed" even when N sessions failed enumeration).
    failed_session_ids = arc.get("failed_session_ids", [])

    if arc.get("action") == "no_sessions":
        walker_results["archive"] = {
            "status": "completed",
            "action": "no_sessions",
            "rows_touched": 0,
        }
        return

    base_result = {
        "rows_touched": arc["objects_found"],
        "versions_found": arc["versions_found"],
    }
    if failed_session_ids:
        manual_followups.append(
            f"archive: {len(failed_session_ids)} session_id(s) failed to "
            f"enumerate — see CloudWatch logs for session ids; retry advised "
            f"or manual `aws s3api list-object-versions --prefix sessions/<sid>/`."
        )
        base_result["failed_session_ids_count"] = len(failed_session_ids)

    if arc.get("action") == "exported":
        exported_rows["archive"] = arc["exported_keys"]
        walker_results["archive"] = {
            **base_result,
            "status": "errored" if failed_session_ids else "completed",
            "action": "exported",
            **({"error": "failed_session_ids"} if failed_session_ids else {}),
        }
        return

    if arc.get("action") == "dry_run_count":
        manual_followups.append(
            f"archive: dry_run=true; {arc['objects_found']} object(s) / "
            f"{arc['versions_found']} version(s) would be deleted; "
            f"re-invoke with dry_run=false to delete"
        )
        walker_results["archive"] = {
            **base_result,
            "status": "errored" if failed_session_ids else "completed",
            "action": "dry_run_count",
            **({"error": "failed_session_ids"} if failed_session_ids else {}),
        }
        return

    # action == "deleted"
    result = {
        **base_result,
        "status": "completed",
        "action": "deleted",
        "versions_deleted": arc.get("versions_deleted", 0),
        "versions_delete_failed": arc.get("versions_delete_failed", 0),
    }
    if arc.get("versions_delete_failed", 0) > 0:
        manual_followups.append(
            f"archive: {arc['versions_delete_failed']} version(s) failed "
            f"to delete — see CloudWatch logs for (key_sha256_12, version_id) "
            f"values; manual `aws s3api delete-object` required."
        )
        result["status"] = "errored"
        result["error"] = "versions_delete_failed"
    elif failed_session_ids:
        result["status"] = "errored"
        result["error"] = "failed_session_ids"
    walker_results["archive"] = result


def _apply_fulfillment_walker_result(
    tenant_id, form_submissions_rows, request_type, dry_run,
    rows_touched, manual_followups, exported_rows, walker_results,
):
    """Invoke _walk_fulfillment_s3 and fold its output into the dispatcher's
    accumulators. M2 Sprint D helper — per-tenant S3 fulfillment walker is
    chained off the email-path form-submissions walker only (psid path does
    NOT submit forms, so there is no fulfillment surface to walk).

    Mutates rows_touched, manual_followups, exported_rows, walker_results
    in place. No return value.

    Surface key in walker_results: "fulfillment".
    """
    fr = _walk_fulfillment_s3(
        tenant_id, form_submissions_rows, request_type, dry_run,
    )
    rows_touched["fulfillment"] = fr["objects_found"]

    base_result = {
        "rows_touched": fr["objects_found"],
        "rows_with_path": fr.get("rows_with_path", 0),
        "rows_without_path": fr.get("rows_without_path", 0),
    }
    if fr.get("failed_paths"):
        base_result["failed_paths"] = fr["failed_paths"]
    if fr.get("skipped_cross_tenant"):
        base_result["skipped_cross_tenant"] = fr["skipped_cross_tenant"]

    # Surface the writer-extension-pending gap whenever any matched form-
    # submission row lacked `fulfillment_path`. This is the dominant case
    # today (writer extension is a follow-up PR per the module docstring);
    # operator may need to manually walk per-tenant bucket if subject's
    # rows pre-date the writer extension.
    if fr.get("rows_without_path", 0) > 0:
        manual_followups.append(
            f"fulfillment: {fr['rows_without_path']} form-submission row(s) "
            f"lack `fulfillment_path` attribute (writer extension pending — "
            f"see module docstring 'WHAT IT DOES NOT YET DO'). Per-row path "
            f"defense (N3 stale-config) is unavailable for these rows. "
            f"Operator-actionable: query tenant config for "
            f"`forms.<form_type>.fulfillment.bucket` and manually list "
            f"`s3://<bucket>/submissions/{tenant_id}/<form_type>/<submission_id>.json` "
            f"for any submission_id without path."
        )

    if fr.get("failed_paths", 0) > 0:
        manual_followups.append(
            f"fulfillment: {fr['failed_paths']} row(s) had unparseable "
            f"`fulfillment_path` values (not s3:// URI); see CloudWatch "
            f"logs. Manual inspection required."
        )
    if fr.get("skipped_cross_tenant", 0) > 0:
        manual_followups.append(
            f"fulfillment: {fr['skipped_cross_tenant']} row(s) had "
            f"`fulfillment_path` pointing OUTSIDE the requested tenant "
            f"prefix — rejected as cross-tenant pointer (writer drift OR "
            f"stale row). NOT deleted; manual inspection required."
        )

    if fr.get("action") == "no_fulfillment_paths":
        walker_results["fulfillment"] = {
            **base_result,
            "status": "completed",
            "action": "no_fulfillment_paths",
        }
        return

    if fr.get("action") == "exported":
        exported_rows["fulfillment"] = fr["exported_keys"]
        walker_results["fulfillment"] = {
            **base_result,
            "status": "completed",
            "action": "exported",
            # Audit row #18: per-object sha256[:12] hashes propagated to the
            # audit event for per-object deletion attestability.
            "key_sha256_12": fr.get("key_sha256_12", []),
        }
        return

    if fr.get("action") == "dry_run_count":
        manual_followups.append(
            f"fulfillment: dry_run=true; {fr['objects_found']} object(s) "
            f"would be deleted; re-invoke with dry_run=false to delete"
        )
        walker_results["fulfillment"] = {
            **base_result,
            "status": "completed",
            "action": "dry_run_count",
            "key_sha256_12": fr.get("key_sha256_12", []),
        }
        return

    # action == "deleted"
    result = {
        **base_result,
        "status": "completed",
        "action": "deleted",
        "objects_deleted": fr.get("objects_deleted", 0),
        "objects_delete_failed": fr.get("objects_delete_failed", 0),
        # Audit row #18: per-object hashes recorded in the audit row so an
        # operator can attest "object X was deleted" without exposing the
        # raw submission_id (D1 redaction).
        "deleted_key_sha256_12": fr.get("deleted_key_sha256_12", []),
        "failed_key_sha256_12": fr.get("failed_key_sha256_12", []),
    }
    if fr.get("objects_delete_failed", 0) > 0:
        # Per-design "hard partial-failure, never silent". Most common
        # cause: AccessDenied because the (bucket, tenant_id) IAM grant
        # has not been added per the runbook §14 Q4 procedure (Arm 3
        # design intent — fail-closed on unknown bucket).
        manual_followups.append(
            f"fulfillment: {fr['objects_delete_failed']} object(s) failed "
            f"to delete — see CloudWatch logs for (bucket, key_sha256_12) "
            f"values. Most likely cause: missing IAM grant for "
            f"`s3:DeleteObject` on `arn:aws:s3:::<bucket>/submissions/"
            f"{tenant_id}/*` (Sprint D walker code-only; per-(bucket, "
            f"tenant_id) IAM additions ship in follow-up PRs per "
            f"PII_DELETE_PIPELINE_DESIGN.md §14 Q4)."
        )
        result["status"] = "errored"
        result["error"] = "objects_delete_failed"
    walker_results["fulfillment"] = result


def _apply_session_events_walker_result(
    tenant_id, session_ids, request_type, dry_run,
    rows_touched, manual_followups, exported_rows, walker_results,
):
    """Invoke _walk_session_events and fold its output into the dispatcher's
    accumulators. Audit fix #1: extracted to a helper so the email-path
    dispatcher (_walk_mfs_surfaces) can reuse the same integration block
    that the psid-path dispatcher (_walk_psid_surfaces) shipped in Sprint B.
    Sprint A design walker matrix §4 line 76 specified session-events as a
    walked surface on BOTH email + psid paths; Sprint B only wired the psid
    path. This helper closes that gap.

    Mutates rows_touched, manual_followups, exported_rows, walker_results
    in place. No return value.

    Surface key in walker_results: "session-events".
    """
    se = _walk_session_events(tenant_id, session_ids, request_type, dry_run)
    rows_touched["session-events"] = se["rows_found"]
    if se.get("action") == "no_sessions":
        walker_results["session-events"] = {
            "status": "completed",
            "action": "no_sessions",
            "rows_touched": 0,
        }
        return
    if se.get("action") == "exported":
        exported_rows["session-events"] = se["exported_rows"]
        result = {
            "status": "completed",
            "action": "exported",
            "rows_touched": se["rows_found"],
        }
        # Audit fix #9 sibling: surface the truncation flag if soft cap hit.
        if se.get("exported_steps_truncated_count"):
            result["exported_steps_truncated_count"] = se["exported_steps_truncated_count"]
            manual_followups.append(
                f"session-events: access export truncated at "
                f"MAX_EXPORTED_STEPS={MAX_EXPORTED_STEPS}; "
                f"{se['exported_steps_truncated_count']} STEP row(s) elided."
            )
        walker_results["session-events"] = result
        return
    if se.get("action") == "dry_run_count":
        manual_followups.append(
            f"session-events: dry_run=true; {se['rows_found']} row(s) would "
            f"be deleted; re-invoke with dry_run=false to delete"
        )
        walker_results["session-events"] = {
            "status": "completed",
            "action": "dry_run_count",
            "rows_touched": se["rows_found"],
        }
        return
    # action == "deleted"
    # Audit fix #19: propagate rows_delete_failed from the walker. Pre-fix,
    # the dispatcher dropped this field and only checked corrupted rows for
    # status tainting — per-row delete failures were invisible to operator.
    result = {
        "status": "completed",
        "action": "deleted",
        "rows_touched": se["rows_found"],
        "rows_deleted": se.get("rows_deleted", 0),
        "rows_delete_failed": se.get("rows_delete_failed", 0),
        "rows_skipped_corrupted": se.get("rows_skipped_corrupted", 0),
    }
    if se.get("rows_skipped_corrupted", 0) > 0:
        manual_followups.append(
            f"session-events: {se['rows_skipped_corrupted']} row(s) "
            f"skipped due to corrupted PK/SK schema — see CloudWatch "
            f"logs for pk+sk values; manual inspection required."
        )
        result["status"] = "errored"
        result["error"] = "rows_skipped_corrupted"
    if se.get("rows_delete_failed", 0) > 0:
        manual_followups.append(
            f"session-events: {se['rows_delete_failed']} row(s) failed "
            f"DeleteItem — see CloudWatch logs for pk+sk values; manual "
            f"`aws dynamodb delete-item` required."
        )
        result["status"] = "errored"
        result.setdefault("error", "rows_delete_failed")
    walker_results["session-events"] = result


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
            is_smoke_test=inputs.get("smoke_test_marker", False),
        )
    except AuditCollision as exc:
        return {
            "dsar_id": dsar_id,
            "status": "failed",
            "error": "audit_collision",
            "message": str(exc),
        }

    # E2 (PR1 fix-now-4): subject resolution is a DDB call — ClientError
    # (throttle, AccessDenied, network) previously propagated uncaught and
    # crashed the Lambda with no audit row. Now: audit-write the failure and
    # return failed cleanly. subject_identifier NOT logged/returned (consumer PII).
    #
    # M2 Sprint B: branch on identifier_type.
    #   - email path: _resolve_subject → pii_subject_id → _walk_mfs_surfaces
    #     (email-keyed surfaces: form-submissions + notification-sends/events
    #     + recent-messages chained from form-submissions session_ids).
    #   - psid path: _resolve_psid_subject → list of Meta sessionIds →
    #     _walk_psid_surfaces (recent-messages + session-events).
    # pii_subject_id is None on the psid path (Meta-only subjects have no
    # subject-index entry); the close-event payload reflects this honestly.
    pii_subject_id = None
    psid_session_ids = None
    try:
        if inputs["identifier_type"] == "email":
            pii_subject_id = _resolve_subject(
                tenant_id=inputs["tenant_id"],
                normalized_email=inputs["subject_identifier"],
            )
        else:  # identifier_type == "psid" — guaranteed by _validate
            psid_session_ids = _resolve_psid_subject(
                tenant_id=inputs["tenant_id"],
                psid=inputs["subject_identifier"],
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
                    "identifier_type": inputs["identifier_type"],
                    "error_code": error_code,
                },
                is_smoke_test=inputs.get("smoke_test_marker", False),
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
            "message": f"DDB ClientError on subject resolution: {error_code}",
        }
    if inputs["identifier_type"] == "email":
        logger.info(
            "dsar_subject_resolved: dsar_id=%s tenant=%s found=%s",
            dsar_id, inputs["tenant_id"], pii_subject_id is not None,
        )
    else:
        logger.info(
            "dsar_psid_subject_resolved: dsar_id=%s tenant=%s sessionIds=%d",
            dsar_id, inputs["tenant_id"], len(psid_session_ids),
        )

    if inputs["identifier_type"] == "email":
        rows_touched, manual_followups, exported_rows, walker_results = (
            _walk_mfs_surfaces(
                pii_subject_id=pii_subject_id,
                tenant_id=inputs["tenant_id"],
                normalized_email=inputs["subject_identifier"],
                request_type=inputs["request_type"],
                dry_run=inputs["dry_run"],
                tenant_hash=inputs["tenant_hash"],
            )
        )
    else:  # psid path
        rows_touched, manual_followups, exported_rows, walker_results = (
            _walk_psid_surfaces(
                tenant_id=inputs["tenant_id"],
                psid=inputs["subject_identifier"],
                session_ids=psid_session_ids,
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
                is_smoke_test=inputs.get("smoke_test_marker", False),
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
            is_smoke_test=inputs.get("smoke_test_marker", False),
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
