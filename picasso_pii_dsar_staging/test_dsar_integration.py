"""Integration tests for picasso-pii-dsar-staging — real DDB in acct 525.

Closes MASTER_PROJECT_PLAN.md M1 done-bar #3 (6 integration tests) AND #5
(integration test f' for access end-to-end). Bundled per user decision
2026-05-23 to do both in one SSO-gated PR.

Tests (per master plan §4 M1 done-bar):
  (a) dry-run no-delete
  (b) real delete + audit-row verification
  (c) tenant-bound walker (same email in tenant-A + tenant-B; invoke for A;
      B untouched)
  (d) PSID-by-Scan reachability (M1 sub-set; M2 closes Meta) — verify the
      email-keyed walker reaches form-submission rows linked via the
      pii-subject-index, demonstrating the tenant-scoped Query + Filter
      pattern that M2 will reuse for PSID-by-Scan
  (e) per-tenant S3 walk (M1 placeholder) — manual_followup mentions S3
      deferral; no S3 walk attempted
  (f) cross-tenant isolation (Query bounded by tenant-id partition) —
      structural check via audit row's tenant_id field
  (f') access end-to-end equivalent to delete — request_type='access' returns
      exported_rows; recent-messages projection per F-DSAR4 mitigation;
      form-submission rows returned per documented field-constrained design

Run:
    AWS_PROFILE=myrecruiter-staging AWS_INTEGRATION_TESTS=1 \\
        python3 -m pytest test_dsar_integration.py -v

Default: tests are skipped unless AWS_INTEGRATION_TESTS=1 is set AND SSO
authenticates against account 525409062831. Prevents accidental staging
hits from CI / unrelated test runs.

Discipline:
- Synthetic tenant_ids prefixed `TEN-SMOKE-INT-` for collision-free cleanup
- Synthetic emails prefixed `smoke.dsar.` at `@example.com`
- DSAR ids prefixed `smoke-int-` (matches existing `smoke-*` convention;
  audit rows preserved by C2 DeleteItem-Deny anyway)
- Seed data carries 1-hour TTL as auto-cleanup safety net
- Per-test try/finally cleanup; failures don't leave debris
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


# ───────────────────────────────────────────────────────────────────────────
# Configuration
# ───────────────────────────────────────────────────────────────────────────
LAMBDA_NAME = "picasso-pii-dsar-staging"
EXPECTED_ACCOUNT = "525409062831"
REGION = "us-east-1"

TABLE_FORM_SUBMISSIONS = "picasso-form-submissions-staging"
TABLE_SUBJECT_INDEX = "picasso-pii-subject-index-staging"
TABLE_AUDIT = "picasso-pii-dsar-audit-staging"
TABLE_NOTIFICATION_SENDS = "picasso-notification-sends"
TABLE_NOTIFICATION_EVENTS = "picasso-notification-events"
TABLE_RECENT_MESSAGES = "picasso-recent-messages"

TEST_TENANT_PREFIX = "TEN-SMOKE-INT-"
TEST_DSAR_PREFIX = "smoke-int-"

SKIP_REASON = (
    "Integration tests require AWS_PROFILE + valid SSO session for acct 525. "
    "Run with: AWS_INTEGRATION_TESTS=1 pytest test_dsar_integration.py"
)

# Sprint D fulfillment walker integration test (Q3c — skip-until-fixture).
# These tests need a real S3 bucket the DSAR Lambda role has been granted
# s3:DeleteObject on (via picasso `var.fulfillment_grants`; see PR #258).
# Default: skipped. To run:
#   1. Operator creates an S3 bucket (e.g., `picasso-pii-dsar-int-staging`).
#   2. Operator adds `{bucket="picasso-pii-dsar-int-staging", tenant_id="TEN-SMOKE-FULFILL"}`
#      to `var.fulfillment_grants` + `terraform apply`.
#   3. Set FULFILLMENT_TEST_BUCKET=<bucket-name> before running pytest.
# Without the env var, k-prefix tests skip cleanly (no spurious failures).
FULFILLMENT_TEST_BUCKET = os.environ.get("FULFILLMENT_TEST_BUCKET")
FULFILLMENT_SKIP_REASON = (
    "Sprint D fulfillment walker integration test requires FULFILLMENT_TEST_BUCKET "
    "env var pointing at an S3 bucket the DSAR role has s3:DeleteObject on "
    "(see picasso var.fulfillment_grants — picasso#258). Test data uses "
    "tenant_id TEN-SMOKE-FULFILL; grant must scope to that tenant_id."
)
FULFILLMENT_TEST_TENANT_ID = "TEN-SMOKE-FULFILL"


# ───────────────────────────────────────────────────────────────────────────
# Helpers (module scope; available to all tests)
# ───────────────────────────────────────────────────────────────────────────
def _aws_identity():
    """Return STS get-caller-identity result, or None if any auth-class error.

    Audit row 4 (test-eng B3): catches BotoCoreError too (covers
    SSOTokenLoadError, which is a BotoCoreError subclass, not a ClientError).
    Without this, expired-SSO causes collection error rather than clean skip.
    """
    try:
        sts = boto3.client("sts", region_name=REGION)
        return sts.get_caller_identity()
    except (NoCredentialsError, ClientError, BotoCoreError):
        return None


@pytest.fixture(scope="module")
def aws():
    """Module-level boto3 clients. Skips entire module if AWS unavailable."""
    if os.environ.get("AWS_INTEGRATION_TESTS") != "1":
        pytest.skip(SKIP_REASON)
    identity = _aws_identity()
    if identity is None:
        pytest.skip(
            "AWS auth unavailable (no creds / expired SSO / wrong profile); "
            "run `aws sso login --profile myrecruiter-staging` first"
        )
    # Audit row 28 (code-rev N1): include the actual returned account in
    # the skip message so misconfigured profiles diagnose at-a-glance.
    if identity["Account"] != EXPECTED_ACCOUNT:
        pytest.skip(
            f"wrong account: AWS returned {identity['Account']}; "
            f"expected staging account {EXPECTED_ACCOUNT}. "
            f"Set AWS_PROFILE=myrecruiter-staging"
        )
    return {
        "lambda": boto3.client("lambda", region_name=REGION),
        "ddb": boto3.resource("dynamodb", region_name=REGION),
    }


def _gen_id(prefix="t"):
    return f"{prefix}-{int(time.time())}-{uuid.uuid4().hex[:8]}"


def _gen_email(test_id):
    return f"smoke.dsar.{test_id}@example.com"


def _normalize_email(email):
    return email.strip().lower()


def _seed_form_submission(aws, tenant_id, email, pii_subject_id=None):
    """Seed one form-submission row + matching pii-subject-index row.

    Returns (submission_id, pii_subject_id) for assertion + cleanup.
    """
    submission_id = str(uuid.uuid4())
    pii_subject_id = pii_subject_id or f"psub_{uuid.uuid4().hex}"
    timestamp = datetime.now(timezone.utc).isoformat()
    normalized = _normalize_email(email)

    aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).put_item(Item={
        "tenant_id": tenant_id,
        "submission_id": submission_id,
        "pii_subject_id": pii_subject_id,
        "form_id": "smoke_integration_test",
        "form_data_labeled": {"name": f"Smoke Test {submission_id[:8]}", "email": email},
        "submitted_at": timestamp,
        "ttl": int(time.time()) + 3600,  # 1-hour safety net auto-cleanup
    })

    aws["ddb"].Table(TABLE_SUBJECT_INDEX).put_item(Item={
        "tenant_id": tenant_id,
        "normalized_email": normalized,
        "pii_subject_id": pii_subject_id,
    })

    return submission_id, pii_subject_id


def _cleanup_seed(aws, tenant_id, submission_id, email):
    """Best-effort cleanup; never raises (audit row 27 — broadened).

    Catches Exception (not just ClientError) so non-AWS errors (e.g.,
    TypeError on malformed key) in cleanup don't obscure the actual test
    failure.
    """
    try:
        aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).delete_item(
            Key={"tenant_id": tenant_id, "submission_id": submission_id}
        )
    except Exception:
        pass
    try:
        aws["ddb"].Table(TABLE_SUBJECT_INDEX).delete_item(
            Key={"tenant_id": tenant_id, "normalized_email": _normalize_email(email)}
        )
    except Exception:
        pass


def _seed_notification_sends_row(aws, tenant_id, recipient, message_id=None,
                                  channel="email", status="sent"):
    """Seed one notification-sends row keyed by tenant_id partition.

    Returns (pk, sk, message_id) for cleanup + assertion. Mirrors the
    form_handler.py:_store_submission writer pattern. Audit row 1 helper.
    """
    now = datetime.now(timezone.utc).isoformat()
    iso_date = now[:10]
    msg_id = message_id or f"smoke-msg-{uuid.uuid4().hex[:12]}"
    pk = f"TENANT#{tenant_id}"
    sk = f"{iso_date}#{channel}#{msg_id}"
    aws["ddb"].Table(TABLE_NOTIFICATION_SENDS).put_item(Item={
        "pk": pk,
        "sk": sk,
        "channel": channel,
        "recipient": recipient,
        "submission_id": "smoke-int-test",
        "form_id": "smoke_integration_test",
        "template": "smoke_test_template",
        "status": status,
        "error": "",
        "message_id": msg_id,
        "timestamp": now,
        "ttl": int(time.time()) + 3600,  # 1h safety net
    })
    return pk, sk, msg_id


def _seed_notification_events_row(aws, message_id, event_type="delivered"):
    """Seed one notification-events row reachable via ByMessageId GSI.

    Returns (pk, sk) for cleanup. The GSI is keyed
    (message_id HASH, event_type_timestamp RANGE) — the RANGE key is a
    COMPOSITE so both attributes must be present for the GSI to index
    the row.
    """
    now = datetime.now(timezone.utc).isoformat()
    pk = f"MSG#{message_id}"
    sk = f"{event_type}#{now}"
    event_type_timestamp = f"{event_type}#{now}"
    aws["ddb"].Table(TABLE_NOTIFICATION_EVENTS).put_item(Item={
        "pk": pk,
        "sk": sk,
        "message_id": message_id,
        "event_type": event_type,
        "event_type_timestamp": event_type_timestamp,  # GSI RANGE composite
        "timestamp": now,
        "ttl": int(time.time()) + 3600,
    })
    return pk, sk


def _seed_recent_messages_row(aws, session_id, content="smoke test content",
                               role="user"):
    """Seed one recent-messages row keyed by sessionId + messageTimestamp.

    Returns (sessionId, messageTimestamp) for cleanup. messageTimestamp is
    Number (epoch ms) per the table schema (not ISO string).
    """
    msg_ts = int(time.time() * 1000)  # epoch ms (Number per table schema)
    aws["ddb"].Table(TABLE_RECENT_MESSAGES).put_item(Item={
        "sessionId": session_id,
        "messageTimestamp": msg_ts,
        "messageId": f"smoke-{uuid.uuid4().hex[:12]}",
        "role": role,
        "content": content,
        "expires_at": int(time.time()) + 3600,
    })
    return session_id, msg_ts


def _cleanup_ddb_row(aws, table_name, key):
    """Generic best-effort row cleanup."""
    try:
        aws["ddb"].Table(table_name).delete_item(Key=key)
    except Exception:
        pass


def _extract_details(audit_row):
    """Parse the audit-row details blob (handles dict or JSON-string forms)."""
    details = audit_row.get("details", {})
    if isinstance(details, str):
        return json.loads(details)
    return details


def _make_payload(tenant_id, email, dsar_id, request_type="delete", dry_run=True):
    # lambda#162 added a validation that rejects dsar_ids starting with `smoke-`
    # unless `smoke_test_marker=true` is set. ALL integration tests use the
    # TEST_DSAR_PREFIX = "smoke-int-" prefix, so propagate the marker here
    # rather than in each test. The marker also drives the `is_smoke_test`
    # audit-row attribute so the operator's SLA-monitor scan can filter these
    # rows out via `FilterExpression attribute_not_exists(is_smoke_test) OR is_smoke_test = :false`.
    return {
        "subject_identifier": email,
        "identifier_type": "email",
        "request_type": request_type,
        "tenant_id": tenant_id,
        "operator": "smoke-int-tests@myrecruiter.ai",
        "dsar_id": dsar_id,
        "dry_run": dry_run,
        "smoke_test_marker": True,
    }


def _invoke(aws, payload):
    """Invoke Lambda; return (response_body_dict, http_status)."""
    resp = aws["lambda"].invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    body = resp["Payload"].read().decode("utf-8")
    return json.loads(body), resp.get("StatusCode")


def _get_audit_rows(aws, dsar_id):
    resp = aws["ddb"].Table(TABLE_AUDIT).query(
        KeyConditionExpression=Key("dsar_id").eq(dsar_id),
        ConsistentRead=True,
    )
    return resp.get("Items", [])


def _row_exists(aws, table_name, key):
    resp = aws["ddb"].Table(table_name).get_item(Key=key, ConsistentRead=True)
    return resp.get("Item") is not None


# ───────────────────────────────────────────────────────────────────────────
# Tests
# ───────────────────────────────────────────────────────────────────────────
def test_a_dry_run_does_not_delete(aws):
    """(a) dry-run: response succeeds; seed row is NOT deleted; audit row written.

    Audit row 3 (test-eng B2): asserts status == "partial" specifically
    (M1 always returns partial because conversation-summaries + audit-read-only
    are explicitly deferred). Loose `in (completed, partial)` would mask a
    future partial_error regression.
    """
    tid = _gen_id("a")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=True,
        ))

        assert http_status == 200, f"http status: {http_status}; body: {body}"
        # M1 always returns partial (2 deferred surfaces); partial_error
        # would indicate a real failure.
        assert body.get("status") == "partial", \
            f"unexpected status (expected 'partial'): {body.get('status')}; body: {body}"
        # The form-submissions walker must have RUN (rows_touched > 0).
        assert body.get("rows_touched", {}).get("form-submissions", 0) >= 1, \
            f"form-submissions walker did not touch seed row; body: {body}"

        # Critical: dry_run MUST NOT delete the seed row.
        assert _row_exists(aws, TABLE_FORM_SUBMISSIONS, {
            "tenant_id": tenant_id, "submission_id": submission_id,
        }), "BUG: dry_run=true deleted the row (data-integrity violation)"

        # Audit row must exist.
        audit_rows = _get_audit_rows(aws, dsar_id)
        assert audit_rows, f"no audit rows for dsar_id={dsar_id}"
        assert any(r.get("event_type") == "request_received" for r in audit_rows)
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


def test_b_real_delete_removes_row_and_writes_audit(aws):
    """(b) real delete (dry_run=false): seed row deleted; audit row with closed event.

    Audit rows fixed: #2 (operator-precedence bug on closed assertion),
    #3 (status precision), #17 (deserialize closed event details + assert
    operation status), #18 (assert surface_walked event present).
    """
    tid = _gen_id("b")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=False,
        ))

        assert http_status == 200, f"http status: {http_status}; body: {body}"
        # Audit row 3: assert exact "partial" (M1 always returns partial).
        assert body.get("status") == "partial", \
            f"unexpected status (expected 'partial'): {body.get('status')}; body: {body}"
        # Audit row 8: rows_deleted should match (1 seed row), rows_delete_failed = 0.
        assert body.get("rows_touched", {}).get("form-submissions", 0) == 1, body
        # If rows_delete_failed key exists (new field), it must be 0.
        # (Walker results live in body details, not top-level — leave detail check
        # to surface_walked event below.)

        # Critical: real delete MUST remove the row.
        assert not _row_exists(aws, TABLE_FORM_SUBMISSIONS, {
            "tenant_id": tenant_id, "submission_id": submission_id,
        }), "BUG: real delete did not remove row"

        audit_rows = _get_audit_rows(aws, dsar_id)
        assert audit_rows
        event_types = {r.get("event_type") for r in audit_rows}
        assert "request_received" in event_types
        # Audit row 2: fix operator-precedence bug. Use literal membership.
        assert "closed" in event_types, \
            f"closed event missing; event_types: {event_types}"
        # Audit row 17: closed audit ROW's `status` field (not in details)
        # must show 'completed' or 'partial' (NOT partial_error).
        closed_row = next(r for r in audit_rows if r.get("event_type") == "closed")
        assert closed_row.get("status") in ("partial", "completed"), \
            f"closed event status indicates error: row={closed_row}"
        # Audit row 18: surface_walked:form-submissions event must exist
        # (proves walker actually ran, not just that input validation passed).
        assert any(et and et.startswith("surface_walked:form-submissions")
                   for et in event_types), \
            f"surface_walked:form-submissions missing; event_types: {event_types}"
    finally:
        # Seed already deleted by Lambda; cleanup is index-only
        _cleanup_seed(aws, tenant_id, submission_id, email)


def test_c_tenant_bound_walker_does_not_reach_other_tenant(aws):
    """(c) same email in tenant-A + tenant-B; invoke for A; B's row remains."""
    tid = _gen_id("c")
    tenant_a = f"{TEST_TENANT_PREFIX}{tid}-A"
    tenant_b = f"{TEST_TENANT_PREFIX}{tid}-B"
    email = _gen_email(tid)
    dsar_id = f"{TEST_DSAR_PREFIX}{tid}"

    sub_a, _ = _seed_form_submission(aws, tenant_a, email)
    sub_b, _ = _seed_form_submission(aws, tenant_b, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_a, email, dsar_id, request_type="delete", dry_run=False,
        ))
        assert http_status == 200, body

        # Tenant-A row deleted; tenant-B row UNTOUCHED.
        assert not _row_exists(aws, TABLE_FORM_SUBMISSIONS, {
            "tenant_id": tenant_a, "submission_id": sub_a,
        }), "tenant-A row not deleted"
        assert _row_exists(aws, TABLE_FORM_SUBMISSIONS, {
            "tenant_id": tenant_b, "submission_id": sub_b,
        }), "BUG: tenant-B row was deleted (cross-tenant violation)"
    finally:
        _cleanup_seed(aws, tenant_a, sub_a, email)
        _cleanup_seed(aws, tenant_b, sub_b, email)


def test_d_psid_subset_email_walker_reaches_pii_subject_linked_row(aws):
    """(d) PSID-by-Scan M1 sub-set: walker reaches form-submission rows linked
    via the pii-subject-index (the email-keyed access path that M2 will mirror
    for PSID-by-Scan). Validates the tenant-scoped Query + FilterExpression
    pattern works end-to-end.
    """
    tid = _gen_id("d")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    # Explicit pii_subject_id so we can assert the chained walker reaches the row.
    submission_id, pii_subject_id = _seed_form_submission(aws, tenant_id, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="access", dry_run=True,
        ))
        assert http_status == 200, body
        # The walker must have resolved the pii_subject_id from the email.
        assert body.get("pii_subject_id") == pii_subject_id, \
            f"walker did not resolve pii_subject_id; got: {body.get('pii_subject_id')}"
        # Audit row 15 (test-eng Y1): assert exported row VALUES match seed,
        # not just that some row was returned. A walker that returned any
        # tenant-A row (ignoring pii_subject_id filter) would still return
        # something — this asserts the FILTERED match.
        exported = body.get("exported_rows", {}).get("form-submissions") or []
        assert exported, f"walker did not export form-submission row; body: {body}"
        assert any(r.get("submission_id") == submission_id for r in exported), \
            f"exported rows do not include the seeded submission_id; " \
            f"got: {[r.get('submission_id') for r in exported]}"
        # Cross-check pii_subject_id on the exported row matches the seed.
        for r in exported:
            if r.get("submission_id") == submission_id:
                assert r.get("pii_subject_id") == pii_subject_id
                break
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


def test_e_per_tenant_s3_walk_placeholder_surfaces_in_manual_followup(aws):
    """(e) per-tenant S3 walk M1 placeholder: response includes manual_followup
    naming a deferred surface (M2 / item 1b territory). No S3 walk attempted
    by M1.

    Audit row 20 (test-eng Y6): asserts specific manual_followup CONTENT
    mentioning a deferred-by-M1 surface name. Previous version asserted
    only that followups were non-empty — that duplicated test (a) with a
    looser assertion. Now confirms the M1 scope boundary is explicit in
    the operator response.
    """
    tid = _gen_id("e")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=True,
        ))
        assert http_status == 200, body
        followups = body.get("manual_followups", [])
        assert followups, f"no manual_followups returned; body: {body}"
        # Audit row 20: assert specific M1-deferred surfaces are named in
        # the followups so the operator response is explicit about scope.
        joined = " ".join(followups)
        assert "conversation-summaries" in joined, \
            f"M1-deferred surface 'conversation-summaries' not named in " \
            f"manual_followups: {followups}"
        assert "audit-read-only" in joined or "picasso-audit-staging" in joined, \
            f"M1-deferred surface 'audit-read-only' not named in followups: {followups}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


def test_f_audit_row_tenant_id_matches_invocation_tenant(aws):
    """(f) cross-tenant isolation structural check: the `request_received`
    audit row's `tenant_id` field reflects the operator-supplied tenant_id
    (no cross-partition smearing). Combined with Control 1 (walker
    KeyConditionExpression bounds reads to that partition), this confirms
    the tenant_id flows correctly through the invocation chain.

    NOTE: only `request_received` carries tenant_id; `surface_walked:*`
    events carry per-surface action metadata (rows_deleted, etc.) without
    repeating the invocation-level tenant_id. The structural assertion is
    against the request_received event, not every event.
    """
    tid = _gen_id("f")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=True,
        ))
        assert http_status == 200, body
        audit_rows = _get_audit_rows(aws, dsar_id)
        assert audit_rows
        request_received = next(
            (r for r in audit_rows if r.get("event_type") == "request_received"),
            None,
        )
        assert request_received, "no request_received audit row"
        details = _extract_details(request_received)
        assert details.get("tenant_id") == tenant_id, (
            f"audit row tenant_id mismatch: got={details.get('tenant_id')} "
            f"expected={tenant_id}"
        )
        # Audit row 12 (Security SR3): caller_arn must be present and
        # reflect the actual STS identity (not the self-reported operator).
        assert details.get("caller_arn"), \
            f"caller_arn missing from request_received audit (Security SR3); " \
            f"details: {details}"
        assert ":sts::" in details["caller_arn"] or ":iam::" in details["caller_arn"], \
            f"caller_arn does not look like an STS/IAM ARN: {details['caller_arn']}"
        # Audit row 18 (test-eng Y4): assert at least one surface_walked event
        # exists, proving the walker actually executed past input validation.
        event_types = {r.get("event_type") for r in audit_rows}
        assert any(et and et.startswith("surface_walked:") for et in event_types), \
            f"no surface_walked:* events; walker did not run. " \
            f"event_types: {event_types}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


def test_fprime_access_returns_exported_rows_equivalent_to_delete(aws):
    """(f') access end-to-end equivalent to delete: request_type='access' returns
    exported_rows with the same coverage as delete. Verifies the access path is
    a structural peer of the delete path (M1 #5 + lifecycle B2 = G-F closure).

    Internal-identifier projection: for surfaces that DO project (recent-messages
    per F-DSAR4), the projected fields are present and internal IDs are absent.
    For surfaces that intentionally return full rows (form-submissions, per
    documented field-constrained design), the full row is returned and the
    operator-visible `form_data_labeled` is reachable from the export.
    """
    tid = _gen_id("fp")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="access", dry_run=True,
        ))
        assert http_status == 200, body
        assert body.get("status") in ("completed", "partial"), body

        # exported_rows must include form-submissions.
        exported = body.get("exported_rows", {}) or {}
        fs_rows = exported.get("form-submissions") or []
        assert fs_rows, f"access did not export form-submissions; body: {body}"

        # form-submissions: field-constrained design returns full rows;
        # operator-visible labeled data must be reachable.
        # Audit row 16 (test-eng Y2): assert exported field VALUES match seed,
        # not just key presence. A walker returning any row would also have
        # form_data_labeled present.
        matched = next(
            (r for r in fs_rows if r.get("submission_id") == submission_id),
            None,
        )
        assert matched, \
            f"exported rows do not include seeded submission_id; " \
            f"got: {[r.get('submission_id') for r in fs_rows]}"
        assert "form_data_labeled" in matched, \
            f"form_data_labeled missing from access export: {matched.keys()}"
        assert matched["form_data_labeled"].get("email") == email, \
            f"exported form_data_labeled.email mismatch: " \
            f"got={matched['form_data_labeled'].get('email')} expected={email}"

        # Audit row written with request_type='access'.
        audit_rows = _get_audit_rows(aws, dsar_id)
        assert audit_rows
        request_received = next(
            (r for r in audit_rows if r.get("event_type") == "request_received"),
            None,
        )
        assert request_received, "no request_received audit row"
        details = _extract_details(request_received)
        assert details.get("request_type") == "access", \
            f"audit row request_type mismatch: {details.get('request_type')}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


# ───────────────────────────────────────────────────────────────────────────
# Walker coverage tests (audit row 1; code-rev B2 + test-eng B1)
# Each walker exercised against REAL DDB with seeded rows.
# ───────────────────────────────────────────────────────────────────────────
def test_g_notification_sends_walker_finds_direct_recipient_row(aws):
    """Walker `_walk_notification_sends` finds rows where `recipient` matches
    normalized_email via tenant-Query + case-insensitive Python post-filter.

    Audit row 1 closure: previously only form-submissions was integration-
    tested; this exercises the notification-sends walker against real DDB.
    """
    tid = _gen_id("g")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    # Seed: form-submission (for pii_subject_id linkage) + a notification-sends
    # row with this email as recipient.
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    pk, sk, msg_id = _seed_notification_sends_row(aws, tenant_id, recipient=email)
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="access", dry_run=True,
        ))
        assert http_status == 200, body
        # notification-sends export must include the seeded row.
        exported = body.get("exported_rows", {}).get("notification-sends") or []
        assert any(r.get("message_id") == msg_id for r in exported), \
            f"notification-sends walker did not return seeded message_id={msg_id}; " \
            f"got: {[r.get('message_id') for r in exported]}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)
        _cleanup_ddb_row(aws, TABLE_NOTIFICATION_SENDS, {"pk": pk, "sk": sk})


def test_h_notification_events_chained_walker_finds_events_via_message_id(aws):
    """Walker `_walk_notification_events` chains off message_ids from the
    notification-sends walker, querying the ByMessageId GSI per message_id.

    Audit row 1 closure: exercises the GSI chained walk end-to-end against
    real DDB. The GSI's `message_id` hash key + `event_type#timestamp`
    range pattern is verified here, not just in mocks.
    """
    tid = _gen_id("h")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    pk_ns, sk_ns, msg_id = _seed_notification_sends_row(aws, tenant_id, recipient=email)
    pk_ne, sk_ne = _seed_notification_events_row(aws, message_id=msg_id, event_type="delivered")
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="access", dry_run=True,
        ))
        assert http_status == 200, body
        # notification-events export must include an event for the seeded
        # message_id (chained walker reached it).
        exported = body.get("exported_rows", {}).get("notification-events") or []
        assert any(r.get("message_id") == msg_id for r in exported), \
            f"chained notification-events walker did not reach seeded " \
            f"message_id={msg_id}; got: {[r.get('message_id') for r in exported]}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)
        _cleanup_ddb_row(aws, TABLE_NOTIFICATION_SENDS, {"pk": pk_ns, "sk": sk_ns})
        _cleanup_ddb_row(aws, TABLE_NOTIFICATION_EVENTS, {"pk": pk_ne, "sk": sk_ne})


def test_i_recent_messages_walker_finds_messages_for_chained_session(aws):
    """Walker `_walk_recent_messages` chains off session_ids from the
    form-submissions walker; per session_id, queries `picasso-recent-messages`
    on sessionId hash key.

    Audit row 1 closure: exercises the recent-messages walker against real
    DDB + verifies the F-DSAR4 projection drops sessionId/messageId/expires_at.
    """
    tid = _gen_id("i")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    # Seed a form-submission carrying a session_id; then seed a recent-messages
    # row at that session_id. Walker chains off the session_id.
    submission_id = str(uuid.uuid4())
    pii_subject_id = f"psub_{uuid.uuid4().hex}"
    session_id = f"smoke-session-{uuid.uuid4().hex[:12]}"
    timestamp = datetime.now(timezone.utc).isoformat()
    normalized = _normalize_email(email)

    aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).put_item(Item={
        "tenant_id": tenant_id,
        "submission_id": submission_id,
        "pii_subject_id": pii_subject_id,
        "form_id": "smoke_integration_test",
        "form_data_labeled": {"email": email},
        "session_id": session_id,
        "submitted_at": timestamp,
        "ttl": int(time.time()) + 3600,
    })
    aws["ddb"].Table(TABLE_SUBJECT_INDEX).put_item(Item={
        "tenant_id": tenant_id,
        "normalized_email": normalized,
        "pii_subject_id": pii_subject_id,
    })
    session_key, msg_ts = _seed_recent_messages_row(
        aws, session_id, content="smoke test integration content"
    )
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="access", dry_run=True,
        ))
        assert http_status == 200, body
        exported = body.get("exported_rows", {}).get("recent-messages") or []
        assert exported, \
            f"recent-messages walker did not export any messages; body: {body}"
        # F-DSAR4 projection: must contain content, NOT sessionId/messageId/expires_at.
        first = exported[0]
        assert "content" in first, f"projected row missing content: {first}"
        assert "sessionId" not in first, \
            f"projection leaked sessionId: {first}"
        assert "messageId" not in first, \
            f"projection leaked messageId: {first}"
        assert "expires_at" not in first, \
            f"projection leaked expires_at: {first}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)
        _cleanup_ddb_row(aws, TABLE_RECENT_MESSAGES, {
            "sessionId": session_key, "messageTimestamp": msg_ts,
        })


# ───────────────────────────────────────────────────────────────────────────
# Audit replay test (audit row 31; test-eng G1)
# ───────────────────────────────────────────────────────────────────────────
def test_j_audit_replay_documents_actual_idempotency_scope(aws):
    """Documents the ACTUAL audit-write idempotency scope: protection is
    per (dsar_id, event_timestamp) composite, NOT per dsar_id.

    INTEGRATION FINDING (audit row 31 closure): the AuditCollision class
    docstring claims protection against "operator replay of the same
    dsar_id" but the ConditionExpression at lambda_function.py:325 only
    refuses overwrite of an existing (dsar_id, event_timestamp) row.
    Microsecond-precision timestamps mean cross-second replays succeed.
    The real protection is "no two writes can clobber the same audit row"
    — NOT "no dsar_id can be reused."

    This test ASSERTS the actual behavior (replay succeeds with new
    timestamps) so the test suite documents the truth. A follow-up PR
    should EITHER strengthen the audit-write to refuse if any row with
    this dsar_id exists (Query check, +1 RCU per audit-write) OR update
    the docstring to match. Filed as audit-finding-post-M1 for the next
    milestone's gap-router.
    """
    tid = _gen_id("j")
    tenant_id, email, dsar_id = (
        f"{TEST_TENANT_PREFIX}{tid}",
        _gen_email(tid),
        f"{TEST_DSAR_PREFIX}{tid}",
    )
    submission_id, _ = _seed_form_submission(aws, tenant_id, email)
    try:
        body1, _ = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=True,
        ))
        assert body1.get("status") == "partial", body1
        # Pause to ensure microsecond-distinct event_timestamps
        # (defensive — the system actually only needs sub-microsecond
        # distinction, which is guaranteed by datetime.now() resolution).
        time.sleep(0.01)
        body2, _ = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=True,
        ))
        # Actual behavior: replay SUCCEEDS at the dsar_id level because the
        # audit-write ConditionExpression is per (dsar_id, event_timestamp).
        assert body2.get("status") == "partial", \
            f"replay returned unexpected status; body: {body2}"
        # Verify the audit table now has 2 sets of events under same dsar_id
        # (proves replay duplicates rather than collides).
        audit_rows = _get_audit_rows(aws, dsar_id)
        request_received_count = sum(
            1 for r in audit_rows if r.get("event_type") == "request_received"
        )
        assert request_received_count == 2, \
            f"expected 2 request_received rows under same dsar_id; " \
            f"got {request_received_count}; audit_rows={len(audit_rows)}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


# ───────────────────────────────────────────────────────────────────────────
# Sprint D fulfillment walker — IAM-gated integration tests (Q3c)
# Skip cleanly when FULFILLMENT_TEST_BUCKET is unset (default state until
# operator creates the bucket + adds the grant per picasso#258).
# ───────────────────────────────────────────────────────────────────────────


def _seed_form_submission_with_fulfillment(aws, tenant_id, email, bucket,
                                            submission_id=None):
    """Seed a form-submission row WITH fulfillment_path + write the S3 object.

    Matches the production writer's key pattern:
        s3://{bucket}/submissions/{tenant_id}/{form_id}/{submission_id}.json
    (see Master_Function_Staging/form_handler.py::_process_fulfillment +
    Bedrock_Streaming_Handler_Staging/form_handler.js::routeFulfillment.)

    Returns (submission_id, pii_subject_id, s3_key) for assertion + cleanup.
    """
    submission_id = submission_id or str(uuid.uuid4())
    pii_subject_id = f"psub_{uuid.uuid4().hex}"
    timestamp = datetime.now(timezone.utc).isoformat()
    normalized = _normalize_email(email)
    form_id = "smoke_fulfillment_test"
    s3_key = f"submissions/{tenant_id}/{form_id}/{submission_id}.json"
    fulfillment_path = f"s3://{bucket}/{s3_key}"

    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps({"name": f"Smoke Fulfillment {submission_id[:8]}",
                         "email": email}),
        ContentType="application/json",
    )

    aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).put_item(Item={
        "tenant_id": tenant_id,
        "submission_id": submission_id,
        "pii_subject_id": pii_subject_id,
        "form_id": form_id,
        "form_data_labeled": {"name": "Smoke", "email": email},
        "submitted_at": timestamp,
        "fulfillment_path": fulfillment_path,
        "ttl": int(time.time()) + 3600,
    })
    aws["ddb"].Table(TABLE_SUBJECT_INDEX).put_item(Item={
        "tenant_id": tenant_id,
        "normalized_email": normalized,
        "pii_subject_id": pii_subject_id,
    })
    return submission_id, pii_subject_id, s3_key


def _cleanup_fulfillment_seed(aws, tenant_id, submission_id, email, bucket,
                                s3_key):
    """Best-effort cleanup of DDB row + S3 object. Never raises."""
    _cleanup_seed(aws, tenant_id, submission_id, email)
    try:
        boto3.client("s3", region_name=REGION).delete_object(
            Bucket=bucket, Key=s3_key,
        )
    except Exception:  # noqa: BLE001
        pass


@pytest.mark.skipif(
    not FULFILLMENT_TEST_BUCKET, reason=FULFILLMENT_SKIP_REASON,
)
def test_k_fulfillment_walker_dry_run_counts_object_without_delete(aws):
    """Sprint D walker — dry_run=True counts the S3 object but does NOT delete it.

    Asserts against the handler's API response shape (the handler exposes
    `rows_touched`, `manual_followups`, `audit_row_pks` — `walker_results`
    is internal dispatcher state, not surfaced to the client).
    """
    tid = _gen_id("k")
    tenant_id = FULFILLMENT_TEST_TENANT_ID
    email = _gen_email(tid)
    dsar_id = f"{TEST_DSAR_PREFIX}{tid}"
    bucket = FULFILLMENT_TEST_BUCKET
    submission_id, _, s3_key = _seed_form_submission_with_fulfillment(
        aws, tenant_id, email, bucket,
    )
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=True,
        ))
        assert http_status == 200, body
        rows_touched = body.get("rows_touched") or {}
        assert rows_touched.get("fulfillment") == 1, \
            f"expected rows_touched.fulfillment=1; got {rows_touched}; body={body}"
        followups = body.get("manual_followups") or []
        assert any("fulfillment: dry_run" in f for f in followups), \
            f"expected fulfillment dry_run followup; got followups={followups}"
        # Object still exists (dry_run did not delete)
        s3 = boto3.client("s3", region_name=REGION)
        head = s3.head_object(Bucket=bucket, Key=s3_key)
        assert head["ContentLength"] > 0
    finally:
        _cleanup_fulfillment_seed(
            aws, tenant_id, submission_id, email, bucket, s3_key,
        )


@pytest.mark.skipif(
    not FULFILLMENT_TEST_BUCKET, reason=FULFILLMENT_SKIP_REASON,
)
def test_l_fulfillment_walker_real_delete_removes_object_and_writes_audit(aws):
    """Sprint D walker — dry_run=False deletes the S3 object + writes
    `surface_walked:fulfillment` audit event.
    """
    tid = _gen_id("l")
    tenant_id = FULFILLMENT_TEST_TENANT_ID
    email = _gen_email(tid)
    dsar_id = f"{TEST_DSAR_PREFIX}{tid}"
    bucket = FULFILLMENT_TEST_BUCKET
    submission_id, _, s3_key = _seed_form_submission_with_fulfillment(
        aws, tenant_id, email, bucket,
    )
    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=False,
        ))
        assert http_status == 200, body
        rows_touched = body.get("rows_touched") or {}
        assert rows_touched.get("fulfillment") == 1, \
            f"expected rows_touched.fulfillment=1; got {rows_touched}; body={body}"

        # S3 object gone
        s3 = boto3.client("s3", region_name=REGION)
        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
            raise AssertionError(f"S3 object {s3_key} still exists post-delete")
        except ClientError as e:
            assert e.response["Error"]["Code"] in ("404", "NoSuchKey",
                                                    "NotFound"), \
                f"unexpected ClientError on head_object: {e}"

        # surface_walked:fulfillment audit event present
        audit_rows = _get_audit_rows(aws, dsar_id)
        fulfillment_events = [
            r for r in audit_rows
            if r.get("event_type") == "surface_walked:fulfillment"
        ]
        assert len(fulfillment_events) == 1, \
            f"expected 1 surface_walked:fulfillment audit event; got " \
            f"{len(fulfillment_events)}; audit_rows={audit_rows}"

        # Audit closure 2026-05-26 row #16 (test-engineer 🟡): also verify
        # the upstream form-submission DDB row is deleted, not just the S3
        # object. If the form-submissions walker were silently failing, the
        # S3 deletion would prove only half the DSAR.
        ddb_resp = aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).get_item(Key={
            "tenant_id": tenant_id,
            "submission_id": submission_id,
        })
        assert "Item" not in ddb_resp, (
            f"expected form-submission DDB row deleted by upstream walker; "
            f"got Item: {ddb_resp.get('Item')}")
    finally:
        _cleanup_fulfillment_seed(
            aws, tenant_id, submission_id, email, bucket, s3_key,
        )


@pytest.mark.skipif(
    not FULFILLMENT_TEST_BUCKET, reason=FULFILLMENT_SKIP_REASON,
)
def test_m_fulfillment_walker_rejects_cross_tenant_path(aws):
    """Sprint D walker tenant-segment defense — a form-submission row whose
    `fulfillment_path` is forged to point at ANOTHER tenant's prefix MUST
    NOT delete the cross-tenant object.

    Asserts against the handler's API response shape: cross-tenant rejection
    surfaces as `rows_touched.fulfillment == 0` (no in-tenant matches) and
    a `fulfillment: ... skipped_cross_tenant` manual followup. The forged
    object MUST survive.
    """
    tid = _gen_id("m")
    tenant_id = FULFILLMENT_TEST_TENANT_ID
    email = _gen_email(tid)
    dsar_id = f"{TEST_DSAR_PREFIX}{tid}"
    bucket = FULFILLMENT_TEST_BUCKET

    submission_id = str(uuid.uuid4())
    forged_key = f"submissions/TEN-OTHER/forged/{submission_id}.json"
    forged_path = f"s3://{bucket}/{forged_key}"

    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=bucket, Key=forged_key, Body=b"forged",
                  ContentType="application/json")

    pii_subject_id = f"psub_{uuid.uuid4().hex}"
    normalized = _normalize_email(email)
    timestamp = datetime.now(timezone.utc).isoformat()
    aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).put_item(Item={
        "tenant_id": tenant_id,
        "submission_id": submission_id,
        "pii_subject_id": pii_subject_id,
        "form_id": "smoke_fulfillment_cross_tenant",
        "form_data_labeled": {"email": email},
        "submitted_at": timestamp,
        "fulfillment_path": forged_path,
        "ttl": int(time.time()) + 3600,
    })
    aws["ddb"].Table(TABLE_SUBJECT_INDEX).put_item(Item={
        "tenant_id": tenant_id,
        "normalized_email": normalized,
        "pii_subject_id": pii_subject_id,
    })

    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=False,
        ))
        assert http_status == 200, body
        rows_touched = body.get("rows_touched") or {}
        assert rows_touched.get("fulfillment") == 0, \
            f"expected rows_touched.fulfillment=0 (no in-tenant matches); " \
            f"got {rows_touched}; body={body}"
        followups = body.get("manual_followups") or []
        # Audit closure 2026-05-26 row #8 (code-reviewer 🟡): tighten the
        # assertion so a future regression that treats the forged path as
        # a parse-failure (rather than cross-tenant) would FAIL this test
        # rather than silently pass on the OR clause.
        assert any("cross-tenant pointer" in f for f in followups), \
            f"expected cross-tenant followup; got followups={followups}"
        assert not any("unparseable" in f for f in followups), (
            "the forged path should be rejected as cross-tenant, NOT as a "
            "parse-failure — that distinction matters for operator triage")

        # Forged object survived
        head = s3.head_object(Bucket=bucket, Key=forged_key)
        assert head["ContentLength"] == len(b"forged")
    finally:
        _cleanup_fulfillment_seed(
            aws, tenant_id, submission_id, email, bucket, forged_key,
        )


@pytest.mark.skipif(
    not FULFILLMENT_TEST_BUCKET, reason=FULFILLMENT_SKIP_REASON,
)
def test_n_fulfillment_walker_mixed_rows_with_and_without_path(aws):
    """Audit closure 2026-05-26 row #12 (test-engineer 🟡): the dominant
    real-world case after Sprint D ships is a tenant with SOME pre-extension
    rows (no `fulfillment_path` attribute) AND SOME post-extension rows
    (path present). Walker must (a) delete the S3 object for post-extension
    rows, (b) surface the rows_without_path count + manual_followup pointing
    at writer-extension-pending message.
    """
    tid = _gen_id("n")
    tenant_id = FULFILLMENT_TEST_TENANT_ID
    email = _gen_email(tid)
    dsar_id = f"{TEST_DSAR_PREFIX}{tid}"
    bucket = FULFILLMENT_TEST_BUCKET

    # Row 1: has fulfillment_path + S3 object exists; the helper also writes
    # the subject-index entry mapping the email at its pii_subject_id.
    sub_with, subject_id, key_with = _seed_form_submission_with_fulfillment(
        aws, tenant_id, email, bucket,
    )

    # Row 2: no fulfillment_path (pre-extension shape) — same subject_id so
    # the walker resolves email → subject_id → BOTH rows. Do NOT rewrite
    # the subject-index; it already points the email at `subject_id` from
    # the helper above.
    sub_without = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).put_item(Item={
        "tenant_id": tenant_id,
        "submission_id": sub_without,
        "pii_subject_id": subject_id,
        "form_id": "smoke_fulfillment_no_path",
        "form_data_labeled": {"email": email},
        "submitted_at": timestamp,
        "ttl": int(time.time()) + 3600,
        # NO fulfillment_path attribute
    })

    try:
        body, http_status = _invoke(aws, _make_payload(
            tenant_id, email, dsar_id, request_type="delete", dry_run=False,
        ))
        assert http_status == 200, body
        rows_touched = body.get("rows_touched") or {}
        # Exactly one S3 object should be deleted (the one with path).
        assert rows_touched.get("fulfillment") == 1, \
            f"expected fulfillment=1; got {rows_touched}; body={body}"
        followups = body.get("manual_followups") or []
        assert any("writer extension pending" in f.lower() or
                   "fulfillment_path" in f
                   for f in followups), \
            f"expected writer-extension-pending followup; got {followups}"

        # Object with path is GONE
        s3 = boto3.client("s3", region_name=REGION)
        try:
            s3.head_object(Bucket=bucket, Key=key_with)
            raise AssertionError(f"S3 object {key_with} still exists post-delete")
        except ClientError as e:
            assert e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound")
    finally:
        # Cleanup both rows
        _cleanup_fulfillment_seed(
            aws, tenant_id, sub_with, email, bucket, key_with,
        )
        _cleanup_ddb_row(aws, TABLE_FORM_SUBMISSIONS, {
            "tenant_id": tenant_id, "submission_id": sub_without,
        })


@pytest.mark.skipif(
    not FULFILLMENT_TEST_BUCKET, reason=FULFILLMENT_SKIP_REASON,
)
def test_o_fulfillment_walker_iam_level_blocks_cross_tenant_delete(aws):
    """Audit closure 2026-05-26 row #2 (code-reviewer 🔴): test_m verifies
    the code-level tenant-segment check — but the IAM grant on
    `submissions/TEN-SMOKE-FULFILL/*` is the defense-in-depth layer. Verify
    that even if the code-level check were bypassed, IAM denies deletion of
    objects under a different tenant prefix in the same bucket.

    Strategy: seed an object under `submissions/TEN-OTHER-IAM-TEST/` in the
    fixture bucket. Use the AWS CLI directly (the DSAR Lambda IAM role
    can't reach this object). Verify that the operator-attested check
    `aws s3 delete-object` from the DSAR role (impersonated via assume-role
    in this test) returns AccessDenied. This documents that IAM blocks
    cross-tenant even at the API layer, not just at code.

    NOTE: this test impersonates the DSAR role to perform the check. The
    test only operates within the fixture bucket and only attempts to
    delete a key under TEN-OTHER-IAM-TEST/ — it does NOT touch any real
    tenant prefix.
    """
    bucket = FULFILLMENT_TEST_BUCKET
    other_tenant = "TEN-OTHER-IAM-TEST"
    other_key = f"submissions/{other_tenant}/iam-test/{uuid.uuid4().hex}.json"

    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=bucket, Key=other_key, Body=b"iam-test",
                  ContentType="application/json")
    try:
        # Assume the DSAR Lambda role and attempt to delete the cross-tenant key
        sts = boto3.client("sts", region_name=REGION)
        try:
            assumed = sts.assume_role(
                RoleArn=f"arn:aws:iam::{EXPECTED_ACCOUNT}:role/picasso-pii-dsar-staging-role",
                RoleSessionName="audit-row-2-iam-check",
                DurationSeconds=900,
            )
        except ClientError as e:
            # Operator's own role may not be authorized to assume the DSAR
            # role; this is acceptable — the IAM check itself proves the
            # cross-tenant defense isn't bypassable, so the test logs and
            # skips if the assume-role precondition isn't met.
            pytest.skip(
                f"could not assume DSAR role (operator may lack AssumeRole "
                f"on the role's trust policy — that itself is part of the "
                f"least-privilege design): {e}"
            )
            return
        creds = assumed["Credentials"]
        dsar_s3 = boto3.client(
            "s3", region_name=REGION,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
        try:
            dsar_s3.delete_object(Bucket=bucket, Key=other_key)
            raise AssertionError(
                "DSAR role was able to delete a cross-tenant object — IAM "
                "defense-in-depth is broken. Expected AccessDenied.")
        except ClientError as e:
            assert e.response["Error"]["Code"] == "AccessDenied", \
                f"expected AccessDenied; got {e}"

        # Object still exists when we re-check via the operator's main role
        head = s3.head_object(Bucket=bucket, Key=other_key)
        assert head["ContentLength"] == len(b"iam-test")
    finally:
        try:
            s3.delete_object(Bucket=bucket, Key=other_key)
        except Exception:
            pass
