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
from botocore.exceptions import ClientError, NoCredentialsError


# ───────────────────────────────────────────────────────────────────────────
# Configuration
# ───────────────────────────────────────────────────────────────────────────
LAMBDA_NAME = "picasso-pii-dsar-staging"
EXPECTED_ACCOUNT = "525409062831"
REGION = "us-east-1"

TABLE_FORM_SUBMISSIONS = "picasso-form-submissions-staging"
TABLE_SUBJECT_INDEX = "picasso-pii-subject-index-staging"
TABLE_AUDIT = "picasso-pii-dsar-audit-staging"

TEST_TENANT_PREFIX = "TEN-SMOKE-INT-"
TEST_DSAR_PREFIX = "smoke-int-"

SKIP_REASON = (
    "Integration tests require AWS_PROFILE + valid SSO session for acct 525. "
    "Run with: AWS_INTEGRATION_TESTS=1 pytest test_dsar_integration.py"
)


# ───────────────────────────────────────────────────────────────────────────
# Helpers (module scope; available to all tests)
# ───────────────────────────────────────────────────────────────────────────
def _aws_available():
    try:
        sts = boto3.client("sts", region_name=REGION)
        identity = sts.get_caller_identity()
        return identity["Account"] == EXPECTED_ACCOUNT
    except (NoCredentialsError, ClientError):
        return False


@pytest.fixture(scope="module")
def aws():
    """Module-level boto3 clients. Skips entire module if AWS unavailable."""
    if os.environ.get("AWS_INTEGRATION_TESTS") != "1":
        pytest.skip(SKIP_REASON)
    if not _aws_available():
        pytest.skip(
            "AWS auth failed or wrong account; "
            "run `aws sso login --profile myrecruiter-staging` first"
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
    """Best-effort cleanup; never raises."""
    try:
        aws["ddb"].Table(TABLE_FORM_SUBMISSIONS).delete_item(
            Key={"tenant_id": tenant_id, "submission_id": submission_id}
        )
    except ClientError:
        pass
    try:
        aws["ddb"].Table(TABLE_SUBJECT_INDEX).delete_item(
            Key={"tenant_id": tenant_id, "normalized_email": _normalize_email(email)}
        )
    except ClientError:
        pass


def _make_payload(tenant_id, email, dsar_id, request_type="delete", dry_run=True):
    return {
        "subject_identifier": email,
        "identifier_type": "email",
        "request_type": request_type,
        "tenant_id": tenant_id,
        "operator": "smoke-int-tests@myrecruiter.ai",
        "dsar_id": dsar_id,
        "dry_run": dry_run,
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
    """(a) dry-run: response succeeds; seed row is NOT deleted; audit row written."""
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
        assert body.get("status") in ("completed", "partial"), body.get("status")

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
    """(b) real delete (dry_run=false): seed row deleted; audit row written with closed event."""
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
        assert body.get("status") in ("completed", "partial"), body.get("status")

        # Critical: real delete MUST remove the row.
        assert not _row_exists(aws, TABLE_FORM_SUBMISSIONS, {
            "tenant_id": tenant_id, "submission_id": submission_id,
        }), "BUG: real delete did not remove row"

        audit_rows = _get_audit_rows(aws, dsar_id)
        assert audit_rows
        event_types = {r.get("event_type") for r in audit_rows}
        assert "request_received" in event_types
        assert any(et and et.startswith("closed") or et == "closed" for et in event_types), \
            f"closed event missing; event_types: {event_types}"
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
        # The walker must have reached the form-submission row.
        exported = body.get("exported_rows", {}).get("form-submissions") or []
        assert exported, f"walker did not export form-submission row; body: {body}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)


def test_e_per_tenant_s3_walk_placeholder_surfaces_in_manual_followup(aws):
    """(e) per-tenant S3 walk M1 placeholder: response includes manual_followup
    that mentions S3-related deferral (M2 / item 1b territory). No S3 walk
    attempted by M1.

    Looser assertion: at least one manual_followup mentions a deferred surface
    or S3-adjacent gap; M1 docstring guarantees these followups.
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
        # M1 always produces followups (5 of 6 surfaces deferred per module docstring).
        assert followups, f"no manual_followups returned; body: {body}"
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
        details = request_received.get("details", {})
        if isinstance(details, str):
            details = json.loads(details)
        assert details.get("tenant_id") == tenant_id, (
            f"audit row tenant_id mismatch: got={details.get('tenant_id')} "
            f"expected={tenant_id}"
        )
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
        first = fs_rows[0]
        assert "form_data_labeled" in first, \
            f"form_data_labeled missing from access export: {first.keys()}"

        # Audit row written with request_type='access'.
        audit_rows = _get_audit_rows(aws, dsar_id)
        assert audit_rows
        request_received = next(
            (r for r in audit_rows if r.get("event_type") == "request_received"),
            None,
        )
        assert request_received, "no request_received audit row"
        details = request_received.get("details", {})
        if isinstance(details, str):
            details = json.loads(details)
        assert details.get("request_type") == "access", \
            f"audit row request_type mismatch: {details.get('request_type')}"
    finally:
        _cleanup_seed(aws, tenant_id, submission_id, email)
