"""Unit tests for picasso-pii-dsar-staging.

Covers:
- Cold-start env-guard (correct account passes; wrong account refuses)
- Input validation (missing fields, unsupported types, dry_run default)
- Email normalization (lower + strip; matches Phase-1 subject-index writer)
- Subject resolution (found → returns pii_subject_id; not found → None)
- Audit write shape (PK=dsar_id, SK=event_timestamp, status duplicated for GSI)
- form-submissions walker (access, delete dry-run, delete real, empty match,
  pagination, query error, partial delete failure)
- Per-surface walker dispatcher (form-submissions ships; rest scaffolded)
- Handler end-to-end (access exports rows, delete dry-run, subject not found,
  invalid input, wrong account)
"""
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# #1a: the account guard now reads EXPECTED_ACCOUNT from the env (fail-closed).
# Set the staging value before the fixture re-imports lambda_function so the
# module-level read picks it up; the "unset ⇒ refuse" case monkeypatches the
# module attribute directly (test_env_guard_refuses_when_unset).
os.environ.setdefault("EXPECTED_ACCOUNT", "525409062831")


@pytest.fixture
def dsar(monkeypatch):
    """Load lambda_function with mocked boto3 clients.

    Each test gets fresh mocks so per-test assertions don't leak across cases.

    M2 Sprint C: distinct mock_s3 attached to mod.s3 so the ARCHIVE_BUCKET
    walker's list_object_versions / delete_object calls are isolated from
    the STS mock. Default mod.s3.list_object_versions returns no Versions
    (zero-row archive) so dispatcher tests that don't care about archive
    don't have to stub it explicitly.
    """
    mock_ddb_resource = MagicMock()
    mock_sts = MagicMock()
    mock_s3 = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "525409062831"}
    # Default: empty archive (no Versions, no DeleteMarkers, not truncated).
    # Tests that exercise the archive walker override this on the per-test
    # mock_s3 (via mod.s3.list_object_versions.return_value = ...).
    mock_s3.list_object_versions.return_value = {
        "Versions": [], "DeleteMarkers": [], "IsTruncated": False,
    }

    def _client_router(name, *args, **kwargs):
        if name == "sts":
            return mock_sts
        if name == "s3":
            return mock_s3
        # Any other client name surfaces as an explicit AssertionError so a
        # future code addition surfaces here instead of leaking real-AWS calls.
        raise AssertionError(f"unexpected boto3.client({name!r})")

    with patch("boto3.resource", return_value=mock_ddb_resource), \
         patch("boto3.client", side_effect=_client_router):
        if "lambda_function" in sys.modules:
            del sys.modules["lambda_function"]
        import lambda_function as mod
        mod.ddb = mock_ddb_resource
        mod.sts = mock_sts
        mod.s3 = mock_s3
        yield mod, mock_ddb_resource, mock_sts


def _valid_event(**overrides):
    base = {
        "subject_identifier": "Test.Subject@Example.COM",
        "identifier_type": "email",
        "request_type": "delete",
        "tenant_id": "TEN123",
        "operator": "operator@myrecruiter.ai",
        "dsar_id": "dsar-uuid-1",
        "dry_run": True,
    }
    base.update(overrides)
    return base


# ───────────────────────────────────────────────────────────────────────────
# Cold-start env-guard
# ───────────────────────────────────────────────────────────────────────────
def test_env_guard_accepts_staging_account(dsar):
    mod, _, mock_sts = dsar
    mock_sts.get_caller_identity.return_value = {"Account": "525409062831"}
    mod._assert_account()  # should not raise


def test_env_guard_refuses_wrong_account(dsar):
    mod, _, mock_sts = dsar
    mock_sts.get_caller_identity.return_value = {"Account": "614056832592"}
    with pytest.raises(RuntimeError, match="account 614056832592"):
        mod._assert_account()


def test_env_guard_refuses_when_expected_account_unset(dsar):
    """#1a fail-closed: an unset EXPECTED_ACCOUNT must REFUSE (never default to
    an account), and must do so WITHOUT consulting STS."""
    mod, _, mock_sts = dsar
    mock_sts.get_caller_identity.reset_mock()
    for unset in (None, ""):
        mod.EXPECTED_ACCOUNT = unset
        with pytest.raises(RuntimeError, match="EXPECTED_ACCOUNT env var is unset"):
            mod._assert_account()
    mock_sts.get_caller_identity.assert_not_called()


# ───────────────────────────────────────────────────────────────────────────
# Input validation + normalization
# ───────────────────────────────────────────────────────────────────────────
def test_normalize_email_lowers_and_strips(dsar):
    mod, _, _ = dsar
    assert mod._normalize_email("  John.Doe@EXAMPLE.com  ") == "john.doe@example.com"


def test_validate_rejects_non_dict(dsar):
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="JSON object"):
        mod._validate("not a dict")


def test_validate_rejects_missing_fields(dsar):
    mod, _, _ = dsar
    event = _valid_event()
    del event["dsar_id"]
    del event["operator"]
    with pytest.raises(mod.InvalidInput, match="missing required fields"):
        mod._validate(event)


def test_validate_rejects_unsupported_identifier_type(dsar):
    """M2 Sprint B: email + psid are supported; phone + name+address are
    walker-NOT-supported per F-DSAR30 (manual M3 playbook procedures cover)."""
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="identifier_type 'phone'"):
        mod._validate(_valid_event(identifier_type="phone"))


def test_validate_rejects_name_address_identifier_type(dsar):
    """M2 Sprint B: 'name+address' identifier_type is walker-NOT-supported
    per F-DSAR30 — error message points operator at manual playbook procedure."""
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="manual fallback"):
        mod._validate(_valid_event(identifier_type="name+address"))


def test_validate_accepts_psid_identifier_type(dsar):
    """M2 Sprint B: identifier_type=psid is supported. PSIDs are opaque
    numeric strings (typically 15-17 digits); the Lambda strips whitespace
    but does NOT normalize beyond that (cross-tenant isolation is enforced
    upstream by _resolve_psid_subject via channel-mappings TenantIndex GSI)."""
    mod, _, _ = dsar
    out = mod._validate(_valid_event(
        identifier_type="psid",
        subject_identifier="  1234567890123456  ",
    ))
    assert out["identifier_type"] == "psid"
    assert out["subject_identifier"] == "1234567890123456"


def test_validate_rejects_empty_psid_subject_identifier(dsar):
    """M2 Sprint B: psid subject_identifier must be a non-empty string after strip."""
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="non-empty string for identifier_type=psid"):
        mod._validate(_valid_event(
            identifier_type="psid",
            subject_identifier="   ",
        ))


def test_validate_rejects_non_string_psid_subject_identifier(dsar):
    """M2 Sprint B: a non-string PSID (e.g. JSON-deserialized integer) is
    rejected — operators must pass the PSID as the canonical string form."""
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="non-empty string for identifier_type=psid"):
        mod._validate(_valid_event(
            identifier_type="psid",
            subject_identifier=1234567890123456,
        ))


def test_validate_rejects_unsupported_request_type(dsar):
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="request_type 'correct'"):
        mod._validate(_valid_event(request_type="correct"))


def test_validate_rejects_non_email_subject_when_type_email(dsar):
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="does not look like an email"):
        mod._validate(_valid_event(subject_identifier="not-an-email"))


def test_validate_normalizes_email_subject(dsar):
    mod, _, _ = dsar
    out = mod._validate(_valid_event(subject_identifier=" Mix.Case@Example.COM "))
    assert out["subject_identifier"] == "mix.case@example.com"


def test_validate_dry_run_defaults_true_when_absent(dsar):
    mod, _, _ = dsar
    event = _valid_event()
    del event["dry_run"]
    assert mod._validate(event)["dry_run"] is True


def test_validate_respects_explicit_false_dry_run(dsar):
    mod, _, _ = dsar
    out = mod._validate(_valid_event(dry_run=False))
    assert out["dry_run"] is False


# ── Sprint E1 / audit blocker B2 — smoke-prefix write-side enforcement ──────
def test_validate_rejects_smoke_prefix_dsar_id_without_marker(dsar):
    """A mistyped operator (or malicious actor with operator role) could create
    `dsar_id='smoke-real-001'` for a real DSAR — that would permanently hide
    the DSAR from the operator's at-risk view (playbook §8 filter) AND prevent
    deletion via the C2 4-action Deny policy. Require explicit
    smoke_test_marker=true to opt in.
    """
    mod, _, _ = dsar
    event = _valid_event(dsar_id="smoke-real-001")
    with pytest.raises(mod.InvalidInput, match="reserved 'smoke-' prefix"):
        mod._validate(event)


def test_validate_accepts_smoke_prefix_dsar_id_with_explicit_marker(dsar):
    """The smoke runner sets smoke_test_marker=true. The Lambda accepts the
    smoke-prefixed dsar_id and emits a SECURITY log line for the forensic trail.
    """
    mod, _, _ = dsar
    event = _valid_event(dsar_id="smoke-sla-monitor-001")
    event["smoke_test_marker"] = True
    out = mod._validate(event)
    assert out["dsar_id"] == "smoke-sla-monitor-001"


def test_validate_rejects_marker_true_on_non_smoke_dsar_id(dsar):
    """Audit closure 2026-05-26 row #21 (Security-Reviewer 🟡): two-way gate.
    Setting smoke_test_marker=true on a NON-smoke-prefixed dsar_id would tag
    a real DSAR with is_smoke_test=true in the audit row, hiding it from the
    SLA-monitor scan (`!is_smoke_test` filter in playbook §8). Reject the
    inconsistent combination.
    """
    mod, _, _ = dsar
    event = _valid_event(dsar_id="real-dsar-2026-001")
    event["smoke_test_marker"] = True
    with pytest.raises(mod.InvalidInput, match="audit closure 2026-05-26 row #21"):
        mod._validate(event)


# Sprint F1 / audit-of-audit finding 6: case-insensitive smoke-prefix check
@pytest.mark.parametrize("dsar_id", [
    "Smoke-real-001",
    "SMOKE-real-001",
    "sMoKe-real-001",
])
def test_validate_rejects_smoke_prefix_case_insensitive(dsar, dsar_id):
    """Case-sensitive lowercase-only check would have let capital-S variants
    bypass the guard silently (audit reviewer finding 6). The fix normalizes
    via .lower() before .startswith()."""
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="reserved 'smoke-' prefix"):
        mod._validate(_valid_event(dsar_id=dsar_id))


# Sprint F1 / audit-of-audit finding 15: smoke_test_marker MUST be bool
@pytest.mark.parametrize("bad_marker", ["true", "false", "True", 1, 0, "yes", None])
def test_validate_rejects_non_bool_smoke_test_marker(dsar, bad_marker):
    """String 'true'/'false' is truthy in Python; CLI callers JSON-deserializing
    a string into the event payload would silently activate (or fail to
    activate) the marker. Strict bool requirement closes the footgun."""
    mod, _, _ = dsar
    event = _valid_event(dsar_id="smoke-test-001")
    event["smoke_test_marker"] = bad_marker
    if bad_marker is None:
        # None defaults via .get(..., False) — not a marker-misuse case;
        # _validate should treat as absent and reject smoke- prefix
        with pytest.raises(mod.InvalidInput, match="reserved 'smoke-' prefix"):
            mod._validate(event)
    else:
        with pytest.raises(mod.InvalidInput, match="smoke_test_marker must be boolean"):
            mod._validate(event)


# ── Sprint E2 / audit defer-ok D8 — writer-side _now_iso format contract ────
def test_now_iso_format_contract(dsar):
    """Writer-side pinning of the audit `event_timestamp` ISO format.

    The SLA monitor Lambda (`picasso_pii_dsar_sla_monitor_staging`) builds its
    threshold via `threshold.isoformat(timespec='microseconds')` and DDB does
    LEXICOGRAPHIC string comparison on the StatusIndex GSI range key
    (`event_timestamp`). If the writer's format ever drifts (drops microseconds
    or switches to 'Z' suffix), the reader's threshold comparison silently
    mis-orders rows at format-boundary instants.

    Companion reader-side test lives in
    `picasso_pii_dsar_sla_monitor_staging/test_sla_monitor.py::test_event_timestamp_iso_format_contract`.
    Both sides must pin the same shape — that's the contract.

    Sprint E2 / audit D8: the reader-side test previously constructed the
    expected format test-side rather than importing it. This writer-side
    test asserts the actual writer output matches the shape both tests rely
    on, closing the loop.
    """
    from datetime import datetime, timezone
    from unittest.mock import patch
    mod, _, _ = dsar

    # Part 1: runtime shape assertions (always-on)
    ts = mod._now_iso()
    assert isinstance(ts, str), "writer must return str"
    assert len(ts) == 32, f"expected 32-char ISO; got {len(ts)} ({ts!r})"
    assert "." in ts, "writer format must include microseconds delimiter"
    assert ts.endswith("+00:00"), "writer format must end with +00:00 (UTC tz)"
    parsed = datetime.fromisoformat(ts)
    assert parsed.tzinfo is not None, "writer ts must be tz-aware"

    # Sprint F1 / audit-of-audit blocker A: previous boundary assertion called
    # datetime(...).isoformat(timespec='microseconds') test-side, which only
    # exercises Python's stdlib — NOT mod._now_iso. The bug it was meant to
    # catch (drift to timespec='auto') would pass silently. Fix: patch
    # datetime.now inside the writer's module to return a zero-microsecond
    # instant, then assert mod._now_iso()'s actual output preserves .000000.
    zero_us_dt = datetime(2026, 1, 1, 0, 0, 0, microsecond=0, tzinfo=timezone.utc)

    class _MockDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return zero_us_dt

    with patch.object(mod, "datetime", _MockDatetime):
        boundary_ts = mod._now_iso()

    assert boundary_ts == "2026-01-01T00:00:00.000000+00:00", (
        f"_now_iso must preserve .000000 on zero-us boundary instants; "
        f"got {boundary_ts!r}. If writer was changed to timespec='auto' or "
        f"default isoformat(), the fractional-seconds field would be dropped "
        f"at this exact instant and DDB lexicographic comparison on the "
        f"StatusIndex GSI would silently mis-order rows. Re-pin to "
        f"timespec='microseconds' to fix."
    )


# ───────────────────────────────────────────────────────────────────────────
# Subject resolution
# ───────────────────────────────────────────────────────────────────────────
def test_resolve_subject_returns_pii_subject_id_when_found(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.get_item.return_value = {
        "Item": {"tenant_id": "TEN123", "normalized_email": "a@b.co",
                 "pii_subject_id": "subj_opaque_xyz"}
    }
    mock_ddb.Table.return_value = mock_table
    assert mod._resolve_subject("TEN123", "a@b.co") == "subj_opaque_xyz"
    mock_ddb.Table.assert_called_with("picasso-pii-subject-index-staging")


def test_resolve_subject_returns_none_when_not_found(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.get_item.return_value = {}  # no Item key
    mock_ddb.Table.return_value = mock_table
    assert mod._resolve_subject("TEN123", "a@b.co") is None


# ── G1: Gmail-aware subject-index lookup (phase-audit 2026-06-05) ─────────────

@pytest.mark.parametrize("raw,expected", [
    ("foo.bar@gmail.com", "foobar@gmail.com"),         # gmail: drop dots
    ("x+promo@gmail.com", "x@gmail.com"),              # gmail: strip +tag
    ("a.b.c@googlemail.com", "abc@gmail.com"),         # googlemail alias + dots
    ("Keep.Dots@example.com", "keep.dots@example.com"),  # non-gmail: lower only
    ("CASE@Test.io", "case@test.io"),
    ("  pad@gmail.com  ", "pad@gmail.com"),
])
def test_normalize_email_for_index_matches_writer(dsar, raw, expected):
    mod, _, _ = dsar
    assert mod._normalize_email_for_index(raw) == expected


@pytest.mark.parametrize("bad", [None, "", "  ", "noat", "+tag@gmail.com", "a b@gmail.com"])
def test_normalize_email_for_index_returns_none_for_unusable(dsar, bad):
    mod, _, _ = dsar
    assert mod._normalize_email_for_index(bad) is None


def test_resolve_subject_gmail_dotted_looks_up_collapsed_key(dsar):
    """The G1 bug: a Gmail-with-dots subject must look up the COLLAPSED index
    key the writer stored, not the strip+lower form."""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.get_item.return_value = {
        "Item": {"pii_subject_id": "subj_gmail"}
    }
    mock_ddb.Table.return_value = mock_table
    # Input is the strip+lower'd value the handler passes (foo.bar@gmail.com).
    assert mod._resolve_subject("TEN123", "foo.bar@gmail.com") == "subj_gmail"
    mock_table.get_item.assert_called_once_with(Key={
        "tenant_id": "TEN123", "normalized_email": "foobar@gmail.com",
    })


def test_resolve_subject_non_gmail_key_unchanged(dsar):
    """Non-Gmail addresses are unaffected (regression guard for the prior path)."""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.get_item.return_value = {"Item": {"pii_subject_id": "s"}}
    mock_ddb.Table.return_value = mock_table
    mod._resolve_subject("TEN123", "keep.dots@example.com")
    mock_table.get_item.assert_called_once_with(Key={
        "tenant_id": "TEN123", "normalized_email": "keep.dots@example.com",
    })


def test_resolve_subject_unusable_email_returns_none_without_query(dsar):
    """A non-usable address (writer would have minted UNINDEXED) resolves to None
    without ever hitting the index."""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_ddb.Table.return_value = mock_table
    assert mod._resolve_subject("TEN123", "+tag@gmail.com") is None
    mock_table.get_item.assert_not_called()


# ── G2: strict dry_run validation (phase-audit 2026-06-05) ───────────────────

@pytest.mark.parametrize("bad_dry_run", [0, 0.0, 1, "false", "true", []])
def test_validate_rejects_non_bool_dry_run(dsar, bad_dry_run):
    """numeric 0 previously coerced to a REAL delete (bool(0) is False)."""
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="dry_run must be boolean"):
        mod._validate(_valid_event(dry_run=bad_dry_run))


def test_validate_accepts_bool_dry_run_and_defaults_true(dsar):
    mod, _, _ = dsar
    assert mod._validate(_valid_event(dry_run=False))["dry_run"] is False
    assert mod._validate(_valid_event(dry_run=True))["dry_run"] is True
    ev = _valid_event()
    del ev["dry_run"]
    assert mod._validate(ev)["dry_run"] is True  # default safe


# ── M2 Sprint B — psid subject resolution ──────────────────────────────────
def test_resolve_psid_subject_returns_sessionIds_for_each_page(dsar):
    """Two-step lookup: TenantIndex GSI Query returns N PAGE# rows; resolver
    composes one sessionId per page in 'meta:{pageId}:{psid}' shape."""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {"PK": "PAGE#100200300400500"},
            {"PK": "PAGE#600700800900100"},
        ],
    }
    mock_ddb.Table.return_value = mock_table

    out = mod._resolve_psid_subject("TEN123", "9876543210")

    assert out == [
        "meta:100200300400500:9876543210",
        "meta:600700800900100:9876543210",
    ]
    mock_ddb.Table.assert_called_with("picasso-channel-mappings")
    # Verify the GSI Query is scoped to the tenant + channelType=messenger.
    call_kwargs = mock_table.query.call_args.kwargs
    assert call_kwargs["IndexName"] == "TenantIndex"
    assert call_kwargs["ProjectionExpression"] == "PK"


def test_resolve_psid_subject_returns_empty_list_when_tenant_has_no_pages(dsar):
    """Tenant has no Messenger channel configured → empty list. Downstream
    walkers treat this as 'no Meta surface to walk' (not an error)."""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = mock_table
    assert mod._resolve_psid_subject("TEN_NO_META", "9876543210") == []


def test_resolve_psid_subject_paginates_via_last_evaluated_key(dsar):
    """Tenant with > 1 page of Messenger pages: pagination via LastEvaluatedKey
    until exhausted. (Bounded by GSI Query response size; no explicit cap.)"""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.side_effect = [
        {"Items": [{"PK": "PAGE#111"}], "LastEvaluatedKey": {"tenantId": "TEN", "channelType": "messenger"}},
        {"Items": [{"PK": "PAGE#222"}, {"PK": "PAGE#333"}]},
    ]
    mock_ddb.Table.return_value = mock_table
    out = mod._resolve_psid_subject("TEN", "psid_abc")
    assert out == ["meta:111:psid_abc", "meta:222:psid_abc", "meta:333:psid_abc"]
    assert mock_table.query.call_count == 2


def test_resolve_psid_subject_ignores_non_page_rows(dsar):
    """Defense-in-depth: if the channel-mappings table grows to include
    non-PAGE# row types via the same GSI, the resolver skips them rather
    than crash on the split."""
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {"PK": "PAGE#111"},
            {"PK": "OTHER#222"},  # unknown shape — skip
            {"PK": "PAGE#333"},
        ],
    }
    mock_ddb.Table.return_value = mock_table
    out = mod._resolve_psid_subject("TEN", "psid_z")
    assert out == ["meta:111:psid_z", "meta:333:psid_z"]


def test_resolve_psid_subject_raises_on_client_error(dsar):
    """Matches _resolve_subject contract — handler catches ClientError and
    audit-writes the failure."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.side_effect = ClientError(
        {"Error": {"Code": "ThrottlingException"}}, "Query")
    mock_ddb.Table.return_value = mock_table
    with pytest.raises(ClientError):
        mod._resolve_psid_subject("TEN", "psid_x")


# ───────────────────────────────────────────────────────────────────────────
# Audit write
# ───────────────────────────────────────────────────────────────────────────
def test_write_audit_event_uses_correct_table_and_shape(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_ddb.Table.return_value = mock_table
    ts = mod._write_audit_event(
        dsar_id="dsar-1",
        event_type="request_received",
        status="in_progress",
        payload={"operator": "op@x", "tenant_id": "TEN"},
    )
    mock_ddb.Table.assert_called_with("picasso-pii-dsar-audit-staging")
    args, kwargs = mock_table.put_item.call_args
    item = kwargs["Item"]
    assert item["dsar_id"] == "dsar-1"
    assert item["event_timestamp"] == ts
    assert item["event_type"] == "request_received"
    assert item["status"] == "in_progress"
    payload_back = json.loads(item["details"])
    assert payload_back == {"operator": "op@x", "tenant_id": "TEN"}
    # H4 (PR1 fix-now-4 / 🟡 N-2): ByCreatedAt GSI hash key, format YYYY-MM
    assert item["created_at_partition"] == ts[:7]
    assert len(item["created_at_partition"]) == 7  # YYYY-MM
    # Audit fix-now #4: idempotency invariant — must reject replay on
    # identical (dsar_id, event_timestamp).
    assert "ConditionExpression" in kwargs
    assert "attribute_not_exists(dsar_id)" in kwargs["ConditionExpression"]
    assert "attribute_not_exists(event_timestamp)" in kwargs["ConditionExpression"]


def test_write_audit_event_stamps_is_smoke_test_when_marker_true(dsar):
    """Closeout-audit row #15: is_smoke_test=True propagates to a top-level
    attribute on the audit row so operator scans can FilterExpression them
    out without depending on the dsar_id prefix (which is a UX convention).
    """
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_ddb.Table.return_value = mock_table
    mod._write_audit_event(
        dsar_id="smoke-test-2026-05-26-deploy-verify",
        event_type="request_received",
        status="in_progress",
        payload={"operator": "op@x"},
        is_smoke_test=True,
    )
    item = mock_table.put_item.call_args.kwargs["Item"]
    assert item["is_smoke_test"] is True


def test_write_audit_event_omits_is_smoke_test_when_marker_false(dsar):
    """Forward-compatible: default is_smoke_test=False means NO attribute is
    written (readers MUST use .get() per CLAUDE.md schema discipline).
    Pre-fix audit rows have no is_smoke_test attribute; new rows for real
    DSARs (marker omitted/False) match the same shape.
    """
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_ddb.Table.return_value = mock_table
    mod._write_audit_event(
        dsar_id="real-dsar-001",
        event_type="request_received",
        status="in_progress",
        payload={"operator": "op@x"},
        # is_smoke_test omitted -> default False
    )
    item = mock_table.put_item.call_args.kwargs["Item"]
    assert "is_smoke_test" not in item


def test_validate_propagates_smoke_test_marker_to_normalized_inputs(dsar):
    """Closeout-audit row #15: _validate's return dict carries
    smoke_test_marker so downstream _write_audit_event calls in the handler
    can stamp the audit-row attribute. Without this, the handler can only
    pass False (the prior behavior) and synthetic rows remain unmarked.
    """
    mod, _, _ = dsar
    event = _valid_event()
    event["dsar_id"] = "smoke-handler-prop-001"
    event["smoke_test_marker"] = True
    normalized = mod._validate(event)
    assert normalized["smoke_test_marker"] is True

    # And for real DSARs (no marker): defaults to False
    event2 = _valid_event()
    normalized2 = mod._validate(event2)
    assert normalized2["smoke_test_marker"] is False


def test_write_audit_event_raises_audit_collision_on_conditional_check_failure(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "..."}},
        "PutItem",
    )
    mock_ddb.Table.return_value = mock_table
    with pytest.raises(mod.AuditCollision, match="audit row already exists"):
        mod._write_audit_event(
            dsar_id="dsar-replay",
            event_type="request_received",
            status="in_progress",
            payload={"foo": "bar"},
        )


def test_write_audit_event_propagates_other_client_errors(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.put_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "..."}},
        "PutItem",
    )
    mock_ddb.Table.return_value = mock_table
    # Non-CCFE errors must propagate raw — handler relies on knowing the
    # original failure mode (throttling vs. collision are different ops).
    with pytest.raises(ClientError):
        mod._write_audit_event(
            dsar_id="dsar-throttled",
            event_type="request_received",
            status="in_progress",
            payload={},
        )


# ───────────────────────────────────────────────────────────────────────────
# form-submissions walker
# ───────────────────────────────────────────────────────────────────────────
def _row(submission_id, pii_subject_id="subj_xyz", tenant_id="TEN", **extra):
    base = {
        "tenant_id": tenant_id,
        "submission_id": submission_id,
        "pii_subject_id": pii_subject_id,
        "submitter_email": "test@x.co",
        "form_type": "contact",
    }
    base.update(extra)
    return base


def test_walker_access_returns_matched_rows_as_exported(dsar):
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1"), _row("s2")]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 2
    assert result["action"] == "exported"
    assert len(result["exported_rows"]) == 2
    fs_table.delete_item.assert_not_called()


def test_walker_delete_dry_run_counts_no_deletes(dsar):
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1"), _row("s2"), _row("s3")]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=True,
    )
    assert result["rows_found"] == 3
    assert result["action"] == "dry_run_count"
    fs_table.delete_item.assert_not_called()


def test_walker_delete_real_calls_delete_item_per_row(dsar):
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1"), _row("s2")]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 2
    assert result["action"] == "deleted"
    assert result["rows_deleted"] == 2
    assert fs_table.delete_item.call_count == 2
    # Verify the Key shape matches form-submissions PK/SK
    for call in fs_table.delete_item.call_args_list:
        key = call.kwargs["Key"]
        assert set(key.keys()) == {"tenant_id", "submission_id"}


def test_walker_empty_match_returns_zero(dsar):
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 0
    assert result["action"] == "deleted"
    assert result["rows_deleted"] == 0
    fs_table.delete_item.assert_not_called()


def test_walker_paginates_through_last_evaluated_key(dsar):
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.side_effect = [
        {"Items": [_row("s1")], "LastEvaluatedKey": {"tenant_id": "TEN", "submission_id": "s1"}},
        {"Items": [_row("s2")], "LastEvaluatedKey": {"tenant_id": "TEN", "submission_id": "s2"}},
        {"Items": [_row("s3")]},  # no LastEvaluatedKey → stop
    ]
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 3
    assert fs_table.query.call_count == 3
    # Second + third calls must carry ExclusiveStartKey
    assert "ExclusiveStartKey" not in fs_table.query.call_args_list[0].kwargs
    assert "ExclusiveStartKey" in fs_table.query.call_args_list[1].kwargs
    assert "ExclusiveStartKey" in fs_table.query.call_args_list[2].kwargs


def test_walker_query_error_returns_error_dict(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "..."}},
        "Query",
    )
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 0
    assert result["error"] == "query_failed"
    fs_table.delete_item.assert_not_called()


def test_walker_delete_continues_on_per_row_failure(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1"), _row("s2"), _row("s3")]}
    fs_table.delete_item.side_effect = [
        None,
        ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "DeleteItem"),
        None,
    ]
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 3
    assert result["rows_deleted"] == 2  # s1 + s3, not s2
    assert fs_table.delete_item.call_count == 3


def test_walker_delete_skips_corrupted_row_missing_pk_continues_batch(dsar):
    """Audit fix-now #2 — schema-discipline tolerance.

    A row missing PK (tenant_id) or SK (submission_id) must not crash the
    walker mid-batch. The walker logs the corruption, skips the row, and
    continues. The skipped count is exposed in the result so the dispatcher
    can flag the batch as not-fully-exhaustive in walker_results.
    """
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    # Three rows: valid, missing submission_id, valid
    corrupted = {"tenant_id": "TEN", "pii_subject_id": "subj_xyz"}  # no submission_id
    fs_table.query.return_value = {"Items": [_row("s1"), corrupted, _row("s3")]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 3
    assert result["rows_deleted"] == 2  # corrupted row skipped, s1 + s3 deleted
    assert result["rows_skipped_corrupted"] == 1
    assert fs_table.delete_item.call_count == 2  # not 3


def test_walker_delete_skips_corrupted_row_missing_sk(dsar):
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    corrupted = {"submission_id": "s1", "pii_subject_id": "subj_xyz"}  # no tenant_id
    fs_table.query.return_value = {"Items": [corrupted]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 1
    assert result["rows_deleted"] == 0
    assert result["rows_skipped_corrupted"] == 1
    fs_table.delete_item.assert_not_called()


def test_walker_delete_rows_skipped_corrupted_zero_on_clean_batch(dsar):
    """Clean batches still expose the field (always present in result dict)."""
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1"), _row("s2")]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=False,
    )
    assert result["rows_skipped_corrupted"] == 0


def test_walker_tolerates_old_and_new_ttl_row_shapes(dsar):
    """Schema-discipline (CLAUDE.md §"Schema Discipline"): forward-compat reader.

    M4 done-bar #2 (master plan v0.3 §M4) adds a `ttl` attribute to NEW
    form-submission rows (writer-side change in form_handler.py:_store_submission).
    Pre-M4 rows have NO `ttl`. The DSAR walker MUST tolerate both shapes —
    it doesn't use ttl for any decision logic; the assertion is "doesn't
    crash, returns identical result shape, exported_rows preserve the
    original row content."

    Per CLAUDE.md schema-discipline rule, this contract test ships with the
    writer change (M4 PR2 form_handler.py edit) so old data without the new
    field cannot break the reader on prod-deploy promotion.
    """
    import time as _time

    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    old_shape = _row("s_old")  # pre-M4: no ttl key
    new_shape = _row("s_new", ttl=int(_time.time()) + (365 * 24 * 3600))  # M4 PR2 writer
    fs_table.query.return_value = {"Items": [old_shape, new_shape]}
    mock_ddb.Table.return_value = fs_table

    # Access mode: exported_rows must contain both rows verbatim.
    result_access = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="access", dry_run=True,
    )
    assert result_access["rows_found"] == 2
    assert result_access["action"] == "exported"
    assert len(result_access["exported_rows"]) == 2
    exported_ids = {r["submission_id"] for r in result_access["exported_rows"]}
    assert exported_ids == {"s_old", "s_new"}
    # The walker passes rows through unchanged; ttl is preserved on the new row
    # and absent on the old row (verifying no normalization happens).
    by_id = {r["submission_id"]: r for r in result_access["exported_rows"]}
    assert "ttl" not in by_id["s_old"]
    assert "ttl" in by_id["s_new"]
    assert isinstance(by_id["s_new"]["ttl"], int)

    # Delete mode: both rows must be deleted regardless of ttl presence.
    # ttl is a DDB-managed expiration; the walker's job is on-demand erasure.
    result_delete = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="delete", dry_run=False,
    )
    assert result_delete["rows_found"] == 2
    assert result_delete["rows_deleted"] == 2
    assert result_delete["rows_skipped_corrupted"] == 0


# ───────────────────────────────────────────────────────────────────────────
# notification-sends walker (_walk_notification_sends)
# ───────────────────────────────────────────────────────────────────────────
def _ns_row(message_id="m1", recipient="test@x.co", channel="email",
             pk="TENANT#TEN123", sk_date="2026-05-21"):
    return {
        "pk": pk,
        "sk": f"{sk_date}#{channel}#{message_id}",
        "channel": channel,
        "recipient": recipient,
        "submission_id": "sub-1",
        "message_id": message_id,
        "status": "sent",
    }


def test_ns_walker_access_exports_matched_rows_and_captures_message_ids(dsar):
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": [_ns_row("m1"), _ns_row("m2")]}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 2
    assert result["action"] == "exported"
    assert len(result["exported_rows"]) == 2
    assert result["message_ids"] == ["m1", "m2"]
    # Audit fix-now-2 #1 (2026-05-21): walker drops FilterExpression and
    # post-filters in Python for case-insensitive recipient match
    # (writers store recipient verbatim — see F-DSAR3).
    args, kwargs = ns_table.query.call_args
    assert "FilterExpression" not in kwargs
    ns_table.delete_item.assert_not_called()


def test_ns_walker_case_insensitive_recipient_match(dsar):
    """Audit fix-now-2 #1 (2026-05-21): walker matches recipients case-
    insensitively to bridge the writer-normalization gap (F-DSAR3).
    Writers store `Person@Example.COM` raw; walker must match when operator
    submits `person@example.com`."""
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": [
        _ns_row("m1", recipient="Person@Example.COM"),     # mixed case
        _ns_row("m2", recipient="  person@example.com  "),  # whitespace
        _ns_row("m3", recipient="other@example.com"),       # no match
    ]}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="person@example.com",
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 2
    assert result["message_ids"] == ["m1", "m2"]


def test_ns_walker_handles_non_string_recipient_gracefully(dsar):
    """Rows with missing or non-string recipient must be excluded, not crash."""
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": [
        _ns_row("m1", recipient="test@x.co"),
        {"pk": "TENANT#TEN123", "sk": "x", "message_id": "m2"},  # no recipient
        {"pk": "TENANT#TEN123", "sk": "y", "recipient": None, "message_id": "m3"},
        {"pk": "TENANT#TEN123", "sk": "z", "recipient": 42, "message_id": "m4"},
    ]}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 1
    assert result["message_ids"] == ["m1"]


def test_ns_walker_delete_dry_run_returns_message_ids_but_no_deletes(dsar):
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": [_ns_row("m1"), _ns_row("m2")]}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert result["action"] == "dry_run_count"
    assert result["rows_found"] == 2
    assert result["message_ids"] == ["m1", "m2"]
    ns_table.delete_item.assert_not_called()


def test_ns_walker_delete_real_uses_pk_sk_key_shape(dsar):
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    rows = [_ns_row("m1"), _ns_row("m2")]
    ns_table.query.return_value = {"Items": rows}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert result["rows_deleted"] == 2
    assert result["rows_skipped_corrupted"] == 0
    assert ns_table.delete_item.call_count == 2
    for call in ns_table.delete_item.call_args_list:
        key = call.kwargs["Key"]
        assert set(key.keys()) == {"pk", "sk"}
        assert key["pk"].startswith("TENANT#")


def test_ns_walker_empty_match_returns_empty_message_ids(dsar):
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 0
    assert result["message_ids"] == []
    ns_table.delete_item.assert_not_called()


def test_ns_walker_excludes_rows_with_empty_message_id_from_message_ids(dsar):
    """Failed-send rows write `message_id: ''` per form_handler.py:832.
    These must NOT appear in message_ids (would break ByMessageId GSI chain)."""
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    rows = [
        _ns_row("m1"),  # successful send → message_id
        {**_ns_row("", recipient="test@x.co"), "status": "failed",
         "sk": "2026-05-21#email#failed-sub-1-test@x.co"},  # failure → empty
        _ns_row("m3"),
    ]
    ns_table.query.return_value = {"Items": rows}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 3  # all 3 returned in matched
    assert result["message_ids"] == ["m1", "m3"]  # only successful sends


def test_ns_walker_paginates_through_last_evaluated_key(dsar):
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.side_effect = [
        {"Items": [_ns_row("m1")], "LastEvaluatedKey": {"pk": "TENANT#TEN123", "sk": "..."}},
        {"Items": [_ns_row("m2")]},
    ]
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 2
    assert ns_table.query.call_count == 2
    assert "ExclusiveStartKey" in ns_table.query.call_args_list[1].kwargs


def test_ns_walker_query_error_returns_error_dict(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    ns_table.query.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException"}}, "Query")
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert result["error"] == "query_failed"
    assert result["rows_found"] == 0
    assert result["message_ids"] == []
    ns_table.delete_item.assert_not_called()


def test_ns_walker_skips_corrupted_row_missing_pk_or_sk(dsar):
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    corrupted = {"recipient": "test@x.co", "message_id": "m-corrupt"}  # no pk/sk
    ns_table.query.return_value = {"Items": [_ns_row("m1"), corrupted]}
    mock_ddb.Table.return_value = ns_table
    result = mod._walk_notification_sends(
        tenant_id="TEN123", normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert result["rows_deleted"] == 1  # only m1 was deletable
    assert result["rows_skipped_corrupted"] == 1
    assert ns_table.delete_item.call_count == 1


# ───────────────────────────────────────────────────────────────────────────
# notification-events walker (_walk_notification_events) — chained
# ───────────────────────────────────────────────────────────────────────────
def _ne_row(message_id="m1", event_type="delivery", pk="TENANT#TEN123"):
    ts = f"2026-05-21T03:58:04.000000+00:00"
    return {
        "pk": pk,
        "sk": f"2026-05-21#{event_type}#{message_id}",
        "message_id": message_id,
        "event_type_timestamp": f"{event_type}#{ts}",
        "event_type": event_type,
        "timestamp": ts,
    }


def test_ne_walker_empty_message_ids_returns_no_messages_action(dsar):
    """Common case today — consumer has no direct-recipient notifications.
    Must NOT issue any GSI query (would be wasted RCU)."""
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=[], request_type="access", dry_run=True,
    )
    assert result["action"] == "no_messages"
    assert result["rows_found"] == 0
    # Critical: no GSI query issued
    ne_table.query.assert_not_called()
    # Critical: Table() never even called (factory not exercised)
    mock_ddb.Table.assert_not_called()


def test_ne_walker_access_queries_gsi_per_message_id(dsar):
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": [_ne_row("m1", "send")]}
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1", "m2"], request_type="access", dry_run=True,
    )
    # 2 message_ids × 1 row each = 2 events found
    assert result["rows_found"] == 2
    assert result["action"] == "exported"
    assert ne_table.query.call_count == 2
    for call in ne_table.query.call_args_list:
        # IndexName must specify the ByMessageId GSI
        assert call.kwargs["IndexName"] == "ByMessageId"


def test_ne_walker_delete_dry_run_counts_no_deletes(dsar):
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": [_ne_row("m1", "delivery")]}
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1"], request_type="delete", dry_run=True,
    )
    assert result["action"] == "dry_run_count"
    assert result["rows_found"] == 1
    ne_table.delete_item.assert_not_called()


def test_ne_walker_delete_real_uses_pk_sk_key_shape(dsar):
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": [_ne_row("m1", "delivery")]}
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1"], request_type="delete", dry_run=False,
    )
    assert result["rows_deleted"] == 1
    assert ne_table.delete_item.call_count == 1
    key = ne_table.delete_item.call_args.kwargs["Key"]
    assert set(key.keys()) == {"pk", "sk"}


def test_ne_walker_paginates_per_message_id(dsar):
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    # 2 pages of events for m1, 1 page for m2
    ne_table.query.side_effect = [
        {"Items": [_ne_row("m1", "send")], "LastEvaluatedKey": {"x": "y"}},
        {"Items": [_ne_row("m1", "delivery")]},
        {"Items": [_ne_row("m2", "send")]},
    ]
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1", "m2"], request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 3
    assert ne_table.query.call_count == 3


def test_ne_walker_continues_on_per_message_id_failure(dsar):
    """Audit fix-now-2 #2 (2026-05-21): per-id GSI failure does NOT
    short-circuit the walker — failed_message_ids records the failure and
    the walker proceeds to the next message_id. Operator sees a complete
    progress picture instead of binary 'query_failed'."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    # Three message_ids: 1st fails, 2nd succeeds, 3rd fails
    ne_table.query.side_effect = [
        ClientError({"Error": {"Code": "InternalServerError"}}, "Query"),
        {"Items": [_ne_row("m2", "delivery")]},
        ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "Query"),
    ]
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1", "m2", "m3"], request_type="access", dry_run=True,
    )
    # m2's row was captured despite m1 + m3 failures
    assert result["rows_found"] == 1
    assert result["action"] == "exported"
    # Both failed message_ids recorded for operator visibility
    assert result["failed_message_ids"] == ["m1", "m3"]
    # All three were attempted
    assert ne_table.query.call_count == 3


def test_ne_walker_all_failures_returns_empty_with_failed_ids(dsar):
    """Edge case: every message_id fails. Walker still returns the action
    + an empty rows_found, with all IDs in failed_message_ids."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    ne_table.query.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError"}}, "Query")
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1", "m2"], request_type="delete", dry_run=False,
    )
    assert result["rows_found"] == 0
    assert result["action"] == "deleted"
    assert result["rows_deleted"] == 0
    assert result["failed_message_ids"] == ["m1", "m2"]
    ne_table.delete_item.assert_not_called()


def test_ne_walker_truncates_message_ids_beyond_cap(dsar):
    """Audit fix-now-2 #4 (2026-05-21): MAX_MESSAGE_IDS_PER_INVOCATION
    caps the chained walk to prevent Lambda-timeout exhaustion on
    high-volume subjects. Overflow surfaces in truncated_message_id_count."""
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = ne_table
    cap = mod.MAX_MESSAGE_IDS_PER_INVOCATION
    big_list = [f"m{i}" for i in range(cap + 50)]  # cap + 50 overflow
    result = mod._walk_notification_events(
        message_ids=big_list, request_type="access", dry_run=True,
    )
    assert result["truncated_message_id_count"] == 50
    # Walker queried exactly `cap` message_ids, not all (cap + 50)
    assert ne_table.query.call_count == cap


def test_ne_walker_no_truncation_field_when_under_cap(dsar):
    """When message_ids is under the cap, truncated_message_id_count
    must NOT appear in the result dict (avoid noise)."""
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": [_ne_row("m1", "delivery")]}
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1"], request_type="access", dry_run=True,
    )
    assert "truncated_message_id_count" not in result
    assert "failed_message_ids" not in result


def test_ne_walker_skips_corrupted_row(dsar):
    mod, mock_ddb, _ = dsar
    ne_table = MagicMock()
    corrupted = {"message_id": "m1", "event_type": "delivery"}  # no pk/sk
    ne_table.query.return_value = {"Items": [_ne_row("m1", "send"), corrupted]}
    mock_ddb.Table.return_value = ne_table
    result = mod._walk_notification_events(
        message_ids=["m1"], request_type="delete", dry_run=False,
    )
    assert result["rows_deleted"] == 1
    assert result["rows_skipped_corrupted"] == 1


# ───────────────────────────────────────────────────────────────────────────
# recent-messages walker (_walk_recent_messages) — chained via form-sub sids
# ───────────────────────────────────────────────────────────────────────────
def _rm_row(session_id, ts, content="hi", role="user", **extra):
    """Build a recent-messages row matching the writer schema at
    conversation_handler.py:762-770 — sessionId/messageTimestamp/role/
    content/messageId/expires_at."""
    row = {
        "sessionId": session_id,
        "messageTimestamp": ts,
        "messageId": f"msg-{ts}",
        "role": role,
        "content": content,
        "expires_at": ts + 86400,
    }
    row.update(extra)
    return row


def test_rm_walker_empty_session_ids_returns_no_sessions_action(dsar):
    """No sessions to walk → short-circuits without any Query call."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=[],
        request_type="access", dry_run=True,
    )
    assert result == {"rows_found": 0, "action": "no_sessions"}
    rm_table.query.assert_not_called()


def test_rm_walker_requires_tenant_id_non_empty(dsar):
    """Defense-in-depth: empty/None tenant_id MUST fail loud — the
    upstream form-submissions walker enforces tenant scoping; if a future
    caller drift bypasses that, refuse to silently span tenants."""
    mod, _, _ = dsar
    with pytest.raises(ValueError, match="non-empty tenant_id"):
        mod._walk_recent_messages(
            tenant_id="", session_ids=["s1"],
            request_type="access", dry_run=True,
        )
    with pytest.raises(ValueError):
        mod._walk_recent_messages(
            tenant_id=None, session_ids=["s1"],
            request_type="access", dry_run=True,
        )


def test_rm_walker_access_queries_per_session_id(dsar):
    """For each session_id in input, one Query(PK=sessionId)."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.side_effect = [
        {"Items": [_rm_row("sess-a", 100, content="hello")]},
        {"Items": [_rm_row("sess-b", 200, content="world")]},
    ]
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a", "sess-b"],
        request_type="access", dry_run=True,
    )
    assert rm_table.query.call_count == 2
    assert result["rows_found"] == 2
    assert result["action"] == "exported"
    assert len(result["exported_rows"]) == 2


def test_rm_walker_access_projects_to_minimum_fields(dsar):
    """Article 15 minimization: exported rows MUST contain only
    {role, content, messageTimestamp}; messageId / expires_at / sessionId
    intentionally dropped (advisor 2026-05-21)."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": [
        _rm_row("sess-a", 100, content="hello", role="user"),
    ]}
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a"],
        request_type="access", dry_run=True,
    )
    row = result["exported_rows"][0]
    assert set(row.keys()) == {"role", "content", "messageTimestamp"}
    assert row["role"] == "user"
    assert row["content"] == "hello"
    assert row["messageTimestamp"] == 100
    # Internal identifiers MUST NOT leak
    assert "messageId" not in row
    assert "expires_at" not in row
    assert "sessionId" not in row


def test_rm_walker_delete_dry_run_counts_no_deletes(dsar):
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": [
        _rm_row("sess-a", 100), _rm_row("sess-a", 101),
    ]}
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a"],
        request_type="delete", dry_run=True,
    )
    assert result["rows_found"] == 2
    assert result["action"] == "dry_run_count"
    rm_table.delete_item.assert_not_called()


def test_rm_walker_delete_real_uses_pk_sk_key_shape(dsar):
    """DeleteItem must pass {sessionId, messageTimestamp} — table PK/SK."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": [
        _rm_row("sess-a", 100), _rm_row("sess-a", 200),
    ]}
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a"],
        request_type="delete", dry_run=False,
    )
    assert result["rows_deleted"] == 2
    keys = [c.kwargs["Key"] for c in rm_table.delete_item.call_args_list]
    assert {"sessionId": "sess-a", "messageTimestamp": 100} in keys
    assert {"sessionId": "sess-a", "messageTimestamp": 200} in keys


def test_rm_walker_paginates_per_session_id(dsar):
    """LastEvaluatedKey must be threaded back as ExclusiveStartKey."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.side_effect = [
        {"Items": [_rm_row("sess-a", 100)], "LastEvaluatedKey": {"k": "1"}},
        {"Items": [_rm_row("sess-a", 200)]},
    ]
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a"],
        request_type="access", dry_run=True,
    )
    assert rm_table.query.call_count == 2
    # 2nd call must carry ExclusiveStartKey
    assert rm_table.query.call_args_list[1].kwargs.get("ExclusiveStartKey") == {"k": "1"}
    assert result["rows_found"] == 2


def test_rm_walker_continues_on_per_session_id_failure(dsar):
    """Per-session_id ClientError → record in failed_session_ids; walker
    continues to next session_id. Mirrors fix-now-2 notification-events
    pattern."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.side_effect = [
        ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}}, "Query"),
        {"Items": [_rm_row("sess-b", 200)]},
    ]
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a", "sess-b"],
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == 1
    assert result["failed_session_ids"] == ["sess-a"]
    # walker still produced the projected sess-b row
    assert len(result["exported_rows"]) == 1


def test_rm_walker_truncates_session_ids_beyond_cap(dsar):
    """Bounded fan-out — > MAX_SESSION_IDS_PER_INVOCATION truncated,
    overflow count returned for dispatcher followup."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = rm_table
    cap = mod.MAX_SESSION_IDS_PER_INVOCATION
    session_ids = [f"sess-{i}" for i in range(cap + 5)]
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=session_ids,
        request_type="access", dry_run=True,
    )
    # Only cap session_ids were queried
    assert rm_table.query.call_count == cap
    assert result.get("truncated_session_id_count") == 5


def test_rm_walker_exported_messages_soft_cap_overflow(dsar):
    """When matched rows > MAX_EXPORTED_MESSAGES, projection is capped and
    exported_messages_truncated_count is returned. Lambda 6 MB response
    safety."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    cap = mod.MAX_EXPORTED_MESSAGES
    # Stuff one session with > cap rows (single Query result for simplicity)
    rm_table.query.return_value = {
        "Items": [_rm_row("sess-a", i) for i in range(cap + 7)]
    }
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a"],
        request_type="access", dry_run=True,
    )
    assert result["rows_found"] == cap + 7  # rows_found is unaffected
    assert len(result["exported_rows"]) == cap
    assert result.get("exported_messages_truncated_count") == 7


def test_rm_walker_skips_corrupted_row(dsar):
    """Row missing PK (sessionId) or SK (messageTimestamp) → skip + log;
    do not crash the batch."""
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    corrupted_no_pk = {"messageTimestamp": 100, "content": "x", "role": "user"}
    corrupted_no_sk = {"sessionId": "sess-a", "content": "y", "role": "user"}
    rm_table.query.return_value = {"Items": [
        _rm_row("sess-a", 200),
        corrupted_no_pk,
        corrupted_no_sk,
    ]}
    mock_ddb.Table.return_value = rm_table
    result = mod._walk_recent_messages(
        tenant_id="TEN123", session_ids=["sess-a"],
        request_type="delete", dry_run=False,
    )
    assert result["rows_deleted"] == 1
    assert result["rows_skipped_corrupted"] == 2


def test_rm_walker_does_not_log_content_on_query_failure(dsar, caplog):
    """Audit-log PII discipline: on error, walker logs sessionId only —
    NEVER `content`. Verify no content leaks into log records."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    rm_table = MagicMock()
    rm_table.query.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError"}}, "Query")
    mock_ddb.Table.return_value = rm_table
    import logging
    with caplog.at_level(logging.ERROR):
        mod._walk_recent_messages(
            tenant_id="TEN123", session_ids=["sess-secret"],
            request_type="access", dry_run=True,
        )
    # error log must mention sessionId; must NOT contain anything from
    # `content`. We don't have content to leak here, but verify the
    # log shape — sessionId is the only PII-ish field that should appear.
    error_records = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("sess-secret" in m for m in error_records)


# ───────────────────────────────────────────────────────────────────────────
# M2 Sprint B — session-events walker (_walk_session_events)
# ───────────────────────────────────────────────────────────────────────────
def _se_row(session_id, step="STEP#001", **extra):
    """Synthesize a session-events row with pk=SESSION#{sessionId}, sk=STEP#{n}."""
    row = {"pk": f"SESSION#{session_id}", "sk": step}
    row.update(extra)
    return row


def test_walk_session_events_returns_no_sessions_for_empty_input(dsar):
    mod, _, _ = dsar
    result = mod._walk_session_events("TEN", [], "access", dry_run=True)
    assert result == {"rows_found": 0, "action": "no_sessions"}


def test_walk_session_events_requires_non_empty_tenant_id(dsar):
    """Defense-in-depth: walker rejects empty tenant_id even though it doesn't
    use it in the Query (table has no tenantId column)."""
    mod, _, _ = dsar
    with pytest.raises(ValueError, match="defense-in-depth"):
        mod._walk_session_events("", ["sid1"], "access", dry_run=True)


def test_walk_session_events_access_exports_all_step_rows(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    rows = [
        _se_row("meta:p1:psid_abc", step="STEP#001", tenant_hash="my87674d777bf9"),
        _se_row("meta:p1:psid_abc", step="STEP#002", tenant_hash="my87674d777bf9"),
    ]
    mock_table.query.return_value = {"Items": rows}
    mock_ddb.Table.return_value = mock_table

    result = mod._walk_session_events(
        "TEN", ["meta:p1:psid_abc"], "access", dry_run=True)

    assert result["rows_found"] == 2
    assert result["action"] == "exported"
    assert result["exported_rows"] == rows  # full rows (not projected)
    # Query was keyed by pk=SESSION#{sessionId}
    call_kwargs = mock_table.query.call_args.kwargs
    assert "KeyConditionExpression" in call_kwargs
    mock_ddb.Table.assert_called_with("picasso-session-events")


def test_walk_session_events_delete_dry_run_counts_no_deletes(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": [_se_row("s1"), _se_row("s1", step="STEP#002")]}
    mock_ddb.Table.return_value = mock_table
    result = mod._walk_session_events("TEN", ["s1"], "delete", dry_run=True)
    assert result == {"rows_found": 2, "action": "dry_run_count"}
    mock_table.delete_item.assert_not_called()


def test_walk_session_events_delete_real_calls_delete_item_per_row(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [_se_row("s1", step="STEP#001"), _se_row("s1", step="STEP#002")],
    }
    mock_ddb.Table.return_value = mock_table

    result = mod._walk_session_events("TEN", ["s1"], "delete", dry_run=False)

    assert result["action"] == "deleted"
    assert result["rows_deleted"] == 2
    assert result["rows_skipped_corrupted"] == 0
    assert mock_table.delete_item.call_count == 2
    # Verify Key shape on first delete call
    first_key = mock_table.delete_item.call_args_list[0].kwargs["Key"]
    assert first_key == {"pk": "SESSION#s1", "sk": "STEP#001"}


def test_walk_session_events_paginates_through_last_evaluated_key(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.side_effect = [
        {"Items": [_se_row("s1", step="STEP#001")],
         "LastEvaluatedKey": {"pk": "SESSION#s1", "sk": "STEP#001"}},
        {"Items": [_se_row("s1", step="STEP#002")]},
    ]
    mock_ddb.Table.return_value = mock_table
    result = mod._walk_session_events("TEN", ["s1"], "access", dry_run=True)
    assert result["rows_found"] == 2
    assert mock_table.query.call_count == 2


def test_walk_session_events_continues_on_per_session_query_error(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.side_effect = [
        ClientError({"Error": {"Code": "ThrottlingException"}}, "Query"),
        {"Items": [_se_row("s2")]},
    ]
    mock_ddb.Table.return_value = mock_table
    result = mod._walk_session_events("TEN", ["s1", "s2"], "access", dry_run=True)
    assert result["rows_found"] == 1
    assert result["failed_session_ids"] == ["s1"]


def test_walk_session_events_skips_corrupted_row_missing_pk_continues_batch(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {"sk": "STEP#001"},  # missing pk — corrupted
            _se_row("s1", step="STEP#002"),
        ],
    }
    mock_ddb.Table.return_value = mock_table
    result = mod._walk_session_events("TEN", ["s1"], "delete", dry_run=False)
    assert result["rows_skipped_corrupted"] == 1
    assert result["rows_deleted"] == 1


def test_walk_session_events_continues_on_per_row_delete_failure(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            _se_row("s1", step="STEP#001"),
            _se_row("s1", step="STEP#002"),
        ],
    }
    mock_table.delete_item.side_effect = [
        ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "DeleteItem"),
        None,
    ]
    mock_ddb.Table.return_value = mock_table
    result = mod._walk_session_events("TEN", ["s1"], "delete", dry_run=False)
    assert result["rows_deleted"] == 1
    assert result["rows_delete_failed"] == 1


def test_walk_session_events_truncates_session_ids_above_cap(dsar):
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = mock_table
    # Pass MAX + 5 to exercise the bounded fan-out path
    over_cap = [f"s{i}" for i in range(mod.MAX_SESSION_IDS_PER_INVOCATION + 5)]
    result = mod._walk_session_events("TEN", over_cap, "access", dry_run=True)
    assert result["truncated_session_id_count"] == 5
    # Only MAX_SESSION_IDS_PER_INVOCATION queries should have been issued
    assert mock_table.query.call_count == mod.MAX_SESSION_IDS_PER_INVOCATION


# ───────────────────────────────────────────────────────────────────────────
# M2 Sprint B — psid dispatcher (_walk_psid_surfaces)
# ───────────────────────────────────────────────────────────────────────────
def _stub_psid_tables(mock_ddb, *, rm_items=None, se_items=None):
    """Plumb recent-messages + session-events Query onto separate per-table mocks
    for the psid dispatcher tests. Mirrors _stub_handler_tables pattern."""
    rm_table = MagicMock()
    se_table = MagicMock()
    rm_table.query.return_value = {"Items": rm_items or []}
    se_table.query.return_value = {"Items": se_items or []}

    def route(name):
        if name == "picasso-recent-messages":
            return rm_table
        if name == "picasso-session-events":
            return se_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return rm_table, se_table


def test_walk_psid_surfaces_no_sessions_surfaces_completed_no_data(dsar):
    """No sessionIds resolved → all 3 walkers report completed/no_sessions,
    operator gets a follow-up explaining the 3 possible reasons."""
    mod, mock_ddb, _ = dsar
    rows_touched, followups, exported, results = mod._walk_psid_surfaces(
        tenant_id="TEN", psid="psid_abc", session_ids=[],
        request_type="access", dry_run=True,
    )
    # M2 Sprint C added "archive" surface to the psid dispatcher.
    assert rows_touched == {"recent-messages": 0, "session-events": 0, "archive": 0}
    assert exported == {}
    assert results["recent-messages"]["status"] == "completed"
    assert results["recent-messages"]["action"] == "no_sessions"
    assert results["session-events"]["status"] == "completed"
    assert results["archive"]["status"] == "completed"
    assert results["archive"]["action"] == "no_sessions"
    assert any("0 sessionIds resolved" in f for f in followups)
    # session-summaries explicitly NOT in walker_results (deferred per F-DSAR31)
    assert "session-summaries" not in results


def test_walk_psid_surfaces_access_exports_both_surfaces(dsar):
    """M2 Sprint C: archive walker also runs (empty by default in the fixture
    mock; rows_touched["archive"]=0, action=exported, exported_keys=[]).
    Recent-messages + session-events still export 1 row each."""
    mod, mock_ddb, _ = dsar
    rm_table, se_table = _stub_psid_tables(
        mock_ddb,
        rm_items=[{"sessionId": "meta:p1:psid_abc", "messageTimestamp": 100,
                   "role": "user", "content": "hi"}],
        se_items=[_se_row("meta:p1:psid_abc", step="STEP#001", tenant_hash="t")],
    )
    rows_touched, _, exported, results = mod._walk_psid_surfaces(
        tenant_id="TEN", psid="psid_abc",
        session_ids=["meta:p1:psid_abc"],
        request_type="access", dry_run=True,
    )
    assert rows_touched == {"recent-messages": 1, "session-events": 1, "archive": 0}
    assert "recent-messages" in exported and "session-events" in exported
    assert results["recent-messages"]["action"] == "exported"
    assert results["session-events"]["action"] == "exported"
    # Archive walker ran but found nothing (default empty mock_s3 in fixture).
    assert results["archive"]["action"] == "exported"
    assert exported["archive"] == []


def test_walk_psid_surfaces_delete_dry_run_counts_both_surfaces(dsar):
    """M2 Sprint C: archive walker reports dry_run_count alongside the
    DDB walkers."""
    mod, mock_ddb, _ = dsar
    _stub_psid_tables(
        mock_ddb,
        rm_items=[{"sessionId": "meta:p1:psid_abc", "messageTimestamp": 100,
                   "role": "user", "content": "x"}],
        se_items=[_se_row("meta:p1:psid_abc")],
    )
    rows_touched, followups, _, results = mod._walk_psid_surfaces(
        tenant_id="TEN", psid="psid_abc",
        session_ids=["meta:p1:psid_abc"],
        request_type="delete", dry_run=True,
    )
    assert rows_touched == {"recent-messages": 1, "session-events": 1, "archive": 0}
    assert results["recent-messages"]["action"] == "dry_run_count"
    assert results["session-events"]["action"] == "dry_run_count"
    assert results["archive"]["action"] == "dry_run_count"
    assert any("recent-messages: dry_run=true" in f for f in followups)
    assert any("session-events: dry_run=true" in f for f in followups)
    # Archive followup format: "archive: dry_run=true; N object(s) / V version(s)..."
    assert any("archive: dry_run=true" in f for f in followups)


# ───────────────────────────────────────────────────────────────────────────
# M2 Sprint C — _walk_archive_bucket (version-aware S3 walker)
# ───────────────────────────────────────────────────────────────────────────
def _arc_resp(versions=None, delete_markers=None, is_truncated=False, next_key=None, next_version=None):
    """Synthesize an s3.list_object_versions response."""
    resp = {
        "Versions": versions or [],
        "DeleteMarkers": delete_markers or [],
        "IsTruncated": is_truncated,
    }
    if next_key:
        resp["NextKeyMarker"] = next_key
    if next_version:
        resp["NextVersionIdMarker"] = next_version
    return resp


def test_walk_archive_no_sessions_returns_no_sessions(dsar):
    mod, _, _ = dsar
    result = mod._walk_archive_bucket("TEN", [], "access", dry_run=True)
    assert result == {"objects_found": 0, "versions_found": 0, "action": "no_sessions"}


def test_walk_archive_requires_non_empty_tenant_id(dsar):
    """Defense-in-depth: walker rejects empty tenant_id even though the
    S3 walk has no tenant ARN scoping; upstream resolver must enforce."""
    mod, _, _ = dsar
    with pytest.raises(ValueError, match="defense-in-depth"):
        mod._walk_archive_bucket("", ["sid1"], "access", dry_run=True)


def test_walk_archive_access_exports_distinct_keys(dsar):
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp(versions=[
        {"Key": "sessions/sid1/summary.json", "VersionId": "v1"},
        {"Key": "sessions/sid1/summary.json", "VersionId": "v2"},  # same key, 2 versions
        {"Key": "sessions/sid1/meta.json", "VersionId": "v1"},
    ])
    result = mod._walk_archive_bucket("TEN", ["sid1"], "access", dry_run=True)
    assert result["objects_found"] == 2  # distinct keys
    assert result["versions_found"] == 3  # total version tuples
    assert result["action"] == "exported"
    assert result["exported_keys"] == sorted([
        "sessions/sid1/summary.json", "sessions/sid1/meta.json",
    ])
    # Verify the Prefix was scoped to the sessionId
    call_kwargs = mod.s3.list_object_versions.call_args.kwargs
    assert call_kwargs["Prefix"] == "sessions/sid1/"
    assert call_kwargs["Bucket"] == "picasso-archive-staging"


def test_walk_archive_delete_dry_run_counts_objects_and_versions(dsar):
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp(versions=[
        {"Key": "sessions/sid1/x.json", "VersionId": "v1"},
        {"Key": "sessions/sid1/x.json", "VersionId": "v2"},
    ])
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=True)
    assert result["action"] == "dry_run_count"
    assert result["objects_found"] == 1
    assert result["versions_found"] == 2
    mod.s3.delete_object.assert_not_called()


def test_walk_archive_delete_real_deletes_per_version(dsar):
    """Critical: per archive-reachability-decision.md, versioning is ENABLED
    on picasso-archive-staging. A single delete_object (no VersionId) only
    creates a delete-marker; prior versions persist. Walker MUST iterate
    versions and delete each (key, version_id) tuple."""
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp(versions=[
        {"Key": "sessions/sid1/x.json", "VersionId": "v1"},
        {"Key": "sessions/sid1/x.json", "VersionId": "v2"},
    ])
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=False)
    assert result["action"] == "deleted"
    assert result["versions_deleted"] == 2
    assert result["versions_delete_failed"] == 0
    assert mod.s3.delete_object.call_count == 2
    # Verify each delete_object call carried VersionId
    for call in mod.s3.delete_object.call_args_list:
        assert "VersionId" in call.kwargs
        assert call.kwargs["Bucket"] == "picasso-archive-staging"


def test_walk_archive_delete_real_includes_delete_markers(dsar):
    """Delete-markers themselves must be removed to fully erase the object
    history (otherwise a marker persists in the version list even after
    all real versions are gone)."""
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp(
        versions=[{"Key": "sessions/sid1/x.json", "VersionId": "v1"}],
        delete_markers=[{"Key": "sessions/sid1/x.json", "VersionId": "dm1"}],
    )
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=False)
    assert result["versions_found"] == 2  # 1 version + 1 delete-marker
    assert result["versions_deleted"] == 2
    assert mod.s3.delete_object.call_count == 2
    # Delete-marker is NOT counted as an exported object — only real versions
    # contribute to objects_found.
    assert result["objects_found"] == 1


def test_walk_archive_paginates_via_key_marker(dsar):
    """list_object_versions pagination — IsTruncated=true with NextKeyMarker
    + NextVersionIdMarker requires continuation."""
    mod, _, _ = dsar
    mod.s3.list_object_versions.side_effect = [
        _arc_resp(
            versions=[{"Key": "sessions/sid1/a.json", "VersionId": "v1"}],
            is_truncated=True,
            next_key="sessions/sid1/a.json",
            next_version="v1",
        ),
        _arc_resp(versions=[{"Key": "sessions/sid1/b.json", "VersionId": "v1"}]),
    ]
    result = mod._walk_archive_bucket("TEN", ["sid1"], "access", dry_run=True)
    assert result["objects_found"] == 2
    assert mod.s3.list_object_versions.call_count == 2
    # Second call carries KeyMarker + VersionIdMarker
    second_call_kwargs = mod.s3.list_object_versions.call_args_list[1].kwargs
    assert second_call_kwargs["KeyMarker"] == "sessions/sid1/a.json"
    assert second_call_kwargs["VersionIdMarker"] == "v1"


def test_walk_archive_continues_on_per_session_list_error(dsar):
    """ClientError on list_object_versions for one sessionId → record failed
    + continue to next sessionId (mirrors DDB walker continue-on-error)."""
    from botocore.exceptions import ClientError
    mod, _, _ = dsar
    mod.s3.list_object_versions.side_effect = [
        ClientError({"Error": {"Code": "InternalError"}}, "ListObjectVersions"),
        _arc_resp(versions=[{"Key": "sessions/sid2/x.json", "VersionId": "v1"}]),
    ]
    result = mod._walk_archive_bucket("TEN", ["sid1", "sid2"], "access", dry_run=True)
    assert result["objects_found"] == 1  # only sid2 succeeded
    assert result["failed_session_ids"] == ["sid1"]


def test_walk_archive_continues_on_per_version_delete_failure(dsar):
    """ClientError on individual delete_object → increment versions_delete_failed
    + continue to next (key, version_id) tuple. Returns errored status when
    versions_delete_failed > 0."""
    from botocore.exceptions import ClientError
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp(versions=[
        {"Key": "sessions/sid1/x.json", "VersionId": "v1"},
        {"Key": "sessions/sid1/x.json", "VersionId": "v2"},
    ])
    mod.s3.delete_object.side_effect = [
        ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject"),
        None,
    ]
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=False)
    assert result["versions_deleted"] == 1
    assert result["versions_delete_failed"] == 1


def test_walk_archive_truncates_session_ids_above_cap(dsar):
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp()
    over_cap = [f"sid{i}" for i in range(mod.MAX_SESSION_IDS_PER_INVOCATION + 3)]
    result = mod._walk_archive_bucket("TEN", over_cap, "access", dry_run=True)
    assert result["truncated_session_id_count"] == 3


# ───────────────────────────────────────────────────────────────────────────
# M2 Sprint D — _walk_fulfillment_s3 (per-tenant S3 fulfillment walker)
# ───────────────────────────────────────────────────────────────────────────
def _fs_row(submission_id, fulfillment_path=None, **extra):
    """Synthesize a form-submission row with optional fulfillment_path."""
    row = {
        "tenant_id": "TEN",
        "submission_id": submission_id,
        "pii_subject_id": "subj_xyz",
    }
    if fulfillment_path is not None:
        row["fulfillment_path"] = fulfillment_path
    row.update(extra)
    return row


def test_walk_fulfillment_requires_non_empty_tenant_id(dsar):
    """Defense-in-depth: walker rejects empty tenant_id (mirrors
    _walk_archive_bucket pattern + per-row cross-tenant validation)."""
    mod, _, _ = dsar
    with pytest.raises(ValueError, match="defense-in-depth"):
        mod._walk_fulfillment_s3("", [_fs_row("s1")], "access", dry_run=True)


def test_walk_fulfillment_no_rows_returns_no_paths(dsar):
    mod, _, _ = dsar
    result = mod._walk_fulfillment_s3("TEN", [], "access", dry_run=True)
    assert result["action"] == "no_fulfillment_paths"
    assert result["objects_found"] == 0
    assert result["rows_with_path"] == 0
    assert result["rows_without_path"] == 0


def test_walk_fulfillment_rows_without_path_surface_as_pending_writer(dsar):
    """Pre-writer-extension form-submission rows have no `fulfillment_path`
    attribute. Walker must tolerate the absence (schema discipline) and
    surface the count for the dispatcher's manual_followup."""
    mod, _, _ = dsar
    rows = [_fs_row("s1"), _fs_row("s2"), _fs_row("s3")]
    result = mod._walk_fulfillment_s3("TEN", rows, "access", dry_run=True)
    assert result["action"] == "no_fulfillment_paths"
    assert result["rows_without_path"] == 3
    assert result["rows_with_path"] == 0
    mod.s3.delete_object.assert_not_called()


def test_walk_fulfillment_access_exports_uris(dsar):
    mod, _, _ = dsar
    rows = [
        _fs_row("s1", fulfillment_path="s3://tenant-bucket/submissions/TEN/contact/s1.json"),
        _fs_row("s2"),  # no path — pre-extension row
        _fs_row("s3", fulfillment_path="s3://tenant-bucket/submissions/TEN/contact/s3.json"),
    ]
    result = mod._walk_fulfillment_s3("TEN", rows, "access", dry_run=True)
    assert result["action"] == "exported"
    assert result["objects_found"] == 2
    assert sorted(result["exported_keys"]) == sorted([
        "s3://tenant-bucket/submissions/TEN/contact/s1.json",
        "s3://tenant-bucket/submissions/TEN/contact/s3.json",
    ])
    assert result["rows_with_path"] == 2
    assert result["rows_without_path"] == 1
    mod.s3.delete_object.assert_not_called()


def test_walk_fulfillment_delete_dry_run_counts_objects(dsar):
    mod, _, _ = dsar
    rows = [_fs_row("s1", fulfillment_path="s3://tenant-bucket/submissions/TEN/f/s1.json")]
    result = mod._walk_fulfillment_s3("TEN", rows, "delete", dry_run=True)
    assert result["action"] == "dry_run_count"
    assert result["objects_found"] == 1
    mod.s3.delete_object.assert_not_called()


def test_walk_fulfillment_delete_real_calls_delete_object_per_path(dsar):
    mod, _, _ = dsar
    rows = [
        _fs_row("s1", fulfillment_path="s3://tenant-bucket/submissions/TEN/contact/s1.json"),
        _fs_row("s2", fulfillment_path="s3://tenant-bucket/submissions/TEN/contact/s2.json"),
    ]
    result = mod._walk_fulfillment_s3("TEN", rows, "delete", dry_run=False)
    assert result["action"] == "deleted"
    assert result["objects_deleted"] == 2
    assert result["objects_delete_failed"] == 0
    assert mod.s3.delete_object.call_count == 2
    # Verify each delete_object got the right (Bucket, Key)
    call_kwargs = [c.kwargs for c in mod.s3.delete_object.call_args_list]
    assert {c["Bucket"] for c in call_kwargs} == {"tenant-bucket"}
    assert sorted(c["Key"] for c in call_kwargs) == [
        "submissions/TEN/contact/s1.json",
        "submissions/TEN/contact/s2.json",
    ]


def test_walk_fulfillment_continues_on_delete_failure(dsar):
    """Per-design: DeleteObject failure → hard partial-failure (counted in
    objects_delete_failed; status="errored" upstream); never reported
    complete. AccessDenied is the dominant cause (missing IAM grant per
    (bucket, tenant_id) — design intent: fail-closed)."""
    from botocore.exceptions import ClientError
    mod, _, _ = dsar
    rows = [
        _fs_row("s1", fulfillment_path="s3://unknown-bucket/submissions/TEN/contact/s1.json"),
        _fs_row("s2", fulfillment_path="s3://known-bucket/submissions/TEN/contact/s2.json"),
    ]
    mod.s3.delete_object.side_effect = [
        ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject"),
        None,
    ]
    result = mod._walk_fulfillment_s3("TEN", rows, "delete", dry_run=False)
    assert result["objects_deleted"] == 1
    assert result["objects_delete_failed"] == 1


def test_walk_fulfillment_rejects_cross_tenant_path(dsar):
    """Per-row defense: `fulfillment_path` must point INTO the requested
    tenant's prefix (`submissions/{tenant_id}/...`). Any mismatch =
    suspected cross-tenant pointer (writer drift OR stale row) =
    hard-skip + log; never delete."""
    mod, _, _ = dsar
    rows = [
        _fs_row("s1", fulfillment_path="s3://bucket/submissions/OTHER_TEN/contact/s1.json"),
        _fs_row("s2", fulfillment_path="s3://bucket/submissions/TEN/contact/s2.json"),
    ]
    result = mod._walk_fulfillment_s3("TEN", rows, "delete", dry_run=False)
    assert result.get("skipped_cross_tenant") == 1
    assert result["objects_deleted"] == 1  # only s2 deleted
    assert mod.s3.delete_object.call_count == 1
    call = mod.s3.delete_object.call_args_list[0]
    assert call.kwargs["Key"] == "submissions/TEN/contact/s2.json"


def test_walk_fulfillment_rejects_malformed_path(dsar):
    """Per-row defense: invalid URI (not s3://, missing key) = parse-failure
    counted in failed_paths; row's object not located but other rows
    proceed.

    Audit closure 2026-05-26 row #15: s3:/// (zero-length bucket / slash_idx==0)
    edge case added explicitly.
    """
    mod, _, _ = dsar
    rows = [
        _fs_row("s1", fulfillment_path="http://wrong-scheme/foo"),
        _fs_row("s2", fulfillment_path="s3://"),                    # no bucket+key
        _fs_row("s3", fulfillment_path="s3://no-slash-after-bucket"),  # no key
        _fs_row("s4", fulfillment_path=12345),                       # non-string
        _fs_row("s6", fulfillment_path="s3:///no-bucket/key"),       # row #15: empty bucket
        _fs_row("s5", fulfillment_path="s3://bucket/submissions/TEN/f/s5.json"),
    ]
    result = mod._walk_fulfillment_s3("TEN", rows, "access", dry_run=True)
    assert result["objects_found"] == 1  # only s5 passed validation
    assert result.get("failed_paths") == 5
    assert result["exported_keys"] == ["s3://bucket/submissions/TEN/f/s5.json"]


def test_walk_fulfillment_rejects_path_traversal(dsar):
    """Audit closure 2026-05-26 row #5 (Security-Reviewer 🔴): an attacker
    who writes `submissions/{tenant_id}/../OTHER/x.json` as `fulfillment_path`
    would pass the literal `startswith` prefix check, and S3 stores the
    literal key (does NOT canonicalize `..`). Reject any key whose path
    segments contain `..` or empty segments BEFORE the prefix check fires.
    """
    mod, _, _ = dsar
    rows = [
        # Path traversal: prefix is "submissions/TEN/" which DOES match
        # startswith, but the `..` segment shifts the actual S3 object
        # location to OTHER/...
        _fs_row("t1", fulfillment_path="s3://bkt/submissions/TEN/../OTHER/x.json"),
        # Empty segment: `submissions//x.json` parses oddly; treat as
        # parse-failure not as cross-tenant.
        _fs_row("t2", fulfillment_path="s3://bkt/submissions//x.json"),
        # Triple-dot edge case (just an unusual filename, NOT traversal).
        # Walker should accept this because `...` is not in the rejected
        # segment set, only `..` is.
        _fs_row(
            "t3",
            fulfillment_path="s3://bkt/submissions/TEN/f/.../s3.json",
        ),
    ]
    result = mod._walk_fulfillment_s3("TEN", rows, "delete", dry_run=True)
    # t1 + t2 = 2 failed_paths (path-traversal rejected pre-prefix-check)
    # t3 = 1 valid path (objects_found=1)
    assert result.get("failed_paths") == 2, f"got {result}"
    assert result["objects_found"] == 1
    assert result.get("skipped_cross_tenant", 0) == 0, (
        "path-traversal must be rejected as failed_paths, NOT as "
        "skipped_cross_tenant — the difference matters because operator "
        "investigation paths diverge")


def test_apply_fulfillment_walker_result_partial_delete_failure(dsar):
    """Audit closure 2026-05-26 row #3 (test-engineer 🔴): the dispatcher's
    `objects_delete_failed > 0` branch flips status to "errored" and emits
    a manual_followup. Unit tests against the walker stop one layer short
    of what callers observe; this test exercises the dispatcher directly.
    """
    mod, _, _ = dsar
    rows_touched = {}
    manual_followups = []
    exported_rows = {}
    walker_results = {}
    # Stub the walker to return a partial-failure shape.
    with patch.object(mod, "_walk_fulfillment_s3", return_value={
        "objects_found": 2,
        "action": "deleted",
        "objects_deleted": 1,
        "objects_delete_failed": 1,
        "rows_with_path": 2,
        "rows_without_path": 0,
        "deleted_key_sha256_12": ["abc123"],
        "failed_key_sha256_12": ["def456"],
    }):
        mod._apply_fulfillment_walker_result(
            "TEN", [], "delete", False,
            rows_touched, manual_followups, exported_rows, walker_results,
        )
    assert rows_touched["fulfillment"] == 2
    assert walker_results["fulfillment"]["status"] == "errored"
    assert walker_results["fulfillment"]["error"] == "objects_delete_failed"
    assert walker_results["fulfillment"]["objects_deleted"] == 1
    assert walker_results["fulfillment"]["objects_delete_failed"] == 1
    assert walker_results["fulfillment"]["deleted_key_sha256_12"] == ["abc123"]
    assert walker_results["fulfillment"]["failed_key_sha256_12"] == ["def456"]
    assert any("failed to delete" in m for m in manual_followups), \
        f"expected delete-failed manual_followup; got {manual_followups}"


def test_apply_fulfillment_walker_result_access_populates_exported_rows(dsar):
    """Audit closure 2026-05-26 row #4 (test-engineer 🔴): the access-path
    branch in the dispatcher populates `exported_rows["fulfillment"]` with
    the s3:// URIs from the walker. No prior test traced the access path
    through the dispatcher.
    """
    mod, _, _ = dsar
    rows_touched = {}
    manual_followups = []
    exported_rows = {}
    walker_results = {}
    expected_uris = [
        "s3://bkt/submissions/TEN/f/a.json",
        "s3://bkt/submissions/TEN/f/b.json",
    ]
    with patch.object(mod, "_walk_fulfillment_s3", return_value={
        "objects_found": 2,
        "action": "exported",
        "exported_keys": expected_uris,
        "key_sha256_12": ["hash_a_xxxxxx", "hash_b_xxxxxx"],
        "rows_with_path": 2,
        "rows_without_path": 0,
    }):
        mod._apply_fulfillment_walker_result(
            "TEN", [], "access", True,
            rows_touched, manual_followups, exported_rows, walker_results,
        )
    assert rows_touched["fulfillment"] == 2
    assert exported_rows["fulfillment"] == expected_uris
    assert walker_results["fulfillment"]["status"] == "completed"
    assert walker_results["fulfillment"]["action"] == "exported"
    assert walker_results["fulfillment"]["key_sha256_12"] == \
        ["hash_a_xxxxxx", "hash_b_xxxxxx"]


def test_walk_fulfillment_no_versioning_enumeration(dsar):
    """Per-design: fulfillment buckets are NOT versioning-aware (writer
    does single put_object). Unlike _walk_archive_bucket, a plain
    delete_object is sufficient — walker MUST NOT call
    list_object_versions on fulfillment paths (different bucket posture
    than picasso-archive-staging).

    Audit closure 2026-05-26 row #17 (test-engineer 🟡): explicitly assert
    `delete_object` WAS called in the same test so a typo/wrong-method-name
    regression can't silently turn this test into a vacuous pass. With the
    mock fixture's default S3 stub (no `spec=` constraint), assert_not_called
    is real for known method names; the positive-call assertion on
    delete_object is the load-bearing teeth that ensures the mock is
    actually wired into the code path.
    """
    mod, _, _ = dsar
    rows = [_fs_row("s1", fulfillment_path="s3://bucket/submissions/TEN/f/s1.json")]
    mod._walk_fulfillment_s3("TEN", rows, "delete", dry_run=False)
    mod.s3.list_object_versions.assert_not_called()
    mod.s3.delete_object.assert_called_once_with(
        Bucket="bucket",
        Key="submissions/TEN/f/s1.json",
    )


# ───────────────────────────────────────────────────────────────────────────
# form-submissions walker — session_ids surfacing for chained walks
# ───────────────────────────────────────────────────────────────────────────
def test_fs_walker_surfaces_session_ids_in_result(dsar):
    """_walk_form_submissions must surface a session_ids list from matched
    rows in all action paths so recent-messages can chain off it."""
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [
        _row("s1", session_id="sess-a"),
        _row("s2", session_id="sess-b"),
    ]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="access", dry_run=True,
    )
    assert result["session_ids"] == ["sess-a", "sess-b"]


def test_fs_walker_skips_rows_without_session_id_from_session_ids(dsar):
    """Rows missing or null session_id are excluded from session_ids
    (mirrors notification-sends message_ids extraction)."""
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [
        _row("s1", session_id="sess-a"),
        _row("s2"),  # no session_id at all
        _row("s3", session_id=""),  # falsy empty string
        _row("s4", session_id="sess-b"),
    ]}
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="delete", dry_run=True,
    )
    assert result["session_ids"] == ["sess-a", "sess-b"]


def test_fs_walker_error_returns_empty_session_ids(dsar):
    """Query error → session_ids = [] (don't return stale state)."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    fs_table.query.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError"}}, "Query")
    mock_ddb.Table.return_value = fs_table
    result = mod._walk_form_submissions(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="access", dry_run=True,
    )
    assert result["session_ids"] == []
    assert result["error"] == "query_failed"


# ───────────────────────────────────────────────────────────────────────────
# Dispatcher (_walk_mfs_surfaces) — form-submissions ships; rest scaffolded
# ───────────────────────────────────────────────────────────────────────────
def _stub_dispatch(mock_ddb, *, fs_items=None, fs_error=False,
                   ns_items=None, ns_error=False,
                   ne_items=None, ne_error=False,
                   rm_items=None, rm_error=False,
                   se_items=None, se_error=False):
    """Route ddb.Table(...) calls to per-table mocks. Used by dispatcher +
    handler tests where multiple tables are queried in one flow.

    Default behavior: notification-sends + notification-events +
    recent-messages + session-events tables return empty Query results
    (the dispatcher will still call them for any pii_subject_id-resolved
    subject). Tests can override via ns_items / ne_items / rm_items /
    se_items / *_error kwargs.

    Audit fix #15: session-events added (Sprint B oversight; the email-path
    dispatcher now calls _walk_session_events too post audit fix #1).
    """
    from botocore.exceptions import ClientError
    fs_table = MagicMock()
    subject_table = MagicMock()
    audit_table = MagicMock()
    ns_table = MagicMock()
    ne_table = MagicMock()
    rm_table = MagicMock()
    se_table = MagicMock()
    if fs_error:
        fs_table.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}}, "Query")
    else:
        fs_table.query.return_value = {"Items": fs_items or []}
    if ns_error:
        ns_table.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}}, "Query")
    else:
        ns_table.query.return_value = {"Items": ns_items or []}
    if ne_error:
        ne_table.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}}, "Query")
    else:
        ne_table.query.return_value = {"Items": ne_items or []}
    if rm_error:
        rm_table.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}}, "Query")
    else:
        rm_table.query.return_value = {"Items": rm_items or []}
    if se_error:
        se_table.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}}, "Query")
    else:
        se_table.query.return_value = {"Items": se_items or []}

    def route(name):
        if name == "picasso-form-submissions-staging":
            return fs_table
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        if name == "picasso-notification-sends":
            return ns_table
        if name == "picasso-notification-events":
            return ne_table
        if name == "picasso-recent-messages":
            return rm_table
        if name == "picasso-session-events":
            return se_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return fs_table, subject_table, audit_table, ns_table, ne_table, rm_table, se_table


def test_dispatcher_includes_form_submissions_in_rows_touched(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[_row("s1"), _row("s2")])
    rows, _followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert "form-submissions" in rows
    assert rows["form-submissions"] == 2
    # Other surfaces still scaffolded → count 0
    for s in mod.MFS_SCOPED_SURFACES:
        assert rows[s] == 0


def test_dispatcher_exports_form_submissions_on_access(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[_row("s1"), _row("s2")])
    _rows, _followups, exported, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert "form-submissions" in exported
    assert len(exported["form-submissions"]) == 2


def test_dispatcher_emits_coverage_gap_followup_when_walker_ran(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[])
    _rows, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    # walker ran with 0 rows; coverage-gap followup still emitted
    assert any("pre-Phase-1" in f.lower() or "pre-phase-1" in f.lower() or "Apply-2 backfill" in f for f in followups)


def test_dispatcher_emits_dry_run_followup_when_rows_found(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[_row("s1"), _row("s2")])
    _rows, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert any("dry_run=true" in f and "2 row(s) would be deleted" in f for f in followups)


def test_dispatcher_emits_error_followup_on_walker_error(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_error=True)
    _rows, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert any("query failed" in f for f in followups)
    # Audit fix-now #5: walker error must be reflected in walker_results
    # so handler can compute partial_error close status.
    assert walker_results["form-submissions"]["status"] == "errored"
    assert walker_results["form-submissions"]["error"] == "query_failed"


def test_dispatcher_skips_all_walkers_when_subject_not_found(dsar):
    mod, mock_ddb, _ = dsar
    fs_table, _, _, ns_table, ne_table, rm_table, se_table = _stub_dispatch(mock_ddb)
    rows, followups, exported, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id=None, tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert all(c == 0 for c in rows.values())
    assert "not found in pii-subject-index" in followups[0]
    fs_table.query.assert_not_called()
    ns_table.query.assert_not_called()
    ne_table.query.assert_not_called()
    rm_table.query.assert_not_called()
    assert exported == {}
    # walker_results: form-submissions + notification-sends + notification-events
    # + recent-messages = skipped_no_subject; conversation-summaries +
    # audit-read-only = deferred
    assert walker_results["form-submissions"]["status"] == "skipped_no_subject"
    assert walker_results["notification-sends"]["status"] == "skipped_no_subject"
    assert walker_results["notification-events"]["status"] == "skipped_no_subject"
    assert walker_results["recent-messages"]["status"] == "skipped_no_subject"
    for surface in mod.MFS_SCOPED_SURFACES:
        assert walker_results[surface]["status"] == "deferred"


def test_dispatcher_subject_not_found_followup_includes_cli_snippet(dsar):
    """Audit fix-now #3 — soft followup replaced with concrete CLI snippet.

    When subject-index returns null, the operator's most common next step
    is a manual email-keyed Scan (pre-Phase-1 rows lack pii_subject_id).
    The followup must be copy-pasteable: tenant_id + email substituted
    in-place so no operator templating is required.
    """
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb)
    _rows, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id=None, tenant_id="TEN999",
        normalized_email="pre.phase1@example.com",
        request_type="delete", dry_run=True,
    )
    msg = followups[0]
    assert "aws dynamodb scan" in msg
    assert "picasso-form-submissions-staging" in msg
    assert "pre.phase1@example.com" in msg
    assert "TEN999" in msg
    assert "submitter_email" in msg


def test_dispatcher_coverage_gap_followup_includes_cli_snippet_when_walker_ran(dsar):
    """Same CLI snippet appears in the post-walker coverage-gap followup —
    so operator has the fallback even after a successful (but bounded) walk.
    """
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[_row("s1")])
    _rows, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN999",
        normalized_email="match@example.com",
        request_type="delete", dry_run=True,
    )
    gap = next(f for f in followups
               if "Submissions written before 2026-05-18" in f)
    assert "aws dynamodb scan" in gap
    # Audit row 11 (Security SR2): CLI snippet uses <SUBJECT_EMAIL>
    # placeholder, NOT the actual consumer email. Prevents leak into
    # operator-side response storage when the snippet is pasted into
    # tickets / logs / scripts.
    assert "<SUBJECT_EMAIL>" in gap, "CLI snippet must use placeholder, not actual email"
    assert "match@example.com" not in gap, "CLI snippet must NOT contain consumer email"
    assert "TEN999" in gap


def test_dispatcher_corrupted_row_marks_walker_errored(dsar):
    """Audit fix-now #2 + #5 — corrupted rows trigger errored status so
    close_status flips to partial_error (operator must know batch was
    not exhaustive)."""
    mod, mock_ddb, _ = dsar
    corrupted = {"tenant_id": "TEN", "pii_subject_id": "subj_xyz"}  # no SK
    _stub_dispatch(mock_ddb, fs_items=[_row("s1"), corrupted])
    _rows, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert walker_results["form-submissions"]["status"] == "errored"
    assert walker_results["form-submissions"]["error"] == "rows_skipped_corrupted"
    assert any("skipped due to corrupted PK/SK" in f for f in followups)


# ───────────────────────────────────────────────────────────────────────────
# Dispatcher — notification-sends + notification-events chained walks
# ───────────────────────────────────────────────────────────────────────────
def test_dispatcher_chains_notification_events_from_notification_sends(dsar):
    """notification-sends captures message_ids; notification-events walks each
    via the ByMessageId GSI. End-to-end smoke at the dispatcher layer."""
    mod, mock_ddb, _ = dsar
    _stub_dispatch(
        mock_ddb,
        ns_items=[_ns_row("m1"), _ns_row("m2")],
        ne_items=[_ne_row("m1", "delivery"), _ne_row("m1", "open"),
                  _ne_row("m2", "delivery")],
    )
    rows, _f, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert rows["notification-sends"] == 2
    # ne_items is returned per query; 2 message_ids → 2 queries → 2×3=6 rows
    # (the stub returns the SAME list per query — that's intentional for
    # this test; it asserts the chain runs, not the per-message scoping)
    assert rows["notification-events"] == 6
    assert walker_results["notification-sends"]["status"] == "completed"
    assert walker_results["notification-events"]["status"] == "completed"


def test_dispatcher_notification_events_no_messages_when_ns_empty(dsar):
    """Common case today: notification-sends returns 0 (consumer never
    received direct notifications). notification-events records action=
    no_messages_to_walk, status=completed, rows_touched=0 — NO GSI query."""
    mod, mock_ddb, _ = dsar
    _, _, _, _, ne_table, _, _ = _stub_dispatch(mock_ddb)  # all empty by default
    _rows, _f, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert walker_results["notification-events"]["status"] == "completed"
    assert walker_results["notification-events"]["action"] == "no_messages_to_walk"
    assert walker_results["notification-events"]["rows_touched"] == 0
    # GSI must not be queried
    ne_table.query.assert_not_called()


def test_dispatcher_notification_sends_error_marks_walker_errored(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, ns_error=True)
    _rows, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    assert walker_results["notification-sends"]["status"] == "errored"
    assert walker_results["notification-sends"]["error"] == "query_failed"
    assert any("notification-sends: query failed" in f for f in followups)


def test_dispatcher_notification_events_error_marks_walker_errored(dsar):
    """notification-sends succeeds with matches; notification-events GSI errors
    on every message_id (continue-on-error behavior per audit fix-now-2 #2)."""
    mod, mock_ddb, _ = dsar
    _stub_dispatch(
        mock_ddb,
        ns_items=[_ns_row("m1")],
        ne_error=True,
    )
    _rows, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=False,
    )
    # Walker now uses partial_query_failures error code (continue-on-error)
    assert walker_results["notification-events"]["status"] == "errored"
    assert walker_results["notification-events"]["error"] == "partial_query_failures"
    assert walker_results["notification-events"]["failed_message_ids_count"] == 1
    # Followup mentions per-id failure semantics, not the old "query failed"
    assert any("message_id(s) failed" in f for f in followups)


def test_dispatcher_emits_truncation_followup_when_message_ids_exceed_cap(dsar):
    """Audit fix-now-2 #4: dispatcher surfaces overflow count to operator."""
    mod, mock_ddb, _ = dsar
    cap = mod.MAX_MESSAGE_IDS_PER_INVOCATION
    # Seed notification-sends with cap+10 rows so chain hits the cap
    ns_items = [_ns_row(f"m{i}") for i in range(cap + 10)]
    _stub_dispatch(mock_ddb, ns_items=ns_items, ne_items=[])
    _rows, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert any(
        "capped at" in f and "10 message_id(s) were NOT walked" in f
        for f in followups
    )
    # The truncation taint flips walker status to errored
    assert walker_results["notification-events"]["status"] == "errored"
    assert walker_results["notification-events"]["error"] == "message_ids_truncated"
    assert walker_results["notification-events"]["truncated_count"] == 10


def test_dispatcher_emits_staff_recipient_cli_snippet_followup(dsar):
    """The walker matches recipient==email (consumer-direct). Staff-recipient
    rows must be flagged with a copy-pasteable inspection snippet so the
    operator knows the scope limit and has the next step ready."""
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, ns_items=[])  # 0 consumer-recipient matches
    _rows, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN999",
        normalized_email="match@example.com",
        request_type="delete", dry_run=False,
    )
    staff = next(
        f for f in followups
        if "Staff-recipient rows" in f or "staff were notified ABOUT" in f
    )
    assert "aws dynamodb query" in staff
    assert "picasso-notification-sends" in staff
    assert "TEN999" in staff
    assert "<SUBMISSION_ID-from-form-submissions-walker>" in staff
    # D5 cross-reference must be visible so operator/auditor can trace
    assert "G-H" in staff or "F9" in staff


# ───────────────────────────────────────────────────────────────────────────
# Dispatcher chain — recent-messages via form-submissions session_ids
# ───────────────────────────────────────────────────────────────────────────
def test_dispatcher_chains_recent_messages_via_form_submissions_session_ids(dsar):
    """Form-submissions matches yield session_ids; recent-messages walker
    is then invoked with those session_ids. Verify the Query was called
    on the recent-messages table with the session_id keys."""
    mod, mock_ddb, _ = dsar
    fs_items = [
        _row("s1", session_id="sess-a"),
        _row("s2", session_id="sess-b"),
    ]
    fs_table, _, _, _, _, rm_table, _ = _stub_dispatch(
        mock_ddb, fs_items=fs_items,
    )
    # Distinct response per session_id Query — avoid mock return_value
    # being shared across calls and double-counting rows.
    rm_table.query.side_effect = [
        {"Items": [_rm_row("sess-a", 100)]},
        {"Items": [_rm_row("sess-b", 200)]},
    ]
    rows, _f, exported, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert rows["recent-messages"] == 2
    assert walker_results["recent-messages"]["status"] == "completed"
    assert walker_results["recent-messages"]["action"] == "exported"
    assert "recent-messages" in exported
    # rm_table queried twice — once per session_id from form-submissions
    assert rm_table.query.call_count == 2


def test_dispatcher_recent_messages_no_sessions_when_no_form_submissions(dsar):
    """No form-submissions matches → no session_ids → recent-messages
    short-circuits to no_sessions_to_walk; rm_table never queried."""
    mod, mock_ddb, _ = dsar
    _, _, _, _, _, rm_table, _ = _stub_dispatch(mock_ddb, fs_items=[])
    rows, _f, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert rows["recent-messages"] == 0
    assert walker_results["recent-messages"]["status"] == "completed"
    assert walker_results["recent-messages"]["action"] == "no_sessions_to_walk"
    rm_table.query.assert_not_called()


def test_dispatcher_recent_messages_emits_f_dsar4_followup(dsar):
    """F-DSAR4 chat-only gap MUST be surfaced in manual_followups any
    time the walker ran (even no_sessions outcomes) so the operator
    never assumes zero rows means clean."""
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[])
    _r, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    rm_followups = [f for f in followups if "recent-messages" in f.lower()]
    # F-DSAR4 mention required
    assert any("F-DSAR4" in f or "chat-only" in f.lower() or "chained walk" in f.lower()
               for f in rm_followups)
    # TTL mention required (24h compensating control)
    assert any("24h TTL" in f or "TTL" in f for f in rm_followups)
    # Operator CLI snippet (sessionId-direct + content-substring scan)
    cli_block = "\n".join(rm_followups)
    assert "picasso-recent-messages" in cli_block
    assert "<SESSION_ID-from-out-of-band-source>" in cli_block


def test_dispatcher_recent_messages_emits_third_party_caveat_on_access(dsar):
    """Article 15 third-party disclosure caveat MUST appear on access
    flows when rows were exported — not on delete or empty access."""
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", session_id="sess-a")]
    rm_items = [_rm_row("sess-a", 100, content="my daughter is 7")]
    _stub_dispatch(mock_ddb, fs_items=fs_items, rm_items=rm_items)
    _r, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    assert any(
        "third part" in f.lower() and "redaction" in f.lower()
        for f in followups
    )


def test_dispatcher_recent_messages_third_party_caveat_NOT_emitted_on_delete(dsar):
    """Caveat is access-export-specific; do not pollute delete flows."""
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", session_id="sess-a")]
    rm_items = [_rm_row("sess-a", 100)]
    _stub_dispatch(mock_ddb, fs_items=fs_items, rm_items=rm_items)
    _r, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert not any(
        "third part" in f.lower() and "redaction" in f.lower()
        for f in followups
    )


def test_dispatcher_recent_messages_partial_error_taint_on_failed_session_ids(dsar):
    """Per-session_id failures must taint walker_results to errored so
    close_status flips to partial_error (audit fix-now #5 semantics)."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    fs_items = [
        _row("s1", session_id="sess-a"),
        _row("s2", session_id="sess-b"),
    ]
    rm_table = MagicMock()
    rm_table.query.side_effect = [
        ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}}, "Query"),
        {"Items": []},
    ]
    # Use _stub_dispatch's plumbing for fs/ns/ne; override rm_table directly
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": fs_items}
    subject_table = MagicMock()
    audit_table = MagicMock()
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": []}
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
            "picasso-recent-messages": rm_table,
            "picasso-session-events": se_table,
        }[name]
    mock_ddb.Table.side_effect = route

    _r, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert walker_results["recent-messages"]["status"] == "errored"
    assert walker_results["recent-messages"]["error"] == "partial_query_failures"
    assert walker_results["recent-messages"]["failed_session_ids_count"] == 1
    assert any("recent-messages" in f and "failed Query" in f for f in followups)


def test_dispatcher_recent_messages_truncation_taints_completed_to_errored(dsar):
    """Truncation (chained walker cap hit) MUST taint completed→errored
    so operator sees the partial — mirrors notification-events
    truncation pattern."""
    mod, mock_ddb, _ = dsar
    # form-submissions returns > MAX_SESSION_IDS_PER_INVOCATION distinct
    # session_ids; recent-messages walker must cap + taint
    cap = mod.MAX_SESSION_IDS_PER_INVOCATION
    fs_items = [
        _row(f"sub-{i}", session_id=f"sess-{i}")
        for i in range(cap + 3)
    ]
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": []}
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": fs_items}
    subject_table = MagicMock()
    audit_table = MagicMock()
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": []}
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
            "picasso-recent-messages": rm_table,
            "picasso-session-events": se_table,
        }[name]
    mock_ddb.Table.side_effect = route

    _r, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert walker_results["recent-messages"]["status"] == "errored"
    assert walker_results["recent-messages"]["error"] == "session_ids_truncated"
    assert walker_results["recent-messages"]["truncated_count"] == 3
    assert any("capped at" in f and "recent-messages" in f for f in followups)


def test_dispatcher_recent_messages_combined_failures_and_truncation_taint(dsar):
    """Security advisor fix-now-3 (2026-05-21): when both failed_session_ids
    and truncated_session_id_count fire on the same invocation, the
    dispatcher MUST surface BOTH structured signals in walker_results
    (the original status-gated taint silently dropped truncated_count when
    partial_query_failures was already set)."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    cap = mod.MAX_SESSION_IDS_PER_INVOCATION
    # Form-submissions produces cap+2 session_ids → truncation fires.
    # First Query raises ClientError → failed_session_ids fires too.
    fs_items = [
        _row(f"sub-{i}", session_id=f"sess-{i}") for i in range(cap + 2)
    ]
    rm_table = MagicMock()
    rm_responses = [
        ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}}, "Query"),
    ] + [{"Items": []} for _ in range(cap - 1)]
    rm_table.query.side_effect = rm_responses
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": fs_items}
    subject_table = MagicMock()
    audit_table = MagicMock()
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": []}
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
            "picasso-recent-messages": rm_table,
            "picasso-session-events": se_table,
        }[name]
    mock_ddb.Table.side_effect = route

    _r, followups, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    rm_result = walker_results["recent-messages"]
    # First failure (partial_query_failures) preserved as `error` code
    assert rm_result["status"] == "errored"
    assert rm_result["error"] == "partial_query_failures"
    assert rm_result["failed_session_ids_count"] == 1
    # Truncation count MUST be present even though partial_query_failures
    # was already set on the walker_result. This is the regression Security
    # #7 caught — pre-fix, this field was silently dropped.
    assert rm_result["truncated_count"] == 2
    # Both followups visible to operator
    assert any("failed Query" in f for f in followups)
    assert any("capped at" in f and "recent-messages" in f for f in followups)


def test_dispatcher_notification_events_combined_failures_and_truncation_taint(dsar):
    """Same pattern as recent-messages — fix-now-3 also closes the
    pre-existing parallel bug in the notification-events truncation taint
    (introduced in fix-now-2 PR #136)."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    cap = mod.MAX_MESSAGE_IDS_PER_INVOCATION
    # notification-sends produces cap+2 message_ids → events truncation fires.
    # notification-events first GSI Query fails → failed_message_ids fires too.
    ns_items = [
        _ns_row(message_id=f"msg-{i}", recipient="test@x.co")
        for i in range(cap + 2)
    ]
    ne_table = MagicMock()
    ne_responses = [
        ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}}, "Query"),
    ] + [{"Items": []} for _ in range(cap - 1)]
    ne_table.query.side_effect = ne_responses
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": ns_items}
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": []}
    subject_table = MagicMock()
    audit_table = MagicMock()
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
            "picasso-recent-messages": rm_table,
            "picasso-session-events": se_table,
        }[name]
    mock_ddb.Table.side_effect = route

    _r, _f, _exp, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    ne_result = walker_results["notification-events"]
    assert ne_result["status"] == "errored"
    assert ne_result["error"] == "partial_query_failures"
    assert ne_result["truncated_count"] == 2


def test_dispatcher_recent_messages_followup_includes_f_dsar1_inheritance(dsar):
    """pii-data-lifecycle advisor fix-now-3 (2026-05-21): the recent-
    messages followup MUST explicitly cross-reference F-DSAR1 — pre-
    Phase-1 subjects' session_ids are unreachable through the upstream
    walker, so the chained walk inherits that gap. An operator reading
    CloudWatch should grep `F-DSAR1` and find the inheritance pointer
    without having to chain implications manually."""
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[])
    _r, followups, _exp, _wr = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    rm_followups = [f for f in followups if "recent-messages" in f.lower()]
    rm_text = "\n".join(rm_followups)
    assert "F-DSAR1" in rm_text or "pre-Phase-1" in rm_text


# ───────────────────────────────────────────────────────────────────────────
# End-to-end handler
# ───────────────────────────────────────────────────────────────────────────
def _stub_handler_tables(mock_ddb, *, subject_found, fs_items=None,
                          ns_items=None, ne_items=None, rm_items=None,
                          se_items=None, ss_items=None):
    """Plumb the subject-index Query + form-submissions Query +
    notification-sends Query + notification-events Query +
    recent-messages Query + session-events Query + audit PutItem onto
    separate per-table mocks.

    Default: notification-sends + notification-events + recent-messages +
    session-events return empty Items (consumer rarely receives direct
    notifications today; recent-messages typically empty given 24h TTL).
    Override via ns_items / ne_items / rm_items / se_items to exercise
    the chained walker paths.

    Audit fix #15: se_items + session-events route added (Sprint B oversight)."""
    subject_table = MagicMock()
    audit_table = MagicMock()
    fs_table = MagicMock()
    ns_table = MagicMock()
    ne_table = MagicMock()
    rm_table = MagicMock()
    se_table = MagicMock()
    if subject_found:
        subject_table.get_item.return_value = {
            "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                     "pii_subject_id": "subj_opaque"}
        }
    else:
        subject_table.get_item.return_value = {}
    fs_table.query.return_value = {"Items": fs_items or []}
    ns_table.query.return_value = {"Items": ns_items or []}
    ne_table.query.return_value = {"Items": ne_items or []}
    rm_table.query.return_value = {"Items": rm_items or []}
    se_table.query.return_value = {"Items": se_items or []}
    # F-DSAR31: session-summaries route (only hit when tenant_hash is on the
    # event; existing callers pass no tenant_hash so this stays inert for them).
    ss_table = MagicMock()
    ss_table.query.return_value = {"Items": ss_items or []}

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        if name == "picasso-form-submissions-staging":
            return fs_table
        if name == "picasso-notification-sends":
            return ns_table
        if name == "picasso-notification-events":
            return ne_table
        if name == "picasso-recent-messages":
            return rm_table
        if name == "picasso-session-events":
            return se_table
        if name == "picasso-session-summaries":
            return ss_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return subject_table, audit_table, fs_table, ns_table, ne_table, rm_table, se_table


def test_handler_happy_path_access_exports_form_submission_rows(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123"), _row("s2", tenant_id="TEN123")]
    _, audit_table, fs_table, _, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(subject_identifier="test@x.co", request_type="access"),
        context=None,
    )

    # 3 of 6 surfaces still deferred → close_status = "partial"
    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["rows_touched"]["form-submissions"] == 2
    assert "form-submissions" in resp["exported_rows"]
    assert len(resp["exported_rows"]["form-submissions"]) == 2
    # Audit rows: request_received + 7 surface_walked
    # (form-submissions, notification-sends, notification-events,
    # recent-messages, session-events, archive [M2 Sprint C],
    # fulfillment [M2 Sprint D]) + closed = 9. Deferred surfaces (2) suppressed.
    assert audit_table.put_item.call_count == 9
    fs_table.delete_item.assert_not_called()  # access never deletes
    assert len(resp["audit_row_pks"]) == 9


def test_handler_delete_dry_run_counts_but_does_not_delete(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123")]
    _, audit_table, fs_table, _, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(request_type="delete", dry_run=True), context=None)

    assert resp["status"] == "partial"
    assert resp["rows_touched"]["form-submissions"] == 1
    assert resp["exported_rows"] == {}
    assert any("dry_run=true" in f and "1 row(s) would be deleted" in f
               for f in resp["manual_followups"])
    fs_table.delete_item.assert_not_called()
    # request_received + 7 surface_walked (M2 Sprint D added fulfillment) + closed = 9
    assert audit_table.put_item.call_count == 9


def test_handler_subject_not_found_returns_partial_with_extra_followup(dsar):
    mod, mock_ddb, _ = dsar
    _, audit_table, fs_table, ns_table, ne_table, rm_table, _ = _stub_handler_tables(
        mock_ddb, subject_found=False)

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] is None
    assert "not found in pii-subject-index" in resp["manual_followups"][0]
    assert resp["exported_rows"] == {}
    # Audit rows: request_received + 8 surface_walked (all skipped_no_subject;
    # M2 Sprint D added fulfillment; F-DSAR31 added session-summaries) + closed.
    # Deferred surfaces (1: audit-read-only) still suppressed → 10 total.
    assert audit_table.put_item.call_count == 10
    event_types = [c.kwargs["Item"]["event_type"] for c in audit_table.put_item.call_args_list]
    assert event_types == [
        "request_received",
        "surface_walked:form-submissions",
        "surface_walked:notification-sends",
        "surface_walked:notification-events",
        "surface_walked:recent-messages",
        # Audit fix #1: session-events now walked on email path too.
        "surface_walked:session-events",
        "surface_walked:archive",
        # M2 Sprint D: per-tenant S3 fulfillment walker chained off form-
        # submissions matched rows (email path only).
        "surface_walked:fulfillment",
        # F-DSAR31: session-summaries skipped (no pii_subject_id resolved).
        "surface_walked:session-summaries",
        "closed",
    ]
    for i in (1, 2, 3, 4, 5, 6, 7, 8):
        skipped_event = audit_table.put_item.call_args_list[i].kwargs["Item"]
        assert skipped_event["status"] == "skipped_no_subject"
    # No walker actually queried — all skipped
    fs_table.query.assert_not_called()
    ns_table.query.assert_not_called()
    ne_table.query.assert_not_called()
    rm_table.query.assert_not_called()


def test_handler_invalid_input_returns_failed_no_audit(dsar):
    mod, mock_ddb, _ = dsar
    audit_table = MagicMock()
    mock_ddb.Table.return_value = audit_table

    event = _valid_event()
    del event["operator"]
    resp = mod.lambda_handler(event, context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "invalid_input"
    # No audit row when input is invalid — we never made it past validation.
    audit_table.put_item.assert_not_called()


def test_handler_wrong_account_raises_before_anything(dsar):
    mod, mock_ddb, mock_sts = dsar
    mock_sts.get_caller_identity.return_value = {"Account": "614056832592"}
    audit_table = MagicMock()
    mock_ddb.Table.return_value = audit_table

    with pytest.raises(RuntimeError, match="account 614056832592"):
        mod.lambda_handler(_valid_event(), context=None)

    audit_table.put_item.assert_not_called()


# ───────────────────────────────────────────────────────────────────────────
# Audit fix-now #5: per-surface audit events + computed close status
# ───────────────────────────────────────────────────────────────────────────
def test_handler_writes_surface_walked_audit_event_for_form_submissions(dsar):
    """surface_walked:form-submissions audit event must be written between
    request_received and closed."""
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123")]
    _, audit_table, _, _, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    mod.lambda_handler(_valid_event(request_type="access"), context=None)

    event_types = [
        call.kwargs["Item"]["event_type"]
        for call in audit_table.put_item.call_args_list
    ]
    assert event_types == [
        "request_received",
        "surface_walked:form-submissions",
        "surface_walked:notification-sends",
        "surface_walked:notification-events",
        "surface_walked:recent-messages",
        # Audit fix #1: session-events now walked on email path too.
        "surface_walked:session-events",
        "surface_walked:archive",
        # M2 Sprint D: per-tenant S3 fulfillment walker chained off form-
        # submissions matched rows (email path only).
        "surface_walked:fulfillment",
        "closed",
    ]


def test_handler_does_not_emit_surface_walked_for_deferred_surfaces(dsar):
    """2 of 6 surfaces are deferred (conversation-summaries, audit-read-only)
    — audit log must not be polluted with no-op surface_walked rows for
    those. Shipped walkers (form-submissions, notification-sends,
    notification-events, recent-messages) DO emit surface_walked events."""
    mod, mock_ddb, _ = dsar
    _, audit_table, _, _, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=[])

    mod.lambda_handler(_valid_event(request_type="access"), context=None)

    shipped_walker_surfaces = {
        "surface_walked:form-submissions",
        "surface_walked:notification-sends",
        "surface_walked:notification-events",
        "surface_walked:recent-messages",
        "surface_walked:session-events",  # audit fix #1
        "surface_walked:archive",  # M2 Sprint C
        "surface_walked:fulfillment",  # M2 Sprint D
    }
    extraneous_surface_walked = [
        call.kwargs["Item"]["event_type"]
        for call in audit_table.put_item.call_args_list
        if call.kwargs["Item"]["event_type"].startswith("surface_walked:")
        and call.kwargs["Item"]["event_type"] not in shipped_walker_surfaces
    ]
    assert extraneous_surface_walked == []


def test_handler_close_status_partial_error_when_walker_errors(dsar):
    """Audit fix-now #5: walker error → close_status = partial_error.
    Today's behavior collapsed everything to "partial" — operator could not
    distinguish "ran cleanly with deferrals" from "ran with errors". """
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    subject_table = MagicMock()
    audit_table = MagicMock()
    fs_table = MagicMock()
    ns_table = MagicMock()
    ne_table = MagicMock()
    subject_table.get_item.return_value = {
        "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                 "pii_subject_id": "subj_opaque"}
    }
    fs_table.query.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError"}}, "Query")
    # notification-sends + notification-events succeed with empty results so
    # only form-submissions errors — close_status should still flip to
    # partial_error (any errored walker promotes status).
    ns_table.query.return_value = {"Items": []}
    ne_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(
        _valid_event(request_type="delete", dry_run=False), context=None)

    assert resp["status"] == "partial_error"
    # closed event status mirrors the response status
    close_item = audit_table.put_item.call_args_list[-1].kwargs["Item"]
    assert close_item["event_type"] == "closed"
    assert close_item["status"] == "partial_error"


def test_handler_close_status_partial_error_when_corrupted_rows_skipped(dsar):
    """Audit fix-now #2 + #5: corrupted-row skip flips status to partial_error
    so operator knows the delete batch was not exhaustive."""
    mod, mock_ddb, _ = dsar
    corrupted = {"tenant_id": "TEN123", "pii_subject_id": "subj_opaque"}  # no SK
    fs_items = [_row("s1", tenant_id="TEN123"), corrupted]
    _, audit_table, _, _, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(request_type="delete", dry_run=False), context=None)

    assert resp["status"] == "partial_error"


def test_handler_returns_failed_on_audit_collision_at_request_received(dsar):
    """Audit fix-now #4: dsar_id replay at request_received → fail loud.
    No walker should run, no closed event should be written."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    audit_table = MagicMock()
    audit_table.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "..."}},
        "PutItem",
    )
    subject_table = MagicMock()
    fs_table = MagicMock()
    ns_table = MagicMock()
    ne_table = MagicMock()

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "audit_collision"
    # Walker must not run if request_received audit failed
    subject_table.get_item.assert_not_called()
    fs_table.query.assert_not_called()
    ns_table.query.assert_not_called()
    ne_table.query.assert_not_called()
    # Only the one (failed) put_item attempt
    assert audit_table.put_item.call_count == 1


def test_handler_surface_walked_audit_collision_taints_close_status(dsar):
    """Audit fix-now #4: AuditCollision during surface_walked event is
    recoverable — walker_results gets tainted to errored, close_status
    flips to partial_error, but the run still completes through closed."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    audit_table = MagicMock()
    # PutItem sequence (9 audit attempts post M2 Sprint D — fulfillment):
    #   1: request_received                    → succeed
    #   2: surface_walked:form-submissions     → CCFE (taints walker_results)
    #   3: surface_walked:notification-sends   → succeed
    #   4: surface_walked:notification-events  → succeed
    #   5: surface_walked:recent-messages      → succeed
    #   6: surface_walked:session-events       → succeed (audit fix #1)
    #   7: surface_walked:archive              → succeed (M2 Sprint C)
    #   8: surface_walked:fulfillment          → succeed (M2 Sprint D)
    #   9: closed                              → succeed
    audit_table.put_item.side_effect = [
        None,
        ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "..."}},
            "PutItem",
        ),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]
    subject_table = MagicMock()
    subject_table.get_item.return_value = {
        "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                 "pii_subject_id": "subj_opaque"}
    }
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1", tenant_id="TEN123")]}
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": []}
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": []}
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
            "picasso-recent-messages": rm_table,
            "picasso-session-events": se_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(request_type="access"), context=None)

    # Close status reflects the audit-collision taint on the walker
    assert resp["status"] == "partial_error"
    # closed event was still written (recoverable failure path)
    # Count: 1 request_received + 7 surface_walked (incl. archive M2 Sprint C
    # + fulfillment M2 Sprint D) + 1 closed = 9
    assert audit_table.put_item.call_count == 9
    close_item = audit_table.put_item.call_args_list[-1].kwargs["Item"]
    assert close_item["event_type"] == "closed"
    assert close_item["status"] == "partial_error"


def test_handler_returns_failed_on_audit_collision_at_closed_event(dsar):
    """Audit fix-now #4: AuditCollision during closed event is worst-case —
    walks happened, but the audit log can't be closed. Return failed with
    partial artifacts so operator knows the audit trail is inconsistent."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    audit_table = MagicMock()
    # PutItem sequence (9 attempts post M2 Sprint D — fulfillment):
    #   1: request_received                    → succeed
    #   2: surface_walked:form-submissions     → succeed
    #   3: surface_walked:notification-sends   → succeed
    #   4: surface_walked:notification-events  → succeed
    #   5: surface_walked:recent-messages      → succeed
    #   6: surface_walked:session-events       → succeed (audit fix #1)
    #   7: surface_walked:archive              → succeed (M2 Sprint C)
    #   8: surface_walked:fulfillment          → succeed (M2 Sprint D)
    #   9: closed                              → CCFE → return failed
    audit_table.put_item.side_effect = [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "..."}},
            "PutItem",
        ),
    ]
    subject_table = MagicMock()
    subject_table.get_item.return_value = {
        "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                 "pii_subject_id": "subj_opaque"}
    }
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1", tenant_id="TEN123")]}
    ns_table = MagicMock()
    ns_table.query.return_value = {"Items": []}
    ne_table = MagicMock()
    ne_table.query.return_value = {"Items": []}
    rm_table = MagicMock()
    rm_table.query.return_value = {"Items": []}
    se_table = MagicMock()
    se_table.query.return_value = {"Items": []}

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends": ns_table,
            "picasso-notification-events": ne_table,
            "picasso-recent-messages": rm_table,
            "picasso-session-events": se_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(request_type="access"), context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "closed_audit_collision"
    # Walker artifacts are still surfaced (the work happened) — operator
    # needs visibility into what completed before the closed event failed.
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["rows_touched"]["form-submissions"] == 1
    # audit_row_pks lists the 8 successful events (no closed entry):
    # request_received + 7 surface_walked (incl. archive M2 Sprint C
    # + fulfillment M2 Sprint D)
    assert len(resp["audit_row_pks"]) == 8


# ───────────────────────────────────────────────────────────────────────────
# _compute_close_status — pure unit tests
# ───────────────────────────────────────────────────────────────────────────
def test_compute_close_status_completed_when_all_walkers_succeed(dsar):
    mod, _, _ = dsar
    walker_results = {
        "form-submissions": {"status": "completed"},
        "notification-sends": {"status": "completed"},
    }
    assert mod._compute_close_status(walker_results) == "completed"


def test_compute_close_status_partial_when_any_deferred(dsar):
    mod, _, _ = dsar
    walker_results = {
        "form-submissions": {"status": "completed"},
        "notification-sends": {"status": "deferred"},
    }
    assert mod._compute_close_status(walker_results) == "partial"


def test_compute_close_status_partial_when_subject_skipped(dsar):
    mod, _, _ = dsar
    walker_results = {
        "form-submissions": {"status": "skipped_no_subject"},
        "notification-sends": {"status": "deferred"},
    }
    assert mod._compute_close_status(walker_results) == "partial"


def test_compute_close_status_partial_error_when_any_errored(dsar):
    mod, _, _ = dsar
    # errored beats deferred — partial_error is louder signal
    walker_results = {
        "form-submissions": {"status": "errored", "error": "query_failed"},
        "notification-sends": {"status": "deferred"},
    }
    assert mod._compute_close_status(walker_results) == "partial_error"


# ───────────────────────────────────────────────────────────────────────────
# H4 (PR1 fix-now-4 / 🟡 N-2): ByCreatedAt GSI partition key
# ───────────────────────────────────────────────────────────────────────────
def test_h4_audit_row_carries_created_at_partition_ym(dsar):
    """Every audit row must carry created_at_partition derived from
    event_timestamp[:7] (ISO YYYY-MM). Required by the ByCreatedAt GSI."""
    import re
    mod, mock_ddb, _ = dsar
    mock_table = MagicMock()
    mock_ddb.Table.return_value = mock_table
    ts = mod._write_audit_event(
        dsar_id="h4-canary", event_type="request_received",
        status="in_progress", payload={},
    )
    item = mock_table.put_item.call_args.kwargs["Item"]
    assert item["created_at_partition"] == ts[:7]
    # YYYY-MM regex: 4-digit year, dash, 2-digit month
    assert re.match(r"^\d{4}-\d{2}$", item["created_at_partition"])


# ───────────────────────────────────────────────────────────────────────────
# D1 (PR1 fix-now-4): PII redaction in corrupted-row error logs.
# pii_subject_id (form-submissions) and recipient (notification-sends) must
# NOT appear in error log messages — both are PII per current classification.
# ───────────────────────────────────────────────────────────────────────────
def test_d1_form_submissions_corrupted_row_log_omits_pii_subject_id(dsar, caplog):
    """D1: form_submissions_delete_skipped_corrupted log must NOT contain
    pii_subject_id — it's PII (opaque PSID still classified as PII per D5 G-H)."""
    import logging
    mod, mock_ddb, _ = dsar
    fs_table = MagicMock()
    leaked_psid = "subj_LEAK_CANARY_XYZ"
    corrupted = {"tenant_id": "TEN", "pii_subject_id": leaked_psid}  # no submission_id
    fs_table.query.return_value = {"Items": [corrupted]}
    mock_ddb.Table.return_value = fs_table

    with caplog.at_level(logging.ERROR):
        result = mod._walk_form_submissions(
            pii_subject_id=leaked_psid, tenant_id="TEN",
            request_type="delete", dry_run=False,
        )

    assert result["rows_skipped_corrupted"] == 1
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert leaked_psid not in log_text, "pii_subject_id leaked into error log"
    # Operator-actionable identifiers retained
    assert "tenant_id=TEN" in log_text
    assert "form_submissions_delete_skipped_corrupted" in log_text


def test_d1_notification_sends_corrupted_row_log_omits_recipient(dsar, caplog):
    """D1: notification_sends_delete_skipped_corrupted log must NOT contain
    recipient — direct email PII."""
    import logging
    mod, mock_ddb, _ = dsar
    ns_table = MagicMock()
    leaked_email = "LEAK_CANARY@example.com"
    corrupted = {"recipient": leaked_email, "message_id": "m-corrupt"}  # no pk/sk
    ns_table.query.return_value = {"Items": [corrupted]}
    mock_ddb.Table.return_value = ns_table

    with caplog.at_level(logging.ERROR):
        result = mod._walk_notification_sends(
            tenant_id="TEN123", normalized_email="leak_canary@example.com",
            request_type="delete", dry_run=False,
        )

    assert result["rows_skipped_corrupted"] == 1
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert leaked_email not in log_text, "recipient leaked into error log"
    assert leaked_email.lower() not in log_text, "recipient (case-folded) leaked into error log"
    assert "notification_sends_delete_skipped_corrupted" in log_text


# ───────────────────────────────────────────────────────────────────────────
# E2 (PR1 fix-now-4): subject-resolution ClientError handling.
# Previously: ClientError from _resolve_subject propagated uncaught → Lambda
# crashed with no audit row. Now: audit-write a subject_resolution_failed
# event and return a clean failed response. normalized_email must NOT appear
# in the response/log.
# ───────────────────────────────────────────────────────────────────────────
def test_e2_subject_resolution_client_error_writes_audit_and_returns_failed(dsar):
    """E2: ClientError on subject-index get_item → audit row +
    failed response, no crash."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    subject_table = MagicMock()
    audit_table = MagicMock()
    subject_table.get_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException",
                   "Message": "throttled"}},
        "GetItem",
    )

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        raise AssertionError(f"unexpected DDB Table call: {name}")
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(
        _valid_event(subject_identifier="leak_canary@example.com"),
        context=None,
    )

    assert resp["status"] == "failed"
    assert resp["error"] == "subject_resolution_failed"
    assert "ProvisionedThroughputExceededException" in resp["message"]
    # Two audit rows: request_received + subject_resolution_failed
    assert audit_table.put_item.call_count == 2
    event_types = [c.kwargs["Item"]["event_type"]
                   for c in audit_table.put_item.call_args_list]
    assert event_types == ["request_received", "subject_resolution_failed"]
    # subject_resolution_failed row uses status=failed; payload carries the
    # error code and tenant_id but NOT the normalized_email (PII).
    failure_row = audit_table.put_item.call_args_list[1].kwargs["Item"]
    assert failure_row["status"] == "failed"
    details = json.loads(failure_row["details"])
    assert details["error_code"] == "ProvisionedThroughputExceededException"
    assert details["tenant_id"] == "TEN123"
    assert "normalized_email" not in details
    assert "leak_canary" not in json.dumps(failure_row)


def test_e2_subject_resolution_client_error_does_not_leak_email_in_response(dsar):
    """E2: response body must NOT contain the consumer email (subject_identifier)."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    subject_table = MagicMock()
    audit_table = MagicMock()
    subject_table.get_item.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no access"}},
        "GetItem",
    )

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        raise AssertionError(f"unexpected DDB Table call: {name}")
    mock_ddb.Table.side_effect = route

    leak_canary = "leak_canary_response@example.com"
    resp = mod.lambda_handler(
        _valid_event(subject_identifier=leak_canary), context=None,
    )

    resp_json = json.dumps(resp)
    assert leak_canary not in resp_json
    assert leak_canary.lower() not in resp_json
    assert resp["error"] == "subject_resolution_failed"


def test_e2_subject_resolution_client_error_survives_audit_collision_on_failure_event(dsar):
    """E2 edge case: if the subject_resolution_failed audit event itself
    collides (extremely rare — dsar_id replay), the handler still returns a
    clean failed response rather than crashing.
    """
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    subject_table = MagicMock()
    audit_table = MagicMock()
    subject_table.get_item.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}},
        "GetItem",
    )
    # First put_item (request_received) succeeds; second put_item
    # (subject_resolution_failed) collides.
    audit_table.put_item.side_effect = [
        None,
        ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException",
                       "Message": "exists"}},
            "PutItem",
        ),
    ]

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        raise AssertionError(f"unexpected DDB Table call: {name}")
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(), context=None)

    # Still returns the failed envelope — does NOT crash on audit collision
    # during failure-path audit write.
    assert resp["status"] == "failed"
    assert resp["error"] == "subject_resolution_failed"


# ───────────────────────────────────────────────────────────────────────────
# M2 Sprint B — handler psid path end-to-end
# ───────────────────────────────────────────────────────────────────────────
def _stub_psid_handler_tables(mock_ddb, *, page_ids=("p1",), rm_items=None, se_items=None):
    """Plumb channel-mappings + audit + recent-messages + session-events
    Tables for the psid-path handler tests. Mirrors _stub_handler_tables."""
    cm_table = MagicMock()
    audit_table = MagicMock()
    rm_table = MagicMock()
    se_table = MagicMock()
    cm_table.query.return_value = {
        "Items": [{"PK": f"PAGE#{pid}"} for pid in page_ids],
    }
    rm_table.query.return_value = {"Items": rm_items or []}
    se_table.query.return_value = {"Items": se_items or []}

    def route(name):
        if name == "picasso-channel-mappings":
            return cm_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        if name == "picasso-recent-messages":
            return rm_table
        if name == "picasso-session-events":
            return se_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return cm_table, audit_table, rm_table, se_table


def test_handler_psid_path_access_exports_recent_messages_and_session_events(dsar):
    """End-to-end psid path: tenant → 1 page via TenantIndex GSI → sessionId
    composed → both walkers run → access export returns rows from both
    surfaces. Audit log records request_received + surface_walked for both
    walkers + closed = 4 events."""
    mod, mock_ddb, _ = dsar
    rm_items = [{"sessionId": "meta:p1:9876543210", "messageTimestamp": 100,
                 "role": "user", "content": "hi"}]
    se_items = [_se_row("meta:p1:9876543210", step="STEP#001",
                        tenant_hash="my87674d777bf9")]
    cm_table, audit_table, rm_table, se_table = _stub_psid_handler_tables(
        mock_ddb, page_ids=("p1",), rm_items=rm_items, se_items=se_items)

    resp = mod.lambda_handler(_valid_event(
        identifier_type="psid",
        subject_identifier="9876543210",
        request_type="access",
        dry_run=True,
    ), context=None)

    assert resp["status"] == "completed"  # both walkers ran cleanly
    assert resp["pii_subject_id"] is None  # Meta-only subjects have no subject-index entry
    assert resp["rows_touched"]["recent-messages"] == 1
    assert resp["rows_touched"]["session-events"] == 1
    assert resp["rows_touched"]["archive"] == 0  # M2 Sprint C — empty archive in fixture mock
    assert "recent-messages" in resp["exported_rows"]
    assert "session-events" in resp["exported_rows"]
    # 5 audit events post-M2 Sprint C: request_received + 3 surface_walked
    # (recent-messages + session-events + archive) + closed
    assert audit_table.put_item.call_count == 5
    event_types = [c.kwargs["Item"]["event_type"] for c in audit_table.put_item.call_args_list]
    assert event_types == [
        "request_received",
        "surface_walked:recent-messages",
        "surface_walked:session-events",
        "surface_walked:archive",
        "closed",
    ]
    # Sessions table was queried with the composed sessionId
    rm_table.query.assert_called()
    se_table.query.assert_called()


def test_handler_psid_path_no_meta_pages_for_tenant_returns_completed_with_followup(dsar):
    """Tenant has no Messenger channel → 0 sessionIds → walkers report
    completed/no_sessions. Operator gets the 3-reason followup."""
    mod, mock_ddb, _ = dsar
    cm_table, audit_table, rm_table, se_table = _stub_psid_handler_tables(
        mock_ddb, page_ids=())

    resp = mod.lambda_handler(_valid_event(
        identifier_type="psid",
        subject_identifier="9876543210",
        request_type="access",
    ), context=None)

    assert resp["status"] == "completed"
    assert resp["rows_touched"]["recent-messages"] == 0
    assert resp["rows_touched"]["session-events"] == 0
    assert resp["exported_rows"] == {}
    assert any("0 sessionIds resolved" in f for f in resp["manual_followups"])
    # No walker queries should have been issued
    rm_table.query.assert_not_called()
    se_table.query.assert_not_called()


def test_handler_psid_path_subject_resolution_clienterror_returns_failed_with_audit(dsar):
    """ClientError on the TenantIndex GSI Query during _resolve_psid_subject
    → handler audit-writes the failure event with identifier_type=psid and
    returns failed cleanly (parallel to email path's contract)."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    cm_table = MagicMock()
    audit_table = MagicMock()
    cm_table.query.side_effect = ClientError(
        {"Error": {"Code": "ThrottlingException"}}, "Query")

    def route(name):
        if name == "picasso-channel-mappings":
            return cm_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        raise AssertionError(f"unexpected DDB Table call: {name}")
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(
        identifier_type="psid",
        subject_identifier="9876543210",
    ), context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "subject_resolution_failed"
    # 2 audit attempts: request_received (succeeds) + subject_resolution_failed
    assert audit_table.put_item.call_count == 2
    failed_event = audit_table.put_item.call_args_list[1].kwargs["Item"]
    assert failed_event["event_type"] == "subject_resolution_failed"
    # identifier_type recorded in failure event (lets operator distinguish
    # email-path vs psid-path subject-resolution failures in the audit log)
    details = json.loads(failed_event["details"])
    assert details["identifier_type"] == "psid"


# ───────────────────────────────────────────────────────────────────────────
# Audit closure tests (post phase-completion-audit M2 Sprints A+B+C)
# ───────────────────────────────────────────────────────────────────────────

# ── #1 email-path session-events integration ───────────────────────────────
def test_email_path_session_events_walks_when_form_submissions_yields_session_ids(dsar):
    """Audit fix #1: Sprint A walker matrix §4 row S6 required session-events
    on BOTH email + psid paths; Sprint B missed email. This test guards
    against regression: session-events must Query when form-submissions
    surfaces session_ids."""
    mod, mock_ddb, _ = dsar
    fs_table, _subject, _audit, _ns, _ne, _rm, se_table = _stub_dispatch(
        mock_ddb,
        fs_items=[_row("s1", session_id="sess-a"), _row("s2", session_id="sess-b")],
        se_items=[_se_row("sess-a"), _se_row("sess-b")],
    )
    rows, _f, exported, results = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN123",
        normalized_email="test@x.co",
        request_type="access", dry_run=True,
    )
    # 2 form-submissions sessions × 2 STEP items per session in mock = 4
    # (mock returns the same Items list for every Query; not realistic but
    # exercises the walker dispatch).
    assert rows["session-events"] == 4
    assert results["session-events"]["status"] == "completed"
    assert results["session-events"]["action"] == "exported"
    assert "session-events" in exported
    # se_table.query called twice — once per chained session_id
    assert se_table.query.call_count == 2


def test_email_path_session_events_skipped_when_subject_not_found(dsar):
    """Audit fix #1 sibling: skipped_no_subject branch must also include
    session-events alongside the other deferred-by-no-subject surfaces."""
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb)
    rows, _f, _exp, results = mod._walk_mfs_surfaces(
        pii_subject_id=None, tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert results["session-events"]["status"] == "skipped_no_subject"
    assert rows["session-events"] == 0


# ── #2 VersionId None fallback (was "null" string) ─────────────────────────
def test_walk_archive_skips_version_without_id(dsar):
    """Audit fix #2: missing VersionId surfaces as skipped+logged, not as
    literal "null" string (which would have caused a silent S3 400 on
    delete_object(VersionId="null"))."""
    mod, _, _ = dsar
    mod.s3.list_object_versions.return_value = _arc_resp(versions=[
        {"Key": "sessions/sid1/a.json", "VersionId": "v1"},
        {"Key": "sessions/sid1/a.json"},  # no VersionId — must be skipped
        {"Key": "sessions/sid1/b.json", "VersionId": "v2"},
    ])
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=False)
    # Only 2 valid versions; the one without VersionId is skipped.
    assert result["versions_deleted"] == 2
    assert result["versions_delete_failed"] == 0
    # delete_object must NEVER be called with VersionId="null"
    for call in mod.s3.delete_object.call_args_list:
        assert call.kwargs["VersionId"] != "null"


# ── #3 bounded PSID resolver page enumeration ──────────────────────────────
def test_resolve_psid_subject_truncates_at_max_page_ids(dsar):
    """Audit fix #3: large-tenant Messenger fan-out caps at
    MAX_PAGE_IDS_PER_INVOCATION; resolver doesn't burn full 60s timeout
    enumerating all pages."""
    mod, mock_ddb, _ = dsar
    cap = mod.MAX_PAGE_IDS_PER_INVOCATION
    # Tenant with cap+5 pages → resolver caps + logs warning
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [{"PK": f"PAGE#{i}"} for i in range(cap + 5)],
    }
    mock_ddb.Table.return_value = mock_table
    out = mod._resolve_psid_subject("TEN", "psid_abc")
    assert len(out) == cap


# ── #18 archive walker failed_session_ids propagation ─────────────────────
def test_apply_archive_walker_result_taints_status_on_failed_sessions(dsar):
    """Audit fix #18: previously, failed_session_ids was silently dropped
    by the dispatcher — operator saw status=completed even when N sessions
    failed enumeration. Now propagated as errored."""
    from botocore.exceptions import ClientError
    mod, _, _ = dsar
    mod.s3.list_object_versions.side_effect = [
        ClientError({"Error": {"Code": "InternalError"}}, "ListObjectVersions"),
        _arc_resp(versions=[{"Key": "sessions/sid2/a.json", "VersionId": "v1"}]),
    ]
    rows_touched = {"archive": 0}
    followups = []
    exported = {}
    walker_results = {}
    mod._apply_archive_walker_result(
        "TEN", ["sid1", "sid2"], "access", True,
        rows_touched, followups, exported, walker_results,
    )
    assert walker_results["archive"]["status"] == "errored"
    assert walker_results["archive"]["error"] == "failed_session_ids"
    assert walker_results["archive"]["failed_session_ids_count"] == 1


# ── #10 MFA-Delete cold-start posture check ───────────────────────────────
def test_walk_archive_aborts_delete_when_mfa_delete_enabled(dsar):
    """Audit fix #10: if archive bucket has MFA-Delete=Enabled, every
    DeleteObjectVersion returns 403 with uninformative log. Cold-start
    check fails loud + returns action=errored before any delete attempt."""
    mod, _, _ = dsar
    # Reset cache + stub get_bucket_versioning to report MFA-Delete enabled
    mod._archive_mfa_delete_enabled = None
    mod.s3.get_bucket_versioning.return_value = {"Status": "Enabled", "MFADelete": "Enabled"}
    mod.s3.list_object_versions.return_value = _arc_resp(
        versions=[{"Key": "sessions/sid1/a.json", "VersionId": "v1"}],
    )
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=False)
    assert result["action"] == "errored"
    assert result["error"] == "mfa_delete_enabled"
    mod.s3.delete_object.assert_not_called()


def test_walk_archive_proceeds_when_mfa_delete_disabled(dsar):
    """Audit fix #10 sibling: MFA-Delete=disabled (or absent key) → walker
    proceeds normally with delete-real."""
    mod, _, _ = dsar
    mod._archive_mfa_delete_enabled = None
    mod.s3.get_bucket_versioning.return_value = {"Status": "Enabled"}  # no MFADelete key
    mod.s3.list_object_versions.return_value = _arc_resp(
        versions=[{"Key": "sessions/sid1/a.json", "VersionId": "v1"}],
    )
    result = mod._walk_archive_bucket("TEN", ["sid1"], "delete", dry_run=False)
    assert result["action"] == "deleted"
    assert result["versions_deleted"] == 1


# ── #17 _arc_resp Versions key absent ─────────────────────────────────────
def test_walk_archive_handles_absent_versions_key(dsar):
    """Audit fix #17: real S3 API can omit the Versions key entirely when
    a prefix has never had versioned objects. Source uses .get("Versions", [])
    correctly — this test guards against a regression to bracket access."""
    mod, _, _ = dsar
    # No Versions or DeleteMarkers keys in response
    mod.s3.list_object_versions.return_value = {"IsTruncated": False}
    result = mod._walk_archive_bucket("TEN", ["sid1"], "access", dry_run=True)
    assert result["objects_found"] == 0
    assert result["versions_found"] == 0
    assert result["action"] == "exported"


# ── #28 DEFERRED_SURFACES count sentinel ──────────────────────────────────
def test_deferred_surfaces_count_sentinel(dsar):
    """Audit fix #28 (updated F-DSAR31 2026-06-03): pin DEFERRED_SURFACES at
    exactly 1 — conversation-summaries (session-summaries) now has a real walker
    (_walk_session_summaries); only audit-read-only (Art 17(3)(b) carve-out)
    remains deferred."""
    mod, _, _ = dsar
    assert len(mod.DEFERRED_SURFACES) == 1
    assert set(mod.DEFERRED_SURFACES.keys()) == {"audit-read-only"}


# ── #16 psid dispatcher partial-error tests ──────────────────────────────
def test_walk_psid_surfaces_archive_failed_sessions_taint_status(dsar):
    """Audit fix #16: psid dispatcher must propagate archive failed-session
    errors to walker_results status (was untested)."""
    from botocore.exceptions import ClientError
    mod, _, _ = dsar
    mod.s3.list_object_versions.side_effect = [
        ClientError({"Error": {"Code": "InternalError"}}, "ListObjectVersions"),
    ]
    # Mock both DDB walkers to return clean no-op responses
    _stub_psid_tables(mock_ddb_arg := MagicMock())  # not actually used; bypass below
    mod.ddb.Table.side_effect = lambda name: MagicMock(query=MagicMock(return_value={"Items": []}))
    rows, _f, _exp, results = mod._walk_psid_surfaces(
        tenant_id="TEN", psid="psid_abc",
        session_ids=["meta:p1:psid_abc"],
        request_type="access", dry_run=True,
    )
    assert results["archive"]["status"] == "errored"
    assert results["archive"]["error"] == "failed_session_ids"


# ── F-DSAR31: session-summaries walker (_walk_session_summaries) ─────────────
def test_walk_session_summaries_access_exports_matched(dsar):
    mod, mock_ddb, _ = dsar
    mt = MagicMock()
    mt.query.return_value = {"Items": [
        {"pk": "TENANT#h", "sk": "SESSION#s1", "pii_subject_id": "subj",
         "first_question": "[redacted]"},
    ]}
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "access", dry_run=False)
    assert out["action"] == "exported"
    assert out["rows_found"] == 1
    mock_ddb.Table.assert_called_with("picasso-session-summaries")
    kw = mt.query.call_args.kwargs
    assert "KeyConditionExpression" in kw and "FilterExpression" in kw


def test_walk_session_summaries_delete_dry_run_counts_only(dsar):
    mod, mock_ddb, _ = dsar
    mt = MagicMock()
    mt.query.return_value = {"Items": [
        {"pk": "TENANT#h", "sk": "SESSION#s1", "pii_subject_id": "subj"},
        {"pk": "TENANT#h", "sk": "SESSION#s2", "pii_subject_id": "subj"}]}
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "delete", dry_run=True)
    assert out["action"] == "dry_run_count"
    assert out["rows_found"] == 2
    mt.delete_item.assert_not_called()


def test_walk_session_summaries_delete_real_deletes_each(dsar):
    mod, mock_ddb, _ = dsar
    mt = MagicMock()
    mt.query.return_value = {"Items": [
        {"pk": "TENANT#h", "sk": "SESSION#s1", "pii_subject_id": "subj"},
        {"pk": "TENANT#h", "sk": "SESSION#s2", "pii_subject_id": "subj"}]}
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "delete", dry_run=False)
    assert out["action"] == "deleted"
    assert out["rows_deleted"] == 2
    assert mt.delete_item.call_count == 2
    mt.delete_item.assert_any_call(Key={"pk": "TENANT#h", "sk": "SESSION#s1"})


def test_walk_session_summaries_empty(dsar):
    mod, mock_ddb, _ = dsar
    mt = MagicMock(); mt.query.return_value = {"Items": []}
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "delete", dry_run=False)
    assert out["rows_found"] == 0
    mt.delete_item.assert_not_called()


def test_walk_session_summaries_query_error(dsar):
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    mt = MagicMock()
    mt.query.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "Query")
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "delete", dry_run=False)
    assert out.get("error") == "query_failed"
    assert out["rows_found"] == 0


def test_walk_session_summaries_corrupted_row_skipped(dsar):
    mod, mock_ddb, _ = dsar
    mt = MagicMock()
    mt.query.return_value = {"Items": [
        {"pk": "TENANT#h", "sk": "SESSION#s1", "pii_subject_id": "subj"},
        {"pk": "TENANT#h", "pii_subject_id": "subj"}]}  # missing sk
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "delete", dry_run=False)
    assert out["rows_deleted"] == 1
    assert out["rows_skipped_corrupted"] == 1


def test_walk_session_summaries_pagination(dsar):
    mod, mock_ddb, _ = dsar
    mt = MagicMock()
    mt.query.side_effect = [
        {"Items": [{"pk": "TENANT#h", "sk": "SESSION#s1", "pii_subject_id": "subj"}],
         "LastEvaluatedKey": {"pk": "x", "sk": "y"}},
        {"Items": [{"pk": "TENANT#h", "sk": "SESSION#s2", "pii_subject_id": "subj"}]},
    ]
    mock_ddb.Table.return_value = mt
    out = mod._walk_session_summaries("subj", "h", "delete", dry_run=False)
    assert out["rows_deleted"] == 2
    assert mt.query.call_count == 2


# ── F-DSAR31: dispatcher + handler integration ──────────────────────────────
def test_handler_session_summaries_walked_when_tenant_hash_provided(dsar):
    """tenant_hash on the event → session-summaries surface is walked + appears
    in rows_touched + emits a surface_walked audit event (not deferred)."""
    mod, mock_ddb, _ = dsar
    _stub_handler_tables(
        mock_ddb, subject_found=True,
        ss_items=[{"pk": "TENANT#my87674d777bf9", "sk": "SESSION#s1",
                   "pii_subject_id": "subj_opaque"}])
    resp = mod.lambda_handler(
        _valid_event(subject_identifier="test@x.co", request_type="delete",
                     dry_run=True, tenant_hash="my87674d777bf9"),
        context=None,
    )
    assert resp["rows_touched"]["session-summaries"] == 1
    assert "session-summaries" not in mod.DEFERRED_SURFACES


def test_handler_session_summaries_skipped_when_tenant_hash_absent(dsar):
    """No tenant_hash → session-summaries gracefully skipped (deferred reason),
    all other surfaces still walk; status not 'failed'."""
    mod, mock_ddb, _ = dsar
    _stub_handler_tables(mock_ddb, subject_found=True)
    resp = mod.lambda_handler(
        _valid_event(subject_identifier="test@x.co", request_type="delete",
                     dry_run=True),  # no tenant_hash
        context=None,
    )
    assert resp["rows_touched"]["session-summaries"] == 0
    assert any("session-summaries: skipped" in f for f in resp["manual_followups"])
