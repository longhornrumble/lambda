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


@pytest.fixture
def dsar(monkeypatch):
    """Load lambda_function with mocked boto3 clients.

    Each test gets fresh mocks so per-test assertions don't leak across cases.
    """
    mock_ddb_resource = MagicMock()
    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "525409062831"}

    with patch("boto3.resource", return_value=mock_ddb_resource), \
         patch("boto3.client", return_value=mock_sts):
        if "lambda_function" in sys.modules:
            del sys.modules["lambda_function"]
        import lambda_function as mod
        mod.ddb = mock_ddb_resource
        mod.sts = mock_sts
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
    mod, _, _ = dsar
    with pytest.raises(mod.InvalidInput, match="identifier_type 'psid'"):
        mod._validate(_valid_event(identifier_type="psid"))


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


# ───────────────────────────────────────────────────────────────────────────
# Dispatcher (_walk_mfs_surfaces) — form-submissions ships; rest scaffolded
# ───────────────────────────────────────────────────────────────────────────
def _stub_dispatch(mock_ddb, *, fs_items=None, fs_error=False):
    """Route ddb.Table(...) calls to per-table mocks. Used by dispatcher +
    handler tests where multiple tables are queried in one flow."""
    fs_table = MagicMock()
    subject_table = MagicMock()
    audit_table = MagicMock()
    if fs_error:
        from botocore.exceptions import ClientError
        fs_table.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}}, "Query")
    else:
        fs_table.query.return_value = {"Items": fs_items or []}

    def route(name):
        if name == "picasso-form-submissions-staging":
            return fs_table
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return fs_table, subject_table, audit_table


def test_dispatcher_includes_form_submissions_in_rows_touched(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[_row("s1"), _row("s2")])
    rows, _followups, _exp = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
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
    _rows, _followups, exported = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="access", dry_run=True,
    )
    assert "form-submissions" in exported
    assert len(exported["form-submissions"]) == 2


def test_dispatcher_emits_coverage_gap_followup_when_walker_ran(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[])
    _rows, followups, _exp = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="delete", dry_run=True,
    )
    # walker ran with 0 rows; coverage-gap followup still emitted
    assert any("pre-Phase-1" in f.lower() or "pre-phase-1" in f.lower() or "Apply-2 backfill" in f for f in followups)


def test_dispatcher_emits_dry_run_followup_when_rows_found(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_items=[_row("s1"), _row("s2")])
    _rows, followups, _exp = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="delete", dry_run=True,
    )
    assert any("dry_run=true" in f and "2 row(s) would be deleted" in f for f in followups)


def test_dispatcher_emits_error_followup_on_walker_error(dsar):
    mod, mock_ddb, _ = dsar
    _stub_dispatch(mock_ddb, fs_error=True)
    _rows, followups, _exp = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN",
        request_type="delete", dry_run=False,
    )
    assert any("query failed" in f for f in followups)


def test_dispatcher_skips_all_walkers_when_subject_not_found(dsar):
    mod, mock_ddb, _ = dsar
    fs_table, _, _ = _stub_dispatch(mock_ddb)
    rows, followups, exported = mod._walk_mfs_surfaces(
        pii_subject_id=None, tenant_id="TEN",
        request_type="delete", dry_run=True,
    )
    assert all(c == 0 for c in rows.values())
    assert "Subject not found" in followups[0]
    fs_table.query.assert_not_called()
    assert exported == {}


# ───────────────────────────────────────────────────────────────────────────
# End-to-end handler
# ───────────────────────────────────────────────────────────────────────────
def _stub_handler_tables(mock_ddb, *, subject_found, fs_items=None):
    """Plumb the subject-index Query + form-submissions Query + audit PutItem
    onto separate per-table mocks."""
    subject_table = MagicMock()
    audit_table = MagicMock()
    fs_table = MagicMock()
    if subject_found:
        subject_table.get_item.return_value = {
            "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                     "pii_subject_id": "subj_opaque"}
        }
    else:
        subject_table.get_item.return_value = {}
    fs_table.query.return_value = {"Items": fs_items or []}

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        if name == "picasso-form-submissions-staging":
            return fs_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return subject_table, audit_table, fs_table


def test_handler_happy_path_access_exports_form_submission_rows(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123"), _row("s2", tenant_id="TEN123")]
    _, audit_table, fs_table = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(subject_identifier="test@x.co", request_type="access"),
        context=None,
    )

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["rows_touched"]["form-submissions"] == 2
    assert "form-submissions" in resp["exported_rows"]
    assert len(resp["exported_rows"]["form-submissions"]) == 2
    assert audit_table.put_item.call_count == 2  # request_received + closed
    fs_table.delete_item.assert_not_called()  # access never deletes


def test_handler_delete_dry_run_counts_but_does_not_delete(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123")]
    _, _, fs_table = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(request_type="delete", dry_run=True), context=None)

    assert resp["status"] == "partial"
    assert resp["rows_touched"]["form-submissions"] == 1
    assert resp["exported_rows"] == {}
    assert any("dry_run=true" in f and "1 row(s) would be deleted" in f
               for f in resp["manual_followups"])
    fs_table.delete_item.assert_not_called()


def test_handler_subject_not_found_returns_partial_with_extra_followup(dsar):
    mod, mock_ddb, _ = dsar
    _, audit_table, fs_table = _stub_handler_tables(
        mock_ddb, subject_found=False)

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] is None
    assert "Subject not found" in resp["manual_followups"][0]
    assert resp["exported_rows"] == {}
    assert audit_table.put_item.call_count == 2
    fs_table.query.assert_not_called()  # walker skipped when no subject


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
