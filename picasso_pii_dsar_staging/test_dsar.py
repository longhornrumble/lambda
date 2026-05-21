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
    # Audit fix-now #4: idempotency invariant — must reject replay on
    # identical (dsar_id, event_timestamp).
    assert "ConditionExpression" in kwargs
    assert "attribute_not_exists(dsar_id)" in kwargs["ConditionExpression"]
    assert "attribute_not_exists(event_timestamp)" in kwargs["ConditionExpression"]


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
    fs_table, _, _ = _stub_dispatch(mock_ddb)
    rows, followups, exported, walker_results = mod._walk_mfs_surfaces(
        pii_subject_id=None, tenant_id="TEN",
        normalized_email="test@x.co",
        request_type="delete", dry_run=True,
    )
    assert all(c == 0 for c in rows.values())
    assert "not found in pii-subject-index" in followups[0]
    fs_table.query.assert_not_called()
    assert exported == {}
    # walker_results: form-submissions = skipped_no_subject, others = deferred
    assert walker_results["form-submissions"]["status"] == "skipped_no_subject"
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
    assert "match@example.com" in gap
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

    # 5 of 6 surfaces still deferred → close_status = "partial"
    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["rows_touched"]["form-submissions"] == 2
    assert "form-submissions" in resp["exported_rows"]
    assert len(resp["exported_rows"]["form-submissions"]) == 2
    # Audit fix-now #5: 3 audit rows now —
    # request_received + surface_walked:form-submissions + closed
    assert audit_table.put_item.call_count == 3
    fs_table.delete_item.assert_not_called()  # access never deletes
    # audit_row_pks reflects all 3 events
    assert len(resp["audit_row_pks"]) == 3


def test_handler_delete_dry_run_counts_but_does_not_delete(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123")]
    _, audit_table, fs_table = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(request_type="delete", dry_run=True), context=None)

    assert resp["status"] == "partial"
    assert resp["rows_touched"]["form-submissions"] == 1
    assert resp["exported_rows"] == {}
    assert any("dry_run=true" in f and "1 row(s) would be deleted" in f
               for f in resp["manual_followups"])
    fs_table.delete_item.assert_not_called()
    # request_received + surface_walked + closed
    assert audit_table.put_item.call_count == 3


def test_handler_subject_not_found_returns_partial_with_extra_followup(dsar):
    mod, mock_ddb, _ = dsar
    _, audit_table, fs_table = _stub_handler_tables(
        mock_ddb, subject_found=False)

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] is None
    assert "not found in pii-subject-index" in resp["manual_followups"][0]
    assert resp["exported_rows"] == {}
    # Audit rows: request_received + surface_walked:form-submissions
    # (status=skipped_no_subject — real walker outcome, not a deferral) + closed.
    # Deferred surfaces still suppressed, so 3 total (not 8).
    assert audit_table.put_item.call_count == 3
    event_types = [c.kwargs["Item"]["event_type"] for c in audit_table.put_item.call_args_list]
    assert event_types == [
        "request_received",
        "surface_walked:form-submissions",
        "closed",
    ]
    skipped_event = audit_table.put_item.call_args_list[1].kwargs["Item"]
    assert skipped_event["status"] == "skipped_no_subject"
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


# ───────────────────────────────────────────────────────────────────────────
# Audit fix-now #5: per-surface audit events + computed close status
# ───────────────────────────────────────────────────────────────────────────
def test_handler_writes_surface_walked_audit_event_for_form_submissions(dsar):
    """surface_walked:form-submissions audit event must be written between
    request_received and closed."""
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123")]
    _, audit_table, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    mod.lambda_handler(_valid_event(request_type="access"), context=None)

    event_types = [
        call.kwargs["Item"]["event_type"]
        for call in audit_table.put_item.call_args_list
    ]
    assert event_types == [
        "request_received",
        "surface_walked:form-submissions",
        "closed",
    ]


def test_handler_does_not_emit_surface_walked_for_deferred_surfaces(dsar):
    """5 of 6 surfaces are deferred — audit log must not be polluted with
    no-op surface_walked rows for them."""
    mod, mock_ddb, _ = dsar
    _, audit_table, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=[])

    mod.lambda_handler(_valid_event(request_type="access"), context=None)

    deferred_audit_count = sum(
        1 for call in audit_table.put_item.call_args_list
        if call.kwargs["Item"]["event_type"].startswith("surface_walked:")
        and call.kwargs["Item"]["event_type"] != "surface_walked:form-submissions"
    )
    assert deferred_audit_count == 0


def test_handler_close_status_partial_error_when_walker_errors(dsar):
    """Audit fix-now #5: walker error → close_status = partial_error.
    Today's behavior collapsed everything to "partial" — operator could not
    distinguish "ran cleanly with deferrals" from "ran with errors". """
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    subject_table = MagicMock()
    audit_table = MagicMock()
    fs_table = MagicMock()
    subject_table.get_item.return_value = {
        "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                 "pii_subject_id": "subj_opaque"}
    }
    fs_table.query.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError"}}, "Query")

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
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
    _, audit_table, _ = _stub_handler_tables(
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

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "audit_collision"
    # Walker must not run if request_received audit failed
    subject_table.get_item.assert_not_called()
    fs_table.query.assert_not_called()
    # Only the one (failed) put_item attempt
    assert audit_table.put_item.call_count == 1


def test_handler_surface_walked_audit_collision_taints_close_status(dsar):
    """Audit fix-now #4: AuditCollision during surface_walked event is
    recoverable — walker_results gets tainted to errored, close_status
    flips to partial_error, but the run still completes through closed."""
    from botocore.exceptions import ClientError
    mod, mock_ddb, _ = dsar
    audit_table = MagicMock()
    # First PutItem (request_received): succeed
    # Second PutItem (surface_walked:form-submissions): CCFE
    # Third PutItem (closed): succeed
    audit_table.put_item.side_effect = [
        None,
        ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "..."}},
            "PutItem",
        ),
        None,
    ]
    subject_table = MagicMock()
    subject_table.get_item.return_value = {
        "Item": {"tenant_id": "TEN123", "normalized_email": "test@x.co",
                 "pii_subject_id": "subj_opaque"}
    }
    fs_table = MagicMock()
    fs_table.query.return_value = {"Items": [_row("s1", tenant_id="TEN123")]}

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(request_type="access"), context=None)

    # Close status reflects the audit-collision taint on the walker
    assert resp["status"] == "partial_error"
    # closed event was still written (recoverable failure path)
    assert audit_table.put_item.call_count == 3
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
    # request_received: succeed
    # surface_walked: succeed
    # closed: CCFE → return failed
    audit_table.put_item.side_effect = [
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

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(request_type="access"), context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "closed_audit_collision"
    # Walker artifacts are still surfaced (the work happened) — operator
    # needs visibility into what completed before the closed event failed.
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["rows_touched"]["form-submissions"] == 1
    # audit_row_pks lists the successful events only (no closed entry)
    assert len(resp["audit_row_pks"]) == 2  # request_received + surface_walked


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
