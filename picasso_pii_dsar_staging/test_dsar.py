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
                   rm_items=None, rm_error=False):
    """Route ddb.Table(...) calls to per-table mocks. Used by dispatcher +
    handler tests where multiple tables are queried in one flow.

    Default behavior: notification-sends + notification-events +
    recent-messages tables return empty Query results (the dispatcher will
    still call them for any pii_subject_id-resolved subject). Tests can
    override via ns_items / ne_items / rm_items / *_error kwargs.
    """
    from botocore.exceptions import ClientError
    fs_table = MagicMock()
    subject_table = MagicMock()
    audit_table = MagicMock()
    ns_table = MagicMock()
    ne_table = MagicMock()
    rm_table = MagicMock()
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

    def route(name):
        if name == "picasso-form-submissions-staging":
            return fs_table
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        if name == "picasso-notification-sends-staging":
            return ns_table
        if name == "picasso-notification-events-staging":
            return ne_table
        if name == "staging-recent-messages":
            return rm_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return fs_table, subject_table, audit_table, ns_table, ne_table, rm_table


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
    fs_table, _, _, ns_table, ne_table, rm_table = _stub_dispatch(mock_ddb)
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
    _, _, _, _, ne_table, _ = _stub_dispatch(mock_ddb)  # all empty by default
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
    assert "picasso-notification-sends-staging" in staff
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
    fs_table, _, _, _, _, rm_table = _stub_dispatch(
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
    _, _, _, _, _, rm_table = _stub_dispatch(mock_ddb, fs_items=[])
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
    assert "staging-recent-messages" in cli_block
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

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
            "staging-recent-messages": rm_table,
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

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
            "staging-recent-messages": rm_table,
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

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
            "staging-recent-messages": rm_table,
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

    def route(name):
        return {
            "picasso-form-submissions-staging": fs_table,
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
            "staging-recent-messages": rm_table,
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
                          ns_items=None, ne_items=None, rm_items=None):
    """Plumb the subject-index Query + form-submissions Query +
    notification-sends Query + notification-events Query +
    recent-messages Query + audit PutItem onto separate per-table mocks.

    Default: notification-sends + notification-events + recent-messages
    return empty Items (consumer rarely receives direct notifications
    today; recent-messages typically empty given 24h TTL). Override via
    ns_items / ne_items / rm_items to exercise the chained walker paths."""
    subject_table = MagicMock()
    audit_table = MagicMock()
    fs_table = MagicMock()
    ns_table = MagicMock()
    ne_table = MagicMock()
    rm_table = MagicMock()
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

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        if name == "picasso-form-submissions-staging":
            return fs_table
        if name == "picasso-notification-sends-staging":
            return ns_table
        if name == "picasso-notification-events-staging":
            return ne_table
        if name == "staging-recent-messages":
            return rm_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return subject_table, audit_table, fs_table, ns_table, ne_table, rm_table


def test_handler_happy_path_access_exports_form_submission_rows(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123"), _row("s2", tenant_id="TEN123")]
    _, audit_table, fs_table, _, _, _ = _stub_handler_tables(
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
    # Audit rows: request_received + surface_walked:form-submissions +
    # surface_walked:notification-sends + surface_walked:notification-events
    # + surface_walked:recent-messages + closed = 6. Deferred surfaces (2)
    # still suppressed.
    assert audit_table.put_item.call_count == 6
    fs_table.delete_item.assert_not_called()  # access never deletes
    assert len(resp["audit_row_pks"]) == 6


def test_handler_delete_dry_run_counts_but_does_not_delete(dsar):
    mod, mock_ddb, _ = dsar
    fs_items = [_row("s1", tenant_id="TEN123")]
    _, audit_table, fs_table, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=fs_items)

    resp = mod.lambda_handler(
        _valid_event(request_type="delete", dry_run=True), context=None)

    assert resp["status"] == "partial"
    assert resp["rows_touched"]["form-submissions"] == 1
    assert resp["exported_rows"] == {}
    assert any("dry_run=true" in f and "1 row(s) would be deleted" in f
               for f in resp["manual_followups"])
    fs_table.delete_item.assert_not_called()
    # request_received + 4 surface_walked + closed
    assert audit_table.put_item.call_count == 6


def test_handler_subject_not_found_returns_partial_with_extra_followup(dsar):
    mod, mock_ddb, _ = dsar
    _, audit_table, fs_table, ns_table, ne_table, rm_table = _stub_handler_tables(
        mock_ddb, subject_found=False)

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] is None
    assert "not found in pii-subject-index" in resp["manual_followups"][0]
    assert resp["exported_rows"] == {}
    # Audit rows: request_received + surface_walked for form-submissions +
    # notification-sends + notification-events + recent-messages (all
    # skipped_no_subject) + closed. Deferred surfaces (2) still suppressed
    # → 6 total.
    assert audit_table.put_item.call_count == 6
    event_types = [c.kwargs["Item"]["event_type"] for c in audit_table.put_item.call_args_list]
    assert event_types == [
        "request_received",
        "surface_walked:form-submissions",
        "surface_walked:notification-sends",
        "surface_walked:notification-events",
        "surface_walked:recent-messages",
        "closed",
    ]
    for i in (1, 2, 3, 4):
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
    _, audit_table, _, _, _, _ = _stub_handler_tables(
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
        "closed",
    ]


def test_handler_does_not_emit_surface_walked_for_deferred_surfaces(dsar):
    """2 of 6 surfaces are deferred (conversation-summaries, audit-read-only)
    — audit log must not be polluted with no-op surface_walked rows for
    those. Shipped walkers (form-submissions, notification-sends,
    notification-events, recent-messages) DO emit surface_walked events."""
    mod, mock_ddb, _ = dsar
    _, audit_table, _, _, _, _ = _stub_handler_tables(
        mock_ddb, subject_found=True, fs_items=[])

    mod.lambda_handler(_valid_event(request_type="access"), context=None)

    shipped_walker_surfaces = {
        "surface_walked:form-submissions",
        "surface_walked:notification-sends",
        "surface_walked:notification-events",
        "surface_walked:recent-messages",
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

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
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
    _, audit_table, _, _, _, _ = _stub_handler_tables(
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
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
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
    # PutItem sequence (6 audit attempts):
    #   1: request_received                    → succeed
    #   2: surface_walked:form-submissions     → CCFE (taints walker_results)
    #   3: surface_walked:notification-sends   → succeed
    #   4: surface_walked:notification-events  → succeed
    #   5: surface_walked:recent-messages      → succeed
    #   6: closed                              → succeed
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

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
            "staging-recent-messages": rm_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(request_type="access"), context=None)

    # Close status reflects the audit-collision taint on the walker
    assert resp["status"] == "partial_error"
    # closed event was still written (recoverable failure path)
    assert audit_table.put_item.call_count == 6
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
    # PutItem sequence (6 attempts):
    #   1: request_received                    → succeed
    #   2: surface_walked:form-submissions     → succeed
    #   3: surface_walked:notification-sends   → succeed
    #   4: surface_walked:notification-events  → succeed
    #   5: surface_walked:recent-messages      → succeed
    #   6: closed                              → CCFE → return failed
    audit_table.put_item.side_effect = [
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

    def route(name):
        return {
            "picasso-pii-subject-index-staging": subject_table,
            "picasso-pii-dsar-audit-staging": audit_table,
            "picasso-form-submissions-staging": fs_table,
            "picasso-notification-sends-staging": ns_table,
            "picasso-notification-events-staging": ne_table,
            "staging-recent-messages": rm_table,
        }[name]
    mock_ddb.Table.side_effect = route

    resp = mod.lambda_handler(_valid_event(request_type="access"), context=None)

    assert resp["status"] == "failed"
    assert resp["error"] == "closed_audit_collision"
    # Walker artifacts are still surfaced (the work happened) — operator
    # needs visibility into what completed before the closed event failed.
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["rows_touched"]["form-submissions"] == 1
    # audit_row_pks lists the 5 successful events (no closed entry):
    # request_received + 4 surface_walked
    assert len(resp["audit_row_pks"]) == 5


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
