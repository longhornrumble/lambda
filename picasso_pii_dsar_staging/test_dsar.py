"""Unit tests for picasso-pii-dsar-staging.

Covers:
- Cold-start env-guard (correct account passes; wrong account refuses)
- Input validation (missing fields, unsupported types, dry_run default)
- Email normalization (lower + strip; matches Phase-1 subject-index writer)
- Subject resolution (found → returns pii_subject_id; not found → None)
- Audit write shape (PK=dsar_id, SK=event_timestamp, status duplicated for GSI)
- Per-surface walker scaffolding (all surfaces return manual_followup)
- Handler end-to-end (subject found, subject not found, invalid input, wrong account)
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
# Per-surface walker scaffolding
# ───────────────────────────────────────────────────────────────────────────
def test_walk_returns_zero_rows_for_every_surface(dsar):
    mod, _, _ = dsar
    rows, _ = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=True,
    )
    assert set(rows.keys()) == set(mod.MFS_SCOPED_SURFACES.keys())
    assert all(count == 0 for count in rows.values())


def test_walk_returns_manual_followup_per_surface(dsar):
    mod, _, _ = dsar
    _, followups = mod._walk_mfs_surfaces(
        pii_subject_id="subj_xyz", tenant_id="TEN", request_type="delete", dry_run=True,
    )
    # one followup per known surface (subject was found, so no "not found" prefix)
    assert len(followups) == len(mod.MFS_SCOPED_SURFACES)
    for surface in mod.MFS_SCOPED_SURFACES:
        assert any(fup.startswith(f"{surface}:") for fup in followups)


def test_walk_prepends_not_found_followup_when_subject_missing(dsar):
    mod, _, _ = dsar
    _, followups = mod._walk_mfs_surfaces(
        pii_subject_id=None, tenant_id="TEN", request_type="delete", dry_run=True,
    )
    assert "Subject not found" in followups[0]
    assert len(followups) == len(mod.MFS_SCOPED_SURFACES) + 1


# ───────────────────────────────────────────────────────────────────────────
# End-to-end handler
# ───────────────────────────────────────────────────────────────────────────
def _stub_tables(mock_ddb, *, subject_found):
    """Return-value plumbing so the same mock serves both the subject-index
    Query and the audit PutItem calls."""
    subject_table = MagicMock()
    audit_table = MagicMock()
    if subject_found:
        subject_table.get_item.return_value = {
            "Item": {"tenant_id": "TEN", "normalized_email": "test@x.co",
                     "pii_subject_id": "subj_opaque"}
        }
    else:
        subject_table.get_item.return_value = {}

    def route(name):
        if name == "picasso-pii-subject-index-staging":
            return subject_table
        if name == "picasso-pii-dsar-audit-staging":
            return audit_table
        raise AssertionError(f"unexpected DDB Table call: {name}")

    mock_ddb.Table.side_effect = route
    return subject_table, audit_table


def test_handler_happy_path_subject_found(dsar):
    mod, mock_ddb, _ = dsar
    _, audit_table = _stub_tables(mock_ddb, subject_found=True)

    resp = mod.lambda_handler(_valid_event(subject_identifier="test@x.co"), context=None)

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] == "subj_opaque"
    assert resp["dsar_id"] == "dsar-uuid-1"
    assert set(resp["rows_touched"].keys()) == set(mod.MFS_SCOPED_SURFACES.keys())
    assert all(c == 0 for c in resp["rows_touched"].values())
    assert len(resp["manual_followups"]) == len(mod.MFS_SCOPED_SURFACES)
    assert len(resp["audit_row_pks"]) == 2  # request_received + closed
    # 2 audit PutItems (request_received + closed)
    assert audit_table.put_item.call_count == 2


def test_handler_subject_not_found_returns_partial_with_extra_followup(dsar):
    mod, mock_ddb, _ = dsar
    _, audit_table = _stub_tables(mock_ddb, subject_found=False)

    resp = mod.lambda_handler(_valid_event(), context=None)

    assert resp["status"] == "partial"
    assert resp["pii_subject_id"] is None
    assert "Subject not found" in resp["manual_followups"][0]
    assert audit_table.put_item.call_count == 2


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
