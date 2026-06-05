"""Unit tests for picasso-pii-tenant-purge-staging (P1 — Class A purge).

Covers:
- Cold-start account guard (525 passes; wrong account refuses, no DDB ops)
- Input validation (missing fields, non-bool grace_confirmed/dry_run, empties,
  non-dict event)
- Dual gate (default dry-run; dry_run=false alone → dry-run + followup;
  both flags → real delete)
- Each Class-A partition purger (dry-run count, real delete, corrupted-row
  skip, delete-failure count, query error, pagination)
- notification-events chain (no messages, real delete, GSI query failure,
  truncation cap)
- Audit (purge_requested / surface_purged:* / closed rows; idempotency
  collision → failed)
- Response shape (carve_outs_retained, rows_touched, audit_row_pks)
- Idempotent re-run (empty tables → zero counts)

Style mirrors the sibling picasso_pii_dsar_staging/test_dsar.py: pure
MagicMock for the ddb resource + sts client (no moto), module reloaded per
test so per-test stubs don't leak.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# #1a: the account guard now reads EXPECTED_ACCOUNT from the env (fail-closed).
# Set the staging value before the fixture re-imports the module; the
# "unset ⇒ refuse" case monkeypatches the module attribute directly.
os.environ.setdefault("EXPECTED_ACCOUNT", "525409062831")


@pytest.fixture
def purge():
    """Load lambda_function with mocked boto3 ddb resource + sts client.

    Yields (mod, mock_ddb, mock_sts). `mock_ddb.Table(name)` returns a shared
    MagicMock per table name (wired lazily via `_wire_tables`), so a walker's
    `ddb.Table(TABLE_X)` resolves to the same stub the test configured.
    """
    mock_ddb_resource = MagicMock()
    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "525409062831"}
    # D2: default form-submissions key schema = staging composite
    # (tenant_id HASH, submission_id RANGE), consumed by
    # _form_submissions_key_schema via ddb.meta.client.describe_table.
    # Prod-shape tests override this with the single submission_id key.
    mock_ddb_resource.meta.client.describe_table.return_value = {
        "Table": {"KeySchema": [
            {"AttributeName": "tenant_id", "KeyType": "HASH"},
            {"AttributeName": "submission_id", "KeyType": "RANGE"},
        ]}
    }

    def _client_router(name, *args, **kwargs):
        if name == "sts":
            return mock_sts
        raise AssertionError(f"unexpected boto3.client({name!r})")

    with patch("boto3.resource", return_value=mock_ddb_resource), \
         patch("boto3.client", side_effect=_client_router):
        if "lambda_function" in sys.modules:
            del sys.modules["lambda_function"]
        import lambda_function as mod
        mod.ddb = mock_ddb_resource
        mod.sts = mock_sts
        yield mod, mock_ddb_resource, mock_sts


def _table_mock(items=None, query_pages=None):
    """Build a per-table MagicMock.

    items: single Query page of Items.
    query_pages: list of full Query responses (for pagination tests); overrides
                 items.
    """
    t = MagicMock()
    if query_pages is not None:
        t.query.side_effect = query_pages
    else:
        t.query.return_value = {"Items": items or []}
    t.delete_item.return_value = {}
    t.put_item.return_value = {}
    return t


def _wire_tables(mod, mock_ddb, mapping):
    """Route mod.ddb.Table(name) → mapping[name] (a _table_mock).

    Any table not in `mapping` gets a fresh empty mock (so the audit table and
    untouched surfaces don't blow up). The audit table is auto-provided unless
    the test supplies its own.
    """
    store = dict(mapping)
    if mod.TABLE_PURGE_AUDIT not in store:
        store[mod.TABLE_PURGE_AUDIT] = _table_mock()

    def _table(name):
        if name not in store:
            store[name] = _table_mock()
        return store[name]

    mock_ddb.Table.side_effect = _table
    return store


def _event(**overrides):
    base = {
        "tenant_id": "TEN-X",
        "operator": "operator@myrecruiter.ai",
        "purge_id": "purge-uuid-1",
        "grace_confirmed": True,
        "dry_run": True,
    }
    base.update(overrides)
    return base


def _client_error(code="InternalServerError", op="Query"):
    return ClientError({"Error": {"Code": code}}, op)


# ───────────────────────────────────────────────────────────────────────────
# Account guard
# ───────────────────────────────────────────────────────────────────────────
def test_account_guard_correct_account_proceeds(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(), None)
    assert resp["status"] == "completed"


def test_account_guard_wrong_account_fails_no_ddb(purge):
    mod, mock_ddb, mock_sts = purge
    mock_sts.get_caller_identity.return_value = {"Account": "614056832592"}
    store = _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(), None)
    assert resp["status"] == "failed"
    assert "614056832592" in resp["error"]
    # No surface or audit table touched.
    assert mock_ddb.Table.call_count == 0


@pytest.mark.parametrize("unset", [None, ""])
def test_account_guard_unset_expected_account_fails_closed(purge, unset):
    """#1a fail-closed: an unset (None) OR empty ("") EXPECTED_ACCOUNT must
    REFUSE (no default to an account), without consulting STS or touching any
    table. os.environ.get returns "" when the var is set-but-empty."""
    mod, mock_ddb, mock_sts = purge
    mock_sts.get_caller_identity.reset_mock()
    mod.EXPECTED_ACCOUNT = unset
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(), None)
    assert resp["status"] == "failed"
    assert "EXPECTED_ACCOUNT env var is unset" in resp["error"]
    assert mock_ddb.Table.call_count == 0
    mock_sts.get_caller_identity.assert_not_called()


# ───────────────────────────────────────────────────────────────────────────
# Validation
# ───────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("missing", ["tenant_id", "operator", "purge_id", "grace_confirmed"])
def test_validate_missing_required_field_fails(purge, missing):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    ev = _event()
    del ev[missing]
    resp = mod.lambda_handler(ev, None)
    assert resp["status"] == "failed"
    assert missing in resp["error"]


def test_validate_event_not_dict_fails(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler("not-a-dict", None)
    assert resp["status"] == "failed"


def test_validate_grace_confirmed_non_bool_fails(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(grace_confirmed="true"), None)
    assert resp["status"] == "failed"
    assert "grace_confirmed must be boolean" in resp["error"]


def test_validate_dry_run_non_bool_fails(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(dry_run="false"), None)
    assert resp["status"] == "failed"
    assert "dry_run must be boolean" in resp["error"]


@pytest.mark.parametrize("field", ["tenant_id", "purge_id", "operator"])
def test_validate_empty_string_field_fails(purge, field):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(**{field: "   "}), None)
    assert resp["status"] == "failed"


def test_validate_dry_run_defaults_true_when_absent(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"}]),
    })
    ev = _event()
    del ev["dry_run"]  # absent → defaults True
    resp = mod.lambda_handler(ev, None)
    assert resp["deleted"] is False
    store["picasso-form-submissions-staging"].delete_item.assert_not_called()


# ───────────────────────────────────────────────────────────────────────────
# Dual gate
# ───────────────────────────────────────────────────────────────────────────
def test_dual_gate_default_dry_run_no_deletes(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"},
                   {"tenant_id": "TEN-X", "submission_id": "s2"}]),
    })
    resp = mod.lambda_handler(_event(), None)
    assert resp["deleted"] is False
    assert resp["rows_touched"]["form-submissions"] == 2  # counted, not deleted
    store["picasso-form-submissions-staging"].delete_item.assert_not_called()


def test_dual_gate_dry_run_false_but_grace_unconfirmed_is_dry_run(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"}]),
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=False), None)
    assert resp["deleted"] is False
    store["picasso-form-submissions-staging"].delete_item.assert_not_called()
    assert any("dual gate" in f for f in resp["manual_followups"])


def test_dual_gate_both_flags_deletes(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"},
                   {"tenant_id": "TEN-X", "submission_id": "s2"}]),
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["deleted"] is True
    assert resp["rows_touched"]["form-submissions"] == 2
    assert store["picasso-form-submissions-staging"].delete_item.call_count == 2


# ───────────────────────────────────────────────────────────────────────────
# form-submissions purger
# ───────────────────────────────────────────────────────────────────────────
def test_form_submissions_real_delete_uses_correct_key(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1", "email": "a@b.c"}]),
    })
    mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    store["picasso-form-submissions-staging"].delete_item.assert_called_once_with(
        Key={"tenant_id": "TEN-X", "submission_id": "s1"})


# ───────────────────────────────────────────────────────────────────────────
# D2: schema-adaptive form-submissions purge (staging composite vs prod single)
# ───────────────────────────────────────────────────────────────────────────
def _set_form_schema(mock_ddb, *, single_key):
    """Override the form-submissions key schema the purger discovers.

    single_key=True  → prod shape: PK=submission_id only.
    single_key=False → staging shape: PK=tenant_id, SK=submission_id.
    """
    if single_key:
        schema = [{"AttributeName": "submission_id", "KeyType": "HASH"}]
    else:
        schema = [
            {"AttributeName": "tenant_id", "KeyType": "HASH"},
            {"AttributeName": "submission_id", "KeyType": "RANGE"},
        ]
    mock_ddb.meta.client.describe_table.return_value = {"Table": {"KeySchema": schema}}


def test_purge_form_submissions_prod_single_key_uses_gsi_and_deletes_by_submission_id(purge):
    """Prod shape: Query the tenant-timestamp-index GSI on tenant_id and delete
    by submission_id ONLY (the prod single-key table). A base-table Query on
    tenant_id, or a delete with the staging composite key, would raise
    ValidationException against the prod table."""
    mod, mock_ddb, _ = purge
    _set_form_schema(mock_ddb, single_key=True)
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"},
                   {"tenant_id": "TEN-X", "submission_id": "s2"}]),
    })
    result = mod._purge_form_submissions("TEN-X", dry_run=False)
    assert result["rows_deleted"] == 2
    fs = store["picasso-form-submissions-staging"]
    assert fs.query.call_args.kwargs["IndexName"] == mod.TENANT_TIMESTAMP_INDEX
    for call in fs.delete_item.call_args_list:
        assert set(call.kwargs["Key"].keys()) == {"submission_id"}


def test_purge_form_submissions_staging_composite_no_gsi(purge):
    """Staging shape: Query the base table (no GSI) and delete by the composite
    key — the proven path, unchanged."""
    mod, mock_ddb, _ = purge
    _set_form_schema(mock_ddb, single_key=False)
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"}]),
    })
    result = mod._purge_form_submissions("TEN-X", dry_run=False)
    assert result["rows_deleted"] == 1
    fs = store["picasso-form-submissions-staging"]
    assert "IndexName" not in fs.query.call_args.kwargs
    assert fs.delete_item.call_args.kwargs["Key"] == {
        "tenant_id": "TEN-X", "submission_id": "s1"}


def test_purge_form_submissions_prod_single_key_paginates(purge):
    """Audit GAP: the GSI (prod single-key) path must paginate — the IndexName
    kwarg must not break ExclusiveStartKey threading in _query_partition."""
    mod, mock_ddb, _ = purge
    _set_form_schema(mock_ddb, single_key=True)
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(query_pages=[
            {"Items": [{"submission_id": "s1"}], "LastEvaluatedKey": {"submission_id": "s1"}},
            {"Items": [{"submission_id": "s2"}]},
        ]),
    })
    result = mod._purge_form_submissions("TEN-X", dry_run=False)
    assert result["rows_deleted"] == 2
    fs = store["picasso-form-submissions-staging"]
    assert all(c.kwargs.get("IndexName") == mod.TENANT_TIMESTAMP_INDEX
               for c in fs.query.call_args_list)
    assert "ExclusiveStartKey" in fs.query.call_args_list[1].kwargs
    for call in fs.delete_item.call_args_list:
        assert set(call.kwargs["Key"].keys()) == {"submission_id"}


def test_purge_form_submissions_prod_single_key_dry_run(purge):
    """Audit GAP: dry-run on the prod single-key shape — query via the GSI,
    count only, no DeleteItem."""
    mod, mock_ddb, _ = purge
    _set_form_schema(mock_ddb, single_key=True)
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"submission_id": "s1"}, {"submission_id": "s2"}]),
    })
    result = mod._purge_form_submissions("TEN-X", dry_run=True)
    assert result["rows_found"] == 2
    assert result["action"] == "dry_run_count"
    fs = store["picasso-form-submissions-staging"]
    assert fs.query.call_args.kwargs["IndexName"] == mod.TENANT_TIMESTAMP_INDEX
    fs.delete_item.assert_not_called()


def test_purge_form_submissions_describe_failure_marks_error(purge):
    """A DescribeTable denial/outage surfaces the surface as errored (not a
    silent zero-row no-op) and never issues a Query."""
    mod, mock_ddb, _ = purge
    mock_ddb.meta.client.describe_table.side_effect = _client_error(
        code="AccessDeniedException", op="DescribeTable")
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"}]),
    })
    result = mod._purge_form_submissions("TEN-X", dry_run=False)
    assert result["action"] == "error"
    assert result["error"] == "query_failed"
    store["picasso-form-submissions-staging"].query.assert_not_called()


def test_form_submissions_corrupted_row_skipped(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(
            items=[{"tenant_id": "TEN-X", "submission_id": "s1"},
                   {"tenant_id": "TEN-X"}]),  # missing submission_id
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    # 1 deleted, 1 skipped corrupted.
    assert store["picasso-form-submissions-staging"].delete_item.call_count == 1
    assert any("corrupted" in f for f in resp["manual_followups"])


def test_form_submissions_delete_failure_counted(purge):
    mod, mock_ddb, _ = purge
    t = _table_mock(items=[{"tenant_id": "TEN-X", "submission_id": "s1"},
                           {"tenant_id": "TEN-X", "submission_id": "s2"}])
    t.delete_item.side_effect = [_client_error(op="DeleteItem"), {}]
    store = _wire_tables(mod, mock_ddb, {"picasso-form-submissions-staging": t})
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["status"] == "partial_error"
    assert any("failed to" in f for f in resp["manual_followups"])
    # rows_touched reports successful deletes only.
    assert resp["rows_touched"]["form-submissions"] == 1


def test_form_submissions_query_error_surfaces(purge):
    mod, mock_ddb, _ = purge
    t = MagicMock()
    t.query.side_effect = _client_error(op="Query")
    t.put_item.return_value = {}
    store = _wire_tables(mod, mock_ddb, {"picasso-form-submissions-staging": t})
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["status"] == "partial_error"
    assert any("form-submissions" in f and "query failed" in f
               for f in resp["manual_followups"])


def test_form_submissions_pagination(purge):
    mod, mock_ddb, _ = purge
    pages = [
        {"Items": [{"tenant_id": "TEN-X", "submission_id": "s1"}],
         "LastEvaluatedKey": {"tenant_id": "TEN-X", "submission_id": "s1"}},
        {"Items": [{"tenant_id": "TEN-X", "submission_id": "s2"}]},
    ]
    store = _wire_tables(mod, mock_ddb, {
        "picasso-form-submissions-staging": _table_mock(query_pages=pages),
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["rows_touched"]["form-submissions"] == 2
    assert store["picasso-form-submissions-staging"].delete_item.call_count == 2


# ───────────────────────────────────────────────────────────────────────────
# notification-sends + events chain
# ───────────────────────────────────────────────────────────────────────────
def test_notification_sends_uses_tenant_prefixed_pk(purge):
    mod, mock_ddb, _ = purge
    t = _table_mock(items=[{"pk": "TENANT#TEN-X", "sk": "m1", "message_id": "mid-1"}])
    store = _wire_tables(mod, mock_ddb, {"picasso-notification-sends": t})
    mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    # Query keyed on TENANT#<id>; delete on (pk, sk).
    t.delete_item.assert_called_once_with(Key={"pk": "TENANT#TEN-X", "sk": "m1"})


def test_notification_events_chained_from_send_message_ids(purge):
    mod, mock_ddb, _ = purge
    sends = _table_mock(items=[
        {"pk": "TENANT#TEN-X", "sk": "m1", "message_id": "mid-1"},
        {"pk": "TENANT#TEN-X", "sk": "m2", "message_id": ""},  # failed send: skipped
    ])
    events = _table_mock(items=[{"pk": "EVT#mid-1", "sk": "delivered", "message_id": "mid-1"}])
    store = _wire_tables(mod, mock_ddb, {
        "picasso-notification-sends": sends,
        "picasso-notification-events": events,
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    # events Query issued once (only the non-empty message_id).
    assert events.query.call_count == 1
    assert resp["rows_touched"]["notification-events"] == 1
    events.delete_item.assert_called_once_with(Key={"pk": "EVT#mid-1", "sk": "delivered"})


def test_notification_events_no_messages(purge):
    mod, mock_ddb, _ = purge
    sends = _table_mock(items=[])  # no sends → no message_ids
    events = _table_mock(items=[{"pk": "x", "sk": "y", "message_id": "z"}])
    store = _wire_tables(mod, mock_ddb, {
        "picasso-notification-sends": sends,
        "picasso-notification-events": events,
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["rows_touched"]["notification-events"] == 0
    events.query.assert_not_called()


def test_notification_events_gsi_failure_recorded(purge):
    mod, mock_ddb, _ = purge
    sends = _table_mock(items=[{"pk": "TENANT#TEN-X", "sk": "m1", "message_id": "mid-1"}])
    events = MagicMock()
    events.query.side_effect = _client_error(op="Query")
    events.put_item.return_value = {}
    store = _wire_tables(mod, mock_ddb, {
        "picasso-notification-sends": sends,
        "picasso-notification-events": events,
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["status"] == "partial_error"
    assert any("message_id(s) failed GSI" in f for f in resp["manual_followups"])


def test_notification_events_truncation_cap(purge):
    mod, mock_ddb, _ = purge
    cap = 500  # MAX_MESSAGE_IDS_PER_INVOCATION
    sends_items = [{"pk": "TENANT#TEN-X", "sk": f"m{i}", "message_id": f"mid-{i}"}
                   for i in range(cap + 3)]
    sends = _table_mock(items=sends_items)
    events = _table_mock(items=[])
    store = _wire_tables(mod, mock_ddb, {
        "picasso-notification-sends": sends,
        "picasso-notification-events": events,
    })
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    # Only `cap` GSI queries issued despite cap+3 message_ids.
    assert events.query.call_count == cap
    assert any("over the 500 cap" in f for f in resp["manual_followups"])


# ───────────────────────────────────────────────────────────────────────────
# subject-index + sms-usage
# ───────────────────────────────────────────────────────────────────────────
def test_subject_index_real_delete_key(purge):
    mod, mock_ddb, _ = purge
    t = _table_mock(items=[{"tenant_id": "TEN-X", "normalized_email": "a@b.c",
                            "pii_subject_id": "psid-1"}])
    store = _wire_tables(mod, mock_ddb, {"picasso-pii-subject-index": t})
    mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    t.delete_item.assert_called_once_with(
        Key={"tenant_id": "TEN-X", "normalized_email": "a@b.c"})


def test_sms_usage_real_delete_key(purge):
    mod, mock_ddb, _ = purge
    t = _table_mock(items=[{"tenant_id": "TEN-X", "month": "2026-06", "count": 3}])
    store = _wire_tables(mod, mock_ddb, {"picasso-sms-usage": t})
    mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    t.delete_item.assert_called_once_with(Key={"tenant_id": "TEN-X", "month": "2026-06"})


# ───────────────────────────────────────────────────────────────────────────
# Carve-outs must NOT be touched
# ───────────────────────────────────────────────────────────────────────────
def test_carve_out_tables_never_queried_or_deleted(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {})
    mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    touched = [c.args[0] for c in mock_ddb.Table.call_args_list]
    # No consent/suppression table should ever be addressed.
    assert not any("sms-consent" in name for name in touched)
    assert not any("suppression" in name for name in touched)


def test_carve_outs_reported_in_response_and_present(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(), None)
    assert resp["carve_outs_retained"] == mod.CARVE_OUTS_RETAINED
    assert any("sms-consent" in c for c in resp["carve_outs_retained"])
    assert any("STOP" in c for c in resp["carve_outs_retained"])


# ───────────────────────────────────────────────────────────────────────────
# Audit
# ───────────────────────────────────────────────────────────────────────────
def test_audit_rows_written_opening_per_surface_closing(purge):
    mod, mock_ddb, _ = purge
    audit = _table_mock()
    store = _wire_tables(mod, mock_ddb, {mod.TABLE_PURGE_AUDIT: audit})
    resp = mod.lambda_handler(_event(), None)
    # 1 opening + 6 surface (incl. session-summaries) + 1 closing = 8 put_items.
    assert audit.put_item.call_count == 8
    assert len(resp["audit_row_pks"]) == 8
    # Append-only condition present on each put.
    for call in audit.put_item.call_args_list:
        assert "attribute_not_exists" in call.kwargs["ConditionExpression"]


def test_audit_opening_collision_fails_closed(purge):
    mod, mock_ddb, _ = purge
    audit = MagicMock()
    audit.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
    store = _wire_tables(mod, mock_ddb, {mod.TABLE_PURGE_AUDIT: audit})
    resp = mod.lambda_handler(_event(), None)
    assert resp["status"] == "failed"
    assert "already exists" in resp["error"]


def test_audit_opening_records_delete_authorized_flag(purge):
    mod, mock_ddb, _ = purge
    audit = _table_mock()
    store = _wire_tables(mod, mock_ddb, {mod.TABLE_PURGE_AUDIT: audit})
    mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    opening = audit.put_item.call_args_list[0].kwargs["Item"]
    assert opening["event_type"] == "purge_requested"
    assert opening["status"] == "in_progress"


# ───────────────────────────────────────────────────────────────────────────
# Response shape + idempotent re-run
# ───────────────────────────────────────────────────────────────────────────
def test_response_shape_has_all_surfaces(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})
    resp = mod.lambda_handler(_event(), None)
    assert set(resp["rows_touched"].keys()) == {
        "form-submissions", "notification-sends", "notification-events",
        "subject-index", "sms-usage", "session-summaries"}
    assert resp["purge_id"] == "purge-uuid-1"
    assert resp["tenant_id"] == "TEN-X"


def test_idempotent_rerun_empty_tables_zero_counts(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})  # all empty
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["status"] == "completed"
    assert all(v == 0 for v in resp["rows_touched"].values())
    assert resp["deleted"] is True  # authorized, just nothing left to delete


# ── Class C (F-DSAR31): session-summaries surface ───────────────────────────
def test_session_summaries_purged_when_tenant_hash_provided(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-session-summaries": _table_mock(items=[
            {"pk": "TENANT#h", "sk": "SESSION#s1"},
            {"pk": "TENANT#h", "sk": "SESSION#s2"}]),
    })
    resp = mod.lambda_handler(
        _event(dry_run=False, grace_confirmed=True, tenant_hash="h"), None)
    assert resp["rows_touched"]["session-summaries"] == 2
    ss = store["picasso-session-summaries"]
    assert ss.delete_item.call_count == 2
    ss.delete_item.assert_any_call(Key={"pk": "TENANT#h", "sk": "SESSION#s1"})


def test_session_summaries_dry_run_counts_only(purge):
    mod, mock_ddb, _ = purge
    store = _wire_tables(mod, mock_ddb, {
        "picasso-session-summaries": _table_mock(items=[
            {"pk": "TENANT#h", "sk": "SESSION#s1"}]),
    })
    resp = mod.lambda_handler(_event(tenant_hash="h"), None)  # dry_run defaults True
    assert resp["deleted"] is False
    assert resp["rows_touched"]["session-summaries"] == 1
    store["picasso-session-summaries"].delete_item.assert_not_called()


def test_session_summaries_skipped_without_tenant_hash(purge):
    mod, mock_ddb, _ = purge
    _wire_tables(mod, mock_ddb, {})  # no tenant_hash on event
    resp = mod.lambda_handler(_event(dry_run=False, grace_confirmed=True), None)
    assert resp["rows_touched"]["session-summaries"] == 0
    assert any("session-summaries: skipped" in f for f in resp["manual_followups"])
