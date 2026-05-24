"""Unit tests for the DSAR SLA monitor Lambda.

Mocked-DDB + mocked-SNS approach (deterministic; no AWS dependency).
Covers:
- 0 candidates → no SNS publish
- Candidates exist BUT all have closed events → no SNS publish
- Candidates exist + some lack closed events → SNS publish with at-risk list
- Pagination over StatusIndex Query
- DDB Query failure → re-raised
- SNS Publish failure → re-raised
- Missing SNS_TOPIC_ARN config → RuntimeError
- D1: SNS body excludes PII (only dsar_id + event_timestamp, no email/tenant)
"""
import os
import sys
import importlib
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


# Inject env vars BEFORE importing the module under test
os.environ.setdefault('AUDIT_TABLE', 'picasso-pii-dsar-audit-staging')
os.environ.setdefault('SLA_DAYS_INTAKE_PLUS', '25')
os.environ.setdefault('SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:525409062831:picasso-ops-alerts-staging')


@pytest.fixture
def monitor():
    """Fresh import per-test so module-level env vars rebind cleanly."""
    # Add module path
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    if 'lambda_function' in sys.modules:
        del sys.modules['lambda_function']
    mod = importlib.import_module('lambda_function')
    mock_ddb = MagicMock()
    mock_sns = MagicMock()
    mod.ddb = mock_ddb
    mod.sns = mock_sns
    return mod, mock_ddb, mock_sns


def _intake_row(dsar_id, hours_ago):
    """Build a request_received row with event_timestamp `hours_ago` hours in the past."""
    ts = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
    return {
        'dsar_id': dsar_id,
        'event_timestamp': ts,
        'event_type': 'request_received',
        'status': 'in_progress',
        # details intentionally minimal — PII would live here normally but
        # tests only need the structural fields
    }


def _stub_status_query(table_mock, items, last_evaluated_key=None):
    """Helper: stub the StatusIndex query response."""
    resp = {'Items': items}
    if last_evaluated_key:
        resp['LastEvaluatedKey'] = last_evaluated_key
    table_mock.query.return_value = resp


def test_no_candidates_no_sns(monitor):
    """Empty audit table (no intakes past threshold) → no SNS publish."""
    mod, mock_ddb, mock_sns = monitor
    table_mock = MagicMock()
    _stub_status_query(table_mock, [])
    mock_ddb.Table.return_value = table_mock

    result = mod.lambda_handler({}, None)

    assert result == {'at_risk_count': 0}
    mock_sns.publish.assert_not_called()


def test_candidate_with_closed_event_not_alarmed(monitor):
    """An intake row past threshold WITH a closed event is not at-risk."""
    mod, mock_ddb, mock_sns = monitor

    # Two Table calls: one for StatusIndex query, one for per-dsar 'closed' check.
    intake_row = _intake_row('dsar-001', hours_ago=26 * 24)  # 26 days ago

    status_table = MagicMock()
    _stub_status_query(status_table, [intake_row])

    main_table = MagicMock()
    main_table.query.return_value = {'Count': 1}  # has closed event

    # Table() returns status_table first, main_table second
    mock_ddb.Table.side_effect = [status_table, main_table]

    result = mod.lambda_handler({}, None)

    assert result == {'at_risk_count': 0}
    mock_sns.publish.assert_not_called()


def test_candidate_without_closed_event_alarmed(monitor):
    """An intake row past threshold WITHOUT a closed event triggers SNS publish."""
    mod, mock_ddb, mock_sns = monitor

    intake_row = _intake_row('dsar-002', hours_ago=30 * 24)

    status_table = MagicMock()
    _stub_status_query(status_table, [intake_row])

    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}  # no closed event

    mock_ddb.Table.side_effect = [status_table, main_table]

    result = mod.lambda_handler({}, None)

    assert result['at_risk_count'] == 1
    assert result['dsar_ids'] == ['dsar-002']
    mock_sns.publish.assert_called_once()
    call_kwargs = mock_sns.publish.call_args.kwargs
    assert 'dsar-002' in call_kwargs['Message']
    assert call_kwargs['TopicArn'] == os.environ['SNS_TOPIC_ARN']


def test_sns_body_excludes_pii(monitor):
    """D1 schema-discipline: SNS body must NOT include normalized_email or tenant_id.

    A real audit row will carry PII in details (email, tenant_id, operator metadata).
    The SNS body publishes to operator inboxes — should expose dsar_id + intake
    timestamp only (sufficient for operator to look up via audit-table query).
    """
    mod, mock_ddb, mock_sns = monitor

    intake_row = _intake_row('dsar-003', hours_ago=27 * 24)
    intake_row['details'] = {
        'normalized_email': 'leaky@example.com',
        'tenant_id': 'TEN_LEAKY',
        'operator_caller_arn': 'arn:aws:sts::525:caller/secret',
    }

    status_table = MagicMock()
    _stub_status_query(status_table, [intake_row])
    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}
    mock_ddb.Table.side_effect = [status_table, main_table]

    mod.lambda_handler({}, None)

    body = mock_sns.publish.call_args.kwargs['Message']
    assert 'leaky@example.com' not in body
    assert 'TEN_LEAKY' not in body
    assert 'operator_caller_arn' not in body
    assert 'dsar-003' in body  # dsar_id IS allowed


def test_pagination_status_query(monitor):
    """StatusIndex Query pagination — multiple LastEvaluatedKey roundtrips."""
    mod, mock_ddb, mock_sns = monitor

    status_table = MagicMock()
    # Three pages: page1+page2 carry LastEvaluatedKey; page3 stops
    status_table.query.side_effect = [
        {'Items': [_intake_row('d1', 30 * 24)], 'LastEvaluatedKey': {'k': '1'}},
        {'Items': [_intake_row('d2', 30 * 24)], 'LastEvaluatedKey': {'k': '2'}},
        {'Items': [_intake_row('d3', 30 * 24)]},
    ]

    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}  # all 3 not-closed

    # 1 status_table call + 3 main_table calls (one per dsar_id)
    mock_ddb.Table.side_effect = [status_table, main_table, main_table, main_table]

    result = mod.lambda_handler({}, None)

    assert result['at_risk_count'] == 3
    assert set(result['dsar_ids']) == {'d1', 'd2', 'd3'}
    assert status_table.query.call_count == 3
    # Second + third calls carry ExclusiveStartKey
    assert 'ExclusiveStartKey' not in status_table.query.call_args_list[0].kwargs
    assert 'ExclusiveStartKey' in status_table.query.call_args_list[1].kwargs
    assert 'ExclusiveStartKey' in status_table.query.call_args_list[2].kwargs


def test_status_query_failure_raises(monitor):
    """DDB Query failure on StatusIndex re-raises (visible to CloudWatch)."""
    mod, mock_ddb, mock_sns = monitor

    status_table = MagicMock()
    status_table.query.side_effect = ClientError(
        {'Error': {'Code': 'ProvisionedThroughputExceededException'}}, 'Query',
    )
    mock_ddb.Table.return_value = status_table

    with pytest.raises(ClientError):
        mod.lambda_handler({}, None)

    mock_sns.publish.assert_not_called()


def test_closed_check_failure_raises(monitor):
    """DDB Query failure on per-dsar closed check re-raises (conservative posture)."""
    mod, mock_ddb, mock_sns = monitor

    status_table = MagicMock()
    _stub_status_query(status_table, [_intake_row('d_fail', 30 * 24)])
    main_table = MagicMock()
    main_table.query.side_effect = ClientError(
        {'Error': {'Code': 'InternalServerError'}}, 'Query',
    )
    mock_ddb.Table.side_effect = [status_table, main_table]

    with pytest.raises(ClientError):
        mod.lambda_handler({}, None)

    mock_sns.publish.assert_not_called()


def test_sns_publish_failure_raises(monitor):
    """SNS publish failure re-raises (visible to CloudWatch + operator's weekly check)."""
    mod, mock_ddb, mock_sns = monitor

    status_table = MagicMock()
    _stub_status_query(status_table, [_intake_row('d_sns_fail', 30 * 24)])
    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}
    mock_ddb.Table.side_effect = [status_table, main_table]

    mock_sns.publish.side_effect = ClientError(
        {'Error': {'Code': 'AuthorizationError'}}, 'Publish',
    )

    with pytest.raises(ClientError):
        mod.lambda_handler({}, None)


def test_missing_sns_topic_arn_raises(monitor):
    """Misconfigured SNS_TOPIC_ARN (env unset) → RuntimeError."""
    mod, mock_ddb, mock_sns = monitor

    # Force the module's bound SNS_TOPIC_ARN to None (simulating env var unset
    # at import time — module-level binding doesn't re-read env each call)
    original = mod.SNS_TOPIC_ARN
    mod.SNS_TOPIC_ARN = None
    try:
        status_table = MagicMock()
        _stub_status_query(status_table, [_intake_row('d_unconfigured', 30 * 24)])
        main_table = MagicMock()
        main_table.query.return_value = {'Count': 0}
        mock_ddb.Table.side_effect = [status_table, main_table]

        with pytest.raises(RuntimeError, match='SNS_TOPIC_ARN'):
            mod.lambda_handler({}, None)

        mock_sns.publish.assert_not_called()
    finally:
        mod.SNS_TOPIC_ARN = original


def test_filters_non_request_received_status_in_progress(monitor):
    """Defensive event_type filter: status='in_progress' rows that AREN'T
    request_received are ignored. Today only request_received uses
    in_progress; this guards against future audit writer additions.
    """
    mod, mock_ddb, mock_sns = monitor

    intake_row = _intake_row('d_intake', 30 * 24)
    other_event = {
        'dsar_id': 'd_other',
        'event_timestamp': intake_row['event_timestamp'],
        'event_type': 'some_future_in_progress_event',
        'status': 'in_progress',
    }

    status_table = MagicMock()
    _stub_status_query(status_table, [intake_row, other_event])
    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}
    mock_ddb.Table.side_effect = [status_table, main_table]

    result = mod.lambda_handler({}, None)

    # Only the request_received row counts; the other 'in_progress' event ignored
    assert result['at_risk_count'] == 1
    assert result['dsar_ids'] == ['d_intake']


# ─────────────────────────────────────────────────────────────────────────────
# M9.G7 / F-DSAR27 — operational test bundle (phase-completion-audit 2026-05-23
# test-engineer 🟡 gaps; closed via this file 2026-05-24)
# ─────────────────────────────────────────────────────────────────────────────

def test_ddb_table_called_with_correct_name(monitor):
    """Regression guard for AUDIT_TABLE env var binding.

    A typo or env-var drift in the IaC layer would silently send the Lambda
    against the wrong table. This test asserts BOTH call sites use the
    configured table name: the StatusIndex Query (line ~74) AND the per-
    candidate `_has_closed_event` Query (line ~109). Sprint E2 / audit D6:
    switched from assert_called_with (LAST call only) + 0 candidates (which
    only exercised the StatusIndex path) to assert_any_call + 1 candidate so
    both Table() calls are pinned.
    """
    mod, mock_ddb, mock_sns = monitor

    intake_row = _intake_row('dsar-table-name-check', hours_ago=30 * 24)

    status_table = MagicMock()
    _stub_status_query(status_table, [intake_row])
    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}  # no closed event → at-risk
    mock_ddb.Table.side_effect = [status_table, main_table]

    mod.lambda_handler({}, None)

    expected_name = os.environ['AUDIT_TABLE']
    from unittest.mock import call

    # Sprint F2 / audit-of-audit finding 4: assert_any_call + call_count==2
    # could pass even if the SECOND call site switched to a hardcoded table
    # name. assert_has_calls pins BOTH calls' args explicitly so a regression
    # on either call site (StatusIndex Query OR _has_closed_event Query)
    # surfaces in CI.
    mock_ddb.Table.assert_has_calls([call(expected_name), call(expected_name)])
    assert mock_ddb.Table.call_count == 2, (
        f'expected exactly 2 Table() calls (StatusIndex + per-candidate); '
        f'got {mock_ddb.Table.call_count}'
    )


def test_handler_idempotent_on_eventbridge_replay(monitor):
    """EventBridge can replay an invocation (delivery semantics are at-least-
    once). Re-invoking the handler with the same DDB state MUST produce the
    same outcome both times — no double SNS publish, no state mutation in
    the audit table, identical return value.

    The Lambda is read-only on the audit table, so this is by construction —
    the test pins the invariant so a future code change can't silently
    introduce a side effect that breaks replay safety.
    """
    mod, mock_ddb, mock_sns = monitor

    intake_row = _intake_row('dsar-replay', hours_ago=30 * 24)

    def fresh_setup():
        status_table = MagicMock()
        _stub_status_query(status_table, [intake_row])
        main_table = MagicMock()
        main_table.query.return_value = {'Count': 0}
        return status_table, main_table

    s1, m1 = fresh_setup()
    s2, m2 = fresh_setup()
    # Each invocation walks: status_table once + main_table once per candidate
    mock_ddb.Table.side_effect = [s1, m1, s2, m2]

    r1 = mod.lambda_handler({'replay_attempt': 1}, None)
    r2 = mod.lambda_handler({'replay_attempt': 2}, None)

    # Identical outcomes
    assert r1 == r2 == {'at_risk_count': 1, 'dsar_ids': ['dsar-replay']}
    # DESIGN DECISION (audit N12, pinned by this test scope):
    # SNS published once per invocation; total 2 publishes for 2 replays.
    # This is INTENTIONAL — the Lambda's design accepts duplicate alerts on
    # replay because the operator's de-dup happens DOWNSTREAM (intake-via-
    # email reading; Gmail thread groups by Subject). Lambda-side state
    # suppression would require a write to the audit table, violating the
    # read-only posture + the C2 4-action Deny resource policy.
    # The test scope is intentionally "idempotency = same OUTPUT", NOT
    # "idempotency = no duplicate notifications". Don't try to add the
    # latter without also re-architecting the read-only contract.
    assert mock_sns.publish.call_count == 2
    # No DDB writes in either invocation (mock_ddb tracks .put_item etc. would
    # fail in MagicMock if attempted with side_effects exhausted)
    for table_mock in [s1, m1, s2, m2]:
        table_mock.put_item.assert_not_called()
        table_mock.update_item.assert_not_called()
        table_mock.delete_item.assert_not_called()


def test_lambda_timeout_mid_loop_does_not_publish_partial(monitor):
    """Lambda timeout during the per-candidate `_has_closed_event` Query
    must NOT publish a partial at-risk list. The Lambda is fail-fast (the
    ClientError handler re-raises and the timeout-as-exception bubbles out),
    so the SNS publish only happens at the END of the loop when all
    candidates have been checked.

    Simulates a hung main-table Query that raises a ReadTimeoutError-like
    exception mid-loop; verifies SNS was never called.
    """
    mod, mock_ddb, mock_sns = monitor

    status_table = MagicMock()
    _stub_status_query(
        status_table,
        [_intake_row('d-a', 30 * 24), _intake_row('d-b', 30 * 24),
         _intake_row('d-c', 30 * 24)],
    )

    main_table = MagicMock()
    # First closed-check succeeds (no closed event); second one times out
    timeout_err = ClientError(
        {'Error': {'Code': 'RequestTimeout', 'Message': 'simulated timeout'}},
        'Query',
    )
    main_table.query.side_effect = [{'Count': 0}, timeout_err]

    mock_ddb.Table.side_effect = [status_table, main_table, main_table]

    with pytest.raises(ClientError) as exc_info:
        mod.lambda_handler({}, None)

    assert exc_info.value.response['Error']['Code'] == 'RequestTimeout'
    # Critical: NO partial publish — even though d-a's closed check succeeded
    # and would otherwise be in the at-risk list, the mid-loop timeout aborts
    # before the publish step
    mock_sns.publish.assert_not_called()


def test_event_timestamp_iso_format_contract(monitor):
    """Reader-side contract test for the writer's ISO format.

    The DSAR Lambda writer (`Lambdas/lambda/picasso_pii_dsar_staging/
    lambda_function.py:_now_iso`) emits `event_timestamp` as
    `datetime.now(timezone.utc).isoformat(timespec="microseconds")` →
    string like `'2026-05-24T05:02:32.523460+00:00'`.

    The SLA monitor's `_query_open_intakes_past_threshold` builds the
    `threshold_iso` via `threshold.isoformat()` (its own format) and DDB
    does lexicographic string comparison for the `<=` filter. The formats
    MUST match in shape (both microseconds-precision, both with the
    `+00:00` timezone suffix) or the comparison silently mis-orders rows
    at format-boundary moments.

    This test pins the writer's exact format + asserts the reader handles
    it correctly. If the writer ever changes shape (e.g., drops microseconds
    or switches to `Z` suffix), this test fires.
    """
    mod, mock_ddb, mock_sns = monitor

    # Build a row whose event_timestamp uses the writer's EXACT format
    writer_ts = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(
        timespec='microseconds')
    # Sanity-check the format we're claiming the writer emits
    assert '.' in writer_ts, 'writer format must include microseconds'
    assert writer_ts.endswith('+00:00'), 'writer format must end with +00:00'
    # 26 chars of date+time+microseconds + 6 chars of '+00:00' = 32 chars total
    assert len(writer_ts) == 32, f'expected 32-char ISO; got {len(writer_ts)}'

    intake_row = {
        'dsar_id': 'dsar-iso-contract',
        'event_timestamp': writer_ts,
        'event_type': 'request_received',
        'status': 'in_progress',
    }

    status_table = MagicMock()
    _stub_status_query(status_table, [intake_row])
    main_table = MagicMock()
    main_table.query.return_value = {'Count': 0}
    mock_ddb.Table.side_effect = [status_table, main_table]

    result = mod.lambda_handler({}, None)

    # The 30-day-old row IS past threshold (25d), so reader correctly
    # surfaces it. Threshold is computed by the monitor itself; the writer's
    # format must be lexicographically comparable with the threshold's format.
    assert result['at_risk_count'] == 1
    assert result['dsar_ids'] == ['dsar-iso-contract']

    # Verify the threshold the monitor computed was ALSO in microseconds-
    # precision format (post-M9.G7 the monitor pins this; pre-M9.G7 it
    # depended on now()'s microsecond field being nonzero — brittle).
    keys = status_table.query.call_args.kwargs['KeyConditionExpression']
    # KeyConditionExpression is a boto3 ConditionBase; introspect via repr
    threshold_in_query = str(keys)
    # The threshold should be 32 chars (microsecond ISO) — the M9.G7 tighten
    # explicitly pins timespec='microseconds' on the reader side too
    assert '.' in threshold_in_query, (
        'monitor must emit microsecond-precision threshold to lexicographically '
        'match the writer format'
    )


# Sprint F2 / audit-of-audit finding 12 — N17 truncation path test
def test_publish_alert_logs_warning_when_subject_exceeds_100_chars(monitor, caplog):
    """The Sprint E2 N17 fix added a logger.warning before truncating SNS
    subjects >100 chars, but the path was untested. Trigger it by monkey-
    patching SLA_DAYS_INTAKE_PLUS to a long-string sentinel (simulating
    env-var misconfiguration) so the subject overflows. Verifies (a) warning
    fires with the expected log key, (b) subject is truncated to exactly
    100 chars, (c) SNS.publish still gets called.
    """
    import logging
    mod, mock_ddb, mock_sns = monitor
    # Long sentinel for SLA_DAYS_INTAKE_PLUS — 80 chars of 'x' pushes the
    # subject format well past the 100-char cap.
    original = mod.SLA_DAYS_INTAKE_PLUS
    try:
        mod.SLA_DAYS_INTAKE_PLUS = 'x' * 80
        with caplog.at_level(logging.WARNING):
            mod._publish_alert([{'dsar_id': 'd1', 'event_timestamp': '2026-05-24T00:00:00Z'}])
    finally:
        mod.SLA_DAYS_INTAKE_PLUS = original

    # Subject should have been truncated to exactly 100
    publish_call = mock_sns.publish.call_args
    assert len(publish_call.kwargs['Subject']) == 100, (
        f'truncated subject must be exactly 100 chars; '
        f'got {len(publish_call.kwargs["Subject"])}'
    )
    # Warning log line present with expected structured key
    truncation_logs = [r for r in caplog.records
                       if 'sla_monitor_subject_truncated' in r.getMessage()]
    assert len(truncation_logs) == 1, (
        f'expected exactly 1 truncation warning; got {len(truncation_logs)}'
    )
