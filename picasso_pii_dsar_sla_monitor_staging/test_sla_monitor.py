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
