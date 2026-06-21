"""
Unit tests for the §G5/E15 scheduling metrics endpoint: handle_scheduling_metrics().

Phase 1 = booking COUNTS derivable from §A rows: a status breakdown + a per-appointment-type
breakdown, over the bounded tenantId-start_at-index window. Covers: admin-only + feature gate
(the security surface), the bounded GSI query (no full scan), multi-page aggregation, the
schema-discipline defaults (missing status/type), the DDB error path, and the documented
`unavailable` Phase-2 metrics.
"""

import json
from unittest.mock import patch

import pytest

import lambda_function
from lambda_function import handle_scheduling_metrics


def _row(status=None, atype=None):
    item = {}
    if status is not None:
        item['status'] = {'S': status}
    if atype is not None:
        item['appointment_type_id'] = {'S': atype}
    return item


def _body(resp):
    assert resp['statusCode'] == 200, resp
    return json.loads(resp['body'])


@pytest.fixture(autouse=True)
def _grant_feature():
    # Default every test to scheduling-granted; the gate-denied test overrides.
    with patch.object(lambda_function, 'validate_feature_access', return_value=None):
        yield


# --------------------------------------------------------------------------- #
# security surface
# --------------------------------------------------------------------------- #

def test_non_admin_403():
    for role in ('member', None, 'viewer'):
        resp = handle_scheduling_metrics('TEN1', {}, role)
        assert resp['statusCode'] == 403, role


def test_feature_gate_denied_403():
    denied = lambda_function.cors_response(403, {'error': 'Feature not available'})
    with patch.object(lambda_function, 'validate_feature_access', return_value=denied) as gate:
        resp = handle_scheduling_metrics('TEN1', {}, 'admin')
    assert resp['statusCode'] == 403
    gate.assert_called_once_with('TEN1', 'dashboard_scheduling', 'admin')


@patch('lambda_function.dynamodb')
def test_super_admin_allowed(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    resp = handle_scheduling_metrics('TEN1', {}, 'super_admin')
    assert resp['statusCode'] == 200


# --------------------------------------------------------------------------- #
# aggregation
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_aggregates_by_status_and_type(mock_ddb):
    mock_ddb.query.return_value = {'Items': [
        _row('booked', 'intro'),
        _row('booked', 'intro'),
        _row('canceled', 'intro'),
        _row('completed', 'deep-dive'),
        _row('no_show', 'deep-dive'),
    ]}
    body = _body(handle_scheduling_metrics('TEN1', {}, 'admin'))
    assert body['total'] == 5
    assert body['by_status'] == {'booked': 2, 'canceled': 1, 'completed': 1, 'no_show': 1}
    by_type = {t['appointment_type_id']: t for t in body['by_type']}
    assert by_type['intro']['count'] == 3
    assert by_type['intro']['by_status'] == {'booked': 2, 'canceled': 1}
    assert by_type['deep-dive']['count'] == 2
    assert by_type['deep-dive']['by_status'] == {'completed': 1, 'no_show': 1}
    # by_type sorted by count desc → intro (3) before deep-dive (2)
    assert [t['appointment_type_id'] for t in body['by_type']] == ['intro', 'deep-dive']


@patch('lambda_function.dynamodb')
def test_uses_bounded_start_at_index_with_window_and_projection(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_metrics('TEN1', {}, 'admin')
    kwargs = mock_ddb.query.call_args.kwargs
    assert kwargs['IndexName'] == lambda_function.BOOKING_TENANT_START_INDEX
    assert 'start_at BETWEEN' in kwargs['KeyConditionExpression']
    assert kwargs['ExpressionAttributeValues'][':t'] == {'S': 'TEN1'}  # tenant from auth, not a param
    assert kwargs['ProjectionExpression'] == '#s, appointment_type_id'
    assert kwargs['ExpressionAttributeNames'] == {'#s': 'status'}  # `status` is reserved


@patch('lambda_function.dynamodb')
def test_paginates_across_pages(mock_ddb):
    mock_ddb.query.side_effect = [
        {'Items': [_row('booked', 'intro')], 'LastEvaluatedKey': {'k': {'S': '1'}}},
        {'Items': [_row('booked', 'intro'), _row('canceled', 'intro')]},  # no LEK → stop
    ]
    body = _body(handle_scheduling_metrics('TEN1', {}, 'admin'))
    assert body['total'] == 3
    assert body['by_status'] == {'booked': 2, 'canceled': 1}
    assert mock_ddb.query.call_count == 2
    # 2nd query carried the ExclusiveStartKey from page 1
    assert mock_ddb.query.call_args_list[1].kwargs['ExclusiveStartKey'] == {'k': {'S': '1'}}


@patch('lambda_function.dynamodb')
def test_empty(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    body = _body(handle_scheduling_metrics('TEN1', {}, 'admin'))
    assert body['total'] == 0
    assert body['by_status'] == {}
    assert body['by_type'] == []


@patch('lambda_function.dynamodb')
def test_missing_status_or_type_defaults(mock_ddb):
    # Schema-discipline: a row missing status/appointment_type_id must not crash.
    mock_ddb.query.return_value = {'Items': [_row(None, None), _row('booked', None)]}
    body = _body(handle_scheduling_metrics('TEN1', {}, 'admin'))
    assert body['total'] == 2
    assert body['by_status'] == {'unknown': 1, 'booked': 1}
    by_type = {t['appointment_type_id']: t['count'] for t in body['by_type']}
    assert by_type['unassigned'] == 2


@patch('lambda_function.dynamodb')
def test_unavailable_lists_phase2_metrics(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    body = _body(handle_scheduling_metrics('TEN1', {}, 'admin'))
    assert set(body['unavailable']) == {'time_to_book', 'reschedule_rate'}


@patch('lambda_function.dynamodb')
def test_ddb_error_502(mock_ddb):
    from botocore.exceptions import ClientError
    mock_ddb.query.side_effect = ClientError(
        {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'x'}}, 'Query')
    resp = handle_scheduling_metrics('TEN1', {}, 'admin')
    assert resp['statusCode'] == 502


@patch('lambda_function.dynamodb')
def test_page_cap_marks_partial(mock_ddb):
    # Always returns a LastEvaluatedKey → the page cap must stop the loop and flag partial.
    mock_ddb.query.return_value = {'Items': [_row('booked', 'intro')], 'LastEvaluatedKey': {'k': {'S': 'x'}}}
    body = _body(handle_scheduling_metrics('TEN1', {}, 'admin'))
    assert body.get('partial') is True
    assert mock_ddb.query.call_count == lambda_function._SCHED_METRICS_MAX_PAGES


@patch('lambda_function.dynamodb')
def test_metrics_query_excludes_synthetic(mock_ddb):
    """Synthetic-monitor canary rows must not inflate a tenant's metrics."""
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_metrics('TEN1', {}, 'admin')
    kw = mock_ddb.query.call_args.kwargs
    assert 'attribute_not_exists(is_synthetic)' in kw['FilterExpression']
    assert kw['ExpressionAttributeValues'][':synfalse'] == {'BOOL': False}
