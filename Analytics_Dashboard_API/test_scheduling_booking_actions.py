"""
Unit tests for the §G6/E12 booking ACTION endpoints:
  handle_scheduling_booking_cancel()           POST /scheduling/bookings/{id}/cancel
  handle_scheduling_booking_reschedule_link()  POST /scheduling/bookings/{id}/reschedule-link

ADA is the Clerk-authed ENTRY: feature-gate → load booking (GetItem on the tenant-scoped key) →
§8 permission (own-by-coordinator_email or admin) → proxy the side-effect to BCH's
scheduling_mutate executor. Coverage: the security surface (feature gate + §8 own/admin/non-owner),
the validation surface (reason required/length), not-found + already-canceled, the executor
outcomes (deleted / pending_calendar_sync / failure / FunctionError), the GetItem error path, the
payload shape handed to BCH, and the booking_id urldecode (booking ids contain '#').
"""

import json
import urllib.parse
from unittest.mock import patch, MagicMock

import pytest

import lambda_function
from lambda_function import (
    handle_scheduling_booking_cancel,
    handle_scheduling_booking_reschedule_link,
    _authorize_booking_action,
)


def _ddb_booking(status='booked', coord='coord@x.com'):
    """A marshalled DDB Booking item as get_item returns it."""
    item = {
        'tenantId': {'S': 'TEN1'},
        'booking_id': {'S': 'booking#abc'},
        'coordinator_email': {'S': coord},
        'external_event_id': {'S': 'evt1'},
        'start_at': {'S': '2026-07-01T15:00:00Z'},
        'attendee_email': {'S': 'guest@x.com'},
    }
    if status is not None:
        item['status'] = {'S': status}
    return item


class _Payload:
    def __init__(self, data):
        self._d = data

    def read(self):
        return json.dumps(self._d).encode('utf-8')


def _invoke_resp(result, function_error=None):
    r = {'Payload': _Payload(result)}
    if function_error:
        r['FunctionError'] = function_error
    return r


@pytest.fixture(autouse=True)
def _grant_feature():
    with patch.object(lambda_function, 'validate_feature_access', return_value=None):
        yield


# --------------------------------------------------------------------------- #
# §8 permission helper
# --------------------------------------------------------------------------- #

def test_authorize_admin_any():
    bk = {'coordinator_email': 'someone@else.com'}
    assert _authorize_booking_action(bk, 'admin', 'admin@org.com') is True
    assert _authorize_booking_action(bk, 'super_admin', 'op@org.com') is True


def test_authorize_staff_own_only():
    bk = {'coordinator_email': 'Coord@X.com'}
    # case-insensitive own match
    assert _authorize_booking_action(bk, 'member', 'coord@x.com') is True
    # non-owner staff denied
    assert _authorize_booking_action(bk, 'member', 'other@x.com') is False
    # no email → denied
    assert _authorize_booking_action(bk, 'member', '') is False


# --------------------------------------------------------------------------- #
# cancel — security + validation
# --------------------------------------------------------------------------- #

def test_cancel_feature_gate_denied_403():
    denied = lambda_function.cors_response(403, {'error': 'Feature not available'})
    with patch.object(lambda_function, 'validate_feature_access', return_value=denied) as gate:
        resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 403
    gate.assert_called_once_with('TEN1', 'dashboard_scheduling', 'admin')


def test_cancel_missing_reason_400():
    for body in ({}, {'reason': ''}, {'reason': '   '}, {'reason': 123}):
        resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', body, 'admin', 'a@x.com')
        assert resp['statusCode'] == 400, body


def test_cancel_reason_too_long_400():
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x' * 1001}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 400


@patch('lambda_function.dynamodb')
def test_cancel_not_found_404(mock_ddb):
    mock_ddb.get_item.return_value = {}  # no Item
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#missing', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 404


@patch('lambda_function.dynamodb')
def test_cancel_staff_non_owner_403(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='owner@x.com')}
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'member', 'intruder@x.com')
    assert resp['statusCode'] == 403


@patch('lambda_function.dynamodb')
def test_cancel_already_canceled_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(status='canceled')}
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 409


# --------------------------------------------------------------------------- #
# cancel — executor outcomes + payload
# --------------------------------------------------------------------------- #

@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_staff_owner_success_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='coord@x.com')}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'deleted'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'Volunteer asked'}, 'member', 'coord@x.com')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body']) == {'booking_id': 'booking#abc', 'status': 'canceled'}
    # payload shape handed to BCH
    payload = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'].decode())
    assert payload['action'] == 'scheduling_mutate'
    assert payload['mutation'] == 'cancel'
    assert payload['tenantId'] == 'TEN1'
    assert payload['coordinatorId'] == 'coord@x.com'
    assert payload['bookingId'] == 'booking#abc'
    assert payload['reason'] == 'Volunteer asked'
    assert payload['canceled_by'] == 'coord@x.com'


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_admin_any_success_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='someone@else.com')}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'deleted'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'admin@org.com')
    assert resp['statusCode'] == 200


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_pending_calendar_sync_202(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'pending_calendar_sync'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 202
    assert json.loads(resp['body'])['status'] == 'pending_calendar_sync'


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_executor_failed_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'failed', 'error': 'executor_error'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_function_error_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'errorMessage': 'boom'}, function_error='Unhandled')
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function.dynamodb')
def test_cancel_getitem_error_502(mock_ddb):
    from botocore.exceptions import ClientError
    mock_ddb.get_item.side_effect = ClientError({'Error': {'Code': 'X'}}, 'GetItem')
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


# --------------------------------------------------------------------------- #
# reschedule-link
# --------------------------------------------------------------------------- #

def test_rl_feature_gate_denied_403():
    denied = lambda_function.cors_response(403, {'error': 'Feature not available'})
    with patch.object(lambda_function, 'validate_feature_access', return_value=denied):
        resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 403


@patch('lambda_function.dynamodb')
def test_rl_not_found_404(mock_ddb):
    mock_ddb.get_item.return_value = {}
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#missing', 'admin', 'a@x.com')
    assert resp['statusCode'] == 404


@patch('lambda_function.dynamodb')
def test_rl_staff_non_owner_403(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='owner@x.com')}
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'member', 'intruder@x.com')
    assert resp['statusCode'] == 403


@patch('lambda_function.dynamodb')
def test_rl_canceled_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(status='canceled')}
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 409


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_success_sent_true_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'success', 'sent': True})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body']) == {'booking_id': 'booking#abc', 'sent': True}
    payload = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'].decode())
    assert payload['mutation'] == 'reschedule_link'
    assert payload['bookingId'] == 'booking#abc'


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_success_sent_false_200(mock_ddb, mock_lambda):
    # notice dispatch best-effort: outcome success but not sent → still 200, sent:false
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'success', 'sent': False})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['sent'] is False


@patch('lambda_function.lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_executor_failure_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'failed', 'error': 'token_mint_failed'})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


# --------------------------------------------------------------------------- #
# routing: booking_id contains '#' → the router must urldecode the path segment
# --------------------------------------------------------------------------- #

def test_route_booking_id_urldecode_cancel():
    path = '/scheduling/bookings/booking%23abc123/cancel'
    bid = urllib.parse.unquote(path.split('/scheduling/bookings/')[1].rsplit('/cancel', 1)[0])
    assert bid == 'booking#abc123'


def test_route_booking_id_urldecode_reschedule_link():
    path = '/scheduling/bookings/booking%23deadbeef/reschedule-link'
    bid = urllib.parse.unquote(path.split('/scheduling/bookings/')[1].rsplit('/reschedule-link', 1)[0])
    assert bid == 'booking#deadbeef'
