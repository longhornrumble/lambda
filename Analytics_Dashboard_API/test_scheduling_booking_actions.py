"""
Unit tests for the §G6/E12 booking ACTION endpoints:
  handle_scheduling_booking_cancel()           POST /scheduling/bookings/{id}/cancel
  handle_scheduling_booking_reschedule_link()  POST /scheduling/bookings/{id}/reschedule-link

ADA is the Clerk-authed ENTRY: feature-gate → load booking (GetItem on the tenant-scoped key) →
§8 permission (own-by-coordinator_email or admin) → terminal-status guard → proxy the side-effect
to BCH's scheduling_mutate executor (via the BOUNDED _booking_action_lambda_client). Coverage: the
security surface (feature gate; §8 own/admin/non-owner; enumeration-safe 404; terminal-status 409;
super_admin; null/old-shape rows), the validation surface (reason required/length/strip), the
executor outcomes (deleted / pending_calendar_sync / failure / FunctionError / rate_limited / invoke
ClientError), the GetItem error path, the payload shape, and the booking_id urldecode.
"""

import json
import urllib.parse
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

import lambda_function
from lambda_function import (
    handle_scheduling_booking_cancel,
    handle_scheduling_booking_reschedule_link,
    _authorize_booking_action,
)


def _ddb_booking(status='booked', coord='coord@x.com', include_coord=True):
    """A marshalled DDB Booking item as get_item returns it."""
    item = {
        'tenantId': {'S': 'TEN1'},
        'booking_id': {'S': 'booking#abc'},
        'external_event_id': {'S': 'evt1'},
        'start_at': {'S': '2026-07-01T15:00:00Z'},
        'attendee_email': {'S': 'guest@x.com'},
    }
    if include_coord:
        item['coordinator_email'] = {'S': coord}
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


def _client_error(op='Invoke'):
    return ClientError({'Error': {'Code': 'X'}}, op)


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
    assert _authorize_booking_action(bk, 'member', 'coord@x.com') is True  # case-insensitive own
    assert _authorize_booking_action(bk, 'member', 'other@x.com') is False  # non-owner denied
    assert _authorize_booking_action(bk, 'member', '') is False            # no email → denied


def test_authorize_none_role_falls_to_email_match():
    # A null role is NOT admin → must match coordinator_email (safe default).
    bk = {'coordinator_email': 'coord@x.com'}
    assert _authorize_booking_action(bk, None, 'coord@x.com') is True
    assert _authorize_booking_action(bk, None, 'other@x.com') is False


def test_authorize_old_shape_booking_without_coordinator_email_denies_non_admin():
    # An old-shape row missing coordinator_email → '' → non-admin denied; admin still allowed.
    bk = {}
    assert _authorize_booking_action(bk, 'member', 'coord@x.com') is False
    assert _authorize_booking_action(bk, 'admin', 'coord@x.com') is True


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
def test_cancel_staff_non_owner_404_not_403(mock_ddb):
    # Enumeration-safe: a non-owner gets 404 (indistinguishable from absent), NOT 403.
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='owner@x.com')}
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'member', 'intruder@x.com')
    assert resp['statusCode'] == 404


@patch('lambda_function.dynamodb')
def test_cancel_already_canceled_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(status='canceled')}
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 409


@patch('lambda_function.dynamodb')
def test_cancel_british_cancelled_spelling_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(status='cancelled')}
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 409


@patch('lambda_function.dynamodb')
def test_cancel_terminal_statuses_409(mock_ddb):
    # An attended/closed booking must not be re-cancelled (would corrupt the row).
    for st in ('completed', 'no_show', 'coordinator_no_show'):
        mock_ddb.get_item.return_value = {'Item': _ddb_booking(status=st)}
        resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
        assert resp['statusCode'] == 409, st
        assert json.loads(resp['body'])['status'] == st


# --------------------------------------------------------------------------- #
# cancel — executor outcomes + payload
# --------------------------------------------------------------------------- #

@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_staff_owner_success_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='coord@x.com')}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'deleted'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': '  Volunteer asked  '}, 'member', 'coord@x.com')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body']) == {'booking_id': 'booking#abc', 'status': 'canceled'}
    payload = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'].decode())
    assert payload['action'] == 'scheduling_mutate'
    assert payload['mutation'] == 'cancel'
    assert payload['tenantId'] == 'TEN1'
    assert payload['coordinatorId'] == 'coord@x.com'
    assert payload['bookingId'] == 'booking#abc'
    assert payload['reason'] == 'Volunteer asked'   # stripped
    assert payload['canceled_by'] == 'coord@x.com'


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_admin_any_success_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='someone@else.com')}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'deleted'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'admin@org.com')
    assert resp['statusCode'] == 200


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_pending_calendar_sync_202(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'pending_calendar_sync'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 202
    assert json.loads(resp['body'])['status'] == 'pending_calendar_sync'


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_executor_failed_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'failed', 'error': 'executor_error'})
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_function_error_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'errorMessage': 'boom'}, function_error='Unhandled')
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_cancel_invoke_clienterror_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.side_effect = _client_error('Invoke')
    resp = handle_scheduling_booking_cancel('TEN1', 'booking#abc', {'reason': 'x'}, 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function.dynamodb')
def test_cancel_getitem_error_502(mock_ddb):
    mock_ddb.get_item.side_effect = _client_error('GetItem')
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
def test_rl_staff_non_owner_404(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(coord='owner@x.com')}
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'member', 'intruder@x.com')
    assert resp['statusCode'] == 404


@patch('lambda_function.dynamodb')
def test_rl_canceled_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(status='canceled')}
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 409


@patch('lambda_function.dynamodb')
def test_rl_terminal_status_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking(status='completed')}
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 409


@patch('lambda_function.dynamodb')
def test_rl_getitem_error_502(mock_ddb):
    mock_ddb.get_item.side_effect = _client_error('GetItem')
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_success_sent_true_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'success', 'sent': True})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body']) == {'booking_id': 'booking#abc', 'sent': True}
    payload = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'].decode())
    assert payload['mutation'] == 'reschedule_link'
    assert payload['tenantId'] == 'TEN1'
    assert payload['coordinatorId'] == 'coord@x.com'
    assert payload['bookingId'] == 'booking#abc'
    assert payload['booking']['booking_id'] == 'booking#abc'


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_success_sent_false_200(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'success', 'sent': False})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['sent'] is False


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_rate_limited_429(mock_ddb, mock_lambda):
    # BCH refused a repeat send within the cooldown → 429 (distinct from a 502 error).
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'rate_limited'})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 429


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_executor_failure_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'outcome': 'failed', 'error': 'token_mint_failed'})
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_function_error_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.return_value = _invoke_resp({'errorMessage': 'boom'}, function_error='Unhandled')
    resp = handle_scheduling_booking_reschedule_link('TEN1', 'booking#abc', 'admin', 'a@x.com')
    assert resp['statusCode'] == 502


@patch('lambda_function._booking_action_lambda_client')
@patch('lambda_function.dynamodb')
def test_rl_invoke_clienterror_502(mock_ddb, mock_lambda):
    mock_ddb.get_item.return_value = {'Item': _ddb_booking()}
    mock_lambda.invoke.side_effect = _client_error('Invoke')
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
