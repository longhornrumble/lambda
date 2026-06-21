"""
Unit tests for the §E7 bookings read API: handle_scheduling_bookings().

Covers auth/scope gating (the security surface), the bounded GSI queries (no full
scans), cursor pagination, and the schema-discipline projection (old-shape rows).
"""

import json
import base64
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

import lambda_function
from lambda_function import handle_scheduling_bookings, _booking_projection


def _item(**over):
    """A full §A Booking DDB item; override individual attrs per test."""
    base = {
        'booking_id': {'S': 'BKG-1'},
        'tenantId': {'S': 'TEN1'},
        'status': {'S': 'BOOKED'},
        'start_at': {'S': '2026-06-10T18:00:00Z'},
        'end_at': {'S': '2026-06-10T18:30:00Z'},
        'coordinator_email': {'S': 'coord@example.com'},
        'resource_id': {'S': 'coord@example.com'},
        'appointment_type_id': {'S': 'intro'},
        'attendee_email': {'S': 'volunteer@example.com'},
        'attendee_name': {'S': 'Vol Unteer'},
        'attendee_phone': {'S': '+15125550123'},
        'created_at': {'S': '2026-06-01T12:00:00Z'},
        'last_calendar_mutation_at': {'S': '2026-06-01T12:00:00Z'},
        'html_link': {'S': 'https://cal/evt'},
    }
    base.update(over)
    return base


def _body(resp):
    assert resp['statusCode'] == 200, resp
    return json.loads(resp['body'])


# --------------------------------------------------------------------------- #
# Scope gating — the security surface
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_staff_self_queries_coordinator_index_with_own_lowercased_email(mock_ddb):
    mock_ddb.query.return_value = {'Items': [_item()]}
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'Coord@Example.com')
    body = _body(resp)
    assert len(body['bookings']) == 1
    kw = mock_ddb.query.call_args.kwargs
    assert kw['IndexName'] == 'tenantId-coordinator_email-index'
    assert kw['KeyConditionExpression'] == 'tenantId = :t AND coordinator_email = :email'
    assert kw['ExpressionAttributeValues'][':t'] == {'S': 'TEN1'}
    # email lowercased for the case-insensitive coordinator match
    assert kw['ExpressionAttributeValues'][':email'] == {'S': 'coord@example.com'}


@patch('lambda_function.dynamodb')
def test_staff_self_missing_email_returns_empty_not_error(mock_ddb):
    for email in (None, '', 'unknown'):
        resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', email)
        assert _body(resp)['bookings'] == []
    mock_ddb.query.assert_not_called()  # never query without a real identity


@patch('lambda_function.dynamodb')
def test_tenant_aggregate_admin_uses_bounded_start_index(mock_ddb):
    mock_ddb.query.return_value = {'Items': [_item()]}
    resp = handle_scheduling_bookings('TEN1', {'scope': 'tenant_aggregate'}, 'admin', 'admin@example.com')
    body = _body(resp)
    assert len(body['bookings']) == 1
    kw = mock_ddb.query.call_args.kwargs
    assert kw['IndexName'] == 'tenantId-start_at-index'
    # BOUNDED window — never a full scan
    assert 'BETWEEN :lo AND :hi' in kw['KeyConditionExpression']
    lo = kw['ExpressionAttributeValues'][':lo']['S']
    hi = kw['ExpressionAttributeValues'][':hi']['S']
    assert lo.endswith('Z') and hi.endswith('Z') and lo < hi  # matches stored '...Z' format, sortable


@patch('lambda_function.dynamodb')
def test_tenant_aggregate_super_admin_allowed(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    resp = handle_scheduling_bookings('TEN1', {'scope': 'tenant_aggregate'}, 'super_admin', 'sa@example.com')
    assert resp['statusCode'] == 200


@patch('lambda_function.dynamodb')
def test_tenant_aggregate_non_admin_is_forbidden(mock_ddb):
    resp = handle_scheduling_bookings('TEN1', {'scope': 'tenant_aggregate'}, 'member', 'm@example.com')
    assert resp['statusCode'] == 403
    mock_ddb.query.assert_not_called()  # blocked BEFORE any data access


@patch('lambda_function.dynamodb')
def test_invalid_scope_is_400(mock_ddb):
    resp = handle_scheduling_bookings('TEN1', {'scope': 'all_tenants'}, 'super_admin', 'x@example.com')
    assert resp['statusCode'] == 400
    mock_ddb.query.assert_not_called()


@patch('lambda_function.dynamodb')
def test_default_scope_is_staff_self(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_bookings('TEN1', {}, 'member', 'c@example.com')
    assert mock_ddb.query.call_args.kwargs['IndexName'] == 'tenantId-coordinator_email-index'


@patch('lambda_function.dynamodb')
def test_tenant_id_comes_from_session_never_a_param(mock_ddb):
    """Cross-tenant safety: the query :t is the authenticated tenant_id, even if a
    rogue 'tenant_id' query param is supplied."""
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'tenant_id': 'OTHER'}, 'member', 'c@example.com')
    assert mock_ddb.query.call_args.kwargs['ExpressionAttributeValues'][':t'] == {'S': 'TEN1'}


# --------------------------------------------------------------------------- #
# Projection — schema discipline (PII surface)
# --------------------------------------------------------------------------- #

def test_projection_full_item_nests_attendee():
    p = _booking_projection(_item())
    assert p['booking_id'] == 'BKG-1'
    assert p['status'] == 'BOOKED'
    assert p['attendee'] == {'name': 'Vol Unteer', 'email': 'volunteer@example.com', 'phone': '+15125550123'}
    assert p['html_link'] == 'https://cal/evt'


def test_projection_old_shape_tolerates_missing_fields():
    """Old rows lack attendee_name/phone, html_link, last_calendar_mutation_at — must
    yield None, never KeyError (forward-compatible reads)."""
    skinny = {
        'booking_id': {'S': 'BKG-2'},
        'tenantId': {'S': 'TEN1'},
        'status': {'S': 'BOOKED'},
        'start_at': {'S': '2026-06-10T18:00:00Z'},
        'coordinator_email': {'S': 'coord@example.com'},
        'attendee_email': {'S': 'v@example.com'},
    }
    p = _booking_projection(skinny)
    assert p['attendee'] == {'name': None, 'email': 'v@example.com', 'phone': None}
    assert p['html_link'] is None
    assert p['last_calendar_mutation_at'] is None
    assert p['end_at'] is None


@patch('lambda_function.dynamodb')
def test_response_only_returns_projected_fields(mock_ddb):
    """PII minimization: the client gets the §E7 projection, not raw extra attrs."""
    raw = _item(internal_secret={'S': 'do-not-leak'})
    mock_ddb.query.return_value = {'Items': [raw]}
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'coord@example.com'))
    assert 'internal_secret' not in body['bookings'][0]


# --------------------------------------------------------------------------- #
# Pagination + failure paths
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_pagination_emits_next_cursor_and_consumes_it(mock_ddb):
    lek = {'tenantId': {'S': 'TEN1'}, 'booking_id': {'S': 'BKG-1'}}
    mock_ddb.query.return_value = {'Items': [_item()], 'LastEvaluatedKey': lek}
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'coord@example.com'))
    cursor = body['nextCursor']
    # opaque cursor round-trips to the LastEvaluatedKey
    assert json.loads(base64.b64decode(cursor).decode()) == lek
    # and a follow-up request feeds it back as ExclusiveStartKey
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'cursor': cursor}, 'member', 'coord@example.com')
    assert mock_ddb.query.call_args.kwargs['ExclusiveStartKey'] == lek


@patch('lambda_function.dynamodb')
def test_no_next_cursor_when_no_more_pages(mock_ddb):
    mock_ddb.query.return_value = {'Items': [_item()]}  # no LastEvaluatedKey
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'coord@example.com'))
    assert 'nextCursor' not in body


@patch('lambda_function.dynamodb')
def test_invalid_cursor_is_400(mock_ddb):
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'cursor': 'not-base64!!'}, 'member', 'coord@example.com')
    assert resp['statusCode'] == 400
    mock_ddb.query.assert_not_called()


@patch('lambda_function.dynamodb')
def test_ddb_clienterror_is_502_not_500(mock_ddb):
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'ProvisionedThroughputExceeded'}}, 'Query')
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'coord@example.com')
    assert resp['statusCode'] == 502


# --------------------------------------------------------------------------- #
# Audit-hardening: page-size cap, cursor tenant-binding, role=None
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_query_sets_a_limit(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'c@example.com')
    assert mock_ddb.query.call_args.kwargs['Limit'] == lambda_function.BOOKING_PAGE_SIZE


@patch('lambda_function.dynamodb')
def test_page_size_is_capped(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'page_size': '99999'}, 'member', 'c@example.com')
    assert mock_ddb.query.call_args.kwargs['Limit'] == lambda_function.BOOKING_PAGE_SIZE_MAX


@patch('lambda_function.dynamodb')
def test_invalid_page_size_is_400(mock_ddb):
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'page_size': 'lots'}, 'member', 'c@example.com')
    assert resp['statusCode'] == 400
    mock_ddb.query.assert_not_called()


def _cursor(d):
    return base64.b64encode(json.dumps(d).encode()).decode()


@patch('lambda_function.dynamodb')
def test_cursor_pointing_at_another_tenant_is_rejected(mock_ddb):
    """A crafted cursor carrying a DIFFERENT tenantId must be refused (no cross-tenant seek)."""
    bad = _cursor({'tenantId': {'S': 'OTHER'}, 'booking_id': {'S': 'X'}, 'coordinator_email': {'S': 'c@example.com'}})
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'cursor': bad}, 'member', 'c@example.com')
    assert resp['statusCode'] == 400
    mock_ddb.query.assert_not_called()


@patch('lambda_function.dynamodb')
def test_cursor_with_unexpected_keys_is_rejected(mock_ddb):
    bad = _cursor({'tenantId': {'S': 'TEN1'}, 'booking_id': {'S': 'X'}, 'evil': {'S': 'inject'}})
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'cursor': bad}, 'member', 'c@example.com')
    assert resp['statusCode'] == 400
    mock_ddb.query.assert_not_called()


@patch('lambda_function.dynamodb')
def test_valid_same_tenant_cursor_is_accepted(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    good = _cursor({'tenantId': {'S': 'TEN1'}, 'booking_id': {'S': 'X'}, 'coordinator_email': {'S': 'c@example.com'}})
    resp = handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'cursor': good}, 'member', 'c@example.com')
    assert resp['statusCode'] == 200
    assert mock_ddb.query.call_args.kwargs['ExclusiveStartKey']['tenantId'] == {'S': 'TEN1'}


@patch('lambda_function.dynamodb')
def test_tenant_aggregate_role_none_is_forbidden(mock_ddb):
    resp = handle_scheduling_bookings('TEN1', {'scope': 'tenant_aggregate'}, None, 'x@example.com')
    assert resp['statusCode'] == 403
    mock_ddb.query.assert_not_called()


@patch('lambda_function.dynamodb')
def test_projection_handles_fractional_z_start_at(mock_ddb):
    """BCH writes start_at as '...:00.000Z'; the bare-Z BETWEEN bounds must still bracket it."""
    item = _item(start_at={'S': '2026-06-10T18:00:00.000Z'})
    mock_ddb.query.return_value = {'Items': [item]}
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'tenant_aggregate'}, 'admin', 'a@example.com'))
    assert body['bookings'][0]['start_at'] == '2026-06-10T18:00:00.000Z'


# --------------------------------------------------------------------------- #
# De-noise: synthetic-monitor canary exclusion (FilterExpression + accumulation)
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_query_carries_synthetic_exclusion_filter(mock_ddb):
    """Every §E7 query excludes is_synthetic canary rows server-side (real rows lack the attr)."""
    mock_ddb.query.return_value = {'Items': [_item()]}
    handle_scheduling_bookings('TEN1', {'scope': 'tenant_aggregate'}, 'admin', 'a@example.com')
    kw = mock_ddb.query.call_args.kwargs
    assert 'attribute_not_exists(is_synthetic)' in kw['FilterExpression']
    assert kw['ExpressionAttributeValues'][':synfalse'] == {'BOOL': False}


@patch('lambda_function.dynamodb')
def test_accumulates_across_sparse_pages_until_full(mock_ddb):
    """A FilterExpression is applied AFTER Limit, so DDB can return a sparse page + a cursor.
    The handler follows pages until it has a full page of real rows."""
    lek = {'tenantId': {'S': 'TEN1'}, 'booking_id': {'S': 'X'}, 'coordinator_email': {'S': 'c@example.com'}}
    mock_ddb.query.side_effect = [
        {'Items': [_item(booking_id={'S': f'A{i}'}) for i in range(20)], 'LastEvaluatedKey': lek},
        {'Items': [_item(booking_id={'S': f'B{i}'}) for i in range(20)], 'LastEvaluatedKey': lek},
        {'Items': [_item(booking_id={'S': f'C{i}'}) for i in range(20)]},  # no LEK
    ]
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'staff_self', 'page_size': '50'}, 'member', 'c@example.com'))
    assert mock_ddb.query.call_count == 3   # 20+20 < 50 -> a third page
    assert len(body['bookings']) == 50      # sliced to page_size


@patch('lambda_function.dynamodb')
def test_accumulation_stops_when_window_exhausted(mock_ddb):
    """Window fully scanned before page_size real rows -> return what we have, no cursor."""
    lek = {'tenantId': {'S': 'TEN1'}, 'booking_id': {'S': 'A1'}, 'coordinator_email': {'S': 'c@example.com'}}
    mock_ddb.query.side_effect = [
        {'Items': [_item(booking_id={'S': 'A1'})], 'LastEvaluatedKey': lek},
        {'Items': [_item(booking_id={'S': 'A2'})]},  # no LEK -> exhausted
    ]
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'c@example.com'))
    assert mock_ddb.query.call_count == 2
    assert len(body['bookings']) == 2
    assert 'nextCursor' not in body


@patch('lambda_function.dynamodb')
def test_accumulation_respects_page_cap(mock_ddb):
    """An all-synthetic window (every page filtered to empty but still cursored) stops at the
    cap and emits a resume cursor rather than looping unbounded."""
    lek = {'tenantId': {'S': 'TEN1'}, 'booking_id': {'S': 'x'}, 'coordinator_email': {'S': 'c@example.com'}}
    mock_ddb.query.return_value = {'Items': [], 'LastEvaluatedKey': lek}
    body = _body(handle_scheduling_bookings('TEN1', {'scope': 'staff_self'}, 'member', 'c@example.com'))
    assert mock_ddb.query.call_count == lambda_function._SCHED_BOOKINGS_MAX_PAGES
    assert body['bookings'] == []
    assert body['nextCursor']  # client can resume past the scanned window
