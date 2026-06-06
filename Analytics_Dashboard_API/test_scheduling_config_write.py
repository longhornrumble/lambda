"""
Unit tests for the §E13b AppointmentType/RoutingPolicy write API + vocab-validation.

Covers the security surface (admin-only write), vocab-validation (FAIL-CLOSED on
unknown tags), the optimistic lock (If-Match), the FK integrity check, and that
PATCH uses UpdateItem (so the commit-owned round-robin state is never wiped).
"""

import json
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

import lambda_function as lf
from lambda_function import (
    _validate_tag_conditions,
    handle_scheduling_appointment_type_write,
    handle_scheduling_routing_policy_write,
    handle_scheduling_appointment_types_get,
    handle_scheduling_routing_policies_get,
)

TENANT = 'TEN1'
ADMIN = 'admin'
EMAIL = 'admin@example.com'


def _conditional_failed():
    return ClientError(
        {'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'x'}}, 'PutItem'
    )


# --------------------------------------------------------------------------- #
# vocab-validation (pure) — FAIL-CLOSED
# --------------------------------------------------------------------------- #

def test_vocab_none_and_empty_are_solo():
    assert _validate_tag_conditions(None, ['mentoring']) == ([], None)
    assert _validate_tag_conditions([], ['mentoring']) == ([], None)


def test_vocab_known_tag_passes_and_defaults_operator():
    norm, err = _validate_tag_conditions([{'values': ['mentoring']}], ['mentoring', 'esl'])
    assert err is None
    assert norm == [{'operator': 'equals', 'values': ['mentoring']}]


def test_vocab_unknown_tag_fails_closed_422():
    norm, err = _validate_tag_conditions([{'operator': 'in_any', 'values': ['typo']}], ['mentoring'])
    assert norm is None
    assert err['statusCode'] == 422
    assert json.loads(err['body'])['unknownTags'] == ['typo']


def test_vocab_empty_vocabulary_rejects_any_tag():
    _, err = _validate_tag_conditions([{'values': ['mentoring']}], [])
    assert err['statusCode'] == 422


def test_vocab_bad_shapes_400():
    assert _validate_tag_conditions('nope', ['m'])[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'operator': 'bogus', 'values': ['m']}], ['m'])[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'values': []}], ['m'])[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'values': [1]}], ['m'])[1]['statusCode'] == 400


# --------------------------------------------------------------------------- #
# Auth — write is admin-only
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize('role', ['member', None, 'viewer'])
def test_write_requires_admin(role):
    r1 = handle_scheduling_appointment_type_write(TENANT, None, {}, role, EMAIL, None)
    r2 = handle_scheduling_routing_policy_write(TENANT, None, {}, role, EMAIL, None)
    assert r1['statusCode'] == 403 and r2['statusCode'] == 403


# --------------------------------------------------------------------------- #
# RoutingPolicy create
# --------------------------------------------------------------------------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_create_solo_201(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'tie_breaker': 'round_robin', 'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    pol = json.loads(resp['body'])['routing_policy']
    assert pol['tie_breaker'] == 'round_robin'
    assert pol['tag_conditions'] == []
    assert pol['routing_policy_id'].startswith('rp_')
    assert pol['modified_at']['by'] == EMAIL
    # PutItem with attribute_not_exists guard
    kw = mock_ddb.put_item.call_args.kwargs
    assert kw['TableName'] == lf.ROUTING_POLICY_TABLE
    assert 'attribute_not_exists(routing_policy_id)' in kw['ConditionExpression']


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_create_tagged_builds_runtime_shape(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'tag_conditions': [{'operator': 'in_any', 'values': ['mentoring']}]}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    pol = json.loads(resp['body'])['routing_policy']
    assert pol['tag_conditions'] == [{'operator': 'in_any', 'values': ['mentoring']}]


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_create_unknown_tag_422_no_write(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'tag_conditions': [{'values': ['nope']}]}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 422
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.get_tenant_config', return_value={})
@patch('lambda_function.dynamodb')
def test_routing_policy_bad_tie_breaker_400(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(TENANT, None, {'tie_breaker': 'random'}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_create_dup_id_409(mock_ddb, _cfg):
    mock_ddb.put_item.side_effect = _conditional_failed()
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'routing_policy_id': 'rp_x', 'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 409


# --------------------------------------------------------------------------- #
# RoutingPolicy update — UpdateItem, optimistic lock, RR state preserved
# --------------------------------------------------------------------------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_uses_updateitem_preserving_rr_state(mock_ddb, _cfg):
    mock_ddb.update_item.return_value = {'Attributes': {
        'routing_policy_id': {'S': 'rp_x'}, 'tenantId': {'S': TENANT},
        'tie_breaker': {'S': 'first_available'}, 'tag_conditions': {'L': []},
        'last_assigned_resource_id': {'S': 'res_maya'},  # commit-owned, untouched
        'modified_at': {'M': {'at': {'S': '2026-06-06T00:00:01Z'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x', {'tie_breaker': 'first_available', 'tag_conditions': []},
        ADMIN, EMAIL, '2026-06-06T00:00:00Z')
    assert resp['statusCode'] == 200
    mock_ddb.put_item.assert_not_called()          # NOT a full-replace
    kw = mock_ddb.update_item.call_args.kwargs
    assert kw['UpdateExpression'].startswith('SET ')
    # last_assigned_* must NOT appear in the SET expression -> RR state preserved
    assert 'last_assigned' not in json.dumps({k: str(v) for k, v in kw['ExpressionAttributeNames'].items()})
    assert kw['ConditionExpression'] == 'attribute_exists(#pk) AND attribute_exists(#mod) AND #mod.#at = :ifmatch'
    assert kw['ExpressionAttributeValues'][':ifmatch'] == {'S': '2026-06-06T00:00:00Z'}
    body = json.loads(resp['body'])['routing_policy']
    assert body['last_assigned_resource_id'] == 'res_maya'  # survived


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_missing_if_match_428(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(TENANT, 'rp_x', {'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 428
    mock_ddb.update_item.assert_not_called()


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_stale_if_match_409(mock_ddb, _cfg):
    mock_ddb.update_item.side_effect = _conditional_failed()
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x', {'tag_conditions': []}, ADMIN, EMAIL, 'stale')
    assert resp['statusCode'] == 409


# --------------------------------------------------------------------------- #
# AppointmentType create/update — FK + field validation
# --------------------------------------------------------------------------- #

def _at_body(**over):
    base = {'name': 'Intro Call', 'duration_minutes': 30, 'routing_policy_id': 'rp_x'}
    base.update(over)
    return base


@patch('lambda_function.dynamodb')
def test_appointment_type_create_201_with_fk_ok(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}  # FK exists
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    at = json.loads(resp['body'])['appointment_type']
    assert at['appointment_type_id'].startswith('at_')
    assert at['duration_minutes'] == 30
    assert at['buffer_before_minutes'] == 0 and at['lead_time_minutes'] == 0  # defaults
    kw = mock_ddb.put_item.call_args.kwargs
    assert 'attribute_not_exists(appointment_type_id)' in kw['ConditionExpression']


@patch('lambda_function.dynamodb')
def test_appointment_type_fk_missing_422(mock_ddb):
    mock_ddb.get_item.return_value = {}  # FK does not exist
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 422
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_appointment_type_field_validation(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    assert handle_scheduling_appointment_type_write(TENANT, None, _at_body(name=''), ADMIN, EMAIL, None)['statusCode'] == 400
    assert handle_scheduling_appointment_type_write(TENANT, None, _at_body(duration_minutes=0), ADMIN, EMAIL, None)['statusCode'] == 400
    assert handle_scheduling_appointment_type_write(TENANT, None, _at_body(duration_minutes=999), ADMIN, EMAIL, None)['statusCode'] == 400
    assert handle_scheduling_appointment_type_write(TENANT, None, _at_body(buffer_before_minutes=-1), ADMIN, EMAIL, None)['statusCode'] == 400
    no_fk = {k: v for k, v in _at_body().items() if k != 'routing_policy_id'}
    assert handle_scheduling_appointment_type_write(TENANT, None, no_fk, ADMIN, EMAIL, None)['statusCode'] == 400


@patch('lambda_function.dynamodb')
def test_appointment_type_update_uses_updateitem(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    mock_ddb.update_item.return_value = {'Attributes': {
        'appointment_type_id': {'S': 'at_1'}, 'tenantId': {'S': TENANT},
        'name': {'S': 'Intro Call'}, 'duration_minutes': {'N': '45'},
        'routing_policy_id': {'S': 'rp_x'},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_appointment_type_write(
        TENANT, 'at_1', _at_body(duration_minutes=45), ADMIN, EMAIL, '2026-06-06T00:00:00Z')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['appointment_type']['duration_minutes'] == 45
    mock_ddb.put_item.assert_not_called()


# --------------------------------------------------------------------------- #
# GET list handlers
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_get_appointment_types_lists_unmarshalled(mock_ddb):
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT}, 'appointment_type_id': {'S': 'at_1'},
        'name': {'S': 'Intro'}, 'duration_minutes': {'N': '30'}, 'routing_policy_id': {'S': 'rp_x'},
    }]}
    resp = handle_scheduling_appointment_types_get(TENANT, ADMIN)
    assert resp['statusCode'] == 200
    rows = json.loads(resp['body'])['appointment_types']
    assert rows[0]['duration_minutes'] == 30  # Decimal -> int
    kw = mock_ddb.query.call_args.kwargs
    assert kw['KeyConditionExpression'] == 'tenantId = :t'
    assert kw['Limit'] == 500  # B3: bounded read


@patch('lambda_function.dynamodb')
def test_get_routing_policies_query_failure_502(mock_ddb):
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'X', 'Message': 'y'}}, 'Query')
    resp = handle_scheduling_routing_policies_get(TENANT, ADMIN)
    assert resp['statusCode'] == 502


# =========================================================================== #
# phase-completion-audit fixes (2026-06-06): B1/B2/B3 + S1/S2/S3/S4 + gaps
# =========================================================================== #

# --- GAP-1: super_admin is allowed to write (not just 'admin') --------------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': []}})
@patch('lambda_function.dynamodb')
def test_routing_policy_write_allows_super_admin(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(TENANT, None, {'tag_conditions': []}, 'super_admin', EMAIL, None)
    assert resp['statusCode'] == 201


@patch('lambda_function.dynamodb')
def test_appointment_type_write_allows_super_admin(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), 'super_admin', EMAIL, None)
    assert resp['statusCode'] == 201


# --- B2: GET endpoints are admin-gated --------------------------------------- #

@pytest.mark.parametrize('role', ['member', None, 'viewer'])
@patch('lambda_function.dynamodb')
def test_get_endpoints_require_admin(mock_ddb, role):
    assert handle_scheduling_appointment_types_get(TENANT, role)['statusCode'] == 403
    assert handle_scheduling_routing_policies_get(TENANT, role)['statusCode'] == 403
    mock_ddb.query.assert_not_called()


# --- B3 / GAP-8: GET empty list + Limit -------------------------------------- #

@patch('lambda_function.dynamodb')
def test_get_routing_policies_empty_list_200(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    resp = handle_scheduling_routing_policies_get(TENANT, ADMIN)
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['routing_policies'] == []
    assert mock_ddb.query.call_args.kwargs['Limit'] == 500


# --- GAP-7: GET appointment-types DDB error 502 ------------------------------ #

@patch('lambda_function.dynamodb')
def test_get_appointment_types_query_failure_502(mock_ddb):
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'X', 'Message': 'y'}}, 'Query')
    assert handle_scheduling_appointment_types_get(TENANT, ADMIN)['statusCode'] == 502


# --- GAP-5: nested tag_conditions DDB round-trip (the _native recursion) ----- #

@patch('lambda_function.dynamodb')
def test_get_routing_policies_unmarshals_nested_tag_conditions(mock_ddb):
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT}, 'routing_policy_id': {'S': 'rp_1'},
        'tie_breaker': {'S': 'round_robin'},
        'tag_conditions': {'L': [{'M': {'operator': {'S': 'in_any'},
                                        'values': {'L': [{'S': 'mentoring'}, {'S': 'esl'}]}}}]},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }]}
    resp = handle_scheduling_routing_policies_get(TENANT, ADMIN)
    pol = json.loads(resp['body'])['routing_policies'][0]
    assert pol['tag_conditions'] == [{'operator': 'in_any', 'values': ['mentoring', 'esl']}]


# --- S6 / GAP: AT create asserts the DDB Item carries routing_policy_id ------- #

@patch('lambda_function.dynamodb')
def test_appointment_type_create_item_carries_fk_and_keys(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    handle_scheduling_appointment_type_write(TENANT, None, _at_body(), ADMIN, EMAIL, None)
    item = mock_ddb.put_item.call_args.kwargs['Item']
    assert item['tenantId'] == {'S': TENANT}
    assert item['routing_policy_id'] == {'S': 'rp_x'}
    assert item['appointment_type_id']['S'].startswith('at_')
    assert item['duration_minutes'] == {'N': '30'}


# --- GAP-2: FK GetItem DDB error -> 502 -------------------------------------- #

@patch('lambda_function.dynamodb')
def test_appointment_type_fk_check_ddb_error_502(mock_ddb):
    mock_ddb.get_item.side_effect = ClientError({'Error': {'Code': 'X', 'Message': 'y'}}, 'GetItem')
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 502
    mock_ddb.put_item.assert_not_called()


# --- GAP-6: AT PATCH re-validates the FK (422 when the new policy is missing) - #

@patch('lambda_function.dynamodb')
def test_appointment_type_update_revalidates_fk_422(mock_ddb):
    mock_ddb.get_item.return_value = {}  # FK does not exist
    resp = handle_scheduling_appointment_type_write(
        TENANT, 'at_1', _at_body(routing_policy_id='rp_gone'), ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 422
    mock_ddb.update_item.assert_not_called()


# --- GAP-3 / GAP-4: AT update missing / stale If-Match ----------------------- #

@patch('lambda_function.dynamodb')
def test_appointment_type_update_missing_if_match_428(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(TENANT, 'at_1', _at_body(), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 428
    mock_ddb.update_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_appointment_type_update_stale_if_match_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    mock_ddb.update_item.side_effect = _conditional_failed()
    resp = handle_scheduling_appointment_type_write(TENANT, 'at_1', _at_body(), ADMIN, EMAIL, 'stale')
    assert resp['statusCode'] == 409


# --- S1: body expected_modified_at is accepted in lieu of the If-Match header  #

@patch('lambda_function.dynamodb')
def test_appointment_type_update_body_expected_modified_at(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    mock_ddb.update_item.return_value = {'Attributes': {
        'appointment_type_id': {'S': 'at_1'}, 'tenantId': {'S': TENANT},
        'name': {'S': 'Intro Call'}, 'duration_minutes': {'N': '30'}, 'routing_policy_id': {'S': 'rp_x'},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }}
    body = _at_body()
    body['expected_modified_at'] = '2026-06-06T00:00:00.000000Z'
    resp = handle_scheduling_appointment_type_write(TENANT, 'at_1', body, ADMIN, EMAIL, None)  # no header
    assert resp['statusCode'] == 200
    assert mock_ddb.update_item.call_args.kwargs['ExpressionAttributeValues'][':ifmatch'] == \
        {'S': '2026-06-06T00:00:00.000000Z'}


# --- B1: first edit of a LEGACY row (no modified_at) via If-Match "*" --------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': []}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_legacy_row_sentinel(mock_ddb, _cfg):
    mock_ddb.update_item.return_value = {'Attributes': {
        'routing_policy_id': {'S': 'rp_fixture'}, 'tenantId': {'S': TENANT},
        'tie_breaker': {'S': 'round_robin'}, 'tag_conditions': {'L': []},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_routing_policy_write(TENANT, 'rp_fixture', {'tag_conditions': []}, ADMIN, EMAIL, '*')
    assert resp['statusCode'] == 200
    kw = mock_ddb.update_item.call_args.kwargs
    assert kw['ConditionExpression'] == 'attribute_exists(#pk) AND attribute_not_exists(#mod)'
    assert ':ifmatch' not in kw['ExpressionAttributeValues']  # no token compared for first-stamp


# --- S3: body routing_policy_id (FK) is regex-validated ---------------------- #

@patch('lambda_function.dynamodb')
def test_appointment_type_bad_fk_format_400_no_ddb(mock_ddb):
    resp = handle_scheduling_appointment_type_write(
        TENANT, None, _at_body(routing_policy_id='bad id!'), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.get_item.assert_not_called()  # rejected before the FK round-trip


# --- S4: name length cap ----------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_appointment_type_name_too_long_400(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(name='x' * 201), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400


# --- GAP-9 / GAP-10: caller-supplied invalid id -> 400 ----------------------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': []}})
@patch('lambda_function.dynamodb')
def test_routing_policy_create_invalid_supplied_id_400(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'routing_policy_id': 'bad id!', 'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.put_item.assert_not_called()


# --- GAP-11: bool inputs rejected (bool is an int subclass) ------------------ #

@patch('lambda_function.dynamodb')
def test_appointment_type_bool_fields_rejected_400(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    assert handle_scheduling_appointment_type_write(TENANT, None, _at_body(duration_minutes=True), ADMIN, EMAIL, None)['statusCode'] == 400
    assert handle_scheduling_appointment_type_write(TENANT, None, _at_body(buffer_before_minutes=False), ADMIN, EMAIL, None)['statusCode'] == 400


# --- GAP-12 / GAP-13: non-conditional DDB error -> 502 ----------------------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': []}})
@patch('lambda_function.dynamodb')
def test_routing_policy_create_ddb_error_502(mock_ddb, _cfg):
    mock_ddb.put_item.side_effect = ClientError(
        {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'x'}}, 'PutItem')
    resp = handle_scheduling_routing_policy_write(TENANT, None, {'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 502


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': []}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_ddb_error_502(mock_ddb, _cfg):
    mock_ddb.update_item.side_effect = ClientError(
        {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'x'}}, 'UpdateItem')
    resp = handle_scheduling_routing_policy_write(TENANT, 'rp_x', {'tag_conditions': []}, ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 502


# --- GAP-14: mixed known/unknown tags across conditions reports all unknowns -- #

def test_vocab_mixed_known_unknown_reports_unknown():
    _, err = _validate_tag_conditions(
        [{'values': ['mentoring']}, {'operator': 'in_any', 'values': ['typo1', 'typo2']}], ['mentoring'])
    assert err['statusCode'] == 422
    assert json.loads(err['body'])['unknownTags'] == ['typo1', 'typo2']


# --- GAP-15: get_tag_vocabulary filters non-string entries ------------------- #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': [1, 'mentoring', None, 'esl']}})
def test_get_tag_vocabulary_filters_non_strings(_cfg):
    assert lf.get_tag_vocabulary(TENANT) == ['mentoring', 'esl']


# --- GAP-16: non-dict body -> 400 ------------------------------------------- #

def test_write_handlers_reject_non_dict_body():
    assert handle_scheduling_appointment_type_write(TENANT, None, 'nope', ADMIN, EMAIL, None)['statusCode'] == 400
    assert handle_scheduling_routing_policy_write(TENANT, None, ['nope'], ADMIN, EMAIL, None)['statusCode'] == 400


# --- GAP-17: RP update with an unknown tagged condition -> 422, no write ------ #

@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_unknown_tag_422_no_write(mock_ddb, _cfg):
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x', {'tag_conditions': [{'values': ['typo']}]}, ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 422
    mock_ddb.update_item.assert_not_called()
