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
    handle_scheduling_appointment_type_delete,
    handle_scheduling_routing_policy_write,
    handle_scheduling_appointment_types_get,
    handle_scheduling_routing_policies_get,
    handle_scheduling_programs_get,
    get_programs,
)

TENANT = 'TEN1'
ADMIN = 'admin'
EMAIL = 'admin@example.com'

# The program the appointment-type FK check must find in config.programs. Appointment-type
# write tests patch get_programs to return this so the program_id FK passes.
_PROGRAMS = [{'program_id': 'prog_x', 'program_name': 'Program X'}]


@pytest.fixture(autouse=True)
def _default_config_with_programs():
    """Appointment-type writes now FK-check `program_id` against config.programs. Default the
    config read so write tests that don't care about programs still pass the FK. Tests that
    assert on the config read (the /scheduling/programs projection) or on program validation
    override get_tenant_config / get_programs with their own @patch, which wins in the body."""
    with patch('lambda_function.get_tenant_config',
               return_value={'programs': {'prog_x': {'program_id': 'prog_x',
                                                      'program_name': 'Program X'}}}):
        yield


def _conditional_failed(op='PutItem'):
    return ClientError(
        {'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'x'}}, op
    )


def _rp_row(rp_id, names, tie_breaker='round_robin', stamped='2026-06-29T00:00:00Z'):
    """A marshalled routing-policy (Team) row. names=[] → the 'Everyone' team. `stamped`=None
    leaves it unstamped (legacy, If-Match '*')."""
    item = {
        'tenantId': {'S': TENANT}, 'routing_policy_id': {'S': rp_id},
        'tie_breaker': {'S': tie_breaker},
        'tag_conditions': {'L': ([{'M': {'operator': {'S': 'in_any'},
                                         'values': {'L': [{'S': n} for n in names]}}}] if names else [])},
    }
    if stamped:
        item['modified_at'] = {'M': {'at': {'S': stamped}, 'by': {'S': EMAIL}}}
    return item


# --------------------------------------------------------------------------- #
# tag_conditions validation (pure, SHAPE-ONLY post-unification — no membership)
# --------------------------------------------------------------------------- #

def test_tag_conditions_none_and_empty_are_everyone():
    assert _validate_tag_conditions(None) == ([], None)
    assert _validate_tag_conditions([]) == ([], None)


def test_tag_conditions_pass_and_default_operator_and_trim():
    norm, err = _validate_tag_conditions([{'values': ['  Mentors  ']}])
    assert err is None
    assert norm == [{'operator': 'equals', 'values': ['Mentors']}]  # default operator + trimmed


def test_tag_conditions_any_name_is_accepted_now():
    # Post-unification there is NO closed-vocabulary check — a brand-new name is authored here.
    norm, err = _validate_tag_conditions([{'operator': 'in_any', 'values': ['Brand New Team']}])
    assert err is None
    assert norm == [{'operator': 'in_any', 'values': ['Brand New Team']}]


def test_tag_conditions_bad_shapes_400():
    assert _validate_tag_conditions('nope')[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'operator': 'bogus', 'values': ['m']}])[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'values': []}])[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'values': [1]}])[1]['statusCode'] == 400
    assert _validate_tag_conditions([{'values': ['   ']}])[1]['statusCode'] == 400      # blank after trim
    assert _validate_tag_conditions([{'values': ['x' * 51]}])[1]['statusCode'] == 400   # over char cap


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


@patch('lambda_function.get_tenant_config', return_value={'scheduling': {'scheduling_tag_vocabulary': ['mentoring']}})
@patch('lambda_function.dynamodb')
def test_routing_policy_update_star_if_match_no_unused_names(mock_ddb, _cfg):
    """First edit of a legacy/fixture row (If-Match '*') takes the attribute_not_exists
    branch, which must NOT leave an unused '#at' in ExpressionAttributeNames. Regression:
    DynamoDB 502'd these with 'unused in expressions: keys: {#at}' (only mocked tests ran,
    so the real ValidationException never surfaced)."""
    mock_ddb.update_item.return_value = {'Attributes': {
        'routing_policy_id': {'S': 'rp_x'}, 'tenantId': {'S': TENANT},
        'tie_breaker': {'S': 'first_available'}, 'tag_conditions': {'L': []},
        'modified_at': {'M': {'at': {'S': '2026-06-29T00:00:00Z'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x', {'tie_breaker': 'first_available', 'tag_conditions': []},
        ADMIN, EMAIL, '*')
    assert resp['statusCode'] == 200
    kw = mock_ddb.update_item.call_args.kwargs
    assert kw['ConditionExpression'] == 'attribute_exists(#pk) AND attribute_not_exists(#mod)'
    assert '#at' not in kw['ExpressionAttributeNames']
    # General invariant: every declared ExpressionAttributeName must be referenced —
    # DynamoDB rejects an UpdateItem with any unused key.
    expr = kw['UpdateExpression'] + ' ' + kw['ConditionExpression']
    for placeholder in kw['ExpressionAttributeNames']:
        assert placeholder in expr, f'unused ExpressionAttributeName: {placeholder}'


# --------------------------------------------------------------------------- #
# RoutingPolicy program binding (§1 "Who handles bookings") — program_id + bookable
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_routing_policy_create_with_program_binding_201(mock_ddb):
    """Make a program bookable = create its team bound to program_id; defaults bookable=True."""
    resp = handle_scheduling_routing_policy_write(
        TENANT, None,
        {'program_id': 'prog_x', 'tie_breaker': 'round_robin',
         'tag_conditions': [{'operator': 'in_any', 'values': ['Volunteer Coordinators']}]},
        ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    pol = json.loads(resp['body'])['routing_policy']
    assert pol['program_id'] == 'prog_x'
    assert pol['bookable'] is True


@patch('lambda_function.dynamodb')
def test_routing_policy_unknown_program_422(mock_ddb):
    """program_id must be a real config.programs entry (shared-key guarantee)."""
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'program_id': 'ghost', 'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 422
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_routing_policy_program_already_bound_409(mock_ddb):
    """program<->team is 1:1 — a program already bound to another team can't be re-bound."""
    other = _rp_row('rp_other', ['Other Team'])
    other['program_id'] = {'S': 'prog_x'}
    mock_ddb.query.return_value = {'Items': [other]}
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'program_id': 'prog_x', 'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 409
    assert json.loads(resp['body'])['program_id'] == 'prog_x'
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_routing_policy_bad_bookable_400(mock_ddb):
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'program_id': 'prog_x', 'bookable': 'yes', 'tag_conditions': []},
        ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_routing_policy_unpublish_sets_bookable_false(mock_ddb):
    """Stop making bookable = PATCH bookable=false (non-destructive; program_id preserved via SET)."""
    mock_ddb.update_item.return_value = {'Attributes': {
        'routing_policy_id': {'S': 'rp_x'}, 'tenantId': {'S': TENANT},
        'program_id': {'S': 'prog_x'}, 'bookable': {'BOOL': False},
        'tie_breaker': {'S': 'round_robin'}, 'tag_conditions': {'L': []},
        'modified_at': {'M': {'at': {'S': '2026-07-01T00:00:01Z'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x',
        {'bookable': False, 'tie_breaker': 'round_robin',
         'tag_conditions': [{'operator': 'in_any', 'values': ['Volunteer Coordinators']}]},
        ADMIN, EMAIL, '2026-07-01T00:00:00Z')
    assert resp['statusCode'] == 200
    kw = mock_ddb.update_item.call_args.kwargs
    assert {'BOOL': False} in kw['ExpressionAttributeValues'].values()  # bookable=false in SET
    assert json.loads(resp['body'])['routing_policy']['bookable'] is False


# --------------------------------------------------------------------------- #
# AppointmentType create/update — FK + field validation
# --------------------------------------------------------------------------- #

def _at_body(**over):
    base = {'name': 'Intro Call', 'duration_minutes': 30, 'routing_policy_id': 'rp_x',
            'program_id': 'prog_x'}
    base.update(over)
    return base


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
@patch('lambda_function.dynamodb')
def test_appointment_type_create_201_with_fk_ok(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}  # FK exists
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    at = json.loads(resp['body'])['appointment_type']
    assert at['appointment_type_id'].startswith('at_')
    assert at['duration_minutes'] == 30
    assert at['buffer_before_minutes'] == 0 and at['lead_time_minutes'] == 0  # defaults
    assert at['program_id'] == 'prog_x'  # the program binding is persisted
    kw = mock_ddb.put_item.call_args.kwargs
    assert 'attribute_not_exists(appointment_type_id)' in kw['ConditionExpression']


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
@patch('lambda_function.dynamodb')
def test_appointment_type_agenda_persisted(mock_ddb):
    """Optional 'Comments' (agenda) is stored so Booking_Commit_Handler can put it in the invite."""
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(
        TENANT, None, _at_body(agenda='Bring your questions.'), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    assert json.loads(resp['body'])['appointment_type']['agenda'] == 'Bring your questions.'


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
@patch('lambda_function.dynamodb')
def test_appointment_type_agenda_too_long_400(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(
        TENANT, None, _at_body(agenda='x' * 2001), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
@patch('lambda_function.dynamodb')
def test_appointment_type_program_id_required_400(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}  # routing FK ok
    no_prog = {k: v for k, v in _at_body().items() if k != 'program_id'}
    resp = handle_scheduling_appointment_type_write(TENANT, None, no_prog, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
@patch('lambda_function.dynamodb')
def test_appointment_type_unknown_program_422(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}  # routing FK ok
    resp = handle_scheduling_appointment_type_write(
        TENANT, None, _at_body(program_id='ghost'), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 422
    mock_ddb.put_item.assert_not_called()


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


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
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


@patch('lambda_function.get_programs', new=lambda _t: _PROGRAMS)
@patch('lambda_function.dynamodb')
def test_appointment_type_write_allows_super_admin(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), 'super_admin', EMAIL, None)
    assert resp['statusCode'] == 201


# --------------------------------------------------------------------------- #
# AppointmentType delete — leaf record, optimistic-locked, admin-only
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_appointment_type_delete_200(mock_ddb):
    resp = handle_scheduling_appointment_type_delete(TENANT, 'at_1', ADMIN, EMAIL, '2026-07-01T00:00:00Z')
    assert resp['statusCode'] == 200
    kw = mock_ddb.delete_item.call_args.kwargs
    assert kw['TableName'] == lf.APPOINTMENT_TYPE_TABLE
    # deleted under the optimistic lock (row must exist + modified_at matches)
    assert kw['ConditionExpression'] == 'attribute_exists(#pk) AND attribute_exists(#mod) AND #mod.#at = :ifmatch'


@patch('lambda_function.dynamodb')
def test_appointment_type_delete_missing_if_match_428(mock_ddb):
    resp = handle_scheduling_appointment_type_delete(TENANT, 'at_1', ADMIN, EMAIL, None)
    assert resp['statusCode'] == 428
    mock_ddb.delete_item.assert_not_called()


@pytest.mark.parametrize('role', ['member', None, 'viewer'])
@patch('lambda_function.dynamodb')
def test_appointment_type_delete_requires_admin(mock_ddb, role):
    resp = handle_scheduling_appointment_type_delete(TENANT, 'at_1', role, EMAIL, '*')
    assert resp['statusCode'] == 403
    mock_ddb.delete_item.assert_not_called()


# --------------------------------------------------------------------------- #
# GET /scheduling/programs — projection of config.programs (forward-compatible)
# --------------------------------------------------------------------------- #

@patch('lambda_function.get_tenant_config')
def test_get_programs_projects_config_programs(mock_cfg):
    # A real config keys programs by a slug and carries a nested program_id; a program without
    # a nested program_id falls back to its object key (schema-discipline).
    mock_cfg.return_value = {'programs': {
        'love_box_application': {'program_id': 'love_box_request', 'program_name': 'Love Box Request'},
        'no_pid': {'program_name': 'Fallback To Key'},
    }}
    resp = handle_scheduling_programs_get(TENANT, ADMIN)
    assert resp['statusCode'] == 200
    progs = json.loads(resp['body'])['programs']
    assert {'program_id': 'love_box_request', 'program_name': 'Love Box Request'} in progs
    assert {'program_id': 'no_pid', 'program_name': 'Fallback To Key'} in progs


@patch('lambda_function.get_tenant_config', return_value=None)
def test_get_programs_no_config_is_empty(_cfg):
    assert json.loads(handle_scheduling_programs_get(TENANT, ADMIN)['body'])['programs'] == []


@pytest.mark.parametrize('role', ['member', None, 'viewer'])
@patch('lambda_function.get_tenant_config')
def test_get_programs_requires_admin(mock_cfg, role):
    assert handle_scheduling_programs_get(TENANT, role)['statusCode'] == 403
    mock_cfg.assert_not_called()


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


# --- get_tag_vocabulary is DERIVED from routing policies (distinct non-empty team names) --- #

@patch('lambda_function.dynamodb')
def test_get_tag_vocabulary_derives_sorted_distinct_team_names(mock_ddb):
    mock_ddb.query.return_value = {'Items': [
        _rp_row('rp1', ['Mentors']),
        _rp_row('rp2', ['ESL', 'Mentors']),   # multi + duplicate name
        _rp_row('rp3', []),                    # the 'Everyone' team — contributes no name
    ]}
    assert lf.get_tag_vocabulary(TENANT) == ['ESL', 'Mentors']  # sorted, de-duped, Everyone excluded


@patch('lambda_function.dynamodb')
def test_get_tag_vocabulary_query_error_returns_empty(mock_ddb):
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'X', 'Message': 'y'}}, 'Query')
    assert lf.get_tag_vocabulary(TENANT) == []


# --- GAP-16: non-dict body -> 400 ------------------------------------------- #

def test_write_handlers_reject_non_dict_body():
    assert handle_scheduling_appointment_type_write(TENANT, None, 'nope', ADMIN, EMAIL, None)['statusCode'] == 400
    assert handle_scheduling_routing_policy_write(TENANT, None, ['nope'], ADMIN, EMAIL, None)['statusCode'] == 400


# =========================================================================== #
# §B18b conference_type (meeting "location") — Phase 1: google_meet | zoom only
# =========================================================================== #

@patch('lambda_function.dynamodb')
def test_appointment_type_conference_type_defaults_google_meet(mock_ddb):
    # Old-shape client omits conference_type → server defaults to google_meet (read-side default).
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    assert json.loads(resp['body'])['appointment_type']['conference_type'] == 'google_meet'
    assert mock_ddb.put_item.call_args.kwargs['Item']['conference_type'] == {'S': 'google_meet'}


@patch('lambda_function.dynamodb')
def test_appointment_type_conference_type_empty_string_defaults(mock_ddb):
    # An empty value is treated as absent → default (a client clearing the field falls back).
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(TENANT, None, _at_body(conference_type=''), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    assert json.loads(resp['body'])['appointment_type']['conference_type'] == 'google_meet'


@patch('lambda_function.dynamodb')
def test_appointment_type_conference_type_zoom_persists(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(
        TENANT, None, _at_body(conference_type='zoom'), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    assert json.loads(resp['body'])['appointment_type']['conference_type'] == 'zoom'
    assert mock_ddb.put_item.call_args.kwargs['Item']['conference_type'] == {'S': 'zoom'}


@pytest.mark.parametrize('bad', ['phone', 'in_person', 'skype', 'teams', 'GOOGLE_MEET', 123])
@patch('lambda_function.dynamodb')
def test_appointment_type_conference_type_invalid_400_no_write(mock_ddb, bad):
    # Future/unknown providers (incl. the still-unbuilt phone/in_person) are rejected so a
    # booking can never carry a conference_type that resolveProvider would THROW on.
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    resp = handle_scheduling_appointment_type_write(
        TENANT, None, _at_body(conference_type=bad), ADMIN, EMAIL, None)
    assert resp['statusCode'] == 400
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_appointment_type_update_persists_conference_type(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': {'routing_policy_id': {'S': 'rp_x'}}}
    mock_ddb.update_item.return_value = {'Attributes': {
        'appointment_type_id': {'S': 'at_1'}, 'tenantId': {'S': TENANT},
        'name': {'S': 'Intro Call'}, 'duration_minutes': {'N': '30'},
        'conference_type': {'S': 'zoom'}, 'routing_policy_id': {'S': 'rp_x'},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_appointment_type_write(
        TENANT, 'at_1', _at_body(conference_type='zoom'), ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 200
    assert 'conference_type' in mock_ddb.update_item.call_args.kwargs['ExpressionAttributeNames'].values()
    assert json.loads(resp['body'])['appointment_type']['conference_type'] == 'zoom'


# =========================================================================== #
# Teams unification (2026-06-29): a team IS its name — uniqueness, single
# "Everyone", rename cascade to staff tags, and team DELETE.
# =========================================================================== #

@patch('lambda_function.dynamodb')
def test_routing_policy_create_duplicate_name_409(mock_ddb):
    mock_ddb.query.return_value = {'Items': [_rp_row('rp1', ['Mentors'])]}  # team 'Mentors' exists
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'tag_conditions': [{'operator': 'in_any', 'values': ['Mentors']}]}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 409
    assert 'Mentors' in json.loads(resp['body'])['duplicateNames']
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_routing_policy_create_second_everyone_409(mock_ddb):
    mock_ddb.query.return_value = {'Items': [_rp_row('rp1', [])]}  # an Everyone team already exists
    resp = handle_scheduling_routing_policy_write(TENANT, None, {'tag_conditions': []}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 409
    assert 'Everyone' in json.loads(resp['body'])['error']
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_routing_policy_create_unique_name_201(mock_ddb):
    mock_ddb.query.return_value = {'Items': [_rp_row('rp1', ['Mentors'])]}
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'tag_conditions': [{'operator': 'in_any', 'values': ['ESL']}]}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 201
    assert json.loads(resp['body'])['routing_policy']['tag_conditions'] == [{'operator': 'in_any', 'values': ['ESL']}]


@patch('lambda_function.dynamodb')
def test_routing_policy_update_keeping_own_name_not_a_dup_200(mock_ddb):
    # Editing a team's assignment without renaming must NOT trip uniqueness against itself.
    mock_ddb.query.return_value = {'Items': [_rp_row('rp_x', ['Mentors'])]}
    mock_ddb.update_item.return_value = {'Attributes': {
        'routing_policy_id': {'S': 'rp_x'}, 'tenantId': {'S': TENANT}, 'tie_breaker': {'S': 'first_available'},
        'tag_conditions': {'L': [{'M': {'operator': {'S': 'in_any'}, 'values': {'L': [{'S': 'Mentors'}]}}}]},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }}
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x',
        {'tie_breaker': 'first_available', 'tag_conditions': [{'operator': 'in_any', 'values': ['Mentors']}]},
        ADMIN, EMAIL, '2026-06-29T00:00:00Z')
    assert resp['statusCode'] == 200


@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function.dynamodb')
def test_routing_policy_rename_cascades_staff_tags(mock_ddb, mock_reg):
    # rp_x is 'Mentors' → rename to 'Tutors'; staff carrying 'Mentors' are re-tagged.
    mock_ddb.query.return_value = {'Items': [_rp_row('rp_x', ['Mentors'])]}
    mock_ddb.update_item.return_value = {'Attributes': {
        'routing_policy_id': {'S': 'rp_x'}, 'tenantId': {'S': TENANT}, 'tie_breaker': {'S': 'round_robin'},
        'tag_conditions': {'L': [{'M': {'operator': {'S': 'in_any'}, 'values': {'L': [{'S': 'Tutors'}]}}}]},
        'modified_at': {'M': {'at': {'S': 'now'}, 'by': {'S': EMAIL}}},
    }}
    mock_reg.list_employees.return_value = [
        {'employeeId': 'e1', 'scheduling_tags': ['Mentors', 'ESL']},
        {'employeeId': 'e2', 'scheduling_tags': ['ESL']},  # untouched
    ]
    resp = handle_scheduling_routing_policy_write(
        TENANT, 'rp_x', {'tag_conditions': [{'operator': 'in_any', 'values': ['Tutors']}]},
        ADMIN, EMAIL, '2026-06-29T00:00:00Z')
    assert resp['statusCode'] == 200
    mock_reg.update_employee.assert_called_once()
    args = mock_reg.update_employee.call_args.args
    assert args[0] == TENANT and args[1] == 'e1'
    assert args[2]['scheduling_tags'] == ['Tutors', 'ESL']  # renamed in place, sibling kept


# --- DELETE team (routing policy) ------------------------------------------- #

@pytest.mark.parametrize('role', ['member', None, 'viewer'])
def test_delete_team_requires_admin(role):
    assert lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', role, EMAIL, 'tok')['statusCode'] == 403


@patch('lambda_function.dynamodb')
def test_delete_team_not_found_404(mock_ddb):
    mock_ddb.get_item.return_value = {}
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 404
    mock_ddb.delete_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_delete_team_blocked_by_appointment_type_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _rp_row('rp_x', ['Mentors'])}
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT}, 'appointment_type_id': {'S': 'at1'},
        'name': {'S': 'Intro Call'}, 'routing_policy_id': {'S': 'rp_x'},
    }]}
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 409
    assert json.loads(resp['body'])['appointmentTypes'][0]['appointment_type_id'] == 'at1'
    mock_ddb.delete_item.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function.dynamodb')
def test_delete_team_success_cascades_untag(mock_ddb, mock_reg):
    mock_ddb.get_item.return_value = {'Item': _rp_row('rp_x', ['Mentors'])}
    mock_ddb.query.return_value = {'Items': []}  # no appointment type FKs it
    mock_reg.list_employees.return_value = [{'employeeId': 'e1', 'scheduling_tags': ['Mentors', 'ESL']}]
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, '2026-06-29T00:00:00Z')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body']) == {'deleted': True, 'routing_policy_id': 'rp_x'}
    dkw = mock_ddb.delete_item.call_args.kwargs
    assert dkw['ConditionExpression'] == 'attribute_exists(#pk) AND attribute_exists(#mod) AND #mod.#at = :ifmatch'
    assert dkw['ExpressionAttributeValues'][':ifmatch'] == {'S': '2026-06-29T00:00:00Z'}
    assert mock_reg.update_employee.call_args.args[2]['scheduling_tags'] == ['ESL']  # 'Mentors' dropped


@patch('lambda_function.dynamodb')
def test_delete_team_missing_if_match_428(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _rp_row('rp_x', ['Mentors'])}
    mock_ddb.query.return_value = {'Items': []}
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, None)
    assert resp['statusCode'] == 428
    mock_ddb.delete_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_delete_team_stale_if_match_409(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _rp_row('rp_x', ['Mentors'])}
    mock_ddb.query.return_value = {'Items': []}
    mock_ddb.delete_item.side_effect = _conditional_failed('DeleteItem')
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, 'stale')
    assert resp['statusCode'] == 409


@patch('lambda_function.dynamodb')
def test_routing_policy_write_uniqueness_query_error_502(mock_ddb):
    # The name-uniqueness load failing aborts the write (no create/update) with a 502.
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'X', 'Message': 'y'}}, 'Query')
    resp = handle_scheduling_routing_policy_write(
        TENANT, None, {'tag_conditions': [{'operator': 'in_any', 'values': ['ESL']}]}, ADMIN, EMAIL, None)
    assert resp['statusCode'] == 502
    mock_ddb.put_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_delete_team_fk_query_error_502(mock_ddb):
    mock_ddb.get_item.return_value = {'Item': _rp_row('rp_x', ['Mentors'])}
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'X', 'Message': 'y'}}, 'Query')
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, 'tok')
    assert resp['statusCode'] == 502
    mock_ddb.delete_item.assert_not_called()


@patch('lambda_function.dynamodb')
def test_delete_team_star_if_match_no_unused_at(mock_ddb):
    # legacy/unstamped 'Everyone' team deleted with '*' → attribute_not_exists branch, no unused #at
    mock_ddb.get_item.return_value = {'Item': _rp_row('rp_x', [], stamped=None)}
    mock_ddb.query.return_value = {'Items': []}
    resp = lf.handle_scheduling_routing_policy_delete(TENANT, 'rp_x', ADMIN, EMAIL, '*')
    assert resp['statusCode'] == 200
    dkw = mock_ddb.delete_item.call_args.kwargs
    assert dkw['ConditionExpression'] == 'attribute_exists(#pk) AND attribute_not_exists(#mod)'
    assert '#at' not in dkw['ExpressionAttributeNames']
    assert 'ExpressionAttributeValues' not in dkw
    for ph in dkw['ExpressionAttributeNames']:
        assert ph in dkw['ConditionExpression'], f'unused ExpressionAttributeName {ph}'
