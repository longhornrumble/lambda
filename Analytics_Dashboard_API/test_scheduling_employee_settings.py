"""
Unit tests for G1 (per-staff scheduling settings write + read projection) and
G4 (tag-vocabulary read).

Covers the §8 permission matrix (scheduling_tags + bookable_override = admin-only;
calendar_email_override = self-or-admin), FAIL-CLOSED vocab validation (422),
field-presence semantics (null clears), input validation, the registry failure
paths (404 / 502), and that the read projection surfaces the 3 additive fields
(present -> value, absent -> defaults) per schema discipline.
"""

import json
from unittest.mock import patch, MagicMock

import pytest

import lambda_function as lf
from lambda_function import (
    _validate_scheduling_tags,
    handle_scheduling_employee_settings_write,
    handle_scheduling_tag_vocabulary_get,
    handle_team_members_list,
)

TENANT = 'TEN1'
ADMIN = 'admin'
MEMBER = 'member'
ADMIN_EMAIL = 'admin@example.com'
STAFF_EMAIL = 'staff@example.com'
EMP_ID = 'a1b2c3d4-e5f6-7890-abcd-ef0123456789'  # uuid4 shape
VOCAB = {'scheduling': {'scheduling_tag_vocabulary': ['mentoring', 'esl', 'weekend']}}


def _emp(email=STAFF_EMAIL, **extra):
    base = {'employeeId': EMP_ID, 'tenantId': TENANT, 'email': email, 'status': 'active'}
    base.update(extra)
    return base


# --------------------------------------------------------------------------- #
# _validate_scheduling_tags (pure) — FAIL-CLOSED
# --------------------------------------------------------------------------- #

def test_tags_valid_dedupe_order_preserved():
    norm, err = _validate_scheduling_tags(['esl', 'mentoring', 'esl'], ['mentoring', 'esl'])
    assert err is None
    assert norm == ['esl', 'mentoring']  # first-seen order, deduped


def test_tags_empty_list_clears_no_error():
    norm, err = _validate_scheduling_tags([], ['mentoring'])
    assert err is None
    assert norm == []


def test_tags_unknown_fails_closed_422_sorted():
    norm, err = _validate_scheduling_tags(['typo', 'also_bad', 'mentoring'], ['mentoring'])
    assert norm is None
    assert err['statusCode'] == 422
    assert json.loads(err['body'])['unknownTags'] == ['also_bad', 'typo']


def test_tags_empty_vocabulary_rejects_any_tag():
    _, err = _validate_scheduling_tags(['mentoring'], [])
    assert err['statusCode'] == 422


def test_tags_non_list_400():
    assert _validate_scheduling_tags('nope', ['m'])[1]['statusCode'] == 400


def test_tags_non_string_or_empty_member_400():
    assert _validate_scheduling_tags(['ok', ''], ['ok'])[1]['statusCode'] == 400
    assert _validate_scheduling_tags(['ok', 1], ['ok'])[1]['statusCode'] == 400


# --------------------------------------------------------------------------- #
# G4 — tag-vocabulary read (admin-only)
# --------------------------------------------------------------------------- #

@patch('lambda_function.get_tenant_config', return_value=VOCAB)
def test_vocab_get_admin_200(_cfg):
    r = handle_scheduling_tag_vocabulary_get(TENANT, ADMIN)
    assert r['statusCode'] == 200
    assert json.loads(r['body'])['scheduling_tag_vocabulary'] == ['mentoring', 'esl', 'weekend']


@pytest.mark.parametrize('role', ['member', None, 'viewer'])
def test_vocab_get_non_admin_403(role):
    assert handle_scheduling_tag_vocabulary_get(TENANT, role)['statusCode'] == 403


@patch('lambda_function.get_tenant_config', return_value={})
def test_vocab_get_missing_config_empty(_cfg):
    r = handle_scheduling_tag_vocabulary_get(TENANT, ADMIN)
    assert json.loads(r['body'])['scheduling_tag_vocabulary'] == []


# --------------------------------------------------------------------------- #
# G1 write — scheduling_tags (ADMIN-only)
# --------------------------------------------------------------------------- #

@patch('lambda_function.get_tenant_config', return_value=VOCAB)
@patch('lambda_function.tenant_registry_ops')
def test_tags_admin_write_200(mock_reg, _cfg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'scheduling_tags': ['mentoring', 'esl']}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 200
    t, e, fields = mock_reg.update_employee.call_args.args
    assert t == TENANT and e == EMP_ID
    assert fields == {'scheduling_tags': ['mentoring', 'esl']}
    assert json.loads(r['body'])['scheduling_tags'] == ['mentoring', 'esl']


@patch('lambda_function.get_tenant_config', return_value=VOCAB)
@patch('lambda_function.tenant_registry_ops')
def test_tags_member_write_403(mock_reg, _cfg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'scheduling_tags': ['mentoring']}, MEMBER, STAFF_EMAIL)
    assert r['statusCode'] == 403
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.get_tenant_config', return_value=VOCAB)
@patch('lambda_function.tenant_registry_ops')
def test_tags_unknown_422_no_write(mock_reg, _cfg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'scheduling_tags': ['nope']}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 422
    assert json.loads(r['body'])['unknownTags'] == ['nope']
    mock_reg.update_employee.assert_not_called()


# --------------------------------------------------------------------------- #
# G1 write — bookable_override (ADMIN-only force-OFF)
# --------------------------------------------------------------------------- #

@patch('lambda_function.tenant_registry_ops')
def test_bookable_off_admin_200(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'off'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 200
    assert mock_reg.update_employee.call_args.args[2] == {'bookable_override': 'off'}


@patch('lambda_function.tenant_registry_ops')
def test_bookable_null_clears_200(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': None}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 200
    assert mock_reg.update_employee.call_args.args[2] == {'bookable_override': None}


@patch('lambda_function.tenant_registry_ops')
def test_bookable_bad_value_400(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'on'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 400
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_bookable_member_403(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'off'}, MEMBER, STAFF_EMAIL)
    assert r['statusCode'] == 403
    mock_reg.update_employee.assert_not_called()


# --------------------------------------------------------------------------- #
# G1 write — calendar_email_override (SELF or ADMIN)
# --------------------------------------------------------------------------- #

@patch('lambda_function.tenant_registry_ops')
def test_cal_email_self_member_200(mock_reg):
    # member editing OWN record (auth email == record email)
    mock_reg.get_employee.return_value = _emp(email=STAFF_EMAIL)
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'calendar_email_override': 'Booking@Example.com'}, MEMBER, STAFF_EMAIL)
    assert r['statusCode'] == 200
    # normalized: trimmed + lowercased
    assert mock_reg.update_employee.call_args.args[2] == {'calendar_email_override': 'booking@example.com'}


@patch('lambda_function.tenant_registry_ops')
def test_cal_email_non_self_member_403(mock_reg):
    # member editing SOMEONE ELSE's record (auth email != record email)
    mock_reg.get_employee.return_value = _emp(email='someone_else@example.com')
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'calendar_email_override': 'x@example.com'}, MEMBER, STAFF_EMAIL)
    assert r['statusCode'] == 403
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_cal_email_admin_any_record_200(mock_reg):
    mock_reg.get_employee.return_value = _emp(email='someone_else@example.com')
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'calendar_email_override': 'x@example.com'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 200


@patch('lambda_function.tenant_registry_ops')
def test_cal_email_invalid_400(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'calendar_email_override': 'not-an-email'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 400
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_cal_email_null_clears_200(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'calendar_email_override': None}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 200
    assert mock_reg.update_employee.call_args.args[2] == {'calendar_email_override': None}


# --------------------------------------------------------------------------- #
# G1 write — validation / failure paths
# --------------------------------------------------------------------------- #

@patch('lambda_function.tenant_registry_ops')
def test_no_fields_400(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(TENANT, EMP_ID, {}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 400
    mock_reg.get_employee.assert_not_called()  # rejected before lookup
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_invalid_employee_id_400(mock_reg):
    r = handle_scheduling_employee_settings_write(
        TENANT, 'bad id/with slash', {'bookable_override': 'off'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 400
    mock_reg.get_employee.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_employee_not_found_404(mock_reg):
    mock_reg.get_employee.return_value = None
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'off'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 404
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_get_employee_raises_502(mock_reg):
    mock_reg.get_employee.side_effect = RuntimeError('ddb down')
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'off'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 502


@patch('lambda_function.tenant_registry_ops')
def test_update_employee_raises_502(mock_reg):
    mock_reg.get_employee.return_value = _emp()
    mock_reg.update_employee.side_effect = RuntimeError('ddb down')
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'off'}, ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 502


@patch('lambda_function.get_tenant_config', return_value=VOCAB)
@patch('lambda_function.tenant_registry_ops')
def test_member_cannot_smuggle_admin_field_with_self_email(mock_reg, _cfg):
    # member editing own record tries to also set admin-only scheduling_tags -> 403, no write
    mock_reg.get_employee.return_value = _emp(email=STAFF_EMAIL)
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID,
        {'calendar_email_override': 'x@example.com', 'scheduling_tags': ['mentoring']},
        MEMBER, STAFF_EMAIL)
    assert r['statusCode'] == 403
    mock_reg.update_employee.assert_not_called()


@patch('lambda_function.get_tenant_config', return_value=VOCAB)
@patch('lambda_function.tenant_registry_ops')
def test_admin_sets_all_three_together_200(mock_reg, _cfg):
    mock_reg.get_employee.return_value = _emp()
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID,
        {'scheduling_tags': ['weekend'], 'bookable_override': 'off',
         'calendar_email_override': 'cal@example.com'},
        ADMIN, ADMIN_EMAIL)
    assert r['statusCode'] == 200
    fields = mock_reg.update_employee.call_args.args[2]
    assert fields == {'scheduling_tags': ['weekend'], 'bookable_override': 'off',
                      'calendar_email_override': 'cal@example.com'}


@patch('lambda_function.tenant_registry_ops')
def test_response_excludes_updatedat_and_passes_copy(mock_reg):
    # update_employee mutates its arg (adds updatedAt). The handler must pass a COPY so the
    # 200 response stays exactly the fields the caller set.
    def _mutate(t, e, fields):
        fields['updatedAt'] = 'now'
    mock_reg.get_employee.return_value = _emp()
    mock_reg.update_employee.side_effect = _mutate
    r = handle_scheduling_employee_settings_write(
        TENANT, EMP_ID, {'bookable_override': 'off'}, ADMIN, ADMIN_EMAIL)
    body = json.loads(r['body'])
    assert 'updatedAt' not in body
    assert body == {'employee_id': EMP_ID, 'bookable_override': 'off'}


# --------------------------------------------------------------------------- #
# G1 read projection — handle_team_members_list surfaces the 3 fields
# --------------------------------------------------------------------------- #

@patch('lambda_function._resolve_team_org_id', return_value=(None, None))
@patch('lambda_function.tenant_registry_ops')
def test_read_projection_surfaces_scheduling_fields(mock_reg, _org):
    mock_reg.list_employees.return_value = [
        # record WITH scheduling fields
        _emp(email='a@x.com', type='local_only', name='A',
             scheduling_tags=['mentoring'], calendar_email_override='cal@x.com',
             bookable_override='off'),
        # record WITHOUT scheduling fields -> defaults
        _emp(email='b@x.com', type='local_only', name='B'),
    ]
    r = handle_team_members_list({'role': ADMIN}, TENANT)
    assert r['statusCode'] == 200
    members = {m['email']: m for m in json.loads(r['body'])['members']}
    assert members['a@x.com']['scheduling_tags'] == ['mentoring']
    assert members['a@x.com']['calendar_email_override'] == 'cal@x.com'
    assert members['a@x.com']['bookable_override'] == 'off'
    # absent -> schema-discipline defaults
    assert members['b@x.com']['scheduling_tags'] == []
    assert members['b@x.com']['calendar_email_override'] is None
    assert members['b@x.com']['bookable_override'] is None
