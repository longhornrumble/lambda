"""
Unit tests for G2/E14 scheduling notification-template overrides (ADA API).

Covers admin-only auth, GET effective-merge (override-over-default + is_override + default
echo), PATCH upsert-merge (partial save, empty-clears, validation, modified_at), failure
paths (404 unknown moment / 400 bad field / 502 DDB), and the defaults-mirror parity guard.
"""

import json
from unittest.mock import patch

import pytest

import lambda_function as lf
from lambda_function import (
    handle_scheduling_notification_templates_get,
    handle_scheduling_notification_template_write,
)

TENANT = 'TEN1'
ADMIN = 'admin'
SUPER = 'super_admin'
EMAIL = 'admin@example.com'


# --------------------------------------------------------------------------- #
# defaults parity / compliance
# --------------------------------------------------------------------------- #

def test_defaults_cover_exactly_the_three_dispatched_moments():
    assert tuple(lf._SCHED_NOTIF_DEFAULTS.keys()) == lf._SCHED_NOTIF_MOMENTS
    assert set(lf._SCHED_NOTIF_MOMENTS) == {'reschedule_link', 'reoffer', 'cancel_notice'}


def test_defaults_carry_no_stop_line():
    # STOP is appended by notify.js at dispatch, never part of the editable default body.
    for moment, tpl in lf._SCHED_NOTIF_DEFAULTS.items():
        for f in ('subject', 'body_text', 'body_html'):
            assert 'STOP' not in tpl[f], f'{moment}.{f} must not bake in STOP'


# --------------------------------------------------------------------------- #
# GET
# --------------------------------------------------------------------------- #

@patch('lambda_function.dynamodb')
def test_get_no_overrides_returns_defaults(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    r = handle_scheduling_notification_templates_get(TENANT, ADMIN)
    assert r['statusCode'] == 200
    body = json.loads(r['body'])
    assert set(body['moments'].keys()) == set(lf._SCHED_NOTIF_MOMENTS)
    rl = body['moments']['reschedule_link']
    assert rl['is_override'] is False
    assert rl['subject'] == lf._SCHED_NOTIF_DEFAULTS['reschedule_link']['subject']
    assert rl['body_html'] == lf._SCHED_NOTIF_DEFAULTS['reschedule_link']['body_html']  # GAP-6
    assert rl['default']['subject'] == lf._SCHED_NOTIF_DEFAULTS['reschedule_link']['subject']
    assert rl['modified_at'] is None
    # per-moment available_variables (reschedule has actionUrl, NOT rebook*)
    assert '{{firstName}}' in rl['available_variables']
    assert '{{actionUrl}}' in rl['available_variables']
    assert '{{rebookHtml}}' not in rl['available_variables']
    # cancel_notice exposes the rebook vars, not actionUrl
    cn = body['moments']['cancel_notice']
    assert '{{rebookHtml}}' in cn['available_variables']
    assert '{{actionUrl}}' not in cn['available_variables']
    assert 'cannot be removed' in body['stop_footer_note']  # GAP-6 compliance signal


@patch('lambda_function.dynamodb')
def test_get_with_override_shows_effective_and_flag(mock_ddb):
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT},
        'moment': {'S': 'reschedule_link'},
        'subject': {'S': 'Custom subject for {{org}}'},
        'modified_at': {'M': {'at': {'S': '2026-06-06T00:00:00.000000Z'}, 'by': {'S': EMAIL}}},
    }]}
    r = handle_scheduling_notification_templates_get(TENANT, ADMIN)
    rl = json.loads(r['body'])['moments']['reschedule_link']
    assert rl['is_override'] is True
    assert rl['subject'] == 'Custom subject for {{org}}'
    # body_text not overridden -> falls back to default
    assert rl['body_text'] == lf._SCHED_NOTIF_DEFAULTS['reschedule_link']['body_text']
    assert rl['modified_at']['by'] == EMAIL


@patch('lambda_function.dynamodb')
def test_get_blank_override_field_falls_back_to_default(mock_ddb):
    # an empty-string stored override clears that field -> default shown, is_override False for it
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT}, 'moment': {'S': 'reoffer'}, 'subject': {'S': '   '},
    }]}
    rf = json.loads(handle_scheduling_notification_templates_get(TENANT, ADMIN)['body'])['moments']['reoffer']
    assert rf['is_override'] is False
    assert rf['subject'] == lf._SCHED_NOTIF_DEFAULTS['reoffer']['subject']


@pytest.mark.parametrize('role', ['member', None, 'viewer'])
def test_get_non_admin_403(role):
    assert handle_scheduling_notification_templates_get(TENANT, role)['statusCode'] == 403


@patch('lambda_function.dynamodb')
def test_get_query_error_502(mock_ddb):
    from botocore.exceptions import ClientError
    mock_ddb.query.side_effect = ClientError({'Error': {'Code': 'X'}}, 'Query')
    assert handle_scheduling_notification_templates_get(TENANT, ADMIN)['statusCode'] == 502


# --------------------------------------------------------------------------- #
# PATCH
# --------------------------------------------------------------------------- #

def _attrs(**kv):
    out = {'tenantId': {'S': TENANT}, 'moment': {'S': kv.pop('moment', 'reschedule_link')}}
    for k, v in kv.items():
        out[k] = {'S': v}
    return {'Attributes': out}


@patch('lambda_function.dynamodb')
def test_patch_admin_sets_subject_200(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(subject='Hi {{org}}')
    r = handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'subject': 'Hi {{org}}'}, ADMIN, EMAIL)
    assert r['statusCode'] == 200
    kw = mock_ddb.update_item.call_args.kwargs
    assert kw['Key'] == {'tenantId': {'S': TENANT}, 'moment': {'S': 'reschedule_link'}}
    assert 'SET' in kw['UpdateExpression']
    # modified_at always written
    assert any(n == 'modified_at' for n in kw['ExpressionAttributeNames'].values())


@patch('lambda_function.dynamodb')
def test_patch_super_admin_ok(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(subject='x')
    assert handle_scheduling_notification_template_write(
        TENANT, 'cancel_notice', {'subject': 'x'}, SUPER, EMAIL)['statusCode'] == 200


@patch('lambda_function.dynamodb')
def test_patch_partial_only_sets_provided_fields(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(body_text='B')
    handle_scheduling_notification_template_write(
        TENANT, 'reoffer', {'body_text': 'B'}, ADMIN, EMAIL)
    set_names = list(mock_ddb.update_item.call_args.kwargs['ExpressionAttributeNames'].values())
    assert 'body_text' in set_names and 'modified_at' in set_names
    assert 'subject' not in set_names and 'body_html' not in set_names


@patch('lambda_function.dynamodb')
def test_patch_empty_string_clears_field_200(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(subject='')
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'subject': ''}, ADMIN, EMAIL)['statusCode'] == 200


def test_patch_unknown_moment_404():
    r = handle_scheduling_notification_template_write(
        TENANT, 'reminder_24h', {'subject': 'x'}, ADMIN, EMAIL)
    assert r['statusCode'] == 404


@pytest.mark.parametrize('role', ['member', None])
def test_patch_non_admin_403(role):
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'subject': 'x'}, role, EMAIL)['statusCode'] == 403


def test_patch_no_fields_400():
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {}, ADMIN, EMAIL)['statusCode'] == 400


def test_patch_non_string_field_400():
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'subject': 123}, ADMIN, EMAIL)['statusCode'] == 400


def test_patch_field_too_long_400():
    big = 'x' * (lf._SCHED_TPL_FIELD_MAX + 1)
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'body_html': big}, ADMIN, EMAIL)['statusCode'] == 400


@patch('lambda_function.dynamodb')
def test_patch_write_error_502(mock_ddb):
    from botocore.exceptions import ClientError
    mock_ddb.update_item.side_effect = ClientError({'Error': {'Code': 'X'}}, 'UpdateItem')
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'subject': 'x'}, ADMIN, EMAIL)['statusCode'] == 502


# =========================================================================== #
# phase-completion-audit fixes — B1 (unused alias) + GAP-7/8
# =========================================================================== #

@patch('lambda_function.dynamodb')
def test_patch_no_unused_expression_alias_b1(mock_ddb):
    # B1 regression: a stray ExpressionAttributeNames entry not referenced by any expression
    # makes DynamoDB reject every UpdateItem (ValidationException). Assert names contains ONLY
    # the SET field aliases (no 'tenantId'/'moment' key alias).
    mock_ddb.update_item.return_value = _attrs(subject='x')
    handle_scheduling_notification_template_write(TENANT, 'reschedule_link', {'subject': 'x'}, ADMIN, EMAIL)
    names = mock_ddb.update_item.call_args.kwargs['ExpressionAttributeNames']
    assert 'tenantId' not in names.values()
    assert 'moment' not in names.values()
    # every alias must appear in the UpdateExpression
    expr = mock_ddb.update_item.call_args.kwargs['UpdateExpression']
    for alias in names:
        assert alias in expr


@patch('lambda_function.dynamodb')
def test_patch_200_body_shape_gap7(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(moment='cancel_notice', subject='Bye {{org}}')
    r = handle_scheduling_notification_template_write(TENANT, 'cancel_notice', {'subject': 'Bye {{org}}'}, ADMIN, EMAIL)
    body = json.loads(r['body'])
    assert body['moment'] == 'cancel_notice'
    assert body['template']['subject'] == 'Bye {{org}}'


def test_patch_non_dict_body_400_gap8():
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', "not-a-dict", ADMIN, EMAIL)['statusCode'] == 400


# --------------------------------------------------------------------------- #
# G7a — SMS editor surface (items 1-3; the SEND path is held)
# --------------------------------------------------------------------------- #

def test_sms_defaults_cover_the_three_moments_no_stop_no_html_vars():
    assert set(lf._SCHED_NOTIF_SMS_DEFAULTS.keys()) == set(lf._SCHED_NOTIF_MOMENTS)
    for moment, txt in lf._SCHED_NOTIF_SMS_DEFAULTS.items():
        assert 'STOP' not in txt, f'{moment} SMS default must not bake in STOP (appended at dispatch)'
    # SMS vars are plain-text only — never the html-only {{rebookHtml}}
    for moment, vars_ in lf._SCHED_NOTIF_SMS_VARS.items():
        assert '{{rebookHtml}}' not in vars_


@patch('lambda_function.dynamodb')
def test_get_exposes_sms_surface_defaults(mock_ddb):
    mock_ddb.query.return_value = {'Items': []}
    body = json.loads(handle_scheduling_notification_templates_get(TENANT, ADMIN)['body'])
    assert 'sms_footer_note' in body and 'cannot be removed' in body['sms_footer_note']
    rl = body['moments']['reschedule_link']
    assert rl['sms_is_override'] is False
    assert rl['sms_text'] == lf._SCHED_NOTIF_SMS_DEFAULTS['reschedule_link']
    assert rl['sms_default'] == lf._SCHED_NOTIF_SMS_DEFAULTS['reschedule_link']
    assert '{{actionUrl}}' in rl['sms_available_variables']
    assert '{{rebookHtml}}' not in rl['sms_available_variables']


@patch('lambda_function.dynamodb')
def test_get_with_sms_override_shows_effective_and_flag(mock_ddb):
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT}, 'moment': {'S': 'reschedule_link'},
        'sms_text': {'S': 'Custom SMS {{actionUrl}}'},
    }]}
    rl = json.loads(handle_scheduling_notification_templates_get(TENANT, ADMIN)['body'])['moments']['reschedule_link']
    assert rl['sms_is_override'] is True
    assert rl['sms_text'] == 'Custom SMS {{actionUrl}}'
    # email side untouched → still default + not an override
    assert rl['is_override'] is False
    assert rl['subject'] == lf._SCHED_NOTIF_DEFAULTS['reschedule_link']['subject']


@patch('lambda_function.dynamodb')
def test_get_blank_sms_override_falls_back_to_sms_default(mock_ddb):
    mock_ddb.query.return_value = {'Items': [{
        'tenantId': {'S': TENANT}, 'moment': {'S': 'reoffer'}, 'sms_text': {'S': '   '},
    }]}
    rf = json.loads(handle_scheduling_notification_templates_get(TENANT, ADMIN)['body'])['moments']['reoffer']
    assert rf['sms_is_override'] is False
    assert rf['sms_text'] == lf._SCHED_NOTIF_SMS_DEFAULTS['reoffer']


@patch('lambda_function.dynamodb')
def test_patch_accepts_sms_text_only_200(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(sms_text='Hi {{firstName}} {{actionUrl}}')
    r = handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'sms_text': 'Hi {{firstName}} {{actionUrl}}'}, ADMIN, EMAIL)
    assert r['statusCode'] == 200
    kw = mock_ddb.update_item.call_args.kwargs
    assert any(n == 'sms_text' for n in kw['ExpressionAttributeNames'].values())


@patch('lambda_function.dynamodb')
def test_patch_sms_text_non_string_400(mock_ddb):
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'sms_text': 123}, ADMIN, EMAIL)['statusCode'] == 400


@patch('lambda_function.dynamodb')
def test_patch_sms_text_too_long_400(mock_ddb):
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'sms_text': 'x' * (lf._SCHED_SMS_FIELD_MAX + 1)}, ADMIN, EMAIL)['statusCode'] == 400


@patch('lambda_function.dynamodb')
def test_patch_empty_sms_text_clears_override_200(mock_ddb):
    mock_ddb.update_item.return_value = _attrs(sms_text='')
    assert handle_scheduling_notification_template_write(
        TENANT, 'reschedule_link', {'sms_text': ''}, ADMIN, EMAIL)['statusCode'] == 200
