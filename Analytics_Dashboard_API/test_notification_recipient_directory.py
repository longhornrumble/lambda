"""
Tests for the recipients_directory that GET /settings/notifications now returns.

It resolves each form's recipient_employee_ids to {name, email, status} INCLUDING
inactive (soft-deleted) employees, so the portal can show a former team member by
name instead of a bare UUID. IDs with no registry record are omitted on purpose →
the portal keeps its "Former team member <id>" fallback for those (true erasure).
"""

import json
from unittest.mock import patch

from lambda_function import handle_settings_notifications_get


def _config_with_recipients(ids):
    return {
        'conversational_forms': {
            'form_contact': {
                'form_title': 'Contact',
                'notifications': {'internal': {'enabled': True, 'recipient_employee_ids': ids}},
            }
        }
    }


@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function.get_tenant_config')
def test_directory_includes_inactive_and_omits_unknown(mock_cfg, mock_ops):
    mock_cfg.return_value = _config_with_recipients(['active-1', 'inactive-2', 'ghost-3'])
    mock_ops.list_employees.return_value = [
        {'employeeId': 'active-1', 'name': 'Active Annie', 'email': 'annie@x', 'status': 'active'},
        {'employeeId': 'inactive-2', 'name': 'Former Fred', 'email': 'fred@x', 'status': 'inactive'},
    ]
    resp = handle_settings_notifications_get('MYR384719')
    assert resp['statusCode'] == 200
    directory = json.loads(resp['body'])['recipients_directory']

    # inactive employee resolves to its retained name + status (the whole point)
    assert directory['inactive-2'] == {'name': 'Former Fred', 'email': 'fred@x', 'status': 'inactive'}
    # active employee is present too
    assert directory['active-1']['status'] == 'active'
    # an ID with no registry record is absent → portal falls back to the UUID row
    assert 'ghost-3' not in directory


@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function.get_tenant_config')
def test_no_recipient_ids_skips_registry_lookup(mock_cfg, mock_ops):
    # Form with no recipient_employee_ids → empty directory, registry never queried.
    mock_cfg.return_value = {
        'conversational_forms': {'form_contact': {'notifications': {'internal': {'enabled': True}}}}
    }
    resp = handle_settings_notifications_get('MYR384719')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['recipients_directory'] == {}
    mock_ops.list_employees.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function.get_tenant_config')
def test_directory_tolerates_registry_failure(mock_cfg, mock_ops):
    # A registry read error must not 500 the settings page — directory just stays empty.
    mock_cfg.return_value = _config_with_recipients(['active-1'])
    mock_ops.list_employees.side_effect = RuntimeError('ddb down')
    resp = handle_settings_notifications_get('MYR384719')
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['recipients_directory'] == {}
