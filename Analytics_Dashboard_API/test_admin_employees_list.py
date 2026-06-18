"""
Tests for handle_admin_employees_list after the tenant-scoping change:
the admin employee roster is always scoped to one tenant (a DynamoDB Query),
never a cross-tenant Scan, and the _ADMIN_EXCLUDED_TENANTS filter is gone.
"""

from unittest.mock import patch

from lambda_function import handle_admin_employees_list


def test_requires_tenant_id():
    # No tenant_id -> 400 (the cross-tenant Scan path was removed).
    resp = handle_admin_employees_list('super_admin', {})
    assert resp['statusCode'] == 400


def test_non_super_admin_forbidden():
    resp = handle_admin_employees_list('admin', {'tenant_id': 'MYR384719'})
    assert resp['statusCode'] == 403


@patch('lambda_function.tenant_registry_ops')
def test_tenant_scoped_query_not_scan(mock_ops):
    mock_ops.list_employees.return_value = [
        {'tenantId': 'MYR384719', 'name': 'Zoe', 'email': 'z@x', 'status': 'active'},
        {'tenantId': 'MYR384719', 'name': 'Abe', 'email': 'a@x', 'status': 'active'},
    ]
    resp = handle_admin_employees_list('super_admin', {'tenant_id': 'MYR384719'})
    assert resp['statusCode'] == 200
    # Query for the one tenant; the cross-tenant Scan helper is never called.
    mock_ops.list_employees.assert_called_once_with('MYR384719')
    mock_ops.list_all_employees.assert_not_called()


@patch('lambda_function.tenant_registry_ops')
def test_no_exclusion_filter(mock_ops):
    # MYR384719 used to be excluded; now its employees are returned (sorted).
    mock_ops.list_employees.return_value = [
        {'tenantId': 'MYR384719', 'name': 'Chris', 'email': 'c@x', 'status': 'active'},
    ]
    resp = handle_admin_employees_list('super_admin', {'tenant_id': 'MYR384719'})
    assert resp['statusCode'] == 200
    import json
    body = json.loads(resp['body'])
    assert body['total'] == 1
    assert body['employees'][0]['email'] == 'c@x'
