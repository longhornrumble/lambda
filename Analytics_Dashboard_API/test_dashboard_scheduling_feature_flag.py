"""
Tests for the dashboard_scheduling feature flag — the gate dash#11's Scheduling
tab reads (App.tsx). Before this fix the backend never emitted it, so the tab was
permanently hidden. These pin: super_admin always sees it; a tenant sees it only
when its config enables it (D1 Flag A entitlement); absent → off (never a crash).
"""

import json
from unittest.mock import patch

import lambda_function
from lambda_function import get_tenant_features, handle_features


@patch('lambda_function.get_tenant_config')
def test_no_config_scheduling_off(mock_cfg):
    mock_cfg.return_value = None
    assert get_tenant_features('TEN1')['dashboard_scheduling'] is False


@patch('lambda_function.get_tenant_config')
def test_config_without_flag_scheduling_off(mock_cfg):
    mock_cfg.return_value = {'features': {}, 'feature_flags': {}}
    assert get_tenant_features('TEN1')['dashboard_scheduling'] is False


@patch('lambda_function.get_tenant_config')
def test_config_features_enables_scheduling(mock_cfg):
    mock_cfg.return_value = {'features': {'dashboard_scheduling': True}, 'feature_flags': {}}
    assert get_tenant_features('TEN1')['dashboard_scheduling'] is True


@patch('lambda_function.get_tenant_config')
def test_config_feature_flags_enables_scheduling(mock_cfg):
    mock_cfg.return_value = {'features': {}, 'feature_flags': {'dashboard_scheduling': True}}
    assert get_tenant_features('TEN1')['dashboard_scheduling'] is True


def test_super_admin_features_endpoint_includes_scheduling():
    prev = getattr(lambda_function, '_request_user_role', None)
    lambda_function._request_user_role = 'super_admin'
    try:
        resp = handle_features('ANY')
        body = json.loads(resp['body'])
        assert body['features']['dashboard_scheduling'] is True
    finally:
        lambda_function._request_user_role = prev
