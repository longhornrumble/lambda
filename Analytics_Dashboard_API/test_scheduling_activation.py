"""
Tests for the org-level scheduling activation endpoints:
  GET   /settings/scheduling/activation  -> raw feature_flags.scheduling_enabled + can_manage
  PATCH /settings/scheduling/activation  -> admin-only flip of that flag

scheduling_enabled is the master switch the whole platform reads (widget agent,
booking, Calendar_OAuth_Connect connect-gate, dashboard tab visibility). The admin
toggle writes it; readers pick it up immediately.
"""

import json
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

import lambda_function
from lambda_function import (
    handle_scheduling_activation_get,
    handle_scheduling_activation_patch,
    update_tenant_scheduling_activation,
)


# --- GET: raw state + can_manage -------------------------------------------

@patch('lambda_function.get_tenant_config')
def test_get_returns_raw_enabled_true(mock_cfg):
    mock_cfg.return_value = {'feature_flags': {'scheduling_enabled': True}}
    body = json.loads(handle_scheduling_activation_get('MYR384719', 'admin')['body'])
    assert body['enabled'] is True
    assert body['can_manage'] is True


@patch('lambda_function.get_tenant_config')
def test_get_absent_flag_false_member_cannot_manage(mock_cfg):
    mock_cfg.return_value = {'feature_flags': {}}
    body = json.loads(handle_scheduling_activation_get('T', 'member')['body'])
    assert body['enabled'] is False
    assert body['can_manage'] is False


@patch('lambda_function.get_tenant_config')
def test_get_no_config_false_superadmin_can_manage(mock_cfg):
    mock_cfg.return_value = None
    body = json.loads(handle_scheduling_activation_get('T', 'super_admin')['body'])
    assert body['enabled'] is False
    assert body['can_manage'] is True


# --- PATCH: auth + validation ----------------------------------------------

def test_patch_member_forbidden():
    resp = handle_scheduling_activation_patch({'role': 'member'}, 'T', {'enabled': True})
    assert resp['statusCode'] == 403


def test_patch_missing_enabled_400():
    resp = handle_scheduling_activation_patch({'role': 'admin'}, 'T', {})
    assert resp['statusCode'] == 400


def test_patch_non_bool_enabled_400():
    resp = handle_scheduling_activation_patch({'role': 'admin'}, 'T', {'enabled': 'yes'})
    assert resp['statusCode'] == 400


@patch('lambda_function.update_tenant_scheduling_activation')
def test_patch_admin_enables(mock_upd):
    mock_upd.return_value = True
    resp = handle_scheduling_activation_patch({'role': 'admin'}, 'MYR', {'enabled': True})
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['enabled'] is True
    mock_upd.assert_called_once_with('MYR', True)


@patch('lambda_function.update_tenant_scheduling_activation')
def test_patch_concurrent_modification_409(mock_upd):
    mock_upd.side_effect = lambda_function.ConcurrentModificationError('busy')
    resp = handle_scheduling_activation_patch({'role': 'super_admin'}, 'MYR', {'enabled': False})
    assert resp['statusCode'] == 409


# --- helper write logic (ETag read-modify-write) ---------------------------

@patch('lambda_function.s3')
def test_update_sets_flag_preserving_others(mock_s3):
    existing = {'feature_flags': {'scheduling_enabled': False, 'AGENTIC_SCHEDULING': True}, 'foo': 'bar'}
    stream = MagicMock(); stream.read.return_value = json.dumps(existing).encode()
    mock_s3.get_object.return_value = {'ETag': '"abc"', 'Body': stream}
    assert update_tenant_scheduling_activation('MYR', True) is True
    _, kwargs = mock_s3.put_object.call_args
    assert kwargs['IfMatch'] == '"abc"'           # optimistic lock
    written = json.loads(kwargs['Body'])
    assert written['feature_flags']['scheduling_enabled'] is True
    assert written['feature_flags']['AGENTIC_SCHEDULING'] is True  # sibling flag preserved
    assert written['foo'] == 'bar'                                  # other config preserved


@patch('lambda_function.s3')
def test_update_creates_feature_flags_if_missing(mock_s3):
    stream = MagicMock(); stream.read.return_value = json.dumps({'other': 1}).encode()
    mock_s3.get_object.return_value = {'ETag': '"e"', 'Body': stream}
    update_tenant_scheduling_activation('T', True)
    written = json.loads(mock_s3.put_object.call_args.kwargs['Body'])
    assert written['feature_flags']['scheduling_enabled'] is True


@patch('lambda_function.s3')
def test_update_etag_mismatch_raises_concurrent(mock_s3):
    stream = MagicMock(); stream.read.return_value = json.dumps({'feature_flags': {}}).encode()
    mock_s3.get_object.return_value = {'ETag': '"e"', 'Body': stream}
    mock_s3.put_object.side_effect = ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
    with pytest.raises(lambda_function.ConcurrentModificationError):
        update_tenant_scheduling_activation('T', True)
