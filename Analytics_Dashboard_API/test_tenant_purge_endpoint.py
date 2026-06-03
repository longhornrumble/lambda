"""Unit tests for handle_admin_tenant_purge — the super-admin tenant-purge
UI trigger (POST /admin/tenants/{id}/purge).

The endpoint is a thin identity-stamping proxy to the picasso-pii-tenant-purge
Lambda. These tests assert the proxy's contract:
- super_admin guard
- structural env block (staging/dev only — prod gated)
- body whitelist + boolean validation
- tenant existence check
- operator stamped from the AUTH email, never the client body
- purge_id server-generated (client cannot supply it)
- dry_run defaults true / grace_confirmed defaults false (preview-by-default)
- Lambda FunctionError + invoke ClientError → 502

Design: docs/roadmap/PII-Project/tenant-purge-ui-trigger-design.md
"""
import json
import os
from unittest.mock import patch, MagicMock

import pytest

import lambda_function
from lambda_function import handle_admin_tenant_purge


def _invoke_response(result_dict, function_error=None):
    """Build a fake boto3 lambda.invoke() return value."""
    payload = MagicMock()
    payload.read.return_value = json.dumps(result_dict).encode('utf-8')
    resp = {'Payload': payload}
    if function_error:
        resp['FunctionError'] = function_error
    return resp


# A representative Lambda dry-run result the proxy forwards.
_LAMBDA_DRYRUN_RESULT = {
    'purge_id': 'lambda-echoed',
    'tenant_id': 'TEN-X',
    'status': 'completed',
    'deleted': False,
    'rows_touched': {'form-submissions': 3, 'notification-sends': 0,
                     'notification-events': 0, 'subject-index': 1, 'sms-usage': 0},
    'carve_outs_retained': ['sms-consent (incl. STOP/opt-out proof) - legal floor 4-5yr'],
    'manual_followups': [],
    'audit_row_pks': ['x|t1'],
}


@pytest.fixture
def staging_env():
    with patch.dict(os.environ, {'ENVIRONMENT': 'staging'}):
        yield


# ── auth + env guards ───────────────────────────────────────────────────────
def test_non_super_admin_forbidden(staging_env):
    resp = handle_admin_tenant_purge('admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 403


def test_prod_env_blocked():
    with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
        resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 403
    assert 'prod promotion gated' in json.loads(resp['body'])['error']


def test_absent_env_fails_closed():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop('ENVIRONMENT', None)
        resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 403


# ── body validation ─────────────────────────────────────────────────────────
def test_non_boolean_flags_rejected(staging_env):
    resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {'dry_run': 'yes'}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 400


def test_tenant_not_found(staging_env):
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value=None):
        resp = handle_admin_tenant_purge('super_admin', 'TEN-MISSING', {}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 404


# ── happy paths ─────────────────────────────────────────────────────────────
def test_dry_run_default_previews(staging_env):
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value={'tenantId': 'TEN-X'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.return_value = _invoke_response(_LAMBDA_DRYRUN_RESULT)
        resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')

    assert resp['statusCode'] == 200
    # Invoked the env-resolved staging function.
    kwargs = mock_lambda.invoke.call_args.kwargs
    assert kwargs['FunctionName'] == 'picasso-pii-tenant-purge-staging'
    sent = json.loads(kwargs['Payload'])
    assert sent['dry_run'] is True            # default
    assert sent['grace_confirmed'] is False   # default
    assert sent['tenant_id'] == 'TEN-X'


def test_real_delete_forwards_both_flags(staging_env):
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value={'tenantId': 'TEN-X'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.return_value = _invoke_response({**_LAMBDA_DRYRUN_RESULT, 'deleted': True})
        resp = handle_admin_tenant_purge(
            'super_admin', 'TEN-X', {'dry_run': False, 'grace_confirmed': True}, 'op@myrecruiter.ai')

    assert resp['statusCode'] == 200
    sent = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'])
    assert sent['dry_run'] is False
    assert sent['grace_confirmed'] is True


# ── security: identity + purge_id are server-controlled ─────────────────────
def test_operator_stamped_from_auth_not_body(staging_env):
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value={'tenantId': 'TEN-X'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.return_value = _invoke_response(_LAMBDA_DRYRUN_RESULT)
        # Client tries to spoof operator + purge_id via the body.
        handle_admin_tenant_purge(
            'super_admin', 'TEN-X',
            {'operator': 'attacker@evil.com', 'purge_id': 'attacker-controlled'},
            'real-admin@myrecruiter.ai')

    sent = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'])
    assert sent['operator'] == 'real-admin@myrecruiter.ai'   # from auth
    assert sent['purge_id'] != 'attacker-controlled'         # server-generated
    # uuid4 shape (5 hyphen-separated groups)
    assert len(sent['purge_id'].split('-')) == 5


def test_response_purge_id_is_server_value(staging_env):
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value={'tenantId': 'TEN-X'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.return_value = _invoke_response(_LAMBDA_DRYRUN_RESULT)
        resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    body = json.loads(resp['body'])
    sent = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'])
    # The response carries the server purge_id, not the Lambda's echoed one.
    assert body['purge_id'] == sent['purge_id']


# ── failure modes ───────────────────────────────────────────────────────────
def test_lambda_function_error_502(staging_env):
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value={'tenantId': 'TEN-X'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.return_value = _invoke_response(
            {'errorMessage': 'boom'}, function_error='Unhandled')
        resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 502


def test_invoke_client_error_502(staging_env):
    from botocore.exceptions import ClientError
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant', return_value={'tenantId': 'TEN-X'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException'}}, 'Invoke')
        resp = handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    assert resp['statusCode'] == 502


def test_purge_passes_tenant_hash_from_registry(staging_env):
    """F-DSAR31: the dashboard forwards the registry tenantHash so the purge
    Lambda can reach the Class-C session-summaries surface."""
    with patch.object(lambda_function.tenant_registry_ops, 'get_tenant',
                      return_value={'tenantId': 'TEN-X', 'tenantHash': 'my87674d777bf9'}), \
         patch.object(lambda_function, 'lambda_client') as mock_lambda:
        mock_lambda.invoke.return_value = _invoke_response(_LAMBDA_DRYRUN_RESULT)
        handle_admin_tenant_purge('super_admin', 'TEN-X', {}, 'op@myrecruiter.ai')
    sent = json.loads(mock_lambda.invoke.call_args.kwargs['Payload'])
    assert sent['tenant_hash'] == 'my87674d777bf9'
