"""
Tests for the portal messenger-behavior write path (Messenger Product Surface
T3c):
  GET   /settings/messenger  -> raw messenger_behavior section (or {})
  PATCH /settings/messenger  -> admin-only deep-merge write (escalation_email,
                                 strings.escalation_confirmation)

`messenger_behavior` is otherwise owned by the Config Builder / Picasso_Config_
Manager, which wholesale-replaces the whole section on save. This endpoint is
the portal's independent write path (mirrors update_tenant_notifications /
update_tenant_scheduling_activation): S3 ETag/If-Match read-modify-write with
a targeted deep-merge, so a portal edit to just the escalation recipient must
never wipe sibling keys (tone_override, welcome, channel_overrides, ...)
written by the Config Builder. That preservation guarantee is the load-bearing
assertion in this file.
"""

import json
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

import lambda_function
from lambda_function import (
    handle_settings_messenger_get,
    handle_settings_messenger_patch,
    update_tenant_messenger_behavior,
)


# --- GET: raw section, forward-compatible ------------------------------------

@patch('lambda_function.get_tenant_config')
def test_get_returns_existing_section(mock_cfg):
    mock_cfg.return_value = {'messenger_behavior': {'escalation_email': 'ops@example.org', 'tone_override': 'Warm'}}
    body = json.loads(handle_settings_messenger_get('MYR384719')['body'])
    assert body['messenger_behavior']['escalation_email'] == 'ops@example.org'
    assert body['messenger_behavior']['tone_override'] == 'Warm'


@patch('lambda_function.get_tenant_config')
def test_get_old_shape_config_no_crash(mock_cfg):
    """Old config predating messenger_behavior must not crash — forward-compatible read."""
    mock_cfg.return_value = {'chat_title': 'Acme'}
    body = json.loads(handle_settings_messenger_get('T')['body'])
    assert body['messenger_behavior'] == {}


@patch('lambda_function.get_tenant_config')
def test_get_no_config_404(mock_cfg):
    mock_cfg.return_value = None
    resp = handle_settings_messenger_get('T')
    assert resp['statusCode'] == 404


# --- PATCH: auth + validation -------------------------------------------------

def test_patch_member_forbidden():
    resp = handle_settings_messenger_patch({'role': 'member'}, 'T', {'escalation_email': 'a@b.com'})
    assert resp['statusCode'] == 403


def test_patch_empty_body_400():
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'T', {})
    assert resp['statusCode'] == 400


def test_patch_non_string_email_400():
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'T', {'escalation_email': 12345})
    assert resp['statusCode'] == 400


def test_patch_malformed_email_400():
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'T', {'escalation_email': 'not-an-email'})
    assert resp['statusCode'] == 400


def test_patch_strings_unknown_key_400():
    resp = handle_settings_messenger_patch(
        {'role': 'admin'}, 'T', {'strings': {'button_intro': 'Tap here'}}
    )
    assert resp['statusCode'] == 400


def test_patch_strings_non_string_value_400():
    resp = handle_settings_messenger_patch(
        {'role': 'admin'}, 'T', {'strings': {'escalation_confirmation': 42}}
    )
    assert resp['statusCode'] == 400


def test_patch_strings_empty_dict_400():
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'T', {'strings': {}})
    assert resp['statusCode'] == 400


def test_patch_email_too_long_400():
    huge = 'a' * 250 + '@example.com'  # > 254 chars
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'T', {'escalation_email': huge})
    assert resp['statusCode'] == 400


def test_patch_string_too_long_400():
    resp = handle_settings_messenger_patch(
        {'role': 'admin'}, 'T', {'strings': {'escalation_confirmation': 'x' * 501}}
    )
    assert resp['statusCode'] == 400


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_admin_sets_email(mock_upd):
    mock_upd.return_value = {'escalation_email': 'ops@example.org'}
    resp = handle_settings_messenger_patch(
        {'role': 'admin'}, 'MYR', {'escalation_email': 'ops@example.org'}
    )
    assert resp['statusCode'] == 200
    assert json.loads(resp['body'])['messenger_behavior']['escalation_email'] == 'ops@example.org'
    mock_upd.assert_called_once_with('MYR', {'escalation_email': 'ops@example.org'})


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_strips_whitespace_from_email(mock_upd):
    mock_upd.return_value = {'escalation_email': 'ops@example.org'}
    handle_settings_messenger_patch({'role': 'admin'}, 'MYR', {'escalation_email': '  ops@example.org  '})
    mock_upd.assert_called_once_with('MYR', {'escalation_email': 'ops@example.org'})


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_combined_email_and_strings_in_one_call(mock_upd):
    """Both fields sent together must both land in the single update payload."""
    mock_upd.return_value = {
        'escalation_email': 'ops@example.org',
        'strings': {'escalation_confirmation': 'One sec, connecting you.'},
    }
    resp = handle_settings_messenger_patch(
        {'role': 'admin'},
        'MYR',
        {
            'escalation_email': 'ops@example.org',
            'strings': {'escalation_confirmation': 'One sec, connecting you.'},
        },
    )
    assert resp['statusCode'] == 200
    mock_upd.assert_called_once_with('MYR', {
        'escalation_email': 'ops@example.org',
        'strings': {'escalation_confirmation': 'One sec, connecting you.'},
    })


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_allows_clearing_email(mock_upd):
    """Empty string clears the recipient — falls back to ESCALATION_EMAIL env default."""
    mock_upd.return_value = {'escalation_email': ''}
    resp = handle_settings_messenger_patch({'role': 'super_admin'}, 'MYR', {'escalation_email': ''})
    assert resp['statusCode'] == 200
    mock_upd.assert_called_once_with('MYR', {'escalation_email': ''})


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_sets_strings(mock_upd):
    mock_upd.return_value = {'strings': {'escalation_confirmation': 'Connecting you now.'}}
    resp = handle_settings_messenger_patch(
        {'role': 'admin'}, 'MYR', {'strings': {'escalation_confirmation': 'Connecting you now.'}}
    )
    assert resp['statusCode'] == 200
    mock_upd.assert_called_once_with('MYR', {'strings': {'escalation_confirmation': 'Connecting you now.'}})


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_concurrent_modification_409(mock_upd):
    mock_upd.side_effect = lambda_function.ConcurrentModificationError('busy')
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'MYR', {'escalation_email': 'a@b.com'})
    assert resp['statusCode'] == 409


@patch('lambda_function.update_tenant_messenger_behavior')
def test_patch_s3_client_error_500(mock_upd):
    mock_upd.side_effect = ClientError({'Error': {'Code': 'InternalError'}}, 'PutObject')
    resp = handle_settings_messenger_patch({'role': 'admin'}, 'MYR', {'escalation_email': 'a@b.com'})
    assert resp['statusCode'] == 500


# --- helper write logic (ETag read-modify-write + deep-merge) ----------------

@patch('lambda_function.s3')
def test_update_sets_email_preserving_siblings(mock_s3):
    """Load-bearing: a patch that only sends escalation_email must NOT wipe
    pre-existing sibling keys (tone_override) in messenger_behavior — this is
    the whole reason this endpoint deep-merges instead of wholesale-replacing
    like Picasso_Config_Manager does."""
    existing = {
        'messenger_behavior': {
            'escalation_email': 'old@example.org',
            'tone_override': 'Friendly and concise',
            'welcome': {'greeting': 'Hi there!'},
        },
        'foo': 'bar',
    }
    stream = MagicMock(); stream.read.return_value = json.dumps(existing).encode()
    mock_s3.get_object.return_value = {'ETag': '"abc"', 'Body': stream}

    result = update_tenant_messenger_behavior('MYR', {'escalation_email': 'new@example.org'})

    assert result['escalation_email'] == 'new@example.org'
    assert result['tone_override'] == 'Friendly and concise'  # sibling preserved

    _, kwargs = mock_s3.put_object.call_args
    assert kwargs['IfMatch'] == '"abc"'  # optimistic lock
    written = json.loads(kwargs['Body'])
    assert written['messenger_behavior']['escalation_email'] == 'new@example.org'
    assert written['messenger_behavior']['tone_override'] == 'Friendly and concise'
    assert written['messenger_behavior']['welcome'] == {'greeting': 'Hi there!'}
    assert written['foo'] == 'bar'  # other top-level config preserved


@patch('lambda_function.s3')
def test_update_creates_messenger_behavior_if_missing(mock_s3):
    """Old-shape config with no messenger_behavior section at all — must not crash."""
    stream = MagicMock(); stream.read.return_value = json.dumps({'chat_title': 'Acme'}).encode()
    mock_s3.get_object.return_value = {'ETag': '"e"', 'Body': stream}

    result = update_tenant_messenger_behavior('T', {'escalation_email': 'a@b.com'})

    assert result == {'escalation_email': 'a@b.com'}
    written = json.loads(mock_s3.put_object.call_args.kwargs['Body'])
    assert written['messenger_behavior']['escalation_email'] == 'a@b.com'
    assert written['chat_title'] == 'Acme'  # unrelated top-level key preserved


@patch('lambda_function.s3')
def test_update_etag_mismatch_raises_concurrent(mock_s3):
    stream = MagicMock(); stream.read.return_value = json.dumps({'messenger_behavior': {}}).encode()
    mock_s3.get_object.return_value = {'ETag': '"e"', 'Body': stream}
    mock_s3.put_object.side_effect = ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
    with pytest.raises(lambda_function.ConcurrentModificationError):
        update_tenant_messenger_behavior('T', {'escalation_email': 'a@b.com'})


@patch('lambda_function.s3')
def test_update_non_precondition_client_error_propagates(mock_s3):
    """A non-ETag S3 error (e.g. transient InternalError) must NOT be swallowed
    or mis-mapped to ConcurrentModificationError — it should propagate as-is
    for the handler's generic ClientError -> 500 branch to catch."""
    stream = MagicMock(); stream.read.return_value = json.dumps({'messenger_behavior': {}}).encode()
    mock_s3.get_object.return_value = {'ETag': '"e"', 'Body': stream}
    mock_s3.put_object.side_effect = ClientError({'Error': {'Code': 'InternalError'}}, 'PutObject')
    with pytest.raises(ClientError):
        update_tenant_messenger_behavior('T', {'escalation_email': 'a@b.com'})


@patch('lambda_function.s3')
def test_update_never_logs_email_value(mock_s3, caplog):
    """PII discipline: the escalation email is a value we write but never emit to logs."""
    stream = MagicMock(); stream.read.return_value = json.dumps({'messenger_behavior': {}}).encode()
    mock_s3.get_object.return_value = {'ETag': '"e"', 'Body': stream}
    secret_email = 'do-not-log-me@example.org'
    with caplog.at_level('INFO'):
        update_tenant_messenger_behavior('T', {'escalation_email': secret_email})
    assert secret_email not in caplog.text
