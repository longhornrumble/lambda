"""
Tests for get_clerk_secret_key() — the env-var → Secrets Manager migration aid.

Env var must keep winning when set (safe rollout). Once removed, code falls
back to Secrets Manager and caches the value for the Lambda's lifetime.
"""

import json
import os
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture(autouse=True)
def reset_cache():
    """Clear the module-level cache between tests."""
    import lambda_function
    lambda_function._clerk_secret_cache.clear()
    yield
    lambda_function._clerk_secret_cache.clear()


def test_returns_env_var_when_set_and_does_not_call_secrets_manager(monkeypatch):
    monkeypatch.setenv('CLERK_SECRET_KEY', 'sk_test_from_env')
    import lambda_function

    with patch.object(lambda_function, '_secrets_client') as mock_sm:
        result = lambda_function.get_clerk_secret_key()

    assert result == 'sk_test_from_env'
    mock_sm.get_secret_value.assert_not_called()


def test_falls_back_to_secrets_manager_when_env_var_absent(monkeypatch):
    monkeypatch.delenv('CLERK_SECRET_KEY', raising=False)
    import lambda_function

    with patch.object(lambda_function, '_secrets_client') as mock_sm:
        mock_sm.get_secret_value.return_value = {
            'SecretString': json.dumps({'secret_key': 'sk_live_from_sm'}),
        }
        result = lambda_function.get_clerk_secret_key()

    assert result == 'sk_live_from_sm'
    mock_sm.get_secret_value.assert_called_once_with(SecretId='prod/clerk/picasso/secret_key')


def test_caches_secrets_manager_result_across_calls(monkeypatch):
    monkeypatch.delenv('CLERK_SECRET_KEY', raising=False)
    import lambda_function

    with patch.object(lambda_function, '_secrets_client') as mock_sm:
        mock_sm.get_secret_value.return_value = {
            'SecretString': json.dumps({'secret_key': 'sk_live_cached'}),
        }
        for _ in range(5):
            assert lambda_function.get_clerk_secret_key() == 'sk_live_cached'

    mock_sm.get_secret_value.assert_called_once()


def test_handles_plaintext_secret_format(monkeypatch):
    monkeypatch.delenv('CLERK_SECRET_KEY', raising=False)
    import lambda_function

    with patch.object(lambda_function, '_secrets_client') as mock_sm:
        mock_sm.get_secret_value.return_value = {'SecretString': 'sk_live_plaintext'}
        assert lambda_function.get_clerk_secret_key() == 'sk_live_plaintext'


def test_secret_id_overridable_via_env_var(monkeypatch):
    """CLERK_SECRET_KEY_SECRET_ID overrides the default secret name (for staging/dev)."""
    monkeypatch.delenv('CLERK_SECRET_KEY', raising=False)
    monkeypatch.setenv('CLERK_SECRET_KEY_SECRET_ID', 'override/secret/path')
    # Re-import to pick up the new env var (CLERK_SECRET_ID is computed at import).
    import importlib
    import lambda_function
    importlib.reload(lambda_function)

    with patch.object(lambda_function, '_secrets_client') as mock_sm:
        mock_sm.get_secret_value.return_value = {'SecretString': 'sk_test_override'}
        result = lambda_function.get_clerk_secret_key()

    assert result == 'sk_test_override'
    mock_sm.get_secret_value.assert_called_once_with(SecretId='override/secret/path')
