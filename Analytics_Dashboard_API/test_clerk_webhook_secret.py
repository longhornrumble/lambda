"""
Tests for get_clerk_webhook_secret — env-prefer / Secrets-Manager-fallback.
Prod uses the raw CLERK_WEBHOOK_SECRET env; staging reads it from SM (F2).
"""

import os
from unittest.mock import patch

import lambda_function
from lambda_function import get_clerk_webhook_secret


def _no_env():
    return {k: v for k, v in os.environ.items() if k != 'CLERK_WEBHOOK_SECRET'}


def test_prefers_raw_env():
    lambda_function._clerk_webhook_secret_cache.clear()
    with patch.dict(os.environ, {'CLERK_WEBHOOK_SECRET': 'whsec_RAW'}):
        assert get_clerk_webhook_secret() == 'whsec_RAW'


def test_reads_from_secrets_manager_when_env_unset():
    lambda_function._clerk_webhook_secret_cache.clear()
    with patch.dict(os.environ, _no_env(), clear=True), \
         patch.object(lambda_function, 'CLERK_WEBHOOK_SECRET_SECRET_ID', 'picasso/staging/clerk-webhook-secret'), \
         patch.object(lambda_function, '_secrets_client') as mock_sm:
        mock_sm.get_secret_value.return_value = {'SecretString': 'whsec_FROM_SM'}
        assert get_clerk_webhook_secret() == 'whsec_FROM_SM'
        mock_sm.get_secret_value.assert_called_once_with(SecretId='picasso/staging/clerk-webhook-secret')


def test_returns_empty_when_unconfigured():
    lambda_function._clerk_webhook_secret_cache.clear()
    with patch.dict(os.environ, _no_env(), clear=True), \
         patch.object(lambda_function, 'CLERK_WEBHOOK_SECRET_SECRET_ID', ''):
        assert get_clerk_webhook_secret() == ''
