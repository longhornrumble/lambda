"""
Tests for _invite_redirect_url — the env-aware Clerk-invitation redirect.
Without DASHBOARD_APP_URL, a staging invite would redirect accepters to prod.
"""

import os
from unittest.mock import patch

from lambda_function import _invite_redirect_url


def test_defaults_to_prod_when_env_unset():
    env = {k: v for k, v in os.environ.items() if k != 'DASHBOARD_APP_URL'}
    with patch.dict(os.environ, env, clear=True):
        assert _invite_redirect_url('', '') == 'https://app.myrecruiter.ai/sign-up'


def test_uses_env_override_for_staging():
    with patch.dict(os.environ, {'DASHBOARD_APP_URL': 'https://staging.app.myrecruiter.ai'}):
        assert _invite_redirect_url('', '') == 'https://staging.app.myrecruiter.ai/sign-up'


def test_includes_names_when_present():
    with patch.dict(os.environ, {'DASHBOARD_APP_URL': 'https://staging.app.myrecruiter.ai'}):
        assert _invite_redirect_url('Ann', 'Lee') == \
            'https://staging.app.myrecruiter.ai/sign-up?first_name=Ann&last_name=Lee'
