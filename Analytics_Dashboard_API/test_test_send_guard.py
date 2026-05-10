"""
Tests for the env-var-driven SES_SENDER and TEST_SEND_ALLOWED_DOMAINS guard
introduced in Phase 4.2 of the AWS-native versioning + staging-twin project.

Both are module-level constants computed at import time; tests use
importlib.reload() to re-evaluate them under different env conditions.
"""

import importlib
import os
from unittest.mock import patch

import pytest


@pytest.fixture
def reload_module(monkeypatch):
    """
    Reload lambda_function so SES_SENDER and _TEST_SEND_ALLOWED_DOMAINS
    are re-derived from the current env. Returns the reloaded module.
    """
    def _reload():
        import lambda_function
        return importlib.reload(lambda_function)
    yield _reload


# ---------------------------------------------------------------------------
# SES_SENDER env-var override
# ---------------------------------------------------------------------------

def test_ses_sender_defaults_when_env_unset(monkeypatch, reload_module):
    monkeypatch.delenv('SES_SENDER_ADDRESS', raising=False)
    lf = reload_module()
    assert lf.SES_SENDER == 'notify@myrecruiter.ai'


def test_ses_sender_uses_env_when_set(monkeypatch, reload_module):
    monkeypatch.setenv('SES_SENDER_ADDRESS', 'notify@staging.myrecruiter.ai')
    lf = reload_module()
    assert lf.SES_SENDER == 'notify@staging.myrecruiter.ai'


# ---------------------------------------------------------------------------
# _test_send_recipient_allowed — TEST_SEND_ALLOWED_DOMAINS guard
# ---------------------------------------------------------------------------

def test_allows_any_recipient_when_env_unset(monkeypatch, reload_module):
    monkeypatch.delenv('TEST_SEND_ALLOWED_DOMAINS', raising=False)
    lf = reload_module()
    assert lf._test_send_recipient_allowed('person@example.com') is True
    assert lf._test_send_recipient_allowed('other@anywhere.org') is True


def test_allows_recipient_in_allowlist(monkeypatch, reload_module):
    monkeypatch.setenv('TEST_SEND_ALLOWED_DOMAINS', 'myrecruiter.ai,staging.myrecruiter.ai')
    lf = reload_module()
    assert lf._test_send_recipient_allowed('chris@myrecruiter.ai') is True
    assert lf._test_send_recipient_allowed('test@staging.myrecruiter.ai') is True


def test_rejects_recipient_not_in_allowlist(monkeypatch, reload_module):
    monkeypatch.setenv('TEST_SEND_ALLOWED_DOMAINS', 'myrecruiter.ai,staging.myrecruiter.ai')
    lf = reload_module()
    assert lf._test_send_recipient_allowed('customer@gmail.com') is False
    assert lf._test_send_recipient_allowed('victim@example.com') is False


def test_allowlist_match_is_case_insensitive(monkeypatch, reload_module):
    monkeypatch.setenv('TEST_SEND_ALLOWED_DOMAINS', 'MyRecruiter.ai')
    lf = reload_module()
    assert lf._test_send_recipient_allowed('Chris@MYRECRUITER.AI') is True


def test_allowlist_does_not_match_substring(monkeypatch, reload_module):
    """A whitelisted 'myrecruiter.ai' must not allow 'evilmyrecruiter.ai'."""
    monkeypatch.setenv('TEST_SEND_ALLOWED_DOMAINS', 'myrecruiter.ai')
    lf = reload_module()
    assert lf._test_send_recipient_allowed('attacker@evilmyrecruiter.ai') is False
    assert lf._test_send_recipient_allowed('attacker@myrecruiter.ai.evil.com') is False


def test_allowlist_handles_whitespace_and_blanks(monkeypatch, reload_module):
    """Comma-list parsing strips whitespace and ignores empty entries."""
    monkeypatch.setenv('TEST_SEND_ALLOWED_DOMAINS', '  myrecruiter.ai , , staging.myrecruiter.ai  ,')
    lf = reload_module()
    assert lf._test_send_recipient_allowed('a@myrecruiter.ai') is True
    assert lf._test_send_recipient_allowed('b@staging.myrecruiter.ai') is True
    assert lf._test_send_recipient_allowed('c@gmail.com') is False


# ---------------------------------------------------------------------------
# _fetch_clerk_user — HTTPError body surfacing
# ---------------------------------------------------------------------------

def test_fetch_clerk_user_surfaces_http_error_body(monkeypatch, reload_module):
    """
    When Clerk returns a 4xx/5xx, the caller should see the response body
    in the raised ValueError so root-cause diagnostics aren't lost. Regression
    test for the carry-forward fix from the working tree.
    """
    import urllib.error
    import io

    monkeypatch.setenv('CLERK_SECRET_KEY', 'sk_test_dummy')
    lf = reload_module()
    lf._clerk_user_cache.clear()
    lf._clerk_user_cache_time.clear()

    err = urllib.error.HTTPError(
        url='https://api.clerk.com/v1/users/user_bad',
        code=404,
        msg='Not Found',
        hdrs=None,
        fp=io.BytesIO(b'{"errors":[{"message":"User not found"}]}'),
    )

    with patch.object(lf.urllib.request, 'urlopen', side_effect=err):
        with pytest.raises(ValueError) as exc:
            lf._fetch_clerk_user('user_bad')

    assert 'HTTP 404' in str(exc.value)
    assert 'User not found' in str(exc.value)


def test_handler_returns_400_when_recipient_blocked(monkeypatch, reload_module):
    """
    handle_notification_recipients_test_send returns 400 + domain_not_in_allowlist
    BEFORE any tenant config lookup or SES call when the recipient domain isn't
    in the allowlist. Verifies the guard short-circuits early.
    """
    monkeypatch.setenv('TEST_SEND_ALLOWED_DOMAINS', 'myrecruiter.ai')
    lf = reload_module()

    # No mocking of get_tenant_config or SES — the guard must reject before
    # those are reached. If the guard is missing/broken, this test will fail
    # with an AttributeError or unexpected status (proving the regression).
    response = lf.handle_notification_recipients_test_send(
        tenant_id='MYR384719',
        body={'email': 'customer@gmail.com', 'form_id': 'volunteer_signup'},
        user_role='admin',
    )
    assert response['statusCode'] == 400
    import json
    body = json.loads(response['body'])
    assert body['error'] == 'domain_not_in_allowlist'
