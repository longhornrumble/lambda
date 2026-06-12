"""
Unit tests for §E11b scheduling OAuth disconnect ADA endpoint.

POST /scheduling/connection/disconnect (Clerk-authed; SELF-ONLY; E11b).

Contract assertions (FROZEN_CONTRACTS §E11b):
  - Auth gating: unauthenticated / missing-identity → 400 before any work
  - SELF-ONLY: coordinator identity from verified Clerk auth (never client-supplied)
  - Body-carried token: the init token MUST NOT appear in any URL or log line
  - Upstream 4xx relay: 4xx from the OAuth Lambda is relayed generically
  - Upstream 5xx relay: 5xx from the OAuth Lambda → 5xx with generic error body
  - Timeout handling: network/timeout → 503, generic body, no URL leak
  - Feature gate: tenant without 'dashboard_scheduling' → 403
  - OAUTH_FUNCTION_URL unconfigured → 503
  - Mint failure (SM error) → 503, no secret name leak
  - Happy path: 200 { status, watch } relayed from upstream
  - Generic errors: OAUTH_FUNCTION_URL never appears in any response body
"""

import json
import urllib.error
import urllib.request
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

import lambda_function as lf
from lambda_function import handle_scheduling_connection_disconnect

KEY = 'test-signing-key-deadbeef'
OAUTH_URL = 'https://abc123.lambda-url.us-east-1.on.aws'
TENANT = 'TEN1'
EMAIL = 'Staff@Example.com'
LOWER_EMAIL = 'staff@example.com'


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def _seed_key():
    """Seed the container-lifetime signing-key cache."""
    lf._oauth_state_key_cache.clear()
    lf._oauth_state_key_cache['value'] = KEY
    yield
    lf._oauth_state_key_cache.clear()


@pytest.fixture(autouse=True)
def _grant_feature_and_role():
    """Default: role = admin, scheduling feature granted."""
    lf._request_user_role = 'admin'
    with patch.object(lf, 'validate_feature_access', return_value=None):
        yield


def _make_upstream_response(body: dict, status: int = 200):
    """Return a mock urllib response object."""
    encoded = json.dumps(body).encode('utf-8')
    resp = MagicMock()
    resp.read.return_value = encoded
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# --------------------------------------------------------------------------- #
# auth gating
# --------------------------------------------------------------------------- #

def test_no_identity_returns_400():
    """Missing or blank caller email → 400 before any network call."""
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen') as mock_open:
        for bad in (None, '', '   ', '\t\n', 'unknown', '  unknown  '):
            resp = handle_scheduling_connection_disconnect(TENANT, bad)
            assert resp['statusCode'] == 400, f'expected 400 for email={bad!r}'
        mock_open.assert_not_called()


def test_no_tenant_returns_400():
    """Missing or blank tenant_id → 400."""
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen') as mock_open:
        for bad_tid in (None, '', '   '):
            resp = handle_scheduling_connection_disconnect(bad_tid, EMAIL)
            assert resp['statusCode'] == 400, f'expected 400 for tenant={bad_tid!r}'
        mock_open.assert_not_called()


# --------------------------------------------------------------------------- #
# SELF-ONLY identity (slot-poisoning defense)
# --------------------------------------------------------------------------- #

def test_coordinator_id_is_lower_cased_caller_email():
    """coordinator_id in the minted token must be the LOWER-CASED caller email."""
    captured_tokens = []

    original_sign = lf._sign_oauth_state

    def capture_sign(typ, claims, ttl, **kwargs):
        if typ == 'init':
            captured_tokens.append(claims.copy())
        return original_sign(typ, claims, ttl, **kwargs)

    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(lf, '_sign_oauth_state', side_effect=capture_sign), \
         patch.object(urllib.request, 'urlopen',
                      return_value=_make_upstream_response({'status': 'disconnected', 'watch': 'stopped'})):
        handle_scheduling_connection_disconnect(TENANT, 'Staff@Example.com')

    assert len(captured_tokens) == 1
    assert captured_tokens[0]['coordinator_id'] == 'staff@example.com'   # lower-cased
    assert captured_tokens[0]['coordinator_email'] == 'Staff@Example.com'  # original case
    assert captured_tokens[0]['tenant_id'] == TENANT


# --------------------------------------------------------------------------- #
# body-carried token: must NOT appear in any URL or log
# --------------------------------------------------------------------------- #

def test_token_is_body_carried_not_in_url(caplog):
    """The init token must be in the POST body, not the request URL."""
    captured_reqs = []

    def fake_urlopen(req, timeout=None):
        captured_reqs.append(req)
        return _make_upstream_response({'status': 'disconnected', 'watch': 'stopped'})

    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=fake_urlopen):
        handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert len(captured_reqs) == 1
    req = captured_reqs[0]
    # The URL must NOT contain the token (never in the query string)
    assert 'init=' not in req.full_url
    # The URL must point to the correct disconnect endpoint
    assert req.full_url == f'{OAUTH_URL}/connection/disconnect'
    # The body must be JSON with an 'init' key
    body = json.loads(req.data.decode('utf-8'))
    assert 'init' in body
    assert isinstance(body['init'], str) and len(body['init']) > 10
    # Method must be POST
    assert req.get_method() == 'POST'


def test_token_never_logged(caplog):
    """The minted token must not appear in any log output."""
    minted = []

    original_sign = lf._sign_oauth_state

    def capture_sign(typ, claims, ttl, **kwargs):
        tok = original_sign(typ, claims, ttl, **kwargs)
        if typ == 'init':
            minted.append(tok)
        return tok

    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(lf, '_sign_oauth_state', side_effect=capture_sign), \
         patch.object(urllib.request, 'urlopen',
                      return_value=_make_upstream_response({'status': 'disconnected', 'watch': 'stopped'})):
        handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert len(minted) == 1
    # The token must not appear in any log record
    for record in caplog.records:
        assert minted[0] not in record.getMessage(), 'init token must never be logged'


# --------------------------------------------------------------------------- #
# upstream 4xx relay
# --------------------------------------------------------------------------- #

def test_upstream_4xx_relayed_generically():
    """A 4xx from the OAuth Lambda is relayed with a generic error body (no URL leak)."""
    err = urllib.error.HTTPError(
        url=f'{OAUTH_URL}/connection/disconnect',
        code=400,
        msg='Bad Request',
        hdrs=None,
        fp=BytesIO(b'{"error":"invalid_request"}'),
    )
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=err):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert resp['statusCode'] == 400
    body = json.loads(resp['body'])
    assert 'error' in body
    # Must NOT leak the OAuth URL or upstream detail
    assert OAUTH_URL not in resp['body']
    assert 'invalid_request' not in resp['body']  # no upstream detail leak


def test_upstream_403_relayed():
    err = urllib.error.HTTPError(
        url=f'{OAUTH_URL}/connection/disconnect',
        code=403, msg='Forbidden', hdrs=None,
        fp=BytesIO(b'{}'),
    )
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=err):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert resp['statusCode'] == 403


# --------------------------------------------------------------------------- #
# upstream 5xx relay
# --------------------------------------------------------------------------- #

def test_upstream_500_relayed():
    """A 5xx from the OAuth Lambda → 5xx from ADA with a generic body."""
    err = urllib.error.HTTPError(
        url=f'{OAUTH_URL}/connection/disconnect',
        code=500, msg='Internal Server Error', hdrs=None,
        fp=BytesIO(b'{}'),
    )
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=err):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert resp['statusCode'] == 500
    body = json.loads(resp['body'])
    assert 'error' in body
    assert OAUTH_URL not in resp['body']


def test_upstream_502_relayed():
    err = urllib.error.HTTPError(
        url=f'{OAUTH_URL}/connection/disconnect',
        code=502, msg='Bad Gateway', hdrs=None,
        fp=BytesIO(b'{}'),
    )
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=err):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert resp['statusCode'] == 502


# --------------------------------------------------------------------------- #
# timeout handling
# --------------------------------------------------------------------------- #

def test_timeout_returns_503_no_url_leak():
    """A network timeout → 503 with a generic body; the OAUTH_FUNCTION_URL must not appear."""
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=TimeoutError('timed out')):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert resp['statusCode'] == 503
    body = json.loads(resp['body'])
    assert 'error' in body
    assert OAUTH_URL not in resp['body']


def test_connection_error_returns_503():
    """Any network error (OSError/ConnectionError) → 503 generic."""
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=OSError('connection refused')):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert resp['statusCode'] == 503
    assert OAUTH_URL not in resp['body']


# --------------------------------------------------------------------------- #
# feature gate
# --------------------------------------------------------------------------- #

def test_feature_gate_denied_returns_403():
    """A tenant without 'dashboard_scheduling' must be refused before minting."""
    denied = lf.cors_response(403, {'error': 'Feature not available'})
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(lf, 'validate_feature_access', return_value=denied) as gate, \
         patch.object(urllib.request, 'urlopen') as mock_open:
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert resp['statusCode'] == 403
    gate.assert_called_once_with(TENANT, 'dashboard_scheduling', lf._request_user_role)
    mock_open.assert_not_called()


# --------------------------------------------------------------------------- #
# OAUTH_FUNCTION_URL unconfigured
# --------------------------------------------------------------------------- #

def test_unconfigured_oauth_url_returns_503():
    """If OAUTH_FUNCTION_URL is blank, return 503 before minting."""
    with patch.object(lf, 'OAUTH_FUNCTION_URL', ''), \
         patch.object(urllib.request, 'urlopen') as mock_open:
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert resp['statusCode'] == 503
    mock_open.assert_not_called()


# --------------------------------------------------------------------------- #
# Mint failure (Secrets Manager error)
# --------------------------------------------------------------------------- #

def test_mint_failure_returns_503_no_secret_name_leak():
    """A Secrets Manager failure during mint → 503; secret name must not appear in response."""
    from botocore.exceptions import ClientError
    lf._oauth_state_key_cache.clear()
    sm_err = ClientError(
        {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'x'}}, 'GetSecretValue'
    )
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(lf._secrets_client, 'get_secret_value', side_effect=sm_err), \
         patch.object(urllib.request, 'urlopen') as mock_open:
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert resp['statusCode'] == 503
    assert 'picasso/scheduling/oauth' not in resp['body']
    assert '_state-signing-key' not in resp['body']
    mock_open.assert_not_called()


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #

def test_happy_path_relays_status_and_watch():
    """200 { status:'disconnected', watch:'stopped' } relayed from upstream."""
    upstream = {'status': 'disconnected', 'watch': 'stopped'}
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen',
                      return_value=_make_upstream_response(upstream)):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert resp['statusCode'] == 200
    body = json.loads(resp['body'])
    assert body['status'] == 'disconnected'
    assert body['watch'] == 'stopped'


def test_happy_path_watch_pending_relayed():
    """watch:'pending' is also relayed correctly."""
    upstream = {'status': 'disconnected', 'watch': 'pending'}
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen',
                      return_value=_make_upstream_response(upstream)):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert json.loads(resp['body'])['watch'] == 'pending'


def test_happy_path_watch_none_relayed():
    """watch:'none' (idempotent / already disconnected) is relayed correctly."""
    upstream = {'status': 'disconnected', 'watch': 'none'}
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen',
                      return_value=_make_upstream_response(upstream)):
        resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
    assert json.loads(resp['body'])['watch'] == 'none'


def test_strips_trailing_slash_on_base_url():
    """Trailing slash on OAUTH_FUNCTION_URL must not produce a double-slash in the target URL."""
    captured_reqs = []

    def fake_urlopen(req, timeout=None):
        captured_reqs.append(req)
        return _make_upstream_response({'status': 'disconnected', 'watch': 'stopped'})

    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL + '/'), \
         patch.object(urllib.request, 'urlopen', side_effect=fake_urlopen):
        handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert len(captured_reqs) == 1
    url = captured_reqs[0].full_url
    assert '//' not in url.replace('https://', '')


# --------------------------------------------------------------------------- #
# Generic error responses: OAUTH_FUNCTION_URL must never appear in any response
# --------------------------------------------------------------------------- #

def test_oauth_url_never_in_error_responses():
    """Under various failure modes, OAUTH_FUNCTION_URL must not appear in any response body."""
    scenarios = [
        # (side_effect or None, patch_urlopen_return, expected_status)
        (TimeoutError('t/o'), None, 503),
        (urllib.error.HTTPError(OAUTH_URL, 500, 'err', None, BytesIO(b'{}')), None, 500),
        (urllib.error.HTTPError(OAUTH_URL, 400, 'bad', None, BytesIO(b'{}')), None, 400),
    ]
    for side_effect, _, _ in scenarios:
        with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
             patch.object(urllib.request, 'urlopen', side_effect=side_effect):
            resp = handle_scheduling_connection_disconnect(TENANT, EMAIL)
        assert OAUTH_URL not in resp['body'], f'URL leaked for side_effect={side_effect}'


# --------------------------------------------------------------------------- #
# Content-Type header
# --------------------------------------------------------------------------- #

def test_post_request_has_json_content_type():
    """The server-side POST must send Content-Type: application/json."""
    captured_reqs = []

    def fake_urlopen(req, timeout=None):
        captured_reqs.append(req)
        return _make_upstream_response({'status': 'disconnected', 'watch': 'stopped'})

    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(urllib.request, 'urlopen', side_effect=fake_urlopen):
        handle_scheduling_connection_disconnect(TENANT, EMAIL)

    assert len(captured_reqs) == 1
    assert captured_reqs[0].get_header('Content-type') == 'application/json'
