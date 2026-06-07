"""
Unit tests for G3/E0 scheduling OAuth init-token mint (ADA API).

The mint (Python) must produce tokens that Calendar_OAuth_Connect/state.js verify (Node)
accepts byte-for-byte. These tests:
  - replicate state.js verify() FAITHFULLY in Python and assert a freshly-minted token passes
    (HMAC match, typ, exp, claims roundtrip) — the wire-format self-consistency proof;
  - pin a DETERMINISTIC golden token (fixed key/nonce/clock) so any format drift fails loudly
    (the SAME golden is verified by the real Node state.verify in
    Calendar_OAuth_Connect/__tests__/state-python-compat.test.js — the cross-language proof);
  - cover the handler: self-mint identity from auth (never client-supplied), lower-cased
    coordinator_id, 400 no-identity, 503 OAuth-not-configured, and the key-shape variants.
"""

import base64
import hashlib
import hmac
import json
from unittest.mock import patch

import pytest

import lambda_function as lf
from lambda_function import handle_scheduling_connection_init

KEY = 'test-signing-key-deadbeef'
NOW_MS = 1_900_000_000_000          # iat 1_900_000_000, exp 1_900_000_300 (ttl 300)
FIXED_NONCE = b'\x00' * 16          # -> "0" * 32 hex
OAUTH_URL = 'https://abc123.lambda-url.us-east-1.on.aws'

# The deterministic golden token (see test_golden_token_is_stable). Mirrored verbatim in the
# Node compat test; if either side changes the wire format, BOTH break.
GOLDEN_PAYLOAD = (
    '{"typ":"init","tenant_id":"TEN1","coordinator_id":"staff@example.com",'
    '"coordinator_email":"staff@example.com","iat":1900000000,"exp":1900000300,'
    '"nonce":"00000000000000000000000000000000"}'
)


# --------------------------------------------------------------------------- #
# helpers: a faithful Python mirror of Calendar_OAuth_Connect/state.js verify()
# --------------------------------------------------------------------------- #

def _b64url_decode(s: str) -> bytes:
    pad = '' if len(s) % 4 == 0 else '=' * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def js_verify(token: str, key: str, expected_type: str, now_ms: int) -> dict:
    """Faithful re-implementation of state.js verify() — the algorithm the Node side runs."""
    assert isinstance(token, str) and 0 < len(token) <= 4096, 'malformed'
    dot = token.find('.')
    assert dot > 0 and dot != len(token) - 1 and token.find('.', dot + 1) == -1, 'malformed'
    payload_b64, sig_b64 = token[:dot], token[dot + 1:]
    expected_sig = hmac.new(key.encode('utf-8'), payload_b64.encode('ascii'), hashlib.sha256).digest()
    given_sig = _b64url_decode(sig_b64)
    assert len(given_sig) == len(expected_sig) and hmac.compare_digest(given_sig, expected_sig), 'bad_signature'
    claims = json.loads(_b64url_decode(payload_b64).decode('utf-8'))
    assert isinstance(claims, dict), 'malformed'
    assert claims.get('typ') == expected_type, 'wrong_type'
    assert isinstance(claims.get('exp'), int) and claims['exp'] * 1000 > now_ms, 'expired'
    return claims


@pytest.fixture(autouse=True)
def _seed_key():
    """Seed the container-lifetime key cache so the mint never hits Secrets Manager."""
    lf._oauth_state_key_cache.clear()
    lf._oauth_state_key_cache['value'] = KEY
    yield
    lf._oauth_state_key_cache.clear()


@pytest.fixture(autouse=True)
def _grant_feature_and_role():
    """The mint is feature-gated (validate_feature_access) + reads the _request_user_role module
    global. Default every test to: role set + scheduling granted. The gate-denied test overrides."""
    lf._request_user_role = 'admin'
    with patch.object(lf, 'validate_feature_access', return_value=None):
        yield


# --------------------------------------------------------------------------- #
# wire-format compat
# --------------------------------------------------------------------------- #

def test_minted_token_verifies_under_js_rules():
    token = lf._sign_oauth_state('init', {
        'tenant_id': 'TEN1', 'coordinator_id': 'staff@example.com',
        'coordinator_email': 'staff@example.com',
    }, 300, now_ms=NOW_MS)
    claims = js_verify(token, KEY, 'init', now_ms=NOW_MS + 1000)
    assert claims['typ'] == 'init'
    assert claims['tenant_id'] == 'TEN1'
    assert claims['coordinator_id'] == 'staff@example.com'
    assert claims['coordinator_email'] == 'staff@example.com'
    assert claims['iat'] == NOW_MS // 1000
    assert claims['exp'] == NOW_MS // 1000 + 300
    assert len(claims['nonce']) == 32  # 16 random bytes hex


def test_token_is_two_b64url_parts():
    token = lf._sign_oauth_state('init', {'tenant_id': 'T'}, 300, now_ms=NOW_MS)
    parts = token.split('.')
    assert len(parts) == 2
    import re
    assert re.fullmatch(r'[A-Za-z0-9_-]+', parts[0])  # b64url, no padding
    assert re.fullmatch(r'[A-Za-z0-9_-]+', parts[1])


def test_wrong_key_fails_verify():
    token = lf._sign_oauth_state('init', {'tenant_id': 'T'}, 300, now_ms=NOW_MS)
    with pytest.raises(AssertionError, match='bad_signature'):
        js_verify(token, 'a-different-key', 'init', now_ms=NOW_MS + 1000)


def test_expired_token_fails_verify():
    token = lf._sign_oauth_state('init', {'tenant_id': 'T'}, 300, now_ms=NOW_MS)
    # now is past exp (iat + 300s)
    with pytest.raises(AssertionError, match='expired'):
        js_verify(token, KEY, 'init', now_ms=(NOW_MS // 1000 + 301) * 1000)


def test_wrong_type_fails_verify():
    token = lf._sign_oauth_state('init', {'tenant_id': 'T'}, 300, now_ms=NOW_MS)
    with pytest.raises(AssertionError, match='wrong_type'):
        js_verify(token, KEY, 'state', now_ms=NOW_MS + 1000)


def test_golden_token_is_stable():
    """Deterministic golden — locks the byte-for-byte wire format. Mirrored in the Node test."""
    with patch.object(lf.os, 'urandom', return_value=FIXED_NONCE):
        token = lf._sign_oauth_state('init', {
            'tenant_id': 'TEN1', 'coordinator_id': 'staff@example.com',
            'coordinator_email': 'staff@example.com',
        }, 300, now_ms=NOW_MS)
    payload_b64, sig_b64 = token.split('.')
    # the payload decodes to the exact compact JSON the Node side will JSON.parse
    assert _b64url_decode(payload_b64).decode('utf-8') == GOLDEN_PAYLOAD
    # and the signature is reproducible from the documented algorithm
    expected_sig = base64.urlsafe_b64encode(
        hmac.new(KEY.encode('utf-8'), payload_b64.encode('ascii'), hashlib.sha256).digest()
    ).rstrip(b'=').decode('ascii')
    assert sig_b64 == expected_sig
    # emit the full golden so the Node compat fixture can be regenerated if needed
    print('GOLDEN_TOKEN=' + token)


# --------------------------------------------------------------------------- #
# key shapes (mirror state.js getSigningKey)
# --------------------------------------------------------------------------- #

def test_key_raw_string():
    lf._oauth_state_key_cache.clear()
    with patch.object(lf._secrets_client, 'get_secret_value', return_value={'SecretString': 'rawkey123'}):
        assert lf.get_oauth_state_signing_key() == 'rawkey123'


def test_key_json_shape():
    lf._oauth_state_key_cache.clear()
    with patch.object(lf._secrets_client, 'get_secret_value',
                      return_value={'SecretString': '{"key":"jsonkey456"}'}):
        assert lf.get_oauth_state_signing_key() == 'jsonkey456'


def test_key_empty_raises():
    lf._oauth_state_key_cache.clear()
    with patch.object(lf._secrets_client, 'get_secret_value', return_value={'SecretString': ''}):
        with pytest.raises(ValueError):
            lf.get_oauth_state_signing_key()


def test_key_whitespace_only_raises():
    # A whitespace-only key is a useless/misconfigured secret (G3 SR2).
    lf._oauth_state_key_cache.clear()
    with patch.object(lf._secrets_client, 'get_secret_value', return_value={'SecretString': '   \t\n'}):
        with pytest.raises(ValueError):
            lf.get_oauth_state_signing_key()


def test_key_json_missing_field_raises():
    lf._oauth_state_key_cache.clear()
    with patch.object(lf._secrets_client, 'get_secret_value',
                      return_value={'SecretString': '{"notkey":"x"}'}):
        with pytest.raises(ValueError):
            lf.get_oauth_state_signing_key()


# --------------------------------------------------------------------------- #
# handler
# --------------------------------------------------------------------------- #

def test_handler_happy_path_returns_connect_and_status_urls():
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL):
        resp = handle_scheduling_connection_init('TEN1', 'Staff@Example.com')
    assert resp['statusCode'] == 200
    body = json.loads(resp['body'])
    assert body['expires_in'] == 300
    assert body['connect_url'].startswith(f'{OAUTH_URL}/connect?init=')
    assert body['status_url'].startswith(f'{OAUTH_URL}/connection/status?init=')
    # both URLs carry the SAME token
    t_connect = body['connect_url'].split('init=')[1]
    t_status = body['status_url'].split('init=')[1]
    assert t_connect == t_status
    # the minted token is self-scoped: coordinator_id is the LOWER-CASED caller email
    claims = js_verify(t_connect, KEY, 'init', now_ms=int(__import__('time').time() * 1000))
    assert claims['coordinator_id'] == 'staff@example.com'      # lower-cased
    assert claims['coordinator_email'] == 'Staff@Example.com'   # original-case identity
    assert claims['tenant_id'] == 'TEN1'


def test_handler_strips_trailing_slash_on_base_url():
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL + '/'):
        resp = handle_scheduling_connection_init('TEN1', 'staff@example.com')
    body = json.loads(resp['body'])
    assert '//connect' not in body['connect_url'].replace('https://', '')


def test_handler_no_identity_400():
    # includes whitespace-only + padded-'unknown' — must NOT mint a token with an empty
    # coordinator_id (G3 test-B2).
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL):
        for bad in (None, '', '   ', '\t\n', 'unknown', '  unknown  '):
            resp = handle_scheduling_connection_init('TEN1', bad)
            assert resp['statusCode'] == 400, f'expected 400 for {bad!r}'


def test_handler_empty_tenant_400():
    # tenant_id from auth should never be blank, but guard it anyway (G3 SR1).
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL):
        for bad_tid in (None, '', '   '):
            resp = handle_scheduling_connection_init(bad_tid, 'staff@example.com')
            assert resp['statusCode'] == 400, f'expected 400 for tenant {bad_tid!r}'


def test_handler_feature_gate_denied_returns_403():
    # A tenant without scheduling must be refused BEFORE minting (G3 code-High).
    denied = lf.cors_response(403, {'error': 'Feature not available'})
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(lf, 'validate_feature_access', return_value=denied) as gate:
        resp = handle_scheduling_connection_init('TEN1', 'staff@example.com')
    assert resp['statusCode'] == 403
    gate.assert_called_once_with('TEN1', 'dashboard_scheduling', lf._request_user_role)


def test_handler_secrets_error_503_no_secret_name_leak():
    # A Secrets Manager failure must NOT bubble to the router's 500 (which echoes str(e),
    # leaking the secret name). Handler catches -> generic 503. (G3 test-B1.)
    from botocore.exceptions import ClientError
    lf._oauth_state_key_cache.clear()
    err = ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'x'}}, 'GetSecretValue')
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL), \
         patch.object(lf._secrets_client, 'get_secret_value', side_effect=err):
        resp = handle_scheduling_connection_init('TEN1', 'staff@example.com')
    assert resp['statusCode'] == 503
    assert 'picasso/scheduling/oauth' not in resp['body']  # secret name must not leak
    assert '_state-signing-key' not in resp['body']


def test_handler_oauth_not_configured_503():
    with patch.object(lf, 'OAUTH_FUNCTION_URL', ''):
        resp = handle_scheduling_connection_init('TEN1', 'staff@example.com')
    assert resp['statusCode'] == 503


def test_token_is_url_safe_no_encoding_needed():
    """The token rides raw in the query string — assert it's all RFC-3986-unreserved."""
    import re
    with patch.object(lf, 'OAUTH_FUNCTION_URL', OAUTH_URL):
        resp = handle_scheduling_connection_init('TEN1', 'staff@example.com')
    token = json.loads(resp['body'])['connect_url'].split('init=')[1]
    assert re.fullmatch(r'[A-Za-z0-9_.-]+', token)  # base64url alphabet + the '.' separator
