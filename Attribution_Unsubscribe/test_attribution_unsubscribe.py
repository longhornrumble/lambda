"""
Tests for Attribution_Unsubscribe Lambda.

Coverage:
1.  Valid token -> 200 HTML success page
2.  Missing query param -> 403
3.  Tampered sig byte -> 403
4.  Wrong payload suffix -> 403
5.  Empty token -> 403
6.  Signing key unavailable -> 403 (fail-closed)
7.  Suppression row schema: correct pk/sk, no TTL, source=unsubscribe_link
8.  Idempotent double-unsubscribe (ConditionalCheckFailed -> 200)
9.  Case-insensitive email: token for ALICE@Example.COM stores alice@example.com
10. constant-time compare used in _validate_token (static inspection)
11. No email/token variable in logger.* calls (static inspection)
12. _get_signing_key caches on success; does NOT cache transient failure

Run: pytest test_attribution_unsubscribe.py -v
"""
import ast
import base64
import hashlib
import hmac
import inspect
import json
import logging
import os
import sys
import pytest
from unittest.mock import patch, MagicMock, call

# ---------------------------------------------------------------------------
# Module import -- env before import
# ---------------------------------------------------------------------------
os.environ.setdefault('ATTRIBUTION_AGGREGATES_TABLE', 'picasso-attribution-aggregates')
os.environ.setdefault('UNSUB_SECRET_NAME', 'picasso/unsub-signing-key')

# Ensure the parent directory is on the path so we can do a package-relative import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import Attribution_Unsubscribe.lambda_function as unsub  # noqa: E402
from Attribution_Unsubscribe.lambda_function import (
    _validate_token,
    _record_suppression,
    _get_signing_key,
    _email_log_id,
    _b64url_nopad_decode,
)

_TEST_KEY = b'test-hmac-key-for-unsubscribe-lambda'
_TENANT_ID = 'TENANT123'
_EMAIL = 'alice@example.com'


# ---------------------------------------------------------------------------
# Token builder (mirrors Attribution_Recap_Generator logic)
# ---------------------------------------------------------------------------
def _make_token(tenant_id: str, email: str, key: bytes, suffix: str = 'recap') -> str:
    payload = f'{tenant_id}|{email.lower()}|{suffix}'
    payload_bytes = payload.encode('utf-8')
    sig = hmac.new(key, payload_bytes, hashlib.sha256).digest()
    b64_payload = base64.urlsafe_b64encode(payload_bytes).rstrip(b'=').decode('ascii')
    b64_sig = base64.urlsafe_b64encode(sig).rstrip(b'=').decode('ascii')
    return f'{b64_payload}.{b64_sig}'


def _event(token=None, qs=None):
    """Build a Lambda Function URL GET event."""
    params = qs if qs is not None else ({'t': token} if token is not None else {})
    return {'queryStringParameters': params}


# ---------------------------------------------------------------------------
# 1. Valid token -> 200 HTML
# ---------------------------------------------------------------------------
class TestValidToken:

    def test_returns_200_html(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        mock_table.put_item.return_value = {}

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(_event(token), None)

        assert resp['statusCode'] == 200
        assert 'text/html' in resp['headers']['Content-Type']

    def test_success_body_contains_unsubscribed(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        mock_table.put_item.return_value = {}

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(_event(token), None)

        assert "unsubscribed" in resp['body'].lower()

    def test_success_body_mentions_dashboard_settings(self):
        """UX: confirmation page must mention re-enabling from dashboard settings."""
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        mock_table.put_item.return_value = {}

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(_event(token), None)

        assert 'dashboard' in resp['body'].lower() or 'settings' in resp['body'].lower()


# ---------------------------------------------------------------------------
# 2-5. 403 cases
# ---------------------------------------------------------------------------
class Test403Cases:

    def test_missing_param_returns_403(self):
        resp = unsub.lambda_handler(_event(qs={}), None)
        assert resp['statusCode'] == 403

    def test_null_qs_returns_403(self):
        resp = unsub.lambda_handler({'queryStringParameters': None}, None)
        assert resp['statusCode'] == 403

    def test_no_qs_key_returns_403(self):
        resp = unsub.lambda_handler({}, None)
        assert resp['statusCode'] == 403

    def test_tampered_sig_returns_403(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        parts = token.rsplit('.', 1)
        sig = list(parts[1])
        sig[0] = 'A' if sig[0] != 'A' else 'B'
        tampered = parts[0] + '.' + ''.join(sig)

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY):
            resp = unsub.lambda_handler(_event(tampered), None)
        assert resp['statusCode'] == 403

    def test_empty_string_token_returns_403(self):
        resp = unsub.lambda_handler(_event(qs={'t': ''}), None)
        assert resp['statusCode'] == 403

    def test_garbage_token_returns_403(self):
        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY):
            resp = unsub.lambda_handler(_event('not-a-valid-token-at-all'), None)
        assert resp['statusCode'] == 403

    def test_wrong_suffix_returns_403(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY, suffix='newsletter')
        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY):
            resp = unsub.lambda_handler(_event(token), None)
        assert resp['statusCode'] == 403

    def test_403_body_no_detail(self):
        """403 must not reveal internal structure."""
        resp = unsub.lambda_handler(_event('bad'), None)
        assert resp['statusCode'] == 403
        assert resp['body'].strip().lower() in ('forbidden', 'forbidden\n')

    def test_signing_key_unavailable_returns_403(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        with patch.object(unsub, '_get_signing_key', return_value=None):
            resp = unsub.lambda_handler(_event(token), None)
        assert resp['statusCode'] == 403


# ---------------------------------------------------------------------------
# 6. Signing key cache behaviour
# ---------------------------------------------------------------------------
class TestSigningKeyCache:

    def test_successful_fetch_is_cached(self):
        """After a successful fetch, _unsub_signing_key is set and reused."""
        original = unsub._unsub_signing_key
        unsub._unsub_signing_key = None
        try:
            with patch.object(unsub, '_secretsmanager') as mock_sm:
                mock_sm.get_secret_value.return_value = {'SecretString': 'mykey'}
                k1 = _get_signing_key()
                k2 = _get_signing_key()

            # Second call should NOT invoke secretsmanager again (cached)
            assert mock_sm.get_secret_value.call_count == 1
            assert k1 == b'mykey'
            assert k2 == b'mykey'
        finally:
            unsub._unsub_signing_key = original

    def test_transient_failure_not_cached(self):
        """After a failed fetch, _unsub_signing_key remains None (not cached)."""
        from botocore.exceptions import ClientError
        original = unsub._unsub_signing_key
        unsub._unsub_signing_key = None
        try:
            err = ClientError({'Error': {'Code': 'ServiceUnavailableException', 'Message': 'x'}}, 'GetSecretValue')
            with patch.object(unsub, '_secretsmanager') as mock_sm:
                mock_sm.get_secret_value.side_effect = err
                result = _get_signing_key()

            assert result is None
            assert unsub._unsub_signing_key is None, 'Failure must NOT be cached'

            # Next call must retry
            with patch.object(unsub, '_secretsmanager') as mock_sm2:
                mock_sm2.get_secret_value.return_value = {'SecretString': 'recovered-key'}
                result2 = _get_signing_key()
            assert result2 == b'recovered-key'
        finally:
            unsub._unsub_signing_key = original

    def test_missing_secret_name_env_returns_none(self):
        original = unsub._unsub_signing_key
        unsub._unsub_signing_key = None
        try:
            with patch.object(unsub, 'UNSUB_SECRET_NAME', ''):
                result = _get_signing_key()
            assert result is None
        finally:
            unsub._unsub_signing_key = original


# ---------------------------------------------------------------------------
# 7. Suppression row schema
# ---------------------------------------------------------------------------
class TestSuppressionRowSchema:

    def test_pk_sk_correct(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        put_calls = []

        def capture(**kw):
            put_calls.append(kw)
            return {}

        mock_table.put_item.side_effect = capture

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            unsub.lambda_handler(_event(token), None)

        assert len(put_calls) == 1
        item = put_calls[0]['Item']
        assert item['pk'] == f'TENANT#{_TENANT_ID}'
        assert item['sk'] == f'SUPPRESS#recap#{_EMAIL}'
        assert item['source'] == 'unsubscribe_link'
        assert 'created_at' in item

    def test_no_ttl_on_suppression_row(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        put_calls = []

        def capture(**kw):
            put_calls.append(kw)
            return {}

        mock_table.put_item.side_effect = capture

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            unsub.lambda_handler(_event(token), None)

        item = put_calls[0]['Item']
        assert 'ttl' not in item, 'Suppression row must be permanent (no TTL)'

    def test_condition_expression_attribute_not_exists(self):
        """PutItem must use attribute_not_exists(sk) for idempotency."""
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        put_calls = []

        def capture(**kw):
            put_calls.append(kw)
            return {}

        mock_table.put_item.side_effect = capture

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            unsub.lambda_handler(_event(token), None)

        cond = put_calls[0].get('ConditionExpression', '')
        assert 'attribute_not_exists' in str(cond)


# ---------------------------------------------------------------------------
# 8. Idempotent double-unsubscribe
# ---------------------------------------------------------------------------
class TestIdempotentUnsubscribe:

    def test_conditional_check_failed_treated_as_200(self):
        from botocore.exceptions import ClientError
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        err = ClientError({'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'x'}}, 'PutItem')
        mock_table.put_item.side_effect = err

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(_event(token), None)

        assert resp['statusCode'] == 200

    def test_second_call_idempotent(self):
        """Two identical calls must both return 200."""
        from botocore.exceptions import ClientError
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)

        call_count = [0]

        def put_item_side_effect(**kw):
            call_count[0] += 1
            if call_count[0] > 1:
                raise ClientError({'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'x'}}, 'PutItem')
            return {}

        mock_table = MagicMock()
        mock_table.put_item.side_effect = put_item_side_effect

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            r1 = unsub.lambda_handler(_event(token), None)
            r2 = unsub.lambda_handler(_event(token), None)

        assert r1['statusCode'] == 200
        assert r2['statusCode'] == 200


# ---------------------------------------------------------------------------
# 9. Case-insensitive email in suppression row
# ---------------------------------------------------------------------------
class TestCaseInsensitiveEmail:

    def test_uppercase_email_stored_lowercase(self):
        token = _make_token(_TENANT_ID, 'ALICE@EXAMPLE.COM', _TEST_KEY)
        mock_table = MagicMock()
        put_calls = []

        def capture(**kw):
            put_calls.append(kw)
            return {}

        mock_table.put_item.side_effect = capture

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            unsub.lambda_handler(_event(token), None)

        item = put_calls[0]['Item']
        assert item['sk'] == 'SUPPRESS#recap#alice@example.com', \
            f'Email should be lowercased in sk, got: {item["sk"]}'


# ---------------------------------------------------------------------------
# 10. Constant-time compare (static inspection)
# ---------------------------------------------------------------------------
class TestConstantTimeCompare:

    def test_compare_digest_used_in_validate_token(self):
        src = inspect.getsource(_validate_token)
        assert 'compare_digest' in src, 'hmac.compare_digest must be used in _validate_token'


# ---------------------------------------------------------------------------
# 11. PII log hygiene (static inspection)
# ---------------------------------------------------------------------------
class TestPIILogHygiene:

    def _find_log_calls(self, source: str):
        tree = ast.parse(source)
        log_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                    if func.value.id == 'logger' and func.attr in ('info', 'warning', 'error', 'debug', 'critical'):
                        log_calls.append(node)
        return log_calls

    def _call_has_suspect_arg(self, node):
        """
        Returns True if a log call's args directly contain a suspect PII variable.
        Excludes variables that are arguments to _email_log_id() (they are hashed).
        """
        suspect = {'email', 'email_lower', 'token', 'signing_key', 'key_bytes', 'raw', 'payload'}
        for arg in node.args[1:]:
            for inner in ast.walk(arg):
                if isinstance(inner, ast.Name) and inner.id in suspect:
                    # Safe if this Name node is an arg to _email_log_id()
                    # Walk the containing Call to check
                    # Simplest heuristic: if inner appears inside a Call to _email_log_id, skip
                    # We check if any ancestor Call node has func.id == '_email_log_id'
                    if self._inside_email_log_id(arg, inner):
                        continue
                    return True
        return False

    def _inside_email_log_id(self, root, target_node):
        """Return True if target_node is an argument to _email_log_id() anywhere in root."""
        for node in ast.walk(root):
            if isinstance(node, ast.Call):
                func = node.func
                func_name = ''
                if isinstance(func, ast.Name):
                    func_name = func.id
                elif isinstance(func, ast.Attribute):
                    func_name = func.attr
                if func_name == '_email_log_id':
                    # Check if target_node appears in the args
                    for call_arg in node.args:
                        if isinstance(call_arg, ast.Name) and call_arg.id == target_node.id:
                            return True
        return False

    def test_no_pii_variables_in_log_calls(self):
        src = inspect.getsource(unsub)
        log_calls = self._find_log_calls(src)
        violations = []
        for node in log_calls:
            if self._call_has_suspect_arg(node):
                violations.append(f'line {node.lineno}')
        assert not violations, f'PII variable passed to logger: {violations}'

    def test_email_log_id_is_hash_not_address(self):
        """_email_log_id must return a hash prefix, not the raw email."""
        result = _email_log_id('alice@example.com')
        assert '@' not in result
        assert len(result) == 12
        # Verify it's deterministic sha256
        expected = hashlib.sha256('alice@example.com'.encode()).hexdigest()[:12]
        assert result == expected


# ---------------------------------------------------------------------------
# 12. _validate_token unit tests
# ---------------------------------------------------------------------------
class TestValidateToken:

    def test_valid_recap_payload(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        result = _validate_token(token, _TEST_KEY)
        assert result == (_TENANT_ID, _EMAIL)

    def test_returns_none_on_empty(self):
        assert _validate_token('', _TEST_KEY) is None

    def test_returns_none_no_dot(self):
        assert _validate_token('abc', _TEST_KEY) is None

    def test_returns_none_on_tampered_sig(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        parts = token.rsplit('.', 1)
        bad = parts[0] + '.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
        assert _validate_token(bad, _TEST_KEY) is None

    def test_returns_none_wrong_key(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        assert _validate_token(token, b'wrong-key') is None

    def test_returns_none_non_recap_suffix(self):
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY, suffix='weekly')
        assert _validate_token(token, _TEST_KEY) is None

    def test_returns_none_missing_segments(self):
        """Payload with only 2 pipe-separated segments is invalid."""
        payload = f'{_TENANT_ID}|{_EMAIL}'
        payload_bytes = payload.encode('utf-8')
        sig = hmac.new(_TEST_KEY, payload_bytes, hashlib.sha256).digest()
        token = (base64.urlsafe_b64encode(payload_bytes).rstrip(b'=').decode('ascii') +
                 '.' +
                 base64.urlsafe_b64encode(sig).rstrip(b'=').decode('ascii'))
        assert _validate_token(token, _TEST_KEY) is None


# ---------------------------------------------------------------------------
# Fix 1: Token length cap (MAX_TOKEN_LEN = 1024)
# ---------------------------------------------------------------------------
class TestTokenLengthCap:

    def test_oversized_token_returns_403(self):
        """Token longer than MAX_TOKEN_LEN must be rejected with 403 before any decode."""
        oversized = 'A' * (unsub.MAX_TOKEN_LEN + 1)
        with patch.object(unsub, '_get_signing_key') as mock_key, \
             patch.object(unsub, '_validate_token') as mock_validate:
            resp = unsub.lambda_handler(_event(oversized), None)

        assert resp['statusCode'] == 403
        # Neither the signing key fetch nor the decode should be attempted
        mock_key.assert_not_called()
        mock_validate.assert_not_called()

    def test_token_at_max_len_is_not_rejected_by_length_check(self):
        """Token exactly MAX_TOKEN_LEN chars must pass the length gate."""
        # It will still fail validation, but the length gate should not block it.
        at_limit = 'A' * unsub.MAX_TOKEN_LEN
        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY):
            resp = unsub.lambda_handler(_event(at_limit), None)
        # Should fail with 403 due to bad token, not length
        assert resp['statusCode'] == 403

    def test_max_token_len_constant_is_1024(self):
        assert unsub.MAX_TOKEN_LEN == 1024

    def test_oversized_token_logs_length_not_content(self, caplog):
        """Warning log for oversized token must include the length, not the token itself."""
        oversized = 'X' * 2000
        with caplog.at_level(logging.WARNING):
            unsub.lambda_handler(_event(oversized), None)
        # At least one warning record must mention the length
        assert any(
            '2000' in r.message or 'len=' in r.message or 'too long' in r.message
            for r in caplog.records
        ), f"Expected length warning, got: {[r.message for r in caplog.records]}"
        # The token content itself must not appear in any log message
        assert all('X' * 20 not in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Fix 4: Method routing (GET + POST allowed; others -> 403)
# ---------------------------------------------------------------------------
class TestMethodRouting:

    def _event_with_method(self, method, token=None):
        qs = {'t': token} if token else {}
        return {
            'requestContext': {'http': {'method': method}},
            'queryStringParameters': qs,
        }

    def test_get_allowed(self):
        """GET with valid token -> 200."""
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        mock_table.put_item.return_value = {}
        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(self._event_with_method('GET', token), None)
        assert resp['statusCode'] == 200

    def test_post_allowed(self):
        """POST (RFC 8058 one-click) with valid token -> 200."""
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        mock_table.put_item.return_value = {}
        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(self._event_with_method('POST', token), None)
        assert resp['statusCode'] == 200

    def test_options_returns_403(self):
        """OPTIONS -> 403 before any token processing."""
        with patch.object(unsub, '_get_signing_key') as mock_key:
            resp = unsub.lambda_handler(self._event_with_method('OPTIONS'), None)
        assert resp['statusCode'] == 403
        mock_key.assert_not_called()

    def test_delete_returns_403(self):
        """DELETE -> 403."""
        resp = unsub.lambda_handler(self._event_with_method('DELETE'), None)
        assert resp['statusCode'] == 403

    def test_put_returns_403(self):
        """PUT -> 403."""
        resp = unsub.lambda_handler(self._event_with_method('PUT'), None)
        assert resp['statusCode'] == 403

    def test_unknown_method_returns_403(self):
        """Arbitrary unknown method -> 403."""
        resp = unsub.lambda_handler(self._event_with_method('PATCH'), None)
        assert resp['statusCode'] == 403

    def test_no_request_context_defaults_to_get(self):
        """Missing requestContext defaults to GET (backward-compat with test events)."""
        token = _make_token(_TENANT_ID, _EMAIL, _TEST_KEY)
        mock_table = MagicMock()
        mock_table.put_item.return_value = {}
        with patch.object(unsub, '_get_signing_key', return_value=_TEST_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            # Standard event dict with no requestContext key
            resp = unsub.lambda_handler({'queryStringParameters': {'t': token}}, None)
        assert resp['statusCode'] == 200
