#!/usr/bin/env python3
"""CF origin header validation — Function URL WAF bypass defense.

Phase 4 cumulative audit blocker #1: Lambda Function URLs are publicly
addressable and have no built-in WAF. An attacker who learns the Function
URL can bypass CloudFront (and the WAF attached to it). This validator
enforces that requests carry a CloudFront-injected secret header so that
direct Function URL hits are rejected.

Feature-flagged via REQUIRE_CF_ORIGIN_HEADER (default off during rollout).
"""

import json
import os
import unittest
from unittest.mock import patch


class TestValidateCfOriginHeader(unittest.TestCase):

    def setUp(self):
        # Reset module-level cache between tests so secret reads don't leak.
        import lambda_function
        lambda_function._cf_origin_secret_cache = None

    def _event(self, headers=None):
        return {
            'httpMethod': 'GET',
            'headers': headers or {},
        }

    def test_skip_when_feature_flag_unset(self):
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('REQUIRE_CF_ORIGIN_HEADER', None)
            ok, reason = validate_cf_origin_header(self._event())
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_skip_when_feature_flag_false(self):
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'false'}):
            ok, reason = validate_cf_origin_header(self._event())
        self.assertTrue(ok)

    def test_skip_when_feature_flag_arbitrary(self):
        """Anything other than literal 'true' (case-insensitive) is treated as off."""
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'maybe'}):
            ok, reason = validate_cf_origin_header(self._event())
        self.assertTrue(ok)

    def test_rejects_missing_header_when_enabled(self):
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value='canonical-secret'):
            ok, reason = validate_cf_origin_header(self._event())
        self.assertFalse(ok)
        self.assertIn('missing', reason)

    def test_accepts_matching_header_lowercase(self):
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value='canonical-secret'):
            ok, reason = validate_cf_origin_header(
                self._event({'x-picasso-cf-origin': 'canonical-secret'})
            )
        self.assertTrue(ok, f"unexpected rejection: {reason}")

    def test_accepts_matching_header_mixed_case(self):
        """API Gateway / Function URL header casing varies — must be case-insensitive."""
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value='canonical-secret'):
            ok, reason = validate_cf_origin_header(
                self._event({'X-Picasso-CF-Origin': 'canonical-secret'})
            )
        self.assertTrue(ok)

    def test_rejects_mismatched_header(self):
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value='canonical-secret'):
            ok, reason = validate_cf_origin_header(
                self._event({'x-picasso-cf-origin': 'wrong-value'})
            )
        self.assertFalse(ok)
        self.assertIn('mismatch', reason)

    def test_fails_closed_when_secret_unavailable(self):
        """Secrets Manager unavailable + flag on → reject. No fail-open."""
        from lambda_function import validate_cf_origin_header
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value=None):
            ok, reason = validate_cf_origin_header(
                self._event({'x-picasso-cf-origin': 'any-value'})
            )
        self.assertFalse(ok)
        self.assertIn('unavailable', reason)


class TestGetCfOriginSecret(unittest.TestCase):

    def setUp(self):
        import lambda_function
        lambda_function._cf_origin_secret_cache = None

    def test_returns_plain_string_when_not_json(self):
        from lambda_function import get_cf_origin_secret
        with patch('boto3.client') as mock_client:
            mock_client.return_value.get_secret_value.return_value = {
                'SecretString': 'plain-secret-value',
            }
            secret = get_cf_origin_secret()
        self.assertEqual(secret, 'plain-secret-value')

    def test_extracts_secret_key_from_json(self):
        from lambda_function import get_cf_origin_secret
        with patch('boto3.client') as mock_client:
            mock_client.return_value.get_secret_value.return_value = {
                'SecretString': json.dumps({'secret': 'json-secret-value'}),
            }
            secret = get_cf_origin_secret()
        self.assertEqual(secret, 'json-secret-value')

    def test_returns_none_when_secrets_manager_fails(self):
        from lambda_function import get_cf_origin_secret
        with patch('boto3.client') as mock_client:
            mock_client.return_value.get_secret_value.side_effect = Exception('AccessDenied')
            secret = get_cf_origin_secret()
        self.assertIsNone(secret)

    def test_caches_after_first_call(self):
        from lambda_function import get_cf_origin_secret
        with patch('boto3.client') as mock_client:
            mock_client.return_value.get_secret_value.return_value = {
                'SecretString': 'cached-value',
            }
            first = get_cf_origin_secret()
            second = get_cf_origin_secret()
        self.assertEqual(first, second)
        # boto3.client itself called twice in lazy import, but get_secret_value
        # called only once due to module-level cache.
        self.assertEqual(
            mock_client.return_value.get_secret_value.call_count,
            1,
        )


class TestLambdaHandlerCfOriginEnforcement(unittest.TestCase):
    """Integration: the handler returns 403 when origin validation fails."""

    def setUp(self):
        import lambda_function
        lambda_function._cf_origin_secret_cache = None

    def test_handler_returns_403_when_header_missing_and_flag_on(self):
        from lambda_function import lambda_handler

        event = {
            'httpMethod': 'GET',
            'queryStringParameters': {'action': 'health_check'},
            'headers': {},
        }
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value='secret-value'):
            response = lambda_handler(event, None)

        self.assertEqual(response['statusCode'], 403)
        body = json.loads(response['body'])
        self.assertEqual(body['error'], 'Forbidden')

    def test_handler_passes_through_when_flag_off(self):
        """Default rollout state — flag off, no header required."""
        from lambda_function import lambda_handler

        event = {
            'httpMethod': 'GET',
            'queryStringParameters': {'action': 'health_check'},
            'headers': {},
        }
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('REQUIRE_CF_ORIGIN_HEADER', None)
            response = lambda_handler(event, None)

        # Health check should succeed (200) without the header when flag is off.
        self.assertEqual(response['statusCode'], 200)

    def test_handler_passes_through_when_header_valid(self):
        from lambda_function import lambda_handler

        event = {
            'httpMethod': 'GET',
            'queryStringParameters': {'action': 'health_check'},
            'headers': {'x-picasso-cf-origin': 'secret-value'},
        }
        with patch.dict(os.environ, {'REQUIRE_CF_ORIGIN_HEADER': 'true'}), \
             patch('lambda_function.get_cf_origin_secret', return_value='secret-value'):
            response = lambda_handler(event, None)

        self.assertEqual(response['statusCode'], 200)


if __name__ == '__main__':
    unittest.main()
