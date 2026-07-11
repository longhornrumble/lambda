"""
Consolidation batch (operator decisions 2026-07-11):
  #2 signing-key — "paranoid everywhere": lambda_function.get_jwt_signing_key
     delegates to conversation_handler._get_jwt_signing_key; the JWT_SECRET
     env-var fallback is DEAD.
  #3 CORS — "union both, one builder": both assemblers emit the canonical
     CORS_ALLOW_METHODS / CORS_ALLOW_HEADERS constants (set can't drift).
  #4 D13 — message appends are conditional; a same-ms collision re-keys +2ms
     and retries once instead of silently overwriting.
"""

import os
import unittest
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError


class TestSigningKeyConsolidation(unittest.TestCase):
    """#2 — single source of truth; env fallback dead."""

    def test_delegates_to_conversation_handler(self):
        import lambda_function
        with patch('conversation_handler._get_jwt_signing_key', return_value='k' * 64) as ch:
            self.assertEqual(lambda_function.get_jwt_signing_key(), 'k' * 64)
            ch.assert_called_once()

    def test_env_fallback_is_dead(self):
        """A Secrets failure must return None — NEVER the JWT_SECRET env var."""
        import lambda_function
        with patch.dict(os.environ, {'JWT_SECRET': 'env-key-should-never-be-used'}):
            with patch('conversation_handler._get_jwt_signing_key', side_effect=RuntimeError('secrets down')):
                self.assertIsNone(lambda_function.get_jwt_signing_key())

    def test_source_guard_no_env_fallback_text(self):
        """The retired env-fallback branch must not resurface in lambda_function."""
        import inspect
        import lambda_function
        src = inspect.getsource(lambda_function.get_jwt_signing_key)
        self.assertNotIn("os.environ.get('JWT_SECRET')", src)


class TestCorsUnionParity(unittest.TestCase):
    """#3 — both assemblers emit the SAME canonical union set."""

    CANON_METHODS = 'GET, POST, OPTIONS, DELETE'
    CANON_HEADERS = 'Content-Type, Authorization, X-Requested-With, x-api-key'

    def test_canonical_constants(self):
        from lambda_function import CORS_ALLOW_METHODS, CORS_ALLOW_HEADERS
        self.assertEqual(CORS_ALLOW_METHODS, self.CANON_METHODS)
        self.assertEqual(CORS_ALLOW_HEADERS, self.CANON_HEADERS)

    def test_add_cors_headers_emits_canonical_set(self):
        from lambda_function import add_cors_headers
        resp = add_cors_headers({'statusCode': 200}, {'headers': {}})
        self.assertEqual(resp['headers']['Access-Control-Allow-Methods'], self.CANON_METHODS)
        self.assertEqual(resp['headers']['Access-Control-Allow-Headers'], self.CANON_HEADERS)

    def test_response_formatter_emits_identical_set(self):
        """The drift the consolidation closed: both assemblers now agree."""
        from response_formatter import format_http_error
        result = format_http_error(400, 'nope')
        self.assertEqual(result['headers']['Access-Control-Allow-Methods'], self.CANON_METHODS)
        self.assertEqual(result['headers']['Access-Control-Allow-Headers'], self.CANON_HEADERS)


class TestD13AppendCollision(unittest.TestCase):
    """#4 — conditional message append: collision re-keys +2ms, retries once."""

    def _collision(self):
        return ClientError(
            {'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'exists'}},
            'PutItem',
        )

    def test_same_ms_collision_rekeys_and_retries(self):
        import conversation_handler as ch

        calls = []

        def fake_op(operation, **kwargs):
            if operation != 'put_item':
                return {}
            item = kwargs.get('Item', {})
            if 'messageTimestamp' in item:  # message write (not the summary)
                self.assertEqual(kwargs.get('ConditionExpression'), 'attribute_not_exists(sessionId)')
                calls.append(int(item['messageTimestamp']['N']))
                if len(calls) == 1:
                    raise self._collision()
            return {}

        with patch.object(ch, 'AWS_CLIENT_MANAGER_AVAILABLE', True), \
             patch.object(ch, 'protected_dynamodb_operation', side_effect=fake_op):
            ch._save_conversation_to_db(
                session_id='sess-d13',
                tenant_id='TEN12345',
                delta={'appendUser': {'text': 'hello'}},
                expected_turn=None,
            )

        # First attempt collided; retry landed at +2ms.
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1], calls[0] + 2)

    def test_double_collision_raises_conversation_error(self):
        import conversation_handler as ch

        def always_collide(operation, **kwargs):
            item = kwargs.get('Item', {})
            if operation == 'put_item' and 'messageTimestamp' in item:
                raise self._collision()
            return {}

        with patch.object(ch, 'AWS_CLIENT_MANAGER_AVAILABLE', True), \
             patch.object(ch, 'protected_dynamodb_operation', side_effect=always_collide):
            with self.assertRaises(ch.ConversationError):
                ch._save_conversation_to_db(
                    session_id='sess-d13b',
                    tenant_id='TEN12345',
                    delta={'appendUser': {'text': 'hello'}},
                    expected_turn=None,
                )


if __name__ == '__main__':
    unittest.main()
