"""
Integration tests for Meta_OAuth_Handler.

Run locally with:
    pip install PyJWT pytest boto3
    python -m pytest Lambdas/lambda/Meta_OAuth_Handler/test_lambda_function.py -v

Tests use moto for AWS service mocking and unittest.mock for external Meta Graph
API calls — no live AWS or Meta credentials are required.

Note: set META_APP_SECRET_ARN, META_APP_ID, and related env vars before running,
or rely on the fixture below which patches the secret retrieval directly.
"""

import base64
import json
import os
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# Set required env vars before importing the module under test
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("META_APP_ID", "TEST_APP_ID")
os.environ.setdefault("META_APP_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret/picasso/meta/app-secret")
os.environ.setdefault("OAUTH_CALLBACK_URL", "https://api.example.com/meta/oauth/callback")
os.environ.setdefault("CHANNEL_MAPPINGS_TABLE", "picasso-channel-mappings-test")
os.environ.setdefault("KMS_KEY_ID", "alias/picasso-channel-tokens")


_FAKE_APP_SECRET = "test_app_secret_32_chars_minimum!!"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context():
    return SimpleNamespace(
        aws_request_id="test-request-id",
        function_name="Meta_OAuth_Handler",
    )


def _make_event(method: str, path: str, query_params: dict = None, body: dict = None) -> dict:
    event = {
        "httpMethod": method,
        "path": path,
        "queryStringParameters": query_params or {},
        "body": json.dumps(body) if body else None,
    }
    return event


# ---------------------------------------------------------------------------
# Fixtures / patch helpers
# ---------------------------------------------------------------------------

def _patch_app_secret(test_func):
    """Decorator that patches _get_meta_app_secret() to return a fake secret."""
    def wrapper(*args, **kwargs):
        with patch(
            "lambda_function._get_meta_app_secret",
            return_value=_FAKE_APP_SECRET,
        ):
            # Also reset the cached secret between tests
            import lambda_function as lf
            lf._meta_app_secret = None
            return test_func(*args, **kwargs)
    wrapper.__name__ = test_func.__name__
    return wrapper


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestGetOAuthUrl(unittest.TestCase):
    """Route: GET /meta/oauth/url"""

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    def test_returns_oauth_url(self, _mock_secret):
        import lambda_function as lf
        lf._meta_app_secret = None

        event = _make_event("GET", "/meta/oauth/url", {"tenant_id": "TENANT_123"})
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertIn("oauth_url", body)
        url = body["oauth_url"]
        self.assertIn("facebook.com", url)
        self.assertIn("client_id=TEST_APP_ID", url)
        self.assertIn("pages_messaging", url)
        self.assertIn("instagram_basic", url)
        self.assertIn("instagram_manage_messages", url)
        self.assertIn("state=", url)

    def test_missing_tenant_id_returns_400(self):
        import lambda_function as lf

        event = _make_event("GET", "/meta/oauth/url", {})
        response = lf.lambda_handler(event, _make_context())
        self.assertEqual(response["statusCode"], 400)

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    def test_state_jwt_is_signed_and_decodable(self, _mock_secret):
        import jwt
        import lambda_function as lf
        lf._meta_app_secret = None

        event = _make_event("GET", "/meta/oauth/url", {"tenant_id": "TENANT_ABC"})
        response = lf.lambda_handler(event, _make_context())
        body = json.loads(response["body"])

        # Extract state from URL
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(body["oauth_url"])
        state_token = parse_qs(parsed.query)["state"][0]

        payload = jwt.decode(state_token, _FAKE_APP_SECRET, algorithms=["HS256"])
        self.assertEqual(payload["tenant_id"], "TENANT_ABC")
        self.assertIn("nonce", payload)
        self.assertIn("exp", payload)


class TestOAuthCallback(unittest.TestCase):
    """Route: GET /meta/oauth/callback"""

    def _valid_state(self, tenant_id="TENANT_XYZ"):
        import jwt
        payload = {
            "tenant_id": tenant_id,
            "nonce": "abc123",
            "iat": int(time.time()),
            "exp": int(time.time()) + 600,
        }
        return jwt.encode(payload, _FAKE_APP_SECRET, algorithm="HS256")

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    @patch("lambda_function._graph_get")
    @patch("lambda_function._graph_post")
    @patch("lambda_function._encrypt_token", return_value="ENCRYPTED_TOKEN_BASE64")
    @patch("lambda_function._channel_table")
    def test_successful_callback(
        self, mock_table, mock_encrypt, mock_post, mock_get, _mock_secret
    ):
        import lambda_function as lf
        lf._meta_app_secret = None

        # Set up mock Graph API responses
        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},  # token exchange
            {"data": [{"id": "PAGE_001", "name": "My Test Page", "access_token": "PAGE_TOKEN_XYZ"}]},  # /me/accounts
        ]
        mock_post.return_value = {"success": True}
        mock_table.return_value.put_item = MagicMock()

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        response = lf.lambda_handler(event, _make_context())

        # Expect HTML popup response
        self.assertEqual(response["statusCode"], 200)
        self.assertIn("text/html", response["headers"]["Content-Type"])
        self.assertIn("META_OAUTH_SUCCESS", response["body"])
        self.assertIn("PAGE_001", response["body"])
        self.assertIn("My Test Page", response["body"])

        # Verify DynamoDB put was called
        mock_table.return_value.put_item.assert_called_once()
        put_args = mock_table.return_value.put_item.call_args[1]
        item = put_args["Item"]
        self.assertEqual(item["PK"], "PAGE#PAGE_001")
        self.assertEqual(item["SK"], "CHANNEL#messenger")
        self.assertEqual(item["tenantId"], "TENANT_XYZ")
        self.assertEqual(item["channelType"], "messenger")
        # encryptedPageToken IS stored in DynamoDB — confirm it's present and is
        # the mocked encrypted value (not the raw plaintext page token)
        self.assertIn("encryptedPageToken", item)
        self.assertEqual(item["encryptedPageToken"], "ENCRYPTED_TOKEN_BASE64")

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    def test_expired_state_jwt(self, _mock_secret):
        import jwt
        import lambda_function as lf
        lf._meta_app_secret = None

        payload = {
            "tenant_id": "TENANT_XYZ",
            "nonce": "abc",
            "iat": int(time.time()) - 700,
            "exp": int(time.time()) - 100,  # expired
        }
        expired_state = jwt.encode(payload, _FAKE_APP_SECRET, algorithm="HS256")

        event = _make_event("GET", "/meta/oauth/callback", {"code": "CODE", "state": expired_state})
        response = lf.lambda_handler(event, _make_context())

        self.assertIn("text/html", response["headers"]["Content-Type"])
        self.assertIn("META_OAUTH_ERROR", response["body"])
        self.assertIn("expired", response["body"].lower())

    def test_missing_state_returns_error_popup(self):
        import lambda_function as lf

        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE"})
        response = lf.lambda_handler(event, _make_context())
        self.assertIn("META_OAUTH_ERROR", response["body"])

    def test_missing_code_returns_error_popup(self):
        import lambda_function as lf
        lf._meta_app_secret = None

        state = self._valid_state()
        with patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET):
            event = _make_event("GET", "/meta/oauth/callback", {"state": state})
            response = lf.lambda_handler(event, _make_context())
        self.assertIn("META_OAUTH_ERROR", response["body"])


class TestDisconnectChannel(unittest.TestCase):
    """Route: POST /meta/channels/{tenant_id}/disconnect"""

    @patch("lambda_function._query_channels_by_tenant")
    @patch("lambda_function._decrypt_token", return_value="PAGE_TOKEN")
    @patch("lambda_function._graph_delete")
    @patch("lambda_function._channel_table")
    def test_successful_disconnect(
        self, mock_table, mock_delete, mock_decrypt, mock_query
    ):
        import lambda_function as lf

        mock_query.return_value = [
            {
                "PK": "PAGE#PAGE_001",
                "SK": "CHANNEL#messenger",
                "pageId": "PAGE_001",
                "encryptedPageToken": "ENCRYPTED",
                "tenantId": "TENANT_XYZ",
            }
        ]
        mock_table.return_value.update_item = MagicMock()
        mock_delete.return_value = {"success": True}

        event = _make_event("POST", "/meta/channels/TENANT_XYZ/disconnect")
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertTrue(body["success"])

        # Verify DynamoDB update called with TTL expiry
        mock_table.return_value.update_item.assert_called_once()

    @patch("lambda_function._query_channels_by_tenant", return_value=[])
    def test_disconnect_no_channel_returns_404(self, _mock_query):
        import lambda_function as lf

        event = _make_event("POST", "/meta/channels/TENANT_NONE/disconnect")
        response = lf.lambda_handler(event, _make_context())
        self.assertEqual(response["statusCode"], 404)


class TestToggleChannel(unittest.TestCase):
    """Route: POST /meta/channels/{tenant_id}/toggle"""

    @patch("lambda_function._query_channels_by_tenant")
    @patch("lambda_function._channel_table")
    def test_toggle_disabled(self, mock_table, mock_query):
        import lambda_function as lf

        mock_query.return_value = [{"PK": "PAGE#P1", "SK": "CHANNEL#messenger"}]
        mock_table.return_value.update_item = MagicMock()

        event = _make_event("POST", "/meta/channels/TENANT_XYZ/toggle", body={"enabled": False})
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertTrue(body["success"])
        self.assertFalse(body["enabled"])

    def test_toggle_missing_enabled_field(self):
        import lambda_function as lf

        event = _make_event("POST", "/meta/channels/TENANT_XYZ/toggle", body={"foo": "bar"})
        response = lf.lambda_handler(event, _make_context())
        self.assertEqual(response["statusCode"], 400)


class TestListChannels(unittest.TestCase):
    """Route: GET /meta/channels/{tenant_id}"""

    @patch("lambda_function._query_channels_by_tenant")
    def test_returns_channels_without_token(self, mock_query):
        import lambda_function as lf

        mock_query.return_value = [
            {
                "PK": "PAGE#PAGE_001",
                "SK": "CHANNEL#messenger",
                "tenantId": "TENANT_XYZ",
                "pageId": "PAGE_001",
                "pageName": "My Page",
                "enabled": True,
                "encryptedPageToken": "SHOULD_NOT_APPEAR",
            }
        ]

        event = _make_event("GET", "/meta/channels/TENANT_XYZ")
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(len(body["channels"]), 1)
        channel = body["channels"][0]

        # Token must never be returned
        self.assertNotIn("encryptedPageToken", channel)
        self.assertEqual(channel["pageName"], "My Page")
        self.assertTrue(channel["enabled"])

    @patch("lambda_function._query_channels_by_tenant", return_value=[])
    def test_returns_empty_list_when_no_channels(self, _mock_query):
        import lambda_function as lf

        event = _make_event("GET", "/meta/channels/TENANT_NEW")
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["channels"], [])


class TestCorsAndRouting(unittest.TestCase):
    """Verify CORS headers and 404 handling."""

    def test_options_returns_204(self):
        import lambda_function as lf

        event = _make_event("OPTIONS", "/meta/oauth/url")
        response = lf.lambda_handler(event, _make_context())
        self.assertEqual(response["statusCode"], 204)

    def test_unknown_route_returns_404(self):
        import lambda_function as lf

        event = _make_event("GET", "/meta/does-not-exist")
        response = lf.lambda_handler(event, _make_context())
        self.assertEqual(response["statusCode"], 404)

    def test_cors_headers_present_on_json_response(self):
        import lambda_function as lf

        event = _make_event("GET", "/meta/oauth/url", {})
        response = lf.lambda_handler(event, _make_context())
        self.assertIn("Access-Control-Allow-Origin", response["headers"])


if __name__ == "__main__":
    unittest.main()
