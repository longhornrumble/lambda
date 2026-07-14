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

from botocore.exceptions import ClientError

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
    def test_config_id_replaces_scope_when_env_set(self, _mock_secret):
        import lambda_function as lf
        lf._meta_app_secret = None

        with patch.object(lf, "_META_LOGIN_CONFIG_ID", "CONFIG_992"):
            event = _make_event("GET", "/meta/oauth/url", {"tenant_id": "TENANT_123"})
            response = lf.lambda_handler(event, _make_context())

        body = json.loads(response["body"])
        url = body["oauth_url"]
        # FLB dialog: config_id present, scope absent (Meta: "should not be used")
        self.assertIn("config_id=CONFIG_992", url)
        self.assertNotIn("scope=", url)

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
            {},  # /<page_id>?fields=instagram_business_account — none linked
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
    @patch("lambda_function._graph_get")
    @patch("lambda_function._graph_post")
    @patch("lambda_function._encrypt_token", return_value="ENCRYPTED_TOKEN_BASE64")
    @patch("lambda_function._channel_table")
    def test_granular_scope_fallback_when_me_accounts_empty(
        self, mock_table, mock_encrypt, mock_post, mock_get, _mock_secret
    ):
        """FLB grant on a portfolio-owned Page: /me/accounts is empty but
        debug_token granular scopes carry the Page ID → direct fetch succeeds."""
        import lambda_function as lf
        lf._meta_app_secret = None

        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},  # token exchange
            {"data": []},  # /me/accounts — empty (the FLB symptom)
            {  # /debug_token
                "data": {
                    "granular_scopes": [
                        {"scope": "pages_show_list", "target_ids": ["PAGE_777"]},
                        {"scope": "pages_messaging", "target_ids": ["PAGE_777"]},
                    ]
                }
            },
            {"id": "PAGE_777", "name": "Portfolio Page", "access_token": "PAGE_TOKEN_777"},  # direct fetch
            {},  # instagram_business_account lookup — none linked
        ]
        mock_post.return_value = {"success": True}
        mock_table.return_value.put_item = MagicMock()

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        self.assertIn("META_OAUTH_SUCCESS", response["body"])
        self.assertIn("PAGE_777", response["body"])

        # debug_token must be called with the app token (app_id|app_secret)
        debug_call = mock_get.call_args_list[2]
        self.assertEqual(debug_call[0][0], "/debug_token")
        self.assertEqual(debug_call[0][1]["access_token"], f"TEST_APP_ID|{_FAKE_APP_SECRET}")

        item = mock_table.return_value.put_item.call_args[1]["Item"]
        self.assertEqual(item["PK"], "PAGE#PAGE_777")
        self.assertEqual(item["encryptedPageToken"], "ENCRYPTED_TOKEN_BASE64")

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    @patch("lambda_function._graph_get")
    def test_no_pages_even_after_granular_fallback(self, mock_get, _mock_secret):
        """Empty /me/accounts AND no pages_show_list targets → clean error popup."""
        import lambda_function as lf
        lf._meta_app_secret = None

        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},  # token exchange
            {"data": []},  # /me/accounts — empty
            {"data": {"granular_scopes": []}},  # /debug_token — nothing granted
        ]

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        response = lf.lambda_handler(event, _make_context())

        self.assertIn("META_OAUTH_ERROR", response["body"])
        self.assertIn("No Facebook Pages found", response["body"])

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    @patch("lambda_function._graph_get")
    def test_granular_fallback_page_fetch_failure_yields_error(self, mock_get, _mock_secret):
        """Granted Page whose direct fetch throws is skipped; none left → error popup."""
        import lambda_function as lf
        lf._meta_app_secret = None

        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},  # token exchange
            {"data": []},  # /me/accounts — empty
            {"data": {"granular_scopes": [{"scope": "pages_show_list", "target_ids": ["PAGE_888"]}]}},
            Exception("Graph 500 on page fetch"),  # direct fetch fails
        ]

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        response = lf.lambda_handler(event, _make_context())

        self.assertIn("META_OAUTH_ERROR", response["body"])
        self.assertIn("No Facebook Pages found", response["body"])

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    @patch("lambda_function._graph_get")
    @patch("lambda_function._graph_post")
    @patch("lambda_function._encrypt_token", return_value="ENCRYPTED_TOKEN_BASE64")
    @patch("lambda_function._channel_table")
    @patch("lambda_function._dynamodb")
    def test_tenant_hash_resolved_from_registry(
        self, mock_ddb, mock_table, mock_encrypt, mock_post, mock_get, _mock_secret
    ):
        """Stored tenantHash must be the PLATFORM registry hash, not the
        computed sha256 (bedrock-core resolves configs by the registry hash)."""
        import lambda_function as lf
        lf._meta_app_secret = None

        mock_ddb.Table.return_value.get_item.return_value = {
            "Item": {"tenantId": "TENANT_XYZ", "tenantHash": "reg1234567890abc"}
        }
        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},
            {"data": [{"id": "PAGE_001", "name": "My Test Page", "access_token": "PAGE_TOKEN_XYZ"}]},
            {},  # instagram_business_account lookup — none linked
        ]
        mock_post.return_value = {"success": True}
        mock_table.return_value.put_item = MagicMock()

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        with patch.object(lf, "_TENANT_REGISTRY_TABLE", "picasso-tenant-registry-test"):
            response = lf.lambda_handler(event, _make_context())

        self.assertIn("META_OAUTH_SUCCESS", response["body"])
        item = mock_table.return_value.put_item.call_args[1]["Item"]
        self.assertEqual(item["tenantHash"], "reg1234567890abc")

    def test_tenant_hash_falls_back_to_computed_when_registry_empty(self):
        """No registry row (or table unset) → legacy computed hash."""
        import hashlib
        import lambda_function as lf

        with patch.object(lf, "_TENANT_REGISTRY_TABLE", "picasso-tenant-registry-test"), \
             patch.object(lf, "_dynamodb") as mock_ddb:
            mock_ddb.Table.return_value.get_item.return_value = {}
            result = lf._tenant_hash("TENANT_XYZ")
        self.assertEqual(result, hashlib.sha256(b"TENANT_XYZ").hexdigest()[:16])

        with patch.object(lf, "_TENANT_REGISTRY_TABLE", ""):
            result = lf._tenant_hash("TENANT_XYZ")
        self.assertEqual(result, hashlib.sha256(b"TENANT_XYZ").hexdigest()[:16])

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    @patch("lambda_function._graph_get")
    @patch("lambda_function._graph_post")
    @patch("lambda_function._encrypt_token", return_value="ENCRYPTED_TOKEN_BASE64")
    @patch("lambda_function._channel_table")
    def test_instagram_row_written_when_page_linked(
        self, mock_table, mock_encrypt, mock_post, mock_get, _mock_secret
    ):
        """A Page with a linked IG Professional account gets a SECOND channel
        row keyed by the IG account id — the webhook resolves IG DMs by it."""
        import lambda_function as lf
        lf._meta_app_secret = None

        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},
            {"data": [{"id": "PAGE_001", "name": "My Test Page", "access_token": "PAGE_TOKEN_XYZ"}]},
            {"instagram_business_account": {"id": "IG_555"}},
        ]
        mock_post.return_value = {"success": True}
        mock_table.return_value.put_item = MagicMock()

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        response = lf.lambda_handler(event, _make_context())

        self.assertIn("META_OAUTH_SUCCESS", response["body"])
        calls = mock_table.return_value.put_item.call_args_list
        self.assertEqual(len(calls), 2)
        ig_item = calls[1][1]["Item"]
        self.assertEqual(ig_item["PK"], "PAGE#IG_555")
        self.assertEqual(ig_item["SK"], "CHANNEL#instagram")
        self.assertEqual(ig_item["channelType"], "instagram")
        self.assertEqual(ig_item["igAccountId"], "IG_555")
        self.assertEqual(ig_item["tenantId"], "TENANT_XYZ")
        self.assertEqual(ig_item["encryptedPageToken"], "ENCRYPTED_TOKEN_BASE64")

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


class TestPushWelcomeSurfaces(unittest.TestCase):
    """M5: push_welcome_surfaces() — Messenger Profile API push on connect."""

    @staticmethod
    def _s3_get_object_response(config: dict) -> dict:
        return {"Body": MagicMock(read=MagicMock(return_value=json.dumps(config).encode()))}

    def test_config_bucket_not_configured_skips(self):
        import lambda_function as lf

        with patch.object(lf, "_CONFIG_BUCKET", ""), \
             patch("lambda_function._graph_post") as mock_post:
            result = lf.push_welcome_surfaces("PAGE_TOKEN", "TENANT_XYZ")

        self.assertEqual(result, {"skipped": "config bucket not configured"})
        mock_post.assert_not_called()

    def test_config_s3_miss_skips_and_flow_would_still_succeed(self):
        import lambda_function as lf

        not_found = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
        )
        with patch.object(lf, "_CONFIG_BUCKET", "test-bucket"), \
             patch.object(lf, "_s3_client") as mock_s3, \
             patch("lambda_function._graph_post") as mock_post:
            mock_s3.get_object.side_effect = not_found
            result = lf.push_welcome_surfaces("PAGE_TOKEN", "TENANT_XYZ")

        self.assertEqual(result, {"skipped": "tenant config not found or unreadable"})
        mock_post.assert_not_called()

    def test_flag_off_skips_and_makes_no_graph_call(self):
        import lambda_function as lf

        config = {
            "feature_flags": {"MESSENGER_CHANNEL": False},
            "messenger_behavior": {
                "welcome": {"ice_breakers": [{"question": "Q", "payload": "PIC1:cta:x"}]}
            },
        }
        with patch.object(lf, "_CONFIG_BUCKET", "test-bucket"), \
             patch.object(lf, "_s3_client") as mock_s3, \
             patch("lambda_function._graph_post") as mock_post:
            mock_s3.get_object.return_value = self._s3_get_object_response(config)
            result = lf.push_welcome_surfaces("PAGE_TOKEN", "TENANT_XYZ")

        self.assertEqual(result, {"skipped": "MESSENGER_CHANNEL flag not enabled"})
        mock_post.assert_not_called()

    def test_welcome_absent_skips_and_makes_no_graph_call(self):
        import lambda_function as lf

        config = {"feature_flags": {"MESSENGER_CHANNEL": True}}
        with patch.object(lf, "_CONFIG_BUCKET", "test-bucket"), \
             patch.object(lf, "_s3_client") as mock_s3, \
             patch("lambda_function._graph_post") as mock_post:
            mock_s3.get_object.return_value = self._s3_get_object_response(config)
            result = lf.push_welcome_surfaces("PAGE_TOKEN", "TENANT_XYZ")

        self.assertEqual(result, {"skipped": "no welcome surfaces configured"})
        mock_post.assert_not_called()

    def test_flag_on_pushes_capped_ice_breakers_and_menu(self):
        """5 configured ice breakers -> capped to 4 (C5); menu mixes postback
        (payload) + web_url (url) items; a malformed menu item (neither) is
        skipped; titles are truncated to 20 chars; payloads pass through
        verbatim (routing is M4's job)."""
        import lambda_function as lf

        config = {
            "feature_flags": {"MESSENGER_CHANNEL": True},
            "messenger_behavior": {
                "welcome": {
                    "ice_breakers": [
                        {"question": f"Q{i}", "payload": f"PIC1:cta:ib{i}"} for i in range(5)
                    ],
                    "persistent_menu": [
                        {"title": "This Title Is Way Too Long For Meta", "payload": "PIC1:cta:menu1"},
                        {"title": "Visit Site", "url": "https://example.com"},
                        {"title": "Bad Item"},  # malformed: no payload AND no url
                    ],
                }
            },
        }
        with patch.object(lf, "_CONFIG_BUCKET", "test-bucket"), \
             patch.object(lf, "_s3_client") as mock_s3, \
             patch("lambda_function._graph_post") as mock_post:
            mock_s3.get_object.return_value = self._s3_get_object_response(config)
            mock_post.return_value = {"success": True}
            result = lf.push_welcome_surfaces("PAGE_TOKEN", "TENANT_XYZ")

        mock_post.assert_called_once()
        call_args, call_kwargs = mock_post.call_args
        path, params = call_args[0], call_args[1]
        profile_payload = call_kwargs["json_body"]

        self.assertEqual(path, "/me/messenger_profile")
        self.assertEqual(params, {"access_token": "PAGE_TOKEN"})
        self.assertEqual(profile_payload["get_started"], {"payload": "GET_STARTED"})

        # Capped at 4 of the 5 configured ice breakers; payloads verbatim
        ice_ctas = profile_payload["ice_breakers"][0]["call_to_actions"]
        self.assertEqual(len(ice_ctas), 4)
        self.assertEqual(ice_ctas[0], {"question": "Q0", "payload": "PIC1:cta:ib0"})
        self.assertEqual(ice_ctas[3], {"question": "Q3", "payload": "PIC1:cta:ib3"})

        # Menu: postback + web_url items; malformed item skipped; title capped
        menu_ctas = profile_payload["persistent_menu"][0]["call_to_actions"]
        self.assertEqual(len(menu_ctas), 2)
        self.assertEqual(menu_ctas[0]["type"], "postback")
        self.assertEqual(menu_ctas[0]["payload"], "PIC1:cta:menu1")
        self.assertLessEqual(len(menu_ctas[0]["title"]), 20)
        self.assertEqual(
            menu_ctas[1], {"type": "web_url", "title": "Visit Site", "url": "https://example.com"}
        )

        self.assertEqual(
            result,
            {"pushed": {"get_started": True, "ice_breakers": 4, "persistent_menu": 2}},
        )

    def test_graph_error_on_push_returns_error_dict_never_raises(self):
        import lambda_function as lf

        config = {
            "feature_flags": {"MESSENGER_CHANNEL": True},
            "messenger_behavior": {
                "welcome": {"ice_breakers": [{"question": "Q", "payload": "PIC1:cta:x"}]}
            },
        }
        with patch.object(lf, "_CONFIG_BUCKET", "test-bucket"), \
             patch.object(lf, "_s3_client") as mock_s3, \
             patch("lambda_function._graph_post", side_effect=Exception("HTTP 400: Bad Request")):
            mock_s3.get_object.return_value = self._s3_get_object_response(config)
            result = lf.push_welcome_surfaces("PAGE_TOKEN", "TENANT_XYZ")

        self.assertIn("error", result)


class TestWelcomeSurfacesOAuthWiring(unittest.TestCase):
    """M5: push_welcome_surfaces() is called best-effort from the OAuth
    callback and must never affect the callback's own success/failure."""

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
    def test_flag_off_oauth_flow_still_succeeds_no_profile_push(
        self, mock_table, mock_encrypt, mock_post, mock_get, _mock_secret
    ):
        import lambda_function as lf
        lf._meta_app_secret = None

        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},
            {"data": [{"id": "PAGE_001", "name": "My Test Page", "access_token": "PAGE_TOKEN_XYZ"}]},
            {},  # instagram_business_account lookup — none linked
        ]
        mock_post.return_value = {"success": True}
        mock_table.return_value.put_item = MagicMock()

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        with patch.object(lf, "_CONFIG_BUCKET", ""):
            response = lf.lambda_handler(event, _make_context())

        self.assertIn("META_OAUTH_SUCCESS", response["body"])
        # CONFIG_BUCKET unset -> push_welcome_surfaces() never calls Graph.
        # The only messenger_profile POST left is the pre-existing Get
        # Started button config, which targets /{page_id}/messenger_profile
        # (distinct path from push_welcome_surfaces's /me/messenger_profile).
        profile_paths = [c.args[0] for c in mock_post.call_args_list]
        self.assertNotIn("/me/messenger_profile", profile_paths)
        self.assertIn("/PAGE_001/messenger_profile", profile_paths)

    @patch("lambda_function._get_meta_app_secret", return_value=_FAKE_APP_SECRET)
    @patch("lambda_function._graph_get")
    @patch("lambda_function._graph_post")
    @patch("lambda_function._encrypt_token", return_value="ENCRYPTED_TOKEN_BASE64")
    @patch("lambda_function._channel_table")
    @patch("lambda_function._s3_client")
    def test_graph_400_on_profile_push_oauth_flow_still_succeeds(
        self, mock_s3, mock_table, mock_encrypt, mock_post, mock_get, _mock_secret
    ):
        """D1/D2-style pin: a Graph error on the welcome-surface push must
        NEVER surface as an OAuth callback failure (best-effort)."""
        import lambda_function as lf
        lf._meta_app_secret = None

        config = {
            "feature_flags": {"MESSENGER_CHANNEL": True},
            "messenger_behavior": {
                "welcome": {"ice_breakers": [{"question": "Q", "payload": "PIC1:cta:x"}]}
            },
        }
        mock_s3.get_object.return_value = TestPushWelcomeSurfaces._s3_get_object_response(config)

        mock_get.side_effect = [
            {"access_token": "USER_TOKEN_XYZ"},
            {"data": [{"id": "PAGE_001", "name": "My Test Page", "access_token": "PAGE_TOKEN_XYZ"}]},
            {},  # instagram_business_account lookup — none linked
        ]

        def graph_post_side_effect(path, params, json_body=None):
            if path == "/me/messenger_profile":
                raise Exception("HTTP 400: Bad Request")
            return {"success": True}

        mock_post.side_effect = graph_post_side_effect
        mock_table.return_value.put_item = MagicMock()

        state = self._valid_state()
        event = _make_event("GET", "/meta/oauth/callback", {"code": "AUTH_CODE", "state": state})
        with patch.object(lf, "_CONFIG_BUCKET", "test-bucket"):
            response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        self.assertIn("META_OAUTH_SUCCESS", response["body"])


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


class TestRepushWelcome(unittest.TestCase):
    """POST /meta/channels/{tenant_id}/repush-welcome — re-push welcome surfaces."""

    _PATH = "/meta/channels/TENANT_123/repush-welcome"

    @patch("lambda_function.push_welcome_surfaces")
    @patch("lambda_function._decrypt_token")
    @patch("lambda_function._query_channels_by_tenant")
    def test_success_pushes_and_returns_result(self, mock_query, mock_decrypt, mock_push):
        import lambda_function as lf

        mock_query.return_value = [
            {"channelType": "messenger", "pageId": "PAGE_1", "encryptedPageToken": "ENC"}
        ]
        mock_decrypt.return_value = "PLAINTEXT_PAGE_TOKEN"
        mock_push.return_value = {"pushed": {"ice_breakers": 3, "persistent_menu": 2, "get_started": True}}

        event = _make_event("POST", self._PATH)
        response = lf.lambda_handler(event, _make_context())

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["tenant_id"], "TENANT_123")
        self.assertEqual(body["result"], {"pushed": {"ice_breakers": 3, "persistent_menu": 2, "get_started": True}})
        # The stored token was decrypted and handed to the push (never the ciphertext).
        mock_decrypt.assert_called_once_with("ENC")
        mock_push.assert_called_once_with("PLAINTEXT_PAGE_TOKEN", "TENANT_123")

    @patch("lambda_function.push_welcome_surfaces")
    @patch("lambda_function._decrypt_token")
    @patch("lambda_function._query_channels_by_tenant")
    def test_passes_through_best_effort_skip(self, mock_query, mock_decrypt, mock_push):
        import lambda_function as lf

        mock_query.return_value = [{"channelType": "messenger", "encryptedPageToken": "ENC"}]
        mock_decrypt.return_value = "TOKEN"
        # push_welcome_surfaces is best-effort — a flag-off tenant returns a skip summary.
        mock_push.return_value = {"skipped": "MESSENGER_CHANNEL flag not enabled"}

        response = lf.lambda_handler(_make_event("POST", self._PATH), _make_context())

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(json.loads(response["body"])["result"], {"skipped": "MESSENGER_CHANNEL flag not enabled"})

    @patch("lambda_function._query_channels_by_tenant")
    def test_no_connected_channel_returns_404(self, mock_query):
        import lambda_function as lf

        mock_query.return_value = []  # nothing connected
        response = lf.lambda_handler(_make_event("POST", self._PATH), _make_context())
        self.assertEqual(response["statusCode"], 404)

    @patch("lambda_function._query_channels_by_tenant")
    def test_channel_without_token_returns_400(self, mock_query):
        import lambda_function as lf

        mock_query.return_value = [{"channelType": "messenger"}]  # connected but no stored token
        response = lf.lambda_handler(_make_event("POST", self._PATH), _make_context())
        self.assertEqual(response["statusCode"], 400)

    @patch("lambda_function.push_welcome_surfaces")
    @patch("lambda_function._decrypt_token")
    @patch("lambda_function._query_channels_by_tenant")
    def test_does_not_push_before_resolving_a_channel(self, mock_query, mock_decrypt, mock_push):
        import lambda_function as lf

        mock_query.return_value = [{"channelType": "instagram", "encryptedPageToken": "ENC"}]  # only IG, no messenger
        lf.lambda_handler(_make_event("POST", self._PATH), _make_context())
        # Default channel is messenger; an IG-only tenant yields 404 and never decrypts or pushes.
        mock_decrypt.assert_not_called()
        mock_push.assert_not_called()


if __name__ == "__main__":
    unittest.main()
