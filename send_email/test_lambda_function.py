"""
Tests for send_email/lambda_function.py

Coverage targets:
  - fix_bubble_json: all repair branches + passthrough
  - lambda_handler: OPTIONS preflight, missing/empty 'to', missing subject,
    missing both bodies, JSON parse error (both parse attempts fail), Bubble
    fix path activated, successful send, ClientError → 500, generic Exception → 500
  - send_email: single + multiple recipients, cc/bcc/reply_to present/absent,
    html-only, text-only, both bodies, valid attachment, invalid attachment,
    tags forwarded, returns message_id
  - cors_response: status code, CORS headers, JSON body

The module-level ``ses = boto3.client('ses')`` is patched via
``patch.object(lambda_function, 'ses', ...)`` following the same pattern as
ses_event_handler/test_lambda_function.py which patches module-level clients.

For integration tests that actually call through to moto's SES, we use
``@mock_ses`` from moto v4 together with ``importlib.reload`` so the module
re-creates the client inside the mock context.  For pure-function tests and
handler tests that don't need real SES, we patch the client directly on the
module.
"""

import base64
import importlib
import json
import os
from email import message_from_string
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_ses

# ── make sure the module-level boto3.client('ses') is created inside a
#    mock context when we import for the first time.  We do a lazy import
#    inside each moto-decorated test that needs real SES; for unit tests we
#    import normally and patch.
import lambda_function


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SENDER = "notify@myrecruiter.ai"
RECIPIENT = "test@example.com"

DEFAULT_VALID_EVENT = {
    "body": json.dumps({
        "to": [RECIPIENT],
        "subject": "Hello",
        "html_body": "<p>Hello</p>",
        "from": SENDER,
    })
}


def _make_event(body_dict=None, raw_body=None, http_method=None):
    """Build a minimal API GW proxy event."""
    event = {}
    if http_method:
        event["httpMethod"] = http_method
    if raw_body is not None:
        event["body"] = raw_body
    elif body_dict is not None:
        event["body"] = json.dumps(body_dict)
    return event


def _ses_client_error(code="MessageRejected", message="Email address not verified"):
    """Return a ClientError that looks like an SES error."""
    error_response = {"Error": {"Code": code, "Message": message}}
    return ClientError(error_response, "SendRawEmail")


# ---------------------------------------------------------------------------
# cors_response — pure helper, no AWS needed
# ---------------------------------------------------------------------------

class TestCorsResponse:
    def test_status_code_passed_through(self):
        result = lambda_function.cors_response(200, {"ok": True})
        assert result["statusCode"] == 200

    def test_status_code_400(self):
        result = lambda_function.cors_response(400, {"error": "bad"})
        assert result["statusCode"] == 400

    def test_status_code_500(self):
        result = lambda_function.cors_response(500, {"error": "boom"})
        assert result["statusCode"] == 500

    def test_cors_origin_header(self):
        result = lambda_function.cors_response(200, {})
        assert result["headers"]["Access-Control-Allow-Origin"] == "*"

    def test_cors_methods_header(self):
        result = lambda_function.cors_response(200, {})
        assert "POST" in result["headers"]["Access-Control-Allow-Methods"]
        assert "OPTIONS" in result["headers"]["Access-Control-Allow-Methods"]

    def test_cors_allow_headers(self):
        result = lambda_function.cors_response(200, {})
        headers = result["headers"]["Access-Control-Allow-Headers"]
        assert "Content-Type" in headers
        assert "x-api-key" in headers

    def test_content_type_header(self):
        result = lambda_function.cors_response(200, {})
        assert result["headers"]["Content-Type"] == "application/json"

    def test_body_is_json_encoded(self):
        result = lambda_function.cors_response(200, {"key": "value"})
        parsed = json.loads(result["body"])
        assert parsed == {"key": "value"}

    def test_body_complex_object(self):
        payload = {"success": True, "message_id": "abc123", "count": 3}
        result = lambda_function.cors_response(200, payload)
        assert json.loads(result["body"]) == payload


# ---------------------------------------------------------------------------
# fix_bubble_json — pure function, all branches
# ---------------------------------------------------------------------------

class TestFixBubbleJson:
    # --- passthrough cases ---
    def test_none_returns_none(self):
        assert lambda_function.fix_bubble_json(None) is None

    def test_empty_string_returns_empty(self):
        assert lambda_function.fix_bubble_json("") == ""

    def test_valid_json_unchanged(self):
        raw = '{"key": "value", "num": 42}'
        result = lambda_function.fix_bubble_json(raw)
        # The regex passes won't alter properly-formed JSON
        assert json.loads(result) == {"key": "value", "num": 42}

    # --- newline normalisation ---
    def test_crlf_replaced_with_escaped_newline(self):
        raw = '{"a": "line1\r\nline2"}'
        result = lambda_function.fix_bubble_json(raw)
        assert "\r\n" not in result
        assert "\\n" in result

    def test_bare_cr_replaced(self):
        raw = '{"a": "line1\rline2"}'
        result = lambda_function.fix_bubble_json(raw)
        assert "\r" not in result

    # --- Step 1: opening double-quotes after colon ---
    def test_opening_double_quote_after_colon_fixed(self):
        # ': ""value"' → ': "value"'
        raw = '{"html_body": ""<html>hi</html>""}'
        result = lambda_function.fix_bubble_json(raw)
        assert json.loads(result) == {"html_body": "<html>hi</html>"}

    def test_opening_double_quote_with_space_after_colon(self):
        raw = '{"subject":  ""Hello world""}'
        result = lambda_function.fix_bubble_json(raw)
        assert json.loads(result) == {"subject": "Hello world"}

    # --- Step 2: closing double-quotes before comma ---
    def test_closing_double_quote_before_comma(self):
        raw = '{"a": ""first"", "b": ""second""}'
        result = lambda_function.fix_bubble_json(raw)
        parsed = json.loads(result)
        assert parsed == {"a": "first", "b": "second"}

    # --- Step 2: closing double-quotes before closing brace ---
    def test_closing_double_quote_before_brace(self):
        raw = '{"last": ""value""}'
        result = lambda_function.fix_bubble_json(raw)
        assert json.loads(result) == {"last": "value"}

    # --- Step 2: closing double-quotes before closing bracket ---
    def test_closing_double_quote_before_bracket(self):
        # NOTE: fix_bubble_json only handles double-quotes after a colon (Step 1)
        # or before ,/}/] when the *closing* "" is present (Step 2).  Array-element
        # opening double-quotes (no preceding colon) are NOT repaired by Step 1.
        # This test documents the actual repair boundary: Step 2 alone strips the
        # closing "" from an already-opened value that ends with "" before "]".
        # Use a simpler case: a single string value that ends before "]".
        raw = '{"key": ""value""]'   # degenerate but exercises the ] branch
        result = lambda_function.fix_bubble_json(raw)
        # After Step 1: ': ""value""]' → ': "value""]'
        # After Step 2: '"value""]' → '"value"]'   (the "" before ] becomes ")
        # Result should end with ']' without double-quote
        assert result.endswith(']')
        assert '""' not in result

    # --- Step 3: closing double-quotes at end of string ---
    def test_closing_double_quote_at_end(self):
        # A body that ends with ""
        raw = '{"text": ""final value""'
        # Step 3 pattern: ""\s*$ → "
        result = lambda_function.fix_bubble_json(raw)
        # After fix, should end with a single " making it parsable
        assert result.endswith('"')

    # --- realistic Bubble payload ---
    def test_realistic_bubble_html_body_repair(self):
        # Simulate what Bubble emits when it double-quotes a template value
        raw = (
            '{"to": ["user@example.com"], '
            '"subject": ""Welcome!"", '
            '"html_body": ""<h1>Hello</h1><p>Welcome to the platform.</p>"", '
            '"text_body": ""Hello, welcome!""}'
        )
        result = lambda_function.fix_bubble_json(raw)
        parsed = json.loads(result)
        assert parsed["subject"] == "Welcome!"
        assert "<h1>Hello</h1>" in parsed["html_body"]
        assert parsed["text_body"] == "Hello, welcome!"

    # --- no-op on single-quoted strings (already correct) ---
    def test_single_quoted_values_not_damaged(self):
        raw = '{"key": "simple value"}'
        result = lambda_function.fix_bubble_json(raw)
        assert json.loads(result) == {"key": "simple value"}


# ---------------------------------------------------------------------------
# lambda_handler — patching module-level ses client
# ---------------------------------------------------------------------------

class TestLambdaHandlerCors:
    """CORS preflight → 200, no email sent."""

    def test_options_via_httpMethod(self):
        event = {"httpMethod": "OPTIONS"}
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["message"] == "OK"

    def test_options_via_requestContext(self):
        event = {"requestContext": {"http": {"method": "OPTIONS"}}}
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_options_has_cors_headers(self):
        event = {"httpMethod": "OPTIONS"}
        result = lambda_function.lambda_handler(event, None)
        assert result["headers"]["Access-Control-Allow-Origin"] == "*"


class TestLambdaHandlerInputValidation:
    """400 validation failures."""

    def test_missing_to_field(self):
        event = _make_event({"subject": "Hi", "html_body": "<p>hi</p>"})
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert body["success"] is False
        assert "to" in body["error"]

    def test_empty_to_list(self):
        event = _make_event({"to": [], "subject": "Hi", "html_body": "<p>hi</p>"})
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "to" in body["error"]

    def test_missing_subject(self):
        event = _make_event({"to": [RECIPIENT], "html_body": "<p>hi</p>"})
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "subject" in body["error"]

    def test_empty_subject(self):
        event = _make_event({"to": [RECIPIENT], "subject": "", "html_body": "<p>hi</p>"})
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "subject" in body["error"]

    def test_missing_both_bodies(self):
        event = _make_event({"to": [RECIPIENT], "subject": "Hi"})
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "html_body" in body["error"] or "text_body" in body["error"] or "body" in body["error"].lower()

    def test_empty_both_bodies(self):
        event = _make_event({"to": [RECIPIENT], "subject": "Hi", "html_body": "", "text_body": ""})
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_400_response_has_success_false(self):
        event = _make_event({"subject": "Hi", "html_body": "<p>hi</p>"})
        result = lambda_function.lambda_handler(event, None)
        assert json.loads(result["body"])["success"] is False

    def test_400_has_cors_headers(self):
        event = _make_event({"subject": "Hi", "html_body": "<p>hi</p>"})
        result = lambda_function.lambda_handler(event, None)
        assert result["headers"]["Access-Control-Allow-Origin"] == "*"


class TestLambdaHandlerJsonParsing:
    """JSON parse failure branches."""

    def test_completely_invalid_json_returns_400(self):
        event = {"body": "NOT JSON AT ALL {{{{"}
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert body["success"] is False
        assert "JSON" in body["error"] or "json" in body["error"].lower()

    def test_bubble_json_fix_applied_on_parse_failure(self):
        """
        If initial parse fails but fix_bubble_json repairs it, handler succeeds.
        We patch send_email to avoid real SES calls.
        """
        # Build a payload that has Bubble's double-quoting around html_body
        raw = (
            '{"to": ["user@example.com"], '
            '"subject": ""Hello"", '
            '"html_body": ""<p>Fixed</p>""}'
        )
        event = {"body": raw}
        mock_send = MagicMock(return_value="msg-fixed-001")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["success"] is True
        assert body["message_id"] == "msg-fixed-001"

    def test_bubble_fix_fails_both_parses_returns_400(self):
        """
        If both parse attempts fail (unfixable JSON), handler must return 400.
        """
        # Something fix_bubble_json can't repair
        event = {"body": '{"key": "value", BROKEN BEYOND REPAIR {{{'}
        result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert body["success"] is False

    def test_none_body_defaults_to_empty_dict(self):
        """event.get('body', '{}') means None body → '{}' → empty dict → 400 missing 'to'."""
        event = {}  # no 'body' key
        result = lambda_function.lambda_handler(event, None)
        # No 'to' → 400, not a 500
        assert result["statusCode"] == 400


class TestLambdaHandlerSesClientError:
    """ClientError from SES → 500 with error_code in response."""

    def test_ses_client_error_returns_500(self):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.side_effect = _ses_client_error("MessageRejected", "Address not verified")
        with patch.object(lambda_function, "ses", mock_ses):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert result["statusCode"] == 500

    def test_ses_client_error_body_has_success_false(self):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.side_effect = _ses_client_error("MessageRejected", "Address not verified")
        with patch.object(lambda_function, "ses", mock_ses):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        body = json.loads(result["body"])
        assert body["success"] is False

    def test_ses_client_error_body_has_error_code(self):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.side_effect = _ses_client_error("MessageRejected", "Address not verified")
        with patch.object(lambda_function, "ses", mock_ses):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        body = json.loads(result["body"])
        assert body["error_code"] == "MessageRejected"

    def test_ses_client_error_body_includes_message(self):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.side_effect = _ses_client_error("Throttling", "Daily quota exceeded")
        with patch.object(lambda_function, "ses", mock_ses):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        body = json.loads(result["body"])
        assert "Daily quota exceeded" in body["error"]

    def test_ses_client_error_different_codes(self):
        for code in ["AccountSendingPaused", "MailFromDomainNotVerified", "Throttling"]:
            mock_ses_obj = MagicMock()
            mock_ses_obj.send_raw_email.side_effect = _ses_client_error(code, "Some message")
            with patch.object(lambda_function, "ses", mock_ses_obj):
                result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
            assert result["statusCode"] == 500
            assert json.loads(result["body"])["error_code"] == code

    def test_ses_client_error_has_cors_headers(self):
        mock_ses_obj = MagicMock()
        mock_ses_obj.send_raw_email.side_effect = _ses_client_error()
        with patch.object(lambda_function, "ses", mock_ses_obj):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert result["headers"]["Access-Control-Allow-Origin"] == "*"


class TestLambdaHandlerGenericException:
    """Generic Exception (non-ClientError) → 500."""

    def test_generic_exception_returns_500(self):
        with patch.object(lambda_function, "send_email", side_effect=RuntimeError("disk full")):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert result["statusCode"] == 500

    def test_generic_exception_success_false(self):
        with patch.object(lambda_function, "send_email", side_effect=RuntimeError("disk full")):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert json.loads(result["body"])["success"] is False

    def test_generic_exception_error_message_in_body(self):
        with patch.object(lambda_function, "send_email", side_effect=RuntimeError("disk full")):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert "disk full" in json.loads(result["body"])["error"]

    def test_generic_exception_has_cors_headers(self):
        with patch.object(lambda_function, "send_email", side_effect=ValueError("bad value")):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert result["headers"]["Access-Control-Allow-Origin"] == "*"

    def test_invalid_attachment_triggers_500(self):
        """
        Invalid base64 in attachment → ValueError inside send_email → generic 500.
        Uses 'abc' (incorrect padding) because Python's b64decode silently strips
        non-alphabet chars (e.g. '!') from most malformed strings.
        """
        event = _make_event({
            "to": [RECIPIENT],
            "subject": "Hi",
            "html_body": "<p>hi</p>",
            "from": SENDER,
            "attachments": [{"filename": "bad.pdf", "content_base64": "abc"}],
        })
        mock_ses_obj = MagicMock()
        with patch.object(lambda_function, "ses", mock_ses_obj):
            result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert body["success"] is False


class TestLambdaHandlerSuccessfulSend:
    """Happy-path: send succeeds, 200 with message_id."""

    def test_successful_send_returns_200(self):
        mock_send = MagicMock(return_value="msg-abc-123")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        assert result["statusCode"] == 200

    def test_successful_send_body_success_true(self):
        mock_send = MagicMock(return_value="msg-abc-123")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        body = json.loads(result["body"])
        assert body["success"] is True

    def test_successful_send_body_has_message_id(self):
        mock_send = MagicMock(return_value="msg-abc-123")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
        body = json.loads(result["body"])
        assert body["message_id"] == "msg-abc-123"

    def test_optional_fields_passed_to_send_email(self):
        """cc, bcc, reply_to, tags, from all forwarded when present."""
        body_dict = {
            "to": [RECIPIENT],
            "from": "custom@example.com",
            "cc": ["cc@example.com"],
            "bcc": ["bcc@example.com"],
            "reply_to": ["reply@example.com"],
            "subject": "Test",
            "html_body": "<p>hi</p>",
            "tags": {"tenant": "AUS123957"},
        }
        mock_send = MagicMock(return_value="msg-opts-001")
        with patch.object(lambda_function, "send_email", mock_send):
            lambda_function.lambda_handler(_make_event(body_dict), None)
        _, kwargs = mock_send.call_args
        assert kwargs["cc"] == ["cc@example.com"]
        assert kwargs["bcc"] == ["bcc@example.com"]
        assert kwargs["reply_to"] == ["reply@example.com"]
        assert kwargs["sender"] == "custom@example.com"
        assert kwargs["tags"] == {"tenant": "AUS123957"}

    def test_missing_from_uses_default_sender(self):
        body_dict = {
            "to": [RECIPIENT],
            "subject": "Test",
            "html_body": "<p>hi</p>",
        }
        mock_send = MagicMock(return_value="msg-def-001")
        with patch.object(lambda_function, "send_email", mock_send):
            lambda_function.lambda_handler(_make_event(body_dict), None)
        _, kwargs = mock_send.call_args
        # Must use DEFAULT_SENDER (env or hardcoded fallback)
        assert kwargs["sender"] == lambda_function.DEFAULT_SENDER

    def test_text_only_body_accepted(self):
        event = _make_event({
            "to": [RECIPIENT],
            "subject": "Plain only",
            "text_body": "Plain text email",
            "from": SENDER,
        })
        mock_send = MagicMock(return_value="msg-text-001")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_html_only_body_accepted(self):
        event = _make_event({
            "to": [RECIPIENT],
            "subject": "HTML only",
            "html_body": "<p>HTML email</p>",
            "from": SENDER,
        })
        mock_send = MagicMock(return_value="msg-html-001")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_both_bodies_accepted(self):
        event = _make_event({
            "to": [RECIPIENT],
            "subject": "Both",
            "html_body": "<p>HTML</p>",
            "text_body": "Plain",
            "from": SENDER,
        })
        mock_send = MagicMock(return_value="msg-both-001")
        with patch.object(lambda_function, "send_email", mock_send):
            result = lambda_function.lambda_handler(event, None)
        assert result["statusCode"] == 200


# ---------------------------------------------------------------------------
# send_email — integration via moto mock_ses
# ---------------------------------------------------------------------------

def _setup_moto_ses():
    """Verify sender and return a boto3 SES client pointed at moto."""
    client = boto3.client("ses", region_name="us-east-1")
    client.verify_email_identity(EmailAddress=SENDER)
    return client


@mock_ses
def test_send_email_single_recipient_returns_message_id():
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Single recipient test",
            html_body="<p>Hello</p>",
            text_body="Hello",
            attachments=[],
            tags={},
        )
    assert isinstance(msg_id, str)
    assert len(msg_id) > 0


@mock_ses
def test_send_email_multiple_recipients():
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=["a@example.com", "b@example.com"],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Multi-recipient",
            html_body="<p>Hi all</p>",
            text_body="Hi all",
            attachments=[],
            tags={},
        )
    assert msg_id


@mock_ses
def test_send_email_with_cc_and_bcc():
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=["cc@example.com"],
            bcc=["bcc@example.com"],
            reply_to=[],
            subject="CC and BCC test",
            html_body="<p>CC/BCC</p>",
            text_body="CC/BCC",
            attachments=[],
            tags={},
        )
    assert msg_id


@mock_ses
def test_send_email_with_reply_to():
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=["reply@example.com"],
            subject="Reply-To test",
            html_body="<p>Reply-To</p>",
            text_body="Reply-To",
            attachments=[],
            tags={},
        )
    assert msg_id


@mock_ses
def test_send_email_html_only():
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="HTML only",
            html_body="<h1>HTML only</h1>",
            text_body="",  # no text body
            attachments=[],
            tags={},
        )
    assert msg_id


@mock_ses
def test_send_email_text_only():
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Text only",
            html_body="",  # no html body
            text_body="Plain text only",
            attachments=[],
            tags={},
        )
    assert msg_id


@mock_ses
def test_send_email_mime_headers_correct():
    """Inspect the raw MIME message to verify To/From/Subject headers."""
    captured_calls = []

    client = _setup_moto_ses()

    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=["alice@example.com", "bob@example.com"],
            cc=["cc@example.com"],
            bcc=[],
            reply_to=["reply@example.com"],
            subject="MIME Header Test",
            html_body="<p>test</p>",
            text_body="test",
            attachments=[],
            tags={},
        )

    assert len(captured_calls) == 1
    raw_msg = captured_calls[0]["RawMessage"]["Data"]
    parsed = message_from_string(raw_msg)
    assert "alice@example.com" in parsed["To"]
    assert "bob@example.com" in parsed["To"]
    assert parsed["Subject"] == "MIME Header Test"
    assert parsed["From"] == SENDER
    assert "cc@example.com" in parsed["Cc"]
    assert "reply@example.com" in parsed["Reply-To"]
    # BCC must NOT appear in MIME headers
    assert parsed["Bcc"] is None


@mock_ses
def test_send_email_bcc_in_destinations_not_in_mime():
    """BCC must be in Destinations list but NOT in MIME headers."""
    captured_calls = []
    client = _setup_moto_ses()
    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=["hidden@example.com"],
            reply_to=[],
            subject="BCC test",
            html_body="<p>hi</p>",
            text_body="",
            attachments=[],
            tags={},
        )

    call = captured_calls[0]
    assert "hidden@example.com" in call["Destinations"]
    raw_msg = call["RawMessage"]["Data"]
    parsed = message_from_string(raw_msg)
    assert parsed["Bcc"] is None


@mock_ses
def test_send_email_valid_attachment():
    """Valid base64 attachment is decoded and attached."""
    client = _setup_moto_ses()
    pdf_content = b"%PDF-1.4 fake pdf content"
    encoded = base64.b64encode(pdf_content).decode("utf-8")
    attachment = {
        "filename": "report.pdf",
        "content_base64": encoded,
        "content_type": "application/pdf",
    }
    with patch.object(lambda_function, "ses", client):
        msg_id = lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Attachment test",
            html_body="<p>See attached</p>",
            text_body="See attached",
            attachments=[attachment],
            tags={},
        )
    assert msg_id


@mock_ses
def test_send_email_invalid_attachment_raises_value_error():
    """Non-base64 attachment content must raise ValueError.

    The handler decodes with validate=True, so any non-base64 input raises
    binascii.Error, which is re-raised as ValueError. Here 'abc' (3 chars =
    incorrect padding) triggers it; the non-alphabet-character case (which the
    pre-validate=True handler silently accepted) is covered by
    test_send_email_rejects_non_alphabet_base64 below.
    """
    client = _setup_moto_ses()
    bad_attachment = {
        "filename": "corrupt.pdf",
        "content_base64": "abc",  # 3 chars = incorrect padding → binascii.Error
        "content_type": "application/pdf",
    }
    with patch.object(lambda_function, "ses", client):
        with pytest.raises(ValueError, match="Invalid attachment"):
            lambda_function.send_email(
                sender=SENDER,
                to=[RECIPIENT],
                cc=[],
                bcc=[],
                reply_to=[],
                subject="Bad attachment",
                html_body="<p>hi</p>",
                text_body="",
                attachments=[bad_attachment],
                tags={},
            )


@mock_ses
def test_send_email_rejects_non_alphabet_base64():
    """Regression guard for the validate=True hardening.

    'aGVs bG8=' contains a space (a non-base64-alphabet char). Before the fix,
    base64.b64decode() (default validate=False) silently stripped the space and
    decoded the remainder to b'hello' — a corrupt attachment slipped through.
    With validate=True the space raises binascii.Error, re-raised as ValueError.
    If this test ever fails with "did not raise", validate=True was lost.
    """
    client = _setup_moto_ses()
    sneaky_attachment = {
        "filename": "sneaky.pdf",
        "content_base64": "aGVs bG8=",  # valid-after-strip, but contains a space
        "content_type": "application/pdf",
    }
    with patch.object(lambda_function, "ses", client):
        with pytest.raises(ValueError, match="Invalid attachment"):
            lambda_function.send_email(
                sender=SENDER,
                to=[RECIPIENT],
                cc=[],
                bcc=[],
                reply_to=[],
                subject="Sneaky attachment",
                html_body="<p>hi</p>",
                text_body="",
                attachments=[sneaky_attachment],
                tags={},
            )


@mock_ses
def test_send_email_tags_forwarded():
    """Tags dict is converted to Name/Value list and passed to SES."""
    captured_calls = []
    client = _setup_moto_ses()
    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Tag test",
            html_body="<p>hi</p>",
            text_body="",
            attachments=[],
            tags={"tenant_id": "AUS123957", "campaign": "welcome"},
        )

    tags_sent = captured_calls[0]["Tags"]
    tag_map = {t["Name"]: t["Value"] for t in tags_sent}
    assert tag_map["tenant_id"] == "AUS123957"
    assert tag_map["campaign"] == "welcome"


@mock_ses
def test_send_email_no_cc_no_reply_to():
    """cc=[] and reply_to=[] must not add Cc/Reply-To MIME headers."""
    captured_calls = []
    client = _setup_moto_ses()
    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="No CC test",
            html_body="<p>hi</p>",
            text_body="",
            attachments=[],
            tags={},
        )

    raw_msg = captured_calls[0]["RawMessage"]["Data"]
    parsed = message_from_string(raw_msg)
    assert parsed["Cc"] is None
    assert parsed["Reply-To"] is None


@mock_ses
def test_send_email_empty_tags():
    """tags={} must result in Tags=[] (empty list), not an error."""
    captured_calls = []
    client = _setup_moto_ses()
    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="No tags",
            html_body="<p>hi</p>",
            text_body="",
            attachments=[],
            tags={},
        )

    assert captured_calls[0]["Tags"] == []


@mock_ses
def test_send_email_tag_value_truncated_at_256():
    """Tag values longer than 256 chars must be truncated to 256."""
    captured_calls = []
    client = _setup_moto_ses()
    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    long_value = "x" * 500
    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Tag truncation",
            html_body="<p>hi</p>",
            text_body="",
            attachments=[],
            tags={"long_tag": long_value},
        )

    tags_sent = captured_calls[0]["Tags"]
    tag_map = {t["Name"]: t["Value"] for t in tags_sent}
    assert len(tag_map["long_tag"]) == 256


@mock_ses
def test_send_email_uses_configuration_set():
    """ConfigurationSetName must be forwarded to send_raw_email."""
    captured_calls = []
    client = _setup_moto_ses()
    original_send = client.send_raw_email

    def capture_send(**kwargs):
        captured_calls.append(kwargs)
        return original_send(**kwargs)

    client.send_raw_email = capture_send

    with patch.object(lambda_function, "ses", client):
        lambda_function.send_email(
            sender=SENDER,
            to=[RECIPIENT],
            cc=[],
            bcc=[],
            reply_to=[],
            subject="Config set test",
            html_body="<p>hi</p>",
            text_body="",
            attachments=[],
            tags={},
        )

    assert captured_calls[0]["ConfigurationSetName"] == lambda_function.CONFIGURATION_SET


@mock_ses
def test_lambda_handler_full_integration():
    """End-to-end: handler → send_email → moto SES → 200 with message_id."""
    client = _setup_moto_ses()
    with patch.object(lambda_function, "ses", client):
        result = lambda_function.lambda_handler(DEFAULT_VALID_EVENT, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["success"] is True
    assert "message_id" in body
    assert isinstance(body["message_id"], str)
    assert len(body["message_id"]) > 0
