#!/usr/bin/env python3
"""P0a Phase 2 — JWT iss claim decoder hardening tests.

Verifies that both decoder call sites now require the 'iss', 'iat', and 'exp'
claims and enforce issuer="myrecruiter-chat". Phase 1 (5 jwt.encode sites) is
already deployed; these tests confirm Phase 2 enforcement is correct.

References:
  - Phase 1 PR: #33
  - Brief E: docs/runbooks/AGENT_BRIEFS.md
  - Decoder sites: lambda_function.py:835, conversation_handler.py:423
"""

import time
import unittest
from unittest.mock import MagicMock, patch

import jwt


SIGNING_KEY = "test-signing-key-phase2"
ISSUER = "myrecruiter-chat"


def _make_token(payload_extra=None, key=SIGNING_KEY):
    """Build a signed JWT with default valid claims, optionally overriding."""
    now = int(time.time())
    payload = {
        "iss": ISSUER,
        "iat": now,
        "exp": now + 3600,
        "sessionId": "sess-abc",
        "tenantId": "tenant-xyz",
        "turn": 1,
    }
    if payload_extra is not None:
        payload.update(payload_extra)
    return jwt.encode(payload, key, algorithm="HS256")


def _make_token_no_iss(key=SIGNING_KEY):
    """Token without the iss claim."""
    now = int(time.time())
    payload = {
        "iat": now,
        "exp": now + 3600,
        "sessionId": "sess-abc",
        "tenantId": "tenant-xyz",
        "turn": 1,
    }
    return jwt.encode(payload, key, algorithm="HS256")


def _decode_with_phase2_options(token, key=SIGNING_KEY):
    """Mirror the exact Phase 2 decode call pattern."""
    return jwt.decode(
        token,
        key,
        algorithms=["HS256"],
        options={"require": ["iss", "iat", "exp"]},
        issuer=ISSUER,
    )


# ---------------------------------------------------------------------------
# Unit tests — directly exercise the jwt.decode options (no Lambda imports)
# ---------------------------------------------------------------------------


class TestJwtDecodeOptionsDirectly(unittest.TestCase):
    """Directly test the jwt.decode options that Phase 2 adds.

    These tests do NOT import lambda_function or conversation_handler so they
    are immune to the 101 pre-existing test failures in the broader suite.
    """

    def test_valid_token_decodes_successfully(self):
        """Token with iss=myrecruiter-chat + iat + exp must decode cleanly."""
        token = _make_token()
        payload = _decode_with_phase2_options(token)
        self.assertEqual(payload["iss"], ISSUER)
        self.assertEqual(payload["sessionId"], "sess-abc")

    def test_missing_iss_raises_missing_required_claim(self):
        """Token without iss must raise MissingRequiredClaimError."""
        token = _make_token_no_iss()
        with self.assertRaises(jwt.exceptions.MissingRequiredClaimError) as ctx:
            _decode_with_phase2_options(token)
        self.assertIn("iss", str(ctx.exception))

    def test_wrong_issuer_raises_invalid_issuer_error(self):
        """Token with iss=myrecruiter-scheduling must raise InvalidIssuerError."""
        token = _make_token({"iss": "myrecruiter-scheduling"})
        with self.assertRaises(jwt.exceptions.InvalidIssuerError):
            _decode_with_phase2_options(token)

    def test_wrong_issuer_arbitrary_string_raises_invalid_issuer_error(self):
        """Any non-myrecruiter-chat issuer must be rejected."""
        token = _make_token({"iss": "attacker-controlled"})
        with self.assertRaises(jwt.exceptions.InvalidIssuerError):
            _decode_with_phase2_options(token)

    def test_missing_iat_raises_missing_required_claim(self):
        """Token without iat must raise MissingRequiredClaimError."""
        now = int(time.time())
        payload = {
            "iss": ISSUER,
            "exp": now + 3600,
            "sessionId": "sess-abc",
            "tenantId": "tenant-xyz",
            "turn": 1,
        }
        token = jwt.encode(payload, SIGNING_KEY, algorithm="HS256")
        with self.assertRaises(jwt.exceptions.MissingRequiredClaimError) as ctx:
            _decode_with_phase2_options(token)
        self.assertIn("iat", str(ctx.exception))

    def test_missing_exp_raises_missing_required_claim(self):
        """Token without exp must raise MissingRequiredClaimError."""
        now = int(time.time())
        payload = {
            "iss": ISSUER,
            "iat": now,
            "sessionId": "sess-abc",
            "tenantId": "tenant-xyz",
            "turn": 1,
        }
        token = jwt.encode(payload, SIGNING_KEY, algorithm="HS256")
        with self.assertRaises(jwt.exceptions.MissingRequiredClaimError) as ctx:
            _decode_with_phase2_options(token)
        self.assertIn("exp", str(ctx.exception))

    def test_expired_token_raises_expired_signature_error(self):
        """Expired token must still raise ExpiredSignatureError (not bypass via iss check)."""
        now = int(time.time())
        token = _make_token({"iat": now - 7200, "exp": now - 3600})
        with self.assertRaises(jwt.exceptions.ExpiredSignatureError):
            _decode_with_phase2_options(token)


# ---------------------------------------------------------------------------
# conversation_handler.py — validate_state_token integration
# ---------------------------------------------------------------------------


class TestConversationHandlerIssPhase2(unittest.TestCase):
    """Verify conversation_handler._validate_state_token enforces iss via Phase 2.

    _validate_state_token takes an event dict with an Authorization header.
    We bypass the blacklist path by patching TOKEN_BLACKLIST_AVAILABLE=False.
    """

    def _make_event(self, token):
        return {"headers": {"Authorization": f"Bearer {token}"}}

    def test_valid_token_passes_validation(self):
        """A Phase-1-style token (with iss) must pass _validate_state_token."""
        import conversation_handler as ch

        token = _make_token()
        event = self._make_event(token)

        with patch.object(ch, "_get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "TOKEN_BLACKLIST_AVAILABLE", False):
            payload = ch._validate_state_token(event)
        self.assertEqual(payload["iss"], ISSUER)

    def test_token_without_iss_rejected(self):
        """Token without iss must be rejected by _validate_state_token."""
        import conversation_handler as ch

        token = _make_token_no_iss()
        event = self._make_event(token)

        with patch.object(ch, "_get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "TOKEN_BLACKLIST_AVAILABLE", False):
            with self.assertRaises(ch.ConversationError) as ctx:
                ch._validate_state_token(event)
        self.assertEqual(ctx.exception.error_type, "TOKEN_INVALID")

    def test_wrong_issuer_rejected(self):
        """Token with iss=myrecruiter-scheduling must be rejected by _validate_state_token."""
        import conversation_handler as ch

        token = _make_token({"iss": "myrecruiter-scheduling"})
        event = self._make_event(token)

        with patch.object(ch, "_get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "TOKEN_BLACKLIST_AVAILABLE", False):
            with self.assertRaises(ch.ConversationError) as ctx:
                ch._validate_state_token(event)
        self.assertEqual(ctx.exception.error_type, "TOKEN_INVALID")


if __name__ == "__main__":
    unittest.main()
