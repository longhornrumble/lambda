#!/usr/bin/env python3
"""C1 P1 — ownership-proven conversation resume (compat-open).

SECURITY_REVIEW_2026-07-02 §C1: `init_session` accepted a raw client-supplied
`session_id` and signed a valid state JWT with no proof the caller owned the
session — a same-tenant cross-session hijack (read/inject/clear another visitor's
transcript). P1 opens the secure path (present the prior signed token to prove
ownership) and adds a tenant-binding cross-check, while STILL accepting the legacy
raw-`session_id` resume during the widget-rollout window (removed in P3).

Two surfaces are covered:
  1. conversation_handler._validate_state_token — token `tenantId` vs query `t`.
  2. lambda_function.handle_init_session — new / legacy / authenticated-resume
     branches + the reject paths (wrong tenant, expired, garbage).

Mirrors the harness style of test_jwt_iss_phase2.py (patch the signing-key getter
+ TOKEN_BLACKLIST_AVAILABLE; build events with an Authorization Bearer header).
"""

import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import jwt

# Ensure module resolution works when pytest runs from project root
sys.path.insert(0, str(Path(__file__).parent))

SIGNING_KEY = "test-signing-key-c1p1"
ISSUER = "myrecruiter-chat"


def _make_state_token(tenant_id="tenant-xyz", session_id="session_owned_abc",
                      turn=3, expired=False, key=SIGNING_KEY):
    """Build a signed state JWT with default valid claims."""
    now = int(time.time())
    payload = {
        "iss": ISSUER,
        "iat": now,
        "exp": now - 10 if expired else now + 3600,
        "sessionId": session_id,
        "tenantId": tenant_id,
        "turn": turn,
    }
    return jwt.encode(payload, key, algorithm="HS256")


# ---------------------------------------------------------------------------
# Surface 1 — _validate_state_token tenant-binding cross-check
# ---------------------------------------------------------------------------
class TestValidateStateTokenTenantBinding(unittest.TestCase):
    """The token's tenantId claim must match the query `t` when `t` is present."""

    def _event(self, token, t=None):
        ev = {"headers": {"Authorization": f"Bearer {token}"}}
        if t is not None:
            ev["queryStringParameters"] = {"t": t}
        return ev

    def test_matching_tenant_passes(self):
        import conversation_handler as ch
        ev = self._event(_make_state_token(tenant_id="tenant-xyz"), t="tenant-xyz")
        with patch.object(ch, "_get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "TOKEN_BLACKLIST_AVAILABLE", False):
            payload = ch._validate_state_token(ev)
        self.assertEqual(payload["tenantId"], "tenant-xyz")

    def test_mismatched_tenant_rejected_401(self):
        import conversation_handler as ch
        ev = self._event(_make_state_token(tenant_id="tenant-xyz"), t="tenant-EVIL")
        with patch.object(ch, "_get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "TOKEN_BLACKLIST_AVAILABLE", False):
            with self.assertRaises(ch.ConversationError) as ctx:
                ch._validate_state_token(ev)
        self.assertEqual(ctx.exception.error_type, "TENANT_MISMATCH")
        self.assertEqual(ctx.exception.status_code, 401)

    def test_absent_t_still_passes(self):
        """No `t` query param -> no comparison; authorizes off the token as before."""
        import conversation_handler as ch
        ev = self._event(_make_state_token(tenant_id="tenant-xyz"), t=None)
        with patch.object(ch, "_get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "TOKEN_BLACKLIST_AVAILABLE", False):
            payload = ch._validate_state_token(ev)
        self.assertEqual(payload["tenantId"], "tenant-xyz")


# ---------------------------------------------------------------------------
# Surface 2 — handle_init_session new / legacy / authenticated-resume branches
# ---------------------------------------------------------------------------
class TestInitSessionOwnershipResume(unittest.TestCase):
    TENANT = "tenanthash123"

    def _call(self, event, db_item=None):
        """Invoke handle_init_session with the signing key + DB read patched."""
        import lambda_function as lf
        import conversation_handler as ch

        def _fake_db(session_id, tenant_id):
            return {"Item": db_item} if db_item is not None else None

        with patch.object(lf, "get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "_get_conversation_from_db", _fake_db):
            return lf.handle_init_session(event, self.TENANT)

    # --- new session: no token, no existing conversation --------------------
    def test_new_session_no_token_mints_fresh(self):
        resp = self._call({"headers": {}, "body": None})
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertTrue(data["state_token"])
        self.assertEqual(data["turn"], 0)
        self.assertNotIn("authenticated_resume", data)

    # --- legacy compat: no token, raw session_id + existing conversation ----
    def test_legacy_raw_session_resume_still_served(self):
        resp = self._call(
            {"headers": {}, "body": json.dumps({"session_id": "session_legacy_1"})},
            db_item={"turn": {"N": "5"}, "messages": {"L": []}},
        )
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertTrue(data.get("existing"))
        self.assertNotIn("authenticated_resume", data)  # legacy path, not the secure one

    # --- authenticated resume: valid token via Authorization header ---------
    def test_valid_token_authenticated_resume(self):
        token = _make_state_token(tenant_id=self.TENANT, session_id="session_owned_abc")
        event = {
            "headers": {"Authorization": f"Bearer {token}"},
            "body": json.dumps({"session_id": "session_owned_abc"}),
        }
        resp = self._call(event, db_item={"turn": {"N": "7"}, "messages": {"L": [1, 2]}})
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertTrue(data["authenticated_resume"])
        self.assertEqual(data["session_id"], "session_owned_abc")
        self.assertEqual(data["turn"], 7)
        self.assertTrue(data["state_token"])

    # --- authenticated resume: token can also arrive in the body ------------
    def test_valid_token_via_body_state_token(self):
        token = _make_state_token(tenant_id=self.TENANT, session_id="session_body_tok")
        event = {"headers": {}, "body": json.dumps({"state_token": token})}
        resp = self._call(event, db_item={"turn": {"N": "2"}})
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertTrue(data["authenticated_resume"])
        self.assertEqual(data["session_id"], "session_body_tok")
        self.assertEqual(data["turn"], 2)

    # --- authenticated resume with no DB history: still authenticated -------
    def test_valid_token_no_history_returns_turn_zero(self):
        token = _make_state_token(tenant_id=self.TENANT, session_id="session_fresh_tok")
        event = {"headers": {"Authorization": f"Bearer {token}"}, "body": None}
        resp = self._call(event, db_item=None)  # no row in DB
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertTrue(data["authenticated_resume"])
        self.assertEqual(data["turn"], 0)
        self.assertFalse(data["existing"])

    # --- reject: token minted for another tenant ----------------------------
    def test_valid_token_wrong_tenant_rejected_401(self):
        token = _make_state_token(tenant_id="OTHER_TENANT", session_id="session_x")
        event = {"headers": {"Authorization": f"Bearer {token}"}, "body": None}
        resp = self._call(event)
        self.assertEqual(resp["statusCode"], 401)
        self.assertEqual(json.loads(resp["body"])["error"], "TENANT_MISMATCH")

    # --- reject: expired token ----------------------------------------------
    def test_expired_token_rejected_401(self):
        token = _make_state_token(tenant_id=self.TENANT, expired=True)
        event = {"headers": {"Authorization": f"Bearer {token}"}, "body": None}
        resp = self._call(event)
        self.assertEqual(resp["statusCode"], 401)
        self.assertEqual(json.loads(resp["body"])["error"], "TOKEN_EXPIRED")

    # --- reject: garbage / unsigned token -----------------------------------
    def test_garbage_token_rejected_401(self):
        event = {"headers": {"Authorization": "Bearer not.a.jwt"}, "body": None}
        resp = self._call(event)
        self.assertEqual(resp["statusCode"], 401)
        self.assertEqual(json.loads(resp["body"])["error"], "TOKEN_INVALID")

    # --- reject: valid signature but no sessionId claim ---------------------
    def test_token_without_session_id_rejected_401(self):
        now = int(time.time())
        token = jwt.encode(
            {"iss": ISSUER, "iat": now, "exp": now + 3600,
             "tenantId": self.TENANT, "turn": 1},  # no sessionId
            SIGNING_KEY, algorithm="HS256",
        )
        event = {"headers": {"Authorization": f"Bearer {token}"}, "body": None}
        resp = self._call(event)
        self.assertEqual(resp["statusCode"], 401)
        self.assertEqual(json.loads(resp["body"])["error"], "TENANT_MISMATCH")

    # --- authenticated resume is resilient to a DB read failure -------------
    def test_valid_token_db_read_error_non_fatal(self):
        """A DynamoDB read failure during resume must not 500 — still authenticate."""
        import lambda_function as lf
        import conversation_handler as ch

        def _raising_db(session_id, tenant_id):
            raise RuntimeError("dynamo unavailable")

        token = _make_state_token(tenant_id=self.TENANT, session_id="session_dberr")
        event = {"headers": {"Authorization": f"Bearer {token}"}, "body": None}
        with patch.object(lf, "get_jwt_signing_key", return_value=SIGNING_KEY), \
             patch.object(ch, "_get_conversation_from_db", _raising_db):
            resp = lf.handle_init_session(event, self.TENANT)
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertTrue(data["authenticated_resume"])
        self.assertEqual(data["turn"], 0)

    # --- reject: token signed with the wrong key ----------------------------
    def test_wrong_signing_key_rejected_401(self):
        token = _make_state_token(tenant_id=self.TENANT, key="attacker-key")
        event = {"headers": {"Authorization": f"Bearer {token}"}, "body": None}
        resp = self._call(event)
        self.assertEqual(resp["statusCode"], 401)
        self.assertEqual(json.loads(resp["body"])["error"], "TOKEN_INVALID")


if __name__ == "__main__":
    unittest.main()
