"""
Test the CORS default-fallback origin in add_cors_headers().

Phase D3 audit (2026-05-13) flagged that the prior default of
`_CORS_ALLOWED_ORIGINS_DEFAULT[0]` resolved to `http://localhost:8000` —
combined with `Access-Control-Allow-Credentials: true`, that was a live
misconfiguration: any page at http://localhost:8000 in a victim's browser
could read responses with credentials.

Fix: introduce `_CORS_DEFAULT_FALLBACK_ORIGIN = 'https://chat.myrecruiter.ai'`
and use it at both fallback sites (no-event AND rejected-origin paths).

These tests exercise the three fallback paths.
"""
import unittest
from lambda_function import (
    add_cors_headers,
    _CORS_DEFAULT_FALLBACK_ORIGIN,
    _CORS_ALLOWED_ORIGINS_DEFAULT,
)


class TestCorsDefaultFallback(unittest.TestCase):
    """Confirm no-origin and rejected-origin paths land on canonical chat host."""

    def test_no_event_falls_back_to_canonical_chat_host(self):
        """When event is None entirely, ACAO must be the canonical chat host."""
        response = add_cors_headers({}, event=None)
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'https://chat.myrecruiter.ai',
        )

    def test_event_without_origin_header_falls_back_to_canonical(self):
        """No Origin header in event.headers → canonical chat host (not localhost)."""
        event = {'headers': {}}
        response = add_cors_headers({}, event=event)
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'https://chat.myrecruiter.ai',
        )

    def test_unrecognized_origin_falls_back_to_canonical(self):
        """Origin present but not in allowlist → canonical chat host (not localhost)."""
        event = {'headers': {'origin': 'https://evil.example.com'}}
        response = add_cors_headers({}, event=event)
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'https://chat.myrecruiter.ai',
        )

    def test_allowed_origin_is_reflected(self):
        """Origin in allowlist → reflect it (regression guard for the happy path)."""
        event = {'headers': {'Origin': 'https://chat.myrecruiter.ai'}}
        response = add_cors_headers({}, event=event)
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'https://chat.myrecruiter.ai',
        )

    def test_staging_origin_is_reflected(self):
        """staging.chat.myrecruiter.ai is in the allowlist → reflect it."""
        event = {'headers': {'origin': 'https://staging.chat.myrecruiter.ai'}}
        response = add_cors_headers({}, event=event)
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'https://staging.chat.myrecruiter.ai',
        )

    def test_localhost_origin_is_reflected(self):
        """Any localhost port is reflected (developer machines)."""
        event = {'headers': {'origin': 'http://localhost:4200'}}
        response = add_cors_headers({}, event=event)
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'http://localhost:4200',
        )

    def test_default_fallback_constant_is_canonical(self):
        """Constant itself must point at chat.myrecruiter.ai."""
        self.assertEqual(_CORS_DEFAULT_FALLBACK_ORIGIN, 'https://chat.myrecruiter.ai')

    def test_default_fallback_is_decoupled_from_allowlist_order(self):
        """Even if the allowlist gets reordered, the fallback must not change."""
        # Sanity: the first allowlist entry is currently localhost (kept for dev),
        # but the fallback must NEVER be that. This guards against reintroducing
        # the bug if someone reaches for `_CORS_ALLOWED_ORIGINS_DEFAULT[0]` again.
        self.assertNotEqual(
            _CORS_DEFAULT_FALLBACK_ORIGIN,
            _CORS_ALLOWED_ORIGINS_DEFAULT[0],
            "fallback must not be coupled to allowlist[0] (which is localhost)",
        )


if __name__ == '__main__':
    unittest.main()
