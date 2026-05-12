#!/usr/bin/env python3
"""Webhook SSRF URL validation.

Phase 4 cumulative audit blocker #3: tenant-config-supplied webhook URLs
were passed directly to requests.post() with no validation. A malicious
tenant config could point the Lambda at internal services (RFC1918),
loopback (127.0.0.1), or the EC2 IMDS link-local (169.254.169.254).

Tests verify:
- Non-HTTPS schemes rejected (http, file, javascript, gopher)
- Hostnames that resolve to private/loopback/link-local IPs rejected
- Direct-IP URLs in disallowed ranges rejected
- Empty/None/malformed URLs rejected
- Valid public-HTTPS URLs accepted
- _send_webhook_notifications skips POST when validation fails
"""

import unittest
from unittest.mock import patch, MagicMock

from form_handler import _validate_webhook_url, FormHandler


class TestValidateWebhookURL(unittest.TestCase):

    def test_rejects_http_scheme(self):
        ok, reason = _validate_webhook_url('http://example.com/hook')
        self.assertFalse(ok)
        self.assertIn('https', reason)

    def test_rejects_file_scheme(self):
        ok, reason = _validate_webhook_url('file:///etc/passwd')
        self.assertFalse(ok)
        self.assertIn('https', reason)

    def test_rejects_javascript_scheme(self):
        ok, reason = _validate_webhook_url('javascript:alert(1)')
        self.assertFalse(ok)

    def test_rejects_gopher_scheme(self):
        ok, reason = _validate_webhook_url('gopher://internal-host/')
        self.assertFalse(ok)

    def test_rejects_empty_url(self):
        ok, reason = _validate_webhook_url('')
        self.assertFalse(ok)

    def test_rejects_none_url(self):
        ok, reason = _validate_webhook_url(None)
        self.assertFalse(ok)

    def test_rejects_non_string_url(self):
        ok, reason = _validate_webhook_url(12345)
        self.assertFalse(ok)

    def test_rejects_missing_hostname(self):
        # https:/// is parsed with empty hostname
        ok, reason = _validate_webhook_url('https:///path')
        self.assertFalse(ok)
        self.assertIn('hostname', reason)

    def test_rejects_loopback_by_ip(self):
        ok, reason = _validate_webhook_url('https://127.0.0.1/hook')
        self.assertFalse(ok)
        self.assertIn('disallowed', reason)

    def test_rejects_loopback_by_hostname(self):
        # 'localhost' resolves to 127.0.0.1
        ok, reason = _validate_webhook_url('https://localhost/hook')
        self.assertFalse(ok)
        self.assertIn('disallowed', reason)

    def test_rejects_rfc1918_10_0(self):
        ok, reason = _validate_webhook_url('https://10.0.0.1/hook')
        self.assertFalse(ok)

    def test_rejects_rfc1918_172_16(self):
        ok, reason = _validate_webhook_url('https://172.16.0.1/hook')
        self.assertFalse(ok)

    def test_rejects_rfc1918_192_168(self):
        ok, reason = _validate_webhook_url('https://192.168.1.1/hook')
        self.assertFalse(ok)

    def test_rejects_link_local_aws_imds(self):
        # The classic SSRF target on EC2 — must be blocked.
        ok, reason = _validate_webhook_url('https://169.254.169.254/latest/meta-data/iam/security-credentials/')
        self.assertFalse(ok)

    def test_rejects_dns_failure(self):
        with patch('form_handler.socket.getaddrinfo') as mock_resolve:
            mock_resolve.side_effect = OSError('DNS lookup failed')
            ok, reason = _validate_webhook_url('https://does-not-exist.invalid/hook')
            self.assertFalse(ok)
            self.assertIn('resolution', reason)

    def test_accepts_valid_public_https(self):
        # Mock DNS so the test does not depend on network.
        with patch('form_handler.socket.getaddrinfo') as mock_resolve:
            mock_resolve.return_value = [(2, 1, 6, '', ('93.184.216.34', 443))]
            ok, reason = _validate_webhook_url('https://example.com/hook')
            self.assertTrue(ok, f"unexpected rejection: {reason}")
            self.assertIsNone(reason)

    def test_rejects_ipv6_loopback(self):
        ok, reason = _validate_webhook_url('https://[::1]/hook')
        self.assertFalse(ok)
        self.assertIn('disallowed', reason)

    def test_rejects_ipv6_link_local(self):
        ok, reason = _validate_webhook_url('https://[fe80::1]/hook')
        self.assertFalse(ok)
        self.assertIn('disallowed', reason)

    def test_rejects_ipv4_mapped_ipv6_imds(self):
        """Audit follow-up: IPv4-mapped IPv6 notation for the IMDS address
        (`::ffff:169.254.169.254`) must be blocked. ipaddress.ip_address
        on this string reports is_reserved=True (the IPv4-mapped subspace
        falls inside the reserved IANA block ::ffff:0:0/96), so the existing
        rejection logic catches it — but no test exercised this path."""
        ok, reason = _validate_webhook_url('https://[::ffff:169.254.169.254]/latest/meta-data/')
        self.assertFalse(ok)
        self.assertIn('disallowed', reason)

    def test_rejects_ipv4_mapped_ipv6_rfc1918(self):
        """Same coverage for IPv4-mapped IPv6 pointing at RFC1918."""
        ok, reason = _validate_webhook_url('https://[::ffff:10.0.0.1]/hook')
        self.assertFalse(ok)
        self.assertIn('disallowed', reason)

    def test_rejects_when_any_resolution_returns_private_ip(self):
        # Defense-in-depth: a hostname that resolves to multiple IPs where
        # any one is in a disallowed range must be rejected. This blocks
        # split-horizon DNS tricks.
        with patch('form_handler.socket.getaddrinfo') as mock_resolve:
            mock_resolve.return_value = [
                (2, 1, 6, '', ('93.184.216.34', 443)),  # public
                (2, 1, 6, '', ('10.0.0.5', 443)),        # private
            ]
            ok, reason = _validate_webhook_url('https://mixed.example.com/hook')
            self.assertFalse(ok)
            self.assertIn('disallowed', reason)


class TestSendWebhookNotificationsSSRFBlock(unittest.TestCase):
    """Confirm the validator is wired into the send path."""

    def _make_handler(self):
        return FormHandler({'tenant_id': 't', 'tenant_hash': 'h'})

    def test_send_skips_post_when_url_invalid(self):
        """Invalid URL must NOT reach requests.post."""
        handler = self._make_handler()
        webhook_config = {'url': 'http://10.0.0.1/hook', 'headers': {}}

        with patch('requests.post') as mock_post:
            sent = handler._send_webhook_notifications(webhook_config, {'field': 'value'})

        mock_post.assert_not_called()
        self.assertEqual(sent, [])

    def test_send_skips_post_when_imds_url(self):
        """The headline SSRF target — IMDS — must be blocked at the validator."""
        handler = self._make_handler()
        webhook_config = {
            'url': 'https://169.254.169.254/latest/meta-data/',
            'headers': {},
        }

        with patch('requests.post') as mock_post:
            sent = handler._send_webhook_notifications(webhook_config, {})

        mock_post.assert_not_called()
        self.assertEqual(sent, [])

    def test_send_proceeds_when_url_valid(self):
        """Valid public-HTTPS URL passes through to requests.post."""
        handler = self._make_handler()
        webhook_config = {'url': 'https://example.com/hook', 'headers': {}}

        with patch('form_handler.socket.getaddrinfo') as mock_resolve, \
             patch('requests.post') as mock_post:
            mock_resolve.return_value = [(2, 1, 6, '', ('93.184.216.34', 443))]
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            sent = handler._send_webhook_notifications(webhook_config, {'field': 'value'})

        mock_post.assert_called_once()
        self.assertEqual(sent, ['webhook:200'])

    def test_send_passes_allow_redirects_false(self):
        """Audit follow-up: a public webhook server returning 302 → IMDS would
        bypass the static-URL validator if requests.post follows redirects.
        The send path must pass allow_redirects=False so the redirect target
        is never fetched."""
        handler = self._make_handler()
        webhook_config = {'url': 'https://example.com/hook', 'headers': {}}

        with patch('form_handler.socket.getaddrinfo') as mock_resolve, \
             patch('requests.post') as mock_post:
            mock_resolve.return_value = [(2, 1, 6, '', ('93.184.216.34', 443))]
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            handler._send_webhook_notifications(webhook_config, {'field': 'value'})

        # The call must include allow_redirects=False explicitly.
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs.get('allow_redirects'), False)

    def test_send_treats_3xx_as_security_event(self):
        """Audit follow-up: a 302 response (which we no longer follow) must
        NOT be reported as a successful send. The validator can't recurse
        into the redirect target; treating 3xx as success would let an
        attacker confirm IMDS reachability via timing or response code."""
        handler = self._make_handler()
        webhook_config = {'url': 'https://example.com/hook', 'headers': {}}

        with patch('form_handler.socket.getaddrinfo') as mock_resolve, \
             patch('requests.post') as mock_post:
            mock_resolve.return_value = [(2, 1, 6, '', ('93.184.216.34', 443))]
            mock_response = MagicMock()
            mock_response.status_code = 302
            mock_response.headers = {'Location': 'http://169.254.169.254/'}
            mock_post.return_value = mock_response

            sent = handler._send_webhook_notifications(webhook_config, {'field': 'value'})

        # 302 must not be recorded as "webhook:302".
        self.assertEqual(sent, [])


if __name__ == '__main__':
    unittest.main()
