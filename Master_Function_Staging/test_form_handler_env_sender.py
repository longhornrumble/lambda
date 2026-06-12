#!/usr/bin/env python3
"""form_handler.py default SES sender env-var resolution tests.

Verifies _default_from_email() resolves from SES_FROM_EMAIL when set, and
falls back to the legacy hardcoded sender with a loud SENDER_ENV_MISSING
warning when unset — never crashing (prod envs aren't wired yet).
"""

import os
import unittest
from unittest.mock import patch

import form_handler


class TestDefaultFromEmail(unittest.TestCase):
    """_default_from_email reads env at call time — no module reload needed."""

    def test_env_set_is_used(self):
        with patch.dict(os.environ, {'SES_FROM_EMAIL': 'notify@staging.myrecruiter.ai'}):
            self.assertEqual(form_handler._default_from_email(), 'notify@staging.myrecruiter.ai')

    def test_env_set_emits_no_warning(self):
        with patch.dict(os.environ, {'SES_FROM_EMAIL': 'notify@staging.myrecruiter.ai'}):
            with self.assertNoLogs(level='WARNING'):
                form_handler._default_from_email()

    def test_env_missing_falls_back_to_legacy_sender(self):
        if 'SES_FROM_EMAIL' in os.environ:
            del os.environ['SES_FROM_EMAIL']
        self.assertEqual(form_handler._default_from_email(), 'notify@myrecruiter.ai')

    def test_env_missing_emits_loud_warning(self):
        if 'SES_FROM_EMAIL' in os.environ:
            del os.environ['SES_FROM_EMAIL']
        with self.assertLogs(level='WARNING') as cm:
            form_handler._default_from_email()
        self.assertTrue(any('SENDER_ENV_MISSING' in line for line in cm.output))


if __name__ == '__main__':
    unittest.main()
