#!/usr/bin/env python3
"""form_handler.py table-name env-var resolution tests.

Verifies that the four DDB table constants resolve from environment variables
when set, with fallback to the legacy prod-account hardcoded names. Lets
staging route to staging-suffixed tables without touching prod behavior.
"""

import importlib
import os
import sys
import unittest
from unittest.mock import patch


class TestFormHandlerEnvTables(unittest.TestCase):
    """Constants are read at module-import time, so each test patches env
    BEFORE reloading the module."""

    def _reload_with_env(self, env_overrides):
        # Drop the cached module so re-import re-evaluates the constants
        # against the patched env. importlib.reload would keep the same
        # module object; del + import gives a fresh evaluation.
        if 'form_handler' in sys.modules:
            del sys.modules['form_handler']
        with patch.dict(os.environ, env_overrides, clear=False):
            import form_handler
            return form_handler

    def test_defaults_match_legacy_prod_names(self):
        """No env vars set → constants resolve to prod-account legacy names."""
        # Clear the four env vars to ensure defaults are exercised.
        for key in ('FORM_SUBMISSIONS_TABLE', 'SMS_USAGE_TABLE', 'FORM_AUDIT_TABLE', 'NOTIFICATION_SENDS_TABLE'):
            if key in os.environ:
                del os.environ[key]
        fh = self._reload_with_env({})

        self.assertEqual(fh.SUBMISSIONS_TABLE, 'picasso_form_submissions')
        self.assertEqual(fh.SMS_USAGE_TABLE, 'picasso_sms_usage')
        self.assertEqual(fh.AUDIT_TABLE, 'picasso_audit_logs')
        self.assertEqual(fh.NOTIFICATION_SENDS_TABLE, 'picasso-notification-sends')

    def test_env_vars_override_defaults(self):
        """Env vars set → constants resolve to staging-suffixed values."""
        fh = self._reload_with_env({
            'FORM_SUBMISSIONS_TABLE': 'picasso-form-submissions-staging',
            'SMS_USAGE_TABLE': 'picasso-sms-usage-staging',
            'FORM_AUDIT_TABLE': 'picasso-audit-staging',
            'NOTIFICATION_SENDS_TABLE': 'picasso-notification-sends-staging',
        })

        self.assertEqual(fh.SUBMISSIONS_TABLE, 'picasso-form-submissions-staging')
        self.assertEqual(fh.SMS_USAGE_TABLE, 'picasso-sms-usage-staging')
        self.assertEqual(fh.AUDIT_TABLE, 'picasso-audit-staging')
        self.assertEqual(fh.NOTIFICATION_SENDS_TABLE, 'picasso-notification-sends-staging')

    def test_partial_override_keeps_defaults_for_unset(self):
        """Setting one env var only changes that constant; others stay default."""
        # Clear the others first
        for key in ('SMS_USAGE_TABLE', 'FORM_AUDIT_TABLE', 'NOTIFICATION_SENDS_TABLE'):
            if key in os.environ:
                del os.environ[key]

        fh = self._reload_with_env({
            'FORM_SUBMISSIONS_TABLE': 'only-this-overridden',
        })

        self.assertEqual(fh.SUBMISSIONS_TABLE, 'only-this-overridden')
        self.assertEqual(fh.SMS_USAGE_TABLE, 'picasso_sms_usage')
        self.assertEqual(fh.AUDIT_TABLE, 'picasso_audit_logs')
        self.assertEqual(fh.NOTIFICATION_SENDS_TABLE, 'picasso-notification-sends')


if __name__ == '__main__':
    unittest.main()
