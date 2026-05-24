"""Unit tests for the DSAR weekly reminder Lambda.

Pure unit tests using unittest.mock — no boto3 / DDB / SNS infrastructure
needed. The Lambda's responsibility is small (compose message + publish to
SNS), so the tests focus on:

  1. Message composition is correct + includes the right env-var values
  2. SNS publish is called with the right TopicArn + Subject + Message
  3. Misconfiguration (SNS_TOPIC_ARN unset) fails closed
  4. SNS ClientError re-raises (alarmable surface)
  5. The reminder body does not leak PII or consumer data (D1 posture)
"""
import importlib
import os
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError


# Sprint E2 / audit N14 — env-var test isolation.
# Tests below mutate os.environ via _reload_module(); the keys we manipulate
# are enumerated here so the per-class setUp/tearDown snapshots and restores
# them. Without this, a test's env mutation persists to later tests
# (especially across test files when pytest collects modules in series),
# producing brittle pass/fail depending on collection order.
_TEST_ENV_KEYS = (
    'SNS_TOPIC_ARN',
    'SLA_MONITOR_FUNCTION_NAME',
    'AUDIT_TABLE',
    'SLA_DAYS_INTAKE_PLUS',
    'PLAYBOOK_URL',
)


def _reload_module(env: dict):
    """Reload lambda_function with a clean env so env-var reads are fresh.

    NOTE: this mutates os.environ. The TestCase classes below snapshot+restore
    via setUp/tearDown so the mutation does not leak across tests (audit N14).
    """
    for k, v in env.items():
        os.environ[k] = v
    import lambda_function
    return importlib.reload(lambda_function)


class _EnvIsolatedTestCase(unittest.TestCase):
    """Base class providing per-test env snapshot+restore for the keys
    _reload_module mutates. Subclasses inherit setUp/tearDown automatically;
    if they define their own, they MUST call super().setUp()/tearDown()
    (enforced via __init_subclass__ guard, audit-of-audit finding 16).
    """

    def setUp(self):
        self._env_snapshot = {k: os.environ.get(k) for k in _TEST_ENV_KEYS}

    def tearDown(self):
        for k, v in self._env_snapshot.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # Sprint F2 / audit-of-audit finding 16: previous "subclasses MUST call
    # super()" rule was convention-only. A subclass defining setUp/tearDown
    # without invoking super() would silently break env isolation. Reviewer
    # (test-engineer) flagged this as latent fragility. __init_subclass__ now
    # AST-walks the override at class-definition time and asserts at least
    # one `super(...)` call exists in the function body. (String-match on
    # source would false-positive on `# super()` in a comment.)
    # If detection ever needs to be bypassed (e.g., super() invoked via an
    # unusual mechanism), opt-out by setting _SUPER_SETUP_VERIFIED = True.
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, "_SUPER_SETUP_VERIFIED", False):
            return
        import ast
        import inspect
        import textwrap
        for method_name in ("setUp", "tearDown"):
            if method_name not in cls.__dict__:
                continue  # not overridden
            method = cls.__dict__[method_name]
            try:
                src = textwrap.dedent(inspect.getsource(method))
                tree = ast.parse(src)
            except (OSError, TypeError, SyntaxError):
                # Can't parse — skip rather than break test collection.
                continue
            # Walk the AST looking for `super(...)` calls
            has_super_call = any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "super"
                for node in ast.walk(tree)
            )
            if not has_super_call:
                raise TypeError(
                    f"{cls.__name__}.{method_name}() overrides "
                    f"_EnvIsolatedTestCase but does not call super() — "
                    f"env-var snapshot/restore will silently break. Either "
                    f"call super().{method_name}() or set "
                    f"_SUPER_SETUP_VERIFIED = True on the subclass to opt "
                    f"out of this check."
                )


class TestBuildMessage(_EnvIsolatedTestCase):
    def test_message_includes_function_name(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
            'SLA_MONITOR_FUNCTION_NAME': 'picasso-pii-dsar-sla-monitor-staging',
            'AUDIT_TABLE': 'picasso-pii-dsar-audit-staging',
            'SLA_DAYS_INTAKE_PLUS': '25',
        })
        body = mod._build_message()
        self.assertIn('picasso-pii-dsar-sla-monitor-staging', body)

    def test_message_includes_audit_table_name(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
            'AUDIT_TABLE': 'picasso-pii-dsar-audit-staging',
        })
        body = mod._build_message()
        self.assertIn('picasso-pii-dsar-audit-staging', body)

    def test_message_uses_in_progress_status(self):
        """Regression guard: must NOT use status='open' (which is the bug
        in playbook §8 pre-M9.G6 fix). Must use 'in_progress' to match
        what the DSAR Lambda's audit writer actually emits."""
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        body = mod._build_message()
        self.assertIn('"in_progress"', body)
        self.assertNotIn('":open"', body)

    def test_message_includes_threshold_days(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
            'SLA_DAYS_INTAKE_PLUS': '25',
        })
        body = mod._build_message()
        self.assertIn('25', body)

    def test_message_template_excludes_pii_terms_regression_guard(self):
        """Regression guard against a future code change adding consumer or
        operator metadata to the reminder body. The body is static text today
        (the message-building function takes no PII inputs), so this test is
        vacuously true at present — that is intentional. Its purpose is to
        FIRE when someone later adds a parameter that surfaces email / phone /
        tenant_id / caller_arn into the body, which would violate the D1
        posture. Renamed from `test_message_no_consumer_pii` per audit N13
        to make the regression-guard intent explicit."""
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        body = mod._build_message().lower()
        for forbidden in ['email', 'phone', 'subject_identifier', 'tenant_id', 'caller_arn']:
            self.assertNotIn(forbidden, body, f'leaked: {forbidden}')

    def test_message_references_playbook(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
            'PLAYBOOK_URL': 'https://example.com/playbook',
        })
        body = mod._build_message()
        self.assertIn('https://example.com/playbook', body)

    def test_message_explicitly_frames_as_reminder_only(self):
        """The reminder body must explicitly state it is a REMINDER ONLY
        and does not fetch live data — operator was confused by missing
        status field in the first test-fire (2026-05-23). Framing prevents
        the missing-status-as-bug interpretation."""
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        body = mod._build_message()
        self.assertIn('REMINDER ONLY', body)
        # The reason for no-live-data MUST also be stated (the independence
        # property is the whole point of M9.G6).
        self.assertIn('independent', body.lower())


class TestPublishReminder(_EnvIsolatedTestCase):
    def test_publish_called_with_topic_and_subject(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        mock_sns = MagicMock()
        with patch.object(mod, 'sns', mock_sns):
            mod._publish_reminder('hello body')
        mock_sns.publish.assert_called_once()
        call_kwargs = mock_sns.publish.call_args.kwargs
        self.assertEqual(call_kwargs['TopicArn'], 'arn:aws:sns:us-east-1:000000000000:test')
        self.assertEqual(call_kwargs['Message'], 'hello body')
        self.assertIn('[Picasso DSAR]', call_kwargs['Subject'])
        # SNS subject hard limit
        self.assertLessEqual(len(call_kwargs['Subject']), 100)

    def test_publish_fails_closed_when_topic_arn_unset(self):
        # Explicitly empty the SNS_TOPIC_ARN
        if 'SNS_TOPIC_ARN' in os.environ:
            del os.environ['SNS_TOPIC_ARN']
        import lambda_function
        mod = importlib.reload(lambda_function)
        with self.assertRaises(RuntimeError) as ctx:
            mod._publish_reminder('body')
        self.assertIn('SNS_TOPIC_ARN', str(ctx.exception))

    def test_publish_reraises_client_error(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = ClientError(
            {'Error': {'Code': 'InternalError'}}, 'Publish',
        )
        with patch.object(mod, 'sns', mock_sns):
            with self.assertRaises(ClientError):
                mod._publish_reminder('body')


class TestLambdaHandler(_EnvIsolatedTestCase):
    def test_handler_publishes_and_returns_published(self):
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        mock_sns = MagicMock()
        with patch.object(mod, 'sns', mock_sns):
            result = mod.lambda_handler({}, None)
        self.assertEqual(result, {'published': True})
        mock_sns.publish.assert_called_once()

    def test_handler_ignores_event_payload(self):
        """Idempotency: any input event must produce the same output (and
        same call to SNS). The Lambda is fire-and-forget from EventBridge."""
        mod = _reload_module({
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:test',
        })
        mock_sns = MagicMock()
        with patch.object(mod, 'sns', mock_sns):
            mod.lambda_handler({'unrelated': 'payload'}, None)
            mod.lambda_handler({}, None)
        self.assertEqual(mock_sns.publish.call_count, 2)
        # Both calls have same Message
        msg_1 = mock_sns.publish.call_args_list[0].kwargs['Message']
        msg_2 = mock_sns.publish.call_args_list[1].kwargs['Message']
        self.assertEqual(msg_1, msg_2)


class TestEnvIsolationSubclassGuard(unittest.TestCase):
    """Sprint F2 / audit-of-audit finding 16: assert __init_subclass__ guard
    fires when a subclass overrides setUp without calling super()."""

    def test_subclass_setup_without_super_raises_typeerror(self):
        with self.assertRaises(TypeError) as ctx:
            class _BadSubclass(_EnvIsolatedTestCase):
                def setUp(self):
                    # Intentionally NOT calling super().setUp()
                    pass
        self.assertIn('setUp', str(ctx.exception))
        self.assertIn('super()', str(ctx.exception))

    def test_subclass_teardown_without_super_raises_typeerror(self):
        with self.assertRaises(TypeError) as ctx:
            class _BadSubclass(_EnvIsolatedTestCase):
                def tearDown(self):
                    pass
        self.assertIn('tearDown', str(ctx.exception))

    def test_subclass_with_super_call_passes(self):
        """No raise: legitimate subclass that does call super()."""
        class _GoodSubclass(_EnvIsolatedTestCase):
            def setUp(self):
                super().setUp()
            def tearDown(self):
                super().tearDown()
        # Reaching here means no TypeError raised at class-def time
        self.assertTrue(True)

    def test_subclass_with_opt_out_marker_passes(self):
        """Escape hatch: _SUPER_SETUP_VERIFIED bypasses the check."""
        class _OptOut(_EnvIsolatedTestCase):
            _SUPER_SETUP_VERIFIED = True
            def setUp(self):
                pass  # No super() call, but opted out
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
