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


def _reload_module(env: dict):
    """Reload lambda_function with a clean env so env-var reads are fresh."""
    for k, v in env.items():
        os.environ[k] = v
    import lambda_function
    return importlib.reload(lambda_function)


class TestBuildMessage(unittest.TestCase):
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

    def test_message_no_consumer_pii(self):
        """The reminder body is static text — must not include any consumer
        identifier fields (email, phone, name, IP, PSID, etc.) or operator
        metadata (operator name, tenant id). D1 posture."""
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


class TestPublishReminder(unittest.TestCase):
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


class TestLambdaHandler(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
