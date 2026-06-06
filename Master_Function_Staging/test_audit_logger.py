"""Unit tests for audit_logger.log_form_submission (lambda#251).

These assert the writer produces the CANONICAL picasso-audit shape
(tenant_hash + timestamp_event_id), which is the exact property the bespoke
form-handler writer lacked (it wrote tenant_id + timestamp → ValidationException
against every granted table). DynamoDB is mocked; no AWS calls.
"""
import json
import unittest
from unittest.mock import patch, MagicMock

import audit_logger as al


class TestLogFormSubmission(unittest.TestCase):
    def _logger_with_mock_ddb(self):
        lg = al.AuditLogger()
        lg.dynamodb = MagicMock()
        lg.cloudwatch = MagicMock()
        return lg

    def test_writes_canonical_picasso_audit_shape(self):
        lg = self._logger_with_mock_ddb()

        ok = lg.log_form_submission(
            tenant_id='TEN123',
            session_id='sess1',
            submission_id='sub_1',
            form_type='volunteer_signup',
            notification_count=2,
            fulfillment_status='sent',
        )

        self.assertTrue(ok)
        lg.dynamodb.put_item.assert_called_once()
        kwargs = lg.dynamodb.put_item.call_args.kwargs
        item = kwargs['Item']
        # The canonical key shape — the whole point of the fix
        self.assertIn('tenant_hash', item)
        self.assertIn('timestamp_event_id', item)
        # NOT the broken bespoke shape
        self.assertNotIn('tenant_id', item)
        self.assertEqual(item['event_type']['S'], 'FORM_SUBMISSION')
        self.assertEqual(item['severity']['S'], al.AuditSeverity.LOW)
        # Context carries the metadata, PII-free
        ctx = json.loads(item['context']['S'])
        self.assertEqual(ctx['submission_id'], 'sub_1')
        self.assertEqual(ctx['form_type'], 'volunteer_signup')
        self.assertEqual(ctx['notification_count'], 2)
        self.assertEqual(ctx['fulfillment_status'], 'sent')

    def test_pii_in_context_is_redacted(self):
        """Anything passed is still PII-scanned — an email-shaped value is redacted."""
        lg = self._logger_with_mock_ddb()

        lg.log_form_submission(tenant_id='T', form_type='person@example.com')

        item = lg.dynamodb.put_item.call_args.kwargs['Item']
        ctx_raw = item['context']['S']
        self.assertNotIn('person@example.com', ctx_raw)
        self.assertIn('REDACTED', ctx_raw)

    def test_write_failure_returns_false_non_fatal(self):
        from botocore.exceptions import ClientError
        lg = self._logger_with_mock_ddb()
        lg.dynamodb.put_item.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'no'}}, 'PutItem'
        )

        # Must not raise; returns False
        self.assertFalse(lg.log_form_submission(tenant_id='T', submission_id='s'))


if __name__ == '__main__':
    unittest.main()
