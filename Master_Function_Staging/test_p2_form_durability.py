"""
P2 form-path durability tests (MFS analysis 2026-07-11 — the BSH FS-twin
backlog, superrepo docs/audits/master_function_analysis_2026-07-11.md).

D1  — email fulfillment reports 'sent' only when the SES call succeeded;
      a lost store emits the alarmable [FORM_SUBMISSION_LOST] marker and
      the visitor is NOT told success.
D4  — every post-store step is best-effort: a stored submission always
      reports success; visitor-supplied 'urgency' of any type and
      malformed field entries never crash the pipeline.
D5  — float form values are coerced to Decimal before the boto3 resource
      put_item (which raises TypeError on float).
D10 — SMS monthly cap: skip-and-continue with the alarmable
      [SMS_RATE_LIMITED] marker (BSH-FS4 semantics; cap stays a soft,
      non-atomic cost guardrail).
"""
import json
import unittest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError


def _fh():
    """Resolve form_handler at call time.

    test_form_handler_env_tables.py del's sys.modules['form_handler'] and
    re-imports it, so module-level bindings taken at collection time go
    stale in a full-suite run (patch('form_handler.X') would then target a
    different module object than the class under test).
    """
    import form_handler
    return form_handler


TENANT_CONFIG = {
    'tenant_id': 'T1',
    'tenant_hash': 'h1',
    'conversational_forms': {
        'volunteer': {
            'title': 'Volunteer Signup',
            'fields': [{'id': 'email', 'label': 'Email'}],
        },
    },
}


def _submit(handler, responses):
    return handler.handle_form_submission(
        {'form_type': 'volunteer', 'responses': responses})


def _patch_pii(test):
    p = patch('pii_subject.get_or_create_pii_subject_id', return_value='psub_x')
    p.start()
    test.addCleanup(p.stop)


class TestD1StoreFailure(unittest.TestCase):

    def test_store_failure_emits_marker_and_fails_honestly(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        _patch_pii(self)
        table = MagicMock()
        table.put_item.side_effect = ClientError(
            {'Error': {'Code': 'ProvisionedThroughputExceededException'}}, 'PutItem')
        with patch.object(_fh(), 'dynamodb') as md, \
             self.assertLogs(level='ERROR') as captured:
            md.Table.return_value = table
            result = _submit(handler, {'email': 'a@b.org'})
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], 'form_processing_failed')
        joined = '\n'.join(captured.output)
        self.assertIn('[FORM_SUBMISSION_LOST]', joined)
        self.assertIn('tenant_id=T1', joined)
        self.assertIn('form_id=volunteer', joined)

    def test_float_store_typeerror_also_hits_marker_path(self):
        # Belt-and-braces: even if a non-ClientError escapes the store, it
        # must fail honestly, not surface as an unhandled 200.
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        _patch_pii(self)
        table = MagicMock()
        table.put_item.side_effect = TypeError('Float types are not supported')
        with patch.object(_fh(), 'dynamodb') as md, \
             self.assertLogs(level='ERROR') as captured:
            md.Table.return_value = table
            result = _submit(handler, {'email': 'a@b.org'})
        self.assertFalse(result['success'])
        self.assertIn('[FORM_SUBMISSION_LOST]', '\n'.join(captured.output))


class TestD4PostStoreIsolation(unittest.TestCase):

    def _handler_with_working_store(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        _patch_pii(self)
        self.stored = {}
        table = MagicMock()
        table.put_item.side_effect = (
            # FS5: accept ConditionExpression (and future kwargs) — the real
            # put_item now carries attribute_not_exists(submission_id).
            lambda Item, **kw: self.stored.__setitem__(Item['submission_id'], Item))
        patcher = patch.object(_fh(), 'dynamodb')
        md = patcher.start()
        self.addCleanup(patcher.stop)
        md.Table.return_value = table
        return handler

    def test_notification_crash_does_not_fail_stored_submission(self):
        handler = self._handler_with_working_store()
        with patch.object(handler, '_send_notifications',
                          side_effect=RuntimeError('SES exploded')):
            result = _submit(handler, {'email': 'a@b.org'})
        self.assertTrue(result['success'])
        self.assertEqual(result['notifications_sent'], [])

    def test_fulfillment_crash_does_not_fail_stored_submission(self):
        handler = self._handler_with_working_store()
        with patch.object(handler, '_process_fulfillment',
                          side_effect=RuntimeError('boom')):
            result = _submit(handler, {'email': 'a@b.org'})
        self.assertTrue(result['success'])
        self.assertEqual(result['fulfillment']['status'], 'error')

    def test_non_string_urgency_does_not_crash(self):
        handler = self._handler_with_working_store()
        result = _submit(handler, {'email': 'a@b.org',
                                   'urgency': ['immediate', 'asap']})
        self.assertTrue(result['success'])

    def test_build_display_text_tolerates_field_without_id(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        text = handler._build_display_text(
            {'email': 'a@b.org'},
            {'fields': [{'label': 'no id here'}, {'id': 'email', 'label': 'Email'}]})
        self.assertIn('Email: a@b.org', text)


class TestD5FloatCoercion(unittest.TestCase):

    def test_floats_stored_as_decimal(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        _patch_pii(self)
        stored = {}
        table = MagicMock()
        table.put_item.side_effect = (
            # FS5: accept ConditionExpression (and future kwargs).
            lambda Item, **kw: stored.__setitem__(Item['submission_id'], Item))
        with patch.object(_fh(), 'dynamodb') as md:
            md.Table.return_value = table
            result = _submit(handler, {'email': 'a@b.org',
                                       'donation': 25.50,
                                       'nested': {'rating': 4.5}})
        self.assertTrue(result['success'])
        item = list(stored.values())[0]
        self.assertIsInstance(item['form_data']['donation'], Decimal)
        self.assertIsInstance(item['form_data']['nested']['rating'], Decimal)
        self.assertEqual(item['form_data']['donation'], Decimal('25.5'))


class TestD1EmailFulfillmentTruthfulness(unittest.TestCase):

    def _fulfill(self, ses_fails):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        with patch.object(_fh(), 'ses') as mock_ses:
            if ses_fails:
                mock_ses.send_email.side_effect = ClientError(
                    {'Error': {'Code': 'MessageRejected'}}, 'SendEmail')
            else:
                mock_ses.send_email.return_value = {'MessageId': 'mid-1'}
            return handler._process_fulfillment(
                form_config={'fulfillment': {'type': 'email', 'template': 'thank_you'}},
                form_type='volunteer',
                responses={'email': 'a@b.org'},
                submission_id='sub1',
            )

    def test_ses_failure_reports_error_not_sent(self):
        result = self._fulfill(ses_fails=True)
        self.assertEqual(result['type'], 'email')
        self.assertEqual(result['status'], 'error')

    def test_ses_success_reports_sent(self):
        result = self._fulfill(ses_fails=False)
        self.assertEqual(result['status'], 'sent')

    def test_missing_email_reports_skipped(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        result = handler._process_fulfillment(
            form_config={'fulfillment': {'type': 'email'}},
            form_type='volunteer', responses={}, submission_id='sub1')
        self.assertEqual(result['status'], 'skipped')


class TestD10SmsRateLimitMarker(unittest.TestCase):

    def test_cap_hit_skips_with_alarmable_marker(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        with patch.object(handler, '_get_monthly_sms_usage', return_value=100), \
             patch.object(_fh(), 'sns') as mock_sns, \
             self.assertLogs(level='ERROR') as captured:
            sent = handler._send_sms_notifications(
                {'monthly_limit': 100, 'recipients': ['+15551234567']},
                {'form_type': 'volunteer', 'responses': {}, 'submission_id': 's1'})
        self.assertEqual(sent, [])
        mock_sns.publish.assert_not_called()
        joined = '\n'.join(captured.output)
        self.assertIn('[SMS_RATE_LIMITED]', joined)
        self.assertIn('tenant_id=T1', joined)
        self.assertIn('usage=100 limit=100', joined)
        self.assertIn('skipped=1', joined)

    def test_under_cap_sends_normally(self):
        fh = _fh(); handler = fh.FormHandler(TENANT_CONFIG)
        with patch.object(handler, '_get_monthly_sms_usage', return_value=0), \
             patch.object(handler, '_increment_sms_usage'), \
             patch.object(_fh(), 'sns') as mock_sns:
            mock_sns.publish.return_value = {'MessageId': 'sms-1'}
            sent = handler._send_sms_notifications(
                {'monthly_limit': 100, 'recipients': ['+15551234567']},
                {'form_type': 'volunteer', 'responses': {}, 'submission_id': 's1'})
        self.assertEqual(sent, ['sms:+15551234567'])


if __name__ == '__main__':
    unittest.main()
