"""
P1 security hardening tests (MFS analysis 2026-07-11, superrepo
docs/audits/master_function_analysis_2026-07-11.md).

S1  — get_config serves a frontend-safe projection: staff notification
      emails/phones, webhook secrets, fulfillment targets, and internal
      tenant_id must never reach the public endpoint (tenant_hash is
      embedded in client pages, so the response is world-readable).
S2  — visitor-supplied form values are HTML-escaped before interpolation
      into notification/confirmation email HTML.
S3  — email/phone are redacted at logging sites (redact_pii).
S4/D2 — failed form submissions return honest 4xx/5xx with generic
      client messages; str(e) internals stay server-side.
"""
import json
import unittest
from unittest.mock import patch, MagicMock

from lambda_function import _frontend_safe_config, _frontend_safe_form


# A full-shape config with every sensitive surface populated. The projection
# must keep the widget-read keys and drop everything else.
FULL_CONFIG = {
    # Widget-read keys (must survive)
    'tenant_hash': 'abc123def456',
    'chat_title': 'Test Org',
    'welcome_message': 'Hi!',
    'branding': {'primary_color': '#0f0'},
    'features': {'streaming_enabled': True},
    'feature_flags': {'REACH_PING': True},
    'action_chips': {'chips': []},
    'cta_definitions': {'cta1': {'label': 'Apply', 'action': 'start_form'}},
    'conversation_branches': {},
    'quick_help': ['q1'],
    'privacy_notice_url': 'https://example.org/privacy',
    'scheduling': {'appointment_types': {'intro': {'max_advance_days': 30}}},
    'widget_behavior': {'auto_open': False},
    'streaming': {'method': 'sse'},
    'metadata': {'source': 's3'},
    'conversational_forms': {
        'volunteer': {
            'title': 'Volunteer Signup',
            'fields': [{'id': 'email', 'type': 'email', 'label': 'Email'}],
            'post_submission': {
                'confirmation_message': 'Thanks!',
                'next_steps': 'We will call you.',
                'actions': [],
                'fulfillment': {   # SENSITIVE (nested)
                    'method': 'email',
                    'recipients': ['staff@example.org'],
                    'webhook_url': 'https://hooks.example.org/x',
                },
            },
            'notifications': {     # SENSITIVE
                'internal': {
                    'recipients': ['staff@example.org'],
                    'sms_recipients': ['+15551234567'],
                },
                'applicant_confirmation': {'enabled': True},
            },
            'fulfillment': {       # SENSITIVE (legacy form-level)
                'type': 'email',
                'template': 'thank_you',
            },
        },
    },
    # Server-side-only keys (must be dropped)
    'tenant_id': 'AUS123957',
    'tone_prompt': 'You are ...',
    'aws': {'knowledge_base_id': 'KB123'},
    'webhook': {'url': 'https://internal.example.org', 'headers': {'x-api-key': 'SECRET'}},
    'notification_settings': {'from_email': 'noreply@example.org'},
    'channels': {'messenger': {'page_token': 'PAGETOKEN'}},
    'monitor': {'webhookUrl': 'https://hooks.slack.example/T000'},
    'bedrock_instructions': {'formatting': 'x'},
    'email_templates': {'thank_you': {'subject': 's', 'body': 'b'}},
}

SENSITIVE_STRINGS = (
    'AUS123957', 'KB123', 'SECRET', 'PAGETOKEN',
    'staff@example.org', '+15551234567',
    'hooks.example.org', 'internal.example.org', 'hooks.slack.example',
    'noreply@example.org',
)


class TestS1FrontendSafeConfig(unittest.TestCase):

    def test_no_sensitive_material_in_served_body(self):
        served = json.dumps(_frontend_safe_config(FULL_CONFIG))
        for needle in SENSITIVE_STRINGS:
            self.assertNotIn(needle, served,
                             f"sensitive value {needle!r} leaked through get_config projection")

    def test_server_side_keys_dropped(self):
        safe = _frontend_safe_config(FULL_CONFIG)
        for key in ('tenant_id', 'tone_prompt', 'aws', 'webhook',
                    'notification_settings', 'channels', 'monitor',
                    'bedrock_instructions', 'email_templates'):
            self.assertNotIn(key, safe)

    def test_widget_read_keys_survive(self):
        safe = _frontend_safe_config(FULL_CONFIG)
        for key in ('tenant_hash', 'chat_title', 'welcome_message', 'branding',
                    'features', 'feature_flags', 'conversational_forms',
                    'action_chips', 'cta_definitions', 'conversation_branches',
                    'quick_help', 'privacy_notice_url', 'scheduling',
                    'widget_behavior', 'streaming', 'metadata'):
            self.assertIn(key, safe, f"widget-read key {key!r} was dropped")

    def test_form_keeps_fields_and_post_submission_messaging(self):
        form = _frontend_safe_config(FULL_CONFIG)['conversational_forms']['volunteer']
        self.assertEqual(form['title'], 'Volunteer Signup')
        self.assertEqual(form['fields'][0]['id'], 'email')
        # post_submission messaging survives; only its fulfillment child is stripped
        self.assertEqual(form['post_submission']['confirmation_message'], 'Thanks!')
        self.assertEqual(form['post_submission']['next_steps'], 'We will call you.')
        self.assertNotIn('fulfillment', form['post_submission'])
        self.assertNotIn('notifications', form)
        self.assertNotIn('fulfillment', form)

    def test_old_shape_config_does_not_crash(self):
        # Forward-compatible reads: a minimal legacy config with none of the
        # newer keys must project cleanly.
        old = {'tenant_hash': 'abc', 'chat_title': 'Old Org'}
        safe = _frontend_safe_config(old)
        self.assertEqual(safe, {'tenant_hash': 'abc', 'chat_title': 'Old Org'})

    def test_projection_does_not_mutate_source(self):
        # The loader caches the config object; the projection must never
        # mutate it (shallow-copy hazard, analysis D14).
        _frontend_safe_config(FULL_CONFIG)
        self.assertIn('notifications', FULL_CONFIG['conversational_forms']['volunteer'])
        self.assertIn('fulfillment',
                      FULL_CONFIG['conversational_forms']['volunteer']['post_submission'])

    def test_non_dict_form_passthrough(self):
        self.assertEqual(_frontend_safe_form('weird'), 'weird')


class TestS2EmailHtmlEscaping(unittest.TestCase):

    def _handler(self):
        from form_handler import FormHandler
        return FormHandler({'tenant_id': 'T1', 'tenant_hash': 'h1'})

    def test_build_email_body_escapes_values(self):
        handler = self._handler()
        body = handler._build_email_body(
            {
                'form_type': 'volunteer',
                'submission_id': 'sub1',
                'responses': {'name': '<script>alert(1)</script>', 'note': 'a & b'},
            },
            template_name=None,
        )
        self.assertNotIn('<script>', body)
        self.assertIn('&lt;script&gt;', body)
        self.assertIn('a &amp; b', body)

    def test_applicant_confirmation_escapes_values(self):
        handler = self._handler()
        handler.tenant_config = {'chat_title': 'Org'}
        with patch('form_handler.ses') as mock_ses:
            handler._send_applicant_confirmation(
                'user@example.org',
                {'enabled': True, 'subject': 'Hi {first_name}',
                 'body_template': 'Hello {first_name}'},
                {'first_name': '<img src=x onerror=alert(1)>'},
                'volunteer', {}, 'sub1',
            )
            html_body = mock_ses.send_email.call_args.kwargs['Message']['Body']['Html']['Data']
            self.assertNotIn('<img', html_body)
            self.assertIn('&lt;img', html_body)


class TestS3LogRedaction(unittest.TestCase):

    def test_email_send_log_redacts_recipient(self):
        from form_handler import FormHandler
        handler = FormHandler({'tenant_id': 'T1', 'tenant_hash': 'h1'})
        with patch('form_handler.ses') as mock_ses, \
             patch('form_handler.notification_sends_table'), \
             self.assertLogs(level='INFO') as captured:
            mock_ses.send_email.return_value = {'MessageId': 'mid-1'}
            handler._send_email_notifications(
                {'recipients': ['victim@example.org'], 'sender': 'noreply@x.org'},
                {'form_type': 'volunteer', 'responses': {}, 'submission_id': 's1'},
                'normal',
            )
        joined = '\n'.join(captured.output)
        self.assertNotIn('victim@example.org', joined)
        self.assertIn('[EMAIL]', joined)

    def test_intent_router_redacts_user_input(self):
        import intent_router
        event = {
            'queryStringParameters': {'t': 'abcdef123456'},
            'body': json.dumps({'user_input': 'reach me at victim@example.org please'}),
        }
        with patch.object(intent_router, 'TENANT_CONFIG_AVAILABLE', False), \
             self.assertLogs(level='INFO') as captured:
            intent_router.route_intent(event)
        joined = '\n'.join(captured.output)
        self.assertNotIn('victim@example.org', joined)


class TestS4D2HonestErrors(unittest.TestCase):

    def _submit(self, handler_result):
        import lambda_function as lf
        event = {'body': json.dumps({'form_type': 'volunteer', 'responses': {'a': 1}})}
        mock_handler = MagicMock()
        mock_handler.handle_form_submission.return_value = handler_result
        with patch('form_handler.FormHandler', return_value=mock_handler), \
             patch('tenant_config_loader.get_config_for_tenant_by_hash', return_value={'x': 1}), \
             patch('tenant_config_loader.resolve_tenant_hash', return_value='T1'):
            return lf.handle_form_submission(event, 'abcdef123456', 'req-1')

    def test_success_returns_200(self):
        response = self._submit({'success': True, 'submission_id': 's1'})
        self.assertEqual(response['statusCode'], 200)

    def test_processing_failure_returns_502_generic(self):
        response = self._submit({'success': False, 'error': 'form_processing_failed'})
        self.assertEqual(response['statusCode'], 502)
        body = json.loads(response['body'])
        self.assertEqual(body['error'], 'form_processing_failed')
        # No internal detail in the client body
        self.assertNotIn('Traceback', response['body'])

    def test_invalid_form_returns_400(self):
        response = self._submit({'success': False, 'error': 'invalid_form'})
        self.assertEqual(response['statusCode'], 400)

    def test_form_handler_exception_is_not_leaked(self):
        from form_handler import FormHandler
        handler = FormHandler({'tenant_id': 'T1', 'tenant_hash': 'h1',
                               'conversational_forms': {'volunteer': {'title': 'V'}}})
        with patch.object(handler, '_store_submission',
                          side_effect=RuntimeError('picasso-form-submissions ARN detail')):
            result = handler.handle_form_submission(
                {'form_type': 'volunteer', 'responses': {'a': 1}})
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], 'form_processing_failed')
        self.assertNotIn('ARN', json.dumps(result))

    def test_unknown_form_type_maps_to_invalid_form(self):
        from form_handler import FormHandler
        handler = FormHandler({'tenant_id': 'T1', 'tenant_hash': 'h1',
                               'conversational_forms': {}})
        result = handler.handle_form_submission(
            {'form_type': 'nope', 'responses': {'a': 1}})
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], 'invalid_form')


if __name__ == '__main__':
    unittest.main()
