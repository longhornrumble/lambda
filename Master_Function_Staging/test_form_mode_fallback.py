#!/usr/bin/env python3
"""Forms-fallback wire-up tests for handle_chat.

When the widget HTTP path POSTs `{form_mode: true, action: 'submit_form', ...}`
to ?action=chat (BSH streaming unavailable), handle_chat must delegate to the
existing handle_form_submission so MFS can serve forms as a fallback.

Strict `is True` check — string "true" or missing key must NOT delegate, so
non-form chat requests are unaffected.
"""

import json
import unittest
from unittest.mock import patch


def _chat_event(body_dict):
    return {
        'httpMethod': 'POST',
        'queryStringParameters': {'action': 'chat', 't': 'test_tenant_hash'},
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body_dict),
    }


class TestHandleChatFormModeFallback(unittest.TestCase):

    def test_form_mode_true_delegates_to_form_handler(self):
        from lambda_function import handle_chat

        event = _chat_event({
            'form_mode': True,
            'action': 'submit_form',
            'form_id': 'volunteer_signup',
            'form_data': {'first_name': 'John', 'email': 'john@example.com'},
            'session_id': 'sess_1',
        })
        sentinel = {'statusCode': 200, 'body': json.dumps({'success': True})}

        with patch('lambda_function.handle_form_submission', return_value=sentinel) as mock_form:
            response = handle_chat(event, 'test_tenant_hash', 'req-1')

        mock_form.assert_called_once()
        args = mock_form.call_args[0]
        self.assertEqual(args[1], 'test_tenant_hash')
        self.assertEqual(args[2], 'req-1')
        self.assertEqual(response, sentinel)

    def test_form_mode_translates_widget_shape(self):
        """Widget body (form_id, form_data) → FormHandler body (form_type, responses)."""
        from lambda_function import handle_chat

        event = _chat_event({
            'form_mode': True,
            'action': 'submit_form',
            'form_id': 'volunteer_signup',
            'form_data': {'first_name': 'John', 'email': 'john@example.com'},
            'session_id': 'sess_w',
        })
        sentinel = {'statusCode': 200, 'body': json.dumps({'success': True})}

        with patch('lambda_function.handle_form_submission', return_value=sentinel) as mock_form:
            handle_chat(event, 'test_tenant_hash', 'req-w')

        called_event = mock_form.call_args[0][0]
        translated = json.loads(called_event['body'])
        self.assertEqual(translated['form_type'], 'volunteer_signup')
        self.assertEqual(translated['responses'], {'first_name': 'John', 'email': 'john@example.com'})

    def test_form_mode_preserves_form_handler_shape(self):
        """Body already in FormHandler shape — no translation, no overwrite."""
        from lambda_function import handle_chat

        event = _chat_event({
            'form_mode': True,
            'form_type': 'contact',
            'responses': {'name': 'X'},
            'session_id': 'sess_f',
        })
        sentinel = {'statusCode': 200, 'body': json.dumps({'success': True})}

        with patch('lambda_function.handle_form_submission', return_value=sentinel) as mock_form:
            handle_chat(event, 'test_tenant_hash', 'req-f')

        called_event = mock_form.call_args[0][0]
        body_seen = json.loads(called_event['body'])
        self.assertEqual(body_seen['form_type'], 'contact')
        self.assertEqual(body_seen['responses'], {'name': 'X'})

    def test_form_mode_explicit_form_type_wins_over_form_id(self):
        """If body has BOTH form_id AND form_type, form_type wins (no overwrite)."""
        from lambda_function import handle_chat

        event = _chat_event({
            'form_mode': True,
            'form_id': 'should_not_win',
            'form_type': 'explicit_type',
            'form_data': {'a': 1},
            'session_id': 'sess_m',
        })
        sentinel = {'statusCode': 200, 'body': json.dumps({'success': True})}

        with patch('lambda_function.handle_form_submission', return_value=sentinel) as mock_form:
            handle_chat(event, 'test_tenant_hash', 'req-m')

        called_event = mock_form.call_args[0][0]
        body_seen = json.loads(called_event['body'])
        self.assertEqual(body_seen['form_type'], 'explicit_type')

    def test_form_mode_false_does_not_delegate(self):
        from lambda_function import handle_chat

        event = _chat_event({'form_mode': False, 'user_input': 'hello'})

        with patch('lambda_function.handle_form_submission') as mock_form, \
             patch('intent_router.route_intent', return_value={'statusCode': 200, 'body': '{}'}):
            handle_chat(event, 'test_tenant_hash', 'req-2')

        mock_form.assert_not_called()

    def test_form_mode_missing_does_not_delegate(self):
        from lambda_function import handle_chat

        event = _chat_event({'user_input': 'hello'})

        with patch('lambda_function.handle_form_submission') as mock_form, \
             patch('intent_router.route_intent', return_value={'statusCode': 200, 'body': '{}'}):
            handle_chat(event, 'test_tenant_hash', 'req-3')

        mock_form.assert_not_called()

    def test_form_mode_string_true_does_not_delegate(self):
        """Strict identity check: body{form_mode: "true"} (string) must NOT delegate."""
        from lambda_function import handle_chat

        event = _chat_event({'form_mode': 'true', 'user_input': 'hello'})

        with patch('lambda_function.handle_form_submission') as mock_form, \
             patch('intent_router.route_intent', return_value={'statusCode': 200, 'body': '{}'}):
            handle_chat(event, 'test_tenant_hash', 'req-4')

        mock_form.assert_not_called()


if __name__ == '__main__':
    unittest.main()
