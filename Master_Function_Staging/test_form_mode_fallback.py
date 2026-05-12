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

        mock_form.assert_called_once_with(event, 'test_tenant_hash', 'req-1')
        self.assertEqual(response, sentinel)

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
