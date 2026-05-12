#!/usr/bin/env python3
"""handle_chat metadata-key parity with BSH.

Widget (both HTTPChatProvider and StreamingChatProvider) sends action-chip /
CTA routing metadata under `body.routing_metadata`. MFS reads it for 3-tier
explicit routing — used to read `body.metadata`, now reads `routing_metadata`
with a `metadata` fallback for backward compat with synthetic callers.

Tests verify the read-key precedence and fallback behavior.
"""

import json
import unittest
from unittest.mock import MagicMock, patch


def _chat_event(body_dict):
    return {
        'httpMethod': 'POST',
        'queryStringParameters': {'action': 'chat', 't': 'test_tenant_hash'},
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body_dict),
    }


class TestHandleChatRoutingMetadata(unittest.TestCase):

    def _call_handle_chat_capture_routing(self, body_dict):
        """Call handle_chat and capture the routing metadata it built.

        Mocks intent_router so handle_chat reaches the routing-extraction
        block without exercising the rest of the chat path. The returned
        request_metadata is captured via a patched get_conversation_branch.
        """
        from lambda_function import handle_chat

        captured = {}

        def capture_branch(metadata, config):
            captured['metadata'] = dict(metadata) if metadata else {}
            return None  # branch unmatched → falls through, doesn't matter for this test

        event = _chat_event(body_dict)

        with patch('intent_router.route_intent', return_value={'statusCode': 200, 'body': '{}'}), \
             patch('lambda_function.get_conversation_branch', side_effect=capture_branch), \
             patch('lambda_function.get_jwt_signing_key', return_value='unused'), \
             patch('tenant_config_loader.get_config_for_tenant_by_hash', return_value={'tenant_id': 't'}):
            handle_chat(event, 'test_tenant_hash', 'req-rm')

        return captured.get('metadata', {})

    def test_reads_routing_metadata_when_present(self):
        """BSH-parity wire shape: widget sends routing_metadata."""
        captured = self._call_handle_chat_capture_routing({
            'user_input': 'hello',
            'routing_metadata': {
                'action_chip_triggered': True,
                'target_branch': 'branch_contact',
                'action_chip_id': 'contact_us',
            },
        })
        self.assertTrue(captured.get('action_chip_triggered'))
        self.assertEqual(captured.get('target_branch'), 'branch_contact')
        self.assertEqual(captured.get('action_chip_id'), 'contact_us')

    def test_falls_back_to_metadata_when_routing_metadata_missing(self):
        """Backward-compat fallback for callers still sending body.metadata."""
        captured = self._call_handle_chat_capture_routing({
            'user_input': 'hello',
            'metadata': {
                'action_chip_triggered': True,
                'target_branch': 'branch_compat',
            },
        })
        self.assertTrue(captured.get('action_chip_triggered'))
        self.assertEqual(captured.get('target_branch'), 'branch_compat')

    def test_routing_metadata_takes_precedence_over_metadata(self):
        """If both keys present, routing_metadata wins (matches BSH order)."""
        captured = self._call_handle_chat_capture_routing({
            'user_input': 'hello',
            'routing_metadata': {'target_branch': 'from_routing'},
            'metadata': {'target_branch': 'from_legacy'},
        })
        self.assertEqual(captured.get('target_branch'), 'from_routing')

    def test_empty_when_neither_key_present(self):
        """Free-form query with no metadata → empty dict, no branch fields."""
        captured = self._call_handle_chat_capture_routing({
            'user_input': 'hello',
        })
        self.assertEqual(captured, {})


if __name__ == '__main__':
    unittest.main()
