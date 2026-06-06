"""Pytest tests for the §E5 Chain-1 `text_en` write slot in conversation_handler.

WS-E-TEXTEN / FROZEN_CONTRACTS §E5: every recent-messages turn write gains an
additive `text_en` sibling; v1 `text_en = content` (verbatim copy). Readers
tolerate absence (schema discipline). This test pins `text_en === content` on
both the user and assistant rows.
"""
import sys
from pathlib import Path
from unittest.mock import patch

# Ensure module resolution works when pytest runs from project root
sys.path.insert(0, str(Path(__file__).parent))

import conversation_handler  # noqa: E402


def _capture_message_items(delta):
    """Run the message-write path and return the captured put_item Items."""
    captured = []

    def _fake_op(op_name, **kwargs):
        captured.append(kwargs)
        return {}

    # AWS client manager is the active path (AWS_CLIENT_MANAGER_AVAILABLE=True);
    # patch it so no real DynamoDB call is made and we can inspect the Item.
    with patch.object(conversation_handler, 'protected_dynamodb_operation', _fake_op):
        conversation_handler._save_conversation_to_db(
            session_id='sess-text-en-001',
            tenant_id='TEN-TEXTEN',
            delta=delta,
            expected_turn=0,
        )
    # Only message writes — no summary keys in delta — so every captured call is a row.
    return [c['Item'] for c in captured]


def test_text_en_equals_content_on_user_and_assistant_rows():
    items = _capture_message_items({
        'appendUser': {'text': 'Hola, necesito ayuda'},
        'appendAssistant': {'text': 'Sure — here is how to get started.'},
    })

    assert len(items) == 2, "expected one row per appended turn"
    for item in items:
        assert 'text_en' in item, "text_en slot must be present on every turn write"
        assert item['text_en'] == item['content'], "v1: text_en is a verbatim copy of content"

    # Spot-check the verbatim values survive untouched.
    assert items[0]['content'] == {'S': 'Hola, necesito ayuda'}
    assert items[0]['text_en'] == {'S': 'Hola, necesito ayuda'}
    assert items[1]['text_en'] == {'S': 'Sure — here is how to get started.'}


def test_text_en_present_for_empty_text():
    # Empty text string → content is '' and text_en mirrors it (still present).
    items = _capture_message_items({'appendUser': {'text': ''}})
    assert len(items) == 1
    assert items[0]['content'] == {'S': ''}
    assert items[0]['text_en'] == {'S': ''}
