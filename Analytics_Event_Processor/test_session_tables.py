"""
Unit tests for DynamoDB Session Tables functionality.

Tests the write_session_event() and write_events_to_dynamodb() functions.
update_session_summary() was deleted 2026-05-11 (phase audit B7) — its
UpdateExpression used invalid DynamoDB syntax and the call site was already
removed in PR #57.
"""

import pytest
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Import functions under test
from lambda_function import (
    calculate_ttl,
    write_session_event,
    write_events_to_dynamodb
)


class TestCalculateTTL:
    """Tests for TTL calculation."""

    def test_calculate_ttl_default_90_days(self):
        """TTL should be ~90 days in the future."""
        ttl = calculate_ttl()
        now = datetime.utcnow()
        expected = now + timedelta(days=90)

        # Allow 1 minute tolerance
        assert abs(ttl - int(expected.timestamp())) < 60

    def test_calculate_ttl_custom_days(self):
        """TTL should respect custom days parameter."""
        ttl = calculate_ttl(days=30)
        now = datetime.utcnow()
        expected = now + timedelta(days=30)

        # Allow 1 minute tolerance
        assert abs(ttl - int(expected.timestamp())) < 60


class TestWriteSessionEvent:
    """Tests for write_session_event() function."""

    @patch('lambda_function.dynamodb')
    def test_write_session_event_success(self, mock_dynamodb):
        """Should successfully write event to DynamoDB."""
        mock_dynamodb.put_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'tenant_id': 'FOS123',
            'step_number': 1,
            'event_type': 'WIDGET_OPENED',
            'client_timestamp': '2025-12-26T10:00:00Z',
            'event_payload': {'trigger': 'button'}
        }

        result = write_session_event(event)

        assert result is True
        mock_dynamodb.put_item.assert_called_once()

        # Verify key structure
        call_args = mock_dynamodb.put_item.call_args
        item = call_args[1]['Item']
        assert item['pk']['S'] == 'SESSION#sess_123'
        assert item['sk']['S'] == 'STEP#001'  # Zero-padded
        assert item['event_type']['S'] == 'WIDGET_OPENED'
        assert 'ttl' in item

    @patch('lambda_function.dynamodb')
    def test_write_session_event_step_padding(self, mock_dynamodb):
        """Step number should be zero-padded to 3 digits."""
        mock_dynamodb.put_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'step_number': 42,
            'event_type': 'MESSAGE_SENT',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }

        write_session_event(event)

        call_args = mock_dynamodb.put_item.call_args
        item = call_args[1]['Item']
        assert item['sk']['S'] == 'STEP#042'

    @patch('lambda_function.dynamodb')
    def test_write_session_event_missing_session_id(self, mock_dynamodb):
        """Should return False if session_id is missing."""
        event = {
            'tenant_hash': 'fo85e6a06dcdf4',
            'step_number': 1,
            'event_type': 'WIDGET_OPENED'
        }

        result = write_session_event(event)

        assert result is False
        mock_dynamodb.put_item.assert_not_called()

    @patch('lambda_function.dynamodb')
    def test_write_session_event_missing_tenant_hash(self, mock_dynamodb):
        """Should return False if tenant_hash is missing."""
        event = {
            'session_id': 'sess_123',
            'step_number': 1,
            'event_type': 'WIDGET_OPENED'
        }

        result = write_session_event(event)

        assert result is False
        mock_dynamodb.put_item.assert_not_called()

    @patch('lambda_function.dynamodb')
    def test_write_session_event_dynamodb_error(self, mock_dynamodb):
        """Should return False and log error on DynamoDB failure."""
        mock_dynamodb.put_item.side_effect = ClientError(
            {'Error': {'Code': 'InternalError', 'Message': 'Test error'}},
            'PutItem'
        )

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'step_number': 1,
            'event_type': 'WIDGET_OPENED',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }

        result = write_session_event(event)

        assert result is False


class TestWriteEventsToDynamoDB:
    """Tests for write_events_to_dynamodb() orchestration function."""

    @patch('lambda_function.write_session_event')
    def test_write_events_to_dynamodb_multiple_events(self, mock_write):
        """Should write all events to picasso-session-events. Session summaries are
        written by chat-path Lambdas, not by this orchestrator (update_session_summary
        deleted 2026-05-11 phase audit B7)."""
        mock_write.return_value = True

        events = [
            {'session_id': 'sess_1', 'tenant_hash': 'th1', 'event_type': 'E1'},
            {'session_id': 'sess_2', 'tenant_hash': 'th2', 'event_type': 'E2'},
            {'session_id': 'sess_3', 'tenant_hash': 'th3', 'event_type': 'E3'}
        ]

        write_events_to_dynamodb(events)

        assert mock_write.call_count == 3

    @patch('lambda_function.write_session_event')
    def test_write_events_to_dynamodb_empty_list(self, mock_write):
        """Should handle empty event list gracefully."""
        write_events_to_dynamodb([])

        mock_write.assert_not_called()

    @patch('lambda_function.write_session_event')
    def test_write_events_partial_failure(self, mock_write):
        """Should continue processing even if some writes fail."""
        mock_write.side_effect = [False, True, True]

        events = [
            {'session_id': 'sess_1', 'tenant_hash': 'th1', 'event_type': 'E1'},
            {'session_id': 'sess_2', 'tenant_hash': 'th2', 'event_type': 'E2'},
            {'session_id': 'sess_3', 'tenant_hash': 'th3', 'event_type': 'E3'}
        ]

        # Should not raise
        write_events_to_dynamodb(events)

        # All events should be attempted
        assert mock_write.call_count == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
