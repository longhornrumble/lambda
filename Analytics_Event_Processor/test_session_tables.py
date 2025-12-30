"""
Unit tests for DynamoDB Session Tables functionality.

Tests the write_session_event(), update_session_summary(), and write_events_to_dynamodb()
functions added for User Journey Analytics Phase 1.
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
    update_session_summary,
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


class TestUpdateSessionSummary:
    """Tests for update_session_summary() function."""

    @patch('lambda_function.dynamodb')
    def test_update_session_summary_basic(self, mock_dynamodb):
        """Should update session summary with basic event."""
        mock_dynamodb.update_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'tenant_id': 'FOS123',
            'event_type': 'WIDGET_OPENED',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }

        result = update_session_summary(event)

        assert result is True
        mock_dynamodb.update_item.assert_called_once()

        # Verify key structure
        call_args = mock_dynamodb.update_item.call_args
        key = call_args[1]['Key']
        assert key['pk']['S'] == 'TENANT#fo85e6a06dcdf4'
        assert 'SESSION#' in key['sk']['S']
        assert 'sess_123' in key['sk']['S']

    @patch('lambda_function.dynamodb')
    def test_update_session_summary_message_sent(self, mock_dynamodb):
        """MESSAGE_SENT should increment user_message_count and message_count."""
        mock_dynamodb.update_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'MESSAGE_SENT',
            'client_timestamp': '2025-12-26T10:00:00Z',
            'event_payload': {'content_preview': 'What are your volunteer opportunities?'}
        }

        result = update_session_summary(event)

        assert result is True
        call_args = mock_dynamodb.update_item.call_args
        update_expr = call_args[1]['UpdateExpression']
        assert 'user_message_count' in update_expr
        assert 'message_count' in update_expr
        assert 'first_question' in update_expr

    @patch('lambda_function.dynamodb')
    def test_update_session_summary_message_received(self, mock_dynamodb):
        """MESSAGE_RECEIVED should increment bot_message_count and message_count."""
        mock_dynamodb.update_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'MESSAGE_RECEIVED',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }

        result = update_session_summary(event)

        assert result is True
        call_args = mock_dynamodb.update_item.call_args
        update_expr = call_args[1]['UpdateExpression']
        assert 'bot_message_count' in update_expr
        assert 'message_count' in update_expr

    @patch('lambda_function.dynamodb')
    def test_update_session_summary_form_completed(self, mock_dynamodb):
        """FORM_COMPLETED should set outcome to form_completed."""
        mock_dynamodb.update_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'FORM_COMPLETED',
            'client_timestamp': '2025-12-26T10:00:00Z',
            'event_payload': {'form_id': 'volunteer_signup'}
        }

        result = update_session_summary(event)

        assert result is True
        call_args = mock_dynamodb.update_item.call_args
        expr_values = call_args[1]['ExpressionAttributeValues']
        assert ':outcome' in expr_values
        assert expr_values[':outcome']['S'] == 'form_completed'
        assert ':form_id' in expr_values

    @patch('lambda_function.dynamodb')
    def test_update_session_summary_link_clicked(self, mock_dynamodb):
        """LINK_CLICKED should set outcome only if not already set."""
        mock_dynamodb.update_item.return_value = {}

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'LINK_CLICKED',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }

        result = update_session_summary(event)

        assert result is True
        call_args = mock_dynamodb.update_item.call_args
        update_expr = call_args[1]['UpdateExpression']
        # Should use if_not_exists for weaker outcome
        assert 'if_not_exists' in update_expr

    @patch('lambda_function.dynamodb')
    def test_update_session_summary_dynamodb_error(self, mock_dynamodb):
        """Should return False on DynamoDB error."""
        mock_dynamodb.update_item.side_effect = ClientError(
            {'Error': {'Code': 'InternalError', 'Message': 'Test error'}},
            'UpdateItem'
        )

        event = {
            'session_id': 'sess_123',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'WIDGET_OPENED',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }

        result = update_session_summary(event)

        assert result is False


class TestWriteEventsToDynamoDB:
    """Tests for write_events_to_dynamodb() orchestration function."""

    @patch('lambda_function.update_session_summary')
    @patch('lambda_function.write_session_event')
    def test_write_events_to_dynamodb_multiple_events(self, mock_write, mock_update):
        """Should write all events and update all summaries."""
        mock_write.return_value = True
        mock_update.return_value = True

        events = [
            {'session_id': 'sess_1', 'tenant_hash': 'th1', 'event_type': 'E1'},
            {'session_id': 'sess_2', 'tenant_hash': 'th2', 'event_type': 'E2'},
            {'session_id': 'sess_3', 'tenant_hash': 'th3', 'event_type': 'E3'}
        ]

        write_events_to_dynamodb(events)

        assert mock_write.call_count == 3
        assert mock_update.call_count == 3

    @patch('lambda_function.update_session_summary')
    @patch('lambda_function.write_session_event')
    def test_write_events_to_dynamodb_empty_list(self, mock_write, mock_update):
        """Should handle empty event list gracefully."""
        write_events_to_dynamodb([])

        mock_write.assert_not_called()
        mock_update.assert_not_called()

    @patch('lambda_function.update_session_summary')
    @patch('lambda_function.write_session_event')
    def test_write_events_partial_failure(self, mock_write, mock_update):
        """Should continue processing even if some writes fail."""
        # First write fails, second and third succeed
        mock_write.side_effect = [False, True, True]
        mock_update.return_value = True

        events = [
            {'session_id': 'sess_1', 'tenant_hash': 'th1', 'event_type': 'E1'},
            {'session_id': 'sess_2', 'tenant_hash': 'th2', 'event_type': 'E2'},
            {'session_id': 'sess_3', 'tenant_hash': 'th3', 'event_type': 'E3'}
        ]

        # Should not raise
        write_events_to_dynamodb(events)

        # All events should be attempted
        assert mock_write.call_count == 3
        assert mock_update.call_count == 3


class TestConcurrentWrites:
    """Tests for atomic update behavior under concurrent writes."""

    @patch('lambda_function.dynamodb')
    def test_concurrent_message_sent_uses_atomic_increment(self, mock_dynamodb):
        """
        Multiple MESSAGE_SENT events for same session should use atomic ADD.
        This verifies the UpdateExpression uses ADD for counters, not SET.
        """
        mock_dynamodb.update_item.return_value = {}

        event = {
            'session_id': 'sess_concurrent',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'MESSAGE_SENT',
            'client_timestamp': '2025-12-26T10:00:00Z',
            'event_payload': {'content_preview': 'Test message'}
        }

        update_session_summary(event)

        call_args = mock_dynamodb.update_item.call_args
        update_expr = call_args[1]['UpdateExpression']

        # Verify atomic increment pattern (not SET x = x + 1)
        assert 'if_not_exists(user_message_count' in update_expr
        assert 'if_not_exists(message_count' in update_expr
        # The pattern is: SET field = if_not_exists(field, :zero) + :one
        # This is atomic because DynamoDB executes SET atomically

    @patch('lambda_function.dynamodb')
    def test_form_completed_overwrites_link_clicked(self, mock_dynamodb):
        """
        FORM_COMPLETED should always set outcome (strong outcome).
        LINK_CLICKED should only set if outcome not already set (weak outcome).
        """
        mock_dynamodb.update_item.return_value = {}

        # Form completed - strong outcome, uses SET directly
        form_event = {
            'session_id': 'sess_outcome',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'FORM_COMPLETED',
            'client_timestamp': '2025-12-26T10:00:00Z',
            'event_payload': {'form_id': 'test_form'}
        }
        update_session_summary(form_event)
        form_call = mock_dynamodb.update_item.call_args
        form_update = form_call[1]['UpdateExpression']

        # Link clicked - weak outcome, uses if_not_exists
        mock_dynamodb.reset_mock()
        link_event = {
            'session_id': 'sess_outcome',
            'tenant_hash': 'fo85e6a06dcdf4',
            'event_type': 'LINK_CLICKED',
            'client_timestamp': '2025-12-26T10:00:00Z'
        }
        update_session_summary(link_event)
        link_call = mock_dynamodb.update_item.call_args
        link_update = link_call[1]['UpdateExpression']

        # Form should use direct SET for outcome
        assert '#outcome = :outcome' in form_update
        # Link should use if_not_exists for outcome
        assert 'if_not_exists(#outcome, :outcome)' in link_update


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
