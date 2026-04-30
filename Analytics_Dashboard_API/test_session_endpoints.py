"""
Unit tests for Session Detail endpoints (User Journey Analytics).

Tests the handle_session_detail() and handle_sessions_list() functions
that query the DynamoDB session tables.
"""

import pytest
import json
import base64
import time
from unittest.mock import patch, MagicMock
from datetime import datetime

import lambda_function
# Import functions under test
from lambda_function import (
    get_tenant_hash,
    handle_session_detail,
    handle_sessions_list,
    cors_response
)


def _mock_s3_mappings(mock_s3, mappings):
    """Wire mock_s3 to serve the given {tenant_id: tenant_hash} mappings via the
    list_objects_v2 paginator + get_object pattern that get_tenant_hash uses."""
    contents = [{'Key': f'mappings/{h}.json'} for h in mappings.values()]
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{'Contents': contents}]
    mock_s3.get_paginator.return_value = mock_paginator

    def fake_get_object(Bucket, Key):
        candidate_hash = Key.split('/')[-1].replace('.json', '')
        tenant_id = next(tid for tid, h in mappings.items() if h == candidate_hash)
        body = MagicMock()
        body.read.return_value = json.dumps({
            'tenant_id': tenant_id,
            'tenant_hash': candidate_hash
        }).encode('utf-8')
        return {'Body': body}

    mock_s3.get_object.side_effect = fake_get_object


@pytest.fixture(autouse=True)
def _reset_tenant_hash_cache():
    """Cache leaks across tests will mask mock setup — reset before every test."""
    lambda_function._tenant_hash_cache = {}
    lambda_function._tenant_hash_cache_time = 0
    yield


class TestGetTenantHash:
    """Tests for tenant hash generation."""

    @patch('lambda_function.s3')
    def test_tenant_hash_format(self, mock_s3):
        """Hash should be the 14-char value resolved from the S3 mapping (2-char prefix + 12-char hash)."""
        _mock_s3_mappings(mock_s3, {'FOS123': 'fo70c7c68b4dd6'})
        result = get_tenant_hash('FOS123')
        assert len(result) == 14  # 2 char prefix + 12 char hash
        assert result.startswith('fo')  # lowercase prefix

    @patch('lambda_function.s3')
    def test_tenant_hash_consistency(self, mock_s3):
        """Same input should produce same hash (cache hit on second call)."""
        _mock_s3_mappings(mock_s3, {'TEST_TENANT': 'te1234567890ab'})
        hash1 = get_tenant_hash('TEST_TENANT')
        hash2 = get_tenant_hash('TEST_TENANT')
        assert hash1 == hash2

    @patch('lambda_function.s3')
    def test_tenant_hash_different_tenants(self, mock_s3):
        """Different tenants should have different hashes."""
        _mock_s3_mappings(mock_s3, {
            'TENANT_A': 'te0000000000aa',
            'TENANT_B': 'te0000000000bb',
        })
        hash1 = get_tenant_hash('TENANT_A')
        hash2 = get_tenant_hash('TENANT_B')
        assert hash1 != hash2


class TestHandleSessionDetail:
    """Tests for handle_session_detail() function."""

    @patch('lambda_function.get_tenant_hash', return_value='fo70c7c68b4dd6')
    @patch('lambda_function.dynamodb')
    def test_session_detail_success(self, mock_dynamodb, mock_get_tenant_hash):
        """Should return session with all events."""
        mock_dynamodb.query.return_value = {
            'Items': [
                {
                    'pk': {'S': 'SESSION#sess_123'},
                    'sk': {'S': 'STEP#001'},
                    'tenant_hash': {'S': 'fo70c7c68b4dd6'},
                    'event_type': {'S': 'WIDGET_OPENED'},
                    'timestamp': {'S': '2025-12-26T10:00:00Z'},
                    'step_number': {'N': '1'}
                },
                {
                    'pk': {'S': 'SESSION#sess_123'},
                    'sk': {'S': 'STEP#002'},
                    'tenant_hash': {'S': 'fo70c7c68b4dd6'},
                    'event_type': {'S': 'MESSAGE_SENT'},
                    'timestamp': {'S': '2025-12-26T10:01:00Z'},
                    'step_number': {'N': '2'},
                    'event_payload': {'S': '{"content_preview": "Hello"}'}
                },
                {
                    'pk': {'S': 'SESSION#sess_123'},
                    'sk': {'S': 'STEP#003'},
                    'tenant_hash': {'S': 'fo70c7c68b4dd6'},
                    'event_type': {'S': 'MESSAGE_RECEIVED'},
                    'timestamp': {'S': '2025-12-26T10:01:05Z'},
                    'step_number': {'N': '3'}
                }
            ]
        }

        result = handle_session_detail('FOS123', 'sess_123', {})
        body = json.loads(result['body'])

        assert result['statusCode'] == 200
        assert body['session_id'] == 'sess_123'
        assert len(body['events']) == 3
        assert body['summary']['message_count'] == 2
        assert body['summary']['user_message_count'] == 1
        assert body['summary']['bot_message_count'] == 1
        assert body['summary']['first_question'] == 'Hello'

    @patch('lambda_function.dynamodb')
    def test_session_detail_not_found(self, mock_dynamodb):
        """Should return 404 if session not found."""
        mock_dynamodb.query.return_value = {'Items': []}

        result = handle_session_detail('FOS123', 'nonexistent', {})
        body = json.loads(result['body'])

        assert result['statusCode'] == 404
        assert 'not found' in body['error'].lower()

    @patch('lambda_function.dynamodb')
    def test_session_detail_tenant_mismatch(self, mock_dynamodb):
        """Should return 403 if tenant doesn't match."""
        mock_dynamodb.query.return_value = {
            'Items': [{
                'pk': {'S': 'SESSION#sess_123'},
                'sk': {'S': 'STEP#001'},
                'tenant_hash': {'S': 'different_tenant'},
                'event_type': {'S': 'WIDGET_OPENED'},
                'timestamp': {'S': '2025-12-26T10:00:00Z'},
                'step_number': {'N': '1'}
            }]
        }

        result = handle_session_detail('FOS123', 'sess_123', {})
        body = json.loads(result['body'])

        assert result['statusCode'] == 403
        assert 'access denied' in body['error'].lower()

    def test_session_detail_invalid_session_id(self):
        """Should return 400 for invalid session_id format."""
        result = handle_session_detail('FOS123', 'sess<script>alert(1)</script>', {})
        body = json.loads(result['body'])

        assert result['statusCode'] == 400
        assert 'invalid' in body['error'].lower()

    def test_session_detail_empty_session_id(self):
        """Should return 400 for empty session_id."""
        result = handle_session_detail('FOS123', '', {})
        body = json.loads(result['body'])

        assert result['statusCode'] == 400
        assert 'required' in body['error'].lower()

    @patch('lambda_function.get_tenant_hash', return_value='fo70c7c68b4dd6')
    @patch('lambda_function.dynamodb')
    def test_session_detail_outcome_tracking(self, mock_dynamodb, mock_get_tenant_hash):
        """Should correctly determine outcome from events."""
        mock_dynamodb.query.return_value = {
            'Items': [
                {
                    'pk': {'S': 'SESSION#sess_123'},
                    'sk': {'S': 'STEP#001'},
                    'tenant_hash': {'S': 'fo70c7c68b4dd6'},
                    'event_type': {'S': 'LINK_CLICKED'},
                    'timestamp': {'S': '2025-12-26T10:00:00Z'},
                    'step_number': {'N': '1'}
                },
                {
                    'pk': {'S': 'SESSION#sess_123'},
                    'sk': {'S': 'STEP#002'},
                    'tenant_hash': {'S': 'fo70c7c68b4dd6'},
                    'event_type': {'S': 'FORM_COMPLETED'},
                    'timestamp': {'S': '2025-12-26T10:05:00Z'},
                    'step_number': {'N': '2'}
                }
            ]
        }

        result = handle_session_detail('FOS123', 'sess_123', {})
        body = json.loads(result['body'])

        # FORM_COMPLETED should override LINK_CLICKED
        assert body['summary']['outcome'] == 'form_completed'


class TestHandleSessionsList:
    """Tests for handle_sessions_list() function."""

    @patch('lambda_function.enrich_sessions_with_events',
           return_value={'sess_123': {'event_count': 5, 'outcome': 'form_completed'}})
    @patch('lambda_function.get_tenant_hash', return_value='fo85e6a06dcdf4')
    @patch('lambda_function.dynamodb')
    def test_sessions_list_success(self, mock_dynamodb, mock_get_tenant_hash, mock_enrich):
        """Should return paginated list of sessions."""
        # SK format is SESSION#{session_id} (the timestamp-prefixed legacy format
        # was retired — see lambda_function.py:3947).
        mock_dynamodb.query.return_value = {
            'Items': [
                {
                    'pk': {'S': 'TENANT#fo85e6a06dcdf4'},
                    'sk': {'S': 'SESSION#sess_123'},
                    'started_at': {'S': '2025-12-26T10:00:00Z'},
                    'ended_at': {'S': '2025-12-26T10:10:00Z'},
                    'outcome': {'S': 'form_completed'},
                    'message_count': {'N': '5'},
                    'user_message_count': {'N': '2'},
                    'bot_message_count': {'N': '3'},
                    'first_question': {'S': 'How can I volunteer?'}
                }
            ]
        }

        result = handle_sessions_list('FOS123', {'range': '30d', 'limit': '25'})
        body = json.loads(result['body'])

        assert result['statusCode'] == 200
        assert len(body['sessions']) == 1
        assert body['sessions'][0]['session_id'] == 'sess_123'
        assert body['sessions'][0]['outcome'] == 'form_completed'
        assert body['pagination']['count'] == 1

    @patch('lambda_function.dynamodb')
    def test_sessions_list_empty(self, mock_dynamodb):
        """Should return empty list when no sessions."""
        mock_dynamodb.query.return_value = {'Items': []}

        result = handle_sessions_list('FOS123', {'range': '7d'})
        body = json.loads(result['body'])

        assert result['statusCode'] == 200
        assert body['sessions'] == []
        assert body['pagination']['count'] == 0

    @patch('lambda_function.get_tenant_hash', return_value='fo85e6a06dcdf4')
    @patch('lambda_function.dynamodb')
    def test_sessions_list_with_outcome_filter(self, mock_dynamodb, mock_get_tenant_hash):
        """Outcome filter is applied post-enrichment in Python (computed outcomes
        from the events table are authoritative), not in DynamoDB. The DynamoDB
        FilterExpression is the date range; the visible signal that outcome
        filtering is active is the inflated fetch_limit (3x requested limit)."""
        mock_dynamodb.query.return_value = {'Items': []}

        handle_sessions_list('FOS123', {'outcome': 'form_completed', 'limit': '25'})

        call_args = mock_dynamodb.query.call_args
        # Date filter is always present and is the *only* DynamoDB-side filter.
        assert 'started_at' in call_args[1]['FilterExpression']
        assert '#outcome' not in call_args[1]['FilterExpression']
        # Outcome filter triggers oversampling so post-enrichment filter has headroom.
        assert call_args[1]['Limit'] == 75  # limit (25) * 3

    def test_sessions_list_invalid_outcome(self):
        """Should return 400 for invalid outcome filter."""
        result = handle_sessions_list('FOS123', {'outcome': 'invalid_outcome'})
        body = json.loads(result['body'])

        assert result['statusCode'] == 400
        assert 'invalid outcome' in body['error'].lower()

    @patch('lambda_function.dynamodb')
    def test_sessions_list_pagination(self, mock_dynamodb):
        """Should return next_cursor when more results available."""
        mock_dynamodb.query.return_value = {
            'Items': [{
                'pk': {'S': 'TENANT#fo85e6a06dcdf4'},
                'sk': {'S': 'SESSION#2025-12-26T10:00:00Z#sess_123'},
                'started_at': {'S': '2025-12-26T10:00:00Z'},
                'message_count': {'N': '5'}
            }],
            'LastEvaluatedKey': {
                'pk': {'S': 'TENANT#fo85e6a06dcdf4'},
                'sk': {'S': 'SESSION#2025-12-26T10:00:00Z#sess_123'}
            }
        }

        result = handle_sessions_list('FOS123', {'limit': '1'})
        body = json.loads(result['body'])

        assert result['statusCode'] == 200
        assert body['pagination']['has_more'] is True
        assert body['pagination']['next_cursor'] is not None

    @patch('lambda_function.dynamodb')
    def test_sessions_list_with_cursor(self, mock_dynamodb):
        """Should use pagination cursor when provided."""
        mock_dynamodb.query.return_value = {'Items': []}

        # Create a valid cursor
        cursor_data = {'pk': {'S': 'TENANT#abc'}, 'sk': {'S': 'SESSION#123'}}
        cursor = base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode()

        handle_sessions_list('FOS123', {'cursor': cursor})

        call_args = mock_dynamodb.query.call_args
        assert 'ExclusiveStartKey' in call_args[1]

    def test_sessions_list_invalid_cursor(self):
        """Should return 400 for invalid cursor."""
        result = handle_sessions_list('FOS123', {'cursor': 'invalid_cursor!!!'})
        body = json.loads(result['body'])

        assert result['statusCode'] == 400
        assert 'cursor' in body['error'].lower()

    @patch('lambda_function.dynamodb')
    def test_sessions_list_limit_bounds(self, mock_dynamodb):
        """Should enforce limit bounds (1-100)."""
        mock_dynamodb.query.return_value = {'Items': []}

        # Test upper bound
        handle_sessions_list('FOS123', {'limit': '999'})
        call_args = mock_dynamodb.query.call_args
        assert call_args[1]['Limit'] == 100

        # Test lower bound
        handle_sessions_list('FOS123', {'limit': '0'})
        call_args = mock_dynamodb.query.call_args
        assert call_args[1]['Limit'] == 1


class TestCorsResponse:
    """Tests for CORS response helper."""

    def test_cors_response_format(self):
        """Should return properly formatted response."""
        result = cors_response(200, {'test': 'data'})

        assert result['statusCode'] == 200
        assert result['headers']['Content-Type'] == 'application/json'
        assert json.loads(result['body']) == {'test': 'data'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
