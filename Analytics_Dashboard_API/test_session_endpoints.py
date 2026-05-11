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
    handle_recent_conversations,
    cors_response,
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


class TestRecentConversationsResponseTime:
    """Issue #5 PR C — verify response_time_seconds is computed from
    total_response_time_ms / response_count, not the always-zero
    avg_response_time_ms field that doesn't exist on session rows."""

    @patch('lambda_function.get_tenant_hash', return_value='hash_abc123')
    @patch('lambda_function.fetch_session_summaries')
    def test_response_time_seconds_averaged_correctly(self, mock_fetch, _hash):
        mock_fetch.return_value = [{
            'session_id': 'sess_1',
            'started_at': '2026-05-04T12:00:00Z',
            'first_question': 'How do I apply?',
            'message_count': 4,
            'outcome': 'conversation',
            'total_response_time_ms': 3000,
            'response_count': 2,  # avg = 1500ms = 1.5s
        }]
        result = handle_recent_conversations('TEST123', {})
        body = json.loads(result['body'])
        conv = body['conversations'][0]
        assert conv['response_time_seconds'] == 1.5

    @patch('lambda_function.get_tenant_hash', return_value='hash_abc123')
    @patch('lambda_function.fetch_session_summaries')
    def test_response_time_seconds_zero_when_no_responses(self, mock_fetch, _hash):
        """response_count=0 must NOT divide by zero — should return 0."""
        mock_fetch.return_value = [{
            'session_id': 'sess_1',
            'started_at': '2026-05-04T12:00:00Z',
            'first_question': 'Hi',
            'message_count': 1,
            'outcome': None,
            'total_response_time_ms': 0,
            'response_count': 0,
        }]
        result = handle_recent_conversations('TEST123', {})
        body = json.loads(result['body'])
        assert body['conversations'][0]['response_time_seconds'] == 0

    @patch('lambda_function.get_tenant_hash', return_value='hash_abc123')
    @patch('lambda_function.fetch_session_summaries')
    def test_forward_compat_old_shape_row(self, mock_fetch, _hash):
        """Per CLAUDE.md schema discipline: old session rows missing the
        new total_response_time_ms / response_count fields must not crash
        the reader. Defaults to 0."""
        mock_fetch.return_value = [{
            'session_id': 'sess_legacy',
            'started_at': '2026-05-04T12:00:00Z',
            'first_question': 'Old session',
            'message_count': 2,
            'outcome': 'conversation',
            # NO total_response_time_ms / response_count keys
        }]
        result = handle_recent_conversations('TEST123', {})
        body = json.loads(result['body'])
        assert body['conversations'][0]['response_time_seconds'] == 0


# ---------------------------------------------------------------------------
# Phase 2.8 — Tier-3 S3 archive read path
# ---------------------------------------------------------------------------

class TestS3ArchiveReadPath:
    """Tests for _fetch_archived_sessions + the Tier-3 merge in
    fetch_session_summaries / handle_sessions_list."""

    def _wire_archive_objects(self, mock_s3, objects):
        """Set up mock_s3 to serve list_objects_v2 + get_object for archive items.

        `objects` is a list of (key, body_dict) tuples.
        """
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': k} for k, _ in objects]}
        ]
        mock_s3.get_paginator.return_value = mock_paginator

        body_by_key = {k: b for k, b in objects}

        def fake_get_object(Bucket, Key):
            body = MagicMock()
            body.read.return_value = json.dumps(body_by_key[Key]).encode('utf-8')
            return {'Body': body}

        mock_s3.get_object.side_effect = fake_get_object

    def _archived_item(self, session_id, started_at, tenant_hash='hash_abc123', tenant_id='TEST123', **extra):
        item = {
            'pk': f'TENANT#{tenant_hash}',
            'sk': f'SESSION#{session_id}',
            'session_id': session_id,
            'tenant_id': tenant_id,
            'started_at': started_at,
            'ended_at': started_at,
            'message_count': 2,
            'user_message_count': 1,
            'bot_message_count': 1,
            'first_question': 'archived test',
        }
        item.update(extra)
        return item

    @patch('lambda_function.s3')
    def test_archive_helper_filters_by_tenant_pk(self, mock_s3):
        """_fetch_archived_sessions should return only sessions whose pk matches the tenant hash."""
        self._wire_archive_objects(mock_s3, [
            ('sessions/year=2024/month=06/day=01/match.json',
             self._archived_item('match_sid', '2024-06-01T12:00:00Z', tenant_hash='hash_abc123')),
            ('sessions/year=2024/month=06/day=01/other_tenant.json',
             self._archived_item('other_sid', '2024-06-01T12:00:00Z', tenant_hash='other_hash')),
        ])
        result = lambda_function._fetch_archived_sessions(
            'hash_abc123',
            {'start_date_iso': '2024-05-01', 'end_date_iso': '2024-06-30'},
        )
        assert len(result) == 1
        assert result[0]['session_id'] == 'match_sid'

    @patch('lambda_function.s3')
    def test_archive_helper_filters_by_started_at_range(self, mock_s3):
        """Archive scan must filter by started_at, not by S3 key date (key is archive-write date)."""
        self._wire_archive_objects(mock_s3, [
            # Both keys are written on the SAME archive date but the session
            # started_at values straddle the range boundary.
            ('sessions/year=2026/month=05/day=11/in_range.json',
             self._archived_item('in_range', '2024-06-15T10:00:00Z')),
            ('sessions/year=2026/month=05/day=11/out_of_range.json',
             self._archived_item('out_of_range', '2023-01-01T10:00:00Z')),
        ])
        result = lambda_function._fetch_archived_sessions(
            'hash_abc123',
            {'start_date_iso': '2024-06-01', 'end_date_iso': '2024-06-30'},
        )
        assert {s['session_id'] for s in result} == {'in_range'}

    @patch('lambda_function.s3')
    def test_archive_helper_returns_correct_shape(self, mock_s3):
        """Returned dicts must match fetch_session_summaries shape so callers can merge."""
        self._wire_archive_objects(mock_s3, [
            ('sessions/year=2024/month=06/day=01/sess.json',
             self._archived_item(
                 'sess_shape', '2024-06-01T10:00:00Z',
                 ended_at='2024-06-01T10:05:00Z',
                 outcome='form_completed', form_id='volunteer_form',
                 total_response_time_ms=1500, response_count=3,
             )),
        ])
        result = lambda_function._fetch_archived_sessions(
            'hash_abc123',
            {'start_date_iso': '2024-05-01', 'end_date_iso': '2024-06-30'},
        )
        assert len(result) == 1
        s = result[0]
        assert s['session_id'] == 'sess_shape'
        assert s['outcome'] == 'form_completed'
        assert s['form_id'] == 'volunteer_form'
        assert s['duration_seconds'] == 300
        assert s['total_response_time_ms'] == 1500
        assert s['response_count'] == 3

    @patch('lambda_function.s3')
    def test_archive_helper_normalizes_legacy_browsing_outcome(self, mock_s3):
        """Per existing DDB reader: 'browsing' outcome maps to 'conversation'."""
        self._wire_archive_objects(mock_s3, [
            ('sessions/year=2024/month=06/day=01/sess.json',
             self._archived_item('sess_legacy_outcome', '2024-06-01T10:00:00Z', outcome='browsing')),
        ])
        result = lambda_function._fetch_archived_sessions(
            'hash_abc123',
            {'start_date_iso': '2024-05-01', 'end_date_iso': '2024-06-30'},
        )
        assert result[0]['outcome'] == 'conversation'

    @patch('lambda_function.s3')
    def test_archive_helper_skips_malformed_json(self, mock_s3):
        """Malformed JSON in S3 should not break the scan — log + skip."""
        good_item = self._archived_item('good', '2024-06-01T10:00:00Z')
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{
            'Contents': [
                {'Key': 'sessions/year=2024/month=06/day=01/bad.json'},
                {'Key': 'sessions/year=2024/month=06/day=01/good.json'},
            ]
        }]
        mock_s3.get_paginator.return_value = mock_paginator

        def fake_get_object(Bucket, Key):
            body = MagicMock()
            if 'bad' in Key:
                body.read.return_value = b'{this is not json'
            else:
                body.read.return_value = json.dumps(good_item).encode('utf-8')
            return {'Body': body}

        mock_s3.get_object.side_effect = fake_get_object
        result = lambda_function._fetch_archived_sessions(
            'hash_abc123',
            {'start_date_iso': '2024-05-01', 'end_date_iso': '2024-06-30'},
        )
        assert [s['session_id'] for s in result] == ['good']

    @patch('lambda_function.s3')
    def test_archive_helper_returns_empty_on_list_failure(self, mock_s3):
        """An S3 ClientError on list_objects should degrade gracefully to [],
        not crash the calling endpoint."""
        from botocore.exceptions import ClientError
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}}, 'ListObjectsV2'
        )
        mock_s3.get_paginator.return_value = mock_paginator

        result = lambda_function._fetch_archived_sessions(
            'hash_abc123',
            {'start_date_iso': '2020-01-01', 'end_date_iso': '2020-12-31'},
        )
        assert result == []

    def test_date_range_cutoff_triggers_archive(self):
        """_date_range_extends_past_ttl returns True iff start_date is older
        than ARCHIVE_TTL_DAYS (90) days ago."""
        from datetime import timedelta as td
        from datetime import datetime as dt
        from datetime import timezone as tz
        old = (dt.now(tz.utc) - td(days=95)).strftime('%Y-%m-%d')
        recent = (dt.now(tz.utc) - td(days=30)).strftime('%Y-%m-%d')
        assert lambda_function._date_range_extends_past_ttl({'start_date_iso': old}) is True
        assert lambda_function._date_range_extends_past_ttl({'start_date_iso': recent}) is False
        assert lambda_function._date_range_extends_past_ttl({}) is False

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_fetch_session_summaries_merges_archive_when_range_old(self, mock_ddb, mock_s3):
        """When the date range extends past 90d, fetch_session_summaries should
        include archived sessions alongside DDB rows. Dedupe by session_id."""
        # DDB returns one recent session
        mock_ddb.query.return_value = {
            'Items': [{
                'sk': {'S': 'SESSION#recent_sid'},
                'started_at': {'S': '2026-05-01T10:00:00Z'},
                'ended_at': {'S': '2026-05-01T10:05:00Z'},
                'outcome': {'S': 'conversation'},
                'message_count': {'N': '2'},
                'user_message_count': {'N': '1'},
                'bot_message_count': {'N': '1'},
                'first_question': {'S': 'recent'},
                'total_response_time_ms': {'N': '500'},
                'response_count': {'N': '1'},
            }],
        }
        # S3 returns one older archived session
        old_item = self._archived_item('archived_sid', '2024-06-01T10:00:00Z')
        self._wire_archive_objects(mock_s3, [
            ('sessions/year=2024/month=09/day=01/archived.json', old_item),
        ])

        # Range starts >90 days ago → archive merge triggers
        date_range = {'start_date_iso': '2024-01-01', 'end_date_iso': '2026-05-31'}
        result = lambda_function.fetch_session_summaries('hash_abc123', date_range, limit=100)

        sids = {s['session_id'] for s in result}
        assert 'recent_sid' in sids
        assert 'archived_sid' in sids

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_fetch_session_summaries_skips_archive_when_range_recent(self, mock_ddb, mock_s3):
        """When the date range is fully within the 90d TTL window, archive must
        NOT be hit (saves S3 list/get calls)."""
        from datetime import timedelta as td
        from datetime import datetime as dt
        from datetime import timezone as tz
        mock_ddb.query.return_value = {'Items': []}

        recent = (dt.now(tz.utc) - td(days=30)).strftime('%Y-%m-%d')
        result = lambda_function.fetch_session_summaries(
            'hash_abc123',
            {'start_date_iso': recent, 'end_date_iso': recent},
            limit=100,
        )
        assert result == []
        # Archive helper should not have invoked S3 list/get
        mock_s3.get_paginator.assert_not_called()
        mock_s3.get_object.assert_not_called()

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_fetch_session_summaries_dedupes_overlap(self, mock_ddb, mock_s3):
        """If DDB and S3 both have the same session_id (overlap at the 90d boundary),
        DDB wins — the archive entry is dropped from the merge."""
        mock_ddb.query.return_value = {
            'Items': [{
                'sk': {'S': 'SESSION#dup_sid'},
                'started_at': {'S': '2025-12-01T10:00:00Z'},
                'ended_at': {'S': '2025-12-01T10:05:00Z'},
                'outcome': {'S': 'form_completed'},  # DDB version says form
                'message_count': {'N': '5'},
                'user_message_count': {'N': '3'},
                'bot_message_count': {'N': '2'},
                'first_question': {'S': 'ddb'},
                'total_response_time_ms': {'N': '0'},
                'response_count': {'N': '0'},
            }],
        }
        dup_item = self._archived_item('dup_sid', '2025-12-01T10:00:00Z', outcome='conversation')
        self._wire_archive_objects(mock_s3, [
            ('sessions/year=2025/month=12/day=01/dup.json', dup_item),
        ])

        date_range = {'start_date_iso': '2024-01-01', 'end_date_iso': '2025-12-31'}
        result = lambda_function.fetch_session_summaries('hash_abc123', date_range, limit=100)

        matching = [s for s in result if s['session_id'] == 'dup_sid']
        assert len(matching) == 1
        # DDB version wins
        assert matching[0]['outcome'] == 'form_completed'
        assert matching[0]['message_count'] == 5


# ---------------------------------------------------------------------------
# Phase 2.8 audit follow-up — REAL archiver-produced object fixtures (B2/B3)
#
# The 10 TestS3ArchiveReadPath tests use synthetic _archived_item() builders
# that include a top-level `tenant_hash` field. Real archiver output (verified
# 2026-05-11 via aws s3 cp from picasso-archive-staging) does NOT include that
# field — analytics_writer.py only writes tenant_hash into the DDB Key.pk, not
# as a separate attribute. This means every synthetic test exercises a fallback
# filter branch (`item.get('tenant_hash') != tenant_hash`) that real data never
# populates. These tests validate the reader against actual archiver output.
# ---------------------------------------------------------------------------

class TestArchivedRealFixture:
    """Tests against literal JSON captured from real archiver-produced objects
    in s3://picasso-archive-staging on 2026-05-11. These fixtures are the
    exact bytes the deployed archiver writes when a real DDB Streams REMOVE
    event fires — no synthetic embellishment."""

    # Captured from sessions/year=2026/month=05/day=11/archive_verify_1778513282.json
    # via: aws s3 cp s3://picasso-archive-staging/sessions/.../*.json -
    REAL_ARCHIVER_OUTPUT_FULL = {
        "tenant_id": "MYR384719",
        "first_question": "Phase 2.7 verification test (retry)",
        "sk": "SESSION#archive_verify_1778513282",
        "session_id": "archive_verify_1778513282",
        "started_at": "2026-05-11T15:28:02.000Z",
        "pk": "TENANT#my87674d777bf9",
        "message_count": 2,
        "ttl": 1778509682,
        "ended_at": "2026-05-11T15:28:02.000Z",
    }

    # Captured from sessions/year=2026/month=05/day=11/verify_1778513346.json
    # Minimal shape — TTL-deleted before bot/user_message_count etc were written.
    REAL_ARCHIVER_OUTPUT_MINIMAL = {
        "tenant_id": "MYR384719",
        "sk": "SESSION#verify_1778513346",
        "session_id": "verify_1778513346",
        "pk": "TENANT#my87674d777bf9",
    }

    # What a real MFS-produced session-summary row would archive when TTL fires.
    # Captured from picasso-session-summaries-staging::bsh_smoke_1777997575 row
    # via: aws dynamodb scan --table-name picasso-session-summaries-staging
    # (then conceptually deserialized via the archiver's TypeDeserializer path).
    REAL_PRODUCTION_SHAPE = {
        "total_response_time_ms": 1739,
        "first_question": "hi",
        "last_request_id_message_sent": "2800f690-1d5d-4e4a-b323-11d235e5d77a",
        "ttl": 1785773575,
        "last_request_id_message_received": "2800f690-1d5d-4e4a-b323-11d235e5d77a",
        "user_message_count": 1,
        "message_count": 2,
        "bot_message_count": 1,
        "ended_at": "2026-05-05T16:12:55.251Z",
        "tenant_id": "MYR384719",
        "session_id": "bsh_smoke_1777997575",
        "sk": "SESSION#bsh_smoke_1777997575",
        "pk": "TENANT#my87674d777bf9",
        "response_count": 1,
        "started_at": "2026-05-05T16:12:55.251Z",
    }

    def _wire_one(self, mock_s3, item):
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'sessions/year=2026/month=05/day=11/real.json'}]}
        ]
        mock_s3.get_paginator.return_value = mock_paginator
        body = MagicMock()
        body.read.return_value = json.dumps(item).encode('utf-8')
        mock_s3.get_object.return_value = {'Body': body}

    @patch('lambda_function.s3')
    def test_minimal_archive_without_started_at_is_dropped(self, mock_s3):
        """Documents a known gap: archived rows lacking `started_at` are dropped
        at the date filter (lambda_function.py:2138 `if not started_at: continue`)
        BEFORE the tenant filter runs. A session deleted before MFS wrote
        started_at is therefore unrecoverable via the archive read path.

        Renamed 2026-05-11 (audit B3 closure): prior name claimed the test
        verified the tenant filter, but the row never reaches the tenant filter
        — it exits at the started_at guard first."""
        self._wire_one(mock_s3, self.REAL_ARCHIVER_OUTPUT_MINIMAL)
        result = lambda_function._fetch_archived_sessions(
            'my87674d777bf9',
            {'start_date_iso': '2020-01-01', 'end_date_iso': '2030-12-31'},
        )
        assert result == []

    @patch('lambda_function.s3')
    def test_pk_only_tenant_filter_accepts_matching_real_archive(self, mock_s3):
        """B3 verification (the real test): real archiver output has no `tenant_hash`
        attribute, only `pk`. The pk-only filter (audit B3 closure) must accept rows
        whose pk matches expected_pk, and reject rows whose pk does not match."""
        matching = dict(self.REAL_ARCHIVER_OUTPUT_FULL)  # pk = TENANT#my87674d777bf9
        non_matching = dict(self.REAL_ARCHIVER_OUTPUT_FULL)
        non_matching['pk'] = 'TENANT#some_other_tenant'
        non_matching['session_id'] = 'other_tenant_sess'
        non_matching['sk'] = 'SESSION#other_tenant_sess'

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{
            'Contents': [
                {'Key': 'sessions/year=2026/month=05/day=11/match.json'},
                {'Key': 'sessions/year=2026/month=05/day=11/other.json'},
            ]
        }]
        mock_s3.get_paginator.return_value = mock_paginator

        def fake_get_object(Bucket, Key):
            body = MagicMock()
            body.read.return_value = json.dumps(matching if 'match' in Key else non_matching).encode('utf-8')
            return {'Body': body}

        mock_s3.get_object.side_effect = fake_get_object

        result = lambda_function._fetch_archived_sessions(
            'my87674d777bf9',
            {'start_date_iso': '2026-05-01', 'end_date_iso': '2026-05-31'},
        )
        assert len(result) == 1
        assert result[0]['session_id'] == 'archive_verify_1778513282'

    @patch('lambda_function.s3')
    def test_real_archiver_output_full_passes_filter_and_shapes_correctly(self, mock_s3):
        """B2/B3 verification: feed REAL archiver-produced JSON into the reader.
        Must (a) pass the tenant filter via `pk` alone, (b) match the date range,
        (c) shape correctly with sane defaults for fields not present in this row."""
        self._wire_one(mock_s3, self.REAL_ARCHIVER_OUTPUT_FULL)
        result = lambda_function._fetch_archived_sessions(
            'my87674d777bf9',
            {'start_date_iso': '2026-05-01', 'end_date_iso': '2026-05-31'},
        )
        assert len(result) == 1
        s = result[0]
        assert s['session_id'] == 'archive_verify_1778513282'
        assert s['started_at'] == '2026-05-11T15:28:02.000Z'
        assert s['ended_at'] == '2026-05-11T15:28:02.000Z'
        assert s['first_question'] == 'Phase 2.7 verification test (retry)'
        assert s['message_count'] == 2
        # Fields absent in this archiver-output → reader must default sanely:
        assert s['user_message_count'] == 0
        assert s['bot_message_count'] == 0
        assert s['outcome'] == 'conversation'   # missing → default
        assert s['form_id'] == ''               # missing → default
        assert s['total_response_time_ms'] == 0
        assert s['response_count'] == 0
        assert s['duration_seconds'] == 0       # ended_at == started_at

    @patch('lambda_function.s3')
    def test_real_production_shape_archives_correctly(self, mock_s3):
        """The 'when real soak data ages past 90d TTL' case. Verifies the reader
        handles the full-shape MFS row (5 message-count fields + last_request_id_*
        idempotency keys + ttl as int + no outcome field)."""
        self._wire_one(mock_s3, self.REAL_PRODUCTION_SHAPE)
        result = lambda_function._fetch_archived_sessions(
            'my87674d777bf9',
            {'start_date_iso': '2026-05-01', 'end_date_iso': '2026-05-31'},
        )
        assert len(result) == 1
        s = result[0]
        assert s['session_id'] == 'bsh_smoke_1777997575'
        assert s['message_count'] == 2
        assert s['user_message_count'] == 1
        assert s['bot_message_count'] == 1
        assert s['total_response_time_ms'] == 1739
        assert s['response_count'] == 1
        assert s['outcome'] == 'conversation'   # MFS never sets outcome — reader defaults
        assert s['form_id'] == ''
        # last_request_id_* fields are silently ignored by _archived_item_to_session_shape —
        # that's correct, they're idempotency keys not user-facing data.


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
