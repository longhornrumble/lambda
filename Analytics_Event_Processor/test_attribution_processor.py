"""
Tests for Analytics_Event_Processor attribution additions.

Covers:
- PAGE_VIEW happy path (frontend nested format, pv_ session id, optional ga_client_id)
- Old-shape event contract/fixture (reader must not crash without new fields)
- Duplicate PAGE_VIEW idempotency (same PK/SK overwrite — C8.5)
- CONVERSATION_STARTED entry_point_id extraction
- No IP enrichment (C8.6)
- Attribution field storage

Run: pytest test_attribution_processor.py -v
"""
import json
import pytest
from unittest.mock import patch, MagicMock, call
from botocore.exceptions import ClientError
import lambda_function
from lambda_function import (
    enrich_event,
    write_session_event,
    lambda_handler,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _page_view_event(session_id='pv_test123', ga_client_id=None, path='/about'):
    """Frontend PAGE_VIEW envelope — FROZEN_CONTRACTS C1.3."""
    evt = {
        'schema_version': '1.0.0',
        'tenant_id': 'fo85e6a06dcdf4',  # hash (frontend format)
        'session_id': session_id,
        'timestamp': '2026-06-12T10:30:00.000Z',
        'step_number': 1,
        'event': {
            'type': 'PAGE_VIEW',
            'payload': {
                'path': path,
                'referrer_host': 'google.com',
                'device_class': 'mobile',
            },
        },
    }
    if ga_client_id is not None:
        evt['ga_client_id'] = ga_client_id
    return evt


def _conversation_started_event(session_id='sess_abc', entry_point_id='ep_ABCD1234', ga_client_id='111.222'):
    """CONVERSATION_STARTED with attribution payload — C1.1."""
    return {
        'schema_version': '1.0.0',
        'tenant_id': 'fo85e6a06dcdf4',
        'session_id': session_id,
        'timestamp': '2026-06-12T10:00:00.000Z',
        'step_number': 1,
        'ga_client_id': ga_client_id,
        'event': {
            'type': 'CONVERSATION_STARTED',
            'payload': {
                'entry_point_id': entry_point_id,
                'attribution': {
                    'ga_client_id': ga_client_id,
                    'utm_source': 'email',
                    'utm_campaign': 'spring_gala',
                    'referrer': 'https://example.com',
                    'landing_page': 'https://myrecruiter.ai/chat',
                    'captured_at': '2026-06-12T10:00:00Z',
                },
            },
        },
    }


def _old_shape_event():
    """
    CONTRACT/FIXTURE: old-shape event WITHOUT new attribution fields.
    The reader (enrich_event) must not crash when these are absent.
    FROZEN_CONTRACTS C2 / Schema Discipline.
    """
    return {
        'schema_version': '1.0.0',
        'tenant_id': 'fo85e6a06dcdf4',
        'tenant_hash': 'fo85e6a06dcdf4',
        'tenant_id': 'FOS402334',
        'session_id': 'sess_old_shape',
        'timestamp': '2025-01-01T10:00:00.000Z',
        'step_number': 1,
        'event_type': 'WIDGET_OPENED',
        'event_payload': {},
        # NOTE: no ga_client_id, no attribution, no entry_point_id — old shape
    }


def _sqs_record(message_id, body):
    return {'messageId': message_id, 'body': json.dumps(body)}


def _mock_s3_mapping():
    """Mock S3 response for tenant hash lookup."""
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=lambda: json.dumps({
            'tenant_id': 'FOS402334',
            'tenant_hash': 'fo85e6a06dcdf4',
        }).encode('utf-8'))
    }
    return mock_s3


# ---------------------------------------------------------------------------
# PAGE_VIEW happy path tests
# ---------------------------------------------------------------------------
class TestPageViewHappyPath:
    """PAGE_VIEW events must flow through without error."""

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_page_view_enriched_successfully(self, mock_ddb, mock_s3):
        """PAGE_VIEW event produces valid enriched record."""
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        evt = _page_view_event(ga_client_id='111.222')
        # Clear cache so S3 lookup fires
        lambda_function._tenant_mapping_cache.clear()
        enriched = enrich_event(evt)

        assert enriched is not None
        assert enriched['event_type'] == 'PAGE_VIEW'
        assert enriched['session_id'] == 'pv_test123'
        assert enriched['ga_client_id'] == '111.222'
        assert enriched['tenant_id'] == 'FOS402334'

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_page_view_pv_session_id_written_to_ddb(self, mock_ddb, mock_s3):
        """pv_ session_id must produce correct PK/SK in DynamoDB."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()
        evt = _page_view_event(session_id='pv_xyzabc789')
        enriched = enrich_event(evt)
        assert enriched is not None
        result = write_session_event(enriched)

        assert result is True
        call_args = mock_ddb.put_item.call_args[1]
        item = call_args['Item']
        assert item['pk']['S'] == 'SESSION#pv_xyzabc789'
        assert item['sk']['S'] == 'STEP#001'

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_page_view_missing_ga_client_id_succeeds(self, mock_ddb, mock_s3):
        """PAGE_VIEW without ga_client_id must succeed (ga_client_id is optional)."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()
        evt = _page_view_event(ga_client_id=None)  # no ga_client_id
        enriched = enrich_event(evt)

        assert enriched is not None
        assert 'ga_client_id' not in enriched
        result = write_session_event(enriched)
        assert result is True


# ---------------------------------------------------------------------------
# pv_ session id tests
# ---------------------------------------------------------------------------
class TestPvSessionId:
    """pv_ prefixed session IDs must be handled identically to sess_ IDs."""

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_pv_session_id_in_lambda_handler(self, mock_ddb, mock_s3):
        """Lambda handler must process a batch containing pv_ session IDs."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()

        pv_evt = _page_view_event(session_id='pv_unique99')
        batch = {'batch': True, 'events': [pv_evt]}
        result = lambda_handler({'Records': [_sqs_record('msg-pv', batch)]}, None)

        assert result == {'batchItemFailures': []}
        assert mock_ddb.put_item.call_count == 1

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_pv_session_pk_format(self, mock_ddb, mock_s3):
        """DynamoDB PK must be SESSION#pv_... for page-view session IDs."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()

        pv_evt = _page_view_event(session_id='pv_abcdef12')
        enriched = enrich_event(pv_evt)
        write_session_event(enriched)

        item = mock_ddb.put_item.call_args[1]['Item']
        assert item['pk']['S'].startswith('SESSION#pv_')


# ---------------------------------------------------------------------------
# Missing ga_client_id tests
# ---------------------------------------------------------------------------
class TestMissingGaClientId:
    """Absence of ga_client_id must never break processing."""

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_missing_ga_client_id_not_in_ddb_item(self, mock_ddb, mock_s3):
        """Item written to DDB must not have ga_client_id key when absent."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()

        evt = _page_view_event(ga_client_id=None)
        enriched = enrich_event(evt)
        write_session_event(enriched)

        item = mock_ddb.put_item.call_args[1]['Item']
        assert 'ga_client_id' not in item


# ---------------------------------------------------------------------------
# Old-shape event contract/fixture test (FROZEN_CONTRACTS C2 / Schema Discipline)
# ---------------------------------------------------------------------------
class TestOldShapeEventTolerance:
    """
    CONTRACT/FIXTURE: readers must not crash on old-shape events without
    new attribution/entry_point_id fields.
    FROZEN_CONTRACTS C2; Schema Discipline rule.
    """

    def test_old_shape_enrich_does_not_crash(self):
        """enrich_event must succeed on a pre-attribution old-shape event."""
        evt = _old_shape_event()
        # Old shape has server-resolved tenant_id + tenant_hash — no S3 lookup needed
        enriched = enrich_event(evt)
        assert enriched is not None
        # Should not have entry_point_id (wasn't in old shape)
        assert 'entry_point_id' not in enriched or enriched.get('entry_point_id') is None

    @patch('lambda_function.dynamodb')
    def test_old_shape_write_does_not_crash(self, mock_ddb):
        """write_session_event must succeed on old-shape enriched event."""
        mock_ddb.put_item.return_value = {}
        evt = _old_shape_event()
        enriched = enrich_event(evt)
        assert enriched is not None
        result = write_session_event(enriched)
        assert result is True

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_old_shape_through_lambda_handler(self, mock_ddb, mock_s3):
        """Full handler pipeline must not fail on old-shape event."""
        mock_ddb.put_item.return_value = {}
        evt = _old_shape_event()
        result = lambda_handler({'Records': [_sqs_record('msg-old', evt)]}, None)
        assert result == {'batchItemFailures': []}


# ---------------------------------------------------------------------------
# Duplicate (idempotent) PAGE_VIEW tests (C8.5)
# ---------------------------------------------------------------------------
class TestPageViewIdempotency:
    """
    Same PK/SK must result in an overwrite (DDB put_item is idempotent on key).
    C8.5: tolerate/drop PAGE_VIEW duplicates idempotently.
    """

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_duplicate_page_view_both_succeed(self, mock_ddb, mock_s3):
        """Two PAGE_VIEW events with same session_id/step_number both call put_item."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()

        evt = _page_view_event(session_id='pv_dup123')
        evt2 = dict(evt)  # same session/step

        batch = {'batch': True, 'events': [evt, evt2]}
        result = lambda_handler({'Records': [_sqs_record('msg-dup', batch)]}, None)

        # Both processed, no failures (DDB overwrites are idempotent)
        assert result == {'batchItemFailures': []}
        assert mock_ddb.put_item.call_count == 2
        # Both writes target the same PK/SK — that's correct (DDB upsert behavior)
        calls = mock_ddb.put_item.call_args_list
        pk0 = calls[0][1]['Item']['pk']['S']
        pk1 = calls[1][1]['Item']['pk']['S']
        assert pk0 == pk1  # same key — idempotent overwrite


# ---------------------------------------------------------------------------
# CONVERSATION_STARTED entry_point_id extraction
# ---------------------------------------------------------------------------
class TestConversationStartedEntryPoint:
    """entry_point_id must be extracted from CONVERSATION_STARTED attribution payload."""

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_entry_point_id_stored_in_ddb(self, mock_ddb, mock_s3):
        """entry_point_id extracted from CONVERSATION_STARTED and stored as top-level attribute."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()

        evt = _conversation_started_event(entry_point_id='ep_TESTEP12')
        enriched = enrich_event(evt)

        assert enriched is not None
        assert enriched.get('entry_point_id') == 'ep_TESTEP12'

        write_session_event(enriched)
        item = mock_ddb.put_item.call_args[1]['Item']
        assert item.get('entry_point_id', {}).get('S') == 'ep_TESTEP12'

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_no_entry_point_id_no_crash(self, mock_ddb, mock_s3):
        """CONVERSATION_STARTED without entry_point_id must not crash."""
        mock_ddb.put_item.return_value = {}
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()

        evt = {
            'schema_version': '1.0.0',
            'tenant_id': 'fo85e6a06dcdf4',
            'session_id': 'sess_noep',
            'timestamp': '2026-06-12T10:00:00Z',
            'step_number': 1,
            'event': {
                'type': 'CONVERSATION_STARTED',
                'payload': {'entry_point_id': None, 'attribution': {}},
            },
        }
        enriched = enrich_event(evt)
        assert enriched is not None
        assert 'entry_point_id' not in enriched


# ---------------------------------------------------------------------------
# No IP enrichment (C8.6)
# ---------------------------------------------------------------------------
class TestNoIpEnrichment:
    """C8.6: NO IP enrichment anywhere in the processor."""

    @patch('lambda_function.s3')
    @patch('lambda_function.dynamodb')
    def test_no_ip_in_enriched_event(self, mock_ddb, mock_s3):
        """Enriched event must never contain ip, source_ip, or geo fields."""
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps({
                'tenant_id': 'FOS402334',
                'tenant_hash': 'fo85e6a06dcdf4',
            }).encode())
        }
        lambda_function._tenant_mapping_cache.clear()
        evt = _page_view_event(ga_client_id='111.222')
        enriched = enrich_event(evt)

        ip_fields = {'ip', 'source_ip', 'client_ip', 'geo', 'country', 'city', 'region'}
        assert not (ip_fields & set((enriched or {}).keys())), \
            f'IP/geo fields found in enriched event: {ip_fields & set(enriched.keys())}'

    def test_enrich_event_function_has_no_ip_code(self):
        """enrich_event source must not contain any IP-enrichment logic (static check)."""
        import inspect
        source = inspect.getsource(enrich_event)
        forbidden = ['source_ip', 'client_ip', 'geo_ip', 'geoip', 'geo_lookup']
        for token in forbidden:
            assert token not in source.lower(), f'IP enrichment code found: {token}'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
