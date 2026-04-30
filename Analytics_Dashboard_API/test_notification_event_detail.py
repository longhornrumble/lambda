"""
Tests for handle_notification_event_detail() and handle_notification_events().

Verifies both notification dashboard endpoints display the per-event
timestamp captured in the `detail` map, with a fallback to the legacy
event_type_timestamp SK when detail lacks a timestamp. Pre-fix, both
endpoints used mail.timestamp from the SK for every event type, which
collapsed display timestamps to the message's send time.
"""

import json
from unittest.mock import patch

import pytest

from lambda_function import handle_notification_event_detail, handle_notification_events


def _ddb_event(event_type, sk_timestamp=None, **detail_fields):
    """Build a DynamoDB-shaped Item for picasso-notification-events."""
    sk_ts = sk_timestamp if sk_timestamp is not None else '2026-04-26T20:00:00.000Z'
    detail_map = {k: {'S': v} for k, v in detail_fields.items()}
    return {
        'event_type': {'S': event_type},
        'event_type_timestamp': {'S': f'{event_type}#{sk_ts}'},
        'detail': {'M': detail_map},
    }


@patch('lambda_function.validate_feature_access', return_value=None)
@patch('lambda_function.dynamodb')
def test_events_sorted_chronologically_by_per_event_timestamp(mock_ddb, _vf):
    """delivery + open should sort by their own timestamps, not by mail.timestamp."""
    # All three events share the same mail.timestamp in the SK (the bug condition
    # for old-style writes). Detail map carries the real event times.
    mock_ddb.query.return_value = {
        'Items': [
            # Returned alphabetically by GSI sort key — delivery, open, send
            _ddb_event(
                'delivery',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                delivery_timestamp='2026-04-26T20:01:30.000Z',
            ),
            _ddb_event(
                'open',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                open_timestamp='2026-04-27T08:15:00.000Z',
            ),
            _ddb_event(
                'send',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                send_timestamp='2026-04-26T20:00:00.000Z',
            ),
        ]
    }

    result = handle_notification_event_detail('AUS123957', 'msg-1')
    body = json.loads(result['body'])

    assert result['statusCode'] == 200
    assert [e['event_type'] for e in body['events']] == ['send', 'delivery', 'open']
    assert [e['timestamp'] for e in body['events']] == [
        '2026-04-26T20:00:00.000Z',
        '2026-04-26T20:01:30.000Z',
        '2026-04-27T08:15:00.000Z',
    ]


@patch('lambda_function.validate_feature_access', return_value=None)
@patch('lambda_function.dynamodb')
def test_falls_back_to_sk_timestamp_when_detail_lacks_per_event_ts(mock_ddb, _vf):
    """Pre-migration rows without per-event detail timestamps still render."""
    mock_ddb.query.return_value = {
        'Items': [
            {
                'event_type': {'S': 'send'},
                'event_type_timestamp': {'S': 'send#2026-04-26T20:00:00.000Z'},
                'detail': {'M': {}},
            },
        ]
    }

    result = handle_notification_event_detail('AUS123957', 'msg-1')
    body = json.loads(result['body'])

    assert result['statusCode'] == 200
    assert body['events'] == [
        {'event_type': 'send', 'timestamp': '2026-04-26T20:00:00.000Z', 'detail': {}},
    ]


@patch('lambda_function.validate_feature_access', return_value=None)
@patch('lambda_function.dynamodb')
def test_bounce_uses_bounce_timestamp_from_detail(mock_ddb, _vf):
    """Bounce events should sort by bounce.timestamp."""
    mock_ddb.query.return_value = {
        'Items': [
            _ddb_event(
                'bounce',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                bounce_timestamp='2026-04-26T20:00:42.000Z',
                bounce_type='Permanent',
            ),
            _ddb_event(
                'send',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                send_timestamp='2026-04-26T20:00:00.000Z',
            ),
        ]
    }

    result = handle_notification_event_detail('AUS123957', 'msg-1')
    body = json.loads(result['body'])

    assert [e['event_type'] for e in body['events']] == ['send', 'bounce']
    assert body['events'][1]['timestamp'] == '2026-04-26T20:00:42.000Z'
    assert body['events'][1]['detail']['bounce_type'] == 'Permanent'


@patch('lambda_function.validate_feature_access', return_value=None)
@patch('lambda_function.dynamodb')
def test_click_and_complaint_use_their_own_timestamps(mock_ddb, _vf):
    """click_timestamp and complaint_timestamp drive ordering for those event types."""
    mock_ddb.query.return_value = {
        'Items': [
            _ddb_event(
                'click',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                click_timestamp='2026-04-27T09:30:00.000Z',
            ),
            _ddb_event(
                'complaint',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                complaint_timestamp='2026-04-26T20:05:00.000Z',
            ),
            _ddb_event(
                'send',
                sk_timestamp='2026-04-26T20:00:00.000Z',
                send_timestamp='2026-04-26T20:00:00.000Z',
            ),
        ]
    }

    result = handle_notification_event_detail('AUS123957', 'msg-1')
    body = json.loads(result['body'])

    assert [e['event_type'] for e in body['events']] == ['send', 'complaint', 'click']
    assert [e['timestamp'] for e in body['events']] == [
        '2026-04-26T20:00:00.000Z',
        '2026-04-26T20:05:00.000Z',
        '2026-04-27T09:30:00.000Z',
    ]


@patch('lambda_function.validate_feature_access', return_value=None)
def test_invalid_message_id_rejected(_vf):
    """Reject obviously malformed message_id without touching DynamoDB."""
    result = handle_notification_event_detail('AUS123957', 'not<valid>')
    body = json.loads(result['body'])
    assert result['statusCode'] == 400
    assert 'invalid' in body['error'].lower()


# -----------------------------------------------------------------------------
# handle_notification_events (events list endpoint)
# -----------------------------------------------------------------------------


def _list_event(event_type, sk_date='2026-04-26', sk_timestamp='2026-04-26T20:00:00.000Z',
                recipient='r@example.com', message_id='msg-list-1', **detail_fields):
    """Full DynamoDB Item shape for an events-list query."""
    detail_map = {k: {'S': v} for k, v in detail_fields.items()}
    return {
        'pk': {'S': 'TENANT#AUS123957'},
        'sk': {'S': f'{sk_date}#{event_type}#{message_id}'},
        'event_type': {'S': event_type},
        'event_type_timestamp': {'S': f'{event_type}#{sk_timestamp}'},
        'channel': {'S': 'email'},
        'destination': {'L': [{'S': recipient}]},
        'context': {'M': {'form_id': {'S': 'mentor_app'}}},
        'tags': {'M': {'email_type': {'S': 'internal_notification'}}},
        'detail': {'M': detail_map},
    }


@patch('lambda_function.get_tenant_config', return_value=None)
@patch('lambda_function.validate_feature_access', return_value=None)
@patch('lambda_function.dynamodb')
def test_events_list_uses_per_event_timestamp_from_detail(mock_ddb, _vf, _tc):
    """Events list display timestamp must come from detail.* per-event time, not the SK."""
    mock_ddb.query.return_value = {
        'Items': [
            _list_event('open',
                        sk_timestamp='2026-04-26T20:00:00.000Z',
                        open_timestamp='2026-04-27T08:15:00.000Z'),
            _list_event('delivery',
                        sk_timestamp='2026-04-26T20:00:00.000Z',
                        delivery_timestamp='2026-04-26T20:01:30.000Z'),
            _list_event('send',
                        sk_timestamp='2026-04-26T20:00:00.000Z',
                        send_timestamp='2026-04-26T20:00:00.000Z'),
        ]
    }

    result = handle_notification_events('AUS123957', {'range': '7d'})
    body = json.loads(result['body'])

    assert result['statusCode'] == 200
    timestamps_by_type = {e['event_type']: e['timestamp'] for e in body['events']}
    assert timestamps_by_type['send']     == '2026-04-26T20:00:00.000Z'
    assert timestamps_by_type['delivery'] == '2026-04-26T20:01:30.000Z'
    assert timestamps_by_type['open']     == '2026-04-27T08:15:00.000Z'


@patch('lambda_function.get_tenant_config', return_value=None)
@patch('lambda_function.validate_feature_access', return_value=None)
@patch('lambda_function.dynamodb')
def test_events_list_falls_back_to_sk_when_detail_lacks_timestamp(mock_ddb, _vf, _tc):
    """Pre-migration rows without per-event detail timestamps fall back to legacy SK."""
    mock_ddb.query.return_value = {
        'Items': [
            {
                'pk': {'S': 'TENANT#AUS123957'},
                'sk': {'S': '2026-04-26#send#msg-legacy'},
                'event_type': {'S': 'send'},
                'event_type_timestamp': {'S': 'send#2026-04-26T20:00:00.000Z'},
                'channel': {'S': 'email'},
                'destination': {'L': [{'S': 'r@example.com'}]},
                'context': {'M': {}},
                'tags': {'M': {}},
                'detail': {'M': {}},
            }
        ]
    }

    result = handle_notification_events('AUS123957', {'range': '7d'})
    body = json.loads(result['body'])

    assert result['statusCode'] == 200
    assert body['events'][0]['timestamp'] == '2026-04-26T20:00:00.000Z'
