"""
Tests for ses_event_handler.

Focuses on the per-event timestamp resolution that drives the
event_type_timestamp GSI sort key. Prior to the fix, every event type
reused mail.timestamp, collapsing GSI ordering to alphabetical by
event_type. After the fix, delivery/open/click/bounce/complaint use their
own timestamps; send/reject fall back to mail.timestamp.
"""

import json
from unittest.mock import patch, MagicMock

import pytest

import lambda_function


def _sns_record(sns_message: dict) -> dict:
    return {'Sns': {'Message': json.dumps(sns_message)}}


def _mail(message_id='msg-1', timestamp='2026-04-26T20:00:00.000Z'):
    return {
        'messageId': message_id,
        'timestamp': timestamp,
        'source': 'noreply@example.com',
        'destination': ['recipient@example.com'],
        'tags': {'tenant_id': ['AUS123957']},
    }


@patch.object(lambda_function, 'notification_events_table')
def test_delivery_event_uses_delivery_timestamp_in_sk(mock_table):
    """event_type_timestamp must use delivery.timestamp, not mail.timestamp."""
    sns_msg = {
        'eventType': 'Delivery',
        'mail': _mail(timestamp='2026-04-26T20:00:00.000Z'),
        'delivery': {
            'timestamp': '2026-04-26T20:01:30.000Z',
            'recipients': ['recipient@example.com'],
            'smtpResponse': '250 OK',
        },
    }

    lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    args, kwargs = mock_table.put_item.call_args
    item = kwargs['Item']
    assert item['event_type_timestamp'] == 'delivery#2026-04-26T20:01:30.000Z'
    assert item['sk'].startswith('2026-04-26#delivery#')
    assert item['detail']['delivery_timestamp'] == '2026-04-26T20:01:30.000Z'


@patch.object(lambda_function, 'notification_events_table')
def test_open_event_uses_open_timestamp_in_sk(mock_table):
    sns_msg = {
        'eventType': 'Open',
        'mail': _mail(timestamp='2026-04-26T20:00:00.000Z'),
        'open': {
            'timestamp': '2026-04-27T08:15:00.000Z',
            'userAgent': 'Mozilla/5.0',
            'ipAddress': '198.51.100.10',
        },
    }

    lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    item = mock_table.put_item.call_args.kwargs['Item']
    assert item['event_type_timestamp'] == 'open#2026-04-27T08:15:00.000Z'
    # SK partition uses the open date — open happened a day after send
    assert item['sk'].startswith('2026-04-27#open#')


@patch.object(lambda_function, 'notification_events_table')
def test_send_event_falls_back_to_mail_timestamp(mock_table):
    """SES Send event has no event-specific timestamp beyond mail.timestamp."""
    sns_msg = {
        'eventType': 'Send',
        'mail': _mail(timestamp='2026-04-26T20:00:00.000Z'),
        'send': {},
    }

    lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    item = mock_table.put_item.call_args.kwargs['Item']
    assert item['event_type_timestamp'] == 'send#2026-04-26T20:00:00.000Z'
    assert item['sk'].startswith('2026-04-26#send#')


@patch.object(lambda_function, 'notification_events_table')
def test_bounce_event_uses_bounce_timestamp(mock_table):
    sns_msg = {
        'eventType': 'Bounce',
        'mail': _mail(timestamp='2026-04-26T20:00:00.000Z'),
        'bounce': {
            'bounceType': 'Permanent',
            'bounceSubType': 'General',
            'bouncedRecipients': [{'emailAddress': 'bad@example.com'}],
            'timestamp': '2026-04-26T20:00:42.000Z',
            'feedbackId': 'fb-1',
        },
    }

    lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    item = mock_table.put_item.call_args.kwargs['Item']
    assert item['event_type_timestamp'] == 'bounce#2026-04-26T20:00:42.000Z'


@patch.object(lambda_function, 'notification_events_table')
def test_click_event_uses_click_timestamp(mock_table):
    sns_msg = {
        'eventType': 'Click',
        'mail': _mail(timestamp='2026-04-26T20:00:00.000Z'),
        'click': {
            'timestamp': '2026-04-27T09:30:00.000Z',
            'link': 'https://example.com/apply',
            'userAgent': 'Mozilla/5.0',
        },
    }

    lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    item = mock_table.put_item.call_args.kwargs['Item']
    assert item['event_type_timestamp'] == 'click#2026-04-27T09:30:00.000Z'


@patch.object(lambda_function, 'notification_events_table')
def test_complaint_event_uses_complaint_timestamp(mock_table):
    sns_msg = {
        'eventType': 'Complaint',
        'mail': _mail(timestamp='2026-04-26T20:00:00.000Z'),
        'complaint': {
            'timestamp': '2026-04-26T20:05:00.000Z',
            'complainedRecipients': [{'emailAddress': 'r@example.com'}],
            'complaintFeedbackType': 'abuse',
        },
    }

    lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    item = mock_table.put_item.call_args.kwargs['Item']
    assert item['event_type_timestamp'] == 'complaint#2026-04-26T20:05:00.000Z'


@patch.object(lambda_function, 'notification_events_table')
def test_no_tenant_tag_skips_dynamodb_write(mock_table):
    sns_msg = {
        'eventType': 'Send',
        'mail': {
            'messageId': 'msg-2',
            'timestamp': '2026-04-26T20:00:00.000Z',
            'tags': {},
        },
        'send': {},
    }

    result = lambda_function.lambda_handler({'Records': [_sns_record(sns_msg)]}, None)

    assert result['statusCode'] == 200
    mock_table.put_item.assert_not_called()
