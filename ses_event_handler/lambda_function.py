"""
SES Event Handler Lambda Function

This Lambda receives SES event notifications (bounce, complaint, delivery, etc.)
from SNS and forwards them to Bubble via webhook.
"""

import json
import logging
import os
import time
import urllib.request
import urllib.error

import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
BUBBLE_WEBHOOK_URL = os.environ.get(
    'BUBBLE_WEBHOOK_URL',
    'https://hrfx.bubbleapps.io/api/1.1/wf/ses_event'
)
WEBHOOK_TIMEOUT = int(os.environ.get('WEBHOOK_TIMEOUT', '10'))
BUBBLE_FORWARDING_ENABLED = os.environ.get('BUBBLE_FORWARDING_ENABLED', 'true').lower() == 'true'

# DynamoDB
_dynamodb = boto3.resource('dynamodb')
notification_events_table = _dynamodb.Table('picasso-notification-events')


def lambda_handler(event, context):
    """
    Process SES events from SNS and forward to Bubble.

    SNS delivers events in the Records array, where each record contains
    an Sns object with the Message field containing the SES notification JSON.
    """
    processed = 0
    errors = 0

    for record in event.get('Records', []):
        try:
            # Parse SNS message
            sns_message_str = record.get('Sns', {}).get('Message', '{}')
            sns_message = json.loads(sns_message_str)

            # Extract event type (handles both old and new notification formats)
            event_type = sns_message.get('eventType') or sns_message.get('notificationType')

            if not event_type:
                logger.warning(f"Unknown event format: {sns_message_str[:200]}")
                continue

            # Extract mail object (common to all event types)
            mail = sns_message.get('mail', {})

            # Build base payload for Bubble
            payload = {
                'event_type': event_type.lower(),
                'message_id': mail.get('messageId'),
                'timestamp': mail.get('timestamp'),
                'source': mail.get('source'),
                'destination': mail.get('destination', []),
                'source_arn': mail.get('sourceArn'),
                'source_ip': mail.get('sourceIp'),
                'sending_account_id': mail.get('sendingAccountId'),
            }

            # Extract tags as a dictionary
            # SES tags in SNS events come as {"key": ["val1", "val2"], ...}
            raw_tags = mail.get('tags', {})
            tags = {}
            if isinstance(raw_tags, dict):
                for tag_name, tag_values in raw_tags.items():
                    if isinstance(tag_values, list) and len(tag_values) == 1:
                        tags[tag_name] = tag_values[0]
                    else:
                        tags[tag_name] = tag_values
            elif isinstance(raw_tags, list):
                # Legacy format: [{"name": "key", "value": ["val"]}]
                for tag in raw_tags:
                    tag_name = tag.get('name')
                    tag_values = tag.get('value', [])
                    if tag_name and tag_values:
                        tags[tag_name] = tag_values[0] if len(tag_values) == 1 else tag_values
            payload['tags'] = tags

            # Transition guard: check for tenant_id tag added by Phase 1 migration.
            # Pre-migration emails (sent before ConfigurationSet/Tags were added) will
            # not carry tenant_id. For those, we skip future DynamoDB writes but still
            # forward to Bubble for backwards compatibility.
            tenant_id = tags.get('tenant_id')
            if not tenant_id:
                logger.warning(
                    f"No tenant_id tag found on message {mail.get('messageId')} — "
                    "pre-migration email, skipping DynamoDB write"
                )

            # Write to picasso-notification-events (post-migration emails only)
            if tenant_id:
                try:
                    event_type_lower = payload['event_type']
                    message_id = payload.get('message_id') or 'unknown'
                    iso_date = (payload.get('timestamp') or '').split('T')[0] or \
                        __import__('datetime').datetime.utcnow().strftime('%Y-%m-%d')

                    # Build event-specific detail map from existing payload keys
                    skip_keys = {
                        'event_type', 'message_id', 'timestamp', 'source',
                        'destination', 'source_arn', 'source_ip',
                        'sending_account_id', 'tags'
                    }
                    detail = {k: v for k, v in payload.items() if k not in skip_keys and v is not None}

                    notification_events_table.put_item(Item={
                        'pk': f'TENANT#{tenant_id}',
                        'sk': f'{iso_date}#{event_type_lower}#{message_id}',
                        'message_id': message_id,
                        'event_type': event_type_lower,
                        'channel': 'email',
                        'destination': payload.get('destination', []),
                        'source': payload.get('source', ''),
                        'context': {
                            'form_id': tags.get('form_id', ''),
                            'submission_id': tags.get('submission_id', ''),
                            'session_id': tags.get('session_id', ''),
                        },
                        'detail': detail,
                        'tags': tags,
                        'ttl': int(time.time()) + (90 * 24 * 3600),
                        'event_type_timestamp': f'{event_type_lower}#{payload.get("timestamp", "")}',
                    })
                    logger.info(
                        f"DynamoDB write: picasso-notification-events "
                        f"TENANT#{tenant_id} / {event_type_lower} / {message_id}"
                    )
                except Exception as ddb_err:
                    logger.error(
                        f"Failed to write notification event to DynamoDB: {ddb_err}",
                        exc_info=True
                    )

            # Add event-specific details
            if event_type.lower() == 'bounce':
                bounce = sns_message.get('bounce', {})
                payload.update({
                    'bounce_type': bounce.get('bounceType'),
                    'bounce_subtype': bounce.get('bounceSubType'),
                    'bounced_recipients': [
                        r.get('emailAddress') for r in bounce.get('bouncedRecipients', [])
                    ],
                    'bounce_timestamp': bounce.get('timestamp'),
                    'feedback_id': bounce.get('feedbackId'),
                    'remote_mta_ip': bounce.get('remoteMtaIp'),
                    'reporting_mta': bounce.get('reportingMTA'),
                })

            elif event_type.lower() == 'complaint':
                complaint = sns_message.get('complaint', {})
                payload.update({
                    'complaint_type': complaint.get('complaintFeedbackType'),
                    'complained_recipients': [
                        r.get('emailAddress') for r in complaint.get('complainedRecipients', [])
                    ],
                    'complaint_timestamp': complaint.get('timestamp'),
                    'feedback_id': complaint.get('feedbackId'),
                    'user_agent': complaint.get('userAgent'),
                    'complaint_sub_type': complaint.get('complaintSubType'),
                })

            elif event_type.lower() == 'delivery':
                delivery = sns_message.get('delivery', {})
                payload.update({
                    'delivery_timestamp': delivery.get('timestamp'),
                    'processing_time_millis': delivery.get('processingTimeMillis'),
                    'recipients': delivery.get('recipients', []),
                    'smtp_response': delivery.get('smtpResponse'),
                    'remote_mta_ip': delivery.get('remoteMtaIp'),
                    'reporting_mta': delivery.get('reportingMTA'),
                })

            elif event_type.lower() == 'send':
                # Send event - minimal additional data
                send_info = sns_message.get('send', {})
                payload.update({
                    'send_timestamp': mail.get('timestamp'),
                })

            elif event_type.lower() == 'reject':
                # Reject event
                reject = sns_message.get('reject', {})
                payload.update({
                    'reject_reason': reject.get('reason'),
                })

            elif event_type.lower() == 'open':
                # Open tracking event
                open_info = sns_message.get('open', {})
                payload.update({
                    'open_timestamp': open_info.get('timestamp'),
                    'user_agent': open_info.get('userAgent'),
                    'ip_address': open_info.get('ipAddress'),
                })

            elif event_type.lower() == 'click':
                # Click tracking event
                click = sns_message.get('click', {})
                payload.update({
                    'click_timestamp': click.get('timestamp'),
                    'link': click.get('link'),
                    'link_tags': click.get('linkTags', {}),
                    'user_agent': click.get('userAgent'),
                    'ip_address': click.get('ipAddress'),
                })

            # Forward to Bubble (controlled by BUBBLE_FORWARDING_ENABLED env var)
            bubble_success = True
            if BUBBLE_FORWARDING_ENABLED:
                bubble_success = forward_to_bubble(payload)

            if bubble_success:
                processed += 1
                logger.info(f"Processed {event_type} event for message {mail.get('messageId')}")
            else:
                errors += 1

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse SNS message: {str(e)}")
            errors += 1
        except Exception as e:
            logger.error(f"Error processing SES event: {str(e)}", exc_info=True)
            errors += 1

    logger.info(f"Completed: {processed} processed, {errors} errors")

    # Return non-200 when all records failed so SNS will retry delivery.
    # Partial failures still return 200 — individual error details are in logs.
    if errors > 0 and processed == 0:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'processed': processed,
                'errors': errors
            })
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'errors': errors
        })
    }


def forward_to_bubble(payload):
    """
    POST event to Bubble webhook.

    Returns True if successful, False otherwise.
    """
    data = json.dumps(payload).encode('utf-8')

    req = urllib.request.Request(
        BUBBLE_WEBHOOK_URL,
        data=data,
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'SES-Event-Handler/1.0'
        },
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=WEBHOOK_TIMEOUT) as response:
            status = response.status
            logger.info(f"Forwarded {payload['event_type']} event to Bubble: HTTP {status}")
            return True

    except urllib.error.HTTPError as e:
        logger.error(f"Bubble webhook HTTP error {e.code}: {e.reason}")
        # Read response body for more details
        try:
            error_body = e.read().decode('utf-8')
            logger.error(f"Bubble error response: {error_body[:500]}")
        except Exception:
            pass
        return False

    except urllib.error.URLError as e:
        logger.error(f"Bubble webhook URL error: {str(e)}")
        return False

    except Exception as e:
        logger.error(f"Failed to forward to Bubble: {str(e)}")
        return False
