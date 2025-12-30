"""
SES Event Handler Lambda Function

This Lambda receives SES event notifications (bounce, complaint, delivery, etc.)
from SNS and forwards them to Bubble via webhook.
"""

import json
import logging
import os
import urllib.request
import urllib.error

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
BUBBLE_WEBHOOK_URL = os.environ.get(
    'BUBBLE_WEBHOOK_URL',
    'https://hrfx.bubbleapps.io/api/1.1/wf/ses_event'
)
WEBHOOK_TIMEOUT = int(os.environ.get('WEBHOOK_TIMEOUT', '10'))


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
            tags = {}
            for tag in mail.get('tags', []):
                tag_name = tag.get('name')
                tag_values = tag.get('value', [])
                if tag_name and tag_values:
                    # Tags can have multiple values, but usually there's just one
                    tags[tag_name] = tag_values[0] if len(tag_values) == 1 else tag_values
            payload['tags'] = tags

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

            # Forward to Bubble
            success = forward_to_bubble(payload)

            if success:
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
