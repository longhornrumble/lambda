"""
Analytics Event Processor Lambda

Processes analytics events from SQS queue and stores them in DynamoDB.

Architecture:
- Triggered by SQS: picasso-analytics-events
- Writes to DynamoDB: picasso-session-events
- Generates session summaries on WIDGET_CLOSED

Event Schema (v1.0.0):
{
    "schema_version": "1.0.0",
    "session_id": "sess_abc123_xyz789",
    "tenant_id": "fo85e6a06dcdf4",
    "timestamp": "2025-12-19T06:00:00.000Z",
    "step_number": 1,
    "event": {
        "type": "ACTION_CHIP_CLICKED",
        "payload": {...}
    },
    "ga_client_id": "123456789.1234567890" (optional)
}
"""

import json
import os
import logging
import time
from datetime import datetime, timedelta
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
EVENTS_TABLE = os.environ.get('EVENTS_TABLE', 'picasso-session-events')
SUMMARIES_TABLE = os.environ.get('SUMMARIES_TABLE', 'picasso-session-summaries')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
events_table = dynamodb.Table(EVENTS_TABLE)
summaries_table = dynamodb.Table(SUMMARIES_TABLE)

# Schema version we support
SUPPORTED_SCHEMA_VERSIONS = ['1.0.0', '1.0']

# Event types that indicate session end
SESSION_END_EVENTS = ['WIDGET_CLOSED', 'SESSION_ENDED']

# TTL durations (in seconds)
EVENTS_TTL_DAYS = 7
SUMMARIES_TTL_DAYS = 90


def lambda_handler(event, context):
    """
    Main handler for SQS-triggered Lambda.
    Processes batch of analytics events from queue.
    """
    logger.info(f"Processing {len(event.get('Records', []))} records")

    processed_count = 0
    error_count = 0

    for record in event.get('Records', []):
        try:
            # Parse SQS message body
            body = json.loads(record['body'])

            # Handle both single events and batched events
            if body.get('batch'):
                events = body.get('events', [])
                for evt in events:
                    process_single_event(evt)
                    processed_count += 1
            else:
                process_single_event(body)
                processed_count += 1

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in SQS message: {e}")
            error_count += 1
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            error_count += 1
            # Re-raise to trigger DLQ after retries
            raise

    logger.info(f"Processed: {processed_count}, Errors: {error_count}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'errors': error_count
        })
    }


def process_single_event(event_data):
    """
    Process a single analytics event.

    Steps:
    1. Validate schema version
    2. Enrich with server timestamp
    3. Write to DynamoDB
    4. If session end, generate summary
    """
    # Validate schema version
    schema_version = event_data.get('schema_version', '1.0')
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logger.warning(f"Unknown schema version: {schema_version}, processing anyway")

    # Extract required fields
    session_id = event_data.get('session_id')
    tenant_id = event_data.get('tenant_id')
    step_number = event_data.get('step_number', 0)
    client_timestamp = event_data.get('timestamp')
    event_info = event_data.get('event', {})
    event_type = event_info.get('type', 'UNKNOWN')
    payload = event_info.get('payload', {})

    if not session_id:
        logger.error("Missing session_id, skipping event")
        return

    # Generate server timestamp
    server_timestamp = datetime.utcnow().isoformat() + 'Z'
    timestamp_ms = int(time.time() * 1000)

    # Calculate TTL (7 days for events)
    ttl = int(time.time()) + (EVENTS_TTL_DAYS * 24 * 60 * 60)

    # Build DynamoDB item
    item = {
        'PK': f'SESSION#{session_id}',
        'SK': f'STEP#{step_number:06d}#{timestamp_ms}',
        'session_id': session_id,
        'tenant_id': tenant_id or 'unknown',
        'step_number': step_number,
        'event_type': event_type,
        'payload': convert_floats_to_decimal(payload),
        'client_timestamp': client_timestamp,
        'server_timestamp': server_timestamp,
        'schema_version': schema_version,
        'ttl': ttl
    }

    # Add optional fields
    if event_data.get('ga_client_id'):
        item['ga_client_id'] = event_data['ga_client_id']

    if event_data.get('attribution'):
        item['attribution'] = convert_floats_to_decimal(event_data['attribution'])

    # Write to DynamoDB
    try:
        events_table.put_item(Item=item)
        logger.info(f"Stored event: {session_id}/{event_type}/step_{step_number}")
    except ClientError as e:
        logger.error(f"DynamoDB put_item error: {e}")
        raise

    # Check if this is a session end event
    if event_type in SESSION_END_EVENTS:
        generate_session_summary(session_id, tenant_id)


def generate_session_summary(session_id, tenant_id):
    """
    Generate a session summary when the session ends.

    Queries all events for the session and computes:
    - Duration
    - Message count
    - Outcome (form_completed, left_satisfied, abandoned, etc.)
    - Topics visited (branch_ids)
    """
    logger.info(f"Generating summary for session: {session_id}")

    try:
        # Query all events for this session
        response = events_table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'SESSION#{session_id}'},
            ScanIndexForward=True  # Oldest first
        )

        events = response.get('Items', [])

        if not events:
            logger.warning(f"No events found for session: {session_id}")
            return

        # Compute summary metrics
        first_event = events[0]
        last_event = events[-1]

        # Parse timestamps
        start_time = parse_timestamp(first_event.get('client_timestamp') or first_event.get('server_timestamp'))
        end_time = parse_timestamp(last_event.get('client_timestamp') or last_event.get('server_timestamp'))

        # Calculate duration
        duration_seconds = 0
        if start_time and end_time:
            duration_seconds = int((end_time - start_time).total_seconds())

        # Count messages and determine outcome
        message_count = 0
        form_started = False
        form_completed = False
        topics = set()
        last_form_id = None

        for evt in events:
            event_type = evt.get('event_type', '')
            payload = evt.get('payload', {})

            if event_type == 'MESSAGE_SENT':
                message_count += 1
            elif event_type == 'MESSAGE_RECEIVED':
                message_count += 1
            elif event_type == 'ACTION_CHIP_CLICKED':
                topic = payload.get('target_branch') or payload.get('chip_id')
                if topic:
                    topics.add(topic)
            elif event_type == 'FORM_STARTED':
                form_started = True
                last_form_id = payload.get('form_id')
            elif event_type == 'FORM_COMPLETED':
                form_completed = True

        # Determine outcome
        outcome = 'abandoned'
        if form_completed:
            outcome = 'form_completed'
        elif message_count > 2:
            outcome = 'engaged'
        elif message_count > 0:
            outcome = 'minimal_engagement'

        # Calculate TTL (90 days for summaries)
        ttl = int(time.time()) + (SUMMARIES_TTL_DAYS * 24 * 60 * 60)

        # Build summary item
        summary = {
            'PK': f'TENANT#{tenant_id}',
            'SK': f'SESSION#{session_id}',
            'session_id': session_id,
            'tenant_id': tenant_id,
            'started_at': first_event.get('client_timestamp') or first_event.get('server_timestamp'),
            'ended_at': last_event.get('client_timestamp') or last_event.get('server_timestamp'),
            'duration_seconds': duration_seconds,
            'message_count': message_count,
            'event_count': len(events),
            'outcome': outcome,
            'topics': list(topics),
            'form_started': form_started,
            'form_completed': form_completed,
            'last_form_id': last_form_id,
            'ttl': ttl
        }

        # Add attribution from first event if available
        if first_event.get('attribution'):
            summary['attribution'] = first_event['attribution']
        if first_event.get('ga_client_id'):
            summary['ga_client_id'] = first_event['ga_client_id']

        # Write summary to DynamoDB
        summaries_table.put_item(Item=summary)
        logger.info(f"Created summary for session {session_id}: {outcome}, {message_count} messages, {duration_seconds}s")

    except ClientError as e:
        logger.error(f"Error generating session summary: {e}")
        raise


def parse_timestamp(ts_string):
    """Parse ISO 8601 timestamp string to datetime."""
    if not ts_string:
        return None
    try:
        # Handle various ISO formats
        ts_string = ts_string.replace('Z', '+00:00')
        if '.' in ts_string:
            return datetime.fromisoformat(ts_string.split('+')[0])
        return datetime.fromisoformat(ts_string.split('+')[0])
    except ValueError:
        return None


def convert_floats_to_decimal(obj):
    """
    Convert floats to Decimal for DynamoDB compatibility.
    DynamoDB doesn't support float type, only Decimal.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(v) for v in obj]
    return obj


# Direct API handler (for non-SQS invocations)
def api_handler(event, context):
    """
    Handle direct API Gateway invocations.
    Accepts POST with event data and writes directly to DynamoDB.

    This is used when:
    - Full-page mode sends events directly
    - Testing/debugging
    """
    try:
        # Parse request body
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)

        # Handle batch or single event
        if body.get('batch'):
            events = body.get('events', [])
            for evt in events:
                process_single_event(evt)
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'status': 'success',
                    'processed': len(events)
                })
            }
        else:
            process_single_event(body)
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'status': 'success',
                    'processed': 1
                })
            }

    except Exception as e:
        logger.error(f"API handler error: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }
