"""
Analytics Event Processor Lambda

Processes analytics events from SQS queue and stores them in S3 for Athena queries.

Architecture:
- Triggered by SQS: picasso-analytics-events
- Decodes tenant_hash → tenant_id via S3 mappings
- Writes to S3: s3://{bucket}/analytics/tenant_id={}/year={}/month={}/day={}/
- Athena queries S3 for dashboard analytics

Event Schema (v1.0.0):
{
    "schema_version": "1.0.0",
    "session_id": "sess_abc123_xyz789",
    "tenant_id": "fo85e6a06dcdf4",  // Actually tenant_hash from frontend
    "timestamp": "2025-12-19T06:00:00.000Z",
    "step_number": 1,
    "event": {
        "type": "ACTION_CHIP_CLICKED",
        "payload": {...}
    },
    "ga_client_id": "123456789.1234567890" (optional)
}

S3 Partitioning:
- Partition by tenant_id (decoded from hash), year, month, day
- JSON format (Athena reads directly, no Glue needed)

Tenant Hash → ID Mapping:
- Mappings stored in s3://myrecruiter-picasso/mappings/{tenant_hash}.json
- Cached in Lambda memory for duration of invocation
"""

import json
import os
import logging
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
ANALYTICS_BUCKET = os.environ.get('ANALYTICS_BUCKET', 'picasso-analytics')
MAPPINGS_BUCKET = os.environ.get('MAPPINGS_BUCKET', 'myrecruiter-picasso')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# DynamoDB Session Tables (Phase 1 - User Journey Analytics)
SESSION_EVENTS_TABLE = os.environ.get('SESSION_EVENTS_TABLE', 'picasso-session-events')
SESSION_SUMMARIES_TABLE = os.environ.get('SESSION_SUMMARIES_TABLE', 'picasso-session-summaries')
DYNAMODB_WRITE_ENABLED = os.environ.get('DYNAMODB_WRITE_ENABLED', 'false').lower() == 'true'

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# Schema version we support
SUPPORTED_SCHEMA_VERSIONS = ['1.0.0', '1.0']

# In-memory cache for tenant mappings (persists across warm Lambda invocations)
_tenant_mapping_cache = {}


def get_tenant_mapping(tenant_hash):
    """
    Look up tenant_id from tenant_hash using S3 mappings.
    Caches results in memory for Lambda lifetime.

    Returns dict with tenant_id, tenant_hash, host, etc.
    Returns None if mapping not found.
    """
    global _tenant_mapping_cache

    # Check cache first
    if tenant_hash in _tenant_mapping_cache:
        return _tenant_mapping_cache[tenant_hash]

    # Fetch from S3
    try:
        response = s3.get_object(
            Bucket=MAPPINGS_BUCKET,
            Key=f'mappings/{tenant_hash}.json'
        )
        mapping = json.loads(response['Body'].read().decode('utf-8'))

        # Cache it
        _tenant_mapping_cache[tenant_hash] = mapping
        logger.info(f"Loaded mapping: {tenant_hash} → {mapping.get('tenant_id')}")

        return mapping

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f"No mapping found for tenant_hash: {tenant_hash}")
            # Cache the miss to avoid repeated lookups
            _tenant_mapping_cache[tenant_hash] = None
            return None
        else:
            logger.error(f"Error fetching mapping for {tenant_hash}: {e}")
            raise


def lambda_handler(event, context):
    """
    Main handler for SQS-triggered Lambda.
    Processes batch of analytics events from queue.

    Uses partial batch failure reporting - only failed messages are retried,
    not the entire batch. This prevents duplicate processing of successful records.
    """
    records = event.get('Records', [])
    logger.info(f"Processing {len(records)} records")

    processed_count = 0
    error_count = 0

    # Track failed message IDs for partial batch failure reporting
    failed_message_ids = []

    # Collect all events for batch write
    events_to_write = []

    for record in records:
        message_id = record.get('messageId', 'unknown')
        try:
            # Parse SQS message body
            body = json.loads(record['body'])

            # Handle both single events and batched events
            if body.get('batch'):
                events = body.get('events', [])
                for evt in events:
                    enriched = enrich_event(evt)
                    if enriched:
                        events_to_write.append(enriched)
                        processed_count += 1
            else:
                enriched = enrich_event(body)
                if enriched:
                    events_to_write.append(enriched)
                    processed_count += 1

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in SQS message {message_id}: {e}")
            error_count += 1
            failed_message_ids.append(message_id)
        except Exception as e:
            logger.error(f"Error processing record {message_id}: {e}")
            error_count += 1
            failed_message_ids.append(message_id)
            # Continue processing remaining records instead of raising

    # Write all successfully parsed events to S3 (REQUIRED - source of truth)
    if events_to_write:
        try:
            write_events_to_s3(events_to_write)
        except Exception as e:
            # If S3 write fails, all records in this batch need retry
            logger.error(f"S3 write failed, marking all records as failed: {e}")
            failed_message_ids = [r.get('messageId', 'unknown') for r in records]

        # DynamoDB writes (OPTIONAL - only if S3 succeeded and enabled)
        # Sequential execution: S3 must succeed before DynamoDB attempt
        # Failures logged but don't raise - S3 already has the data
        if DYNAMODB_WRITE_ENABLED and not failed_message_ids:
            try:
                write_events_to_dynamodb(events_to_write)
            except Exception as e:
                # Log but don't fail - S3 already has the data
                logger.warning(f"DynamoDB write failed (non-fatal): {e}")

    logger.info(f"Processed: {processed_count}, Errors: {error_count}, Failed IDs: {len(failed_message_ids)}")

    # Return partial batch failure response
    # SQS will only retry the failed message IDs, not the entire batch
    return {
        'batchItemFailures': [
            {'itemIdentifier': msg_id} for msg_id in failed_message_ids
        ]
    }


def enrich_event(event_data):
    """
    Enrich event with server-side metadata.
    Decodes tenant_hash to tenant_id when needed.
    Returns None if event is invalid.

    Supports two sources:
    1. Frontend events: tenant_id field contains hash, needs lookup
    2. Server events (Bedrock Lambda): tenant_id already resolved, tenant_hash separate
    """
    # Validate schema version
    schema_version = event_data.get('schema_version', '1.0')
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logger.warning(f"Unknown schema version: {schema_version}, processing anyway")

    # Extract required fields
    session_id = event_data.get('session_id')

    # Handle event type - support both flat (server) and nested (frontend) formats
    if event_data.get('event_type'):
        # Server-side format: event_type at top level
        event_type = event_data.get('event_type')
        event_payload = event_data.get('event_payload', {})
        client_timestamp = event_data.get('client_timestamp') or event_data.get('timestamp')
    else:
        # Frontend format: nested event object
        event_info = event_data.get('event', {})
        event_type = event_info.get('type', 'UNKNOWN')
        event_payload = event_info.get('payload', {})
        client_timestamp = event_data.get('timestamp')

    # Handle tenant identification
    # Server-side events have both tenant_id (resolved) and tenant_hash (original)
    # Frontend events have hash in tenant_id field
    if event_data.get('tenant_hash') and event_data.get('tenant_id'):
        # Server-side: already resolved
        tenant_hash = event_data.get('tenant_hash')
        tenant_id = event_data.get('tenant_id')
    else:
        # Frontend: tenant_id contains hash, needs lookup
        tenant_hash = event_data.get('tenant_id')
        tenant_id = None

    if not session_id:
        logger.error("Missing session_id, skipping event")
        return None

    if not tenant_hash and not tenant_id:
        logger.error("Missing tenant identification, skipping event")
        return None

    # Only do lookup if tenant_id not already resolved
    if not tenant_id:
        mapping = get_tenant_mapping(tenant_hash)
        if mapping:
            tenant_id = mapping.get('tenant_id')
        if not tenant_id:
            logger.error(f"No mapping found for tenant_hash: {tenant_hash}")
            return None

    # Generate server timestamp
    server_timestamp = datetime.utcnow().isoformat() + 'Z'

    # Build enriched event (flat structure for Athena)
    enriched = {
        'event_id': str(uuid.uuid4()),
        'schema_version': schema_version,
        'session_id': session_id,
        'tenant_id': tenant_id,        # Decoded tenant ID (e.g., "FOS402334")
        'tenant_hash': tenant_hash,    # Original hash (e.g., "fo85e6a06dcdf4")
        'step_number': event_data.get('step_number', 0),
        'event_type': event_type,
        'event_payload': event_payload,
        'client_timestamp': client_timestamp,
        'server_timestamp': server_timestamp,
        'environment': ENVIRONMENT
    }

    # Add optional fields
    if event_data.get('ga_client_id'):
        enriched['ga_client_id'] = event_data['ga_client_id']

    if event_data.get('attribution'):
        enriched['attribution'] = event_data['attribution']

    return enriched


def write_events_to_s3(events):
    """
    Write events to S3 with partitioned paths for Athena.

    Path structure:
    s3://{bucket}/analytics/tenant_id={tenant}/year={Y}/month={M}/day={D}/{batch_id}.json

    Each file contains newline-delimited JSON (NDJSON) for efficient Athena parsing.
    """
    # Group events by partition (tenant_id + date)
    partitions = {}

    for event in events:
        tenant_id = event['tenant_id']

        # Parse timestamp for partitioning
        ts = event.get('client_timestamp') or event.get('server_timestamp')
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except ValueError:
                dt = datetime.utcnow()
        else:
            dt = datetime.utcnow()

        # Create partition key
        partition_key = (
            tenant_id,
            dt.year,
            dt.month,
            dt.day
        )

        if partition_key not in partitions:
            partitions[partition_key] = []
        partitions[partition_key].append(event)

    # Write each partition
    for (tenant_id, year, month, day), partition_events in partitions.items():
        # Generate unique batch ID
        batch_id = f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        # Build S3 key with Hive-style partitioning
        s3_key = (
            f"analytics/"
            f"tenant_id={tenant_id}/"
            f"year={year}/"
            f"month={month:02d}/"
            f"day={day:02d}/"
            f"{batch_id}.json"
        )

        # Create NDJSON content (newline-delimited JSON)
        ndjson_content = '\n'.join(json.dumps(e) for e in partition_events)

        try:
            s3.put_object(
                Bucket=ANALYTICS_BUCKET,
                Key=s3_key,
                Body=ndjson_content.encode('utf-8'),
                ContentType='application/x-ndjson'
            )
            logger.info(f"✅ Wrote {len(partition_events)} events to s3://{ANALYTICS_BUCKET}/{s3_key}")
        except ClientError as e:
            logger.error(f"S3 put_object error: {e}")
            raise


# Direct API handler (for non-SQS invocations)
def api_handler(event, context):
    """
    Handle direct API Gateway invocations.
    Accepts POST with event data and writes directly to S3.

    This is used when:
    - Full-page mode sends events directly
    - Testing/debugging
    """
    try:
        # Parse request body
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)

        events_to_write = []

        # Handle batch or single event
        if body.get('batch'):
            events = body.get('events', [])
            for evt in events:
                enriched = enrich_event(evt)
                if enriched:
                    events_to_write.append(enriched)
        else:
            enriched = enrich_event(body)
            if enriched:
                events_to_write.append(enriched)

        # Write to S3
        if events_to_write:
            write_events_to_s3(events_to_write)

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'success',
                'processed': len(events_to_write)
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


# ============================================================================
# DYNAMODB SESSION TABLE FUNCTIONS (User Journey Analytics)
# ============================================================================

def calculate_ttl(days=90):
    """Calculate TTL timestamp for DynamoDB (90 days from now)."""
    return int((datetime.utcnow() + timedelta(days=days)).timestamp())


def write_session_event(event):
    """
    Write a single event to picasso-session-events table.

    Key Structure:
    - PK: SESSION#{session_id}
    - SK: STEP#{step_number:03d} (zero-padded for proper sort order)

    This enables efficient session reconstruction by querying all steps for a session.
    """
    session_id = event.get('session_id')
    step_number = event.get('step_number', 0)
    tenant_hash = event.get('tenant_hash')
    timestamp = event.get('client_timestamp') or event.get('server_timestamp')

    if not session_id or not tenant_hash:
        logger.warning(f"Skipping session event write: missing session_id or tenant_hash")
        return False

    item = {
        'pk': {'S': f"SESSION#{session_id}"},
        'sk': {'S': f"STEP#{step_number:03d}"},
        'session_id': {'S': session_id},
        'tenant_hash': {'S': tenant_hash},
        'tenant_id': {'S': event.get('tenant_id', '')},
        'step_number': {'N': str(step_number)},
        'event_type': {'S': event.get('event_type', 'UNKNOWN')},
        'timestamp': {'S': timestamp or datetime.utcnow().isoformat() + 'Z'},
        'ttl': {'N': str(calculate_ttl())}
    }

    # Add event payload if present (as JSON string)
    if event.get('event_payload'):
        item['event_payload'] = {'S': json.dumps(event['event_payload'])}

    # Add optional fields
    if event.get('ga_client_id'):
        item['ga_client_id'] = {'S': event['ga_client_id']}

    try:
        dynamodb.put_item(
            TableName=SESSION_EVENTS_TABLE,
            Item=item
        )
        return True
    except ClientError as e:
        logger.error(f"Error writing session event: {e}")
        return False


def update_session_summary(event):
    """
    Atomic update to picasso-session-summaries table.

    Key Structure:
    - PK: TENANT#{tenant_hash}
    - SK: SESSION#{started_at}#{session_id}

    Uses atomic UPDATE operations with ADD and if_not_exists to prevent race conditions
    when multiple events for the same session arrive in parallel.
    """
    session_id = event.get('session_id')
    tenant_hash = event.get('tenant_hash')
    timestamp = event.get('client_timestamp') or event.get('server_timestamp') or datetime.utcnow().isoformat() + 'Z'
    event_type = event.get('event_type', 'UNKNOWN')

    if not session_id or not tenant_hash:
        logger.warning(f"Skipping session summary update: missing session_id or tenant_hash")
        return False

    # Build the update expression based on event type
    # Note: 'ttl' is a reserved keyword in DynamoDB, must use expression attribute name
    update_parts = [
        "SET ended_at = :ended_at",
        "session_id = if_not_exists(session_id, :session_id)",
        "tenant_id = if_not_exists(tenant_id, :tenant_id)",
        "started_at = if_not_exists(started_at, :started_at)",
        "#ttl = :ttl"
    ]

    expression_values = {
        ':ended_at': {'S': timestamp},
        ':session_id': {'S': session_id},
        ':tenant_id': {'S': event.get('tenant_id', '')},
        ':started_at': {'S': timestamp},
        ':ttl': {'N': str(calculate_ttl())},
        ':one': {'N': '1'},
        ':zero': {'N': '0'}
    }

    expression_names = {'#ttl': 'ttl'}

    # Increment message counts based on event type
    if event_type == 'MESSAGE_SENT':
        update_parts.append("user_message_count = if_not_exists(user_message_count, :zero) + :one")
        update_parts.append("message_count = if_not_exists(message_count, :zero) + :one")

        # Capture first question if this is a user message
        payload = event.get('event_payload', {})
        if payload.get('content_preview'):
            update_parts.append("first_question = if_not_exists(first_question, :first_question)")
            expression_values[':first_question'] = {'S': payload['content_preview'][:200]}

    elif event_type == 'MESSAGE_RECEIVED':
        update_parts.append("bot_message_count = if_not_exists(bot_message_count, :zero) + :one")
        update_parts.append("message_count = if_not_exists(message_count, :zero) + :one")

        # Use response_time_ms from event payload (sent by frontend)
        # This is the time from user message to first character displayed
        payload = event.get('event_payload', {})
        response_time_ms = payload.get('response_time_ms', 0)
        if response_time_ms and 0 < response_time_ms < 60000:  # Sanity check: 0-60 seconds
            # Add to running totals for averaging
            update_parts.append("total_response_time_ms = if_not_exists(total_response_time_ms, :zero) + :response_time")
            update_parts.append("response_count = if_not_exists(response_count, :zero) + :one")
            expression_values[':response_time'] = {'N': str(int(response_time_ms))}
            logger.info(f"Response time from payload: {response_time_ms}ms for session {session_id}")

    elif event_type == 'FORM_COMPLETED':
        # Form completion is a strong outcome - always set
        update_parts.append("#outcome = :outcome")
        expression_names['#outcome'] = 'outcome'
        expression_values[':outcome'] = {'S': 'form_completed'}

        # Capture form_id
        payload = event.get('event_payload', {})
        if payload.get('form_id'):
            update_parts.append("form_id = :form_id")
            expression_values[':form_id'] = {'S': payload['form_id']}

    elif event_type == 'LINK_CLICKED':
        # Link click is a weaker outcome - only set if not already set
        update_parts.append("#outcome = if_not_exists(#outcome, :outcome)")
        expression_names['#outcome'] = 'outcome'
        expression_values[':outcome'] = {'S': 'link_clicked'}

    elif event_type == 'CTA_CLICKED':
        # CTA click is also a meaningful outcome
        update_parts.append("#outcome = if_not_exists(#outcome, :outcome)")
        expression_names['#outcome'] = 'outcome'
        expression_values[':outcome'] = {'S': 'cta_clicked'}

    # Build final update expression
    update_expression = ', '.join(update_parts)

    try:
        update_params = {
            'TableName': SESSION_SUMMARIES_TABLE,
            'Key': {
                'pk': {'S': f"TENANT#{tenant_hash}"},
                'sk': {'S': f"SESSION#{session_id}"}  # Use session_id only - not timestamp
            },
            'UpdateExpression': update_expression,
            'ExpressionAttributeValues': expression_values
        }

        if expression_names:
            update_params['ExpressionAttributeNames'] = expression_names

        dynamodb.update_item(**update_params)
        return True

    except ClientError as e:
        logger.error(f"Error updating session summary: {e}")
        return False


def write_events_to_dynamodb(events):
    """
    Orchestrate writes to both session tables.

    This is called AFTER S3 write succeeds (sequential, not parallel).
    Failures are logged but don't raise - S3 already has the data.
    """
    if not events:
        return

    events_written = 0
    summaries_updated = 0

    for event in events:
        # Write to picasso-session-events
        if write_session_event(event):
            events_written += 1

        # Update picasso-session-summaries (atomic)
        if update_session_summary(event):
            summaries_updated += 1

    logger.info(f"DynamoDB writes: {events_written}/{len(events)} events, {summaries_updated}/{len(events)} summaries")
