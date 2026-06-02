"""
Analytics Event Processor Lambda

Processes analytics events from an SQS queue and stores them in DynamoDB
(picasso-session-events), the sole durable store.
CI: pr-checks.yml runs test_session_tables.py via the python-tests matrix
(see issue #42 / PR #43 for the wiring).

Architecture:
- Triggered by SQS: picasso-analytics-events
- Decodes tenant_hash → tenant_id via S3 mappings
- Writes per-event records to DynamoDB picasso-session-events (sole durable store)
- Dashboard reads DynamoDB; a DDB write failure drives SQS partial-batch retry

The orphaned picasso-analytics S3 lake write was removed (data-retention-strategy
§5/§9 — zero consumer: Athena dormant, no Glue, no S3 notifications, dashboard
reads DynamoDB). The per-event DDB record already covers everything used.

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

DynamoDB Key Structure (picasso-session-events):
- PK: SESSION#{session_id}, SK: STEP#{step_number:03d} (zero-padded for sort order)
- 90-day TTL on the `ttl` attribute; re-delivery overwrites the same key (idempotent)

Tenant Hash → ID Mapping:
- Mappings stored in s3://myrecruiter-picasso/mappings/{tenant_hash}.json
- Cached in Lambda memory for duration of invocation
"""

import json
import os
import logging
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
MAPPINGS_BUCKET = os.environ.get('MAPPINGS_BUCKET', 'myrecruiter-picasso')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# DynamoDB Session Tables (Phase 1 - User Journey Analytics)
SESSION_EVENTS_TABLE = os.environ.get('SESSION_EVENTS_TABLE', 'picasso-session-events')
SESSION_SUMMARIES_TABLE = os.environ.get('SESSION_SUMMARIES_TABLE', 'picasso-session-summaries')
# DynamoDB is the sole durable store, so the write is unconditional — there is no
# S3 backstop to fall back to. The former DYNAMODB_WRITE_ENABLED gate was removed
# (defaulting 'false') so the durable write can never be silently disabled by config.

# Initialize AWS clients
# s3 is retained ONLY for tenant_hash → tenant_id mapping lookups (MAPPINGS_BUCKET);
# the analytics-lake write (ANALYTICS_BUCKET) was removed.
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

    for record in records:
        message_id = record.get('messageId', 'unknown')
        try:
            # Parse SQS message body
            body = json.loads(record['body'])

            # Normalize to a list of raw events (batched or single)
            raw_events = body.get('events', []) if body.get('batch') else [body]

            # Enrich + durably persist each event to DynamoDB (the sole source of
            # truth — there is no longer an S3 backstop). A DDB write failure for
            # ANY event in this message marks the whole SQS message for retry.
            # write_session_event puts on a deterministic key
            # (SESSION#{session_id} / STEP#{step:03d}), so re-delivery overwrites
            # identically — retries are idempotent, no duplicates.
            message_failed = False
            for evt in raw_events:
                enriched = enrich_event(evt)
                if enriched is None:
                    # Invalid/unmappable event (missing session_id or no tenant
                    # mapping) — permanently skipped, NOT retried. Retrying cannot
                    # fix malformed input and would loop until the queue's redrive.
                    continue
                if write_session_event(enriched):
                    processed_count += 1
                else:
                    message_failed = True

            if message_failed:
                logger.error(f"DynamoDB write failed for one or more events in message {message_id}; marking for retry")
                error_count += 1
                failed_message_ids.append(message_id)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in SQS message {message_id}: {e}")
            error_count += 1
            failed_message_ids.append(message_id)
        except Exception as e:
            logger.error(f"Error processing record {message_id}: {e}")
            error_count += 1
            failed_message_ids.append(message_id)
            # Continue processing remaining records instead of raising

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

    # Build enriched event (flat structure)
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

        # Write to DynamoDB (sole durable store)
        if events_to_write:
            write_events_to_dynamodb(events_to_write)

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


def write_events_to_dynamodb(events):
    """
    Orchestrate writes to both session tables.

    This is called AFTER S3 write succeeds (sequential, not parallel).
    Failures are logged but don't raise - S3 already has the data.
    """
    if not events:
        return

    events_written = 0

    # The update_session_summary() function was removed entirely 2026-05-11 (phase audit B7).
    # It used invalid `if_not_exists(X, :zero) + :one` syntax (DDB requires `ADD X :one`)
    # so every call failed with ValidationException; the call site was removed in PR #57
    # and the dead definition was deleted in phase-1.5 hardening to eliminate the
    # revert-vector that would re-enable unredacted-PII writes.
    # Session summaries are now written exclusively by the chat-path Lambdas
    # (Master_Function_Staging/analytics_writer.py + Bedrock_Streaming_Handler_Staging/
    # analytics_writer.js) with correct idempotency via last_request_id_<event>
    # ConditionExpression. The SQS path remains the canonical writer for
    # picasso-session-events (per-event records).
    for event in events:
        if write_session_event(event):
            events_written += 1

    logger.info(f"DynamoDB writes: {events_written}/{len(events)} events to picasso-session-events")
