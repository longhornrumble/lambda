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
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
ANALYTICS_BUCKET = os.environ.get('ANALYTICS_BUCKET', 'picasso-analytics')
MAPPINGS_BUCKET = os.environ.get('MAPPINGS_BUCKET', 'myrecruiter-picasso')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# Initialize AWS clients
s3 = boto3.client('s3')

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

    # Write all successfully parsed events to S3
    if events_to_write:
        try:
            write_events_to_s3(events_to_write)
        except Exception as e:
            # If S3 write fails, all records in this batch need retry
            logger.error(f"S3 write failed, marking all records as failed: {e}")
            failed_message_ids = [r.get('messageId', 'unknown') for r in records]

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
    Decodes tenant_hash to tenant_id.
    Returns None if event is invalid.
    """
    # Validate schema version
    schema_version = event_data.get('schema_version', '1.0')
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logger.warning(f"Unknown schema version: {schema_version}, processing anyway")

    # Extract required fields
    session_id = event_data.get('session_id')
    tenant_hash = event_data.get('tenant_id')  # Frontend sends hash as "tenant_id"
    client_timestamp = event_data.get('timestamp')
    event_info = event_data.get('event', {})
    event_type = event_info.get('type', 'UNKNOWN')

    if not session_id:
        logger.error("Missing session_id, skipping event")
        return None

    if not tenant_hash:
        logger.error("Missing tenant_id (hash), skipping event")
        return None

    # Decode tenant_hash → tenant_id
    mapping = get_tenant_mapping(tenant_hash)
    if mapping:
        tenant_id = mapping.get('tenant_id', tenant_hash)
    else:
        # Fallback: use hash as ID if mapping not found
        tenant_id = tenant_hash
        logger.warning(f"Using tenant_hash as tenant_id (no mapping): {tenant_hash}")

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
        'event_payload': event_info.get('payload', {}),
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
