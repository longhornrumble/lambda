import json
import os
import logging
import boto3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from decimal import Decimal
from collections import defaultdict
import hashlib
import gzip
try:
    import pytz
except ImportError:
    pytz = None

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
logs_client = boto3.client('logs')
s3 = boto3.client('s3')

# Configuration
ANALYTICS_TABLE = os.environ.get('ANALYTICS_TABLE', 'picasso-analytics-daily')
CONFIG_BUCKET = os.environ.get('CONFIG_BUCKET', 'myrecruiter-picasso')
ARCHIVE_BUCKET = os.environ.get('ARCHIVE_BUCKET', 'picasso-analytics-archive')
LOG_GROUP_STREAMING = '/aws/lambda/Bedrock_Streaming_Handler'
LOG_GROUP_MASTER = '/aws/lambda/Master_Function'
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'production')
MAX_QUERY_RESULTS = 1000

def lambda_handler(event, context):
    """
    Daily aggregation of CloudWatch logs to DynamoDB.
    Can process specific date via event['date'] or defaults to yesterday.
    """
    # Determine date to process
    if 'date' in event:
        process_date = event['date']
    else:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        process_date = yesterday.strftime('%Y-%m-%d')
    
    logger.info(f"Processing analytics for date: {process_date}")
    
    # Get all tenant mappings from S3
    tenants = get_all_tenant_mappings()
    
    # Track processing results
    results = {
        'date': process_date,
        'tenants_processed': 0,
        'tenants_failed': [],
        'total_conversations': 0
    }
    
    # Process each tenant
    for tenant_mapping in tenants:
        try:
            tenant_id = tenant_mapping.get('tenant_id')
            tenant_hash = tenant_mapping.get('tenant_hash')
            
            if not tenant_id or not tenant_hash:
                logger.warning(f"Invalid mapping, skipping: {tenant_mapping}")
                continue
            
            logger.info(f"Processing tenant: {tenant_id} with hash: {tenant_hash}")
            
            # Parse date range for the day
            start_time = datetime.strptime(process_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_time = start_time + timedelta(days=1)
            
            # Get QA_COMPLETE logs using the same logic as Analytics_Function
            qa_logs = get_qa_complete_logs(tenant_hash, start_time, end_time)
            
            # Process the logs into metrics (same as Analytics_Function)
            metrics = process_qa_logs(qa_logs, tenant_id)
            
            # Store in DynamoDB if there's data
            if metrics['conversation_count'] > 0:
                store_metrics(tenant_id, tenant_hash, process_date, metrics)
                results['total_conversations'] += metrics['conversation_count']
                logger.info(f"Stored {metrics['conversation_count']} conversations for {tenant_id}")
            
            results['tenants_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing tenant {tenant_id}: {str(e)}")
            results['tenants_failed'].append(tenant_id)
    
    logger.info(f"Aggregation complete: {json.dumps(results)}")
    return results

def get_all_tenant_mappings():
    """Get all tenant mappings from S3 mappings directory."""
    tenants = []
    
    try:
        # List all mapping files
        response = s3.list_objects_v2(
            Bucket=CONFIG_BUCKET,
            Prefix='mappings/'
        )
        
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.json'):
                # Get tenant mapping
                mapping_response = s3.get_object(
                    Bucket=CONFIG_BUCKET,
                    Key=obj['Key']
                )
                mapping = json.loads(mapping_response['Body'].read())
                if 'tenant_hash' in mapping and 'tenant_id' in mapping:
                    tenants.append(mapping)
                    logger.info(f"Loaded mapping: {mapping['tenant_id']} -> {mapping['tenant_hash']}")
    
    except Exception as e:
        logger.error(f"Error loading tenant mappings: {str(e)}")
    
    return tenants

def get_qa_complete_logs(tenant_hash: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Query CloudWatch Insights for QA_COMPLETE logs (copied from Analytics_Function).
    """
    logger.info(f"Querying CloudWatch for tenant_hash: {tenant_hash[:8]}... from {start_time.isoformat()} to {end_time.isoformat()}")
    
    query = f"""
    fields @timestamp, @message
    | filter @message like /QA_COMPLETE/
    | filter @message like /{tenant_hash}/
    | sort @timestamp desc
    | limit {MAX_QUERY_RESULTS}
    """
    
    all_results = []
    
    for log_group in [LOG_GROUP_STREAMING, LOG_GROUP_MASTER]:
        try:
            logger.info(f"Querying log group: {log_group}")
            
            response = logs_client.start_query(
                logGroupName=log_group,
                startTime=int(start_time.timestamp()),
                endTime=int(end_time.timestamp()),
                queryString=query
            )
            
            query_id = response['queryId']
            
            # Wait for query completion
            status = 'Running'
            max_wait = 30
            wait_time = 0
            
            while status in ['Running', 'Scheduled'] and wait_time < max_wait:
                response = logs_client.get_query_results(queryId=query_id)
                status = response['status']
                
                if status == 'Complete':
                    # Parse the results
                    for result in response['results']:
                        parsed = _parse_log_entry(result)
                        if parsed:
                            all_results.append(parsed)
                    break
                elif status in ['Failed', 'Cancelled']:
                    logger.error(f"Query failed with status: {status}")
                    break
                
                wait_time += 1
                
        except Exception as e:
            logger.error(f"Error querying {log_group}: {str(e)}")
            continue
    
    logger.info(f"Total QA_COMPLETE logs found: {len(all_results)}")
    return all_results

def _parse_log_entry(log_entry: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    """Parse a CloudWatch Insights log entry (copied from cloudwatch_reader.py)"""
    try:
        message_field = next((field for field in log_entry if field.get('field') == '@message'), None)
        timestamp_field = next((field for field in log_entry if field.get('field') == '@timestamp'), None)
        
        if not message_field:
            return None
        
        message = message_field.get('value', '')
        timestamp = timestamp_field.get('value', '') if timestamp_field else ''
        
        # Handle potential log prefix (timestamp and request ID)
        if '\t' in message:
            parts = message.split('\t')
            if len(parts) > 1:
                message = parts[-1]
        
        # Try to extract JSON from the message
        try:
            log_data = json.loads(message)
        except json.JSONDecodeError:
            # Try to find JSON in the message
            start_idx = message.find('{')
            end_idx = message.rfind('}') + 1
            if start_idx != -1 and end_idx > start_idx:
                try:
                    log_data = json.loads(message[start_idx:end_idx])
                except json.JSONDecodeError:
                    return None
            else:
                return None
        
        # Check if this is a QA_COMPLETE log
        if log_data.get('type') != 'QA_COMPLETE':
            return None
        
        return {
            'timestamp': log_data.get('timestamp', timestamp),
            'tenant_hash': log_data.get('tenant_hash'),
            'tenant_id': log_data.get('tenant_id'),
            'session_id': log_data.get('session_id'),
            'conversation_id': log_data.get('conversation_id'),
            'question': log_data.get('question', ''),
            'answer': log_data.get('answer', ''),
            'metrics': log_data.get('metrics', {})
        }
        
    except Exception as e:
        logger.debug(f"Error parsing log entry: {str(e)}")
        return None

def get_tenant_timezone(tenant_id: str) -> Optional[str]:
    """Get tenant's timezone from config file"""
    try:
        # Try to load config from S3
        config_key = f'tenants/{tenant_id}/{tenant_id}-config.json'
        response = s3.get_object(
            Bucket=CONFIG_BUCKET,
            Key=config_key
        )
        config = json.loads(response['Body'].read())
        timezone = config.get('timezone', None)

        if timezone:
            logger.info(f"Found timezone {timezone} for tenant {tenant_id}")
        else:
            logger.warning(f"No timezone configured for tenant {tenant_id}")

        return timezone
    except Exception as e:
        logger.warning(f"Could not load timezone for tenant {tenant_id}: {str(e)}")
        return None

def process_qa_logs(qa_logs: List[Dict[str, Any]], tenant_id: str = None) -> Dict[str, Any]:
    """Process QA logs into metrics (same logic as Analytics_Function)"""
    metrics = {
        'conversation_count': 0,
        'total_messages': 0,
        'response_times': [],
        'first_token_times': [],
        'total_times': [],
        'questions': defaultdict(int),
        'hourly_distribution': defaultdict(int),
        'daily_distribution': defaultdict(int),
        'conversations': [],
        'after_hours_count': 0,
        'streaming_enabled_count': 0
    }

    # Get tenant's timezone
    tenant_timezone = get_tenant_timezone(tenant_id) if tenant_id else None

    # Track unique sessions for conversation count
    unique_sessions = set()

    for log in qa_logs:
        try:
            if 'question' in log:
                metrics['questions'][log['question']] += 1
                metrics['total_messages'] += 1

                # Track unique session for conversation counting
                session_id = log.get('session_id')
                if session_id:
                    unique_sessions.add(session_id)

                # Extract timestamp for distribution
                if 'timestamp' in log:
                    dt = datetime.fromisoformat(log['timestamp'].replace('Z', '+00:00'))

                    # Convert to tenant's local timezone if available
                    if tenant_timezone and pytz:
                        tz = pytz.timezone(tenant_timezone)
                        local_dt = dt.astimezone(tz)
                    else:
                        # Fallback to UTC if no timezone configured
                        local_dt = dt
                        if tenant_id:
                            logger.warning(f"No timezone for tenant {tenant_id}, using UTC")

                    hour = local_dt.hour
                    day = local_dt.weekday()
                    metrics['hourly_distribution'][hour] += 1
                    metrics['daily_distribution'][day] += 1

                    # Check if after hours (before 9am or after 5pm in LOCAL time)
                    if hour < 9 or hour >= 17:
                        metrics['after_hours_count'] += 1
                
                # Extract metrics if available
                if 'metrics' in log:
                    log_metrics = log['metrics']
                    if 'first_token_ms' in log_metrics:
                        metrics['first_token_times'].append(log_metrics['first_token_ms'])
                    if 'total_time_ms' in log_metrics:
                        metrics['total_times'].append(log_metrics['total_time_ms'])
                    if 'response_time_ms' in log_metrics:
                        metrics['response_times'].append(log_metrics['response_time_ms'])
                
                # Add to conversations list
                # Extract timing metrics from the metrics sub-object
                log_metrics_data = log.get('metrics', {})
                first_token = log_metrics_data.get('first_token_ms', 0)
                total_time = log_metrics_data.get('total_time_ms', 0)
                response_time = log_metrics_data.get('response_time_ms', 0)

                conversation = {
                    'timestamp': log.get('timestamp', ''),
                    'session_id': log.get('session_id', ''),
                    'conversation_id': log.get('conversation_id'),
                    'question': log.get('question', ''),
                    'answer': log.get('answer', ''),
                    'response_time_ms': response_time,  # Keep for compatibility
                    'first_token_ms': first_token,       # Time to first token
                    'total_time_ms': total_time          # Total completion time
                }
                metrics['conversations'].append(conversation)

        except Exception as e:
            logger.warning(f"Error processing log entry: {str(e)}")

    # Set conversation count to number of unique sessions
    metrics['conversation_count'] = len(unique_sessions)
    logger.info(f"Aggregator: Found {len(unique_sessions)} unique sessions, {metrics['total_messages']} total messages")

    # Calculate aggregated values
    if metrics['response_times']:
        metrics['avg_response_time_ms'] = sum(metrics['response_times']) / len(metrics['response_times'])
    else:
        metrics['avg_response_time_ms'] = 0
    
    if metrics['first_token_times']:
        metrics['avg_first_token_ms'] = sum(metrics['first_token_times']) / len(metrics['first_token_times'])
    else:
        metrics['avg_first_token_ms'] = 0
    
    if metrics['total_times']:
        metrics['avg_total_time_ms'] = sum(metrics['total_times']) / len(metrics['total_times'])
    else:
        metrics['avg_total_time_ms'] = 0
    
    # Format top questions
    top_questions = []
    for question, count in sorted(metrics['questions'].items(), key=lambda x: x[1], reverse=True)[:10]:
        top_questions.append({
            'question': question,
            'count': count,
            'percentage': (count / metrics['total_messages'] * 100) if metrics['total_messages'] > 0 else 0
        })
    metrics['top_questions'] = top_questions
    
    return metrics

def convert_floats_to_decimal(obj):
    """Recursively convert floats to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(v) for v in obj]
    return obj

def store_metrics(tenant_id, tenant_hash, process_date, metrics):
    """Store aggregated metrics in DynamoDB."""
    table = dynamodb.Table(ANALYTICS_TABLE)
    
    # Convert all floats to Decimal for DynamoDB
    item = {
        'pk': f"TENANT#{tenant_id}",
        'sk': f"DATE#{process_date}",
        'tenant_id': tenant_id,
        'tenant_hash': tenant_hash,
        'date': process_date,
        'conversation_count': metrics['conversation_count'],
        'total_messages': metrics['total_messages'],
        'avg_response_time_ms': Decimal(str(metrics['avg_response_time_ms'])),
        'avg_first_token_ms': Decimal(str(metrics['avg_first_token_ms'])),
        'avg_total_time_ms': Decimal(str(metrics['avg_total_time_ms'])),
        'top_questions': convert_floats_to_decimal(metrics['top_questions']),
        'hourly_distribution': {str(k): v for k, v in metrics['hourly_distribution'].items()},
        'daily_distribution': {str(k): v for k, v in metrics['daily_distribution'].items()},
        'conversations': convert_floats_to_decimal(metrics['conversations'][:100]),  # Store up to 100 conversations
        'ttl': int((datetime.now(timezone.utc) + timedelta(days=90)).timestamp()),
        'created_at': datetime.now(timezone.utc).isoformat(),
        'environment': ENVIRONMENT
    }
    
    # Path 1: Write to DynamoDB (existing)
    try:
        table.put_item(Item=item)
        logger.info(f"Stored metrics in DynamoDB for {tenant_id} on {process_date}")
    except Exception as e:
        logger.error(f"Error storing metrics in DynamoDB: {str(e)}")
        raise

    # Path 2: Write to S3/Glacier IR (new)
    try:
        # Calculate percentages if not already present
        if 'after_hours_percentage' not in metrics:
            if metrics['total_messages'] > 0:
                metrics['after_hours_percentage'] = (metrics['after_hours_count'] / metrics['total_messages']) * 100
            else:
                metrics['after_hours_percentage'] = 0

        if 'streaming_enabled_percentage' not in metrics:
            if metrics['total_messages'] > 0:
                metrics['streaming_enabled_percentage'] = (metrics['streaming_enabled_count'] / metrics['total_messages']) * 100
            else:
                metrics['streaming_enabled_percentage'] = 0

        # Prepare data for S3 (convert Decimal to float for JSON serialization)
        s3_data = {
            'tenant_id': tenant_id,
            'tenant_hash': tenant_hash,
            'date': process_date,
            'conversation_count': int(metrics['conversation_count']),
            'total_messages': int(metrics['total_messages']),
            'avg_response_time_ms': float(metrics['avg_response_time_ms']),
            'avg_first_token_ms': float(metrics['avg_first_token_ms']),
            'avg_total_time_ms': float(metrics['avg_total_time_ms']),
            'after_hours_percentage': float(metrics['after_hours_percentage']),
            'after_hours_count': int(metrics['after_hours_count']),
            'streaming_enabled_percentage': float(metrics['streaming_enabled_percentage']),
            'streaming_enabled_count': int(metrics['streaming_enabled_count']),
            'top_questions': metrics['top_questions'],  # Already has floats
            'hourly_distribution': dict(metrics['hourly_distribution']),
            'daily_distribution': dict(metrics['daily_distribution']),
            'heat_grid': metrics.get('heat_grid', [0] * 56),
            'conversations': metrics['conversations'][:100],  # Store up to 100 conversations
            'created_at': datetime.now(timezone.utc).isoformat(),
            'environment': ENVIRONMENT
        }

        # Compress the JSON data
        json_str = json.dumps(s3_data, separators=(',', ':'))
        compressed_data = gzip.compress(json_str.encode('utf-8'))

        # Create S3 key with date partitioning for efficient querying
        s3_key = f"daily-aggregates/{process_date[:4]}/{process_date[5:7]}/{process_date}/{tenant_id}.json.gz"

        # Upload to S3 with Glacier Instant Retrieval storage class
        s3.put_object(
            Bucket=ARCHIVE_BUCKET,
            Key=s3_key,
            Body=compressed_data,
            StorageClass='GLACIER_IR',
            ContentType='application/json',
            ContentEncoding='gzip',
            Metadata={
                'tenant_id': tenant_id,
                'tenant_hash': tenant_hash,
                'date': process_date,
                'conversation_count': str(metrics['conversation_count']),
                'environment': ENVIRONMENT
            }
        )
        logger.info(f"Archived metrics to S3 for {tenant_id} on {process_date}: s3://{ARCHIVE_BUCKET}/{s3_key}")

    except Exception as e:
        logger.error(f"Error archiving metrics to S3: {str(e)}")
        # Don't raise here - we want DynamoDB write to succeed even if S3 fails
        # but log it for monitoring

# For local testing
if __name__ == "__main__":
    test_event = {
        'date': '2025-09-14'
    }
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))