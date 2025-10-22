import json
import os
import logging
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from decimal import Decimal
from collections import defaultdict
import time
import gzip
import io
try:
    import pytz
except ImportError:
    # Fallback if pytz not available
    pytz = None

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from config import ENVIRONMENT, CONFIG_BUCKET
from cloudwatch_reader import CloudWatchReader
from tenant_resolver import TenantResolver

# Initialize AWS services
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
ANALYTICS_TABLE = os.environ.get('ANALYTICS_TABLE', 'production-picasso-analytics-daily')
ARCHIVE_BUCKET = os.environ.get('ARCHIVE_BUCKET', 'picasso-analytics-archive')

class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from DynamoDB"""
    def default(self, o):
        if isinstance(o, Decimal):
            if float(o).is_integer():
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

class AnalyticsFunction:
    def __init__(self):
        self.cloudwatch = CloudWatchReader()
        self.tenant_resolver = TenantResolver()
        self.analytics_table = dynamodb.Table(ANALYTICS_TABLE)
        self.s3_client = boto3.client('s3')
        self._timezone_cache = {}  # Cache tenant timezones
        
    def process_tenant(self, tenant_hash: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None, top_questions_limit: int = 5,
                       include_heat_map: bool = False, include_full_conversations: bool = False,
                       full_conversations_limit: int = 50, include_forms: bool = True) -> Dict[str, Any]:
        """Process analytics for a specific tenant with date range using hybrid approach"""
        logger.debug(f"Processing analytics for tenant_hash: {tenant_hash[:8]}... from {start_date} to {end_date}")
        
        # Parse dates or use defaults
        if end_date:
            # Check if it's a date-only string (YYYY-MM-DD format)
            if len(end_date) == 10 and end_date[4] == '-' and end_date[7] == '-':
                # For date-only strings, set to end of day
                end_time = datetime.strptime(end_date, "%Y-%m-%d")
                end_time = end_time.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                logger.debug(f"Parsed end_date '{end_date}' as end-of-day: {end_time.isoformat()}")
            else:
                # For full ISO timestamps
                try:
                    end_time = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                except:
                    # Fallback
                    end_time = datetime.now(timezone.utc)
        else:
            end_time = datetime.now(timezone.utc)
        
        if start_date:
            try:
                start_time = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
            except:
                start_time = datetime.strptime(start_date, "%Y-%m-%d")
                start_time = start_time.replace(tzinfo=timezone.utc)
        else:
            # Default to 30 days ago
            start_time = end_time - timedelta(days=30)
        
        # Calculate period in days for display
        period_days = (end_time - start_time).days
        
        tenant_id = self.tenant_resolver.resolve_tenant_hash(tenant_hash)
        
        # Determine cutoffs
        cutoff_7_days = datetime.now(timezone.utc) - timedelta(days=7)
        cutoff_90_days = datetime.now(timezone.utc) - timedelta(days=90)
        logger.debug(f"Data source cutoffs: S3 Archive < {cutoff_90_days.date()} < DynamoDB < {cutoff_7_days.date()} < CloudWatch")

        # Initialize combined metrics
        combined_metrics = {
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

        # Query S3 Archive for very old data (older than 90 days)
        if start_time < cutoff_90_days:
            s3_end = min(end_time, cutoff_90_days)
            logger.info(f"Querying S3 Archive from {start_time.date()} to {s3_end.date()}")
            s3_data = self.query_s3_archive(tenant_id, start_time, s3_end)
            logger.info(f"S3 Archive returned {s3_data['conversation_count']} conversations")
            self.merge_metrics(combined_metrics, s3_data)

        # Query DynamoDB for historical data (7-90 days old)
        if start_time < cutoff_7_days and end_time > cutoff_90_days:
            dynamo_start = max(start_time, cutoff_90_days)
            dynamo_end = min(end_time, cutoff_7_days)
            logger.info(f"Querying DynamoDB from {dynamo_start.date()} to {dynamo_end.date()}")
            dynamo_data = self.query_dynamodb(tenant_id, dynamo_start, dynamo_end)
            logger.info(f"DynamoDB returned {dynamo_data['conversation_count']} conversations")
            self.merge_metrics(combined_metrics, dynamo_data)
        
        # Query CloudWatch for recent data (within 7 days)
        if end_time >= cutoff_7_days:
            cloudwatch_start = max(start_time, cutoff_7_days)
            logger.info(f"Querying CloudWatch from {cloudwatch_start.date()} to {end_time.date()}")
            cloudwatch_data = self.query_cloudwatch(
                tenant_hash, tenant_id, cloudwatch_start, end_time,
                include_full_conversations, full_conversations_limit
            )
            logger.info(f"CloudWatch returned {cloudwatch_data['conversation_count']} conversations")
            self.merge_metrics(combined_metrics, cloudwatch_data)
        
        # Calculate final metrics
        logger.info(f"Total combined metrics: {combined_metrics['conversation_count']} conversations, {combined_metrics['total_messages']} messages")

        # Query form submissions if requested
        form_metrics = None
        if include_forms:
            logger.info(f"Querying form submissions from {start_time.date()} to {end_time.date()}")
            form_metrics = self.query_form_submissions(tenant_id, start_time, end_time)
            logger.info(f"Form submissions: {form_metrics['total_submissions']} total")

        return self.format_response(
            combined_metrics, tenant_id, tenant_hash,
            start_time, end_time, period_days,
            top_questions_limit, include_heat_map,
            include_full_conversations, full_conversations_limit,
            form_metrics
        )
    
    def get_tenant_timezone(self, tenant_id: str) -> Optional[str]:
        """Get tenant's timezone from config file"""
        # Check cache first
        if tenant_id in self._timezone_cache:
            return self._timezone_cache[tenant_id]

        try:
            # Try to load config from S3
            config_key = f'tenants/{tenant_id}/{tenant_id}-config.json'
            response = self.s3_client.get_object(
                Bucket=CONFIG_BUCKET,
                Key=config_key
            )
            config = json.loads(response['Body'].read())
            timezone = config.get('timezone', None)

            # Cache the result
            self._timezone_cache[tenant_id] = timezone

            if timezone:
                logger.info(f"Found timezone {timezone} for tenant {tenant_id}")
            else:
                logger.warning(f"No timezone configured for tenant {tenant_id}")

            return timezone

        except Exception as e:
            logger.warning(f"Could not load timezone for tenant {tenant_id}: {str(e)}")
            self._timezone_cache[tenant_id] = None
            return None

    def query_dynamodb(self, tenant_id: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Query DynamoDB for aggregated analytics."""
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

        logger.info(f"Querying DynamoDB for tenant {tenant_id} from {start_date.date()} to {end_date.date()}")

        # Query each day in the range
        current_date = start_date
        days_queried = 0
        days_with_data = 0

        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            days_queried += 1

            try:
                logger.debug(f"Querying DynamoDB for pk=TENANT#{tenant_id}, sk=DATE#{date_str}")
                response = self.analytics_table.get_item(
                    Key={
                        'pk': f"TENANT#{tenant_id}",
                        'sk': f"DATE#{date_str}"
                    }
                )

                if 'Item' in response:
                    item = response['Item']
                    days_with_data += 1

                    # Convert Decimal to int/float for proper handling
                    conv_count = int(item.get('conversation_count', 0))
                    msg_count = int(item.get('total_messages', 0))

                    logger.info(f"Found {conv_count} conversations on {date_str}")

                    # Aggregate metrics
                    metrics['conversation_count'] += conv_count
                    metrics['total_messages'] += msg_count
                    
                    # Add response times (approximate from average)
                    if 'avg_response_time_ms' in item and conv_count > 0:
                        avg_resp_time = float(item['avg_response_time_ms'])
                        for _ in range(conv_count):
                            metrics['response_times'].append(avg_resp_time)

                    # Add first token times
                    if 'avg_first_token_ms' in item and conv_count > 0:
                        avg_first_token = float(item['avg_first_token_ms'])
                        for _ in range(conv_count):
                            metrics['first_token_times'].append(avg_first_token)

                    # Add total times
                    if 'avg_total_time_ms' in item and conv_count > 0:
                        avg_total_time = float(item['avg_total_time_ms'])
                        for _ in range(conv_count):
                            metrics['total_times'].append(avg_total_time)
                    
                    # Merge questions
                    for q in item.get('top_questions', []):
                        metrics['questions'][q['question']] += q['count']
                    
                    # Extract after_hours_count if available (from updated aggregator)
                    if 'after_hours_count' in item:
                        metrics['after_hours_count'] += int(item['after_hours_count'])

                    # Merge distributions (handle Decimal types)
                    for hour, count in item.get('hourly_distribution', {}).items():
                        metrics['hourly_distribution'][int(hour)] += int(count)

                    for day, count in item.get('daily_distribution', {}).items():
                        metrics['daily_distribution'][int(day)] += int(count)
                    
                    # Add conversations with backwards compatibility for timing fields
                    dynamo_conversations = item.get('conversations', [])
                    for conv in dynamo_conversations:
                        # Handle legacy data that only has response_time_ms
                        if 'first_token_ms' not in conv and 'response_time_ms' in conv:
                            # Use response_time_ms as a fallback for both metrics
                            conv['first_token_ms'] = conv['response_time_ms']
                            conv['total_time_ms'] = conv['response_time_ms']
                        elif 'first_token_ms' not in conv:
                            # No timing data at all
                            conv['first_token_ms'] = 0
                            conv['total_time_ms'] = 0
                    metrics['conversations'].extend(dynamo_conversations)
                else:
                    logger.debug(f"No data found for {date_str}")

            except Exception as e:
                logger.warning(f"Error querying DynamoDB for {date_str}: {str(e)}")

            current_date += timedelta(days=1)

        logger.info(f"DynamoDB query complete: {days_queried} days queried, {days_with_data} days with data, {metrics['conversation_count']} total conversations")

        return metrics

    def query_s3_archive(self, tenant_id: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Query S3 archive for analytics data older than 90 days."""

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

        current_date = start_time
        days_queried = 0
        days_with_data = 0

        while current_date <= end_time:
            date_str = current_date.strftime('%Y-%m-%d')
            year = date_str[:4]
            month = date_str[5:7]

            # Construct S3 key based on our partitioning scheme
            s3_key = f"daily-aggregates/{year}/{month}/{date_str}/{tenant_id}.json.gz"

            try:
                # Get object from S3
                response = s3.get_object(
                    Bucket=ARCHIVE_BUCKET,
                    Key=s3_key
                )

                # Decompress and parse JSON
                compressed_data = response['Body'].read()
                decompressed_data = gzip.decompress(compressed_data)
                data = json.loads(decompressed_data.decode('utf-8'))

                days_with_data += 1

                # Extract metrics
                metrics['conversation_count'] += data.get('conversation_count', 0)
                metrics['total_messages'] += data.get('total_messages', 0)
                metrics['after_hours_count'] += data.get('after_hours_count', 0)
                metrics['streaming_enabled_count'] += data.get('streaming_enabled_count', 0)

                # Merge response times
                if data.get('avg_response_time_ms', 0) > 0:
                    # Store averages weighted by conversation count
                    for _ in range(data.get('conversation_count', 1)):
                        metrics['response_times'].append(data.get('avg_response_time_ms', 0))
                        metrics['first_token_times'].append(data.get('avg_first_token_ms', 0))
                        metrics['total_times'].append(data.get('avg_total_time_ms', 0))

                # Merge top questions
                for q in data.get('top_questions', []):
                    metrics['questions'][q['question']] += q.get('count', 1)

                # Merge hourly and daily distributions
                for hour, count in data.get('hourly_distribution', {}).items():
                    metrics['hourly_distribution'][int(hour)] += count

                for day, count in data.get('daily_distribution', {}).items():
                    metrics['daily_distribution'][int(day)] += count

                # Add conversations (limited to prevent memory issues)
                s3_conversations = data.get('conversations', [])[:10]  # Limit per day
                for conv in s3_conversations:
                    # Ensure proper format
                    if 'timestamp' not in conv:
                        conv['timestamp'] = f"{date_str}T00:00:00Z"
                    if 'first_token_ms' not in conv and 'response_time_ms' in conv:
                        conv['first_token_ms'] = conv['response_time_ms']
                        conv['total_time_ms'] = conv['response_time_ms']
                metrics['conversations'].extend(s3_conversations)

                logger.info(f"S3 Archive: Found {data.get('conversation_count', 0)} conversations for {date_str}")

            except s3.exceptions.NoSuchKey:
                logger.debug(f"No S3 archive found for {tenant_id} on {date_str}")
            except Exception as e:
                logger.warning(f"Error querying S3 archive for {date_str}: {str(e)}")

            days_queried += 1
            current_date += timedelta(days=1)

        logger.info(f"S3 Archive query complete: {days_queried} days queried, {days_with_data} days with data, {metrics['conversation_count']} total conversations")

        return metrics

    def query_cloudwatch(self, tenant_hash: str, tenant_id: str,
                        start_time: datetime, end_time: datetime,
                        include_full_conversations: bool,
                        full_conversations_limit: int) -> Dict[str, Any]:
        """Query CloudWatch for recent analytics."""

        # Get tenant's timezone from config
        tenant_timezone = self.get_tenant_timezone(tenant_id)

        # Get QA logs from CloudWatch
        qa_logs = self.cloudwatch.get_qa_complete_logs(
            tenant_hash=tenant_hash,
            start_time=start_time,
            end_time=end_time
        )

        # Process logs to extract metrics
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

        # Track unique sessions for conversation count
        unique_sessions = set()

        logger.info(f"Processing {len(qa_logs)} QA logs from CloudWatch")

        # Process each QA log
        for idx, log in enumerate(qa_logs):
            try:
                # Parse the log message
                if 'question' in log:
                    metrics['questions'][log['question']] += 1
                    metrics['total_messages'] += 1

                    # Track unique session for conversation counting
                    session_id = log.get('session_id')
                    if session_id:
                        if session_id not in unique_sessions:
                            logger.debug(f"New unique session found: {session_id}")
                        unique_sessions.add(session_id)
                    else:
                        logger.warning(f"Log {idx} missing session_id: {log.get('question', 'unknown')[:50]}")

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
                            logger.warning(f"No timezone configured for tenant {tenant_id}, using UTC")

                        hour = local_dt.hour
                        day = local_dt.weekday()
                        metrics['hourly_distribution'][hour] += 1
                        metrics['daily_distribution'][day] += 1

                        # Check if after hours:
                        # - Weekends (Saturday=5, Sunday=6) are always after hours
                        # - Weekdays: before 9am or after 5pm in LOCAL time
                        if day >= 5 or hour < 9 or hour >= 17:
                            metrics['after_hours_count'] += 1

                    # Add response times if available (check in metrics sub-object)
                    log_metrics = log.get('metrics', {})
                    if 'response_time_ms' in log_metrics:
                        metrics['response_times'].append(log_metrics['response_time_ms'])
                    elif 'response_time_ms' in log:  # Fallback to direct field
                        metrics['response_times'].append(log['response_time_ms'])

                    if 'first_token_ms' in log_metrics:
                        metrics['first_token_times'].append(log_metrics['first_token_ms'])
                    elif 'first_token_time' in log_metrics:
                        metrics['first_token_times'].append(log_metrics['first_token_time'])
                    elif 'first_token_time' in log:  # Fallback
                        metrics['first_token_times'].append(log['first_token_time'])

                    if 'total_time_ms' in log_metrics:
                        metrics['total_times'].append(log_metrics['total_time_ms'])
                    elif 'total_time' in log_metrics:
                        metrics['total_times'].append(log_metrics['total_time'])
                    elif 'total_time' in log:  # Fallback
                        metrics['total_times'].append(log['total_time'])

                    # Check if streaming was enabled
                    if log.get('streaming_enabled', False):
                        metrics['streaming_enabled_count'] += 1

                    # Add to conversations list
                    if include_full_conversations:
                        # Get timing metrics - prefer first_token time for user experience
                        first_token = log_metrics.get('first_token_ms') or log_metrics.get('first_token_time', 0)
                        total_time = log_metrics.get('total_time_ms') or log_metrics.get('total_time', 0)
                        response_time = log_metrics.get('response_time_ms', 0)

                        conversation = {
                            'timestamp': log.get('timestamp', ''),
                            'session_id': log.get('session_id', ''),
                            'conversation_id': log.get('conversation_id'),
                            'question': log.get('question', ''),
                            'answer': log.get('answer', ''),
                            'response_time_ms': response_time,  # Keep for compatibility
                            'first_token_ms': first_token,       # Time to first token (what users feel)
                            'total_time_ms': total_time          # Total completion time
                        }
                        metrics['conversations'].append(conversation)
                    
            except Exception as e:
                logger.warning(f"Error processing log entry: {str(e)}")

        # Set conversation count to number of unique sessions
        metrics['conversation_count'] = len(unique_sessions)
        logger.info(f"CloudWatch: Found {len(unique_sessions)} unique sessions, {metrics['total_messages']} total messages")

        return metrics
    
    def merge_metrics(self, target: Dict, source: Dict):
        """Merge source metrics into target."""
        target['conversation_count'] += source['conversation_count']
        target['total_messages'] += source['total_messages']
        target['response_times'].extend(source['response_times'])
        target['first_token_times'].extend(source.get('first_token_times', []))
        target['total_times'].extend(source.get('total_times', []))
        target['conversations'].extend(source['conversations'])
        target['after_hours_count'] += source.get('after_hours_count', 0)
        target['streaming_enabled_count'] += source.get('streaming_enabled_count', 0)

        # Merge dictionaries
        for question, count in source['questions'].items():
            target['questions'][question] += count

        for hour, count in source['hourly_distribution'].items():
            target['hourly_distribution'][hour] += count

        for day, count in source['daily_distribution'].items():
            target['daily_distribution'][day] += count

    def query_form_submissions(self, tenant_id: str, start_date: datetime, end_date: datetime) -> Dict:
        """Query aggregated form submissions from DynamoDB"""
        combined_forms = {
            'total_submissions': 0,
            'form_counts': defaultdict(int),
            'submissions_by_date': {},
            'recent_submissions': []
        }

        # Query DynamoDB for each day in range
        current_date = start_date.date()
        end_date_only = end_date.date()

        while current_date <= end_date_only:
            date_str = current_date.strftime('%Y-%m-%d')

            try:
                response = self.analytics_table.get_item(
                    Key={
                        'pk': f"TENANT#{tenant_id}",
                        'sk': f"FORMS#{date_str}"
                    }
                )

                if 'Item' in response:
                    item = response['Item']
                    combined_forms['total_submissions'] += item.get('total_submissions', 0)

                    # Merge form counts
                    for form_type, count in item.get('form_counts', {}).items():
                        combined_forms['form_counts'][form_type] += count

                    # Store by date
                    combined_forms['submissions_by_date'][date_str] = {
                        'count': item.get('total_submissions', 0),
                        'forms': item.get('form_counts', {})
                    }

                    # Add recent submissions
                    combined_forms['recent_submissions'].extend(
                        item.get('submissions', [])[:10]
                    )

            except Exception as e:
                logger.error(f"Error querying forms for {date_str}: {str(e)}")

            current_date += timedelta(days=1)

        # Limit recent submissions
        combined_forms['recent_submissions'] = combined_forms['recent_submissions'][:50]
        combined_forms['form_counts'] = dict(combined_forms['form_counts'])

        return combined_forms
    
    def format_response(self, metrics: Dict, tenant_id: str, tenant_hash: str,
                       start_time: datetime, end_time: datetime, period_days: int,
                       top_questions_limit: int, include_heat_map: bool,
                       include_full_conversations: bool, full_conversations_limit: int,
                       form_metrics: Optional[Dict] = None) -> Dict[str, Any]:
        """Format the final response."""
        
        # Calculate averages
        avg_response_time = 0
        avg_first_token = 0
        avg_total_time = 0
        
        if metrics['response_times']:
            avg_response_time = sum(metrics['response_times']) / len(metrics['response_times'])
        
        if metrics['first_token_times']:
            avg_first_token = sum(metrics['first_token_times']) / len(metrics['first_token_times'])
        
        if metrics['total_times']:
            avg_total_time = sum(metrics['total_times']) / len(metrics['total_times'])
        
        # Calculate after hours percentage from actual conversations in the date range
        after_hours_count = 0
        total_conversations_with_timestamps = 0

        # Get tenant timezone for after_hours calculation
        tenant_timezone = self.get_tenant_timezone(tenant_id)

        if tenant_timezone and pytz:
            tz = pytz.timezone(tenant_timezone)

            # Check each conversation's timestamp
            for conv in metrics['conversations']:
                timestamp_str = conv.get('timestamp', '')
                if timestamp_str:
                    try:
                        # Parse UTC timestamp and convert to local timezone
                        utc_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        local_dt = utc_dt.astimezone(tz)
                        hour = local_dt.hour
                        weekday = local_dt.weekday()  # Monday=0, Sunday=6

                        # Check if after hours:
                        # - Weekends (Saturday=5, Sunday=6) are always after hours
                        # - Weekdays: before 9am or after 5pm in local time
                        if weekday >= 5 or hour < 9 or hour >= 17:
                            after_hours_count += 1

                        total_conversations_with_timestamps += 1
                    except Exception as e:
                        logger.debug(f"Error parsing timestamp {timestamp_str}: {e}")

        # Calculate percentage based on conversations with valid timestamps
        after_hours_percentage = 0
        if total_conversations_with_timestamps > 0:
            after_hours_percentage = (after_hours_count / total_conversations_with_timestamps) * 100
            logger.info(f"After hours: {after_hours_count}/{total_conversations_with_timestamps} = {after_hours_percentage:.1f}%")
        elif metrics['after_hours_count'] > 0 and metrics['total_messages'] > 0:
            # Fallback to pre-calculated if no conversation timestamps available
            after_hours_percentage = (metrics['after_hours_count'] / metrics['total_messages']) * 100
        
        # Calculate streaming percentage
        streaming_percentage = 0
        if metrics['conversation_count'] > 0:
            streaming_percentage = (metrics['streaming_enabled_count'] / metrics['conversation_count']) * 100
        
        # Format top questions
        top_questions = []
        for question, count in sorted(metrics['questions'].items(), key=lambda x: x[1], reverse=True)[:top_questions_limit]:
            percentage = (count / metrics['total_messages'] * 100) if metrics['total_messages'] > 0 else 0
            top_questions.append({
                'question': question,
                'count': count,
                'percentage': round(percentage, 1)
            })
        
        # Format response
        response = {
            'tenant_id': tenant_id,
            'tenant_hash': tenant_hash,
            'start_date': start_time.isoformat(),
            'end_date': end_time.isoformat(),
            'period_days': period_days,
            'metrics': {
                'conversation_count': metrics['conversation_count'],
                'avg_response_time_ms': round(avg_response_time),
                'avg_first_token_ms': round(avg_first_token),
                'avg_total_time_ms': round(avg_total_time),
                'after_hours_percentage': round(after_hours_percentage, 1),
                'total_messages': metrics['total_messages'],
                'streaming_enabled_percentage': round(streaming_percentage, 1),
                'processing_time_ms': int(time.time() * 1000) % 10000  # Mock processing time
            },
            'top_questions': top_questions,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        # Add heat map data if requested
        if include_heat_map:
            response['heat_map_data'] = self.format_heat_map_data(
                metrics['hourly_distribution'],
                metrics['daily_distribution'],
                metrics['conversations'],
                tenant_id
            )
        
        # Add full conversations if requested
        if include_full_conversations:
            sorted_conversations = sorted(
                metrics['conversations'],
                key=lambda x: x.get('timestamp', ''),
                reverse=True
            )[:full_conversations_limit]

            response['full_conversations'] = sorted_conversations
            response['full_conversations_total'] = len(metrics['conversations'])
            response['full_conversations_returned'] = len(sorted_conversations)

        # Add form submissions if available
        if form_metrics:
            response['form_submissions'] = form_metrics

        return response
    
    def format_heat_map_data(self, hourly_dist: Dict, daily_dist: Dict,
                            conversations: List, tenant_id: str = None) -> Dict[str, Any]:
        """Format heat map data for visualization."""
        
        # Initialize distributions
        hourly_distribution = {str(i): hourly_dist.get(i, 0) for i in range(24)}
        daily_distribution = {str(i): daily_dist.get(i, 0) for i in range(7)}
        
        # Find peaks
        peak_hour = max(hourly_distribution.items(), key=lambda x: x[1], default=('0', 0))[0]
        peak_day = max(daily_distribution.items(), key=lambda x: x[1], default=('0', 0))[0]
        
        # Format labels
        day_labels = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        hour_labels = [f"{i%12 or 12}{'am' if i < 12 else 'pm'}" for i in range(24)]
        
        peak_hour_formatted = hour_labels[int(peak_hour)]
        peak_day_formatted = day_labels[int(peak_day)]
        
        # Create heat grid
        heat_grid = []

        # Debug: Log first few conversations
        if conversations and len(conversations) > 0:
            logger.info(f"Processing {len(conversations)} conversations for heat_grid with tenant {tenant_id}")
            for c in conversations[:3]:
                ts = c.get('timestamp', '')
                h = self.get_hour_from_timestamp(ts, tenant_id)
                d = self.get_day_from_timestamp(ts, tenant_id)
                logger.info(f"Conv timestamp {ts} -> hour={h}, day={d}")

        for hour in [0, 3, 6, 9, 12, 15, 18, 21]:
            row = {
                'time_label': hour_labels[hour],
                'hour': hour
            }
            for day_idx, day_name in enumerate(['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']):
                # Count conversations for this 3-hour window/day combination
                count = sum(1 for c in conversations
                          if hour <= self.get_hour_from_timestamp(c.get('timestamp', ''), tenant_id) < min(hour + 3, 24)
                          and self.get_day_from_timestamp(c.get('timestamp', ''), tenant_id) == day_idx)
                row[day_name] = count
            heat_grid.append(row)
        
        # Question timestamps
        question_timestamps = []
        for conv in conversations[:100]:  # Limit to recent 100
            if 'timestamp' in conv and 'question' in conv:
                timestamp = conv['timestamp']
                question_timestamps.append({
                    'timestamp': timestamp,
                    'hour': self.get_hour_from_timestamp(timestamp, tenant_id),
                    'day_of_week': self.get_day_from_timestamp(timestamp, tenant_id),
                    'question': conv['question'][:100]  # Truncate long questions
                })
        
        return {
            'hourly_distribution': hourly_distribution,
            'daily_distribution': daily_distribution,
            'day_labels': day_labels,
            'peak_hour': int(peak_hour),
            'peak_day': int(peak_day),
            'peak_hour_formatted': peak_hour_formatted,
            'peak_day_formatted': peak_day_formatted,
            'peak_time_formatted': f"{peak_day_formatted} at {peak_hour_formatted}",
            'question_timestamps': question_timestamps,
            'heat_grid': heat_grid,
            'hourly_chart_values': [hourly_distribution[str(i)] for i in range(24)],
            'hourly_chart_labels': hour_labels,
            'hourly_chart_values_string': ', '.join(str(hourly_distribution[str(i)]) for i in range(24)),
            'hourly_chart_labels_string': ', '.join(hour_labels)
        }
    
    def get_hour_from_timestamp(self, timestamp: str, tenant_id: str = None) -> int:
        """Extract hour from ISO timestamp, converting to tenant's timezone."""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

            # Apply timezone if available (same logic as after_hours)
            if tenant_id and pytz:
                tenant_timezone = self.get_tenant_timezone(tenant_id)
                if tenant_timezone:
                    tz = pytz.timezone(tenant_timezone)
                    dt = dt.astimezone(tz)

            return dt.hour
        except Exception as e:
            logger.debug(f"Error getting hour from timestamp {timestamp}: {e}")
            return 0

    def get_day_from_timestamp(self, timestamp: str, tenant_id: str = None) -> int:
        """Extract day of week from ISO timestamp (0=Monday), converting to tenant's timezone."""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

            # Apply timezone if available (same logic as after_hours)
            if tenant_id and pytz:
                tenant_timezone = self.get_tenant_timezone(tenant_id)
                if tenant_timezone:
                    tz = pytz.timezone(tenant_timezone)
                    dt = dt.astimezone(tz)

            return dt.weekday()
        except Exception as e:
            logger.debug(f"Error getting day from timestamp {timestamp}: {e}")
            return 0

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for analytics API"""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Initialize analytics function
        analytics = AnalyticsFunction()
        
        # Extract parameters
        params = event.get('queryStringParameters', {}) or {}
        body = {}
        
        if event.get('body'):
            try:
                body = json.loads(event['body'])
            except:
                pass
        
        # Get parameters from query string or body
        tenant_hash = params.get('tenant_hash') or body.get('tenant_hash')
        tenant_id = params.get('tenant_id') or body.get('tenant_id')
        start_date = params.get('start_date') or body.get('start_date')
        end_date = params.get('end_date') or body.get('end_date')
        top_questions_limit = int(params.get('top_questions_limit', body.get('top_questions_limit', 5)))
        
        # Handle both string and boolean values for include flags
        heat_map_val = params.get('include_heat_map', body.get('include_heat_map', 'false'))
        include_heat_map = str(heat_map_val).lower() == 'true' if heat_map_val is not None else False

        conv_val = params.get('include_full_conversations', body.get('include_full_conversations', 'false'))
        include_full_conversations = str(conv_val).lower() == 'true' if conv_val is not None else False

        forms_val = params.get('include_forms', body.get('include_forms', 'true'))
        include_forms = str(forms_val).lower() == 'true' if forms_val is not None else True

        full_conversations_limit = int(params.get('full_conversations_limit', body.get('full_conversations_limit', 50)))
        
        # If tenant_id provided but no tenant_hash, look it up from S3
        if tenant_id and not tenant_hash:
            tenant_hash = analytics.tenant_resolver.get_tenant_hash_by_id(tenant_id)
            if not tenant_hash:
                logger.error(f"Could not find tenant_hash for tenant_id: {tenant_id}")
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({'error': f'Tenant not found: {tenant_id}'})
                }
        
        if not tenant_hash:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'tenant_hash or tenant_id required'})
            }
        
        # Process analytics
        result = analytics.process_tenant(
            tenant_hash=tenant_hash,
            start_date=start_date,
            end_date=end_date,
            top_questions_limit=top_questions_limit,
            include_heat_map=include_heat_map,
            include_full_conversations=include_full_conversations,
            full_conversations_limit=full_conversations_limit,
            include_forms=include_forms
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }

# For local testing
if __name__ == "__main__":
    test_event = {
        'queryStringParameters': {
            'tenant_id': 'MYR384719',
            'start_date': '2025-08-01',
            'end_date': '2025-09-16',
            'include_heat_map': 'true',
            'include_full_conversations': 'true'
        }
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(json.loads(result['body']), indent=2, cls=DecimalEncoder))