"""
Analytics Aggregator Lambda

Pre-computes dashboard metrics from Athena and stores them in DynamoDB for fast retrieval.
Runs on a schedule (hourly via EventBridge) to keep aggregates fresh.

This enables sub-100ms dashboard queries instead of 10-30s Athena queries.

DynamoDB Table: picasso-dashboard-aggregates
- PK: TENANT#{tenant_id}
- SK: METRIC#{metric_type}#{range}  (e.g., METRIC#conversations_summary#30d)
- TTL: 24 hours (auto-refresh on next run)

Metrics computed:
- conversations_summary: Total conversations, messages, response time, after-hours %
- conversations_heatmap: Day x Hour grid for conversation volume
- conversations_trend: Daily/hourly conversation counts
- top_questions: Most frequently asked questions
- forms_summary: Form views, starts, completions, abandonment rates
- forms_bottlenecks: Field-level abandonment analysis
- forms_top_performers: Form performance rankings
- analytics_summary: Overall analytics metrics
- analytics_funnel: Conversion funnel data
"""

import json
import os
import logging
import time
import re
import boto3
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from decimal import Decimal
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'picasso_analytics')
ATHENA_TABLE = os.environ.get('ATHENA_TABLE', 'events')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION', 's3://picasso-analytics/athena-results/')
AGGREGATES_TABLE = os.environ.get('AGGREGATES_TABLE', 'picasso-dashboard-aggregates')
CONFIG_BUCKET = os.environ.get('CONFIG_BUCKET', 'myrecruiter-picasso')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# AWS clients
athena = boto3.client('athena')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# DynamoDB table
aggregates_table = dynamodb.Table(AGGREGATES_TABLE)

# Time ranges to pre-compute
TIME_RANGES = ['1d', '7d', '30d', '90d']

# TTL: 24 hours for fresh data
TTL_HOURS = 24


def lambda_handler(event, context):
    """
    Main handler - aggregates data for all tenants.

    Can be triggered by:
    1. EventBridge schedule (hourly/daily)
    2. Manual invocation with specific tenant_id
    """
    logger.info(f"Analytics Aggregator started: {json.dumps(event)}")

    # Check for specific tenant in event
    specific_tenant = event.get('tenant_id')

    if specific_tenant:
        # Aggregate for specific tenant only
        tenants = [specific_tenant]
    else:
        # Aggregate for all active tenants
        tenants = get_active_tenants()

    logger.info(f"Processing {len(tenants)} tenants")

    results = []
    for tenant_id in tenants:
        try:
            result = aggregate_tenant_metrics(tenant_id)
            results.append({
                'tenant_id': tenant_id,
                'status': 'success',
                'metrics_updated': result.get('metrics_updated', 0)
            })
        except Exception as e:
            logger.error(f"Error aggregating tenant {tenant_id}: {e}")
            results.append({
                'tenant_id': tenant_id,
                'status': 'error',
                'error': str(e)
            })

    summary = {
        'total_tenants': len(tenants),
        'successful': sum(1 for r in results if r['status'] == 'success'),
        'failed': sum(1 for r in results if r['status'] == 'error'),
        'results': results
    }

    logger.info(f"Aggregation complete: {summary['successful']}/{summary['total_tenants']} successful")
    return summary


def get_active_tenants() -> List[str]:
    """
    Get list of active tenants from S3 config bucket.

    Discovery methods (in order):
    1. Read mappings.json if it exists (centralized tenant registry)
    2. Fallback: List tenant directories under tenants/ prefix
    """
    tenants = []

    # Method 1: Try centralized mappings.json
    try:
        response = s3.get_object(
            Bucket=CONFIG_BUCKET,
            Key='mappings.json'
        )
        mappings = json.loads(response['Body'].read().decode('utf-8'))
        tenants = list(mappings.get('tenants', {}).values())
        if tenants:
            logger.info(f"Found {len(tenants)} tenants from mappings.json")
            return tenants
    except s3.exceptions.NoSuchKey:
        logger.info("mappings.json not found, using fallback discovery")
    except Exception as e:
        logger.warning(f"Error reading mappings.json: {e}, using fallback")

    # Method 2: Fallback - list tenant directories
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=CONFIG_BUCKET, Prefix='tenants/', Delimiter='/'):
            for prefix in page.get('CommonPrefixes', []):
                # Extract tenant_id from path like 'tenants/AUS123957/'
                tenant_path = prefix.get('Prefix', '')
                tenant_id = tenant_path.rstrip('/').split('/')[-1]
                if tenant_id and len(tenant_id) > 5:
                    tenants.append(tenant_id)

        logger.info(f"Found {len(tenants)} tenants from directory listing")
        return tenants

    except Exception as e:
        logger.error(f"Error listing tenant directories: {e}")
        return []


def aggregate_tenant_metrics(tenant_id: str) -> Dict[str, Any]:
    """
    Aggregate all metrics for a single tenant.
    """
    logger.info(f"Aggregating metrics for tenant: {tenant_id}")

    metrics_updated = 0
    ttl = int((datetime.now(timezone.utc) + timedelta(hours=TTL_HOURS)).timestamp())

    for range_str in TIME_RANGES:
        date_range = parse_date_range(range_str)

        # 1. Conversations Summary
        try:
            summary = compute_conversations_summary(tenant_id, date_range)
            store_metric(tenant_id, f'conversations_summary#{range_str}', summary, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing conversations_summary for {tenant_id}: {e}")

        # 2. Conversations Heatmap (only for 30d to save Athena costs)
        if range_str == '30d':
            try:
                heatmap = compute_conversations_heatmap(tenant_id, date_range)
                store_metric(tenant_id, f'conversations_heatmap#{range_str}', heatmap, ttl)
                metrics_updated += 1
            except Exception as e:
                logger.error(f"Error computing heatmap for {tenant_id}: {e}")

        # 3. Conversations Trend
        try:
            trend = compute_conversations_trend(tenant_id, date_range, range_str)
            store_metric(tenant_id, f'conversations_trend#{range_str}', trend, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing trend for {tenant_id}: {e}")

        # 4. Top Questions
        try:
            questions = compute_top_questions(tenant_id, date_range)
            store_metric(tenant_id, f'top_questions#{range_str}', questions, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing top_questions for {tenant_id}: {e}")

        # 5. Recent Conversations (only for 30d)
        if range_str == '30d':
            try:
                recent = compute_recent_conversations(tenant_id, date_range)
                store_metric(tenant_id, f'recent_conversations#{range_str}', recent, ttl)
                metrics_updated += 1
            except Exception as e:
                logger.error(f"Error computing recent_conversations for {tenant_id}: {e}")

        # 6. Forms Summary
        try:
            forms = compute_forms_summary(tenant_id, date_range)
            store_metric(tenant_id, f'forms_summary#{range_str}', forms, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing forms_summary for {tenant_id}: {e}")

        # 7. Forms Bottlenecks (only for 30d)
        if range_str == '30d':
            try:
                bottlenecks = compute_forms_bottlenecks(tenant_id, date_range)
                store_metric(tenant_id, f'forms_bottlenecks#{range_str}', bottlenecks, ttl)
                metrics_updated += 1
            except Exception as e:
                logger.error(f"Error computing bottlenecks for {tenant_id}: {e}")

        # 8. Forms Top Performers
        try:
            top_forms = compute_forms_top_performers(tenant_id, date_range)
            store_metric(tenant_id, f'forms_top_performers#{range_str}', top_forms, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing top_performers for {tenant_id}: {e}")

        # 9. Analytics Summary
        try:
            analytics = compute_analytics_summary(tenant_id, date_range)
            store_metric(tenant_id, f'analytics_summary#{range_str}', analytics, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing analytics_summary for {tenant_id}: {e}")

        # 10. Analytics Funnel
        try:
            funnel = compute_analytics_funnel(tenant_id, date_range)
            store_metric(tenant_id, f'analytics_funnel#{range_str}', funnel, ttl)
            metrics_updated += 1
        except Exception as e:
            logger.error(f"Error computing funnel for {tenant_id}: {e}")

    # Store last aggregation timestamp
    store_metric(tenant_id, 'last_aggregation', {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'metrics_updated': metrics_updated
    }, ttl)

    return {'metrics_updated': metrics_updated}


def store_metric(tenant_id: str, metric_key: str, data: Dict[str, Any], ttl: int):
    """
    Store computed metric in DynamoDB.
    """
    # Convert floats to Decimal for DynamoDB
    data_for_dynamo = convert_floats_to_decimal(data)

    item = {
        'pk': f'TENANT#{tenant_id}',
        'sk': f'METRIC#{metric_key}',
        'data': data_for_dynamo,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'ttl': ttl
    }

    try:
        aggregates_table.put_item(Item=item)
        logger.debug(f"Stored metric {metric_key} for tenant {tenant_id}")
    except Exception as e:
        logger.error(f"Error storing metric {metric_key}: {e}")
        raise


def convert_floats_to_decimal(obj):
    """
    Recursively convert floats to Decimal for DynamoDB compatibility.
    """
    if isinstance(obj, float):
        return Decimal(str(round(obj, 2)))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    return obj


def parse_date_range(range_str: str) -> Dict[str, Any]:
    """Parse date range string into date components."""
    days = 30
    if range_str.endswith('d'):
        try:
            days = int(range_str[:-1])
        except ValueError:
            pass

    start_date = datetime.now(timezone.utc) - timedelta(days=days)

    return {
        'start_date_iso': start_date.strftime('%Y-%m-%d'),
        'days': days
    }


def execute_athena_query(query: str, timeout: int = 60) -> Optional[List[Dict[str, Any]]]:
    """Execute Athena query and return results."""
    logger.debug(f"Executing query: {query[:200]}...")

    try:
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        query_id = response['QueryExecutionId']

        # Wait for completion
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']

            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                logger.error(f"Query failed: {error}")
                return None

            time.sleep(1)
        else:
            logger.error("Query timed out")
            return None

        # Get results
        results = athena.get_query_results(QueryExecutionId=query_id)
        rows = results.get('ResultSet', {}).get('Rows', [])

        if len(rows) < 2:
            return []

        headers = [col.get('VarCharValue', '') for col in rows[0].get('Data', [])]

        data = []
        for row in rows[1:]:
            row_data = {}
            for i, col in enumerate(row.get('Data', [])):
                if i < len(headers):
                    row_data[headers[i]] = col.get('VarCharValue')
            data.append(row_data)

        return data

    except Exception as e:
        logger.error(f"Athena query error: {e}")
        return None


# =============================================================================
# Metric Computation Functions
# =============================================================================

def compute_conversations_summary(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute conversations summary metrics."""
    query = f"""
    WITH conversation_sessions AS (
        SELECT DISTINCT session_id
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'MESSAGE_SENT'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
    ),
    message_stats AS (
        SELECT
            COUNT(*) as total_messages,
            AVG(CASE
                WHEN event_type = 'MESSAGE_RECEIVED'
                THEN CAST(json_extract_scalar(event_payload, '$.response_time_ms') AS DOUBLE) / 1000
                ELSE NULL
            END) as avg_response_time
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type IN ('MESSAGE_SENT', 'MESSAGE_RECEIVED')
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
    ),
    after_hours AS (
        SELECT COUNT(DISTINCT session_id) as after_hours_sessions
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'MESSAGE_SENT'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
          AND (HOUR(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ')) < 9
               OR HOUR(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ')) >= 17)
    )
    SELECT
        (SELECT COUNT(*) FROM conversation_sessions) as total_conversations,
        m.total_messages,
        COALESCE(m.avg_response_time, 0) as avg_response_time_seconds,
        CASE
            WHEN (SELECT COUNT(*) FROM conversation_sessions) > 0
            THEN CAST(a.after_hours_sessions AS DOUBLE) / CAST((SELECT COUNT(*) FROM conversation_sessions) AS DOUBLE) * 100
            ELSE 0
        END as after_hours_percentage
    FROM message_stats m
    CROSS JOIN after_hours a
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        return {
            'total_conversations': int(row.get('total_conversations', 0) or 0),
            'total_messages': int(row.get('total_messages', 0) or 0),
            'avg_response_time_seconds': round(float(row.get('avg_response_time_seconds', 0) or 0), 1),
            'after_hours_percentage': round(float(row.get('after_hours_percentage', 0) or 0), 1)
        }

    return {
        'total_conversations': 0,
        'total_messages': 0,
        'avg_response_time_seconds': 0,
        'after_hours_percentage': 0
    }


def compute_conversations_heatmap(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute conversations heatmap data."""
    # Pre-compute for multiple common timezones
    timezones = ['UTC', 'America/Chicago', 'America/New_York', 'America/Los_Angeles']
    heatmaps = {}

    for tz in timezones:
        query = f"""
        WITH local_times AS (
            SELECT session_id,
                   date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ') AT TIME ZONE '{tz}' as local_ts
            FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
            WHERE tenant_id = '{tenant_id}'
              AND event_type = 'MESSAGE_SENT'
              AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                       LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                       LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
                  >= DATE '{date_range['start_date_iso']}'
        )
        SELECT day_name, hour_block, conversation_count FROM (
            SELECT
                day_of_week(local_ts) as day_order,
                CASE day_of_week(local_ts)
                    WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed'
                    WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat'
                    WHEN 7 THEN 'Sun'
                END as day_name,
                CASE
                    WHEN HOUR(local_ts) < 3 THEN 0 WHEN HOUR(local_ts) < 6 THEN 1
                    WHEN HOUR(local_ts) < 9 THEN 2 WHEN HOUR(local_ts) < 12 THEN 3
                    WHEN HOUR(local_ts) < 15 THEN 4 WHEN HOUR(local_ts) < 18 THEN 5
                    WHEN HOUR(local_ts) < 21 THEN 6 ELSE 7
                END as hour_order,
                CASE
                    WHEN HOUR(local_ts) < 3 THEN '12AM' WHEN HOUR(local_ts) < 6 THEN '3AM'
                    WHEN HOUR(local_ts) < 9 THEN '6AM' WHEN HOUR(local_ts) < 12 THEN '9AM'
                    WHEN HOUR(local_ts) < 15 THEN '12PM' WHEN HOUR(local_ts) < 18 THEN '3PM'
                    WHEN HOUR(local_ts) < 21 THEN '6PM' ELSE '9PM'
                END as hour_block,
                COUNT(DISTINCT session_id) as conversation_count
            FROM local_times
            GROUP BY 1, 2, 3, 4
        )
        ORDER BY day_order, hour_order
        """

        results = execute_athena_query(query)

        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        hour_blocks = ['12AM', '3AM', '6AM', '9AM', '12PM', '3PM', '6PM', '9PM']

        grid = {hb: {d: 0 for d in days} for hb in hour_blocks}
        total = 0
        peak = {'day': None, 'hour_block': None, 'count': 0}

        for row in (results or []):
            day = row.get('day_name')
            hb = row.get('hour_block')
            count = int(row.get('conversation_count', 0) or 0)

            if day and hb and hb in grid:
                grid[hb][day] = count
                total += count
                if count > peak['count']:
                    peak = {'day': day, 'hour_block': hb, 'count': count}

        heatmap_data = []
        for hb in hour_blocks:
            heatmap_data.append({
                'hour_block': hb,
                'data': [{'day': d, 'value': grid[hb][d]} for d in days]
            })

        heatmaps[tz] = {
            'heatmap': heatmap_data,
            'peak': peak if peak['day'] else None,
            'total_conversations': total
        }

    return heatmaps


def compute_conversations_trend(tenant_id: str, date_range: Dict, range_str: str) -> Dict[str, Any]:
    """Compute conversations trend data."""
    granularity = 'hour' if date_range['days'] <= 1 else 'day'

    if granularity == 'hour':
        query = f"""
        SELECT period, value FROM (
            SELECT
                CONCAT(CAST(HOUR(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ')) AS VARCHAR),
                    CASE WHEN HOUR(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ')) < 12 THEN 'am' ELSE 'pm' END) as period,
                HOUR(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ')) as hour_num,
                COUNT(DISTINCT session_id) as value
            FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
            WHERE tenant_id = '{tenant_id}'
              AND event_type = 'MESSAGE_SENT'
              AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                       LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                       LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
                  >= DATE '{date_range['start_date_iso']}'
            GROUP BY 1, 2
        ) ORDER BY hour_num
        """
        legend = 'Questions per hour'
    else:
        query = f"""
        SELECT period, value FROM (
            SELECT
                date_format(DATE(CONCAT(CAST(year AS VARCHAR), '-',
                     LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                     LPAD(CAST(day AS VARCHAR), 2, '0'))), '%b %d') as period,
                CAST(CONCAT(CAST(year AS VARCHAR), '-',
                     LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                     LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE) as sort_date,
                COUNT(DISTINCT session_id) as value
            FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
            WHERE tenant_id = '{tenant_id}'
              AND event_type = 'MESSAGE_SENT'
              AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                       LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                       LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
                  >= DATE '{date_range['start_date_iso']}'
            GROUP BY year, month, day
        ) ORDER BY sort_date
        """
        legend = 'Questions per day'

    results = execute_athena_query(query)

    trend = [{'period': r.get('period', ''), 'value': int(r.get('value', 0) or 0)} for r in (results or [])]

    return {'trend': trend, 'legend': legend}


def compute_top_questions(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute top questions."""
    query = f"""
    WITH first_messages AS (
        SELECT session_id,
               json_extract_scalar(event_payload, '$.content_preview') as question_text,
               ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY step_number) as rn
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'MESSAGE_SENT'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
    )
    SELECT COALESCE(SUBSTR(question_text, 1, 100), 'Unknown') as question_text,
           COUNT(*) as question_count
    FROM first_messages
    WHERE rn = 1 AND question_text IS NOT NULL AND LENGTH(question_text) > 0
    GROUP BY COALESCE(SUBSTR(question_text, 1, 100), 'Unknown')
    ORDER BY question_count DESC
    LIMIT 10
    """

    results = execute_athena_query(query)

    # Get total
    total_query = f"""
    SELECT COUNT(DISTINCT session_id) as total
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND event_type = 'MESSAGE_SENT'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
    """
    total_result = execute_athena_query(total_query)
    total = int(total_result[0].get('total', 0) or 0) if total_result else 0

    questions = []
    for row in (results or []):
        count = int(row.get('question_count', 0) or 0)
        questions.append({
            'question_text': row.get('question_text', 'Unknown'),
            'count': count,
            'percentage': round((count / total * 100) if total > 0 else 0, 1)
        })

    return {'questions': questions, 'total_questions': total}


def compute_recent_conversations(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute recent conversations (first 50)."""
    query = f"""
    WITH session_messages AS (
        SELECT session_id, event_type, client_timestamp,
               json_extract_scalar(event_payload, '$.content_preview') as content,
               CAST(json_extract_scalar(event_payload, '$.response_time_ms') AS DOUBLE) / 1000 as response_time_sec,
               ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY step_number) as msg_rank
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type IN ('MESSAGE_SENT', 'MESSAGE_RECEIVED')
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
    ),
    session_summary AS (
        SELECT session_id, MIN(client_timestamp) as started_at, COUNT(*) as message_count
        FROM session_messages
        GROUP BY session_id
    ),
    first_qa AS (
        SELECT s.session_id, s.started_at, s.message_count,
               q.content as first_question, a.content as first_answer,
               COALESCE(a.response_time_sec, 0) as response_time_seconds
        FROM session_summary s
        LEFT JOIN session_messages q ON s.session_id = q.session_id AND q.event_type = 'MESSAGE_SENT' AND q.msg_rank = 1
        LEFT JOIN session_messages a ON s.session_id = a.session_id AND a.event_type = 'MESSAGE_RECEIVED' AND a.msg_rank = 1
    )
    SELECT * FROM first_qa WHERE first_question IS NOT NULL ORDER BY started_at DESC LIMIT 50
    """

    results = execute_athena_query(query, timeout=90)

    def categorize(q):
        q = (q or '').lower()
        if 'volunteer' in q: return 'Volunteer'
        if 'donate' in q: return 'Donation'
        if 'event' in q: return 'Events'
        if 'service' in q: return 'Services'
        return 'General'

    conversations = []
    for row in (results or []):
        q = row.get('first_question', '')
        conversations.append({
            'session_id': row.get('session_id', ''),
            'started_at': row.get('started_at', ''),
            'topic': categorize(q),
            'first_question': q,
            'first_answer': row.get('first_answer', ''),
            'response_time_seconds': round(float(row.get('response_time_seconds', 0) or 0), 1),
            'message_count': int(row.get('message_count', 0) or 0)
        })

    return {'conversations': conversations, 'total_count': len(conversations)}


def compute_forms_summary(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute forms summary metrics."""
    query = f"""
    SELECT
        SUM(CASE WHEN event_type = 'FORM_VIEWED' THEN 1 ELSE 0 END) as form_views,
        SUM(CASE WHEN event_type = 'FORM_STARTED' THEN 1 ELSE 0 END) as forms_started,
        SUM(CASE WHEN event_type = 'FORM_COMPLETED' THEN 1 ELSE 0 END) as forms_completed,
        SUM(CASE WHEN event_type = 'FORM_ABANDONED' THEN 1 ELSE 0 END) as forms_abandoned,
        AVG(CASE WHEN event_type = 'FORM_COMPLETED'
            THEN CAST(json_extract_scalar(event_payload, '$.duration_seconds') AS DOUBLE)
            ELSE NULL END) as avg_completion_time
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND event_type IN ('FORM_VIEWED', 'FORM_STARTED', 'FORM_COMPLETED', 'FORM_ABANDONED')
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        views = int(row.get('form_views', 0) or 0)
        started = int(row.get('forms_started', 0) or 0)
        completed = int(row.get('forms_completed', 0) or 0)
        abandoned = int(row.get('forms_abandoned', 0) or 0)
        avg_time = float(row.get('avg_completion_time', 0) or 0)

        total_outcomes = completed + abandoned
        completion_rate = (completed / total_outcomes * 100) if total_outcomes > 0 else 0
        abandon_rate = (abandoned / total_outcomes * 100) if total_outcomes > 0 else 0

        return {
            'form_views': views,
            'forms_started': started,
            'forms_completed': completed,
            'forms_abandoned': abandoned,
            'completion_rate': round(completion_rate, 1),
            'abandon_rate': round(abandon_rate, 1),
            'avg_completion_time_seconds': round(avg_time)
        }

    return {
        'form_views': 0, 'forms_started': 0, 'forms_completed': 0,
        'forms_abandoned': 0, 'completion_rate': 0, 'abandon_rate': 0,
        'avg_completion_time_seconds': 0
    }


def compute_forms_bottlenecks(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute form field bottlenecks."""
    query = f"""
    SELECT
        json_extract_scalar(event_payload, '$.last_field_id') as field_id,
        json_extract_scalar(event_payload, '$.last_field_label') as field_label,
        json_extract_scalar(event_payload, '$.form_id') as form_id,
        COUNT(*) as abandon_count
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND event_type = 'FORM_ABANDONED'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
    GROUP BY 1, 2, 3
    ORDER BY abandon_count DESC
    LIMIT 10
    """

    results = execute_athena_query(query)

    # Get total
    total_query = f"""
    SELECT COUNT(*) as total FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND event_type = 'FORM_ABANDONED'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
    """
    total_result = execute_athena_query(total_query)
    total = int(total_result[0].get('total', 0) or 0) if total_result else 0

    bottlenecks = []
    for row in (results or []):
        field_id = row.get('field_id', 'unknown')
        field_label = row.get('field_label', field_id)
        count = int(row.get('abandon_count', 0) or 0)
        pct = round((count / total * 100) if total > 0 else 0, 1)

        insight = generate_field_insight(field_id, field_label)

        bottlenecks.append({
            'field_id': field_id,
            'field_label': field_label,
            'form_id': row.get('form_id'),
            'abandon_count': count,
            'abandon_percentage': pct,
            'insight': insight['insight'],
            'recommendation': insight['recommendation']
        })

    return {'bottlenecks': bottlenecks, 'total_abandonments': total}


def generate_field_insight(field_id: str, field_label: str) -> Dict[str, str]:
    """Generate actionable insights for form field bottlenecks."""
    field_lower = (field_id + ' ' + field_label).lower()

    if any(w in field_lower for w in ['background', 'check', 'screening']):
        return {'insight': 'Trust concerns at background check.', 'recommendation': 'Add trust badge or explanation.'}
    if any(w in field_lower for w in ['phone', 'mobile', 'cell']):
        return {'insight': 'Phone requests trigger privacy concerns.', 'recommendation': 'Explain how phone will be used.'}
    if any(w in field_lower for w in ['email']):
        return {'insight': 'Email may cause spam anxiety.', 'recommendation': 'Add "no spam" assurance.'}
    if any(w in field_lower for w in ['address', 'street', 'zip']):
        return {'insight': 'Address is high-friction.', 'recommendation': 'Defer to follow-up form if possible.'}

    return {'insight': 'Elevated abandonment on this field.', 'recommendation': 'Review placement and wording.'}


def compute_forms_top_performers(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute form performance rankings."""
    query = f"""
    WITH form_starts AS (
        SELECT json_extract_scalar(event_payload, '$.form_id') as form_id, COUNT(*) as started
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}' AND event_type = 'FORM_STARTED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0'), '-', LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE) >= DATE '{date_range['start_date_iso']}'
        GROUP BY 1
    ),
    form_completions AS (
        SELECT json_extract_scalar(event_payload, '$.form_id') as form_id,
               json_extract_scalar(event_payload, '$.form_label') as form_label,
               COUNT(*) as completions,
               AVG(CAST(json_extract_scalar(event_payload, '$.duration_seconds') AS DOUBLE)) as avg_duration
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}' AND event_type = 'FORM_COMPLETED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0'), '-', LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE) >= DATE '{date_range['start_date_iso']}'
        GROUP BY 1, 2
    ),
    form_abandons AS (
        SELECT json_extract_scalar(event_payload, '$.form_id') as form_id, COUNT(*) as abandons
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}' AND event_type = 'FORM_ABANDONED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0'), '-', LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE) >= DATE '{date_range['start_date_iso']}'
        GROUP BY 1
    )
    SELECT
        COALESCE(c.form_id, s.form_id, a.form_id) as form_id,
        c.form_label, COALESCE(s.started, 0) as started, COALESCE(c.completions, 0) as completions,
        COALESCE(a.abandons, 0) as abandons, COALESCE(c.avg_duration, 0) as avg_completion_time,
        CASE WHEN (COALESCE(c.completions, 0) + COALESCE(a.abandons, 0)) > 0
            THEN ROUND(CAST(COALESCE(c.completions, 0) AS DOUBLE) / CAST(COALESCE(c.completions, 0) + COALESCE(a.abandons, 0) AS DOUBLE) * 100, 1)
            ELSE 0 END as conversion_rate
    FROM form_completions c
    FULL OUTER JOIN form_starts s ON c.form_id = s.form_id
    FULL OUTER JOIN form_abandons a ON COALESCE(c.form_id, s.form_id) = a.form_id
    WHERE COALESCE(c.form_id, s.form_id, a.form_id) IS NOT NULL
    ORDER BY conversion_rate DESC LIMIT 10
    """

    results = execute_athena_query(query)

    forms = []
    total_completions = 0
    for row in (results or []):
        completions = int(row.get('completions', 0) or 0)
        total_completions += completions
        rate = float(row.get('conversion_rate', 0) or 0)

        forms.append({
            'form_id': row.get('form_id', ''),
            'form_label': row.get('form_label', row.get('form_id', 'Unknown')),
            'started': int(row.get('started', 0) or 0),
            'completions': completions,
            'conversion_rate': rate,
            'avg_completion_time_seconds': round(float(row.get('avg_completion_time', 0) or 0)),
            'trend': 'trending' if rate >= 70 else ('stable' if rate >= 40 else 'low')
        })

    return {'forms': forms, 'total_completions': total_completions}


def compute_analytics_summary(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute overall analytics summary."""
    query = f"""
    SELECT
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(*) as total_events,
        COUNT(CASE WHEN event_type = 'WIDGET_OPENED' THEN 1 END) as widget_opens,
        COUNT(CASE WHEN event_type = 'FORM_STARTED' THEN 1 END) as forms_started,
        COUNT(CASE WHEN event_type = 'FORM_COMPLETED' THEN 1 END) as forms_completed,
        COUNT(CASE WHEN event_type = 'ACTION_CHIP_CLICKED' THEN 1 END) as chip_clicks,
        COUNT(CASE WHEN event_type = 'CTA_CLICKED' THEN 1 END) as cta_clicks
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        opens = int(row.get('widget_opens', 0) or 0)
        completed = int(row.get('forms_completed', 0) or 0)
        rate = (completed / opens * 100) if opens > 0 else 0

        return {
            'total_sessions': int(row.get('total_sessions', 0) or 0),
            'total_events': int(row.get('total_events', 0) or 0),
            'widget_opens': opens,
            'forms_started': int(row.get('forms_started', 0) or 0),
            'forms_completed': completed,
            'chip_clicks': int(row.get('chip_clicks', 0) or 0),
            'cta_clicks': int(row.get('cta_clicks', 0) or 0),
            'conversion_rate': round(rate, 2)
        }

    return {
        'total_sessions': 0, 'total_events': 0, 'widget_opens': 0,
        'forms_started': 0, 'forms_completed': 0, 'chip_clicks': 0,
        'cta_clicks': 0, 'conversion_rate': 0
    }


def compute_analytics_funnel(tenant_id: str, date_range: Dict) -> Dict[str, Any]:
    """Compute conversion funnel."""
    query = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN event_type = 'WIDGET_OPENED' THEN session_id END) as stage1,
        COUNT(DISTINCT CASE WHEN event_type = 'ACTION_CHIP_CLICKED' THEN session_id END) as stage2,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_STARTED' THEN session_id END) as stage3,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_COMPLETED' THEN session_id END) as stage4
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        s1 = int(row.get('stage1', 0) or 0)
        s2 = int(row.get('stage2', 0) or 0)
        s3 = int(row.get('stage3', 0) or 0)
        s4 = int(row.get('stage4', 0) or 0)

        return {
            'funnel': [
                {'stage': 'Widget Opened', 'count': s1, 'rate': 100.0},
                {'stage': 'Chip Clicked', 'count': s2, 'rate': round((s2/s1*100) if s1 > 0 else 0, 2)},
                {'stage': 'Form Started', 'count': s3, 'rate': round((s3/s1*100) if s1 > 0 else 0, 2)},
                {'stage': 'Form Completed', 'count': s4, 'rate': round((s4/s1*100) if s1 > 0 else 0, 2)}
            ],
            'overall_conversion': round((s4/s1*100) if s1 > 0 else 0, 2)
        }

    return {'funnel': [], 'overall_conversion': 0}
