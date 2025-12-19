"""
Analytics Dashboard API Lambda

Provides REST API endpoints for querying analytics data from Athena.
Used by the Picasso Config Builder dashboard to display tenant metrics.

Endpoints:
- GET /analytics/summary    - Overview metrics (sessions, events, forms)
- GET /analytics/sessions   - Session counts over time
- GET /analytics/events     - Event breakdown by type
- GET /analytics/funnel     - Conversion funnel analysis

Authentication:
- JWT token in Authorization header (Bearer token)
- Token contains tenant_id for data isolation

Environment Variables:
- ATHENA_DATABASE: Athena database name (default: picasso_analytics)
- ATHENA_OUTPUT_LOCATION: S3 location for query results
- JWT_SECRET_KEY_NAME: Secrets Manager key name for JWT secret
"""

import json
import os
import logging
import time
import boto3
import hashlib
import hmac
import base64
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'picasso_analytics')
ATHENA_TABLE = os.environ.get('ATHENA_TABLE', 'events')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION', 's3://picasso-analytics/athena-results/')
JWT_SECRET_KEY_NAME = os.environ.get('JWT_SECRET_KEY_NAME', 'picasso/staging/jwt/signing-key')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# AWS clients
athena = boto3.client('athena')
secrets_manager = boto3.client('secretsmanager')

# Cache for JWT secret
_jwt_secret_cache = None
_jwt_secret_cache_time = 0
JWT_SECRET_CACHE_TTL = 300  # 5 minutes


def lambda_handler(event, context):
    """
    Main handler - routes requests to appropriate endpoint handlers.
    Supports both API Gateway and Lambda Function URL event formats.
    """
    # Handle both API Gateway and Function URL event formats
    if 'requestContext' in event and 'http' in event.get('requestContext', {}):
        # Lambda Function URL format
        http_context = event['requestContext']['http']
        method = http_context.get('method', 'GET')
        path = event.get('rawPath', '')
    else:
        # API Gateway format
        method = event.get('httpMethod', 'GET')
        path = event.get('path', '')

    logger.info(f"Analytics API request: {method} {path}")

    # Handle CORS preflight
    if method == 'OPTIONS':
        return cors_response(200, {})

    # Authenticate request
    auth_result = authenticate_request(event)
    if not auth_result['success']:
        return cors_response(401, {'error': auth_result['error']})

    tenant_id = auth_result['tenant_id']
    logger.info(f"Authenticated request for tenant: {tenant_id[:8]}...")

    # Parse query parameters
    params = event.get('queryStringParameters') or {}

    # Route to appropriate handler
    try:
        if path.endswith('/summary'):
            return handle_summary(tenant_id, params)
        elif path.endswith('/sessions'):
            return handle_sessions(tenant_id, params)
        elif path.endswith('/events'):
            return handle_events(tenant_id, params)
        elif path.endswith('/funnel'):
            return handle_funnel(tenant_id, params)
        else:
            return cors_response(404, {'error': f'Unknown endpoint: {path}'})

    except Exception as e:
        logger.exception(f"Error handling request: {e}")
        return cors_response(500, {'error': 'Internal server error', 'details': str(e)})


# =============================================================================
# Authentication
# =============================================================================

def authenticate_request(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Authenticate request using JWT token from Authorization header.
    Returns {'success': True, 'tenant_id': '...'} or {'success': False, 'error': '...'}
    """
    # Get Authorization header
    headers = event.get('headers', {}) or {}
    auth_header = headers.get('Authorization') or headers.get('authorization', '')

    if not auth_header:
        return {'success': False, 'error': 'Missing Authorization header'}

    # Extract token
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    else:
        token = auth_header

    # Validate JWT
    try:
        payload = validate_jwt(token)
        tenant_id = payload.get('tenant_id') or payload.get('sub')

        if not tenant_id:
            return {'success': False, 'error': 'Token missing tenant_id'}

        return {'success': True, 'tenant_id': tenant_id}

    except Exception as e:
        logger.warning(f"JWT validation failed: {e}")
        return {'success': False, 'error': f'Invalid token: {str(e)}'}


def get_jwt_secret() -> str:
    """Get JWT secret from Secrets Manager with caching."""
    global _jwt_secret_cache, _jwt_secret_cache_time

    now = time.time()
    if _jwt_secret_cache and (now - _jwt_secret_cache_time) < JWT_SECRET_CACHE_TTL:
        return _jwt_secret_cache

    try:
        response = secrets_manager.get_secret_value(SecretId=JWT_SECRET_KEY_NAME)
        secret = response.get('SecretString', '')

        # Handle JSON-wrapped secrets
        try:
            secret_data = json.loads(secret)
            # Try common key names
            secret = secret_data.get('signingKey') or secret_data.get('key') or secret_data.get('secret') or secret
        except json.JSONDecodeError:
            pass

        _jwt_secret_cache = secret
        _jwt_secret_cache_time = now
        return secret

    except ClientError as e:
        logger.error(f"Failed to get JWT secret: {e}")
        raise


def validate_jwt(token: str) -> Dict[str, Any]:
    """
    Validate JWT token and return payload.
    Simple HS256 validation without external libraries.
    """
    parts = token.split('.')
    if len(parts) != 3:
        raise ValueError('Invalid token format')

    header_b64, payload_b64, signature_b64 = parts

    # Decode payload
    payload_json = base64.urlsafe_b64decode(payload_b64 + '==')
    payload = json.loads(payload_json)

    # Check expiration
    exp = payload.get('exp')
    if exp and time.time() > exp:
        raise ValueError('Token expired')

    # Verify signature
    secret = get_jwt_secret()
    message = f"{header_b64}.{payload_b64}".encode('utf-8')
    expected_sig = hmac.new(secret.encode('utf-8'), message, hashlib.sha256).digest()
    expected_sig_b64 = base64.urlsafe_b64encode(expected_sig).rstrip(b'=').decode('utf-8')

    # Normalize signature (remove padding)
    actual_sig_b64 = signature_b64.rstrip('=')

    if not hmac.compare_digest(actual_sig_b64, expected_sig_b64):
        raise ValueError('Invalid signature')

    return payload


# =============================================================================
# Endpoint Handlers
# =============================================================================

def handle_summary(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /analytics/summary
    Returns overview metrics for the dashboard.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    """
    date_range = parse_date_range(params.get('range', '30d'))

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
      AND year >= {date_range['start_year']}
      AND month >= {date_range['start_month']}
      AND day >= {date_range['start_day']}
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        # Calculate conversion rate
        widget_opens = int(row.get('widget_opens', 0) or 0)
        forms_completed = int(row.get('forms_completed', 0) or 0)
        conversion_rate = (forms_completed / widget_opens * 100) if widget_opens > 0 else 0

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': params.get('range', '30d'),
            'metrics': {
                'total_sessions': int(row.get('total_sessions', 0) or 0),
                'total_events': int(row.get('total_events', 0) or 0),
                'widget_opens': widget_opens,
                'forms_started': int(row.get('forms_started', 0) or 0),
                'forms_completed': forms_completed,
                'chip_clicks': int(row.get('chip_clicks', 0) or 0),
                'cta_clicks': int(row.get('cta_clicks', 0) or 0),
                'conversion_rate': round(conversion_rate, 2)
            }
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'metrics': {
            'total_sessions': 0,
            'total_events': 0,
            'widget_opens': 0,
            'forms_started': 0,
            'forms_completed': 0,
            'chip_clicks': 0,
            'cta_clicks': 0,
            'conversion_rate': 0
        }
    })


def handle_sessions(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /analytics/sessions
    Returns session counts over time.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - granularity: day, week, month - default day
    """
    date_range = parse_date_range(params.get('range', '30d'))
    granularity = params.get('granularity', 'day')

    # Build GROUP BY based on granularity
    if granularity == 'month':
        group_by = "year, month"
        select_date = "CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') as period"
    elif granularity == 'week':
        group_by = "year, week(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ'))"
        select_date = "CAST(year AS VARCHAR) || '-W' || LPAD(CAST(week(date_parse(client_timestamp, '%Y-%m-%dT%H:%i:%s.%fZ')) AS VARCHAR), 2, '0') as period"
    else:  # day
        group_by = "year, month, day"
        select_date = "CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') || '-' || LPAD(CAST(day AS VARCHAR), 2, '0') as period"

    query = f"""
    SELECT
        {select_date},
        COUNT(DISTINCT session_id) as sessions,
        COUNT(*) as events
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND year >= {date_range['start_year']}
      AND month >= {date_range['start_month']}
    GROUP BY {group_by}
    ORDER BY period
    """

    results = execute_athena_query(query)

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'granularity': granularity,
        'data': [
            {
                'period': row.get('period'),
                'sessions': int(row.get('sessions', 0) or 0),
                'events': int(row.get('events', 0) or 0)
            }
            for row in (results or [])
        ]
    })


def handle_events(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /analytics/events
    Returns event breakdown by type.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - type: Filter by specific event type (optional)
    """
    date_range = parse_date_range(params.get('range', '30d'))
    event_type_filter = params.get('type')

    type_clause = f"AND event_type = '{event_type_filter}'" if event_type_filter else ""

    query = f"""
    SELECT
        event_type,
        COUNT(*) as count,
        COUNT(DISTINCT session_id) as unique_sessions
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND year >= {date_range['start_year']}
      AND month >= {date_range['start_month']}
      {type_clause}
    GROUP BY event_type
    ORDER BY count DESC
    """

    results = execute_athena_query(query)

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'events': [
            {
                'type': row.get('event_type'),
                'count': int(row.get('count', 0) or 0),
                'unique_sessions': int(row.get('unique_sessions', 0) or 0)
            }
            for row in (results or [])
        ]
    })


def handle_funnel(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /analytics/funnel
    Returns conversion funnel analysis.

    Funnel stages:
    1. Widget Opened
    2. Action Chip Clicked (optional)
    3. Form Started
    4. Form Completed

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    """
    date_range = parse_date_range(params.get('range', '30d'))

    query = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN event_type = 'WIDGET_OPENED' THEN session_id END) as stage1_widget_opened,
        COUNT(DISTINCT CASE WHEN event_type = 'ACTION_CHIP_CLICKED' THEN session_id END) as stage2_chip_clicked,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_STARTED' THEN session_id END) as stage3_form_started,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_COMPLETED' THEN session_id END) as stage4_form_completed
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND year >= {date_range['start_year']}
      AND month >= {date_range['start_month']}
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        stage1 = int(row.get('stage1_widget_opened', 0) or 0)
        stage2 = int(row.get('stage2_chip_clicked', 0) or 0)
        stage3 = int(row.get('stage3_form_started', 0) or 0)
        stage4 = int(row.get('stage4_form_completed', 0) or 0)

        funnel = [
            {
                'stage': 'Widget Opened',
                'count': stage1,
                'rate': 100.0
            },
            {
                'stage': 'Chip Clicked',
                'count': stage2,
                'rate': round((stage2 / stage1 * 100) if stage1 > 0 else 0, 2)
            },
            {
                'stage': 'Form Started',
                'count': stage3,
                'rate': round((stage3 / stage1 * 100) if stage1 > 0 else 0, 2)
            },
            {
                'stage': 'Form Completed',
                'count': stage4,
                'rate': round((stage4 / stage1 * 100) if stage1 > 0 else 0, 2)
            }
        ]

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': params.get('range', '30d'),
            'funnel': funnel,
            'overall_conversion': round((stage4 / stage1 * 100) if stage1 > 0 else 0, 2)
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'funnel': [],
        'overall_conversion': 0
    })


# =============================================================================
# Athena Query Helpers
# =============================================================================

def execute_athena_query(query: str, timeout: int = 30) -> Optional[List[Dict[str, Any]]]:
    """
    Execute Athena query and return results as list of dicts.
    """
    logger.info(f"Executing Athena query: {query[:200]}...")

    try:
        # Start query execution
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        query_id = response['QueryExecutionId']

        # Wait for query to complete
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']

            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"Athena query failed: {error}")
                return None

            time.sleep(0.5)
        else:
            logger.error("Athena query timed out")
            return None

        # Get results
        results = athena.get_query_results(QueryExecutionId=query_id)

        # Parse results into list of dicts
        rows = results.get('ResultSet', {}).get('Rows', [])
        if len(rows) < 2:
            return []

        # First row is headers
        headers = [col.get('VarCharValue', '') for col in rows[0].get('Data', [])]

        # Parse data rows
        data = []
        for row in rows[1:]:
            row_data = {}
            for i, col in enumerate(row.get('Data', [])):
                if i < len(headers):
                    row_data[headers[i]] = col.get('VarCharValue')
            data.append(row_data)

        logger.info(f"Query returned {len(data)} rows")
        return data

    except Exception as e:
        logger.exception(f"Athena query error: {e}")
        return None


def parse_date_range(range_str: str) -> Dict[str, int]:
    """
    Parse date range string (7d, 30d, 90d) into date components.
    """
    days = 30  # default
    if range_str.endswith('d'):
        try:
            days = int(range_str[:-1])
        except ValueError:
            pass

    start_date = datetime.utcnow() - timedelta(days=days)

    return {
        'start_year': start_date.year,
        'start_month': start_date.month,
        'start_day': start_date.day,
        'days': days
    }


# =============================================================================
# Response Helpers
# =============================================================================

def cors_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Build response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        },
        'body': json.dumps(body)
    }
