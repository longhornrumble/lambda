"""
Analytics Dashboard API Lambda

Provides REST API endpoints for querying analytics data from Athena.
Used by the Picasso Analytics Dashboard to display tenant metrics.

Endpoints:
- GET /analytics/summary    - Overview metrics (sessions, events, forms)
- GET /analytics/sessions   - Session counts over time
- GET /analytics/events     - Event breakdown by type
- GET /analytics/funnel     - Conversion funnel analysis
- GET /forms/bottlenecks    - Field-level abandonment analysis
- GET /forms/submissions    - Recent form submissions (paginated)
- GET /forms/top-performers - Form performance rankings

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
import re
import boto3
import hashlib
import hmac
import base64
from datetime import datetime, timedelta, timezone
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
dynamodb = boto3.client('dynamodb')

# DynamoDB table for form submissions (contains PII)
FORM_SUBMISSIONS_TABLE = os.environ.get('FORM_SUBMISSIONS_TABLE', 'picasso_form_submissions')

# Cache for JWT secret
_jwt_secret_cache = None
_jwt_secret_cache_time = 0
JWT_SECRET_CACHE_TTL = 300  # 5 minutes

# Security: Allowed event types whitelist
ALLOWED_EVENT_TYPES = {
    'WIDGET_OPENED', 'WIDGET_CLOSED', 'MESSAGE_SENT', 'FORM_STARTED',
    'FORM_COMPLETED', 'FORM_ABANDONED', 'FORM_VIEWED', 'FORM_FIELD_SUBMITTED',
    'ACTION_CHIP_CLICKED', 'CTA_CLICKED', 'LINK_CLICKED', 'HELP_MENU_CLICKED',
    'SHOWCASE_CTA_CLICKED', 'MESSAGE_RECEIVED', 'CONVERSATION_STARTED',
    'SESSION_STARTED', 'SESSION_ENDED', 'ERROR'
}

# Security: Tenant ID validation pattern (alphanumeric, underscore, hyphen only)
TENANT_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]+$')


def sanitize_tenant_id(tenant_id: str) -> str:
    """
    Validate tenant_id is safe for SQL interpolation.
    Prevents SQL injection by ensuring only alphanumeric characters.

    Raises ValueError if tenant_id is invalid.
    """
    if not tenant_id:
        raise ValueError("tenant_id is required")

    if len(tenant_id) > 50:
        raise ValueError("tenant_id too long (max 50 chars)")

    if not TENANT_ID_PATTERN.match(tenant_id):
        raise ValueError(f"Invalid tenant_id format: must be alphanumeric")

    return tenant_id


def sanitize_event_type(event_type: str) -> str:
    """
    Validate event_type against whitelist.
    Prevents SQL injection by only allowing known event types.

    Raises ValueError if event_type is not in whitelist.
    """
    if not event_type:
        return None

    if event_type not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"Invalid event type: {event_type}")

    return event_type


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

    # Sanitize tenant_id to prevent SQL injection
    try:
        tenant_id = sanitize_tenant_id(auth_result['tenant_id'])
    except ValueError as e:
        logger.warning(f"Invalid tenant_id in token: {e}")
        return cors_response(400, {'error': str(e)})

    logger.info(f"Authenticated request for tenant: {tenant_id[:8]}...")

    # Parse query parameters
    params = event.get('queryStringParameters') or {}

    # Route to appropriate handler
    # NOTE: More specific routes must come before generic ones
    # (e.g., /forms/summary before /summary)
    try:
        # Forms endpoints (more specific - check first)
        if path.endswith('/forms/summary'):
            return handle_form_summary(tenant_id, params)
        elif path.endswith('/bottlenecks'):
            return handle_form_bottlenecks(tenant_id, params)
        elif path.endswith('/submissions'):
            return handle_form_submissions(tenant_id, params)
        elif path.endswith('/top-performers'):
            return handle_form_top_performers(tenant_id, params)
        # Analytics endpoints (generic)
        elif path.endswith('/summary'):
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

    # Use ISO date comparison for proper cross-month-boundary filtering
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

    # Use ISO date comparison for proper cross-month-boundary filtering
    query = f"""
    SELECT
        {select_date},
        COUNT(DISTINCT session_id) as sessions,
        COUNT(*) as events
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
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
    - type: Filter by specific event type (optional, must be in whitelist)
    """
    date_range = parse_date_range(params.get('range', '30d'))

    # Validate event_type against whitelist to prevent SQL injection
    event_type_filter = params.get('type')
    if event_type_filter:
        try:
            event_type_filter = sanitize_event_type(event_type_filter)
        except ValueError as e:
            return cors_response(400, {'error': str(e), 'allowed_types': list(ALLOWED_EVENT_TYPES)})

    type_clause = f"AND event_type = '{event_type_filter}'" if event_type_filter else ""

    # Use ISO date comparison for proper cross-month-boundary filtering
    query = f"""
    SELECT
        event_type,
        COUNT(*) as count,
        COUNT(DISTINCT session_id) as unique_sessions
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
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

    # Use ISO date comparison for proper cross-month-boundary filtering
    query = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN event_type = 'WIDGET_OPENED' THEN session_id END) as stage1_widget_opened,
        COUNT(DISTINCT CASE WHEN event_type = 'ACTION_CHIP_CLICKED' THEN session_id END) as stage2_chip_clicked,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_STARTED' THEN session_id END) as stage3_form_started,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_COMPLETED' THEN session_id END) as stage4_form_completed
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
# Forms Endpoint Handlers
# =============================================================================

def handle_form_summary(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /forms/summary
    Returns form-specific summary metrics.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - form_id: Filter by specific form (optional)
    """
    date_range = parse_date_range(params.get('range', '30d'))
    form_id = params.get('form_id')

    # Build optional form filter
    form_filter = ""
    if form_id:
        if not TENANT_ID_PATTERN.match(form_id):
            return cors_response(400, {'error': 'Invalid form_id format'})
        form_filter = f"AND json_extract_scalar(event_payload, '$.form_id') = '{form_id}'"

    # Query for form metrics using a single aggregation query
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
      {form_filter}
    """

    results = execute_athena_query(query)

    if results and len(results) > 0:
        row = results[0]
        form_views = int(row.get('form_views', 0) or 0)
        forms_started = int(row.get('forms_started', 0) or 0)
        forms_completed = int(row.get('forms_completed', 0) or 0)
        forms_abandoned = int(row.get('forms_abandoned', 0) or 0)
        avg_completion_time = float(row.get('avg_completion_time', 0) or 0)

        # Calculate rates based on total outcomes (completed + abandoned)
        # This is more accurate than using forms_started because:
        # 1. Same session can start multiple times (retry after abandon)
        # 2. Rates should add up to 100% for clarity
        total_outcomes = forms_completed + forms_abandoned
        completion_rate = (forms_completed / total_outcomes * 100) if total_outcomes > 0 else 0
        abandon_rate = (forms_abandoned / total_outcomes * 100) if total_outcomes > 0 else 0

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': params.get('range', '30d'),
            'metrics': {
                'form_views': form_views,
                'forms_started': forms_started,
                'forms_completed': forms_completed,
                'forms_abandoned': forms_abandoned,
                'completion_rate': round(completion_rate, 1),
                'abandon_rate': round(abandon_rate, 1),
                'avg_completion_time_seconds': round(avg_completion_time)
            }
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'metrics': {
            'form_views': 0,
            'forms_started': 0,
            'forms_completed': 0,
            'forms_abandoned': 0,
            'completion_rate': 0,
            'abandon_rate': 0,
            'avg_completion_time_seconds': 0
        }
    })


def handle_form_bottlenecks(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /forms/bottlenecks
    Returns field-level abandonment analysis.

    Shows which form fields cause the most drop-offs.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - form_id: Filter by specific form (optional)
    - limit: Number of results (default 5, max 20)
    """
    date_range = parse_date_range(params.get('range', '30d'))
    limit = min(int(params.get('limit', '5')), 20)
    form_id = params.get('form_id')

    # Build optional form filter
    form_filter = ""
    if form_id:
        # Sanitize form_id (alphanumeric, underscore, hyphen only)
        if not TENANT_ID_PATTERN.match(form_id):
            return cors_response(400, {'error': 'Invalid form_id format'})
        form_filter = f"AND json_extract_scalar(event_payload, '$.form_id') = '{form_id}'"

    # Query FORM_ABANDONED events, group by last_field_id
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
      {form_filter}
    GROUP BY
        json_extract_scalar(event_payload, '$.last_field_id'),
        json_extract_scalar(event_payload, '$.last_field_label'),
        json_extract_scalar(event_payload, '$.form_id')
    ORDER BY abandon_count DESC
    LIMIT {limit}
    """

    results = execute_athena_query(query)

    # Calculate total abandonments for percentage
    total_query = f"""
    SELECT COUNT(*) as total
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{tenant_id}'
      AND event_type = 'FORM_ABANDONED'
      AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
               LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
               LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
          >= DATE '{date_range['start_date_iso']}'
      {form_filter}
    """

    total_result = execute_athena_query(total_query)
    total_abandons = int(total_result[0].get('total', 0) or 0) if total_result else 0

    # Generate insights based on field characteristics
    bottlenecks = []
    for row in (results or []):
        field_id = row.get('field_id', 'unknown')
        field_label = row.get('field_label', field_id)
        abandon_count = int(row.get('abandon_count', 0) or 0)
        abandon_pct = round((abandon_count / total_abandons * 100) if total_abandons > 0 else 0, 1)

        # Generate insight based on field characteristics
        insight = generate_field_insight(field_id, field_label)

        bottlenecks.append({
            'field_id': field_id,
            'field_label': field_label,
            'form_id': row.get('form_id'),
            'abandon_count': abandon_count,
            'abandon_percentage': abandon_pct,
            'insight': insight['insight'],
            'recommendation': insight['recommendation']
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'bottlenecks': bottlenecks,
        'total_abandonments': total_abandons
    })


def generate_field_insight(field_id: str, field_label: str) -> Dict[str, str]:
    """
    Generate actionable insights based on field characteristics.
    Rule-based pattern matching for common abandonment reasons.
    """
    field_lower = (field_id + ' ' + field_label).lower()

    # Pattern matching for common abandonment reasons
    if any(word in field_lower for word in ['background', 'check', 'screening']):
        return {
            'insight': 'Trust and privacy concerns may cause hesitation at background check fields.',
            'recommendation': 'Add a trust badge or explanatory text: "Background checks help us ensure safety and are required by regulations."'
        }

    if any(word in field_lower for word in ['phone', 'tel', 'mobile', 'cell']):
        return {
            'insight': 'Phone number requests often trigger privacy concerns.',
            'recommendation': 'Add reassuring text: "We\'ll only call to schedule your orientation."'
        }

    if any(word in field_lower for word in ['email', 'e-mail']):
        return {
            'insight': 'Email requests may cause spam anxiety.',
            'recommendation': 'Add text: "We\'ll never share your email or send spam."'
        }

    if any(word in field_lower for word in ['address', 'street', 'city', 'zip', 'postal']):
        return {
            'insight': 'Address fields are perceived as high-friction and raise privacy concerns.',
            'recommendation': 'Consider if full address is needed upfront, or defer to a follow-up form after initial contact.'
        }

    if any(word in field_lower for word in ['ssn', 'social security', 'tax', 'ein']):
        return {
            'insight': 'Sensitive financial identifiers cause significant abandonment.',
            'recommendation': 'Move to a later stage after trust is established, or explain why it\'s required with security assurances.'
        }

    if any(word in field_lower for word in ['reference', 'referral', 'recommend']):
        return {
            'insight': 'Reference requirements add friction and require preparation.',
            'recommendation': 'Consider making optional, or allow submission to be completed later with references.'
        }

    if any(word in field_lower for word in ['age', 'birth', 'dob', 'date of birth']):
        return {
            'insight': 'Age verification may filter out users who don\'t meet requirements.',
            'recommendation': 'Show age requirements earlier in the process to set expectations.'
        }

    if any(word in field_lower for word in ['consent', 'agree', 'terms', 'policy']):
        return {
            'insight': 'Legal consent fields require users to pause and review terms.',
            'recommendation': 'Keep consent text brief and scannable. Link to full terms rather than displaying inline.'
        }

    # Default insight
    return {
        'insight': 'This field has elevated abandonment compared to others in the form.',
        'recommendation': 'Review field placement, wording, and whether it\'s essential at this stage of the process.'
    }


def handle_form_submissions(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /forms/submissions
    Returns recent form submissions with pagination from DynamoDB.

    This queries the picasso_form_submissions table which contains actual
    form data including PII (names, emails, etc.).

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - form_id: Filter by specific form (optional)
    - page: Page number (default 1)
    - limit: Results per page (default 25, max 100)
    - search: Search query for name/email (optional)
    """
    date_range = parse_date_range(params.get('range', '30d'))
    page = max(1, int(params.get('page', '1')))
    limit = min(int(params.get('limit', '25')), 100)
    form_id_filter = params.get('form_id')
    search = params.get('search', '').strip().lower()

    # Query DynamoDB using tenant-timestamp-index
    try:
        query_params = {
            'TableName': FORM_SUBMISSIONS_TABLE,
            'IndexName': 'tenant-timestamp-index',
            'KeyConditionExpression': 'tenant_id = :tid AND #ts >= :start_date',
            'ExpressionAttributeNames': {'#ts': 'timestamp'},
            'ExpressionAttributeValues': {
                ':tid': {'S': tenant_id},
                ':start_date': {'S': date_range['start_date_iso']}
            },
            'ScanIndexForward': False,  # Descending order (newest first)
            'Limit': 200  # Get more items to allow for filtering
        }

        # Add form_id filter if specified
        if form_id_filter:
            if not TENANT_ID_PATTERN.match(form_id_filter):
                return cors_response(400, {'error': 'Invalid form_id format'})
            query_params['FilterExpression'] = 'form_id = :fid'
            query_params['ExpressionAttributeValues'][':fid'] = {'S': form_id_filter}

        response = dynamodb.query(**query_params)
        items = response.get('Items', [])

    except Exception as e:
        logger.error(f"DynamoDB query error: {e}")
        return cors_response(500, {'error': 'Failed to query submissions'})

    # Process and filter submissions
    all_submissions = []
    for item in items:
        # Extract basic fields
        submission_id = item.get('submission_id', {}).get('S', '')
        session_id = item.get('session_id', {}).get('S', '')
        form_id = item.get('form_id', {}).get('S', '')
        form_title = item.get('form_title', {}).get('S', form_id)
        submitted_at = item.get('submitted_at', {}).get('S', '')

        # Extract name and email from form_data
        name, email = extract_name_email_from_form_data(item.get('form_data', {}))

        # Apply search filter
        if search:
            search_fields = f"{name} {email} {form_title}".lower()
            if search not in search_fields:
                continue

        # Format date
        try:
            dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
            formatted_date = dt.strftime('%b %d')
        except (ValueError, AttributeError):
            formatted_date = submitted_at[:10] if submitted_at else 'Unknown'

        all_submissions.append({
            'submission_id': submission_id,
            'session_id': session_id,
            'form_id': form_id,
            'form_label': form_title,
            'submitted_at': submitted_at,
            'submitted_date': formatted_date,
            'duration_seconds': 0,  # Not stored in DynamoDB
            'fields_completed': 0,  # Not stored in DynamoDB
            'fields': {
                'name': name,
                'email': email
            }
        })

    # Apply pagination
    total_count = len(all_submissions)
    total_pages = (total_count + limit - 1) // limit if limit > 0 else 0
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_submissions = all_submissions[start_idx:end_idx]

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'submissions': paginated_submissions,
        'pagination': {
            'total_count': total_count,
            'page': page,
            'limit': limit,
            'total_pages': total_pages,
            'has_next_page': page < total_pages,
            'has_previous_page': page > 1
        }
    })


def extract_name_email_from_form_data(form_data: Dict) -> tuple:
    """
    Extract name and email from DynamoDB form_data structure.
    Form data contains nested field values with dynamic field IDs.
    """
    name_parts = []
    email = ''

    # Get the nested map
    data_map = form_data.get('M', {})

    for field_id, field_value in data_map.items():
        if not isinstance(field_value, dict):
            continue

        # Check for nested map (compound fields like name with first/last)
        if 'M' in field_value:
            nested_map = field_value['M']
            for sub_key, sub_val in nested_map.items():
                if isinstance(sub_val, dict) and 'S' in sub_val:
                    val = sub_val['S']
                    sub_key_lower = sub_key.lower()
                    if 'first_name' in sub_key_lower:
                        name_parts.insert(0, val)  # First name at start
                    elif 'last_name' in sub_key_lower:
                        name_parts.append(val)  # Last name at end
                    elif 'name' in sub_key_lower and val:
                        name_parts.append(val)

        # Check for simple string value
        elif 'S' in field_value:
            val = field_value['S']
            field_id_lower = field_id.lower()

            # Look for email
            if '@' in val and '.' in val:
                email = val
            # Look for name fields by field ID pattern
            elif 'name' in field_id_lower and not name_parts:
                name_parts.append(val)

    name = ' '.join(filter(None, name_parts)).strip()
    return (name if name else 'Anonymous', email)


def handle_form_top_performers(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /forms/top-performers
    Returns form performance rankings by conversion rate.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - limit: Number of results (default 5, max 20)
    - sort_by: Sort field (conversion_rate, completions, avg_time) - default conversion_rate
    """
    date_range = parse_date_range(params.get('range', '30d'))
    limit = min(int(params.get('limit', '5')), 20)
    sort_by = params.get('sort_by', 'conversion_rate')

    # Validate sort_by
    valid_sorts = {'conversion_rate', 'completions', 'avg_time'}
    if sort_by not in valid_sorts:
        sort_by = 'conversion_rate'

    # Map sort field to SQL column
    sort_column = {
        'conversion_rate': 'conversion_rate',
        'completions': 'completions',
        'avg_time': 'avg_completion_time'
    }.get(sort_by, 'conversion_rate')

    # Query form stats - get views, starts, completions, and abandons per form
    query = f"""
    WITH form_starts AS (
        SELECT
            json_extract_scalar(event_payload, '$.form_id') as form_id,
            COUNT(*) as started
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'FORM_STARTED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
        GROUP BY json_extract_scalar(event_payload, '$.form_id')
    ),
    form_completions AS (
        SELECT
            json_extract_scalar(event_payload, '$.form_id') as form_id,
            json_extract_scalar(event_payload, '$.form_label') as form_label,
            COUNT(*) as completions,
            AVG(CAST(json_extract_scalar(event_payload, '$.duration_seconds') AS DOUBLE)) as avg_duration
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'FORM_COMPLETED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
        GROUP BY
            json_extract_scalar(event_payload, '$.form_id'),
            json_extract_scalar(event_payload, '$.form_label')
    ),
    form_abandons AS (
        SELECT
            json_extract_scalar(event_payload, '$.form_id') as form_id,
            COUNT(*) as abandons
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'FORM_ABANDONED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
        GROUP BY json_extract_scalar(event_payload, '$.form_id')
    ),
    form_views AS (
        SELECT
            json_extract_scalar(event_payload, '$.form_id') as form_id,
            COUNT(*) as views
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE tenant_id = '{tenant_id}'
          AND event_type = 'FORM_VIEWED'
          AND CAST(CONCAT(CAST(year AS VARCHAR), '-',
                   LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                   LPAD(CAST(day AS VARCHAR), 2, '0')) AS DATE)
              >= DATE '{date_range['start_date_iso']}'
        GROUP BY json_extract_scalar(event_payload, '$.form_id')
    )
    SELECT
        COALESCE(c.form_id, s.form_id, a.form_id, v.form_id) as form_id,
        c.form_label,
        COALESCE(v.views, 0) as views,
        COALESCE(s.started, 0) as started,
        COALESCE(c.completions, 0) as completions,
        COALESCE(a.abandons, 0) as abandons,
        COALESCE(c.avg_duration, 0) as avg_completion_time,
        CASE
            WHEN (COALESCE(c.completions, 0) + COALESCE(a.abandons, 0)) > 0
            THEN ROUND(CAST(COALESCE(c.completions, 0) AS DOUBLE) / CAST(COALESCE(c.completions, 0) + COALESCE(a.abandons, 0) AS DOUBLE) * 100, 1)
            ELSE 0
        END as conversion_rate,
        CASE
            WHEN (COALESCE(c.completions, 0) + COALESCE(a.abandons, 0)) > 0
            THEN ROUND(CAST(COALESCE(a.abandons, 0) AS DOUBLE) / CAST(COALESCE(c.completions, 0) + COALESCE(a.abandons, 0) AS DOUBLE) * 100, 1)
            ELSE 0
        END as abandon_rate
    FROM form_completions c
    FULL OUTER JOIN form_starts s ON c.form_id = s.form_id
    FULL OUTER JOIN form_abandons a ON COALESCE(c.form_id, s.form_id) = a.form_id
    FULL OUTER JOIN form_views v ON COALESCE(c.form_id, s.form_id, a.form_id) = v.form_id
    WHERE COALESCE(c.form_id, s.form_id, a.form_id, v.form_id) IS NOT NULL
    ORDER BY {sort_column} DESC
    LIMIT {limit}
    """

    results = execute_athena_query(query)

    # Calculate totals
    total_completions = 0
    forms = []
    for row in (results or []):
        completions = int(row.get('completions', 0) or 0)
        total_completions += completions

        avg_time = float(row.get('avg_completion_time', 0) or 0)
        conversion_rate = float(row.get('conversion_rate', 0) or 0)

        # Determine trend indicator (would need historical comparison for real trends)
        # For now, use conversion rate thresholds
        if conversion_rate >= 70:
            trend = 'trending'
        elif conversion_rate >= 40:
            trend = 'stable'
        else:
            trend = 'low'

        forms.append({
            'form_id': row.get('form_id', ''),
            'form_label': row.get('form_label', row.get('form_id', 'Unknown Form')),
            'views': int(row.get('views', 0) or 0),
            'started': int(row.get('started', 0) or 0),
            'completions': completions,
            'conversion_rate': conversion_rate,
            'abandon_rate': float(row.get('abandon_rate', 0) or 0),
            'avg_completion_time_seconds': round(avg_time),
            'trend': trend
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': params.get('range', '30d'),
        'forms': forms,
        'total_completions': total_completions
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


def parse_date_range(range_str: str) -> Dict[str, Any]:
    """
    Parse date range string (7d, 30d, 90d) into date components.
    Returns ISO date string for proper cross-month-boundary filtering.
    """
    days = 30  # default
    if range_str.endswith('d'):
        try:
            days = int(range_str[:-1])
        except ValueError:
            pass

    start_date = datetime.now(timezone.utc) - timedelta(days=days)

    return {
        'start_year': start_date.year,
        'start_month': start_date.month,
        'start_day': start_date.day,
        'start_date_iso': start_date.strftime('%Y-%m-%d'),
        'days': days
    }


# =============================================================================
# Response Helpers
# =============================================================================

def cors_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Build response with JSON content type.
    CORS headers are handled by Lambda Function URL configuration.
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body)
    }
