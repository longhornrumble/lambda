"""
Analytics Dashboard API Lambda

Provides REST API endpoints for querying analytics data.
**HOT PATH**: Reads pre-computed aggregates from DynamoDB for sub-100ms responses.
**COLD PATH**: Falls back to Athena for real-time queries if DynamoDB cache is stale.

Architecture:
- DynamoDB Table: picasso-dashboard-aggregates (pre-computed by Analytics_Aggregator Lambda)
- Athena: Fallback for fresh queries, historical data >90 days
- Pre-computed aggregates refreshed hourly by EventBridge-triggered Aggregator

Endpoints:
- GET /analytics/summary    - Overview metrics (sessions, events, forms)
- GET /analytics/sessions   - Session counts over time
- GET /analytics/events     - Event breakdown by type
- GET /analytics/funnel     - Conversion funnel analysis
- GET /forms/bottlenecks    - Field-level abandonment analysis
- GET /forms/submissions    - Recent form submissions (paginated)
- GET /forms/top-performers - Form performance rankings
- GET /sessions/{session_id} - Full session timeline (User Journey)
- GET /sessions/list        - Paginated session list with filters (User Journey)
- GET /features             - Dashboard feature flags for tenant

Authentication:
- JWT token in Authorization header (Bearer token)
- Token contains tenant_id for data isolation

Environment Variables:
- ATHENA_DATABASE: Athena database name (default: picasso_analytics)
- ATHENA_OUTPUT_LOCATION: S3 location for query results
- JWT_SECRET_KEY_NAME: Secrets Manager key name for JWT secret
- AGGREGATES_TABLE: DynamoDB table for pre-computed aggregates (default: picasso-dashboard-aggregates)
- USE_DYNAMO_CACHE: Enable DynamoDB hot path (default: true)
- SESSION_EVENTS_TABLE: DynamoDB table for session events (default: picasso-session-events)
- SESSION_SUMMARIES_TABLE: DynamoDB table for session summaries (default: picasso-session-summaries)
- S3_CONFIG_BUCKET: S3 bucket for tenant configurations (default: picasso-configs)
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
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, List
from decimal import Decimal
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

# DynamoDB hot path configuration
AGGREGATES_TABLE = os.environ.get('AGGREGATES_TABLE', 'picasso-dashboard-aggregates')
USE_DYNAMO_CACHE = os.environ.get('USE_DYNAMO_CACHE', 'true').lower() == 'true'
CACHE_MAX_AGE_HOURS = int(os.environ.get('CACHE_MAX_AGE_HOURS', '2'))  # Consider stale after 2 hours

# AWS clients
athena = boto3.client('athena')
secrets_manager = boto3.client('secretsmanager')
dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

# DynamoDB tables
aggregates_table = dynamodb_resource.Table(AGGREGATES_TABLE)

# DynamoDB table for form submissions (contains PII)
FORM_SUBMISSIONS_TABLE = os.environ.get('FORM_SUBMISSIONS_TABLE', 'picasso_form_submissions')

# DynamoDB Session Tables (User Journey Analytics)
SESSION_EVENTS_TABLE = os.environ.get('SESSION_EVENTS_TABLE', 'picasso-session-events')
SESSION_SUMMARIES_TABLE = os.environ.get('SESSION_SUMMARIES_TABLE', 'picasso-session-summaries')

# S3 Tenant Configuration
S3_CONFIG_BUCKET = os.environ.get('S3_CONFIG_BUCKET', 'picasso-configs')

# S3 client
s3 = boto3.client('s3')

# Cache for tenant configs (TTL: 5 minutes)
_tenant_config_cache: Dict[str, Dict[str, Any]] = {}
_tenant_config_cache_time: Dict[str, float] = {}
TENANT_CONFIG_CACHE_TTL = 300  # 5 minutes

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


def sanitize_tenant_id(tenant_id: str, for_sql: bool = False) -> str:
    """
    Validate tenant_id is safe for SQL interpolation.
    Prevents SQL injection by ensuring only alphanumeric characters.

    Args:
        tenant_id: The tenant ID to validate
        for_sql: If True, also applies SQL escaping for defense-in-depth

    Raises ValueError if tenant_id is invalid.
    """
    if not tenant_id:
        raise ValueError("tenant_id is required")

    if len(tenant_id) > 50:
        raise ValueError("tenant_id too long (max 50 chars)")

    if not TENANT_ID_PATTERN.match(tenant_id):
        raise ValueError(f"Invalid tenant_id format: must be alphanumeric")

    # Defense-in-depth: escape SQL special chars if for SQL use
    # Note: TENANT_ID_PATTERN already prevents quotes, but this is extra safety
    if for_sql:
        return tenant_id.replace("'", "''").replace("\\", "\\\\")

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


# =============================================================================
# PII Redaction Utilities (GDPR/CCPA Compliance)
# =============================================================================
# These functions redact PII for CloudWatch logs while preserving enough info
# for debugging. Full PII is preserved in DynamoDB and API responses.

def redact_email(email: str) -> str:
    """
    Redact email for safe logging.
    user@domain.com -> u***@d***.com
    """
    if not email or '@' not in email:
        return '[invalid-email]'

    try:
        local, domain = email.split('@', 1)
        domain_parts = domain.rsplit('.', 1)
        if len(domain_parts) == 2:
            domain_name, tld = domain_parts
            return f"{local[0]}***@{domain_name[0]}***.{tld}"
        return f"{local[0]}***@{domain[0]}***"
    except (ValueError, IndexError):
        return '[redacted-email]'


def redact_name(name: str) -> str:
    """
    Redact name for safe logging.
    John Smith -> J*** S***
    """
    if not name:
        return '[no-name]'

    parts = name.strip().split()
    redacted_parts = []
    for part in parts:
        if len(part) > 0:
            redacted_parts.append(f"{part[0]}***")

    return ' '.join(redacted_parts) if redacted_parts else '[redacted]'


def redact_tenant_id(tenant_id: str) -> str:
    """
    Redact tenant_id for logging - show only first 8 chars.
    AUS123456789 -> AUS12345...
    """
    if not tenant_id:
        return '[no-tenant]'

    if len(tenant_id) <= 8:
        return tenant_id

    return f"{tenant_id[:8]}..."


def safe_sql_string(value: str) -> str:
    """
    Additional SQL injection protection layer.
    Escapes single quotes for SQL string interpolation.

    NOTE: This is defense-in-depth. Primary protection is sanitize_tenant_id()
    which validates against alphanumeric pattern. This function handles edge cases.
    """
    if not value:
        return ''

    # Escape single quotes (SQL standard: '' for literal single quote)
    return value.replace("'", "''")


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

    # Extract user email for audit purposes
    user_email = auth_result.get('email', 'unknown')

    logger.info(f"Authenticated request for tenant: {tenant_id[:8]}...")

    # Parse query parameters
    params = event.get('queryStringParameters') or {}

    # Route to appropriate handler
    # NOTE: More specific routes must come before generic ones
    # (e.g., /forms/summary before /summary)
    try:
        # Conversations endpoints (most specific - check first)
        if path.endswith('/conversations/summary'):
            return handle_conversation_summary(tenant_id, params)
        elif path.endswith('/conversations/heatmap'):
            return handle_conversation_heatmap(tenant_id, params)
        elif path.endswith('/conversations/top-questions'):
            return handle_top_questions(tenant_id, params)
        elif path.endswith('/conversations/recent'):
            return handle_recent_conversations(tenant_id, params)
        elif path.endswith('/conversations/trend'):
            return handle_conversation_trend(tenant_id, params)
        # Forms endpoints (more specific - check second)
        elif path.endswith('/forms/summary'):
            return handle_form_summary(tenant_id, params)
        elif path.endswith('/bottlenecks'):
            return handle_form_bottlenecks(tenant_id, params)
        elif path.endswith('/submissions'):
            return handle_form_submissions(tenant_id, params)
        elif path.endswith('/top-performers'):
            return handle_form_top_performers(tenant_id, params)
        # Session detail endpoints (User Journey Analytics)
        elif path.endswith('/sessions/list'):
            return handle_sessions_list(tenant_id, params)
        elif '/sessions/' in path and not path.endswith('/sessions'):
            # Extract session_id from path like /sessions/{session_id}
            session_id = path.split('/sessions/')[-1].split('/')[0]
            if session_id and session_id != 'list':
                return handle_session_detail(tenant_id, session_id, params)
        # Analytics endpoints (generic)
        elif path.endswith('/summary'):
            return handle_summary(tenant_id, params)
        elif path.endswith('/sessions'):
            return handle_sessions(tenant_id, params)
        elif path.endswith('/events'):
            return handle_events(tenant_id, params)
        elif path.endswith('/funnel'):
            return handle_funnel(tenant_id, params)
        # Feature flags endpoint
        elif path.endswith('/features'):
            return handle_features(tenant_id)

        # Lead Workspace endpoints
        elif path == '/leads/queue' and method == 'GET':
            return handle_lead_queue(tenant_id, params)
        elif '/leads/' in path and '/status' in path and method == 'PATCH':
            # PATCH /leads/{submission_id}/status
            submission_id = path.split('/leads/')[1].split('/status')[0]
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_lead_status_update(tenant_id, submission_id, body, user_email)
        elif '/leads/' in path and '/notes' in path and method == 'PATCH':
            # PATCH /leads/{submission_id}/notes
            submission_id = path.split('/leads/')[1].split('/notes')[0]
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_lead_notes_update(tenant_id, submission_id, body, user_email)
        elif '/leads/' in path and '/reactivate' in path and method == 'PATCH':
            # PATCH /leads/{submission_id}/reactivate
            submission_id = path.split('/leads/')[1].split('/reactivate')[0]
            return handle_lead_reactivate(tenant_id, submission_id, user_email)
        elif '/leads/' in path and method == 'GET':
            # GET /leads/{submission_id}
            submission_id = path.split('/leads/')[1].split('/')[0]
            if submission_id and submission_id != 'queue':
                return handle_lead_detail(tenant_id, submission_id)

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
    Returns {'success': True, 'tenant_id': '...', 'email': '...'} or {'success': False, 'error': '...'}
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

        # Extract email for audit purposes
        email = payload.get('email', 'unknown')

        return {'success': True, 'tenant_id': tenant_id, 'email': email}

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
# Tenant Configuration & Feature Flags
# =============================================================================

def get_tenant_config(tenant_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch tenant configuration from S3 with caching.
    Returns None if config not found.
    """
    global _tenant_config_cache, _tenant_config_cache_time

    now = time.time()

    # Check cache
    if tenant_id in _tenant_config_cache:
        if (now - _tenant_config_cache_time.get(tenant_id, 0)) < TENANT_CONFIG_CACHE_TTL:
            return _tenant_config_cache[tenant_id]

    try:
        key = f"tenants/{tenant_id}/config.json"
        response = s3.get_object(Bucket=S3_CONFIG_BUCKET, Key=key)
        config = json.loads(response['Body'].read().decode('utf-8'))

        # Cache the config
        _tenant_config_cache[tenant_id] = config
        _tenant_config_cache_time[tenant_id] = now

        logger.info(f"Loaded tenant config for {tenant_id[:8]}...")
        return config

    except s3.exceptions.NoSuchKey:
        logger.warning(f"No config found for tenant {tenant_id[:8]}...")
        return None
    except ClientError as e:
        logger.error(f"Error fetching tenant config: {e}")
        return None


def get_tenant_features(tenant_id: str) -> Dict[str, bool]:
    """
    Get dashboard feature flags for a tenant.
    Returns safe defaults if config not found or flags missing.

    Defaults:
    - dashboard_conversations: True (FREE tier)
    - dashboard_forms: True (legacy tenant support)
    - dashboard_attribution: False (PREMIUM only)
    """
    config = get_tenant_config(tenant_id)

    if not config:
        # No config = legacy tenant, give them conversations + forms
        return {
            'dashboard_conversations': True,
            'dashboard_forms': True,
            'dashboard_attribution': False,
        }

    features = config.get('features', {})

    return {
        'dashboard_conversations': features.get('dashboard_conversations', True),
        'dashboard_forms': features.get('dashboard_forms', True),
        'dashboard_attribution': features.get('dashboard_attribution', False),
    }


def handle_features(tenant_id: str) -> Dict[str, Any]:
    """
    Handle GET /features endpoint.
    Returns dashboard feature flags for the authenticated tenant.
    """
    features = get_tenant_features(tenant_id)

    return cors_response(200, {
        'tenant_id': tenant_id,
        'features': features,
    })


def validate_feature_access(tenant_id: str, required_feature: str) -> Optional[Dict[str, Any]]:
    """
    Validate that tenant has access to a premium feature.
    Returns None if access granted, or a 403 error response if denied.

    Usage:
        error = validate_feature_access(tenant_id, 'dashboard_forms')
        if error:
            return error
    """
    features = get_tenant_features(tenant_id)

    if not features.get(required_feature, False):
        logger.warning(f"Access denied: {tenant_id[:8]}... lacks {required_feature}")
        return cors_response(403, {
            'error': 'Feature not available',
            'message': f'Your subscription does not include access to this feature. Please upgrade to access {required_feature.replace("dashboard_", "")} analytics.',
            'feature': required_feature,
        })

    return None


# =============================================================================
# DynamoDB Hot Path Cache Functions
# =============================================================================

def get_cached_metric(tenant_id: str, metric_key: str) -> Optional[Dict[str, Any]]:
    """
    Get pre-computed metric from DynamoDB cache.
    Returns None if cache miss, stale, or disabled.

    Performance: ~5-20ms vs 5-30s for Athena
    """
    if not USE_DYNAMO_CACHE:
        return None

    try:
        response = aggregates_table.get_item(
            Key={
                'pk': f'TENANT#{tenant_id}',
                'sk': f'METRIC#{metric_key}'
            }
        )

        item = response.get('Item')
        if not item:
            logger.debug(f"Cache miss for {metric_key}")
            return None

        # Check freshness
        updated_at = item.get('updated_at', '')
        if updated_at:
            try:
                update_time = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - update_time).total_seconds() / 3600

                if age_hours > CACHE_MAX_AGE_HOURS:
                    logger.info(f"Cache stale for {metric_key} (age: {age_hours:.1f}h)")
                    return None
            except (ValueError, TypeError):
                pass

        # Convert Decimal to float for JSON serialization
        data = convert_decimal_to_float(item.get('data', {}))
        logger.info(f"Cache hit for {metric_key}")
        return data

    except Exception as e:
        logger.warning(f"Cache error for {metric_key}: {e}")
        return None


def convert_decimal_to_float(obj):
    """
    Recursively convert Decimal to float for JSON serialization.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_to_float(item) for item in obj]
    return obj


# =============================================================================
# DynamoDB Session Summaries Query Functions (Hot Path)
# =============================================================================

def fetch_session_summaries(tenant_hash: str, date_range: Dict[str, str], limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch all session summaries from DynamoDB for a tenant within a date range.

    This queries the picasso-session-summaries table directly for hot data (<90 days).
    Returns raw session data for aggregation.

    Performance: ~50-200ms for most tenants (vs 5-60s for Athena)

    Args:
        tenant_hash: Tenant hash (e.g., 'fo85e6a06dcdf4')
        date_range: Dict with 'start_date_iso' key (e.g., '2025-12-01')
        limit: Max sessions to fetch (default 1000, paginate if more)

    Returns:
        List of session summary dictionaries
    """
    # SK format is now SESSION#{session_id} (no timestamp)
    # Filter by started_at attribute instead
    start_date = date_range['start_date_iso']

    sessions = []
    last_evaluated_key = None

    try:
        while True:
            query_params = {
                'TableName': SESSION_SUMMARIES_TABLE,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'FilterExpression': 'started_at >= :start_date',
                'ExpressionAttributeValues': {
                    ':pk': {'S': f'TENANT#{tenant_hash}'},
                    ':sk_prefix': {'S': 'SESSION#'},
                    ':start_date': {'S': start_date}
                },
                'Limit': min(limit - len(sessions), 1000)  # DynamoDB max page size
            }

            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            response = dynamodb.query(**query_params)
            items = response.get('Items', [])

            for item in items:
                # Parse session data - SK format: SESSION#{session_id}
                sk = item.get('sk', {}).get('S', '')
                sk_parts = sk.split('#')
                session_id = sk_parts[1] if len(sk_parts) >= 2 else ''

                started_at = item.get('started_at', {}).get('S', '')
                ended_at = item.get('ended_at', {}).get('S', started_at)
                # Normalize legacy 'browsing' to 'conversation'
                raw_outcome = item.get('outcome', {}).get('S', 'conversation')
                outcome = 'conversation' if raw_outcome == 'browsing' else raw_outcome

                # Calculate duration
                duration_seconds = 0
                if started_at and ended_at:
                    try:
                        start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                        end_dt = datetime.fromisoformat(ended_at.replace('Z', '+00:00'))
                        duration_seconds = int((end_dt - start_dt).total_seconds())
                    except (ValueError, TypeError):
                        pass

                sessions.append({
                    'session_id': session_id,
                    'started_at': started_at,
                    'ended_at': ended_at,
                    'duration_seconds': duration_seconds,
                    'outcome': outcome,
                    'message_count': int(item.get('message_count', {}).get('N', 0)),
                    'user_message_count': int(item.get('user_message_count', {}).get('N', 0)),
                    'bot_message_count': int(item.get('bot_message_count', {}).get('N', 0)),
                    'first_question': item.get('first_question', {}).get('S', ''),
                    'form_id': item.get('form_id', {}).get('S', ''),
                    # Response time tracking (sum/count for averaging)
                    'total_response_time_ms': int(item.get('total_response_time_ms', {}).get('N', 0)),
                    'response_count': int(item.get('response_count', {}).get('N', 0))
                })

            # Check if we have more pages and haven't hit the limit
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key or len(sessions) >= limit:
                break

        logger.info(f"Fetched {len(sessions)} sessions from DynamoDB for tenant {tenant_hash}")
        return sessions

    except ClientError as e:
        logger.error(f"DynamoDB error fetching sessions: {e}")
        return []
    except Exception as e:
        logger.exception(f"Error fetching sessions: {e}")
        return []


# =============================================================================
# DynamoDB Form Events Query Functions (Hot Path)
# =============================================================================

def fetch_form_events_from_dynamo(tenant_hash: str, date_range: Dict[str, str], form_id: str = None) -> List[Dict[str, Any]]:
    """
    Fetch form events from picasso-session-events table using tenant-date-index GSI.

    Queries for FORM_VIEWED, FORM_STARTED, FORM_COMPLETED, FORM_ABANDONED events.
    Uses FilterExpression for event_type filtering (acceptable at current scale of ~100 events/day).

    Args:
        tenant_hash: Tenant hash (e.g., 'auc5b0ecb0adcb')
        date_range: Dict with 'start_date_iso' key
        form_id: Optional form_id filter

    Returns:
        List of form event dictionaries
    """
    start_date = date_range['start_date_iso']

    # Build filter expression for form event types
    filter_expr = 'event_type IN (:t1, :t2, :t3, :t4)'
    expr_values = {
        ':th': {'S': tenant_hash},
        ':start': {'S': start_date},
        ':t1': {'S': 'FORM_VIEWED'},
        ':t2': {'S': 'FORM_STARTED'},
        ':t3': {'S': 'FORM_COMPLETED'},
        ':t4': {'S': 'FORM_ABANDONED'}
    }

    # Add optional form_id filter
    if form_id:
        filter_expr += ' AND contains(event_payload, :form_id)'
        expr_values[':form_id'] = {'S': f'"form_id": "{form_id}"'}

    events = []
    last_key = None

    try:
        while True:
            query_params = {
                'TableName': SESSION_EVENTS_TABLE,
                'IndexName': 'tenant-date-index',
                'KeyConditionExpression': 'tenant_hash = :th AND #ts >= :start',
                'FilterExpression': filter_expr,
                'ExpressionAttributeNames': {'#ts': 'timestamp'},
                'ExpressionAttributeValues': expr_values
            }

            if last_key:
                query_params['ExclusiveStartKey'] = last_key

            response = dynamodb.query(**query_params)
            items = response.get('Items', [])

            for item in items:
                event_type = item.get('event_type', {}).get('S', '')
                timestamp = item.get('timestamp', {}).get('S', '')

                # Parse event_payload
                payload = {}
                if 'event_payload' in item:
                    try:
                        payload_str = item['event_payload'].get('S', '{}')
                        payload = json.loads(payload_str)
                    except (json.JSONDecodeError, TypeError):
                        pass

                events.append({
                    'event_type': event_type,
                    'timestamp': timestamp,
                    'payload': payload
                })

            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break

        logger.info(f"Fetched {len(events)} form events from DynamoDB for tenant {tenant_hash}")
        return events

    except ClientError as e:
        logger.error(f"DynamoDB error fetching form events: {e}")
        return []
    except Exception as e:
        logger.exception(f"Error fetching form events: {e}")
        return []


def fetch_form_summary_from_dynamo(tenant_hash: str, date_range: Dict[str, str], form_id: str = None) -> Dict[str, Any]:
    """
    Calculate form summary metrics from DynamoDB session events.

    Replaces Athena query for /forms/summary endpoint.
    Performance: ~100-500ms (vs 5-30s for Athena)

    Args:
        tenant_hash: Tenant hash
        date_range: Dict with 'start_date_iso'
        form_id: Optional form filter

    Returns:
        Dict with form metrics (views, starts, completes, abandons, rates, avg_time)
    """
    events = fetch_form_events_from_dynamo(tenant_hash, date_range, form_id)

    # Aggregate counts
    counts = {
        'FORM_VIEWED': 0,
        'FORM_STARTED': 0,
        'FORM_COMPLETED': 0,
        'FORM_ABANDONED': 0
    }
    completion_times = []

    for event in events:
        event_type = event.get('event_type', '')
        if event_type in counts:
            counts[event_type] += 1

        # Extract completion time for FORM_COMPLETED events
        if event_type == 'FORM_COMPLETED':
            duration = event.get('payload', {}).get('duration_seconds')
            if duration and isinstance(duration, (int, float)) and duration > 0:
                completion_times.append(duration)

    # Calculate rates based on outcomes (completed + abandoned)
    total_outcomes = counts['FORM_COMPLETED'] + counts['FORM_ABANDONED']
    completion_rate = (counts['FORM_COMPLETED'] / total_outcomes * 100) if total_outcomes > 0 else 0
    abandon_rate = (counts['FORM_ABANDONED'] / total_outcomes * 100) if total_outcomes > 0 else 0
    avg_time = sum(completion_times) / len(completion_times) if completion_times else 0

    return {
        'form_views': counts['FORM_VIEWED'],
        'forms_started': counts['FORM_STARTED'],
        'forms_completed': counts['FORM_COMPLETED'],
        'forms_abandoned': counts['FORM_ABANDONED'],
        'completion_rate': round(completion_rate, 1),
        'abandon_rate': round(abandon_rate, 1),
        'avg_completion_time_seconds': round(avg_time)
    }


def fetch_form_bottlenecks_from_dynamo(tenant_hash: str, date_range: Dict[str, str], form_id: str = None, limit: int = 5) -> Dict[str, Any]:
    """
    Calculate form bottlenecks (field-level abandonment) from DynamoDB session events.

    Replaces Athena query for /forms/bottlenecks endpoint.
    Performance: ~100-500ms (vs 5-30s for Athena)

    Args:
        tenant_hash: Tenant hash
        date_range: Dict with 'start_date_iso'
        form_id: Optional form filter
        limit: Maximum number of bottlenecks to return

    Returns:
        Dict with bottlenecks list and total_abandonments count
    """
    start_date = date_range['start_date_iso']

    # Query only FORM_ABANDONED events
    filter_expr = 'event_type = :t1'
    expr_values = {
        ':th': {'S': tenant_hash},
        ':start': {'S': start_date},
        ':t1': {'S': 'FORM_ABANDONED'}
    }

    # Add optional form_id filter
    if form_id:
        filter_expr += ' AND contains(event_payload, :form_id)'
        expr_values[':form_id'] = {'S': f'"form_id": "{form_id}"'}

    events = []
    last_key = None

    try:
        while True:
            query_params = {
                'TableName': SESSION_EVENTS_TABLE,
                'IndexName': 'tenant-date-index',
                'KeyConditionExpression': 'tenant_hash = :th AND #ts >= :start',
                'FilterExpression': filter_expr,
                'ExpressionAttributeNames': {'#ts': 'timestamp'},
                'ExpressionAttributeValues': expr_values
            }

            if last_key:
                query_params['ExclusiveStartKey'] = last_key

            response = dynamodb.query(**query_params)
            items = response.get('Items', [])

            for item in items:
                # Parse event_payload
                payload = {}
                if 'event_payload' in item:
                    try:
                        payload_str = item['event_payload'].get('S', '{}')
                        payload = json.loads(payload_str)
                    except (json.JSONDecodeError, TypeError):
                        pass

                events.append(payload)

            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break

        logger.info(f"Fetched {len(events)} FORM_ABANDONED events from DynamoDB for tenant {tenant_hash}")

        # Aggregate by last_field_id
        field_abandons = {}  # field_id -> {'count': N, 'label': '...', 'form_id': '...'}

        for event in events:
            field_id = event.get('last_field_id', 'unknown')
            field_label = event.get('last_field_label', field_id)
            event_form_id = event.get('form_id')

            if field_id not in field_abandons:
                field_abandons[field_id] = {
                    'count': 0,
                    'label': field_label,
                    'form_id': event_form_id
                }
            field_abandons[field_id]['count'] += 1

        # Calculate total and sort by count descending
        total_abandons = len(events)
        sorted_fields = sorted(
            field_abandons.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )[:limit]

        # Build bottlenecks list with insights
        bottlenecks = []
        for field_id, data in sorted_fields:
            abandon_count = data['count']
            abandon_pct = round((abandon_count / total_abandons * 100) if total_abandons > 0 else 0, 1)

            # Generate insight based on field characteristics
            insight = generate_field_insight(field_id, data['label'])

            bottlenecks.append({
                'field_id': field_id,
                'field_label': data['label'],
                'form_id': data['form_id'],
                'abandon_count': abandon_count,
                'abandon_percentage': abandon_pct,
                'insight': insight['insight'],
                'recommendation': insight['recommendation']
            })

        return {
            'bottlenecks': bottlenecks,
            'total_abandonments': total_abandons
        }

    except ClientError as e:
        logger.error(f"DynamoDB error fetching form bottlenecks: {e}")
        return {'bottlenecks': [], 'total_abandonments': 0}
    except Exception as e:
        logger.exception(f"Error fetching form bottlenecks: {e}")
        return {'bottlenecks': [], 'total_abandonments': 0}


def fetch_form_top_performers_from_dynamo(tenant_hash: str, date_range: Dict[str, str], limit: int = 5, sort_by: str = 'conversion_rate') -> Dict[str, Any]:
    """
    Calculate form performance rankings from DynamoDB session events.

    Replaces Athena query for /forms/top-performers endpoint.
    Performance: ~100-500ms (vs 5-30s for Athena)

    Args:
        tenant_hash: Tenant hash
        date_range: Dict with 'start_date_iso'
        limit: Maximum number of forms to return
        sort_by: Sort field (conversion_rate, completions, avg_time)

    Returns:
        Dict with forms list and total_completions count
    """
    # Reuse existing function to get all form events
    events = fetch_form_events_from_dynamo(tenant_hash, date_range)

    # Aggregate per form_id
    form_stats = {}  # form_id -> {views, started, completions, abandons, durations[], label}

    for event in events:
        event_type = event.get('event_type', '')
        payload = event.get('payload', {})
        form_id = payload.get('form_id', 'unknown')
        form_label = payload.get('form_label', form_id)

        if form_id not in form_stats:
            form_stats[form_id] = {
                'views': 0,
                'started': 0,
                'completions': 0,
                'abandons': 0,
                'durations': [],
                'label': form_label
            }

        if event_type == 'FORM_VIEWED':
            form_stats[form_id]['views'] += 1
        elif event_type == 'FORM_STARTED':
            form_stats[form_id]['started'] += 1
        elif event_type == 'FORM_COMPLETED':
            form_stats[form_id]['completions'] += 1
            # Collect duration for average
            duration = payload.get('duration_seconds')
            if duration and isinstance(duration, (int, float)) and duration > 0:
                form_stats[form_id]['durations'].append(duration)
            # Update label if available
            if form_label and form_label != 'unknown':
                form_stats[form_id]['label'] = form_label
        elif event_type == 'FORM_ABANDONED':
            form_stats[form_id]['abandons'] += 1

    # Calculate metrics for each form
    forms_list = []
    total_completions = 0

    for form_id, stats in form_stats.items():
        completions = stats['completions']
        abandons = stats['abandons']
        total_outcomes = completions + abandons
        total_completions += completions

        conversion_rate = (completions / total_outcomes * 100) if total_outcomes > 0 else 0
        abandon_rate = (abandons / total_outcomes * 100) if total_outcomes > 0 else 0
        avg_time = sum(stats['durations']) / len(stats['durations']) if stats['durations'] else 0

        # Determine trend indicator based on conversion rate thresholds
        if conversion_rate >= 70:
            trend = 'trending'
        elif conversion_rate >= 40:
            trend = 'stable'
        else:
            trend = 'low'

        forms_list.append({
            'form_id': form_id,
            'form_label': stats['label'] if stats['label'] != 'unknown' else form_id,
            'views': stats['views'],
            'started': stats['started'],
            'completions': completions,
            'conversion_rate': round(conversion_rate, 1),
            'abandon_rate': round(abandon_rate, 1),
            'avg_completion_time_seconds': round(avg_time),
            'trend': trend
        })

    # Sort by requested field
    sort_key_map = {
        'conversion_rate': lambda x: x['conversion_rate'],
        'completions': lambda x: x['completions'],
        'avg_time': lambda x: x['avg_completion_time_seconds']
    }
    sort_key = sort_key_map.get(sort_by, sort_key_map['conversion_rate'])

    # Sort descending and limit
    forms_list.sort(key=sort_key, reverse=True)
    forms_list = forms_list[:limit]

    logger.info(f"Calculated top performers for {len(form_stats)} forms from DynamoDB for tenant {tenant_hash}")

    return {
        'forms': forms_list,
        'total_completions': total_completions
    }


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
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)

    # Try DynamoDB cache first (sub-100ms)
    cached = get_cached_metric(tenant_id, f'analytics_summary#{range_str}')
    if cached:
        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'metrics': cached,
            'source': 'cache'
        })

    # Fallback to Athena (5-30s)
    logger.info(f"Cache miss - querying Athena for analytics_summary#{range_str}")

    # Use ISO date comparison for proper cross-month-boundary filtering
    # Defense-in-depth: apply safe_sql_string even though tenant_id is already validated
    safe_tenant = safe_sql_string(tenant_id)
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
    WHERE tenant_id = '{safe_tenant}'
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
    # Defense-in-depth: apply safe_sql_string even though tenant_id is already validated
    safe_tenant = safe_sql_string(tenant_id)
    query = f"""
    SELECT
        {select_date},
        COUNT(DISTINCT session_id) as sessions,
        COUNT(*) as events
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{safe_tenant}'
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
    # Defense-in-depth: apply safe_sql_string even though tenant_id is already validated
    safe_tenant = safe_sql_string(tenant_id)
    query = f"""
    SELECT
        event_type,
        COUNT(*) as count,
        COUNT(DISTINCT session_id) as unique_sessions
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{safe_tenant}'
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
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)

    # Try DynamoDB cache first (sub-100ms)
    cached = get_cached_metric(tenant_id, f'analytics_funnel#{range_str}')
    if cached:
        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'funnel': cached.get('funnel', []),
            'overall_conversion': cached.get('overall_conversion', 0),
            'source': 'cache'
        })

    # Fallback to Athena (5-30s)
    logger.info(f"Cache miss - querying Athena for analytics_funnel#{range_str}")

    # Use ISO date comparison for proper cross-month-boundary filtering
    # Defense-in-depth: apply safe_sql_string even though tenant_id is already validated
    safe_tenant = safe_sql_string(tenant_id)
    query = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN event_type = 'WIDGET_OPENED' THEN session_id END) as stage1_widget_opened,
        COUNT(DISTINCT CASE WHEN event_type = 'ACTION_CHIP_CLICKED' THEN session_id END) as stage2_chip_clicked,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_STARTED' THEN session_id END) as stage3_form_started,
        COUNT(DISTINCT CASE WHEN event_type = 'FORM_COMPLETED' THEN session_id END) as stage4_form_completed
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE tenant_id = '{safe_tenant}'
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
    Returns form-specific summary metrics from DynamoDB.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - form_id: Filter by specific form (optional)

    Data source: picasso-session-events table via tenant-date-index GSI
    Performance: ~100-500ms (vs 5-30s for Athena)
    """
    # Validate premium feature access
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    form_id = params.get('form_id')

    # Validate form_id if provided
    if form_id and not TENANT_ID_PATTERN.match(form_id):
        return cors_response(400, {'error': 'Invalid form_id format'})

    # Get tenant_hash for DynamoDB query
    tenant_hash = get_tenant_hash(tenant_id)
    if not tenant_hash:
        logger.error(f"Could not resolve tenant_hash for tenant_id: {redact_tenant_id(tenant_id)}")
        return cors_response(500, {'error': 'Could not resolve tenant configuration'})

    logger.info(f"Querying DynamoDB for forms_summary: tenant={tenant_hash}, range={range_str}, form_id={form_id}")

    try:
        # Query DynamoDB directly (no Athena fallback)
        metrics = fetch_form_summary_from_dynamo(tenant_hash, date_range, form_id)

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'metrics': metrics,
            'source': 'dynamodb'
        })

    except Exception as e:
        logger.exception(f"Error fetching form summary from DynamoDB: {e}")
        # Return empty metrics on error rather than failing
        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'metrics': {
                'form_views': 0,
                'forms_started': 0,
                'forms_completed': 0,
                'forms_abandoned': 0,
                'completion_rate': 0,
                'abandon_rate': 0,
                'avg_completion_time_seconds': 0
            },
            'source': 'error_fallback'
        })


def handle_form_bottlenecks(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /forms/bottlenecks
    Returns field-level abandonment analysis from DynamoDB.

    Shows which form fields cause the most drop-offs.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - form_id: Filter by specific form (optional)
    - limit: Number of results (default 5, max 20)

    Data source: picasso-session-events table via tenant-date-index GSI
    Performance: ~100-500ms (vs 5-30s for Athena)
    """
    # Validate premium feature access
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    limit = min(int(params.get('limit', '5')), 20)
    form_id = params.get('form_id')

    # Validate form_id if provided
    if form_id and not TENANT_ID_PATTERN.match(form_id):
        return cors_response(400, {'error': 'Invalid form_id format'})

    # Get tenant_hash for DynamoDB query
    tenant_hash = get_tenant_hash(tenant_id)
    if not tenant_hash:
        logger.error(f"Could not resolve tenant_hash for tenant_id: {redact_tenant_id(tenant_id)}")
        return cors_response(500, {'error': 'Could not resolve tenant configuration'})

    logger.info(f"Querying DynamoDB for forms_bottlenecks: tenant={tenant_hash}, range={range_str}, form_id={form_id}")

    try:
        # Query DynamoDB directly (no Athena fallback)
        result = fetch_form_bottlenecks_from_dynamo(tenant_hash, date_range, form_id, limit)

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'bottlenecks': result['bottlenecks'],
            'total_abandonments': result['total_abandonments'],
            'source': 'dynamodb'
        })

    except Exception as e:
        logger.exception(f"Error fetching form bottlenecks from DynamoDB: {e}")
        # Return empty data on error rather than failing
        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'bottlenecks': [],
            'total_abandonments': 0,
            'source': 'error_fallback'
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
    # Validate premium feature access
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

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

        # Extract contact info - prefer canonical contact object (new schema)
        # Fall back to form_data_labeled, then form_data for backwards compatibility
        contact = item.get('contact', {})
        comments_field = item.get('comments', {})

        if contact and contact.get('M'):
            # New schema: use canonical contact object
            contact_map = contact.get('M', {})
            first_name = contact_map.get('first_name', {}).get('S', '') or ''
            last_name = contact_map.get('last_name', {}).get('S', '') or ''
            name = f"{first_name} {last_name}".strip() or 'Anonymous'
            email = contact_map.get('email', {}).get('S', '') or ''
            phone = contact_map.get('phone', {}).get('S', '') or ''
            comments = comments_field.get('S', '') if comments_field else ''
            fields = {'name': name, 'email': email, 'phone': phone, 'comments': comments}
        else:
            # Fall back to form_data_labeled extraction
            form_data_labeled = item.get('form_data_labeled', {})
            if form_data_labeled and form_data_labeled.get('M'):
                fields = extract_all_fields_from_form_data_labeled(form_data_labeled)
            else:
                # Legacy fallback - only has name/email
                name, email = extract_name_email_from_form_data(item.get('form_data', {}))
                fields = {'name': name, 'email': email, 'phone': '', 'comments': ''}

        # Apply search filter
        if search:
            search_fields = f"{fields['name']} {fields['email']} {form_title}".lower()
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
            'fields': fields
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


def extract_name_email_from_form_data_labeled(form_data_labeled: Dict) -> tuple:
    """
    Extract name and email from DynamoDB form_data_labeled structure.
    This uses human-readable field labels (e.g., "Name", "Email") instead of
    cryptic field IDs (e.g., "field_1762286136120").

    Structure: {label: {label, value, type}} where value can be string or nested object
    """
    name_parts = []
    email = ''

    # Get the nested map from DynamoDB format
    data_map = form_data_labeled.get('M', {})

    for field_label, field_wrapper in data_map.items():
        if not isinstance(field_wrapper, dict) or 'M' not in field_wrapper:
            continue

        field_obj = field_wrapper['M']
        field_type = field_obj.get('type', {}).get('S', 'text')
        value_obj = field_obj.get('value', {})

        label_lower = field_label.lower()

        # Handle email field
        if 'email' in label_lower or field_type == 'email':
            if 'S' in value_obj:
                email = value_obj['S']

        # Handle name field (could be simple string or composite with First/Last)
        elif 'name' in label_lower:
            if 'S' in value_obj:
                # Simple string name
                name_parts.append(value_obj['S'])
            elif 'M' in value_obj:
                # Composite name with First Name / Last Name subfields
                nested = value_obj['M']
                first = ''
                last = ''
                for sub_key, sub_val in nested.items():
                    if isinstance(sub_val, dict) and 'S' in sub_val:
                        val = sub_val['S']
                        sub_key_lower = sub_key.lower()
                        if 'first' in sub_key_lower:
                            first = val
                        elif 'last' in sub_key_lower:
                            last = val
                if first or last:
                    name_parts = [first, last]

    name = ' '.join(filter(None, name_parts)).strip()
    return (name if name else 'Anonymous', email)


def extract_all_fields_from_form_data_labeled(form_data_labeled: Dict) -> Dict[str, str]:
    """
    Extract all common form fields from DynamoDB form_data_labeled structure.
    Returns a dict with: name, email, phone, comments
    """
    fields = {
        'name': '',
        'email': '',
        'phone': '',
        'comments': ''
    }
    name_parts = []

    # Get the nested map from DynamoDB format
    data_map = form_data_labeled.get('M', {})

    for field_label, field_wrapper in data_map.items():
        if not isinstance(field_wrapper, dict) or 'M' not in field_wrapper:
            continue

        field_obj = field_wrapper['M']
        field_type = field_obj.get('type', {}).get('S', 'text')
        value_obj = field_obj.get('value', {})

        label_lower = field_label.lower()

        # Handle email field
        if 'email' in label_lower or field_type == 'email':
            if 'S' in value_obj:
                fields['email'] = value_obj['S']

        # Handle phone field
        elif 'phone' in label_lower or 'mobile' in label_lower or 'cell' in label_lower or field_type == 'phone':
            if 'S' in value_obj:
                fields['phone'] = value_obj['S']

        # Handle comments/message field (includes "about" for "About You" type fields)
        elif any(kw in label_lower for kw in ['comment', 'message', 'note', 'question', 'additional', 'tell us', 'about']):
            if 'S' in value_obj:
                fields['comments'] = value_obj['S']

        # Handle name field (could be simple string or composite with First/Last)
        elif 'name' in label_lower:
            if 'S' in value_obj:
                # Simple string name
                name_parts.append(value_obj['S'])
            elif 'M' in value_obj:
                # Composite name with First Name / Last Name subfields
                nested = value_obj['M']
                first = ''
                last = ''
                for sub_key, sub_val in nested.items():
                    if isinstance(sub_val, dict) and 'S' in sub_val:
                        val = sub_val['S']
                        sub_key_lower = sub_key.lower()
                        if 'first' in sub_key_lower:
                            first = val
                        elif 'last' in sub_key_lower:
                            last = val
                if first or last:
                    name_parts = [first, last]

    fields['name'] = ' '.join(filter(None, name_parts)).strip() or 'Anonymous'
    return fields


def extract_name_email_from_form_data(form_data: Dict) -> tuple:
    """
    LEGACY: Extract name and email from DynamoDB form_data structure.
    Used for backwards compatibility with old submissions that don't have form_data_labeled.
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
    Returns form performance rankings by conversion rate from DynamoDB.

    Query params:
    - range: Time range (7d, 30d, 90d) - default 30d
    - limit: Number of results (default 5, max 20)
    - sort_by: Sort field (conversion_rate, completions, avg_time) - default conversion_rate

    Data source: picasso-session-events table via tenant-date-index GSI
    Performance: ~100-500ms (vs 5-30s for Athena)
    """
    # Validate premium feature access
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    limit = min(int(params.get('limit', '5')), 20)
    sort_by = params.get('sort_by', 'conversion_rate')

    # Validate sort_by
    valid_sorts = {'conversion_rate', 'completions', 'avg_time'}
    if sort_by not in valid_sorts:
        sort_by = 'conversion_rate'

    # Get tenant_hash for DynamoDB query
    tenant_hash = get_tenant_hash(tenant_id)
    if not tenant_hash:
        logger.error(f"Could not resolve tenant_hash for tenant_id: {redact_tenant_id(tenant_id)}")
        return cors_response(500, {'error': 'Could not resolve tenant configuration'})

    logger.info(f"Querying DynamoDB for forms_top_performers: tenant={tenant_hash}, range={range_str}, sort_by={sort_by}")

    try:
        # Query DynamoDB directly (no Athena fallback)
        result = fetch_form_top_performers_from_dynamo(tenant_hash, date_range, limit, sort_by)

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'forms': result['forms'],
            'total_completions': result['total_completions'],
            'source': 'dynamodb'
        })

    except Exception as e:
        logger.exception(f"Error fetching form top performers from DynamoDB: {e}")
        # Return empty data on error rather than failing
        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'forms': [],
            'total_completions': 0,
            'source': 'error_fallback'
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


# =============================================================================
# Conversations Endpoint Handlers
# =============================================================================

def handle_conversation_summary(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /conversations/summary
    Returns conversation metrics: total conversations, messages, response time, after-hours %.

    HOT PATH: Queries DynamoDB session-summaries directly (~50-200ms)

    Query params:
    - range: Time range (1d, 7d, 30d, 90d) - default 30d
    """
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    tenant_hash = get_tenant_hash(tenant_id)

    logger.info(f"Fetching conversation summary from DynamoDB for tenant: {tenant_hash}, range: {range_str}")

    # Query DynamoDB directly (hot path)
    sessions = fetch_session_summaries(tenant_hash, date_range)

    # Aggregate metrics from sessions
    total_conversations = len(sessions)
    total_messages = sum(s.get('message_count', 0) for s in sessions)

    # Calculate average response time from running totals
    # Each session stores total_response_time_ms and response_count
    total_response_ms = 0
    total_response_count = 0
    for s in sessions:
        total_response_ms += s.get('total_response_time_ms', 0)
        total_response_count += s.get('response_count', 0)
    avg_response_time_seconds = (total_response_ms / total_response_count / 1000) if total_response_count > 0 else 0

    # Calculate after-hours percentage (sessions started outside 9am-5pm in local timezone)
    # Default to America/Chicago (Central Time)
    tz_param = params.get('timezone', 'America/Chicago')
    try:
        local_tz = ZoneInfo(tz_param)
    except Exception:
        local_tz = ZoneInfo('America/Chicago')

    after_hours_count = 0
    for session in sessions:
        started_at = session.get('started_at', '')
        if started_at:
            try:
                start_dt_utc = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                start_dt = start_dt_utc.astimezone(local_tz)
                hour = start_dt.hour
                if hour < 9 or hour >= 17:
                    after_hours_count += 1
            except (ValueError, TypeError):
                pass

    after_hours_percentage = (after_hours_count / total_conversations * 100) if total_conversations > 0 else 0

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': range_str,
        'metrics': {
            'total_conversations': total_conversations,
            'total_messages': total_messages,
            'avg_response_time_seconds': round(avg_response_time_seconds, 1),
            'after_hours_percentage': round(after_hours_percentage, 1)
        },
        'source': 'dynamodb',
        'date_range': {
            'start': date_range['start_date_iso'],
            'end': datetime.now(timezone.utc).strftime('%Y-%m-%d')
        }
    })


def handle_conversation_heatmap(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /conversations/heatmap
    Returns day × hour grid for conversation volume.

    HOT PATH: Queries DynamoDB session-summaries directly (~50-200ms)

    Query params:
    - range: Time range (1d, 7d, 30d, 90d) - default 30d
    - timezone: IANA timezone (e.g., America/Chicago) - default UTC
    """
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    tenant_hash = get_tenant_hash(tenant_id)

    # Get timezone parameter - default to America/Chicago (Central Time)
    # Common US timezones: America/New_York, America/Chicago, America/Denver, America/Los_Angeles
    tz_param = params.get('timezone', 'America/Chicago')
    if not re.match(r'^[A-Za-z0-9_/]+$', tz_param):
        tz_param = 'America/Chicago'

    # Parse timezone - fall back to Chicago if invalid
    try:
        local_tz = ZoneInfo(tz_param)
    except Exception:
        logger.warning(f"Invalid timezone '{tz_param}', falling back to America/Chicago")
        local_tz = ZoneInfo('America/Chicago')

    logger.info(f"Fetching conversation heatmap from DynamoDB for tenant: {tenant_hash}, range: {range_str}, timezone: {tz_param}")

    # Query DynamoDB directly (hot path)
    sessions = fetch_session_summaries(tenant_hash, date_range)

    # Build heatmap structure
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    hour_blocks = ['12AM', '3AM', '6AM', '9AM', '12PM', '3PM', '6PM', '9PM']

    # Initialize grid
    grid = {hb: {d: 0 for d in days} for hb in hour_blocks}
    total_conversations = 0
    peak = {'day': None, 'hour_block': None, 'count': 0}

    # Aggregate sessions into heatmap grid
    for session in sessions:
        started_at = session.get('started_at', '')
        if not started_at:
            continue

        try:
            # Parse UTC timestamp and convert to local timezone
            start_dt_utc = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
            start_dt = start_dt_utc.astimezone(local_tz)

            # Get day of week (0 = Monday) in LOCAL timezone
            day_idx = start_dt.weekday()
            day_name = days[day_idx]

            # Get hour block (3-hour windows) in LOCAL timezone
            hour = start_dt.hour
            if hour < 3:
                hour_block = '12AM'
            elif hour < 6:
                hour_block = '3AM'
            elif hour < 9:
                hour_block = '6AM'
            elif hour < 12:
                hour_block = '9AM'
            elif hour < 15:
                hour_block = '12PM'
            elif hour < 18:
                hour_block = '3PM'
            elif hour < 21:
                hour_block = '6PM'
            else:
                hour_block = '9PM'

            grid[hour_block][day_name] += 1
            total_conversations += 1

            if grid[hour_block][day_name] > peak['count']:
                peak = {'day': day_name, 'hour_block': hour_block, 'count': grid[hour_block][day_name]}

        except (ValueError, TypeError):
            continue

    # Convert to API response format
    heatmap = []
    for hb in hour_blocks:
        heatmap.append({
            'hour_block': hb,
            'data': [{'day': d, 'value': grid[hb][d]} for d in days]
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': range_str,
        'timezone': tz_param,
        'heatmap': heatmap,
        'peak': peak if peak['day'] else None,
        'total_conversations': total_conversations,
        'source': 'dynamodb'
    })


def handle_top_questions(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /conversations/top-questions
    Returns most frequently asked questions.

    HOT PATH: Queries DynamoDB session-summaries directly (~50-200ms)

    Query params:
    - range: Time range (1d, 7d, 30d, 90d) - default 30d
    - limit: Number of questions (default 5, max 10)
    """
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    limit = min(int(params.get('limit', '5')), 10)
    tenant_hash = get_tenant_hash(tenant_id)

    logger.info(f"Fetching top questions from DynamoDB for tenant: {tenant_hash}, range: {range_str}")

    # Query DynamoDB directly (hot path)
    sessions = fetch_session_summaries(tenant_hash, date_range)

    # Count occurrences of each first_question
    question_counts = {}
    for session in sessions:
        first_question = session.get('first_question', '').strip()
        if first_question:
            # Truncate to 100 chars for grouping
            key = first_question[:100]
            question_counts[key] = question_counts.get(key, 0) + 1

    # Sort by count descending and take top N
    sorted_questions = sorted(question_counts.items(), key=lambda x: x[1], reverse=True)[:limit]

    total_questions = len(sessions)

    questions = []
    for question_text, count in sorted_questions:
        percentage = round((count / total_questions * 100) if total_questions > 0 else 0, 1)
        questions.append({
            'question_text': question_text,
            'count': count,
            'percentage': percentage
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': range_str,
        'questions': questions,
        'total_questions': total_questions,
        'source': 'dynamodb'
    })


def handle_recent_conversations(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /conversations/recent
    Returns recent conversations with Q&A pairs.

    HOT PATH: Queries DynamoDB session-summaries directly (~50-200ms)

    Query params:
    - range: Time range (1d, 7d, 30d, 90d) - default 30d
    - page: Page number (default 1)
    - limit: Results per page (default 10, max 25)
    """
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    page = max(1, int(params.get('page', '1')))
    limit = min(int(params.get('limit', '10')), 25)
    offset = (page - 1) * limit
    tenant_hash = get_tenant_hash(tenant_id)

    logger.info(f"Fetching recent conversations from DynamoDB for tenant: {tenant_hash}, range: {range_str}")

    # Query DynamoDB directly (hot path) - get more than needed for pagination
    all_sessions = fetch_session_summaries(tenant_hash, date_range, limit=offset + limit + 1)

    # Sort by started_at descending (most recent first)
    all_sessions.sort(key=lambda x: x.get('started_at', ''), reverse=True)

    total_count = len(all_sessions)

    # Apply pagination
    paginated_sessions = all_sessions[offset:offset + limit]

    # Categorize questions into topics
    def categorize_question(question: str) -> str:
        q = (question or '').lower()
        if 'volunteer' in q:
            return 'Volunteer'
        if 'donate' in q or 'donation' in q:
            return 'Donation'
        if 'event' in q or 'gathering' in q:
            return 'Events'
        if 'service' in q or 'help' in q:
            return 'Services'
        if 'supplies' in q or 'request' in q:
            return 'Supplies'
        return 'General'

    conversations = []
    for session in paginated_sessions:
        question = session.get('first_question', '')
        conversations.append({
            'session_id': session.get('session_id', ''),
            'started_at': session.get('started_at', ''),
            'topic': categorize_question(question),
            'first_question': question or '',
            'first_answer': '',  # Not stored in session summary - would need session events lookup
            'response_time_seconds': round(session.get('avg_response_time_ms', 0) / 1000, 1),
            'message_count': session.get('message_count', 0),
            'outcome': session.get('outcome')
        })

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': range_str,
        'conversations': conversations,
        'pagination': {
            'total_count': total_count,
            'page': page,
            'limit': limit,
            'has_next': (page * limit) < total_count
        },
        'source': 'dynamodb'
    })


def handle_conversation_trend(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /conversations/trend
    Returns conversation counts over time for trend chart.

    HOT PATH: Queries DynamoDB session-summaries directly (~50-200ms)

    Query params:
    - range: Time range (1d, 7d, 30d, 90d) - default 30d
    - granularity: 'hour' or 'day' - default based on range
    """
    range_str = params.get('range', '30d')
    date_range = parse_date_range(range_str)
    granularity = params.get('granularity', 'hour' if date_range['days'] <= 1 else 'day')
    tenant_hash = get_tenant_hash(tenant_id)

    logger.info(f"Fetching conversation trend from DynamoDB for tenant: {tenant_hash}, range: {range_str}")

    # Query DynamoDB directly (hot path)
    sessions = fetch_session_summaries(tenant_hash, date_range)

    # Aggregate sessions by time period
    period_counts = {}

    for session in sessions:
        started_at = session.get('started_at', '')
        if not started_at:
            continue

        try:
            start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))

            if granularity == 'hour':
                # Group by hour for 1-day view
                hour = start_dt.hour
                suffix = 'am' if hour < 12 else 'pm'
                display_hour = hour if hour <= 12 else hour - 12
                if display_hour == 0:
                    display_hour = 12
                period = f"{display_hour}{suffix}"
                sort_key = hour
            else:
                # Group by day for week/month views
                period = start_dt.strftime('%b %d')  # e.g., "Dec 26"
                sort_key = start_dt.strftime('%Y-%m-%d')

            if period not in period_counts:
                period_counts[period] = {'count': 0, 'sort_key': sort_key}
            period_counts[period]['count'] += 1

        except (ValueError, TypeError):
            continue

    # Sort by sort_key and build trend array
    sorted_periods = sorted(period_counts.items(), key=lambda x: x[1]['sort_key'])

    trend = []
    for period, data in sorted_periods:
        trend.append({
            'period': period,
            'value': data['count']
        })

    legend = 'Questions per hour' if granularity == 'hour' else 'Questions per day'

    return cors_response(200, {
        'tenant_id': tenant_id,
        'range': range_str,
        'trend': trend,
        'legend': legend,
        'source': 'dynamodb'
    })


# =============================================================================
# Session Detail Endpoint Handlers (User Journey Analytics)
# =============================================================================

# Cache for tenant_id → tenant_hash reverse lookups
_tenant_hash_cache = {}
_tenant_hash_cache_time = 0
TENANT_HASH_CACHE_TTL = 300  # 5 minutes

# S3 bucket for tenant mappings
MAPPINGS_BUCKET = os.environ.get('MAPPINGS_BUCKET', 'myrecruiter-picasso')

# S3 client
s3 = boto3.client('s3')


def get_tenant_hash(tenant_id: str) -> str:
    """
    Look up tenant_hash from S3 mappings for a given tenant_id.

    Mappings are stored at s3://myrecruiter-picasso/mappings/{tenant_hash}.json
    Each file contains {"tenant_id": "...", "tenant_hash": "...", ...}

    This performs a reverse lookup by listing mapping files and finding
    the one with matching tenant_id. Results are cached for performance.

    Returns the tenant_hash if found, or None if not found.
    """
    global _tenant_hash_cache, _tenant_hash_cache_time

    # Check cache first
    now = time.time()
    if tenant_id in _tenant_hash_cache and (now - _tenant_hash_cache_time) < TENANT_HASH_CACHE_TTL:
        return _tenant_hash_cache[tenant_id]

    # List mapping files and find the one with matching tenant_id
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=MAPPINGS_BUCKET, Prefix='mappings/')

        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.json'):
                    continue

                # Extract hash from filename (mappings/{hash}.json)
                filename = key.split('/')[-1]
                candidate_hash = filename.replace('.json', '')

                # Fetch and check the mapping
                try:
                    response = s3.get_object(Bucket=MAPPINGS_BUCKET, Key=key)
                    mapping = json.loads(response['Body'].read().decode('utf-8'))

                    # Cache all mappings we see for future lookups
                    mapping_tenant_id = mapping.get('tenant_id')
                    mapping_tenant_hash = mapping.get('tenant_hash', candidate_hash)
                    if mapping_tenant_id:
                        _tenant_hash_cache[mapping_tenant_id] = mapping_tenant_hash

                    if mapping_tenant_id == tenant_id:
                        _tenant_hash_cache_time = now
                        logger.info(f"Found tenant_hash for {redact_tenant_id(tenant_id)}: {mapping_tenant_hash}")
                        return mapping_tenant_hash

                except Exception as e:
                    logger.warning(f"Error reading mapping {key}: {e}")
                    continue

        logger.warning(f"No mapping found for tenant_id: {redact_tenant_id(tenant_id)}")
        return None

    except Exception as e:
        logger.error(f"Error looking up tenant_hash for {redact_tenant_id(tenant_id)}: {e}")
        return None


def handle_session_detail(tenant_id: str, session_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /sessions/{session_id}
    Returns full session timeline with all events for session reconstruction.

    This queries the picasso-session-events table using the session_id.
    Events are returned in step order for timeline visualization.

    URL params:
    - session_id: The session ID to retrieve

    Returns:
    - Session metadata (session_id, started_at, ended_at, duration)
    - Events array with all steps in order
    - Summary metrics (message_count, outcome, etc.)
    """
    if not session_id:
        return cors_response(400, {'error': 'session_id is required'})

    # Sanitize session_id (alphanumeric, underscore, hyphen only)
    if not re.match(r'^[A-Za-z0-9_-]+$', session_id):
        return cors_response(400, {'error': 'Invalid session_id format'})

    tenant_hash = get_tenant_hash(tenant_id)
    logger.info(f"Fetching session detail: {session_id} for tenant: {tenant_hash}")

    try:
        # Query session events from DynamoDB
        response = dynamodb.query(
            TableName=SESSION_EVENTS_TABLE,
            KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={
                ':pk': {'S': f'SESSION#{session_id}'}
            },
            ScanIndexForward=True  # Ascending order by step_number
        )

        items = response.get('Items', [])

        if not items:
            return cors_response(404, {'error': 'Session not found'})

        # Verify tenant access - first event should have matching tenant_hash
        first_event = items[0]
        event_tenant_hash = first_event.get('tenant_hash', {}).get('S', '')
        if event_tenant_hash != tenant_hash:
            logger.warning(f"Tenant mismatch: {tenant_hash} != {event_tenant_hash}")
            return cors_response(403, {'error': 'Access denied to this session'})

        # Parse events
        events = []
        started_at = None
        ended_at = None
        message_count = 0
        user_message_count = 0
        bot_message_count = 0
        outcome = None
        first_question = None

        for item in items:
            event_type = item.get('event_type', {}).get('S', '')
            timestamp = item.get('timestamp', {}).get('S', '')
            step_number = int(item.get('step_number', {}).get('N', 0))

            # Parse event_payload if present
            payload = {}
            if 'event_payload' in item:
                try:
                    payload_str = item['event_payload'].get('S', '{}')
                    payload = json.loads(payload_str)
                except (json.JSONDecodeError, TypeError):
                    pass

            # Track session boundaries
            if not started_at or timestamp < started_at:
                started_at = timestamp
            if not ended_at or timestamp > ended_at:
                ended_at = timestamp

            # Track message counts
            if event_type == 'MESSAGE_SENT':
                message_count += 1
                user_message_count += 1
                if not first_question:
                    first_question = payload.get('content_preview', '')[:100]
            elif event_type == 'MESSAGE_RECEIVED':
                message_count += 1
                bot_message_count += 1

            # Track outcome (stronger outcomes override weaker)
            if event_type == 'FORM_COMPLETED':
                outcome = 'form_completed'
            elif event_type == 'LINK_CLICKED' and outcome != 'form_completed':
                outcome = 'link_clicked'
            elif event_type == 'CTA_CLICKED' and outcome not in ('form_completed', 'link_clicked'):
                outcome = 'cta_clicked'

            events.append({
                'step_number': step_number,
                'event_type': event_type,
                'timestamp': timestamp,
                'payload': payload
            })

        # Calculate duration
        duration_seconds = 0
        if started_at and ended_at:
            try:
                start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(ended_at.replace('Z', '+00:00'))
                duration_seconds = int((end_dt - start_dt).total_seconds())
            except (ValueError, TypeError):
                pass

        return cors_response(200, {
            'session_id': session_id,
            'tenant_id': tenant_id,
            'started_at': started_at,
            'ended_at': ended_at,
            'duration_seconds': duration_seconds,
            'summary': {
                'message_count': message_count,
                'user_message_count': user_message_count,
                'bot_message_count': bot_message_count,
                'outcome': outcome or 'conversation',
                'first_question': first_question
            },
            'events': events,
            'event_count': len(events)
        })

    except ClientError as e:
        logger.error(f"DynamoDB error fetching session: {e}")
        return cors_response(500, {'error': 'Failed to fetch session'})
    except Exception as e:
        logger.exception(f"Error fetching session detail: {e}")
        return cors_response(500, {'error': 'Internal server error'})


def handle_sessions_list(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /sessions/list
    Returns paginated list of sessions for the tenant with summary metrics.

    This queries the picasso-session-summaries table.
    Uses SK format SESSION#{started_at}#{session_id} for time-based queries.

    Query params:
    - range: Time range (1d, 7d, 30d, 90d) - default 30d
    - limit: Results per page (1-100) - default 25
    - cursor: Pagination cursor for next page
    - outcome: Filter by outcome (form_completed, link_clicked, abandoned, conversation)

    Returns:
    - List of session summaries
    - Pagination cursor for next page
    - Total count (estimated)
    """
    range_str = params.get('range', '30d')
    limit = min(max(1, int(params.get('limit', '25'))), 100)
    cursor = params.get('cursor')
    outcome_filter = params.get('outcome')

    # Validate outcome filter
    valid_outcomes = {'form_completed', 'link_clicked', 'abandoned', 'conversation', 'cta_clicked'}
    if outcome_filter and outcome_filter not in valid_outcomes:
        return cors_response(400, {
            'error': 'Invalid outcome filter',
            'valid_outcomes': list(valid_outcomes)
        })

    tenant_hash = get_tenant_hash(tenant_id)
    date_range = parse_date_range(range_str)

    # SK format is now SESSION#{session_id} (no timestamp)
    # Filter by started_at attribute instead
    start_date = date_range['start_date_iso']

    logger.info(f"Fetching sessions list for tenant: {tenant_hash}, range: {range_str}")

    try:
        # Build query parameters
        query_params = {
            'TableName': SESSION_SUMMARIES_TABLE,
            'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
            'FilterExpression': 'started_at >= :start_date',
            'ExpressionAttributeValues': {
                ':pk': {'S': f'TENANT#{tenant_hash}'},
                ':sk_prefix': {'S': 'SESSION#'},
                ':start_date': {'S': start_date}
            },
            'ScanIndexForward': False,  # Most recent first
            'Limit': limit
        }

        # Add outcome filter if specified
        if outcome_filter:
            # Backwards compatibility: 'conversation' also matches:
            # - Records with outcome='conversation'
            # - Records with outcome='browsing' (legacy)
            # - Records with no outcome attribute (default = conversation)
            if outcome_filter == 'conversation':
                query_params['FilterExpression'] += ' AND (#outcome = :outcome OR #outcome = :outcome_legacy OR attribute_not_exists(#outcome))'
                query_params['ExpressionAttributeNames'] = {'#outcome': 'outcome'}
                query_params['ExpressionAttributeValues'][':outcome'] = {'S': 'conversation'}
                query_params['ExpressionAttributeValues'][':outcome_legacy'] = {'S': 'browsing'}
            else:
                query_params['FilterExpression'] += ' AND #outcome = :outcome'
                query_params['ExpressionAttributeNames'] = {'#outcome': 'outcome'}
                query_params['ExpressionAttributeValues'][':outcome'] = {'S': outcome_filter}

        # Add pagination cursor if provided
        if cursor:
            try:
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor).decode('utf-8'))
                query_params['ExclusiveStartKey'] = cursor_data
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Invalid cursor: {e}")
                return cors_response(400, {'error': 'Invalid pagination cursor'})

        response = dynamodb.query(**query_params)
        items = response.get('Items', [])

        # Parse session summaries
        sessions = []
        for item in items:
            sk = item.get('sk', {}).get('S', '')
            # Parse session_id from SK format: SESSION#{session_id}
            sk_parts = sk.split('#')
            session_id = sk_parts[1] if len(sk_parts) >= 2 else ''

            # Extract fields with defaults
            started_at = item.get('started_at', {}).get('S', '')
            ended_at = item.get('ended_at', {}).get('S', started_at)
            # Normalize legacy 'browsing' to 'conversation'
            raw_outcome = item.get('outcome', {}).get('S', 'conversation')
            outcome = 'conversation' if raw_outcome == 'browsing' else raw_outcome
            message_count = int(item.get('message_count', {}).get('N', 0))
            user_message_count = int(item.get('user_message_count', {}).get('N', 0))
            bot_message_count = int(item.get('bot_message_count', {}).get('N', 0))
            first_question = item.get('first_question', {}).get('S', '')
            form_id = item.get('form_id', {}).get('S', '')

            # Calculate duration
            duration_seconds = 0
            if started_at and ended_at:
                try:
                    start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(ended_at.replace('Z', '+00:00'))
                    duration_seconds = int((end_dt - start_dt).total_seconds())
                except (ValueError, TypeError):
                    pass

            sessions.append({
                'session_id': session_id,
                'started_at': started_at,
                'ended_at': ended_at,
                'duration_seconds': duration_seconds,
                'outcome': outcome,
                'message_count': message_count,
                'user_message_count': user_message_count,
                'bot_message_count': bot_message_count,
                'first_question': first_question[:100] if first_question else '',
                'form_id': form_id if form_id else None
            })

        # Build next page cursor
        next_cursor = None
        if 'LastEvaluatedKey' in response:
            cursor_json = json.dumps(response['LastEvaluatedKey'])
            next_cursor = base64.urlsafe_b64encode(cursor_json.encode('utf-8')).decode('utf-8')

        return cors_response(200, {
            'tenant_id': tenant_id,
            'range': range_str,
            'sessions': sessions,
            'pagination': {
                'limit': limit,
                'count': len(sessions),
                'next_cursor': next_cursor,
                'has_more': next_cursor is not None
            },
            'filters': {
                'outcome': outcome_filter
            } if outcome_filter else {}
        })

    except ClientError as e:
        logger.error(f"DynamoDB error listing sessions: {e}")
        return cors_response(500, {'error': 'Failed to list sessions'})
    except Exception as e:
        logger.exception(f"Error listing sessions: {e}")
        return cors_response(500, {'error': 'Internal server error'})


# =============================================================================
# Lead Workspace Endpoints
# =============================================================================

# Valid pipeline status values
VALID_PIPELINE_STATUSES = {'new', 'reviewing', 'contacted', 'archived'}

# Submission ID validation pattern
SUBMISSION_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_\-]+$')


def handle_lead_detail(tenant_id: str, submission_id: str) -> Dict[str, Any]:
    """
    GET /leads/{submission_id}
    Returns full lead details for the workspace drawer.
    """
    # Validate feature access
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    # Validate submission_id format
    if not submission_id or not SUBMISSION_ID_PATTERN.match(submission_id):
        return cors_response(400, {'error': 'Invalid submission_id format'})

    try:
        # Direct GetItem by primary key
        response = dynamodb.get_item(
            TableName=FORM_SUBMISSIONS_TABLE,
            Key={'submission_id': {'S': submission_id}}
        )

        item = response.get('Item')
        if not item:
            return cors_response(404, {'error': 'Lead not found'})

        # Verify tenant ownership
        item_tenant = item.get('tenant_id', {}).get('S', '')
        if item_tenant != tenant_id:
            return cors_response(403, {'error': 'Access denied'})

        # Parse the lead data
        lead = parse_lead_from_dynamodb(item)

        # Get tenant name from config cache
        tenant_name = get_tenant_display_name(tenant_id)

        return cors_response(200, {
            'lead': lead,
            'tenant_name': tenant_name
        })

    except ClientError as e:
        logger.error(f"DynamoDB error fetching lead detail: {e}")
        return cors_response(500, {'error': 'Failed to fetch lead'})
    except Exception as e:
        logger.exception(f"Error fetching lead detail: {e}")
        return cors_response(500, {'error': 'Internal server error'})


def parse_lead_from_dynamodb(item: Dict) -> Dict[str, Any]:
    """
    Parse DynamoDB item into LeadWorkspaceData format.
    Uses pre-computed form_data_display if available, falls back to form_data_labeled.
    """
    submission_id = item.get('submission_id', {}).get('S', '')
    submitted_at = item.get('submitted_at', {}).get('S', '')
    form_id = item.get('form_id', {}).get('S', '')

    # Format date
    try:
        dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
        submitted_date = dt.strftime('%b %d')
    except (ValueError, AttributeError):
        submitted_date = submitted_at[:10] if submitted_at else 'Unknown'

    # Prefer pre-computed form_data_display (flat key-value structure)
    fields = {}
    form_data_display = item.get('form_data_display', {}).get('M', {})

    if form_data_display:
        # Use the pre-computed display format directly
        for label, value_obj in form_data_display.items():
            if isinstance(value_obj, dict) and 'S' in value_obj:
                fields[label] = value_obj['S']
            elif isinstance(value_obj, dict) and 'NULL' in value_obj:
                fields[label] = ''  # Skip null values
    else:
        # Fall back to parsing form_data_labeled (legacy records)
        form_data_labeled = item.get('form_data_labeled', {}).get('M', {})

        for field_label, field_wrapper in form_data_labeled.items():
            if not isinstance(field_wrapper, dict) or 'M' not in field_wrapper:
                continue

            field_obj = field_wrapper['M']
            value_obj = field_obj.get('value', {})

            # Convert label to snake_case key
            field_key = field_label.lower().replace(' ', '_')

            # Handle different value types
            if 'S' in value_obj:
                fields[field_key] = value_obj['S']
            elif 'M' in value_obj:
                # Composite field (e.g., Name with First/Last)
                nested = value_obj['M']
                parts = []
                for sub_key, sub_val in nested.items():
                    if isinstance(sub_val, dict) and 'S' in sub_val:
                        parts.append(sub_val['S'])
                fields[field_key] = ' '.join(parts)
            elif 'BOOL' in value_obj:
                fields[field_key] = 'Yes' if value_obj['BOOL'] else 'No'
            elif 'L' in value_obj:
                # List value
                list_items = []
                for list_item in value_obj['L']:
                    if 'S' in list_item:
                        list_items.append(list_item['S'])
                fields[field_key] = ', '.join(list_items)

    # Infer submission type from form_id
    submission_type = 'general'
    if 'volunteer' in form_id.lower() or 'mentor' in form_id.lower():
        submission_type = 'volunteer'
    elif 'donor' in form_id.lower() or 'donate' in form_id.lower():
        submission_type = 'donor'

    return {
        'submission_id': submission_id,
        'session_id': item.get('session_id', {}).get('S', ''),
        'form_id': form_id,
        'form_label': item.get('form_title', {}).get('S', form_id),
        'submitted_at': submitted_at,
        'submitted_date': submitted_date,
        'duration_seconds': 0,  # Not tracked
        'fields_completed': len(fields),
        'fields': fields,
        'pipeline_status': item.get('pipeline_status', {}).get('S', 'new'),
        'internal_notes': item.get('internal_notes', {}).get('S', ''),
        'processed_by': item.get('processed_by', {}).get('S'),
        'contacted_at': item.get('contacted_at', {}).get('S'),
        'archived_at': item.get('archived_at', {}).get('S'),
        'submission_type': submission_type,
        'program_id': form_id,
        'zip_code': fields.get('zip_code') or fields.get('zip') or fields.get('postal_code', '')
    }


def get_tenant_display_name(tenant_id: str) -> str:
    """Get tenant display name from cached config."""
    try:
        config = load_tenant_config(tenant_id)
        if config:
            return config.get('chat_title', config.get('organization_name', tenant_id))
        return tenant_id
    except Exception:
        return tenant_id


def handle_lead_status_update(
    tenant_id: str,
    submission_id: str,
    body: Dict,
    user_email: str
) -> Dict[str, Any]:
    """
    PATCH /leads/{submission_id}/status
    Update lead pipeline status.
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    # Validate submission_id format
    if not submission_id or not SUBMISSION_ID_PATTERN.match(submission_id):
        return cors_response(400, {'error': 'Invalid submission_id format'})

    # Validate request body
    new_status = body.get('pipeline_status')
    if not new_status or new_status not in VALID_PIPELINE_STATUSES:
        return cors_response(400, {
            'error': f'Invalid pipeline_status. Must be one of: {", ".join(sorted(VALID_PIPELINE_STATUSES))}'
        })

    try:
        # First, verify the lead exists and belongs to tenant
        response = dynamodb.get_item(
            TableName=FORM_SUBMISSIONS_TABLE,
            Key={'submission_id': {'S': submission_id}},
            ProjectionExpression='tenant_id, pipeline_status'
        )

        item = response.get('Item')
        if not item:
            return cors_response(404, {'error': 'Lead not found'})

        if item.get('tenant_id', {}).get('S') != tenant_id:
            return cors_response(403, {'error': 'Access denied'})

        now = datetime.utcnow().isoformat() + 'Z'

        # Build update expression
        update_expr = 'SET pipeline_status = :status, tenant_pipeline_key = :tpk, updated_at = :now, processed_by = :user'
        expr_values = {
            ':status': {'S': new_status},
            ':tpk': {'S': f'{tenant_id}#{new_status}'},
            ':now': {'S': now},
            ':user': {'S': user_email or 'unknown'}
        }
        expr_names = {}

        # Add timestamp for status-specific fields
        if new_status == 'contacted':
            update_expr += ', contacted_at = :contacted'
            expr_values[':contacted'] = {'S': now}
        elif new_status == 'archived':
            update_expr += ', archived_at = :archived'
            expr_values[':archived'] = {'S': now}
            # Set TTL for 1 year
            ttl = int((datetime.utcnow() + timedelta(days=365)).timestamp())
            update_expr += ', #ttl = :ttl'
            expr_values[':ttl'] = {'N': str(ttl)}
            expr_names['#ttl'] = 'ttl'

        # Perform update
        update_params = {
            'TableName': FORM_SUBMISSIONS_TABLE,
            'Key': {'submission_id': {'S': submission_id}},
            'UpdateExpression': update_expr,
            'ExpressionAttributeValues': expr_values
        }
        if expr_names:
            update_params['ExpressionAttributeNames'] = expr_names

        dynamodb.update_item(**update_params)

        logger.info(f"Lead {submission_id} status updated to {new_status} by {user_email}")

        return cors_response(200, {
            'submission_id': submission_id,
            'pipeline_status': new_status,
            'updated_at': now
        })

    except ClientError as e:
        logger.error(f"DynamoDB error updating lead status: {e}")
        return cors_response(500, {'error': 'Failed to update status'})
    except Exception as e:
        logger.exception(f"Error updating lead status: {e}")
        return cors_response(500, {'error': 'Internal server error'})


def handle_lead_notes_update(
    tenant_id: str,
    submission_id: str,
    body: Dict,
    user_email: str
) -> Dict[str, Any]:
    """
    PATCH /leads/{submission_id}/notes
    Update lead internal notes.
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    # Validate submission_id format
    if not submission_id or not SUBMISSION_ID_PATTERN.match(submission_id):
        return cors_response(400, {'error': 'Invalid submission_id format'})

    # Validate request body
    notes = body.get('internal_notes')
    if notes is None:
        return cors_response(400, {'error': 'internal_notes field required'})

    # Limit notes length
    if len(notes) > 10000:
        return cors_response(400, {'error': 'Notes too long (max 10000 characters)'})

    try:
        # Verify lead exists and belongs to tenant
        response = dynamodb.get_item(
            TableName=FORM_SUBMISSIONS_TABLE,
            Key={'submission_id': {'S': submission_id}},
            ProjectionExpression='tenant_id'
        )

        item = response.get('Item')
        if not item:
            return cors_response(404, {'error': 'Lead not found'})

        if item.get('tenant_id', {}).get('S') != tenant_id:
            return cors_response(403, {'error': 'Access denied'})

        now = datetime.utcnow().isoformat() + 'Z'

        # Update notes
        dynamodb.update_item(
            TableName=FORM_SUBMISSIONS_TABLE,
            Key={'submission_id': {'S': submission_id}},
            UpdateExpression='SET internal_notes = :notes, updated_at = :now, processed_by = :user',
            ExpressionAttributeValues={
                ':notes': {'S': notes},
                ':now': {'S': now},
                ':user': {'S': user_email or 'unknown'}
            }
        )

        logger.info(f"Lead {submission_id} notes updated by {user_email}")

        return cors_response(200, {
            'submission_id': submission_id,
            'internal_notes': notes,
            'updated_at': now
        })

    except ClientError as e:
        logger.error(f"DynamoDB error updating lead notes: {e}")
        return cors_response(500, {'error': 'Failed to update notes'})
    except Exception as e:
        logger.exception(f"Error updating lead notes: {e}")
        return cors_response(500, {'error': 'Internal server error'})


def handle_lead_reactivate(
    tenant_id: str,
    submission_id: str,
    user_email: str
) -> Dict[str, Any]:
    """
    PATCH /leads/{submission_id}/reactivate
    Reactivate an archived lead, resetting status to 'new' and prepending system note.

    Per PRD: Emerald Lead Reactivation Engine v4.2.1
    - Idempotency: No-op if lead is already active
    - Audit Trail: Prepends [System] restoration note to internal_notes
    - State Reset: Returns lead to 'new' status
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    # Validate submission_id format
    if not submission_id or not SUBMISSION_ID_PATTERN.match(submission_id):
        return cors_response(400, {'error': 'Invalid submission_id format'})

    try:
        # Fetch current lead state
        response = dynamodb.get_item(
            TableName=FORM_SUBMISSIONS_TABLE,
            Key={'submission_id': {'S': submission_id}},
            ProjectionExpression='tenant_id, pipeline_status, internal_notes'
        )

        item = response.get('Item')
        if not item:
            return cors_response(404, {'error': 'Lead not found'})

        if item.get('tenant_id', {}).get('S') != tenant_id:
            return cors_response(403, {'error': 'Access denied'})

        current_status = item.get('pipeline_status', {}).get('S', 'new')

        # Idempotency check: if not archived, no-op
        if current_status != 'archived':
            logger.info(f"Lead {submission_id} is already active (status: {current_status}), skipping reactivation")
            return cors_response(200, {
                'submission_id': submission_id,
                'pipeline_status': current_status,
                'reactivated': False,
                'message': 'Lead is already active'
            })

        now = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

        # Build system note (per PRD audit trail requirements)
        system_note = f"[System] Restored from Archive at {now}\n---\n"

        # Prepend to existing notes (preserve history)
        existing_notes = item.get('internal_notes', {}).get('S', '')
        new_notes = system_note + existing_notes

        # Build update expression
        # - Reset status to 'new'
        # - Update tenant_pipeline_key for GSI
        # - Prepend system note
        # - Remove archived_at and ttl (un-archiving)
        update_expr = '''
            SET pipeline_status = :status,
                tenant_pipeline_key = :tpk,
                internal_notes = :notes,
                updated_at = :now,
                processed_by = :user,
                reactivated_at = :now
            REMOVE archived_at, #ttl
        '''

        expr_values = {
            ':status': {'S': 'new'},
            ':tpk': {'S': f'{tenant_id}#new'},
            ':notes': {'S': new_notes},
            ':now': {'S': now},
            ':user': {'S': user_email or 'unknown'}
        }

        expr_names = {
            '#ttl': 'ttl'  # ttl is a reserved word
        }

        dynamodb.update_item(
            TableName=FORM_SUBMISSIONS_TABLE,
            Key={'submission_id': {'S': submission_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names
        )

        logger.info(f"Lead {submission_id} reactivated from archive by {user_email}")

        return cors_response(200, {
            'submission_id': submission_id,
            'pipeline_status': 'new',
            'reactivated': True,
            'reactivated_at': now,
            'message': 'Lead restored from archive'
        })

    except ClientError as e:
        logger.error(f"DynamoDB error reactivating lead: {e}")
        return cors_response(500, {'error': 'Failed to reactivate lead'})
    except Exception as e:
        logger.exception(f"Error reactivating lead: {e}")
        return cors_response(500, {'error': 'Internal server error'})


def handle_lead_queue(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /leads/queue
    Get next lead in queue and total count.

    Query params:
    - status: Filter by pipeline status (default: 'new')
    - current_id: Current submission_id to find next after
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_forms')
    if access_error:
        return access_error

    status_filter = params.get('status', 'new')
    current_id = params.get('current_id')

    if status_filter not in VALID_PIPELINE_STATUSES:
        return cors_response(400, {'error': f'Invalid status. Must be one of: {", ".join(sorted(VALID_PIPELINE_STATUSES))}'})

    try:
        # Query using the tenant-pipeline-index GSI
        tenant_pipeline_key = f'{tenant_id}#{status_filter}'

        response = dynamodb.query(
            TableName=FORM_SUBMISSIONS_TABLE,
            IndexName='tenant-pipeline-index',
            KeyConditionExpression='tenant_pipeline_key = :tpk',
            ExpressionAttributeValues={
                ':tpk': {'S': tenant_pipeline_key}
            },
            ScanIndexForward=True,  # Oldest first (FIFO)
            ProjectionExpression='submission_id, submitted_at'
        )

        items = response.get('Items', [])
        queue_count = len(items)

        # Find next lead after current_id
        next_lead_id = None
        if items:
            if current_id:
                # Find position of current lead and return next
                for i, item in enumerate(items):
                    if item.get('submission_id', {}).get('S') == current_id:
                        if i + 1 < len(items):
                            next_lead_id = items[i + 1].get('submission_id', {}).get('S')
                        break
                # If current not found or was last, return first
                if next_lead_id is None and items:
                    next_lead_id = items[0].get('submission_id', {}).get('S')
            else:
                # No current, return first in queue
                next_lead_id = items[0].get('submission_id', {}).get('S')

        return cors_response(200, {
            'next_lead_id': next_lead_id,
            'queue_count': queue_count,
            'status': status_filter
        })

    except ClientError as e:
        logger.error(f"DynamoDB error fetching lead queue: {e}")
        return cors_response(500, {'error': 'Failed to fetch queue'})
    except Exception as e:
        logger.exception(f"Error fetching lead queue: {e}")
        return cors_response(500, {'error': 'Internal server error'})
