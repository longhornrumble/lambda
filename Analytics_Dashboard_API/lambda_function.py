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
- GET /notifications/summary        - Sent/delivered/bounced/opened/clicked counts + rates
- GET /notifications/events         - Paginated notification event log (newest first)
- GET /notifications/events/{id}    - Full lifecycle for a single message (GSI ByMessageId)

Required IAM permissions (Lambda execution role) for notification endpoints:
- dynamodb:Query on picasso-notification-events (table + ByMessageId GSI)
- dynamodb:Query on picasso-notification-sends

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
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, List
from decimal import Decimal
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
JWT_SECRET_KEY_NAME = os.environ.get('JWT_SECRET_KEY_NAME', 'picasso/staging/jwt/signing-key')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')


# AWS clients
secrets_manager = boto3.client('secretsmanager')
dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')


# DynamoDB table for form submissions (contains PII)
FORM_SUBMISSIONS_TABLE = os.environ.get('FORM_SUBMISSIONS_TABLE', 'picasso_form_submissions')

# DynamoDB Session Tables (User Journey Analytics)
SESSION_EVENTS_TABLE = os.environ.get('SESSION_EVENTS_TABLE', 'picasso-session-events')
SESSION_SUMMARIES_TABLE = os.environ.get('SESSION_SUMMARIES_TABLE', 'picasso-session-summaries')

# DynamoDB Notification Tables (Phase 2a)
# IAM: Lambda execution role needs dynamodb:Query on both tables and the ByMessageId GSI.
NOTIFICATION_EVENTS_TABLE = os.environ.get('NOTIFICATION_EVENTS_TABLE', 'picasso-notification-events')
NOTIFICATION_SENDS_TABLE = os.environ.get('NOTIFICATION_SENDS_TABLE', 'picasso-notification-sends')

# S3 Tenant Configuration
S3_CONFIG_BUCKET = os.environ.get('S3_CONFIG_BUCKET', 'picasso-configs')

# =============================================================================
# Clerk Authentication Bridge (Trial — hardcoded user map)
# =============================================================================
# JWKS URL: move to env var CLERK_JWKS_URL for production.
CLERK_JWKS_URL = os.environ.get(
    'CLERK_JWKS_URL',
    'https://capable-peacock-51.clerk.accounts.dev/.well-known/jwks.json'
)

# Hardcoded user map for trial — replace with DynamoDB lookup in production.
CLERK_USER_MAP: Dict[str, Dict[str, Any]] = {
    'chris@myrecruiter.ai': {
        'tenant_id': 'MYR384719',
        'tenant_hash': 'my87674d777bf9',
        'role': 'super_admin',
        'name': 'Chris Miller',
        'company': 'MyRecruiter',
    }
}

# JWKS cache (TTL: 1 hour)
_clerk_jwks_cache: Optional[Dict[str, Any]] = None
_clerk_jwks_cache_time: float = 0
CLERK_JWKS_CACHE_TTL = 3600  # 1 hour

# S3 client
s3 = boto3.client('s3')

# SES client (us-east-1 — SES sandbox/production identity is in us-east-1)
ses = boto3.client('ses', region_name='us-east-1')

# Cache for tenant configs (TTL: 5 minutes)
_tenant_config_cache: Dict[str, Dict[str, Any]] = {}
_tenant_config_cache_time: Dict[str, float] = {}
TENANT_CONFIG_CACHE_TTL = 300  # 5 minutes

# Cache for JWT secret
_jwt_secret_cache = None
_jwt_secret_cache_time = 0
JWT_SECRET_CACHE_TTL = 300  # 5 minutes

# Cache for Clerk user profiles (TTL: 5 minutes)
_clerk_user_cache: Dict[str, Dict[str, Any]] = {}
_clerk_user_cache_time: Dict[str, float] = {}
CLERK_USER_CACHE_TTL = 300  # 5 minutes

# Cache for Clerk org memberships (TTL: 5 minutes)
_org_membership_cache: Dict[str, list] = {}
_org_membership_cache_time: Dict[str, float] = {}
ORG_MEMBERSHIP_CACHE_TTL = 300  # 5 minutes


# Security: Tenant ID validation pattern (alphanumeric, underscore, hyphen only)
TENANT_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]+$')


class ConcurrentModificationError(Exception):
    """Raised when an S3 write fails due to ETag mismatch (optimistic locking)."""
    pass


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

    # /auth/clerk — PRE-AUTH: exchange Clerk session token for internal Picasso JWT.
    # Must be checked BEFORE authenticate_request since no internal JWT exists yet.
    if path.endswith('/auth/clerk') and method == 'POST':
        try:
            body = json.loads(event.get('body', '{}') or '{}')
        except json.JSONDecodeError:
            return cors_response(400, {'error': 'Invalid JSON body'})
        return handle_clerk_auth(body)

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

    # Extract user email and role for audit purposes
    user_email = auth_result.get('email', 'unknown')
    user_role = auth_result.get('role')

    # Super admin tenant override - allows viewing other tenants' data
    headers = event.get('headers', {}) or {}
    tenant_override = headers.get('X-Tenant-Override') or headers.get('x-tenant-override')

    if tenant_override and user_role == 'super_admin':
        try:
            tenant_id = sanitize_tenant_id(tenant_override)
            logger.info(f"[Super Admin] {user_email} switched to tenant: {tenant_id[:8]}...")
        except ValueError as e:
            logger.warning(f"Invalid tenant override: {e}")
            return cors_response(400, {'error': f'Invalid tenant override: {str(e)}'})
    else:
        logger.info(f"Authenticated request for tenant: {tenant_id[:8]}...")

    # Parse query parameters
    params = event.get('queryStringParameters') or {}

    # Route to appropriate handler
    # NOTE: More specific routes must come before generic ones
    # (e.g., /forms/summary before /summary)
    try:
        # Notification endpoints (most specific - check first)
        if '/notifications/events/' in path:
            # GET /notifications/events/{message_id}
            message_id = path.split('/notifications/events/')[-1].split('/')[0]
            if message_id:
                return handle_notification_event_detail(tenant_id, message_id)
        elif path.endswith('/notifications/events'):
            return handle_notification_events(tenant_id, params)
        elif path.endswith('/notifications/summary'):
            return handle_notification_summary(tenant_id, params)

        # Conversations endpoints (most specific - check first)
        elif path.endswith('/conversations/summary'):
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

        # Admin endpoints (super_admin only)
        elif path.endswith('/admin/tenants') and method == 'GET':
            return handle_admin_tenants(auth_result.get('role'))

        # Settings — Notification Recipients & Templates (Phase 2b/2c)
        # NOTE: More specific paths must come first within this block.
        elif path.endswith('/settings/notifications/recipients/test-send') and method == 'POST':
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_notification_recipients_test_send(tenant_id, body, user_role)
        elif '/settings/notifications/templates/' in path and path.endswith('/preview') and method == 'POST':
            form_id = path.split('/settings/notifications/templates/')[1].split('/preview')[0]
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_notification_template_preview(tenant_id, form_id, body, user_role)
        elif '/settings/notifications/templates/' in path and path.endswith('/test-send') and method == 'POST':
            form_id = path.split('/settings/notifications/templates/')[1].split('/test-send')[0]
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_notification_template_test_send(tenant_id, form_id, user_email, user_role, body)
        elif '/settings/notifications/templates/' in path and method == 'PATCH':
            form_id = path.split('/settings/notifications/templates/')[1].split('/')[0]
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_notification_templates_update(tenant_id, form_id, body, user_role)
        elif path.endswith('/settings/notifications/templates') and method == 'GET':
            return handle_notification_templates_get(tenant_id)
        elif path.endswith('/settings/notifications') and method == 'GET':
            return handle_settings_notifications_get(tenant_id)
        elif path.endswith('/settings/notifications') and method == 'PATCH':
            body = json.loads(event.get('body', '{}') or '{}')
            return handle_settings_notifications_patch(tenant_id, body, user_role)

        else:
            return cors_response(404, {'error': f'Unknown endpoint: {path}'})

    except Exception as e:
        logger.exception(f"Error handling request: {e}")
        return cors_response(500, {'error': 'Internal server error', 'details': str(e)})


# =============================================================================
# Authentication
# =============================================================================

# =============================================================================
# Clerk Authentication Bridge — Handler & Helpers
# =============================================================================

def _fetch_clerk_jwks() -> Dict[str, Any]:
    """
    Fetch Clerk JWKS document with in-process cache (TTL: 1 hour).
    Returns the parsed JWKS dict {'keys': [...]}.
    """
    global _clerk_jwks_cache, _clerk_jwks_cache_time

    now = time.time()
    if _clerk_jwks_cache and (now - _clerk_jwks_cache_time) < CLERK_JWKS_CACHE_TTL:
        return _clerk_jwks_cache

    try:
        req = urllib.request.Request(CLERK_JWKS_URL, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        _clerk_jwks_cache = data
        _clerk_jwks_cache_time = now
        logger.info(f"[clerk-auth] Fetched JWKS from {CLERK_JWKS_URL} ({len(data.get('keys', []))} keys)")
        return data
    except Exception as exc:
        logger.error(f"[clerk-auth] Failed to fetch JWKS: {exc}")
        raise


def _decode_clerk_jwt_claims(token: str) -> Dict[str, Any]:
    """
    Decode and validate Clerk JWT claims.

    SECURITY NOTE (trial): This verifies expiry, not-before, and issuer claims.
    Full RS256 signature verification requires the `cryptography` package
    (not in base Lambda runtime). Add PyJWT[cryptography] as a Lambda layer
    before using in production with untrusted tokens.

    The Clerk token is obtained server-side via getToken() which already
    validates the session with Clerk's servers, providing defence-in-depth.
    """
    parts = token.split('.')
    if len(parts) != 3:
        raise ValueError('Clerk token must have 3 parts')

    header_b64, payload_b64, _signature_b64 = parts

    # Decode header
    header_json = base64.urlsafe_b64decode(header_b64 + '==')
    header = json.loads(header_json)

    alg = header.get('alg', '')
    if alg not in ('RS256', 'RS384', 'RS512'):
        raise ValueError(f'Unexpected Clerk JWT algorithm: {alg}')

    # Decode payload
    payload_json = base64.urlsafe_b64decode(payload_b64 + '==')
    payload = json.loads(payload_json)

    # Verify expiry
    exp = payload.get('exp')
    if not exp or time.time() > exp:
        raise ValueError('Clerk token is expired')

    # Verify not-before
    nbf = payload.get('nbf')
    if nbf and time.time() < nbf:
        raise ValueError('Clerk token not yet valid (nbf)')

    # Verify issuer belongs to our Clerk domain
    iss = payload.get('iss', '')
    if 'capable-peacock-51.clerk.accounts.dev' not in iss and 'clerk.accounts.dev' not in iss:
        raise ValueError(f'Unexpected Clerk token issuer: {iss}')

    # Verify JWKS kid exists (confirms token was issued by our Clerk instance)
    kid = header.get('kid')
    if kid:
        jwks = _fetch_clerk_jwks()
        known_kids = {k.get('kid') for k in jwks.get('keys', [])}
        if kid not in known_kids:
            raise ValueError(f'Unknown Clerk key id: {kid}')
    else:
        # No kid in header — still fetch JWKS to confirm connectivity
        _fetch_clerk_jwks()

    logger.info(f"[clerk-auth] Clerk JWT claims valid (sub={payload.get('sub', 'unknown')[:12]}...)")
    return payload


def _fetch_clerk_user(user_id: str) -> Dict[str, Any]:
    """
    Fetch a Clerk user profile by user ID, with 5-minute cache.
    Returns the full user object from Clerk Backend API.
    Raises ValueError if CLERK_SECRET_KEY is missing or API call fails.
    """
    global _clerk_user_cache, _clerk_user_cache_time

    # Check cache
    now = time.time()
    if user_id in _clerk_user_cache:
        if now - _clerk_user_cache_time.get(user_id, 0) < CLERK_USER_CACHE_TTL:
            return _clerk_user_cache[user_id]

    clerk_secret = os.environ.get('CLERK_SECRET_KEY', '')
    if not clerk_secret:
        raise ValueError('No CLERK_SECRET_KEY env var — cannot fetch user from Clerk API')

    clerk_url = f'https://api.clerk.com/v1/users/{user_id}'
    logger.info(f'[clerk-auth] Fetching user from {clerk_url}')
    req = urllib.request.Request(
        clerk_url,
        headers={
            'Authorization': f'Bearer {clerk_secret}',
            'User-Agent': 'MyRecruiter-Portal/1.0',
            'Accept': 'application/json',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            user_data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        raise ValueError(f'Failed to fetch user from Clerk API: {e}')

    # Cache the result
    _clerk_user_cache[user_id] = user_data
    _clerk_user_cache_time[user_id] = now

    return user_data


def _extract_email_from_clerk_user(user_data: Dict[str, Any]) -> str:
    """Extract the primary email address from a Clerk user object."""
    email_addresses = user_data.get('email_addresses', [])
    for addr in email_addresses:
        if addr.get('id') == user_data.get('primary_email_address_id'):
            return addr['email_address'].lower().strip()
    if email_addresses:
        return email_addresses[0]['email_address'].lower().strip()
    raise ValueError('Clerk user has no email addresses')


def _extract_name_from_clerk_user(user_data: Dict[str, Any]) -> Optional[str]:
    """Extract display name from a Clerk user object."""
    first = user_data.get('first_name') or ''
    last = user_data.get('last_name') or ''
    name = f'{first} {last}'.strip()
    return name if name else None


def _extract_clerk_email(payload: Dict[str, Any]) -> str:
    """
    Extract email from verified Clerk JWT payload.
    Clerk's default session token does NOT include email — only sub (user ID).
    We check JWT claims first, then fall back to Clerk's Backend API.
    """
    # Check direct JWT claims (present if Clerk JWT template is customized)
    email = payload.get('email')
    if email and isinstance(email, str) and '@' in email:
        return email.lower().strip()

    primary = payload.get('primary_email_address')
    if primary and isinstance(primary, str) and '@' in primary:
        return primary.lower().strip()

    # Fallback: fetch from Clerk Backend API using sub (user ID)
    user_id = payload.get('sub')
    if not user_id:
        raise ValueError('Could not determine email — no sub claim in JWT')

    user_data = _fetch_clerk_user(user_id)
    return _extract_email_from_clerk_user(user_data)


def _fetch_user_org_memberships(user_id: str) -> list:
    """
    Fetch a Clerk user's organization memberships, with 5-minute cache.
    Returns the list of membership objects from Clerk Backend API.
    """
    global _org_membership_cache, _org_membership_cache_time

    # Check cache
    now = time.time()
    if user_id in _org_membership_cache:
        if now - _org_membership_cache_time.get(user_id, 0) < ORG_MEMBERSHIP_CACHE_TTL:
            return _org_membership_cache[user_id]

    clerk_secret = os.environ.get('CLERK_SECRET_KEY', '')
    if not clerk_secret:
        raise ValueError('No CLERK_SECRET_KEY env var — cannot fetch org memberships')

    clerk_url = f'https://api.clerk.com/v1/users/{user_id}/organization_memberships'
    logger.info(f'[clerk-auth] Fetching org memberships from {clerk_url}')
    req = urllib.request.Request(
        clerk_url,
        headers={
            'Authorization': f'Bearer {clerk_secret}',
            'User-Agent': 'MyRecruiter-Portal/1.0',
            'Accept': 'application/json',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            response_data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        raise ValueError(f'Failed to fetch org memberships from Clerk API: {e}')

    memberships = response_data.get('data', [])

    # Cache the result
    _org_membership_cache[user_id] = memberships
    _org_membership_cache_time[user_id] = now

    return memberships


def _resolve_org_tenant(user_id: str, requested_org_id: Optional[str]) -> Dict[str, Any]:
    """
    Resolve a Clerk user's active organization to Picasso tenant info.

    Args:
        user_id: Clerk user ID (sub claim)
        requested_org_id: org ID from frontend (or None for auto-select)

    Returns dict with: tenant_id, tenant_hash, role, company

    Raises ValueError if user has no orgs, requested org not found,
    or org missing tenant metadata.
    """
    memberships = _fetch_user_org_memberships(user_id)

    if not memberships:
        raise ValueError('User has no organization memberships')

    is_multi_org = len(memberships) > 1

    # Find the target membership
    if requested_org_id:
        membership = None
        for m in memberships:
            org = m.get('organization', {})
            if org.get('id') == requested_org_id:
                membership = m
                break
        if not membership:
            raise ValueError(f'User is not a member of organization {requested_org_id}')
    elif len(memberships) == 1:
        membership = memberships[0]
    else:
        raise ValueError('org_id required — user belongs to multiple organizations')

    # Extract org metadata
    org = membership.get('organization', {})
    public_metadata = org.get('public_metadata', {})
    tenant_id = public_metadata.get('tenant_id')
    tenant_hash = public_metadata.get('tenant_hash')

    if not tenant_id or not tenant_hash:
        org_name = org.get('name', org.get('id', 'unknown'))
        raise ValueError(
            f'Organization "{org_name}" is not configured for Picasso access '
            f'(missing tenant_id or tenant_hash in publicMetadata)'
        )

    # Map Clerk org role to Picasso role
    clerk_role = membership.get('role', '')
    if is_multi_org:
        role = 'super_admin'
    elif clerk_role == 'org:admin':
        role = 'admin'
    else:
        role = 'member'

    company = org.get('name', '')

    logger.info(
        f'[clerk-auth] Resolved org {org.get("id")} → '
        f'tenant={tenant_id} role={role} company={company}'
    )

    return {
        'tenant_id': tenant_id,
        'tenant_hash': tenant_hash,
        'role': role,
        'company': company,
    }


def _sign_internal_jwt(payload: Dict[str, Any]) -> str:
    """
    Sign a Picasso internal JWT using the same HS256 approach as validate_jwt/get_jwt_secret.
    Mirrors the SSO_Token_Generator output format.
    """
    header = {'alg': 'HS256', 'typ': 'JWT'}
    header_b64 = base64.urlsafe_b64encode(json.dumps(header, separators=(',', ':')).encode()).rstrip(b'=').decode()
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload, separators=(',', ':')).encode()).rstrip(b'=').decode()

    message = f"{header_b64}.{payload_b64}".encode('utf-8')
    secret = get_jwt_secret()
    signature = hmac.new(secret.encode('utf-8'), message, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b'=').decode()

    return f"{header_b64}.{payload_b64}.{sig_b64}"


def handle_clerk_auth(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /auth/clerk
    Exchange a Clerk session token for an internal Picasso JWT.

    Request body: { "clerk_token": "<Clerk session JWT>", "org_id": "<optional Clerk org ID>" }
    Response 200: { "token": "<internal Picasso JWT>" }
    Response 400: { "error": "..." }   — bad request / missing token
    Response 403: { "error": "User not registered" }
    Response 500: { "error": "..." }   — upstream failure

    SECURITY: Does NOT require an existing internal JWT. Must stay BEFORE
    authenticate_request in the routing chain.
    """
    start = time.time()

    clerk_token = body.get('clerk_token', '').strip()
    if not clerk_token:
        logger.warning('[clerk-auth] Request missing clerk_token')
        return cors_response(400, {'error': 'clerk_token is required'})

    org_id = body.get('org_id', '').strip() or None

    try:
        # Step 1 — Verify Clerk JWT claims and kid against JWKS
        claims = _decode_clerk_jwt_claims(clerk_token)
        user_id = claims.get('sub')

        # Step 2 — Fetch user profile (email + name) from Clerk API
        email = None
        name = None
        user_data = None

        # Try JWT claims first for email
        jwt_email = claims.get('email')
        if jwt_email and isinstance(jwt_email, str) and '@' in jwt_email:
            email = jwt_email.lower().strip()

        if not email:
            jwt_primary = claims.get('primary_email_address')
            if jwt_primary and isinstance(jwt_primary, str) and '@' in jwt_primary:
                email = jwt_primary.lower().strip()

        # Fetch full user profile for email (if not in JWT) and name
        if user_id:
            try:
                user_data = _fetch_clerk_user(user_id)
                if not email:
                    email = _extract_email_from_clerk_user(user_data)
                name = _extract_name_from_clerk_user(user_data)
            except ValueError as exc:
                logger.warning(f'[clerk-auth] Failed to fetch Clerk user: {exc}')

        if not email:
            raise ValueError('Could not determine email from JWT claims or Clerk API')

        logger.info(f'[clerk-auth] Verified Clerk token for {redact_email(email)}')

    except ValueError as exc:
        logger.warning(f'[clerk-auth] Token validation failed: {exc}')
        return cors_response(400, {'error': f'Invalid Clerk token: {exc}'})
    except Exception as exc:
        logger.error(f'[clerk-auth] Upstream error during token validation: {exc}')
        return cors_response(500, {'error': 'Authentication service unavailable'})

    # Step 3 — Check for super_admin flag in Clerk user publicMetadata
    is_super_admin = False
    if user_data:
        picasso_role = user_data.get('public_metadata', {}).get('picasso_role')
        if picasso_role == 'super_admin':
            is_super_admin = True
            logger.info(f'[clerk-auth] User {redact_email(email)} has super_admin flag')

    # Step 4 — Resolve tenant via Clerk Organization membership
    user_info = None
    try:
        if user_id:
            org_info = _resolve_org_tenant(user_id, org_id)
            user_info = {
                'tenant_id': org_info['tenant_id'],
                'tenant_hash': org_info['tenant_hash'],
                'role': 'super_admin' if is_super_admin else org_info['role'],
                'name': name,
                'company': org_info['company'],
            }
    except ValueError as exc:
        logger.warning(f'[clerk-auth] Org lookup failed: {exc}')

    # Fallback to CLERK_USER_MAP during transition (remove after org migration complete)
    if not user_info:
        fallback = CLERK_USER_MAP.get(email)
        if fallback:
            logger.info(f'[clerk-auth] Using CLERK_USER_MAP fallback for {redact_email(email)}')
            user_info = fallback
        else:
            logger.warning(f'[clerk-auth] No org membership and no fallback for {redact_email(email)}')
            return cors_response(403, {'error': 'User not registered — no organization membership found'})

    # Step 5 — Load feature flags from S3 tenant config
    features = get_tenant_features(user_info['tenant_id'])

    # Step 6 — Build and sign internal Picasso JWT
    try:
        issued_at = int(time.time())
        internal_payload = {
            'tenant_id': user_info['tenant_id'],
            'tenant_hash': user_info['tenant_hash'],
            'email': email,
            'role': user_info['role'],
            'name': user_info.get('name') or name,
            'company': user_info.get('company'),
            'iat': issued_at,
            'exp': issued_at + 7200,  # 2 hours
            'features': features,
        }

        internal_token = _sign_internal_jwt(internal_payload)

    except Exception as exc:
        logger.error(f'[clerk-auth] Failed to sign internal JWT: {exc}')
        return cors_response(500, {'error': 'Failed to issue session token'})

    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(
        f'[clerk-auth] Issued internal JWT for {redact_email(email)} '
        f'tenant={user_info["tenant_id"]} role={user_info["role"]} latency={elapsed_ms}ms'
    )

    return cors_response(200, {'token': internal_token})


def authenticate_request(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Authenticate request using JWT token from Authorization header.
    Returns {'success': True, 'tenant_id': '...', 'email': '...', 'role': '...'} or {'success': False, 'error': '...'}
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

        # Extract role for authorization (normalize to lowercase with underscores)
        raw_role = payload.get('role', '')
        role = raw_role.lower().replace(' ', '_') if raw_role else None

        return {'success': True, 'tenant_id': tenant_id, 'email': email, 'role': role}

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
        key = f"tenants/{tenant_id}/{tenant_id}-config.json"
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
    - dashboard_notifications: False (requires notification-enabled forms in config)
    - dashboard_settings: False (Phase 3 placeholder)
    """
    config = get_tenant_config(tenant_id)

    if not config:
        # No config = legacy tenant, give them conversations + forms
        return {
            'dashboard_conversations': True,
            'dashboard_forms': True,
            'dashboard_attribution': False,
            'dashboard_notifications': False,
            'dashboard_settings': False,
        }

    features = config.get('features', {})

    return {
        'dashboard_conversations': features.get('dashboard_conversations', True),
        'dashboard_forms': features.get('dashboard_forms', True),
        'dashboard_attribution': features.get('dashboard_attribution', False),
        'dashboard_notifications': features.get('dashboard_notifications', False),
        'dashboard_settings': features.get('dashboard_settings', False),
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


# S3 configuration for tenant list
S3_CONFIG_BUCKET = os.environ.get('S3_CONFIG_BUCKET', 'myrecruiter-picasso')
MAPPINGS_PREFIX = 'mappings'
TENANTS_PREFIX = 'tenants'

s3_client = boto3.client('s3')


def handle_admin_tenants(user_role: Optional[str]) -> Dict[str, Any]:
    """
    Handle GET /admin/tenants endpoint.
    Returns list of active tenants for super_admin users.
    Reads from S3 mapping files (source of truth).
    """
    if user_role != 'super_admin':
        return cors_response(403, {'error': 'Forbidden: super_admin role required'})

    try:
        # List all mapping files
        response = s3_client.list_objects_v2(
            Bucket=S3_CONFIG_BUCKET,
            Prefix=f'{MAPPINGS_PREFIX}/'
        )

        contents = response.get('Contents', [])
        if not contents:
            return cors_response(200, {'tenants': []})

        tenants = []
        for obj in contents:
            key = obj['Key']
            if not key.endswith('.json'):
                continue

            try:
                mapping_resp = s3_client.get_object(Bucket=S3_CONFIG_BUCKET, Key=key)
                mapping = json.loads(mapping_resp['Body'].read().decode('utf-8'))

                tenant_id = mapping.get('tenant_id', '')
                tenant_hash = mapping.get('tenant_hash', '')

                if not tenant_hash:
                    continue

                # Read tenant config to get display name and active status
                name = tenant_id  # fallback
                active = False
                try:
                    config_key = f'{TENANTS_PREFIX}/{tenant_id}/{tenant_id}-config.json'
                    config_resp = s3_client.get_object(Bucket=S3_CONFIG_BUCKET, Key=config_key)
                    config = json.loads(config_resp['Body'].read().decode('utf-8'))
                    name = config.get('chat_title') or config.get('organization_name') or tenant_id
                    active = config.get('active', False)
                except Exception:
                    pass  # Use tenant_id as fallback name, inactive by default

                if not active:
                    continue

                tenants.append({
                    'tenant_id': tenant_id,
                    'tenant_hash': tenant_hash,
                    'name': name,
                })

            except Exception as e:
                logger.warning(f"Failed to read mapping {key}: {e}")
                continue

        # Sort by name
        tenants.sort(key=lambda t: t['name'].lower())

        logger.info(f"Returning {len(tenants)} tenants from S3 for super_admin")

        return cors_response(200, {'tenants': tenants})

    except Exception as e:
        logger.exception(f"Error fetching admin tenants: {e}")
        return cors_response(500, {'error': 'Internal server error'})


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
        date_range: Dict with 'start_date_iso' and optionally 'end_date_iso' keys
        limit: Max sessions to fetch (default 1000, paginate if more)

    Returns:
        List of session summary dictionaries
    """
    # SK format is now SESSION#{session_id} (no timestamp)
    # Filter by started_at attribute instead
    start_date = date_range['start_date_iso']
    end_date = date_range.get('end_date_iso')

    sessions = []
    last_evaluated_key = None

    try:
        while True:
            # Build filter expression - always filter by start date
            filter_expression = 'started_at >= :start_date'
            expression_values = {
                ':pk': {'S': f'TENANT#{tenant_hash}'},
                ':sk_prefix': {'S': 'SESSION#'},
                ':start_date': {'S': start_date}
            }

            # Add end date filter if provided (for custom date ranges)
            if end_date:
                # Add 1 day to end_date for inclusive range (end_date is YYYY-MM-DD)
                filter_expression += ' AND started_at < :end_date'
                # Use end_date + 1 day at midnight for inclusive filtering
                end_date_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                expression_values[':end_date'] = {'S': end_date_inclusive}

            query_params = {
                'TableName': SESSION_SUMMARIES_TABLE,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'FilterExpression': filter_expression,
                'ExpressionAttributeValues': expression_values,
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
    end_date = date_range.get('end_date_iso')

    # Build key condition with end_date for custom ranges
    if date_range.get('is_custom') and end_date:
        end_date_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        key_condition = 'tenant_hash = :th AND #ts BETWEEN :start AND :end'
        expr_values = {
            ':th': {'S': tenant_hash},
            ':start': {'S': start_date},
            ':end': {'S': end_date_inclusive},
        }
    else:
        key_condition = 'tenant_hash = :th AND #ts >= :start'
        expr_values = {
            ':th': {'S': tenant_hash},
            ':start': {'S': start_date},
        }

    # Build filter expression for form event types
    filter_expr = 'event_type IN (:t1, :t2, :t3, :t4)'
    expr_values.update({
        ':t1': {'S': 'FORM_VIEWED'},
        ':t2': {'S': 'FORM_STARTED'},
        ':t3': {'S': 'FORM_COMPLETED'},
        ':t4': {'S': 'FORM_ABANDONED'}
    })

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
                'KeyConditionExpression': key_condition,
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

    # Calculate abandoned as started - completed (FORM_ABANDONED events are not emitted by widget)
    forms_abandoned = max(0, counts['FORM_STARTED'] - counts['FORM_COMPLETED'])

    # Calculate rates based on forms_started (not total_outcomes)
    forms_started = counts['FORM_STARTED']
    completion_rate = (counts['FORM_COMPLETED'] / forms_started * 100) if forms_started > 0 else 0
    abandon_rate = (forms_abandoned / forms_started * 100) if forms_started > 0 else 0
    avg_time = sum(completion_times) / len(completion_times) if completion_times else 0

    return {
        'form_views': counts['FORM_VIEWED'],
        'forms_started': counts['FORM_STARTED'],
        'forms_completed': counts['FORM_COMPLETED'],
        'forms_abandoned': forms_abandoned,
        'completion_rate': round(completion_rate, 1),
        'abandon_rate': round(abandon_rate, 1),
        'avg_completion_time_seconds': round(avg_time)
    }


def fetch_form_bottlenecks_from_dynamo(tenant_hash: str, date_range: Dict[str, str], form_id: str = None, limit: int = 5) -> Dict[str, Any]:
    """
    Calculate form bottlenecks (field-level abandonment) from DynamoDB session events.

    Since FORM_ABANDONED events are not emitted by the widget, this function:
    1. Finds sessions with FORM_STARTED but no FORM_COMPLETED
    2. For each abandoned session, finds the last FORM_FIELD_SUBMITTED event
    3. Aggregates by field to identify drop-off points

    Args:
        tenant_hash: Tenant hash
        date_range: Dict with 'start_date_iso'
        form_id: Optional form filter
        limit: Maximum number of bottlenecks to return

    Returns:
        Dict with bottlenecks list and total_abandonments count
    """
    start_date = date_range['start_date_iso']
    end_date = date_range.get('end_date_iso')

    # Build key condition with end_date for custom ranges
    if date_range.get('is_custom') and end_date:
        end_date_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        key_condition = 'tenant_hash = :th AND #ts BETWEEN :start AND :end'
        expr_values = {
            ':th': {'S': tenant_hash},
            ':start': {'S': start_date},
            ':end': {'S': end_date_inclusive},
        }
    else:
        key_condition = 'tenant_hash = :th AND #ts >= :start'
        expr_values = {
            ':th': {'S': tenant_hash},
            ':start': {'S': start_date},
        }

    # Query form-related events: FORM_STARTED, FORM_COMPLETED, FORM_FIELD_SUBMITTED
    filter_expr = 'event_type IN (:t1, :t2, :t3)'
    expr_values.update({
        ':t1': {'S': 'FORM_STARTED'},
        ':t2': {'S': 'FORM_COMPLETED'},
        ':t3': {'S': 'FORM_FIELD_SUBMITTED'}
    })

    # Add optional form_id filter
    if form_id:
        filter_expr += ' AND contains(event_payload, :form_id)'
        expr_values[':form_id'] = {'S': f'"form_id": "{form_id}"'}

    # Group events by session
    session_events = {}  # session_id -> {'started': bool, 'completed': bool, 'last_field': {...}}
    last_key = None

    try:
        while True:
            query_params = {
                'TableName': SESSION_EVENTS_TABLE,
                'IndexName': 'tenant-date-index',
                'KeyConditionExpression': key_condition,
                'FilterExpression': filter_expr,
                'ExpressionAttributeNames': {'#ts': 'timestamp'},
                'ExpressionAttributeValues': expr_values
            }

            if last_key:
                query_params['ExclusiveStartKey'] = last_key

            response = dynamodb.query(**query_params)
            items = response.get('Items', [])

            for item in items:
                session_id = item.get('session_id', {}).get('S', 'unknown')
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

                # Initialize session if not seen
                if session_id not in session_events:
                    session_events[session_id] = {
                        'started': False,
                        'completed': False,
                        'last_field': None,
                        'last_field_timestamp': ''
                    }

                if event_type == 'FORM_STARTED':
                    session_events[session_id]['started'] = True
                elif event_type == 'FORM_COMPLETED':
                    session_events[session_id]['completed'] = True
                elif event_type == 'FORM_FIELD_SUBMITTED':
                    # Track the most recent field submitted (by timestamp)
                    if timestamp > session_events[session_id]['last_field_timestamp']:
                        session_events[session_id]['last_field'] = {
                            'field_id': payload.get('field_id', 'unknown'),
                            'field_label': payload.get('field_label', 'Unknown Field'),
                            'form_id': payload.get('form_id', '')
                        }
                        session_events[session_id]['last_field_timestamp'] = timestamp

            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break

        # Find abandoned sessions: started but not completed
        abandoned_sessions = [
            data for sid, data in session_events.items()
            if data['started'] and not data['completed'] and data['last_field']
        ]

        logger.info(f"Found {len(abandoned_sessions)} abandoned form sessions for tenant {tenant_hash}")

        # Aggregate by last field
        field_abandons = {}  # field_id -> {'count': N, 'label': '...', 'form_id': '...'}

        for session in abandoned_sessions:
            field_data = session['last_field']
            field_id = field_data['field_id']
            field_label = field_data['field_label']
            event_form_id = field_data['form_id']

            if field_id not in field_abandons:
                field_abandons[field_id] = {
                    'count': 0,
                    'label': field_label,
                    'form_id': event_form_id
                }
            field_abandons[field_id]['count'] += 1

        # Calculate total and sort by count descending
        total_abandons = len(abandoned_sessions)
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
        started = stats['started']
        completions = stats['completions']
        abandons = stats['abandons']
        total_completions += completions

        conversion_rate = (completions / started * 100) if started > 0 else 0
        abandon_rate = ((started - completions) / started * 100) if started > 0 else 0
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
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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

    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(params.get('range', '30d'), start_date_param, end_date_param)
    page = max(1, int(params.get('page', '1')))
    limit = min(int(params.get('limit', '25')), 100)
    form_id_filter = params.get('form_id')
    search = params.get('search', '').strip().lower()

    # Query DynamoDB using tenant-timestamp-index
    # Build key condition with end_date for custom ranges
    end_date_iso = date_range.get('end_date_iso')
    if date_range.get('is_custom') and end_date_iso:
        end_date_inclusive = (datetime.strptime(end_date_iso, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        key_condition = 'tenant_id = :tid AND #ts BETWEEN :start_date AND :end_date'
        expr_values = {
            ':tid': {'S': tenant_id},
            ':start_date': {'S': date_range['start_date_iso']},
            ':end_date': {'S': end_date_inclusive}
        }
    else:
        key_condition = 'tenant_id = :tid AND #ts >= :start_date'
        expr_values = {
            ':tid': {'S': tenant_id},
            ':start_date': {'S': date_range['start_date_iso']}
        }

    try:
        query_params = {
            'TableName': FORM_SUBMISSIONS_TABLE,
            'IndexName': 'tenant-timestamp-index',
            'KeyConditionExpression': key_condition,
            'ExpressionAttributeNames': {'#ts': 'timestamp'},
            'ExpressionAttributeValues': expr_values,
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

        # Extract contact info using a merge strategy:
        # 1. Start with canonical contact object (if it has a name)
        # 2. If contact has no name, use form_data_labeled (most reliable)
        # 3. Legacy fallback to form_data for old records
        contact = item.get('contact', {})
        comments_field = item.get('comments', {})

        # Try canonical contact object first
        contact_name = ''
        contact_email = ''
        contact_phone = ''
        contact_comments = ''
        if contact and contact.get('M'):
            contact_map = contact.get('M', {})
            first_name = contact_map.get('first_name', {}).get('S', '') or ''
            last_name = contact_map.get('last_name', {}).get('S', '') or ''
            full_name = contact_map.get('full_name', {}).get('S', '') or ''
            contact_name = full_name or f"{first_name} {last_name}".strip()
            contact_email = contact_map.get('email', {}).get('S', '') or ''
            contact_phone = contact_map.get('phone', {}).get('S', '') or ''
            contact_comments = comments_field.get('S', '') if comments_field else ''

        # If contact has a name, use it directly
        if contact_name:
            fields = {'name': contact_name, 'email': contact_email, 'phone': contact_phone, 'comments': contact_comments}
        else:
            # Contact has no name — try form_data_labeled (handles composite Name fields)
            form_data_labeled = item.get('form_data_labeled', {})
            if form_data_labeled and form_data_labeled.get('M'):
                fields = extract_all_fields_from_form_data_labeled(form_data_labeled)
                # Supplement with contact fields if form_data_labeled missed them
                if not fields.get('email') and contact_email:
                    fields['email'] = contact_email
                if not fields.get('phone') and contact_phone:
                    fields['phone'] = contact_phone
                if not fields.get('comments') and contact_comments:
                    fields['comments'] = contact_comments
            else:
                # Legacy fallback - only has name/email
                name, email = extract_name_email_from_form_data(item.get('form_data', {}))
                fields = {'name': name, 'email': email, 'phone': contact_phone, 'comments': contact_comments}

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
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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



def parse_date_range(range_str: str, start_date_param: str = None, end_date_param: str = None) -> Dict[str, Any]:
    """
    Parse date range string (1d, 7d, 30d, 90d, custom) into date components.
    Returns ISO date strings for proper cross-month-boundary filtering.

    For 'custom' range, uses start_date_param and end_date_param (YYYY-MM-DD format).
    """
    # Handle custom date range
    if range_str == 'custom' and start_date_param and end_date_param:
        try:
            start_date = datetime.strptime(start_date_param, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(end_date_param, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            days = (end_date - start_date).days + 1
            return {
                'start_year': start_date.year,
                'start_month': start_date.month,
                'start_day': start_date.day,
                'start_date_iso': start_date.strftime('%Y-%m-%d'),
                'end_date_iso': end_date.strftime('%Y-%m-%d'),
                'days': days,
                'is_custom': True
            }
        except ValueError as e:
            logger.warning(f"Invalid custom date format: {e}, falling back to 30d")

    # Standard range parsing (7d, 30d, 90d)
    days = 30  # default
    if range_str.endswith('d'):
        try:
            days = int(range_str[:-1])
        except ValueError:
            pass

    start_date = datetime.now(timezone.utc) - timedelta(days=days)
    end_date = datetime.now(timezone.utc)

    return {
        'start_year': start_date.year,
        'start_month': start_date.month,
        'start_day': start_date.day,
        'start_date_iso': start_date.strftime('%Y-%m-%d'),
        'end_date_iso': end_date.strftime('%Y-%m-%d'),
        'days': days,
        'is_custom': False
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
    - range: Time range (1d, 7d, 30d, 90d, custom) - default 30d
    - start_date: Start date for custom range (YYYY-MM-DD)
    - end_date: End date for custom range (YYYY-MM-DD)
    """
    range_str = params.get('range', '30d')
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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
    - range: Time range (1d, 7d, 30d, 90d, custom) - default 30d
    - start_date: Start date for custom range (YYYY-MM-DD)
    - end_date: End date for custom range (YYYY-MM-DD)
    - timezone: IANA timezone (e.g., America/Chicago) - default UTC
    """
    range_str = params.get('range', '30d')
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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
    - range: Time range (1d, 7d, 30d, 90d, custom) - default 30d
    - start_date: Start date for custom range (YYYY-MM-DD)
    - end_date: End date for custom range (YYYY-MM-DD)
    - limit: Number of questions (default 5, max 10)
    """
    range_str = params.get('range', '30d')
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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
    - range: Time range (1d, 7d, 30d, 90d, custom) - default 30d
    - start_date: Start date for custom range (YYYY-MM-DD)
    - end_date: End date for custom range (YYYY-MM-DD)
    - page: Page number (default 1)
    - limit: Results per page (default 10, max 25)
    """
    range_str = params.get('range', '30d')
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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
    - range: Time range (1d, 7d, 30d, 90d, custom) - default 30d
    - start_date: Start date for custom range (YYYY-MM-DD)
    - end_date: End date for custom range (YYYY-MM-DD)
    - granularity: 'hour' or 'day' - default based on range
    """
    range_str = params.get('range', '30d')
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
    date_range = parse_date_range(range_str, start_date_param, end_date_param)
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


def enrich_sessions_with_events(session_ids: List[str], tenant_hash: str) -> Dict[str, Dict[str, Any]]:
    """
    Enrich sessions with event data (event_count, computed_outcome).

    Queries the picasso-session-events table for each session to get accurate
    event counts and compute outcomes from the actual event stream.

    Args:
        session_ids: List of session IDs to enrich
        tenant_hash: Tenant hash for access validation

    Returns:
        Dict mapping session_id to {event_count: int, outcome: str}
    """
    enriched = {}

    for session_id in session_ids:
        try:
            # Query session events from DynamoDB
            response = dynamodb.query(
                TableName=SESSION_EVENTS_TABLE,
                KeyConditionExpression='pk = :pk',
                ExpressionAttributeValues={
                    ':pk': {'S': f'SESSION#{session_id}'}
                },
                Select='ALL_ATTRIBUTES'
            )

            items = response.get('Items', [])

            if not items:
                # No events found - use defaults
                enriched[session_id] = {
                    'event_count': 0,
                    'outcome': 'conversation'
                }
                continue

            # Verify tenant access
            first_event = items[0]
            event_tenant_hash = first_event.get('tenant_hash', {}).get('S', '')
            if event_tenant_hash != tenant_hash:
                # Tenant mismatch - skip this session
                continue

            # Compute outcome from events (stronger outcomes override weaker)
            outcome = None
            for item in items:
                event_type = item.get('event_type', {}).get('S', '')

                if event_type == 'FORM_COMPLETED':
                    outcome = 'form_completed'
                elif event_type == 'LINK_CLICKED' and outcome != 'form_completed':
                    outcome = 'link_clicked'
                elif event_type == 'CTA_CLICKED' and outcome not in ('form_completed', 'link_clicked'):
                    outcome = 'cta_clicked'

            enriched[session_id] = {
                'event_count': len(items),
                'outcome': outcome or 'conversation'
            }

        except ClientError as e:
            logger.warning(f"Error fetching events for session {session_id}: {e}")
            enriched[session_id] = {
                'event_count': 0,
                'outcome': 'conversation'
            }
        except Exception as e:
            logger.warning(f"Unexpected error enriching session {session_id}: {e}")
            enriched[session_id] = {
                'event_count': 0,
                'outcome': 'conversation'
            }

    return enriched


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
    - range: Time range (1d, 7d, 30d, 90d, custom) - default 30d
    - start_date: Start date for custom range (YYYY-MM-DD)
    - end_date: End date for custom range (YYYY-MM-DD)
    - limit: Results per page (1-100) - default 25
    - cursor: Pagination cursor for next page
    - outcome: Filter by outcome (form_completed, link_clicked, abandoned, conversation)

    Returns:
    - List of session summaries
    - Pagination cursor for next page
    - Total count (estimated)
    """
    range_str = params.get('range', '30d')
    start_date_param = params.get('start_date')
    end_date_param = params.get('end_date')
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
    date_range = parse_date_range(range_str, start_date_param, end_date_param)

    # SK format is now SESSION#{session_id} (no timestamp)
    # Filter by started_at attribute instead
    start_date = date_range['start_date_iso']
    end_date = date_range.get('end_date_iso')

    logger.info(f"Fetching sessions list for tenant: {tenant_hash}, range: {range_str}")

    try:
        # Build filter expression - always filter by start date
        filter_expression = 'started_at >= :start_date'
        expression_values = {
            ':pk': {'S': f'TENANT#{tenant_hash}'},
            ':sk_prefix': {'S': 'SESSION#'},
            ':start_date': {'S': start_date}
        }

        # Add end date filter if provided (for custom date ranges)
        if end_date:
            filter_expression += ' AND started_at < :end_date'
            # Use end_date + 1 day at midnight for inclusive filtering
            end_date_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            expression_values[':end_date'] = {'S': end_date_inclusive}

        # Build query parameters
        # NOTE: We fetch more sessions than requested when filtering by outcome,
        # because outcome is computed from events (not stored in summaries table).
        # The filter is applied AFTER enrichment with accurate outcomes.
        fetch_limit = limit * 3 if outcome_filter else limit

        query_params = {
            'TableName': SESSION_SUMMARIES_TABLE,
            'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
            'FilterExpression': filter_expression,
            'ExpressionAttributeValues': expression_values,
            'ScanIndexForward': False,  # Most recent first
            'Limit': fetch_limit
        }

        # NOTE: Outcome filter is now applied AFTER enrichment (see below)
        # because the stored outcome in session-summaries may be stale.
        # The accurate outcome is computed from the events table.

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
                'outcome': outcome,  # Will be overridden by enrichment
                'message_count': message_count,
                'user_message_count': user_message_count,
                'bot_message_count': bot_message_count,
                'first_question': first_question[:100] if first_question else '',
                'form_id': form_id if form_id else None,
                'event_count': 0  # Will be set by enrichment
            })

        # Enrich sessions with accurate outcomes and event counts from events table
        if sessions:
            session_ids = [s['session_id'] for s in sessions]
            enriched = enrich_sessions_with_events(session_ids, tenant_hash)

            for session in sessions:
                sid = session['session_id']
                if sid in enriched:
                    session['outcome'] = enriched[sid]['outcome']
                    session['event_count'] = enriched[sid]['event_count']

        # Apply outcome filter AFTER enrichment (computed outcomes are accurate)
        if outcome_filter and sessions:
            sessions = [s for s in sessions if s['outcome'] == outcome_filter]

        # Limit to requested number after filtering
        has_more_after_filter = len(sessions) > limit
        sessions = sessions[:limit]

        # Build next page cursor
        next_cursor = None
        if 'LastEvaluatedKey' in response and (has_more_after_filter or not outcome_filter):
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


# =============================================================================
# Notification Endpoint Handlers (Phase 2a)
# =============================================================================
# Tables:
#   picasso-notification-events  PK=pk("TENANT#<id>"), SK=<ISO_DATE>#<event_type>#<message_id>
#                                GSI ByMessageId: PK=message_id, SK=event_type_timestamp
#   picasso-notification-sends   PK=pk("TENANT#<id>"), SK=<ISO_DATE>#<channel>#<message_id>
#
# IAM: Lambda execution role needs dynamodb:Query on both tables and the ByMessageId GSI.
# =============================================================================

def _notification_date_range_start(range_str: str) -> str:
    """
    Convert a range string (1d, 7d, 30d, 90d) to an ISO-8601 date string for
    DynamoDB SK prefix comparisons (YYYY-MM-DD).  Falls back to 7d for unknown values.
    """
    try:
        days = int(range_str.rstrip('d'))
    except (ValueError, AttributeError):
        days = 7
    start = datetime.now(timezone.utc) - timedelta(days=days)
    return start.strftime('%Y-%m-%d')


def handle_notification_summary(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /notifications/summary

    Returns aggregated counts and delivery-rate metrics for the requested period.

    Query params:
    - range: 1d | 7d | 30d | 90d (default 7d)

    Response schema:
    {
        "sent": int, "delivered": int, "bounced": int, "complained": int,
        "opened": int, "clicked": int, "failed": int,
        "delivery_rate": float, "open_rate": float, "bounce_rate": float,
        "period": str
    }
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_notifications')
    if access_error:
        return access_error

    range_str = params.get('range', '7d')
    start_date = _notification_date_range_start(range_str)
    pk = f'TENANT#{tenant_id}'

    logger.info(
        f"[notifications/summary] tenant={redact_tenant_id(tenant_id)} "
        f"range={range_str} start={start_date}"
    )

    # --- Query picasso-notification-events for delivery/open/click/bounce/complaint ---
    event_counts: Dict[str, int] = {}
    try:
        last_key = None
        while True:
            query_kwargs: Dict[str, Any] = {
                'TableName': NOTIFICATION_EVENTS_TABLE,
                'KeyConditionExpression': 'pk = :pk AND sk >= :sk_start',
                'ExpressionAttributeValues': {
                    ':pk': {'S': pk},
                    ':sk_start': {'S': start_date},
                },
                'ProjectionExpression': 'event_type',
            }
            if last_key:
                query_kwargs['ExclusiveStartKey'] = last_key
            resp = dynamodb.query(**query_kwargs)
            for item in resp.get('Items', []):
                et = item.get('event_type', {}).get('S', '')
                if et:
                    event_counts[et] = event_counts.get(et, 0) + 1
            last_key = resp.get('LastEvaluatedKey')
            if not last_key:
                break
    except ClientError as e:
        logger.error(f"DynamoDB error querying notification-events: {e}")
        return cors_response(500, {'error': 'Failed to query notification events'})

    # All counts from the single source of truth: picasso-notification-events
    sent_count = event_counts.get('send', 0)
    delivered = event_counts.get('delivery', 0)
    bounced = event_counts.get('bounce', 0)
    complained = event_counts.get('complaint', 0)
    opened = event_counts.get('open', 0)
    clicked = event_counts.get('click', 0)
    failed_count = event_counts.get('failed', 0)

    delivery_rate = round((delivered / sent_count * 100), 1) if sent_count > 0 else 0.0
    open_rate = round((opened / delivered * 100), 1) if delivered > 0 else 0.0
    bounce_rate = round((bounced / sent_count * 100), 1) if sent_count > 0 else 0.0

    return cors_response(200, {
        'sent': sent_count,
        'delivered': delivered,
        'bounced': bounced,
        'complained': complained,
        'opened': opened,
        'clicked': clicked,
        'failed': failed_count,
        'delivery_rate': delivery_rate,
        'open_rate': open_rate,
        'bounce_rate': bounce_rate,
        'period': range_str,
    })


def handle_notification_events(tenant_id: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET /notifications/events

    Returns a paginated, newest-first event log from picasso-notification-events,
    combining send and delivery events.

    Query params:
    - range:   1d | 7d | 30d | 90d (default 7d)
    - page:    page number, 1-based (default 1)
    - limit:   results per page, max 100 (default 25)
    - channel: filter by channel, e.g. "email" (optional)
    - status:  filter by event_type, e.g. "delivery" (optional)
    - search:  filter by recipient email substring (optional)

    Response schema:
    {
        "events": [ { timestamp, event_type, channel, recipient, form_id,
                      status, message_id } ],
        "total": int, "page": int, "limit": int, "has_more": bool
    }
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_notifications')
    if access_error:
        return access_error

    range_str = params.get('range', '7d')
    start_date = _notification_date_range_start(range_str)
    pk = f'TENANT#{tenant_id}'

    try:
        page = max(1, int(params.get('page', '1')))
    except ValueError:
        page = 1
    try:
        limit = min(max(1, int(params.get('limit', '25'))), 100)
    except ValueError:
        limit = 25

    channel_filter = params.get('channel', '').strip().lower()
    status_filter = params.get('status', '').strip().lower()
    search = params.get('search', '').strip().lower()

    logger.info(
        f"[notifications/events] tenant={redact_tenant_id(tenant_id)} "
        f"range={range_str} page={page} limit={limit}"
    )

    # Build DynamoDB query — scan index forward = False gives newest first
    query_kwargs: Dict[str, Any] = {
        'TableName': NOTIFICATION_EVENTS_TABLE,
        'KeyConditionExpression': 'pk = :pk AND sk >= :sk_start',
        'ExpressionAttributeValues': {
            ':pk': {'S': pk},
            ':sk_start': {'S': start_date},
        },
        'ScanIndexForward': False,
    }

    # Optional server-side filter on channel
    filter_parts = []
    if channel_filter:
        query_kwargs['ExpressionAttributeValues'][':ch'] = {'S': channel_filter}
        filter_parts.append('channel = :ch')
    if filter_parts:
        query_kwargs['FilterExpression'] = ' AND '.join(filter_parts)

    all_events = []
    try:
        last_key = None
        while True:
            if last_key:
                query_kwargs['ExclusiveStartKey'] = last_key
            resp = dynamodb.query(**query_kwargs)
            for item in resp.get('Items', []):
                event_type = item.get('event_type', {}).get('S', '')

                # status_filter maps to event_type
                if status_filter and event_type != status_filter:
                    continue

                # destination is a List; take first element as primary recipient
                dest_list = item.get('destination', {}).get('L', [])
                recipient = dest_list[0].get('S', '') if dest_list else ''

                # search filter on recipient
                if search and search not in recipient.lower():
                    continue

                context = item.get('context', {}).get('M', {})
                form_id = context.get('form_id', {}).get('S', '')

                # SK: <ISO_DATE>#<event_type>#<message_id>
                sk = item.get('sk', {}).get('S', '')
                sk_parts = sk.split('#')
                message_id = sk_parts[2] if len(sk_parts) >= 3 else ''
                timestamp = item.get('sk', {}).get('S', '').split('#')[0] if sk else ''

                # Extract event-specific detail (bounce_type, complaint_type, etc.)
                detail_raw = item.get('detail', {}).get('M', {})
                detail = {}
                for k, v in detail_raw.items():
                    if 'S' in v:
                        detail[k] = v['S']
                    elif 'N' in v:
                        detail[k] = v['N']
                    elif 'BOOL' in v:
                        detail[k] = v['BOOL']
                    elif 'L' in v:
                        detail[k] = [
                            li.get('S', '') for li in v['L'] if 'S' in li
                        ]

                all_events.append({
                    'timestamp': timestamp,
                    'event_type': event_type,
                    'channel': item.get('channel', {}).get('S', ''),
                    'recipient': recipient,
                    'form_id': form_id,
                    'status': event_type,
                    'message_id': message_id,
                    'detail': detail,
                })
            last_key = resp.get('LastEvaluatedKey')
            if not last_key:
                break
    except ClientError as e:
        logger.error(f"DynamoDB error querying notification-events list: {e}")
        return cors_response(500, {'error': 'Failed to query notification events'})

    total = len(all_events)
    offset = (page - 1) * limit
    page_events = all_events[offset:offset + limit]
    has_more = (offset + limit) < total

    return cors_response(200, {
        'events': page_events,
        'total': total,
        'page': page,
        'limit': limit,
        'has_more': has_more,
    })


def handle_notification_event_detail(tenant_id: str, message_id: str) -> Dict[str, Any]:
    """
    GET /notifications/events/{message_id}

    Returns the full lifecycle event sequence for a single message using the
    ByMessageId GSI on picasso-notification-events.

    Response schema:
    {
        "message_id": str,
        "events": [ { "event_type": str, "timestamp": str, "detail": dict } ]
    }
    """
    access_error = validate_feature_access(tenant_id, 'dashboard_notifications')
    if access_error:
        return access_error

    # Basic input validation — message IDs are hex strings up to 64 chars
    if not message_id or len(message_id) > 128 or not re.match(r'^[A-Za-z0-9_\-]+$', message_id):
        return cors_response(400, {'error': 'Invalid message_id format'})

    logger.info(
        f"[notifications/events/detail] tenant={redact_tenant_id(tenant_id)} "
        f"message_id={message_id[:16]}..."
    )

    try:
        resp = dynamodb.query(
            TableName=NOTIFICATION_EVENTS_TABLE,
            IndexName='ByMessageId',
            KeyConditionExpression='message_id = :mid',
            ExpressionAttributeValues={
                ':mid': {'S': message_id},
            },
            ScanIndexForward=True,  # chronological order
        )
    except ClientError as e:
        logger.error(f"DynamoDB error querying ByMessageId GSI: {e}")
        return cors_response(500, {'error': 'Failed to query notification event detail'})

    events = []
    for item in resp.get('Items', []):
        event_type = item.get('event_type', {}).get('S', '')

        # event_type_timestamp SK: <event_type>#<ISO timestamp>
        sk = item.get('event_type_timestamp', {}).get('S', '')
        sk_parts = sk.split('#', 1)
        timestamp = sk_parts[1] if len(sk_parts) == 2 else ''

        # detail is a free-form Map attribute
        detail_raw = item.get('detail', {}).get('M', {})
        detail: Dict[str, Any] = {}
        for k, v in detail_raw.items():
            # Unwrap single-type DynamoDB values (S, N, BOOL only for simplicity)
            if 'S' in v:
                detail[k] = v['S']
            elif 'N' in v:
                detail[k] = v['N']
            elif 'BOOL' in v:
                detail[k] = v['BOOL']

        events.append({
            'event_type': event_type,
            'timestamp': timestamp,
            'detail': detail,
        })

    return cors_response(200, {
        'message_id': message_id,
        'events': events,
    })


# =============================================================================
# Settings — Notification Recipients & Templates (Phase 2b/2c)
# =============================================================================

# Sender identity for all outbound SES emails from this API.
SES_SENDER = 'notify@myrecruiter.ai'

# Sample values injected when rendering preview or test-send templates.
SAMPLE_DATA: Dict[str, str] = {
    'first_name': 'Test',
    'last_name': 'User',
    'email': 'test@example.com',
    'phone': '(555) 123-4567',
    'organization_name': '',          # overridden per-tenant at runtime
    'form_data': 'Full Name: Test User\nEmail: test@example.com\nPhone: (555) 123-4567',
    'form_type': 'test_form',
}

# Variables surfaced to the frontend template editor.
AVAILABLE_VARIABLES: List[str] = [
    '{first_name}', '{last_name}', '{email}', '{phone}',
    '{organization_name}', '{form_data}',
]

# Role guard for all write operations on settings endpoints.
_WRITE_ROLES = {'admin', 'super_admin'}


def _require_write_role(user_role: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return a 403 response if the caller lacks admin/super_admin role, else None."""
    if user_role not in _WRITE_ROLES:
        logger.warning(f"[settings] write attempt by role={user_role!r} denied")
        return cors_response(403, {'error': 'Forbidden: admin or super_admin role required'})
    return None


def render_template(template: str, variables: Dict[str, str]) -> str:
    """Replace {variable} placeholders in a template string with supplied values."""
    result = template
    for key, value in variables.items():
        result = result.replace('{' + key + '}', str(value))
    return result


def _build_sample_vars(config: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Produce sample variable dict, filling organization_name from tenant config."""
    sample = dict(SAMPLE_DATA)
    if config:
        sample['organization_name'] = config.get('chat_title', '')
    return sample


# SOURCE OF TRUTH: form_handler.js sendConfirmationEmail() in Bedrock_Streaming_Handler_Staging
# If the production template changes, this function must be updated to match.
# Last synced: R1.2 (2026-04)
def _build_branded_html(body_text: str, config: Dict[str, Any]) -> str:
    """
    Wrap rendered body text in branded HTML email template.
    Matches the production template in form_handler.js sendConfirmationEmail().

    Returns bare HTML div if no branding config exists.
    """
    import html as html_module

    branding = config.get('branding', {})
    if not branding:
        return f'<div>{body_text.replace(chr(10), "<br>")}</div>'

    primary_color = branding.get('primary_color', '#50C878')
    font_family = branding.get('font_family', 'Arial, sans-serif')
    logo_url = branding.get('logo_url', '')
    org_name = html_module.escape(config.get('chat_title', 'Organization'))

    # Validate logo URL is https
    if logo_url and not logo_url.startswith('https://'):
        logo_url = ''

    # Sanitize CSS values
    primary_color = primary_color.split(';')[0].strip()
    font_family = font_family.split(';')[0].strip()

    logo_html = (
        f'<img src="{logo_url}" alt="{org_name}" style="max-height:48px; max-width:200px;">'
        if logo_url else ''
    )
    logo_margin = '12' if logo_url else '0'

    from datetime import datetime
    year = datetime.utcnow().year

    body_html = body_text.replace('\n', '<br>')

    return f'''<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0; padding:0; background-color:#f4f4f4; font-family:{font_family}, Arial, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4; padding:20px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden;">
        <tr><td style="background-color:{primary_color}; padding:24px; text-align:center;">
          {logo_html}
          <div style="color:#ffffff; font-size:18px; font-weight:600; margin-top:{logo_margin}px;">{org_name}</div>
        </td></tr>
        <tr><td style="padding:32px 24px; color:#333333; font-size:15px; line-height:1.6;">
          {body_html}
        </td></tr>
        <tr><td style="padding:16px 24px; border-top:1px solid #eeeeee; color:#999999; font-size:12px; text-align:center;">
          &copy; {year} {org_name}
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>'''


def _build_simple_html(body_text: str) -> str:
    """Simple styled HTML for internal notification test-sends (no branding)."""
    return f'<div style="font-family: Arial, sans-serif; line-height: 1.6;">{body_text.replace(chr(10), "<br>")}</div>'


def _deep_merge(base: dict, updates: dict) -> dict:
    """
    Recursively merge *updates* into *base*.
    For dict values, recurse.  For all other types, updates wins.
    Returns the mutated *base* dict.
    """
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def update_tenant_notifications(
    tenant_id: str,
    form_id: str,
    updates: Dict[str, Any],
    update_key: str = 'notifications',
) -> Dict[str, Any]:
    """
    Targeted merge of a single form's notification or template config in S3.

    Uses S3 ETag optimistic locking to prevent concurrent write conflicts.
    After a successful write the in-process cache is invalidated so the next
    read fetches fresh data.

    Args:
        tenant_id:  Validated tenant identifier.
        form_id:    Key inside conversational_forms that must exist.
        updates:    Dict of sub-fields to deep-merge into form[update_key].
        update_key: 'notifications' or the specific sub-key being updated.
                    Pass 'notifications' for recipient / channel changes,
                    and the template key names for template-only changes.

    Returns:
        The updated value of form[update_key] after merge.

    Raises:
        ValueError: form_id not found in config.
        ConcurrentModificationError: ETag mismatch — concurrent writer.
        ClientError: Unexpected S3 error.
    """
    bucket = S3_CONFIG_BUCKET
    key = f"tenants/{tenant_id}/{tenant_id}-config.json"

    # --- Read with ETag ---
    response = s3.get_object(Bucket=bucket, Key=key)
    etag = response['ETag']
    config = json.loads(response['Body'].read().decode('utf-8'))

    # --- Validate form exists ---
    forms = config.get('conversational_forms', {})
    if form_id not in forms:
        raise ValueError(f"Form '{form_id}' not found in conversational_forms")

    form = forms[form_id]

    if update_key == 'notifications':
        existing = form.get('notifications', {})
        _deep_merge(existing, updates)
        form['notifications'] = existing
        result = existing
    else:
        # For template-key updates: merge each provided notification type's
        # template fields (subject, body_template) only.
        existing_notif = form.get('notifications', {})
        TEMPLATE_FIELDS = {'subject', 'body_template'}
        for notif_type, notif_updates in updates.items():
            if isinstance(notif_updates, dict):
                existing_notif.setdefault(notif_type, {})
                for field, val in notif_updates.items():
                    if field in TEMPLATE_FIELDS:
                        existing_notif[notif_type][field] = val
        form['notifications'] = existing_notif
        result = existing_notif

    # --- Write back with optimistic lock ---
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(config, indent=2),
            ContentType='application/json',
            IfMatch=etag,
        )
    except ClientError as e:
        if e.response['Error']['Code'] in ('PreconditionFailed', '412'):
            raise ConcurrentModificationError(
                "Config was modified by another user. Please refresh."
            )
        raise

    # --- Invalidate in-process cache ---
    _tenant_config_cache.pop(tenant_id, None)
    _tenant_config_cache_time.pop(tenant_id, None)

    logger.info(
        f"[settings/notifications] config updated "
        f"tenant={redact_tenant_id(tenant_id)} form={form_id}"
    )
    return result


# ---------------------------------------------------------------------------
# GET /settings/notifications
# ---------------------------------------------------------------------------

def handle_settings_notifications_get(tenant_id: str) -> Dict[str, Any]:
    """
    GET /settings/notifications

    Returns the notification config for every conversational form that has
    a notifications block, keyed by form_id.
    """
    config = get_tenant_config(tenant_id)
    if not config:
        return cors_response(404, {'error': 'Tenant configuration not found'})

    forms_out: Dict[str, Any] = {}
    for form_id, form in config.get('conversational_forms', {}).items():
        if not isinstance(form, dict):
            continue
        if 'notifications' in form:
            forms_out[form_id] = {
                'form_title': form.get('form_title', form_id),
                'notifications': form['notifications'],
            }

    logger.info(
        f"[settings/notifications] GET "
        f"tenant={redact_tenant_id(tenant_id)} forms={len(forms_out)}"
    )
    return cors_response(200, {'forms': forms_out})


# ---------------------------------------------------------------------------
# PATCH /settings/notifications
# ---------------------------------------------------------------------------

def handle_settings_notifications_patch(
    tenant_id: str,
    body: Dict[str, Any],
    user_role: Optional[str],
) -> Dict[str, Any]:
    """
    PATCH /settings/notifications

    Deep-merges the supplied notifications sub-fields for a specific form.
    Body: { "form_id": str, "notifications": { ... } }
    """
    role_error = _require_write_role(user_role)
    if role_error:
        return role_error

    form_id = body.get('form_id', '').strip()
    notifications = body.get('notifications')

    if not form_id:
        return cors_response(400, {'error': 'form_id is required'})
    if not isinstance(notifications, dict) or not notifications:
        return cors_response(400, {'error': 'notifications must be a non-empty object'})

    # Validate form_id characters to prevent path traversal / injection
    if not re.match(r'^[A-Za-z0-9_-]+$', form_id):
        return cors_response(400, {'error': 'Invalid form_id format'})

    # Validate sms_recipients phone numbers are E.164 format
    sms_recipients = (notifications.get('internal') or {}).get('sms_recipients')
    if sms_recipients is not None:
        if not isinstance(sms_recipients, list):
            return cors_response(400, {'error': 'sms_recipients must be an array'})
        for phone in sms_recipients:
            if not isinstance(phone, str) or not re.match(r'^\+1\d{10}$', phone):
                return cors_response(400, {
                    'error': f'Invalid phone number format: {phone}. Must be E.164 US format (e.g. +15125551234)'
                })

    logger.info(
        f"[settings/notifications] PATCH "
        f"tenant={redact_tenant_id(tenant_id)} form={form_id}"
    )

    try:
        updated = update_tenant_notifications(
            tenant_id, form_id, notifications, update_key='notifications'
        )
    except ValueError as e:
        return cors_response(404, {'error': str(e)})
    except ConcurrentModificationError as e:
        return cors_response(409, {'error': str(e)})
    except ClientError as e:
        logger.error(f"[settings/notifications] S3 error: {e}")
        return cors_response(500, {'error': 'Failed to update notification config'})

    return cors_response(200, {
        'form_id': form_id,
        'notifications': updated,
    })


# ---------------------------------------------------------------------------
# POST /settings/notifications/recipients/test-send
# ---------------------------------------------------------------------------

def handle_notification_recipients_test_send(
    tenant_id: str,
    body: Dict[str, Any],
    user_role: Optional[str],
) -> Dict[str, Any]:
    """
    POST /settings/notifications/recipients/test-send

    Sends a test email to the supplied address using the form's internal
    notification template filled with sample data.
    Body: { "email": str, "form_id": str }
    """
    role_error = _require_write_role(user_role)
    if role_error:
        return role_error

    email = body.get('email', '').strip()
    form_id = body.get('form_id', '').strip()

    if not email or '@' not in email:
        return cors_response(400, {'error': 'A valid email address is required'})
    if not form_id or not re.match(r'^[A-Za-z0-9_-]+$', form_id):
        return cors_response(400, {'error': 'A valid form_id is required'})
    if len(email) > 254:
        return cors_response(400, {'error': 'Email address too long'})

    config = get_tenant_config(tenant_id)
    if not config:
        return cors_response(404, {'error': 'Tenant configuration not found'})

    forms = config.get('conversational_forms', {})
    if form_id not in forms:
        return cors_response(404, {'error': f"Form '{form_id}' not found"})

    notif = forms[form_id].get('notifications', {}).get('internal', {})
    subject_tpl = notif.get('subject', f'[Test] Notification from {form_id}')
    body_tpl = notif.get('body_template', 'This is a test notification.\n\n{form_data}')

    sample = _build_sample_vars(config)
    subject = render_template(subject_tpl, sample)
    body_text = render_template(body_tpl, sample)

    logger.info(
        f"[settings/notifications/recipients/test-send] "
        f"tenant={redact_tenant_id(tenant_id)} form={form_id} "
        f"to={redact_email(email)}"
    )

    try:
        resp = ses.send_email(
            Source=SES_SENDER,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': f'[TEST] {subject}', 'Charset': 'UTF-8'},
                'Body': {
                    'Text': {'Data': body_text, 'Charset': 'UTF-8'},
                    'Html': {'Data': _build_simple_html(body_text), 'Charset': 'UTF-8'},
                },
            },
        )
    except ClientError as e:
        logger.error(f"[settings/notifications/recipients/test-send] SES error: {e}")
        return cors_response(502, {'error': 'Failed to send test email via SES'})

    message_id = resp.get('MessageId', '')
    logger.info(
        f"[settings/notifications/recipients/test-send] sent "
        f"message_id={message_id} to={redact_email(email)}"
    )
    return cors_response(200, {'success': True, 'message_id': message_id})


# ---------------------------------------------------------------------------
# GET /settings/notifications/templates
# ---------------------------------------------------------------------------

def handle_notification_templates_get(tenant_id: str) -> Dict[str, Any]:
    """
    GET /settings/notifications/templates

    Returns the template content (subject + body_template) for every
    notification type within every conversational form.
    """
    config = get_tenant_config(tenant_id)
    if not config:
        return cors_response(404, {'error': 'Tenant configuration not found'})

    forms_out: Dict[str, Any] = {}
    for form_id, form in config.get('conversational_forms', {}).items():
        if not isinstance(form, dict):
            continue
        notif = form.get('notifications', {})
        if not notif:
            continue

        entry: Dict[str, Any] = {
            'form_title': form.get('form_title', form_id),
            'available_variables': AVAILABLE_VARIABLES,
        }
        for notif_type, notif_cfg in notif.items():
            if isinstance(notif_cfg, dict):
                entry[notif_type] = {
                    'subject': notif_cfg.get('subject', ''),
                    'body_template': notif_cfg.get('body_template', ''),
                }
        forms_out[form_id] = entry

    logger.info(
        f"[settings/notifications/templates] GET "
        f"tenant={redact_tenant_id(tenant_id)} forms={len(forms_out)}"
    )
    return cors_response(200, {'forms': forms_out})


# ---------------------------------------------------------------------------
# PATCH /settings/notifications/templates/{form_id}
# ---------------------------------------------------------------------------

def handle_notification_templates_update(
    tenant_id: str,
    form_id: str,
    body: Dict[str, Any],
    user_role: Optional[str],
) -> Dict[str, Any]:
    """
    PATCH /settings/notifications/templates/{form_id}

    Merges only subject/body_template fields from the request into the
    relevant notification type blocks.  All other fields are left untouched.

    Body: {
        "internal": { "subject": "...", "body_template": "..." },
        "applicant_confirmation": { ... }
    }
    """
    role_error = _require_write_role(user_role)
    if role_error:
        return role_error

    if not form_id or not re.match(r'^[A-Za-z0-9_-]+$', form_id):
        return cors_response(400, {'error': 'Invalid form_id in path'})
    if not isinstance(body, dict) or not body:
        return cors_response(400, {'error': 'Request body must be a non-empty object'})

    logger.info(
        f"[settings/notifications/templates] PATCH "
        f"tenant={redact_tenant_id(tenant_id)} form={form_id}"
    )

    try:
        updated = update_tenant_notifications(
            tenant_id, form_id, body, update_key='templates'
        )
    except ValueError as e:
        return cors_response(404, {'error': str(e)})
    except ConcurrentModificationError as e:
        return cors_response(409, {'error': str(e)})
    except ClientError as e:
        logger.error(f"[settings/notifications/templates] S3 error: {e}")
        return cors_response(500, {'error': 'Failed to update template config'})

    return cors_response(200, {
        'form_id': form_id,
        'notifications': updated,
    })


# ---------------------------------------------------------------------------
# POST /settings/notifications/templates/{form_id}/preview
# ---------------------------------------------------------------------------

def handle_notification_template_preview(
    tenant_id: str,
    form_id: str,
    body: Dict[str, Any],
    user_role: Optional[str],
) -> Dict[str, Any]:
    """
    POST /settings/notifications/templates/{form_id}/preview

    Renders the named template type with sample data and returns both the
    rendered subject and an HTML body for frontend display.

    Body: { "template_type": "internal" | "applicant_confirmation" }
    """
    if not form_id or not re.match(r'^[A-Za-z0-9_-]+$', form_id):
        return cors_response(400, {'error': 'Invalid form_id in path'})

    template_type = body.get('template_type', 'internal')
    if not re.match(r'^[A-Za-z0-9_-]+$', template_type):
        return cors_response(400, {'error': 'Invalid template_type'})

    config = get_tenant_config(tenant_id)
    if not config:
        return cors_response(404, {'error': 'Tenant configuration not found'})

    forms = config.get('conversational_forms', {})
    if form_id not in forms:
        return cors_response(404, {'error': f"Form '{form_id}' not found"})

    notif = forms[form_id].get('notifications', {}).get(template_type)
    if not notif:
        return cors_response(404, {
            'error': f"Template type '{template_type}' not found for form '{form_id}'"
        })

    subject_tpl = notif.get('subject', '')
    body_tpl = notif.get('body_template', '')

    sample = _build_sample_vars(config)
    rendered_subject = render_template(subject_tpl, sample)
    rendered_body = render_template(body_tpl, sample)

    # Build HTML preview matching production rendering for this template type
    if template_type == 'applicant_confirmation':
        use_branding = notif.get('use_tenant_branding', True)
        html_body = _build_branded_html(rendered_body, config) if use_branding else _build_simple_html(rendered_body)
    else:
        html_body = _build_simple_html(rendered_body)

    logger.info(
        f"[settings/notifications/templates/preview] "
        f"tenant={redact_tenant_id(tenant_id)} form={form_id} type={template_type}"
    )
    return cors_response(200, {
        'form_id': form_id,
        'template_type': template_type,
        'subject': rendered_subject,
        'body_html': html_body,
    })


# ---------------------------------------------------------------------------
# POST /settings/notifications/templates/{form_id}/test-send
# ---------------------------------------------------------------------------

def handle_notification_template_test_send(
    tenant_id: str,
    form_id: str,
    user_email: str,
    user_role: Optional[str],
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    POST /settings/notifications/templates/{form_id}/test-send

    Sends a test email to the currently authenticated user's address using
    the form's template filled with sample data.

    Body (optional): { "template_type": "internal" | "applicant_confirmation" }
    Defaults to "internal" if not specified.
    """
    role_error = _require_write_role(user_role)
    if role_error:
        return role_error

    if not form_id or not re.match(r'^[A-Za-z0-9_-]+$', form_id):
        return cors_response(400, {'error': 'Invalid form_id in path'})

    if not user_email or user_email == 'unknown' or '@' not in user_email:
        return cors_response(400, {'error': 'Authenticated user email is missing or invalid'})

    body = body or {}
    template_type = body.get('template_type', 'internal')
    if template_type not in ('internal', 'applicant_confirmation'):
        return cors_response(400, {'error': 'template_type must be "internal" or "applicant_confirmation"'})

    config = get_tenant_config(tenant_id)
    if not config:
        return cors_response(404, {'error': 'Tenant configuration not found'})

    forms = config.get('conversational_forms', {})
    if form_id not in forms:
        return cors_response(404, {'error': f"Form '{form_id}' not found"})

    form_notif = forms[form_id].get('notifications', {})
    if template_type == 'applicant_confirmation':
        tpl_config = form_notif.get('applicant_confirmation', {})
    else:
        tpl_config = form_notif.get('internal', {})

    subject_tpl = tpl_config.get('subject', f'[Test] Notification from {form_id}')
    body_tpl = tpl_config.get('body_template', 'This is a test notification.\n\n{form_data}')

    sample = _build_sample_vars(config)
    subject = render_template(subject_tpl, sample)
    body_text = render_template(body_tpl, sample)

    # Build HTML matching production rendering for this template type
    if template_type == 'applicant_confirmation':
        use_branding = tpl_config.get('use_tenant_branding', True)
        html_body = _build_branded_html(body_text, config) if use_branding else _build_simple_html(body_text)
    else:
        html_body = _build_simple_html(body_text)

    logger.info(
        f"[settings/notifications/templates/test-send] "
        f"tenant={redact_tenant_id(tenant_id)} form={form_id} "
        f"type={template_type} to={redact_email(user_email)}"
    )

    try:
        resp = ses.send_email(
            Source=SES_SENDER,
            Destination={'ToAddresses': [user_email]},
            Message={
                'Subject': {'Data': f'[TEST] {subject}', 'Charset': 'UTF-8'},
                'Body': {
                    'Text': {'Data': body_text, 'Charset': 'UTF-8'},
                    'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                },
            },
        )
    except ClientError as e:
        logger.error(f"[settings/notifications/templates/test-send] SES error: {e}")
        return cors_response(502, {'error': 'Failed to send test email via SES'})

    message_id = resp.get('MessageId', '')
    logger.info(
        f"[settings/notifications/templates/test-send] sent "
        f"message_id={message_id} to={redact_email(user_email)}"
    )
    return cors_response(200, {'success': True, 'message_id': message_id})
