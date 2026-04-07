"""
SSO Token Generator Lambda
Generates JWT tokens for Bubble SSO authentication.

Bubble calls this Lambda after successful user authentication,
receives a signed JWT, and redirects user to the React app.
"""

import json
import hmac
import hashlib
import base64
import time
import boto3
import logging
from functools import lru_cache

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cache signing key for 5 minutes (Lambda container reuse)
secrets_client = None
s3_client = None

# S3 bucket for tenant configurations
S3_CONFIG_BUCKET = 'picasso-configs'

# Cache for tenant configs (TTL: 5 minutes)
_tenant_config_cache = {}
_tenant_config_cache_time = {}
TENANT_CONFIG_CACHE_TTL = 300  # seconds


def get_secrets_client():
    global secrets_client
    if secrets_client is None:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
    return secrets_client


def get_s3_client():
    global s3_client
    if s3_client is None:
        s3_client = boto3.client('s3', region_name='us-east-1')
    return s3_client


def get_tenant_config(tenant_id: str) -> dict:
    """
    Fetch tenant configuration from S3 with per-container caching.
    Returns empty dict if config not found or on error.
    """
    now = time.time()
    if tenant_id in _tenant_config_cache:
        if (now - _tenant_config_cache_time.get(tenant_id, 0)) < TENANT_CONFIG_CACHE_TTL:
            return _tenant_config_cache[tenant_id]

    try:
        s3 = get_s3_client()
        key = f"tenants/{tenant_id}/config.json"
        response = s3.get_object(Bucket=S3_CONFIG_BUCKET, Key=key)
        config = json.loads(response['Body'].read().decode('utf-8'))
        _tenant_config_cache[tenant_id] = config
        _tenant_config_cache_time[tenant_id] = now
        logger.info(f"Loaded tenant config for {tenant_id[:8]}...")
        return config
    except Exception as e:
        logger.warning(f"Could not load tenant config for {tenant_id[:8]}...: {e}")
        return {}

@lru_cache(maxsize=1)
def get_signing_key():
    """Fetch signing key from Secrets Manager (cached)"""
    client = get_secrets_client()
    secret = client.get_secret_value(SecretId='picasso/staging/jwt/signing-key')
    secret_data = json.loads(secret['SecretString'])
    return secret_data['signingKey']

def base64url_encode(data: bytes) -> str:
    """Base64 URL-safe encoding without padding"""
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('utf-8')

def generate_jwt(payload: dict, signing_key: str) -> str:
    """Generate HS256 signed JWT token"""
    header = {"alg": "HS256", "typ": "JWT"}

    header_b64 = base64url_encode(json.dumps(header, separators=(',', ':')).encode())
    payload_b64 = base64url_encode(json.dumps(payload, separators=(',', ':')).encode())

    message = f"{header_b64}.{payload_b64}"
    signature = hmac.new(
        signing_key.encode(),
        message.encode(),
        hashlib.sha256
    ).digest()
    signature_b64 = base64url_encode(signature)

    return f"{message}.{signature_b64}"

def lambda_handler(event, context):
    """
    Generate JWT token for authenticated user.

    Expected POST body:
    {
        "tenant_id": "MYR384719",
        "tenant_hash": "my87674d777bf9",
        "email": "user@example.com",
        "name": "John Doe" (optional),
        "role": "super_admin" (optional),
        "company": "MyRecruiter" (optional),
        "tenants": [...] (optional, for super_admin tenant switching),
        "dashboard_conversations": true (optional),
        "dashboard_forms": true (optional),
        "dashboard_attribution": false (optional),
        "dashboard_notifications": true (optional, auto-derived from tenant config if omitted),
        "dashboard_settings": false (optional, Phase 3 placeholder)
    }

    Returns:
    {
        "token": "eyJhbG..."
    }
    """

    # CORS headers
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': 'https://login.myrecruiter.ai',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
    }

    # Handle preflight
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': ''
        }

    try:
        # Parse request body
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)

        # Validate required fields
        required_fields = ['tenant_id', 'tenant_hash', 'email']
        missing = [f for f in required_fields if not body.get(f)]
        if missing:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({
                    'error': f'Missing required fields: {", ".join(missing)}'
                })
            }

        # Build JWT payload
        now = int(time.time())
        payload = {
            'tenant_id': body['tenant_id'],
            'tenant_hash': body['tenant_hash'],
            'email': body['email'],
            'iat': now,
            'exp': now + (8 * 60 * 60)  # 8 hours
        }

        # Add optional fields
        if body.get('name'):
            payload['name'] = body['name']

        if body.get('role'):
            payload['role'] = body['role']

        if body.get('company'):
            payload['company'] = body['company']

        # Add tenant list for super_admin users (for tenant switching dropdown)
        if body.get('tenants') and isinstance(body['tenants'], list):
            payload['tenants'] = body['tenants']

        # Add dashboard features if any are provided
        features = {}
        if 'dashboard_conversations' in body:
            features['dashboard_conversations'] = body['dashboard_conversations']
        if 'dashboard_forms' in body:
            features['dashboard_forms'] = body['dashboard_forms']
        if 'dashboard_attribution' in body:
            features['dashboard_attribution'] = body['dashboard_attribution']

        # dashboard_notifications: read from tenant config features flag (set in config builder).
        # Caller may pass explicit override.
        if 'dashboard_notifications' in body:
            features['dashboard_notifications'] = bool(body['dashboard_notifications'])
        else:
            config = get_tenant_config(body['tenant_id'])
            features['dashboard_notifications'] = bool(
                config.get('features', {}).get('dashboard_notifications', False)
            )

        # dashboard_settings: Phase 3 placeholder — always False for now
        features['dashboard_settings'] = bool(body.get('dashboard_settings', False))

        if features:
            payload['features'] = features

        # Generate token
        signing_key = get_signing_key()
        token = generate_jwt(payload, signing_key)

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'token': token
            })
        }

    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({'error': 'Invalid JSON in request body'})
        }
    except Exception as e:
        print(f"Error generating token: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': 'Internal server error'})
        }
