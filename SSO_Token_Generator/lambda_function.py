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
from functools import lru_cache

# Cache signing key for 5 minutes (Lambda container reuse)
secrets_client = None

def get_secrets_client():
    global secrets_client
    if secrets_client is None:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
    return secrets_client

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
        "name": "John Doe" (optional)
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
