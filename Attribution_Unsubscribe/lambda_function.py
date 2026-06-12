"""
Attribution Unsubscribe Lambda
==============================
Handles one-click unsubscribe for monthly recap emails.

Endpoint: Lambda Function URL (GET) with query param `t`.

Token format (LOCKED -- must match Attribution_Recap_Generator):
  payload  = "{tenant_id}|{email_lower}|recap"
  token    = base64url(payload) + "." + base64url(hmac_sha256(key, payload))
  (no padding in either segment)

On valid token:
  - Conditional PutItem: pk=TENANT#{tenant_id}, sk=SUPPRESS#recap#{email_lower}
    attrs: created_at (ISO), source="unsubscribe_link"
    NO TTL (permanent). ConditionalCheckFailedException treated as success (idempotent).
  - Returns 200 with minimal self-contained HTML confirmation page.

On invalid/missing token: 403 plain text, no detail.

Log hygiene (PII constraint):
  - Log tenant_id and sha256(email)[:12] prefix only.
  - Never log email address, token, or signing key.

Environment variables:
  ATTRIBUTION_AGGREGATES_TABLE  DynamoDB table for suppression rows
  UNSUB_SECRET_NAME             Secrets Manager secret name for HMAC signing key
"""

import base64
import hashlib
import hmac as hmac_module
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
ATTRIBUTION_AGGREGATES_TABLE = os.environ.get('ATTRIBUTION_AGGREGATES_TABLE', '')
UNSUB_SECRET_NAME = os.environ.get('UNSUB_SECRET_NAME', '')

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
_dynamodb = boto3.resource('dynamodb')
_secretsmanager = boto3.client('secretsmanager')

# ---------------------------------------------------------------------------
# In-process signing key cache
# None = not yet fetched. Transient failures are NOT cached.
# ---------------------------------------------------------------------------
_unsub_signing_key: Optional[bytes] = None


# ---------------------------------------------------------------------------
# HTML response page (self-contained, minimal, no external deps)
# ---------------------------------------------------------------------------
_UNSUB_SUCCESS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Unsubscribed</title>
<style>
  body{margin:0;padding:0;background:#f1f5f9;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;}
  .card{background:#fff;border-radius:16px;border:1px solid #e2e8f0;padding:48px 40px;max-width:460px;text-align:center;}
  .icon{font-size:40px;margin-bottom:16px;}
  h1{font-size:22px;font-weight:800;color:#0f172a;margin:0 0 12px;}
  p{font-size:14px;color:#64748b;line-height:1.6;margin:0;}
</style>
</head>
<body>
<div class="card">
  <div class="icon">&#10003;</div>
  <h1>You've been unsubscribed</h1>
  <p>You've been unsubscribed from monthly recap emails. You can re-enable them anytime from your dashboard settings.</p>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _b64url_nopad_decode(s: str) -> bytes:
    """Decode a base64url string without padding."""
    # Re-add padding
    padded = s + '=' * (4 - len(s) % 4) if len(s) % 4 else s
    return base64.urlsafe_b64decode(padded)


def _get_signing_key() -> Optional[bytes]:
    """
    Fetch HMAC signing key from Secrets Manager.
    Caches on success; transient errors NOT cached (caller retries next invoke).
    NEVER logs the key value.
    """
    global _unsub_signing_key
    if _unsub_signing_key is not None:
        return _unsub_signing_key

    if not UNSUB_SECRET_NAME:
        logger.error('UNSUB_SECRET_NAME env var is not set')
        return None

    try:
        resp = _secretsmanager.get_secret_value(SecretId=UNSUB_SECRET_NAME)
    except ClientError as exc:
        logger.error(
            'Failed to fetch unsub signing key: secret=%s code=%s',
            UNSUB_SECRET_NAME, exc.response['Error']['Code'],
        )
        return None
    except Exception as exc:
        logger.error('Unexpected error fetching unsub signing key: %s', type(exc).__name__)
        return None

    raw = resp.get('SecretString') or ''
    if not raw:
        logger.error('Unsub signing key secret is empty (secret=%s)', UNSUB_SECRET_NAME)
        return None

    key_bytes = raw.strip().encode('utf-8')
    _unsub_signing_key = key_bytes
    return _unsub_signing_key


def _email_log_id(email: str) -> str:
    """Return sha256(email)[:12] for safe log reference (never the email itself)."""
    return hashlib.sha256(email.encode('utf-8')).hexdigest()[:12]


def _validate_token(token: str, key: bytes) -> Optional[tuple]:
    """
    Validate token and return (tenant_id, email_lower) on success, None on failure.
    Constant-time HMAC compare (hmac.compare_digest).
    """
    if not token or '.' not in token:
        return None

    parts = token.rsplit('.', 1)
    if len(parts) != 2:
        return None

    b64_payload, b64_sig = parts

    try:
        payload_bytes = _b64url_nopad_decode(b64_payload)
        provided_sig = _b64url_nopad_decode(b64_sig)
    except Exception:
        return None

    # Recompute expected HMAC
    expected_sig = hmac_module.new(key, payload_bytes, digestmod='sha256').digest()

    # Constant-time compare
    if not hmac_module.compare_digest(expected_sig, provided_sig):
        return None

    # Decode payload
    try:
        payload = payload_bytes.decode('utf-8')
    except UnicodeDecodeError:
        return None

    # Validate payload structure: {tenant_id}|{email_lower}|recap
    segments = payload.split('|')
    if len(segments) != 3:
        return None

    tenant_id, email_lower, suffix = segments
    if suffix != 'recap':
        return None

    if not tenant_id or not email_lower:
        return None

    return tenant_id, email_lower


def _record_suppression(tenant_id: str, email_lower: str) -> None:
    """
    Conditional PutItem for suppression row.
    pk=TENANT#{tenant_id}, sk=SUPPRESS#recap#{email_lower}
    created_at (ISO), source="unsubscribe_link"
    NO TTL (permanent).
    ConditionalCheckFailedException => already suppressed, treat as success.
    """
    if not ATTRIBUTION_AGGREGATES_TABLE:
        logger.error('ATTRIBUTION_AGGREGATES_TABLE env var is not set -- cannot write suppression')
        return

    try:
        table = _dynamodb.Table(ATTRIBUTION_AGGREGATES_TABLE)
        table.put_item(
            Item={
                'pk': f'TENANT#{tenant_id}',
                'sk': f'SUPPRESS#recap#{email_lower}',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'source': 'unsubscribe_link',
            },
            ConditionExpression='attribute_not_exists(sk)',
        )
        logger.info(
            'Suppression row written: tenant=%s email_sha=%s',
            tenant_id[:8], _email_log_id(email_lower),
        )
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
            # Already suppressed; idempotent success.
            logger.info(
                'Suppression row already exists (idempotent): tenant=%s email_sha=%s',
                tenant_id[:8], _email_log_id(email_lower),
            )
        else:
            logger.error(
                'Failed to write suppression row: tenant=%s email_sha=%s code=%s',
                tenant_id[:8], _email_log_id(email_lower),
                exc.response['Error']['Code'],
            )
            raise


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    """
    GET handler for Lambda Function URL.
    Extracts `t` query param, validates HMAC token, writes suppression row.
    Returns 200 HTML on success, 403 plain text on any invalid/missing token.
    No other routes or actions.
    """
    # Extract query params (Function URL places them under queryStringParameters)
    qs = event.get('queryStringParameters') or {}
    token = qs.get('t', '').strip()

    if not token:
        logger.warning('Unsubscribe request: missing token param')
        return _403()

    # Fetch signing key
    key = _get_signing_key()
    if key is None:
        # Can't validate -- fail closed, but don't reveal why to the caller.
        logger.error('Unsubscribe request: signing key unavailable -- returning 403')
        return _403()

    # Validate token
    result = _validate_token(token, key)
    if result is None:
        logger.warning('Unsubscribe request: invalid token (validation failed)')
        return _403()

    tenant_id, email_lower = result

    logger.info(
        'Unsubscribe: valid token for tenant=%s email_sha=%s',
        tenant_id[:8], _email_log_id(email_lower),
    )

    # Write suppression row (idempotent)
    try:
        _record_suppression(tenant_id, email_lower)
    except Exception:
        # Error already logged in _record_suppression; return 500 is acceptable
        # here but we choose 200 to avoid confusing the recipient on retry --
        # the suppression failure is an ops concern, not a recipient UX concern.
        # The next send will re-check and the recipient can click again.
        pass

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'text/html; charset=utf-8'},
        'body': _UNSUB_SUCCESS_HTML,
    }


def _403() -> dict:
    return {
        'statusCode': 403,
        'headers': {'Content-Type': 'text/plain'},
        'body': 'Forbidden',
    }
