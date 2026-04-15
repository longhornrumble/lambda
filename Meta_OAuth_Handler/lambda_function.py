"""
Meta_OAuth_Handler — Phase 2A of the Meta Messenger integration.

Handles Meta (Facebook) OAuth flows for connecting Pages to the Picasso
messaging platform. Exposed via the Config Builder API Gateway with Clerk JWT
authentication on all routes except the OAuth callback (which is protected by
the signed state JWT instead).

Routes:
    GET  /meta/oauth/url?tenant_id=X            — Generate OAuth redirect URL
    GET  /meta/oauth/callback?code=X&state=Y    — Handle OAuth callback
    POST /meta/channels/{tenant_id}/disconnect  — Disconnect a Page
    POST /meta/channels/{tenant_id}/toggle      — Enable / disable a Page
    GET  /meta/channels/{tenant_id}             — List connected channels

Dependencies (must be present in the deployment package or a Lambda Layer):
    PyJWT >= 2.8.0   (pip install PyJWT)

All other dependencies are boto3 (provided by the Lambda runtime) and Python
stdlib modules.

Environment variables:
    ENVIRONMENT             — staging | production
    META_APP_ID             — Facebook App ID
    META_APP_SECRET_ARN     — Secrets Manager ARN for the Meta App Secret
    OAUTH_CALLBACK_URL      — Publicly reachable callback URL for this Lambda
    CHANNEL_MAPPINGS_TABLE  — DynamoDB table (e.g. picasso-channel-mappings-staging)
    KMS_KEY_ID              — KMS key alias or ARN (e.g. alias/picasso-channel-tokens)
    AWS_REGION              — AWS region (set automatically by Lambda runtime)
"""

import base64
import hashlib
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
import jwt  # PyJWT — must be in deployment package or Lambda Layer

# ---------------------------------------------------------------------------
# Module-level singletons — initialised once per Lambda cold start
# ---------------------------------------------------------------------------

_region = os.environ.get("AWS_REGION", "us-east-1")
_secrets_client = boto3.client("secretsmanager", region_name=_region)
_dynamodb = boto3.resource("dynamodb", region_name=_region)
_kms_client = boto3.client("kms", region_name=_region)

# Cached app secret — populated on first use, survives warm invocations
_meta_app_secret: Optional[str] = None

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

_ENV = os.environ.get("ENVIRONMENT", "staging")
_META_APP_ID = os.environ.get("META_APP_ID", "")
_META_APP_SECRET_ARN = os.environ.get("META_APP_SECRET_ARN", "")
_OAUTH_CALLBACK_URL = os.environ.get("OAUTH_CALLBACK_URL", "")
_CHANNEL_MAPPINGS_TABLE = os.environ.get(
    "CHANNEL_MAPPINGS_TABLE", f"picasso-channel-mappings-{_ENV}"
)
_KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "alias/picasso-channel-tokens")

_GRAPH_API_VERSION = "v21.0"
_GRAPH_BASE = f"https://graph.facebook.com/{_GRAPH_API_VERSION}"

# OAuth scopes required for Messenger
_OAUTH_SCOPES = "pages_show_list,pages_messaging,pages_read_engagement"

# State JWT lifetime in seconds (10 minutes)
_STATE_JWT_TTL = 600

# Page token TTL in DynamoDB — 10 years expressed as seconds
_TOKEN_TTL_SECONDS = 10 * 365 * 24 * 3600

# CORS headers applied to every response
_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key",
    "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# Secrets / credentials
# ---------------------------------------------------------------------------


def _get_meta_app_secret() -> str:
    """
    Retrieve the Meta App Secret from Secrets Manager, caching in module scope
    so subsequent warm invocations avoid an extra API call.

    SECURITY: The secret value is never logged.
    """
    global _meta_app_secret
    if _meta_app_secret:
        return _meta_app_secret

    if not _META_APP_SECRET_ARN:
        raise RuntimeError(
            "META_APP_SECRET_ARN env var not set — cannot retrieve Meta App Secret"
        )

    print(
        f"[INFO] Fetching Meta App Secret from Secrets Manager: {_META_APP_SECRET_ARN}"
    )
    response = _secrets_client.get_secret_value(SecretId=_META_APP_SECRET_ARN)
    secret_string = response.get("SecretString", "")

    # Support both plain-string and JSON-encoded secrets
    try:
        secret_data = json.loads(secret_string)
        _meta_app_secret = secret_data.get("appSecret", secret_string)
    except json.JSONDecodeError:
        _meta_app_secret = secret_string

    if not _meta_app_secret:
        raise RuntimeError("Meta App Secret retrieved from Secrets Manager was empty")

    print("[INFO] Meta App Secret loaded successfully")
    return _meta_app_secret


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------


def _json_response(status_code: int, body: Any, extra_headers: Optional[dict] = None) -> dict:
    """Build a Lambda proxy integration response with CORS headers."""
    headers = dict(_CORS_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(body),
    }


def _html_response(status_code: int, html: str) -> dict:
    """Build an HTML Lambda proxy response (used for the OAuth callback popup)."""
    headers = {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
    }
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": html,
    }


def _error_response(status_code: int, message: str) -> dict:
    print(f"[ERROR] HTTP {status_code}: {message}")
    return _json_response(status_code, {"error": message})


# ---------------------------------------------------------------------------
# OAuth state JWT helpers
# ---------------------------------------------------------------------------


def _generate_state_jwt(tenant_id: str) -> str:
    """
    Create a short-lived, signed JWT used as the OAuth `state` parameter.

    Payload: { tenant_id, nonce, iat, exp }
    Algorithm: HS256 with the Meta App Secret as the signing key.
    """
    app_secret = _get_meta_app_secret()
    now = int(time.time())
    payload = {
        "tenant_id": tenant_id,
        "nonce": secrets.token_hex(16),
        "iat": now,
        "exp": now + _STATE_JWT_TTL,
    }
    token = jwt.encode(payload, app_secret, algorithm="HS256")
    print(f"[INFO] Generated state JWT for tenant_id={tenant_id}")
    return token


def _validate_state_jwt(state_token: str) -> dict:
    """
    Verify and decode the state JWT returned by Meta in the OAuth callback.

    Returns the decoded payload dict.
    Raises jwt.InvalidTokenError (or a subclass) on any failure.
    """
    app_secret = _get_meta_app_secret()
    payload = jwt.decode(
        state_token,
        app_secret,
        algorithms=["HS256"],
        options={"require": ["exp", "tenant_id", "nonce"]},
    )
    return payload


# ---------------------------------------------------------------------------
# Meta Graph API helpers
# ---------------------------------------------------------------------------


def _graph_get(path: str, params: dict) -> dict:
    """Execute a GET request against the Meta Graph API."""
    query_string = urllib.parse.urlencode(params)
    url = f"{_GRAPH_BASE}{path}?{query_string}"
    print(f"[INFO] Graph API GET {path}")
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        print(f"[ERROR] Graph API GET {path} failed: HTTP {exc.code} — {body}")
        raise


def _graph_post(path: str, params: dict, json_body: Optional[dict] = None) -> dict:
    """Execute a POST request against the Meta Graph API."""
    query_string = urllib.parse.urlencode(params)
    url = f"{_GRAPH_BASE}{path}?{query_string}"
    print(f"[INFO] Graph API POST {path}")
    data: Optional[bytes] = None
    headers: dict = {}
    if json_body is not None:
        data = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        print(f"[ERROR] Graph API POST {path} failed: HTTP {exc.code} — {body}")
        raise


def _graph_delete(path: str, params: dict) -> dict:
    """Execute a DELETE request against the Meta Graph API."""
    query_string = urllib.parse.urlencode(params)
    url = f"{_GRAPH_BASE}{path}?{query_string}"
    print(f"[INFO] Graph API DELETE {path}")
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        print(f"[ERROR] Graph API DELETE {path} failed: HTTP {exc.code} — {body}")
        raise


# ---------------------------------------------------------------------------
# KMS encryption / decryption helpers
# ---------------------------------------------------------------------------


def _encrypt_token(plaintext: str) -> str:
    """Encrypt a token string with KMS and return a base64-encoded ciphertext."""
    response = _kms_client.encrypt(
        KeyId=_KMS_KEY_ID,
        Plaintext=plaintext.encode(),
    )
    ciphertext_blob = response["CiphertextBlob"]
    return base64.b64encode(ciphertext_blob).decode()


def _decrypt_token(encoded_ciphertext: str) -> str:
    """Decrypt a base64-encoded KMS ciphertext and return the plaintext token."""
    ciphertext_blob = base64.b64decode(encoded_ciphertext)
    response = _kms_client.decrypt(CiphertextBlob=ciphertext_blob)
    return response["Plaintext"].decode()


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def _channel_table():
    """Return the DynamoDB Table resource for channel mappings."""
    return _dynamodb.Table(_CHANNEL_MAPPINGS_TABLE)


def _tenant_hash(tenant_id: str) -> str:
    """
    Derive a stable, short hash from the tenant_id used as a GSI sort key.
    This is a lightweight hash for data locality — not a security primitive.
    """
    return hashlib.sha256(tenant_id.encode()).hexdigest()[:16]


def _query_channels_by_tenant(tenant_id: str) -> list:
    """Query the TenantIndex GSI to find all channel records for a tenant."""
    table = _channel_table()
    response = table.query(
        IndexName="TenantIndex",
        KeyConditionExpression=boto3.dynamodb.conditions.Key("tenantId").eq(tenant_id),
    )
    return response.get("Items", [])


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


def _handle_get_oauth_url(event: dict) -> dict:
    """
    GET /meta/oauth/url?tenant_id=X

    Generates a signed Meta OAuth redirect URL for the Config Builder UI to
    open in a popup window.
    """
    params = event.get("queryStringParameters") or {}
    tenant_id = params.get("tenant_id", "").strip()

    if not tenant_id:
        return _error_response(400, "Missing required query parameter: tenant_id")

    print(f"[INFO] Generating OAuth URL for tenant_id={tenant_id}")

    if not _META_APP_ID:
        return _error_response(500, "META_APP_ID environment variable not configured")
    if not _OAUTH_CALLBACK_URL:
        return _error_response(500, "OAUTH_CALLBACK_URL environment variable not configured")

    try:
        state = _generate_state_jwt(tenant_id)
    except Exception as exc:
        print(f"[ERROR] Failed to generate state JWT: {exc}")
        return _error_response(500, "Failed to generate OAuth state token")

    oauth_params = urllib.parse.urlencode(
        {
            "client_id": _META_APP_ID,
            "redirect_uri": _OAUTH_CALLBACK_URL,
            "scope": _OAUTH_SCOPES,
            "state": state,
            "response_type": "code",
        }
    )
    oauth_url = f"https://www.facebook.com/{_GRAPH_API_VERSION}/dialog/oauth?{oauth_params}"

    print(f"[INFO] OAuth URL generated for tenant_id={tenant_id}")
    return _json_response(200, {"oauth_url": oauth_url})


def _handle_oauth_callback(event: dict) -> dict:
    """
    GET /meta/oauth/callback?code=X&state=Y

    Completes the OAuth flow:
      1. Validates the state JWT to recover tenant_id
      2. Exchanges the auth code for a User Access Token
      3. Lists the user's Pages
      4. Selects the target Page (first, or matching page_id param)
      5. Encrypts the Page Access Token with KMS
      6. Writes the channel mapping to DynamoDB
      7. Subscribes the Page to the webhook
      8. Configures the Get Started button
      9. Returns an HTML popup that posts a message to the opener and closes

    SECURITY: No tokens are logged at any point.
    """
    params = event.get("queryStringParameters") or {}
    code = params.get("code", "").strip()
    state_token = params.get("state", "").strip()
    preferred_page_id = params.get("page_id", "").strip()

    # --- Validate state JWT ---
    if not state_token:
        return _html_error_popup("Missing state parameter in OAuth callback")

    try:
        state_payload = _validate_state_jwt(state_token)
    except jwt.ExpiredSignatureError:
        print("[WARN] State JWT expired in OAuth callback")
        return _html_error_popup("OAuth session expired — please try connecting again")
    except jwt.InvalidTokenError as exc:
        print(f"[WARN] Invalid state JWT in OAuth callback: {exc}")
        return _html_error_popup("Invalid OAuth state — possible CSRF attempt")

    tenant_id: str = state_payload["tenant_id"]
    print(f"[INFO] OAuth callback received for tenant_id={tenant_id}")

    if not code:
        return _html_error_popup("Missing authorization code in OAuth callback")

    # --- Exchange code for User Access Token ---
    try:
        app_secret = _get_meta_app_secret()
        token_response = _graph_get(
            "/oauth/access_token",
            {
                "client_id": _META_APP_ID,
                "client_secret": app_secret,
                "redirect_uri": _OAUTH_CALLBACK_URL,
                "code": code,
            },
        )
        user_access_token = token_response.get("access_token")
        if not user_access_token:
            raise ValueError("access_token missing from token exchange response")
    except Exception as exc:
        print(f"[ERROR] Token exchange failed for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to exchange authorization code for access token")

    # --- List managed Pages ---
    try:
        accounts_response = _graph_get(
            "/me/accounts",
            {"access_token": user_access_token},
        )
        pages = accounts_response.get("data", [])
        if not pages:
            return _html_error_popup(
                "No Facebook Pages found — please ensure you manage at least one Page"
            )
    except Exception as exc:
        print(f"[ERROR] Failed to retrieve Pages for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to retrieve your Facebook Pages")

    # --- Select target Page ---
    if preferred_page_id:
        matched = [p for p in pages if p.get("id") == preferred_page_id]
        page = matched[0] if matched else pages[0]
        if not matched:
            print(
                f"[WARN] Requested page_id={preferred_page_id} not found; "
                f"using first available Page for tenant_id={tenant_id}"
            )
    else:
        page = pages[0]

    page_id: str = page["id"]
    page_name: str = page.get("name", page_id)
    page_access_token: str = page.get("access_token", "")

    if not page_access_token:
        return _html_error_popup(
            f"No access token returned for Page '{page_name}' — please re-authorise"
        )

    print(f"[INFO] Selected Page id={page_id} name='{page_name}' for tenant_id={tenant_id}")

    # --- Encrypt Page Access Token ---
    try:
        encrypted_page_token = _encrypt_token(page_access_token)
    except Exception as exc:
        print(f"[ERROR] KMS encryption failed for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to secure the Page access token")

    # --- Write channel mapping to DynamoDB ---
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    ttl_value = int(time.time()) + _TOKEN_TTL_SECONDS
    item = {
        "PK": f"PAGE#{page_id}",
        "SK": "CHANNEL#messenger",
        "tenantId": tenant_id,
        "tenantHash": _tenant_hash(tenant_id),
        "channelType": "messenger",
        "encryptedPageToken": encrypted_page_token,
        "pageId": page_id,
        "pageName": page_name,
        "connectedAt": now_iso,
        "connectedBy": "oauth",
        "enabled": True,
        "ttl": ttl_value,
    }

    try:
        _channel_table().put_item(Item=item)
        print(f"[INFO] DynamoDB channel mapping written for page_id={page_id}, tenant_id={tenant_id}")
    except Exception as exc:
        print(f"[ERROR] DynamoDB write failed for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to save channel mapping — please try again")

    # --- Subscribe Page to webhook ---
    try:
        _graph_post(
            f"/{page_id}/subscribed_apps",
            {
                "subscribed_fields": "messages,messaging_postbacks",
                "access_token": page_access_token,
            },
        )
        print(f"[INFO] Webhook subscription created for page_id={page_id}")
    except Exception as exc:
        # Non-fatal: channel is saved; user can retry subscription separately
        print(f"[WARN] Webhook subscription failed for page_id={page_id}: {exc}")

    # --- Configure Get Started button ---
    try:
        _graph_post(
            f"/{page_id}/messenger_profile",
            {"access_token": page_access_token},
            json_body={"get_started": {"payload": "GET_STARTED"}},
        )
        print(f"[INFO] Get Started button configured for page_id={page_id}")
    except Exception as exc:
        # Non-fatal: Messenger still works without the Get Started button
        print(f"[WARN] Get Started button configuration failed for page_id={page_id}: {exc}")

    # --- Return popup HTML ---
    return _html_success_popup(page_id, page_name)


def _handle_disconnect_channel(tenant_id: str) -> dict:
    """
    POST /meta/channels/{tenant_id}/disconnect

    Unsubscribes the Page from the webhook and TTL-expires the DynamoDB record.
    """
    if not tenant_id:
        return _error_response(400, "tenant_id path parameter is required")

    print(f"[INFO] Disconnecting Messenger channel for tenant_id={tenant_id}")

    channels = _query_channels_by_tenant(tenant_id)
    if not channels:
        return _error_response(
            404, f"No connected Messenger channel found for tenant_id={tenant_id}"
        )

    # Use the first channel record (one Page per tenant in the current design)
    channel = channels[0]
    page_id: str = channel.get("pageId", "")
    encrypted_token: str = channel.get("encryptedPageToken", "")
    pk: str = channel.get("PK", f"PAGE#{page_id}")
    sk: str = channel.get("SK", "CHANNEL#messenger")

    # --- Decrypt Page Access Token ---
    try:
        page_access_token = _decrypt_token(encrypted_token)
    except Exception as exc:
        print(f"[WARN] KMS decryption failed for page_id={page_id}: {exc}")
        page_access_token = ""

    # --- Unsubscribe from webhook (best-effort) ---
    if page_access_token and page_id:
        try:
            _graph_delete(
                f"/{page_id}/subscribed_apps",
                {"access_token": page_access_token},
            )
            print(f"[INFO] Webhook unsubscribed for page_id={page_id}")
        except Exception as exc:
            # Non-fatal: proceed with DynamoDB cleanup regardless
            print(f"[WARN] Webhook unsubscription failed for page_id={page_id}: {exc}")

    # --- TTL-expire the DynamoDB record ---
    expire_at = int(time.time()) + 60  # expire in 60 seconds
    try:
        _channel_table().update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression="SET #ttl = :expire, enabled = :false",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":expire": expire_at, ":false": False},
        )
        print(
            f"[INFO] DynamoDB record TTL-expired for page_id={page_id}, tenant_id={tenant_id}"
        )
    except Exception as exc:
        print(f"[ERROR] DynamoDB TTL update failed for tenant_id={tenant_id}: {exc}")
        return _error_response(500, "Failed to remove channel mapping")

    return _json_response(200, {"success": True})


def _handle_toggle_channel(tenant_id: str, body: dict) -> dict:
    """
    POST /meta/channels/{tenant_id}/toggle

    Updates the `enabled` flag on the channel mapping record.
    """
    if not tenant_id:
        return _error_response(400, "tenant_id path parameter is required")

    if "enabled" not in body:
        return _error_response(400, "Request body must include 'enabled' boolean field")

    enabled = bool(body["enabled"])
    print(f"[INFO] Toggling channel enabled={enabled} for tenant_id={tenant_id}")

    channels = _query_channels_by_tenant(tenant_id)
    if not channels:
        return _error_response(
            404, f"No connected Messenger channel found for tenant_id={tenant_id}"
        )

    channel = channels[0]
    pk: str = channel.get("PK", "")
    sk: str = channel.get("SK", "CHANNEL#messenger")

    try:
        _channel_table().update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression="SET enabled = :enabled",
            ExpressionAttributeValues={":enabled": enabled},
        )
        print(
            f"[INFO] Channel enabled={enabled} updated for tenant_id={tenant_id}"
        )
    except Exception as exc:
        print(f"[ERROR] DynamoDB toggle update failed for tenant_id={tenant_id}: {exc}")
        return _error_response(500, "Failed to update channel status")

    return _json_response(200, {"success": True, "enabled": enabled})


def _handle_list_channels(tenant_id: str) -> dict:
    """
    GET /meta/channels/{tenant_id}

    Returns the list of connected Messenger Pages for the given tenant.
    Encrypted tokens are never included in the response.
    """
    if not tenant_id:
        return _error_response(400, "tenant_id path parameter is required")

    print(f"[INFO] Listing channels for tenant_id={tenant_id}")

    try:
        channels = _query_channels_by_tenant(tenant_id)
    except Exception as exc:
        print(f"[ERROR] DynamoDB query failed for tenant_id={tenant_id}: {exc}")
        return _error_response(500, "Failed to retrieve channel list")

    # Strip encrypted token before returning — never expose it
    safe_channels = [
        {k: v for k, v in ch.items() if k != "encryptedPageToken"}
        for ch in channels
    ]

    return _json_response(200, {"channels": safe_channels})


# ---------------------------------------------------------------------------
# HTML popup helpers
# ---------------------------------------------------------------------------


def _html_success_popup(page_id: str, page_name: str) -> dict:
    """Return an HTML response that posts META_OAUTH_SUCCESS to the opener popup."""
    # Escape values for safe embedding in a JS string literal
    safe_page_id = page_id.replace("'", "\\'").replace('"', '\\"')
    safe_page_name = page_name.replace("'", "\\'").replace('"', '\\"')
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Connecting...</title></head>
<body>
<p>Connected successfully. This window will close automatically.</p>
<script>
  try {{
    window.opener.postMessage(
      {{ type: 'META_OAUTH_SUCCESS', pageId: '{safe_page_id}', pageName: '{safe_page_name}' }},
      '*'
    );
  }} catch (e) {{
    console.error('Failed to post message to opener:', e);
  }}
  window.close();
</script>
</body>
</html>"""
    return _html_response(200, html)


def _html_error_popup(error_message: str) -> dict:
    """Return an HTML response that posts META_OAUTH_ERROR to the opener popup."""
    print(f"[ERROR] OAuth popup error: {error_message}")
    # Escape for safe embedding
    safe_message = error_message.replace("'", "\\'").replace('"', '\\"')
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Connection Error</title></head>
<body>
<p>An error occurred while connecting your Facebook Page.</p>
<p><strong>{error_message}</strong></p>
<script>
  try {{
    window.opener.postMessage(
      {{ type: 'META_OAUTH_ERROR', error: '{safe_message}' }},
      '*'
    );
  }} catch (e) {{
    console.error('Failed to post message to opener:', e);
  }}
  window.close();
</script>
</body>
</html>"""
    return _html_response(400, html)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


def _route(event: dict) -> dict:
    """
    Dispatch the incoming API Gateway event to the appropriate route handler.

    Supports both API Gateway v1 (REST) and v2 (HTTP) payload formats.
    """
    http_method = (
        event.get("httpMethod")                              # v1
        or (event.get("requestContext", {}).get("http", {}).get("method", ""))  # v2
    ).upper()

    raw_path = (
        event.get("path")                                    # v1
        or event.get("rawPath", "")                         # v2
    )

    # Normalise path — strip trailing slash
    path = raw_path.rstrip("/")

    print(f"[INFO] {http_method} {path}")

    # Pre-flight CORS
    if http_method == "OPTIONS":
        return _json_response(204, {})

    # --- GET /meta/oauth/url ---
    if http_method == "GET" and path.endswith("/meta/oauth/url"):
        return _handle_get_oauth_url(event)

    # --- GET /meta/oauth/callback ---
    if http_method == "GET" and path.endswith("/meta/oauth/callback"):
        return _handle_oauth_callback(event)

    # --- Routes with {tenant_id} path parameter ---
    # /meta/channels/{tenant_id}/disconnect
    # /meta/channels/{tenant_id}/toggle
    # /meta/channels/{tenant_id}
    path_parts = [p for p in path.split("/") if p]

    # Expect at minimum: ["meta", "channels", "{tenant_id}"]
    channels_index = None
    for idx, part in enumerate(path_parts):
        if part == "channels":
            channels_index = idx
            break

    if channels_index is not None and len(path_parts) > channels_index + 1:
        tenant_id = path_parts[channels_index + 1]
        action = path_parts[channels_index + 2] if len(path_parts) > channels_index + 2 else None

        if http_method == "POST" and action == "disconnect":
            return _handle_disconnect_channel(tenant_id)

        if http_method == "POST" and action == "toggle":
            raw_body = event.get("body") or "{}"
            try:
                body = json.loads(raw_body)
            except json.JSONDecodeError:
                return _error_response(400, "Request body is not valid JSON")
            return _handle_toggle_channel(tenant_id, body)

        if http_method == "GET" and action is None:
            return _handle_list_channels(tenant_id)

    return _error_response(404, f"Route not found: {http_method} {path}")


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Entry point for the Meta_OAuth_Handler Lambda function.

    Receives API Gateway proxy integration events, dispatches to route handlers,
    and returns a structured HTTP response. Unhandled exceptions are caught here
    to ensure a well-formed error response is always returned.

    Parameters
    ----------
    event : dict
        API Gateway v1 or v2 proxy integration event.
    context : LambdaContext
        AWS Lambda context object (function name, remaining time, etc.).

    Returns
    -------
    dict
        API Gateway proxy integration response with statusCode, headers, body.
    """
    request_id = getattr(context, "aws_request_id", "unknown")
    print(
        f"[INFO] Meta_OAuth_Handler invoked | "
        f"request_id={request_id} | "
        f"env={_ENV} | "
        f"function={getattr(context, 'function_name', 'unknown')}"
    )

    try:
        response = _route(event)
    except Exception as exc:  # pylint: disable=broad-except
        # Catch-all to prevent Lambda from returning an unstructured 502
        print(f"[ERROR] Unhandled exception in Meta_OAuth_Handler: {exc}")
        import traceback
        traceback.print_exc()
        response = _error_response(500, "Internal server error")

    status = response.get("statusCode", 500)
    print(f"[INFO] Responding with HTTP {status} | request_id={request_id}")
    return response
