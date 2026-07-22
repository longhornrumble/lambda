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
    POST /meta/channels/{tenant_id}/repush-welcome — Re-push welcome surfaces to Meta
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
    CHANNEL_MAPPINGS_TABLE  — DynamoDB table (e.g. picasso-channel-mappings)
    KMS_KEY_ID              — KMS key alias or ARN (e.g. alias/picasso-channel-tokens)
    META_LOGIN_CONFIG_ID    — Facebook Login for Business configuration ID
                              (optional; when set, dialog sends config_id, not scope)
    TENANT_REGISTRY_TABLE   — tenant registry table for platform tenantHash lookup
                              (optional; unset = legacy computed hash)
    CONFIG_BUCKET           — S3 bucket holding tenant configs, used to push
                              Messenger welcome surfaces (ice breakers +
                              persistent menu) on connect (optional; unset =
                              welcome-surface push disabled, no other behavior
                              change — M5)
    AWS_REGION              — AWS region (set automatically by Lambda runtime)
"""

import base64
import hashlib
import hmac
import html
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import boto3
import jwt  # PyJWT — must be in deployment package or Lambda Layer
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Module-level singletons — initialised once per Lambda cold start
# ---------------------------------------------------------------------------

_region = os.environ.get("AWS_REGION", "us-east-1")
_secrets_client = boto3.client("secretsmanager", region_name=_region)
_dynamodb = boto3.resource("dynamodb", region_name=_region)
_kms_client = boto3.client("kms", region_name=_region)
_s3_client = boto3.client("s3", region_name=_region)

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
    "CHANNEL_MAPPINGS_TABLE", "picasso-channel-mappings"
)
_KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "alias/picasso-channel-tokens")
_TENANT_REGISTRY_TABLE = os.environ.get("TENANT_REGISTRY_TABLE", "")

# M5 welcome-surface push — unset (default) means the feature is disabled
# entirely (no S3 read attempted), a safe no-op for envs that haven't wired
# the env var / IAM grant yet.
_CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET", "")

# --- Caller authorization for the /meta/channels/* management routes ---
# The Function URL is AuthType NONE (the OAuth callback must be publicly
# reachable for Meta's redirect), so the management routes authorize the CALLER
# in-handler. Two accepted identities:
#   1. Tenant self-service — the dashboard/portal's internal Picasso JWT
#      (HS256, same secret Analytics_Dashboard_API signs). Authorized only for
#      its OWN tenant (token tenant_id must equal the path tenant_id).
#   2. Operator (super-admin) — the Config Builder's Clerk 'picasso-config' JWT
#      (RS256, verified against the config Clerk JWKS). Authorized for any
#      tenant, mirroring Picasso_Config_Manager's trust model.
# Direct server-to-server invokes (no Function URL requestContext, e.g. Config
# Manager's welcome-repush backstop) are already IAM-authenticated and exempt.
_JWT_SECRET_KEY_NAME = os.environ.get(
    "JWT_SECRET_KEY_NAME", "picasso/staging/jwt/signing-key"
)
_CLERK_CONFIG_JWKS_URL = os.environ.get(
    "CLERK_CONFIG_JWKS_URL",
    "https://clerk.config.myrecruiter.ai/.well-known/jwks.json",
)
_jwt_signing_secret: Optional[str] = None      # cached HS256 secret
_clerk_jwks_cache: Optional[dict] = None        # cached config Clerk JWKS
_clerk_jwks_cache_time: float = 0
_CLERK_JWKS_CACHE_TTL = 3600  # seconds

# C5: ice breakers cap at 4 (both channels); persistent_menu title cap
# mirrors the C5 quick-reply title cap (20 chars).
_MAX_ICE_BREAKERS = 4
_MAX_MENU_TITLE_CHARS = 20

_GRAPH_API_VERSION = "v21.0"
_GRAPH_BASE = f"https://graph.facebook.com/{_GRAPH_API_VERSION}"

# OAuth scopes required for Messenger and Instagram DMs.
# Note: Instagram Professional accounts no longer require a linked Facebook Page
# as of July 2024 — instagram_basic and instagram_manage_messages are sufficient
# for direct Instagram DM access when the account is a Professional account.
_OAUTH_SCOPES = "pages_show_list,pages_messaging,pages_read_engagement,instagram_basic,instagram_manage_messages"

# Facebook Login for Business configuration ID. Use-case ("business type") Meta
# apps commit asset grants through a saved login configuration; for them the
# dialog must send config_id INSTEAD of scope (Meta docs: "config_id has
# replaced scope (which should not be used)"). Unset → legacy scope dialog.
_META_LOGIN_CONFIG_ID = os.environ.get("META_LOGIN_CONFIG_ID", "")

# State JWT lifetime in seconds (10 minutes)
_STATE_JWT_TTL = 600

# Multi-Page selection nonce lifetime — how long the in-popup Page picker stays
# valid before the admin must reconnect (10 minutes, matches the state JWT).
_SELECTION_TTL_SECONDS = 600

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


def _json_default(obj: Any) -> Any:
    """
    json.dumps fallback for types the stdlib encoder can't handle.

    DynamoDB's resource API returns every number as decimal.Decimal, so any
    route that echoes a stored item (e.g. GET /meta/channels/{id} returning
    the `ttl` epoch) would otherwise raise "Object of type Decimal is not JSON
    serializable" and 500. Whole values → int (ttl, counts); fractional → float.
    """
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _json_response(status_code: int, body: Any, extra_headers: Optional[dict] = None) -> dict:
    """Build a Lambda proxy integration response with CORS headers."""
    headers = dict(_CORS_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(body, default=_json_default),
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
    Resolve the tenant's PLATFORM tenantHash from the tenant registry.

    bedrock-core resolves configs by the registry hash (TenantHashIndex →
    tenant_id → tenants/<id>/<id>-config.json). A locally computed hash
    matches nothing downstream — the Response Processor then silently loads
    defaults and answers without KB grounding (found live 2026-07-12).

    Falls back to the legacy sha256 derivation only when the registry is
    unavailable or has no row (keeps the module usable standalone).
    """
    if _TENANT_REGISTRY_TABLE:
        try:
            response = _dynamodb.Table(_TENANT_REGISTRY_TABLE).get_item(
                Key={"tenantId": tenant_id}
            )
            registry_hash = (response.get("Item") or {}).get("tenantHash")
            if registry_hash:
                return registry_hash
            print(f"[WARN] No registry tenantHash for tenant_id={tenant_id}; using computed fallback")
        except Exception as exc:
            print(f"[WARN] Registry hash lookup failed for tenant_id={tenant_id}: {exc}; using computed fallback")
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
# M5 — Welcome surfaces (ice breakers + persistent menu)
# ---------------------------------------------------------------------------


def _load_tenant_config_for_welcome(tenant_id: str) -> Optional[dict]:
    """
    Load the tenant config JSON from S3 for the welcome-surface push.

    Tries `tenants/{tenant_id}/config.json` then the legacy
    `tenants/{tenant_id}/{tenant_id}-config.json` shape. Never raises —
    any miss or parse failure logs a WARN and returns None (schema
    discipline: this is a best-effort read, not the OAuth flow's critical
    path).
    """
    for key in (
        f"tenants/{tenant_id}/config.json",
        f"tenants/{tenant_id}/{tenant_id}-config.json",
    ):
        try:
            obj = _s3_client.get_object(Bucket=_CONFIG_BUCKET, Key=key)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("NoSuchKey", "404"):
                continue
            print(f"[WARN] S3 error loading config s3://{_CONFIG_BUCKET}/{key}: {exc}")
            return None
        except Exception as exc:
            print(f"[WARN] Unexpected error loading config s3://{_CONFIG_BUCKET}/{key}: {exc}")
            return None

        try:
            return json.loads(obj["Body"].read())
        except json.JSONDecodeError as exc:
            print(f"[WARN] Invalid JSON in tenant config s3://{_CONFIG_BUCKET}/{key}: {exc}")
            return None

    print(f"[WARN] No tenant config found in S3 for tenant_id={tenant_id}")
    return None


def _build_welcome_profile_payload(welcome: dict, tenant_id: str) -> tuple:
    """
    Translate C2 `messenger_behavior.welcome` into a Messenger Profile API
    payload. Always includes `get_started`. Malformed ice-breaker/menu
    entries are skipped (logged), never raised.

    Returns (payload, ice_breaker_count, menu_item_count).
    """
    payload: dict = {"get_started": {"payload": "GET_STARTED"}}

    raw_ice_breakers = welcome.get("ice_breakers") or []
    if len(raw_ice_breakers) > _MAX_ICE_BREAKERS:
        print(
            f"[INFO] Truncating ice_breakers to {_MAX_ICE_BREAKERS} for "
            f"tenant_id={tenant_id} (config had {len(raw_ice_breakers)})"
        )
    ice_breakers = []
    for entry in raw_ice_breakers[:_MAX_ICE_BREAKERS]:
        question = entry.get("question")
        ib_payload = entry.get("payload")
        if not question or not ib_payload:
            print(f"[WARN] Skipping malformed ice breaker for tenant_id={tenant_id}: {entry}")
            continue
        ice_breakers.append({"question": question, "payload": ib_payload})
    if ice_breakers:
        payload["ice_breakers"] = [{"call_to_actions": ice_breakers, "locale": "default"}]

    raw_menu = welcome.get("persistent_menu") or []
    menu_items = []
    for entry in raw_menu:
        title = entry.get("title")
        item_payload = entry.get("payload")
        url = entry.get("url")
        if not title or not (item_payload or url):
            print(f"[WARN] Skipping malformed persistent_menu item for tenant_id={tenant_id}: {entry}")
            continue
        truncated_title = title[:_MAX_MENU_TITLE_CHARS]
        if item_payload:
            menu_items.append({"type": "postback", "title": truncated_title, "payload": item_payload})
        else:
            menu_items.append({"type": "web_url", "title": truncated_title, "url": url})
    if menu_items:
        payload["persistent_menu"] = [
            {
                "locale": "default",
                "composer_input_disabled": False,
                "call_to_actions": menu_items,
            }
        ]

    return payload, len(ice_breakers), len(menu_items)


def push_welcome_surfaces(page_access_token: str, tenant_id: str) -> dict:
    """
    Push ice breakers + persistent menu (C2 `messenger_behavior.welcome`) to
    the Messenger Profile API. The same Page access token configures the
    profile for both Facebook Messenger and Page-linked Instagram DM.

    BEST-EFFORT — this must NEVER raise into the OAuth callback flow. Every
    failure path (missing config, flag off, S3 miss, Graph error) returns a
    summary dict instead of raising.
    """
    try:
        if not _CONFIG_BUCKET:
            print("[INFO] CONFIG_BUCKET not configured — welcome-surface push disabled")
            return {"skipped": "config bucket not configured"}

        config = _load_tenant_config_for_welcome(tenant_id)
        if config is None:
            return {"skipped": "tenant config not found or unreadable"}

        feature_flags = config.get("feature_flags") or {}
        if feature_flags.get("MESSENGER_CHANNEL") is not True:
            print(
                f"[INFO] MESSENGER_CHANNEL flag off for tenant_id={tenant_id} — "
                f"skipping welcome-surface push"
            )
            return {"skipped": "MESSENGER_CHANNEL flag not enabled"}

        welcome = (config.get("messenger_behavior") or {}).get("welcome") or {}
        if not welcome.get("ice_breakers") and not welcome.get("persistent_menu"):
            print(f"[INFO] No welcome surfaces configured for tenant_id={tenant_id}")
            return {"skipped": "no welcome surfaces configured"}

        profile_payload, ice_breaker_count, menu_item_count = _build_welcome_profile_payload(
            welcome, tenant_id
        )

        try:
            _graph_post(
                "/me/messenger_profile",
                {"access_token": page_access_token},
                json_body=profile_payload,
            )
        except Exception as exc:
            # _graph_post already logged the Graph error body (no token) for
            # HTTPError; this just short-circuits the OAuth flow safely.
            print(f"[ERROR] Messenger profile push failed for tenant_id={tenant_id}: {exc}")
            return {"error": str(exc)}

        summary = {
            "pushed": {
                "get_started": True,
                "ice_breakers": ice_breaker_count,
                "persistent_menu": menu_item_count,
            }
        }
        print(f"[INFO] Welcome surfaces pushed for tenant_id={tenant_id}: {summary}")
        return summary
    except Exception as exc:  # pylint: disable=broad-except
        # Final safety net — welcome-surface push must never fail OAuth.
        print(f"[ERROR] Unexpected error pushing welcome surfaces for tenant_id={tenant_id}: {exc}")
        return {"error": str(exc)}


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

    dialog_params = {
        "client_id": _META_APP_ID,
        "redirect_uri": _OAUTH_CALLBACK_URL,
        "state": state,
        "response_type": "code",
    }
    if _META_LOGIN_CONFIG_ID:
        dialog_params["config_id"] = _META_LOGIN_CONFIG_ID
    else:
        dialog_params["scope"] = _OAUTH_SCOPES
    oauth_params = urllib.parse.urlencode(dialog_params)
    oauth_url = f"https://www.facebook.com/{_GRAPH_API_VERSION}/dialog/oauth?{oauth_params}"

    print(f"[INFO] OAuth URL generated for tenant_id={tenant_id}")
    return _json_response(200, {"oauth_url": oauth_url})


def _list_pages_via_granular_scopes(user_access_token: str, app_secret: str) -> list:
    """
    Fallback Page discovery for Facebook Login for Business grants.

    /me/accounts returns an empty list for business-portfolio-owned Pages
    granted via an FLB login configuration, even when the grant provably
    includes the Page (visible in debug_token granular_scopes). Recover the
    granted Page IDs from debug_token and fetch each Page directly — the
    direct node lookup DOES honour the granular grant and returns the Page
    access token.
    """
    app_token = f"{_META_APP_ID}|{app_secret}"
    debug_response = _graph_get(
        "/debug_token",
        {"input_token": user_access_token, "access_token": app_token},
    )
    granular_scopes = (debug_response.get("data") or {}).get("granular_scopes") or []
    target_ids = []
    for entry in granular_scopes:
        if entry.get("scope") == "pages_show_list":
            target_ids = entry.get("target_ids") or []
            break

    pages = []
    for page_id in target_ids:
        try:
            page = _graph_get(
                f"/{page_id}",
                {"fields": "id,name,access_token", "access_token": user_access_token},
            )
        except Exception as exc:
            print(f"[WARN] Granular-scope Page fetch failed for page_id={page_id}: {exc}")
            continue
        if page.get("id") and page.get("access_token"):
            pages.append(page)
    return pages


def _handle_oauth_callback(event: dict) -> dict:
    """
    GET /meta/oauth/callback?code=X&state=Y

    Completes the OAuth flow:
      1. Validates the state JWT to recover tenant_id
      2. Exchanges the auth code for a User Access Token
      3. Lists the user's Pages
      4. One Page granted -> connects it via _connect_page. Several granted ->
         hands off to _begin_page_selection (in-popup picker), since Picasso
         connects one Page per tenant.

    SECURITY: No tokens are logged at any point.
    """
    params = event.get("queryStringParameters") or {}
    code = params.get("code", "").strip()
    state_token = params.get("state", "").strip()

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
            # FLB grants on business-portfolio-owned Pages: /me/accounts is
            # empty even though the grant includes the Page — recover it from
            # debug_token granular scopes instead.
            print(f"[INFO] /me/accounts empty for tenant_id={tenant_id}; trying granular-scope fallback")
            pages = _list_pages_via_granular_scopes(user_access_token, app_secret)
        if not pages:
            return _html_error_popup(
                "No Facebook Pages found — please ensure you manage at least one Page"
            )
    except Exception as exc:
        print(f"[ERROR] Failed to retrieve Pages for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to retrieve your Facebook Pages")

    # --- Select target Page ---
    # One granted Page → connect it directly. Several granted Pages → Picasso
    # connects one Page per tenant, so let the admin choose which via an
    # in-popup picker (POST /meta/oauth/select-page). Facebook's own asset
    # picker already covers the common single-Page grant.
    if len(pages) == 1:
        return _connect_page(tenant_id, pages[0])
    return _begin_page_selection(tenant_id, pages)


def _connect_page(tenant_id: str, page: dict) -> dict:
    """
    Finish connecting a single chosen Page: encrypt its access token, write the
    Messenger (and any linked Instagram) channel mapping, push welcome surfaces,
    subscribe the webhook, configure the Get Started button, and return the
    success popup.

    Shared by the single-Page callback path and the multi-Page selection path
    (POST /meta/oauth/select-page). `page` is a dict with id / name /
    access_token, exactly as returned by /me/accounts or the granular fetch.

    SECURITY: No tokens are logged at any point.
    """
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
    except Exception as exc:
        print(f"[ERROR] DynamoDB write failed for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to save channel mapping — please try again")

    # --- Push welcome surfaces (M5, best-effort — never fails the OAuth flow) ---
    # Plaintext page_access_token is still in hand here — no KMS decrypt needed.
    welcome_summary = push_welcome_surfaces(page_access_token, tenant_id)
    print(
        f"[INFO] DynamoDB channel mapping written for page_id={page_id}, tenant_id={tenant_id} | "
        f"welcome_surfaces={welcome_summary}"
    )

    # --- Instagram channel row (Page-linked IG Professional account) ---
    # IG DM webhooks arrive with entry.id = the IG ACCOUNT id, and the webhook
    # resolves PAGE#<igAccountId> / CHANNEL#instagram. IG Send API calls use
    # the SAME Page access token, so the row reuses the encrypted token.
    # Without this row IG DMs pass HMAC then die at tenant resolution
    # (found live 2026-07-12).
    try:
        ig_response = _graph_get(
            f"/{page_id}",
            {"fields": "instagram_business_account", "access_token": page_access_token},
        )
        ig_account_id = (ig_response.get("instagram_business_account") or {}).get("id")
        if ig_account_id:
            ig_item = dict(item)
            ig_item.update(
                {
                    "PK": f"PAGE#{ig_account_id}",
                    "SK": "CHANNEL#instagram",
                    "channelType": "instagram",
                    "igAccountId": ig_account_id,
                }
            )
            _channel_table().put_item(Item=ig_item)
            ig_welcome_summary = push_welcome_surfaces(page_access_token, tenant_id)
            print(
                f"[INFO] Instagram channel mapping written for ig_account_id={ig_account_id}, "
                f"tenant_id={tenant_id} | welcome_surfaces={ig_welcome_summary}"
            )
        else:
            print(f"[INFO] No Instagram Professional account linked to page_id={page_id} — messenger only")
    except Exception as exc:
        # Non-fatal: the Messenger channel is saved; IG connects on a re-run.
        print(f"[WARN] Instagram channel mapping failed for page_id={page_id}: {exc}")

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


def _begin_page_selection(tenant_id: str, pages: list) -> dict:
    """
    The account granted more than one Page. Persist each granted Page's access
    token (KMS-encrypted) under a single-use, short-TTL selection nonce and
    render an in-popup picker; the admin's choice completes via
    POST /meta/oauth/select-page.

    The transient record deliberately stores the tenant under `pendingTenantId`
    (NOT `tenantId`) so it stays out of the TenantIndex GSI and never surfaces
    as a live channel to list/disconnect/toggle.

    SECURITY: Page tokens live only KMS-encrypted at rest; only Page ids/names
    (non-secret) are ever sent to the browser.
    """
    usable = [p for p in pages if p.get("id") and p.get("access_token")]
    if not usable:
        return _html_error_popup(
            "No connectable Pages were returned — please re-authorise"
        )
    # Only one Page actually carried a usable token → nothing to choose.
    if len(usable) == 1:
        return _connect_page(tenant_id, usable[0])

    stored_pages = []
    for p in usable:
        try:
            enc = _encrypt_token(p["access_token"])
        except Exception as exc:
            print(f"[ERROR] KMS encryption failed during page selection for tenant_id={tenant_id}: {exc}")
            return _html_error_popup("Failed to secure the Page access tokens")
        stored_pages.append({"id": p["id"], "name": p.get("name", p["id"]), "encToken": enc})

    nonce = secrets.token_urlsafe(24)
    item = {
        "PK": f"SELECT#{nonce}",
        "SK": "OAUTH_SELECTION",
        "pendingTenantId": tenant_id,
        "pages": stored_pages,
        "ttl": int(time.time()) + _SELECTION_TTL_SECONDS,
    }
    try:
        _channel_table().put_item(Item=item)
    except Exception as exc:
        print(f"[ERROR] Failed to persist page selection for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to start Page selection — please try again")

    print(
        f"[INFO] Multiple Pages granted for tenant_id={tenant_id}; "
        f"rendering picker ({len(stored_pages)} pages)"
    )
    return _html_page_picker(
        nonce, [{"id": p["id"], "name": p["name"]} for p in stored_pages]
    )


def _parse_form_body(event: dict) -> dict:
    """Parse a urlencoded (or JSON) POST body into a flat {key: str} dict."""
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode()
        except Exception:
            raw = ""
    raw = raw.strip()
    if not raw:
        return {}
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
            return {k: str(v) for k, v in data.items()}
        except json.JSONDecodeError:
            return {}
    parsed = urllib.parse.parse_qs(raw, keep_blank_values=True)
    return {k: v[0] for k, v in parsed.items()}


def _handle_select_page(event: dict) -> dict:
    """
    POST /meta/oauth/select-page   (form body: nonce, page_id)

    Completes a multi-Page connection: looks up the single-use selection nonce,
    connects the chosen Page, and deletes the transient record. Public at the
    URL layer like the OAuth callback — protected instead by the unguessable,
    single-use, short-TTL nonce (never a Clerk/internal JWT, which the popup
    form submit does not carry).
    """
    form = _parse_form_body(event)
    nonce = (form.get("nonce") or "").strip()
    page_id = (form.get("page_id") or "").strip()
    if not nonce or not page_id:
        return _html_error_popup("Missing Page selection — please try connecting again")

    # Atomically CLAIM the nonce: conditional delete returns the record only to
    # the single caller that wins, so a double-submit can't connect twice. The
    # delete IS the claim (no separate get) — no replay window.
    key = {"PK": f"SELECT#{nonce}", "SK": "OAUTH_SELECTION"}
    try:
        record = _channel_table().delete_item(
            Key=key,
            ConditionExpression="attribute_exists(PK)",
            ReturnValues="ALL_OLD",
        ).get("Attributes")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            # Already claimed by a concurrent submit, or never existed.
            return _html_error_popup("Your Page selection expired — please connect again")
        print(f"[ERROR] Failed to claim page-selection nonce: {exc}")
        return _html_error_popup("Could not complete Page selection — please try again")

    # DynamoDB TTL deletion lags, so enforce expiry in code too.
    if not record or int(record.get("ttl", 0)) < int(time.time()):
        return _html_error_popup("Your Page selection expired — please connect again")

    tenant_id = record.get("pendingTenantId", "")
    chosen = next((p for p in record.get("pages", []) if p.get("id") == page_id), None)
    if not tenant_id or not chosen:
        return _html_error_popup("That Page is no longer available — please connect again")

    try:
        page_access_token = _decrypt_token(chosen["encToken"])
    except Exception as exc:
        print(f"[ERROR] KMS decryption failed during page selection for tenant_id={tenant_id}: {exc}")
        return _html_error_popup("Failed to read the Page access token — please re-authorise")

    return _connect_page(
        tenant_id,
        {"id": chosen["id"], "name": chosen.get("name", chosen["id"]), "access_token": page_access_token},
    )


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


def _handle_repush_welcome(tenant_id: str, channel: str = "messenger") -> dict:
    """
    POST /meta/channels/{tenant_id}/repush-welcome

    Re-push the tenant's `messenger_behavior.welcome` surfaces (ice breakers +
    persistent menu) to the Messenger Profile API using the stored Page token —
    the exact code path the OAuth callback and `scripts/repush_welcome_surfaces.py`
    use. The Config Builder calls this after a deploy so welcome-surface edits
    reach the live profile without a manual script run.

    Best-effort by design: `push_welcome_surfaces()` never raises and returns a
    summary (`{"pushed": …}` / `{"skipped": …}` / `{"error": …}`), so the caller
    can treat a non-2xx as "not connected / no token" and a 200 body's `result`
    as the push outcome.
    """
    if not tenant_id:
        return _error_response(400, "tenant_id path parameter is required")

    print(f"[INFO] Re-pushing welcome surfaces for tenant_id={tenant_id} channel={channel}")

    try:
        channels = _query_channels_by_tenant(tenant_id)
    except Exception as exc:
        print(f"[ERROR] DynamoDB query failed for tenant_id={tenant_id}: {exc}")
        return _error_response(500, "Failed to resolve channel mapping")

    matches = [c for c in channels if c.get("channelType") == channel]
    if not matches:
        return _error_response(404, f"No {channel} channel connected for this tenant")

    encrypted_token = matches[0].get("encryptedPageToken", "")
    if not encrypted_token:
        return _error_response(400, "Connected channel has no stored Page token")

    try:
        page_token = _decrypt_token(encrypted_token)
    except Exception as exc:
        print(f"[ERROR] Token decrypt failed for tenant_id={tenant_id}: {exc}")
        return _error_response(500, "Failed to decrypt Page token")

    summary = push_welcome_surfaces(page_token, tenant_id)
    return _json_response(200, {"tenant_id": tenant_id, "channel": channel, "result": summary})


# ---------------------------------------------------------------------------
# HTML popup helpers
# ---------------------------------------------------------------------------


def _js_string(value: str) -> str:
    """
    Return a safe, fully-quoted JS string literal for embedding inside an inline
    <script>. json.dumps handles quotes/backslashes/control chars; the extra
    escapes stop a `</script>` (or `<!--`) in the value from breaking out of the
    script element — the HTML tokenizer scans for those byte sequences regardless
    of JS-string quoting. Page names / error text are attacker-controllable
    (a Facebook Page's display name), so this is load-bearing, not cosmetic.
    """
    return (
        json.dumps(value)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def _html_success_popup(page_id: str, page_name: str) -> dict:
    """Return an HTML response that posts META_OAUTH_SUCCESS to the opener popup."""
    # _js_string returns the quoted literal — no surrounding quotes in the f-string.
    js_page_id = _js_string(page_id)
    js_page_name = _js_string(page_name)
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Connecting...</title></head>
<body>
<p>Connected successfully. This window will close automatically.</p>
<script>
  try {{
    window.opener.postMessage(
      {{ type: 'META_OAUTH_SUCCESS', payload: {{ pageId: {js_page_id}, pageName: {js_page_name} }} }},
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
    # HTML context for the visible line; JS-string context for the postMessage.
    # error_message can embed an attacker-controlled Page name — escape both.
    safe_message_html = html.escape(error_message)
    js_message = _js_string(error_message)
    body = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Connection Error</title></head>
<body>
<p>An error occurred while connecting your Facebook Page.</p>
<p><strong>{safe_message_html}</strong></p>
<script>
  try {{
    window.opener.postMessage(
      {{ type: 'META_OAUTH_ERROR', error: {js_message} }},
      '*'
    );
  }} catch (e) {{
    console.error('Failed to post message to opener:', e);
  }}
  window.close();
</script>
</body>
</html>"""
    return _html_response(400, body)


def _select_page_action_url() -> str:
    """Derive the select-page POST URL from the configured callback URL."""
    suffix = "/meta/oauth/callback"
    if _OAUTH_CALLBACK_URL.endswith(suffix):
        return _OAUTH_CALLBACK_URL[: -len(suffix)] + "/meta/oauth/select-page"
    # Fallback: relative path — resolves to the same origin as the popup.
    return "/meta/oauth/select-page"


def _html_page_picker(nonce: str, pages: list) -> dict:
    """
    Render the multi-Page chooser shown when an account granted more than one
    Page. Submitting posts { nonce, page_id } to POST /meta/oauth/select-page,
    which completes the connection and returns the success popup.
    """
    action = html.escape(_select_page_action_url(), quote=True)
    safe_nonce = html.escape(nonce, quote=True)
    rows = []
    for i, p in enumerate(pages):
        pid = html.escape(str(p.get("id", "")), quote=True)
        pname = html.escape(str(p.get("name", p.get("id", ""))))
        checked = " checked" if i == 0 else ""
        rows.append(
            f'  <label class="row"><input type="radio" name="page_id" value="{pid}"{checked}>'
            f'<span class="name">{pname}</span></label>'
        )
    rows_html = "\n".join(rows)
    body = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Choose a Page</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #1e293b; margin: 0; padding: 24px; }}
  h1 {{ font-size: 18px; margin: 0 0 6px; }}
  p.sub {{ font-size: 13px; color: #64748b; margin: 0 0 18px; line-height: 1.4; }}
  .row {{ display: flex; align-items: center; gap: 10px; padding: 12px 14px; border: 1px solid #e2e8f0; border-radius: 10px; margin-bottom: 8px; cursor: pointer; }}
  .row:hover {{ border-color: #50C878; }}
  .name {{ font-size: 14px; font-weight: 600; }}
  button {{ margin-top: 12px; width: 100%; font-size: 14px; font-weight: 600; color: #fff; background: #50C878; border: none; border-radius: 999px; padding: 12px; cursor: pointer; }}
  button:hover {{ background: #059669; }}
</style>
</head>
<body>
<h1>Choose the Page to connect</h1>
<p class="sub">Your Facebook account manages more than one Page. Picasso connects one Page &mdash; pick the Page whose Messenger and Instagram messages should route here.</p>
<form method="POST" action="{action}">
  <input type="hidden" name="nonce" value="{safe_nonce}">
{rows_html}
  <button type="submit">Connect this Page</button>
</form>
</body>
</html>"""
    return _html_response(200, body)


# ---------------------------------------------------------------------------
# Caller authorization (management routes)
# ---------------------------------------------------------------------------


def _bearer_token(event: dict) -> Optional[str]:
    """Extract the bearer token from the Authorization header (case-insensitive)."""
    headers = event.get("headers") or {}
    auth = headers.get("authorization") or headers.get("Authorization") or ""
    if auth[:7].lower() == "bearer ":
        auth = auth[7:]
    return auth.strip() or None


def _get_jwt_signing_secret() -> str:
    """Fetch + cache the HS256 signing secret for the internal Picasso JWT."""
    global _jwt_signing_secret
    if _jwt_signing_secret:
        return _jwt_signing_secret
    response = _secrets_client.get_secret_value(SecretId=_JWT_SECRET_KEY_NAME)
    secret = response.get("SecretString", "")
    # Support JSON-wrapped secrets (mirror Analytics_Dashboard_API).
    try:
        data = json.loads(secret)
        secret = data.get("signingKey") or data.get("key") or data.get("secret") or secret
    except (json.JSONDecodeError, TypeError):
        pass
    _jwt_signing_secret = secret
    return secret


def _verify_internal_jwt(token: str) -> Optional[dict]:
    """
    Verify an internal Picasso JWT (HS256) and return its claims, or None.

    Port of Analytics_Dashboard_API.validate_jwt — hand-rolled HMAC-SHA256, no
    third-party dependency. Never raises: any malformed/failed token → None.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header_b64, payload_b64, signature_b64 = parts
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=="))
        exp = payload.get("exp")
        if exp and time.time() > exp:
            return None
        secret = _get_jwt_signing_secret()
        message = f"{header_b64}.{payload_b64}".encode("utf-8")
        expected = (
            base64.urlsafe_b64encode(
                hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
            )
            .rstrip(b"=")
            .decode("utf-8")
        )
        if not hmac.compare_digest(signature_b64.rstrip("="), expected):
            return None
        return payload
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[WARN] internal JWT verification error: {exc}")
        return None


def _fetch_config_clerk_jwks() -> dict:
    """Fetch + cache the Config Builder Clerk JWKS."""
    global _clerk_jwks_cache, _clerk_jwks_cache_time
    now = time.time()
    if _clerk_jwks_cache and (now - _clerk_jwks_cache_time) < _CLERK_JWKS_CACHE_TTL:
        return _clerk_jwks_cache
    req = urllib.request.Request(
        _CLERK_CONFIG_JWKS_URL, headers={"Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # nosec B310 (https literal)
        data = json.loads(resp.read().decode("utf-8"))
    _clerk_jwks_cache = data
    _clerk_jwks_cache_time = now
    return data


def _verify_operator_clerk_jwt(token: str) -> bool:
    """
    Verify a Config Builder Clerk 'picasso-config' JWT (RS256) via JWKS.

    Mirrors Analytics_Dashboard_API._decode_clerk_jwt_claims (PyJWT + the
    cryptography layer). Returns True on a valid, unexpired operator token.
    Never raises.
    """
    try:
        kid = jwt.get_unverified_header(token).get("kid")
        if not kid:
            return False
        signing_key = None
        for key_data in _fetch_config_clerk_jwks().get("keys", []):
            if key_data.get("kid") == kid:
                from jwt.algorithms import RSAAlgorithm

                signing_key = RSAAlgorithm.from_jwk(key_data)
                break
        if signing_key is None:
            return False
        jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            options={"require": ["exp"], "verify_exp": True, "verify_aud": False},
        )
        return True
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[WARN] operator Clerk JWT verification failed: {exc}")
        return False


def _authorize_channel_request(event: dict, tenant_id: str) -> Optional[dict]:
    """
    Authorize a caller for a /meta/channels/{tenant_id}/* management route.

    Returns None when authorized, or an `_error_response(...)` to return verbatim.

    Order:
      * Direct server-to-server invoke (no Function URL requestContext) → allow
        (already IAM-authenticated; e.g. the welcome-repush backstop).
      * Internal Picasso JWT whose tenant_id == path tenant_id → allow (tenant).
      * Internal Picasso JWT with a DIFFERENT tenant → 403 (IDOR block).
      * Valid operator Clerk JWT → allow (super-admin, any tenant).
      * Otherwise → 401.
    """
    if not event.get("requestContext"):
        return None  # internal server-to-server invoke (IAM-authenticated)

    token = _bearer_token(event)
    if not token:
        return _error_response(401, "Missing or invalid Authorization header")

    claims = _verify_internal_jwt(token)
    if claims is not None:
        token_tenant = claims.get("tenant_id") or claims.get("sub")
        if token_tenant == tenant_id:
            return None
        print(f"[WARN] tenant mismatch: token tenant != path tenant ({tenant_id})")
        return _error_response(403, "Not authorized for this tenant")

    if _verify_operator_clerk_jwt(token):
        return None

    return _error_response(401, "Unauthorized")


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

    # --- POST /meta/oauth/select-page (nonce-gated, like the callback) ---
    if http_method == "POST" and path.endswith("/meta/oauth/select-page"):
        return _handle_select_page(event)

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

        # Every /meta/channels/{tenant_id}/* route reads or mutates a tenant's
        # channel state — gate the caller before dispatching.
        denied = _authorize_channel_request(event, tenant_id)
        if denied is not None:
            return denied

        if http_method == "POST" and action == "disconnect":
            return _handle_disconnect_channel(tenant_id)

        if http_method == "POST" and action == "toggle":
            raw_body = event.get("body") or "{}"
            try:
                body = json.loads(raw_body)
            except json.JSONDecodeError:
                return _error_response(400, "Request body is not valid JSON")
            return _handle_toggle_channel(tenant_id, body)

        if http_method == "POST" and action == "repush-welcome":
            return _handle_repush_welcome(tenant_id)

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
