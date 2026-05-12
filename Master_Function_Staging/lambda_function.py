import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import parse_qs

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Track JWT signing key retrieval warnings (only log once per Lambda instance)
_jwt_key_warning_logged = False

def get_jwt_signing_key() -> Optional[str]:
    """
    Get JWT signing key from Secrets Manager, falling back to env var.
    SECURITY: Never use hardcoded default keys - fail explicitly if no key available.
    """
    global _jwt_key_warning_logged

    # Try Secrets Manager first (preferred)
    try:
        import boto3
        secrets_client = boto3.client('secretsmanager', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        secret_name = os.environ.get('JWT_SECRET_KEY_NAME', 'picasso/jwt/signing-key')
        response = secrets_client.get_secret_value(SecretId=secret_name)

        # Handle both JSON and plain string formats
        secret_string = response.get('SecretString', '')
        try:
            secret_data = json.loads(secret_string)
            return secret_data.get('signingKey', secret_string)
        except json.JSONDecodeError:
            return secret_string

    except Exception as e:
        if not _jwt_key_warning_logged:
            logger.critical(f"SECURITY WARNING: Failed to retrieve JWT signing key from Secrets Manager: {e}")
            _jwt_key_warning_logged = True

        # Fall back to environment variable (NOT a hardcoded default)
        env_key = os.environ.get('JWT_SECRET')
        if env_key:
            if not _jwt_key_warning_logged:
                logger.warning("Using JWT_SECRET environment variable as fallback (Secrets Manager unavailable)")
            return env_key

        # No key available - log critical error
        logger.critical("SECURITY CRITICAL: No JWT signing key available - authentication will fail")
        return None


# Lambda Function URLs are public AWS endpoints with no built-in WAF binding.
# Without an out-of-band auth signal, an attacker who learns the Function URL
# can bypass CloudFront (and any WAF rules attached there) entirely. This
# header is the out-of-band signal: CloudFront injects it on every origin
# request via the distribution's "Custom Headers" feature; the Function URL
# refuses any request that lacks (or mismatches) the secret.
#
# Rollout is feature-flagged via REQUIRE_CF_ORIGIN_HEADER. Default is off so
# this PR can ship and deploy without coupling to the CF config change. See
# the PR body for the activation runbook (provision secret → configure CF →
# flip flag).
_CF_ORIGIN_HEADER_NAME = 'x-picasso-cf-origin'
# Sentinel distinct from None so the cache can record a known-failed lookup
# and avoid hammering Secrets Manager on every invocation during an outage.
_CF_ORIGIN_SECRET_UNAVAILABLE = object()
_cf_origin_secret_cache: Any = None


def get_cf_origin_secret() -> Optional[str]:
    """Read the CF origin secret from Secrets Manager. Cached per Lambda instance.

    Returns the secret string, or None if unavailable. Caller decides how to
    handle unavailability — under REQUIRE_CF_ORIGIN_HEADER='true' we fail
    closed (treat as missing-header → 403).

    Caching: both success AND failure are cached for the lifetime of the
    Lambda instance. Caching failure prevents O(RPS) Secrets Manager calls
    during a SM outage when the feature flag is on (every request would
    otherwise re-attempt the lookup). To force a refresh after rotation,
    publish a new Lambda version.
    """
    global _cf_origin_secret_cache
    if _cf_origin_secret_cache is _CF_ORIGIN_SECRET_UNAVAILABLE:
        return None
    if _cf_origin_secret_cache is not None:
        return _cf_origin_secret_cache

    try:
        import boto3
        client = boto3.client('secretsmanager', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        secret_name = os.environ.get('CF_ORIGIN_SECRET_NAME', 'picasso/mfs/cf-origin-secret')
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get('SecretString', '')
        try:
            data = json.loads(secret_string)
            candidate = data.get('secret', secret_string)
        except json.JSONDecodeError:
            candidate = secret_string

        # Empty or whitespace-only secret would compare equal to a same-shaped
        # header value — explicit fail-closed. `not candidate` catches "", but
        # whitespace is truthy in Python ("   " evaluates True), so the strip
        # check is needed to catch misconfigured secrets like "  \n".
        if not candidate or not str(candidate).strip():
            logger.error("SECURITY: CF origin secret is empty or whitespace-only; treating as unavailable")
            _cf_origin_secret_cache = _CF_ORIGIN_SECRET_UNAVAILABLE
            return None

        _cf_origin_secret_cache = candidate
        return _cf_origin_secret_cache
    except Exception as e:
        logger.error(f"SECURITY: failed to retrieve CF origin secret: {e}")
        _cf_origin_secret_cache = _CF_ORIGIN_SECRET_UNAVAILABLE
        return None


def validate_cf_origin_header(event: Dict[str, Any]) -> tuple:
    """Validate the CloudFront-injected origin secret header on the request.

    Returns (is_valid, reason). When REQUIRE_CF_ORIGIN_HEADER is unset or
    'false', validation is skipped and returns (True, None) regardless of
    header state — this is the default during rollout.

    When enabled, the request must carry the header with a value that
    matches the Secrets Manager secret via constant-time compare. Missing,
    mismatched, or unavailable-secret all fail-closed (return False).
    """
    if os.environ.get('REQUIRE_CF_ORIGIN_HEADER', 'false').lower() != 'true':
        return True, None

    headers = event.get('headers') or {}
    received: Optional[str] = None
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == _CF_ORIGIN_HEADER_NAME:
            received = value
            break

    if not received:
        return False, "missing CF origin header"

    expected = get_cf_origin_secret()
    if not expected:
        return False, "CF origin secret unavailable (failing closed)"

    import hmac
    if not hmac.compare_digest(str(received), str(expected)):
        return False, "CF origin header mismatch"

    return True, None


def sanitize_user_input(user_input: str, max_length: int = 4000) -> str:
    """
    Sanitize and validate user input before sending to Bedrock.
    Prevents prompt injection attacks and handles excessive length.
    """
    if not user_input or not isinstance(user_input, str):
        return "Hello"

    # Truncate to prevent token limit issues
    sanitized = user_input[:max_length]

    # Log suspiciously long inputs
    if len(user_input) > max_length:
        logger.warning(f"SECURITY: Truncated excessively long input: {len(user_input)} chars")

    # Detect potential prompt injection patterns
    injection_patterns = [
        "ignore previous", "ignore all", "system:", "admin mode",
        "dev mode", "override instructions", "jailbreak", "disregard"
    ]
    lower_input = sanitized.lower()
    for pattern in injection_patterns:
        if pattern in lower_input:
            logger.warning(f"SECURITY: Potential prompt injection detected: {pattern}")

    return sanitized.strip()

# Single source of truth for tenant-agnostic CORS allowlist. Used by both
# add_cors_headers() and validate_cors_origin(). When a tenant config is
# available, it can extend this list via `cors.allowed_origins`.
_CORS_ALLOWED_ORIGINS_DEFAULT = [
    'http://localhost:8000',
    'http://localhost:5173',
    'http://localhost:3000',
    'https://chat.myrecruiter.ai',
    'https://staging.chat.myrecruiter.ai',  # Added 2026-05-02 — staging widget origin was missing
    'https://picassocode.s3.amazonaws.com',
    'https://picassostaging.s3.amazonaws.com',
]


def validate_cors_origin(request_headers, tenant_hash, config_data):
    """
    Validate the request's Origin header against the allowlist.

    Restored 2026-05-02: response_formatter.py imports this name from
    lambda_function but it had been undefined, causing an ImportError on every
    error-response code path and dropping CORS headers. Implementation mirrors
    add_cors_headers() origin logic for consistency.

    Args:
        request_headers: dict of HTTP request headers (case-insensitive lookup).
        tenant_hash: tenant identifier (currently unused for the allowlist
            decision but kept in the signature for future per-tenant overrides).
        config_data: tenant config dict (or None). When present, may carry a
            `cors.allowed_origins` list to extend the default allowlist.

    Returns:
        (allowed_origin, is_valid):
            - origin in allowlist → (origin_string, True)
            - localhost (any port) → (origin_string, True)
            - no Origin header (server-to-server) → (None, True)
            - origin present but rejected → (None, False)
    """
    # Case-insensitive Origin header lookup.
    origin = None
    if request_headers:
        for key in ('origin', 'Origin', 'ORIGIN'):
            if key in request_headers and request_headers[key]:
                origin = request_headers[key]
                break

    # No Origin header — typical for server-to-server or non-browser clients.
    # Not a CORS violation; caller can omit the Access-Control-Allow-Origin header.
    if not origin:
        return (None, True)

    # Build the allowlist: defaults + any tenant-config extensions.
    allowed_origins = list(_CORS_ALLOWED_ORIGINS_DEFAULT)
    if isinstance(config_data, dict):
        tenant_extras = (config_data.get('cors') or {}).get('allowed_origins')
        if isinstance(tenant_extras, list):
            allowed_origins.extend(o for o in tenant_extras if isinstance(o, str))

    # Localhost matches any port (developer machines).
    if origin.startswith('http://localhost:') or origin.startswith('https://localhost:'):
        return (origin, True)

    if origin in allowed_origins:
        return (origin, True)

    # Origin present but not allowed — explicit CORS violation.
    return (None, False)


def add_cors_headers(response: Dict[str, Any], event: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Add CORS headers to response. This is the ONLY place CORS headers are added.

    Phase-audit B6 (2026-05-11): default no longer falls back to '*'. When the
    request has no Origin header (direct invocation, test harness, or missing
    event), we use the first entry from the allowlist — never wildcard. The
    streaming-handler responses now route through this function instead of
    constructing their own headers (which previously hardcoded '*').
    """
    if 'headers' not in response:
        response['headers'] = {}

    # Safe default — never wildcard. If no Origin header is present, fall back
    # to the canonical chat origin. Browsers will refuse the actual cross-origin
    # request if their origin doesn't match, which is the desired CORS posture.
    allowed_origin = _CORS_ALLOWED_ORIGINS_DEFAULT[0]

    # Check for origin header in various formats (Lambda can provide headers in different cases)
    if event and 'headers' in event:
        headers = event.get('headers', {})
        origin = None

        # Try different header key variations
        for key in ['origin', 'Origin', 'ORIGIN']:
            if key in headers:
                origin = headers[key]
                break

        if origin:
            # Use the shared allowlist defined above (single source of truth with
            # validate_cors_origin). Includes staging.chat.myrecruiter.ai as of
            # 2026-05-02 — was missing previously, causing staging widget origins
            # to fall back to the production default.
            allowed_origins = _CORS_ALLOWED_ORIGINS_DEFAULT

            # Allow any localhost origin or specific trusted origins
            if any(origin.startswith(allowed) for allowed in ['http://localhost:', 'https://localhost:']) or origin in allowed_origins:
                allowed_origin = origin
                logger.info(f"CORS: Allowing specific origin {origin}")
            else:
                logger.warning(f"CORS: Origin {origin} not in allowed list, using default origin (not wildcard)")
                # allowed_origin already set to allowed_origins[0] above; explicit reassign for clarity
                allowed_origin = allowed_origins[0] if allowed_origins else 'https://chat.myrecruiter.ai'

    # Add CORS headers - this is the single source of truth
    response['headers']['Access-Control-Allow-Origin'] = allowed_origin
    response['headers']['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, DELETE'
    response['headers']['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
    # Always set credentials true — we never use wildcard now, so this is safe.
    response['headers']['Access-Control-Allow-Credentials'] = 'true'
    
    # Set content type if not already set
    if 'Content-Type' not in response['headers']:
        response['headers']['Content-Type'] = 'application/json'
    
    return response

def handle_options(event: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Handle OPTIONS requests for CORS preflight.
    Routes through add_cors_headers() so the allowlist is applied.
    """
    logger.info("OPTIONS request received for CORS preflight")

    response = {
        'statusCode': 200,
        'body': '',
    }
    return add_cors_headers(response, event)

def health_check(event: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Simple health check endpoint
    """
    logger.info("Health check requested")
    
    response = {
        'statusCode': 200,
        'body': json.dumps({
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'function': 'Master_Function_Staging'
        })
    }
    
    return add_cors_headers(response, event)

def get_config_for_tenant(tenant_hash: str, event: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Return configuration for a specific tenant
    """
    logger.info(f"Config requested for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    
    if not tenant_hash:
        response = {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Bad Request',
                'message': 'Tenant hash is required'
            })
        }
        return add_cors_headers(response, event)
    
    # Try to load real config using tenant_config_loader if available
    try:
        from tenant_config_loader import get_config_for_tenant_by_hash
        logger.info(f"Loading real config for tenant: {tenant_hash[:8]}...")
        
        # Call the real config loader
        config_data = get_config_for_tenant_by_hash(tenant_hash)
        
        if config_data:
            logger.info(f"Successfully loaded config for tenant: {tenant_hash[:8]}...")
            response = {
                'statusCode': 200,
                'body': json.dumps(config_data)
            }
        else:
            logger.warning(f"No config found for tenant: {tenant_hash[:8]}...")
            response = {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Not Found',
                    'message': f'Configuration not found for tenant {tenant_hash[:8]}...'
                })
            }
        
    except ImportError:
        logger.error("tenant_config_loader module unavailable — cannot serve tenant config")
        response = {
            'statusCode': 503,
            'body': json.dumps({
                'error': 'Service Unavailable',
                'message': 'Tenant configuration service is not available'
            })
        }
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Failed to load configuration'
            })
        }
    
    return add_cors_headers(response, event)

def handle_streaming_chat(event: Dict[str, Any], tenant_hash: str, request_id: str = None):
    """
    Entry point for ?action=chat&streaming=true. Delegates to the batch fallback
    because Master_Function_Staging is NOT deployed with InvokeMode=RESPONSE_STREAM
    (the standard Python 3.13 Lambda runtime does not export
    awslambdaric.StreamingBody). The yield-based streaming code path was removed
    in Phase 4.5b; restore it only if/when the Lambda is reconfigured for
    response streaming.
    """
    logger.info(f"Streaming chat request for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    return handle_streaming_chat_fallback(event, tenant_hash)

def handle_streaming_chat_fallback(event: Dict[str, Any], tenant_hash: str) -> Dict[str, Any]:
    """
    Fallback implementation that collects all chunks before returning
    Used when StreamingBody is not available
    NOW WITH KNOWLEDGE BASE INTEGRATION
    """
    logger.info(f"Using fallback streaming for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    
    try:
        # Check if this is a GET request (from EventSource) or POST
        http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method', 'GET'))
        
        if http_method == 'GET':
            # EventSource uses GET with query parameters
            query_params = event.get('queryStringParameters', {}) or {}
            user_input = sanitize_user_input(query_params.get('user_input', 'Hello'))
            session_id = query_params.get('session_id', 'default_session')
            conversation_context = None
        else:
            # POST request with body
            body = json.loads(event.get('body', '{}')) if event.get('body') else {}
            user_input = sanitize_user_input(body.get('user_input', 'Hello'))
            session_id = body.get('session_id', 'default_session')
            
            # Extract conversation context from POST body
            request_context = body.get('conversation_context', {})
            messages_from_context = request_context.get('recentMessages', request_context.get('messages', []))
            conversation_context = None
            if request_context and messages_from_context:
                conversation_context = {
                    'messages': messages_from_context,
                    'recentMessages': messages_from_context,
                    'session_id': body.get('session_id'),
                    'conversation_id': body.get('conversation_id'),
                    'turn': body.get('turn', 0)
                }
        
        # Load tenant configuration for Knowledge Base access
        config = None
        try:
            from tenant_config_loader import get_config_for_tenant_by_hash
            config = get_config_for_tenant_by_hash(tenant_hash)
            logger.info(f"[{tenant_hash[:8]}...] ✅ Config loaded for fallback streaming with KB")
        except Exception as e:
            logger.warning(f"[{tenant_hash[:8]}...] ⚠️ Could not load config: {e}")
        
        # Retrieve Knowledge Base chunks and build enhanced prompt
        enhanced_prompt = user_input  # Default to raw input if KB fails
        try:
            from bedrock_handler_optimized import retrieve_kb_chunks, build_prompt

            # Get tenant tone
            tone = config.get("tone_prompt", "You are a helpful assistant.") if config else "You are a helpful assistant."

            # Retrieve relevant chunks from Knowledge Base
            if config:
                kb_context, sources = retrieve_kb_chunks(user_input, config)
                logger.info(f"[{tenant_hash[:8]}...] 📚 Retrieved KB context for fallback streaming")
                
                # Build the enhanced prompt with KB context
                enhanced_prompt = build_prompt(user_input, kb_context, tone, conversation_context)
                logger.info(f"[{tenant_hash[:8]}...] 🧩 Built enhanced prompt with KB context")
            else:
                logger.warning(f"[{tenant_hash[:8]}...] ⚠️ No config - using direct prompt without KB")
                
        except Exception as e:
            logger.error(f"[{tenant_hash[:8]}...] ❌ KB retrieval failed for fallback streaming: {e}")
        
        # Initialize Bedrock client
        import boto3
        bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=os.environ.get('AWS_REGION', 'us-east-1')
        )
        
        # Model ID resolution: tenant config wins; fall back to Lambda default
        # (env var BEDROCK_MODEL_ID per Phase 4 EC-P4-2). KeyError fail-loud
        # if env var missing.
        model_id = (config or {}).get("model_id") or os.environ['BEDROCK_MODEL_ID']
        
        # Prepare the message for Claude with enhanced prompt
        messages = [
            {
                "role": "user",
                "content": enhanced_prompt  # Use the KB-enhanced prompt instead of raw input
            }
        ]
        
        # Bedrock request body for Claude 3 Haiku
        bedrock_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "messages": messages,
            "temperature": 0.7,
        }

        logger.info("Invoking Bedrock with streaming (fallback mode)...")
        
        # Call Bedrock with streaming using the model from config
        response = bedrock_client.invoke_model_with_response_stream(
            modelId=model_id,  # Use the model ID from config
            body=json.dumps(bedrock_body),
            contentType="application/json"
        )
        
        # Process the streaming response
        sse_chunks = []
        
        # Process the event stream
        for event in response['body']:
            if 'chunk' in event:
                chunk = event['chunk']
                if 'bytes' in chunk:
                    chunk_data = json.loads(chunk['bytes'].decode('utf-8'))
                    
                    if chunk_data.get('type') == 'content_block_delta':
                        # Extract the text content from the delta
                        delta = chunk_data.get('delta', {})
                        if delta.get('type') == 'text_delta':
                            text_content = delta.get('text', '')
                            if text_content:
                                # Format as SSE data
                                sse_data = json.dumps({
                                    "type": "text",
                                    "content": text_content,
                                    "session_id": session_id
                                })
                                sse_chunks.append(f'data: {sse_data}\n\n')
                    
                    elif chunk_data.get('type') == 'message_stop':
                        # End of message
                        logger.info("Bedrock streaming completed (fallback)")
                        break
        
        # Add the final [DONE] marker
        sse_chunks.append('data: [DONE]\n\n')
        
        # Combine all SSE chunks
        sse_body = ''.join(sse_chunks)
        
        # Create SSE response with proper headers. CORS comes from
        # add_cors_headers (single source of truth — phase-audit B6).
        response = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
            },
            'body': sse_body
        }
        response = add_cors_headers(response, event)

        logger.info(f"Fallback streaming response sent for tenant: {tenant_hash[:8]}... with {len(sse_chunks)-1} chunks")
        return response

    except Exception as e:
        logger.error(f"Error in fallback streaming chat: {str(e)}", exc_info=True)

        # Return error as SSE format
        error_data = json.dumps({
            "type": "error",
            "content": f"Streaming error: {str(e)}",
            "session_id": session_id if 'session_id' in locals() else 'unknown'
        })
        error_sse = f'data: {error_data}\n\ndata: [DONE]\n\n'

        response = {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
            },
            'body': error_sse
        }

        return add_cors_headers(response, event)

def get_conversation_branch(metadata: Dict[str, Any], tenant_config: Dict[str, Any]) -> Optional[str]:
    """
    Determine conversation branch using 3-tier hierarchy.

    This function implements the Action Chips Explicit Routing PRD (FR-5).
    It eliminates keyword matching in favor of explicit routing metadata.

    Tier 1: Explicit action chip routing (chip.target_branch)
    Tier 2: Explicit CTA routing (cta.target_branch)
    Tier 3: Fallback navigation hub (cta_settings.fallback_branch)

    Args:
        metadata: Request metadata from frontend containing routing information
        tenant_config: Full tenant configuration from S3

    Returns:
        str: Branch name to use for CTA selection
        None: No CTAs should be shown (backward compatibility)

    Examples:
        # Action chip clicked with valid target
        metadata = {"action_chip_triggered": True, "target_branch": "volunteer_interest"}
        → Returns "volunteer_interest"

        # Free-form query (no metadata)
        metadata = {}
        → Returns fallback_branch from cta_settings
    """
    branches = tenant_config.get('conversation_branches', {})
    cta_settings = tenant_config.get('cta_settings', {})

    # TIER 1: Explicit action chip routing
    if metadata.get('action_chip_triggered'):
        target_branch = metadata.get('target_branch')
        if target_branch and target_branch in branches:
            logger.info(f"[Tier 1] Routing via action chip to branch: {target_branch}")
            return target_branch
        if target_branch:
            logger.warning(f"[Tier 1] Invalid target_branch: {target_branch}, falling back to next tier")

    # TIER 2: Explicit CTA routing
    if metadata.get('cta_triggered'):
        target_branch = metadata.get('target_branch')
        if target_branch and target_branch in branches:
            logger.info(f"[Tier 2] Routing via CTA to branch: {target_branch}")
            return target_branch
        if target_branch:
            logger.warning(f"[Tier 2] Invalid target_branch: {target_branch}, falling back to next tier")

    # TIER 3: Fallback navigation hub
    fallback_branch = cta_settings.get('fallback_branch')
    if fallback_branch and fallback_branch in branches:
        logger.info(f"[Tier 3] Routing to fallback branch: {fallback_branch}")
        return fallback_branch

    # No routing match - graceful degradation (backward compatibility)
    if fallback_branch:
        logger.warning(f"[Tier 3] Fallback branch '{fallback_branch}' not found in conversation_branches")
    else:
        logger.warning("[Tier 3] No fallback_branch configured - no CTAs will be shown")

    return None

def build_ctas_for_branch(
    branch_name: str,
    tenant_config: Dict[str, Any],
    completed_forms: list = None
) -> list:
    """
    Build CTA cards for a specific conversation branch (no keyword matching).

    This function implements explicit CTA selection based on a pre-determined branch,
    bypassing the keyword detection logic in form_cta_enhancer.

    Args:
        branch_name: Name of the conversation branch to use
        tenant_config: Full tenant configuration from S3
        completed_forms: List of completed form IDs to filter out

    Returns:
        list: CTA cards to display (max 3)
    """
    completed_forms = completed_forms or []
    branches = tenant_config.get('conversation_branches', {})
    cta_definitions = tenant_config.get('cta_definitions', {})

    if not branch_name or branch_name not in branches:
        logger.warning(f"[CTA Builder] Branch '{branch_name}' not found")
        return []

    branch = branches[branch_name]
    available_ctas = branch.get('available_ctas', {})

    ctas = []

    # Add primary CTA if defined
    primary_cta_id = available_ctas.get('primary')
    if primary_cta_id and primary_cta_id in cta_definitions:
        primary_cta = cta_definitions[primary_cta_id]

        # Check if this is a form CTA
        is_form_cta = (
            primary_cta.get('action') in ['start_form', 'form_trigger'] or
            primary_cta.get('type') == 'form_cta'
        )

        if is_form_cta:
            # Extract program from CTA
            program = primary_cta.get('program') or primary_cta.get('program_id')
            form_id = primary_cta.get('formId') or primary_cta.get('form_id')

            # Map formIds to programs if needed
            if not program and form_id:
                if form_id == 'lb_apply':
                    program = 'lovebox'
                elif form_id == 'dd_apply':
                    program = 'daretodream'

            # Filter if completed
            if program and program in completed_forms:
                logger.info(f"[CTA Builder] Filtering completed program: {program}")
            else:
                ctas.append({**primary_cta, 'id': primary_cta_id})
        else:
            # Not a form CTA, always show
            ctas.append({**primary_cta, 'id': primary_cta_id})

    # Add secondary CTAs if defined
    secondary_ctas = available_ctas.get('secondary', [])
    for cta_id in secondary_ctas:
        if cta_id not in cta_definitions:
            continue

        cta = cta_definitions[cta_id]

        # Check if this is a form CTA
        is_form_cta = (
            cta.get('action') in ['start_form', 'form_trigger'] or
            cta.get('type') == 'form_cta'
        )

        if is_form_cta:
            # Extract program from CTA
            program = cta.get('program') or cta.get('program_id')
            form_id = cta.get('formId') or cta.get('form_id')

            # Map formIds to programs if needed
            if not program and form_id:
                if form_id == 'lb_apply':
                    program = 'lovebox'
                elif form_id == 'dd_apply':
                    program = 'daretodream'

            # Filter if completed
            if program and program in completed_forms:
                logger.info(f"[CTA Builder] Filtering completed program: {program}")
                continue

        ctas.append({**cta, 'id': cta_id})

    # Return max 3 CTAs
    final_ctas = ctas[:3]
    logger.info(f"[CTA Builder] Built {len(final_ctas)} CTAs for branch '{branch_name}'")
    return final_ctas

def handle_chat(event: Dict[str, Any], tenant_hash: str, request_id: str = None) -> Dict[str, Any]:
    """
    Handle chat messages using real intent router with conversation memory support.

    ``request_id`` is ``context.aws_request_id`` from lambda_handler, threaded
    through for analytics_writer's per-event idempotency key. Defaults to None
    for backward compatibility with older call sites; analytics writes are
    skipped when None (writer rejects with `request_id_missing`).
    """
    logger.info(f"Chat request for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")

    # Check for streaming parameter
    query_params = event.get('queryStringParameters', {}) or {}
    streaming_enabled = query_params.get('streaming', '').lower() == 'true'

    if streaming_enabled:
        logger.info("Streaming mode detected - returning SSE response")
        return handle_streaming_chat(event, tenant_hash, request_id)
    
    try:
        # Try to use the real intent router
        from intent_router import route_intent

        # Parse request body
        body = json.loads(event.get('body', '{}')) if event.get('body') else {}

        # Forms-fallback wire-up: widget HTTP path POSTs form submissions to
        # ?action=chat with body {form_mode: true, action: 'submit_form', ...}
        # when BSH streaming is unavailable. Route those to the existing form
        # submission handler so MFS can serve forms as fallback.
        #
        # Widget body uses form_id/form_data; MFS FormHandler reads
        # form_type/responses. Translate widget shape → FormHandler shape so
        # widget-originated submissions process correctly. Preserve any
        # FormHandler-shape fields already present (don't overwrite).
        if body.get('form_mode') is True:
            if 'form_id' in body and 'form_type' not in body:
                body['form_type'] = body['form_id']
            if 'form_data' in body and 'responses' not in body:
                body['responses'] = body['form_data'] or {}
            event = {**event, 'body': json.dumps(body)}
            logger.info("form_mode=True in chat body — routing to handle_form_submission")
            return handle_form_submission(event, tenant_hash, request_id)

        # Extract Authorization header for state token (conversation memory)
        headers = event.get('headers', {})
        auth_header = headers.get('Authorization', headers.get('authorization', ''))
        state_token = None
        conversation_context = None
        
        if auth_header and auth_header.startswith('Bearer '):
            state_token = auth_header.replace('Bearer ', '').strip()
            logger.info("State token found in Authorization header")
            
            # Try to decode as JWT first, then fall back to base64
            try:
                import jwt
                jwt_signing_key = get_jwt_signing_key()

                if not jwt_signing_key:
                    raise ValueError("No JWT signing key available")

                # Decode JWT token
                token_data = jwt.decode(state_token, jwt_signing_key, algorithms=['HS256'])
                
                # Extract conversation context from token AND request body
                request_context = body.get('conversation_context', {})
                messages = request_context.get('recentMessages', request_context.get('messages', []))
                conversation_context = {
                    'session_id': token_data.get('sessionId'),  # Use camelCase field
                    'turn': token_data.get('turn', 0),
                    'conversation_id': body.get('conversation_id'),
                    'messages': messages,
                    'recentMessages': messages,
                    'previous_messages': messages
                }
                logger.info(f"JWT token decoded: turn {conversation_context['turn']}, {len(messages)} messages")
                
            except Exception as jwt_error:
                # SECURITY FIX: Removed base64 fallback — unsigned tokens bypass auth and blacklist
                logger.error(f"JWT decode failed, rejecting token: {jwt_error}")
                logger.warning("Base64 token fallback has been removed for security — only signed JWTs are accepted")
        else:
            logger.info("No Authorization header found - starting new conversation")
        
        # Prepare the event for intent router - it expects Lambda event structure
        chat_event = {
            'queryStringParameters': {
                't': tenant_hash
            },
            'headers': headers,  # Pass headers through
            'body': json.dumps({
                'tenant_hash': tenant_hash,
                'user_input': body.get('user_input', ''),
                'session_id': body.get('session_id', ''),
                'context': body.get('context', {}),
                'metadata': body.get('metadata', {}),
                'conversation_id': body.get('conversation_id'),
                'turn': body.get('turn', 0),
                'state_token': state_token,
                'conversation_context': body.get('conversation_context', {})
            })
        }
        
        logger.info(f"Routing chat to intent handler for tenant: {tenant_hash[:8]}...")
        
        # Track timing for metrics
        start_time = datetime.utcnow()
        
        # Call the real intent router with conversation context
        logger.info(f"Calling route_intent with conversation_context: {conversation_context is not None}")
        response_data = route_intent(chat_event, conversation_context=conversation_context)

        # Phase 1B: Extract session_context from request body for CTA enhancement
        session_context = body.get('session_context', {})
        logger.info(f"Session context extracted: completed_forms={session_context.get('completed_forms', [])}")

        # ACTION CHIPS EXPLICIT ROUTING (PRD FR-3, FR-5)
        # Extract metadata from request for 3-tier routing hierarchy.
        # Wire-shape parity with BSH: widget sends body.routing_metadata
        # (both HTTPChatProvider and StreamingChatProvider). Fall back to
        # body.metadata for backward compatibility with older callers and
        # synthetic tests. Key presence — not truthiness — gates the fallback,
        # so an explicit empty routing_metadata suppresses legacy metadata.
        # Type guard: only dicts are valid; a list/str/int would survive the
        # `or {}` check but blow up on `.get()` downstream.
        if 'routing_metadata' in body:
            candidate = body['routing_metadata']
        else:
            candidate = body.get('metadata')
        request_metadata = candidate if isinstance(candidate, dict) else {}
        logger.info(f"[Routing] Extracted metadata: action_chip_triggered={request_metadata.get('action_chip_triggered')}, "
                   f"cta_triggered={request_metadata.get('cta_triggered')}, "
                   f"target_branch={request_metadata.get('target_branch')}")

        # Phase 1B: Enhance response with form CTAs (HTTP mode parity with streaming)
        try:
            from form_cta_enhancer import enhance_response_with_form_cta

            # Parse the response body from route_intent (it returns a Lambda response structure)
            response_body = json.loads(response_data.get('body', '{}'))

            # Extract conversation history for readiness scoring
            conversation_history = []
            if conversation_context and conversation_context.get('messages'):
                conversation_history = conversation_context['messages']

            # Load tenant config for explicit routing (3-tier hierarchy)
            tenant_config = None
            try:
                from tenant_config_loader import get_config_for_tenant_by_hash
                tenant_config = get_config_for_tenant_by_hash(tenant_hash)
                logger.info(f"[Routing] Loaded tenant config for explicit routing")
            except Exception as config_error:
                logger.warning(f"[Routing] Could not load tenant config: {config_error}")

            # Determine conversation branch using 3-tier explicit routing
            # This replaces keyword-based detection in form_cta_enhancer
            selected_branch = None
            cta_cards = []

            if tenant_config:
                selected_branch = get_conversation_branch(request_metadata, tenant_config)
                if selected_branch:
                    logger.info(f"[Routing] Selected branch via 3-tier routing: {selected_branch}")

                    # Build CTAs for the selected branch (explicit, no keyword matching)
                    completed_forms = session_context.get('completed_forms', [])
                    cta_cards = build_ctas_for_branch(selected_branch, tenant_config, completed_forms)
                    logger.info(f"[Routing] Built {len(cta_cards)} CTA cards for branch '{selected_branch}'")
                else:
                    logger.info(f"[Routing] No branch selected via 3-tier routing - falling back to form_cta_enhancer")

                    # Fallback to existing enhance_response_with_form_cta for backward compatibility
                    # This handles cases where tenant config doesn't have explicit routing configured
                    enhanced_response = enhance_response_with_form_cta(
                        response_text=response_body.get('content', ''),
                        user_message=body.get('user_input', ''),
                        tenant_hash=tenant_hash,
                        conversation_history=conversation_history,
                        session_context=session_context,
                        tenant_config=tenant_config,  # Phase 4.5b SF-2: skip redundant S3 fetch
                    )

                    if enhanced_response:
                        response_body['content'] = enhanced_response.get('message', response_body.get('content', ''))
                        response_body['ctaButtons'] = enhanced_response.get('cards', [])
                        response_body['metadata'] = {
                            **(response_body.get('metadata', {})),
                            **enhanced_response.get('metadata', {})
                        }
                        logger.info(f"[Routing] Fallback enhancement applied: {len(enhanced_response.get('cards', []))} cards")

            # If we have CTAs from explicit routing, use them
            if cta_cards:
                # Convert CTAs to frontend format
                formatted_cards = []
                for cta in cta_cards:
                    card = {
                        "type": cta.get("type", "cta_button"),
                        "label": cta.get("label") or cta.get("text"),
                        "action": cta.get("action", "link"),
                    }

                    # Add optional fields
                    if cta.get("formId"):
                        card["formId"] = cta["formId"]
                    if cta.get("url"):
                        card["url"] = cta["url"]
                    if cta.get("fields"):
                        card["fields"] = cta["fields"]
                    if cta.get("style"):
                        card["style"] = cta["style"]
                    if cta.get("program"):
                        card["program"] = cta["program"]
                    elif cta.get("program_id"):
                        card["program"] = cta["program_id"]

                    formatted_cards.append(card)

                response_body['ctaButtons'] = formatted_cards
                response_body['metadata'] = {
                    **(response_body.get('metadata', {})),
                    'explicit_routing': True,
                    'branch_used': selected_branch,
                    'cta_count': len(formatted_cards)
                }
                logger.info(f"[Routing] Explicit routing complete: {len(formatted_cards)} CTAs from branch '{selected_branch}'")

            # Re-serialize the enhanced body back into response_data
            response_data['body'] = json.dumps(response_body)
        except Exception as enhance_error:
            logger.warning(f"CTA enhancement failed, continuing with unenhanced response: {enhance_error}")

        # Calculate response time
        end_time = datetime.utcnow()
        response_time_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Extract tenant_id from tenant_hash
        tenant_id = None
        try:
            from tenant_config_loader import resolve_tenant_hash
            tenant_id = resolve_tenant_hash(tenant_hash)
        except Exception as e:
            logger.warning(f"Could not resolve tenant_id: {e}")
        
        # Log structured QA_COMPLETE for analytics (matching Bedrock_Streaming_Handler format).
        # CloudWatch logs are operational — redact email/phone before emitting per
        # Issue #5 PR A finding B1 (employee outreach uses form submissions, untouched).
        try:
            log_body = json.loads(response_data.get('body', '{}'))
            if log_body and 'content' in log_body:
                session_id = body.get('session_id', '')
                conversation_id = body.get('conversation_id', session_id)
                user_input_raw = body.get('user_input', '')
                answer_raw = log_body.get('content', '')

                from redact_pii import redact_pii
                question_redacted = redact_pii(user_input_raw)
                answer_redacted = redact_pii(answer_raw)

                qa_complete_log = {
                    "type": "QA_COMPLETE",
                    "timestamp": datetime.utcnow().isoformat(),
                    "session_id": session_id,
                    "tenant_hash": tenant_hash,
                    "tenant_id": tenant_id,
                    "conversation_id": conversation_id,
                    "question": question_redacted,
                    "answer": answer_redacted,
                    "metrics": {
                        "response_time_ms": response_time_ms,
                        "source": "master_function_http"  # Identify non-streaming source
                    }
                }
                logger.info(json.dumps(qa_complete_log))

                # Issue #5 PR A2: server-side analytics writes. Awaited (not
                # fire-and-forget) on the HTTP-fallback path. Writer logs its
                # own errors; never raises. See analytics_writer.py + the
                # analytics_writer_contract.json wire-format contract.
                if session_id and tenant_hash and request_id:
                    from analytics_writer import write_session_summary
                    client_timestamp = body.get('client_timestamp') or start_time.isoformat() + 'Z'
                    write_session_summary({
                        'event_type': 'MESSAGE_SENT',
                        'session_id': session_id,
                        'tenant_hash': tenant_hash,
                        'tenant_id': tenant_id or '',
                        'client_timestamp': client_timestamp,
                        'request_id': request_id,
                        'event_payload': {'first_question': user_input_raw},
                    })
                    write_session_summary({
                        'event_type': 'MESSAGE_RECEIVED',
                        'session_id': session_id,
                        'tenant_hash': tenant_hash,
                        'tenant_id': tenant_id or '',
                        'client_timestamp': client_timestamp,
                        'request_id': request_id,
                        'event_payload': {'response_time_ms': response_time_ms},
                    })
        except Exception as log_error:
            logger.warning(f"Failed to log QA_COMPLETE: {log_error}")

        # response_data is already a properly formatted Lambda response from route_intent()
        # with statusCode, headers, and body (the body has been enhanced with ctaButtons above)
        response = response_data
        
    except ImportError:
        logger.error("Intent router not available")
        response = {
            'statusCode': 503,
            'body': json.dumps({
                'error': 'Service Unavailable',
                'message': 'Chat service temporarily unavailable'
            })
        }
    except json.JSONDecodeError:
        response = {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Bad Request',
                'message': 'Invalid JSON in request body'
            })
        }
    except Exception as e:
        logger.error(f"Error handling chat: {str(e)}")
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Failed to process chat message'
            })
        }
    
    return add_cors_headers(response, event)

def handle_conversation(event: Dict[str, Any], tenant_hash: str, operation: str) -> Dict[str, Any]:
    """
    Handle conversation operations using real conversation handler
    """
    logger.info(f"Conversation {operation} for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    
    try:
        from conversation_handler import handle_conversation_action
        logger.info("✅ Successfully imported conversation_handler")
        
        # Prepare event for conversation handler
        conv_event = {
            'queryStringParameters': event.get('queryStringParameters', {}),
            'body': event.get('body', '{}'),
            'httpMethod': event.get('httpMethod', 'GET'),
            'headers': event.get('headers', {})  # CRITICAL: Pass headers for JWT token
        }
        conv_event['queryStringParameters']['operation'] = operation
        conv_event['queryStringParameters']['t'] = tenant_hash
        
        # Call the real conversation handler
        response = handle_conversation_action(conv_event, None)
        
        # Response already includes headers from handler
        if 'headers' not in response:
            response = add_cors_headers(response)
        
        return response
        
    except ImportError as e:
        logger.error(f"❌ Failed to import conversation_handler: {e}")
        
        # Debug import chain to find the specific issue
        import sys
        logger.error(f"Python version: {sys.version}")
        logger.error(f"Python path: {sys.path[:3]}")  # Show first 3 paths
        
        # Test individual imports to identify the failure point
        try:
            import boto3
            logger.info("✅ boto3 imported successfully")
        except ImportError as boto_e:
            logger.error(f"❌ boto3 import failed: {boto_e}")
            
        try:
            import jwt
            logger.info("✅ jwt imported successfully")
        except ImportError as jwt_e:
            logger.error(f"❌ jwt import failed: {jwt_e}")
            
        try:
            import aws_client_manager
            logger.info("✅ aws_client_manager imported successfully")
        except ImportError as acm_e:
            logger.error(f"❌ aws_client_manager import failed: {acm_e}")
            
        try:
            import audit_logger
            logger.info("✅ audit_logger imported successfully")
        except ImportError as al_e:
            logger.error(f"❌ audit_logger import failed: {al_e}")
            
        try:
            import token_blacklist
            logger.info("✅ token_blacklist imported successfully")
        except ImportError as tb_e:
            logger.error(f"❌ token_blacklist import failed: {tb_e}")
        
        # Try to get the actual traceback
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        
        logger.warning("Conversation handler not available, returning empty conversation")
        response_data = {
            'conversation': {
                'messages': [],
                'session_id': event.get('queryStringParameters', {}).get('session_id', ''),
                'created_at': '2025-08-17T00:00:00Z'
            }
        }
        
        response = {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
        return add_cors_headers(response, event)
    except Exception as e:
        logger.error(f"Error handling conversation: {str(e)}")
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Failed to handle conversation operation'
            })
        }
        return add_cors_headers(response, event)

def handle_form_submission(event: Dict[str, Any], tenant_hash: str, request_id: str = None) -> Dict[str, Any]:
    """
    Handle form submission from Picasso conversational forms
    """
    logger.info(f"Form submission for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")

    try:
        # Parse request body
        body = json.loads(event.get('body', '{}')) if event.get('body') else {}

        if not body:
            logger.error("No form data in request body")
            response = {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Bad Request',
                    'message': 'Form data is required'
                })
            }
            return add_cors_headers(response, event)

        # Import and use form handler
        from form_handler import FormHandler

        # Get tenant configuration
        config = None
        try:
            from tenant_config_loader import get_config_for_tenant_by_hash, resolve_tenant_hash
            config = get_config_for_tenant_by_hash(tenant_hash)

            # Add tenant_id back for backend operations (it's stripped for frontend security)
            if config:
                tenant_id = resolve_tenant_hash(tenant_hash)
                config['tenant_id'] = tenant_id
        except Exception as e:
            logger.warning(f"Could not load config: {e}")

        # Initialize form handler
        handler = FormHandler(config)

        # Issue #5 PR A2: thread tenant_hash + aws_request_id into the
        # form-data body so _store_submission can fire the FORM_COMPLETED
        # analytics write with full attribution.
        body['_tenant_hash'] = tenant_hash
        body['_request_id'] = request_id

        # Process form submission
        result = handler.handle_form_submission(body)

        logger.info(f"Form submission processed: {result.get('submission_id')}")

        response = {
            'statusCode': 200,
            'body': json.dumps(result)
        }

        return add_cors_headers(response, event)

    except ImportError as e:
        logger.error(f"Failed to import form_handler: {e}")
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Form processing module not available'
            })
        }
        return add_cors_headers(response, event)

    except Exception as e:
        logger.error(f"Error processing form submission: {e}", exc_info=True)
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Failed to process form submission'
            })
        }
        return add_cors_headers(response, event)

def handle_init_session(event: Dict[str, Any], tenant_hash: str) -> Dict[str, Any]:
    """
    Initialize a new chat session using session utils
    """
    logger.info(f"Init session for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    
    try:
        from session_utils import generate_session_id
        import time
        import jwt
        import boto3
        from botocore.exceptions import ClientError
        
        # Parse request body if it exists
        body = json.loads(event.get('body', '{}')) if event.get('body') else {}
        
        # Check if we already have a session_id from the client
        client_session_id = body.get('session_id', '')
        
        # Generate or use existing session ID
        session_id = client_session_id if client_session_id else generate_session_id()
        
        # IMPORTANT: For public endpoints, we use tenant_hash as tenant_id
        # The conversation handler expects tenantId field
        tenant_id = tenant_hash
        
        # SURGICAL FIX: Check for existing conversation before creating new
        existing_conversation = None
        if client_session_id and client_session_id.startswith('session_'):
            try:
                # Safe import with fallback
                try:
                    from conversation_handler import _get_conversation_from_db
                    logger.info(f"🔍 Checking for existing conversation: {client_session_id[:16]}...")
                    
                    # This is non-blocking - if it fails, we continue normally
                    result = _get_conversation_from_db(client_session_id, tenant_hash)
                    
                    if result and result.get('Item'):
                        existing_conversation = result['Item']
                        existing_turn = existing_conversation.get('turn', {}).get('N', '0')
                        logger.info(f"✅ Found existing conversation at turn {existing_turn}")
                except ImportError:
                    logger.debug("conversation_handler not available, skipping existing check")
                except Exception as e:
                    logger.debug(f"Existing conversation check failed (non-fatal): {e}")
            except Exception as outer_e:
                logger.debug(f"Outer exception in existing check (non-fatal): {outer_e}")
        
        # If we found existing conversation, return it (ADDITIVE PATH)
        if existing_conversation:
            try:
                existing_turn = int(existing_conversation.get('turn', {}).get('N', 0))
                
                # Try to use existing state token or generate new one
                existing_token = existing_conversation.get('stateToken', {}).get('S')
                
                # Get JWT signing key using secure helper function
                jwt_signing_key = get_jwt_signing_key()
                
                if not existing_token or existing_token == 'null':
                    # Generate new token with current turn
                    state_token_payload = {
                        'iss': 'myrecruiter-chat',  # P0a Phase 1 (2026-05-02): claim distinguishes chat-session tokens from scheduling tokens; required by Phase 2 decoder hardening
                        'sessionId': session_id,
                        'tenantId': tenant_hash,
                        'turn': existing_turn,
                        'iat': int(time.time()),
                        'exp': int(time.time()) + (24 * 3600)
                    }
                    existing_token = jwt.encode(state_token_payload, jwt_signing_key, algorithm='HS256')
                    logger.info(f"Generated fresh token for existing conversation")
                
                # Extract conversation data if available
                conversation_data = None
                if 'messages' in existing_conversation:
                    try:
                        messages_list = existing_conversation.get('messages', {}).get('L', [])
                        conversation_data = {
                            'turn': existing_turn,
                            'messageCount': len(messages_list),
                            'hasHistory': True
                        }
                    except:
                        pass
                
                response_data = {
                    'session_id': session_id,
                    'state_token': existing_token,
                    'turn': existing_turn,
                    'tenant_hash': tenant_hash,
                    'tenant_id': tenant_id,
                    'existing': True,  # Flag for debugging
                    'initialized': True,
                    'timestamp': '2025-08-23T00:00:00Z',
                    'config': {
                        'timeout': 86400,
                        'max_messages': 100
                    }
                }
                
                # Add conversation data if we extracted it
                if conversation_data:
                    response_data['conversation'] = conversation_data
                
                logger.info(f"✅ Returning existing conversation at turn {existing_turn}")
                
                response = {
                    'statusCode': 200,
                    'body': json.dumps(response_data)
                }
                return add_cors_headers(response, event)
                
            except Exception as e:
                logger.warning(f"Failed to process existing conversation, creating new: {e}")
                # Fall through to normal flow
        
        # Get JWT signing key using secure helper function
        jwt_signing_key = get_jwt_signing_key()
        
        # Generate proper JWT token matching conversation handler expectations
        # CRITICAL: Use camelCase field names to match conversation_handler.py
        state_token_payload = {
            'iss': 'myrecruiter-chat',  # P0a Phase 1 (2026-05-02): claim distinguishes chat-session tokens from scheduling tokens; required by Phase 2 decoder hardening
            'sessionId': session_id,  # camelCase required!
            'tenantId': tenant_id,     # camelCase required! Using hash as ID
            'turn': 0,                 # Initial turn
            'iat': int(time.time()),   # JWT standard: issued at
            'exp': int(time.time()) + (24 * 3600)  # JWT standard: expires (24 hours)
        }
        
        # Create JWT token signed with HS256
        state_token = jwt.encode(state_token_payload, jwt_signing_key, algorithm='HS256')
        logger.info(f"Generated JWT token for session {session_id[:16]}... with proper field names")
        
        response_data = {
            'session_id': session_id,
            'state_token': state_token,  # Proper JWT token
            'turn': 0,
            'tenant_hash': tenant_hash,  # Keep for backward compatibility
            'tenant_id': tenant_id,       # Add for internal consistency
            'initialized': True,
            'timestamp': '2025-08-17T00:00:00Z',
            'config': {
                'timeout': 86400,  # 24 hours to match token expiry
                'max_messages': 100
            }
        }
        
        logger.info(f"Init session success: session={session_id[:16]}..., JWT token created")
        
        response = {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
    except ImportError as e:
        logger.warning(f"Required modules not available: {e}, using fallback")
        # Fallback implementation
        import uuid
        import time
        
        # Parse request body if it exists
        body = json.loads(event.get('body', '{}')) if event.get('body') else {}
        client_session_id = body.get('session_id', '')
        
        session_id = client_session_id if client_session_id else f'session_{uuid.uuid4().hex[:16]}'
        tenant_id = tenant_hash  # Use hash as ID for public endpoints
        
        # Try to create JWT even in fallback
        try:
            import jwt
            jwt_signing_key = get_jwt_signing_key()

            state_token_payload = {
                'iss': 'myrecruiter-chat',  # P0a Phase 1 (2026-05-02): claim distinguishes chat-session tokens from scheduling tokens; required by Phase 2 decoder hardening
                'sessionId': session_id,
                'tenantId': tenant_id,
                'turn': 0,
                'iat': int(time.time()),
                'exp': int(time.time()) + (24 * 3600)
            }
            state_token = jwt.encode(state_token_payload, jwt_signing_key, algorithm='HS256')
            logger.info("Created JWT token in fallback mode")
        except Exception as e:
            # SECURITY FIX: Removed base64 fallback — unsigned tokens are a security risk
            # If JWT signing fails, the session cannot be established securely
            logger.error(f"JWT signing failed, cannot create secure session: {e}")
            raise ValueError("Unable to create secure session token — JWT signing key unavailable")
        
        response_data = {
            'session_id': session_id,
            'state_token': state_token,
            'turn': 0,
            'tenant_hash': tenant_hash,
            'tenant_id': tenant_id,
            'initialized': True,
            'timestamp': '2025-08-17T00:00:00Z',
            'config': {
                'timeout': 86400,
                'max_messages': 100
            }
        }
        
        response = {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        logger.error(f"Error initializing session: {str(e)}")
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Failed to initialize session'
            })
        }
    
    return add_cors_headers(response, event)

def handle_generate_stream_token(event: Dict[str, Any], tenant_hash: str) -> Dict[str, Any]:
    """
    Generate JWT token specifically for streaming operations.
    Separate from init_session to maintain single responsibility principle.
    Streaming tokens have purpose='stream' while conversation tokens have no purpose field.
    """
    logger.info(f"Generate stream token for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    
    try:
        import time
        import jwt
        import boto3
        import uuid
        from botocore.exceptions import ClientError
        
        # Parse request body if it exists
        body = json.loads(event.get('body', '{}')) if event.get('body') else {}
        
        # Use existing session_id if provided, otherwise generate new one
        session_id = body.get('session_id', '')
        if not session_id:
            # Generate session ID same way as init_session fallback
            session_id = f'session_{uuid.uuid4().hex[:16]}'
            logger.info(f"Generated new session ID for streaming: {session_id[:16]}...")
        else:
            logger.info(f"Using existing session ID for streaming: {session_id[:16]}...")
        
        # For streaming, tenant_hash is the tenant_id
        tenant_id = tenant_hash
        
        # Get JWT signing key using secure helper function
        jwt_signing_key = get_jwt_signing_key()
        
        # Generate streaming-specific JWT token
        # CRITICAL: Must include 'purpose': 'stream' for streaming handler validation
        stream_token_payload = {
            'iss': 'myrecruiter-chat',     # P0a Phase 1 (2026-05-02): claim distinguishes chat-class tokens (state + stream) from scheduling tokens; required by Phase 2 decoder hardening
            'sessionId': session_id,      # camelCase required by streaming handler
            'tenantId': tenant_id,         # camelCase required by streaming handler
            'purpose': 'stream',           # REQUIRED for streaming authentication
            'iat': int(time.time()),       # JWT standard: issued at
            'exp': int(time.time()) + 3600 # JWT standard: expires in 1 hour for streaming
        }
        
        # Create JWT token signed with HS256
        stream_token = jwt.encode(stream_token_payload, jwt_signing_key, algorithm='HS256')
        
        # Prepare response with all necessary information
        response_data = {
            'stream_token': stream_token,
            'session_id': session_id,
            'tenant_hash': tenant_hash,
            'tenant_id': tenant_id,
            'expires_in': 3600,
            # STREAMING_ENDPOINT env var is required (Phase 4 EC-P4-4).
            # KeyError fail-loud if missing — prevents silently routing to
            # the prior prod-account BSH URL that was previously hardcoded
            # as the fallback (cross-account leakage risk).
            'streaming_endpoint': os.environ['STREAMING_ENDPOINT'],
            'purpose': 'stream',
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        logger.info(f"Stream token generated successfully for session {session_id[:16]}...")
        
        response = {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        logger.error(f"Failed to generate stream token: {str(e)}", exc_info=True)
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'Failed to generate stream token'
            })
        }
    
    return add_cors_headers(response, event)

def get_cache_status(event: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Get cache status information
    """
    logger.info("Cache status requested")
    
    response_data = {
        'cache_enabled': True,
        'cache_size': 0,
        'cache_entries': 0,
        'cache_ttl': 300,  # 5 minutes
        'environment': os.environ.get('ENVIRONMENT', 'staging')
    }
    
    response = {
        'statusCode': 200,
        'body': json.dumps(response_data)
    }
    
    return add_cors_headers(response, event)

def clear_cache(tenant_hash: str, event: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Clear cache for a specific tenant or all tenants
    """
    logger.info(f"Clear cache requested for tenant: {tenant_hash[:8] if tenant_hash else 'all'}...")
    
    response_data = {
        'success': True,
        'message': f"Cache cleared for {'tenant ' + tenant_hash[:8] if tenant_hash else 'all tenants'}",
        'timestamp': '2025-08-17T00:00:00Z'
    }
    
    response = {
        'statusCode': 200,
        'body': json.dumps(response_data)
    }
    
    return add_cors_headers(response, event)

def handle_cache_warming(event: Dict[str, Any], tenant_hash: str) -> Dict[str, Any]:
    """
    Warm the cache for a specific tenant by pre-caching action cards and quick help questions
    """
    logger.info(f"🔥 Cache warming requested for tenant: {tenant_hash[:8] if tenant_hash else 'unknown'}...")
    
    if not tenant_hash:
        response = {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Bad Request',
                'message': 'Tenant hash required for cache warming'
            })
        }
        return add_cors_headers(response, event)
    
    try:
        # Load tenant config
        from tenant_config_loader import get_config_for_tenant_by_hash
        config = get_config_for_tenant_by_hash(tenant_hash)
        
        if not config:
            response = {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Not Found',
                    'message': f'Configuration not found for tenant {tenant_hash[:8]}...'
                })
            }
            return add_cors_headers(response, event)
        
        # Try to use optimized handler with cache warming
        try:
            from bedrock_handler_optimized import warm_cache_for_tenant
            questions_cached = warm_cache_for_tenant(tenant_hash, config)
            
            from datetime import datetime
            response_data = {
                'success': True,
                'message': f'Cache warmed successfully for tenant {tenant_hash[:8]}...',
                'questions_cached': questions_cached,
                'tenant_hash': tenant_hash[:8] + '...',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        except ImportError:
            response_data = {
                'success': False,
                'message': 'Cache warming not available (using standard handler)',
                'tenant_hash': tenant_hash[:8] + '...'
            }
        
        response = {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        logger.error(f"❌ Cache warming failed: {str(e)}", exc_info=True)
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': f'Cache warming failed: {str(e)}'
            })
        }
    
    return add_cors_headers(response, event)

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Main Lambda handler with centralized routing and CORS
    """
    try:
        logger.info(f"🚀 LAMBDA INVOKED at {datetime.utcnow().isoformat()}")

        # Extract HTTP method first thing
        http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method', 'GET'))

        # Handle OPTIONS requests immediately for CORS preflight
        if http_method == 'OPTIONS':
            logger.info("OPTIONS request - returning CORS headers immediately")
            return handle_options(event)

        # CF origin header validation — guards against direct Function URL
        # access that bypasses CloudFront (and its WAF). Feature-flagged via
        # REQUIRE_CF_ORIGIN_HEADER env var; default skip during rollout.
        is_valid_origin, origin_reason = validate_cf_origin_header(event)
        if not is_valid_origin:
            logger.warning(f"SECURITY: rejected request with invalid CF origin header — {origin_reason}")
            response = {
                'statusCode': 403,
                'body': json.dumps({
                    'error': 'Forbidden',
                    'message': 'Request must originate from CloudFront',
                })
            }
            return add_cors_headers(response, event)

        # Parse query parameters
        query_params = event.get('queryStringParameters', {}) or {}

        # If no queryStringParameters, try to parse from rawQueryString (Lambda Function URL format)
        if not query_params and event.get('rawQueryString'):
            parsed = parse_qs(event.get('rawQueryString', ''))
            query_params = {k: v[0] if v else None for k, v in parsed.items()}
        
        # Get action and tenant hash from query parameters
        action = query_params.get('action', '')
        tenant_hash = query_params.get('t', '')
        
        logger.info(f"Processing {http_method} request with action: {action}, tenant: {tenant_hash[:8]}..." if tenant_hash else f"Processing {http_method} request with action: {action}")
        
        # Route based on action parameter
        if action == 'health_check' or action == 'health':
            return health_check(event)
        elif action == 'get_config':
            return get_config_for_tenant(tenant_hash, event)
        elif action == 'chat':
            return handle_chat(event, tenant_hash, getattr(context, 'aws_request_id', None))
        elif action == 'conversation':
            operation = query_params.get('operation', 'get')
            return handle_conversation(event, tenant_hash, operation)
        elif action == 'init_session':
            return handle_init_session(event, tenant_hash)
        elif action == 'form_submission':
            return handle_form_submission(event, tenant_hash, getattr(context, 'aws_request_id', None))
        elif action == 'generate_stream_token':
            return handle_generate_stream_token(event, tenant_hash)
        elif action == 'cache_status':
            return get_cache_status(event)
        elif action == 'clear_cache':
            return clear_cache(tenant_hash, event)
        elif action == 'warm_cache':
            return handle_cache_warming(event, tenant_hash)
        elif not action:
            # No action specified, default to health check
            return health_check(event)
        else:
            # Unknown action
            logger.warning(f"Unknown action requested: {action}")
            response = {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Not Found',
                    'message': f'Action {action} not found',
                    'available_actions': ['health_check', 'get_config', 'chat', 'conversation', 'init_session', 'generate_stream_token', 'cache_status', 'clear_cache', 'warm_cache']
                })
            }
            return add_cors_headers(response, event)
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': 'An unexpected error occurred'
            })
        }
        return add_cors_headers(response, event)