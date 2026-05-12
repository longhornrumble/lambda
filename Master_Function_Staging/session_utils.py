import uuid
import logging
import re
import hashlib
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Security patterns for tenant validation
TENANT_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{3,50}$')
SESSION_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{8,50}$')

def generate_session_id():
    """
    Generate a unique session ID for chat sessions
    Format: session_{timestamp}_{random_string}
    """
    timestamp = int(time.time() * 1000)  # Milliseconds since epoch
    random_str = uuid.uuid4().hex[:12]  # 12 character random string
    session_id = f"session_{timestamp}_{random_str}"
    logger.info(f"Generated new session ID: {session_id[:16]}...")
    return session_id

def extract_session_data(event, tenant_info=None):
    """
    Enhanced session data extraction with tenant inference integration
    Enforces tenant-prefixed keys for DynamoDB security
    """
    session_attrs = event.get("sessionState", {}).get("sessionAttributes", {}) or {}
    session_id = event.get("sessionId") or str(uuid.uuid4())
    
    # Extract tenant from multiple sources with priority
    tenant_id = None
    
    # 1. Use tenant from inference system (highest priority)
    if tenant_info and tenant_info.get('tenant_hash'):
        tenant_id = tenant_info.get('tenant_hash')
    # 2. Fallback to session attributes
    elif session_attrs.get("tenant_id"):
        tenant_id = session_attrs.get("tenant_id")
    # 3. Try to extract from JWT if available
    elif event.get('headers', {}).get('authorization'):
        try:
            from tenant_inference import extract_tenant_from_token
            jwt_tenant = extract_tenant_from_token(event)
            if jwt_tenant:
                tenant_id = jwt_tenant.get('tenant_id')
        except ImportError:
            pass

    # Validate tenant_id for security
    if tenant_id and not _is_valid_tenant_id(tenant_id):
        logger.error(f"SECURITY: Invalid tenant_id format: {tenant_id}")
        tenant_id = None
    
    # Validate session_id for security
    if not _is_valid_session_id(session_id):
        logger.error(f"SECURITY: Invalid session_id format: {session_id}")
        session_id = str(uuid.uuid4())

    logger.info(f"[{tenant_id[:8] if tenant_id else 'UNKNOWN'}...] 🧾 Extracted session data: session_id={session_id[:12]}..., topic={session_attrs.get('current_topic', '')}")
    
    return {
        "tenant_id": tenant_id,
        "prompt_index": int(session_attrs.get("prompt_variant_index", 0)),
        "topic": session_attrs.get("current_topic", ""),
        "session_id": session_id,
        "tenant_prefixed_key": generate_tenant_prefixed_key(tenant_id, session_id) if tenant_id else None,
        "raw": session_attrs
    }

def generate_tenant_prefixed_key(tenant_id, session_id, key_type="SESSION"):
    """
    Generate tenant-prefixed DynamoDB keys for cross-tenant prevention
    Format: TENANT#{tenantId}#SESSION#{sessionId}
    """
    if not tenant_id or not session_id:
        raise ValueError("Both tenant_id and session_id are required for prefixed keys")
    
    if not _is_valid_tenant_id(tenant_id):
        raise ValueError(f"Invalid tenant_id format: {tenant_id}")
    
    if not _is_valid_session_id(session_id):
        raise ValueError(f"Invalid session_id format: {session_id}")
    
    # Generate secure tenant-prefixed key
    prefixed_key = f"TENANT#{tenant_id}#{key_type}#{session_id}"
    
    logger.info(f"🔐 Generated tenant-prefixed key: {prefixed_key[:30]}... (length: {len(prefixed_key)})")
    return prefixed_key

# Internal validation functions

def _is_valid_tenant_id(tenant_id):
    """Validate tenant_id format for security"""
    return (
        tenant_id and 
        isinstance(tenant_id, str) and 
        len(tenant_id) >= 3 and len(tenant_id) <= 50 and
        TENANT_ID_PATTERN.match(tenant_id)
    )

def _is_valid_session_id(session_id):
    """Validate session_id format for security"""
    return (
        session_id and 
        isinstance(session_id, str) and 
        len(session_id) >= 8 and len(session_id) <= 50 and
        SESSION_ID_PATTERN.match(session_id)
    )

