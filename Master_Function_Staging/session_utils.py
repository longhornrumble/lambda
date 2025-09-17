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

def build_session_attributes(tenant_id, prompt_index=0, topic="", session_id=None):
    """
    Enhanced session attributes builder with tenant validation
    """
    if not _is_valid_tenant_id(tenant_id):
        logger.error(f"SECURITY: Cannot build session attributes with invalid tenant_id: {tenant_id}")
        raise ValueError("Invalid tenant_id for session attributes")
    
    logger.info(f"[{tenant_id[:8]}...] 🧩 Building session attributes: prompt_index={prompt_index}, topic={topic}")
    
    attrs = {
        "tenant_id": tenant_id,
        "prompt_variant_index": str(prompt_index),
        "current_topic": topic
    }
    
    # Add tenant-prefixed key for DynamoDB operations
    if session_id:
        attrs["tenant_prefixed_key"] = generate_tenant_prefixed_key(tenant_id, session_id)
    
    return attrs

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

def validate_tenant_access(provided_tenant_id, dynamodb_key):
    """
    Validate that the provided tenant_id matches the tenant in the DynamoDB key
    Prevents cross-tenant data access
    """
    if not provided_tenant_id or not dynamodb_key:
        logger.error("SECURITY: Missing tenant_id or dynamodb_key for validation")
        return False
    
    # Extract tenant from DynamoDB key
    try:
        if not dynamodb_key.startswith("TENANT#"):
            logger.error(f"SECURITY: Invalid DynamoDB key format: {dynamodb_key[:30]}...")
            return False
        
        # Parse tenant from key: TENANT#{tenantId}#SESSION#{sessionId}
        parts = dynamodb_key.split("#")
        if len(parts) < 4:
            logger.error(f"SECURITY: Malformed DynamoDB key: {dynamodb_key[:30]}...")
            return False
        
        key_tenant_id = parts[1]  # Extract tenant_id from TENANT#{tenantId}#...
        
        # Compare tenant IDs
        if provided_tenant_id != key_tenant_id:
            logger.error(f"SECURITY: Tenant access violation - provided: {provided_tenant_id[:8]}..., key: {key_tenant_id[:8]}...")
            return False
        
        logger.info(f"✅ Tenant access validated: {provided_tenant_id[:8]}...")
        return True
        
    except Exception as e:
        logger.error(f"SECURITY: Tenant validation failed: {str(e)}")
        return False

def generate_bedrock_filter_key(tenant_id, filter_type="GENERAL"):
    """
    Generate tenant-specific Bedrock filter keys
    Ensures tenant data isolation in Bedrock KB queries
    """
    if not _is_valid_tenant_id(tenant_id):
        raise ValueError(f"Invalid tenant_id for Bedrock filter: {tenant_id}")
    
    # Create secure hash-based filter key
    filter_input = f"{tenant_id}#{filter_type}#BEDROCK_FILTER"
    filter_hash = hashlib.sha256(filter_input.encode()).hexdigest()[:16]
    
    bedrock_key = f"TENANT#{tenant_id}#FILTER#{filter_hash}"
    logger.info(f"🤖 Generated Bedrock filter key for {tenant_id[:8]}...")
    
    return bedrock_key

def extract_tenant_from_key(dynamodb_key):
    """
    Extract tenant_id from a tenant-prefixed DynamoDB key
    Used for security validation and logging
    """
    if not dynamodb_key or not isinstance(dynamodb_key, str):
        return None
    
    try:
        if not dynamodb_key.startswith("TENANT#"):
            return None
        
        parts = dynamodb_key.split("#")
        if len(parts) >= 2:
            return parts[1]  # Return tenant_id from TENANT#{tenantId}#...
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting tenant from key: {str(e)}")
        return None

def secure_session_cleanup(tenant_id, session_id):
    """
    Generate keys for secure session cleanup operations
    Ensures only tenant-owned sessions are cleaned up
    """
    if not _is_valid_tenant_id(tenant_id) or not _is_valid_session_id(session_id):
        raise ValueError("Invalid tenant_id or session_id for cleanup")
    
    cleanup_keys = {
        'session_key': generate_tenant_prefixed_key(tenant_id, session_id, "SESSION"),
        'cache_key': generate_tenant_prefixed_key(tenant_id, session_id, "CACHE"),
        'metrics_key': generate_tenant_prefixed_key(tenant_id, session_id, "METRICS")
    }
    
    logger.info(f"🧹 Generated cleanup keys for tenant {tenant_id[:8]}..., session {session_id[:12]}...")
    return cleanup_keys

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

# Security monitoring functions

def log_tenant_access_attempt(tenant_id, session_id, operation, success=True):
    """Log tenant access attempts for security monitoring"""
    log_data = {
        'event': 'tenant_access_attempt',
        'tenant_id': tenant_id[:8] + '...' if tenant_id else 'UNKNOWN',
        'session_id': session_id[:12] + '...' if session_id else 'UNKNOWN',
        'operation': operation,
        'success': success,
        'timestamp': int(time.time()) if 'time' in globals() else 0
    }
    
    if success:
        logger.info(f"SECURITY_ACCESS: {log_data}")
    else:
        logger.error(f"SECURITY_VIOLATION: {log_data}")

def validate_bedrock_tenant_filter(tenant_id, bedrock_query):
    """
    Validate that Bedrock queries include proper tenant filtering
    Prevents cross-tenant data leakage in KB queries
    """
    if not tenant_id:
        logger.error("SECURITY: Missing tenant_id for Bedrock query validation")
        return False
    
    # Check if query includes tenant-specific filtering
    if isinstance(bedrock_query, dict):
        # Check for tenant filter in query structure
        filters = bedrock_query.get('filter', {})
        if not filters:
            logger.error(f"SECURITY: Bedrock query missing tenant filter for {tenant_id[:8]}...")
            return False
        
        # Validate tenant filter is present
        tenant_filters = [f for f in filters if 'tenant' in str(f).lower()]
        if not tenant_filters:
            logger.error(f"SECURITY: Bedrock query missing tenant-specific filter for {tenant_id[:8]}...")
            return False
    
    logger.info(f"✅ Bedrock tenant filter validated for {tenant_id[:8]}...")
    return True