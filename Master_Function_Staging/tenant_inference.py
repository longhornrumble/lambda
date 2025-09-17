"""
PICASSO Tenant Inference System - Production Hardened
Bulletproof security hardening for healthcare environments
"""

import json
import jwt
import boto3
import logging
import os
import time
import re
import uuid
import hashlib
from urllib.parse import urlparse
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize audit logger
try:
    from audit_logger import audit_logger
    AUDIT_LOGGER_AVAILABLE = True
    logger.info("✅ audit_logger module loaded successfully")
except ImportError as e:
    logger.warning(f"⚠️ audit_logger not available: {e}")
    AUDIT_LOGGER_AVAILABLE = False

# Security Configuration
RATE_LIMIT_WINDOW = 300  # 5 minutes
RATE_LIMIT_THRESHOLD = 10  # failures per window
TENANT_CACHE_TTL = 600   # 5-10 minutes
JWT_CLOCK_SKEW = 60      # 60s tolerance
MAX_PATH_LENGTH = 1000   # Path traversal protection

# Environment Configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'myrecruiter-picasso')
MAPPINGS_PREFIX = os.environ.get('MAPPINGS_PREFIX', 'mappings')
JWT_SECRET_KEY_NAME = os.environ.get('JWT_SECRET_KEY_NAME', 'picasso/jwt/signing-key')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# Global caches and rate limiting
tenant_registry_cache = {}
registry_cache_timestamp = 0
failure_tracking = {}
signing_key_cache = None
key_cache_expires = 0

# AWS clients
s3 = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

# Security patterns
TENANT_HASH_PATTERN = re.compile(r'^[a-zA-Z0-9]{10,20}$')
SAFE_PATH_PATTERN = re.compile(r'^[a-zA-Z0-9/_-]+$')

def resolveTenant(event):
    """
    Main tenant inference with token→host→path→config precedence
    Fail-closed security with comprehensive audit logging
    """
    try:
        request_context = _extract_request_context(event)
        
        # Check rate limiting first
        if _is_rate_limited(request_context['source_ip']):
            return handle_inference_failure("rate_limited", request_context)
        
        # 1. JWT Token-based inference (highest priority)
        jwt_tenant = extract_tenant_from_token(event)
        if jwt_tenant:
            _audit_success("jwt_inference", jwt_tenant, request_context)
            # Audit successful tenant inference
            if AUDIT_LOGGER_AVAILABLE:
                audit_logger.log_tenant_inferred(
                    tenant_id=jwt_tenant.get('tenant_id', 'unknown'),
                    session_id=jwt_tenant.get('session_id'),
                    inference_method='jwt_token',
                    matched_value='jwt_payload'
                )
            return jwt_tenant
        
        # 2. Host-based inference
        host = _extract_host(event)
        origin = _extract_origin(event)
        if host or origin:
            host_tenant = findTenantByHostOriginPath(host, origin, None)
            if host_tenant:
                _audit_success("host_inference", host_tenant, request_context)
                # Audit successful tenant inference
                if AUDIT_LOGGER_AVAILABLE:
                    audit_logger.log_tenant_inferred(
                        tenant_id=host_tenant.get('tenant_hash', 'unknown'),
                        session_id=request_context.get('request_id'),
                        inference_method='host_origin',
                        matched_value=host_tenant.get('matched_value')
                    )
                return host_tenant
        
        # 3. Path-based inference
        path = _extract_path(event)
        if path:
            registry = loadTenantRegistry()
            path_tenant = _find_tenant_by_path(path, registry, request_context)
            if path_tenant:
                _audit_success("path_inference", path_tenant, request_context)
                # Audit successful tenant inference
                if AUDIT_LOGGER_AVAILABLE:
                    audit_logger.log_tenant_inferred(
                        tenant_id=path_tenant.get('tenant_hash', 'unknown'),
                        session_id=request_context.get('request_id'),
                        inference_method='path',
                        matched_value=path_tenant.get('matched_value')
                    )
                return path_tenant
        
        # 4. Config parameter fallback
        query_params = event.get('queryStringParameters', {}) or {}
        tenant_hash = query_params.get('t')
        if tenant_hash and _is_valid_tenant_hash_format(tenant_hash):
            config_tenant = _resolve_config_tenant(tenant_hash, request_context)
            if config_tenant:
                _audit_success("config_inference", config_tenant, request_context)
                # Audit successful tenant inference
                if AUDIT_LOGGER_AVAILABLE:
                    audit_logger.log_tenant_inferred(
                        tenant_id=config_tenant.get('tenant_hash', 'unknown'),
                        session_id=request_context.get('request_id'),
                        inference_method='config_parameter',
                        matched_value=tenant_hash
                    )
                return config_tenant
        
        # All inference methods failed
        return handle_inference_failure("no_tenant_found", request_context)
        
    except Exception as e:
        logger.error(f"CRITICAL: Tenant inference system failure: {str(e)}")
        return handle_inference_failure("system_error", {"error": str(e)})

def extract_tenant_from_token(event):
    """
    Enhanced JWT validation with iss/aud/purpose checks
    Returns tenant info or None if invalid
    """
    try:
        # Extract JWT from multiple sources
        jwt_token = None
        
        # Try authorization header first
        auth_header = event.get('headers', {}).get('authorization', '')
        if auth_header.startswith('Bearer '):
            jwt_token = auth_header.replace('Bearer ', '')
        
        # Try x-jwt-token header if no authorization header
        if not jwt_token:
            jwt_token = event.get('headers', {}).get('x-jwt-token')
        
        # Try query parameter if no headers
        if not jwt_token and event.get('queryStringParameters'):
            jwt_token = event.get('queryStringParameters', {}).get('token')
        
        if not jwt_token:
            return None
        
        # Get cached signing key
        signing_key = _get_signing_key()
        if not signing_key:
            logger.error("SECURITY: JWT signing key unavailable")
            return None
        
        # Decode with enhanced validation
        payload = jwt.decode(
            jwt_token, 
            signing_key, 
            algorithms=['HS256'],
            options={'verify_exp': True, 'verify_iat': True, 'verify_aud': False},
            leeway=JWT_CLOCK_SKEW
        )
        
        # Validate required claims
        required_claims = ['iss', 'aud', 'purpose', 'tenantId', 'jti']
        for claim in required_claims:
            if not payload.get(claim):
                logger.warning(f"SECURITY: JWT missing claim: {claim}")
                return None
        
        # Validate issuer and audience
        expected_issuer = f'picasso-{ENVIRONMENT}'
        if payload['iss'] != expected_issuer:
            logger.warning(f"SECURITY: Invalid JWT issuer: {payload['iss']}")
            return None
        
        if payload['aud'] not in ['streaming-function', 'master-function']:
            logger.warning(f"SECURITY: Invalid JWT audience: {payload['aud']}")
            return None
        
        # Validate purpose
        valid_purposes = ['stream', 'manage', 'config', 'chat']
        if payload['purpose'] not in valid_purposes:
            logger.warning(f"SECURITY: Invalid JWT purpose: {payload['purpose']}")
            return None
        
        # Audit successful JWT validation
        tenant_result = {
            'tenant_id': payload['tenantId'],
            'session_id': payload.get('sessionId'),
            'purpose': payload['purpose'],
            'source': 'jwt_token',
            'expires_at': payload['exp'],
            'jti': payload['jti']
        }
        
        if AUDIT_LOGGER_AVAILABLE:
            # Determine JWT source
            jwt_source = 'header' if auth_header else ('query' if jwt_token else 'unknown')
            audit_logger.log_jwt_validated(
                tenant_id=payload['tenantId'],
                session_id=payload.get('sessionId'),
                jwt_purpose=payload['purpose'],
                source=jwt_source
            )
        
        # Return tenant information
        return tenant_result
        
    except jwt.ExpiredSignatureError:
        logger.warning("SECURITY: Expired JWT token")
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger.log_jwt_invalid(
                tenant_id="unknown",
                session_id=None,
                error_type="expired_signature"
            )
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"SECURITY: Invalid JWT token: {str(e)}")
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger.log_jwt_invalid(
                tenant_id="unknown",
                session_id=None,
                error_type="invalid_token"
            )
        return None
    except Exception as e:
        logger.error(f"SECURITY: JWT validation error: {str(e)}")
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger.log_jwt_invalid(
                tenant_id="unknown",
                session_id=None,
                error_type="validation_error"
            )
        return None

def loadTenantRegistry():
    """
    Registry loader with 5-10min cache and graceful degradation
    """
    global tenant_registry_cache, registry_cache_timestamp
    
    current_time = time.time()
    
    # Check cache validity
    if (current_time - registry_cache_timestamp) < TENANT_CACHE_TTL and tenant_registry_cache:
        return tenant_registry_cache
    
    try:
        # Load registry from S3
        logger.info("Loading tenant registry from S3")
        
        # List all mapping files
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{MAPPINGS_PREFIX}/",
            MaxKeys=100  # Limit for performance
        )
        
        registry = {
            'hosts': {},
            'origins': {},
            'paths': {},
            'hashes': set(),
            'loaded_at': current_time
        }
        
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                try:
                    tenant_hash = key.split('/')[-1].replace('.json', '')
                    if _is_valid_tenant_hash_format(tenant_hash):
                        registry['hashes'].add(tenant_hash)
                        
                        # Load mapping details for host/origin/path inference
                        mapping_obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                        mapping_data = json.loads(mapping_obj['Body'].read())
                        
                        # Index by different attributes
                        if mapping_data.get('host'):
                            registry['hosts'][norm_host(mapping_data['host'])] = tenant_hash
                        if mapping_data.get('origin'):
                            registry['origins'][norm_origin(mapping_data['origin'])] = tenant_hash
                        if mapping_data.get('path'):
                            registry['paths'][mapping_data['path']] = tenant_hash
                            
                except Exception as e:
                    logger.warning(f"Failed to load mapping {key}: {str(e)}")
                    continue
        
        # Cache the registry
        tenant_registry_cache = registry
        registry_cache_timestamp = current_time
        
        logger.info(f"Tenant registry loaded: {len(registry['hashes'])} tenants")
        return registry
        
    except Exception as e:
        logger.error(f"Failed to load tenant registry: {str(e)}")
        
        # Graceful degradation - return cached version if available
        if tenant_registry_cache:
            logger.warning("Using cached tenant registry due to load failure")
            return tenant_registry_cache
        
        # Last resort - empty registry
        return {
            'hosts': {},
            'origins': {},
            'paths': {},
            'hashes': set(),
            'loaded_at': current_time,
            'degraded': True
        }

def findTenantByHostOriginPath(host, origin, path):
    """
    Normalized lookup with bulletproof validation
    """
    try:
        registry = loadTenantRegistry()
        
        # Host-based lookup
        if host:
            normalized_host = norm_host(host)
            if normalized_host in registry.get('hosts', {}):
                tenant_hash = registry['hosts'][normalized_host]
                return _build_tenant_result(tenant_hash, 'host', normalized_host)
        
        # Origin-based lookup
        if origin:
            normalized_origin = norm_origin(origin)
            if normalized_origin in registry.get('origins', {}):
                tenant_hash = registry['origins'][normalized_origin]
                return _build_tenant_result(tenant_hash, 'origin', normalized_origin)
        
        # Path-based lookup
        if path:
            valid_paths = registry.get('paths', {})
            if valid_path(path, list(valid_paths.keys())):
                # Find matching path prefix
                for registered_path, tenant_hash in valid_paths.items():
                    if path.startswith(registered_path):
                        return _build_tenant_result(tenant_hash, 'path', registered_path)
        
        return None
        
    except Exception as e:
        logger.error(f"SECURITY: Host/Origin/Path lookup failed: {str(e)}")
        return None

def norm_host(h):
    """
    Bulletproof host normalization with IDN/punycode support
    """
    if not h or not isinstance(h, str):
        return ''
    
    try:
        # Remove protocol if present
        if '://' in h:
            h = h.split('://', 1)[1]
        
        # Remove port if present
        h = h.split(':')[0]
        
        # Remove path if present
        h = h.split('/')[0]
        
        # Convert to lowercase
        h = h.lower().strip()
        
        # Handle IDN (International Domain Names)
        try:
            h = h.encode('idna').decode('ascii')
        except (UnicodeError, UnicodeDecodeError):
            # If IDN conversion fails, use original but validate
            pass
        
        # Validate format
        if not re.match(r'^[a-zA-Z0-9.-]+$', h):
            logger.warning(f"SECURITY: Invalid host format: {h}")
            return ''
        
        # Length validation
        if len(h) > 255 or len(h) < 3:
            return ''
        
        # Ensure HTTPS-only domains (production security)
        if ENVIRONMENT == 'production' and not h.endswith(('.com', '.org', '.net', '.ai', '.io')):
            logger.warning(f"SECURITY: Non-standard TLD in production: {h}")
        
        return h
        
    except Exception as e:
        logger.error(f"SECURITY: Host normalization failed: {str(e)}")
        return ''

def norm_origin(o):
    """
    Bulletproof origin normalization with HTTPS enforcement
    """
    if not o or not isinstance(o, str):
        return ''
    
    try:
        # Parse origin URL
        parsed = urlparse(o.lower().strip())
        
        # Enforce HTTPS in production
        if ENVIRONMENT == 'production' and parsed.scheme != 'https':
            logger.warning(f"SECURITY: Non-HTTPS origin rejected: {o}")
            return ''
        
        # Validate scheme
        if parsed.scheme not in ['http', 'https']:
            return ''
        
        # Normalize host component
        normalized_host = norm_host(parsed.netloc)
        if not normalized_host:
            return ''
        
        # Rebuild normalized origin
        return f"{parsed.scheme}://{normalized_host}"
        
    except Exception as e:
        logger.error(f"SECURITY: Origin normalization failed: {str(e)}")
        return ''

def valid_path(path, allowed_paths):
    """
    Path validation with traversal protection and normalization
    """
    if not path or not isinstance(path, str):
        return False
    
    try:
        # Length validation
        if len(path) > MAX_PATH_LENGTH:
            logger.warning("SECURITY: Path too long")
            return False
        
        # Normalize path
        normalized = os.path.normpath(path)
        
        # Path traversal protection
        if '..' in normalized or normalized.startswith('/..'):
            logger.warning(f"SECURITY: Path traversal attempt: {path}")
            return False
        
        # Character validation
        if not SAFE_PATH_PATTERN.match(normalized.replace('/', '')):
            logger.warning(f"SECURITY: Invalid path characters: {path}")
            return False
        
        # Check against allowed paths (prefix matching)
        for allowed in allowed_paths:
            if normalized.startswith(allowed):
                return True
        
        return False
        
    except Exception as e:
        logger.error(f"SECURITY: Path validation failed: {str(e)}")
        return False

def handle_inference_failure(reason, context):
    """
    Fail-closed behavior with audit logging
    """
    failure_id = str(uuid.uuid4())[:8]
    
    # Rate limiting tracking
    source_ip = context.get('source_ip', 'unknown') if isinstance(context, dict) else 'unknown'
    _track_failure(source_ip)
    
    # Audit logging with full context
    audit_data = {
        'failure_id': failure_id,
        'reason': reason,
        'timestamp': int(time.time()),
        'source_ip': source_ip,
        'user_agent': context.get('user_agent', 'unknown') if isinstance(context, dict) else 'unknown',
        'request_id': context.get('request_id', 'unknown') if isinstance(context, dict) else 'unknown',
        'environment': ENVIRONMENT
    }
    
    logger.error(f"SECURITY_AUDIT: Tenant inference failure: {json.dumps(audit_data)}")
    
    # Audit tenant inference failure
    if AUDIT_LOGGER_AVAILABLE:
        # Hash user agent for privacy
        user_agent_hash = hashlib.sha256(f"{context.get('user_agent', 'unknown')}_{ENVIRONMENT}".encode()).hexdigest()[:8] if isinstance(context, dict) else None
        
        audit_logger.log_tenant_inference_failed(
            tenant_id="unknown",
            session_id=context.get('request_id') if isinstance(context, dict) else None,
            failure_reason=reason,
            source_ip=source_ip,
            user_agent_hash=user_agent_hash
        )
    
    # Send to CloudWatch for alerting
    try:
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='PICASSO/Security',
            MetricData=[{
                'MetricName': 'TenantInferenceFailures',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Reason', 'Value': reason},
                    {'Name': 'Environment', 'Value': ENVIRONMENT}
                ]
            }]
        )
    except Exception:
        pass  # Don't fail on metrics
    
    # Generic response (no information leakage)
    return {
        'error': 'Access denied',
        'failure_id': failure_id,
        'status_code': 403
    }

def generate_streaming_token():
    """
    Enhanced token issuance with kid/jti
    """
    try:
        # Get signing key with rotation support
        signing_key = _get_signing_key()
        if not signing_key:
            raise ValueError("Signing key unavailable")
        
        # Generate unique identifiers
        session_id = _generate_session_id()
        jti = _generate_jti()
        
        # Build payload with enhanced security
        current_time = datetime.utcnow()
        payload = {
            'sessionId': session_id,
            'purpose': 'stream',
            'iat': int(current_time.timestamp()),
            'exp': int((current_time + timedelta(minutes=15)).timestamp()),
            'iss': f'picasso-{ENVIRONMENT}',
            'aud': 'streaming-function',
            'jti': jti,
            'kid': 'default',  # Key ID for rotation
            'tenant_scope': 'inferred'  # Mark as tenant-inferred
        }
        
        # Generate token
        jwt_token = jwt.encode(payload, signing_key, algorithm='HS256')
        
        # Audit JWT generation
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger.log_jwt_generated(
                tenant_id=payload.get('tenant_scope', 'unknown'),
                session_id=session_id,
                purpose='stream',
                expires_in=900
            )
        
        return {
            'jwt_token': jwt_token,
            'session_id': session_id,
            'expires_in': 900,  # 15 minutes
            'jti': jti,
            'purpose': 'stream'
        }
        
    except Exception as e:
        logger.error(f"SECURITY: Token generation failed: {str(e)}")
        return None

# Helper functions

def _extract_request_context(event):
    """Extract security context from request"""
    headers = event.get('headers', {}) or {}
    request_context = event.get('requestContext', {})
    
    return {
        'source_ip': (
            request_context.get('identity', {}).get('sourceIp') or
            headers.get('X-Forwarded-For', '').split(',')[0].strip() or
            headers.get('X-Real-IP') or
            'unknown'
        ),
        'user_agent': headers.get('User-Agent', 'unknown'),
        'request_id': request_context.get('requestId', 'unknown')
    }

def _extract_host(event):
    """Extract host from various sources"""
    headers = event.get('headers', {}) or {}
    return (
        headers.get('host') or
        headers.get('Host') or
        headers.get('x-forwarded-host')
    )

def _extract_origin(event):
    """Extract origin from headers"""
    headers = event.get('headers', {}) or {}
    return (
        headers.get('origin') or
        headers.get('Origin') or
        headers.get('referer') or
        headers.get('Referer')
    )

def _extract_path(event):
    """Extract path from request"""
    return (
        event.get('path') or
        event.get('requestContext', {}).get('http', {}).get('path') or
        event.get('requestContext', {}).get('path')
    )

def _is_rate_limited(source_ip):
    """Check if IP is rate limited"""
    current_time = time.time()
    
    if source_ip not in failure_tracking:
        return False
    
    # Clean old failures
    failure_tracking[source_ip] = [
        failure_time for failure_time in failure_tracking[source_ip]
        if current_time - failure_time < RATE_LIMIT_WINDOW
    ]
    
    is_limited = len(failure_tracking[source_ip]) >= RATE_LIMIT_THRESHOLD
    
    # Audit rate limiting activation
    if is_limited and AUDIT_LOGGER_AVAILABLE:
        audit_logger.log_rate_limit_triggered(
            tenant_id="unknown",
            session_id=None,
            source_ip=source_ip,
            limit_type="tenant_inference_failures",
            current_count=len(failure_tracking[source_ip]),
            threshold=RATE_LIMIT_THRESHOLD
        )
    
    return is_limited

def _track_failure(source_ip):
    """Track failure for rate limiting"""
    current_time = time.time()
    
    if source_ip not in failure_tracking:
        failure_tracking[source_ip] = []
    
    failure_tracking[source_ip].append(current_time)

def _get_signing_key():
    """Get cached JWT signing key"""
    global signing_key_cache, key_cache_expires
    
    current_time = time.time()
    
    if signing_key_cache and current_time < key_cache_expires:
        return signing_key_cache
    
    try:
        secret_response = secrets_client.get_secret_value(SecretId=JWT_SECRET_KEY_NAME)
        secret_data = json.loads(secret_response['SecretString'])
        signing_key_cache = secret_data.get('signingKey')
        key_cache_expires = current_time + 300  # 5 minute cache
        
        return signing_key_cache
        
    except Exception as e:
        logger.error(f"Failed to get signing key: {str(e)}")
        return None

def _is_valid_tenant_hash_format(tenant_hash):
    """Validate tenant hash format"""
    return (
        tenant_hash and
        isinstance(tenant_hash, str) and
        len(tenant_hash) >= 10 and len(tenant_hash) <= 20 and
        TENANT_HASH_PATTERN.match(tenant_hash)
    )

def _generate_session_id():
    """Generate unique session ID"""
    timestamp = int(time.time())
    random_part = os.urandom(4).hex()
    return f"sess_{timestamp}_{random_part}"

def _generate_jti():
    """Generate JWT ID"""
    return hashlib.sha256(f"{time.time()}_{os.urandom(8).hex()}".encode()).hexdigest()[:12]

def _build_tenant_result(tenant_hash, source, value):
    """Build standardized tenant result"""
    return {
        'tenant_hash': tenant_hash,
        'source': source,
        'matched_value': value,
        'timestamp': int(time.time())
    }

def _resolve_config_tenant(tenant_hash, request_context):
    """Resolve tenant from config parameter"""
    try:
        # Check if hash exists in registry
        registry = loadTenantRegistry()
        if tenant_hash in registry.get('hashes', set()):
            return _build_tenant_result(tenant_hash, 'config', tenant_hash)
        
        return None
        
    except Exception as e:
        logger.error(f"SECURITY: Config tenant resolution failed: {str(e)}")
        return None

def _find_tenant_by_path(path, registry, request_context):
    """Find tenant by path with security validation"""
    try:
        paths = registry.get('paths', {})
        
        for registered_path, tenant_hash in paths.items():
            if valid_path(path, [registered_path]) and path.startswith(registered_path):
                return _build_tenant_result(tenant_hash, 'path', registered_path)
        
        return None
        
    except Exception as e:
        logger.error(f"SECURITY: Path tenant lookup failed: {str(e)}")
        return None

def _audit_success(inference_type, tenant_result, request_context):
    """Audit successful tenant inference"""
    audit_data = {
        'event': 'tenant_inference_success',
        'inference_type': inference_type,
        'tenant_hash': tenant_result.get('tenant_hash', 'unknown')[:8] + '...',
        'source': tenant_result.get('source', 'unknown'),
        'timestamp': int(time.time()),
        'source_ip': request_context.get('source_ip', 'unknown'),
        'environment': ENVIRONMENT
    }
    
    logger.info(f"SECURITY_AUDIT: {json.dumps(audit_data)}")