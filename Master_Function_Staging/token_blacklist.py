"""
PICASSO Token Blacklisting System
Healthcare-grade JWT token revocation with DynamoDB persistence
Implements immediate token invalidation for security compliance
"""

import json
import logging
import time
import boto3
import os
import hashlib
import jwt
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Environment Configuration
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
BLACKLIST_TABLE_NAME = os.environ.get('BLACKLIST_TABLE_NAME', f'picasso-token-blacklist-{ENVIRONMENT}')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
JWT_SECRET_KEY_NAME = os.environ.get('JWT_SECRET_KEY_NAME', 'picasso/jwt/signing-key')

# Performance and Security Settings
BLACKLIST_CACHE_TTL = 300  # 5 minutes cache for frequently checked tokens
MAX_BATCH_SIZE = 25  # DynamoDB batch write limit
RATE_LIMIT_REVOCATIONS = 50  # Max revocations per tenant per hour

# Import AWS client manager for timeout protection
try:
    from aws_client_manager import (
        protected_dynamodb_operation,
        protected_secrets_operation,
        timeout_handler,
        CircuitBreakerError,
        aws_client_manager
    )
    AWS_CLIENT_MANAGER_AVAILABLE = True
    logger.info("✅ AWS client manager loaded with timeout protection for blacklist")
except ImportError as e:
    logger.warning(f"⚠️ AWS client manager not available, using legacy clients: {e}")
    # Fallback to legacy clients without timeout protection
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    secrets_client = boto3.client('secretsmanager', region_name=AWS_REGION)
    AWS_CLIENT_MANAGER_AVAILABLE = False

# In-memory cache for frequently checked blacklisted tokens (security optimization)
blacklist_cache = {}
cache_expiry = {}

# Import audit logger for compliance logging
try:
    from audit_logger import audit_logger
    AUDIT_LOGGER_AVAILABLE = True
    logger.info("✅ audit_logger module loaded for token blacklist")
except ImportError as e:
    logger.warning(f"⚠️ audit_logger not available: {e}")
    AUDIT_LOGGER_AVAILABLE = False

class TokenBlacklistError(Exception):
    """Base exception for token blacklist operations"""
    def __init__(self, error_type: str, message: str, status_code: int = 500):
        self.error_type = error_type
        self.message = message
        self.status_code = status_code
        super().__init__(message)

def add_token_to_blacklist(token: str, reason: str, expires_at: datetime, tenant_id: str = None, session_id: str = None) -> Dict[str, Any]:
    """
    Add JWT token to blacklist with proper TTL and audit trail
    
    Args:
        token: JWT token to blacklist (will be hashed for storage)
        reason: Reason for blacklisting (user_logout|security_incident|admin_revoke|session_timeout)
        expires_at: When the token naturally expires (for TTL optimization)
        tenant_id: Tenant ID for audit trail (optional)
        session_id: Session ID for audit trail (optional)
    
    Returns:
        Dict containing blacklist operation result
    
    Raises:
        TokenBlacklistError: If blacklisting fails
    """
    try:
        # Validate inputs
        if not token or not reason or not expires_at:
            raise TokenBlacklistError("INVALID_INPUT", "Token, reason, and expires_at are required", 400)
        
        # Extract tenant_id and session_id from token if not provided
        if not tenant_id or not session_id:
            try:
                token_data = _decode_token_for_metadata(token)
                tenant_id = tenant_id or token_data.get('tenantId', 'unknown')
                session_id = session_id or token_data.get('sessionId', 'unknown')
            except Exception as e:
                logger.warning(f"⚠️ Could not extract metadata from token: {e}")
                tenant_id = tenant_id or 'unknown'
                session_id = session_id or 'unknown'
        
        # Generate secure hash of token for storage (never store raw JWT)
        token_hash = _hash_token(token)
        
        # Calculate TTL for automatic cleanup
        ttl_timestamp = int(expires_at.timestamp())
        current_time = datetime.utcnow()
        
        # Prepare blacklist entry
        blacklist_entry = {
            'token_hash': {'S': token_hash},
            'blacklisted_at': {'S': current_time.isoformat() + 'Z'},
            'reason': {'S': reason},
            'tenant_id': {'S': tenant_id},
            'session_id': {'S': session_id},
            'expires_at': {'N': str(ttl_timestamp)},
            'blacklist_id': {'S': f"{tenant_id}#{session_id}#{int(current_time.timestamp())}"}
        }
        
        # Store in DynamoDB with conditional check to prevent duplicates with timeout protection
        try:
            if AWS_CLIENT_MANAGER_AVAILABLE:
                try:
                    protected_dynamodb_operation(
                        'put_item',
                        TableName=BLACKLIST_TABLE_NAME,
                        Item=blacklist_entry,
                        ConditionExpression='attribute_not_exists(token_hash)'
                    )
                except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
                    logger.error(f"⏰ DynamoDB timeout adding token to blacklist {token_hash[:12]}...: {e}")
                    raise TokenBlacklistError("BLACKLIST_TIMEOUT", "Blacklist service temporarily unavailable", 503)
            else:
                # Fallback to legacy client
                dynamodb.put_item(
                    TableName=BLACKLIST_TABLE_NAME,
                    Item=blacklist_entry,
                    ConditionExpression='attribute_not_exists(token_hash)'
                )
            logger.info(f"🚫 Token blacklisted: {token_hash[:12]}... reason={reason} tenant={tenant_id[:8]}...")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.info(f"ℹ️ Token already blacklisted: {token_hash[:12]}...")
                # Return success - token is already blacklisted (idempotent operation)
            else:
                raise TokenBlacklistError("DB_ERROR", f"Failed to store blacklist entry: {str(e)}", 500)
        
        # Update in-memory cache for immediate effect
        _update_blacklist_cache(token_hash, True)
        
        # Audit the blacklist operation
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger._log_audit_event(
                tenant_id=tenant_id,
                event_type='TOKEN_BLACKLISTED',
                session_id=session_id,
                context={
                    'reason': reason,
                    'token_hash': token_hash[:12] + '...',
                    'expires_at': expires_at.isoformat() + 'Z',
                    'blacklist_operation': 'add_token'
                }
            )
        
        return {
            'success': True,
            'token_hash': token_hash[:12] + '...',
            'reason': reason,
            'blacklisted_at': current_time.isoformat() + 'Z',
            'expires_at': expires_at.isoformat() + 'Z',
            'tenant_id': tenant_id,
            'session_id': session_id
        }
        
    except TokenBlacklistError:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to add token to blacklist: {str(e)}")
        raise TokenBlacklistError("BLACKLIST_FAILED", f"Blacklist operation failed: {str(e)}", 500)

def is_token_blacklisted(token: str) -> bool:
    """
    Fast blacklist check with memory cache optimization
    Critical path function - must complete in <10ms
    
    Args:
        token: JWT token to check
    
    Returns:
        True if token is blacklisted, False otherwise
    
    Raises:
        TokenBlacklistError: If blacklist check fails (fail-closed security)
    """
    try:
        if not token:
            return True  # Fail-closed: treat empty token as blacklisted
        
        token_hash = _hash_token(token)
        
        # Check in-memory cache first (performance optimization)
        cache_result = _check_blacklist_cache(token_hash)
        if cache_result is not None:
            logger.debug(f"🎯 Cache hit for token check: {token_hash[:12]}... = {cache_result}")
            return cache_result
        
        # Check DynamoDB blacklist with timeout protection
        try:
            if AWS_CLIENT_MANAGER_AVAILABLE:
                try:
                    response = protected_dynamodb_operation(
                        'get_item',
                        TableName=BLACKLIST_TABLE_NAME,
                        Key={'token_hash': {'S': token_hash}},
                        ProjectionExpression='token_hash, expires_at'
                    )
                except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
                    logger.error(f"⏰ DynamoDB timeout checking blacklist for {token_hash[:12]}...: {e}")
                    # Fail-closed: treat timeout as blacklisted for security
                    raise TokenBlacklistError("BLACKLIST_CHECK_TIMEOUT", "Token validation service temporarily unavailable", 503)
            else:
                # Fallback to legacy client
                response = dynamodb.get_item(
                    TableName=BLACKLIST_TABLE_NAME,
                    Key={'token_hash': {'S': token_hash}},
                    ProjectionExpression='token_hash, expires_at'
                )
            
            is_blacklisted = 'Item' in response
            
            # Update cache with result
            _update_blacklist_cache(token_hash, is_blacklisted)
            
            if is_blacklisted:
                logger.info(f"🚫 Token is blacklisted: {token_hash[:12]}...")
            else:
                logger.debug(f"✅ Token not blacklisted: {token_hash[:12]}...")
            
            return is_blacklisted
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"❌ Blacklist table not found: {BLACKLIST_TABLE_NAME}")
                # Fail-closed: if blacklist table doesn't exist, consider all tokens potentially compromised
                raise TokenBlacklistError("BLACKLIST_UNAVAILABLE", "Token blacklist service unavailable", 503)
            else:
                logger.error(f"❌ DynamoDB error during blacklist check: {e}")
                # Fail-closed: on DB error, reject token for security
                raise TokenBlacklistError("BLACKLIST_CHECK_FAILED", "Blacklist verification failed", 500)
        
    except TokenBlacklistError:
        raise
    except Exception as e:
        logger.error(f"❌ Critical error in blacklist check: {str(e)}")
        # Fail-closed: on any unexpected error, reject token
        raise TokenBlacklistError("BLACKLIST_CHECK_FAILED", "Token validation service error", 500)

def revoke_tenant_tokens(tenant_id: str, reason: str, requester_context: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Revoke all active tokens for a specific tenant (emergency logout)
    Used for tenant-wide security incidents or compliance requirements
    
    Args:
        tenant_id: Tenant ID to revoke tokens for
        reason: Reason for mass revocation
        requester_context: Context about who requested the revocation
    
    Returns:
        Dict containing revocation operation result
    
    Raises:
        TokenBlacklistError: If mass revocation fails
    """
    try:
        if not tenant_id or not reason:
            raise TokenBlacklistError("INVALID_INPUT", "Tenant ID and reason are required", 400)
        
        logger.info(f"[{tenant_id[:8]}...] 🚨 Initiating tenant-wide token revocation: {reason}")
        
        # Generate a tenant-wide blacklist marker
        current_time = datetime.utcnow()
        revocation_id = f"TENANT_REVOKE_{tenant_id}_{int(current_time.timestamp())}"
        
        # Create tenant-wide revocation record (24-hour TTL for active tokens)
        expires_at = current_time + timedelta(hours=24)
        tenant_revocation_entry = {
            'token_hash': {'S': f"TENANT_REVOCATION_{tenant_id}"},
            'blacklisted_at': {'S': current_time.isoformat() + 'Z'},
            'reason': {'S': f"TENANT_REVOCATION: {reason}"},
            'tenant_id': {'S': tenant_id},
            'session_id': {'S': 'ALL_SESSIONS'},
            'expires_at': {'N': str(int(expires_at.timestamp()))},
            'revocation_id': {'S': revocation_id},
            'revocation_type': {'S': 'TENANT_WIDE'}
        }
        
        # Store tenant revocation marker with timeout protection
        try:
            if AWS_CLIENT_MANAGER_AVAILABLE:
                try:
                    protected_dynamodb_operation(
                        'put_item',
                        TableName=BLACKLIST_TABLE_NAME,
                        Item=tenant_revocation_entry
                    )
                except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
                    logger.error(f"⏰ DynamoDB timeout storing tenant revocation: {e}")
                    raise TokenBlacklistError("TENANT_REVOCATION_TIMEOUT", "Revocation service temporarily unavailable", 503)
            else:
                # Fallback to legacy client
                dynamodb.put_item(
                    TableName=BLACKLIST_TABLE_NAME,
                    Item=tenant_revocation_entry
                )
        except TokenBlacklistError:
            raise
        except ClientError as e:
            logger.error(f"❌ DynamoDB error storing tenant revocation: {e}")
            raise TokenBlacklistError("TENANT_REVOCATION_DB_ERROR", "Failed to store tenant revocation", 500)
        
        # Clear any cached tokens for this tenant (security measure)
        _clear_tenant_cache(tenant_id)
        
        # Audit the mass revocation
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger._log_audit_event(
                tenant_id=tenant_id,
                event_type='TENANT_TOKENS_REVOKED',
                session_id=revocation_id,
                context={
                    'reason': reason,
                    'revocation_type': 'tenant_wide',
                    'requester': requester_context.get('requester_id', 'system') if requester_context else 'system',
                    'expires_at': expires_at.isoformat() + 'Z'
                }
            )
        
        logger.info(f"[{tenant_id[:8]}...] ✅ Tenant-wide token revocation completed: {revocation_id}")
        
        return {
            'success': True,
            'revocation_id': revocation_id,
            'tenant_id': tenant_id,
            'reason': reason,
            'revoked_at': current_time.isoformat() + 'Z',
            'expires_at': expires_at.isoformat() + 'Z',
            'revocation_type': 'tenant_wide'
        }
        
    except TokenBlacklistError:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to revoke tenant tokens: {str(e)}")
        raise TokenBlacklistError("TENANT_REVOCATION_FAILED", f"Tenant revocation failed: {str(e)}", 500)

def cleanup_expired_blacklist_entries() -> Dict[str, Any]:
    """
    Maintenance function to clean up expired blacklist entries
    Should be called periodically by a maintenance lambda or cron job
    
    Returns:
        Dict containing cleanup operation statistics
    """
    try:
        logger.info("🧹 Starting blacklist cleanup operation")
        
        current_timestamp = int(datetime.utcnow().timestamp())
        cleaned_count = 0
        scan_count = 0
        
        # Scan for expired entries with timeout protection (DynamoDB TTL should handle this automatically, but this is a backup)
        if AWS_CLIENT_MANAGER_AVAILABLE:
            client = aws_client_manager.get_client('dynamodb')
        else:
            client = dynamodb
        
        paginator = client.get_paginator('scan')
        
        for page in paginator.paginate(
            TableName=BLACKLIST_TABLE_NAME,
            FilterExpression='expires_at < :current_time',
            ExpressionAttributeValues={':current_time': {'N': str(current_timestamp)}},
            ProjectionExpression='token_hash'
        ):
            items = page.get('Items', [])
            scan_count += len(items)
            
            # Batch delete expired entries
            if items:
                delete_requests = []
                for item in items:
                    delete_requests.append({
                        'DeleteRequest': {
                            'Key': {'token_hash': item['token_hash']}
                        }
                    })
                    
                    # Process in batches of 25 (DynamoDB limit) with timeout protection
                    if len(delete_requests) >= MAX_BATCH_SIZE:
                        try:
                            if AWS_CLIENT_MANAGER_AVAILABLE:
                                protected_dynamodb_operation(
                                    'batch_write_item',
                                    RequestItems={BLACKLIST_TABLE_NAME: delete_requests}
                                )
                            else:
                                dynamodb.batch_write_item(
                                    RequestItems={BLACKLIST_TABLE_NAME: delete_requests}
                                )
                            cleaned_count += len(delete_requests)
                        except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
                            logger.error(f"⏰ DynamoDB timeout during batch cleanup: {e}")
                            # Continue with remaining items despite timeout
                        delete_requests = []
                
                # Process remaining items with timeout protection
                if delete_requests:
                    try:
                        if AWS_CLIENT_MANAGER_AVAILABLE:
                            protected_dynamodb_operation(
                                'batch_write_item',
                                RequestItems={BLACKLIST_TABLE_NAME: delete_requests}
                            )
                        else:
                            dynamodb.batch_write_item(
                                RequestItems={BLACKLIST_TABLE_NAME: delete_requests}
                            )
                        cleaned_count += len(delete_requests)
                    except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
                        logger.error(f"⏰ DynamoDB timeout during final batch cleanup: {e}")
                        # Log but don't fail the entire cleanup
        
        # Clear memory cache of expired entries
        _cleanup_expired_cache()
        
        logger.info(f"🧹 Blacklist cleanup completed: scanned={scan_count}, cleaned={cleaned_count}")
        
        return {
            'success': True,
            'scanned_entries': scan_count,
            'cleaned_entries': cleaned_count,
            'cleanup_timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
    except Exception as e:
        logger.error(f"❌ Blacklist cleanup failed: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'cleanup_timestamp': datetime.utcnow().isoformat() + 'Z'
        }

def get_blacklist_stats(tenant_id: str = None) -> Dict[str, Any]:
    """
    Get blacklist statistics for monitoring and debugging
    
    Args:
        tenant_id: Optional tenant ID to filter stats
    
    Returns:
        Dict containing blacklist statistics
    """
    try:
        current_time = datetime.utcnow()
        stats = {
            'timestamp': current_time.isoformat() + 'Z',
            'table_name': BLACKLIST_TABLE_NAME,
            'cache_size': len(blacklist_cache),
            'total_entries': 0,
            'active_entries': 0,
            'expired_entries': 0
        }
        
        # Count entries by scanning table (expensive operation - use sparingly)
        current_timestamp = int(current_time.timestamp())
        
        scan_params = {
            'TableName': BLACKLIST_TABLE_NAME,
            'Select': 'COUNT'
        }
        
        if tenant_id:
            scan_params['FilterExpression'] = 'tenant_id = :tenant_id'
            scan_params['ExpressionAttributeValues'] = {':tenant_id': {'S': tenant_id}}
            stats['filtered_by_tenant'] = tenant_id
        
        # Count total entries with timeout protection
        try:
            if AWS_CLIENT_MANAGER_AVAILABLE:
                response = protected_dynamodb_operation('scan', **scan_params)
            else:
                response = dynamodb.scan(**scan_params)
            stats['total_entries'] = response.get('Count', 0)
        except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
            logger.error(f"⏰ DynamoDB timeout getting total entries: {e}")
            stats['total_entries'] = 'timeout_error'
        
        # Count active entries (not expired) with timeout protection
        try:
            scan_params['FilterExpression'] = 'expires_at > :current_time'
            if tenant_id:
                scan_params['FilterExpression'] += ' AND tenant_id = :tenant_id'
            scan_params['ExpressionAttributeValues'] = {':current_time': {'N': str(current_timestamp)}}
            if tenant_id:
                scan_params['ExpressionAttributeValues'][':tenant_id'] = {'S': tenant_id}
            
            if AWS_CLIENT_MANAGER_AVAILABLE:
                response = protected_dynamodb_operation('scan', **scan_params)
            else:
                response = dynamodb.scan(**scan_params)
            stats['active_entries'] = response.get('Count', 0)
        except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
            logger.error(f"⏰ DynamoDB timeout getting active entries: {e}")
            stats['active_entries'] = 'timeout_error'
        # Calculate expired entries safely
        if isinstance(stats['total_entries'], int) and isinstance(stats['active_entries'], int):
            stats['expired_entries'] = stats['total_entries'] - stats['active_entries']
        else:
            stats['expired_entries'] = 'calculation_error'
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to get blacklist stats: {str(e)}")
        return {
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

# Internal Helper Functions

def _hash_token(token: str) -> str:
    """
    Generate SHA256 hash of JWT token for secure storage
    Never stores raw JWT tokens in database
    """
    return hashlib.sha256(token.encode('utf-8')).hexdigest()

def _decode_token_for_metadata(token: str) -> Dict[str, Any]:
    """
    Decode JWT token to extract metadata (tenant_id, session_id)
    Does not validate signature - only used for metadata extraction
    """
    try:
        # Decode without verification to extract metadata
        decoded = jwt.decode(token, options={"verify_signature": False})
        return decoded
    except Exception as e:
        logger.warning(f"⚠️ Could not decode token for metadata: {e}")
        return {}

def _check_blacklist_cache(token_hash: str) -> Optional[bool]:
    """
    Check in-memory cache for blacklist status
    Returns None if not in cache, True/False if cached
    """
    global blacklist_cache, cache_expiry
    
    current_time = time.time()
    
    if token_hash in blacklist_cache:
        # Check if cache entry is still valid
        if current_time < cache_expiry.get(token_hash, 0):
            return blacklist_cache[token_hash]
        else:
            # Cache expired, remove entry
            del blacklist_cache[token_hash]
            if token_hash in cache_expiry:
                del cache_expiry[token_hash]
    
    return None

def _update_blacklist_cache(token_hash: str, is_blacklisted: bool):
    """
    Update in-memory cache with blacklist status
    Implements LRU-style cache with TTL
    """
    global blacklist_cache, cache_expiry
    
    current_time = time.time()
    expiry_time = current_time + BLACKLIST_CACHE_TTL
    
    # Prevent cache from growing too large (memory protection)
    if len(blacklist_cache) > 10000:
        _cleanup_expired_cache()
        
        # If still too large after cleanup, remove oldest entries
        if len(blacklist_cache) > 10000:
            oldest_keys = sorted(cache_expiry.keys(), key=lambda k: cache_expiry[k])[:1000]
            for key in oldest_keys:
                blacklist_cache.pop(key, None)
                cache_expiry.pop(key, None)
    
    blacklist_cache[token_hash] = is_blacklisted
    cache_expiry[token_hash] = expiry_time

def _cleanup_expired_cache():
    """
    Remove expired entries from in-memory cache
    """
    global blacklist_cache, cache_expiry
    
    current_time = time.time()
    expired_keys = [
        token_hash for token_hash, expiry in cache_expiry.items()
        if current_time >= expiry
    ]
    
    for key in expired_keys:
        blacklist_cache.pop(key, None)
        cache_expiry.pop(key, None)
    
    if expired_keys:
        logger.debug(f"🧹 Cleaned {len(expired_keys)} expired cache entries")

def _clear_tenant_cache(tenant_id: str):
    """
    Clear all cached entries for a specific tenant
    Used during tenant-wide revocations
    """
    global blacklist_cache, cache_expiry
    
    # Note: This is a simplified implementation
    # In a production system, you might want to track tenant_id in cache keys
    # For now, we clear the entire cache as a security measure
    blacklist_cache.clear()
    cache_expiry.clear()
    logger.info(f"🧹 Cleared blacklist cache for tenant security: {tenant_id[:8]}...")

def verify_blacklist_integration() -> Dict[str, Any]:
    """
    Verify that the blacklist system is properly integrated and functional
    Used for health checks and deployment validation
    """
    try:
        test_results = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'table_accessible': False,
            'cache_functional': False,
            'audit_logger_available': AUDIT_LOGGER_AVAILABLE,
            'environment': ENVIRONMENT,
            'table_name': BLACKLIST_TABLE_NAME
        }
        
        # Test DynamoDB table access with timeout protection
        try:
            if AWS_CLIENT_MANAGER_AVAILABLE:
                try:
                    protected_dynamodb_operation('describe_table', TableName=BLACKLIST_TABLE_NAME)
                    test_results['table_accessible'] = True
                except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
                    test_results['table_error'] = f"Timeout: {str(e)}"
            else:
                # Fallback to legacy client
                dynamodb.describe_table(TableName=BLACKLIST_TABLE_NAME)
                test_results['table_accessible'] = True
        except ClientError as e:
            test_results['table_error'] = str(e)
        
        # Test cache functionality
        try:
            test_hash = "test_hash_12345"
            _update_blacklist_cache(test_hash, True)
            cache_result = _check_blacklist_cache(test_hash)
            test_results['cache_functional'] = (cache_result is True)
            # Clean up test entry
            blacklist_cache.pop(test_hash, None)
            cache_expiry.pop(test_hash, None)
        except Exception as e:
            test_results['cache_error'] = str(e)
        
        test_results['overall_status'] = (
            test_results['table_accessible'] and test_results['cache_functional']
        )
        
        return test_results
        
    except Exception as e:
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'error': str(e),
            'overall_status': False
        }