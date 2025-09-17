"""
PICASSO State Clear Handler with Audit Integration
Demonstrates lean audit system integration for state management operations
"""

import json
import time
import boto3
import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Initialize audit logger
try:
    from audit_logger import audit_logger
    AUDIT_LOGGER_AVAILABLE = True
    logger.info("✅ audit_logger module loaded successfully")
except ImportError as e:
    logger.warning(f"⚠️ audit_logger not available: {e}")
    AUDIT_LOGGER_AVAILABLE = False

class StateClearHandler:
    """
    State clearing operations with comprehensive audit logging
    Demonstrates the audit system integration patterns
    """
    
    def __init__(self):
        self.dynamodb = boto3.client('dynamodb')
        self.s3 = boto3.client('s3')
    
    def handle_state_clear_request(self, tenant_id: str, session_id: str = None, 
                                  clear_type: str = "full", requester_ip: str = None) -> Dict[str, Any]:
        """
        Handle state clear request with full audit logging
        Demonstrates comprehensive audit integration
        """
        start_time = time.time()
        
        # Audit the state clear request
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger.log_state_clear_requested(
                tenant_id=tenant_id,
                session_id=session_id,
                clear_type=clear_type,
                requester_ip=requester_ip
            )
        
        try:
            logger.info(f"Starting state clear operation for tenant {tenant_id[:8]}...")
            
            # Perform state clearing based on type
            if clear_type == "full":
                items_cleared = self._clear_full_state(tenant_id)
            elif clear_type == "cache_only":
                items_cleared = self._clear_cache_only(tenant_id)
            elif clear_type == "session":
                items_cleared = self._clear_session_state(tenant_id, session_id)
            else:
                raise ValueError(f"Invalid clear_type: {clear_type}")
            
            # Calculate operation duration
            duration_ms = (time.time() - start_time) * 1000
            
            # Audit successful completion
            if AUDIT_LOGGER_AVAILABLE:
                audit_logger.log_state_clear_completed(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    clear_type=clear_type,
                    items_cleared=items_cleared,
                    duration_ms=duration_ms
                )
            
            # Check performance target
            if duration_ms > 200:  # 200ms target
                logger.warning(f"State clear exceeded performance target: {duration_ms:.2f}ms > 200ms")
            
            logger.info(f"State clear completed: {items_cleared} items cleared in {duration_ms:.2f}ms")
            
            return {
                'success': True,
                'tenant_id': tenant_id[:8] + "...",
                'clear_type': clear_type,
                'items_cleared': items_cleared,
                'duration_ms': round(duration_ms, 2),
                'timestamp': int(time.time())
            }
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_type = type(e).__name__
            
            logger.error(f"State clear failed for {tenant_id[:8]}...: {str(e)}")
            
            # Audit the failure
            if AUDIT_LOGGER_AVAILABLE:
                audit_logger.log_state_clear_failed(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    clear_type=clear_type,
                    error_type=error_type,
                    partial_success=False
                )
            
            return {
                'success': False,
                'error': str(e),
                'error_type': error_type,
                'tenant_id': tenant_id[:8] + "...",
                'clear_type': clear_type,
                'duration_ms': round(duration_ms, 2),
                'timestamp': int(time.time())
            }
    
    def _clear_full_state(self, tenant_id: str) -> int:
        """
        Clear all state for a tenant (simulation)
        In production, this would clear:
        - Session data from DynamoDB
        - Cached configurations
        - Temporary files from S3
        - Any other tenant-specific state
        """
        # Simulate clearing operations
        items_cleared = 0
        
        # Simulate clearing session data
        logger.info(f"Clearing session data for {tenant_id[:8]}...")
        time.sleep(0.01)  # Simulate DB operations
        items_cleared += 5  # Simulate 5 session records cleared
        
        # Simulate clearing cache data
        logger.info(f"Clearing cache data for {tenant_id[:8]}...")
        time.sleep(0.01)  # Simulate cache operations
        items_cleared += 3  # Simulate 3 cache entries cleared
        
        # Simulate clearing temporary files
        logger.info(f"Clearing temporary files for {tenant_id[:8]}...")
        time.sleep(0.02)  # Simulate S3 operations
        items_cleared += 2  # Simulate 2 temp files cleared
        
        return items_cleared
    
    def _clear_cache_only(self, tenant_id: str) -> int:
        """Clear only cached data for a tenant"""
        logger.info(f"Clearing cache-only for {tenant_id[:8]}...")
        time.sleep(0.01)  # Simulate cache clear
        return 3  # Simulate 3 cache entries cleared
    
    def _clear_session_state(self, tenant_id: str, session_id: str) -> int:
        """Clear state for a specific session"""
        logger.info(f"Clearing session {session_id} for {tenant_id[:8]}...")
        time.sleep(0.005)  # Simulate session clear
        return 1  # Simulate 1 session cleared

def handle_state_clear_action(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for state clear operations
    Integrates with main lambda function
    """
    try:
        # Extract request parameters
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)
        
        query_params = event.get('queryStringParameters', {}) or {}
        
        # Get parameters from body or query
        tenant_id = body.get('tenant_id') or query_params.get('t')
        session_id = body.get('session_id') or query_params.get('session_id')
        clear_type = body.get('clear_type') or query_params.get('clear_type', 'full')
        
        # Extract requester IP
        request_context = event.get('requestContext', {})
        headers = event.get('headers', {}) or {}
        requester_ip = (
            request_context.get('identity', {}).get('sourceIp') or
            headers.get('X-Forwarded-For', '').split(',')[0].strip() or
            headers.get('X-Real-IP') or
            'unknown'
        )
        
        if not tenant_id:
            # Audit unauthorized request
            if AUDIT_LOGGER_AVAILABLE:
                audit_logger.log_unauthorized_access(
                    tenant_id="unknown",
                    session_id=None,
                    resource="state_clear",
                    action="clear_state",
                    source_ip=requester_ip,
                    reason="missing_tenant_id"
                )
            
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Missing tenant_id',
                    'usage': 'POST with {"tenant_id": "...", "clear_type": "full|cache_only|session"}'
                })
            }
        
        # Validate clear_type
        valid_types = ['full', 'cache_only', 'session']
        if clear_type not in valid_types:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': f'Invalid clear_type: {clear_type}',
                    'valid_types': valid_types
                })
            }
        
        # Create handler and process request
        handler = StateClearHandler()
        result = handler.handle_state_clear_request(
            tenant_id=tenant_id,
            session_id=session_id,
            clear_type=clear_type,
            requester_ip=requester_ip
        )
        
        # Return response with secure CORS
        status_code = 200 if result['success'] else 500
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Apply secure CORS validation
        try:
            # Import the secure CORS validation function
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from lambda_function import validate_cors_origin
            
            allowed_origin, is_valid = validate_cors_origin(headers, tenant_id, None)
            
            if allowed_origin:
                headers["Access-Control-Allow-Origin"] = allowed_origin
                headers["Access-Control-Allow-Credentials"] = "true"
                logger.info(f"[{tenant_id[:8] if tenant_id else 'unknown'}...] SECURE CORS: State clear response with origin {allowed_origin}")
            elif not is_valid:
                # CORS violation - browser will reject
                logger.warning(f"[{tenant_id[:8] if tenant_id else 'unknown'}...] CORS VIOLATION: Origin rejected in state clear response")
            else:
                # Direct API access
                logger.info(f"[{tenant_id[:8] if tenant_id else 'unknown'}...] Direct API access - no CORS headers in state clear response")
        except Exception as e:
            logger.error(f"Error validating CORS in state clear response: {e}")
            # Fail closed - don't set CORS headers on error
        
        return {
            'statusCode': status_code,
            'headers': headers,
            'body': json.dumps(result)
        }
        
    except Exception as e:
        logger.error(f"State clear handler error: {str(e)}")
        
        # Audit system error
        if AUDIT_LOGGER_AVAILABLE:
            audit_logger.log_state_clear_failed(
                tenant_id="unknown",
                session_id=None,
                clear_type="unknown",
                error_type=type(e).__name__,
                partial_success=False
            )
        
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            })
        }

# Example usage and testing
if __name__ == '__main__':
    """
    Local testing of state clear functionality
    """
    import os
    
    # Set environment for testing
    os.environ['ENVIRONMENT'] = 'development'
    
    # Test the handler
    handler = StateClearHandler()
    
    print("Testing State Clear Handler with Audit Integration")
    print("=" * 60)
    
    # Test full clear
    print("\n1. Testing full state clear...")
    result = handler.handle_state_clear_request(
        tenant_id="test_tenant_12345",
        session_id="sess_abc123",
        clear_type="full",
        requester_ip="192.168.1.100"
    )
    print(f"Result: {json.dumps(result, indent=2)}")
    
    # Test cache-only clear
    print("\n2. Testing cache-only clear...")
    result = handler.handle_state_clear_request(
        tenant_id="test_tenant_12345",
        session_id="sess_abc123",
        clear_type="cache_only",
        requester_ip="192.168.1.100"
    )
    print(f"Result: {json.dumps(result, indent=2)}")
    
    # Test error case
    print("\n3. Testing error case...")
    result = handler.handle_state_clear_request(
        tenant_id="test_tenant_12345",
        session_id="sess_abc123",
        clear_type="invalid_type",
        requester_ip="192.168.1.100"
    )
    print(f"Result: {json.dumps(result, indent=2)}")
    
    print("\n✅ State clear handler testing completed")
    print("Check audit logs to see the PII-free audit trail")