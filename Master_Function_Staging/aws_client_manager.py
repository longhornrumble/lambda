"""
AWS Client Manager with Timeout Protection and Circuit Breaker Patterns
Provides centralized AWS client configuration with comprehensive timeout protection
Implements circuit breaker patterns to prevent system hangs when external services are slow
"""

import json
import logging
import time
import boto3
import os
from typing import Dict, Any, Optional, Callable, Union
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Environment Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Timeout Configuration (in seconds)
TIMEOUT_CONFIG = {
    'dynamodb': {
        'connect_timeout': 3,
        'read_timeout': 5,
        'retries': 2
    },
    'secretsmanager': {
        'connect_timeout': 2,
        'read_timeout': 3,
        'retries': 2
    },
    's3': {
        'connect_timeout': 2,
        'read_timeout': 3,
        'retries': 2
    },
    'bedrock': {
        'connect_timeout': 5,
        'read_timeout': 30,  # Bedrock needs more time for AI operations
        'retries': 1
    },
    'cloudwatch': {
        'connect_timeout': 2,
        'read_timeout': 5,
        'retries': 2
    }
}

# Circuit Breaker Configuration
CIRCUIT_BREAKER_CONFIG = {
    'failure_threshold': 5,  # Number of failures before opening circuit
    'timeout_duration': 60,  # Seconds to wait before trying again
    'half_open_max_calls': 3  # Max calls to test in half-open state
}

class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open"""
    def __init__(self, service_name: str, message: str = None):
        self.service_name = service_name
        self.message = message or f"Circuit breaker is open for {service_name}"
        super().__init__(self.message)

class CircuitBreaker:
    """
    Lightweight circuit breaker implementation for AWS services
    Tracks failures and prevents calls when service is unhealthy
    """
    
    def __init__(self, service_name: str, failure_threshold: int = 5, timeout: int = 60):
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.half_open_calls = 0
        self.max_half_open_calls = CIRCUIT_BREAKER_CONFIG['half_open_max_calls']
        
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerError: If circuit is open
            Original exception: If function fails
        """
        current_time = time.time()
        
        # Check circuit state
        if self.state == 'OPEN':
            if current_time - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
                self.half_open_calls = 0
                logger.info(f"🔄 Circuit breaker for {self.service_name} moving to HALF_OPEN")
            else:
                logger.warning(f"🚫 Circuit breaker for {self.service_name} is OPEN - blocking call")
                raise CircuitBreakerError(self.service_name)
        
        # Execute function
        try:
            if self.state == 'HALF_OPEN':
                self.half_open_calls += 1
                
            result = func(*args, **kwargs)
            
            # Success - reset failure count and close circuit
            if self.state == 'HALF_OPEN':
                logger.info(f"✅ Circuit breaker for {self.service_name} closing after successful call")
                self.state = 'CLOSED'
                self.failure_count = 0
                self.half_open_calls = 0
            elif self.failure_count > 0:
                self.failure_count = max(0, self.failure_count - 1)  # Gradual recovery
                
            return result
            
        except Exception as e:
            # Record failure
            self.failure_count += 1
            self.last_failure_time = current_time
            
            # Check if we should open the circuit
            if self.state == 'CLOSED' and self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logger.error(f"💥 Circuit breaker for {self.service_name} opening after {self.failure_count} failures")
            elif self.state == 'HALF_OPEN':
                self.state = 'OPEN'
                logger.error(f"💥 Circuit breaker for {self.service_name} reopening after failure in HALF_OPEN")
                
            # Log timeout-specific failures
            if isinstance(e, (ConnectTimeoutError, ReadTimeoutError)):
                logger.error(f"⏰ Timeout error in {self.service_name}: {str(e)}")
            
            raise e
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status"""
        return {
            'service': self.service_name,
            'state': self.state,
            'failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time,
            'half_open_calls': self.half_open_calls if self.state == 'HALF_OPEN' else None
        }

class AWSClientManager:
    """
    Centralized AWS client manager with timeout protection and circuit breakers
    Provides consistent timeout configuration across all AWS services
    """
    
    def __init__(self):
        self.clients = {}
        self.circuit_breakers = {}
        self._initialize_circuit_breakers()
        
    def _initialize_circuit_breakers(self):
        """Initialize circuit breakers for all AWS services"""
        for service_name in TIMEOUT_CONFIG.keys():
            self.circuit_breakers[service_name] = CircuitBreaker(
                service_name=service_name,
                failure_threshold=CIRCUIT_BREAKER_CONFIG['failure_threshold'],
                timeout=CIRCUIT_BREAKER_CONFIG['timeout_duration']
            )
            
    def _create_boto_config(self, service_name: str) -> Config:
        """Create boto3 configuration with timeouts and retries"""
        timeout_config = TIMEOUT_CONFIG.get(service_name, TIMEOUT_CONFIG['s3'])
        
        return Config(
            region_name=AWS_REGION,
            connect_timeout=timeout_config['connect_timeout'],
            read_timeout=timeout_config['read_timeout'],
            retries={
                'max_attempts': timeout_config['retries'],
                'mode': 'adaptive'
            },
            # Additional performance optimizations
            max_pool_connections=50,
            parameter_validation=False  # Skip client-side validation for performance
        )
    
    def get_client(self, service_name: str) -> boto3.client:
        """
        Get or create AWS client with timeout configuration
        
        Args:
            service_name: AWS service name (dynamodb, s3, secretsmanager, etc.)
            
        Returns:
            Configured boto3 client
        """
        if service_name not in self.clients:
            config = self._create_boto_config(service_name)
            self.clients[service_name] = boto3.client(service_name, config=config)
            logger.info(f"🔧 Created {service_name} client with timeout protection")
            
        return self.clients[service_name]
    
    def protected_call(self, service_name: str, operation: str, **kwargs) -> Any:
        """
        Execute AWS operation with circuit breaker protection
        
        Args:
            service_name: AWS service name
            operation: Operation to execute (e.g., 'get_item', 'put_item')
            **kwargs: Operation parameters
            
        Returns:
            Operation result
            
        Raises:
            CircuitBreakerError: If circuit breaker is open
            Original AWS exception: If operation fails
        """
        client = self.get_client(service_name)
        circuit_breaker = self.circuit_breakers[service_name]
        
        # Get the operation method
        operation_method = getattr(client, operation)
        
        # Execute with circuit breaker protection
        return circuit_breaker.call(operation_method, **kwargs)
    
    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """Get status of all circuit breakers"""
        return {
            service: breaker.get_status() 
            for service, breaker in self.circuit_breakers.items()
        }
    
    def reset_circuit_breaker(self, service_name: str):
        """Reset circuit breaker for a specific service"""
        if service_name in self.circuit_breakers:
            breaker = self.circuit_breakers[service_name]
            breaker.state = 'CLOSED'
            breaker.failure_count = 0
            breaker.half_open_calls = 0
            logger.info(f"🔄 Reset circuit breaker for {service_name}")

# Graceful degradation cache for service timeouts
service_cache = {
    'secrets': {},  # Cached secrets with TTL
    's3_configs': {},  # Cached S3 configurations
    'tenant_validations': {}  # Cached tenant validation results
}

cache_ttl = {
    'secrets': {},
    's3_configs': {},
    'tenant_validations': {}
}

# Cache configuration
CACHE_CONFIG = {
    'secrets_ttl': 300,  # 5 minutes for secrets
    's3_configs_ttl': 600,  # 10 minutes for S3 configs
    'tenant_validations_ttl': 120,  # 2 minutes for tenant validations
    'max_cache_size': 1000  # Maximum items per cache type
}

def get_from_cache(cache_type: str, key: str) -> Optional[Any]:
    """Get item from cache if not expired"""
    current_time = time.time()
    
    if key in service_cache.get(cache_type, {}):
        expiry_time = cache_ttl.get(cache_type, {}).get(key, 0)
        if current_time < expiry_time:
            logger.debug(f"📦 Cache hit for {cache_type}: {key[:20]}...")
            return service_cache[cache_type][key]
        else:
            # Remove expired entry
            service_cache[cache_type].pop(key, None)
            cache_ttl[cache_type].pop(key, None)
    
    return None

def set_cache(cache_type: str, key: str, value: Any, ttl_seconds: int = None) -> None:
    """Set item in cache with TTL"""
    if cache_type not in service_cache:
        service_cache[cache_type] = {}
        cache_ttl[cache_type] = {}
    
    # Prevent cache from growing too large
    if len(service_cache[cache_type]) >= CACHE_CONFIG['max_cache_size']:
        _cleanup_cache(cache_type)
    
    ttl_seconds = ttl_seconds or CACHE_CONFIG.get(f"{cache_type}_ttl", 300)
    expiry_time = time.time() + ttl_seconds
    
    service_cache[cache_type][key] = value
    cache_ttl[cache_type][key] = expiry_time
    logger.debug(f"📦 Cached {cache_type}: {key[:20]}... (TTL: {ttl_seconds}s)")

def _cleanup_cache(cache_type: str) -> None:
    """Remove expired entries and oldest items if cache is full"""
    current_time = time.time()
    cache = service_cache.get(cache_type, {})
    ttl_cache = cache_ttl.get(cache_type, {})
    
    # Remove expired entries
    expired_keys = [
        key for key, expiry in ttl_cache.items() 
        if current_time >= expiry
    ]
    
    for key in expired_keys:
        cache.pop(key, None)
        ttl_cache.pop(key, None)
    
    # If still too many items, remove oldest
    if len(cache) >= CACHE_CONFIG['max_cache_size']:
        oldest_keys = sorted(ttl_cache.keys(), key=lambda k: ttl_cache[k])[:100]
        for key in oldest_keys:
            cache.pop(key, None)
            ttl_cache.pop(key, None)
    
    if expired_keys:
        logger.info(f"🧹 Cache cleanup for {cache_type}: removed {len(expired_keys)} expired entries")

# Global instance for use across modules
aws_client_manager = AWSClientManager()

# Convenience functions for common operations
def get_dynamodb_client() -> boto3.client:
    """Get DynamoDB client with timeout protection"""
    return aws_client_manager.get_client('dynamodb')

def get_secrets_client() -> boto3.client:
    """Get Secrets Manager client with timeout protection"""
    return aws_client_manager.get_client('secretsmanager')

def get_s3_client() -> boto3.client:
    """Get S3 client with timeout protection"""
    return aws_client_manager.get_client('s3')

def get_bedrock_client() -> boto3.client:
    """Get Bedrock client with timeout protection"""
    return aws_client_manager.get_client('bedrock-runtime')

def get_cloudwatch_client() -> boto3.client:
    """Get CloudWatch client with timeout protection"""
    return aws_client_manager.get_client('cloudwatch')

# Protected operation functions
def protected_dynamodb_operation(operation: str, **kwargs) -> Any:
    """
    Execute DynamoDB operation with circuit breaker protection
    
    Args:
        operation: DynamoDB operation (get_item, put_item, query, etc.)
        **kwargs: Operation parameters
        
    Returns:
        Operation result
    """
    return aws_client_manager.protected_call('dynamodb', operation, **kwargs)

def protected_secrets_operation(operation: str, **kwargs) -> Any:
    """
    Execute Secrets Manager operation with circuit breaker protection
    
    Args:
        operation: Secrets Manager operation (get_secret_value, etc.)
        **kwargs: Operation parameters
        
    Returns:
        Operation result
    """
    return aws_client_manager.protected_call('secretsmanager', operation, **kwargs)

def protected_s3_operation(operation: str, **kwargs) -> Any:
    """
    Execute S3 operation with circuit breaker protection
    
    Args:
        operation: S3 operation (get_object, head_object, etc.)
        **kwargs: Operation parameters
        
    Returns:
        Operation result
    """
    return aws_client_manager.protected_call('s3', operation, **kwargs)

# Error handling utilities
class TimeoutHandler:
    """Utility class for handling timeout scenarios with graceful degradation"""
    
    @staticmethod
    def handle_dynamodb_timeout(operation: str, table_name: str, error: Exception) -> Dict[str, Any]:
        """
        Handle DynamoDB timeout with appropriate error response
        
        Args:
            operation: Operation that timed out
            table_name: DynamoDB table name
            error: Original exception
            
        Returns:
            Standardized error response
        """
        logger.error(f"⏰ DynamoDB timeout in {operation} for table {table_name}: {str(error)}")
        
        return {
            'error': 'SERVICE_TIMEOUT',
            'service': 'dynamodb',
            'operation': operation,
            'table': table_name,
            'message': 'Database service temporarily unavailable',
            'retry_after': 30,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    
    @staticmethod
    def handle_secrets_timeout(secret_name: str, error: Exception) -> Dict[str, Any]:
        """
        Handle Secrets Manager timeout with appropriate error response
        
        Args:
            secret_name: Secret that failed to retrieve
            error: Original exception
            
        Returns:
            Standardized error response
        """
        logger.error(f"⏰ Secrets Manager timeout for {secret_name}: {str(error)}")
        
        return {
            'error': 'SERVICE_TIMEOUT',
            'service': 'secretsmanager',
            'operation': 'get_secret_value',
            'secret': secret_name,
            'message': 'Authentication service temporarily unavailable',
            'retry_after': 15,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    
    @staticmethod
    def handle_s3_timeout(bucket: str, key: str, operation: str, error: Exception) -> Dict[str, Any]:
        """
        Handle S3 timeout with appropriate error response
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            operation: S3 operation
            error: Original exception
            
        Returns:
            Standardized error response
        """
        logger.error(f"⏰ S3 timeout in {operation} for {bucket}/{key}: {str(error)}")
        
        return {
            'error': 'SERVICE_TIMEOUT',
            'service': 's3',
            'operation': operation,
            'bucket': bucket,
            'key': key,
            'message': 'Configuration service temporarily unavailable',
            'retry_after': 20,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

class GracefulDegradationHandler:
    """Enhanced timeout handler with graceful degradation and caching"""
    
    @staticmethod
    def handle_secrets_with_cache(secret_name: str, operation_func: Callable) -> Any:
        """
        Handle secrets operation with cache fallback on timeout
        
        Args:
            secret_name: Name of the secret
            operation_func: Function that retrieves the secret
            
        Returns:
            Secret value (from cache or fresh)
            
        Raises:
            Exception if both fresh retrieval and cache fail
        """
        # Try to get from cache first during circuit breaker open state
        cache_key = f"secret_{secret_name}"
        cached_value = get_from_cache('secrets', cache_key)
        
        try:
            # Attempt fresh retrieval
            result = operation_func()
            
            # Cache successful result
            if result and 'SecretString' in result:
                set_cache('secrets', cache_key, result['SecretString'])
            
            return result
            
        except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
            logger.warning(f"⏰ Secrets timeout for {secret_name}, attempting cache fallback: {e}")
            
            if cached_value:
                logger.info(f"📦 Using cached secret for {secret_name} during service degradation")
                return {'SecretString': cached_value}
            else:
                logger.error(f"❌ No cached secret available for {secret_name}")
                raise e
    
    @staticmethod
    def handle_s3_config_with_cache(bucket: str, key: str, operation_func: Callable) -> Any:
        """
        Handle S3 configuration retrieval with cache fallback
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            operation_func: Function that retrieves the S3 object
            
        Returns:
            S3 object data (from cache or fresh)
        """
        cache_key = f"s3_{bucket}_{key}"
        cached_value = get_from_cache('s3_configs', cache_key)
        
        try:
            # Attempt fresh retrieval
            result = operation_func()
            
            # Cache successful result
            if result and 'Body' in result:
                body_content = result['Body'].read()
                result['Body'] = body_content  # Replace stream with bytes
                set_cache('s3_configs', cache_key, body_content)
            
            return result
            
        except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
            logger.warning(f"⏰ S3 timeout for {bucket}/{key}, attempting cache fallback: {e}")
            
            if cached_value:
                logger.info(f"📦 Using cached S3 config for {bucket}/{key} during service degradation")
                # Recreate the response structure
                from io import BytesIO
                return {
                    'Body': BytesIO(cached_value) if isinstance(cached_value, bytes) else BytesIO(cached_value.encode()),
                    'ContentLength': len(cached_value),
                    'LastModified': datetime.utcnow(),
                    'ETag': '"cached-content"'
                }
            else:
                logger.error(f"❌ No cached S3 config available for {bucket}/{key}")
                raise e
    
    @staticmethod
    def handle_tenant_validation_with_cache(tenant_hash: str, operation_func: Callable) -> Any:
        """
        Handle tenant validation with short-term caching for performance
        
        Args:
            tenant_hash: Tenant hash to validate
            operation_func: Function that validates the tenant
            
        Returns:
            Validation result (from cache or fresh)
        """
        cache_key = f"tenant_valid_{tenant_hash}"
        cached_value = get_from_cache('tenant_validations', cache_key)
        
        try:
            # Attempt fresh validation
            result = operation_func()
            
            # Cache successful validation (shorter TTL for security)
            set_cache('tenant_validations', cache_key, bool(result), 120)  # 2 minutes only
            
            return result
            
        except (ConnectTimeoutError, ReadTimeoutError, CircuitBreakerError) as e:
            logger.warning(f"⏰ Tenant validation timeout for {tenant_hash[:8]}..., checking cache: {e}")
            
            # For tenant validation, we're more conservative with cache usage
            if cached_value is True:  # Only use positive cache results
                logger.info(f"📦 Using cached tenant validation for {tenant_hash[:8]}... during service degradation")
                return True
            else:
                # Fail-closed for security: don't allow invalid/unknown tenants during degradation
                logger.error(f"❌ Tenant validation failed and no positive cache for {tenant_hash[:8]}...")
                raise e

# Global timeout handler instance
timeout_handler = TimeoutHandler()
graceful_degradation = GracefulDegradationHandler()

def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics for monitoring"""
    stats = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'cache_types': {}
    }
    
    for cache_type in service_cache.keys():
        cache = service_cache[cache_type]
        ttl_cache = cache_ttl[cache_type]
        current_time = time.time()
        
        active_entries = sum(
            1 for expiry in ttl_cache.values() 
            if current_time < expiry
        )
        
        stats['cache_types'][cache_type] = {
            'total_entries': len(cache),
            'active_entries': active_entries,
            'expired_entries': len(cache) - active_entries,
            'hit_potential': active_entries > 0
        }
    
    return stats

def clear_all_caches():
    """Clear all service caches (for testing or emergency)"""
    for cache_type in service_cache.keys():
        service_cache[cache_type].clear()
        cache_ttl[cache_type].clear()
    logger.info("🧹 Cleared all service caches")

def log_service_health_metrics():
    """Log circuit breaker status for monitoring"""
    status = aws_client_manager.get_circuit_breaker_status()
    
    healthy_services = sum(1 for s in status.values() if s['state'] == 'CLOSED')
    total_services = len(status)
    
    logger.info(f"📊 Service health: {healthy_services}/{total_services} services healthy")
    
    for service, breaker_status in status.items():
        if breaker_status['state'] != 'CLOSED':
            logger.warning(f"⚠️ {service} circuit breaker: {breaker_status['state']} "
                          f"(failures: {breaker_status['failure_count']})")
    
    # Include cache statistics
    cache_stats = get_cache_stats()
    
    return {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'healthy_services': healthy_services,
        'total_services': total_services,
        'service_status': status,
        'cache_statistics': cache_stats
    }