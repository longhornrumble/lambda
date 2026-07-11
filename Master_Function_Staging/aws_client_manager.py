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
    },
    # D11: hot-path notification/fulfillment clients. Without an explicit
    # Config, boto3 defaults to 60s connect + 60s read — an SES/SNS brownout
    # would burn the whole invocation mid-form-pipeline instead of failing
    # fast like the protected DDB calls.
    'ses': {
        'connect_timeout': 2,
        'read_timeout': 5,
        'retries': 2
    },
    'sns': {
        'connect_timeout': 2,
        'read_timeout': 5,
        'retries': 2
    },
    'lambda': {
        'connect_timeout': 2,
        'read_timeout': 10,  # fulfillment invokes are InvocationType=Event
        'retries': 2
    }
}

# Circuit Breaker Configuration
CIRCUIT_BREAKER_CONFIG = {
    'failure_threshold': 5,  # Number of failures before opening circuit
    'timeout_duration': 60,  # Seconds to wait before trying again
    'half_open_max_calls': 3  # Max calls to test in half-open state
}

def _is_conditional_check_failure(exc: Exception) -> bool:
    """True for DynamoDB ConditionalCheckFailedException (expected 409s)."""
    return (
        isinstance(exc, ClientError)
        and exc.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException'
    )


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
            # D3: ConditionalCheckFailedException is application-level control
            # flow, not a service failure — analytics_writer uses it for
            # idempotency and conversation_handler for CAS version conflicts.
            # Counting it opened the breaker on benign 409 bursts, which made
            # the fail-closed blacklist check 503 every conversation on the
            # container for 60s. Re-raise without touching breaker state.
            if _is_conditional_check_failure(e):
                raise e

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


def boto_config_for(service_name: str) -> Config:
    """Public timeout Config for module-level boto3 clients/resources.

    D11 adoption path: modules that build their own clients at import time
    (form_handler, tenant_config_loader, bedrock_handler_optimized) attach
    this Config instead of routing every call through protected_call —
    fail-fast timeouts without widening circuit-breaker adoption.
    """
    return aws_client_manager._create_boto_config(service_name)

# Protected operation functions (the unused get_*_client convenience
# wrappers and protected_s3_operation were removed 2026-07-11 — zero callers)
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
    
# Global graceful degradation handler instance
graceful_degradation = GracefulDegradationHandler()