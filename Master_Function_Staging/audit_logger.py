"""
PICASSO Lean Audit System - PII-Free SaaS Compliance
Production-ready audit logging with zero PII storage
"""

import json
import boto3
import hashlib
import time
import re
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configuration
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
AUDIT_TABLE_NAME = f"picasso-audit-{ENVIRONMENT}"
DEFAULT_RETENTION_DAYS = 90
MAX_RETENTION_DAYS = 365

# Performance targets
AUDIT_TIMEOUT_MS = 10
QUERY_TIMEOUT_MS = 500
STATE_CLEAR_TIMEOUT_MS = 200

# PII Detection Patterns - Comprehensive but lean
PII_PATTERNS = {
    'email': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
    'phone': re.compile(r'(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),
    'ssn': re.compile(r'\b\d{3}-?\d{2}-?\d{4}\b'),
    'credit_card': re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b'),
    'name_patterns': re.compile(r'\b(first_?name|last_?name|full_?name|display_?name)\b', re.IGNORECASE),
    'conversation': re.compile(r'\b(message|content|text|conversation|chat|reply|help|account|problem|need|assistance)\b', re.IGNORECASE)
}

# Event severity levels
class AuditSeverity:
    LOW = "LOW"
    MEDIUM = "MEDIUM" 
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

# Event categories mapping to severity
EVENT_SEVERITY_MAP = {
    # Authentication & Tenant Events (LOW-MEDIUM)
    'AUTH_JWT_GENERATED': AuditSeverity.LOW,
    'AUTH_JWT_VALIDATED': AuditSeverity.LOW,
    'AUTH_JWT_INVALID': AuditSeverity.MEDIUM,
    'TENANT_INFERRED': AuditSeverity.LOW,
    'TENANT_INFERENCE_FAILED': AuditSeverity.MEDIUM,
    
    # Security Events (HIGH-CRITICAL)
    'SECURITY_CROSS_TENANT_ATTEMPT': AuditSeverity.CRITICAL,
    'SECURITY_RATE_LIMIT_TRIGGERED': AuditSeverity.HIGH,
    'SECURITY_INVALID_JWT_FLOOD': AuditSeverity.CRITICAL,
    'SECURITY_UNAUTHORIZED_ACCESS': AuditSeverity.HIGH,
    
    # State Management (MEDIUM)
    'STATE_CLEAR_REQUESTED': AuditSeverity.MEDIUM,
    'STATE_CLEAR_COMPLETED': AuditSeverity.MEDIUM,
    'STATE_CLEAR_FAILED': AuditSeverity.HIGH,
    
    # Handoff Events (LOW-MEDIUM)
    'HANDOFF_TO_SECURE_FORM': AuditSeverity.MEDIUM,
    'HANDOFF_COMPLETED': AuditSeverity.LOW
}

class AuditLogger:
    """
    Lean, PII-free audit logger for SaaS compliance
    Designed for high performance with <10ms overhead
    """
    
    def __init__(self):
        self.dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
        self.cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)
        
        # Performance tracking
        self._start_time = None
        self._metrics_buffer = []
    
    def _hash_tenant_id(self, tenant_id: str) -> str:
        """Create consistent hash for tenant partitioning"""
        if not tenant_id:
            return "unknown_tenant"
        return hashlib.sha256(f"tenant_{tenant_id}_{ENVIRONMENT}".encode()).hexdigest()[:16]
    
    def _generate_sort_key(self, event_type: str) -> str:
        """Generate timestamp-based sort key with event info"""
        now = datetime.utcnow()
        timestamp = now.strftime("%Y%m%dT%H%M%S")
        event_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_evt_{event_id}_{event_type}"
    
    def _calculate_ttl(self, retention_days: int = DEFAULT_RETENTION_DAYS) -> int:
        """Calculate TTL for automatic cleanup"""
        retention_days = min(retention_days, MAX_RETENTION_DAYS)
        expiry_date = datetime.utcnow() + timedelta(days=retention_days)
        return int(expiry_date.timestamp())
    
    def _scan_for_pii(self, data: Any) -> Any:
        """
        Aggressive PII detection and redaction
        Scans all string values recursively
        """
        if isinstance(data, dict):
            return {k: self._scan_for_pii(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._scan_for_pii(item) for item in data]
        elif isinstance(data, str):
            return self._redact_pii_from_string(data)
        else:
            return data
    
    def _redact_pii_from_string(self, text: str) -> str:
        """Redact PII patterns from string"""
        if not text or not isinstance(text, str):
            return text
        
        # Apply all PII patterns
        redacted = text
        for pattern_name, pattern in PII_PATTERNS.items():
            redacted = pattern.sub(f"[REDACTED_{pattern_name.upper()}]", redacted)
        
        # Additional safety - truncate very long strings that might contain conversation data
        if len(redacted) > 200:
            redacted = redacted[:197] + "..."
        
        return redacted
    
    def _create_audit_item(self, tenant_id: str, event_type: str, session_id: str, 
                          context: Dict[str, Any], severity: str = None, 
                          retention_days: int = DEFAULT_RETENTION_DAYS) -> Dict[str, Any]:
        """Create DynamoDB audit item with PII protection"""
        
        # Start performance tracking
        self._start_time = time.time()
        
        # Hash tenant for partitioning
        tenant_hash = self._hash_tenant_id(tenant_id)
        
        # Generate sort key
        sort_key = self._generate_sort_key(event_type)
        
        # Determine severity
        if not severity:
            severity = EVENT_SEVERITY_MAP.get(event_type, AuditSeverity.MEDIUM)
        
        # Clean context of PII
        clean_context = self._scan_for_pii(context) if context else {}
        
        # Create integrity hash
        content_for_hash = f"{tenant_hash}_{sort_key}_{event_type}_{session_id}"
        integrity_hash = hashlib.sha256(content_for_hash.encode()).hexdigest()[:16]
        
        # Build audit item
        item = {
            'tenant_hash': {'S': tenant_hash},
            'timestamp_event_id': {'S': sort_key},
            'event_type': {'S': event_type},
            'session_id': {'S': session_id or 'unknown'},
            'timestamp': {'S': datetime.utcnow().isoformat() + 'Z'},
            'context': {'S': json.dumps(clean_context, separators=(',', ':'))},
            'severity': {'S': severity},
            'retention_expires_at': {'N': str(self._calculate_ttl(retention_days))},
            'integrity_hash': {'S': integrity_hash},
            'environment': {'S': ENVIRONMENT}
        }
        
        return item
    
    def _log_audit_event(self, tenant_id: str, event_type: str, session_id: str = None,
                        context: Dict[str, Any] = None, severity: str = None,
                        retention_days: int = DEFAULT_RETENTION_DAYS) -> bool:
        """
        Core audit logging method with performance monitoring
        Returns True if logged successfully, False otherwise
        """
        try:
            # Create audit item
            item = self._create_audit_item(
                tenant_id=tenant_id,
                event_type=event_type, 
                session_id=session_id,
                context=context,
                severity=severity,
                retention_days=retention_days
            )
            
            # Write to DynamoDB with timeout
            try:
                self.dynamodb.put_item(
                    TableName=AUDIT_TABLE_NAME,
                    Item=item,
                    ConditionExpression='attribute_not_exists(timestamp_event_id)'  # Prevent duplicates
                )
                
                # Track performance
                if self._start_time:
                    duration_ms = (time.time() - self._start_time) * 1000
                    self._track_performance_metric('AuditLogDuration', duration_ms)
                
                # Buffer CloudWatch metric
                self._buffer_metric('AuditEventsLogged', 1, [
                    {'Name': 'EventType', 'Value': event_type},
                    {'Name': 'Severity', 'Value': severity or 'MEDIUM'},
                    {'Name': 'Environment', 'Value': ENVIRONMENT}
                ])
                
                return True
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                logger.error(f"DynamoDB audit write failed: {error_code}")
                
                self._buffer_metric('AuditWriteFailures', 1, [
                    {'Name': 'ErrorCode', 'Value': error_code},
                    {'Name': 'Environment', 'Value': ENVIRONMENT}
                ])
                
                return False
                
        except Exception as e:
            logger.error(f"Audit logging failed: {str(e)}")
            self._buffer_metric('AuditSystemFailures', 1)
            return False
    
    # Authentication & Tenant Events
    
    def log_jwt_generated(self, tenant_id: str, session_id: str = None, 
                         purpose: str = None, expires_in: int = None) -> bool:
        """Log JWT token generation"""
        context = {
            'purpose': purpose,
            'expires_in_seconds': expires_in,
            'operation': 'jwt_generation'
        }
        return self._log_audit_event(tenant_id, 'AUTH_JWT_GENERATED', session_id, context)
    
    def log_jwt_validated(self, tenant_id: str, session_id: str = None,
                         jwt_purpose: str = None, source: str = None) -> bool:
        """Log successful JWT validation"""
        context = {
            'jwt_purpose': jwt_purpose,
            'token_source': source,  # header, query, body
            'operation': 'jwt_validation'
        }
        return self._log_audit_event(tenant_id, 'AUTH_JWT_VALIDATED', session_id, context)
    
    def log_jwt_invalid(self, tenant_id: str, session_id: str = None,
                       error_type: str = None, source_ip: str = None) -> bool:
        """Log JWT validation failure"""
        context = {
            'error_type': error_type,  # expired, malformed, invalid_signature, etc.
            'source_ip_hash': hashlib.sha256(f"{source_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if source_ip else None,
            'operation': 'jwt_validation_failed'
        }
        return self._log_audit_event(tenant_id, 'AUTH_JWT_INVALID', session_id, context)
    
    def log_tenant_inferred(self, tenant_id: str, session_id: str = None,
                           inference_method: str = None, matched_value: str = None) -> bool:
        """Log successful tenant inference"""
        context = {
            'inference_method': inference_method,  # jwt, host, path, config
            'matched_value_hash': hashlib.sha256(f"{matched_value}".encode()).hexdigest()[:8] if matched_value else None,
            'operation': 'tenant_inference'
        }
        return self._log_audit_event(tenant_id, 'TENANT_INFERRED', session_id, context)
    
    def log_tenant_inference_failed(self, tenant_id: str = "unknown", session_id: str = None,
                                   failure_reason: str = None, source_ip: str = None,
                                   user_agent_hash: str = None) -> bool:
        """Log tenant inference failure"""
        context = {
            'failure_reason': failure_reason,  # no_token, invalid_host, no_mapping, etc.
            'source_ip_hash': hashlib.sha256(f"{source_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if source_ip else None,
            'user_agent_hash': user_agent_hash,
            'operation': 'tenant_inference_failed'
        }
        return self._log_audit_event(tenant_id, 'TENANT_INFERENCE_FAILED', session_id, context)
    
    # Security Events
    
    def log_cross_tenant_attempt(self, tenant_id: str, session_id: str = None,
                                attempted_tenant: str = None, source_ip: str = None,
                                request_id: str = None) -> bool:
        """Log cross-tenant access attempt"""
        context = {
            'attempted_tenant_hash': hashlib.sha256(f"{attempted_tenant}".encode()).hexdigest()[:8] if attempted_tenant else None,
            'source_ip_hash': hashlib.sha256(f"{source_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if source_ip else None,
            'request_id': request_id,
            'operation': 'cross_tenant_security_violation'
        }
        return self._log_audit_event(tenant_id, 'SECURITY_CROSS_TENANT_ATTEMPT', session_id, context, AuditSeverity.CRITICAL)
    
    def log_rate_limit_triggered(self, tenant_id: str = "unknown", session_id: str = None,
                                source_ip: str = None, limit_type: str = None,
                                current_count: int = None, threshold: int = None) -> bool:
        """Log rate limiting activation"""
        context = {
            'source_ip_hash': hashlib.sha256(f"{source_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if source_ip else None,
            'limit_type': limit_type,  # request_rate, failure_rate, etc.
            'current_count': current_count,
            'threshold': threshold,
            'operation': 'rate_limit_triggered'
        }
        return self._log_audit_event(tenant_id, 'SECURITY_RATE_LIMIT_TRIGGERED', session_id, context, AuditSeverity.HIGH)
    
    def log_invalid_jwt_flood(self, tenant_id: str = "unknown", session_id: str = None,
                             source_ip: str = None, failure_count: int = None,
                             time_window_minutes: int = None) -> bool:
        """Log JWT flood attack detection"""
        context = {
            'source_ip_hash': hashlib.sha256(f"{source_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if source_ip else None,
            'failure_count': failure_count,
            'time_window_minutes': time_window_minutes,
            'operation': 'jwt_flood_detected'
        }
        return self._log_audit_event(tenant_id, 'SECURITY_INVALID_JWT_FLOOD', session_id, context, AuditSeverity.CRITICAL)
    
    def log_unauthorized_access(self, tenant_id: str = "unknown", session_id: str = None,
                               resource: str = None, action: str = None, source_ip: str = None,
                               reason: str = None) -> bool:
        """Log unauthorized access attempt"""
        context = {
            'resource': resource,
            'action': action,
            'source_ip_hash': hashlib.sha256(f"{source_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if source_ip else None,
            'denial_reason': reason,
            'operation': 'unauthorized_access_denied'
        }
        return self._log_audit_event(tenant_id, 'SECURITY_UNAUTHORIZED_ACCESS', session_id, context, AuditSeverity.HIGH)
    
    # State Management Events
    
    def log_state_clear_requested(self, tenant_id: str, session_id: str = None,
                                 clear_type: str = None, requester_ip: str = None) -> bool:
        """Log state clear operation request"""
        context = {
            'clear_type': clear_type,  # full, partial, cache_only
            'requester_ip_hash': hashlib.sha256(f"{requester_ip}_{ENVIRONMENT}".encode()).hexdigest()[:8] if requester_ip else None,
            'operation': 'state_clear_initiated'
        }
        return self._log_audit_event(tenant_id, 'STATE_CLEAR_REQUESTED', session_id, context, AuditSeverity.MEDIUM)
    
    def log_state_clear_completed(self, tenant_id: str, session_id: str = None,
                                 clear_type: str = None, items_cleared: int = None,
                                 duration_ms: float = None) -> bool:
        """Log successful state clear completion"""
        context = {
            'clear_type': clear_type,
            'items_cleared': items_cleared,
            'duration_ms': round(duration_ms, 2) if duration_ms else None,
            'operation': 'state_clear_completed'
        }
        return self._log_audit_event(tenant_id, 'STATE_CLEAR_COMPLETED', session_id, context, AuditSeverity.MEDIUM)
    
    def log_state_clear_failed(self, tenant_id: str, session_id: str = None,
                              clear_type: str = None, error_type: str = None,
                              partial_success: bool = False) -> bool:
        """Log state clear operation failure"""
        context = {
            'clear_type': clear_type,
            'error_type': error_type,
            'partial_success': partial_success,
            'operation': 'state_clear_failed'
        }
        return self._log_audit_event(tenant_id, 'STATE_CLEAR_FAILED', session_id, context, AuditSeverity.HIGH)
    
    # Handoff Events
    
    def log_handoff_to_secure_form(self, tenant_id: str, session_id: str = None,
                                  form_type: str = None, handoff_reason: str = None,
                                  security_level: str = None) -> bool:
        """Log handoff to secure form"""
        context = {
            'form_type': form_type,
            'handoff_reason': handoff_reason,  # pii_detected, security_upgrade, compliance_required
            'security_level': security_level,
            'operation': 'secure_form_handoff'
        }
        return self._log_audit_event(tenant_id, 'HANDOFF_TO_SECURE_FORM', session_id, context, AuditSeverity.MEDIUM)
    
    def log_handoff_completed(self, tenant_id: str, session_id: str = None,
                             form_type: str = None, completion_status: str = None,
                             duration_seconds: int = None) -> bool:
        """Log handoff completion"""
        context = {
            'form_type': form_type,
            'completion_status': completion_status,  # success, abandoned, timeout
            'duration_seconds': duration_seconds,
            'operation': 'secure_form_completed'
        }
        return self._log_audit_event(tenant_id, 'HANDOFF_COMPLETED', session_id, context, AuditSeverity.LOW)
    
    # Performance and Metrics
    
    def _track_performance_metric(self, metric_name: str, value: float) -> None:
        """Track performance metrics for monitoring"""
        if value > AUDIT_TIMEOUT_MS:
            logger.warning(f"Audit operation exceeded target: {value:.2f}ms > {AUDIT_TIMEOUT_MS}ms")
        
        self._buffer_metric(metric_name, value, [
            {'Name': 'Environment', 'Value': ENVIRONMENT}
        ], 'Milliseconds')
    
    def _buffer_metric(self, metric_name: str, value: float, 
                      dimensions: List[Dict[str, str]] = None, unit: str = 'Count') -> None:
        """Buffer CloudWatch metrics for batch sending"""
        metric_data = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.utcnow()
        }
        
        if dimensions:
            metric_data['Dimensions'] = dimensions
        
        self._metrics_buffer.append(metric_data)
        
        # Send metrics in batches to avoid API limits
        if len(self._metrics_buffer) >= 20:
            self.flush_metrics()
    
    def flush_metrics(self) -> None:
        """Flush buffered metrics to CloudWatch"""
        if not self._metrics_buffer:
            return
        
        try:
            self.cloudwatch.put_metric_data(
                Namespace=f'PICASSO/Audit/{ENVIRONMENT}',
                MetricData=self._metrics_buffer
            )
            self._metrics_buffer.clear()
        except Exception as e:
            logger.error(f"Failed to send audit metrics: {str(e)}")
            self._metrics_buffer.clear()  # Clear to prevent memory buildup
    
    # Query and Compliance Methods
    
    def query_events_by_tenant(self, tenant_id: str, hours_back: int = 24,
                              event_types: List[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Query audit events for compliance reporting"""
        try:
            tenant_hash = self._hash_tenant_id(tenant_id)
            
            # Calculate time range
            start_time = datetime.utcnow() - timedelta(hours=hours_back)
            start_key = start_time.strftime("%Y%m%dT%H%M%S")
            
            # Build query parameters
            query_params = {
                'TableName': AUDIT_TABLE_NAME,
                'KeyConditionExpression': 'tenant_hash = :tenant_hash AND timestamp_event_id >= :start_time',
                'ExpressionAttributeValues': {
                    ':tenant_hash': {'S': tenant_hash},
                    ':start_time': {'S': start_key}
                },
                'Limit': limit,
                'ScanIndexForward': False  # Most recent first
            }
            
            # Add event type filter if specified
            if event_types:
                filter_expressions = []
                for i, event_type in enumerate(event_types):
                    attr_name = f':event_type_{i}'
                    filter_expressions.append(f'event_type = {attr_name}')
                    query_params['ExpressionAttributeValues'][attr_name] = {'S': event_type}
                
                query_params['FilterExpression'] = ' OR '.join(filter_expressions)
            
            # Execute query
            response = self.dynamodb.query(**query_params)
            
            # Convert DynamoDB items to readable format
            events = []
            for item in response.get('Items', []):
                event = {
                    'event_type': item.get('event_type', {}).get('S', ''),
                    'timestamp': item.get('timestamp', {}).get('S', ''),
                    'session_id': item.get('session_id', {}).get('S', ''),
                    'severity': item.get('severity', {}).get('S', ''),
                    'context': json.loads(item.get('context', {}).get('S', '{}')),
                    'integrity_hash': item.get('integrity_hash', {}).get('S', '')
                }
                events.append(event)
            
            return events
            
        except Exception as e:
            logger.error(f"Audit query failed: {str(e)}")
            return []
    
    def get_security_summary(self, tenant_id: str, hours_back: int = 24) -> Dict[str, Any]:
        """Get security event summary for monitoring dashboard"""
        try:
            # Query security events
            security_events = [
                'SECURITY_CROSS_TENANT_ATTEMPT',
                'SECURITY_RATE_LIMIT_TRIGGERED', 
                'SECURITY_INVALID_JWT_FLOOD',
                'SECURITY_UNAUTHORIZED_ACCESS',
                'AUTH_JWT_INVALID',
                'TENANT_INFERENCE_FAILED'
            ]
            
            events = self.query_events_by_tenant(tenant_id, hours_back, security_events)
            
            # Aggregate by event type and severity
            summary = {
                'tenant_id': tenant_id[:8] + "..." if tenant_id else "unknown",
                'time_range_hours': hours_back,
                'total_security_events': len(events),
                'by_event_type': {},
                'by_severity': {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0},
                'most_recent_critical': None,
                'generated_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            for event in events:
                event_type = event['event_type']
                severity = event['severity']
                
                # Count by type
                summary['by_event_type'][event_type] = summary['by_event_type'].get(event_type, 0) + 1
                
                # Count by severity
                summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
                
                # Track most recent critical event
                if severity == 'CRITICAL' and not summary['most_recent_critical']:
                    summary['most_recent_critical'] = {
                        'event_type': event_type,
                        'timestamp': event['timestamp'],
                        'session_id': event['session_id']
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"Security summary generation failed: {str(e)}")
            return {
                'error': 'Failed to generate security summary',
                'tenant_id': tenant_id[:8] + "..." if tenant_id else "unknown"
            }

# Global audit logger instance
audit_logger = AuditLogger()

# Convenience functions for easy integration
def log_tenant_inferred(tenant_id: str, session_id: str = None, inference_method: str = None, matched_value: str = None) -> bool:
    """Quick logging for tenant inference success"""
    return audit_logger.log_tenant_inferred(tenant_id, session_id, inference_method, matched_value)

def log_security_event(event_type: str, tenant_id: str = "unknown", **kwargs) -> bool:
    """Quick logging for security events"""
    method_map = {
        'cross_tenant': audit_logger.log_cross_tenant_attempt,
        'rate_limit': audit_logger.log_rate_limit_triggered,
        'jwt_flood': audit_logger.log_invalid_jwt_flood,
        'unauthorized': audit_logger.log_unauthorized_access
    }
    
    method = method_map.get(event_type)
    if method:
        return method(tenant_id, **kwargs)
    
    logger.warning(f"Unknown security event type: {event_type}")
    return False

def get_audit_logger() -> AuditLogger:
    """Get the global audit logger instance"""
    return audit_logger