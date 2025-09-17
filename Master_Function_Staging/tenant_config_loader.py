import os
import json
import logging
import boto3
import time
import re
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CloudFront Configuration
S3_BUCKET = os.environ.get("CONFIG_BUCKET", "myrecruiter-picasso")
CLOUDFRONT_DOMAIN = "chat.myrecruiter.ai"
MAPPINGS_PREFIX = "mappings"
TENANTS_PREFIX = "tenants"

s3 = boto3.client("s3")
cached_config = {}  # Cache by hash, not tenant_id
cache_timestamps = {}
hash_to_tenant_cache = {}  # Cache hash→tenant_id mappings for S3 access

# 🛡️ SECURITY: Dynamic hash validation against S3 mapping files
# No hardcoded whitelist - validation happens against actual S3 mapping files

# 🛡️ SECURITY: Tenant hash validation pattern
TENANT_HASH_PATTERN = re.compile(r'^[a-zA-Z0-9]{10,20}$')  # Alphanumeric, 10-20 chars

def is_valid_tenant_hash(tenant_hash):
    """🛡️ SECURITY: Strict tenant hash validation with dynamic S3 mapping check"""
    if not tenant_hash:
        return False
    
    # Check basic format requirements
    if not isinstance(tenant_hash, str):
        return False
        
    # Check length constraints
    if len(tenant_hash) < 10 or len(tenant_hash) > 20:
        return False
    
    # Check pattern matching (alphanumeric only)
    if not TENANT_HASH_PATTERN.match(tenant_hash):
        return False
    
    # 🛡️ SECURITY: Dynamic validation against S3 mapping files
    # This ensures only hashes with valid mapping files are allowed
    try:
        mapping_key = f"{MAPPINGS_PREFIX}/{tenant_hash}.json"
        logger.debug(f"[{tenant_hash[:8]}...] 🔍 Validating hash against S3: s3://{S3_BUCKET}/{mapping_key}")
        
        # Quick check if mapping file exists (HEAD request is faster than GET)
        s3.head_object(Bucket=S3_BUCKET, Key=mapping_key)
        logger.debug(f"[{tenant_hash[:8]}...] ✅ Hash validation passed - mapping file exists")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.warning(f"[{tenant_hash[:8]}...] ⚠️ Hash validation failed - no mapping file: {mapping_key}")
        else:
            logger.error(f"[{tenant_hash[:8]}...] ❌ S3 error during hash validation: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        logger.error(f"[{tenant_hash[:8]}...] ❌ Unexpected error during hash validation: {str(e)}")
        return False

def log_security_event(event_type, tenant_hash, additional_data=None):
    """🛡️ SECURITY: Legacy security event logging - enhanced with new monitoring"""
    # Try to use the new security monitor if available
    try:
        from security_monitor import log_unauthorized_access_attempt, log_invalid_hash_attempt, log_cross_tenant_violation
        
        # Map legacy event types to new security monitor functions
        if event_type == "invalid_hash_attempt":
            log_invalid_hash_attempt(tenant_hash)
        elif event_type == "hash_resolution_failed":
            log_unauthorized_access_attempt(tenant_hash, "config_access", "hash_resolution_failed")
        elif event_type == "cross_tenant_access":
            log_cross_tenant_violation(tenant_hash, "config_access")
        else:
            log_unauthorized_access_attempt(tenant_hash, "legacy_event", event_type)
        
        return  # Exit early if new monitoring is available
    except ImportError:
        pass  # Fall back to legacy logging
    
    # Legacy security event logging
    security_event = {
        "event_type": event_type,
        "tenant_hash": tenant_hash[:8] + "..." if tenant_hash else "None",
        "timestamp": int(time.time()),
        "source_ip": os.environ.get("SOURCE_IP", "unknown"),
        "user_agent": os.environ.get("USER_AGENT", "unknown")
    }
    
    if additional_data:
        security_event.update(additional_data)
    
    # Log to CloudWatch for security monitoring
    logger.warning(f"SECURITY_EVENT: {json.dumps(security_event)}")
    
    # 🛡️ SECURITY: Increment security metrics for monitoring
    try:
        import boto3
        cloudwatch = boto3.client('cloudwatch')
        
        # Send custom metric to CloudWatch for alerting
        cloudwatch.put_metric_data(
            Namespace='PICASSO/Security',
            MetricData=[
                {
                    'MetricName': 'UnauthorizedAccessAttempts',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [
                        {
                            'Name': 'EventType',
                            'Value': event_type
                        },
                        {
                            'Name': 'Environment',
                            'Value': os.environ.get('ENVIRONMENT', 'unknown')
                        }
                    ]
                }
            ]
        )
    except Exception as e:
        # Don't fail the function if monitoring fails
        logger.debug(f"Failed to send security metrics: {str(e)}")
    
    # 🚨 CRITICAL: For high-risk events, consider immediate notification
    critical_events = {'invalid_hash_attempt', 'hash_resolution_failed', 'chat_invalid_hash', 'cross_tenant_access'}
    if event_type in critical_events:
        # Log critical security events more prominently
        logger.error(f"CRITICAL_SECURITY_EVENT: {json.dumps(security_event)}")

def resolve_tenant_hash(tenant_hash):
    """🔒 SECURITY: Resolve tenant hash to internal tenant_id for S3 access only"""
    
    # Check cache first
    if tenant_hash in hash_to_tenant_cache:
        cache_age = time.time() - hash_to_tenant_cache[tenant_hash].get("timestamp", 0)
        if cache_age < 300:  # 5-minute cache for hash mappings
            tenant_id = hash_to_tenant_cache[tenant_hash]["tenant_id"]
            logger.info(f"[{tenant_hash[:8]}...] ✅ Using cached hash resolution: {tenant_id}")
            return tenant_id
    
    try:
        mapping_key = f"{MAPPINGS_PREFIX}/{tenant_hash}.json"
        logger.info(f"[{tenant_hash[:8]}...] 🔍 Resolving hash from S3: s3://{S3_BUCKET}/{mapping_key}")
        
        obj = s3.get_object(Bucket=S3_BUCKET, Key=mapping_key)
        mapping_data = json.loads(obj["Body"].read())
        
        # 🔧 FIXED: Better error handling for mapping data
        tenant_id = mapping_data.get("tenant_id")
        
        if not tenant_id:
            logger.error(f"[{tenant_hash[:8]}...] ❌ Mapping file exists but no tenant_id found: {mapping_data}")
            return None
        
        # 🔧 FIXED: Validate tenant_id format
        if not isinstance(tenant_id, str) or len(tenant_id) < 3:
            logger.error(f"[{tenant_hash[:8]}...] ❌ Invalid tenant_id format: {tenant_id}")
            return None
        
        # Cache the mapping
        hash_to_tenant_cache[tenant_hash] = {
            "tenant_id": tenant_id,
            "timestamp": time.time()
        }
        
        logger.info(f"[{tenant_hash[:8]}...] ✅ Hash resolved for S3 access: {tenant_id}")
        return tenant_id
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.warning(f"[{tenant_hash[:8]}...] ⚠️ Hash mapping not found: {mapping_key}")
        else:
            logger.error(f"[{tenant_hash[:8]}...] ❌ S3 error resolving hash: {e.response['Error']['Message']}")
        return None
        
    except json.JSONDecodeError as e:
        logger.error(f"[{tenant_hash[:8]}...] ❌ Invalid JSON in mapping file: {str(e)}")
        return None
        
    except Exception as e:
        logger.error(f"[{tenant_hash[:8]}...] ❌ Unexpected error resolving hash: {str(e)}")
        return None


def get_config_for_tenant_by_hash(tenant_hash):
    """🔒 PRIMARY: Hash-only config loading (only public entry point)"""
    
    # 🛡️ SECURITY: Strict tenant hash validation
    if not is_valid_tenant_hash(tenant_hash):
        logger.warning(f"❌ SECURITY: Invalid tenant hash rejected: {tenant_hash[:8] if tenant_hash else 'None'}...")
        log_security_event("invalid_hash_attempt", tenant_hash)
        raise ValueError("Invalid tenant hash")
    
    logger.info(f"[{tenant_hash[:8]}...] 🔍 Loading config by hash")
    
    # Get cache timeout from environment (default 5 minutes)
    cache_timeout = int(os.environ.get("CONFIG_CACHE_TIMEOUT", 300))
    
    # Check for manual cache clear override
    if os.environ.get("CLEAR_CACHE", "").lower() == "true":
        logger.info(f"[{tenant_hash[:8]}...] 🗑️ Manual cache clear requested")
        cached_config.clear()
        cache_timestamps.clear()
        hash_to_tenant_cache.clear()
    
    # Check if config is cached by hash
    if tenant_hash in cached_config:
        cache_age = time.time() - cache_timestamps.get(tenant_hash, 0)
        if cache_age < cache_timeout:
            logger.info(f"[{tenant_hash[:8]}...] ✅ Using cached config (age: {int(cache_age)}s)")
            config = cached_config[tenant_hash].copy()
            config = add_cloudfront_metadata_hash_only(config, tenant_hash)
            log_cache_metrics(tenant_hash, "hit", cache_age)
            return config
        else:
            logger.info(f"[{tenant_hash[:8]}...] ⏰ Cache expired (age: {int(cache_age)}s), reloading from S3")
            cached_config.pop(tenant_hash, None)
            cache_timestamps.pop(tenant_hash, None)
            log_cache_metrics(tenant_hash, "expired", cache_age)

    # 🔧 FIXED: Resolve hash to tenant_id for S3 access only
    tenant_id = resolve_tenant_hash(tenant_hash)
    if not tenant_id:
        logger.error(f"[{tenant_hash[:8]}...] ❌ SECURITY: Tenant hash resolution failed - access denied")
        log_security_event("hash_resolution_failed", tenant_hash)
        # 🛡️ SECURITY: Return None instead of fallback config to prevent unauthorized access
        return None

    # 🔧 FIXED: Validate tenant_id before using in S3 path
    if tenant_id == "undefined" or not tenant_id:
        logger.error(f"[{tenant_hash[:8]}...] ❌ SECURITY: Invalid tenant_id resolved - access denied")
        log_security_event("invalid_tenant_id", tenant_hash)
        # 🛡️ SECURITY: Return None instead of fallback config to prevent unauthorized access
        return None

    # Load from S3 using internal tenant_id structure (for file paths only)
    s3_key = f"{TENANTS_PREFIX}/{tenant_id}/{tenant_id}-config.json"
    try:
        logger.info(f"[{tenant_hash[:8]}...] 🔍 Loading config from S3: s3://{S3_BUCKET}/{s3_key}")
        start_time = time.time()
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        config = json.loads(obj["Body"].read())
        load_time = time.time() - start_time
        logger.info(f"[{tenant_hash[:8]}...] ✅ Config loaded from S3 in {int(load_time * 1000)}ms")
        
        log_cache_metrics(tenant_hash, "s3_load", load_time)
        
        # 🔒 REMOVE TENANT_ID: Strip out tenant_id from config
        config.pop("tenant_id", None)
        
        # Ensure the config has all required fields
        config = ensure_frontend_fields_hash_only(config, tenant_hash)
        
        # 🔒 HASH-ONLY: Always use hash in config and metadata
        config["tenant_hash"] = tenant_hash
        config = add_cloudfront_metadata_hash_only(config, tenant_hash)
        
        # Cache config by hash
        cached_config[tenant_hash] = config
        cache_timestamps[tenant_hash] = time.time()
        logger.info(f"[{tenant_hash[:8]}...] 💾 Config cached (timeout: {cache_timeout}s)")
        
        return config
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.warning(f"[{tenant_hash[:8]}...] ⚠️ Config file not found in S3: {s3_key}")
        else:
            logger.error(f"[{tenant_hash[:8]}...] ❌ S3 access error: {e.response['Error']['Message']}")
        
        # 🛡️ SECURITY: Return None instead of default config for invalid access
        logger.error(f"[{tenant_hash[:8]}...] ❌ SECURITY: Config file not found - access denied")
        log_security_event("config_not_found", tenant_hash) 
        return None
        
    except json.JSONDecodeError as e:
        logger.error(f"[{tenant_hash[:8]}...] ❌ SECURITY: Invalid JSON in config file - access denied")
        log_security_event("invalid_json", tenant_hash)
        return None
        
    except Exception as e:
        logger.error(f"[{tenant_hash[:8]}...] ❌ SECURITY: Unexpected error loading config - access denied")
        log_security_event("config_load_error", tenant_hash)
        return None


# 🛡️ SECURITY: Fallback config function REMOVED for healthcare compliance
# Invalid tenant hashes must return 404, never fallback configurations
# This prevents unauthorized access to ANY tenant data


def add_cloudfront_metadata_hash_only(config, tenant_hash):
    """🔒 Hash-only CloudFront metadata (no tenant_id references)"""
    
    config["_cloudfront"] = {
        "domain": CLOUDFRONT_DOMAIN,
        "enabled": True,
        "s3_bucket": S3_BUCKET,
        "tenant_hash": tenant_hash,
        "urls": {
            # ✅ Hash-only action URLs (fully consistent)
            "config_endpoint": f"https://{CLOUDFRONT_DOMAIN}/Master_Function?action=get_config&t={tenant_hash}",
            "chat_endpoint": f"https://{CLOUDFRONT_DOMAIN}/Master_Function?action=chat&t={tenant_hash}",
            "health_endpoint": f"https://{CLOUDFRONT_DOMAIN}/Master_Function?action=health_check&t={tenant_hash}",
            
            # Static URLs
            "widget_js": f"https://{CLOUDFRONT_DOMAIN}/widget.js",
            "embed_script": f"https://{CLOUDFRONT_DOMAIN}/embed/{tenant_hash}.js",
        },
        "cache_strategy": {
            "config_actions": "no-cache, must-revalidate",
            "static_assets": "public, max-age=3600",
            "embed_scripts": "public, max-age=3600"
        }
    }
    return config


def log_cache_metrics(tenant_hash, event_type, value):
    """🔒 Hash-only cache metrics logging"""
    metrics = {
        "tenant_hash": tenant_hash[:8] + "...",  # Truncated for security
        "event_type": event_type,
        "value": value,
        "timestamp": int(time.time())
    }
    
    # Log to CloudWatch (structured logging)
    logger.info(f"CACHE_METRICS: {json.dumps(metrics)}")


def ensure_frontend_fields_hash_only(config, tenant_hash):
    """🔒 Hash-only frontend field defaults (no tenant_id)"""
    
    # 🔧 Log custom overrides detected (hash-only)
    custom_overrides_detected = {
        "chat_title_color": config.get("branding", {}).get("chat_title_color"),
        "widget_icon_color": config.get("branding", {}).get("widget_icon_color"), 
        "callout_text": config.get("features", {}).get("callout", {}).get("text")
    }
    
    active_overrides = {k: v for k, v in custom_overrides_detected.items() if v is not None}
    if active_overrides:
        logger.info(f"[{tenant_hash[:8]}...] 🎨 Custom overrides detected and preserved: {list(active_overrides.keys())}")
    
    # Add default branding if missing
    if "branding" not in config:
        config["branding"] = {}
    
    # Chat title management (no tenant_id dependency)
    root_chat_title = config.get("chat_title")
    branding_chat_title = config.get("branding", {}).get("chat_title")
    
    if root_chat_title:
        config["branding"]["chat_title"] = root_chat_title
    elif branding_chat_title:
        config["chat_title"] = branding_chat_title
    else:
        config["chat_title"] = "Chat"
        config["branding"]["chat_title"] = "Chat"

    # NEW: Chat subtitle management (optional field)
    root_chat_subtitle = config.get("chat_subtitle")
    branding_chat_subtitle = config.get("branding", {}).get("chat_subtitle")
    
    if root_chat_subtitle:
        config["branding"]["chat_subtitle"] = root_chat_subtitle
    elif branding_chat_subtitle:
        config["chat_subtitle"] = branding_chat_subtitle
    # Note: No default subtitle - it's optional
    
    # Generic defaults (ONLY ADDED IF MISSING)
    branding_defaults = {
        "primary_color": "#3b82f6",
        "font_family": "Inter, sans-serif",
        "font_size_base": "14px",
        "border_radius": "12px",
        "company_logo_url": "https://chat.myrecruiter.ai/collateral/MyRecruiterLogo.png"
    }
    
    for key, default_value in branding_defaults.items():
        if key not in config["branding"]:
            config["branding"][key] = default_value
    
    # Enhanced feature set with correct structure
    if "features" not in config:
        config["features"] = {}
    
    feature_defaults = {
        "uploads": True,
        "photo_uploads": True,
        "forms": True,
        "streaming": False,
        "multilingual": False,
        "sms": False,
        "voice_input": False,
        "webchat": True,
        "qr": False,
        "bedrock_kb": True,
        "ats": False,
        "interview_scheduling": False,
        "callout": True
    }
    
    for key, default_value in feature_defaults.items():
        if key not in config["features"]:
            config["features"][key] = default_value
    
    # Add quick_help configuration with generic defaults
    if "quick_help" not in config:
        config["quick_help"] = {
            "enabled": True,
            "title": "Common Questions:",
            "toggle_text": "Help Menu ↑",
            "close_after_selection": True,
            "prompts": [
                "Tell me about your services",
                "How can I get help?",
                "What are your hours?",
                "How do I contact support?",
                "What information do you need?",
                "How does this work?"
            ]
        }
    
    # Add action_chips configuration with generic defaults
    if "action_chips" not in config:
        config["action_chips"] = {
            "enabled": True,
            "max_display": 3,
            "show_on_welcome": True,
            "show_after_responses": False,
            "default_chips": [
                {
                    "label": "Get started",
                    "value": "How do I get started?"
                },
                {
                    "label": "Learn more",
                    "value": "Tell me more about your services"
                },
                {
                    "label": "Contact us",
                    "value": "How can I contact you?"
                }
            ]
        }
    
    # Add widget_behavior configuration with defaults
    if "widget_behavior" not in config:
        config["widget_behavior"] = {
            "start_open": False,
            "remember_state": True,
            "auto_open_delay": 0
        }
    
    # Add other frontend fields
    if "welcome_message" not in config:
        config["welcome_message"] = "Hello! How can I help you today?"
    
    # Ensure CloudFront domain is set
    if "cloudfront_domain" not in config:
        config["cloudfront_domain"] = CLOUDFRONT_DOMAIN
    
    logger.info(f"[{tenant_hash[:8]}...] ✅ Config ensured - preserved {len(active_overrides)} custom overrides")
    
    return config


def clear_config_cache(tenant_hash=None):
    """🔒 Hash-only cache clearing"""
    global cached_config, cache_timestamps, hash_to_tenant_cache
    
    if tenant_hash:
        # Clear specific hash
        cached_config.pop(tenant_hash, None)
        cache_timestamps.pop(tenant_hash, None)
        hash_to_tenant_cache.pop(tenant_hash, None)
        logger.info(f"[{tenant_hash[:8]}...] 🗑️ Config cache cleared")
        log_cache_metrics(tenant_hash, "manual_clear", 0)
    else:
        # Clear all caches
        cached_config.clear()
        cache_timestamps.clear()
        hash_to_tenant_cache.clear()
        logger.info("🗑️ All config caches cleared")
        log_cache_metrics("ALL", "global_clear", len(cached_config))


def get_cache_status():
    """🔒 Hash-only cache status (no tenant_id exposure)"""
    current_time = time.time()
    cache_timeout = int(os.environ.get("CONFIG_CACHE_TIMEOUT", 300))
    
    status = {
        "tenant_configs": {},
        "hash_mappings": {},
        "summary": {
            "total_tenant_configs": len(cached_config),
            "total_hash_mappings": len(hash_to_tenant_cache),
            "cache_timeout": cache_timeout
        }
    }
    
    # Tenant config cache status (by hash)
    for tenant_hash in cached_config.keys():
        cache_age = current_time - cache_timestamps.get(tenant_hash, 0)
        status["tenant_configs"][tenant_hash[:8] + "..."] = {
            "cached": True,
            "age_seconds": int(cache_age),
            "expires_in": int(cache_timeout - cache_age),
            "expired": cache_age >= cache_timeout
        }
    
    # Hash mapping cache status (truncated for security)
    for tenant_hash, mapping_data in hash_to_tenant_cache.items():
        cache_age = current_time - mapping_data.get("timestamp", 0)
        status["hash_mappings"][tenant_hash[:8] + "..."] = {
            "age_seconds": int(cache_age),
            "expires_in": int(300 - cache_age),  # 5-minute cache for mappings
            "expired": cache_age >= 300
        }
    
    return status


def get_config_statistics():
    """🔒 Hash-only config statistics for monitoring"""
    current_time = time.time()
    cache_timeout = int(os.environ.get("CONFIG_CACHE_TIMEOUT", 300))
    
    total_configs = len(cached_config)
    expired_count = 0
    valid_count = 0
    
    for tenant_hash in cached_config.keys():
        cache_age = current_time - cache_timestamps.get(tenant_hash, 0)
        if cache_age >= cache_timeout:
            expired_count += 1
        else:
            valid_count += 1
    
    statistics = {
        "total_cached_configs": total_configs,
        "valid_cache_entries": valid_count,
        "expired_cache_entries": expired_count,
        "cache_timeout_seconds": cache_timeout,
        "hash_mappings_cached": len(hash_to_tenant_cache),
        "timestamp": int(current_time)
    }
    
    return statistics


# 🔧 DEBUG: Test function to verify hash resolution
def debug_hash_resolution(tenant_hash):
    """Debug function to test hash resolution"""
    logger.info(f"🧪 DEBUG: Testing hash resolution for {tenant_hash[:8]}...")
    
    # Test mapping lookup
    mapping_key = f"{MAPPINGS_PREFIX}/{tenant_hash}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=mapping_key)
        mapping_data = json.loads(obj["Body"].read())
        logger.info(f"✅ DEBUG: Mapping found: {mapping_data}")
        
        tenant_id = mapping_data.get("tenant_id")
        if tenant_id:
            logger.info(f"✅ DEBUG: Resolved tenant_id: {tenant_id}")
            
            # Test config file existence
            config_key = f"{TENANTS_PREFIX}/{tenant_id}/{tenant_id}-config.json"
            try:
                s3.head_object(Bucket=S3_BUCKET, Key=config_key)
                logger.info(f"✅ DEBUG: Config file exists: {config_key}")
            except ClientError:
                logger.warning(f"⚠️ DEBUG: Config file missing: {config_key}")
        else:
            logger.error(f"❌ DEBUG: No tenant_id in mapping: {mapping_data}")
            
    except Exception as e:
        logger.error(f"❌ DEBUG: Hash resolution failed: {str(e)}")
        
    return True