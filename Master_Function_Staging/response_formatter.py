import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CloudFront Configuration
CLOUDFRONT_DOMAIN = "chat.myrecruiter.ai"

def format_lex_markdown_response(response_text, session_attributes):
    """Format Lex markdown response with CloudFront context"""
    logger.info("📝 Formatting Lex markdown response with CloudFront integration")
    
    # Add CloudFront context to session attributes if not present
    if "cloudfront_domain" not in session_attributes:
        session_attributes["cloudfront_domain"] = CLOUDFRONT_DOMAIN
    
    return {
        "sessionState": {
            "sessionAttributes": session_attributes,
            "dialogAction": {"type": "Close"}
        },
        "messages": [
            {
                "contentType": "CustomPayload",
                "content": json.dumps({
                    "x-amz-lex:response-format": "markdown",
                    "message": response_text,
                    "cloudfront_domain": CLOUDFRONT_DOMAIN,
                    "delivery_method": "cloudfront"
                })
            }
        ]
    }

def format_http_response(message, session_id, context=None, request_headers=None, tenant_hash=None):
    """
    Format HTTP response with CloudFront optimization headers and secure CORS
    Security: Implements tenant-specific CORS validation
    """
    logger.info(f"📦 Formatting HTTP response for session_id: {session_id} via CloudFront")
    
    # Enhance context with CloudFront information
    enhanced_context = context or {}
    enhanced_context.update({
        "cloudfront_domain": CLOUDFRONT_DOMAIN,
        "delivery_method": "cloudfront",
        "cache_strategy": "no-cache"  # Chat responses should not be cached
    })
    
    # Base headers without CORS
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,x-api-key,Authorization",
        "Cache-Control": "no-cache, must-revalidate",  # Chat responses are dynamic
        "CloudFront-Domain": CLOUDFRONT_DOMAIN,
        "X-Delivery-Method": "cloudfront",
        "X-Response-Type": "chat"
    }
    
    # Apply secure CORS validation
    try:
        # Import the secure CORS validation function
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from lambda_function import validate_cors_origin
        
        allowed_origin, is_valid = validate_cors_origin(request_headers, tenant_hash, None)
        
        if allowed_origin:
            headers["Access-Control-Allow-Origin"] = allowed_origin
            headers["Access-Control-Allow-Credentials"] = "true"
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] SECURE CORS: HTTP response with origin {allowed_origin}")
        elif not is_valid:
            # CORS violation - browser will reject
            logger.warning(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] CORS VIOLATION: Origin rejected in HTTP response")
        else:
            # Direct API access
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] Direct API access - no CORS headers in HTTP response")
    except Exception as e:
        logger.error(f"Error validating CORS in HTTP response: {e}")
        # Fail closed - don't set CORS headers on error
    
    return {
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps({
            "type": "text",
            "content": message,
            "session_id": session_id,
            "context": enhanced_context,
            "_cloudfront": {
                "domain": CLOUDFRONT_DOMAIN,
                "response_time": "server-generated",
                "cache_status": "no-cache"
            }
        })
    }

def format_http_error(status_code, message, details=None, request_headers=None, tenant_hash=None):
    """Format error response with CloudFront-appropriate headers and secure CORS"""
    logger.warning(f"⚠️ Formatting error response: {status_code} - {message} via CloudFront")
    
    # Base headers without CORS
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,x-api-key,Authorization",
        "Cache-Control": "no-cache, must-revalidate",  # Errors should not be cached
        "CloudFront-Domain": CLOUDFRONT_DOMAIN,
        "X-Delivery-Method": "cloudfront",
        "X-Response-Type": "error"
    }
    
    # Apply secure CORS validation
    try:
        # Import the secure CORS validation function
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from lambda_function import validate_cors_origin
        
        allowed_origin, is_valid = validate_cors_origin(request_headers, tenant_hash, None)
        
        if allowed_origin:
            headers["Access-Control-Allow-Origin"] = allowed_origin
            headers["Access-Control-Allow-Credentials"] = "true"
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] SECURE CORS: Error response with origin {allowed_origin}")
        elif not is_valid:
            # CORS violation - browser will reject
            logger.warning(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] CORS VIOLATION: Origin rejected in error response")
        else:
            # Direct API access
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] Direct API access - no CORS headers in error response")
    except Exception as e:
        logger.error(f"Error validating CORS in error response: {e}")
        # Fail closed - don't set CORS headers on error
    
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps({
            "error": message,
            "details": details,
            "status_code": status_code,
            "_cloudfront": {
                "domain": CLOUDFRONT_DOMAIN,
                "error_source": "lambda",
                "cache_status": "no-cache"
            }
        })
    }

def format_config_response(config_data, tenant_id, request_headers=None, tenant_hash=None):
    """Format config response optimized for CloudFront caching with secure CORS"""
    logger.info(f"⚙️ Formatting config response for {tenant_id} via CloudFront")
    
    # Base headers without CORS
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,x-api-key,Authorization",
        "Cache-Control": "no-cache, must-revalidate",  # Configs change frequently
        "CloudFront-Domain": CLOUDFRONT_DOMAIN,
        "X-Delivery-Method": "cloudfront",
        "X-Response-Type": "config",
        "X-Tenant-ID": tenant_id
    }
    
    # Apply secure CORS validation
    try:
        # Import the secure CORS validation function
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from lambda_function import validate_cors_origin
        
        allowed_origin, is_valid = validate_cors_origin(request_headers, tenant_hash, config_data)
        
        if allowed_origin:
            headers["Access-Control-Allow-Origin"] = allowed_origin
            headers["Access-Control-Allow-Credentials"] = "true"
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] SECURE CORS: Config response with origin {allowed_origin}")
        elif not is_valid:
            # CORS violation - browser will reject
            logger.warning(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] CORS VIOLATION: Origin rejected in config response")
        else:
            # Direct API access
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] Direct API access - no CORS headers in config response")
    except Exception as e:
        logger.error(f"Error validating CORS in config response: {e}")
        # Fail closed - don't set CORS headers on error
    
    return {
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps({
            **config_data,
            "_delivery": {
                "cloudfront_domain": CLOUDFRONT_DOMAIN,
                "delivery_method": "cloudfront",
                "cache_policy": "no-cache",
                "tenant_id": tenant_id
            }
        })
    }

def format_static_asset_response(asset_content, content_type, tenant_id=None, request_headers=None, tenant_hash=None):
    """Format static asset response for CloudFront caching (e.g., embed scripts) with secure CORS"""
    logger.info(f"📄 Formatting static asset response via CloudFront")
    
    cache_control = "public, max-age=3600"  # Cache static assets for 1 hour
    if content_type == "application/javascript":
        cache_control = "public, max-age=3600, immutable"  # JavaScript can be cached longer
    
    headers = {
        "Content-Type": content_type,
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": cache_control,
        "CloudFront-Domain": CLOUDFRONT_DOMAIN,
        "X-Delivery-Method": "cloudfront",
        "X-Response-Type": "static-asset"
    }
    
    # Apply secure CORS validation for static assets
    try:
        # Import the secure CORS validation function
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from lambda_function import validate_cors_origin
        
        allowed_origin, is_valid = validate_cors_origin(request_headers, tenant_hash, None)
        
        if allowed_origin:
            headers["Access-Control-Allow-Origin"] = allowed_origin
            headers["Access-Control-Allow-Credentials"] = "true"
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] SECURE CORS: Static asset response with origin {allowed_origin}")
        elif not is_valid:
            # CORS violation - browser will reject
            logger.warning(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] CORS VIOLATION: Origin rejected in static asset response")
        else:
            # Direct API access
            logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] Direct API access - no CORS headers in static asset response")
    except Exception as e:
        logger.error(f"Error validating CORS in static asset response: {e}")
        # Fail closed - don't set CORS headers on error
    
    if tenant_id:
        headers["X-Tenant-ID"] = tenant_id
    
    return {
        "statusCode": 200,
        "headers": headers,
        "body": asset_content
    }