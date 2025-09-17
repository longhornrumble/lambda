import json
import logging
from session_utils import extract_session_data, build_session_attributes

# Try to use optimized handler, fall back to original if not available
try:
    from bedrock_handler_optimized import fetch_tenant_tone, retrieve_kb_chunks, build_prompt, call_claude_with_prompt, get_cache_status, warm_cache_for_tenant
    logging.info("✅ Using optimized bedrock handler with caching")
except ImportError:
    from bedrock_handler import fetch_tenant_tone, retrieve_kb_chunks, build_prompt, call_claude_with_prompt
    get_cache_status = None
    warm_cache_for_tenant = None
    logging.info("⚠️ Using standard bedrock handler without caching")

from response_formatter import format_lex_markdown_response, format_http_response, format_http_error

# Import hash-based config loader
try:
    from tenant_config_loader import get_config_for_tenant_by_hash
    TENANT_CONFIG_AVAILABLE = True
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("✅ tenant_config_loader imported successfully")
except ImportError as e:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.warning(f"⚠️ tenant_config_loader not available: {e}")
    TENANT_CONFIG_AVAILABLE = False

def route_intent(event, config=None, conversation_context=None):
    try:
        logger.info("📨 Routing intent request")
        
        # Extract hash from event (pure hash system)
        tenant_hash = extract_tenant_hash(event)
        user_input = extract_user_input(event)
        session_id = extract_session_id(event)

        logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] Session ID: {session_id}")
        logger.info(f"[{tenant_hash[:8] if tenant_hash else 'unknown'}...] User input: {user_input[:40]}...")
        
        # Use passed conversation_context or try to extract it from the event body
        if conversation_context:
            logger.info(f"[{tenant_hash[:8]}...] 📝 Using passed conversation context: {len(conversation_context.get('messages', []))} messages")
        else:
            try:
                body = event.get("body", "{}")
                if isinstance(body, str):
                    body = json.loads(body)
                
                # Get conversation context from request
                request_context = body.get('conversation_context', {})
                # Check for both 'recentMessages' and 'messages' keys
                messages = request_context.get('recentMessages', request_context.get('messages', []))
                if request_context and messages:
                    conversation_context = {
                        'messages': messages,
                        'recentMessages': messages,  # Support both formats
                        'session_id': body.get('session_id'),
                        'conversation_id': body.get('conversation_id'),
                        'turn': body.get('turn', 0)
                    }
                    logger.info(f"[{tenant_hash[:8]}...] 📝 Extracted conversation context from request body: {len(messages)} messages")
                else:
                    logger.info(f"[{tenant_hash[:8]}...] 📝 No conversation context found in request body")
            except Exception as e:
                logger.warning(f"Could not extract conversation context from body: {e}")

        if not tenant_hash or not user_input:
            logger.warning(f"❌ Missing tenant_hash or user_input")
            if "sessionState" in event:
                return format_lex_markdown_response("Missing tenant information or user input", {})
            else:
                return format_http_error(400, "Missing tenant information or user input")

        # Load config using hash-based system
        if not config and TENANT_CONFIG_AVAILABLE:
            try:
                config = get_config_for_tenant_by_hash(tenant_hash)
                logger.info(f"[{tenant_hash[:8]}...] ✅ Config loaded using hash")
                
                # Warm cache for this tenant if cache warming is available
                if warm_cache_for_tenant and config:
                    # Check if cache is empty (likely cold start or expired)
                    if get_cache_status:
                        cache_status = get_cache_status()
                        if cache_status['kb_cache_size'] == 0 and cache_status['response_cache_size'] == 0:
                            logger.info(f"[{tenant_hash[:8]}...] 🔥 Cold start detected, warming cache...")
                            warm_cache_for_tenant(tenant_hash, config)
                
            except Exception as e:
                logger.warning(f"[{tenant_hash[:8]}...] ⚠️ Could not load config: {e}")
        
        # 🛡️ SECURITY: NO fallback config for healthcare compliance
        # Invalid tenant hashes must return 404, never fallback configurations
        if not config:
            logger.error(f"[{tenant_hash[:8]}...] ❌ SECURITY: Tenant configuration not found - blocking access")
            if "sessionState" in event:
                return format_lex_markdown_response("Tenant configuration not found", {})
            else:
                return format_http_error(404, "Tenant configuration not found", "The requested tenant hash is not authorized or does not exist")
        
        tone = config.get("tone_prompt", "You are a helpful assistant.")
        logger.info(f"[{tenant_hash[:8]}...] 🎨 Using tone: {tone[:40]}...")

        kb_context, sources = retrieve_kb_chunks(user_input, config)
        prompt = build_prompt(user_input, kb_context, tone, conversation_context)
        
        if conversation_context and conversation_context.get('messages'):
            logger.info(f"[{tenant_hash[:8]}...] 🧠 Prompt built with {len(conversation_context['messages'])} conversation messages. Submitting to Claude...")
        else:
            logger.info(f"[{tenant_hash[:8]}...] 🧠 Prompt built without conversation history. Submitting to Claude...")

        response_text = call_claude_with_prompt(prompt, config)
        logger.info(f"[{tenant_hash[:8]}...] ✅ Claude response received")

        # Build session attributes using hash-based system
        if "sessionState" in event:
            return format_lex_markdown_response(
                response_text,
                build_session_attributes_hash(
                    tenant_hash,
                    extract_session_data(event).get("prompt_index", "0"),
                    extract_session_data(event).get("topic", "")
                )
            )
        else:
            return format_http_response(
                response_text,
                session_id,
                context={
                    "x-amz-lex:qna-search-response": response_text,
                    "x-amz-lex:qna-search-response-source": ", ".join(sources) or "unknown",
                    "tenant_hash": tenant_hash
                }
            )

    except Exception as e:
        logger.error(f"❌ Intent routing failed: {str(e)}", exc_info=True)
        return format_http_error(500, "Internal server error", str(e))

def extract_tenant_hash(event):
    """Extract tenant hash from event (hash-only system)"""
    
    tenant_hash = None
    
    # Method 1: From query parameters (action-based requests)
    query_params = event.get("queryStringParameters") or {}
    tenant_hash = query_params.get("t")
    
    # Method 2: From request body (chat requests)
    if not tenant_hash:
        try:
            body = event.get("body", "{}")
            if isinstance(body, str):
                body = json.loads(body)
            tenant_hash = body.get("tenant_hash")
        except:
            pass
    
    # Method 3: From Lex session attributes
    if not tenant_hash:
        session_attrs = event.get("sessionState", {}).get("sessionAttributes", {})
        tenant_hash = session_attrs.get("tenant_hash")
    
    # Method 4: Extract from session data utility
    if not tenant_hash:
        try:
            session = extract_session_data(event)
            tenant_hash = session.get("tenant_hash")
        except:
            pass
    
    return tenant_hash

def extract_user_input(event):
    """Extract user input from various event formats"""
    
    # Lex format
    if "inputTranscript" in event:
        return event["inputTranscript"]
    
    # HTTP API format
    try:
        body = event.get("body", "{}")
        if isinstance(body, str):
            body = json.loads(body)
        return body.get("user_input", "")
    except:
        pass
    
    # Fallback
    return event.get("user_input", "")

def extract_session_id(event):
    """Extract session ID from event"""
    
    # From Lex
    session_id = event.get("sessionId")
    if session_id:
        return session_id
    
    # From HTTP body
    try:
        body = event.get("body", "{}")
        if isinstance(body, str):
            body = json.loads(body)
        context = body.get("context", {})
        return context.get("session_id", f"session_{int(time.time())}")
    except:
        pass
    
    # From session utils
    try:
        session = extract_session_data(event)
        return session.get("session_id", f"session_{int(time.time())}")
    except:
        pass
    
    # Generate fallback
    import time
    return f"session_{int(time.time())}"

def build_session_attributes_hash(tenant_hash, prompt_index, topic):
    """Build session attributes for hash-based system"""
    
    return {
        "tenant_hash": tenant_hash,
        "prompt_index": str(prompt_index),
        "topic": str(topic),
        "cloudfront_domain": "chat.myrecruiter.ai"
    }

# 🛡️ SECURITY: Fallback config function REMOVED for healthcare compliance
# Invalid tenant hashes must return 404, never fallback configurations
# This prevents unauthorized access to ANY tenant data or chat functionality

# REMOVED: get_fallback_config() - All invalid hashes must be blocked with 404