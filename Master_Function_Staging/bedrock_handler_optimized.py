import os
import json
import logging
import boto3
import hashlib
import time
from typing import Dict, Tuple, List, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUBBLE_API_KEY = os.environ.get("BUBBLE_API_KEY")

bedrock_agent = boto3.client("bedrock-agent-runtime")
bedrock = boto3.client("bedrock-runtime")

# In-memory cache for Lambda warm starts
# This cache persists between invocations in the same container
KB_CACHE = {}
RESPONSE_CACHE = {}
CACHE_TTL = 300  # 5 minutes

def get_cache_key(text: str, prefix: str = "") -> str:
    """Generate a cache key from text"""
    return f"{prefix}:{hashlib.md5(text.encode()).hexdigest()}"

def is_cache_valid(cache_entry: Dict) -> bool:
    """Check if a cache entry is still valid"""
    if not cache_entry:
        return False
    return time.time() - cache_entry.get('timestamp', 0) < CACHE_TTL

def fetch_tenant_tone(tenant_id):
    logger.info(f"[{tenant_id}] ✅ Using stub - no API call made")
    return "You are a helpful and friendly assistant."

def retrieve_kb_chunks(user_input, config):
    """
    Retrieve KB chunks with caching for common queries
    """
    try:
        kb_id = config.get("aws", {}).get("knowledge_base_id")
        
        if not kb_id:
            logger.error("❌ No KB ID found in tenant config")
            return "", []

        # Check cache first
        cache_key = get_cache_key(user_input, f"kb:{kb_id}")
        if cache_key in KB_CACHE and is_cache_valid(KB_CACHE[cache_key]):
            logger.info(f"✅ KB Cache hit for: {user_input[:40]}...")
            return KB_CACHE[cache_key]['chunks'], KB_CACHE[cache_key]['sources']

        logger.info(f"📚 Retrieving KB chunks for input: {user_input[:40]}... using KB: {kb_id}")
        
        # Time the KB retrieval
        start_time = time.time()
        
        response = bedrock_agent.retrieve(
            knowledgeBaseId=kb_id,  
            retrievalQuery={"text": user_input},
            retrievalConfiguration={
                "vectorSearchConfiguration": {
                    "numberOfResults": 5  # Reduced from 8 for faster response
                }
            }
        )
        
        kb_duration = time.time() - start_time
        logger.info(f"⏱️ KB retrieval took {kb_duration:.2f} seconds")
        
        results = response.get("retrievalResults", [])
        
        formatted_chunks = []
        sources = []
        
        for idx, result in enumerate(results, 1):
            content = result["content"]["text"]
            metadata = result.get("metadata", {})
            
            logger.info(f"🔍 Result {idx} - Content length: {len(content)} chars")
            
            # Simple formatting - no manipulation of content
            formatted_chunk = f"**Knowledge Base Result {idx}:**\n{content}"
            
            formatted_chunks.append(formatted_chunk)
            
            # Get source info if available
            source_info = metadata.get("source", f"Knowledge Base Result {idx}")
            sources.append(source_info)
        
        if not formatted_chunks:
            logger.warning(f"⚠️ No relevant information found in knowledge base for: {user_input[:40]}...")
            return "", []
        
        logger.info(f"✅ Retrieved {len(formatted_chunks)} chunks from KB")
        
        # Cache the results
        chunks_text = "\n\n---\n\n".join(formatted_chunks)
        KB_CACHE[cache_key] = {
            'chunks': chunks_text,
            'sources': sources,
            'timestamp': time.time()
        }
        
        # Clean old cache entries if cache is getting large
        if len(KB_CACHE) > 100:
            current_time = time.time()
            KB_CACHE_COPY = KB_CACHE.copy()
            for key, entry in KB_CACHE_COPY.items():
                if current_time - entry.get('timestamp', 0) > CACHE_TTL:
                    del KB_CACHE[key]
        
        return chunks_text, sources
        
    except Exception as e:
        logger.error(f"❌ KB retrieval failed: {str(e)}", exc_info=True)
        return "", []

def build_prompt(user_input, query_results, tenant_tone, conversation_context=None):
    """Build prompt - same as original"""
    logger.info(f"🧩 Building prompt with tone, retrieved content, and conversation context")
    
    # Build conversation history section
    conversation_history = ""
    if conversation_context:
        # Support both 'recentMessages' and 'messages' formats
        messages = conversation_context.get('recentMessages') or conversation_context.get('messages') or conversation_context.get('previous_messages', [])
        
        if messages:
            logger.info(f"🔗 Including {len(messages)} messages in conversation history")
            history_lines = []
            for msg in messages:
                role = msg.get('role', 'unknown')
                content = msg.get('content', msg.get('text', ''))
                
                # Skip empty messages
                if not content or content.strip() == '':
                    continue
                    
                if role == 'user':
                    history_lines.append(f"User: {content}")
                elif role == 'assistant':
                    history_lines.append(f"Assistant: {content}")
            
            if history_lines:
                conversation_history = f"""
PREVIOUS CONVERSATION:
{chr(10).join(history_lines)}

REMEMBER: The user's name and any personal information they've shared should be remembered and used in your response when appropriate.

"""
        else:
            logger.info("🔍 No messages found in conversation context")
    
    if not query_results:
        return f"""{tenant_tone}

{conversation_history}I don't have information about this topic in my knowledge base. Would you like me to connect you with someone who can help?

Current User Question: {user_input}
""".strip()
    
    return f"""{tenant_tone}

You are a virtual assistant answering the questions of website visitors. You are always courteous and respectful and respond as if you are an employee of the organization. You replace words like they or their with our, which conveys that you are a representative of the team. You are answering a user's question using information from a knowledge base. Your job is to provide a helpful, natural response based on the information provided below.

{conversation_history}ESSENTIAL INSTRUCTIONS:
- Answer the user's question using only the information from the knowledge base results below
- Use the previous conversation context to provide personalized and coherent responses
- Include ALL contact information exactly as it appears: phone numbers, email addresses, websites, and links
- PRESERVE ALL MARKDOWN FORMATTING: If you see [text](url) keep it as [text](url), not plain text
- Do not modify, shorten, or reformat any URLs, emails, or phone numbers
- When you see markdown links like [donation page](https://example.com), keep them as markdown links
- For any dates, times, or locations of events: Direct users to check the events page or contact the team for current details
- Never include placeholder text like [date], [time], [location], or [topic] in your responses
- Present information naturally without mentioning "results" or "knowledge base"
- If the information doesn't fully answer the question, say "From what I can find..." and provide what you can
- Keep all contact details and links intact and prominent in your response

KNOWLEDGE BASE INFORMATION:
{query_results}

CURRENT USER QUESTION: {user_input}

Important: ALWAYS include complete URLs exactly as they appear in the search results. When you see a URL like https://example.com/page, include the FULL URL, not just "their website" or "example.com". If the URL appears as a markdown link [text](url), preserve the markdown format.

Please provide a helpful response:""".strip()

def call_claude_with_prompt(prompt, config):
    """
    Call Claude with response caching for identical prompts
    """
    model_id = config.get("model_id", "anthropic.claude-3-haiku-20240307-v1:0")  # Use Haiku for faster responses
    
    # For initial conversations, use a simpler cache key (without full history)
    # This allows caching of common first questions
    cache_key = None
    if len(prompt) < 2000:  # Only cache shorter prompts (likely first questions)
        cache_key = get_cache_key(prompt, f"claude:{model_id}")
        if cache_key in RESPONSE_CACHE and is_cache_valid(RESPONSE_CACHE[cache_key]):
            logger.info(f"✅ Response cache hit")
            return RESPONSE_CACHE[cache_key]['response']
    
    logger.info(f"🧠 Calling Claude model {model_id} with constructed prompt")
    
    try:
        # Time the Claude call
        start_time = time.time()
        
        response = bedrock.invoke_model(
            modelId=model_id,
            accept="application/json",
            contentType="application/json",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1000,  # Reduced from default for faster response
                "temperature": 0.1  # Low temperature for consistent RAG responses and effective caching
            })
        )
        
        claude_duration = time.time() - start_time
        logger.info(f"⏱️ Claude call took {claude_duration:.2f} seconds")
        
        response_body = json.loads(response['body'].read())
        response_text = response_body.get("content", [{}])[0].get("text", "")
        
        # Cache the response if we have a cache key
        if cache_key and response_text:
            RESPONSE_CACHE[cache_key] = {
                'response': response_text,
                'timestamp': time.time()
            }
            
            # Clean old cache entries
            if len(RESPONSE_CACHE) > 50:
                current_time = time.time()
                RESPONSE_CACHE_COPY = RESPONSE_CACHE.copy()
                for key, entry in RESPONSE_CACHE_COPY.items():
                    if current_time - entry.get('timestamp', 0) > CACHE_TTL:
                        del RESPONSE_CACHE[key]
        
        logger.info(f"✅ Response generated: {response_text[:60]}...")
        return response_text
    
    except Exception as e:
        logger.error(f"❌ Failed to call Claude: {str(e)}", exc_info=True)
        return "I apologize, but I'm having trouble generating a response right now. Please try again in a moment."

def get_cache_status():
    """Get current cache statistics"""
    return {
        'kb_cache_size': len(KB_CACHE),
        'response_cache_size': len(RESPONSE_CACHE),
        'cache_ttl': CACHE_TTL
    }

def clear_caches():
    """Clear all caches"""
    KB_CACHE.clear()
    RESPONSE_CACHE.clear()
    logger.info("✅ All caches cleared")

def warm_cache_for_tenant(tenant_hash, config):
    """
    Pre-cache common questions from action cards and quick help menu
    """
    try:
        logger.info(f"🔥 Cache warming started for tenant: {tenant_hash[:8]}...")
        questions_cached = 0
        
        # Extract action card questions - check both possible locations
        questions = []
        
        # Check for action_chips (new structure)
        action_chips = config.get("action_chips", {})
        if action_chips.get("enabled"):
            for chip in action_chips.get("default_chips", []):
                if chip.get("value"):  # The actual question is in 'value', not 'text'
                    questions.append(chip["value"])
        
        # Also check branding.action_cards (old structure)
        action_cards = config.get("branding", {}).get("action_cards", [])
        for card in action_cards:
            if card.get("text"):
                questions.append(card["text"])
        
        # Extract quick help menu questions - check both possible locations
        # Check for quick_help (new structure)
        quick_help = config.get("quick_help", {})
        if quick_help.get("enabled"):
            for prompt in quick_help.get("prompts", []):
                if prompt and isinstance(prompt, str):
                    # Remove emoji prefix if present
                    clean_prompt = prompt.split(' ', 1)[-1] if ' ' in prompt else prompt
                    questions.append(clean_prompt)
        
        # Also check branding.quick_help_menu (old structure)
        quick_help_menu = config.get("branding", {}).get("quick_help_menu", {})
        if quick_help_menu.get("enabled"):
            for item in quick_help_menu.get("items", []):
                if item.get("text"):
                    questions.append(item["text"])
        
        # Pre-cache each question
        for question in questions:
            try:
                # Check if already cached
                kb_cache_key = get_cache_key(question, f"kb:{config.get('aws', {}).get('knowledge_base_id')}")
                if kb_cache_key in KB_CACHE and is_cache_valid(KB_CACHE[kb_cache_key]):
                    logger.info(f"⏭️ Already cached: {question[:40]}...")
                    continue
                
                # Retrieve KB chunks and cache them
                kb_context, sources = retrieve_kb_chunks(question, config)
                
                # Build prompt and get response
                tone = config.get("tone_prompt", "You are a helpful assistant.")
                prompt = build_prompt(question, kb_context, tone, conversation_context=None)
                response = call_claude_with_prompt(prompt, config)
                
                questions_cached += 1
                logger.info(f"✅ Cached response for: {question[:40]}...")
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to cache question '{question[:40]}...': {e}")
        
        logger.info(f"🔥 Cache warming complete: {questions_cached} questions cached")
        return questions_cached
        
    except Exception as e:
        logger.error(f"❌ Cache warming failed for tenant {tenant_hash[:8]}...: {e}")
        return 0