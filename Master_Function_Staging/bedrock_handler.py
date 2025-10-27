import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUBBLE_API_KEY = os.environ.get("BUBBLE_API_KEY")

bedrock_agent = boto3.client("bedrock-agent-runtime")
bedrock = boto3.client("bedrock-runtime")

def fetch_tenant_tone(tenant_id):
    logger.info(f"[{tenant_id}] ✅ Using stub - no API call made")
    return "You are a helpful and friendly assistant."

def retrieve_kb_chunks(user_input, config):  
    try:
        kb_id = config.get("aws", {}).get("knowledge_base_id")
        
        if not kb_id:
            logger.error("❌ No KB ID found in tenant config")
            return "", []

        logger.info(f"📚 Retrieving KB chunks for input: {user_input[:40]}... using KB: {kb_id}")
        
        response = bedrock_agent.retrieve(
            knowledgeBaseId=kb_id,  
            retrievalQuery={"text": user_input},
            retrievalConfiguration={
                "vectorSearchConfiguration": {
                    "numberOfResults": 8
                }
            }
        )
        
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
        return "\n\n---\n\n".join(formatted_chunks), sources
        
    except Exception as e:
        logger.error(f"❌ KB retrieval failed: {str(e)}", exc_info=True)
        return "", []

def build_prompt(user_input, query_results, tenant_tone, conversation_context=None):
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

You are a virtual assistant answering the questions of website visitors. You are always couteous and respectful and respponsd if you are an employee of the organization. Your replace words like they or their with our, which conveys that are a representative of the team. You are answering a user's question using information from a knowledge base. Your job is to provide a helpful, natural response based on the information provided below.

{conversation_history}CRITICAL INSTRUCTIONS:
1. NEVER make up or invent ANY details including program names, services, or contact information - if not explicitly in the knowledge base, don't include it
2. ALWAYS include complete URLs exactly as they appear in the search results
3. When you see a URL like https://example.com/page, include the FULL URL, not just "their website"
4. If the URL appears as a markdown link [text](url), preserve the markdown format
5. Answer the user's question comprehensively with all relevant information from the knowledge base, including impact data, outcomes, statistics, and research findings. Do NOT add "Next Steps" or "Getting Involved" sections with contact information or action prompts.

RESPONSE FORMATTING - BALANCED APPROACH:

Write responses that are both informative and well-structured:

1. **START WITH CONTEXT**: Begin with 1-2 sentences providing a warm, helpful introduction
2. **USE STRUCTURE FOR CLARITY**: After the introduction, organize information with clear headings
3. **MIX PARAGRAPHS AND LISTS**: Use short paragraphs to explain concepts, then bullet points for specific details
4. **NO CALLS-TO-ACTION**: Do NOT include action phrases like "Apply here", "Sign up today", "Contact us to get started", or "Visit our website". Action buttons are provided automatically by the system based on conversation context.
5. **USE EMOJIS SPARINGLY**:
   - Maximum 2-3 emojis per response, not in every sentence
   - If using emoji as a bullet point, use EITHER emoji OR dash (-), never both
   - Good: "📞 Call us at..." OR "- Call us at..."
   - Bad: "- 📞 Call us at..."
   - Reserve emojis for adding warmth at key moments, not decoration

KNOWLEDGE BASE INFORMATION:
{query_results}

CURRENT USER QUESTION: {user_input}

Please provide a helpful response:""".strip()

def call_claude_with_prompt(prompt, config):
    model_id = config.get("model_id", "us.anthropic.claude-3-5-haiku-20241022-v1:0")
    logger.info(f"🧠 Calling Claude model {model_id} with constructed prompt")
    
    try:
        response = bedrock.invoke_model(
            modelId=model_id,
            accept="application/json",
            contentType="application/json",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1000,
                "temperature": 0.2
            })
        )
        
        body = json.loads(response['body'].read())
        response_text = body['content'][0]['text'].strip()
        
        logger.info("✅ Claude responded successfully")
        return response_text
        
    except Exception as e:
        logger.error(f"❌ Claude invocation failed: {str(e)}", exc_info=True)
        return "I apologize, but I'm having trouble processing your request right now. Please try again later or contact support for assistance."

def lambda_handler(event, context):
    try:
        # Log the raw event for debugging
        logger.info(f"Raw event received: {json.dumps(event)[:500]}")
        
        # Handle Function URL wrapper
        if 'body' in event and isinstance(event.get('body'), str):
            logger.info("Detected Function URL event wrapper, parsing body")
            event = json.loads(event['body'])
        
        user_input = event.get('user_input', '')
        tenant_id = event.get('tenant_id', '')
        config = event.get('config', {})
        
        if not user_input:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No user input provided'})
            }
        
        # Fetch tenant tone
        tenant_tone = fetch_tenant_tone(tenant_id)
        
        # Retrieve knowledge base chunks
        query_results, sources = retrieve_kb_chunks(user_input, config)
        
        # Build prompt (no conversation context in direct lambda handler calls)
        prompt = build_prompt(user_input, query_results, tenant_tone)
        
        # Call Claude
        response = call_claude_with_prompt(prompt, config)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'response': response,
                'sources_used': len(sources) if sources else 0
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Lambda handler failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }