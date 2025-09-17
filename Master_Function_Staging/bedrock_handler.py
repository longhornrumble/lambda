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