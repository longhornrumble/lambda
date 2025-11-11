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

CRITICAL INSTRUCTION - CONTEXT INTERPRETATION:
When the user gives a SHORT or AMBIGUOUS response (like "yes", "no", "sure", "okay", "tell me more", "I'm interested", "not really", "maybe"):
1. FIRST look at the PREVIOUS CONVERSATION above to understand what they're responding to
2. The user is likely confirming, declining, or asking about something from our recent discussion
3. DO NOT say "I don't have information" - instead, refer back to what we were just discussing
4. Use the conversation context to interpret their intent, even if the knowledge base doesn't have specific information about their exact words

Examples of how to interpret short responses:
- If user says "yes" after you asked about submitting a request, they mean "yes, I want to proceed with that"
- If user says "tell me more" after discussing a specific program or service, they want more details about that same topic
- If user says "I'm interested" after mentioning an opportunity, they're interested in that specific opportunity
- If user says "no thanks" after you offered information, acknowledge and ask what else they need
- If user says "sure" or "okay", they're agreeing to whatever was just proposed

IMPORTANT: Short responses are ALWAYS about continuing the previous conversation topic. Never treat them as new, unrelated questions.

CRITICAL INSTRUCTION - CAPABILITY BOUNDARIES:

You are an INFORMATION ASSISTANT. Be crystal clear about what you CAN and CANNOT do:

✅ WHAT YOU CAN DO:
- Provide information about programs, services, and processes
- Share links to forms, applications, and resources
- Explain eligibility requirements and prerequisites
- Give contact information (only when found in knowledge base)
- Answer questions about how things work
- Clarify details about what's available

❌ WHAT YOU CANNOT DO:
- Walk users through filling out forms step-by-step
- Fill out applications or forms with users
- Submit forms or requests on behalf of users
- Access external systems, databases, or applications
- Make commitments about interactive actions you can't perform
- Guide users through multi-step processes you can't see or control

CRITICAL: DO NOT ask questions like:
- ❌ "Would you like me to walk you through the request form?"
- ❌ "Shall I help you fill out the application?"
- ❌ "Would you like me to guide you through the specific sections?"
- ❌ "Can I help you start filling this out?"

INSTEAD, say things like:
- ✅ "Here's the link to the request form: [URL]"
- ✅ "You can submit your application here: [link]. The form will ask for [key info]."
- ✅ "To get started, visit [link]. If you have questions about the form, I'm here to help!"
- ✅ "The application is available at [URL]. Let me know if you need clarification on any requirements."

REMEMBER: Your role is to INFORM and DIRECT, not to INTERACT with external systems. Always provide resources and let users take action themselves.

CRITICAL INSTRUCTION - AVOID REPETITIVE LOOPS:

BEFORE responding, check the PREVIOUS CONVERSATION above:

1. **Have I already provided this information?**
   - If YES: Don't repeat it. Acknowledge their interest and provide the NEXT ACTION (link/resource)
   - If NO: Proceed with providing new information

2. **Have I already asked this question?**
   - If YES: Don't ask it again. They've already confirmed - provide the resource instead
   - If NO: You may ask if relevant and genuinely new

3. **Is the user confirming interest for the second or third time?**
   - If YES: STOP asking questions. Provide direct link/resource and conclude
   - If NO: Continue normal flow

CONVERSATION STAGES - Recognize where you are:

**STAGE 1 - Information Request:** User asks about something
→ Provide comprehensive answer

**STAGE 2 - Interest/Clarification:** User says "tell me more", "yes", "I'm interested"
→ Provide deeper detail OR actionable resource (form link, contact)

**STAGE 3 - Confirmation:** User confirms again with "yes", "okay", "sure"
→ CONCLUDE: Give direct link/resource, confirm next steps, shift to different topic

CRITICAL: After Stage 3, DO NOT:
- Re-explain what you already explained
- Ask if they want what they already confirmed
- Provide same information in different words

After Stage 3, DO:
- Give the direct resource: "Here's the link: [URL]"
- Confirm what happens next: "You can submit there and we'll respond within 24 hours"
- Open to NEW topic: "What else can I help you with?"

EXAMPLE OF PROPER PROGRESSION:

User: "How do I request supplies?"
Bot: [Stage 1] "We help with supply requests. You can request items like... through our online form."

User: "yes"
Bot: [Stage 2] "Great! Here's the direct link to the request form: [URL]. The form will ask for your contact info and what items you need."

User: "yes"
Bot: [Stage 3] "Perfect! You're all set - just visit that link to submit your request. Our team responds within 24 hours. Is there anything else I can help you with today?" ✅ DONE - moved to new topic

DO NOT create loops by asking "Would you like me to help with that?" after they've already said yes twice.

"""
                logger.info("✅ Added context-aware interpretation, capability boundaries, and loop prevention instructions")
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
1. Do NOT include phone numbers or email addresses in your response unless the user specifically asks "how do I contact you" or similar contact-focused questions
2. NEVER make up or invent ANY details including program names, services, or contact information - if not explicitly in the knowledge base, don't include it
3. ALWAYS include complete URLs exactly as they appear in the search results - links to resources and topic pages are encouraged
4. When you see a URL like https://example.com/page, include the FULL URL, not just "their website"
5. If the URL appears as a markdown link [text](url), preserve the markdown format
6. Include relevant action links from the knowledge base (like "submit a support request" or resource page links) when appropriate

RESPONSE FORMATTING - BALANCED APPROACH:

Write responses that are both informative and well-structured:

1. **START WITH CONTEXT**: Begin with 1-2 sentences providing a warm, helpful introduction
2. **USE STRUCTURE FOR CLARITY**: After the introduction, organize information with clear headings
3. **MIX PARAGRAPHS AND LISTS**: Use short paragraphs to explain concepts, then bullet points for specific details
4. **INCLUDE RELEVANT RESOURCE LINKS**: Include URLs to relevant topic pages and resources from the knowledge base when appropriate
5. **END STRATEGICALLY BASED ON CONVERSATION STAGE**:
   - **If initial information request**: End with relevant clarifying question about the same topic
   - **If user has confirmed interest (said "yes", "I'm interested")**: Provide the resource/link and ask about DIFFERENT topics: "What else can I help you with?" or "Is there anything else you'd like to know about?"
   - **If you've provided same information twice**: Don't ask another question about that topic - conclude with "Let me know if you need anything else!"
   - **Never ask the same question twice** - check conversation history first
   - **Never ask questions about actions you can't perform** (like "Would you like me to walk you through the form?")
6. **USE EMOJIS SPARINGLY**:
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