/**
 * Bedrock Streaming Handler - True Lambda Response Streaming
 * Uses awslambda.streamifyResponse for real SSE streaming
 * No JWT required - uses simple tenant_hash/session_id
 */

const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand } = require('@aws-sdk/client-bedrock-runtime');
const { BedrockAgentRuntimeClient, RetrieveCommand } = require('@aws-sdk/client-bedrock-agent-runtime');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const crypto = require('crypto');

// Default model configuration - single source of truth
const DEFAULT_MODEL_ID = 'us.anthropic.claude-3-5-haiku-20241022-v1:0';
const DEFAULT_MAX_TOKENS = 1000;
const DEFAULT_TEMPERATURE = 0.2;
const DEFAULT_TONE = 'You are a helpful assistant.';

// Lambda streaming - use the global awslambda object when available
// The awslambda global is injected by the Lambda runtime for streaming functions
const streamifyResponse = typeof awslambda !== 'undefined' && awslambda.streamifyResponse 
  ? awslambda.streamifyResponse 
  : null;

if (streamifyResponse) {
  console.log('✅ Lambda streaming support detected via awslambda global');
} else {
  console.log('⚠️ Lambda streaming not available, will use buffered response');
}

// Initialize AWS clients
const bedrock = new BedrockRuntimeClient({ region: 'us-east-1' });
const bedrockAgent = new BedrockAgentRuntimeClient({ region: 'us-east-1' });
const s3 = new S3Client({ region: 'us-east-1' });

// In-memory cache
const KB_CACHE = {};
const CONFIG_CACHE = {};
const CACHE_TTL = 300000; // 5 minutes

// Helper functions
function getCacheKey(text, prefix = '') {
  return `${prefix}:${crypto.createHash('md5').update(text).digest('hex')}`;
}

function isCacheValid(entry) {
  return entry && (Date.now() - entry.timestamp < CACHE_TTL);
}

async function loadConfig(tenantHash) {
  try {
    const cacheKey = `config:${tenantHash}`;
    if (CONFIG_CACHE[cacheKey] && isCacheValid(CONFIG_CACHE[cacheKey])) {
      console.log(`✅ Config cache hit for ${tenantHash.substring(0, 8)}...`);
      const cachedConfig = CONFIG_CACHE[cacheKey].data;
      console.log(`📋 Cached KB ID: ${cachedConfig?.aws?.knowledge_base_id || 'NOT SET'}`);
      return cachedConfig;
    }

    const bucket = process.env.CONFIG_BUCKET || 'myrecruiter-picasso';
    console.log(`🪣 Loading config from bucket: ${bucket}`);
    
    const mappingResponse = await s3.send(new GetObjectCommand({
      Bucket: bucket,
      Key: `mappings/${tenantHash}.json`
    }));
    
    const mapping = JSON.parse(await mappingResponse.Body.transformToString());
    console.log(`📍 Mapping found - tenant_id: ${mapping.tenant_id}`);
    
    if (mapping.tenant_id) {
      // Try both possible config filenames
      const configKeys = [
        `tenants/${mapping.tenant_id}/config.json`,
        `tenants/${mapping.tenant_id}/${mapping.tenant_id}-config.json`
      ];
      
      let config = null;
      for (const key of configKeys) {
        try {
          console.log(`🔍 Trying config at: ${key}`);
          const configResponse = await s3.send(new GetObjectCommand({
            Bucket: bucket,
            Key: key
          }));
          
          config = JSON.parse(await configResponse.Body.transformToString());
          console.log(`✅ Config loaded from S3 at ${key}`);
          break;
        } catch (e) {
          console.log(`❌ Config not found at ${key}`);
        }
      }
      
      if (config) {
        // Add tenant_id to config for downstream use
        config.tenant_id = mapping.tenant_id;
        CONFIG_CACHE[cacheKey] = { data: config, timestamp: Date.now() };
        console.log(`📋 KB ID in config: ${config?.aws?.knowledge_base_id || 'NOT SET'}`);
        console.log(`📋 Full AWS config:`, JSON.stringify(config?.aws || {}, null, 2));
        return config;
      }
    }
  } catch (error) {
    console.error('❌ Config load error:', error.message);
    console.error('Full error:', error);
  }
  
  return null;
}

async function retrieveKB(userInput, config) {
  const kbId = config?.aws?.knowledge_base_id;
  console.log(`🔍 KB Retrieval - KB ID: ${kbId || 'NOT SET'}`);
  console.log(`🔍 User input: "${userInput.substring(0, 50)}..."`);
  
  if (!kbId) {
    console.log('⚠️ No KB ID found in config - returning empty context');
    return '';
  }
  
  try {
    const cacheKey = getCacheKey(userInput, `kb:${kbId}`);
    if (KB_CACHE[cacheKey] && isCacheValid(KB_CACHE[cacheKey])) {
      console.log(`✅ KB cache hit`);
      const cachedData = KB_CACHE[cacheKey].data;
      console.log(`📄 Cached KB context length: ${cachedData.length} chars`);
      return cachedData;
    }
    
    console.log(`📚 Retrieving from KB: ${kbId}`);
    const response = await bedrockAgent.send(new RetrieveCommand({
      knowledgeBaseId: kbId,
      retrievalQuery: { text: userInput },
      retrievalConfiguration: {
        vectorSearchConfiguration: { numberOfResults: 3 }
      }
    }));
    
    console.log(`📊 KB Response - ${response.retrievalResults?.length || 0} results found`);
    
    const chunks = (response.retrievalResults || [])
      .map((r, i) => {
        const text = r.content?.text || '';
        console.log(`  Result ${i+1}: ${text.substring(0, 100)}...`);
        return `**Context ${i+1}:**\n${text}`;
      })
      .join('\n\n---\n\n');
    
    console.log(`✅ KB context retrieved - ${chunks.length} chars`);
    KB_CACHE[cacheKey] = { data: chunks, timestamp: Date.now() };
    return chunks;
    
  } catch (error) {
    console.error('❌ KB retrieval error:', error.message);
    console.error('Full KB error:', error);
    return '';
  }
}

function buildPrompt(userInput, kbContext, tone, conversationHistory, config) {
  const parts = [];
  
  // Start with tone prompt (matching bedrock_handler.py)
  const tonePrompt = tone || DEFAULT_TONE;
  parts.push(tonePrompt);
  
  // Add role instructions (exactly from bedrock_handler.py line 115)
  const roleInstructions = `
You are a virtual assistant answering the questions of website visitors. You are always courteous and respectful and respond as if you are an employee of the organization. You replace words like they or their with our, which conveys that you are a representative of the team. You are answering a user's question using information from a knowledge base. Your job is to provide a helpful, natural response based on the information provided below.`;
  
  parts.push(roleInstructions);
  
  console.log(`🎯 Building prompt - KB context: ${kbContext ? kbContext.length + ' chars' : 'NONE'}`);
  console.log(`💬 Conversation history: ${conversationHistory ? 'PROVIDED' : 'NONE'}`);
  
  // Add conversation history if provided (matching bedrock_handler.py format)
  if (conversationHistory && conversationHistory.length > 0) {
    parts.push('\nPREVIOUS CONVERSATION:');
    conversationHistory.forEach(msg => {
      const role = msg.role === 'user' ? 'User' : 'Assistant';
      const content = msg.content || msg.text || '';
      // Skip empty messages
      if (content && content.trim()) {
        parts.push(`${role}: ${content}`);
      }
    });
    parts.push('\nREMEMBER: The user\'s name and any personal information they\'ve shared should be remembered and used in your response when appropriate.\n');
    console.log(`✅ Added ${conversationHistory.length} messages from history`);
  }
  
  // Add essential instructions if we have KB context (from bedrock_handler.py lines 117-129)
  if (kbContext) {
    parts.push(`ESSENTIAL INSTRUCTIONS:
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
${kbContext}`);
    console.log(`✅ Added KB context to prompt`);
  } else {
    // No KB context fallback (from bedrock_handler.py lines 106-111)
    parts.push(`\nI don't have information about this topic in my knowledge base. Would you like me to connect you with someone who can help?`);
    console.log(`⚠️ No KB context - using fallback response`);
  }
  
  // Add current question and final instruction
  parts.push(`\nCURRENT USER QUESTION: ${userInput}`);
  
  if (kbContext) {
    parts.push(`\nCRITICAL INSTRUCTIONS:
1. ONLY provide contact information (phone, email, addresses) that appears in the knowledge base results
2. NEVER make up or invent contact details - if not in the knowledge base, don't include it
3. ALWAYS include complete URLs exactly as they appear in the search results
4. When you see a URL like https://example.com/page, include the FULL URL, not just "their website"
5. If the URL appears as a markdown link [text](url), preserve the markdown format
6. If no specific contact info is available, suggest visiting the website or contacting the main office

RESPONSE FORMATTING - BALANCED APPROACH:

Write responses that are both informative and well-structured:

1. **START WITH CONTEXT**: Begin with 1-2 sentences providing a warm, helpful introduction
2. **USE STRUCTURE FOR CLARITY**: After the introduction, organize information with clear headings
3. **MIX PARAGRAPHS AND LISTS**: Use short paragraphs to explain concepts, then bullet points for specific details
4. **ALWAYS INCLUDE ACTIONABLE CONTACT INFO**: Every response should end with specific ways to take action - phone numbers, emails, website links (ONLY if found in knowledge base)
5. **USE EMOJIS SPARINGLY**: 
   - Maximum 2-3 emojis per response, not in every sentence
   - If using emoji as a bullet point, use EITHER emoji OR dash (-), never both
   - Good: "📞 Call us at..." OR "- Call us at..."  
   - Bad: "- 📞 Call us at..."
   - Reserve emojis for adding warmth at key moments, not decoration

FORMAT TEMPLATE:
[Opening sentence providing context and understanding - use appropriate emoji if it adds warmth]

**Main Topic:**
[1-2 sentence explanation in paragraph form]

Key features or services:
- Specific item with brief description
- Another specific item
- Additional item if needed

[Additional paragraph if more context is helpful]

**Contact/Next Steps:**
[ALWAYS include specific contact info, links, emails, or phone numbers if available]

GOOD EXAMPLE:
"I understand you're looking for information about our grief counseling services. 💙 We offer comprehensive support designed to help you and your family through this difficult journey.

**Individual Grief Counseling:**
Our professional grief counselors provide one-on-one support in a confidential, compassionate environment. Sessions are customized to meet your individual needs and can be scheduled weekly or bi-weekly.

**Peer Support Groups:**
We also offer a seven-week support group program where you can connect with others experiencing similar loss. This provides:
- A safe space for emotional expression
- Connection with others who understand
- Professional facilitation and guidance

**Additional Support:**
Beyond regular counseling, we provide ongoing bereavement support even after a patient's passing, including our annual Celebration of Life memorial service.

**How to Get Started:**
To learn more about our grief counseling services or schedule a session, please contact our bereavement team through the information provided in our resources, or visit our website for more details.

Our team is here to support you every step of the way. 💙"

Please provide a helpful, well-structured response:`);
  }
  
  const finalPrompt = parts.join('\n');
  console.log(`📝 Final prompt length: ${finalPrompt.length} chars`);
  console.log(`📝 Prompt preview: ${finalPrompt.substring(0, 200)}...`);
  return finalPrompt;
}

/**
 * Main streaming handler - uses true streaming if available, falls back to buffered
 */
const streamingHandler = async (event, responseStream, context) => {
  console.log('🌊 True streaming handler invoked');
  
  // Handle OPTIONS requests - Function URLs handle CORS automatically when configured
  if (event.httpMethod === 'OPTIONS' || event.requestContext?.http?.method === 'OPTIONS') {
    // Don't write empty string, just end the stream
    responseStream.end();
    return;
  }
  
  // Track if stream has ended to prevent write-after-end errors
  let streamEnded = false;
  
  // Buffer for complete Q&A logging - builds in parallel without blocking
  let responseBuffer = '';
  let questionBuffer = '';
  
  // For Lambda Function URL streaming, we write the SSE response directly
  const write = (data) => {
    if (!streamEnded) {
      responseStream.write(data);
    }
  };
  
  // Send prelude to open the pipe immediately
  write(':ok\n\n');
  // Send a tiny data frame to force early paint in some UAs/proxies
  write('data: {"type":"start"}\n\n');
  
  const startTime = Date.now();
  let heartbeatInterval;
  
  try {
    // Parse request - handle both direct invocation and Function URL
    console.log('📥 Event type:', typeof event);
    console.log('📥 Event keys:', Object.keys(event));
    
    // For direct invocation, event IS the body. For Function URL, event.body contains the JSON string
    const body = event.body ? JSON.parse(event.body) : event;
    console.log('📥 Parsed body:', JSON.stringify(body).substring(0, 200));
    
    const tenantHash = body.tenant_hash || '';
    const sessionId = body.session_id || 'default';
    const userInput = body.user_input || '';
    
    if (!tenantHash || !userInput) {
      const error = !tenantHash ? 'Missing tenant_hash' : 'Missing user_input';
      write(`data: {"type": "error", "error": "${error}"}\n\n`);
      write('data: [DONE]\n\n');
      streamEnded = true;
      responseStream.end();
      return;
    }
    
    // Capture the question for logging
    questionBuffer = userInput;
    
    // Extract conversation history from the request
    const conversationHistory = body.conversation_history || 
                               body.conversation_context?.recentMessages || 
                               [];
    
    console.log(`📝 Processing: ${tenantHash.substring(0,8)}... / ${sessionId.substring(0,12)}...`);
    console.log(`💬 Conversation history: ${conversationHistory.length} messages`);
    
    // Start heartbeat to keep connection alive
    heartbeatInterval = setInterval(() => {
      // Use a data frame rather than a comment; comments can be buffered by some intermediaries
      write('data: {"type":"heartbeat"}\n\n');
      console.log('💓 Heartbeat sent');
    }, 2000);
    
    // Load config
    let config = await loadConfig(tenantHash);
    if (!config) {
      config = {
        model_id: DEFAULT_MODEL_ID,
        streaming: { max_tokens: DEFAULT_MAX_TOKENS, temperature: DEFAULT_TEMPERATURE },
        tone_prompt: DEFAULT_TONE
      };
    }
    
    // Get KB context
    const kbContext = await retrieveKB(userInput, config);
    const prompt = buildPrompt(userInput, kbContext, config.tone_prompt, conversationHistory, config);
    
    // Prepare Bedrock request - use config model or default
    const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    const maxTokens = config.streaming?.max_tokens || DEFAULT_MAX_TOKENS;
    const temperature = config.streaming?.temperature || DEFAULT_TEMPERATURE;
    
    console.log(`🚀 Invoking Bedrock with model: ${modelId}`);
    
    const command = new InvokeModelWithResponseStreamCommand({
      modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: maxTokens,
        temperature: temperature
      })
    });
    
    const response = await bedrock.send(command);
    
    let firstTokenTime = null;
    let tokenCount = 0;
    
    // Stream the response - NO BUFFERING!
    for await (const event of response.body) {
      if (event.chunk?.bytes) {
        const chunkData = JSON.parse(new TextDecoder().decode(event.chunk.bytes));
        
        if (chunkData.type === 'content_block_start') {
          // Nudge client: ensure at least one data frame precedes first text delta
          write('data: {"type":"stream_start"}\n\n');
        } else if (chunkData.type === 'content_block_delta') {
          const delta = chunkData.delta;
          if (delta?.type === 'text_delta' && delta.text) {
            tokenCount++;
            if (!firstTokenTime) {
              firstTokenTime = Date.now() - startTime;
              write(`: x-first-token-ms=${firstTokenTime}\n\n`);
              console.log(`⚡ First token in ${firstTokenTime}ms`);
            }
            
            // Stream to client immediately - NO DELAY
            const sseData = JSON.stringify({
              type: 'text',
              content: delta.text,
              session_id: sessionId
            });
            write(`data: ${sseData}\n\n`);
            
            // Also append to buffer in parallel (microseconds, no blocking)
            responseBuffer += delta.text;
          }
        } else if (chunkData.type === 'message_stop') {
          console.log('✅ Bedrock stream complete');
          break;
        }
      }
    }
    
    // Send completion metadata
    const totalTime = Date.now() - startTime;
    write(`: x-total-tokens=${tokenCount}\n`);
    write(`: x-total-time-ms=${totalTime}\n`);
    console.log(`✅ Complete - ${tokenCount} tokens in ${totalTime}ms`);
    
    // Log complete Q&A pair AFTER streaming is done (no impact on user experience)
    if (questionBuffer && responseBuffer) {
      console.log('📝 Q&A Pair Captured:');
      console.log(`  Session: ${sessionId}`);
      console.log(`  Tenant: ${tenantHash.substring(0, 8)}...`);
      console.log(`  Question: "${questionBuffer.substring(0, 100)}${questionBuffer.length > 100 ? '...' : ''}"`);
      console.log(`  Answer: "${responseBuffer.substring(0, 200)}${responseBuffer.length > 200 ? '...' : ''}"`);
      console.log(`  Full Q Length: ${questionBuffer.length} chars`);
      console.log(`  Full A Length: ${responseBuffer.length} chars`);
      
      // Log full Q&A in structured format for analytics
      console.log(JSON.stringify({
        type: 'QA_COMPLETE',
        timestamp: new Date().toISOString(),
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || null,  // Add tenant_id from config
        conversation_id: body.conversation_id || sessionId,  // Add conversation_id
        question: questionBuffer,
        answer: responseBuffer,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: responseBuffer.length
        }
      }));
    }
    
  } catch (error) {
    console.error('❌ Stream error:', error);
    write(`data: {"type": "error", "error": "${error.message}"}\n\n`);
  } finally {
    // Clean up
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
    }
    
    // Send completion marker
    write('data: [DONE]\n\n');
    
    // End the stream
    streamEnded = true;
    responseStream.end();
  }

};

/**
 * Buffered handler for when streaming is not available
 */
const bufferedHandler = async (event, context) => {
  console.log('📡 Using buffered SSE handler');
  
  // Handle OPTIONS
  if (event.httpMethod === 'OPTIONS' || event.requestContext?.http?.method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
      },
      body: ''
    };
  }
  
  const startTime = Date.now();
  const chunks = [];
  let responseBuffer = '';
  let questionBuffer = '';
  
  // Add prelude
  chunks.push(':ok\n\n');
  
  try {
    // Parse request
    const body = event.body ? JSON.parse(event.body) : {};
    const tenantHash = body.tenant_hash || '';
    const sessionId = body.session_id || 'default';
    const userInput = body.user_input || '';
    
    // Capture the question
    questionBuffer = userInput;
    
    // Extract conversation history from the request
    const conversationHistory = body.conversation_history || 
                               body.conversation_context?.recentMessages || 
                               [];
    
    console.log(`💬 Conversation history: ${conversationHistory.length} messages`);
    
    if (!tenantHash || !userInput) {
      const error = !tenantHash ? 'Missing tenant_hash' : 'Missing user_input';
      chunks.push(`data: {"type": "error", "error": "${error}"}\n\n`);
      chunks.push('data: [DONE]\n\n');
      
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache, no-transform',
          'Access-Control-Allow-Origin': '*',
          'X-Accel-Buffering': 'no'
        },
        body: chunks.join('')
      };
    }
    
    console.log(`📝 Processing: ${tenantHash.substring(0,8)}... / ${sessionId.substring(0,12)}...`);
    
    // Load config
    let config = await loadConfig(tenantHash);
    if (!config) {
      config = {
        model_id: DEFAULT_MODEL_ID,
        streaming: { max_tokens: DEFAULT_MAX_TOKENS, temperature: DEFAULT_TEMPERATURE },
        tone_prompt: DEFAULT_TONE
      };
    }
    
    // Get KB context
    const kbContext = await retrieveKB(userInput, config);
    const prompt = buildPrompt(userInput, kbContext, config.tone_prompt, conversationHistory, config);
    
    // Prepare Bedrock request - use config model or default
    const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    const maxTokens = config.streaming?.max_tokens || DEFAULT_MAX_TOKENS;
    const temperature = config.streaming?.temperature || DEFAULT_TEMPERATURE;
    
    // Invoke Bedrock
    const response = await bedrock.send(new InvokeModelWithResponseStreamCommand({
      modelId: modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
        max_tokens: maxTokens,
        temperature: temperature
      })
    }));
    
    let firstTokenTime = null;
    let tokenCount = 0;
    
    // Process stream (buffered)
    for await (const event of response.body) {
      if (event.chunk?.bytes) {
        const chunkData = JSON.parse(new TextDecoder().decode(event.chunk.bytes));
        
        if (chunkData.type === 'content_block_delta') {
          const text = chunkData.delta?.text;
          if (text) {
            tokenCount++;
            
            if (!firstTokenTime) {
              firstTokenTime = Date.now() - startTime;
              chunks.push(`: x-first-token-ms=${firstTokenTime}\n\n`);
            }
            
            chunks.push(`data: {"type": "text", "content": ${JSON.stringify(text)}, "session_id": "${sessionId}"}\n\n`);
            responseBuffer += text;
          }
        } else if (chunkData.type === 'message_stop') {
          break;
        }
      }
    }
    
    // Add completion
    const totalTime = Date.now() - startTime;
    chunks.push(`: x-total-tokens=${tokenCount}\n`);
    chunks.push(`: x-total-time-ms=${totalTime}\n`);
    chunks.push('data: [DONE]\n\n');
    
    console.log(`✅ Complete - ${tokenCount} tokens in ${totalTime}ms`);
    
    // Log complete Q&A pair for analytics
    if (questionBuffer && responseBuffer) {
      console.log(JSON.stringify({
        type: 'QA_COMPLETE',
        timestamp: new Date().toISOString(),
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || null,  // Add tenant_id from config
        conversation_id: body.conversation_id || sessionId,  // Add conversation_id
        question: questionBuffer,
        answer: responseBuffer,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: responseBuffer.length
        }
      }));
    }
    
    // For Lambda Function URLs, we need to return the raw SSE content
    // The Function URL will handle setting the appropriate headers
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
        'X-Accel-Buffering': 'no'
      },
      body: chunks.join(''),
      isBase64Encoded: false
    };
    
  } catch (error) {
    console.error('Handler error:', error);
    
    chunks.push(`data: {"type": "error", "error": "${error.message}"}\n\n`);
    chunks.push('data: [DONE]\n\n');
    
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Access-Control-Allow-Origin': '*'
      },
      body: chunks.join('')
    };
  }
};

// Export the appropriate handler based on streaming support
exports.handler = streamifyResponse ? streamifyResponse(streamingHandler) : bufferedHandler;