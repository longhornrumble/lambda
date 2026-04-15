/**
 * Bedrock Streaming Handler - True Lambda Response Streaming
 * Uses awslambda.streamifyResponse for real SSE streaming
 * No JWT required - uses simple tenant_hash/session_id
 *
 * Architecture: V4.1 Pipeline (three-layer)
 *   Step 1: KB retrieval (Bedrock Agent Runtime)
 *   Step 2: Streaming response generation (prompt_v4.js)
 *   Step 3a: Topic classification (prompt_v4.js — non-streaming Bedrock call)
 *   Step 3b: Dynamic CTA pool selection (prompt_v4.js — no AI)
 *
 * Tier 1-2: Explicit click routing (action chips, CTA buttons) via response_enhancer.js
 * Fallback: enhanceResponse() when no topic_definitions configured
 */

const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand } = require('@aws-sdk/client-bedrock-runtime');
const { SQSClient, SendMessageCommand, SendMessageBatchCommand } = require('@aws-sdk/client-sqs');
const { enhanceResponse } = require('./response_enhancer');
const { handleFormMode } = require('./form_handler'); // Migrated to AWS SDK v3
const {
  buildV4ConversationPrompt,
  classifyTopic,
  selectCTAsFromPool,
  selectActionsV4,
  validateTopicDefinitions,
  V4_STEP2_INFERENCE_PARAMS,
  sanitizeTonePromptV4,
} = require('./prompt_v4');
const { loadConfig, retrieveKB, sanitizeUserInput } = require('../shared/bedrock-core');

// Default model configuration - single source of truth
// Upgraded to Haiku 4.5 for better instruction following (2025-11-26)
const DEFAULT_MODEL_ID = 'global.anthropic.claude-haiku-4-5-20251001-v1:0';
const DEFAULT_MAX_TOKENS = 1000;
const DEFAULT_TEMPERATURE = 0; // Set to 0 for maximum factual accuracy
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

// Initialize AWS clients with configurable region
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const bedrock = new BedrockRuntimeClient({ region: AWS_REGION });
const sqs = new SQSClient({ region: AWS_REGION });

// Analytics SQS queue URL
const ANALYTICS_QUEUE_URL = process.env.ANALYTICS_QUEUE_URL || 'https://sqs.us-east-1.amazonaws.com/614056832592/picasso-analytics-events';

/**
 * Sanitize text for SMS messages - remove special characters that could cause issues
 * @param {string} text - Raw text
 * @returns {string} - SMS-safe text
 */
function sanitizeForSMS(text) {
  if (!text || typeof text !== 'string') {
    return '';
  }
  // Keep only alphanumeric, spaces, and basic punctuation
  return text.replace(/[^\w\s@.-]/g, '').slice(0, 50);
}

/**
 * Analytics event handler - receives events from widget and sends to SQS
 * Supports both single events and batched events for efficiency
 *
 * Request format:
 * Single event: { schema_version, session_id, tenant_id, timestamp, step_number, event: { type, payload } }
 * Batch: { batch: true, events: [...] }
 */
async function handleAnalyticsEvent(event) {
  console.log('📊 Analytics event handler invoked');

  try {
    // Parse request body
    const body = event.body ? JSON.parse(event.body) : event;

    // Handle batch events
    if (body.batch && Array.isArray(body.events)) {
      const events = body.events;
      console.log(`📊 Processing batch of ${events.length} analytics events`);

      if (events.length === 0) {
        return {
          statusCode: 200,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
          },
          body: JSON.stringify({ status: 'success', processed: 0 })
        };
      }

      // For batches up to 10, use SQS batch send
      if (events.length <= 10) {
        const entries = events.map((evt, idx) => ({
          Id: `msg-${idx}`,
          MessageBody: JSON.stringify(evt)
        }));

        await sqs.send(new SendMessageBatchCommand({
          QueueUrl: ANALYTICS_QUEUE_URL,
          Entries: entries
        }));

        console.log(`✅ Sent ${events.length} events to SQS (batch)`);
      } else {
        // For larger batches, send as single message with batch flag
        await sqs.send(new SendMessageCommand({
          QueueUrl: ANALYTICS_QUEUE_URL,
          MessageBody: JSON.stringify({ batch: true, events })
        }));

        console.log(`✅ Sent ${events.length} events to SQS (single batch message)`);
      }

      return {
        statusCode: 200,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ status: 'success', processed: events.length })
      };
    }

    // Handle single event
    if (!body.session_id || !body.event) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ error: 'Missing required fields: session_id, event' })
      };
    }

    // Send single event to SQS
    await sqs.send(new SendMessageCommand({
      QueueUrl: ANALYTICS_QUEUE_URL,
      MessageBody: JSON.stringify(body)
    }));

    console.log(`✅ Sent single event to SQS: ${body.event.type}`);

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({ status: 'success', processed: 1 })
    };

  } catch (error) {
    console.error('❌ Analytics handler error:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({ error: error.message })
    };
  }
}

/**
 * Preview prompt handler - returns the constructed prompt without calling Bedrock
 * This allows the Config Builder UI to preview how prompts will be built
 */
async function handlePromptPreview(event) {
  console.log('🔍 Prompt preview handler invoked');

  try {
    // Parse request
    const body = event.body ? JSON.parse(event.body) : event;
    const tenantHash = body.tenant_hash || '';
    const userInput = body.user_input || 'Hello, how can you help me?';
    const conversationHistory = body.conversation_history || [];
    const kbContext = body.kb_context || 'Sample knowledge base context about our services...';

    if (!tenantHash) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ error: 'Missing tenant_hash' })
      };
    }

    // Load config
    const config = await loadConfig(tenantHash);
    if (!config) {
      return {
        statusCode: 404,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({ error: 'Config not found for tenant' })
      };
    }

    // Build the prompt using V4 pipeline
    const tonePrompt = sanitizeTonePromptV4(config.tone_prompt);
    const prompt = buildV4ConversationPrompt(
      userInput,
      kbContext,
      tonePrompt,
      conversationHistory,
      config
    );

    // Return preview data
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
      },
      body: JSON.stringify({
        tenant_hash: tenantHash,
        tenant_id: config.tenant_id,
        prompt_length: prompt.length,
        prompt: prompt,
        metadata: {
          pipeline: 'v4.1',
          has_topic_definitions: (config.topic_definitions || []).length > 0,
          tone_prompt: tonePrompt ? 'custom' : 'default'
        }
      }, null, 2)
    };

  } catch (error) {
    console.error('❌ Preview error:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({
        error: error.message,
        stack: error.stack
      })
    };
  }
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

  // Route analytics requests (non-streaming) - write JSON response to stream
  const queryParams = event.queryStringParameters || {};
  const parsedBody = event.body ? JSON.parse(event.body) : event;
  if (queryParams.action === 'analytics' || parsedBody.action === 'analytics') {
    console.log('📊 Routing to analytics handler (via streaming handler)');
    const result = await handleAnalyticsEvent(event);
    responseStream.write(JSON.stringify(JSON.parse(result.body)));
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
    const isFormMode = body.form_mode === true;
    const skipConfigCache = body.nocache === true || queryParams.nocache !== undefined;

    // Form mode requests don't require user_input - they have form_data instead
    if (!tenantHash || (!userInput && !isFormMode)) {
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
    if (skipConfigCache) console.log('🔄 Config cache bypass requested (nocache=true)');
    let config = await loadConfig(tenantHash, { skipCache: skipConfigCache });
    if (!config) {
      config = {
        model_id: DEFAULT_MODEL_ID,
        streaming: { max_tokens: DEFAULT_MAX_TOKENS, temperature: DEFAULT_TEMPERATURE },
        tone_prompt: DEFAULT_TONE
      };
    }

    // Support bedrock_instructions_override for testing (GATED - requires ENABLE_INSTRUCTION_OVERRIDE=true)
    if (body.bedrock_instructions_override && process.env.ENABLE_INSTRUCTION_OVERRIDE === 'true') {
      console.log('🔧 Applying bedrock_instructions_override from request (override enabled via env var)');
      config.bedrock_instructions = body.bedrock_instructions_override;
    } else if (body.bedrock_instructions_override) {
      console.log('⚠️ bedrock_instructions_override ignored - ENABLE_INSTRUCTION_OVERRIDE not set');
    }

    // Check for form mode - bypass Bedrock for form field collection
    if (body.form_mode === true) {
      console.log('📝 Form mode detected - handling locally without Bedrock');
      try {
        const formResponse = await handleFormMode(body, config);

        // Send the form response as a single SSE event
        write(`data: ${JSON.stringify(formResponse)}\n\n`);
        write('data: [DONE]\n\n');

        // Clear heartbeat and end stream
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        streamEnded = true;
        responseStream.end();
        return;
      } catch (error) {
        console.error('Form mode error:', error);
        write(`data: {"type": "error", "error": "Form processing failed: ${error.message}"}\n\n`);
        write('data: [DONE]\n\n');
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // Check for show_showcase action - bypass Bedrock and return showcase card directly
    const routingMetadata = body.routing_metadata || {};
    if (routingMetadata.action === 'show_showcase' && routingMetadata.target_showcase_id) {
      console.log(`🎨 Show showcase mode detected - bypassing Bedrock for showcase: ${routingMetadata.target_showcase_id}`);
      try {
        const { getShowcaseById, loadTenantConfig } = require('./response_enhancer');
        const fullConfig = await loadTenantConfig(tenantHash);
        const showcaseCard = getShowcaseById(routingMetadata.target_showcase_id, fullConfig);

        if (showcaseCard) {
          // Send showcase card as SSE event
          const showcaseResponse = JSON.stringify({
            type: 'showcase_card',
            showcaseCard: showcaseCard,
            session_id: sessionId,
            metadata: {
              routing_tier: 'action_chip_direct',
              routing_method: 'show_showcase',
              showcase_id: showcaseCard.id
            }
          });
          write(`data: ${showcaseResponse}\n\n`);
          console.log(`✅ Sent showcase card: ${showcaseCard.id}`);
        } else {
          // Showcase not found - send error
          write(`data: {"type": "error", "error": "Showcase item not found: ${routingMetadata.target_showcase_id}"}\n\n`);
        }

        write('data: [DONE]\n\n');

        // Clear heartbeat and end stream
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        streamEnded = true;
        responseStream.end();
        return;
      } catch (error) {
        console.error('Showcase mode error:', error);
        write(`data: {"type": "error", "error": "Showcase processing failed: ${error.message}"}\n\n`);
        write('data: [DONE]\n\n');
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // Sanitize user input to prevent prompt injection
    const sanitizedInput = sanitizeUserInput(userInput);

    // For short continuation messages ("yes", "sure", "tell me more"), the raw input
    // is too vague for KB retrieval. Use the last substantive user message instead
    // so the KB returns relevant context for the ongoing topic.
    const CONTINUATION_PATTERNS = /^(yes|yeah|yep|sure|ok|okay|please|go ahead|tell me more|more info|continue|absolutely|definitely|of course|why not|sounds good|great|cool|yea|ye|ya|mhm|uh huh)\.?!?$/i;
    let kbQuery = sanitizedInput;
    if (sanitizedInput.trim().length < 30 && CONTINUATION_PATTERNS.test(sanitizedInput.trim())) {
      const lastUserMsg = [...conversationHistory].reverse().find(m => m.role === 'user' && (m.content || m.text || '').trim().length > 10);
      if (lastUserMsg) {
        kbQuery = sanitizeUserInput((lastUserMsg.content || lastUserMsg.text).trim());
        console.log(`🔁 Continuation detected: "${sanitizedInput}" → KB query from previous: "${kbQuery.substring(0, 60)}..."`);
      }
    }

    // Enrich KB query for follow-up turns to pull deeper content.
    // If the user is asking about the same topic again ("tell me more", "what else"),
    // append what the bot already covered so retrieval targets uncovered content.
    if (conversationHistory && conversationHistory.length >= 2) {
      const lastAssistantMsg = [...conversationHistory].reverse().find(m => m.role === 'assistant');
      if (lastAssistantMsg) {
        const covered = (lastAssistantMsg.content || lastAssistantMsg.text || '').trim();
        // If the user is asking for more detail on the same topic, enrich the query
        const morePatterns = /more|else|detail|learn|what about|tell me|specifics|information|further|deeper|everything/i;
        if (morePatterns.test(sanitizedInput)) {
          // Append topic keywords from the last response to diversify retrieval
          const topicKeywords = covered
            .split(/[.!?\n]/)
            .filter(s => s.trim().length > 20)
            .slice(0, 2)
            .map(s => s.trim().substring(0, 80))
            .join(' ');
          kbQuery = `${sanitizedInput} — details beyond: ${topicKeywords}`;
          console.log(`🔍 Enriched KB query for follow-up: "${kbQuery.substring(0, 120)}..."`);
        }
      }
    }

    // Get KB context — errors are handled gracefully so Bedrock can still respond
    let kbContext = '';
    try {
      kbContext = await retrieveKB(kbQuery, config);
    } catch (kbError) {
      console.error('❌ KB retrieval failed, continuing without KB context:', kbError.message);
    }

    const tonePrompt = sanitizeTonePromptV4(config.tone_prompt);
    const prompt = buildV4ConversationPrompt(sanitizedInput, kbContext, tonePrompt, conversationHistory, config);
    const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    const maxTokens = V4_STEP2_INFERENCE_PARAMS.max_tokens;
    const temperature = V4_STEP2_INFERENCE_PARAMS.temperature;

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

      // NOTE: MESSAGE_SENT and MESSAGE_RECEIVED events are now emitted by the frontend
      // (StreamingChatProvider.jsx and HTTPChatProvider.jsx) via the analytics pipeline.
      // This ensures reliable delivery since the frontend knows exactly when messages are sent/received.
    }

    // Enhance response with CTAs after streaming is complete
    try {
      const routingMetadata = body.routing_metadata || {};
      const sessionContext = body.session_context || {};

      const validation = validateTopicDefinitions(config);

      if (routingMetadata.action_chip_triggered || routingMetadata.cta_triggered) {
        // Tiers 1-2: Explicit clicks — use enhanceResponse()
        console.log('[Tier 1-2] Explicit click routing — using enhanceResponse()');
        const enhancedData = await enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata);

        if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
          write(`data: ${JSON.stringify({
            type: 'cta_buttons',
            ctaButtons: enhancedData.ctaButtons,
            metadata: enhancedData.metadata,
            session_id: sessionId
          })}\n\n`);
          console.log(`🎯 [Tier 1-2] sent ${enhancedData.ctaButtons.length} CTAs | tier: ${enhancedData.metadata?.routing_tier || 'explicit'}`);
        }
        // Send showcase card if present
        if (enhancedData.showcaseCard) {
          write(`data: ${JSON.stringify({
            type: 'showcase_card',
            showcaseCard: enhancedData.showcaseCard,
            metadata: enhancedData.metadata,
            session_id: sessionId
          })}\n\n`);
        }

      } else if (config.feature_flags?.V4_ACTION_SELECTOR) {
        // V4.0 Action Selector: AI picks CTAs from the full vocabulary
        console.log('[V4 ActionSelector] Using LLM-based CTA selection');
        const selectedIds = await selectActionsV4(responseBuffer, conversationHistory, config, bedrock);

        if (selectedIds.length > 0) {
          const ctaButtons = selectedIds.map((id, idx) => {
            const { style, ...cleanCta } = config.cta_definitions[id] || {};
            return { ...cleanCta, id, _position: idx === 0 ? 'primary' : 'secondary' };
          });

          write(`data: ${JSON.stringify({
            type: 'cta_buttons',
            ctaButtons,
            metadata: {
              routing_tier: 'v4_action_selector',
              selected_ids: selectedIds,
              conversation_context: { selected_ctas: selectedIds }
            },
            session_id: sessionId
          })}\n\n`);
          console.log(`🎯 [V4 ActionSelector] sent ${ctaButtons.length} CTAs: [${selectedIds.join(', ')}]`);
        } else {
          console.log('[V4 ActionSelector] No CTAs selected');
        }

      } else if (validation.definitions.length > 0) {
        // Step 3a: Topic classification (non-streaming LLM call)
        console.log(`[Step 3a] Classifying topic (${validation.definitions.length} definitions)`);
        let topicName = await classifyTopic(
          userInput,
          conversationHistory,
          { ...config, topic_definitions: validation.definitions },
          bedrock
        );

        // Continuation detection: short/ambiguous messages carry forward the previous topic
        const isShortMessage = userInput.trim().length < 20;
        const isNullOrGeneral = !topicName || topicName === 'general_inquiry';
        const previousTopic = sessionContext.last_classified_topic;
        if (isShortMessage && isNullOrGeneral && previousTopic) {
          console.log(`[Step 3a] Continuation detected: "${userInput}" → carrying forward topic "${previousTopic}"`);
          topicName = previousTopic;
        }

        // Step 3b: Dynamic CTA pool selection (deterministic, no AI)
        const result = selectCTAsFromPool(topicName, config, sessionContext);

        // Send CTA SSE event
        if (result.ctaButtons && result.ctaButtons.length > 0) {
          write(`data: ${JSON.stringify({
            type: 'cta_buttons',
            ctaButtons: result.ctaButtons,
            metadata: result.metadata,
            session_id: sessionId
          })}\n\n`);
          console.log(`🎯 [Step3] sent ${result.ctaButtons.length} CTAs | topic: ${result.metadata?.classified_topic || 'null'} | depth: ${result.metadata?.depth} | method: ${result.metadata?.routing_method}`);
        } else {
          console.log(`[Step3] No CTAs to send | topic: ${topicName || 'null'} | method: ${result.metadata?.routing_method}`);
        }

      } else {
        // No topic_definitions — fallback to enhanceResponse()
        console.log('No topic_definitions configured — using enhanceResponse()');
        const enhancedData = await enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata);

        if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
          write(`data: ${JSON.stringify({
            type: 'cta_buttons',
            ctaButtons: enhancedData.ctaButtons,
            metadata: enhancedData.metadata,
            session_id: sessionId
          })}\n\n`);
          console.log(`🎯 [fallback] sent ${enhancedData.ctaButtons.length} CTAs | tier: ${enhancedData.metadata?.routing_tier || 'unknown'}`);
        }
        if (enhancedData.showcaseCard) {
          write(`data: ${JSON.stringify({
            type: 'showcase_card',
            showcaseCard: enhancedData.showcaseCard,
            metadata: enhancedData.metadata,
            session_id: sessionId
          })}\n\n`);
        }
      }

    } catch (enhanceError) {
      console.error('❌ CTA enhancement error:', enhanceError);
      // Don't fail the response if CTA enhancement fails
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
  console.log('📡 Handler invoked');

  // Check for preview endpoint
  const queryParams = event.queryStringParameters || {};
  const body = event.body ? JSON.parse(event.body) : event;

  if (queryParams.action === 'preview' || body.action === 'preview') {
    console.log('🔍 Routing to preview handler');
    return await handlePromptPreview(event);
  }

  // Route to analytics handler
  if (queryParams.action === 'analytics' || body.action === 'analytics') {
    console.log('📊 Routing to analytics handler');
    return await handleAnalyticsEvent(event);
  }

  console.log('📡 Using buffered SSE handler for streaming');

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
    const skipConfigCache = body.nocache === true || queryParams.nocache !== undefined;

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
    if (skipConfigCache) console.log('🔄 Config cache bypass requested (nocache=true)');
    let config = await loadConfig(tenantHash, { skipCache: skipConfigCache });
    if (!config) {
      config = {
        model_id: DEFAULT_MODEL_ID,
        streaming: { max_tokens: DEFAULT_MAX_TOKENS, temperature: DEFAULT_TEMPERATURE },
        tone_prompt: DEFAULT_TONE
      };
    }

    // Support bedrock_instructions_override for testing (GATED - requires ENABLE_INSTRUCTION_OVERRIDE=true)
    if (body.bedrock_instructions_override && process.env.ENABLE_INSTRUCTION_OVERRIDE === 'true') {
      console.log('🔧 Applying bedrock_instructions_override from request (override enabled via env var)');
      config.bedrock_instructions = body.bedrock_instructions_override;
    } else if (body.bedrock_instructions_override) {
      console.log('⚠️ bedrock_instructions_override ignored - ENABLE_INSTRUCTION_OVERRIDE not set');
    }

    // Check for show_showcase action - bypass Bedrock and return showcase card directly
    const routingMetadata = body.routing_metadata || {};
    if (routingMetadata.action === 'show_showcase' && routingMetadata.target_showcase_id) {
      console.log(`🎨 Show showcase mode detected - bypassing Bedrock for showcase: ${routingMetadata.target_showcase_id}`);
      try {
        const { getShowcaseById, loadTenantConfig } = require('./response_enhancer');
        const fullConfig = await loadTenantConfig(tenantHash);
        const showcaseCard = getShowcaseById(routingMetadata.target_showcase_id, fullConfig);

        if (showcaseCard) {
          // Send showcase card as SSE event
          const showcaseResponse = JSON.stringify({
            type: 'showcase_card',
            showcaseCard: showcaseCard,
            session_id: sessionId,
            metadata: {
              routing_tier: 'action_chip_direct',
              routing_method: 'show_showcase',
              showcase_id: showcaseCard.id
            }
          });
          chunks.push(`data: ${showcaseResponse}\n\n`);
          console.log(`✅ Sent showcase card: ${showcaseCard.id}`);
        } else {
          // Showcase not found - send error
          chunks.push(`data: {"type": "error", "error": "Showcase item not found: ${routingMetadata.target_showcase_id}"}\n\n`);
        }

        chunks.push('data: [DONE]\n\n');

        return {
          statusCode: 200,
          headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            'Access-Control-Allow-Origin': '*',
            'X-Accel-Buffering': 'no'
          },
          body: chunks.join(''),
          isBase64Encoded: false
        };
      } catch (error) {
        console.error('Showcase mode error:', error);
        chunks.push(`data: {"type": "error", "error": "Showcase processing failed: ${error.message}"}\n\n`);
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
    }

    // Sanitize user input to prevent prompt injection
    const sanitizedInput = sanitizeUserInput(userInput);

    // Get KB context
    const kbContext = await retrieveKB(sanitizedInput, config);

    const tonePrompt = sanitizeTonePromptV4(config.tone_prompt);
    const prompt = buildV4ConversationPrompt(sanitizedInput, kbContext, tonePrompt, conversationHistory, config);
    const modelId = config.model_id || config.aws?.model_id || DEFAULT_MODEL_ID;
    const maxTokens = V4_STEP2_INFERENCE_PARAMS.max_tokens;
    const temperature = V4_STEP2_INFERENCE_PARAMS.temperature;

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

      // NOTE: MESSAGE_SENT and MESSAGE_RECEIVED events are now emitted by the frontend
      // (StreamingChatProvider.jsx and HTTPChatProvider.jsx) via the analytics pipeline.
      // This ensures reliable delivery since the frontend knows exactly when messages are sent/received.
    }

    // Enhance response with CTAs after generation is complete
    try {
      const routingMetadata = body.routing_metadata || {};
      const sessionContext = body.session_context || {};

      const validation = validateTopicDefinitions(config);

      if (routingMetadata.action_chip_triggered || routingMetadata.cta_triggered) {
        // Tiers 1-2: Explicit clicks
        const enhancedData = await enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata);
        if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
          const ctaData = JSON.stringify({
            type: 'cta_buttons', ctaButtons: enhancedData.ctaButtons,
            metadata: enhancedData.metadata, session_id: sessionId
          });
          chunks.splice(chunks.length - 1, 0, `data: ${ctaData}\n\n`);
        }
      } else if (config.feature_flags?.V4_ACTION_SELECTOR) {
        // V4.0 Action Selector (buffered path)
        console.log('[V4 ActionSelector buffered] Using LLM-based CTA selection');
        const selectedIds = await selectActionsV4(responseBuffer, conversationHistory, config, bedrock);

        if (selectedIds.length > 0) {
          const ctaButtons = selectedIds.map((id, idx) => {
            const { style, ...cleanCta } = config.cta_definitions[id] || {};
            return { ...cleanCta, id, _position: idx === 0 ? 'primary' : 'secondary' };
          });

          const ctaData = JSON.stringify({
            type: 'cta_buttons', ctaButtons,
            metadata: {
              routing_tier: 'v4_action_selector',
              selected_ids: selectedIds,
              conversation_context: { selected_ctas: selectedIds }
            },
            session_id: sessionId
          });
          chunks.splice(chunks.length - 1, 0, `data: ${ctaData}\n\n`);
          console.log(`🎯 [V4 ActionSelector buffered] sent ${ctaButtons.length} CTAs: [${selectedIds.join(', ')}]`);
        }

      } else if (validation.definitions.length > 0) {
        // Step 3a + 3b: Topic classification → Pool selection
        let topicName = await classifyTopic(
          userInput, conversationHistory,
          { ...config, topic_definitions: validation.definitions }, bedrock
        );

        // Continuation detection: short/ambiguous messages carry forward the previous topic
        const isShortMsg = userInput.trim().length < 20;
        const isNullOrGen = !topicName || topicName === 'general_inquiry';
        const prevTopic = sessionContext.last_classified_topic;
        if (isShortMsg && isNullOrGen && prevTopic) {
          console.log(`[Step 3a buffered] Continuation detected: "${userInput}" → carrying forward topic "${prevTopic}"`);
          topicName = prevTopic;
        }

        const result = selectCTAsFromPool(topicName, config, sessionContext);
        if (result.ctaButtons && result.ctaButtons.length > 0) {
          const ctaData = JSON.stringify({
            type: 'cta_buttons', ctaButtons: result.ctaButtons,
            metadata: result.metadata, session_id: sessionId
          });
          chunks.splice(chunks.length - 1, 0, `data: ${ctaData}\n\n`);
          console.log(`🎯 [Step3 buffered] sent ${result.ctaButtons.length} CTAs | topic: ${result.metadata?.classified_topic || 'null'} | depth: ${result.metadata?.depth}`);
        }
      } else {
        // No topic_definitions — fallback to enhanceResponse()
        const enhancedData = await enhanceResponse(responseBuffer, userInput, tenantHash, sessionContext, routingMetadata);
        if (enhancedData.ctaButtons && enhancedData.ctaButtons.length > 0) {
          const ctaData = JSON.stringify({
            type: 'cta_buttons', ctaButtons: enhancedData.ctaButtons,
            metadata: enhancedData.metadata, session_id: sessionId
          });
          chunks.splice(chunks.length - 1, 0, `data: ${ctaData}\n\n`);
        }
      }
    } catch (enhanceError) {
      console.error('❌ CTA enhancement error:', enhanceError);
      // Don't fail the response if CTA enhancement fails
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