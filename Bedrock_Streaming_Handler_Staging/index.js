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
const { writeSessionSummary } = require('./analytics_writer');
const { redactPII } = require('./redactPII');
const {
  buildV4ConversationPrompt,
  classifyTopic,
  selectCTAsFromPool,
  selectActionsV4,
  validateTopicDefinitions,
  V4_STEP2_INFERENCE_PARAMS,
  sanitizeTonePromptV4,
  V4_CONVERSATION_PROMPT_VERSION,
  ACTION_SELECTOR_PROMPT_VERSION,
} = require('./prompt_v4');
const { loadConfig, retrieveKB, sanitizeUserInput } = require('../shared/bedrock-core');
// WS-C2 (scheduling §5.6): same-session form-data injection. Read-only fetch +
// sanitize + <user_application_context> block. Prompt-injection surface.
const { injectFormContext } = require('./scheduling/formInjection');
// WS-CONVO (B3 keystone): in-chat reschedule/cancel. Pre-turn binding hook +
// post-stream §B14 structured-action boundary. No-op for non-scheduling sessions.
const {
  injectSchedulingContext,
  isSchedulingEnabled,
  resolveNewBookingSessionRow,
  NEW_BOOKING_IN_FLIGHT_STATES,
} = require('./scheduling/bindingContext');
const { runSchedulingTurn } = require('./scheduling/schedulingFlow');
// WS-AG-CORE (§B17): the bounded agent tool loop + its §B17h kill-switch guard. The
// integrator wires the §B17a routing branch below; the module owns the turn itself.
const { agentTurn, isAgentTurnEnabled } = require('./scheduling/agentTurn');
// WS-TRACKD-BE (D3): post-form scheduling offer — wired at the form-completion seam.
const { postFormOffer } = require('./scheduling/postFormOffer');
const { capturePrepNote, tenantHasPostBookingQuestion } = require('./scheduling/postBookingPrepNote');
// WS-NEWBOOK (B-remainder §B16d): the in-chat NEW-booking entry-hook. No-op for normal chat +
// the recovery loop; engages on the widget's scheduling_intent:'new_booking' signal or an
// in-flight new-booking session row.
const { runNewBookingEntry, captureAttendeeEmail, EMAIL_SHAPE } = require('./scheduling/newBookingEntry');
// [Fix-2a B-2] The §B10 binding uuid (body.session) is forwarded into a DynamoDB SK
// (binding#<uuid>) + can drive a reschedule/cancel via the §B14 boundary, so format-gate it:
// accept ONLY a canonical UUID (crypto.randomUUID shape). Rejects probing strings, oversized
// payloads, and the reserved 'unknown'/'default' placeholders in one check.
const BINDING_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Deterministic-pipeline copy: scheduling CLICK turns (entry / select / confirm) and the
// email-capture turn bypass Bedrock, so these templated lines are the ONLY assistant text.
const SCHEDULING_ENTRY_COPY = 'Happy to set that up — let me pull up some openings for you…';
const SCHEDULING_ENTRY_FALLBACK_COPY =
  "I couldn't pull up available times just now. Please try again in a few minutes.";
const SCHEDULING_EMAIL_ASK_COPY = "Great choice! What's the best email for the calendar invite?";
const SCHEDULING_CONFIRM_READY_COPY = 'Perfect — tap "Yes, book it" and I\'ll lock it in.';
const SCHEDULING_SLOT_GONE_COPY =
  'That time isn\'t available anymore — tap another slot, or say "more times".';
const SCHEDULING_BOOKED_COPY = 'Booked! Your calendar invite is on its way.';
const SCHEDULING_EMAIL_GOT_COPY = (email) =>
  `Got it — ${email}. Tap "Yes, book it" to confirm your time.`;
const { buildSchedulingDeps } = require('./scheduling/schedulingStateStore');
const { corsHeaders } = require('./cors-helper');

// Tier-1 deps-wiring: the DDB I/O seam runSchedulingTurn consumes (loadState/saveState
// C9 state row + loadBooking). Built once per container (client reuse). resolveBinding/
// detect/generateSlots/stateMachine use schedulingFlow's bundled defaults; the Google-auth
// calendar EXECUTION seam is Tier 2 (Booking_Commit_Handler executor invoke), not wired here.
const schedulingDeps = buildSchedulingDeps({
  sessionTable: process.env.SCHEDULING_SESSION_TABLE || `picasso-conversation-scheduling-session-${process.env.ENVIRONMENT || 'staging'}`,
  bookingTable: process.env.BOOKING_TABLE || `picasso-booking-${process.env.ENVIRONMENT || 'staging'}`,
});

// Tier-2 calendar-execution seam (architecture option d): invoke the
// Booking_Commit_Handler executor for an already-§B14-authorized reschedule/cancel
// (BSH cannot bundle googleapis). Gated on SCHEDULING_EXECUTOR_FUNCTION_NAME — when
// unset, invokeSchedulingExecutor is undefined and schedulingFlow falls back to its
// (skip-non-fatally) local path, so this stays dormant until the IAM + env land.
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const SCHEDULING_EXECUTOR_FN = process.env.SCHEDULING_EXECUTOR_FUNCTION_NAME || '';
const schedulingExecutorClient = SCHEDULING_EXECUTOR_FN ? new LambdaClient({}) : null;
async function invokeSchedulingExecutor(payload) {
  const out = await schedulingExecutorClient.send(new InvokeCommand({
    FunctionName: SCHEDULING_EXECUTOR_FN,
    InvocationType: 'RequestResponse',
    Payload: Buffer.from(JSON.stringify(payload)),
  }));
  if (out.FunctionError) {
    // Unhandled error inside the executor → throw so the caller hits the email fallback.
    throw new Error(`scheduling executor FunctionError: ${out.FunctionError}`);
  }
  return out.Payload ? JSON.parse(Buffer.from(out.Payload).toString('utf8')) : null;
}
const schedulingExecDep = SCHEDULING_EXECUTOR_FN ? { invokeSchedulingExecutor } : {};
// WS-NEWBOOK (§B16d) entry-hook deps: the new-booking flow invokes BCH for the read-only
// `scheduling_propose` route (§B16a) AND the booking commit (§B16c) — both are RequestResponse
// invokes of the SAME Booking_Commit_Handler function (the executor), so they reuse
// invokeSchedulingExecutor (no new client, no new IAM). Dormant (empty bag) until
// SCHEDULING_EXECUTOR_FUNCTION_NAME + its IAM grant land — the same gate as the Tier-2 executor.
const newBookingDep = SCHEDULING_EXECUTOR_FN
  ? { invokeProposal: invokeSchedulingExecutor, invokeBookingCommit: invokeSchedulingExecutor }
  : {};
const { validateCfOriginHeader } = require('./cf-origin-validator');

// Default model configuration - single source of truth, sourced from
// BEDROCK_MODEL_ID env var per Phase 4 EC-P4-2 (single point of update
// across MFS Python + BSH Node.js, matches CLAUDE.md required-env-var
// contract). Fail-loud at module load if missing — Lambda cold-start
// errors are observable in CloudWatch immediately, no silent fallback
// to a stale model.
const DEFAULT_MODEL_ID = process.env.BEDROCK_MODEL_ID;
if (!DEFAULT_MODEL_ID) {
  throw new Error('BEDROCK_MODEL_ID environment variable is required');
}
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

// Analytics SQS queue URL. Unset in the staging account (per Issue #5
// batch-1 deferral — no staging SQS queue yet); handleAnalyticsEvent
// no-ops when this is empty so cross-account writes can't accidentally
// flow to prod.
const ANALYTICS_QUEUE_URL = process.env.ANALYTICS_QUEUE_URL || '';

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

  // Staging-account guard: when no SQS queue is configured, treat the
  // ?action=analytics endpoint as a no-op rather than 5xx-ing on every call.
  // The server-side writer (writeSessionSummary) is the durable analytics
  // path here; SQS-routed browser events are deferred per Issue #5 batch 1.
  if (!ANALYTICS_QUEUE_URL) {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders(event) },
      body: JSON.stringify({ status: 'noop', reason: 'analytics_queue_not_configured' }),
    };
  }

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
            ...corsHeaders(event)
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
          ...corsHeaders(event)
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
          ...corsHeaders(event)
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
        ...corsHeaders(event)
      },
      body: JSON.stringify({ status: 'success', processed: 1 })
    };

  } catch (error) {
    // Phase C audit F2 closure: never return raw SDK error text to the
    // browser. AWS SDK errors include the queue ARN + role ARN + denied
    // IAM action, which an attacker can use to target subsequent attacks.
    // Log the full error to CloudWatch (still observable for debugging);
    // return an opaque code to the caller.
    console.error('❌ Analytics handler error:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        ...corsHeaders(event)
      },
      body: JSON.stringify({ error: 'analytics_write_failed' })
    };
  }
}

/**
 * Main streaming handler - uses true streaming if available, falls back to buffered
 */
const streamingHandler = async (event, responseStream, context) => {
  console.log('🌊 True streaming handler invoked');

  // Validate CloudFront-injected origin header before any route dispatch.
  // No-op when REQUIRE_CF_ORIGIN_HEADER is unset/false (default rollout).
  // See cf-origin-validator.js + project_bsh_tenant_impersonation_*.
  const cfCheck = await validateCfOriginHeader(event);
  if (!cfCheck.valid) {
    console.warn(`SECURITY: streamingHandler rejected request: ${cfCheck.reason}`);
    // Canonical AWS streaming-mode HTTP-metadata pattern: reassign
    // responseStream after .from() wraps it, then write body + end()
    // separately. Using .end(body) on the wrapped stream produced 502 in
    // the real runtime — observed during PR #6 deploy verification.
    if (typeof awslambda !== 'undefined' && awslambda.HttpResponseStream) {
      responseStream = awslambda.HttpResponseStream.from(responseStream, {
        statusCode: 403,
        headers: { 'Content-Type': 'application/json' },
      });
      responseStream.write(JSON.stringify({ error: 'forbidden' }));
      responseStream.end();
    } else {
      responseStream.end(JSON.stringify({ error: 'forbidden' }));
    }
    return;
  }

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
    // §B12: the §B10 binding uuid the redemption handler mints + the page/widget forwards as
    // ?session= (request body.session). Distinct from the chat session_id — it keys the
    // recovery-binding row (binding#<uuid>) read in injectSchedulingContext. null on normal chat.
    const bindingSessionId = (typeof body.session === 'string' && BINDING_UUID_RE.test(body.session)) ? body.session : null;
    const userInput = body.user_input || '';
    const isFormMode = body.form_mode === true;
    const skipConfigCache = body.nocache === true || queryParams.nocache !== undefined;

    // Form mode requests don't require user_input - they have form_data instead
    if (!tenantHash || (!userInput && !isFormMode)) {
      const error = !tenantHash ? 'Missing tenant_hash' : 'Missing user_input';
      write(`data: ${JSON.stringify({ type: 'error', error: String(error) })}\n\n`);
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
        const formResponse = await handleFormMode(body, config, context.awsRequestId);

        // D3 (WS-TRACKD-BE): applicant_contact is an INTERNAL seam field from
        // form_handler — strip it before the wire write so the widget frame shape is
        // unchanged (the user's own contact data is never echoed back).
        const { applicant_contact: formContact, ...wireFormResponse } = formResponse;

        // Send the form response as a single SSE event
        write(`data: ${JSON.stringify(wireFormResponse)}\n\n`);

        // D3 (design-doc Appendix-A row D3): post-form scheduling offer — AFTER a
        // successful FINAL submission only, for scheduling-enabled tenants.
        // STRICT call-site guard (clobber-guard layer 1): offer ONLY when loadState
        // returns NO session row at all (null) — stricter than the module's own
        // layer-2 guard (this also covers recovery-state rows like 'rescheduling' /
        // 'canceling'). Non-fatal: any throw → err.name log, the form response above
        // already went out and [DONE] still follows.
        if (
          formResponse.type === 'form_complete' &&
          formContact?.email &&
          isSchedulingEnabled(config)
        ) {
          try {
            const existingRow = await schedulingDeps.loadState({
              tenantId: config?.tenant_id,
              sessionId,
            });
            if (existingRow == null) {
              // §B post-booking amendment: the completed form may configure a question to ask
              // AFTER booking ("what would you like to talk about?"). Read it here (old-shape
              // tolerant — absent → null) and hand it to postFormOffer, which stashes it on the
              // proposing session so the booked turn can stream it.
              const postBookingQuestion =
                (config?.conversational_forms?.[body.form_id]?.post_submission?.post_booking_question || '').trim() || null;
              const { offerText } = await postFormOffer({
                tenantConfig: config,
                sessionId,
                attendee: formContact,
                postBookingQuestion,
                deps: {
                  ...schedulingDeps,
                  ...newBookingDep,
                  loadState: schedulingDeps.loadState,
                  emitSse: (evt) => write(`data: ${JSON.stringify(evt)}\n\n`),
                },
              });
              if (offerText) {
                write(`data: ${JSON.stringify({ type: 'text', content: offerText, session_id: sessionId })}\n\n`);
              }
            }
          } catch (err) {
            console.error(`[WS-TRACKD] post-form offer call site failed (non-fatal): error_name=${(err && err.name) || 'unknown'}`);
          }
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
        console.error('Form mode error:', error);
        write(`data: ${JSON.stringify({ type: 'error', error: `Form processing failed: ${error.message}` })}\n\n`);
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
          write(`data: ${JSON.stringify({ type: 'error', error: `Showcase item not found: ${routingMetadata.target_showcase_id}` })}\n\n`);
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
        write(`data: ${JSON.stringify({ type: 'error', error: `Showcase processing failed: ${error.message}` })}\n\n`);
        write('data: [DONE]\n\n');
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // Scheduling CLICK turns — DETERMINISTIC routes, not chat turns (mirrors the
    // show_showcase bypass above). Clicks carry zero ambiguity, and streaming a KB
    // answer co-mingles legacy KB scheduling copy with the live booking flow and
    // narrates outcomes the state machine never produced (QA 2026-06-12, P0-2).
    //   • entry  — scheduling_intent:'new_booking' (start_scheduling CTA)
    //   • select — scheduling_action:'select_slot' + scheduling_slot_id (slot chip)
    //   • confirm— scheduling_action:'confirm_book' (confirm button)
    // Each runs the §B16d entry hook with `bedrock: null` — the action detector is
    // fail-closed without a client, and the widget signal is consumed deterministically
    // by the flow (§B16b amendment) — so NO model call happens anywhere on a click turn.
    // Scheduling disabled → fall through to normal chat (unchanged behavior).
    const widgetSchedulingAction =
      routingMetadata.scheduling_action === 'select_slot' || routingMetadata.scheduling_action === 'confirm_book'
        ? routingMetadata.scheduling_action
        : null;
    const isSchedulingEntryClick = routingMetadata.scheduling_intent === 'new_booking';
    if ((isSchedulingEntryClick || widgetSchedulingAction) && isSchedulingEnabled(config)) {
      console.log(`📅 scheduling click turn (${widgetSchedulingAction || 'new_booking entry'}) — bypassing Bedrock for deterministic handling`);
      const endSchedulingTurn = () => {
        write('data: [DONE]\n\n');
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        streamEnded = true;
        responseStream.end();
      };
      try {
        if (isSchedulingEntryClick && !widgetSchedulingAction) {
          write(`data: ${JSON.stringify({ type: 'text', content: SCHEDULING_ENTRY_COPY, session_id: sessionId })}\n\n`);
        }
        const entry = await runNewBookingEntry({
          responseText: '',
          conversationHistory,
          tenantId: config?.tenant_id,
          sessionId,
          config,
          bedrock: null, // deterministic turn — never invoke the detector model
          write,
          routingMetadata,
          deps: { ...schedulingDeps, ...newBookingDep },
        });
        // Outcome copy — the flow has already emitted its structured events
        // (scheduling_slots / scheduling_confirm / scheduling_booked / notice);
        // this is the human-readable line that accompanies them.
        let copy = null;
        if (widgetSchedulingAction === 'select_slot') {
          if (entry?.state === 'confirming' && entry?.identity) copy = SCHEDULING_CONFIRM_READY_COPY;
          else if (entry?.state === 'confirming') copy = SCHEDULING_EMAIL_ASK_COPY;
          else copy = SCHEDULING_SLOT_GONE_COPY; // unknown_slot / illegal transition / stale click
        } else if (widgetSchedulingAction === 'confirm_book') {
          if (entry?.executed) copy = SCHEDULING_BOOKED_COPY;
          else if (entry?.fallback === 'email') copy = null; // flow emitted the §9.3 notice
          else if (entry?.reason === 'identity_required') copy = SCHEDULING_EMAIL_ASK_COPY;
          else if (entry?.reason === 'slot_unavailable') copy = SCHEDULING_SLOT_GONE_COPY;
          else copy = SCHEDULING_ENTRY_FALLBACK_COPY;
        } else if (!entry || entry.handled !== true || entry.reason === 'propose_failed_outcome') {
          // Entry click: never leave it in dead air (propose seam down, entry error, etc.)
          // `handled:true` alone is NOT enough — the flow returns handled:true with
          // reason:'propose_failed_outcome' when the propose seam reports outcome:'failed'
          // (newBookingFlow's "non-fatal, no picker" branch), which on a CLICK turn has
          // no streamed chat text to fall back on. Without this reason check the user
          // got the entry copy and then silence (staging, 2026-07-03: every appointment
          // type lacked availability_windows, so propose failed universally and the
          // widget dead-aired instead of saying we'll follow up).
          copy = SCHEDULING_ENTRY_FALLBACK_COPY;
        }
        if (copy) {
          write(`data: ${JSON.stringify({ type: 'text', content: copy, session_id: sessionId })}\n\n`);
        }
        endSchedulingTurn();
        return;
      } catch (error) {
        console.error('Scheduling click turn error:', error);
        write(`data: ${JSON.stringify({ type: 'text', content: SCHEDULING_ENTRY_FALLBACK_COPY, session_id: sessionId })}\n\n`);
        endSchedulingTurn();
        return;
      }
    }

    // Deterministic email capture (§B16d amendment): an in-flight booking holding at
    // `confirming` without identity asked the user for their email. A bare email-shaped
    // message is that answer — capture it without a model call (the KB has nothing to
    // say to an email address). Cheap regex gates the state read; any non-capture
    // outcome (no in-flight session, wrong state) falls through to normal chat.
    // §B14 holds: capture NEVER commits — the user still taps the (re-armed) confirm button.
    if (isSchedulingEnabled(config) && typeof userInput === 'string' && EMAIL_SHAPE.test(userInput.trim())) {
      const cap = await captureAttendeeEmail({
        tenantId: config?.tenant_id,
        sessionId,
        email: userInput.trim(),
        deps: schedulingDeps,
        write,
      });
      if (cap.captured) {
        console.log('📅 attendee email captured for confirming session — bypassing Bedrock');
        write(`data: ${JSON.stringify({ type: 'text', content: SCHEDULING_EMAIL_GOT_COPY(cap.email), session_id: sessionId })}\n\n`);
        write('data: [DONE]\n\n');
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // Post-booking prep note (§B post-booking amendment): when the originating form configured
    // a question, the booked session row carries `awaiting_prep_note` + `booking_id`. The user's
    // NEXT plain free-text turn is their answer — capture it DETERMINISTICALLY (no model call;
    // the LLM would give a state-blind answer), attach it to the Booking row, clear the one-shot
    // flag, and ack. Sits after the click router + email capture (those turns returned above) and
    // before the agent/chat path. Fail-soft: any miss/error → fall through to normal chat.
    // The cheap config gate (no form configures a question → no awaiting session can exist)
    // keeps tenants NOT using the feature byte-identical — no per-turn state read.
    if (isSchedulingEnabled(config) && !isSchedulingEntryClick && !widgetSchedulingAction && tenantHasPostBookingQuestion(config)) {
      const prep = await capturePrepNote({
        tenantId: config?.tenant_id,
        sessionId,
        userInput,
        deps: {
          loadState: schedulingDeps.loadState,
          saveState: schedulingDeps.saveState,
          invokeAttachPrepNote: schedulingExecDep.invokeSchedulingExecutor,
        },
        write,
      });
      if (prep.captured) {
        console.log('📅 post-booking prep note captured — bypassing Bedrock');
        write('data: [DONE]\n\n');
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        streamEnded = true;
        responseStream.end();
        return;
      }
    }

    // §B17a — agent-turn routing branch (increment 1; integrator glue). Sits AFTER the
    // deterministic click router and the email-capture block (clicks and bare-email
    // capture turns NEVER reach the agent — §B16b/§B16d semantics untouched) and BEFORE
    // the legacy chat path. Engages only when ALL hold:
    //   • §B17h kill switches pass (env AGENTIC_SCHEDULING_DISABLED first, then
    //     feature_flags.scheduling_enabled, then feature_flags.AGENTIC_SCHEDULING)
    //   • a typed-text turn (no scheduling click metadata; form_mode turns returned above)
    //   • an in-flight NEW-booking session row exists (qualifying | proposing | confirming)
    //     — ONE loadState read via the existing seam (fail-soft: error/null → normal chat)
    // Flag off or no session row → fall through unchanged (§B17h: flag-off tenants are
    // byte-identical to the pre-agent baseline; the flag check gates the state read).
    if (
      isAgentTurnEnabled({ tenantConfig: config }) &&
      !isSchedulingEntryClick &&
      !widgetSchedulingAction
    ) {
      const agentSessionRow = await resolveNewBookingSessionRow({
        tenantId: config?.tenant_id,
        sessionId,
        deps: { loadState: schedulingDeps.loadState },
      });
      if (agentSessionRow && NEW_BOOKING_IN_FLIGHT_STATES.includes(agentSessionRow.state)) {
        console.log(`🤖 §B17a agent turn engaged (session state: ${agentSessionRow.state})`);
        await agentTurn({
          event: { userText: userInput, conversationHistory, sessionId },
          context,
          sessionRow: agentSessionRow,
          tenantConfig: config,
          // retrieveKB: F2 (eval A4) — the agent turn retrieves KB context for the
          // user text via the same shared seam the legacy path uses (fail-soft inside).
          deps: { ...schedulingDeps, ...newBookingDep, bedrock, retrieveKB },
          streamWriter: write,
        });
        // End the stream exactly like the deterministic click router does. agentTurn
        // never throws (all failures degrade to honest copy on the stream), so the
        // connection is never left dead.
        write('data: [DONE]\n\n');
        if (heartbeatInterval) {
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
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

    // NOTE: substantive follow-ups retrieve on the clean current input only. A prior
    // "enrichment" step appended the last assistant answer to any input matching a broad
    // follow-up regex; new-topic turns ("Learn about the volunteer process" after a
    // mentoring exchange) got polluted with the prior topic and retrieval drifted
    // cross-program. See docs/roadmap/CONVERSATION_SESSION_STATE_DESIGN.md §2/§10 step 0.

    // Get KB context — errors are handled gracefully so Bedrock can still respond
    let kbContext = '';
    try {
      kbContext = await retrieveKB(kbQuery, config);
    } catch (kbError) {
      console.error('❌ KB retrieval failed, continuing without KB context:', kbError.message);
    }

    const tonePrompt = sanitizeTonePromptV4(config.tone_prompt);
    const basePrompt = buildV4ConversationPrompt(sanitizedInput, kbContext, tonePrompt, conversationHistory, config);
    // WS-C2 (scheduling §5.6): prepend sanitized same-session form data as a
    // <user_application_context> block so the LLM can skip re-qualification.
    // Non-fatal — returns basePrompt unchanged when there's no form data.
    const formPrompt = await injectFormContext(basePrompt, { tenantId: config?.tenant_id, sessionId });
    // WS-CONVO (B3): prepend <scheduling_context> when a §B10 reschedule/cancel binding
    // governs this session (WS-D4 redemption). No-op (prompt unchanged) for normal chat.
    // Feature-gated: scheduling is OFF unless feature_flags.scheduling_enabled (like Forms);
    // when disabled, skip the binding read entirely so the path is fully dormant.
    // Track-D fix 1 (§B17d): deps.loadState activates the in-flight new-booking state
    // line for the AGENTIC_SCHEDULING-off path — the legacy model stops claiming "no
    // scheduling access" mid-booking. No row → prompt byte-identical (no-regression).
    const schedulingEnabled = isSchedulingEnabled(config);
    const prompt = schedulingEnabled
      ? await injectSchedulingContext(formPrompt, {
          tenantId: config?.tenant_id,
          sessionId,
          bindingSessionId,
          deps: { loadState: schedulingDeps.loadState },
        })
      : formPrompt;
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
    
    // Log complete Q&A pair AFTER streaming is done (no impact on user experience).
    // CloudWatch logs are operational, not employee-facing — redact email/phone before
    // emitting. Employee outreach uses form submissions (full PII) and conversation
    // history table (untouched by this redaction).
    if (questionBuffer && responseBuffer) {
      const questionRedacted = redactPII(questionBuffer);
      const answerRedacted = redactPII(responseBuffer);
      console.log('📝 Q&A Pair Captured:');
      console.log(`  Session: ${sessionId}`);
      console.log(`  Tenant: ${tenantHash.substring(0, 8)}...`);
      console.log(`  Question: "${questionRedacted.substring(0, 100)}${questionRedacted.length > 100 ? '...' : ''}"`);
      console.log(`  Answer: "${answerRedacted.substring(0, 200)}${answerRedacted.length > 200 ? '...' : ''}"`);
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
        prompt_versions: {  // sub-phase 1.1 — eval baselines key on prompt text version
          conversation: V4_CONVERSATION_PROMPT_VERSION,
          action_selector: ACTION_SELECTOR_PROMPT_VERSION,
        },
        question: questionRedacted,
        answer: answerRedacted,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: responseBuffer.length
        }
      }));

      // Server-side analytics writes (Issue #5 PR A). Streaming path is
      // fire-and-forget — caller never sees the rejection, so the writer
      // logs its own errors. Frontend MESSAGE_SENT/RECEIVED beacons remain
      // until PR B (post-soak) purges them.
      const clientTimestamp = body.client_timestamp || new Date(startTime).toISOString();
      writeSessionSummary({
        event_type: 'MESSAGE_SENT',
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || '',
        client_timestamp: clientTimestamp,
        request_id: context.awsRequestId,
        event_payload: { first_question: questionBuffer },
      });
      writeSessionSummary({
        event_type: 'MESSAGE_RECEIVED',
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || '',
        client_timestamp: clientTimestamp,
        request_id: context.awsRequestId,
        event_payload: { response_time_ms: firstTokenTime },
      });
    }

    // Enhance response with CTAs after streaming is complete
    try {
      const routingMetadata = body.routing_metadata || {};
      const sessionContext = body.session_context || {};

      const validation = validateTopicDefinitions(config);

      // WS-CONVO (B3 keystone): post-stream §B14 action boundary. Resolves the §B10
      // binding (returns no-op when absent → the CTA logic below runs unchanged) and, when
      // a reschedule/cancel binding governs this turn, detects a STRUCTURED action and
      // executes via the shipped §B9 modules — NEVER on free text. The Google-auth facade +
      // booking/state DDB I/O are the integrator's in-chat wiring seam (deps); until wired,
      // detection + transitions run and execution is skipped non-fatally.
      // Feature-gated (see schedulingEnabled above): when scheduling is OFF, skip the
      // turn entirely → schedulingResult is null → the CTA chain below runs unchanged.
      const schedulingResult = schedulingEnabled
        ? await runSchedulingTurn({
            responseText: responseBuffer, conversationHistory,
            tenantId: config?.tenant_id, sessionId, bindingSessionId, config, bedrock, write,
            deps: { ...schedulingDeps, ...schedulingExecDep },
          })
        : null;

      // WS-NEWBOOK (§B16d): if the recovery loop didn't own this turn, try the NEW-booking
      // entry-hook (engages on routing_metadata.scheduling_intent:'new_booking' or an in-flight
      // new-booking session row). No-op for normal chat. tenantId from config (audit row 9).
      const newBookingResult = (schedulingEnabled && !schedulingResult?.handled)
        ? await runNewBookingEntry({
            responseText: responseBuffer, conversationHistory,
            tenantId: config?.tenant_id, sessionId, config, bedrock, write,
            routingMetadata, deps: { ...schedulingDeps, ...newBookingDep },
          })
        : null;

      if (schedulingResult?.handled || newBookingResult?.handled) {
        // S-3: this turn was a scheduling turn (slot presentation / selection / state
        // progression / the §B14 boundary). The flow owns the post-stream surface —
        // skip normal CTA selection so a scheduling turn doesn't also append CTAs.
        console.log(`[WS-CONVO] scheduling turn handled (action=${schedulingResult?.action || newBookingResult?.action || 'n/a'}) — skipping CTA selection`);
      } else if (routingMetadata.action_chip_triggered || routingMetadata.cta_triggered) {
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
    write(`data: ${JSON.stringify({ type: 'error', error: String(error.message) })}\n\n`);
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

  // Validate CloudFront-injected origin header before any route dispatch.
  // No-op when REQUIRE_CF_ORIGIN_HEADER is unset/false (default rollout).
  // Quiet 403: no CORS headers in the reject path, mirroring streamingHandler
  // and Ticket 1's "no CORS-header leak on rejected requests" precedent.
  const cfCheck = await validateCfOriginHeader(event);
  if (!cfCheck.valid) {
    console.warn(`SECURITY: bufferedHandler rejected request: ${cfCheck.reason}`);
    return {
      statusCode: 403,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'forbidden' }),
    };
  }

  // Route to analytics handler
  const queryParams = event.queryStringParameters || {};
  const body = event.body ? JSON.parse(event.body) : event;

  if (queryParams.action === 'analytics' || body.action === 'analytics') {
    console.log('📊 Routing to analytics handler');
    return await handleAnalyticsEvent(event);
  }

  console.log('📡 Using buffered SSE handler for streaming');

  // Handle OPTIONS
  if (event.httpMethod === 'OPTIONS' || event.requestContext?.http?.method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: corsHeaders(event),
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
    // §B12: the §B10 binding uuid forwarded as ?session= (body.session) — distinct from the
    // chat session_id; keys the recovery-binding read in injectSchedulingContext. null on normal chat.
    const bindingSessionId = (typeof body.session === 'string' && BINDING_UUID_RE.test(body.session)) ? body.session : null;
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
          'X-Accel-Buffering': 'no',
          ...corsHeaders(event)
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
            'X-Accel-Buffering': 'no',
            ...corsHeaders(event)
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
            ...corsHeaders(event)
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
    const basePrompt = buildV4ConversationPrompt(sanitizedInput, kbContext, tonePrompt, conversationHistory, config);
    // WS-C2 (scheduling §5.6): prepend sanitized same-session form data as a
    // <user_application_context> block so the LLM can skip re-qualification.
    // Non-fatal — returns basePrompt unchanged when there's no form data.
    const formPrompt = await injectFormContext(basePrompt, { tenantId: config?.tenant_id, sessionId });
    // WS-CONVO (B3): prepend <scheduling_context> when a §B10 reschedule/cancel binding
    // governs this session (WS-D4 redemption). No-op (prompt unchanged) for normal chat.
    // Feature-gated: OFF unless feature_flags.scheduling_enabled (like Forms).
    const schedulingEnabled = isSchedulingEnabled(config);
    const prompt = schedulingEnabled
      ? await injectSchedulingContext(formPrompt, { tenantId: config?.tenant_id, sessionId, bindingSessionId })
      : formPrompt;
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
    
    // Log complete Q&A pair for analytics. CloudWatch logs are operational —
    // redact email/phone before emitting (employee outreach uses form
    // submissions + conversation history, both untouched by this).
    if (questionBuffer && responseBuffer) {
      const questionRedacted = redactPII(questionBuffer);
      const answerRedacted = redactPII(responseBuffer);
      console.log(JSON.stringify({
        type: 'QA_COMPLETE',
        timestamp: new Date().toISOString(),
        session_id: sessionId,
        tenant_hash: tenantHash,
        tenant_id: config?.tenant_id || null,  // Add tenant_id from config
        conversation_id: body.conversation_id || sessionId,  // Add conversation_id
        prompt_versions: {  // sub-phase 1.1 — eval baselines key on prompt text version
          conversation: V4_CONVERSATION_PROMPT_VERSION,
          action_selector: ACTION_SELECTOR_PROMPT_VERSION,
        },
        question: questionRedacted,
        answer: answerRedacted,
        metrics: {
          first_token_ms: firstTokenTime,
          total_tokens: tokenCount,
          total_time_ms: totalTime,
          answer_length: responseBuffer.length
        }
      }));

      // Server-side analytics writes (Issue #5 PR A). Buffered path awaits;
      // separated from the CTA-enhancement try/catch below (different timeout
      // budget, different concern). Writer logs its own errors.
      const clientTimestamp = body.client_timestamp || new Date(startTime).toISOString();
      try {
        await writeSessionSummary({
          event_type: 'MESSAGE_SENT',
          session_id: sessionId,
          tenant_hash: tenantHash,
          tenant_id: config?.tenant_id || '',
          client_timestamp: clientTimestamp,
          request_id: context.awsRequestId,
          event_payload: { first_question: questionBuffer },
        });
        await writeSessionSummary({
          event_type: 'MESSAGE_RECEIVED',
          session_id: sessionId,
          tenant_hash: tenantHash,
          tenant_id: config?.tenant_id || '',
          client_timestamp: clientTimestamp,
          request_id: context.awsRequestId,
          event_payload: { response_time_ms: firstTokenTime },
        });
      } catch (e) {
        console.log(JSON.stringify({ evt: 'analytics_write_caller_failure', error: 'internal_error' }));
      }
    }

    // Enhance response with CTAs after generation is complete
    try {
      const routingMetadata = body.routing_metadata || {};
      const sessionContext = body.session_context || {};

      const validation = validateTopicDefinitions(config);

      // WS-CONVO (B3 keystone): post-stream §B14 action boundary (buffered path). Same
      // contract as the streaming path; SSE is emitted by splicing into `chunks` (mirrors
      // the CTA splice below). No-op when no §B10 binding governs the session.
      // Feature-gated (see schedulingEnabled above): OFF → null → CTA chain unchanged.
      const schedulingResult = schedulingEnabled
        ? await runSchedulingTurn({
            responseText: responseBuffer, conversationHistory,
            tenantId: config?.tenant_id, sessionId, bindingSessionId, config, bedrock,
            write: (data) => chunks.splice(chunks.length - 1, 0, data),
            deps: { ...schedulingDeps, ...schedulingExecDep },
          })
        : null;

      // WS-NEWBOOK (§B16d): NEW-booking entry-hook (buffered path), only if the recovery loop
      // didn't own this turn. No-op for normal chat. SSE spliced into `chunks` like above.
      const newBookingResult = (schedulingEnabled && !schedulingResult?.handled)
        ? await runNewBookingEntry({
            responseText: responseBuffer, conversationHistory,
            tenantId: config?.tenant_id, sessionId, config, bedrock,
            write: (data) => chunks.splice(chunks.length - 1, 0, data),
            routingMetadata, deps: { ...schedulingDeps, ...newBookingDep },
          })
        : null;

      if (schedulingResult?.handled || newBookingResult?.handled) {
        // S-3: scheduling turn owned the post-stream surface — skip CTA selection.
        console.log(`[WS-CONVO] scheduling turn handled (action=${schedulingResult?.action || newBookingResult?.action || 'n/a'}) — skipping CTA selection`);
      } else if (routingMetadata.action_chip_triggered || routingMetadata.cta_triggered) {
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
        'X-Accel-Buffering': 'no',
        ...corsHeaders(event)
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
        ...corsHeaders(event)
      },
      body: chunks.join('')
    };
  }
};

// Export the appropriate handler based on streaming support.
//
// CORS-path asymmetry: under streamifyResponse, the streamingHandler's
// returned `{statusCode, headers, body}` has its `headers` field silently
// dropped by AWS — the Lambda URL CORS config is the active CORS gatekeeper
// for that path. The 16 `corsHeaders(event)` calls above are live only when
// this export resolves to `bufferedHandler` (local invocation / non-streaming
// runtime). Keep `cors-helper.js` AND the Lambda URL CORS config in sync;
// see cors-helper.js module header.
exports.handler = streamifyResponse ? streamifyResponse(streamingHandler) : bufferedHandler;