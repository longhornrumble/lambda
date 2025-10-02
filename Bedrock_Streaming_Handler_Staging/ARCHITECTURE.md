# Bedrock Streaming Handler Architecture

## System Overview

The Bedrock Streaming Handler is a Node.js 20.x Lambda function that provides real-time conversational AI responses using Amazon Bedrock, with specialized support for conversational forms. It implements true Lambda response streaming for optimal performance and supports two distinct operational modes.

### Purpose and Responsibilities

- **Primary**: Stream AI responses from Amazon Bedrock (Claude 3.5 Haiku) to frontend clients
- **Secondary**: Handle conversational form validation and submission without AI overhead
- **Supporting**: Enhance responses with context-aware CTAs and manage form workflows

### Key Features

- True Lambda response streaming via `awslambda.streamifyResponse`
- Dual-mode architecture (Normal vs Form Mode)
- Knowledge Base integration for RAG (Retrieval Augmented Generation)
- In-memory caching (5-minute TTL) for configs and KB results
- Multi-channel form fulfillment (Email, SMS, Lambda, S3, Webhooks)
- Intelligent CTA injection based on conversation context
- Phase 1B form interruption and program switching support

### Integration Points

```
┌─────────────────┐
│  Picasso Widget │
│   (Frontend)    │
└────────┬────────┘
         │
         │ HTTPS POST (SSE)
         │
         ↓
┌─────────────────────────────────────────┐
│   Bedrock_Streaming_Handler_Staging     │
│                                         │
│  ┌──────────┐          ┌─────────────┐ │
│  │  Normal  │          │  Form Mode  │ │
│  │   Mode   │          │   (Bypass)  │ │
│  └────┬─────┘          └──────┬──────┘ │
└───────┼─────────────────────--┼────────┘
        │                        │
        ↓                        ↓
┌───────────────┐        ┌──────────────┐
│ Bedrock Agent │        │  DynamoDB    │
│   Runtime     │        │  (Forms)     │
│  (KB Query)   │        └──────────────┘
└───────┬───────┘                │
        │                        ↓
        ↓                ┌──────────────┐
┌───────────────┐        │ SES / SNS /  │
│   Bedrock     │        │ Lambda / S3  │
│   Runtime     │        │  (Fulfill)   │
│  (Streaming)  │        └──────────────┘
└───────────────┘
```

### Request/Response Flow

#### Normal Conversation Flow

```
1. Widget sends user message
   └→ POST /invoke with conversation history

2. Handler loads tenant config from S3
   └→ Cached for 5 minutes

3. Retrieve KB context (if available)
   └→ BedrockAgentRuntime.RetrieveCommand
   └→ Cached per query hash

4. Build prompt with:
   - Tone instructions
   - Conversation history
   - KB context
   - Anti-hallucination constraints

5. Stream Bedrock response
   └→ BedrockRuntime.InvokeModelWithResponseStreamCommand
   └→ SSE chunks sent immediately

6. Enhance with CTAs (after streaming complete)
   └→ response_enhancer checks branches
   └→ Filters completed/suspended forms
   └→ Sends CTA event if matched

7. Close SSE stream with [DONE]
```

#### Form Mode Flow

```
1. Widget sends form action
   └→ POST /invoke with form_mode=true

2. Handler detects form_mode flag
   └→ Skip Bedrock completely

3. Route to form_handler.handleFormMode()

4. If action='validate_field':
   └→ Validate single field (email, phone, etc.)
   └→ Return validation result

5. If action='submit_form':
   └→ Determine priority (high/normal/low)
   └→ Save to DynamoDB
   └→ Route to fulfillment channels
   └→ Send confirmation email
   └→ Return success/error

6. Return form response (no streaming)
```

## Mode Architecture

### Why Two Modes?

The dual-mode architecture optimizes for different use cases:

**Normal Mode** - For conversations requiring AI intelligence:
- User asks questions
- Complex decision making needed
- Knowledge base context required
- Natural language understanding
- **Cost**: ~$0.003 per 1K tokens
- **Latency**: 200-800ms first token

**Form Mode** - For deterministic field operations:
- Real-time field validation
- Form submission processing
- No AI interpretation needed
- Simple business logic
- **Cost**: $0 (no Bedrock call)
- **Latency**: <50ms response

### Decision Flow: When to Use Each Mode

```
User Action
    │
    ├─ Typing in form field? ──→ Form Mode (validate_field)
    │                              - Email regex check
    │                              - Phone format validation
    │                              - Required field check
    │
    ├─ Clicked "Submit"? ──────→ Form Mode (submit_form)
    │                              - Priority determination
    │                              - Multi-channel fulfillment
    │                              - Confirmation workflow
    │
    └─ General conversation? ───→ Normal Mode
                                   - Bedrock streaming
                                   - KB retrieval
                                   - CTA enhancement
```

### Mode Detection (index.js:390-416)

```javascript
// Check for form mode - bypass Bedrock for form field collection
if (body.form_mode === true) {
  console.log('📝 Form mode detected - handling locally without Bedrock');
  const formResponse = await handleFormMode(body, config);

  // Send form response as SSE event
  write(`data: ${JSON.stringify(formResponse)}\n\n`);
  write('data: [DONE]\n\n');

  // End stream immediately
  streamEnded = true;
  responseStream.end();
  return;
}

// Otherwise, proceed with normal Bedrock flow...
```

## Module Responsibilities

### index.js - Main Handler & Orchestration

**Primary Functions:**
- Request parsing and validation
- Config loading with caching
- KB retrieval and caching
- Prompt construction
- Bedrock streaming orchestration
- SSE response formatting
- Mode routing (normal vs form)

**Key Components:**

1. **streamingHandler()** (lines 308-561)
   - True Lambda response streaming
   - Heartbeat mechanism (2s interval)
   - Real-time SSE chunk forwarding
   - Q&A logging for analytics

2. **bufferedHandler()** (lines 566-773)
   - Fallback when streaming unavailable
   - Buffers all chunks then sends
   - Same logic, different delivery

3. **loadConfig()** (lines 51-111)
   - S3 tenant config loading
   - Hash → tenant_id mapping
   - 5-minute TTL caching
   - Handles multiple config paths

4. **retrieveKB()** (lines 113-160)
   - BedrockAgent KB query
   - Returns top 5 results
   - MD5 hash-based caching
   - Graceful degradation on error

5. **buildPrompt()** (lines 162-303)
   - Combines tone + history + KB
   - Anti-hallucination constraints
   - Markdown preservation rules
   - Response formatting template

### form_handler.js - Field Validation & Submission

**Primary Functions:**
- Form field validation (real-time)
- Form submission processing
- Priority determination
- Multi-channel fulfillment routing

**Key Components:**

1. **handleFormMode()** (lines 36-64)
   - Entry point for form mode
   - Routes to validate or submit
   - Returns structured responses

2. **validateFormField()** (lines 73-125)
   - Email regex validation
   - Phone format checking
   - Age/commitment confirmations
   - Required field enforcement

3. **submitForm()** (lines 134-185)
   - End-to-end submission flow
   - Priority determination
   - DynamoDB storage
   - Fulfillment orchestration
   - Confirmation emails

4. **determinePriority()** (lines 191-228)
   - Explicit urgency field check
   - Config-based priority rules
   - Form-type defaults
   - 3-tier system: high/normal/low

5. **routeFulfillment()** (lines 265-384)
   - Lambda async invocation
   - S3 archival storage
   - Email with priority
   - SMS with rate limiting
   - Webhook POST

6. **SMS Rate Limiting** (lines 390-462)
   - getMonthlySMSUsage()
   - incrementSMSUsage()
   - DynamoDB monthly tracking
   - Configurable limit (default 100)
   - Graceful degradation

### response_enhancer.js - CTA Injection & Form Flow

**Primary Functions:**
- Detect conversation context
- Inject relevant CTAs
- Track completed/suspended forms
- Handle program switching (Phase 1B)

**Key Components:**

1. **enhanceResponse()** (lines 274-479)
   - Main enhancement pipeline
   - Loads tenant config
   - Checks suspended forms
   - Detects program switches
   - Filters completed forms
   - Returns enhanced response

2. **detectConversationBranch()** (lines 95-236)
   - Matches response to branches
   - Checks user engagement
   - Filters by completed forms
   - Returns CTAs (max 3)

3. **checkFormTriggers()** (lines 242-269)
   - Matches trigger phrases
   - Returns form metadata
   - Priority over branch CTAs

4. **loadTenantConfig()** (lines 41-89)
   - S3 config loading
   - Hash resolution
   - 5-minute caching
   - Extracts CTA config

## Phase 1B Features

### Suspended Forms Tracking

**Problem**: User starts form, then asks unrelated question. System should remember the incomplete form.

**Solution** (response_enhancer.js:295-374):

```javascript
// Extract from session context
const suspendedForms = sessionContext.suspended_forms || [];

if (suspendedForms.length > 0) {
  console.log(`[Phase 1B] 🔄 Suspended form detected: ${suspendedForms[0]}`);

  // Check if user is asking about DIFFERENT program
  const triggeredForm = checkFormTriggers(bedrockResponse, userMessage, config);

  if (triggeredForm && triggeredForm.formId !== suspendedForms[0]) {
    // Detected program switch - provide metadata for frontend
    return {
      metadata: {
        program_switch_detected: true,
        suspended_form: {...},
        new_form_of_interest: {...}
      }
    };
  }

  // Otherwise, skip CTAs until form resumed/canceled
  return { ctaButtons: [] };
}
```

**Frontend UX**: Widget shows "You have an incomplete application for [Program]. Would you like to resume or switch?"

### Program Switching Detection

**Problem**: User filling "Volunteer Application", selects "Love Box" interest, then asks "Tell me about Dare to Dream" - system should offer intelligent switch.

**Solution** (response_enhancer.js:304-361):

```javascript
if (newFormId !== suspendedFormId) {
  console.log(`[Phase 1B] 🔀 Program switch detected!`);

  // Get program names for UX
  const newProgramName = (triggeredForm.title || '').replace(' Application', '');
  const suspendedProgramName = getSuspendedProgramName(suspendedFormId, programInterest);

  return {
    metadata: {
      program_switch_detected: true,
      suspended_form: {
        form_id: suspendedFormId,
        program_name: suspendedProgramName
      },
      new_form_of_interest: {
        form_id: newFormId,
        program_name: newProgramName,
        cta_text: triggeredForm.ctaText,
        fields: triggeredForm.fields
      }
    }
  };
}
```

**Frontend Behavior**: Show switch modal with options:
- "Continue [Suspended Program]"
- "Switch to [New Program]"
- "Cancel Application"

### Program Interest Mapping

**Problem**: Generic "volunteer_apply" form can apply to multiple programs. Need to show user-friendly names.

**Solution** (response_enhancer.js:331-341):

```javascript
const programInterest = sessionContext.program_interest;

if (programInterest) {
  const programMap = {
    'lovebox': 'Love Box',
    'daretodream': 'Dare to Dream',
    'both': 'both programs',
    'unsure': 'Volunteer'
  };

  suspendedProgramName = programMap[programInterest.toLowerCase()] || suspendedProgramName;
}
```

**Use Case**: User selects "Love Box" in form → Stored as `program_interest: "lovebox"` → Displayed as "Love Box Application"

## Data Flow Diagrams

### Normal Conversation Flow

```
┌─────────┐
│  User   │
│ Widget  │
└────┬────┘
     │
     │ {"tenant_hash": "abc123",
     │  "user_input": "What volunteer opportunities exist?",
     │  "conversation_history": [...],
     │  "session_context": {...}}
     │
     ↓
┌────────────────────────────────┐
│  index.js::streamingHandler()  │
│                                │
│  1. Parse request              │
│  2. loadConfig(tenant_hash)    │  ──→  S3 config (cached 5min)
│  3. retrieveKB(user_input)     │  ──→  Bedrock Agent KB
│  4. buildPrompt(...)           │
│  5. InvokeModelWithStream      │  ──→  Bedrock Runtime
│                                │
│  6. Stream SSE chunks:         │
│     data: {"type":"text",      │
│            "content":"We..."}  │
│                                │
│  7. enhanceResponse(...)       │  ──→  response_enhancer
│     - Check branches           │
│     - Filter completed forms   │
│     - Detect program switch    │
│                                │
│  8. Send CTA event (if any)    │
│     data: {"type":"cta_buttons"│
│            "ctaButtons":[...]} │
│                                │
│  9. Send completion:           │
│     data: [DONE]               │
└────────────────────────────────┘
     │
     │ Server-Sent Events Stream
     │
     ↓
┌─────────┐
│  User   │
│ Widget  │
│ Renders │
└─────────┘
```

### Form Field Validation Flow

```
┌─────────┐
│  User   │
│ Types   │
│ "user@" │
└────┬────┘
     │
     │ {"form_mode": true,
     │  "action": "validate_field",
     │  "field_id": "email",
     │  "field_value": "user@example.com"}
     │
     ↓
┌────────────────────────────────┐
│  index.js::streamingHandler()  │
│                                │
│  if (body.form_mode === true)  │
│    └→ handleFormMode(body)     │
└────────────────────────────────┘
     │
     ↓
┌────────────────────────────────┐
│ form_handler.js::handleFormMode│
│                                │
│  action === 'validate_field'   │
│    └→ validateFormField(...)   │
└────────────────────────────────┘
     │
     ↓
┌────────────────────────────────┐
│ form_handler.js::               │
│   validateFormField()           │
│                                │
│  1. Check required             │
│  2. Regex validation (email)   │
│  3. Format check (phone)       │
│  4. Business rules (age, etc)  │
│                                │
│  Return:                       │
│  - validation_success OR       │
│  - validation_error            │
└────────────────────────────────┘
     │
     │ {"type": "validation_success",
     │  "field": "email",
     │  "status": "success"}
     │
     ↓
┌─────────┐
│ Widget  │
│ Shows ✓ │
└─────────┘
```

### Form Submission Flow

```
┌─────────┐
│  User   │
│ Clicks  │
│ Submit  │
└────┬────┘
     │
     │ {"form_mode": true,
     │  "action": "submit_form",
     │  "form_id": "volunteer_apply",
     │  "form_data": {...}}
     │
     ↓
┌────────────────────────────────┐
│ form_handler.js::submitForm()  │
│                                │
│  1. Validate parameters        │
│  2. determinePriority()        │  ──→ high/normal/low
│  3. saveFormSubmission()       │  ──→ DynamoDB
│  4. routeFulfillment()         │
└────────────────────────────────┘
     │
     ↓
┌────────────────────────────────┐
│ form_handler.js::               │
│   routeFulfillment()            │
│                                │
│  Check config.fulfillment:     │
│                                │
│  ├─ type='lambda'?             │  ──→ InvokeCommand (async)
│  ├─ type='s3'?                 │  ──→ PutObjectCommand
│  ├─ email_to?                  │  ──→ SendEmailCommand
│  ├─ sms_to?                    │
│  │  └─ Check rate limit        │  ──→ DynamoDB usage
│  │     └─ Under limit?         │  ──→ PublishCommand (SMS)
│  │        └─ Increment usage   │  ──→ UpdateCommand
│  └─ webhook_url?               │  ──→ HTTPS POST
│                                │
│  5. sendConfirmationEmail()    │  ──→ SES (non-blocking)
└────────────────────────────────┘
     │
     │ {"type": "form_complete",
     │  "status": "success",
     │  "submissionId": "volunteer_1234",
     │  "priority": "normal",
     │  "fulfillment": [...]}
     │
     ↓
┌─────────┐
│ Widget  │
│ Success │
└─────────┘
```

### Error Handling Paths

```
Error Scenarios                 Handling Strategy
────────────────────────────────────────────────────
S3 Config Load Fails           → Use defaults, log error, continue
KB Retrieval Fails             → Empty context, fallback prompt
Bedrock Stream Error           → Send error event, close stream
DynamoDB Save Fails            → Log warning, continue fulfillment
SES Email Fails                → Log error, record in fulfillment
SMS Rate Limit Hit             → Skip SMS, record reason
Confirmation Email Fails       → Log error, don't block success
Missing Fulfillment Config     → Log warning, graceful degradation
Invalid Form Field             → Return validation_error response
```

## Session Management

### Session Context Structure

The `session_context` object tracks user state across the conversation:

```javascript
{
  "completed_forms": ["lovebox"],          // Form IDs user has completed
  "suspended_forms": ["volunteer_apply"],  // Form IDs user started but didn't finish
  "program_interest": "daretodream",       // Program selected in volunteer form
  "conversation_id": "conv_abc123",        // Unique conversation identifier
  "user_preferences": {...}                // Additional user data
}
```

### Completed Forms Tracking

**Purpose**: Prevent showing CTAs for programs user already applied to.

**Flow**:
1. User completes form → Widget adds to `completed_forms`
2. Response enhancer receives context
3. Filters CTAs: `completedForms.includes(program)` → skip
4. Only shows CTAs for programs not yet applied

**Example** (response_enhancer.js:408):
```javascript
const completedForms = sessionContext.completed_forms || [];

const branchResult = detectConversationBranch(
  bedrockResponse,
  userMessage,
  config,
  completedForms  // Pass to filter CTAs
);
```

### Suspended Forms Tracking

**Purpose**: Enable form resumption after interruptions.

**Flow**:
1. User starts form → Widget adds to `suspended_forms`
2. User asks unrelated question → Form remains suspended
3. Response enhancer detects suspension → Skips new form CTAs
4. User can resume later → Widget removes from `suspended_forms`

**Decision Tree**:
```
Has suspended form?
  ├─ No → Show CTAs normally
  └─ Yes
      ├─ Same form triggered? → Skip CTA (already in progress)
      └─ Different form triggered?
          └─ Return program_switch metadata → Frontend shows switch UI
```

### Program Interest Tracking

**Purpose**: Personalize volunteer applications based on program selection.

**Flow**:
1. User selects program in volunteer form → Stored as `program_interest`
2. User interrupts to ask about other program
3. Response enhancer uses `program_interest` to show correct suspended program name
4. Widget can display: "Resume Love Box Application?" (not "Resume Volunteer")

**Mapping**:
- `"lovebox"` → "Love Box"
- `"daretodream"` → "Dare to Dream"
- `"both"` → "both programs"
- `"unsure"` → "Volunteer"

## Caching Strategy

### Configuration Caching (5-minute TTL)

**Cache Key**: `config:{tenant_hash}`

**Cache Entry**:
```javascript
{
  data: {
    tenant_id: "abc123",
    conversation_branches: {...},
    cta_definitions: {...},
    conversational_forms: {...},
    aws: {...}
  },
  timestamp: 1696184523000
}
```

**Invalidation**:
- Automatic: After 5 minutes (`Date.now() - timestamp > 300000`)
- Manual: Clear cache via Lambda restart or update

**Benefits**:
- Reduces S3 calls from ~100/min to ~1/min per tenant
- Latency reduction: 150ms → <1ms
- Cost savings: $0.0004/1K requests → ~96% reduction

### KB Result Caching (5-minute TTL)

**Cache Key**: `kb:{kb_id}:{md5(user_input)}`

**Cache Entry**:
```javascript
{
  data: "**Context 1:**\n[KB chunk text]\n\n---\n\n**Context 2:**\n[KB chunk text]",
  timestamp: 1696184523000
}
```

**Why MD5 Hash?**
- Deterministic: Same query → Same hash → Cache hit
- Fast: MD5 computation <1ms
- Compact: 32-char key regardless of query length

**Invalidation**:
- Automatic: After 5 minutes
- Query variation: Different wording = different hash = no cache hit

**Benefits**:
- Common queries cached (e.g., "How do I volunteer?")
- Latency reduction: 200-500ms → <1ms
- Bedrock KB cost reduction: ~$0.002/query → free on cache hit

### Cache Warming Strategy

Not currently implemented, but could add:

1. **Predictive warming**: Pre-load common queries after config update
2. **Background refresh**: Async refresh 30s before TTL expiry
3. **LRU eviction**: If memory constrained, keep most-used entries

## Error Handling Philosophy

### Graceful Degradation

**Principle**: System should never completely fail. Degrade functionality but maintain core service.

**Examples**:

1. **KB Unavailable**:
   ```javascript
   if (!kbId) {
     console.log('⚠️ No KB ID - returning empty context');
     return '';  // Bedrock uses just tone + history
   }
   ```

2. **Config Load Fails**:
   ```javascript
   if (!config) {
     config = {
       model_id: DEFAULT_MODEL_ID,
       streaming: { max_tokens: 1000, temperature: 0 },
       tone_prompt: DEFAULT_TONE
     };
   }
   ```

3. **SMS Rate Limit Hit**:
   ```javascript
   if (usage >= SMS_MONTHLY_LIMIT) {
     console.warn(`⚠️ SMS monthly limit reached`);
     results.push({ channel: 'sms', status: 'skipped', reason: 'monthly_limit_reached' });
     // Continue with other channels
   }
   ```

### Non-Blocking Failures

**Principle**: Background operations should not block primary user flow.

**Examples**:

1. **Confirmation Email**:
   ```javascript
   if (formData.email && config.send_confirmation_email !== false) {
     sendConfirmationEmail(formData.email, formId, config).catch(err => {
       console.error('❌ Confirmation email failed:', err.message);
       // Don't throw - form submission already succeeded
     });
   }
   ```

2. **DynamoDB Save**:
   ```javascript
   try {
     await saveFormSubmission(submissionId, formId, formData, config, priority);
   } catch (dbError) {
     console.error('❌ DynamoDB save failed:', dbError);
     // Continue with fulfillment even if DynamoDB save fails
   }
   ```

3. **CTA Enhancement**:
   ```javascript
   try {
     const enhancedData = await enhanceResponse(...);
     if (enhancedData.ctaButtons?.length > 0) {
       write(`data: ${ctaData}\n\n`);
     }
   } catch (enhanceError) {
     console.error('❌ CTA enhancement error:', enhanceError);
     // Don't fail response if CTA enhancement fails
   }
   ```

### Error Response Formats

**Streaming Error**:
```json
{
  "type": "error",
  "error": "Missing tenant_hash"
}
```

**Form Validation Error**:
```json
{
  "type": "validation_error",
  "field": "email",
  "errors": ["Please enter a valid email address"],
  "status": "error"
}
```

**Form Submission Error**:
```json
{
  "type": "form_error",
  "status": "error",
  "message": "There was an error submitting your form. Please try again.",
  "error": "Missing required parameters: formId, formData, or config"
}
```

**Partial Fulfillment Failure**:
```json
{
  "type": "form_complete",
  "status": "success",
  "submissionId": "volunteer_1234",
  "fulfillment": [
    {"channel": "email", "status": "sent"},
    {"channel": "sms", "status": "failed", "error": "SNS error"},
    {"channel": "webhook", "status": "sent"}
  ]
}
```

## Architecture Decision Records

### ADR-1: Dual-Mode Architecture

**Decision**: Implement separate code paths for AI conversation vs form operations.

**Rationale**:
- Form validation is deterministic - doesn't need AI
- Bedrock costs ~$0.003 per 1K tokens - wasteful for simple validation
- Latency requirements differ (50ms validation vs 500ms AI response)
- Allows optimizations specific to each use case

**Consequences**:
- ✅ 10x faster form validation (no Bedrock call)
- ✅ Zero cost for form operations
- ✅ Better error handling (form-specific vs AI errors)
- ❌ More code complexity (two paths to maintain)
- ❌ Need to carefully route requests

### ADR-2: In-Memory Caching

**Decision**: Use simple in-memory objects for caching, not Redis/ElastiCache.

**Rationale**:
- Lambda warm instances persist for 5-15 minutes
- Config changes are infrequent (hours/days)
- Adding Redis adds cost ($50-200/month) and latency (network hop)
- 5-minute TTL matches typical Lambda warm duration

**Consequences**:
- ✅ Zero infrastructure cost
- ✅ Fastest possible cache hits (<1ms)
- ✅ No network dependencies
- ❌ Cache not shared across instances
- ❌ Cache lost on cold start (acceptable trade-off)

### ADR-3: Streaming-First Architecture

**Decision**: Use `awslambda.streamifyResponse` for true streaming, with buffered fallback.

**Rationale**:
- First token latency critical for UX (perceived speed)
- Users see response building in real-time
- Bedrock streaming available - leverage it
- Buffered mode for compatibility

**Consequences**:
- ✅ 200-400ms first token (vs 2-3s buffered)
- ✅ Better perceived performance
- ✅ Can handle long responses (no timeout)
- ❌ More complex error handling
- ❌ Can't modify response after streaming starts

### ADR-4: Phase 1B Form Interruption Support

**Decision**: Track suspended forms in session context and offer intelligent switching.

**Rationale**:
- Users naturally ask clarifying questions mid-form
- Forcing form completion creates friction
- Other programs might be better fit after user learns more
- Matches Master_Function_Staging behavior (parity requirement)

**Consequences**:
- ✅ Better UX - users can explore then return
- ✅ Higher conversion - users find best program
- ✅ Reduced abandonment - can switch vs quit
- ❌ More complex state management
- ❌ Requires frontend support for switch UI

### ADR-5: AWS SDK v3 Migration

**Decision**: Migrate from AWS SDK v2 to v3 for all services.

**Rationale**:
- SDK v2 has known security vulnerabilities
- SDK v3 is modular - smaller bundle size
- SDK v3 is actively maintained
- Required for Phase 1B parity

**Consequences**:
- ✅ Security vulnerabilities eliminated
- ✅ 30-40% smaller deployment package
- ✅ Better TypeScript support
- ✅ Tree-shakeable imports
- ❌ Breaking API changes (one-time migration cost)
- ❌ Some v2 patterns no longer work

## Performance Characteristics

### Latency Benchmarks

**Normal Mode** (with warm cache):
- First token: 200-400ms
- Subsequent tokens: 20-50ms intervals
- Total response: 2-5s (varies by length)
- CTA enhancement: +50-100ms (after streaming)

**Form Mode**:
- Field validation: 30-50ms
- Form submission: 100-200ms (excluding async fulfillment)
- Multi-channel fulfillment: 200-500ms (parallel)

**Cache Performance**:
- Config cache hit: <1ms
- Config cache miss: 150-200ms (S3 load)
- KB cache hit: <1ms
- KB cache miss: 200-500ms (Bedrock Agent query)

### Throughput

**Concurrent Requests**: Lambda auto-scales to 1000 concurrent executions (default)

**Per-Instance Throughput**:
- Streaming: 1 request at a time (long-lived connection)
- Form mode: 10-20 req/sec (quick responses)

**Bottlenecks**:
- Bedrock quotas: 200 TPS (transactions per second) per model
- KB queries: 25 TPS per knowledge base
- S3 gets: 5,500 TPS per prefix (not a concern)

### Cost Analysis

**Per Conversation** (Normal Mode):
- Bedrock inference: $0.003-0.015 (varies by response length)
- KB retrieval: $0.002 per query
- S3 config: $0.0004 per request (or $0 if cached)
- Lambda: $0.000002 per 100ms
- **Total**: ~$0.005-0.020 per conversation

**Per Form Submission** (Form Mode):
- Lambda: $0.000001-0.000005
- DynamoDB: $0.00025 per write
- SES: $0.10 per 1000 emails
- SNS: $0.50 per 1000 SMS
- **Total**: ~$0.001-0.005 (varies by fulfillment channels)

## Security Considerations

### Authentication

- **No JWT required** - Uses tenant_hash for identification
- Tenant hash mapped to tenant_id via S3
- S3 access controlled by Lambda execution role

### Data Privacy

- **PII Handling**: Form data stored in DynamoDB with tenant isolation
- **Encryption**: All data encrypted at rest (S3, DynamoDB) and in transit (TLS)
- **Retention**: No built-in data retention policy (add if needed)

### Input Validation

- **Form fields**: Regex validation for email/phone
- **Required parameters**: Checked before processing
- **Prompt injection**: Mitigated by structured prompt template

### Secrets Management

- **API Keys**: Stored in environment variables (Lambda config)
- **AWS Credentials**: IAM role-based (no hardcoded keys)
- **Tenant Configs**: S3 bucket with restricted access

## Monitoring and Observability

### CloudWatch Logs

**Structured Logging Format**:
```javascript
console.log(JSON.stringify({
  type: 'QA_COMPLETE',
  timestamp: new Date().toISOString(),
  session_id: sessionId,
  tenant_hash: tenantHash,
  tenant_id: config?.tenant_id,
  conversation_id: body.conversation_id,
  question: questionBuffer,
  answer: responseBuffer,
  metrics: {
    first_token_ms: firstTokenTime,
    total_tokens: tokenCount,
    total_time_ms: totalTime,
    answer_length: responseBuffer.length
  }
}));
```

**Log Patterns to Monitor**:
- `❌` - Errors requiring attention
- `⚠️` - Warnings (degraded functionality)
- `✅` - Success confirmations
- `[Phase 1B]` - Form flow events

### Key Metrics

**Operational**:
- Lambda invocations (count)
- Lambda errors (count, rate)
- Lambda duration (p50, p95, p99)
- Lambda concurrent executions

**Business**:
- QA_COMPLETE events (conversation count)
- Form submissions by type
- Form submission priority breakdown
- SMS usage vs limit

**Performance**:
- First token latency (ms)
- Total response time (ms)
- Cache hit rate (config, KB)
- Bedrock throttles (count)

### Alerts to Configure

1. **Error Rate > 5%** (5min window)
2. **P95 Latency > 3s** (5min window)
3. **Bedrock Throttles > 10** (5min window)
4. **Form Submission Failures > 10** (5min window)
5. **SMS Monthly Limit > 90%** (daily check)

## Future Enhancements

### Potential Improvements

1. **Multi-Region Failover**
   - Deploy to us-west-2 as backup
   - Route53 health checks
   - Automatic failover on regional outage

2. **Advanced Caching**
   - ElastiCache Redis for shared cache
   - Cache warming on config update
   - Predictive pre-loading of common queries

3. **Enhanced Analytics**
   - Conversation sentiment analysis
   - Form abandonment tracking
   - A/B testing for prompts

4. **Performance Optimization**
   - Parallel KB + config loading
   - HTTP/2 for S3 requests
   - Compression for large responses

5. **Feature Additions**
   - Multi-language support
   - Voice input transcription
   - Image/document upload in forms

---

**Document Version**: 1.0
**Last Updated**: 2025-10-01
**Maintained By**: Backend Engineering Team
