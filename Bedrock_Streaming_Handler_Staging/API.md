# Bedrock Streaming Handler API Documentation

## Overview

This document provides comprehensive API documentation for the Bedrock_Streaming_Handler_Staging Lambda function. The API supports two distinct modes: Normal Mode for AI conversations and Form Mode for field validation/submission.

## Base Endpoint

**Lambda Function URL**: `https://<function-url-id>.lambda-url.us-east-1.on.aws/`

**Method**: `POST`

**Content-Type**: `application/json`

**Response Type**: `text/event-stream` (Server-Sent Events)

## Authentication

**No JWT Required** - The handler uses `tenant_hash` for tenant identification.

**Headers**:
```
Content-Type: application/json
Accept: text/event-stream
```

## Request Formats

### Normal Mode Request

Used for AI-powered conversations with Bedrock streaming.

```json
{
  "tenant_hash": "abc123def456",
  "user_input": "What volunteer opportunities do you have?",
  "session_id": "session_7f3b9e12",
  "conversation_id": "conv_abc123",
  "conversation_history": [
    {
      "role": "user",
      "content": "Hello"
    },
    {
      "role": "assistant",
      "content": "Hi! How can I help you today?"
    }
  ],
  "session_context": {
    "completed_forms": ["lovebox"],
    "suspended_forms": [],
    "program_interest": null
  }
}
```

**Required Fields**:
- `tenant_hash` (string): Unique identifier for the tenant
- `user_input` (string): User's message/question

**Optional Fields**:
- `session_id` (string): Session identifier (defaults to "default")
- `conversation_id` (string): Conversation tracking ID
- `conversation_history` (array): Previous messages in conversation
- `session_context` (object): Session state tracking

**Session Context Fields**:
- `completed_forms` (array): Form IDs user has completed
- `suspended_forms` (array): Form IDs user started but didn't finish
- `program_interest` (string): Program selected in volunteer form

### Form Mode Request: Field Validation

Used for real-time field validation without calling Bedrock.

```json
{
  "tenant_hash": "abc123def456",
  "form_mode": true,
  "action": "validate_field",
  "field_id": "email",
  "field_value": "user@example.com",
  "form_id": "volunteer_apply",
  "session_context": {
    "completed_forms": [],
    "suspended_forms": ["volunteer_apply"],
    "program_interest": "lovebox"
  }
}
```

**Required Fields**:
- `tenant_hash` (string)
- `form_mode` (boolean): Must be `true`
- `action` (string): Must be `"validate_field"`
- `field_id` (string): ID of field to validate
- `field_value` (string): Value to validate

**Optional Fields**:
- `form_id` (string): Form identifier
- `session_context` (object): Session state

### Form Mode Request: Form Submission

Used for final form submission with multi-channel fulfillment.

```json
{
  "tenant_hash": "abc123def456",
  "form_mode": true,
  "action": "submit_form",
  "form_id": "volunteer_apply",
  "form_data": {
    "first_name": "Jane",
    "last_name": "Smith",
    "email": "jane.smith@example.com",
    "phone": "+1-555-987-6543",
    "program_interest": "lovebox",
    "urgency": "normal"
  },
  "session_context": {
    "completed_forms": [],
    "suspended_forms": ["volunteer_apply"],
    "program_interest": "lovebox"
  }
}
```

**Required Fields**:
- `tenant_hash` (string)
- `form_mode` (boolean): Must be `true`
- `action` (string): Must be `"submit_form"`
- `form_id` (string): Form identifier
- `form_data` (object): Collected form fields

**Optional Fields**:
- `session_context` (object): Session state

## Response Formats

### Normal Mode Response (SSE Stream)

Server-Sent Events stream with multiple event types:

#### 1. Stream Start

```
:ok

data: {"type":"start"}

data: {"type":"stream_start"}
```

#### 2. Heartbeat Events

Sent every 2 seconds to keep connection alive:

```
data: {"type":"heartbeat"}
```

#### 3. Text Chunks

Streaming AI response:

```
data: {"type":"text","content":"We ","session_id":"session_7f3b9e12"}

data: {"type":"text","content":"offer ","session_id":"session_7f3b9e12"}

data: {"type":"text","content":"volunteer ","session_id":"session_7f3b9e12"}

data: {"type":"text","content":"opportunities...","session_id":"session_7f3b9e12"}
```

#### 4. CTA Buttons (if applicable)

Sent after streaming completes:

```json
data: {
  "type": "cta_buttons",
  "ctaButtons": [
    {
      "id": "volunteer_cta",
      "label": "Start Volunteer Application",
      "action": "start_form",
      "type": "form_cta",
      "formId": "volunteer_apply",
      "fields": [
        {
          "id": "first_name",
          "type": "text",
          "label": "First Name",
          "required": true
        },
        {
          "id": "email",
          "type": "email",
          "label": "Email Address",
          "required": true
        }
      ]
    }
  ],
  "metadata": {
    "enhanced": true,
    "branch_detected": "volunteer_interest",
    "filtered_forms": ["lovebox"]
  },
  "session_id": "session_7f3b9e12"
}
```

#### 5. Program Switch Detection (Phase 1B)

When user asks about different program while form suspended:

```json
data: {
  "type": "cta_buttons",
  "ctaButtons": [],
  "metadata": {
    "enhanced": true,
    "program_switch_detected": true,
    "suspended_form": {
      "form_id": "volunteer_apply",
      "program_name": "Love Box"
    },
    "new_form_of_interest": {
      "form_id": "dd_apply",
      "program_name": "Dare to Dream",
      "cta_text": "Apply to Dare to Dream",
      "fields": [...]
    }
  },
  "session_id": "session_7f3b9e12"
}
```

#### 6. Completion Markers

```
: x-total-tokens=125
: x-total-time-ms=2341

data: [DONE]
```

#### 7. Error Response

```json
data: {
  "type": "error",
  "error": "Missing tenant_hash"
}

data: [DONE]
```

### Form Mode Response: Validation Success

```json
{
  "type": "validation_success",
  "field": "email",
  "status": "success",
  "message": "Valid"
}
```

### Form Mode Response: Validation Error

```json
{
  "type": "validation_error",
  "field": "email",
  "errors": [
    "Please enter a valid email address"
  ],
  "status": "error"
}
```

### Form Mode Response: Submission Success

```json
{
  "type": "form_complete",
  "status": "success",
  "message": "Thank you! Your application has been submitted successfully. You will receive a confirmation email shortly.",
  "submissionId": "volunteer_apply_1696184523000",
  "priority": "normal",
  "fulfillment": [
    {
      "channel": "email",
      "status": "sent"
    },
    {
      "channel": "sms",
      "status": "sent",
      "usage": 25,
      "limit": 100
    },
    {
      "channel": "webhook",
      "status": "sent"
    }
  ]
}
```

### Form Mode Response: Submission Error

```json
{
  "type": "form_error",
  "status": "error",
  "message": "There was an error submitting your form. Please try again or contact support.",
  "error": "Missing required parameters: formId, formData, or config"
}
```

### Form Mode Response: Partial Fulfillment Failure

```json
{
  "type": "form_complete",
  "status": "success",
  "submissionId": "volunteer_apply_1696184523000",
  "priority": "high",
  "fulfillment": [
    {
      "channel": "email",
      "status": "sent"
    },
    {
      "channel": "sms",
      "status": "failed",
      "error": "SNS error: Invalid phone number"
    },
    {
      "channel": "lambda",
      "function": "SupportHandler",
      "status": "invoked"
    }
  ]
}
```

### Form Mode Response: SMS Rate Limited

```json
{
  "type": "form_complete",
  "status": "success",
  "submissionId": "volunteer_apply_1696184523000",
  "priority": "normal",
  "fulfillment": [
    {
      "channel": "email",
      "status": "sent"
    },
    {
      "channel": "sms",
      "status": "skipped",
      "reason": "monthly_limit_reached",
      "usage": 100,
      "limit": 100
    }
  ]
}
```

## Complete Request/Response Examples

### Example 1: Simple Conversation

**Request**:
```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "tenant_hash": "abc123",
    "user_input": "What programs do you offer?"
  }'
```

**Response**:
```
:ok

data: {"type":"start"}

data: {"type":"heartbeat"}

data: {"type":"stream_start"}

data: {"type":"text","content":"We ","session_id":"default"}

data: {"type":"text","content":"offer ","session_id":"default"}

data: {"type":"text","content":"several ","session_id":"default"}

data: {"type":"text","content":"programs ","session_id":"default"}

data: {"type":"text","content":"including ","session_id":"default"}

data: {"type":"text","content":"Love ","session_id":"default"}

data: {"type":"text","content":"Box ","session_id":"default"}

data: {"type":"text","content":"and ","session_id":"default"}

data: {"type":"text","content":"Dare ","session_id":"default"}

data: {"type":"text","content":"to ","session_id":"default"}

data: {"type":"text","content":"Dream.","session_id":"default"}

data: {"type":"cta_buttons","ctaButtons":[{"id":"explore_programs","label":"Explore Our Programs","action":"link","url":"https://example.com/programs"},{"id":"lovebox_cta","label":"Learn About Love Box","action":"link","url":"https://example.com/lovebox"}],"metadata":{"enhanced":true,"branch_detected":"program_exploration"},"session_id":"default"}

: x-total-tokens=45
: x-total-time-ms=1523

data: [DONE]
```

### Example 2: Form Field Validation

**Request**:
```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "form_mode": true,
    "action": "validate_field",
    "field_id": "email",
    "field_value": "invalid-email"
  }'
```

**Response**:
```
:ok

data: {"type":"validation_error","field":"email","errors":["Please enter a valid email address"],"status":"error"}

data: [DONE]
```

### Example 3: Form Submission with High Priority

**Request**:
```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "form_mode": true,
    "action": "submit_form",
    "form_id": "request_support",
    "form_data": {
      "first_name": "John",
      "last_name": "Doe",
      "email": "john@example.com",
      "phone": "+1-555-123-4567",
      "urgency": "urgent",
      "message": "Need immediate assistance"
    }
  }'
```

**Response**:
```
:ok

data: {"type":"form_complete","status":"success","message":"Thank you! Your application has been submitted successfully. You will receive a confirmation email shortly.","submissionId":"request_support_1696184900123","priority":"high","fulfillment":[{"channel":"lambda","function":"SupportHandler","status":"invoked"},{"channel":"email","status":"sent"}]}

data: [DONE]
```

### Example 4: Conversation with History

**Request**:
```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "user_input": "What are the requirements?",
    "session_id": "session_456",
    "conversation_history": [
      {
        "role": "user",
        "content": "Tell me about volunteering"
      },
      {
        "role": "assistant",
        "content": "We offer volunteer opportunities in Love Box and Dare to Dream programs."
      }
    ],
    "session_context": {
      "completed_forms": [],
      "suspended_forms": []
    }
  }'
```

**Response**:
```
:ok

data: {"type":"start"}

data: {"type":"stream_start"}

data: {"type":"text","content":"For ","session_id":"session_456"}

data: {"type":"text","content":"our ","session_id":"session_456"}

data: {"type":"text","content":"volunteer ","session_id":"session_456"}

data: {"type":"text","content":"programs, ","session_id":"session_456"}

data: {"type":"text","content":"you ","session_id":"session_456"}

data: {"type":"text","content":"must ","session_id":"session_456"}

data: {"type":"text","content":"be ","session_id":"session_456"}

data: {"type":"text","content":"at ","session_id":"session_456"}

data: {"type":"text","content":"least ","session_id":"session_456"}

data: {"type":"text","content":"22 ","session_id":"session_456"}

data: {"type":"text","content":"years ","session_id":"session_456"}

data: {"type":"text","content":"old...","session_id":"session_456"}

data: {"type":"cta_buttons","ctaButtons":[{"id":"volunteer_cta","label":"Start Volunteer Application","action":"start_form","type":"form_cta","formId":"volunteer_apply","fields":[...]}],"metadata":{"enhanced":true,"branch_detected":"requirements_discussion"},"session_id":"session_456"}

: x-total-tokens=78
: x-total-time-ms=2145

data: [DONE]
```

### Example 5: Program Switch Detection (Phase 1B)

**Request**:
```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "user_input": "Actually, tell me about Dare to Dream",
    "session_id": "session_789",
    "conversation_history": [
      {
        "role": "user",
        "content": "I want to volunteer for Love Box"
      },
      {
        "role": "assistant",
        "content": "Great! Let me collect some information..."
      }
    ],
    "session_context": {
      "completed_forms": [],
      "suspended_forms": ["volunteer_apply"],
      "program_interest": "lovebox"
    }
  }'
```

**Response**:
```
:ok

data: {"type":"start"}

data: {"type":"stream_start"}

data: {"type":"text","content":"Dare ","session_id":"session_789"}

data: {"type":"text","content":"to ","session_id":"session_789"}

data: {"type":"text","content":"Dream ","session_id":"session_789"}

data: {"type":"text","content":"is ","session_id":"session_789"}

data: {"type":"text","content":"our ","session_id":"session_789"}

data: {"type":"text","content":"youth ","session_id":"session_789"}

data: {"type":"text","content":"mentorship ","session_id":"session_789"}

data: {"type":"text","content":"program...","session_id":"session_789"}

data: {"type":"cta_buttons","ctaButtons":[],"metadata":{"enhanced":true,"program_switch_detected":true,"suspended_form":{"form_id":"volunteer_apply","program_name":"Love Box"},"new_form_of_interest":{"form_id":"dd_apply","program_name":"Dare to Dream","cta_text":"Apply to Dare to Dream","fields":[{"id":"first_name","type":"text","required":true},{"id":"email","type":"email","required":true}]}},"session_id":"session_789"}

: x-total-tokens=56
: x-total-time-ms=1876

data: [DONE]
```

### Example 6: Completed Forms Filtering

**Request**:
```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "user_input": "Tell me about Love Box",
    "session_id": "session_101",
    "session_context": {
      "completed_forms": ["lovebox"],
      "suspended_forms": []
    }
  }'
```

**Response**:
```
:ok

data: {"type":"start"}

data: {"type":"stream_start"}

data: {"type":"text","content":"Love ","session_id":"session_101"}

data: {"type":"text","content":"Box ","session_id":"session_101"}

data: {"type":"text","content":"provides ","session_id":"session_101"}

data: {"type":"text","content":"food ","session_id":"session_101"}

data: {"type":"text","content":"assistance...","session_id":"session_101"}

# Note: No CTA for Love Box application since user already completed it

data: {"type":"cta_buttons","ctaButtons":[{"id":"lovebox_cta","label":"Learn More About Love Box","action":"link","url":"https://example.com/lovebox"}],"metadata":{"enhanced":true,"branch_detected":"lovebox_discussion","filtered_forms":["lovebox"]},"session_id":"session_101"}

: x-total-tokens=42
: x-total-time-ms=1456

data: [DONE]
```

## Field Validation Rules

### Email Validation

**Field ID**: `email`

**Regex**: `/^[^\s@]+@[^\s@]+\.[^\s@]+$/`

**Valid Examples**:
- `user@example.com`
- `john.doe@company.co.uk`
- `contact+tag@domain.org`

**Invalid Examples**:
- `invalid-email` (missing @)
- `user@` (missing domain)
- `@example.com` (missing local part)
- `user @example.com` (spaces not allowed)

**Error Message**: `"Please enter a valid email address"`

### Phone Validation

**Field ID**: `phone`

**Regex**: `/^[\d\s\-\(\)\+]+$/`

**Valid Examples**:
- `+1-555-123-4567`
- `(555) 123-4567`
- `5551234567`
- `+44 20 7123 4567`

**Invalid Examples**:
- `abc123` (letters not allowed)
- `555-123-ABCD` (letters not allowed)

**Error Message**: `"Please enter a valid phone number"`

### Age Confirmation

**Field ID**: `age_confirm`

**Valid Values**: `"yes"`

**Invalid Values**: `"no"`

**Error Message**: `"You must be at least 22 years old to volunteer"`

### Commitment Confirmation

**Field ID**: `commitment_confirm`

**Valid Values**: `"yes"`

**Invalid Values**: `"no"`

**Error Message**: `"A one year commitment is required for this program"`

### Required Fields

**Validation**: Value must not be empty or whitespace-only

**Error Message**: `"This field is required"`

## Priority Determination

Forms are assigned priority levels for notification routing:

### Priority Levels

1. **high**: Urgent requests requiring immediate attention
2. **normal**: Standard submissions
3. **low**: Non-urgent submissions (e.g., newsletter signups)

### Priority Rules (in order of precedence)

#### 1. Explicit Urgency Field

Highest priority - overrides all other rules.

**Field**: `urgency` in form_data

**Mappings**:
- `"immediate"`, `"urgent"`, `"high"` → `high`
- `"normal"`, `"this week"` → `normal`
- Any other value → `low`

**Example**:
```json
{
  "form_data": {
    "urgency": "urgent"
  }
}
// Priority: "high"
```

#### 2. Config-Based Priority Rules

Check tenant-specific rules in form configuration.

**Config Example**:
```json
{
  "conversational_forms": {
    "volunteer_apply": {
      "priority_rules": [
        {
          "field": "program",
          "value": "emergency",
          "priority": "high"
        }
      ]
    }
  }
}
```

**Example**:
```json
{
  "form_data": {
    "program": "emergency"
  }
}
// Priority: "high" (matches rule)
```

#### 3. Form-Type Defaults

Fallback when no explicit urgency or matching rules.

**Default Mappings**:
- `request_support` → `high`
- `volunteer_apply` → `normal`
- `lb_apply` → `normal`
- `dd_apply` → `normal`
- `donation` → `normal`
- `contact` → `normal`
- `newsletter` → `low`
- Unknown forms → `normal`

**Example**:
```json
{
  "form_id": "request_support"
}
// Priority: "high" (form-type default)
```

## Fulfillment Channels

Form submissions can route to multiple channels based on configuration:

### 1. Lambda Invocation

**Config**:
```json
{
  "fulfillment": {
    "type": "lambda",
    "function": "SupportHandler",
    "action": "process_support"
  }
}
```

**Payload Sent**:
```json
{
  "action": "process_support",
  "form_type": "request_support",
  "submission_id": "request_support_1696184900123",
  "responses": {
    "first_name": "John",
    "email": "john@example.com"
  },
  "tenant_id": "abc123",
  "priority": "high"
}
```

**Invocation**: Async (Event type)

**Result**:
```json
{
  "channel": "lambda",
  "function": "SupportHandler",
  "status": "invoked"
}
```

### 2. S3 Storage

**Config**:
```json
{
  "fulfillment": {
    "type": "s3",
    "bucket": "tenant-form-submissions"
  }
}
```

**S3 Key Format**: `submissions/{tenant_id}/{form_id}/{submission_id}.json`

**Example**: `submissions/abc123/volunteer_apply/volunteer_1696184900123.json`

**Result**:
```json
{
  "channel": "s3",
  "location": "s3://tenant-form-submissions/submissions/abc123/volunteer_apply/volunteer_1696184900123.json",
  "status": "stored"
}
```

### 3. Email Notification

**Config**:
```json
{
  "fulfillment": {
    "email_to": "team@example.com"
  }
}
```

**Email Format**:
- **Subject**: `New Form Submission: {form_id}`
- **Body**: HTML table with form fields
- **Priority Indicator**: Included in body

**High Priority Example**:
```html
<h2>New request_support Submission</h2>
<table>
  <tr><td><strong>first_name:</strong></td><td>John</td></tr>
  <tr><td><strong>email:</strong></td><td>john@example.com</td></tr>
</table>
<p><strong>Priority:</strong> HIGH</p>
```

**Result**:
```json
{
  "channel": "email",
  "status": "sent"
}
```

### 4. SMS Notification (with Rate Limiting)

**Config**:
```json
{
  "fulfillment": {
    "sms_to": "+15555551234"
  }
}
```

**SMS Format**: `{priority_emoji} New {form_id} submission. Name: {name}, Email: {email}`

**Priority Emojis**:
- High: 🚨
- Normal: 📝
- Low: 📋

**Example**: `🚨 New request_support submission. Name: John Doe, Email: john@example.com`

**Rate Limiting**:
- **Default Limit**: 100 SMS per month per tenant
- **Tracking**: DynamoDB table with month-based partitioning
- **Behavior When Limit Hit**: Skip SMS, log warning

**Result (Under Limit)**:
```json
{
  "channel": "sms",
  "status": "sent",
  "usage": 25,
  "limit": 100
}
```

**Result (Limit Reached)**:
```json
{
  "channel": "sms",
  "status": "skipped",
  "reason": "monthly_limit_reached",
  "usage": 100,
  "limit": 100
}
```

### 5. Webhook

**Config**:
```json
{
  "fulfillment": {
    "webhook_url": "https://hooks.zapier.com/hooks/catch/123456/abcdef"
  }
}
```

**Payload Sent**:
```json
{
  "form_id": "volunteer_apply",
  "submission_id": "volunteer_1696184900123",
  "priority": "normal",
  "timestamp": "2025-10-01T18:45:23.000Z",
  "data": {
    "first_name": "Jane",
    "last_name": "Smith",
    "email": "jane@example.com"
  }
}
```

**Method**: POST

**Content-Type**: application/json

**Result**:
```json
{
  "channel": "webhook",
  "status": "sent"
}
```

### 6. Confirmation Email (to User)

**Automatic**: Sent if `email` field present in form_data

**Disabled By**: Set `send_confirmation_email: false` in config

**Email Format**:
- **Subject**: `Thank you for your {form_id} submission`
- **Body**: Confirmation message with organization name

**Example**:
```html
<h2>Thank you for your submission!</h2>
<p>Dear Applicant,</p>
<p>We have received your volunteer_apply submission to ABC Organization.</p>
<p>Our team will review your information and get back to you soon.</p>
```

**Note**: Non-blocking - errors don't fail submission

## Error Codes and Handling

### HTTP Status Codes

- `200 OK`: Successful request (both normal and error responses use 200 with error in body)
- `400 Bad Request`: Missing required parameters
- `500 Internal Server Error`: Unhandled server errors

### Error Types

#### Missing Required Parameters

**Request**:
```json
{
  "user_input": "Hello"
  // Missing tenant_hash
}
```

**Response**:
```
data: {"type":"error","error":"Missing tenant_hash"}

data: [DONE]
```

#### Invalid Tenant Hash

**Request**:
```json
{
  "tenant_hash": "invalid123",
  "user_input": "Hello"
}
```

**Response**:
```
# Config load fails, uses defaults
data: {"type":"text","content":"I don't have information about this topic in my knowledge base. Would you like me to connect you with someone who can help?","session_id":"default"}

data: [DONE]
```

#### Bedrock Streaming Error

**Scenario**: Bedrock service error during streaming

**Response**:
```
data: {"type":"text","content":"We offer..."}

data: {"type":"error","error":"Bedrock streaming failed: <error message>"}

data: [DONE]
```

#### Form Validation Error

**Request**:
```json
{
  "form_mode": true,
  "action": "validate_field",
  "field_id": "email",
  "field_value": ""
}
```

**Response**:
```
data: {"type":"validation_error","field":"email","errors":["This field is required"],"status":"error"}

data: [DONE]
```

#### Form Submission Error

**Request**:
```json
{
  "form_mode": true,
  "action": "submit_form"
  // Missing form_id and form_data
}
```

**Response**:
```
data: {"type":"form_error","status":"error","message":"There was an error submitting your form. Please try again or contact support.","error":"Missing required parameters: formId, formData, or config"}

data: [DONE]
```

## Rate Limits and Quotas

### Bedrock Quotas

- **Model Invocations**: 200 TPS (transactions per second)
- **KB Queries**: 25 TPS per knowledge base
- **Throttling Behavior**: Automatic retry with exponential backoff

### Lambda Limits

- **Concurrent Executions**: 1000 (default account limit)
- **Function Timeout**: 5 minutes (300 seconds)
- **Payload Size**: 6 MB (request/response)

### SMS Rate Limiting

- **Per Tenant Limit**: 100 SMS per month (configurable)
- **Tracking**: DynamoDB with month-based partitions
- **Reset**: Automatic on month change
- **Behavior**: Skip SMS when limit reached, continue other channels

### Caching

- **Config Cache TTL**: 5 minutes
- **KB Cache TTL**: 5 minutes
- **Cache Scope**: Per Lambda instance (in-memory)

## Best Practices

### For Normal Conversations

1. **Always provide session_id** for tracking and analytics
2. **Include conversation_history** for context-aware responses
3. **Update session_context** with completed/suspended forms
4. **Handle SSE events** incrementally for real-time UI updates
5. **Reconnect on connection drop** (SSE streams can timeout)

### For Form Validation

1. **Validate fields as user types** (debounce 300-500ms)
2. **Show visual feedback** immediately (loading spinner)
3. **Use form_mode=true** to bypass Bedrock (faster, cheaper)
4. **Handle validation_error** by showing inline error messages
5. **Re-validate on blur** to catch paste/autofill values

### For Form Submission

1. **Validate all fields** client-side before submission
2. **Disable submit button** during processing
3. **Set urgency field** when known (for proper prioritization)
4. **Check fulfillment array** to verify delivery channels
5. **Update completed_forms** in session after successful submission
6. **Handle partial failures** gracefully (some channels may fail)

### Error Handling

1. **Parse SSE events** with try-catch (malformed JSON possible)
2. **Timeout SSE connections** after 60 seconds of no data
3. **Retry failed requests** with exponential backoff
4. **Show user-friendly errors** (don't expose technical details)
5. **Log errors** for debugging with request_id/session_id

### Performance Optimization

1. **Reuse connections** when possible (keep-alive)
2. **Cache tenant_hash** on client side
3. **Debounce field validation** to reduce requests
4. **Show skeleton UI** while waiting for first token
5. **Preload next likely form** based on conversation context

## Testing

### Test Cases

#### Normal Conversation
```bash
# Test 1: Basic conversation
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{"tenant_hash":"test123","user_input":"Hello"}'

# Test 2: Conversation with history
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash":"test123",
    "user_input":"What are the requirements?",
    "conversation_history":[
      {"role":"user","content":"Tell me about volunteering"},
      {"role":"assistant","content":"We offer volunteer opportunities..."}
    ]
  }'
```

#### Form Validation
```bash
# Test 3: Valid email
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash":"test123",
    "form_mode":true,
    "action":"validate_field",
    "field_id":"email",
    "field_value":"user@example.com"
  }'

# Test 4: Invalid email
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash":"test123",
    "form_mode":true,
    "action":"validate_field",
    "field_id":"email",
    "field_value":"invalid"
  }'
```

#### Form Submission
```bash
# Test 5: Submit with high priority
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash":"test123",
    "form_mode":true,
    "action":"submit_form",
    "form_id":"volunteer_apply",
    "form_data":{
      "first_name":"Test",
      "last_name":"User",
      "email":"test@example.com",
      "urgency":"urgent"
    }
  }'
```

---

**Document Version**: 1.0
**Last Updated**: 2025-10-01
**Maintained By**: Backend Engineering Team
