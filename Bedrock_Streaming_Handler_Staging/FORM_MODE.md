# Form Mode Integration Guide

## What is Form Mode?

Form Mode is a specialized operational mode of the Bedrock Streaming Handler that **bypasses Amazon Bedrock AI** entirely to handle deterministic form field operations. This design provides instant validation feedback and cost-free form submission processing.

### Key Characteristics

**Performance-Optimized**:
- 30-50ms field validation (vs 500-800ms AI response)
- Zero Bedrock API calls = zero AI costs
- Instant user feedback for better UX

**Purpose-Built**:
- Real-time field validation (email, phone, etc.)
- Form submission with multi-channel fulfillment
- Business logic enforcement (age checks, commitment confirmations)

**Not For**:
- Natural language understanding
- Complex decision making
- Contextual conversation
- Knowledge base queries

## When to Use Form Mode

### ✅ Use Form Mode For:

1. **Real-Time Field Validation**
   ```
   User types: "user@exam"
   → form_mode=true, action='validate_field'
   → Response: validation_error (invalid email)
   → Show inline error immediately
   ```

2. **Form Field Collection**
   ```
   User enters value → Validate → Show ✓ or ✗
   Next field → Repeat
   All valid → Enable submit button
   ```

3. **Final Form Submission**
   ```
   User clicks Submit
   → form_mode=true, action='submit_form'
   → Priority determination
   → Multi-channel fulfillment
   → Confirmation response
   ```

### ❌ Don't Use Form Mode For:

1. **General Conversation**
   ```
   User: "What volunteer opportunities exist?"
   → Use Normal Mode (Bedrock AI required)
   ```

2. **CTA Click Handling**
   ```
   User clicks "Apply Now" button
   → Switch to form collection UI
   → Then use form mode for validation
   ```

3. **Contextual Questions**
   ```
   User: "Do I need to be 21 to volunteer?"
   → Use Normal Mode (KB query + AI response)
   ```

## Form Mode Actions

### Action 1: validate_field

**Purpose**: Real-time validation as user types/changes field value

**When to Trigger**:
- User types in field (debounced 300-500ms)
- User pastes value
- Field loses focus (blur event)
- Autofill detected

**Request Format**:
```json
{
  "tenant_hash": "abc123",
  "form_mode": true,
  "action": "validate_field",
  "field_id": "email",
  "field_value": "user@example.com",
  "form_id": "volunteer_apply"  // Optional but recommended
}
```

**Success Response**:
```json
{
  "type": "validation_success",
  "field": "email",
  "status": "success",
  "message": "Valid"
}
```

**Error Response**:
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

**Response Time**: 30-50ms

### Action 2: submit_form

**Purpose**: Final form submission with fulfillment processing

**When to Trigger**:
- User clicks final Submit button
- All fields validated successfully
- Form completion confirmed

**Request Format**:
```json
{
  "tenant_hash": "abc123",
  "form_mode": true,
  "action": "submit_form",
  "form_id": "volunteer_apply",
  "form_data": {
    "first_name": "Jane",
    "last_name": "Smith",
    "email": "jane@example.com",
    "phone": "+1-555-123-4567",
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

**Success Response**:
```json
{
  "type": "form_complete",
  "status": "success",
  "message": "Thank you! Your application has been submitted successfully. You will receive a confirmation email shortly.",
  "submissionId": "volunteer_apply_1696184900123",
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
    }
  ]
}
```

**Error Response**:
```json
{
  "type": "form_error",
  "status": "error",
  "message": "There was an error submitting your form. Please try again or contact support.",
  "error": "Missing required parameters: formId, formData, or config"
}
```

**Response Time**: 100-200ms (excluding async fulfillment)

## Field Validation Rules

### Email Field

**Field ID**: `email`

**Validation Logic**:
```javascript
// Required check
if (!value || value.trim() === '') {
  return { error: 'This field is required' };
}

// Format check
if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
  return { error: 'Please enter a valid email address' };
}
```

**Valid Examples**:
- `user@example.com`
- `john.doe@company.co.uk`
- `contact+tag@domain.org`

**Invalid Examples**:
- `invalid-email` → "Please enter a valid email address"
- `user@` → "Please enter a valid email address"
- `` (empty) → "This field is required"

### Phone Field

**Field ID**: `phone`

**Validation Logic**:
```javascript
// Required check
if (!value || value.trim() === '') {
  return { error: 'This field is required' };
}

// Format check (digits, spaces, dashes, parens, plus)
if (!/^[\d\s\-\(\)\+]+$/.test(value)) {
  return { error: 'Please enter a valid phone number' };
}
```

**Valid Examples**:
- `+1-555-123-4567`
- `(555) 123-4567`
- `5551234567`

**Invalid Examples**:
- `abc123` → "Please enter a valid phone number"
- `555-ABC-1234` → "Please enter a valid phone number"

### Age Confirmation

**Field ID**: `age_confirm`

**Validation Logic**:
```javascript
if (value === 'no') {
  return { error: 'You must be at least 22 years old to volunteer' };
}
```

**Expected Values**: `"yes"` or `"no"`

**Error Case**: User selects "No" → Show blocking error

### Commitment Confirmation

**Field ID**: `commitment_confirm`

**Validation Logic**:
```javascript
if (value === 'no') {
  return { error: 'A one year commitment is required for this program' };
}
```

**Expected Values**: `"yes"` or `"no"`

**Error Case**: User selects "No" → Show blocking error

### Text Fields (Generic)

**Field IDs**: `first_name`, `last_name`, `message`, etc.

**Validation Logic**:
```javascript
if (!value || value.trim() === '') {
  return { error: 'This field is required' };
}
```

**Note**: No format validation, just required check

## Form Submission Flow

### Step 1: Priority Determination

Form submissions are automatically assigned a priority level for routing:

**Priority Hierarchy**:
1. **Explicit Urgency Field** (highest precedence)
   ```json
   {
     "urgency": "urgent"  // → priority: "high"
   }
   ```

2. **Config-Based Rules**
   ```json
   {
     "program": "emergency"  // Matches config rule → priority: "high"
   }
   ```

3. **Form-Type Defaults** (fallback)
   - `request_support` → `high`
   - `volunteer_apply` → `normal`
   - `newsletter` → `low`

**Example**:
```javascript
// User sets urgency
form_data.urgency = "urgent";

// Priority determined: "high"
// Overrides all other rules
```

### Step 2: DynamoDB Storage

**Table**: `picasso-form-submissions`

**Record Structure**:
```json
{
  "submission_id": "volunteer_apply_1696184900123",
  "form_id": "volunteer_apply",
  "tenant_id": "abc123",
  "form_data": {
    "first_name": "Jane",
    "email": "jane@example.com",
    ...
  },
  "priority": "normal",
  "submitted_at": "2025-10-01T18:45:23.000Z",
  "status": "pending_fulfillment"
}
```

**Note**: Non-blocking - if DynamoDB save fails, fulfillment continues

### Step 3: Fulfillment Routing

Based on tenant configuration, routes to one or more channels:

**Available Channels**:
1. **Lambda** - Async invocation for complex workflows
2. **S3** - Archival storage
3. **Email** - Notification to organization
4. **SMS** - Text notification (with rate limiting)
5. **Webhook** - HTTP POST to external service
6. **Confirmation Email** - Auto-sent to user

**Execution**: All channels processed in parallel

### Step 4: Fulfillment Execution

#### Lambda Fulfillment

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

**Invocation Payload**:
```json
{
  "action": "process_support",
  "form_type": "request_support",
  "submission_id": "request_support_1696184900123",
  "responses": { ... },
  "tenant_id": "abc123",
  "priority": "high"
}
```

**Invocation Type**: Event (async, fire-and-forget)

#### S3 Fulfillment

**Config**:
```json
{
  "fulfillment": {
    "type": "s3",
    "bucket": "tenant-form-submissions"
  }
}
```

**Storage Path**: `submissions/{tenant_id}/{form_id}/{submission_id}.json`

**Example**: `s3://tenant-form-submissions/submissions/abc123/volunteer_apply/volunteer_1696184900123.json`

#### Email Fulfillment

**Config**:
```json
{
  "fulfillment": {
    "email_to": "team@example.com"
  }
}
```

**Email Content**:
```html
<h2>New volunteer_apply Submission</h2>
<p>A new form has been submitted through the chat widget.</p>
<h3>Form Data:</h3>
<table border="1" cellpadding="5">
  <tr><td><strong>first_name:</strong></td><td>Jane</td></tr>
  <tr><td><strong>email:</strong></td><td>jane@example.com</td></tr>
</table>
<p><strong>Priority:</strong> NORMAL</p>
<p>Submitted at: 2025-10-01T18:45:23.000Z</p>
```

#### SMS Fulfillment (with Rate Limiting)

**Config**:
```json
{
  "fulfillment": {
    "sms_to": "+15555551234"
  }
}
```

**Rate Limiting**:
- **Limit**: 100 SMS per month per tenant
- **Tracking**: DynamoDB table (`picasso-sms-usage`)
- **Behavior**: Skip SMS when limit reached, log warning

**SMS Format**:
```
{priority_emoji} New {form_id} submission. Name: {name}, Email: {email}
```

**Priority Emojis**:
- High: 🚨
- Normal: 📝
- Low: 📋

**Example**: `🚨 New request_support submission. Name: Jane Smith, Email: jane@example.com`

**When Limit Hit**:
```json
{
  "channel": "sms",
  "status": "skipped",
  "reason": "monthly_limit_reached",
  "usage": 100,
  "limit": 100
}
```

#### Webhook Fulfillment

**Config**:
```json
{
  "fulfillment": {
    "webhook_url": "https://hooks.zapier.com/hooks/catch/123/abc"
  }
}
```

**HTTP POST Payload**:
```json
{
  "form_id": "volunteer_apply",
  "submission_id": "volunteer_1696184900123",
  "priority": "normal",
  "timestamp": "2025-10-01T18:45:23.000Z",
  "data": {
    "first_name": "Jane",
    "email": "jane@example.com"
  }
}
```

#### Confirmation Email (to User)

**Automatic**: Sent if `email` field present in form_data

**Email Template**:
```html
<h2>Thank you for your submission!</h2>
<p>Dear Applicant,</p>
<p>We have received your volunteer_apply submission to {organization}.</p>
<p>Our team will review your information and get back to you soon.</p>
<p>If you have any questions, please don't hesitate to contact us.</p>
<br>
<p>Best regards,<br>{organization} Team</p>
```

**Note**: Non-blocking - errors don't fail submission

### Step 5: Response to Client

**Success Response**:
```json
{
  "type": "form_complete",
  "status": "success",
  "message": "Thank you! Your application has been submitted successfully. You will receive a confirmation email shortly.",
  "submissionId": "volunteer_apply_1696184900123",
  "priority": "normal",
  "fulfillment": [
    {"channel": "email", "status": "sent"},
    {"channel": "sms", "status": "sent", "usage": 25, "limit": 100},
    {"channel": "webhook", "status": "sent"}
  ]
}
```

## Error Handling

### Validation Errors

**Non-Blocking**: Show inline error, allow user to correct

**Example Flow**:
```
1. User enters invalid email
2. validate_field returns validation_error
3. Show red border + error message
4. User corrects value
5. validate_field returns validation_success
6. Show green checkmark
```

**UI Best Practice**:
- Don't disable submit button (let server validate)
- Show client-side validation as helpful feedback
- Handle server-side validation as source of truth

### Submission Errors

**Blocking**: Show modal or banner with retry option

**Example Flow**:
```
1. User clicks Submit
2. submit_form returns form_error
3. Show error modal: "Submission failed. Please try again."
4. Log error details for debugging
5. Allow user to retry
```

**Error Types**:
- Missing parameters → "Invalid form data"
- Network error → "Connection failed, please retry"
- Server error → "An error occurred, please try again"

### Partial Fulfillment Failures

**Graceful Degradation**: Some channels fail, others succeed

**Example**:
```json
{
  "type": "form_complete",
  "status": "success",
  "fulfillment": [
    {"channel": "email", "status": "sent"},
    {"channel": "sms", "status": "failed", "error": "Invalid phone number"},
    {"channel": "webhook", "status": "sent"}
  ]
}
```

**Handling**:
- Still show success to user (form was saved)
- Log partial failures for admin review
- Monitor fulfillment metrics

## Frontend Integration Examples

### React Component: Field Validation

```jsx
import { useState, useCallback } from 'react';
import { debounce } from 'lodash';

function FormField({ fieldId, label, type, tenantHash, formId }) {
  const [value, setValue] = useState('');
  const [error, setError] = useState(null);
  const [validating, setValidating] = useState(false);

  const validateField = useCallback(
    debounce(async (fieldValue) => {
      if (!fieldValue) {
        setError(null);
        setValidating(false);
        return;
      }

      try {
        const response = await fetch('/api/bedrock-handler', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            tenant_hash: tenantHash,
            form_mode: true,
            action: 'validate_field',
            field_id: fieldId,
            field_value: fieldValue,
            form_id: formId
          })
        });

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let result = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          const chunk = decoder.decode(value);
          const lines = chunk.split('\n');

          for (const line of lines) {
            if (line.startsWith('data: ') && !line.includes('[DONE]')) {
              const data = JSON.parse(line.slice(6));
              result = data;
            }
          }
        }

        if (result.type === 'validation_error') {
          setError(result.errors[0]);
        } else {
          setError(null);
        }
      } catch (err) {
        console.error('Validation error:', err);
      } finally {
        setValidating(false);
      }
    }, 500),
    [fieldId, tenantHash, formId]
  );

  const handleChange = (e) => {
    const newValue = e.target.value;
    setValue(newValue);
    setValidating(true);
    validateField(newValue);
  };

  return (
    <div className="form-field">
      <label htmlFor={fieldId}>{label}</label>
      <input
        id={fieldId}
        type={type}
        value={value}
        onChange={handleChange}
        className={error ? 'error' : ''}
      />
      {validating && <span className="validating">Checking...</span>}
      {error && <span className="error-message">{error}</span>}
    </div>
  );
}
```

### React Component: Form Submission

```jsx
import { useState } from 'react';

function VolunteerForm({ tenantHash, sessionContext, onComplete }) {
  const [formData, setFormData] = useState({
    first_name: '',
    last_name: '',
    email: '',
    phone: '',
    program_interest: 'lovebox'
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      const response = await fetch('/api/bedrock-handler', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenant_hash: tenantHash,
          form_mode: true,
          action: 'submit_form',
          form_id: 'volunteer_apply',
          form_data: formData,
          session_context: sessionContext
        })
      });

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let result = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value);
        const lines = chunk.split('\n');

        for (const line of lines) {
          if (line.startsWith('data: ') && !line.includes('[DONE]')) {
            const data = JSON.parse(line.slice(6));
            if (data.type === 'form_complete' || data.type === 'form_error') {
              result = data;
            }
          }
        }
      }

      if (result.type === 'form_complete') {
        // Success!
        onComplete({
          completed: true,
          submissionId: result.submissionId,
          formId: 'volunteer_apply'
        });
      } else {
        // Error
        setError(result.message || 'Submission failed');
      }
    } catch (err) {
      console.error('Submit error:', err);
      setError('Network error. Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <input
        type="text"
        value={formData.first_name}
        onChange={(e) => setFormData({ ...formData, first_name: e.target.value })}
        placeholder="First Name"
        required
      />
      <input
        type="text"
        value={formData.last_name}
        onChange={(e) => setFormData({ ...formData, last_name: e.target.value })}
        placeholder="Last Name"
        required
      />
      <input
        type="email"
        value={formData.email}
        onChange={(e) => setFormData({ ...formData, email: e.target.value })}
        placeholder="Email"
        required
      />

      {error && <div className="error-banner">{error}</div>}

      <button type="submit" disabled={submitting}>
        {submitting ? 'Submitting...' : 'Submit Application'}
      </button>
    </form>
  );
}
```

### JavaScript: SSE Parser Utility

```javascript
/**
 * Parse SSE stream from form mode requests
 */
async function parseFormModeResponse(response) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let result = null;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value);
      const lines = chunk.split('\n');

      for (const line of lines) {
        // Skip comments and empty lines
        if (!line || line.startsWith(':')) continue;

        // Parse data events
        if (line.startsWith('data: ')) {
          const dataStr = line.slice(6).trim();

          // Skip completion marker
          if (dataStr === '[DONE]') break;

          try {
            result = JSON.parse(dataStr);
          } catch (e) {
            console.error('Failed to parse SSE data:', dataStr);
          }
        }
      }
    }
  } catch (err) {
    console.error('SSE parsing error:', err);
    throw err;
  }

  return result;
}

// Usage
const response = await fetch('/api/bedrock-handler', {
  method: 'POST',
  body: JSON.stringify({ form_mode: true, action: 'validate_field', ... })
});

const result = await parseFormModeResponse(response);

if (result.type === 'validation_error') {
  // Handle error
} else if (result.type === 'validation_success') {
  // Handle success
}
```

### TypeScript: Type Definitions

```typescript
// Form mode request types
interface ValidateFieldRequest {
  tenant_hash: string;
  form_mode: true;
  action: 'validate_field';
  field_id: string;
  field_value: string;
  form_id?: string;
}

interface SubmitFormRequest {
  tenant_hash: string;
  form_mode: true;
  action: 'submit_form';
  form_id: string;
  form_data: Record<string, any>;
  session_context?: SessionContext;
}

// Form mode response types
interface ValidationSuccess {
  type: 'validation_success';
  field: string;
  status: 'success';
  message: string;
}

interface ValidationError {
  type: 'validation_error';
  field: string;
  errors: string[];
  status: 'error';
}

interface FormComplete {
  type: 'form_complete';
  status: 'success';
  message: string;
  submissionId: string;
  priority: 'high' | 'normal' | 'low';
  fulfillment: FulfillmentResult[];
}

interface FormError {
  type: 'form_error';
  status: 'error';
  message: string;
  error: string;
}

interface FulfillmentResult {
  channel: 'email' | 'sms' | 'lambda' | 's3' | 'webhook';
  status: 'sent' | 'failed' | 'skipped' | 'invoked' | 'stored';
  error?: string;
  reason?: string;
  usage?: number;
  limit?: number;
}

type FormModeResponse =
  | ValidationSuccess
  | ValidationError
  | FormComplete
  | FormError;
```

## Best Practices

### Client-Side Validation

1. **Debounce Input**: Wait 300-500ms before validating
   ```javascript
   const validateField = debounce(async (value) => {
     // API call
   }, 500);
   ```

2. **Show Visual Feedback**: Loading spinner while validating
   ```jsx
   {validating && <Spinner />}
   {!validating && error && <ErrorIcon />}
   {!validating && !error && value && <CheckIcon />}
   ```

3. **Cache Validation Results**: Don't re-validate same value
   ```javascript
   const cache = new Map();
   if (cache.has(value)) {
     return cache.get(value);
   }
   ```

### Form Submission

1. **Disable Button**: Prevent double-submission
   ```jsx
   <button disabled={submitting || hasErrors}>
     Submit
   </button>
   ```

2. **Show Progress**: Keep user informed
   ```jsx
   {submitting && (
     <div>Submitting your application...</div>
   )}
   ```

3. **Handle Partial Failures**: Check fulfillment array
   ```javascript
   const failedChannels = result.fulfillment
     .filter(f => f.status === 'failed')
     .map(f => f.channel);

   if (failedChannels.length > 0) {
     console.warn('Some channels failed:', failedChannels);
     // Still show success to user
   }
   ```

### Error Handling

1. **Network Errors**: Retry with exponential backoff
   ```javascript
   async function submitWithRetry(data, maxRetries = 3) {
     for (let i = 0; i < maxRetries; i++) {
       try {
         return await submitForm(data);
       } catch (err) {
         if (i === maxRetries - 1) throw err;
         await sleep(Math.pow(2, i) * 1000);
       }
     }
   }
   ```

2. **Validation Errors**: Show inline, don't block
   ```jsx
   <input
     className={error ? 'error' : 'valid'}
     aria-invalid={!!error}
     aria-describedby={error ? `${fieldId}-error` : undefined}
   />
   {error && (
     <span id={`${fieldId}-error`} role="alert">
       {error}
     </span>
   )}
   ```

3. **Submission Errors**: Show modal with retry
   ```jsx
   {submitError && (
     <Modal>
       <h2>Submission Failed</h2>
       <p>{submitError}</p>
       <button onClick={retry}>Try Again</button>
       <button onClick={cancel}>Cancel</button>
     </Modal>
   )}
   ```

### Performance

1. **Minimize Requests**: Only validate on meaningful changes
   ```javascript
   // Don't validate every keystroke
   const handleChange = debounce((value) => {
     if (value.length >= 3) {  // Min length threshold
       validateField(value);
     }
   }, 500);
   ```

2. **Batch Validation**: Consider validating multiple fields
   ```javascript
   // If backend supports batch validation
   const validateFields = async (fields) => {
     return await fetch('/api/validate-batch', {
       method: 'POST',
       body: JSON.stringify({ fields })
     });
   };
   ```

3. **Progressive Enhancement**: Validate on blur, not on every change
   ```jsx
   <input
     onBlur={() => validateField(value)}  // On blur (better UX)
     // onChange={() => validateField(value)}  // On change (too aggressive)
   />
   ```

## Testing Form Mode

### Manual Testing Checklist

**Field Validation**:
- [ ] Valid email accepted
- [ ] Invalid email rejected with error
- [ ] Empty required field shows error
- [ ] Valid phone number accepted
- [ ] Invalid phone characters rejected
- [ ] Age confirmation "no" blocks form
- [ ] Commitment confirmation "no" blocks form

**Form Submission**:
- [ ] All valid fields → successful submission
- [ ] Missing required field → validation error
- [ ] High urgency → correct priority in response
- [ ] Email fulfillment → email sent
- [ ] SMS under limit → SMS sent
- [ ] SMS over limit → SMS skipped
- [ ] Webhook configured → POST sent
- [ ] Confirmation email → user receives email

**Error Scenarios**:
- [ ] Network timeout → graceful error
- [ ] Invalid tenant_hash → default config used
- [ ] DynamoDB failure → submission continues
- [ ] Email failure → other channels continue
- [ ] Partial fulfillment → success with warnings

### Automated Testing (Jest)

```javascript
describe('Form Mode Integration', () => {
  it('validates email field successfully', async () => {
    const response = await fetch('/api/bedrock-handler', {
      method: 'POST',
      body: JSON.stringify({
        tenant_hash: 'test123',
        form_mode: true,
        action: 'validate_field',
        field_id: 'email',
        field_value: 'test@example.com'
      })
    });

    const result = await parseSSE(response);

    expect(result.type).toBe('validation_success');
    expect(result.field).toBe('email');
    expect(result.status).toBe('success');
  });

  it('rejects invalid email', async () => {
    const response = await fetch('/api/bedrock-handler', {
      method: 'POST',
      body: JSON.stringify({
        tenant_hash: 'test123',
        form_mode: true,
        action: 'validate_field',
        field_id: 'email',
        field_value: 'invalid'
      })
    });

    const result = await parseSSE(response);

    expect(result.type).toBe('validation_error');
    expect(result.errors).toContain('Please enter a valid email address');
  });

  it('submits form with priority', async () => {
    const response = await fetch('/api/bedrock-handler', {
      method: 'POST',
      body: JSON.stringify({
        tenant_hash: 'test123',
        form_mode: true,
        action: 'submit_form',
        form_id: 'volunteer_apply',
        form_data: {
          first_name: 'Test',
          email: 'test@example.com',
          urgency: 'urgent'
        }
      })
    });

    const result = await parseSSE(response);

    expect(result.type).toBe('form_complete');
    expect(result.priority).toBe('high');
    expect(result.submissionId).toMatch(/^volunteer_apply_\d+$/);
  });
});
```

## Troubleshooting

### Issue: Validation Not Firing

**Symptoms**: No validation response when typing

**Possible Causes**:
1. Debounce delay too long
2. form_mode flag not set to true
3. Network request failing silently

**Solutions**:
1. Reduce debounce to 300ms
2. Check request payload: `form_mode: true`
3. Add error logging in catch block

### Issue: Submission Returns Error

**Symptoms**: `form_error` response on submit

**Possible Causes**:
1. Missing required parameters
2. Invalid tenant_hash
3. Form config not found

**Solutions**:
1. Validate all required fields: `form_id`, `form_data`, `tenant_hash`
2. Check tenant_hash is correct
3. Verify form exists in tenant config

### Issue: SMS Not Sending

**Symptoms**: SMS status is "skipped"

**Possible Causes**:
1. Monthly limit reached (100 SMS/month)
2. Invalid phone number in config
3. SNS permissions missing

**Solutions**:
1. Check `fulfillment` array for reason: "monthly_limit_reached"
2. Verify `sms_to` field in config has valid E.164 format
3. Check Lambda IAM role has SNS:Publish permission

### Issue: Partial Fulfillment Failures

**Symptoms**: Some channels succeed, others fail

**Expected Behavior**: This is normal - failures are non-blocking

**How to Handle**:
1. Check `fulfillment` array for failed channels
2. Log failures for admin review
3. Still show success to user (form was saved)
4. Monitor fulfillment metrics in CloudWatch

### Issue: Slow Validation Response

**Symptoms**: >500ms validation latency

**Possible Causes**:
1. Lambda cold start
2. Network latency
3. Too many concurrent requests

**Solutions**:
1. Increase Lambda reserved concurrency to keep warm
2. Use regional endpoint
3. Implement request queuing/throttling

---

**Document Version**: 1.0
**Last Updated**: 2025-10-01
**Maintained By**: Backend Engineering Team
