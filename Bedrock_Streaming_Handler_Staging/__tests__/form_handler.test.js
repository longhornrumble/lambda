/**
 * Form Handler Comprehensive Test Suite
 *
 * Tests for Phase 1 (AWS SDK v3 Migration) and Phase 2 (Priority & Advanced Fulfillment)
 * Ensures parity with Master_Function_Staging/form_handler.py
 *
 * Target: 95%+ code coverage
 */

const { mockClient } = require('aws-sdk-client-mock');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { DynamoDBDocumentClient, PutCommand, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// Create mocks
const sesMock = mockClient(SESClient);
const snsMock = mockClient(SNSClient);
const dynamoMock = mockClient(DynamoDBDocumentClient);
const lambdaMock = mockClient(LambdaClient);
const s3Mock = mockClient(S3Client);

// Capture the real https.request at load time. Several tests below override
// https.request via direct assignment (not jest.spyOn), which jest.restoreAllMocks()
// does NOT undo. If Jest schedules this file in the same worker as a sibling
// (e.g. clerk_helper.test.js), the leftover fake pollutes the sibling's
// jest.spyOn-based mocks — manifesting as an intermittent "Expected 1, Received 6"
// failure in clerk_helper's cache test. Restoring the real reference in a
// top-level afterEach cleans up after every test in this file.
const _https = require('https');
const _ORIGINAL_HTTPS_REQUEST = _https.request;
afterEach(() => {
  _https.request = _ORIGINAL_HTTPS_REQUEST;
});

// Import module under test
const {
  handleFormMode,
  validateFormField,
  submitForm,
  htmlEscapeVars,
  normalizeToE164,
  isPhoneOptedOut,
  validateSubmission
} = require('../form_handler');

// Test fixtures
const mockTenantConfig = {
  tenant_id: 'TEST123',
  chat_title: 'Test Organization',
  conversational_forms: {
    volunteer_apply: {
      form_id: 'volunteer_apply',
      title: 'Volunteer Application',
      fields: [
        { id: 'first_name', type: 'text', required: true },
        { id: 'last_name', type: 'text', required: true },
        { id: 'email', type: 'email', required: true },
        { id: 'phone', type: 'phone', required: false },
        { id: 'urgency', type: 'select', required: false }
      ],
      priority_rules: [
        { field: 'program', value: 'emergency', priority: 'high' }
      ],
      fulfillment: {
        email_to: 'test@example.com',
        sms_to: '+15555551234',
        webhook_url: 'https://hooks.example.com/test'
      }
    },
    request_support: {
      form_id: 'request_support',
      title: 'Support Request',
      fulfillment: {
        type: 'lambda',
        function: 'SupportHandler',
        action: 'process_support'
      }
    },
    newsletter: {
      form_id: 'newsletter',
      title: 'Newsletter Signup',
      fulfillment: {
        type: 's3',
        bucket: 'test-forms-bucket'
      }
    }
  }
};

const mockFormData = {
  first_name: 'John',
  last_name: 'Doe',
  email: 'john.doe@example.com',
  phone: '+1-555-123-4567'
};

describe('Form Handler - Phase 1: AWS SDK v3 Migration', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
  });

  describe('AWS SDK v3 Client Initialization', () => {
    it('should use SESClient with SendEmailCommand for email sending', async () => {
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-message-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      // Fulfillment (internal notification) + applicant confirmation = 2 emails.
      expect(sesMock.commandCalls(SendEmailCommand)).toHaveLength(2);
    });

    it('should use LambdaClient with InvokeCommand for SMS sending (SMS_Sender)', async () => {
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
      dynamoMock.on(UpdateCommand).resolves({});

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      // SMS is now dispatched via Lambda invocation to SMS_Sender, not SNS PublishCommand.
      const smsInvocations = lambdaMock.commandCalls(InvokeCommand).filter(call =>
        call.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
      );
      expect(smsInvocations.length).toBeGreaterThanOrEqual(1);
      expect(snsMock.commandCalls(PublishCommand)).toHaveLength(0);
    });

    it('should use DynamoDBDocumentClient with PutCommand for form storage', async () => {
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      // Handler now writes the submission record + one notification audit record
      // per email recipient (success or failure) to picasso-notification-sends.
      const putCalls = dynamoMock.commandCalls(PutCommand);
      expect(putCalls.length).toBeGreaterThanOrEqual(1);
      const submissionPut = putCalls.find(c => c.args[0].input.TableName === 'test-form-submissions');
      expect(submissionPut).toBeDefined();
      expect(submissionPut.args[0].input).toMatchObject({
        TableName: 'test-form-submissions',
        Item: expect.objectContaining({
          form_id: 'volunteer_apply',
          form_data: mockFormData
        })
      });
    });

    it('should write pii_subject_id attribute on form-submission row — M1.G6 / F-DSAR18 closure', async () => {
      // BSH form_handler is the ACTIVE writer for staging widget chat-form
      // submissions. M1.G6 (master plan v0.12 / F-DSAR18 closure) requires
      // the writer to emit pii_subject_id so the DSAR walker's
      // `_walk_form_submissions` FilterExpression doesn't false-negative
      // every BSH row. Companion: Python writer Master_Function_Staging/
      // form_handler.py:622-640 (PR longhornrumble/lambda#142).
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      // GET on pii-subject-index returns no existing entry, then PUT succeeds
      // → fresh mint + index entry created.
      dynamoMock.on(GetCommand).resolves({});
      dynamoMock.on(PutCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      const submissionPut = dynamoMock.commandCalls(PutCommand)
        .find(c => c.args[0].input.TableName === 'test-form-submissions');
      expect(submissionPut).toBeDefined();
      const item = submissionPut.args[0].input.Item;
      expect(item).toHaveProperty('pii_subject_id');
      // Format: 'psub_' + 32 lowercase hex chars (matches Python uuid4.hex)
      expect(item.pii_subject_id).toMatch(/^psub_[0-9a-f]{32}$/);
    });

    it('should write ttl attribute on form-submission row (~365d from now) — M4 done-bar #2', async () => {
      // BSH form_handler is the ACTIVE writer for staging widget chat-form
      // submissions (empirically verified 2026-05-23 against picasso-form-
      // submissions-staging). M4 done-bar #2 (master plan v0.3 §M4 / D5 G-A
      // writer half) requires the writer to emit `ttl` so the table-level
      // TTL config fires and rows actually expire. Companion: Python writer
      // PR longhornrumble/lambda#142 covers the dormant Master_Function path.
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

      const writeStart = Math.floor(Date.now() / 1000);
      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);
      const writeEnd = Math.floor(Date.now() / 1000);

      const submissionPut = dynamoMock.commandCalls(PutCommand)
        .find(c => c.args[0].input.TableName === 'test-form-submissions');
      expect(submissionPut).toBeDefined();
      const item = submissionPut.args[0].input.Item;
      expect(item).toHaveProperty('ttl');
      expect(typeof item.ttl).toBe('number');
      const expectedMin = writeStart + (365 * 24 * 3600) - 1;
      const expectedMax = writeEnd + (365 * 24 * 3600) + 1;
      expect(item.ttl).toBeGreaterThanOrEqual(expectedMin);
      expect(item.ttl).toBeLessThanOrEqual(expectedMax);
    });

    it('should log structured tenant_id + submission_id when DDB PutCommand fails — M9.G8 / F-DSAR24 closure', async () => {
      // F-DSAR24 (phase-completion-audit code-reviewer 2026-05-23, 🔴 HIGH):
      // saveFormSubmission's catch is intentional (preserves consumer UX
      // when DDB is unreachable) but was previously silent. The 2026-05-14
      // staging incident (AccessDeniedException due to env-var-table-name
      // drift) proved a real submission was lost without operator visibility.
      // M9.G8 fix: keep the catch, but emit structured fields the CW Logs
      // metric filter `bsh-form-handler-ddb-write-error` (picasso IaC) can
      // dimension by tenant_id and alarm on ≥1 in any 5-min window. The
      // literal prefix "Error saving to DynamoDB:" is preserved so the
      // metric filter pattern keeps matching.
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(GetCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
      // Reject ONLY the form-submission PutCommand (not the audit-log or
      // pii-subject-index puts — those use separate table names).
      dynamoMock.on(PutCommand).callsFake((input) => {
        if (input.TableName === 'test-form-submissions') {
          const err = new Error('User: ... is not authorized to perform: dynamodb:PutItem');
          err.name = 'AccessDeniedException';
          return Promise.reject(err);
        }
        return Promise.resolve({});
      });

      const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
      try {
        // Submission must NOT throw — UX preservation is the original intent.
        const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);
        expect(result).toBeDefined();

        // The catch must have logged exactly the structured shape the metric
        // filter expects.
        const ddbErrorCall = errorSpy.mock.calls.find(
          call => typeof call[0] === 'string' && call[0].startsWith('Error saving to DynamoDB:')
        );
        expect(ddbErrorCall).toBeDefined();
        const logLine = ddbErrorCall[0];
        // Metric-filter prefix is preserved verbatim.
        expect(logLine).toMatch(/^Error saving to DynamoDB:/);
        // Structured fields for operator forensics + future EMF dimension.
        expect(logLine).toMatch(/tenant_id=TEST123/);
        expect(logLine).toMatch(/submission_id=\S+/);
        expect(logLine).toMatch(/error_name=AccessDeniedException/);
        expect(logLine).toMatch(/error_message=User: \.\.\. is not authorized/);
      } finally {
        errorSpy.mockRestore();
      }
    });

    it('should escape consumer-controlled form_data in staff notification HTML — M9.G8 / F-DSAR25 closure', async () => {
      // F-DSAR25 (phase-completion-audit security-reviewer 2026-05-23, unasked):
      // form_handler.js sendInternalNotificationEmail default HTML body
      // interpolated formData key + value directly into the staff-facing
      // <table>, while the surrounding code (otherRecipients, orgName) was
      // escaped. A consumer submitting `<img onerror="...">` could render
      // active HTML in the staff email client. Fix: wrap key + value with
      // escapeHtml() at the interpolation site.
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

      const xssFormData = {
        ...mockFormData,
        evil_key: '<img src=x onerror="alert(1)">',
        '<script>': 'value-with-script-key',
        quote_test: 'has "quotes" and <tags>',
      };

      await submitForm('volunteer_apply', xssFormData, mockTenantConfig);

      const internalCalls = sesMock.commandCalls(SendEmailCommand)
        .filter(c => c.args[0].input.Tags?.some(t => t.Name === 'email_type' && t.Value === 'internal_notification'));
      expect(internalCalls.length).toBeGreaterThanOrEqual(1);
      const htmlBody = internalCalls[0].args[0].input.Message.Body.Html.Data;

      // Raw consumer-controlled payloads must NOT appear unescaped in the HTML.
      expect(htmlBody).not.toContain('<img src=x onerror=');
      expect(htmlBody).not.toContain('<script>');
      // Escaped value MUST appear (the realistic attack surface: a value
      // typed by the consumer into a legitimate form field).
      expect(htmlBody).toContain('&lt;img src=x onerror=&quot;alert(1)&quot;&gt;');
      // Quotes + tags inside values are also escaped end-to-end.
      expect(htmlBody).toContain('has &quot;quotes&quot; and &lt;tags&gt;');
      // Defense-in-depth: even though keys flow through buildFormDataDisplay's
      // title-case transform (so `<script>` becomes `<Script>`), the key is
      // ALSO escaped at the interpolation site (matches case-insensitively).
      expect(htmlBody).toMatch(/&lt;script&gt;/i);
    });

    it('should use LambdaClient with InvokeCommand for Lambda fulfillment', async () => {
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
      dynamoMock.on(PutCommand).resolves({});

      const result = await submitForm('request_support', mockFormData, mockTenantConfig);

      expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(1);
      const invokeCall = lambdaMock.commandCalls(InvokeCommand)[0];
      expect(invokeCall.args[0].input).toMatchObject({
        FunctionName: 'SupportHandler',
        InvocationType: 'Event'
      });
      expect(result.fulfillment).toContainEqual(
        expect.objectContaining({ channel: 'lambda', status: 'invoked' })
      );
    });

    it('should use S3Client with PutObjectCommand for S3 fulfillment', async () => {
      s3Mock.on(PutObjectCommand).resolves({});
      dynamoMock.on(PutCommand).resolves({});

      const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

      expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
      const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
      expect(putCall.args[0].input).toMatchObject({
        Bucket: 'test-forms-bucket',
        ContentType: 'application/json'
      });
      expect(result.fulfillment).toContainEqual(
        expect.objectContaining({ channel: 's3', status: 'stored' })
      );
    });
  });
});

describe('Form Handler - Phase 2: Priority Determination', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});
  });

  it('should determine high priority from explicit urgency field - immediate', async () => {
    const formDataWithUrgency = { ...mockFormData, urgency: 'immediate' };
    const result = await submitForm('volunteer_apply', formDataWithUrgency, mockTenantConfig);

    expect(result.priority).toBe('high');
  });

  it('should determine high priority from explicit urgency field - urgent', async () => {
    const formDataWithUrgency = { ...mockFormData, urgency: 'urgent' };
    const result = await submitForm('volunteer_apply', formDataWithUrgency, mockTenantConfig);

    expect(result.priority).toBe('high');
  });

  it('should determine high priority from explicit urgency field - high', async () => {
    const formDataWithUrgency = { ...mockFormData, urgency: 'high' };
    const result = await submitForm('volunteer_apply', formDataWithUrgency, mockTenantConfig);

    expect(result.priority).toBe('high');
  });

  it('should determine normal priority from explicit urgency field - normal', async () => {
    const formDataWithUrgency = { ...mockFormData, urgency: 'normal' };
    const result = await submitForm('volunteer_apply', formDataWithUrgency, mockTenantConfig);

    expect(result.priority).toBe('normal');
  });

  it('should determine normal priority from explicit urgency field - this week', async () => {
    const formDataWithUrgency = { ...mockFormData, urgency: 'this week' };
    const result = await submitForm('volunteer_apply', formDataWithUrgency, mockTenantConfig);

    expect(result.priority).toBe('normal');
  });

  it('should determine low priority from explicit urgency field - low', async () => {
    const formDataWithUrgency = { ...mockFormData, urgency: 'low' };
    const result = await submitForm('volunteer_apply', formDataWithUrgency, mockTenantConfig);

    expect(result.priority).toBe('low');
  });

  it('should determine priority from config-based rules', async () => {
    const formDataWithProgram = { ...mockFormData, program: 'emergency' };
    const result = await submitForm('volunteer_apply', formDataWithProgram, mockTenantConfig);

    expect(result.priority).toBe('high'); // Matches priority_rules in config
  });

  it('should use form-type default priority - request_support → high', async () => {
    const result = await submitForm('request_support', mockFormData, mockTenantConfig);

    expect(result.priority).toBe('high');
  });

  it('should use form-type default priority - volunteer_apply → normal', async () => {
    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.priority).toBe('normal');
  });

  it('should use form-type default priority - newsletter → low', async () => {
    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    expect(result.priority).toBe('low');
  });

  it('should fallback to normal priority for unknown form types', async () => {
    const unknownConfig = {
      ...mockTenantConfig,
      conversational_forms: {
        unknown_form: {
          form_id: 'unknown_form',
          fulfillment: {}
        }
      }
    };
    const result = await submitForm('unknown_form', mockFormData, unknownConfig);

    expect(result.priority).toBe('normal');
  });

  it('should prioritize explicit urgency over config rules', async () => {
    const formData = { ...mockFormData, urgency: 'low', program: 'emergency' };
    const result = await submitForm('volunteer_apply', formData, mockTenantConfig);

    expect(result.priority).toBe('low'); // Explicit urgency takes precedence
  });
});

describe('Form Handler - Phase 2: SMS Rate Limiting', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
  });

  it('should retrieve monthly SMS usage from DynamoDB', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 50 } });

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // BSH may also issue a GetCommand against the pii-subject-index table
    // (M1.G6 / F-DSAR18 closure); narrow this assertion to the SMS-usage
    // table specifically so unrelated table reads don't bleed in.
    const smsUsageGets = dynamoMock.commandCalls(GetCommand)
      .filter(c => c.args[0].input.TableName === 'test-sms-usage');
    expect(smsUsageGets).toHaveLength(1);
    expect(smsUsageGets[0].args[0].input).toMatchObject({
      TableName: 'test-sms-usage',
      Key: {
        tenant_id: 'TEST123',
        month: expect.stringMatching(/^\d{4}-\d{2}$/) // YYYY-MM format
      }
    });
  });

  it('should return 0 when no SMS usage record exists', async () => {
    dynamoMock.on(GetCommand).resolves({}); // No Item
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // SMS fulfillment now dispatches via Lambda invocation (SMS_Sender),
    // reporting status 'invoked' per recipient. Usage/limit fields are no longer
    // included on individual sms result objects.
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'invoked'
      })
    );
  });

  it('should send SMS when under monthly limit', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 50 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // SMS now routed through SMS_Sender Lambda, not SNS.
    const smsInvocations = lambdaMock.commandCalls(InvokeCommand).filter(call =>
      call.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvocations.length).toBeGreaterThanOrEqual(1);
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'invoked'
      })
    );
  });

  it('FS4: skips the staff SMS but still SUCCEEDS when the monthly limit is reached (at cap)', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 100 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // No SMS sent...
    const smsInvokes = lambdaMock.commandCalls(InvokeCommand).filter(
      (c) => c.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvokes).toHaveLength(0);
    // ...but NOT a failure: the form was saved + emailed, so the visitor sees success.
    expect(result.statusCode).not.toBe(429);
    expect(result.status).not.toBe('error');
    // SMS is recorded as skipped (rate_limited) — the alarmable hook for admin notify.
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'sms', status: 'skipped', reason: 'rate_limited' })
    );
    // Counter must NOT be incremented past the cap.
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(0);
  });

  it('FS4: skips + succeeds when the monthly limit is exceeded (over cap)', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 150 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    const smsInvokes = lambdaMock.commandCalls(InvokeCommand).filter(
      (c) => c.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvokes).toHaveLength(0);
    expect(result.statusCode).not.toBe(429);
    expect(result.status).not.toBe('error');
    // Counter must NOT be incremented past the cap.
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(0);
  });

  it('should increment SMS usage counter after sending', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 25 } });

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);
    const updateInput = dynamoMock.commandCalls(UpdateCommand)[0].args[0].input;
    expect(updateInput).toMatchObject({
      TableName: 'test-sms-usage',
      Key: {
        tenant_id: 'TEST123',
        month: expect.stringMatching(/^\d{4}-\d{2}$/)
      },
      UpdateExpression: 'SET #count = if_not_exists(#count, :zero) + :inc, updated_at = :now, #ttl = :ttl'
    });
    // 30-day TTL on the monthly rate-limit counter (data-retention-strategy.md §2/§5 #4)
    expect(updateInput.ExpressionAttributeNames['#ttl']).toBe('ttl');
    const ttl = updateInput.ExpressionAttributeValues[':ttl'];
    expect(typeof ttl).toBe('number');
    expect(Math.abs(ttl - (Math.floor(Date.now() / 1000) + 30 * 24 * 3600))).toBeLessThan(120);
  });

  it('should handle DynamoDB GetCommand errors gracefully (fail-safe to 0)', async () => {
    dynamoMock.on(GetCommand).rejects(new Error('DynamoDB error'));
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // Should default to 0 usage and allow SMS (now via Lambda SMS_Sender invocation).
    const smsInvocations = lambdaMock.commandCalls(InvokeCommand).filter(call =>
      call.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvocations.length).toBeGreaterThanOrEqual(1);
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'invoked'
      })
    );
  });

  it('should handle DynamoDB UpdateCommand errors gracefully', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 10 } });
    dynamoMock.on(UpdateCommand).rejects(new Error('DynamoDB update error'));

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // Should still dispatch SMS even if usage increment fails (now via Lambda).
    const smsInvocations = lambdaMock.commandCalls(InvokeCommand).filter(call =>
      call.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvocations.length).toBeGreaterThanOrEqual(1);
    expect(result.status).toBe('success');
  });

  it.skip('should handle missing SMS_USAGE_TABLE gracefully', async () => {
    // SKIPPED: form_handler.js uses default value 'picasso-sms-usage' at module load time
    // This is actually better production behavior - ensures SMS usage tracking always works
    // Test expectation needs updating to match actual behavior, but skipping for now per user request
    delete process.env.SMS_USAGE_TABLE;

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // Should not attempt to check usage
    expect(dynamoMock.commandCalls(GetCommand)).toHaveLength(0);
    // Should still send SMS (fail-safe behavior)
    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);

    // Restore env var
    process.env.SMS_USAGE_TABLE = 'test-sms-usage';
  });

  // EC-P1.4: T3 threat mitigation — SMS cap-exhausted path returns 429 structured error
  it('FS4/EC-P1.4: SMS cap-exhausted path skips SMS + succeeds (no invoke, no increment)', async () => {
    // Simulate cap exactly at limit (usage === SMS_MONTHLY_LIMIT)
    dynamoMock.on(GetCommand).resolves({ Item: { count: 100 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // No false failure — the submission succeeded (saved + emailed).
    expect(result.statusCode).not.toBe(429);
    expect(result.status).not.toBe('error');

    // SMS counter must NOT be incremented past the cap.
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(0);

    // No SMS Lambda invocation must occur.
    const smsInvocations = lambdaMock.commandCalls(InvokeCommand).filter(call =>
      call.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvocations).toHaveLength(0);
  });
});

describe('Form Handler - Phase 2: Advanced Fulfillment Routing', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});
  });

  it('should invoke Lambda function with correct payload for Lambda fulfillment', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const result = await submitForm('request_support', mockFormData, mockTenantConfig);

    expect(lambdaMock.commandCalls(InvokeCommand)).toHaveLength(1);
    const invokeCall = lambdaMock.commandCalls(InvokeCommand)[0];
    const payload = JSON.parse(invokeCall.args[0].input.Payload);

    expect(payload).toMatchObject({
      action: 'process_support',
      form_type: 'request_support',
      responses: mockFormData,
      tenant_id: 'TEST123',
      priority: 'high'
    });
    expect(payload.submission_id).toMatch(/^request_support_\d+_[0-9a-f]{8}$/);
  });

  it('should store form data in S3 with correct key format', async () => {
    s3Mock.on(PutObjectCommand).resolves({});

    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
    const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
    expect(putCall.args[0].input).toMatchObject({
      Bucket: 'test-forms-bucket',
      Key: expect.stringMatching(/^submissions\/TEST123\/newsletter\/newsletter_\d+_[0-9a-f]{8}\.json$/),
      Body: JSON.stringify(mockFormData),
      ContentType: 'application/json'
    });
  });

  // --- Sprint D writer extension: persistFulfillmentPath ---
  // The PII DSAR fulfillment walker (picasso_pii_dsar_staging) reads
  // `fulfillment_path` per-row to delete the per-tenant S3 object on subject
  // deletion. These tests prove the writer half of that contract.

  it('Sprint D: should write fulfillment_path UpdateCommand after S3 fulfillment stores', async () => {
    s3Mock.on(PutObjectCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    // S3 PUT happened
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);

    // Exactly one UpdateCommand fired with the fulfillment_path attribute
    const updateCalls = dynamoMock.commandCalls(UpdateCommand);
    expect(updateCalls).toHaveLength(1);

    const updateInput = updateCalls[0].args[0].input;
    expect(typeof updateInput.TableName).toBe('string');
    expect(updateInput.TableName.length).toBeGreaterThan(0);
    expect(updateInput.Key).toMatchObject({
      tenant_id: 'TEST123',
    });
    expect(updateInput.Key.submission_id).toMatch(/^newsletter_\d+_[0-9a-f]{8}$/);
    expect(updateInput.UpdateExpression).toBe('SET fulfillment_path = :fp');
    expect(updateInput.ExpressionAttributeValues[':fp']).toMatch(
      /^s3:\/\/test-forms-bucket\/submissions\/TEST123\/newsletter\/newsletter_\d+_[0-9a-f]{8}\.json$/
    );

    // Form submit still succeeds
    expect(result.status).toBe('success');
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 's3', status: 'stored' })
    );
  });

  it('Sprint D: should NOT write fulfillment_path UpdateCommand when fulfillment is not S3', async () => {
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    dynamoMock.on(UpdateCommand).resolves({});

    await submitForm('request_support', mockFormData, mockTenantConfig);

    // Lambda fulfillment ran but NO UpdateCommand should fire (only PutCommand
    // from saveFormSubmission).
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(0);
  });

  it('Sprint D: should NOT write fulfillment_path UpdateCommand when S3 PUT fails', async () => {
    s3Mock.on(PutObjectCommand).rejects(new Error('AccessDenied'));
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    // S3 PUT was attempted
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);

    // But fulfillment_path persistence MUST be skipped (status !== 'stored')
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(0);

    // Form submit still returns success per UX-preservation policy
    expect(result.status).toBe('success');
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 's3', status: 'failed' })
    );
  });

  it('Sprint D: should swallow fulfillment_path UpdateCommand errors (form succeeds)', async () => {
    s3Mock.on(PutObjectCommand).resolves({});
    dynamoMock.on(UpdateCommand).rejects(new Error('UpdateItem AccessDenied'));

    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    // UpdateCommand was attempted
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);

    // Form submit still returns success (walker manual-followup is the fallback)
    expect(result.status).toBe('success');
  });

  it('Sprint D: should NOT write fulfillment_path UpdateCommand when tenant_id is missing (no ghost row)', async () => {
    // Audit row #1 (code-reviewer 🔴): a missing tenant_id MUST refuse the
    // write rather than fall back to `tenant_id='unknown'` (which would create
    // a ghost row whose Key doesn't match the original form-submission Key).
    s3Mock.on(PutObjectCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});

    const tenantlessConfig = { ...mockTenantConfig };
    delete tenantlessConfig.tenant_id;

    const result = await submitForm('newsletter', mockFormData, tenantlessConfig);

    // PutCommand from saveFormSubmission ran, but persistFulfillmentPath
    // refused to write the ghost row.
    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(0);
    expect(result.status).toBe('success');
  });

  // NOTE: Priority indicator is no longer embedded in the internal-notification
  // email HTML template. The current sendFormEmail() default-path template has
  // no "Priority: HIGH" marker anywhere, so this assertion is unreachable.
  it.skip('should send email with priority indicator', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });

    const formDataHighPriority = { ...mockFormData, urgency: 'high' };
    await submitForm('volunteer_apply', formDataHighPriority, mockTenantConfig);

    const emailCalls = sesMock.commandCalls(SendEmailCommand);
    const fulfillmentEmail = emailCalls.find(call =>
      call.args[0].input.Destination.ToAddresses.includes('test@example.com')
    );

    expect(fulfillmentEmail.args[0].input.Message.Body.Html.Data).toContain('Priority:</strong> HIGH');
  });

  // NOTE: SMS priority emoji prefix lived in sendFormSMS() which used SNS directly.
  // SMS now flows through SMS_Sender Lambda using a different body template
  // (defaultSmsBody without priority emoji). The three "SMS with priority emoji"
  // assertions no longer apply to the current dispatch path.
  it.skip('should send SMS with priority emoji - high priority', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const formDataHighPriority = { ...mockFormData, urgency: 'high' };
    await submitForm('volunteer_apply', formDataHighPriority, mockTenantConfig);

    const smsCall = snsMock.commandCalls(PublishCommand)[0];
    expect(smsCall.args[0].input.Message).toMatch(/^🚨/); // High priority emoji
  });

  it.skip('should send SMS with priority emoji - normal priority', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    const smsCall = snsMock.commandCalls(PublishCommand)[0];
    expect(smsCall.args[0].input.Message).toMatch(/^📝/); // Normal priority emoji
  });

  it.skip('should send SMS with priority emoji - low priority', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const formDataLowPriority = { ...mockFormData, urgency: 'low' };
    await submitForm('volunteer_apply', formDataLowPriority, mockTenantConfig);

    const smsCall = snsMock.commandCalls(PublishCommand)[0];
    expect(smsCall.args[0].input.Message).toMatch(/^📋/); // Low priority emoji
  });

  it('should send webhook with priority and submission_id', async () => {
    // Mock HTTPS module
    const mockRequest = {
      on: jest.fn(),
      write: jest.fn(),
      end: jest.fn()
    };

    const https = require('https');
    https.request = jest.fn((options, callback) => {
      setTimeout(() => callback({ statusCode: 200 }), 0);
      return mockRequest;
    });

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({});

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(mockRequest.write).toHaveBeenCalled();
    const payload = JSON.parse(mockRequest.write.mock.calls[0][0]);
    expect(payload).toMatchObject({
      form_id: 'volunteer_apply',
      priority: 'normal',
      data: mockFormData
    });
    expect(payload.submission_id).toBeDefined();
    expect(payload.timestamp).toBeDefined();
  });

  it('should handle multiple fulfillment channels in parallel', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const https = require('https');
    https.request = jest.fn((options, callback) => {
      setTimeout(() => callback({ statusCode: 200 }), 0);
      return { on: jest.fn(), write: jest.fn(), end: jest.fn() };
    });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // Email is 'sent' (internal notification), SMS is 'invoked' (routed through
    // SMS_Sender Lambda), webhook is 'sent'. Three channels fulfilled in parallel.
    expect(result.fulfillment).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ channel: 'email', status: 'sent' }),
        expect.objectContaining({ channel: 'sms', status: 'invoked' }),
        expect.objectContaining({ channel: 'webhook', status: 'sent' })
      ])
    );
  });

  it('should handle partial fulfillment failures gracefully', async () => {
    sesMock.on(SendEmailCommand).rejects(new Error('SES error'));
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.status).toBe('success');
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'email', status: 'failed' })
    );
    // SMS now dispatched via Lambda (status 'invoked'), not SNS 'sent'.
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'sms', status: 'invoked' })
    );
  });

  it('should handle missing Lambda function name gracefully', async () => {
    const configMissingFunction = {
      ...mockTenantConfig,
      conversational_forms: {
        test_form: {
          fulfillment: {
            type: 'lambda'
            // No function name
          }
        }
      }
    };

    const result = await submitForm('test_form', mockFormData, configMissingFunction);

    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'lambda',
        status: 'failed',
        error: 'Missing function name'
      })
    );
  });

  it('should handle missing S3 bucket gracefully', async () => {
    const configMissingBucket = {
      ...mockTenantConfig,
      conversational_forms: {
        test_form: {
          fulfillment: {
            type: 's3'
            // No bucket
          }
        }
      }
    };

    const result = await submitForm('test_form', mockFormData, configMissingBucket);

    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 's3',
        status: 'failed',
        error: 'Missing bucket name'
      })
    );
  });
});

describe('Form Handler - Error Handling', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
  });

  it('should validate required parameters - missing formId', async () => {
    const result = await submitForm(null, mockFormData, mockTenantConfig);

    expect(result).toMatchObject({
      type: 'form_error',
      status: 'error',
      error: 'Missing required parameters: formId, formData, or config'
    });
  });

  it('should validate required parameters - missing formData', async () => {
    const result = await submitForm('volunteer_apply', null, mockTenantConfig);

    expect(result).toMatchObject({
      type: 'form_error',
      status: 'error',
      error: 'Missing required parameters: formId, formData, or config'
    });
  });

  it('should validate required parameters - missing config', async () => {
    const result = await submitForm('volunteer_apply', mockFormData, null);

    expect(result).toMatchObject({
      type: 'form_error',
      status: 'error',
      error: 'Missing required parameters: formId, formData, or config'
    });
  });

  it('should not block fulfillment when DynamoDB save fails', async () => {
    dynamoMock.on(PutCommand).rejects(new Error('DynamoDB error'));
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.status).toBe('success');
    expect(sesMock.commandCalls(SendEmailCommand).length).toBeGreaterThan(0);
  });

  it('should handle confirmation email errors gracefully', async () => {
    sesMock.on(SendEmailCommand)
      .resolvesOnce({ MessageId: 'fulfillment-id' })
      .rejectsOnce(new Error('Confirmation email error'));
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.status).toBe('success');
  });

  it('should handle graceful degradation when SMS_USAGE_TABLE not configured', async () => {
    delete process.env.SMS_USAGE_TABLE;

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    dynamoMock.on(PutCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.status).toBe('success');
    // SMS dispatched via Lambda (SMS_Sender), not SNS PublishCommand.
    const smsInvocations = lambdaMock.commandCalls(InvokeCommand).filter(call =>
      call.args[0].input.FunctionName === (process.env.SMS_SENDER_FUNCTION || 'SMS_Sender')
    );
    expect(smsInvocations.length).toBeGreaterThanOrEqual(1);

    process.env.SMS_USAGE_TABLE = 'test-sms-usage';
  });
});

describe('Form Handler - Field Validation', () => {
  it('should validate email field correctly', async () => {
    const validResult = await validateFormField('email', 'test@example.com', mockTenantConfig);
    expect(validResult.status).toBe('success');

    const invalidResult = await validateFormField('email', 'invalid-email', mockTenantConfig);
    expect(invalidResult.status).toBe('error');
    expect(invalidResult.errors).toContain('Please enter a valid email address');
  });

  it('should validate phone field correctly', async () => {
    const validResult = await validateFormField('phone', '+1-555-123-4567', mockTenantConfig);
    expect(validResult.status).toBe('success');

    // Current validator checks digit count BEFORE character-class validity, so
    // 'abc123' (3 digits) fails the minimum-digits rule first.
    const invalidResult = await validateFormField('phone', 'abc123', mockTenantConfig);
    expect(invalidResult.status).toBe('error');
    expect(invalidResult.errors).toContain('Phone number must have at least 7 digits');

    // A value with enough digits but illegal characters still reports the
    // "valid phone number" message via the character-class check.
    const badCharsResult = await validateFormField('phone', '555abc1234', mockTenantConfig);
    expect(badCharsResult.status).toBe('error');
    expect(badCharsResult.errors).toContain('Please enter a valid phone number');
  });

  it('should validate age_confirm field', async () => {
    const invalidResult = await validateFormField('age_confirm', 'no', mockTenantConfig);
    expect(invalidResult.status).toBe('error');
    expect(invalidResult.errors).toContain('You must be at least 22 years old to volunteer');

    const validResult = await validateFormField('age_confirm', 'yes', mockTenantConfig);
    expect(validResult.status).toBe('success');
  });

  it('should validate commitment_confirm field', async () => {
    const invalidResult = await validateFormField('commitment_confirm', 'no', mockTenantConfig);
    expect(invalidResult.status).toBe('error');
    expect(invalidResult.errors).toContain('A one year commitment is required for this program');

    const validResult = await validateFormField('commitment_confirm', 'yes', mockTenantConfig);
    expect(validResult.status).toBe('success');
  });

  it('should reject empty required fields', async () => {
    const result = await validateFormField('first_name', '', mockTenantConfig);
    expect(result.status).toBe('error');
    expect(result.errors).toContain('This field is required');
  });
});

describe('Form Handler - Integration', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
  });

  it('should handle complete high-priority form submission with all channels', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({ Item: { count: 10 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const https = require('https');
    https.request = jest.fn((options, callback) => {
      setTimeout(() => callback({ statusCode: 200 }), 0);
      return { on: jest.fn(), write: jest.fn(), end: jest.fn() };
    });

    const highPriorityFormData = { ...mockFormData, urgency: 'high' };
    const result = await submitForm('volunteer_apply', highPriorityFormData, mockTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      priority: 'high'
    });
    expect(result.submissionId).toMatch(/^volunteer_apply_\d+_[0-9a-f]{8}$/);
    expect(result.fulfillment).toHaveLength(3);
  });

  it('should handle handleFormMode with validate_field action', async () => {
    const body = {
      form_mode: true,
      action: 'validate_field',
      field_id: 'email',
      field_value: 'test@example.com'
    };

    const result = await handleFormMode(body, mockTenantConfig);

    expect(result).toMatchObject({
      type: 'validation_success',
      field: 'email',
      status: 'success'
    });
  });

  it('should handle handleFormMode with submit_form action', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});

    const body = {
      form_mode: true,
      action: 'submit_form',
      form_id: 'volunteer_apply',
      form_data: mockFormData
    };

    const result = await handleFormMode(body, mockTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success'
    });
  });

  it('threads the canonical applicant contact onto form_complete (D3 internal seam)', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});

    const result = await handleFormMode(
      { form_mode: true, action: 'submit_form', form_id: 'volunteer_apply', form_data: mockFormData },
      mockTenantConfig
    );

    // index.js strips applicant_contact before the SSE write; it must carry the
    // extractor's canonical email so the post-form scheduling offer can pre-fill it
    // (a silent field rename here would orphan the glue's call site).
    expect(result.applicant_contact).toEqual(
      expect.objectContaining({ email: 'john.doe@example.com' })
    );
  });

  it('should pass session_id and conversation_id through handleFormMode', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});

    const body = {
      form_mode: true,
      action: 'submit_form',
      form_id: 'volunteer_apply',
      form_data: mockFormData,
      session_id: 'test-session-123',
      conversation_id: 'test-conv-456'
    };

    const result = await handleFormMode(body, mockTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success'
    });
  });
});

// Bubble integration has been removed from the form handler entirely — the
// platform no longer sends form submissions to Bubble. These tests exercise
// behavior that no longer exists in form_handler.js and are intentionally skipped.
describe.skip('Form Handler - Bubble Integration', () => {
  let httpsRequestMock;

  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});

    // Mock HTTPS module for Bubble webhook
    httpsRequestMock = {
      on: jest.fn().mockReturnThis(),
      write: jest.fn(),
      end: jest.fn()
    };

    const https = require('https');
    https.request = jest.fn((options, callback) => {
      // Simulate successful response
      const mockResponse = {
        statusCode: 200,
        on: jest.fn((event, handler) => {
          if (event === 'data') {
            // No data
          }
          if (event === 'end') {
            setTimeout(handler, 0);
          }
        })
      };
      setTimeout(() => callback(mockResponse), 0);
      return httpsRequestMock;
    });
  });

  it('should send form data to Bubble when bubble_integration is configured', async () => {
    const configWithBubble = {
      ...mockTenantConfig,
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit'
      }
    };

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    const result = await submitForm('volunteer_apply', mockFormData, configWithBubble);

    expect(result.status).toBe('success');
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'bubble', status: 'sent' })
    );
  });

  it('should send form data to Bubble with top-level properties', async () => {
    const configWithBubble = {
      ...mockTenantConfig,
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit'
      }
    };

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    await submitForm('volunteer_apply', mockFormData, configWithBubble);

    // Verify the payload was written with top-level properties
    expect(httpsRequestMock.write).toHaveBeenCalled();
    const payload = JSON.parse(httpsRequestMock.write.mock.calls[0][0]);

    // Check metadata fields
    expect(payload.submission_id).toBeDefined();
    expect(payload.tenant_id).toBe('TEST123');
    expect(payload.form_type).toBe('volunteer_apply');
    expect(payload.timestamp).toBeDefined();

    // Check that form fields are TOP-LEVEL (not nested in data or responses_json)
    expect(payload.first_name).toBe('John');
    expect(payload.last_name).toBe('Doe');
    expect(payload.email).toBe('john.doe@example.com');
    expect(payload.phone).toBe('+1-555-123-4567');

    // Ensure there's no nested data object
    expect(payload.data).toBeUndefined();
    expect(payload.responses_json).toBeUndefined();
  });

  it('should include session_id and conversation_id when provided', async () => {
    const configWithBubble = {
      ...mockTenantConfig,
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit'
      }
    };

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    await submitForm('volunteer_apply', mockFormData, configWithBubble, 'session-123', 'conv-456');

    const payload = JSON.parse(httpsRequestMock.write.mock.calls[0][0]);
    expect(payload.session_id).toBe('session-123');
    expect(payload.conversation_id).toBe('conv-456');
  });

  it('should include Authorization header when api_key is configured', async () => {
    // Config with ONLY Bubble integration (no other webhooks to avoid confusion)
    const configWithBubbleAuth = {
      tenant_id: 'TEST123',
      chat_title: 'Test Organization',
      conversational_forms: {
        volunteer_apply: {
          form_id: 'volunteer_apply',
          fulfillment: {
            email_to: 'test@example.com'
            // No webhook_url here to avoid multiple webhook calls
          }
        }
      },
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit',
        api_key: 'test-api-key-12345'
      }
    };

    const https = require('https');
    let bubbleCallOptions;
    https.request = jest.fn((options, callback) => {
      // Capture the Bubble call (bubbleapps.io hostname)
      if (options.hostname.includes('bubbleapps.io')) {
        bubbleCallOptions = options;
      }
      const mockResponse = {
        statusCode: 200,
        on: jest.fn((event, handler) => {
          if (event === 'end') setTimeout(handler, 0);
        })
      };
      setTimeout(() => callback(mockResponse), 0);
      return httpsRequestMock;
    });

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({});

    await submitForm('volunteer_apply', mockFormData, configWithBubbleAuth);

    expect(bubbleCallOptions).toBeDefined();
    expect(bubbleCallOptions.headers['Authorization']).toBe('Bearer test-api-key-12345');
  });

  it('should skip Bubble webhook when no webhook_url is configured', async () => {
    // Config without bubble_integration
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.fulfillment).not.toContainEqual(
      expect.objectContaining({ channel: 'bubble' })
    );
  });

  it('should handle Bubble webhook errors gracefully without failing form submission', async () => {
    const configWithBubble = {
      ...mockTenantConfig,
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit'
      }
    };

    // Mock error response
    const https = require('https');
    https.request = jest.fn((options, callback) => {
      const mockResponse = {
        statusCode: 500,
        on: jest.fn((event, handler) => {
          if (event === 'data') handler('Internal Server Error');
          if (event === 'end') setTimeout(handler, 0);
        })
      };
      setTimeout(() => callback(mockResponse), 0);
      return httpsRequestMock;
    });

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    const result = await submitForm('volunteer_apply', mockFormData, configWithBubble);

    // Form submission should still succeed
    expect(result.status).toBe('success');
    // Bubble fulfillment should show failed
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'bubble', status: 'failed' })
    );
  });

  it('should handle Bubble webhook network errors gracefully', async () => {
    const configWithBubble = {
      ...mockTenantConfig,
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit'
      }
    };

    // Mock network error
    const https = require('https');
    https.request = jest.fn((options, callback) => {
      const req = {
        on: jest.fn((event, handler) => {
          if (event === 'error') {
            setTimeout(() => handler(new Error('ECONNREFUSED')), 0);
          }
          return req;
        }),
        write: jest.fn(),
        end: jest.fn(),
        destroy: jest.fn()
      };
      return req;
    });

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    const result = await submitForm('volunteer_apply', mockFormData, configWithBubble);

    // Form submission should still succeed
    expect(result.status).toBe('success');
  });

  it('should send custom form fields as top-level properties', async () => {
    const configWithBubble = {
      ...mockTenantConfig,
      bubble_integration: {
        webhook_url: 'https://myapp.bubbleapps.io/api/1.1/wf/form_submit'
      }
    };

    // Form data with custom fields specific to this form type
    const customFormData = {
      ...mockFormData,
      availability: 'weekends',
      has_vehicle: 'yes',
      foster_experience: '3 years',
      preferred_age_group: 'toddlers',
      languages_spoken: 'English, Spanish'
    };

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    await submitForm('volunteer_apply', customFormData, configWithBubble);

    const payload = JSON.parse(httpsRequestMock.write.mock.calls[0][0]);

    // All custom fields should be top-level
    expect(payload.availability).toBe('weekends');
    expect(payload.has_vehicle).toBe('yes');
    expect(payload.foster_experience).toBe('3 years');
    expect(payload.preferred_age_group).toBe('toddlers');
    expect(payload.languages_spoken).toBe('English, Spanish');
  });

  it('should use environment variable BUBBLE_WEBHOOK_URL when config not set', async () => {
    process.env.BUBBLE_WEBHOOK_URL = 'https://env-bubble.bubbleapps.io/api/1.1/wf/submit';

    // Config WITHOUT bubble_integration in config (relies on env var)
    const configWithoutBubble = {
      tenant_id: 'TEST123',
      chat_title: 'Test Organization',
      conversational_forms: {
        volunteer_apply: {
          form_id: 'volunteer_apply',
          fulfillment: {
            email_to: 'test@example.com'
            // No webhook_url here
          }
        }
      }
      // No bubble_integration here - should use env var
    };

    const https = require('https');
    let bubbleCallOptions;
    https.request = jest.fn((options, callback) => {
      // Capture the Bubble call (env-bubble hostname)
      if (options.hostname.includes('bubbleapps.io')) {
        bubbleCallOptions = options;
      }
      const mockResponse = {
        statusCode: 200,
        on: jest.fn((event, handler) => {
          if (event === 'end') setTimeout(handler, 0);
        })
      };
      setTimeout(() => callback(mockResponse), 0);
      return httpsRequestMock;
    });

    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, configWithoutBubble);

    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'bubble', status: 'sent' })
    );
    expect(bubbleCallOptions).toBeDefined();
    expect(bubbleCallOptions.hostname).toBe('env-bubble.bubbleapps.io');

    delete process.env.BUBBLE_WEBHOOK_URL;
  });
});

// ============================================================================
// EMAIL DETAILS BUILDER TESTS
// ============================================================================

// Import internal functions for testing via require
// Note: These functions need to be exported from form_handler.js for direct testing
// For now we test them indirectly through the Bubble webhook payload

// The Email Details Builder (email_details_text, email_subject_suffix, contact
// fields) only ever existed as payload fields sent to the Bubble webhook.
// With Bubble integration removed from the handler, these assertions have no
// production code to exercise and are intentionally skipped.
describe.skip('Email Details Builder - via Bubble Webhook Payload', () => {
  let httpsRequestMock;
  let capturedPayload;

  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });

    capturedPayload = null;
    httpsRequestMock = {
      on: jest.fn().mockReturnThis(),
      write: jest.fn((data) => { capturedPayload = JSON.parse(data); }),
      end: jest.fn()
    };

    const https = require('https');
    https.request = jest.fn((options, callback) => {
      const mockResponse = {
        statusCode: 200,
        on: jest.fn((event, handler) => {
          if (event === 'end') setTimeout(handler, 0);
        })
      };
      setTimeout(() => callback(mockResponse), 0);
      return httpsRequestMock;
    });
  });

  const configWithBubble = {
    tenant_id: 'TEST123',
    tenant_hash: 'abc123',
    chat_title: 'Test Organization',
    conversational_forms: {
      volunteer_apply: {
        form_id: 'volunteer_apply',
        title: 'Volunteer Application',
        program: 'lovebox',
        fields: [
          { id: 'first_name', label: 'First Name', type: 'text' },
          { id: 'last_name', label: 'Last Name', type: 'text' },
          { id: 'email', label: 'Email', type: 'email' }
        ],
        fulfillment: { email_to: 'test@example.com' }
      }
    },
    bubble_integration: {
      webhook_url: 'https://test.bubbleapps.io/api/1.1/wf/form_submit'
    }
  };

  describe('email_details_text field', () => {
    it('should include email_details_text in payload', async () => {
      await submitForm('volunteer_apply', mockFormData, configWithBubble);

      expect(capturedPayload.email_details_text).toBeDefined();
      expect(typeof capturedPayload.email_details_text).toBe('string');
    });

    it('should format fields as "Label: Value" lines', async () => {
      await submitForm('volunteer_apply', mockFormData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('First Name: John');
      expect(capturedPayload.email_details_text).toContain('Last Name: Doe');
      expect(capturedPayload.email_details_text).toContain('Email: john.doe@example.com');
    });

    it('should humanize snake_case keys to Title Case', async () => {
      const formData = {
        first_name: 'Jane',
        caregivers_phone_number: '+15551234567',
        description_of_needs: 'Need help with groceries'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('First Name: Jane');
      expect(capturedPayload.email_details_text).toContain('Caregivers Phone Number: +15551234567');
      expect(capturedPayload.email_details_text).toContain('Description Of Needs: Need help with groceries');
    });

    it('should preserve common acronyms (ZIP, ID, URL)', async () => {
      const formData = {
        zip_code: '78701',
        user_id: '12345',
        website_url: 'https://example.com'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('ZIP Code: 78701');
      expect(capturedPayload.email_details_text).toContain('User ID: 12345');
      expect(capturedPayload.email_details_text).toContain('Website URL: https://example.com');
    });

    it('should order contact fields first (name, email, phone, address)', async () => {
      const formData = {
        description: 'Test description',
        zip_code: '78701',
        first_name: 'Jane',
        email: 'jane@example.com',
        city: 'Austin',
        last_name: 'Smith',
        phone: '+15551234567'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      const lines = capturedPayload.email_details_text.split('\n');
      const firstNameIndex = lines.findIndex(l => l.startsWith('First Name:'));
      const lastNameIndex = lines.findIndex(l => l.startsWith('Last Name:'));
      const emailIndex = lines.findIndex(l => l.startsWith('Email:'));
      const phoneIndex = lines.findIndex(l => l.startsWith('Phone:'));
      const cityIndex = lines.findIndex(l => l.startsWith('City:'));
      const descIndex = lines.findIndex(l => l.startsWith('Description:'));

      // Name fields should come before email, email before phone, phone before address
      expect(firstNameIndex).toBeLessThan(emailIndex);
      expect(lastNameIndex).toBeLessThan(emailIndex);
      expect(emailIndex).toBeLessThan(phoneIndex);
      expect(phoneIndex).toBeLessThan(cityIndex);
      expect(cityIndex).toBeLessThan(descIndex);
    });

    it('should omit empty/null values', async () => {
      const formData = {
        first_name: 'Jane',
        last_name: '',
        email: null,
        notes: 'Has notes'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('First Name: Jane');
      expect(capturedPayload.email_details_text).toContain('Notes: Has notes');
      expect(capturedPayload.email_details_text).not.toContain('Last Name:');
      expect(capturedPayload.email_details_text).not.toContain('Email:');
    });

    it('should format boolean values as Yes/No', async () => {
      const formData = {
        first_name: 'Jane',
        has_children: true,
        has_vehicle: false
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('Has Children: Yes');
      expect(capturedPayload.email_details_text).toContain('Has Vehicle: No');
    });

    it('should join array values with comma', async () => {
      const formData = {
        first_name: 'Jane',
        languages: ['English', 'Spanish', 'French']
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('Languages: English, Spanish, French');
    });

    it('should stringify nested objects', async () => {
      const formData = {
        first_name: 'Jane',
        address: { street: '123 Main', city: 'Austin' }
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_details_text).toContain('Address: {"street":"123 Main","city":"Austin"}');
    });

    it('should truncate long values at 2000 characters', async () => {
      const longValue = 'x'.repeat(2500);
      const formData = {
        first_name: 'Jane',
        comments: longValue
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      const commentsLine = capturedPayload.email_details_text.split('\n').find(l => l.startsWith('Comments:'));
      expect(commentsLine.length).toBeLessThan(2100); // Label + truncated value
      expect(commentsLine).toContain('...');
    });
  });

  describe('email_subject_suffix field', () => {
    it('should include email_subject_suffix in payload', async () => {
      await submitForm('volunteer_apply', mockFormData, configWithBubble);

      expect(capturedPayload.email_subject_suffix).toBeDefined();
      expect(typeof capturedPayload.email_subject_suffix).toBe('string');
    });

    it('should return full name when first_name and last_name present', async () => {
      const formData = {
        first_name: 'Jane',
        last_name: 'Smith',
        email: 'jane@example.com'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_subject_suffix).toBe('Jane Smith');
    });

    it('should return just first name when last_name missing', async () => {
      const formData = {
        first_name: 'Jane',
        email: 'jane@example.com'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_subject_suffix).toBe('Jane');
    });

    it('should return "New submission" when no name fields present', async () => {
      const formData = {
        email: 'anon@example.com',
        comments: 'Just a comment'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.email_subject_suffix).toBe('New submission');
    });
  });

  describe('contact field', () => {
    it('should include contact object in payload', async () => {
      await submitForm('volunteer_apply', mockFormData, configWithBubble);

      expect(capturedPayload.contact).toBeDefined();
      expect(typeof capturedPayload.contact).toBe('object');
    });

    it('should extract name from first_name and last_name', async () => {
      const formData = {
        first_name: 'Jane',
        last_name: 'Smith',
        email: 'jane@example.com'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.contact.name).toBe('Jane Smith');
    });

    it('should extract email from email field', async () => {
      const formData = {
        first_name: 'Jane',
        email: 'jane@example.com'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.contact.email).toBe('jane@example.com');
    });

    it('should extract phone from phone field', async () => {
      const formData = {
        first_name: 'Jane',
        phone: '+15551234567'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.contact.phone).toBe('+15551234567');
    });

    it('should extract phone from mobile or cell fields', async () => {
      const formData = {
        first_name: 'Jane',
        mobile_number: '+15559876543'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.contact.phone).toBe('+15559876543');
    });

    it('should return empty object when no contact fields present', async () => {
      const formData = {
        comments: 'Just a comment',
        program: 'lovebox'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      expect(capturedPayload.contact).toEqual({});
    });
  });

  describe('graceful error handling', () => {
    it('should handle invalid JSON in form_data gracefully', async () => {
      // This test requires modifying the internal buildEmailDetailsText function
      // Since we can't easily inject invalid JSON through submitForm, we verify
      // the function handles non-object input by testing with unusual form data

      const formData = {
        first_name: 123, // Number instead of string
        last_name: null, // Null
        valid_field: 'test'
      };

      await submitForm('volunteer_apply', formData, configWithBubble);

      // Should still produce output without crashing
      expect(capturedPayload.email_details_text).toBeDefined();
      expect(capturedPayload.email_details_text).toContain('First Name: 123');
      expect(capturedPayload.email_details_text).toContain('Valid Field: test');
    });
  });
});

// ---------------------------------------------------------------------------
// Per-recipient email send (multi-recipient internal notifications)
//
// SES open/click tracking embeds a tracking pixel keyed by MessageId. With one
// SendEmail call to multiple ToAddresses, all recipients share a MessageId and
// per-recipient open/click attribution is impossible. Sending one SES call per
// recipient gives each its own MessageId, restoring full tracking.
// ---------------------------------------------------------------------------

describe('Form Handler - Per-recipient email sends', () => {
  // Form config with three internal-notification recipients
  const multiRecipientConfig = {
    tenant_id: 'MULTI_TEST',
    chat_title: 'Multi Org',
    conversational_forms: {
      multi_form: {
        form_id: 'multi_form',
        title: 'Multi Recipient Form',
        fields: [
          { id: 'first_name', type: 'text', required: true },
          { id: 'last_name', type: 'text', required: true },
          { id: 'email', type: 'email', required: true },
        ],
        notifications: {
          internal: {
            enabled: true,
            recipients: ['alice@example.org', 'bob@example.org', 'carol@example.org'],
          },
        },
      },
    },
  };

  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
  });

  it('sends one SES call per internal-notification recipient with unique MessageIds', async () => {
    let callCount = 0;
    sesMock.on(SendEmailCommand).callsFake(() => {
      callCount += 1;
      return Promise.resolve({ MessageId: `msg-${callCount}` });
    });

    await submitForm('multi_form', mockFormData, multiRecipientConfig);

    // 3 internal notifications + 1 applicant confirmation = 4 SES calls total.
    const internalCalls = sesMock.commandCalls(SendEmailCommand)
      .filter(c => c.args[0].input.Tags?.some(t => t.Name === 'email_type' && t.Value === 'internal_notification'));
    expect(internalCalls).toHaveLength(3);

    // Each internal call must have exactly one ToAddress.
    const internalTos = internalCalls.map(c => c.args[0].input.Destination.ToAddresses);
    expect(internalTos).toEqual([
      ['alice@example.org'],
      ['bob@example.org'],
      ['carol@example.org'],
    ]);
  });

  it('writes one notification-sends audit row per recipient with its MessageId', async () => {
    let callCount = 0;
    sesMock.on(SendEmailCommand).callsFake(() => {
      callCount += 1;
      return Promise.resolve({ MessageId: `unique-msg-${callCount}` });
    });

    await submitForm('multi_form', mockFormData, multiRecipientConfig);

    // The audit table receives one row per internal recipient.
    const sendsRows = dynamoMock.commandCalls(PutCommand)
      .map(c => c.args[0].input)
      .filter(input => input.TableName === (process.env.NOTIFICATION_SENDS_TABLE || 'picasso-notification-sends'));

    expect(sendsRows).toHaveLength(3);
    const recipients = sendsRows.map(r => r.Item.recipient).sort();
    expect(recipients).toEqual(['alice@example.org', 'bob@example.org', 'carol@example.org']);

    // Each row carries a distinct MessageId tying it back to its individual SES send.
    const messageIds = new Set(sendsRows.map(r => r.Item.message_id));
    expect(messageIds.size).toBe(3);
    sendsRows.forEach(row => expect(row.Item.status).toBe('sent'));
  });

  it('includes "Also notified" footer listing the other recipients in each email body', async () => {
    let i = 0;
    sesMock.on(SendEmailCommand).callsFake(() => {
      i += 1;
      return Promise.resolve({ MessageId: `msg-${i}` });
    });

    await submitForm('multi_form', mockFormData, multiRecipientConfig);

    const internalCalls = sesMock.commandCalls(SendEmailCommand)
      .filter(c => c.args[0].input.Tags?.some(t => t.Name === 'email_type' && t.Value === 'internal_notification'));
    expect(internalCalls).toHaveLength(3);

    // Map each call to its (recipient → bodyHtml).
    const bodiesByRecipient = {};
    for (const call of internalCalls) {
      const to = call.args[0].input.Destination.ToAddresses[0];
      bodiesByRecipient[to] = call.args[0].input.Message.Body.Html.Data;
    }

    // Alice's body lists bob & carol but not alice.
    expect(bodiesByRecipient['alice@example.org']).toContain('Also notified:');
    expect(bodiesByRecipient['alice@example.org']).toContain('bob@example.org');
    expect(bodiesByRecipient['alice@example.org']).toContain('carol@example.org');
    expect(bodiesByRecipient['alice@example.org']).not.toMatch(/Also notified:[^<]*alice@example\.org/);

    // Bob's body lists alice & carol but not bob.
    expect(bodiesByRecipient['bob@example.org']).toContain('alice@example.org');
    expect(bodiesByRecipient['bob@example.org']).toContain('carol@example.org');
    expect(bodiesByRecipient['bob@example.org']).not.toMatch(/Also notified:[^<]*bob@example\.org/);
  });

  it('omits "Also notified" footer when there is only one recipient', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'msg-1' });

    const singleRecipientConfig = {
      ...multiRecipientConfig,
      conversational_forms: {
        multi_form: {
          ...multiRecipientConfig.conversational_forms.multi_form,
          notifications: { internal: { enabled: true, recipients: ['solo@example.org'] } },
        },
      },
    };

    await submitForm('multi_form', mockFormData, singleRecipientConfig);

    const internalCalls = sesMock.commandCalls(SendEmailCommand)
      .filter(c => c.args[0].input.Tags?.some(t => t.Name === 'email_type' && t.Value === 'internal_notification'));
    expect(internalCalls).toHaveLength(1);
    expect(internalCalls[0].args[0].input.Message.Body.Html.Data).not.toContain('Also notified:');
  });

  it('records per-recipient outcome rows when one recipient fails but others succeed', async () => {
    let i = 0;
    sesMock.on(SendEmailCommand).callsFake(() => {
      i += 1;
      // Fail the second internal notification call only.
      if (i === 2) return Promise.reject(new Error('SES throttle'));
      return Promise.resolve({ MessageId: `msg-${i}` });
    });

    const result = await submitForm('multi_form', mockFormData, multiRecipientConfig);

    // Top-level status is 'sent' because at least one succeeded; recipient
    // count carries the granular signal.
    const emailResult = result.fulfillment.find(r => r.channel === 'email');
    expect(emailResult).toMatchObject({ status: 'sent', recipients: 2, total: 3 });

    // Per-recipient audit rows reflect the actual per-row outcome.
    const sendsRows = dynamoMock.commandCalls(PutCommand)
      .map(c => c.args[0].input)
      .filter(input => input.TableName === (process.env.NOTIFICATION_SENDS_TABLE || 'picasso-notification-sends'));

    const statuses = sendsRows.map(r => r.Item.status).sort();
    expect(statuses).toEqual(['failed', 'sent', 'sent']);
    const failedRow = sendsRows.find(r => r.Item.status === 'failed');
    expect(failedRow.Item.error).toContain('SES throttle');
  });
});

describe('Form Handler - SMS consent record TTL (WS-E-TCPA, FROZEN_CONTRACTS §E3)', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
  });

  it('writeConsentRecord stamps ttl = now + 4yr+30d on the picasso-sms-consent PutCommand', async () => {
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const FOUR_YEARS_30_DAYS = (4 * 365 + 30) * 24 * 3600;
    const consentingFormData = {
      first_name: 'John',
      last_name: 'Doe',
      email: 'john.doe@example.com',
      phone: '+15551234567',     // clean E.164 so writeConsentRecord's strict gate passes
      sms_consent_given: 'yes',  // legacy consent trigger (findSmsConsent fallback)
    };

    const before = Math.floor(Date.now() / 1000);
    await submitForm('volunteer_apply', consentingFormData, mockTenantConfig);
    // writeConsentRecord is fire-and-forget (.catch, not awaited) — flush microtasks.
    await new Promise((r) => setImmediate(r));
    const after = Math.floor(Date.now() / 1000);

    const consentPut = dynamoMock
      .commandCalls(PutCommand)
      .map((c) => c.args[0].input)
      .find((input) => input.TableName === (process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent'));

    expect(consentPut).toBeDefined();
    expect(consentPut.Item.consent_given).toBe(true);
    expect(consentPut.Item.phone_e164).toBe('+15551234567');
    // The new ttl field: now + 4yr+30d (bounded by the call window, no Date mocking).
    expect(consentPut.Item.ttl).toBeGreaterThanOrEqual(before + FOUR_YEARS_30_DAYS);
    expect(consentPut.Item.ttl).toBeLessThanOrEqual(after + FOUR_YEARS_30_DAYS);
  });
});

describe('Form Handler - submission integrity (FS1 truthful outcome, FS2 unique id)', () => {
  // A form with no fulfillment channels and no internal notifications, so
  // routeFulfillment returns [] (nothing delivered) — used to isolate the
  // DB-persistence branch of the truthful-outcome guard.
  const noFulfillmentConfig = {
    tenant_id: 'TEST123',
    chat_title: 'Test Org',
    conversational_forms: {
      barebones: { form_id: 'barebones', title: 'Barebones Form' },
    },
  };

  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    dynamoMock.on(GetCommand).resolves({});
  });

  it('FS1: returns form_error (not success) when the DB write fails AND no channel delivers', async () => {
    dynamoMock.on(PutCommand).rejects(new Error('ddb unavailable'));

    const result = await submitForm('barebones', mockFormData, noFulfillmentConfig);

    expect(result.type).toBe('form_error');
    expect(result.status).toBe('error');
    // The visitor must NOT receive a confirmation implying we got their submission.
    expect(sesMock.commandCalls(SendEmailCommand)).toHaveLength(0);
  });

  it('FS1: reports success when the DB write succeeds even with no fulfillment channel', async () => {
    dynamoMock.on(PutCommand).resolves({});

    const result = await submitForm('barebones', mockFormData, noFulfillmentConfig);

    expect(result.type).toBe('form_complete');
    expect(result.status).toBe('success');
  });

  it('FS1: reports success when the DB write fails but a fulfillment channel delivers', async () => {
    // newsletter form uses S3 fulfillment; let the S3 store succeed while the DB write fails.
    dynamoMock.on(PutCommand).rejects(new Error('ddb unavailable'));
    s3Mock.on(PutObjectCommand).resolves({});

    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    expect(result.type).toBe('form_complete');
    expect(result.status).toBe('success');
  });

  it('FS2: generates a unique submission_id with a random suffix on each call', async () => {
    dynamoMock.on(PutCommand).resolves({});
    s3Mock.on(PutObjectCommand).resolves({});

    const r1 = await submitForm('newsletter', mockFormData, mockTenantConfig);
    const r2 = await submitForm('newsletter', mockFormData, mockTenantConfig);

    expect(r1.submissionId).toMatch(/^newsletter_\d+_[0-9a-f]{8}$/);
    expect(r1.submissionId).not.toBe(r2.submissionId);
  });
});

// SEC: applicant PII (name/email/phone/address/free-text) must never land in
// CloudWatch logs. submitForm logs only form-field KEYS (not values); every
// email/phone log site is redactPII-masked.
describe('Form Handler - SEC: no applicant PII in logs', () => {
  let logSpy;

  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'msg' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    s3Mock.on(PutObjectCommand).resolves({});
    logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  const allLoggedLines = () =>
    logSpy.mock.calls.map((args) =>
      args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ')
    );

  it('submitForm never logs the raw applicant email or phone (confirmation/SMS sites redacted)', async () => {
    // This fulfillment config sends a fulfillment email AND an applicant
    // confirmation + SMS, so the "sent to" log sites actually run here.
    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);
    const logged = allLoggedLines().join('\n');
    expect(logged).not.toContain('john.doe@example.com');
    expect(logged).not.toContain('555-123-4567');
  });

  it('the submit log keeps field KEYS for debugging but not the PII values', async () => {
    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);
    const submitLine = allLoggedLines().find((l) => l.includes('Submitting form'));
    expect(submitLine).toBeDefined();
    expect(submitLine).toContain('email');      // field key retained
    expect(submitLine).toContain('first_name'); // field key retained
    expect(submitLine).not.toContain('john.doe@example.com'); // value NOT logged
    expect(submitLine).not.toContain('John');   // value NOT logged
  });
});

// SEC (C): a tenant custom body_template renders applicant-controlled values
// into an HTML email. The values must be escaped so an injected field cannot
// become markup in a STAFF-facing email (the real vuln; F-DSAR25 covered only
// the auto-generated form_data table, not the custom-template path).
describe('Form Handler - SEC: HTML email template escaping', () => {
  describe('htmlEscapeVars', () => {
    it('escapes HTML metacharacters in every value', () => {
      const out = htmlEscapeVars({
        a: '<script>alert(1)</script>',
        b: 'Tom & "Jerry"',
        c: 'plain',
      });
      expect(out.a).toBe('&lt;script&gt;alert(1)&lt;/script&gt;');
      expect(out.b).toBe('Tom &amp; &quot;Jerry&quot;');
      expect(out.c).toBe('plain');
    });

    it('coerces null/undefined/number to safe strings', () => {
      const out = htmlEscapeVars({ x: null, y: undefined, z: 42 });
      expect(out.x).toBe('');
      expect(out.y).toBe('');
      expect(out.z).toBe('42');
    });

    it('handles a null/empty bag', () => {
      expect(htmlEscapeVars(null)).toEqual({});
      expect(htmlEscapeVars({})).toEqual({});
    });
  });

  it('staff notification email escapes an injected applicant field value', async () => {
    sesMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'm' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });

    const cfg = {
      tenant_id: 'TEST123',
      chat_title: 'Test Org',
      conversational_forms: {
        volunteer_apply: {
          form_id: 'volunteer_apply',
          title: 'Volunteer',
          notifications: {
            internal: {
              enabled: true,
              recipients: ['staff@example.com'],
              body_template: '<p>Applicant: {first_name}</p>',
            },
          },
        },
      },
    };
    const injected = { first_name: '<script>alert(1)</script>', email: 'applicant@example.com' };

    await submitForm('volunteer_apply', injected, cfg);

    const staffCall = sesMock
      .commandCalls(SendEmailCommand)
      .find((c) => (c.args[0].input.Destination.ToAddresses || []).includes('staff@example.com'));
    expect(staffCall).toBeDefined();
    const html = staffCall.args[0].input.Message.Body.Html.Data;
    expect(html).toContain('&lt;script&gt;');       // value escaped
    expect(html).not.toContain('<script>alert(1)'); // raw script NOT injected
  });
});

// FS10 (communications-consent-advisor): never text a number that replied STOP,
// and the transactional confirmation must carry STOP opt-out language.
describe('Form Handler - FS10: applicant SMS consent controls', () => {
  const smsConfig = (formOverrides = {}) => ({
    tenant_id: 'TEST123',
    chat_title: 'Test Org',
    sms_settings: { enabled: true, from_number: '+15550000000' },
    conversational_forms: {
      volunteer_apply: {
        form_id: 'volunteer_apply',
        title: 'Volunteer',
        notifications: { applicant_confirmation: { sms: { enabled: true, ...formOverrides } } },
      },
    },
  });
  const applicant = { first_name: 'Jane', email: 'jane@example.com', phone: '+15551234567' };

  const applicantSmsInvokes = () =>
    lambdaMock
      .commandCalls(InvokeCommand)
      .map((c) => { try { return JSON.parse(c.args[0].input.Payload); } catch { return {}; } })
      .filter((p) => p.type === 'applicant');

  beforeEach(() => {
    sesMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'm' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
    lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
    s3Mock.on(PutObjectCommand).resolves({});
  });

  describe('normalizeToE164', () => {
    it('normalizes common formats to clean E.164', () => {
      expect(normalizeToE164('+15551234567')).toBe('+15551234567');
      expect(normalizeToE164('+1-555-123-4567')).toBe('+15551234567');
      expect(normalizeToE164('(555) 123-4567')).toBe('+15551234567');
      expect(normalizeToE164('5551234567')).toBe('+15551234567');
      expect(normalizeToE164(null)).toBeNull();
      expect(normalizeToE164('')).toBeNull();
    });
  });

  describe('isPhoneOptedOut', () => {
    it('returns true when the consent record is opted out', async () => {
      dynamoMock.on(GetCommand).resolves({ Item: { opted_out_at: '2026-01-01T00:00:00Z', consent_given: false } });
      expect(await isPhoneOptedOut('TEST123', '+15551234567')).toBe(true);
    });
    it('returns false when there is no record (implied-consent path)', async () => {
      dynamoMock.on(GetCommand).resolves({});
      expect(await isPhoneOptedOut('TEST123', '+15551234567')).toBe(false);
    });
    it('fails open (false) on a lookup error', async () => {
      dynamoMock.on(GetCommand).rejects(new Error('ddb down'));
      expect(await isPhoneOptedOut('TEST123', '+15551234567')).toBe(false);
    });
  });

  it('suppresses the applicant SMS when the recipient previously opted out', async () => {
    // Only the consent-table lookup returns an opted-out record; other Gets → {}.
    dynamoMock.on(GetCommand).callsFake((input) =>
      input.TableName === 'picasso-sms-consent'
        ? { Item: { opted_out_at: '2026-01-01T00:00:00Z', consent_given: false } }
        : {}
    );

    await submitForm('volunteer_apply', applicant, smsConfig());

    expect(applicantSmsInvokes().length).toBe(0);
  });

  it('sends the applicant SMS (with STOP language) when not opted out', async () => {
    dynamoMock.on(GetCommand).resolves({}); // no consent record → not opted out

    await submitForm('volunteer_apply', applicant, smsConfig());

    const invokes = applicantSmsInvokes();
    expect(invokes.length).toBe(1);
    expect(invokes[0].body).toMatch(/STOP/i); // opt-out language present
  });

  it('appends STOP to an operator template that omits it', async () => {
    dynamoMock.on(GetCommand).resolves({});

    await submitForm('volunteer_apply', applicant, smsConfig({ template: 'Hi {first_name}, thanks for applying!' }));

    const invokes = applicantSmsInvokes();
    expect(invokes.length).toBe(1);
    expect(invokes[0].body).toContain('Hi Jane, thanks for applying!');
    expect(invokes[0].body).toMatch(/Reply STOP to opt out/i);
  });
});

// FS9: server-side re-validation at the submit gate. Mirrors the per-field
// rules, config-aware (honors required + type), rejecting + identifying the
// field so a bypassed/tampered payload can't slip malformed/oversized data past.
describe('Form Handler - FS9: submit-time re-validation', () => {
  const fieldsConfig = {
    tenant_id: 'TEST123',
    conversational_forms: {
      volunteer_apply: {
        form_id: 'volunteer_apply',
        title: 'Volunteer',
        fields: [
          { id: 'first_name', type: 'text', label: 'First Name', required: true },
          { id: 'email', type: 'email', label: 'Email', required: true },
          { id: 'phone', type: 'phone', label: 'Phone', required: false },
          { id: 'notes', type: 'textarea', label: 'Notes', required: false },
        ],
      },
    },
  };
  const formCfg = fieldsConfig.conversational_forms.volunteer_apply;

  describe('validateSubmission (unit)', () => {
    it('passes a well-formed submission', () => {
      expect(validateSubmission(
        { first_name: 'Jane', email: 'jane@example.com', phone: '555-123-4567' }, formCfg
      )).toEqual([]);
    });

    it('flags a missing REQUIRED field, identifying it', () => {
      const problems = validateSubmission({ email: 'jane@example.com' }, formCfg); // first_name missing
      expect(problems.map((p) => p.field)).toContain('first_name');
    });

    it('allows a blank OPTIONAL field (no false reject)', () => {
      expect(validateSubmission(
        { first_name: 'Jane', email: 'jane@example.com', phone: '' }, formCfg
      )).toEqual([]);
    });

    it('flags a malformed email, identifying it', () => {
      const problems = validateSubmission({ first_name: 'Jane', email: 'not-an-email' }, formCfg);
      expect(problems.map((p) => p.field)).toContain('email');
    });

    it('flags a malformed phone, identifying it', () => {
      const problems = validateSubmission(
        { first_name: 'Jane', email: 'jane@example.com', phone: '12' }, formCfg
      );
      expect(problems.map((p) => p.field)).toContain('phone');
    });

    it('validates the phone in a phone_with_consent composite (object + flat)', () => {
      const cfg = { fields: [{
        id: 'pwc', type: 'phone_with_consent', label: 'Phone', required: true,
        subfields: [{ id: 'pwc_phone', type: 'phone' }, { id: 'pwc_consent', type: 'select', sms_consent: true }],
      }] };
      // object form, bad phone → flagged
      expect(validateSubmission({ pwc: { pwc_phone: '12', pwc_consent: 'yes' } }, cfg).map((p) => p.field)).toContain('pwc');
      // flat form, good phone → OK
      expect(validateSubmission({ pwc_phone: '555-123-4567' }, cfg)).toEqual([]);
    });

    it('rejects an oversized field value', () => {
      const problems = validateSubmission(
        { first_name: 'Jane', email: 'jane@example.com', notes: 'x'.repeat(10001) }, formCfg
      );
      expect(problems.length).toBeGreaterThan(0);
    });

    it('rejects too many fields', () => {
      const many = {};
      for (let i = 0; i < 201; i++) many[`f${i}`] = 'x';
      expect(validateSubmission(many, formCfg).length).toBeGreaterThan(0);
    });

    it('skips per-field checks when the form declares no fields', () => {
      expect(validateSubmission({ anything: 'goes' }, {})).toEqual([]);
    });

    it('rejects a malformed (non-object) payload', () => {
      expect(validateSubmission(null, formCfg).length).toBeGreaterThan(0);
      expect(validateSubmission([], formCfg).length).toBeGreaterThan(0);
    });
  });

  describe('submitForm integration', () => {
    beforeEach(() => {
      sesMock.reset();
      dynamoMock.reset();
      lambdaMock.reset();
      s3Mock.reset();
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'm' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({});
      dynamoMock.on(UpdateCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({ StatusCode: 202 });
      s3Mock.on(PutObjectCommand).resolves({});
    });

    it('rejects a malformed email at submit — identifies the field, does NOT save or send', async () => {
      const result = await submitForm('volunteer_apply', { first_name: 'Jane', email: 'not-an-email' }, fieldsConfig);

      expect(result.type).toBe('form_error');
      expect(result.statusCode).toBe(400);
      expect(result.validation_errors.map((p) => p.field)).toContain('email');
      // Nothing persisted or sent — rejected before save/fulfillment.
      expect(dynamoMock.commandCalls(PutCommand)).toHaveLength(0);
      expect(sesMock.commandCalls(SendEmailCommand)).toHaveLength(0);
    });

    it('accepts a valid submission (proceeds past validation)', async () => {
      const result = await submitForm('volunteer_apply', { first_name: 'Jane', email: 'jane@example.com' }, fieldsConfig);
      expect(result.statusCode).not.toBe(400);
    });
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// FS5 — client idempotency token (client_submission_id)
// A widget-supplied token makes the submission key deterministic; the save's
// ConditionExpression turns a retry into a detected duplicate answered with
// success and NO re-run of fulfillment (no double emails/SMS). Absent or
// malformed token → today's random key, no dedup (backward compatible).
// ─────────────────────────────────────────────────────────────────────────────
describe('FS5: client idempotency token', () => {
  const TOKEN = 'a'.repeat(64); // sha256-hex-shaped

  beforeEach(() => {
    sesMock.reset(); snsMock.reset(); dynamoMock.reset(); lambdaMock.reset(); s3Mock.reset();
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(GetCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'm1' });
  });

  it('valid token → deterministic submission_id + attribute_not_exists condition on the save', async () => {
    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig, null, null, null, null, TOKEN);
    expect(result.status).toBe('success');
    const put = dynamoMock.commandCalls(PutCommand)
      .find((c) => c.args[0].input.TableName === 'test-form-submissions');
    expect(put.args[0].input.Item.submission_id).toBe(`volunteer_apply_idem_${TOKEN}`);
    expect(put.args[0].input.ConditionExpression).toBe('attribute_not_exists(submission_id)');
  });

  it('duplicate retry (conditional check fails) → success + duplicate:true, fulfillment SKIPPED (no emails)', async () => {
    const dupErr = new Error('The conditional request failed');
    dupErr.name = 'ConditionalCheckFailedException';
    dynamoMock.on(PutCommand).rejects(dupErr);

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig, null, null, null, null, TOKEN);
    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      duplicate: true,
      submissionId: `volunteer_apply_idem_${TOKEN}`,
      fulfillment: { status: 'duplicate_skipped' },
    });
    // The FIRST attempt owns fulfillment — the retry must not send anything.
    expect(sesMock.commandCalls(SendEmailCommand)).toHaveLength(0);
    expect(snsMock.calls()).toHaveLength(0);
  });

  it('malformed token (too short / bad charset) → ignored, random-suffix key, no dedup', async () => {
    for (const bad of ['short', 'has spaces in it!!', 'x'.repeat(200)]) {
      dynamoMock.resetHistory();
      const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig, null, null, null, null, bad);
      expect(result.status).toBe('success');
      const put = dynamoMock.commandCalls(PutCommand)
        .find((c) => c.args[0].input.TableName === 'test-form-submissions');
      expect(put.args[0].input.Item.submission_id).toMatch(/^volunteer_apply_\d+_[0-9a-f]{8}$/);
    }
  });

  it('no token → unchanged legacy behavior (random-suffix key)', async () => {
    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);
    expect(result.status).toBe('success');
    const put = dynamoMock.commandCalls(PutCommand)
      .find((c) => c.args[0].input.TableName === 'test-form-submissions');
    expect(put.args[0].input.Item.submission_id).toMatch(/^volunteer_apply_\d+_[0-9a-f]{8}$/);
  });

  it('tokenless conditional failure does NOT short-circuit (fail-open like today, fulfillment continues)', async () => {
    const dupErr = new Error('The conditional request failed');
    dupErr.name = 'ConditionalCheckFailedException';
    // Reject only the form-submissions put; everything else resolves.
    dynamoMock.on(PutCommand).callsFake((input) => {
      if (input.TableName === 'test-form-submissions') throw dupErr;
      return {};
    });
    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);
    // no token → no duplicate short-circuit; flow continues to fulfillment
    expect(result.duplicate).toBeUndefined();
  });

  it('handleFormMode threads body.client_submission_id through to the key', async () => {
    const body = {
      action: 'submit_form',
      form_id: 'volunteer_apply',
      form_data: mockFormData,
      client_submission_id: TOKEN,
    };
    const result = await handleFormMode(body, mockTenantConfig);
    expect(result.status).toBe('success');
    const put = dynamoMock.commandCalls(PutCommand)
      .find((c) => c.args[0].input.TableName === 'test-form-submissions');
    expect(put.args[0].input.Item.submission_id).toBe(`volunteer_apply_idem_${TOKEN}`);
  });
});
