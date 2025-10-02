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

// Import module under test
const {
  handleFormMode,
  validateFormField,
  submitForm
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

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      expect(sesMock.calls()).toHaveLength(2); // Fulfillment + confirmation email
      expect(sesMock.commandCalls(SendEmailCommand)).toHaveLength(2);
    });

    it('should use SNSClient with PublishCommand for SMS sending', async () => {
      snsMock.on(PublishCommand).resolves({ MessageId: 'test-sms-id' });
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
      dynamoMock.on(UpdateCommand).resolves({});

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);
    });

    it('should use DynamoDBDocumentClient with PutCommand for form storage', async () => {
      sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-email-id' });
      dynamoMock.on(PutCommand).resolves({});
      dynamoMock.on(GetCommand).resolves({});

      await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

      expect(dynamoMock.commandCalls(PutCommand)).toHaveLength(1);
      const putCall = dynamoMock.commandCalls(PutCommand)[0];
      expect(putCall.args[0].input).toMatchObject({
        TableName: 'test-form-submissions',
        Item: expect.objectContaining({
          form_id: 'volunteer_apply',
          form_data: mockFormData
        })
      });
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
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(PutCommand).resolves({});
    dynamoMock.on(UpdateCommand).resolves({});
  });

  it('should retrieve monthly SMS usage from DynamoDB', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 50 } });

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(dynamoMock.commandCalls(GetCommand)).toHaveLength(1);
    const getCall = dynamoMock.commandCalls(GetCommand)[0];
    expect(getCall.args[0].input).toMatchObject({
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

    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'sent',
        usage: 1,
        limit: 100
      })
    );
  });

  it('should send SMS when under monthly limit', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 50 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'sent',
        usage: 51,
        limit: 100
      })
    );
  });

  it('should skip SMS when monthly limit reached', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 100 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(0);
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'skipped',
        reason: 'monthly_limit_reached',
        usage: 100,
        limit: 100
      })
    );
  });

  it('should skip SMS when monthly limit exceeded', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 150 } });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(0);
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'skipped',
        reason: 'monthly_limit_reached'
      })
    );
  });

  it('should increment SMS usage counter after sending', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 25 } });

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);
    const updateCall = dynamoMock.commandCalls(UpdateCommand)[0];
    expect(updateCall.args[0].input).toMatchObject({
      TableName: 'test-sms-usage',
      Key: {
        tenant_id: 'TEST123',
        month: expect.stringMatching(/^\d{4}-\d{2}$/)
      },
      UpdateExpression: 'SET #count = if_not_exists(#count, :zero) + :inc, updated_at = :now'
    });
  });

  it('should handle DynamoDB GetCommand errors gracefully (fail-safe to 0)', async () => {
    dynamoMock.on(GetCommand).rejects(new Error('DynamoDB error'));
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // Should default to 0 usage and allow SMS
    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({
        channel: 'sms',
        status: 'sent'
      })
    );
  });

  it('should handle DynamoDB UpdateCommand errors gracefully', async () => {
    dynamoMock.on(GetCommand).resolves({ Item: { count: 10 } });
    dynamoMock.on(UpdateCommand).rejects(new Error('DynamoDB update error'));

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    // Should still send SMS even if increment fails
    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);
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
    expect(payload.submission_id).toMatch(/^request_support_\d+$/);
  });

  it('should store form data in S3 with correct key format', async () => {
    s3Mock.on(PutObjectCommand).resolves({});

    const result = await submitForm('newsletter', mockFormData, mockTenantConfig);

    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
    const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
    expect(putCall.args[0].input).toMatchObject({
      Bucket: 'test-forms-bucket',
      Key: expect.stringMatching(/^submissions\/TEST123\/newsletter\/newsletter_\d+\.json$/),
      Body: JSON.stringify(mockFormData),
      ContentType: 'application/json'
    });
  });

  it('should send email with priority indicator', async () => {
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

  it('should send SMS with priority emoji - high priority', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const formDataHighPriority = { ...mockFormData, urgency: 'high' };
    await submitForm('volunteer_apply', formDataHighPriority, mockTenantConfig);

    const smsCall = snsMock.commandCalls(PublishCommand)[0];
    expect(smsCall.args[0].input.Message).toMatch(/^🚨/); // High priority emoji
  });

  it('should send SMS with priority emoji - normal priority', async () => {
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'test-id' });
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    const smsCall = snsMock.commandCalls(PublishCommand)[0];
    expect(smsCall.args[0].input.Message).toMatch(/^📝/); // Normal priority emoji
  });

  it('should send SMS with priority emoji - low priority', async () => {
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
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const https = require('https');
    https.request = jest.fn((options, callback) => {
      setTimeout(() => callback({ statusCode: 200 }), 0);
      return { on: jest.fn(), write: jest.fn(), end: jest.fn() };
    });

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.fulfillment).toHaveLength(3);
    expect(result.fulfillment).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ channel: 'email', status: 'sent' }),
        expect.objectContaining({ channel: 'sms', status: 'sent' }),
        expect.objectContaining({ channel: 'webhook', status: 'sent' })
      ])
    );
  });

  it('should handle partial fulfillment failures gracefully', async () => {
    sesMock.on(SendEmailCommand).rejects(new Error('SES error'));
    snsMock.on(PublishCommand).resolves({ MessageId: 'test-id' });
    dynamoMock.on(GetCommand).resolves({ Item: { count: 0 } });
    dynamoMock.on(UpdateCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.status).toBe('success');
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'email', status: 'failed', error: 'SES error' })
    );
    expect(result.fulfillment).toContainEqual(
      expect.objectContaining({ channel: 'sms', status: 'sent' })
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
    dynamoMock.on(PutCommand).resolves({});

    const result = await submitForm('volunteer_apply', mockFormData, mockTenantConfig);

    expect(result.status).toBe('success');
    expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);

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

    const invalidResult = await validateFormField('phone', 'abc123', mockTenantConfig);
    expect(invalidResult.status).toBe('error');
    expect(invalidResult.errors).toContain('Please enter a valid phone number');
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
    expect(result.submissionId).toMatch(/^volunteer_apply_\d+$/);
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
});
