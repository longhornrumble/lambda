/**
 * Form Handler Backend Tests
 *
 * Comprehensive test suite for the backend form handling including:
 * - Form field validation (email, phone, custom rules)
 * - Form submission and fulfillment routing
 * - Multi-channel notifications (email, SMS, webhooks)
 * - DynamoDB storage and error handling
 * - Integration with tenant configurations
 * - Security and input sanitization
 *
 * Target: >90% test coverage for backend form processing
 */

const { describe, it, expect, beforeEach, afterEach, vi } = require('vitest');

// Mock AWS services
const mockSES = {
  sendEmail: vi.fn().mockReturnValue({
    promise: vi.fn().mockResolvedValue({ MessageId: 'test-message-id' })
  })
};

const mockSNS = {
  publish: vi.fn().mockReturnValue({
    promise: vi.fn().mockResolvedValue({ MessageId: 'test-sms-id' })
  })
};

const mockDynamoDB = {
  put: vi.fn().mockReturnValue({
    promise: vi.fn().mockResolvedValue({})
  })
};

// Mock AWS SDK
vi.mock('aws-sdk', () => ({
  SES: vi.fn(() => mockSES),
  SNS: vi.fn(() => mockSNS),
  DynamoDB: {
    DocumentClient: vi.fn(() => mockDynamoDB)
  }
}));

// Mock HTTPS for webhook testing
const mockHttpsRequest = vi.fn();
vi.mock('https', () => ({
  request: mockHttpsRequest
}));

// Import the form handler after mocking
const {
  handleFormMode,
  validateFormField,
  submitForm
} = require('../form_handler');

// Test configurations
const testTenantConfig = {
  tenant_id: 'ATL642715',
  chat_title: 'Atlanta Angels',
  conversational_forms: {
    volunteer_application: {
      enabled: true,
      title: 'Volunteer Application',
      fields: [
        { id: 'first_name', label: 'First Name', type: 'text', required: true },
        { id: 'email', label: 'Email', type: 'email', required: true },
        { id: 'phone', label: 'Phone', type: 'phone', required: false },
        { id: 'age_confirm', label: 'Age Confirmation', type: 'select', required: true }
      ],
      fulfillment: {
        email_to: 'applications@atlantaangels.org',
        sms_to: '+1-404-555-0123',
        webhook_url: 'https://hooks.zapier.com/hooks/catch/atlanta-angels'
      }
    }
  },
  send_confirmation_email: true
};

const testFormData = {
  first_name: 'John',
  last_name: 'Doe',
  email: 'john.doe@example.com',
  phone: '404-555-9876',
  age_confirm: 'yes',
  commitment_confirm: 'yes',
  background: 'Experienced educator passionate about mentoring'
};

describe('Form Handler - Core Functionality', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    // Reset environment variables
    process.env.FORM_SUBMISSIONS_TABLE = 'test-form-submissions';
    process.env.SES_FROM_EMAIL = 'noreply@test.com';
    process.env.AWS_REGION = 'us-east-1';

    // Setup console spy
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should handle form mode requests correctly', async () => {
    const requestBody = {
      form_mode: true,
      action: 'validate_field',
      field_id: 'email',
      field_value: 'test@example.com'
    };

    const result = await handleFormMode(requestBody, testTenantConfig);

    expect(result).toMatchObject({
      type: 'validation_success',
      field: 'email',
      status: 'success',
      message: 'Valid'
    });
  });

  it('should handle form submission requests', async () => {
    const requestBody = {
      form_mode: true,
      action: 'submit_form',
      form_id: 'volunteer_application',
      form_data: testFormData
    };

    const result = await handleFormMode(requestBody, testTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      message: expect.stringContaining('successfully'),
      submissionId: expect.stringMatching(/volunteer_application_\d+/)
    });

    // Verify DynamoDB save was called
    expect(mockDynamoDB.put).toHaveBeenCalledWith(
      expect.objectContaining({
        TableName: 'test-form-submissions',
        Item: expect.objectContaining({
          form_id: 'volunteer_application',
          form_data: testFormData,
          status: 'pending_fulfillment'
        })
      })
    );
  });

  it('should return default response for unspecified actions', async () => {
    const requestBody = {
      form_mode: true
      // No action specified
    };

    const result = await handleFormMode(requestBody, testTenantConfig);

    expect(result).toMatchObject({
      type: 'form_response',
      status: 'success',
      message: 'Field accepted',
      continue: true
    });
  });
});

describe('Form Handler - Field Validation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Required field validation', () => {
    it('should reject empty required fields', async () => {
      const result = await validateFormField('first_name', '', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_error',
        field: 'first_name',
        errors: ['This field is required'],
        status: 'error'
      });
    });

    it('should reject whitespace-only required fields', async () => {
      const result = await validateFormField('first_name', '   ', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_error',
        field: 'first_name',
        errors: ['This field is required'],
        status: 'error'
      });
    });

    it('should accept valid required fields', async () => {
      const result = await validateFormField('first_name', 'John', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_success',
        field: 'first_name',
        status: 'success',
        message: 'Valid'
      });
    });
  });

  describe('Email validation', () => {
    it('should reject invalid email formats', async () => {
      const invalidEmails = [
        'invalid-email',
        'missing@domain',
        '@domain.com',
        'user@',
        'user@domain',
        'user name@domain.com',
        'user..name@domain.com'
      ];

      for (const email of invalidEmails) {
        const result = await validateFormField('email', email, testTenantConfig);

        expect(result).toMatchObject({
          type: 'validation_error',
          field: 'email',
          errors: ['Please enter a valid email address'],
          status: 'error'
        });
      }
    });

    it('should accept valid email formats', async () => {
      const validEmails = [
        'user@domain.com',
        'user.name@domain.com',
        'user+tag@domain.co.uk',
        'first.last@subdomain.domain.org',
        'test123@example-domain.net'
      ];

      for (const email of validEmails) {
        const result = await validateFormField('email', email, testTenantConfig);

        expect(result).toMatchObject({
          type: 'validation_success',
          field: 'email',
          status: 'success',
          message: 'Valid'
        });
      }
    });
  });

  describe('Phone validation', () => {
    it('should reject invalid phone formats', async () => {
      const invalidPhones = [
        'abc-def-ghij',
        '123-456-789a',
        'not-a-phone',
        '123.456.7890.ext',
        'phone number'
      ];

      for (const phone of invalidPhones) {
        const result = await validateFormField('phone', phone, testTenantConfig);

        expect(result).toMatchObject({
          type: 'validation_error',
          field: 'phone',
          errors: ['Please enter a valid phone number'],
          status: 'error'
        });
      }
    });

    it('should accept valid phone formats', async () => {
      const validPhones = [
        '123-456-7890',
        '(123) 456-7890',
        '123 456 7890',
        '+1-123-456-7890',
        '1234567890',
        '+1 (123) 456-7890'
      ];

      for (const phone of validPhones) {
        const result = await validateFormField('phone', phone, testTenantConfig);

        expect(result).toMatchObject({
          type: 'validation_success',
          field: 'phone',
          status: 'success',
          message: 'Valid'
        });
      }
    });
  });

  describe('Business rule validation', () => {
    it('should reject users under 22 for age confirmation', async () => {
      const result = await validateFormField('age_confirm', 'no', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_error',
        field: 'age_confirm',
        errors: ['You must be at least 22 years old to volunteer'],
        status: 'error'
      });
    });

    it('should accept users 22 and older', async () => {
      const result = await validateFormField('age_confirm', 'yes', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_success',
        field: 'age_confirm',
        status: 'success',
        message: 'Valid'
      });
    });

    it('should reject users who cannot commit to one year', async () => {
      const result = await validateFormField('commitment_confirm', 'no', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_error',
        field: 'commitment_confirm',
        errors: ['A one year commitment is required for this program'],
        status: 'error'
      });
    });

    it('should accept users who can commit to one year', async () => {
      const result = await validateFormField('commitment_confirm', 'yes', testTenantConfig);

      expect(result).toMatchObject({
        type: 'validation_success',
        field: 'commitment_confirm',
        status: 'success',
        message: 'Valid'
      });
    });
  });
});

describe('Form Handler - Form Submission and Fulfillment', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should submit form and trigger all fulfillment channels', async () => {
    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      message: expect.stringContaining('successfully'),
      fulfillment: expect.arrayContaining([
        expect.objectContaining({ channel: 'email', status: 'sent' }),
        expect.objectContaining({ channel: 'sms', status: 'sent' }),
        expect.objectContaining({ channel: 'webhook', status: 'sent' })
      ])
    });

    // Verify DynamoDB storage
    expect(mockDynamoDB.put).toHaveBeenCalledWith(
      expect.objectContaining({
        TableName: 'test-form-submissions',
        Item: expect.objectContaining({
          form_id: 'volunteer_application',
          tenant_id: 'ATL642715',
          form_data: testFormData,
          status: 'pending_fulfillment'
        })
      })
    );

    // Verify email fulfillment
    expect(mockSES.sendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        Source: 'noreply@test.com',
        Destination: {
          ToAddresses: ['applications@atlantaangels.org']
        },
        Message: expect.objectContaining({
          Subject: { Data: 'New Form Submission: volunteer_application' },
          Body: {
            Html: { Data: expect.stringContaining('John') }
          }
        })
      })
    );

    // Verify SMS fulfillment
    expect(mockSNS.publish).toHaveBeenCalledWith(
      expect.objectContaining({
        Message: expect.stringContaining('John Doe'),
        PhoneNumber: '+1-404-555-0123'
      })
    );

    // Verify confirmation email
    expect(mockSES.sendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        Destination: {
          ToAddresses: ['john.doe@example.com']
        },
        Message: expect.objectContaining({
          Subject: { Data: 'Thank you for your volunteer_application submission' }
        })
      })
    );
  });

  it('should handle email fulfillment failures gracefully', async () => {
    mockSES.sendEmail.mockReturnValueOnce({
      promise: vi.fn().mockRejectedValue(new Error('SES error'))
    });

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      fulfillment: expect.arrayContaining([
        expect.objectContaining({
          channel: 'email',
          status: 'failed',
          error: 'SES error'
        })
      ])
    });
  });

  it('should handle SMS fulfillment failures gracefully', async () => {
    mockSNS.publish.mockReturnValueOnce({
      promise: vi.fn().mockRejectedValue(new Error('SNS error'))
    });

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      fulfillment: expect.arrayContaining([
        expect.objectContaining({
          channel: 'sms',
          status: 'failed',
          error: 'SNS error'
        })
      ])
    });
  });

  it('should handle webhook fulfillment failures gracefully', async () => {
    const mockRequest = {
      on: vi.fn(),
      write: vi.fn(),
      end: vi.fn()
    };

    mockHttpsRequest.mockImplementation((options, callback) => {
      // Simulate request error
      setTimeout(() => {
        mockRequest.on.mock.calls.find(call => call[0] === 'error')?.[1](new Error('Network error'));
      }, 0);
      return mockRequest;
    });

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      fulfillment: expect.arrayContaining([
        expect.objectContaining({
          channel: 'webhook',
          status: 'failed',
          error: 'Network error'
        })
      ])
    });
  });

  it('should handle DynamoDB failures without failing submission', async () => {
    mockDynamoDB.put.mockReturnValueOnce({
      promise: vi.fn().mockRejectedValue(new Error('DynamoDB error'))
    });

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    // Should still succeed even if DynamoDB fails
    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success'
    });

    // Should log the error
    expect(console.error).toHaveBeenCalledWith('Error saving to DynamoDB:', expect.any(Error));
  });

  it('should handle confirmation email failures gracefully', async () => {
    // Mock first call (fulfillment email) to succeed, second call (confirmation) to fail
    mockSES.sendEmail
      .mockReturnValueOnce({
        promise: vi.fn().mockResolvedValue({ MessageId: 'fulfillment-id' })
      })
      .mockReturnValueOnce({
        promise: vi.fn().mockRejectedValue(new Error('Confirmation email error'))
      });

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    // Should still report success
    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success'
    });

    // Should log the confirmation email error
    expect(console.error).toHaveBeenCalledWith('Failed to send confirmation email:', expect.any(Error));
  });
});

describe('Form Handler - Email Generation and Content', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should generate properly formatted fulfillment emails', async () => {
    await submitForm('volunteer_application', testFormData, testTenantConfig);

    const emailCall = mockSES.sendEmail.mock.calls.find(call =>
      call[0].Destination.ToAddresses.includes('applications@atlantaangels.org')
    );

    expect(emailCall[0]).toMatchObject({
      Source: 'noreply@test.com',
      Destination: {
        ToAddresses: ['applications@atlantaangels.org']
      },
      Message: {
        Subject: { Data: 'New Form Submission: volunteer_application' },
        Body: {
          Html: {
            Data: expect.stringMatching(/<h2>New volunteer_application Submission<\/h2>.*<table.*>.*John.*john\.doe@example\.com.*<\/table>/s)
          }
        }
      }
    });

    const htmlContent = emailCall[0].Message.Body.Html.Data;
    expect(htmlContent).toContain('John');
    expect(htmlContent).toContain('john.doe@example.com');
    expect(htmlContent).toContain('404-555-9876');
    expect(htmlContent).toContain('ATL642715');
  });

  it('should generate properly formatted confirmation emails', async () => {
    await submitForm('volunteer_application', testFormData, testTenantConfig);

    const confirmationCall = mockSES.sendEmail.mock.calls.find(call =>
      call[0].Destination.ToAddresses.includes('john.doe@example.com')
    );

    expect(confirmationCall[0]).toMatchObject({
      Source: 'noreply@test.com',
      Destination: {
        ToAddresses: ['john.doe@example.com']
      },
      Message: {
        Subject: { Data: 'Thank you for your volunteer_application submission' },
        Body: {
          Html: {
            Data: expect.stringMatching(/Thank you for your submission.*Atlanta Angels.*volunteer_application.*review your information/s)
          }
        }
      }
    });
  });

  it('should handle missing email in form data', async () => {
    const formDataWithoutEmail = { ...testFormData };
    delete formDataWithoutEmail.email;

    const result = await submitForm('volunteer_application', formDataWithoutEmail, testTenantConfig);

    expect(result.status).toBe('success');

    // Should only have fulfillment email, not confirmation email
    expect(mockSES.sendEmail).toHaveBeenCalledTimes(1);
    expect(mockSES.sendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        Destination: {
          ToAddresses: ['applications@atlantaangels.org']
        }
      })
    );
  });

  it('should respect send_confirmation_email setting', async () => {
    const configWithoutConfirmation = {
      ...testTenantConfig,
      send_confirmation_email: false
    };

    await submitForm('volunteer_application', testFormData, configWithoutConfirmation);

    // Should only have fulfillment email, not confirmation email
    expect(mockSES.sendEmail).toHaveBeenCalledTimes(1);
    expect(mockSES.sendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        Destination: {
          ToAddresses: ['applications@atlantaangels.org']
        }
      })
    );
  });
});

describe('Form Handler - Webhook Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should send properly formatted webhook data', async () => {
    const mockResponse = {
      statusCode: 200
    };

    const mockRequest = {
      on: vi.fn(),
      write: vi.fn(),
      end: vi.fn()
    };

    mockHttpsRequest.mockImplementation((options, callback) => {
      // Simulate successful response
      setTimeout(() => callback(mockResponse), 0);
      return mockRequest;
    });

    await submitForm('volunteer_application', testFormData, testTenantConfig);

    expect(mockHttpsRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        hostname: 'hooks.zapier.com',
        port: 443,
        path: '/hooks/catch/atlanta-angels',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': expect.any(Number)
        }
      }),
      expect.any(Function)
    );

    expect(mockRequest.write).toHaveBeenCalledWith(
      expect.stringContaining('"form_id":"volunteer_application"')
    );

    const writtenData = JSON.parse(mockRequest.write.mock.calls[0][0]);
    expect(writtenData).toMatchObject({
      form_id: 'volunteer_application',
      timestamp: expect.stringMatching(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/),
      data: testFormData
    });
  });

  it('should handle webhook URLs with query parameters', async () => {
    const configWithQueryParams = {
      ...testTenantConfig,
      conversational_forms: {
        ...testTenantConfig.conversational_forms,
        volunteer_application: {
          ...testTenantConfig.conversational_forms.volunteer_application,
          fulfillment: {
            ...testTenantConfig.conversational_forms.volunteer_application.fulfillment,
            webhook_url: 'https://hooks.zapier.com/hooks/catch/atlanta-angels?key=value&test=true'
          }
        }
      }
    };

    const mockRequest = {
      on: vi.fn(),
      write: vi.fn(),
      end: vi.fn()
    };

    mockHttpsRequest.mockImplementation((options, callback) => {
      setTimeout(() => callback({ statusCode: 200 }), 0);
      return mockRequest;
    });

    await submitForm('volunteer_application', testFormData, configWithQueryParams);

    expect(mockHttpsRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        path: '/hooks/catch/atlanta-angels?key=value&test=true'
      }),
      expect.any(Function)
    );
  });
});

describe('Form Handler - Error Handling and Edge Cases', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should handle missing form configuration', async () => {
    const configWithoutForms = {
      tenant_id: 'test',
      chat_title: 'Test'
      // No conversational_forms
    };

    const result = await submitForm('volunteer_application', testFormData, configWithoutForms);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      fulfillment: []
    });
  });

  it('should handle missing fulfillment configuration', async () => {
    const configWithoutFulfillment = {
      ...testTenantConfig,
      conversational_forms: {
        volunteer_application: {
          ...testTenantConfig.conversational_forms.volunteer_application,
          fulfillment: undefined
        }
      }
    };

    const result = await submitForm('volunteer_application', testFormData, configWithoutFulfillment);

    expect(result).toMatchObject({
      type: 'form_complete',
      status: 'success',
      fulfillment: []
    });
  });

  it('should handle completely broken submission gracefully', async () => {
    // Mock all services to fail
    mockDynamoDB.put.mockReturnValue({
      promise: vi.fn().mockRejectedValue(new Error('DynamoDB error'))
    });

    mockSES.sendEmail.mockReturnValue({
      promise: vi.fn().mockRejectedValue(new Error('SES error'))
    });

    mockSNS.publish.mockReturnValue({
      promise: vi.fn().mockRejectedValue(new Error('SNS error'))
    });

    const mockRequest = {
      on: vi.fn(),
      write: vi.fn(),
      end: vi.fn()
    };

    mockHttpsRequest.mockImplementation((options, callback) => {
      setTimeout(() => {
        mockRequest.on.mock.calls.find(call => call[0] === 'error')?.[1](new Error('Webhook error'));
      }, 0);
      return mockRequest;
    });

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    // Should return error result
    expect(result).toMatchObject({
      type: 'form_error',
      status: 'error',
      message: expect.stringContaining('error submitting'),
      error: expect.any(String)
    });
  });

  it('should handle SMS message truncation', async () => {
    const longFormData = {
      ...testFormData,
      first_name: 'A'.repeat(100),
      last_name: 'B'.repeat(100)
    };

    await submitForm('volunteer_application', longFormData, testTenantConfig);

    expect(mockSNS.publish).toHaveBeenCalledWith(
      expect.objectContaining({
        Message: expect.stringMatching(/^.{1,160}$/), // SMS character limit
        PhoneNumber: '+1-404-555-0123'
      })
    );
  });

  it('should handle multiple email recipients', async () => {
    const configWithMultipleEmails = {
      ...testTenantConfig,
      conversational_forms: {
        volunteer_application: {
          ...testTenantConfig.conversational_forms.volunteer_application,
          fulfillment: {
            ...testTenantConfig.conversational_forms.volunteer_application.fulfillment,
            email_to: ['admin@example.com', 'backup@example.com']
          }
        }
      }
    };

    await submitForm('volunteer_application', testFormData, configWithMultipleEmails);

    expect(mockSES.sendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        Destination: {
          ToAddresses: ['admin@example.com', 'backup@example.com']
        }
      })
    );
  });

  it('should handle missing environment variables gracefully', async () => {
    delete process.env.SES_FROM_EMAIL;
    delete process.env.FORM_SUBMISSIONS_TABLE;

    const result = await submitForm('volunteer_application', testFormData, testTenantConfig);

    // Should still work with defaults
    expect(result.status).toBe('success');

    expect(mockDynamoDB.put).toHaveBeenCalledWith(
      expect.objectContaining({
        TableName: 'picasso-form-submissions' // Default table name
      })
    );

    expect(mockSES.sendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        Source: 'noreply@picasso.ai' // Default from email
      })
    );
  });
});