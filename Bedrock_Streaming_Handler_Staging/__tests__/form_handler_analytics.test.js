/**
 * Coverage gap test — Issue #5 PR A
 *
 * Gap 4: form_handler.js submitForm analytics call
 *
 * Tests the `if (sessionId && config.tenant_hash && requestId)` block that
 * calls writeSessionSummary with event_type:'FORM_COMPLETED' after
 * saveFormSubmission succeeds. Also tests the skip behaviour when any of
 * the three guard values is absent.
 *
 * analytics_writer is lazy-required inside submitForm, so jest.mock() hoisting
 * intercepts it without isolateModules.
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// Mock analytics_writer BEFORE requiring form_handler (jest hoists this).
// Variable must be prefixed with 'mock' for jest.mock factory access.
const mockWriteSessionSummary = jest.fn().mockResolvedValue(true);
jest.mock('../analytics_writer', () => ({
  writeSessionSummary: mockWriteSessionSummary,
}));

// AWS client mocks
const sesMock = mockClient(SESClient);
const snsMock = mockClient(SNSClient);
const dynamoMock = mockClient(DynamoDBDocumentClient);
const lambdaMock = mockClient(LambdaClient);
const s3Mock = mockClient(S3Client);

// Restore https.request after each test (mirrors form_handler.test.js pattern)
const _https = require('https');
const _ORIGINAL_HTTPS_REQUEST = _https.request;
afterEach(() => {
  _https.request = _ORIGINAL_HTTPS_REQUEST;
});

const { submitForm, handleFormMode } = require('../form_handler');

// Config fixture that includes tenant_hash (required for the analytics guard)
const configWithTenantHash = {
  tenant_id: 'TEST123',
  tenant_hash: 'my87674d777bf9',
  chat_title: 'Test Organization',
  conversational_forms: {
    volunteer_apply: {
      form_id: 'volunteer_apply',
      title: 'Volunteer Application',
      fields: [
        { id: 'first_name', type: 'text', required: true },
        { id: 'email', type: 'email', required: true },
      ],
      fulfillment: {
        email_to: 'ops@example.com',
      },
    },
  },
};

const minimalFormData = {
  first_name: 'Jane',
  email: 'jane@example.com',
};

describe('Gap 4 — submitForm: FORM_COMPLETED analytics write', () => {
  beforeEach(() => {
    sesMock.reset();
    snsMock.reset();
    dynamoMock.reset();
    lambdaMock.reset();
    s3Mock.reset();
    mockWriteSessionSummary.mockClear();

    // Allow DynamoDB put to succeed silently
    dynamoMock.on(PutCommand).resolves({});
    // Allow SES to succeed silently
    sesMock.on(SendEmailCommand).resolves({ MessageId: 'ses-ok' });
  });

  test('writeSessionSummary called with FORM_COMPLETED when sessionId + tenant_hash + requestId all present', async () => {
    await submitForm(
      'volunteer_apply',
      minimalFormData,
      configWithTenantHash,
      'sess_form_123',      // sessionId
      'conv_form_456',      // conversationId
      'req-form-001',       // requestId
      '2026-05-04T20:00:00.000Z'  // clientTimestamp
    );

    expect(mockWriteSessionSummary).toHaveBeenCalledTimes(1);
    const call = mockWriteSessionSummary.mock.calls[0][0];
    expect(call.event_type).toBe('FORM_COMPLETED');
    expect(call.session_id).toBe('sess_form_123');
    expect(call.tenant_hash).toBe('my87674d777bf9');
    expect(call.request_id).toBe('req-form-001');
    expect(call.event_payload.form_id).toBe('volunteer_apply');
  });

  test('client_timestamp in analytics call matches the provided clientTimestamp', async () => {
    const ts = '2026-05-04T21:30:00.000Z';
    await submitForm(
      'volunteer_apply',
      minimalFormData,
      configWithTenantHash,
      'sess_ts_test',
      'conv_ts_test',
      'req-ts-001',
      ts
    );

    const call = mockWriteSessionSummary.mock.calls[0][0];
    expect(call.client_timestamp).toBe(ts);
  });

  test('writeSessionSummary NOT called when sessionId is missing', async () => {
    await submitForm(
      'volunteer_apply',
      minimalFormData,
      configWithTenantHash,
      null,            // sessionId absent
      'conv_789',
      'req-form-002',
      '2026-05-04T20:00:00.000Z'
    );

    expect(mockWriteSessionSummary).not.toHaveBeenCalled();
  });

  test('writeSessionSummary NOT called when tenant_hash is missing from config', async () => {
    const configWithoutHash = { ...configWithTenantHash };
    delete configWithoutHash.tenant_hash;

    await submitForm(
      'volunteer_apply',
      minimalFormData,
      configWithoutHash,
      'sess_no_hash',
      'conv_no_hash',
      'req-form-003',
      '2026-05-04T20:00:00.000Z'
    );

    expect(mockWriteSessionSummary).not.toHaveBeenCalled();
  });

  test('writeSessionSummary NOT called when requestId is missing', async () => {
    await submitForm(
      'volunteer_apply',
      minimalFormData,
      configWithTenantHash,
      'sess_no_rid',
      'conv_no_rid',
      null,            // requestId absent
      '2026-05-04T20:00:00.000Z'
    );

    expect(mockWriteSessionSummary).not.toHaveBeenCalled();
  });

  test('client_timestamp falls back to current ISO string when not provided', async () => {
    await submitForm(
      'volunteer_apply',
      minimalFormData,
      configWithTenantHash,
      'sess_no_ts',
      'conv_no_ts',
      'req-no-ts-001',
      null  // clientTimestamp absent
    );

    expect(mockWriteSessionSummary).toHaveBeenCalledTimes(1);
    const call = mockWriteSessionSummary.mock.calls[0][0];
    // Should be a valid ISO string (not null/undefined)
    expect(() => new Date(call.client_timestamp).toISOString()).not.toThrow();
  });

  test('submitForm succeeds (does not throw) even if writeSessionSummary rejects', async () => {
    mockWriteSessionSummary.mockRejectedValueOnce(new Error('DDB throttle'));

    await expect(
      submitForm(
        'volunteer_apply',
        minimalFormData,
        configWithTenantHash,
        'sess_err',
        'conv_err',
        'req-err-001',
        '2026-05-04T20:00:00.000Z'
      )
    ).resolves.toBeDefined(); // form submission result, not the analytics write
  });

  // ── Through handleFormMode routing ──

  test('handleFormMode with action:submit_form triggers FORM_COMPLETED analytics', async () => {
    const body = {
      form_mode: true,
      form_id: 'volunteer_apply',
      action: 'submit_form',
      form_data: minimalFormData,
      session_id: 'sess_route_test',
      conversation_id: 'conv_route_test',
      client_timestamp: '2026-05-04T22:00:00.000Z',
    };

    await handleFormMode(body, configWithTenantHash, 'req-route-001');

    expect(mockWriteSessionSummary).toHaveBeenCalledTimes(1);
    expect(mockWriteSessionSummary.mock.calls[0][0].event_type).toBe('FORM_COMPLETED');
  });
});
