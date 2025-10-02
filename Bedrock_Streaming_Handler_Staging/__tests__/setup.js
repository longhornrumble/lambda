/**
 * Jest Test Setup
 * Initializes test environment and global mocks
 */

// Set test environment variables
process.env.AWS_REGION = 'us-east-1';
process.env.FORM_SUBMISSIONS_TABLE = 'test-form-submissions';
process.env.SMS_USAGE_TABLE = 'test-sms-usage';
process.env.SMS_MONTHLY_LIMIT = '100';
process.env.SES_FROM_EMAIL = 'test@example.com';
process.env.CONFIG_BUCKET = 'test-config-bucket';

// Suppress console output during tests (optional)
global.console = {
  ...console,
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};
