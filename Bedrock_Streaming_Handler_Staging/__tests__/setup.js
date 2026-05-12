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
// BEDROCK_MODEL_ID required by index.js fail-loud at module load
// (Phase 4 EC-P4-2). Preserve any value the CI matrix provides.
process.env.BEDROCK_MODEL_ID =
  process.env.BEDROCK_MODEL_ID ||
  'global.anthropic.claude-haiku-4-5-20251001-v1:0';

// Suppress console output during tests (optional)
global.console = {
  ...console,
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};
