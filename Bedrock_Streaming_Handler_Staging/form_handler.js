/**
 * Form Handler for Conversational Forms
 *
 * Handles form field validation and submission without calling Bedrock.
 * Provides local validation and fulfillment routing.
 *
 * MIGRATED TO AWS SDK v3 (October 2025)
 */

const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const {
  extractCanonicalContact,
} = require('./contact_extractor');
const {
  resolveEmailsFromUserIds,
  resolvePhonesFromUserIds,
  resolveQuietHoursFallbackEmails,
} = require('./clerk_helper');
const {
  resolveEmailsFromEmployeeIds,
  resolvePhonesFromEmployeeIds,
  resolveQuietHoursFallbackFromEmployeeIds,
} = require('./recipient_resolver');

// Initialize AWS SDK v3 clients
const sesClient = new SESClient({ region: process.env.AWS_REGION || 'us-east-1' });
const snsClient = new SNSClient({ region: process.env.AWS_REGION || 'us-east-1' });
const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const dynamodb = DynamoDBDocumentClient.from(ddbClient);
const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION || 'us-east-1' });
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

// Form submission table
const FORM_SUBMISSIONS_TABLE = process.env.FORM_SUBMISSIONS_TABLE || 'picasso-form-submissions';
const SMS_USAGE_TABLE = process.env.SMS_USAGE_TABLE || 'picasso-sms-usage';
const NOTIFICATION_SENDS_TABLE = process.env.NOTIFICATION_SENDS_TABLE || 'picasso-notification-sends';
const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';
const SMS_MONTHLY_LIMIT = parseInt(process.env.SMS_MONTHLY_LIMIT || '100', 10);

/**
 * Sanitize text for SMS messages - remove special characters that could cause issues
 * @param {string} text - Raw text
 * @param {number} maxLength - Maximum allowed length (default 50)
 * @returns {string} - SMS-safe text
 */
function sanitizeForSMS(text, maxLength = 50) {
  if (!text || typeof text !== 'string') {
    return '';
  }
  // Keep only alphanumeric, spaces, and basic punctuation safe for SMS
  return text.replace(/[^\w\s@.\-]/g, '').trim().slice(0, maxLength);
}

/**
 * HTML-escape a string to prevent XSS in email templates.
 * Used for tenant-controlled values (chat_title, org name) injected into HTML.
 */
function escapeHtml(str) {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/**
 * Validate a URL is https:// only — prevents javascript: and data: URI injection.
 */
function isValidHttpsUrl(url) {
  try { return url && new URL(url).protocol === 'https:'; } catch { return false; }
}

/**
 * Sanitize a CSS value — strip semicolons and anything after them to prevent CSS injection.
 */
function sanitizeCssValue(val) {
  if (!val || typeof val !== 'string') return '';
  return val.split(';')[0].trim();
}

// Track if we've warned about SES fallback (only warn once per Lambda instance)
let sesEmailWarningLogged = false;

/**
 * Get SES from email with warning if using fallback
 * @returns {string} - SES source email address
 */
function getSESFromEmail() {
  const email = process.env.SES_FROM_EMAIL;
  if (!email) {
    if (!sesEmailWarningLogged) {
      console.warn('⚠️ SES_FROM_EMAIL not set - using fallback notify@myrecruiter.ai');
      sesEmailWarningLogged = true;
    }
    return 'notify@myrecruiter.ai';
  }
  return email;
}

/**
 * Handle form mode requests (bypass Bedrock)
 * @param {Object} body - Request body
 * @param {Object} tenantConfig - Tenant configuration
 * @returns {Object} Response for form field or submission
 */
async function handleFormMode(body, tenantConfig) {
  console.log('📝 Form mode detected, handling locally');

  const {
    form_mode,
    form_id,
    field_id,
    field_value,
    form_data,
    action,
    session_id,
    conversation_id
  } = body;

  // Handle different form actions
  if (action === 'validate_field') {
    return await validateFormField(field_id, field_value, tenantConfig);
  }

  if (action === 'submit_form') {
    return await submitForm(form_id, form_data, tenantConfig, session_id, conversation_id);
  }

  // Default validation response
  return {
    type: 'form_response',
    status: 'success',
    message: 'Field accepted',
    continue: true
  };
}

/**
 * Validate a single form field
 * @param {string} fieldId - Field identifier
 * @param {string} value - User input value
 * @param {Object} config - Tenant config with form definitions
 * @returns {Object} Validation result
 */
async function validateFormField(fieldId, value, config) {
  console.log(`🔍 Validating field ${fieldId}: ${value}`);

  const errors = [];

  // Basic validation rules
  if (!value || value.trim() === '') {
    errors.push('This field is required');
  }

  // Field-specific validation
  switch (fieldId) {
    case 'email':
      // Strengthened email validation with length limits (RFC 5321: max 320 chars)
      if (value) {
        if (value.length > 320) {
          errors.push('Email address is too long');
        } else if (!/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/.test(value)) {
          errors.push('Please enter a valid email address');
        }
      }
      break;

    case 'phone':
      // Strengthened phone validation: must have at least 7 digits, max 20 chars
      if (value) {
        const digitsOnly = value.replace(/\D/g, '');
        if (value.length > 20) {
          errors.push('Phone number is too long');
        } else if (digitsOnly.length < 7) {
          errors.push('Phone number must have at least 7 digits');
        } else if (!/^[\d\s\-\(\)\+]+$/.test(value)) {
          errors.push('Please enter a valid phone number');
        }
      }
      break;

    case 'age_confirm':
      if (value === 'no') {
        errors.push('You must be at least 22 years old to volunteer');
      }
      break;

    case 'commitment_confirm':
      if (value === 'no') {
        errors.push('A one year commitment is required for this program');
      }
      break;
  }

  if (errors.length > 0) {
    return {
      type: 'validation_error',
      field: fieldId,
      errors: errors,
      status: 'error'
    };
  }

  return {
    type: 'validation_success',
    field: fieldId,
    status: 'success',
    message: 'Valid'
  };
}

/**
 * Submit a completed form
 * @param {string} formId - Form identifier
 * @param {Object} formData - Collected form data
 * @param {Object} config - Tenant configuration
 * @param {string} sessionId - Session ID for tracking
 * @param {string} conversationId - Conversation ID for tracking
 * @returns {Object} Submission result
 */
async function submitForm(formId, formData, config, sessionId = null, conversationId = null) {
  console.log(`📨 Submitting form ${formId}:`, formData);

  try {
    // Validate required parameters
    if (!formId || !formData || !config) {
      throw new Error('Missing required parameters: formId, formData, or config');
    }

    // Determine priority for notifications
    const formConfig = config.conversational_forms?.[formId] || {};
    const priority = determinePriority(formId, formData, formConfig);
    console.log(`📊 Form priority determined: ${priority}`);

    // Save to DynamoDB
    const submissionId = `${formId}_${Date.now()}`;
    try {
      await saveFormSubmission(submissionId, formId, formData, config, priority, sessionId, conversationId);
    } catch (dbError) {
      console.error('❌ DynamoDB save failed:', dbError);
      // Continue with fulfillment even if DynamoDB save fails
    }

    // Extract canonical contact early — needed for consent record and confirmation email.
    // Form data uses field IDs (e.g. field_1774282600263), not named keys — extract via
    // the same contact extractor used for DynamoDB and Bubble.
    const transformedForContact = transformFormDataToLabels(formData, formConfig);
    const { contact: applicantContact } = extractCanonicalContact(transformedForContact);
    const applicantEmail = applicantContact?.email;
    const applicantPhone = applicantContact?.phone;

    // Write transactional consent record only if the contact explicitly consented to SMS.
    // Consent comes from:
    //   1. phone_with_consent composite field → subfield value "yes" in form_data
    //   2. Standalone "Text Message Consent" select field → value "yes" in form_data
    //   3. Legacy: sms_consent_given field
    // The consent subfield is identified by scanning form fields for type=select with
    // sms_consent: true, or by checking transformed labels for consent keywords.
    if (applicantPhone) {
      const consentGiven = findSmsConsent(formData, formConfig);
      if (consentGiven) {
        const smsDisclosure = findConsentDisclosure(formConfig) || config.sms_consent_language || '';
        writeConsentRecord(config.tenant_id, applicantPhone, formId, submissionId, smsDisclosure).catch(err => {
          console.error('❌ Consent record write failed:', err.message);
        });
      } else {
        console.log(`ℹ️ No SMS consent given for ${applicantPhone} in form ${formId} — skipping consent record`);
      }
    }

    // Route to appropriate fulfillment channel
    const fulfillmentResult = await routeFulfillment(formId, formData, config, submissionId, priority, sessionId, conversationId);

    // Applicant confirmation: per-form config takes precedence over tenant-level legacy flag.
    // - New config (notifications.applicant_confirmation): requires enabled === true (strict boolean)
    // - Legacy (config.send_confirmation_email): defaults to true if absent
    // Note: strict === true check is a minor tightening from the original truthy check.
    // In practice, the portal toggle always writes boolean true/false, so no behavioral change
    // for any real config. But if a config had enabled: 1 or enabled: "yes", it would now
    // be treated as disabled. This is intentional — enforce clean boolean values.
    const confirmationConfig = formConfig.notifications?.applicant_confirmation;
    const shouldSendConfirmation = applicantEmail && (
      confirmationConfig
        ? confirmationConfig.enabled === true
        : config.send_confirmation_email !== false
    );

    if (shouldSendConfirmation) {
      sendConfirmationEmail(applicantEmail, formId, formData, config, submissionId, sessionId).catch(err => {
        console.error('❌ Confirmation email failed:', err.message);
      });
    }

    // Applicant SMS confirmation: send a transactional SMS to the contact if configured.
    // Requires: phone number present, applicant_confirmation.sms.enabled === true in form config,
    // and SMS not globally disabled for the tenant.
    const applicantSmsConfig = confirmationConfig?.sms;
    const shouldSendApplicantSms = applicantPhone
      && applicantSmsConfig?.enabled === true
      && config.sms_settings?.enabled !== false;

    if (shouldSendApplicantSms) {
      const isDryRun = config.sms_settings?.applicant_sms_dry_run === true;
      const orgName = config.chat_title || config.tenant_id || '';
      const defaultApplicantSmsBody = `${orgName}: We received your submission. Check your email for details or reply HELP for assistance.`;
      const applicantSmsBody = applicantSmsConfig.template
        ? renderTemplate(applicantSmsConfig.template, {
            first_name: applicantContact?.first_name || '',
            last_name: applicantContact?.last_name || '',
            email: applicantEmail || '',
            organization_name: orgName,
            submission_id: submissionId,
          })
        : defaultApplicantSmsBody;

      if (isDryRun) {
        console.log(`🧪 [DRY RUN] Applicant SMS would send to ${applicantPhone}: "${applicantSmsBody}"`);
      } else {
        lambdaClient.send(new InvokeCommand({
          FunctionName: process.env.SMS_SENDER_FUNCTION || 'SMS_Sender',
          InvocationType: 'Event',
          Payload: JSON.stringify({
            to: applicantPhone,
            body: applicantSmsBody,
            tenantId: config.tenant_id,
            formId,
            submissionId,
            sessionId,
            type: 'applicant',
            sendType: 'contact',
            fromNumber: config.sms_settings?.from_number || '',
          })
        })).catch(err => {
          console.error('❌ Applicant SMS invocation failed:', err.message);
        });
      }
    }

    return {
      type: 'form_complete',
      status: 'success',
      message: 'Thank you! Your application has been submitted successfully. You will receive a confirmation email shortly.',
      submissionId: submissionId,
      priority: priority,
      fulfillment: fulfillmentResult
    };

  } catch (error) {
    console.error('❌ Form submission error:', error);
    return {
      type: 'form_error',
      status: 'error',
      message: 'There was an error submitting your form. Please try again or contact support.',
      error: error.message
    };
  }
}

/**
 * Determine notification priority based on form data
 * Ported from Master's form_handler.py:154-187
 */
function determinePriority(formId, formData, formConfig) {
  // Check for explicit urgency field
  if (formData.urgency) {
    const urgency = formData.urgency.toLowerCase();
    if (['immediate', 'urgent', 'high'].includes(urgency)) {
      return 'high';
    } else if (['normal', 'this week'].includes(urgency)) {
      return 'normal';
    } else {
      return 'low';
    }
  }

  // Check priority rules in config
  const priorityRules = formConfig.priority_rules || [];
  for (const rule of priorityRules) {
    const field = rule.field;
    const value = rule.value;
    const priority = rule.priority;

    if (formData[field] === value) {
      return priority;
    }
  }

  // Form-type based defaults
  const priorityDefaults = {
    'request_support': 'high',
    'volunteer_apply': 'normal',
    'lb_apply': 'normal',
    'dd_apply': 'normal',
    'donation': 'normal',
    'contact': 'normal',
    'newsletter': 'low'
  };

  return priorityDefaults[formId] || 'normal';
}

/**
 * Build labeled form data by mapping field IDs to their labels
 * @param {Object} formData - Raw form data with field IDs as keys
 * @param {Object} formConfig - Form configuration with field definitions
 * @returns {Object} Labeled form data with {label, value, type} for each field
 */
function buildLabeledFormData(formData, formConfig) {
  const labeled = {};
  const fields = formConfig?.fields || [];

  // Build a lookup map of field ID -> field definition
  const fieldMap = {};
  for (const field of fields) {
    fieldMap[field.id] = field;
    // Also map subfields for composite fields (name, address)
    if (field.subfields) {
      for (const subfield of field.subfields) {
        fieldMap[subfield.id] = { ...subfield, parentLabel: field.label };
      }
    }
  }

  // Map each form data field to its label
  for (const [fieldId, value] of Object.entries(formData)) {
    const fieldDef = fieldMap[fieldId];

    if (fieldDef) {
      // Handle composite fields (objects with subfield values)
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const subfieldLabels = {};
        for (const [subId, subValue] of Object.entries(value)) {
          const subFieldDef = fieldMap[subId];
          if (subFieldDef) {
            subfieldLabels[subFieldDef.label || subId] = subValue;
          } else {
            // Fallback: extract readable name from subfield ID
            const readableName = subId.split('.').pop().replace(/_/g, ' ');
            subfieldLabels[readableName] = subValue;
          }
        }
        labeled[fieldDef.label || fieldId] = {
          type: fieldDef.type || 'composite',
          value: subfieldLabels
        };
      } else {
        labeled[fieldDef.label || fieldId] = {
          type: fieldDef.type || 'text',
          value: value
        };
      }
    } else {
      // Field not found in config - store with cleaned up ID as label
      const readableLabel = fieldId.replace(/^field_\d+\.?/, '').replace(/_/g, ' ') || fieldId;
      labeled[readableLabel] = {
        type: 'unknown',
        value: value
      };
    }
  }

  return labeled;
}

/**
 * Save form submission to DynamoDB
 * Includes Lead Workspace fields for pipeline processing
 */
async function saveFormSubmission(submissionId, formId, formData, config, priority = 'normal', sessionId = null, conversationId = null) {
  if (!FORM_SUBMISSIONS_TABLE) {
    console.warn('⚠️ FORM_SUBMISSIONS_TABLE not configured, skipping DynamoDB save');
    return;
  }

  // Get form config for field labels
  const formConfig = config.conversational_forms?.[formId] || {};
  const labeledData = buildLabeledFormData(formData, formConfig);

  // Transform to human-readable labels for contact extraction
  const transformedFormData = transformFormDataToLabels(formData, formConfig);

  // Extract canonical contact info (same data sent to Bubble)
  const { contact: canonicalContact, comments } = extractCanonicalContact(transformedFormData);

  // Build display-ready form data (flat key-value for Lead Workspace drawer)
  const formDataDisplay = buildFormDataDisplay(formData, formConfig);

  const now = new Date().toISOString();
  const tenantId = config.tenant_id || 'unknown';

  const params = {
    TableName: FORM_SUBMISSIONS_TABLE,
    Item: {
      // Core submission fields
      submission_id: submissionId,
      form_id: formId,
      form_title: formConfig.title || formId,
      tenant_id: tenantId,
      tenant_hash: config.tenant_hash || 'unknown',
      session_id: sessionId || 'unknown',
      conversation_id: conversationId || sessionId || 'unknown',
      form_data: formData,           // Raw data with field IDs
      form_data_labeled: labeledData, // Human-readable labeled data (nested)
      form_data_display: formDataDisplay, // Flat key-value for UI display
      priority: priority,
      submitted_at: now,
      timestamp: now,                // Required for tenant-timestamp-index GSI
      status: 'pending_fulfillment',

      // Canonical contact info (matches Bubble webhook schema)
      contact: canonicalContact,     // { first_name, last_name, email, phone, address, ... }
      comments: comments,            // Extracted comments/notes text

      // Lead Workspace fields for pipeline processing
      pipeline_status: 'new',
      tenant_pipeline_key: `${tenantId}#new`,  // Composite key for tenant-pipeline-index GSI
      internal_notes: '',
      processed_by: null,
      contacted_at: null,
      archived_at: null,
      updated_at: now
    }
  };

  try {
    await dynamodb.send(new PutCommand(params));
    console.log(`✅ Form saved to DynamoDB with pipeline_status: new, priority: ${priority}`);
  } catch (error) {
    console.error('Error saving to DynamoDB:', error);
    // Don't fail the submission if DynamoDB fails
  }
}

/**
 * Route form to appropriate fulfillment channel
 * Ported from Master's form_handler.py:330-393
 */
async function routeFulfillment(formId, formData, config, submissionId, priority = 'normal', sessionId = null, conversationId = null) {
  const results = [];

  const formConfig = config.conversational_forms?.[formId] || {};

  // Build human-readable form data for email notifications
  const displayData = buildFormDataDisplay(formData, formConfig);

  // Extract canonical contact from raw form data for template variable substitution.
  // This runs the same pipeline as sendConfirmationEmail — field ID → label → contact fields.
  const transformedForEmail = transformFormDataToLabels(formData, formConfig);
  const { contact: emailContact } = extractCanonicalContact(transformedForEmail);

  // Check form-specific fulfillment configuration (formConfig already defined above)
  const fulfillment = formConfig.fulfillment || config.default_fulfillment || {};

  // Validate fulfillment configuration
  if (!fulfillment || Object.keys(fulfillment).length === 0) {
    console.warn(`⚠️ No fulfillment configured for form ${formId}, using default email`);
  }

  // Process advanced fulfillment types first (Lambda, S3)
  const fulfillmentType = fulfillment.type;

  if (fulfillmentType === 'lambda') {
    // Invoke another Lambda function
    const functionName = fulfillment.function;

    if (!functionName) {
      console.error('❌ Lambda fulfillment configured but no function name provided');
      results.push({ channel: 'lambda', status: 'failed', error: 'Missing function name' });
    } else {
      const action = fulfillment.action || 'process_form';

      try {
        const payload = JSON.stringify({
          action: action,
          form_type: formId,
          submission_id: submissionId,
          responses: formData,
          tenant_id: config.tenant_id,
          priority: priority
        });

        await lambdaClient.send(new InvokeCommand({
          FunctionName: functionName,
          InvocationType: 'Event', // Async
          Payload: payload
        }));

        results.push({ channel: 'lambda', function: functionName, status: 'invoked' });
        console.log(`✅ Lambda function invoked: ${functionName}`);
      } catch (error) {
        console.error('❌ Lambda invocation failed:', error);
        results.push({ channel: 'lambda', status: 'failed', error: error.message });
      }
    }
  }

  if (fulfillmentType === 's3') {
    // Store in S3
    const bucket = fulfillment.bucket;

    if (!bucket) {
      console.error('❌ S3 fulfillment configured but no bucket provided');
      results.push({ channel: 's3', status: 'failed', error: 'Missing bucket name' });
    } else {
      const key = `submissions/${config.tenant_id}/${formId}/${submissionId}.json`;

      try {
        await s3Client.send(new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: JSON.stringify(formData),
          ContentType: 'application/json'
        }));

        results.push({ channel: 's3', location: `s3://${bucket}/${key}`, status: 'stored' });
        console.log(`✅ Form stored in S3: ${key}`);
      } catch (error) {
        console.error('❌ S3 storage failed:', error);
        results.push({ channel: 's3', status: 'failed', error: error.message });
      }
    }
  }

  // Email fulfillment (notification to organization)
  // Support: recipient_user_ids (Phase 4), legacy notifications.internal, and fulfillment.email_to
  const notifications = formConfig.notifications || {};
  const internalNotif = notifications.internal || {};

  let emailRecipients;
  let emailUserIdMap = {}; // email → userId for audit records

  if (internalNotif.enabled === false) {
    // Internal notifications explicitly disabled — skip all resolution
    emailRecipients = null;
  } else if (internalNotif.recipient_employee_ids?.length > 0) {
    // Registry-based resolution (preferred)
    try {
      const tenantId = config.tenant_id || '';
      const resolvedPairs = await resolveEmailsFromEmployeeIds(tenantId, internalNotif.recipient_employee_ids);
      const quietHoursFallback = await resolveQuietHoursFallbackFromEmployeeIds(tenantId, internalNotif.recipient_employee_ids);
      const resolvedEmails = resolvedPairs.map(p => p.email);
      emailRecipients = [...new Set([...resolvedEmails, ...quietHoursFallback])];
      for (const pair of resolvedPairs) {
        emailUserIdMap[pair.email] = pair.employeeId;
      }
      if (internalNotif.enabled && internalNotif.recipients?.length > 0) {
        emailRecipients = [...new Set([...emailRecipients, ...internalNotif.recipients])];
      }
    } catch (e) {
      console.error('[routeFulfillment] Error resolving emails from employee_ids:', e);
      emailRecipients = internalNotif.recipients || [];
    }
  } else if (internalNotif.recipient_user_ids?.length > 0) {
    // Phase 4 legacy: resolve user_ids to emails via Clerk API
    try {
      const resolvedPairs = await resolveEmailsFromUserIds(internalNotif.recipient_user_ids);
      const quietHoursFallback = await resolveQuietHoursFallbackEmails(internalNotif.recipient_user_ids);
      const resolvedEmails = resolvedPairs.map(p => p.email);
      emailRecipients = [...new Set([...resolvedEmails, ...quietHoursFallback])];
      // Build userId lookup for audit
      for (const pair of resolvedPairs) {
        emailUserIdMap[pair.email] = pair.userId;
      }
      // Merge with legacy recipients for hybrid configs
      if (internalNotif.enabled && internalNotif.recipients?.length > 0) {
        emailRecipients = [...new Set([...emailRecipients, ...internalNotif.recipients])];
      }
    } catch (e) {
      console.error('[routeFulfillment] Unexpected error resolving emails from user_ids:', e);
      emailRecipients = internalNotif.recipients || [];
    }
  } else {
    // Legacy path: direct email strings
    emailRecipients = fulfillment.email_to ||
      (internalNotif.enabled && internalNotif.channels?.email !== false ? internalNotif.recipients : null);
  }

  if (emailRecipients && emailRecipients.length > 0) {
    try {
      // Use notification config for subject/body if available
      const emailSubject = internalNotif.subject || null;
      const emailBodyTemplate = internalNotif.body_template || null;
      const sesResponse = await sendFormEmail(emailRecipients, formId, displayData, config, priority, submissionId, emailSubject, emailBodyTemplate, sessionId, emailContact);
      results.push({ channel: 'email', status: 'sent', recipients: Array.isArray(emailRecipients) ? emailRecipients.length : 1 });

      // Audit: write one record per recipient to picasso-notification-sends
      const sesMessageId = sesResponse?.MessageId || 'unknown';
      const nowIso = new Date().toISOString();
      const recipientList = Array.isArray(emailRecipients) ? emailRecipients : [emailRecipients];
      for (const recipient of recipientList) {
        try {
          await dynamodb.send(new PutCommand({
            TableName: NOTIFICATION_SENDS_TABLE,
            Item: {
              pk: `TENANT#${config.tenant_id || 'unknown'}`,
              sk: `${nowIso.slice(0, 10)}#email#${sesMessageId}#${recipient}`,
              channel: 'email',
              recipient: recipient,
              user_id: emailUserIdMap[recipient] || '',
              submission_id: submissionId || 'unknown',
              form_id: formId || 'unknown',
              template: 'internal_notification',
              status: 'sent',
              error: '',
              message_id: sesMessageId,
              timestamp: nowIso,
              ttl: Math.floor(Date.now() / 1000) + 90 * 24 * 3600,
            }
          }));
        } catch (ddbErr) {
          console.error('Failed to write notification send record to DynamoDB:', ddbErr);
        }
      }
    } catch (error) {
      console.error('Email fulfillment failed:', error);
      results.push({ channel: 'email', status: 'failed', error: error.message });

      // Audit: record failure for each intended recipient
      const nowIso = new Date().toISOString();
      const recipientList = Array.isArray(emailRecipients) ? emailRecipients : [emailRecipients];
      for (const recipient of recipientList) {
        try {
          await dynamodb.send(new PutCommand({
            TableName: NOTIFICATION_SENDS_TABLE,
            Item: {
              pk: `TENANT#${config.tenant_id || 'unknown'}`,
              sk: `${nowIso.slice(0, 10)}#email#failed-${submissionId || 'unknown'}-${recipient}`,
              channel: 'email',
              recipient: recipient,
              user_id: emailUserIdMap[recipient] || '',
              submission_id: submissionId || 'unknown',
              form_id: formId || 'unknown',
              template: 'internal_notification',
              status: 'failed',
              error: error.message || 'unknown error',
              message_id: '',
              timestamp: nowIso,
              ttl: Math.floor(Date.now() / 1000) + 90 * 24 * 3600,
            }
          }));
        } catch (ddbErr) {
          console.error('Failed to write notification failure record to DynamoDB:', ddbErr);
        }
      }
    }
  }

  // SMS fulfillment via SMS_Sender Lambda (Telnyx)
  // Supports: recipient_user_ids (Phase 4), notifications.internal.sms_recipients, legacy fulfillment.sms_to
  let smsRecipients;
  if (internalNotif.recipient_employee_ids?.length > 0 && internalNotif.enabled && internalNotif.channels?.sms) {
    // Registry-based SMS resolution (preferred)
    try {
      const tenantId = config.tenant_id || '';
      const resolvedPhones = await resolvePhonesFromEmployeeIds(tenantId, internalNotif.recipient_employee_ids);
      smsRecipients = [...new Set([...resolvedPhones, ...(internalNotif.sms_recipients || [])])];
    } catch (e) {
      console.error('[routeFulfillment] Error resolving phones from employee_ids:', e);
      smsRecipients = internalNotif.sms_recipients || [];
    }
  } else if (internalNotif.recipient_user_ids?.length > 0 && internalNotif.enabled && internalNotif.channels?.sms) {
    // Phase 4 legacy: resolve user_ids to phones (skips users not opted in, no phone, or in quiet hours)
    try {
      const resolvedPhones = await resolvePhonesFromUserIds(internalNotif.recipient_user_ids);
      smsRecipients = [...new Set([...resolvedPhones, ...(internalNotif.sms_recipients || [])])];
    } catch (e) {
      console.error('[routeFulfillment] Unexpected error resolving phones from user_ids:', e);
      smsRecipients = internalNotif.sms_recipients || [];
    }
  } else {
    smsRecipients = internalNotif.enabled && internalNotif.channels?.sms
      ? (internalNotif.sms_recipients || [])
      : [];
  }
  const legacySmsTo = fulfillment.sms_to ? [fulfillment.sms_to] : [];
  const allSmsRecipients = [...new Set([...smsRecipients, ...legacySmsTo])];

  if (allSmsRecipients.length > 0) {
    const usage = await getMonthlySMSUsage(config.tenant_id);
    if (usage >= SMS_MONTHLY_LIMIT) {
      console.warn(`⚠️ SMS monthly limit reached for tenant ${config.tenant_id}: ${usage}/${SMS_MONTHLY_LIMIT}`);
      results.push({ channel: 'sms', status: 'skipped', reason: 'monthly_limit_reached', usage, limit: SMS_MONTHLY_LIMIT });
    } else {
      // Build SMS body from template or default
      const safeName = `${sanitizeForSMS(formData.first_name, 30)} ${sanitizeForSMS(formData.last_name, 30)}`.trim();
      const safeEmail = sanitizeForSMS(formData.email, 50);
      const defaultSmsBody = `New ${sanitizeForSMS(formId, 30)} submission. Name: ${safeName}, Email: ${safeEmail}`;
      const smsBody = internalNotif.sms_template
        ? renderTemplate(internalNotif.sms_template, {
            first_name: formData.first_name || '',
            last_name: formData.last_name || '',
            email: formData.email || '',
            organization_name: config.chat_title || config.tenant_id || '',
          })
        : defaultSmsBody;

      let smsCount = 0;
      for (const phone of allSmsRecipients) {
        try {
          await lambdaClient.send(new InvokeCommand({
            FunctionName: process.env.SMS_SENDER_FUNCTION || 'SMS_Sender',
            InvocationType: 'Event',
            Payload: JSON.stringify({
              to: phone,
              body: smsBody,
              tenantId: config.tenant_id,
              formId,
              submissionId,
              sessionId,
              type: 'internal',
              sendType: 'internal',
              fromNumber: config.sms_settings?.from_number || '',
            })
          }));
          smsCount++;
          results.push({ channel: 'sms', status: 'invoked', recipient: phone });
        } catch (error) {
          console.error('SMS invocation failed:', error);
          results.push({ channel: 'sms', status: 'failed', error: error.message });
        }
      }
      // Increment usage by actual count of SMS invoked
      for (let i = 0; i < smsCount; i++) {
        await incrementSMSUsage(config.tenant_id);
      }
    }
  }

  // Webhook fulfillment (e.g., to Google Sheets via Zapier)
  if (fulfillment.webhook_url) {
    try {
      await sendToWebhook(fulfillment.webhook_url, formId, formData, priority, submissionId);
      results.push({ channel: 'webhook', status: 'sent' });
    } catch (error) {
      console.error('Webhook fulfillment failed:', error);
      results.push({ channel: 'webhook', status: 'failed', error: error.message });
    }
  }

  return results;
}

/**
 * Get monthly SMS usage for tenant
 * Ported from Master's form_handler.py:395-413
 */
async function getMonthlySMSUsage(tenantId) {
  if (!SMS_USAGE_TABLE) {
    console.warn('⚠️ SMS_USAGE_TABLE not configured, returning 0');
    return 0;
  }

  if (!tenantId) {
    console.error('❌ getMonthlySMSUsage called without tenantId');
    return 0;
  }

  try {
    const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format

    const result = await dynamodb.send(new GetCommand({
      TableName: SMS_USAGE_TABLE,
      Key: {
        tenant_id: tenantId,
        month: currentMonth
      }
    }));

    if (result.Item) {
      return result.Item.count || 0;
    }
    return 0;
  } catch (error) {
    console.error('❌ Error getting SMS usage:', error);
    return 0; // Fail safe - allow SMS if we can't check usage
  }
}

/**
 * Increment SMS usage counter
 * Ported from Master's form_handler.py:415-436
 */
async function incrementSMSUsage(tenantId) {
  if (!SMS_USAGE_TABLE) {
    console.warn('⚠️ SMS_USAGE_TABLE not configured, skipping increment');
    return;
  }

  if (!tenantId) {
    console.error('❌ incrementSMSUsage called without tenantId');
    return;
  }

  try {
    const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format

    await dynamodb.send(new UpdateCommand({
      TableName: SMS_USAGE_TABLE,
      Key: {
        tenant_id: tenantId,
        month: currentMonth
      },
      UpdateExpression: 'SET #count = if_not_exists(#count, :zero) + :inc, updated_at = :now',
      ExpressionAttributeNames: {
        '#count': 'count'
      },
      ExpressionAttributeValues: {
        ':inc': 1,
        ':zero': 0,
        ':now': new Date().toISOString()
      }
    }));

    console.log(`✅ SMS usage incremented for tenant ${tenantId} in ${currentMonth}`);
  } catch (error) {
    console.error('Error incrementing SMS usage:', error);
    // Don't fail the SMS send if we can't track usage
  }
}

/**
 * Find whether the contact gave SMS consent in this form submission.
 * Checks form_data values against the form field config to find the consent answer.
 *
 * Supports:
 * - phone_with_consent composite: subfield with sms_consent: true or type 'select' inside the composite
 * - Standalone select field with label containing "consent" or "text message"
 * - Explicit sms_consent_given field in form_data
 *
 * @returns {boolean} true if consent was explicitly given ("yes")
 */
function findSmsConsent(formData, formConfig) {
  const fields = formConfig?.fields || [];

  for (const field of fields) {
    // phone_with_consent composite: check subfield values
    if (field.type === 'phone_with_consent' && field.subfields) {
      for (const sub of field.subfields) {
        if (sub.type === 'select' || sub.sms_consent) {
          // Composite subfield value is stored as field.id (the composite value object)
          // or as a flat key matching the subfield id
          const compositeVal = formData[field.id];
          if (typeof compositeVal === 'object' && compositeVal !== null) {
            if (String(compositeVal[sub.id] || '').toLowerCase() === 'yes') return true;
          }
          // Also check flat key (some submissions flatten composite values)
          if (String(formData[sub.id] || '').toLowerCase() === 'yes') return true;
        }
      }
    }

    // Standalone select field for consent (label-based detection)
    if (field.type === 'select') {
      const label = (field.label || '').toLowerCase();
      if (label.includes('consent') || label.includes('text message') || label.includes('sms')) {
        if (String(formData[field.id] || '').toLowerCase() === 'yes') return true;
      }
    }
  }

  // Fallback: check for explicit sms_consent_given in form data
  if (String(formData.sms_consent_given || '').toLowerCase() === 'yes') return true;

  return false;
}

/**
 * Find the SMS consent disclosure text from the form config.
 * This is the exact wording shown to the contact — stored in the consent record for TCPA compliance.
 */
function findConsentDisclosure(formConfig) {
  const fields = formConfig?.fields || [];

  for (const field of fields) {
    if (field.type === 'phone_with_consent' && field.subfields) {
      const consentSub = field.subfields.find(sf => sf.type === 'select' || sf.sms_consent);
      if (consentSub) {
        return consentSub.disclosure || consentSub.prompt || consentSub.label || '';
      }
    }
    if (field.type === 'select') {
      const label = (field.label || '').toLowerCase();
      if (label.includes('consent') || label.includes('text message') || label.includes('sms')) {
        return field.prompt || field.label || '';
      }
    }
  }

  return '';
}

/**
 * Write a transactional consent record when a form submission includes a phone number.
 * Consent is per-org, per-phone, per-type. The consent_type is part of the sort key
 * so marketing consent can be added later without a migration.
 *
 * @param {string} tenantId - Tenant identifier
 * @param {string} phoneE164 - Phone in E.164 format (+1XXXXXXXXXX)
 * @param {string} formId - Form that captured consent
 * @param {string} submissionId - Submission that captured consent
 * @param {string} consentLanguage - Disclosure text shown to contact
 */
async function writeConsentRecord(tenantId, phoneE164, formId, submissionId, consentLanguage = '') {
  if (!SMS_CONSENT_TABLE || !tenantId || !phoneE164) {
    return;
  }

  // Normalize to E.164 if not already
  const phone = phoneE164.startsWith('+') ? phoneE164 : `+1${phoneE164.replace(/\D/g, '')}`;
  if (!/^\+\d{10,15}$/.test(phone)) {
    console.warn(`⚠️ Cannot write consent: invalid phone format ${phoneE164}`);
    return;
  }

  const now = new Date().toISOString();

  try {
    // Conditional put: only create if no record exists yet (don't overwrite existing consent)
    await dynamodb.send(new PutCommand({
      TableName: SMS_CONSENT_TABLE,
      Item: {
        pk: `TENANT#${tenantId}`,
        sk: `CONSENT#transactional#${phone}`,
        phone_e164: phone,
        consent_given: true,
        consent_timestamp: now,
        consent_method: 'web_form',
        consent_language: consentLanguage,
        consent_type: 'transactional',
        opted_out_at: null,
        opt_out_source: null,
        form_id: formId || 'unknown',
        submission_id: submissionId || 'unknown',
        created_at: now,
        updated_at: now,
      },
      ConditionExpression: 'attribute_not_exists(pk)',
    }));
    console.log(`✅ Consent record created for ${phone} (tenant: ${tenantId})`);
  } catch (error) {
    if (error.name === 'ConditionalCheckFailedException') {
      // Record already exists — don't overwrite (consent is immutable once given)
      console.log(`ℹ️ Consent record already exists for ${phone} (tenant: ${tenantId})`);
    } else {
      console.error('Failed to write consent record:', error);
    }
  }
}

/**
 * Send form data via email
 */
async function sendFormEmail(toEmail, formId, formData, config, priority = 'normal', submissionId = 'unknown', customSubject = null, bodyTemplate = null, sessionId = null, contact = null) {
  // Build human-readable form data for template substitution
  const formDataText = Object.entries(formData)
    .map(([key, value]) => `${key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}: ${value}`)
    .join('\n');

  // Build template variables from extracted contact (same pipeline as sendConfirmationEmail)
  const capitalize = (s) => (s && typeof s === 'string') ? s.charAt(0).toUpperCase() + s.slice(1) : '';
  const formConfig = config.conversational_forms?.[formId] || {};
  const formTitle = formConfig.form_title || formConfig.title || formId;
  const templateVars = {
    first_name: capitalize(contact?.first_name),
    last_name: capitalize(contact?.last_name),
    email: contact?.email || '',
    phone: contact?.phone || '',
    organization_name: config.chat_title || config.tenant_id || '',
    form_title: formTitle,
    form_data: formDataText,
    form_type: formId,
  };

  // Use renderTemplate() for consistent variable substitution with cleanup
  let subject;
  if (customSubject) {
    subject = renderTemplate(customSubject, templateVars);
  } else {
    const formConfig = config.conversational_forms?.[formId] || {};
    const formLabel = formConfig.form_title || formConfig.title || formId;
    subject = `New ${formLabel} Submission`;
  }

  let htmlBody;
  if (bodyTemplate) {
    const bodyText = renderTemplate(bodyTemplate, templateVars);
    htmlBody = `<div style="font-family: Arial, sans-serif; line-height: 1.6;">${bodyText.replace(/\n/g, '<br>')}</div>`;
  } else {
    // Default HTML email body
    htmlBody = `
      <h2>New ${formLabel} Submission</h2>
      <p>A new form has been submitted through the chat widget.</p>
      <h3>Form Data:</h3>
      <table border="1" cellpadding="5" cellspacing="0">
    `;

    for (const [key, value] of Object.entries(formData)) {
      htmlBody += `
        <tr>
          <td><strong>${key}:</strong></td>
          <td>${value}</td>
        </tr>
      `;
    }

    htmlBody += `
      </table>
      <p style="color: #666; font-size: 12px; margin-top: 16px;">Submitted: ${new Date().toLocaleString('en-US', { timeZone: 'America/Chicago', month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })} CT</p>
    `;
  }

  const params = {
    Source: getSESFromEmail(),
    Destination: {
      ToAddresses: Array.isArray(toEmail) ? toEmail : [toEmail]
    },
    Message: {
      Subject: { Data: subject },
      Body: {
        Text: { Data: formDataText },
        Html: { Data: htmlBody }
      }
    },
    ConfigurationSetName: 'picasso-emails',
    Tags: [
      { Name: 'tenant_id', Value: String(config.tenant_id || 'unknown').slice(0, 256) },
      { Name: 'form_type', Value: String(formId || 'unknown').slice(0, 256) },
      { Name: 'submission_id', Value: String(submissionId || 'unknown').slice(0, 256) },
      { Name: 'session_id', Value: String(sessionId || '').slice(0, 256) },
      { Name: 'email_type', Value: 'internal_notification' }
    ]
  };

  const sesResponse = await sesClient.send(new SendEmailCommand(params));
  console.log(`✅ Form email sent to ${toEmail}`);
  return sesResponse;
}

/**
 * Send confirmation email to the applicant who submitted the form.
 *
 * Reads per-form `notifications.applicant_confirmation` from tenant config
 * (editable via portal Templates tab). Falls back to a generic template when
 * no per-form config exists.
 *
 * Template variables: {first_name}, {last_name}, {email}, {phone},
 *   {organization_name}, {form_data}
 */
async function sendConfirmationEmail(userEmail, formId, formData, config, submissionId = 'unknown', sessionId = null) {
  const tenantName = config.chat_title || 'Organization';
  const formConfig = config.conversational_forms?.[formId] || {};
  const confirmationConfig = formConfig.notifications?.applicant_confirmation;

  // Build template variables from submitted form data.
  // Form data uses field IDs (e.g. field_1774282600263) — use the same extraction
  // pipeline as DynamoDB save to get human-readable labels and contact info.
  const displayData = buildFormDataDisplay(formData, formConfig);
  const formDataText = Object.entries(displayData)
    .map(([label, value]) => `${label}: ${value}`)
    .join('\n');

  const transformedData = transformFormDataToLabels(formData, formConfig);
  const { contact } = extractCanonicalContact(transformedData);

  // Capitalize name fields — frontend should do this too, but ensure it server-side
  const capitalize = (s) => (s && typeof s === 'string') ? s.charAt(0).toUpperCase() + s.slice(1) : '';

  const formTitle = formConfig.form_title || formConfig.title || formId;
  const templateVars = {
    first_name: capitalize(contact?.first_name),
    last_name: capitalize(contact?.last_name),
    email: userEmail,
    phone: contact?.phone || '',
    organization_name: tenantName,
    form_title: formTitle,
    form_data: formDataText,
  };

  let subject, bodyText;

  if (confirmationConfig?.subject && confirmationConfig?.body_template) {
    // Use per-form template from tenant config
    subject = renderTemplate(confirmationConfig.subject, templateVars);
    bodyText = renderTemplate(confirmationConfig.body_template, templateVars);
  } else {
    // Fallback to generic template
    subject = `Thank you for your ${formConfig.title || formId} submission`;
    bodyText = `Dear Applicant,\n\nWe have received your ${formConfig.title || formId} submission to ${tenantName}.\n\nOur team will review your information and get back to you soon.\n\nIf you have any questions, please don't hesitate to contact us.\n\nBest regards,\n${tenantName} Team`;
  }

  // Build HTML email — branded if use_tenant_branding is enabled and branding config exists
  let htmlBody;
  const safeOrgName = escapeHtml(tenantName);

  if (confirmationConfig?.use_tenant_branding !== false && config.branding) {
    const b = config.branding;
    const primaryColor = sanitizeCssValue(b.primary_color) || '#50C878';
    const fontFamily = sanitizeCssValue(b.font_family) || 'Arial, sans-serif';
    const logoUrl = isValidHttpsUrl(b.logo_url) ? b.logo_url : '';

    htmlBody = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0; padding:0; background-color:#f4f4f4; font-family:${fontFamily}, Arial, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4; padding:20px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden;">
        <tr><td style="background-color:${primaryColor}; padding:24px; text-align:center;">
          ${logoUrl ? `<img src="${logoUrl}" alt="${safeOrgName}" style="max-height:48px; max-width:200px;">` : ''}
          <div style="color:#ffffff; font-size:18px; font-weight:600; margin-top:${logoUrl ? '12' : '0'}px;">${safeOrgName}</div>
        </td></tr>
        <tr><td style="padding:32px 24px; color:#333333; font-size:15px; line-height:1.6;">
          ${bodyText.replace(/\n/g, '<br>')}
        </td></tr>
        <tr><td style="padding:16px 24px; border-top:1px solid #eeeeee; color:#999999; font-size:12px; text-align:center;">
          &copy; ${new Date().getFullYear()} ${safeOrgName}
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`;
  } else {
    // No branding — bare HTML (existing behavior)
    htmlBody = `<div>${bodyText.replace(/\n/g, '<br>')}</div>`;
  }

  const params = {
    Source: getSESFromEmail(),
    Destination: {
      ToAddresses: [userEmail]
    },
    Message: {
      Subject: { Data: subject },
      Body: {
        Text: { Data: bodyText },
        Html: { Data: htmlBody }
      }
    },
    ConfigurationSetName: 'picasso-emails',
    Tags: [
      { Name: 'tenant_id', Value: String(config.tenant_id || 'unknown').slice(0, 256) },
      { Name: 'form_type', Value: String(formId || 'unknown').slice(0, 256) },
      { Name: 'submission_id', Value: String(submissionId || 'unknown').slice(0, 256) },
      { Name: 'session_id', Value: String(sessionId || '').slice(0, 256) },
      { Name: 'email_type', Value: 'applicant_confirmation' }
    ]
  };

  try {
    await sesClient.send(new SendEmailCommand(params));
    console.log(`✅ Confirmation email sent to ${userEmail} using ${confirmationConfig ? 'per-form template' : 'fallback template'}`);
  } catch (error) {
    console.error('Failed to send confirmation email:', error);
    // Don't fail the submission if confirmation email fails
  }
}

/**
 * Replace {variable} placeholders in a template string.
 * Empty or anonymous values are treated as blank — any surrounding whitespace
 * is collapsed so "Hi {first_name}," becomes "Hi," (not "Hi ,").
 */
function renderTemplate(template, variables) {
  const ANONYMOUS = ['anonymous', 'unknown', 'n/a', 'none', ''];
  let result = template;
  for (const [key, value] of Object.entries(variables)) {
    const strVal = String(value ?? '').trim();
    const resolved = ANONYMOUS.includes(strVal.toLowerCase()) ? '' : strVal;
    // Replace placeholder, then collapse any extra space left behind
    result = result.replace(new RegExp(`[^\\S\\n]*\\{${key}\\}`, 'g'), resolved ? ` ${resolved}` : '');
  }
  // Clean up artifacts from removed placeholders
  result = result.replace(/ {2,}/g, ' ');    // collapse double spaces
  result = result.replace(/,\s*!/g, '!');    // ",!" → "!"
  result = result.replace(/,\s*\./g, '.');   // ",." → "."
  result = result.replace(/,\s*,/g, ',');    // ",," → ","
  result = result.replace(/\(\s*\)/g, '');   // "()" → remove empty parens
  result = result.replace(/^\s*,\s*/g, '');  // leading ", " → remove
  result = result.replace(/[^\S\n]+$/gm, ''); // trailing spaces/tabs per line (preserves blank lines)
  return result.trim();
}

/**
 * Send form data via SMS
 */
async function sendFormSMS(phoneNumber, formId, formData, priority = 'normal') {
  const priorityEmoji = priority === 'high' ? '🚨 ' : priority === 'low' ? '📋 ' : '📝 ';
  // Sanitize user-provided data before SMS interpolation to prevent injection
  const safeName = `${sanitizeForSMS(formData.first_name, 30)} ${sanitizeForSMS(formData.last_name, 30)}`.trim();
  const safeEmail = sanitizeForSMS(formData.email, 50);
  const safeFormId = sanitizeForSMS(formId, 30);
  const message = `${priorityEmoji}New ${safeFormId} submission. Name: ${safeName}, Email: ${safeEmail}`;

  const params = {
    Message: message.substring(0, 160), // SMS character limit
    PhoneNumber: phoneNumber
  };

  await snsClient.send(new PublishCommand(params));
  console.log(`✅ SMS notification sent to ${phoneNumber} (priority: ${priority})`);
}

/**
 * Send form data to webhook
 */
async function sendToWebhook(webhookUrl, formId, formData, priority = 'normal', submissionId = null) {
  const https = require('https');
  const url = new URL(webhookUrl);

  const payload = JSON.stringify({
    form_id: formId,
    submission_id: submissionId,
    priority: priority,
    timestamp: new Date().toISOString(),
    data: formData
  });

  return new Promise((resolve, reject) => {
    const options = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': payload.length
      }
    };

    const req = https.request(options, (res) => {
      console.log(`✅ Webhook response: ${res.statusCode}`);
      resolve(res.statusCode);
    });

    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

/**
 * Transform form data field IDs to human-readable labels
 * Converts keys like "field_1761666576305.first_name" to "first_name"
 *
 * @param {Object} formData - Raw form responses with field IDs as keys
 * @param {Object} formConfig - Form configuration with field definitions
 * @returns {Object} Transformed form data with human-readable keys
 */
function transformFormDataToLabels(formData, formConfig) {
  if (!formData || !formConfig?.fields) {
    return formData || {};
  }

  const transformed = {};
  const fields = formConfig.fields || [];

  // Build a lookup map for field IDs to labels
  const fieldMap = {};

  for (const field of fields) {
    const fieldId = field.id;
    // Normalize label to snake_case
    const normalizedLabel = normalizeLabel(field.label);

    // For composite fields with subfields (name, address)
    if (field.subfields && Array.isArray(field.subfields)) {
      for (const subfield of field.subfields) {
        // Subfield ID format: "field_123.first_name"
        const subfieldKey = subfield.id;
        // Use just the subfield label (e.g., "first_name", "last_name")
        const subfieldLabel = normalizeLabel(subfield.label);
        fieldMap[subfieldKey] = subfieldLabel;
      }
    } else {
      // Simple field - use the field's label
      fieldMap[fieldId] = normalizedLabel;
    }
  }

  // Transform the form data keys
  for (const [key, value] of Object.entries(formData)) {
    // Flatten composite parent fields (e.g., "field_1769160911801" with nested subfields)
    // Subfield keys like "field_1769160911801.first_name" live inside the object —
    // promote them to top-level so the mapping logic below can resolve them.
    if (/^field_\d+$/.test(key) && typeof value === 'object' && value !== null) {
      for (const [subKey, subVal] of Object.entries(value)) {
        if (fieldMap[subKey]) {
          transformed[fieldMap[subKey]] = subVal;
        } else {
          const parts = subKey.split('.');
          if (parts.length > 1) {
            transformed[parts[parts.length - 1]] = subVal;
          }
        }
      }
      continue;
    }

    if (fieldMap[key]) {
      // Use the mapped label
      transformed[fieldMap[key]] = value;
    } else {
      // Fallback: try to extract a readable name from the key
      // e.g., "field_123.first_name" -> "first_name"
      const parts = key.split('.');
      if (parts.length > 1) {
        transformed[parts[parts.length - 1]] = value;
      } else {
        // Keep original key if no mapping found
        transformed[key] = value;
      }
    }
  }

  return transformed;
}

/**
 * Normalize a label to snake_case
 * "Caregiver's Phone Number" -> "caregivers_phone_number"
 */
function normalizeLabel(label) {
  if (!label) return '';
  return label
    .toLowerCase()
    .replace(/['']/g, '')           // Remove apostrophes
    .replace(/[^a-z0-9]+/g, '_')    // Replace non-alphanumeric with underscore
    .replace(/^_+|_+$/g, '')        // Trim leading/trailing underscores
    .replace(/_+/g, '_');           // Collapse multiple underscores
}

/**
 * Build display-ready form data with human-readable labels and flat values
 * Used for Lead Workspace drawer display
 *
 * @param {Object} formData - Raw form data with field IDs
 * @param {Object} formConfig - Form configuration with field definitions
 * @returns {Object} { "Name": "John Doe", "Email": "john@example.com", ... }
 */
function buildFormDataDisplay(formData, formConfig) {
  if (!formData) return {};

  const display = {};
  const fields = formConfig?.fields || [];

  // Build lookup map: fieldId -> { label, type, subfields }
  const fieldMap = {};
  for (const field of fields) {
    fieldMap[field.id] = field;
  }

  // Process each form data entry
  for (const [fieldId, value] of Object.entries(formData)) {
    const fieldDef = fieldMap[fieldId];

    if (!fieldDef) {
      // Unknown field - try to make key readable
      const readableKey = fieldId.split('.').pop().replace(/_/g, ' ');
      display[toTitleCase(readableKey)] = String(value || '');
      continue;
    }

    const label = fieldDef.label || fieldId;

    // Handle composite fields (name, address)
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      // Flatten composite field to single string
      if (fieldDef.type === 'name') {
        // Name field: combine first/middle/last
        const parts = [];
        for (const subfield of (fieldDef.subfields || [])) {
          const subValue = value[subfield.id];
          if (subValue) parts.push(subValue);
        }
        display[label] = parts.join(' ');
      } else if (fieldDef.type === 'address') {
        // Address field: format as single line
        const addr = [];
        const subfieldOrder = ['street', 'unit', 'city', 'state', 'zip', 'zip_code'];
        for (const key of subfieldOrder) {
          // Find subfield by checking if key is in subfield id
          for (const subfield of (fieldDef.subfields || [])) {
            if (subfield.id.toLowerCase().includes(key) && value[subfield.id]) {
              addr.push(value[subfield.id]);
              break;
            }
          }
        }
        display[label] = addr.join(' ');
      } else {
        // Generic composite: join values
        const parts = Object.values(value).filter(v => v);
        display[label] = parts.join(' ');
      }
    } else {
      // Simple field
      display[label] = String(value || '');
    }
  }

  return display;
}

/**
 * Convert string to Title Case
 * "first name" -> "First Name"
 */
function toTitleCase(str) {
  if (!str) return '';
  return str.replace(/\b\w/g, c => c.toUpperCase());
}

// ============================================================================
// EMAIL DETAILS BUILDER - Human-readable formatting for Bubble email templates
// ============================================================================

/**
 * Acronyms to preserve in title case (kept uppercase)
 */
const PRESERVED_ACRONYMS = ['ZIP', 'ID', 'URL', 'DOB', 'SSN', 'EIN', 'PO', 'APT', 'LLC', 'INC'];

/**
 * Contact field patterns for ordering (processed first in emails)
 */
const CONTACT_FIELD_PATTERNS = {
  name: ['name', 'first_name', 'last_name', 'full_name', 'firstname', 'lastname'],
  email: ['email', 'e_mail', 'email_address'],
  phone: ['phone', 'mobile', 'cell', 'telephone', 'tel'],
  address: ['street', 'address', 'city', 'state', 'zip', 'postal', 'country', 'apt', 'unit', 'suite']
};

/**
 * Humanize a snake_case or kebab-case key into Title Case
 * Preserves common acronyms like ZIP, ID, URL, DOB
 *
 * @param {string} key - The field key (e.g., "zip_code", "user_id")
 * @returns {string} Human-readable label (e.g., "ZIP Code", "User ID")
 */
function humanizeKey(key) {
  if (!key) return '';

  // Split on underscores, hyphens, or camelCase boundaries
  const words = key
    .replace(/([a-z])([A-Z])/g, '$1 $2')  // camelCase to spaces
    .replace(/[_-]+/g, ' ')                // underscores/hyphens to spaces
    .trim()
    .split(/\s+/);

  return words.map(word => {
    const upperWord = word.toUpperCase();
    // Check if it's a preserved acronym
    if (PRESERVED_ACRONYMS.includes(upperWord)) {
      return upperWord;
    }
    // Title case: first letter upper, rest lower
    return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
  }).join(' ');
}

/**
 * Format a value for display in plain text email
 * Handles booleans, arrays, objects, and long strings
 *
 * @param {any} value - The value to format
 * @param {number} maxLength - Maximum length before truncation (default 2000)
 * @returns {string|null} Formatted string, or null if value should be omitted
 */
function formatValue(value, maxLength = 2000) {
  // Omit null, undefined, or empty strings
  if (value === null || value === undefined || value === '') {
    return null;
  }

  // Boolean: Yes/No
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }

  // Array: join with comma
  if (Array.isArray(value)) {
    const joined = value
      .filter(v => v !== null && v !== undefined && v !== '')
      .join(', ');
    return joined || null;
  }

  // Object: stringify as single line
  if (typeof value === 'object') {
    try {
      const str = JSON.stringify(value);
      if (str.length > maxLength) {
        return str.substring(0, maxLength) + '...';
      }
      return str;
    } catch (e) {
      return '[Object]';
    }
  }

  // String or number: convert to string and truncate if needed
  let str = String(value);
  if (str.length > maxLength) {
    return str.substring(0, maxLength) + '...';
  }
  return str;
}

/**
 * Get the priority score for field ordering
 * Lower score = appears earlier in email
 *
 * @param {string} key - Field key
 * @returns {number} Priority score (0-999)
 */
function getFieldPriority(key) {
  const lowerKey = key.toLowerCase();

  // Name fields: highest priority (0-9)
  for (const pattern of CONTACT_FIELD_PATTERNS.name) {
    if (lowerKey === pattern || lowerKey.includes(pattern)) {
      // Exact matches get lower scores
      if (lowerKey === 'first_name' || lowerKey === 'firstname') return 0;
      if (lowerKey === 'last_name' || lowerKey === 'lastname') return 1;
      if (lowerKey === 'name' || lowerKey === 'full_name') return 2;
      return 9;
    }
  }

  // Email fields: second priority (10-19)
  for (const pattern of CONTACT_FIELD_PATTERNS.email) {
    if (lowerKey === pattern || lowerKey.includes(pattern)) {
      return 10;
    }
  }

  // Phone fields: third priority (20-29)
  for (const pattern of CONTACT_FIELD_PATTERNS.phone) {
    if (lowerKey === pattern || lowerKey.includes(pattern)) {
      return 20;
    }
  }

  // Address fields: fourth priority (30-49)
  for (const pattern of CONTACT_FIELD_PATTERNS.address) {
    if (lowerKey === pattern || lowerKey.includes(pattern)) {
      // Order: street, city, state, zip, country
      if (lowerKey.includes('street') || lowerKey.includes('address')) return 30;
      if (lowerKey.includes('apt') || lowerKey.includes('unit') || lowerKey.includes('suite')) return 31;
      if (lowerKey.includes('city')) return 32;
      if (lowerKey.includes('state')) return 33;
      if (lowerKey.includes('zip') || lowerKey.includes('postal')) return 34;
      if (lowerKey.includes('country')) return 35;
      return 39;
    }
  }

  // All other fields: alphabetical (100+)
  return 100;
}

module.exports = {
  handleFormMode,
  validateFormField,
  submitForm
};
