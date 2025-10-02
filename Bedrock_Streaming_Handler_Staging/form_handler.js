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
const SMS_MONTHLY_LIMIT = parseInt(process.env.SMS_MONTHLY_LIMIT || '100', 10);

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
    action
  } = body;

  // Handle different form actions
  if (action === 'validate_field') {
    return await validateFormField(field_id, field_value, tenantConfig);
  }

  if (action === 'submit_form') {
    return await submitForm(form_id, form_data, tenantConfig);
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
      if (value && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
        errors.push('Please enter a valid email address');
      }
      break;

    case 'phone':
      if (value && !/^[\d\s\-\(\)\+]+$/.test(value)) {
        errors.push('Please enter a valid phone number');
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
 * @returns {Object} Submission result
 */
async function submitForm(formId, formData, config) {
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
      await saveFormSubmission(submissionId, formId, formData, config, priority);
    } catch (dbError) {
      console.error('❌ DynamoDB save failed:', dbError);
      // Continue with fulfillment even if DynamoDB save fails
    }

    // Route to appropriate fulfillment channel
    const fulfillmentResult = await routeFulfillment(formId, formData, config, submissionId, priority);

    // Send confirmation email if configured (non-blocking)
    if (formData.email && config.send_confirmation_email !== false) {
      sendConfirmationEmail(formData.email, formId, config).catch(err => {
        console.error('❌ Confirmation email failed:', err.message);
      });
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
 * Save form submission to DynamoDB
 */
async function saveFormSubmission(submissionId, formId, formData, config, priority = 'normal') {
  if (!FORM_SUBMISSIONS_TABLE) {
    console.warn('⚠️ FORM_SUBMISSIONS_TABLE not configured, skipping DynamoDB save');
    return;
  }

  const params = {
    TableName: FORM_SUBMISSIONS_TABLE,
    Item: {
      submission_id: submissionId,
      form_id: formId,
      tenant_id: config.tenant_id || 'unknown',
      form_data: formData,
      priority: priority,
      submitted_at: new Date().toISOString(),
      status: 'pending_fulfillment'
    }
  };

  try {
    await dynamodb.send(new PutCommand(params));
    console.log(`✅ Form saved to DynamoDB with priority: ${priority}`);
  } catch (error) {
    console.error('Error saving to DynamoDB:', error);
    // Don't fail the submission if DynamoDB fails
  }
}

/**
 * Route form to appropriate fulfillment channel
 * Ported from Master's form_handler.py:330-393
 */
async function routeFulfillment(formId, formData, config, submissionId, priority = 'normal') {
  const results = [];

  // Check form-specific fulfillment configuration
  const formConfig = config.conversational_forms?.[formId] || {};
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
  if (fulfillment.email_to) {
    try {
      await sendFormEmail(fulfillment.email_to, formId, formData, config, priority);
      results.push({ channel: 'email', status: 'sent' });
    } catch (error) {
      console.error('Email fulfillment failed:', error);
      results.push({ channel: 'email', status: 'failed', error: error.message });
    }
  }

  // SMS fulfillment (notification to organization) with rate limiting
  if (fulfillment.sms_to) {
    try {
      // Check SMS rate limit
      const usage = await getMonthlySMSUsage(config.tenant_id);
      if (usage >= SMS_MONTHLY_LIMIT) {
        console.warn(`⚠️ SMS monthly limit reached for tenant ${config.tenant_id}: ${usage}/${SMS_MONTHLY_LIMIT}`);
        results.push({ channel: 'sms', status: 'skipped', reason: 'monthly_limit_reached', usage: usage, limit: SMS_MONTHLY_LIMIT });
      } else {
        await sendFormSMS(fulfillment.sms_to, formId, formData, priority);
        await incrementSMSUsage(config.tenant_id);
        results.push({ channel: 'sms', status: 'sent', usage: usage + 1, limit: SMS_MONTHLY_LIMIT });
      }
    } catch (error) {
      console.error('SMS fulfillment failed:', error);
      results.push({ channel: 'sms', status: 'failed', error: error.message });
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
 * Send form data via email
 */
async function sendFormEmail(toEmail, formId, formData, config, priority = 'normal') {
  const subject = `New Form Submission: ${formId}`;

  let htmlBody = `
    <h2>New ${formId} Submission</h2>
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
    <p><strong>Priority:</strong> ${priority.toUpperCase()}</p>
    <p>Submitted at: ${new Date().toISOString()}</p>
    <p>Tenant: ${config.tenant_id || 'unknown'}</p>
  `;

  const params = {
    Source: process.env.SES_FROM_EMAIL || 'noreply@picasso.ai',
    Destination: {
      ToAddresses: Array.isArray(toEmail) ? toEmail : [toEmail]
    },
    Message: {
      Subject: { Data: subject },
      Body: {
        Html: { Data: htmlBody }
      }
    }
  };

  await sesClient.send(new SendEmailCommand(params));
  console.log(`✅ Form email sent to ${toEmail}`);
}

/**
 * Send confirmation email to user
 */
async function sendConfirmationEmail(userEmail, formId, config) {
  const tenantName = config.chat_title || 'Organization';
  const subject = `Thank you for your ${formId} submission`;

  const htmlBody = `
    <h2>Thank you for your submission!</h2>
    <p>Dear Applicant,</p>
    <p>We have received your ${formId} submission to ${tenantName}.</p>
    <p>Our team will review your information and get back to you soon.</p>
    <p>If you have any questions, please don't hesitate to contact us.</p>
    <br>
    <p>Best regards,<br>${tenantName} Team</p>
  `;

  const params = {
    Source: process.env.SES_FROM_EMAIL || 'noreply@picasso.ai',
    Destination: {
      ToAddresses: [userEmail]
    },
    Message: {
      Subject: { Data: subject },
      Body: {
        Html: { Data: htmlBody }
      }
    }
  };

  try {
    await sesClient.send(new SendEmailCommand(params));
    console.log(`✅ Confirmation email sent to ${userEmail}`);
  } catch (error) {
    console.error('Failed to send confirmation email:', error);
    // Don't fail the submission if confirmation email fails
  }
}

/**
 * Send form data via SMS
 */
async function sendFormSMS(phoneNumber, formId, formData, priority = 'normal') {
  const priorityEmoji = priority === 'high' ? '🚨 ' : priority === 'low' ? '📋 ' : '📝 ';
  const message = `${priorityEmoji}New ${formId} submission. Name: ${formData.first_name} ${formData.last_name}, Email: ${formData.email}`;

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

module.exports = {
  handleFormMode,
  validateFormField,
  submitForm
};
