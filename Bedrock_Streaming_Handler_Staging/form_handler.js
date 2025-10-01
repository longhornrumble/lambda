/**
 * Form Handler for Conversational Forms
 *
 * Handles form field validation and submission without calling Bedrock.
 * Provides local validation and fulfillment routing.
 */

const AWS = require('aws-sdk');

// Initialize AWS services
const ses = new AWS.SES({ region: process.env.AWS_REGION || 'us-east-1' });
const sns = new AWS.SNS({ region: process.env.AWS_REGION || 'us-east-1' });
const dynamodb = new AWS.DynamoDB.DocumentClient();

// Form submission table
const FORM_SUBMISSIONS_TABLE = process.env.FORM_SUBMISSIONS_TABLE || 'picasso-form-submissions';

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
    // Save to DynamoDB
    const submissionId = `${formId}_${Date.now()}`;
    await saveFormSubmission(submissionId, formId, formData, config);

    // Route to appropriate fulfillment channel
    const fulfillmentResult = await routeFulfillment(formId, formData, config);

    // Send confirmation email if configured
    if (formData.email && config.send_confirmation_email !== false) {
      await sendConfirmationEmail(formData.email, formId, config);
    }

    return {
      type: 'form_complete',
      status: 'success',
      message: 'Thank you! Your application has been submitted successfully. You will receive a confirmation email shortly.',
      submissionId: submissionId,
      fulfillment: fulfillmentResult
    };

  } catch (error) {
    console.error('Form submission error:', error);
    return {
      type: 'form_error',
      status: 'error',
      message: 'There was an error submitting your form. Please try again or contact support.',
      error: error.message
    };
  }
}

/**
 * Save form submission to DynamoDB
 */
async function saveFormSubmission(submissionId, formId, formData, config) {
  const params = {
    TableName: FORM_SUBMISSIONS_TABLE,
    Item: {
      submission_id: submissionId,
      form_id: formId,
      tenant_id: config.tenant_id || 'unknown',
      form_data: formData,
      submitted_at: new Date().toISOString(),
      status: 'pending_fulfillment'
    }
  };

  try {
    await dynamodb.put(params).promise();
    console.log('✅ Form saved to DynamoDB');
  } catch (error) {
    console.error('Error saving to DynamoDB:', error);
    // Don't fail the submission if DynamoDB fails
  }
}

/**
 * Route form to appropriate fulfillment channel
 */
async function routeFulfillment(formId, formData, config) {
  const results = [];

  // Check form-specific fulfillment configuration
  const formConfig = config.conversational_forms?.[formId] || {};
  const fulfillment = formConfig.fulfillment || config.default_fulfillment || {};

  // Email fulfillment
  if (fulfillment.email_to) {
    try {
      await sendFormEmail(fulfillment.email_to, formId, formData, config);
      results.push({ channel: 'email', status: 'sent' });
    } catch (error) {
      console.error('Email fulfillment failed:', error);
      results.push({ channel: 'email', status: 'failed', error: error.message });
    }
  }

  // SMS fulfillment (if configured)
  if (fulfillment.sms_to) {
    try {
      await sendFormSMS(fulfillment.sms_to, formId, formData);
      results.push({ channel: 'sms', status: 'sent' });
    } catch (error) {
      console.error('SMS fulfillment failed:', error);
      results.push({ channel: 'sms', status: 'failed', error: error.message });
    }
  }

  // Webhook fulfillment (e.g., to Google Sheets via Zapier)
  if (fulfillment.webhook_url) {
    try {
      await sendToWebhook(fulfillment.webhook_url, formId, formData);
      results.push({ channel: 'webhook', status: 'sent' });
    } catch (error) {
      console.error('Webhook fulfillment failed:', error);
      results.push({ channel: 'webhook', status: 'failed', error: error.message });
    }
  }

  return results;
}

/**
 * Send form data via email
 */
async function sendFormEmail(toEmail, formId, formData, config) {
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

  await ses.sendEmail(params).promise();
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
    await ses.sendEmail(params).promise();
    console.log(`✅ Confirmation email sent to ${userEmail}`);
  } catch (error) {
    console.error('Failed to send confirmation email:', error);
    // Don't fail the submission if confirmation email fails
  }
}

/**
 * Send form data via SMS
 */
async function sendFormSMS(phoneNumber, formId, formData) {
  const message = `New ${formId} submission received. Name: ${formData.first_name} ${formData.last_name}, Email: ${formData.email}`;

  const params = {
    Message: message.substring(0, 160), // SMS character limit
    PhoneNumber: phoneNumber
  };

  await sns.publish(params).promise();
  console.log(`✅ SMS notification sent to ${phoneNumber}`);
}

/**
 * Send form data to webhook
 */
async function sendToWebhook(webhookUrl, formId, formData) {
  const https = require('https');
  const url = new URL(webhookUrl);

  const payload = JSON.stringify({
    form_id: formId,
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