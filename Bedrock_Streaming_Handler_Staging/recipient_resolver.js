/**
 * Registry-based Recipient Resolver for Bedrock Streaming Handler
 * Resolves recipient_employee_ids (UUID) to email/phone from the employee registry.
 * Replaces Clerk-based resolution for registry-managed recipients.
 */

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand } = require('@aws-sdk/lib-dynamodb');

const EMPLOYEE_TABLE = process.env.EMPLOYEE_REGISTRY_TABLE ||
  `picasso-employee-registry-v2-${process.env.ENVIRONMENT || 'staging'}`;

const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * Fetch a single employee record by tenantId + employeeId.
 * Returns the Item or null on miss/error. Never throws.
 */
async function getEmployee(tenantId, employeeId) {
  try {
    const result = await docClient.send(new GetCommand({
      TableName: EMPLOYEE_TABLE,
      Key: { tenantId, employeeId },
    }));
    return result.Item || null;
  } catch (e) {
    console.warn(`[recipient_resolver] Failed to fetch employee ${employeeId} for tenant ${tenantId}: ${e.message}`);
    return null;
  }
}

/**
 * Check if current time falls within an employee's configured quiet hours.
 *
 * Cases:
 * 1. start < end (e.g. 09:00–17:00): quiet if current >= start AND current < end
 * 2. start > end (overnight, e.g. 19:00–07:00): quiet if current >= start OR current < end
 * 3. start === end: always quiet (full 24-hour window)
 * 4. Invalid timezone: log warning, treat as NOT in quiet hours
 */
function isInQuietHours(notifPrefs) {
  const qh = notifPrefs?.sms_quiet_hours;
  if (!qh?.enabled || !qh.start || !qh.end || !qh.timezone) return false;

  let nowInTz;
  try {
    nowInTz = new Date().toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      timeZone: qh.timezone,
    }); // "HH:MM"
  } catch (e) {
    console.warn(`[recipient_resolver] Invalid timezone "${qh.timezone}" for quiet hours — allowing send`);
    return false;
  }

  const current = nowInTz.slice(0, 5);
  const { start, end } = qh;

  if (start === end) return true; // 24-hour quiet window

  if (start < end) {
    return current >= start && current < end;
  } else {
    // Overnight (e.g. 19:00–07:00)
    return current >= start || current < end;
  }
}

/**
 * Resolve employee IDs to { email, employeeId } pairs.
 * Only returns active employees with a non-empty email address.
 * Never throws.
 */
async function resolveEmailsFromEmployeeIds(tenantId, employeeIds) {
  const results = [];
  await Promise.allSettled(employeeIds.map(async (employeeId) => {
    try {
      const employee = await getEmployee(tenantId, employeeId);
      if (!employee) return;
      if (employee.status !== 'active') return;
      const email = (employee.email || '').toLowerCase().trim();
      if (!email) return;
      results.push({ email, employeeId });
    } catch (e) {
      console.warn(`[recipient_resolver] Failed to resolve email for employee ${employeeId}: ${e.message}`);
    }
  }));
  return results;
}

/**
 * Resolve employee IDs to phone number strings.
 * Skips: missing employees, inactive employees, no phone, not SMS opted in, in quiet hours.
 * Never throws.
 */
async function resolvePhonesFromEmployeeIds(tenantId, employeeIds) {
  const results = [];
  await Promise.allSettled(employeeIds.map(async (employeeId) => {
    try {
      const employee = await getEmployee(tenantId, employeeId);
      if (!employee) return;
      if (employee.status !== 'active') return;
      const notifPrefs = employee.notificationPrefs || {};
      if (!notifPrefs.sms) return; // not SMS opted in
      const phone = notifPrefs.phone || employee.phone || null;
      if (!phone) return; // no phone number
      if (isInQuietHours(notifPrefs)) {
        console.log(`[recipient_resolver] Employee ${employeeId} in quiet hours — skipping SMS`);
        return;
      }
      results.push(phone);
    } catch (e) {
      console.warn(`[recipient_resolver] Failed to resolve phone for employee ${employeeId}: ${e.message}`);
    }
  }));
  return results;
}

/**
 * Returns emails for employees who are in quiet hours and have fallback_to_email enabled.
 * These employees should receive email instead of SMS during quiet hours.
 * Never throws.
 */
async function resolveQuietHoursFallbackFromEmployeeIds(tenantId, employeeIds) {
  const fallbackEmails = [];
  await Promise.allSettled(employeeIds.map(async (employeeId) => {
    try {
      const employee = await getEmployee(tenantId, employeeId);
      if (!employee) return;
      if (employee.status !== 'active') return;
      const notifPrefs = employee.notificationPrefs || {};
      if (!notifPrefs.sms) return; // not SMS opted in, not relevant for fallback
      if (isInQuietHours(notifPrefs) && notifPrefs.sms_quiet_hours?.fallback_to_email) {
        const email = (employee.email || '').toLowerCase().trim();
        if (email) fallbackEmails.push(email);
      }
    } catch (e) {
      console.warn(`[recipient_resolver] quiet-hours fallback failed for employee ${employeeId}: ${e.message}`);
    }
  }));
  return fallbackEmails;
}

module.exports = {
  resolveEmailsFromEmployeeIds,
  resolvePhonesFromEmployeeIds,
  resolveQuietHoursFallbackFromEmployeeIds,
};
