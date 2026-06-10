'use strict';

/**
 * sms-consent.js — read-only SMS consent lookup for the G7b reschedule_link SMS gate.
 *
 * The §E3 selectChannels TCPA gate is PURE — the caller passes in the consent record. This
 * is that read for the BCH reschedule_link call-site: a GetItem on the SAME picasso-sms-consent
 * key the SMS_Sender / consent.js writer use (pk=TENANT#{tenantId}, sk=CONSENT#transactional#{E.164}),
 * so the pre-filter and the authoritative SMS_Sender gate agree.
 *
 * FAIL-SAFE: a missing record, a bad phone, or any DDB error returns null → selectChannels
 * reads "no consent" → SMS suppressed → the email floor still sends. A consent-store problem
 * can never block the reschedule link nor (wrongly) enable an SMS.
 */

const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { sdkConfig } = require('./aws-client-config');
const { toE164 } = require('../shared/scheduling/consent');

const ddb = new DynamoDBClient(sdkConfig());
const SMS_CONSENT_TABLE = process.env.SMS_CONSENT_TABLE || 'picasso-sms-consent';

/**
 * @returns {Promise<{consent_given: boolean, opted_out_at?: string} | null>}
 *   The minimal shape selectChannels' consentValid needs; null on any miss/error (fail-safe).
 */
async function readSmsConsent(tenantId, phoneRaw, deps = {}) {
  const client = deps.ddb || ddb;
  const phoneE164 = toE164(phoneRaw);
  if (!tenantId || !phoneE164) return null;
  try {
    const res = await client.send(
      new GetItemCommand({
        TableName: SMS_CONSENT_TABLE,
        Key: {
          pk: { S: `TENANT#${tenantId}` },
          sk: { S: `CONSENT#transactional#${phoneE164}` },
        },
      })
    );
    const it = res.Item;
    if (!it) return null;
    return {
      consent_given: it.consent_given ? it.consent_given.BOOL === true : false,
      opted_out_at: it.opted_out_at && typeof it.opted_out_at.S === 'string' ? it.opted_out_at.S : undefined,
    };
  } catch {
    // Fail-safe: never throw — no consent reachable ⇒ SMS suppressed, email floor stands.
    return null;
  }
}

module.exports = { readSmsConsent };
