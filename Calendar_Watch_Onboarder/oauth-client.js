'use strict';

/**
 * oauth-client.js — Per-tenant Google OAuth2 client factory (Onboarder copy).
 *
 * Canonical secret path per `subphase_b_oauth_provisioning_runbook_2026-05-25.md`:
 *   picasso/scheduling/oauth/{tenantId}/{coordinatorId}
 *
 * Expected SecretString JSON shape (canonical schema per runbook line 244 & 198):
 *   { client_id, client_secret, refresh_token, scopes, coordinator_email }
 *
 * IAM grant is on THIS Lambda's own role `Calendar_Watch_Onboarder-exec-staging`
 * (dedicated per CLAUDE.md never-share-roles rule): secretsmanager:GetSecretValue
 * + DescribeSecret, scoped per-tenant to picasso/scheduling/oauth/{tenant}/*.
 *
 * NO process-level cache (intentional divergence from the Listener's copy).
 * The Onboarder is invoked rarely (once per coordinator onboarding), so a warm
 * container is the exception, not the rule — caching the OAuth2Client buys
 * almost nothing and creates a real footgun: after an operator rotates the
 * OAuth secret, a cached client keeps the stale refresh_token and fails with
 * `invalid_client` until a forced cold-start (observed 2026-05-27). Always
 * fetch fresh.
 *
 * Error messages deliberately omit the secret path so CloudWatch logs don't
 * become a cross-tenant existence oracle.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { OAuth2Client } = require('google-auth-library');

const OAUTH_SECRET_PATH_PREFIX = process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';

const secrets = new SecretsManagerClient({});

function buildSecretPath(tenantId, coordinatorId) {
  if (!tenantId || !coordinatorId) {
    throw new Error('tenantId and coordinatorId are required');
  }
  return `${OAUTH_SECRET_PATH_PREFIX}/${tenantId}/${coordinatorId}`;
}

async function fetchOAuthSecret(secretPath) {
  const result = await secrets.send(new GetSecretValueCommand({ SecretId: secretPath }));
  if (!result.SecretString) {
    throw new Error('OAuth secret has no SecretString for the requested coordinator');
  }
  let parsed;
  try {
    parsed = JSON.parse(result.SecretString);
  } catch (err) {
    throw new Error('OAuth secret is not valid JSON for the requested coordinator');
  }
  for (const required of ['client_id', 'client_secret', 'refresh_token']) {
    if (typeof parsed[required] !== 'string' || parsed[required].length === 0) {
      throw new Error(`OAuth secret missing/empty required field "${required}" for the requested coordinator`);
    }
  }
  return parsed;
}

async function getOAuthClient({ tenantId, coordinatorId }) {
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  const secret = await fetchOAuthSecret(secretPath);
  const client = new OAuth2Client({
    clientId: secret.client_id,
    clientSecret: secret.client_secret,
  });
  client.setCredentials({ refresh_token: secret.refresh_token });
  return client;
}

module.exports = {
  getOAuthClient,
  buildSecretPath,
  fetchOAuthSecret,
};
