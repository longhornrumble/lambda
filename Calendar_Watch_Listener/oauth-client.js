'use strict';

/**
 * oauth-client.js — Per-tenant Google OAuth2 client factory.
 *
 * Phase 2a (B2 plumbing only — handler does not call this yet; Phase 2b wires it in).
 *
 * Canonical secret path per `subphase_b_oauth_provisioning_runbook_2026-05-25.md`:
 *   picasso/scheduling/oauth/{tenantId}/{coordinatorId}
 *
 * Expected SecretString JSON shape (canonical schema per runbook line 244 & 198):
 *   { client_id, client_secret, refresh_token, scopes, coordinator_email }
 *
 * IAM grant is already in place on Calendar_Watch_Listener-exec-staging (verified
 * 2026-05-26): secretsmanager:GetSecretValue + DescribeSecret on
 *   arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/*
 *
 * Process-level cache: OAuth2Client instances are reused across warm Lambda
 * invocations keyed by secret-path. Stale tokens self-refresh via
 * google-auth-library; no manual refresh logic required here.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { OAuth2Client } = require('google-auth-library');

const OAUTH_SECRET_PATH_PREFIX = process.env.OAUTH_SECRET_PATH_PREFIX || 'picasso/scheduling/oauth';

const secrets = new SecretsManagerClient({});

const _clientCache = new Map();

function buildSecretPath(tenantId, coordinatorId) {
  if (!tenantId || !coordinatorId) {
    throw new Error('tenantId and coordinatorId are required');
  }
  return `${OAUTH_SECRET_PATH_PREFIX}/${tenantId}/${coordinatorId}`;
}

async function fetchOAuthSecret(secretPath) {
  const result = await secrets.send(new GetSecretValueCommand({ SecretId: secretPath }));
  if (!result.SecretString) {
    throw new Error(`OAuth secret has no SecretString: ${secretPath}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(result.SecretString);
  } catch (err) {
    throw new Error(`OAuth secret is not valid JSON: ${secretPath}`);
  }
  for (const required of ['client_id', 'client_secret', 'refresh_token']) {
    if (!parsed[required]) {
      throw new Error(`OAuth secret missing required field "${required}": ${secretPath}`);
    }
  }
  return parsed;
}

async function getOAuthClient({ tenantId, coordinatorId }) {
  const secretPath = buildSecretPath(tenantId, coordinatorId);
  if (_clientCache.has(secretPath)) {
    return _clientCache.get(secretPath);
  }
  const secret = await fetchOAuthSecret(secretPath);
  const client = new OAuth2Client({
    clientId: secret.client_id,
    clientSecret: secret.client_secret,
  });
  client.setCredentials({ refresh_token: secret.refresh_token });
  _clientCache.set(secretPath, client);
  return client;
}

function _resetCacheForTests() {
  _clientCache.clear();
}

module.exports = {
  getOAuthClient,
  buildSecretPath,
  fetchOAuthSecret,
  _resetCacheForTests,
};
