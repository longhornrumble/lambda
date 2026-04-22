/**
 * KB Proposal Applier Lambda
 *
 * Route: POST /proposals/{proposalId}/apply
 * Body:  { "tenantId": "MYR384719", "approvedItemIds": ["item-001", "item-002"] }
 *
 * Runtime: Node.js 20.x
 * Invocation: Lambda Function URL (no CORS needed — UI proxies via its own API layer if ever
 * wired there, but the v1 invocation path is manual curl + Lambda InvokeCommand).
 *
 * Auth: Clerk JWT (Authorization: Bearer …) using the `picasso-config` template — same
 * pattern as Picasso_Config_Manager. Users must have `super_admin` role or have `tenantId`
 * in their `tenants` claim.
 *
 * Env:
 *   CLERK_JWKS_URL          — optional override for Clerk JWKS endpoint
 *   CONFIG_BUCKET           — defaults to myrecruiter-picasso
 *   KB_BUCKET               — defaults to kbragdocs
 *   DUB_API_KEY             — required for dub.upsert operations
 *   ENFORCE_AUTH            — "false" to log-only auth (dev). Defaults to enforcement.
 */

import { authenticateRequest } from './auth.mjs';
import { applyProposal } from './applier.mjs';

const ENFORCE_AUTH = process.env.ENFORCE_AUTH !== 'false';
const TENANT_ID_REGEX = /^[A-Za-z0-9_-]{1,50}$/;
const PROPOSAL_ID_REGEX = /^[A-Za-z0-9_-]{1,100}$/;

function jsonResponse(statusCode, body, extraHeaders = {}) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
    body: JSON.stringify(body),
  };
}

export const handler = async (event) => {
  console.log('Event:', JSON.stringify({
    method: event.httpMethod || event.requestContext?.http?.method,
    path: event.path || event.rawPath,
    hasBody: !!event.body,
  }));

  const httpMethod = event.httpMethod || event.requestContext?.http?.method;
  const path = event.path || event.rawPath || event.requestContext?.http?.path || '';

  if (httpMethod === 'OPTIONS') {
    return jsonResponse(200, { message: 'OK' });
  }

  if (httpMethod === 'GET' && path === '/health') {
    return jsonResponse(200, {
      status: 'healthy',
      service: 'kb-proposal-applier',
      timestamp: new Date().toISOString(),
    });
  }

  // POST /proposals/{proposalId}/apply
  const applyMatch = path.match(/^\/proposals\/([^/]+)\/apply$/);
  if (httpMethod === 'POST' && applyMatch) {
    const proposalId = applyMatch[1];

    if (!PROPOSAL_ID_REGEX.test(proposalId)) {
      return jsonResponse(400, {
        error: 'Bad Request',
        message: 'proposalId must be alphanumeric (hyphens/underscores allowed), max 100 characters',
      });
    }

    const auth = await authenticateRequest(event);
    if (!auth.success) {
      console.warn(`Authentication failed: ${auth.error}`);
      if (ENFORCE_AUTH) {
        return jsonResponse(401, { error: 'Unauthorized', message: auth.error });
      }
      console.warn('PERMISSIVE MODE: allowing unauthenticated request');
    }

    let body;
    try {
      body = JSON.parse(event.body || '{}');
    } catch {
      return jsonResponse(400, { error: 'Bad Request', message: 'Invalid JSON body' });
    }

    const { tenantId, approvedItemIds } = body;
    if (!tenantId || !TENANT_ID_REGEX.test(tenantId)) {
      return jsonResponse(400, {
        error: 'Bad Request',
        message: 'tenantId required, alphanumeric (hyphens/underscores allowed), max 50 chars',
      });
    }
    if (!Array.isArray(approvedItemIds)) {
      return jsonResponse(400, {
        error: 'Bad Request',
        message: 'approvedItemIds must be an array (empty array is allowed but will apply nothing)',
      });
    }

    if (auth.success && auth.role !== 'super_admin') {
      const userTenants = auth.tenants || [];
      if (!userTenants.includes(tenantId)) {
        console.warn(`User ${auth.email} attempted to apply proposals for tenant ${tenantId} without permission`);
        if (ENFORCE_AUTH) {
          return jsonResponse(403, {
            error: 'Forbidden',
            message: 'You do not have access to this tenant',
          });
        }
      }
    }

    try {
      const result = await applyProposal({
        tenantId,
        proposalId,
        approvedItemIds,
        dubApiKey: process.env.DUB_API_KEY,
      });

      const statusCode = result.status === 'applied' ? 200 : 207; // 207 Multi-Status for partial
      return jsonResponse(statusCode, result);
    } catch (error) {
      console.error('Applier error:', error);
      return jsonResponse(500, {
        error: 'Internal Server Error',
        message: error.message,
      });
    }
  }

  return jsonResponse(404, {
    error: 'Not Found',
    message: `Route not found: ${httpMethod} ${path}`,
  });
};
