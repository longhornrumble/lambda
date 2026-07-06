/**
 * promoteDispatch.mjs
 *
 * Fires the gated `promote-tenant-config` GitHub Actions workflow that copies a
 * tenant's STAGING config to the PROD bucket. This Lambda deliberately does NOT
 * write prod itself — a staging-account resource must never write prod stores
 * (account isolation). It only *dispatches* the workflow; the ephemeral CI
 * runner assumes the prod write-role and does the copy, fully audited.
 *
 * "Simple button" flow: a single execute dispatch (dry_run=false, no
 * expected_etag). The workflow treats a missing expected_etag as a
 * single-dispatch promote — safe under the born-in-staging Fork-A model (no
 * direct prod edits) + the workflow's tenant-scoped concurrency (prod can't move
 * between the operator's confirm and this write). The Config Builder's confirm
 * dialog is the deliberate gate.
 *
 * Env:
 *   GITHUB_PROMOTE_TOKEN  (required) fine-grained PAT, Actions:write on the repo
 *   GITHUB_PROMOTE_REPO   (optional) default "longhornrumble/picasso"
 */

const GH_API = 'https://api.github.com';
const WORKFLOW_FILE = 'promote-tenant-config.yml';

/**
 * Dispatch the promote workflow for a tenant.
 * @param {string} tenantId
 * @returns {Promise<{success: true, tenant_id: string, message: string, runs_url: string}>}
 * @throws {Error & {statusCode:number}} on missing token / GitHub API failure
 */
export async function dispatchPromoteWorkflow(tenantId) {
  const token = process.env.GITHUB_PROMOTE_TOKEN;
  const repo = process.env.GITHUB_PROMOTE_REPO || 'longhornrumble/picasso';

  if (!token) {
    const err = new Error('Promotion is not configured (GITHUB_PROMOTE_TOKEN missing).');
    err.statusCode = 503;
    throw err;
  }

  let res;
  try {
    res = await fetch(
      `${GH_API}/repos/${repo}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': '2022-11-28',
          'Content-Type': 'application/json',
          'User-Agent': 'picasso-config-manager',
        },
        // dry_run/tenant_id are strings — the workflow compares
        // github.event.inputs.* as strings. expected_etag omitted =
        // single-dispatch (see module header).
        body: JSON.stringify({
          ref: 'main',
          inputs: { tenant_id: tenantId, dry_run: 'false' },
        }),
      }
    );
  } catch (e) {
    const err = new Error(`Could not reach GitHub to dispatch promotion: ${e.message}`);
    err.statusCode = 502;
    throw err;
  }

  // Successful workflow dispatch returns 204 No Content.
  if (res.status !== 204) {
    const detail = (await res.text().catch(() => '')).slice(0, 300);
    // 401/403 = bad/misscoped token; 404 = workflow/ref missing; 422 = bad inputs.
    const err = new Error(
      `GitHub rejected the promotion dispatch (HTTP ${res.status})${detail ? `: ${detail}` : ''}`
    );
    err.statusCode = res.status === 404 || res.status === 422 ? 500 : 502;
    throw err;
  }

  return {
    success: true,
    tenant_id: tenantId,
    message:
      'Promotion dispatched. The gated workflow is copying the staging config to production.',
    // The dispatch API returns no run id; link to the workflow's runs list.
    runs_url: `https://github.com/${repo}/actions/workflows/${WORKFLOW_FILE}`,
  };
}
