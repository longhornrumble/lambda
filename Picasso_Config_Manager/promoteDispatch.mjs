/**
 * promoteDispatch.mjs
 *
 * Fires the gated `promote-tenant-config` GitHub Actions workflow that copies a
 * tenant's STAGING config to the PROD bucket, and lets the Config Builder poll
 * that run's outcome so the UI can show Promoting → ✓ / ✗.
 *
 * This Lambda never writes prod itself — a staging-account resource must not
 * write prod stores (account isolation). It only dispatches the workflow (which
 * does the prod copy via OIDC) and reads run status back.
 *
 * Outcome polling: workflow_dispatch returns no run id, so we capture the
 * newest run id BEFORE dispatching (the "baseline"). The status endpoint then
 * returns the newest run with id > baseline — i.e. the run we just started, not
 * a stale earlier run. This correlation is what keeps the UI from showing a
 * previous run's success.
 *
 * Env:
 *   GITHUB_PROMOTE_TOKEN  (required) fine-grained PAT, Actions:read+write on the repo
 *   GITHUB_PROMOTE_REPO   (optional) default "longhornrumble/picasso"
 */

const GH_API = 'https://api.github.com';
const WORKFLOW_FILE = 'promote-tenant-config.yml';

function statusError(message, statusCode) {
  const err = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function ghHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'User-Agent': 'picasso-config-manager',
  };
}

function config() {
  const token = process.env.GITHUB_PROMOTE_TOKEN;
  if (!token) throw statusError('Promotion is not configured (GITHUB_PROMOTE_TOKEN missing).', 503);
  return { token, repo: process.env.GITHUB_PROMOTE_REPO || 'longhornrumble/picasso' };
}

/** Newest run id for the promote workflow (0 if none / on error). */
async function newestRunId(repo, token) {
  try {
    const res = await fetch(
      `${GH_API}/repos/${repo}/actions/workflows/${WORKFLOW_FILE}/runs?per_page=1`,
      { headers: ghHeaders(token) }
    );
    if (!res.ok) return 0;
    const data = await res.json();
    return data.workflow_runs?.[0]?.id ?? 0;
  } catch {
    return 0;
  }
}

/**
 * Dispatch the promote workflow for a tenant.
 * @returns {Promise<{success:true, tenant_id:string, message:string, baseline:number}>}
 *   `baseline` = newest run id before dispatch; pass it to getPromoteStatus.
 */
export async function dispatchPromoteWorkflow(tenantId) {
  const { token, repo } = config();

  // Correlation baseline BEFORE dispatch (see module header).
  const baseline = await newestRunId(repo, token);

  let res;
  try {
    res = await fetch(
      `${GH_API}/repos/${repo}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
      {
        method: 'POST',
        headers: { ...ghHeaders(token), 'Content-Type': 'application/json' },
        // Strings — the workflow compares github.event.inputs.* as strings.
        // No expected_etag = single-dispatch (safe under Fork-A + concurrency).
        body: JSON.stringify({ ref: 'main', inputs: { tenant_id: tenantId, dry_run: 'false' } }),
      }
    );
  } catch (e) {
    throw statusError(`Could not reach GitHub to dispatch promotion: ${e.message}`, 502);
  }

  if (res.status !== 204) {
    const detail = (await res.text().catch(() => '')).slice(0, 300);
    throw statusError(
      `GitHub rejected the promotion dispatch (HTTP ${res.status})${detail ? `: ${detail}` : ''}`,
      res.status === 404 || res.status === 422 ? 500 : 502
    );
  }

  return {
    success: true,
    tenant_id: tenantId,
    message: 'Promotion started. Copying the staging config to production…',
    baseline,
  };
}

/**
 * Status of the run started after `afterRunId` (the dispatch baseline).
 * @returns {Promise<{found:boolean, status?:string, conclusion?:string|null, run_url?:string}>}
 *   found=false while the run hasn't appeared yet. status: queued|in_progress|
 *   completed. conclusion: success|failure|cancelled|null (null until completed).
 */
export async function getPromoteStatus(afterRunId) {
  const { token, repo } = config();
  const after = Number(afterRunId) || 0;

  let res;
  try {
    res = await fetch(
      `${GH_API}/repos/${repo}/actions/workflows/${WORKFLOW_FILE}/runs?per_page=5`,
      { headers: ghHeaders(token) }
    );
  } catch (e) {
    throw statusError(`Could not reach GitHub to read run status: ${e.message}`, 502);
  }
  if (!res.ok) throw statusError(`GitHub error reading run status (HTTP ${res.status}).`, 502);

  const data = await res.json();
  const runs = data.workflow_runs || [];
  // Newest run strictly newer than the baseline = the one we just started.
  const run = runs.filter((r) => r.id > after).sort((a, b) => b.id - a.id)[0];
  if (!run) return { found: false };

  return {
    found: true,
    status: run.status,
    conclusion: run.conclusion ?? null,
    run_url: run.html_url,
  };
}
