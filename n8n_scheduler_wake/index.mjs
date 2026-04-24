/**
 * n8n-scheduler-wake — EventBridge-triggered orchestrator for the KB-freshness scanner.
 *
 * Replaces the planned n8n Schedule trigger + ExecuteCommand workflow because n8n
 * (both 2.14.2 and 2.17.6) does not register the executeCommand node at runtime.
 *
 * Flow:
 *   1. Check ISO-week parity (biweekly cadence from a weekly EventBridge fire).
 *   2. Ensure EC2 running + n8n /healthz responding (reuses deal-prep-webhook-proxy pattern).
 *   3. SSM SendCommand → docker run scanner against MYR384719.
 *   4. Scanner emits its own webhook to kb-proposal-notifier → notification_hub.
 *
 * Tenants scanned: SCANNER_TENANTS env var (comma-separated). Default: MYR384719.
 *
 * Manual trigger: `aws lambda invoke --function-name n8n-scheduler-wake --payload '{"force":true}' …`
 *   (force=true bypasses week-parity gate; useful for smoke tests.)
 */

import { EC2Client, DescribeInstancesCommand, StartInstancesCommand } from "@aws-sdk/client-ec2";
import { SSMClient, SendCommandCommand } from "@aws-sdk/client-ssm";

const N8N_INSTANCE_ID = process.env.N8N_INSTANCE_ID || "i-04281d9886e3a6c41";
const N8N_HEALTH_URL = process.env.N8N_HEALTH_URL || "http://98.89.202.33:5678/healthz";
const SCANNER_TENANTS = (process.env.SCANNER_TENANTS || "MYR384719")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const MAX_WAIT_MS = 180_000;
const POLL_INTERVAL_MS = 5_000;
const HEALTH_TIMEOUT_MS = 5_000;

const ec2 = new EC2Client({ region: "us-east-1" });
const ssm = new SSMClient({ region: "us-east-1" });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * ISO 8601 week number. Stable across DST and reboots — biweekly cadence
 * derived from "fire weekly + only act on odd ISO weeks" needs no persisted
 * state. To shift the cadence phase (act on even weeks instead), invert the
 * gate condition in the handler.
 */
function getISOWeek(d) {
  const date = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = (date.getUTCDay() + 6) % 7;
  date.setUTCDate(date.getUTCDate() - dayNum + 3);
  const firstThursday = date.getTime();
  date.setUTCMonth(0, 1);
  if (date.getUTCDay() !== 4) {
    date.setUTCMonth(0, 1 + ((4 - date.getUTCDay()) + 7) % 7);
  }
  return 1 + Math.ceil((firstThursday - date.getTime()) / (7 * 24 * 60 * 60 * 1000));
}

async function getInstanceState(id) {
  const res = await ec2.send(new DescribeInstancesCommand({ InstanceIds: [id] }));
  return res.Reservations?.[0]?.Instances?.[0]?.State?.Name ?? "unknown";
}

async function checkHealth() {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), HEALTH_TIMEOUT_MS);
  try {
    const r = await fetch(N8N_HEALTH_URL, { signal: ctrl.signal });
    return r.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(t);
  }
}

async function ensureN8nRunning() {
  if (await checkHealth()) return { ok: true, wasStarted: false };

  const state = await getInstanceState(N8N_INSTANCE_ID);
  console.log(JSON.stringify({ msg: "instance state", state }));

  if (state === "stopped") {
    await ec2.send(new StartInstancesCommand({ InstanceIds: [N8N_INSTANCE_ID] }));
    console.log(JSON.stringify({ msg: "start requested" }));
  } else if (state !== "running" && state !== "pending" && state !== "stopping") {
    return { ok: false, error: `unexpected state: ${state}` };
  }

  const t0 = Date.now();
  while (Date.now() - t0 < MAX_WAIT_MS) {
    await sleep(POLL_INTERVAL_MS);
    if (await checkHealth()) {
      return { ok: true, wasStarted: true, waitMs: Date.now() - t0 };
    }
  }
  return { ok: false, error: "timeout waiting for n8n /healthz" };
}

/**
 * Issue an SSM SendCommand to run the scanner for one tenant. Fire-and-forget:
 * the scanner emits its own webhook on completion. We log the SSM command ID
 * for traceability — see CloudWatch Logs `/aws/ssm/AWS-RunShellScript` or
 * the SSM console to follow execution.
 *
 * Secrets are fetched server-side (on the EC2 host via its IAM role) rather
 * than passed through Lambda → SSM, so they never appear in SSM input/output.
 */
async function dispatchScanner(tenantId) {
  const commands = [
    "set -e",
    "FC=$(aws secretsmanager get-secret-value --region us-east-1 --secret-id n8n/firecrawl-api-key --query SecretString --output text)",
    "AN=$(aws secretsmanager get-secret-value --region us-east-1 --secret-id n8n/anthropic-api-key --query SecretString --output text)",
    "NK=$(aws secretsmanager get-secret-value --region us-east-1 --secret-id n8n/notify-shared-secret --query SecretString --output text)",
    `sudo docker run --rm -v /opt/kb-freshness:/mnt/kb-freshness:ro -w /mnt/kb-freshness/picasso-webscraping/rag-scraper -u 1000:1000 --network bridge -e FIRECRAWL_API_KEY="$FC" -e ANTHROPIC_API_KEY="$AN" -e NOTIFY_SHARED_SECRET="$NK" -e AWS_DEFAULT_REGION=us-east-1 node:20-alpine npx tsx scanner/agent-runner.ts --tenant ${tenantId} 2>&1`,
  ];
  const res = await ssm.send(
    new SendCommandCommand({
      InstanceIds: [N8N_INSTANCE_ID],
      DocumentName: "AWS-RunShellScript",
      Parameters: { commands },
      TimeoutSeconds: 600,
      Comment: `kb-freshness scanner: ${tenantId}`,
    }),
  );
  return res.Command?.CommandId;
}

export const handler = async (event) => {
  const t0 = Date.now();
  const force = event?.force === true;
  const week = getISOWeek(new Date());

  console.log(JSON.stringify({ msg: "wake start", week, force, tenants: SCANNER_TENANTS }));

  // Biweekly gate: act on odd ISO weeks only. Force overrides for smoke tests.
  if (!force && week % 2 === 0) {
    console.log(JSON.stringify({ msg: "skip: even iso week", week }));
    return { ok: true, skipped: "even-iso-week", week };
  }

  // Wake EC2 + wait for n8n healthz.
  const wake = await ensureN8nRunning();
  if (!wake.ok) {
    console.error(JSON.stringify({ msg: "wake failed", ...wake }));
    return { ok: false, ...wake, week };
  }

  // Dispatch one SSM scanner run per tenant. Fire-and-forget; webhook delivers result.
  const dispatched = [];
  for (const tenant of SCANNER_TENANTS) {
    try {
      const cmdId = await dispatchScanner(tenant);
      dispatched.push({ tenant, ssmCommandId: cmdId, status: "dispatched" });
      console.log(JSON.stringify({ msg: "scanner dispatched", tenant, ssmCommandId: cmdId }));
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      dispatched.push({ tenant, status: "error", error: msg });
      console.error(JSON.stringify({ msg: "scanner dispatch failed", tenant, error: msg }));
    }
  }

  return {
    ok: true,
    week,
    wasStarted: wake.wasStarted,
    waitMs: wake.waitMs ?? 0,
    elapsedMs: Date.now() - t0,
    dispatched,
  };
};
