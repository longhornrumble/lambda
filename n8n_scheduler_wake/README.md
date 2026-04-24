# n8n-scheduler-wake

EventBridge-triggered orchestrator for the KB-freshness scanner.

## Why this exists

The plan ([KB_FRESHNESS_LIFECYCLE_SYSTEM.md](../../../docs/roadmap/KB_FRESHNESS_LIFECYCLE_SYSTEM.md)) calls for an n8n Schedule trigger + ExecuteCommand node to fire the scanner biweekly. **`n8n-nodes-base.executeCommand` is not registered at runtime** in n8n 2.14.x or 2.17.x — the node files ship with the image but do not appear in n8n's loaded-types registry, so workflows referencing it fail to activate. This Lambda is the pivot: schedule + dispatch live in AWS, n8n only handles the inbound webhook from the scanner via the existing `kb-proposal-notifier` workflow.

## Architecture

```
[EventBridge: cron(50 12 ? * MON *) — Mon 12:50 UTC]
        ↓
[n8n-scheduler-wake Lambda]
   1. ISO-week parity gate (act on odd weeks → biweekly cadence)
   2. ensure EC2 i-04281d9886e3a6c41 running + n8n /healthz responding
   3. SSM SendCommand → docker run scanner against each tenant
        ↓
[Scanner on EC2]  ─── docker run --rm node:20-alpine npx tsx scanner/agent-runner.ts --tenant ...
   - Reads /opt/kb-freshness/picasso-webscraping/rag-scraper (host clone)
   - On material changes: writes proposal.json to S3 + POSTs webhook
        ↓
[n8n kb-proposal-notifier workflow] → notification_hub → Slack + email
```

Secrets (FIRECRAWL_API_KEY, ANTHROPIC_API_KEY, NOTIFY_SHARED_SECRET) are fetched server-side on the EC2 host via the instance role — never passed through Lambda or visible in SSM input/output.

## Manual trigger

Bypass the biweekly gate for smoke testing:

```bash
aws lambda invoke --function-name n8n-scheduler-wake \
    --payload '{"force":true}' \
    --cli-binary-format raw-in-base64-out \
    --profile chris-admin /tmp/wake.json
cat /tmp/wake.json
```

## Tenants

Comma-separated list in env `SCANNER_TENANTS`. Default: `MYR384719`. To enroll more, update the Lambda env and ensure each tenant has `monitor.enabled: true` + `monitor.urlInventorySnapshot` + `monitor.orgName` + `monitor.dubTag` set in `s3://myrecruiter-picasso/tenants/{tenantId}/{tenantId}-config.json`.

## Deploy

```bash
npm run package
npm run deploy
```

The IAM role (`n8n-scheduler-wake-role`) is described by `iam-policy.json` (inline) and `trust-policy.json` (AssumeRole). Apply changes via:

```bash
aws iam put-role-policy --role-name n8n-scheduler-wake-role \
    --policy-name ec2-and-logs --policy-document file://iam-policy.json \
    --profile chris-admin
```

## EventBridge rule

```bash
aws events describe-rule --name n8n-kb-scanner-wake --profile chris-admin
```

Schedule: `cron(50 12 ? * MON *)` — Monday 12:50 UTC = 07:50 CDT (06:50 CST during standard time). Fires weekly; Lambda's biweekly gate filters odd ISO weeks.
