# picasso_pii_dsar_sla_monitor_staging

DSAR SLA Monitor Lambda — M3 done-bar #1 (master plan v0.3 §M3). Closes D5 G-D.

## What this Lambda does

Daily EventBridge-triggered scan of `picasso-pii-dsar-audit` looking for DSARs whose `request_received` event was written more than 25 days ago AND which have no `closed` event since. Emits a single SNS alert listing at-risk dsar_ids + intake timestamps. Operator subscribes to the topic via Console (no per-alarm subscription wired through IaC).

## Why this is a separate Lambda from `picasso_pii_dsar_staging`

Per CLAUDE.md "Never share IAM roles across Lambdas," the monitor gets a dedicated IAM role (`picasso-pii-dsar-sla-monitor-staging-role`). The DSAR fulfillment Lambda is privileged (DDB Delete on consumer-PII surfaces); the monitor is read-only on the audit table + SNS Publish on one topic. Separate blast radius.

## Files

- `lambda_function.py` — handler + scan + per-dsar closed-event check + SNS publish
- `test_sla_monitor.py` — 10 unit tests (deterministic, mocked DDB + SNS)
- `README.md` — this file

## Tests

```bash
cd Lambdas/lambda/picasso_pii_dsar_sla_monitor_staging
python3 -m pytest test_sla_monitor.py -v
# Expected: 10/10 passed
```

## Deploy

This Lambda is **not** on the `deploy-staging.yml` matrix (matches the M1 DSAR Lambda precedent — both are operator-deployed). After the Terraform module (`infra/modules/lambda-pii-dsar-sla-monitor-staging`) creates the function with the placeholder code, deploy the real code via:

```bash
cd Lambdas/lambda/picasso_pii_dsar_sla_monitor_staging
zip -r deployment.zip . -x "*.pyc" -x "__pycache__/*" -x "test_*.py" -x "*.md"
AWS_PROFILE=myrecruiter-staging aws lambda update-function-code \
  --function-name picasso-pii-dsar-sla-monitor-staging \
  --zip-file fileb://deployment.zip \
  --region us-east-1
rm deployment.zip
```

Verify:

```bash
AWS_PROFILE=myrecruiter-staging aws lambda get-function-configuration \
  --function-name picasso-pii-dsar-sla-monitor-staging \
  --query '{CodeSha256:CodeSha256,LastModified:LastModified,Runtime:Runtime,State:State}'
```

## Environment variables (set by Terraform)

| Var | Purpose | Default |
|---|---|---|
| `AUDIT_TABLE` | DDB table name to scan | `picasso-pii-dsar-audit` |
| `SLA_DAYS_INTAKE_PLUS` | Days from intake before alarm fires | `25` |
| `SNS_TOPIC_ARN` | Topic to publish at-risk alerts to | required (no default) |

## Test fire after deploy

Insert a synthetic `request_received` row past threshold:

```bash
PAST_TS=$(python3 -c "from datetime import datetime, timezone, timedelta; print((datetime.now(timezone.utc) - timedelta(days=30)).isoformat())")

AWS_PROFILE=myrecruiter-staging aws dynamodb put-item \
  --table-name picasso-pii-dsar-audit \
  --item "{
    \"dsar_id\":{\"S\":\"smoke-sla-monitor-001\"},
    \"event_timestamp\":{\"S\":\"$PAST_TS\"},
    \"event_type\":{\"S\":\"request_received\"},
    \"status\":{\"S\":\"in_progress\"},
    \"created_at_partition\":{\"S\":\"${PAST_TS:0:7}\"},
    \"details\":{\"S\":\"{\\\"smoke\\\":true}\"}
  }"
```

Invoke the Lambda manually (don't wait for schedule):

```bash
AWS_PROFILE=myrecruiter-staging aws lambda invoke \
  --function-name picasso-pii-dsar-sla-monitor-staging \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out /tmp/sla.json
cat /tmp/sla.json | python3 -m json.tool
# Expected: {"at_risk_count": 1, "dsar_ids": ["smoke-sla-monitor-001"]}
```

Confirm SNS alert arrived in operator inbox.

## Fault-test (M3 done-bar #2)

See `docs/roadmap/PII-Project/dsar-operator-playbook.md` §8 — disable SNS subscription, simulate at-risk row, confirm secondary check (weekly Monday CLI scan) catches it.

## Related

- Terraform module: `infra/modules/lambda-pii-dsar-sla-monitor-staging/`
- Audit table: `infra/modules/ddb-pii-dsar-audit-staging/` (PR1; 4-action Deny live)
- SNS topic: `picasso-ops-alerts-staging` (created by `ops-alarms-master-function-staging` module)
- Operator playbook: `docs/roadmap/PII-Project/dsar-operator-playbook.md` §8 (SLA timekeeping)
- D5 row G-D: SLA alarm requirement
- Master plan §M3: ACTIVE milestone scope
