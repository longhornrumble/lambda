# picasso_pii_dsar_weekly_reminder_staging

DSAR weekly reminder Lambda — belt-and-suspenders secondary control for the
primary SLA monitor (`picasso-pii-dsar-sla-monitor-staging`).

## Why this Lambda exists

The primary SLA monitor runs daily via EventBridge to scan the audit table
for at-risk DSARs and publish SNS alerts. If the primary fails silently
(Lambda deleted, IAM revoked, EventBridge schedule disabled, OOM mid-loop,
etc.), the operator has no independent signal that alerts have stopped
firing.

Phase-completion-audit 2026-05-23 (F-DSAR22) called out that a secondary
control that depends on operator initiation — e.g., "operator remembers
to check weekly" — is **not** independent verification. This Lambda closes
that gap.

It fires every week on its own EventBridge schedule and publishes a
reminder to the same ops-alerts SNS topic. The reminder includes two
copy-pasteable CLI snippets the operator runs:

1. CloudWatch `Invocations` metric on the primary SLA monitor over the
   last 7 days — fewer than ~7 datapoints = primary missed days.
2. Direct audit table scan for `status="in_progress"` rows whose
   `event_timestamp` is past the SLA threshold.

If both checks pass, the silence in primary alerts is the expected steady
state. If either surfaces something, the operator follows playbook §8.

## Independence properties

- Distinct Lambda function with a dedicated IAM role (per CLAUDE.md
  "Never share IAM roles across Lambdas"). If the primary's IAM/role is
  revoked, this Lambda still runs.
- Separate EventBridge schedule (weekly, not daily). If the primary's
  schedule is disabled, this one keeps firing.
- Shared SNS topic is an acceptable shared-failure mode: if SNS itself
  is broken, the primary's alerts also wouldn't arrive — a separate
  SNS-topic fault-test is documented in playbook §8.
- The reminder body does NOT call CloudWatch or DDB. Those checks are
  the operator's responsibility, which keeps this Lambda independent
  of the primary control's surfaces.

## Security posture

- SNS Publish only on the ops-alerts topic (no other AWS access).
- No DDB access. No CloudWatch metrics access. No PII in the message
  body (D1 redaction posture — verified by `test_message_no_consumer_pii`).
- Static reminder body + env-var-interpolated function/table/threshold
  names; no consumer data.

## Environment variables

| Var | Required | Default | Purpose |
|---|---|---|---|
| `SNS_TOPIC_ARN` | yes | — | Ops-alerts topic to publish to. Fail-closed if unset. |
| `PLAYBOOK_URL` | no | staging branch playbook URL | Reference URL in the reminder body. |
| `SLA_MONITOR_FUNCTION_NAME` | no | `picasso-pii-dsar-sla-monitor-staging` | Used in the CloudWatch CLI snippet. |
| `AUDIT_TABLE` | no | `picasso-pii-dsar-audit-staging` | Used in the audit-table CLI snippet. |
| `SLA_DAYS_INTAKE_PLUS` | no | `25` | Days past intake at which a DSAR is "at risk". Mirrors the primary monitor. |

## Schedule

Set by the Terraform module (`infra/modules/lambda-pii-dsar-weekly-reminder-staging/`).
Default: weekly, Monday 14:00 UTC (~9am ET), to land in the operator's
inbox at start-of-week.

## Testing

```bash
cd Lambdas/lambda/picasso_pii_dsar_weekly_reminder_staging
python3 -m unittest test_weekly_reminder.py -v
```

11 pure-mock unit tests. No boto3 / DDB / SNS infrastructure needed.

## Failure modes

- `SNS_TOPIC_ARN` unset → `RuntimeError` (fail closed).
- SNS Publish ClientError → re-raises so EventBridge marks invocation
  failed and CloudWatch Errors metric ticks (alarmable surface; the
  CW alarm itself ships with M9.G7).
- Lambda Errors metric is the surface watched by M9.G7's CW alarm.

## Deployment

Code deploys via the established CLAUDE.md SOP:

```bash
cd Lambdas/lambda/picasso_pii_dsar_weekly_reminder_staging
zip -r deployment.zip . -x "*.pyc" -x "__pycache__/*" -x "test_*"
AWS_PROFILE=myrecruiter-staging aws lambda update-function-code \
  --function-name picasso-pii-dsar-weekly-reminder-staging \
  --zip-file fileb://deployment.zip
```

The Terraform module provisions a placeholder zip + `lifecycle.ignore_changes`
so Terraform does not revert deployed bundles on re-apply.

## Relationship to other artifacts

- **Primary control:** `picasso_pii_dsar_sla_monitor_staging` (daily; fires
  alerts on at-risk DSARs)
- **Master plan row:** M9.G6
- **D5 row closed:** F-DSAR22 (M3 SLA monitor secondary-control independence)
- **Operator playbook:** §8 (SLA timekeeping + fault-test). M9.G6 also
  ships a small playbook §8 fix for the `status="open"` → `"in_progress"`
  bug in the manual SLA tracking CLI snippet (in the picasso repo PR).
