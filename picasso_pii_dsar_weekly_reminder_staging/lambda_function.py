"""
DSAR Weekly Reminder Lambda — belt-and-suspenders secondary control for the
primary SLA monitor (`picasso-pii-dsar-sla-monitor-staging`).

M9.G6 (master plan v0.12). Closes D5 F-DSAR22.

Why this Lambda exists:
- The primary SLA monitor runs daily via EventBridge. If the primary fails
  silently (Lambda deleted, IAM revoked, EventBridge schedule disabled,
  Lambda OOM mid-loop, etc.), the operator has no independent signal that
  alerts have stopped firing.
- A secondary control that depends on operator initiation (e.g., "operator
  remembers to check weekly") is not independent verification — the F-DSAR22
  audit finding called this out as the M3 done-bar #2 PARTIAL.
- This Lambda is the independent secondary signal: a separate Lambda with
  its own IAM role and its own EventBridge schedule, publishing a weekly
  reminder to the same ops-alerts SNS topic so the operator gets a regular
  prompt to run two specific verification CLIs.

Independence properties:
- Distinct Lambda function + dedicated IAM role (per CLAUDE.md "Never share
  IAM roles across Lambdas"). If the primary's IAM/role is revoked, this
  Lambda still runs.
- Separate EventBridge schedule (weekly, not daily). If the primary's
  schedule is disabled, this one keeps firing.
- Shared SNS topic is an acceptable shared-failure mode: if SNS itself is
  broken, the primary's alerts also wouldn't arrive — there's a separate
  fault-test for SNS health documented in playbook §8.
- The reminder message includes the exact CLI snippets the operator runs
  to verify (a) SLA monitor invocations in the last 7 days, and (b) the
  audit table directly for past-25d open rows. The Lambda itself does NOT
  check these — that would create a dependency on the primary's metrics.

Security posture:
- SNS Publish only on the ops-alerts topic (no other AWS access).
- No DDB access. No CloudWatch metrics access. No PII.
- The reminder body is static text + env-var-driven URLs; no consumer data.

Failure modes:
- SNS Publish failure → re-raises so EventBridge surfaces it to CloudWatch.
  (When M9.G7 ships a CW Errors alarm on this Lambda, a publish failure
  will alarm.)
- SNS_TOPIC_ARN env var unset → fail closed (RuntimeError).
"""
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration via env vars. SNS_TOPIC_ARN has no sensible default —
# fail closed if absent.
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

# URLs default to the staging branch of the picasso repo. Terraform module
# pins these at deploy time so changes flow via PR + plan visibility.
PLAYBOOK_URL = os.environ.get(
    'PLAYBOOK_URL',
    'https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/dsar-operator-playbook.md',
)
SLA_MONITOR_FUNCTION_NAME = os.environ.get(
    'SLA_MONITOR_FUNCTION_NAME',
    'picasso-pii-dsar-sla-monitor-staging',
)
AUDIT_TABLE = os.environ.get('AUDIT_TABLE', 'picasso-pii-dsar-audit-staging')
SLA_DAYS_INTAKE_PLUS = os.environ.get('SLA_DAYS_INTAKE_PLUS', '25')

sns = boto3.client('sns')


def _build_message() -> str:
    """Compose the weekly reminder body.

    Pure function for easy unit testing. Static text + env-var interpolation
    only; intentionally does NOT call DDB or CloudWatch — those checks are
    the operator's responsibility, which keeps this Lambda independent of
    the primary control's surfaces.
    """
    return '\n'.join([
        'Weekly DSAR SLA monitor health-check reminder.',
        '',
        'Status: REMINDER ONLY — this Lambda intentionally does NOT fetch live',
        'data (no DDB Query, no CloudWatch metrics read) so it stays independent',
        'of the primary SLA monitor\'s surfaces. The actual status is what you',
        'find when you run the two CLI checks embedded below.',
        '',
        f'This is the belt-and-suspenders secondary control for the primary',
        f'SLA monitor Lambda ({SLA_MONITOR_FUNCTION_NAME}). It fires every',
        'week regardless of the primary\'s state. If the primary has silently',
        'failed you will still see this message.',
        '',
        'If you received no daily DSAR SLA alerts in the past week, that could',
        'mean (a) no DSARs were past SLA — the expected steady state — or',
        '(b) the primary monitor has stopped firing. Run BOTH checks now to',
        'tell the difference.',
        '',
        '1. Verify the primary monitor ran daily this week:',
        '',
        '   AWS_PROFILE=myrecruiter-staging aws cloudwatch get-metric-statistics \\',
        '     --namespace AWS/Lambda --metric-name Invocations \\',
        f'     --dimensions Name=FunctionName,Value={SLA_MONITOR_FUNCTION_NAME} \\',
        '     --start-time $(date -u -v-7d +%Y-%m-%dT%H:%M:%SZ) \\',
        '     --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \\',
        '     --period 86400 --statistics Sum',
        '',
        '   Expected: ~7 datapoints, each Sum=1. Fewer = primary missed days.',
        '',
        '2. Scan the audit table directly for open DSAR intake events.',
        f'   Any row whose event_timestamp is older than {SLA_DAYS_INTAKE_PLUS}',
        '   days is at risk.',
        '',
        '   AWS_PROFILE=myrecruiter-staging aws dynamodb query \\',
        f'     --table-name {AUDIT_TABLE} --index-name StatusIndex \\',
        '     --key-condition-expression "#s = :s" \\',
        '     --expression-attribute-names \'{"#s":"status"}\' \\',
        '     --expression-attribute-values \'{":s":{"S":"in_progress"}}\' \\',
        '     --query "Items[?event_type.S==\'request_received\'].[dsar_id.S,event_timestamp.S]"',
        '',
        '   Expected: empty array. Any rows = compute age (today - intake)',
        '   and act on each per playbook §3-§6 per request_type.',
        '',
        f'Operator playbook: {PLAYBOOK_URL}',
        '',
        'If both checks pass, you are caught up. If either surfaces something,',
        'follow playbook §8 (SLA timekeeping + fault-test).',
    ])


def _publish_reminder(body: str) -> None:
    """Publish the weekly reminder to the ops SNS topic.

    Fails closed if SNS_TOPIC_ARN is unset (misconfiguration). Re-raises
    ClientError so EventBridge marks the invocation as failed and CloudWatch
    Errors metric ticks (alarmable surface once M9.G7 ships the alarm).
    """
    if not SNS_TOPIC_ARN:
        raise RuntimeError(
            'weekly_reminder_misconfigured: SNS_TOPIC_ARN env var required'
        )

    subject = '[Picasso DSAR] Weekly SLA monitor health-check reminder'
    # SNS Subject capped at 100 chars; this is 58.

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=body,
        )
    except ClientError as exc:
        logger.error(
            'weekly_reminder_publish_failed: code=%s',
            exc.response.get('Error', {}).get('Code'),
        )
        raise


def lambda_handler(event, context):
    """EventBridge invokes this weekly. Event payload is ignored."""
    logger.info('weekly_reminder_start')
    body = _build_message()
    _publish_reminder(body)
    logger.info('weekly_reminder_published')
    return {'published': True}
