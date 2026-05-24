"""
DSAR SLA Monitor Lambda — daily EventBridge-triggered scan for at-risk DSARs.

M3 done-bar #1 (master plan v0.3 §M3). Closes D5 G-D.

Behavior:
- Queries `picasso-pii-dsar-audit-staging` via the StatusIndex GSI for
  `status='in_progress'` audit rows whose `event_timestamp` is older than
  the SLA threshold (default: intake + 25 days, 5 days before CCPA 30-day
  combined SLA).
- For each candidate (which represents a `request_received` event past
  threshold), Queries the main table on PK=dsar_id to check if any
  `event_type='closed'` event has been written since. If a `closed` row
  exists, the DSAR is no longer at-risk — skip.
- For DSARs remaining (truly at-risk), publishes a single SNS alert
  enumerating the dsar_ids + intake timestamps.

Security posture:
- Dedicated IAM role (per CLAUDE.md "Never share IAM roles across Lambdas").
- Read-only on audit table (Query on table + StatusIndex GSI).
- SNS Publish only on the ops-alerts topic (no other SNS access).
- No PII in logs (D1 redaction; same posture as DSAR Lambda).
- No DDB writes; never mutates audit state (preserves the C2 4-action Deny).

Failure modes:
- DDB Query failure → re-raises so EventBridge surfaces the failure to
  CloudWatch (alarm-miss is itself an alarmable condition via §8 fault-test
  procedure in dsar-operator-playbook.md).
- SNS Publish failure → re-raises. Operator's secondary check (weekly CLI
  scan per playbook §8) catches it.
- Empty audit table → returns at_risk_count=0; no SNS publish.
"""
import json
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Env-var-driven config (set by Terraform module). Defaults match the
# staging table + 25d SLA window so the module can omit these when the
# defaults apply.
AUDIT_TABLE = os.environ.get('AUDIT_TABLE', 'picasso-pii-dsar-audit-staging')
SLA_DAYS_INTAKE_PLUS = int(os.environ.get('SLA_DAYS_INTAKE_PLUS', '25'))
INTAKE_EVENT_TYPE = 'request_received'  # status='in_progress' uniquely set on this event
INTAKE_STATUS = 'in_progress'
CLOSED_EVENT_TYPE = 'closed'

# SNS_TOPIC_ARN is REQUIRED — no sensible default; module fails closed.
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

ddb = boto3.resource('dynamodb')
sns = boto3.client('sns')


def _now() -> datetime:
    """UTC now. Wrapper for test stubbing."""
    return datetime.now(timezone.utc)


def _query_open_intakes_past_threshold(threshold_iso: str) -> list:
    """Query StatusIndex GSI for in_progress audit rows older than threshold.

    Returns list of dicts (audit row attributes). Filters by event_type to
    defensively handle future statuses that re-use 'in_progress' for other
    event types (today only request_received uses it, but the audit writer
    is general).
    """
    table = ddb.Table(AUDIT_TABLE)
    results = []
    last_evaluated_key = None
    while True:
        kwargs = {
            'IndexName': 'StatusIndex',
            'KeyConditionExpression': Key('status').eq(INTAKE_STATUS) & Key('event_timestamp').lte(threshold_iso),
        }
        if last_evaluated_key:
            kwargs['ExclusiveStartKey'] = last_evaluated_key
        try:
            resp = table.query(**kwargs)
        except ClientError as exc:
            logger.error(
                'sla_monitor_query_failed: index=StatusIndex code=%s',
                exc.response.get('Error', {}).get('Code'),
            )
            raise
        for item in resp.get('Items', []):
            if item.get('event_type') == INTAKE_EVENT_TYPE:
                results.append(item)
        last_evaluated_key = resp.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    return results


def _has_closed_event(dsar_id: str) -> bool:
    """Query main table on PK=dsar_id, check if any 'closed' event exists.

    Bounded by PK so this is a cheap point-query on a small partition
    (each DSAR has O(10) events maximum). FilterExpression on event_type
    is a server-side filter, not key-condition — DDB returns all events
    for the DSAR and filters in-stream.

    Sprint E2 / audit N16: `ddb.Table()` is intentionally instantiated per
    call here rather than cached at module-level. The handle is a thin
    metadata wrapper (no socket / no DDB call), and per-call instantiation
    keeps the test fixture's `mock_ddb.Table.side_effect = [...]` pattern
    intact (caching would consume only the first side_effect element across
    all calls, breaking per-call test assertions).
    """
    table = ddb.Table(AUDIT_TABLE)
    try:
        resp = table.query(
            KeyConditionExpression=Key('dsar_id').eq(dsar_id),
            FilterExpression='event_type = :closed',
            ExpressionAttributeValues={':closed': CLOSED_EVENT_TYPE},
            Select='COUNT',
        )
    except ClientError as exc:
        logger.error(
            'sla_monitor_closed_check_failed: dsar_id=%s code=%s',
            dsar_id, exc.response.get('Error', {}).get('Code'),
        )
        # Re-raise to surface the failure; conservative posture (better to
        # alarm-miss this DSAR than to silently report it as at-risk when
        # the closed check failed transiently).
        raise
    return resp.get('Count', 0) > 0


def _publish_alert(at_risk_rows: list) -> None:
    """Publish a single SNS message summarizing all at-risk DSARs."""
    if not SNS_TOPIC_ARN:
        # Fail closed — alert would have nowhere to go.
        raise RuntimeError('sla_monitor_misconfigured: SNS_TOPIC_ARN env var required')

    body_lines = [
        f'WARNING: {len(at_risk_rows)} DSAR(s) at risk of SLA breach',
        f'(open longer than {SLA_DAYS_INTAKE_PLUS} days; 5 days before CCPA 30-day target).',
        '',
        'Action: open the DSAR operator playbook and review per-DSAR.',
        'Playbook: https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/dsar-operator-playbook.md',
        '',
        'At-risk DSARs:',
    ]
    for row in at_risk_rows:
        dsar_id = row.get('dsar_id', '<unknown>')
        ts = row.get('event_timestamp', '<unknown>')
        # D1: do NOT include details.normalized_email or details.tenant_id
        # in the SNS body — those carry consumer PII / operator metadata.
        # Operator clicks through to the audit table for full details.
        body_lines.append(f'  - {dsar_id} (intake {ts})')

    message = '\n'.join(body_lines)
    # SNS Subject capped at 100 chars. With realistic at_risk_count values
    # the subject is ~50 chars, but the truncation was silent for any
    # accidental future format change. Sprint E2 / audit N17: log a warning
    # if truncation would actually fire so the operator catches the format
    # drift before it ships.
    subject = f'[Picasso DSAR] {len(at_risk_rows)} DSAR(s) at SLA risk past {SLA_DAYS_INTAKE_PLUS}d'
    if len(subject) > 100:
        logger.warning(
            'sla_monitor_subject_truncated: original_len=%d at_risk_count=%d',
            len(subject), len(at_risk_rows),
        )
        subject = subject[:100]

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
        )
    except ClientError as exc:
        logger.error(
            'sla_monitor_publish_failed: code=%s',
            exc.response.get('Error', {}).get('Code'),
        )
        raise


def lambda_handler(event, context):
    """EventBridge invokes this daily. Event payload ignored (only schedule fires)."""
    now = _now()
    threshold = now - timedelta(days=SLA_DAYS_INTAKE_PLUS)
    # M9.G7 / F-DSAR27: explicit microseconds-precision matches the writer
    # (picasso_pii_dsar_staging/lambda_function.py:_now_iso uses
    # `isoformat(timespec="microseconds")`). DDB does lexicographic string
    # comparison on the GSI range key; format mismatch would silently
    # mis-order rows at boundary moments (e.g., now() landing on a zero-
    # microsecond instant in tests). Don't change without also re-pinning
    # the writer.
    threshold_iso = threshold.isoformat(timespec='microseconds')
    logger.info(
        'sla_monitor_scan_start: now=%s threshold=%s (intake+%dd)',
        now.isoformat(), threshold_iso, SLA_DAYS_INTAKE_PLUS,
    )

    candidates = _query_open_intakes_past_threshold(threshold_iso)
    logger.info('sla_monitor_candidates: count=%d', len(candidates))

    at_risk = []
    for row in candidates:
        dsar_id = row.get('dsar_id')
        if not dsar_id:
            # Defensive — shouldn't happen on the audit table (PK is required)
            continue
        if _has_closed_event(dsar_id):
            logger.info('sla_monitor_skip_closed: dsar_id=%s', dsar_id)
            continue
        at_risk.append(row)

    if not at_risk:
        logger.info('sla_monitor_complete: at_risk_count=0 (no DSARs past SLA)')
        return {'at_risk_count': 0}

    logger.warning('sla_monitor_complete: at_risk_count=%d publishing SNS', len(at_risk))
    _publish_alert(at_risk)
    return {
        'at_risk_count': len(at_risk),
        'dsar_ids': [r.get('dsar_id') for r in at_risk],
    }
