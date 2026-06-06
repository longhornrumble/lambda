'use strict';

/**
 * alerts.js — CloudWatch metric emission + SNS ops alert for the synthetic monitor.
 *
 * Each cycle emits a CycleSuccess/CycleFailure metric (dimensioned by Cycle) so a
 * CloudWatch alarm can enforce §5.1's ">3 failures in 24h = launch blocker" rule (the
 * alarm itself is CI-7 / integrator IaC). On failure the cycle also publishes to the
 * existing ops-alerts SNS topic. Both are best-effort — a metric/alert transport failure
 * must never mask the cycle's own result.
 */

const { CloudWatchClient, PutMetricDataCommand } = require('@aws-sdk/client-cloudwatch');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { sdkConfig } = require('./aws-clients');

const cw = new CloudWatchClient(sdkConfig());
const sns = new SNSClient(sdkConfig());

const METRIC_NAMESPACE = process.env.MONITOR_METRIC_NAMESPACE || 'Picasso/SchedulingSynthetic';
const OPS_ALERTS_TOPIC_ARN = process.env.OPS_ALERTS_TOPIC_ARN || '';

function warn(event, fields) {
  console.warn(JSON.stringify({ event, level: 'WARN', ...fields }));
}

async function emitCycleResult(cycle, success, { client = cw } = {}) {
  try {
    await client.send(
      new PutMetricDataCommand({
        Namespace: METRIC_NAMESPACE,
        MetricData: [
          {
            MetricName: 'CycleSuccess',
            Dimensions: [{ Name: 'Cycle', Value: cycle }],
            Value: success ? 1 : 0,
            Unit: 'Count',
          },
          {
            MetricName: 'CycleFailure',
            Dimensions: [{ Name: 'Cycle', Value: cycle }],
            Value: success ? 0 : 1,
            Unit: 'Count',
          },
        ],
      })
    );
  } catch (err) {
    warn('metric_emit_failed', { cycle, error: err.message });
  }
}

async function alert(subject, detail, { client = sns } = {}) {
  if (!OPS_ALERTS_TOPIC_ARN) {
    warn('alert_skipped_no_topic', { subject });
    return;
  }
  try {
    await client.send(
      new PublishCommand({
        TopicArn: OPS_ALERTS_TOPIC_ARN,
        Subject: String(subject).slice(0, 100),
        Message: JSON.stringify(detail),
      })
    );
  } catch (err) {
    warn('alert_failed', { subject, error: err.message });
  }
}

module.exports = { emitCycleResult, alert, _METRIC_NAMESPACE: METRIC_NAMESPACE };
