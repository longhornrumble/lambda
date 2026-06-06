'use strict';

// alerts.js reads OPS_ALERTS_TOPIC_ARN + MONITOR_METRIC_NAMESPACE at module load, so set
// them before requiring and load in isolation per scenario.

function loadAlertsWith(env) {
  let mod;
  jest.isolateModules(() => {
    const prev = {
      OPS_ALERTS_TOPIC_ARN: process.env.OPS_ALERTS_TOPIC_ARN,
      MONITOR_METRIC_NAMESPACE: process.env.MONITOR_METRIC_NAMESPACE,
    };
    if ('OPS_ALERTS_TOPIC_ARN' in env) process.env.OPS_ALERTS_TOPIC_ARN = env.OPS_ALERTS_TOPIC_ARN;
    else delete process.env.OPS_ALERTS_TOPIC_ARN;
    try {
      mod = require('./alerts');
    } finally {
      process.env.OPS_ALERTS_TOPIC_ARN = prev.OPS_ALERTS_TOPIC_ARN;
      process.env.MONITOR_METRIC_NAMESPACE = prev.MONITOR_METRIC_NAMESPACE;
    }
  });
  return mod;
}

describe('alerts.emitCycleResult', () => {
  test('emits CycleSuccess=1 / CycleFailure=0 on success', async () => {
    const alerts = loadAlertsWith({ OPS_ALERTS_TOPIC_ARN: 'arn:topic' });
    const client = { send: jest.fn().mockResolvedValue({}) };
    await alerts.emitCycleResult('cancel', true, { client });
    const input = client.send.mock.calls[0][0].input;
    const byName = Object.fromEntries(input.MetricData.map((m) => [m.MetricName, m.Value]));
    expect(byName).toEqual({ CycleSuccess: 1, CycleFailure: 0 });
    expect(input.MetricData[0].Dimensions).toEqual([{ Name: 'Cycle', Value: 'cancel' }]);
  });

  test('emits CycleSuccess=0 / CycleFailure=1 on failure', async () => {
    const alerts = loadAlertsWith({ OPS_ALERTS_TOPIC_ARN: 'arn:topic' });
    const client = { send: jest.fn().mockResolvedValue({}) };
    await alerts.emitCycleResult('cleanup', false, { client });
    const byName = Object.fromEntries(client.send.mock.calls[0][0].input.MetricData.map((m) => [m.MetricName, m.Value]));
    expect(byName).toEqual({ CycleSuccess: 0, CycleFailure: 1 });
  });

  test('swallows a CloudWatch transport error (best-effort, never throws)', async () => {
    const alerts = loadAlertsWith({ OPS_ALERTS_TOPIC_ARN: 'arn:topic' });
    const client = { send: jest.fn().mockRejectedValue(new Error('cw down')) };
    await expect(alerts.emitCycleResult('cancel', true, { client })).resolves.toBeUndefined();
  });
});

describe('alerts.alert', () => {
  test('publishes to SNS when the topic ARN is set', async () => {
    const alerts = loadAlertsWith({ OPS_ALERTS_TOPIC_ARN: 'arn:aws:sns:us-east-1:1:ops' });
    const client = { send: jest.fn().mockResolvedValue({}) };
    await alerts.alert('subj', { a: 1 }, { client });
    const input = client.send.mock.calls[0][0].input;
    expect(input.TopicArn).toBe('arn:aws:sns:us-east-1:1:ops');
    expect(input.Subject).toBe('subj');
    expect(JSON.parse(input.Message)).toEqual({ a: 1 });
  });

  test('skips publishing (no throw, no send) when the topic ARN is unset', async () => {
    const alerts = loadAlertsWith({}); // OPS_ALERTS_TOPIC_ARN deleted
    const client = { send: jest.fn() };
    await alerts.alert('subj', { a: 1 }, { client });
    expect(client.send).not.toHaveBeenCalled();
  });

  test('swallows an SNS transport error (best-effort, never throws)', async () => {
    const alerts = loadAlertsWith({ OPS_ALERTS_TOPIC_ARN: 'arn:topic' });
    const client = { send: jest.fn().mockRejectedValue(new Error('sns down')) };
    await expect(alerts.alert('subj', {}, { client })).resolves.toBeUndefined();
  });
});
