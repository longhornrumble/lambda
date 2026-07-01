import { test } from 'node:test';
import assert from 'node:assert/strict';

// §E14 S4b loader, TABLE-SET path. SCHED_NOTIF_TEMPLATE_TABLE binds at module load, so this
// file (its own node:test process) sets the env BEFORE importing index.mjs — the inverse of
// index.test.mjs, which covers the unset-table no-op.
process.env.SCHED_NOTIF_TEMPLATE_TABLE = 'picasso-scheduling-notif-template-test';
const { loadTemplateOverride } = await import('./index.mjs');

const noopLogger = { log() {}, warn() {}, error() {} };

function recordingDdb(response) {
  const calls = [];
  return {
    calls,
    send: async (command) => {
      calls.push({ name: command.constructor.name, input: command.input });
      if (response instanceof Error) throw response;
      return response;
    },
  };
}

test('queries the template table with the {tenantId, moment} key and maps the §E14 fields', async () => {
  const ddb = recordingDdb({
    Item: {
      tenantId: 'AUS123957',
      moment: 'reminder_24h',
      subject: 'S',
      body_text: 'T',
      body_html: '<p>H</p>',
      sms_text: 'M',
    },
  });
  const result = await loadTemplateOverride({
    tenantId: 'AUS123957', moment: 'reminder_24h', ddb, logger: noopLogger,
  });
  assert.equal(ddb.calls.length, 1);
  assert.equal(ddb.calls[0].name, 'GetCommand');
  assert.equal(ddb.calls[0].input.TableName, 'picasso-scheduling-notif-template-test');
  assert.deepEqual(ddb.calls[0].input.Key, { tenantId: 'AUS123957', moment: 'reminder_24h' });
  assert.deepEqual(result, { subject: 'S', text: 'T', html: '<p>H</p>', sms: 'M', enabled: true });
});

test('miss (no Item) → null', async () => {
  const ddb = recordingDdb({});
  assert.equal(await loadTemplateOverride({ tenantId: 'T1', moment: 'reminder_1h', ddb, logger: noopLogger }), null);
});

test('non-string stored fields are dropped (schema discipline), not coerced', async () => {
  const ddb = recordingDdb({ Item: { subject: 42, body_text: 'T', body_html: null } });
  const result = await loadTemplateOverride({ tenantId: 'T1', moment: 'reminder_1h', ddb, logger: noopLogger });
  assert.deepEqual(result, { subject: undefined, text: 'T', html: undefined, sms: undefined, enabled: true });
});

test('enabled:false is surfaced from the row (moment toggled off)', async () => {
  const ddb = recordingDdb({ Item: { subject: 'S', enabled: false } });
  const result = await loadTemplateOverride({ tenantId: 'T1', moment: 'reminder_24h', ddb, logger: noopLogger });
  assert.equal(result.enabled, false);
});

test('DDB error → null (fail-safe, warns)', async () => {
  const warns = [];
  const ddb = recordingDdb(new Error('AccessDeniedException'));
  const result = await loadTemplateOverride({
    tenantId: 'T1', moment: 'reminder_24h', ddb,
    logger: { ...noopLogger, warn: (m) => warns.push(m) },
  });
  assert.equal(result, null);
  assert.equal(warns.length, 1);
  assert.match(warns[0], /using default copy/);
});

test('missing tenantId or moment → null without I/O', async () => {
  const ddb = recordingDdb({ Item: {} });
  assert.equal(await loadTemplateOverride({ tenantId: '', moment: 'reminder_24h', ddb, logger: noopLogger }), null);
  assert.equal(await loadTemplateOverride({ tenantId: 'T1', moment: '', ddb, logger: noopLogger }), null);
  assert.equal(ddb.calls.length, 0);
});
