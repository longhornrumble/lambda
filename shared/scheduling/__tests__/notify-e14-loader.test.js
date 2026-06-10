/**
 * §E14 defaultLoadTemplateOverride DDB-execution path (GAP-1).
 *
 * The guard tests in notify-e14-overrides.test.js short-circuit before any DDB call (table
 * env unset). Here we SET the table env before require + mock DynamoDBClient, so the actual
 * GetItem path is exercised: hit, miss, malformed row, and the fail-safe error catch.
 */

process.env.SCHED_NOTIF_TEMPLATE_TABLE = 'picasso-scheduling-notif-template-test';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');

const { defaultLoadTemplateOverride } = require('../notify.js');

const ddbMock = mockClient(DynamoDBClient);
const silent = { info() {}, warn() {}, error() {} };

beforeEach(() => ddbMock.reset());

test('HIT: a stored override row maps to {subject,text,html,sms} (G7b projects sms_text)', async () => {
  ddbMock.on(GetItemCommand).resolves({
    Item: {
      tenantId: { S: 'T' },
      moment: { S: 'reschedule_link' },
      subject: { S: 'Custom subject' },
      body_text: { S: 'Custom text' },
      body_html: { S: '<p>Custom html</p>' },
      sms_text: { S: 'Custom SMS {{firstName}}' },
    },
  });
  const r = await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'reschedule_link', log: silent });
  // sms MUST be projected from the `sms_text` attribute (the G7b field-name coupling).
  expect(r).toEqual({
    subject: 'Custom subject',
    text: 'Custom text',
    html: '<p>Custom html</p>',
    sms: 'Custom SMS {{firstName}}',
  });
  // keyed correctly (tenantId, moment)
  const call = ddbMock.commandCalls(GetItemCommand)[0].args[0].input;
  expect(call.Key).toEqual({ tenantId: { S: 'T' }, moment: { S: 'reschedule_link' } });
  expect(call.TableName).toBe('picasso-scheduling-notif-template-test');
});

test('HIT partial: only the stored fields map; absent fields (incl. sms) are undefined', async () => {
  ddbMock.on(GetItemCommand).resolves({ Item: { subject: { S: 'Only subject' } } });
  const r = await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'reoffer', log: silent });
  expect(r).toEqual({ subject: 'Only subject', text: undefined, html: undefined, sms: undefined });
});

test('MISS: no Item → null (defaults used downstream)', async () => {
  ddbMock.on(GetItemCommand).resolves({});
  expect(await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'cancel_notice', log: silent })).toBeNull();
});

test('MALFORMED: a non-string (N) attribute is ignored (undefined), never throws', async () => {
  ddbMock.on(GetItemCommand).resolves({ Item: { subject: { N: '42' }, body_text: { S: 'ok' } } });
  const r = await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'reschedule_link', log: silent });
  expect(r).toEqual({ subject: undefined, text: 'ok', html: undefined });
});

test('FAIL-SAFE: a DDB error returns null AND logs a warn (operator visibility)', async () => {
  ddbMock.on(GetItemCommand).rejects(new Error('ddb unavailable'));
  const log = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
  expect(await defaultLoadTemplateOverride({ tenantId: 'T', kind: 'reschedule_link', log })).toBeNull();
  // a silently-failing template override is operator-visible: assert the warn fired.
  expect(log.warn).toHaveBeenCalledWith(expect.stringContaining('template override load failed'));
});
