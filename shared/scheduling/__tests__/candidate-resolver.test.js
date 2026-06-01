'use strict';

/**
 * Unit tests for candidate-resolver.js (WS-SCHED-FOUNDATIONS, contract X).
 *
 * Two layers:
 *  1. Logic via the DI seam (no AWS): tag-match AND/in_any, empty-conditions,
 *     no-eligible, the appointmentType→routingPolicy hop, malformed-employee
 *     tolerance, the resourceId↔email mapping, error cases.
 *  2. The default DI implementations against aws-sdk-client-mock (GetItem / Query +
 *     registry pagination + attribute-value unmarshalling).
 */

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');

const {
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} = require('@aws-sdk/client-dynamodb');

const ddbMock = mockClient(DynamoDBClient);

const {
  resolveCandidates,
  defaultGetRoutingPolicy,
  defaultGetAppointmentType,
  defaultQueryEmployees,
  isEligible,
  unmarshalItem,
} = require('../candidate-resolver');

const TENANT = 'AUS123957';

// Silence the resolver's skip warnings in expected-skip tests.
const quietLog = { warn: jest.fn(), info: jest.fn(), error: jest.fn() };

beforeEach(() => {
  ddbMock.reset();
  jest.clearAllMocks();
});

// Convenience builders for the DI seam.
const emp = (employeeId, email, scheduling_tags) => ({
  employeeId,
  email,
  scheduling_tags,
});
const policy = (tag_conditions) => ({ routing_policy_id: 'rp-1', tag_conditions });
const depsWith = (employees, policyObj, overrides = {}) => ({
  getRoutingPolicy: jest.fn().mockResolvedValue(policyObj),
  queryEmployees: jest.fn().mockResolvedValue(employees),
  log: quietLog,
  ...overrides,
});

// ─── input validation ──────────────────────────────────────────────────────────────

describe('resolveCandidates — input validation', () => {
  test('throws without tenantId', async () => {
    await expect(
      resolveCandidates({ routingPolicyId: 'rp-1' }, depsWith([], policy([])))
    ).rejects.toThrow('tenantId is required');
  });

  test('throws without routingPolicyId or appointmentTypeId', async () => {
    await expect(
      resolveCandidates({ tenantId: TENANT }, depsWith([], policy([])))
    ).rejects.toThrow('routingPolicyId or appointmentTypeId is required');
  });

  test('throws when the routing policy is not found', async () => {
    await expect(
      resolveCandidates(
        { tenantId: TENANT, routingPolicyId: 'rp-missing' },
        depsWith([], null)
      )
    ).rejects.toThrow('routing policy rp-missing not found');
  });
});

// ─── tag-condition eligibility ───────────────────────────────────────────────────────

describe('resolveCandidates — tag-condition eligibility', () => {
  const employees = [
    emp('u1', 'maya@org.com', ['mentoring', 'spanish']),
    emp('u2', 'sam@org.com', ['mentoring']),
    emp('u3', 'lee@org.com', ['donations']),
  ];

  test('empty tag_conditions → every scheduling-tagged employee eligible (§10.3 solo)', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(employees, policy([]))
    );
    expect(out.map((c) => c.resourceId).sort()).toEqual([
      'lee@org.com',
      'maya@org.com',
      'sam@org.com',
    ]);
  });

  test("'equals' (default) requires every value (AND within a condition)", async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(
        employees,
        policy([{ tag: 'program', values: ['mentoring', 'spanish'] }])
      )
    );
    expect(out.map((c) => c.resourceId)).toEqual(['maya@org.com']); // only u1 has both
  });

  test("'in_any' requires at least one value", async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(
        employees,
        policy([{ tag: 'program', operator: 'in_any', values: ['mentoring'] }])
      )
    );
    expect(out.map((c) => c.resourceId).sort()).toEqual([
      'maya@org.com',
      'sam@org.com',
    ]);
  });

  test('multiple conditions AND together', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(employees, policy([
        { tag: 'program', operator: 'in_any', values: ['mentoring'] },
        { tag: 'language', operator: 'in_any', values: ['spanish'] },
      ]))
    );
    expect(out.map((c) => c.resourceId)).toEqual(['maya@org.com']); // u1: mentoring AND spanish
  });

  test('no eligible candidate → empty array', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(employees, policy([{ tag: 'program', values: ['nonexistent'] }]))
    );
    expect(out).toEqual([]);
  });

  test('missing tag_conditions field on policy → treated as empty (forward-compat)', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(employees, { routing_policy_id: 'rp-1' }) // no tag_conditions key
    );
    expect(out).toHaveLength(3);
  });
});

// ─── resourceId ↔ email mapping ──────────────────────────────────────────────────────

describe('resolveCandidates — resourceId/coordinatorEmail mapping', () => {
  test('maps each eligible employee to {resourceId, scheduling_tags, coordinatorEmail}', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith([emp('u1', 'Maya@Org.com', ['mentoring'])], policy([]))
    );
    expect(out).toEqual([
      {
        resourceId: 'maya@org.com', // lower-cased + trimmed (byte-stable cursor)
        scheduling_tags: ['mentoring'],
        coordinatorEmail: 'maya@org.com',
      },
    ]);
  });

  test('trims surrounding whitespace on the email', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith([emp('u1', '  sam@org.com  ', ['mentoring'])], policy([]))
    );
    expect(out[0].resourceId).toBe('sam@org.com');
  });
});

// ─── malformed-employee tolerance ────────────────────────────────────────────────────

describe('resolveCandidates — malformed-employee tolerance', () => {
  test('skips null rows, missing/empty/non-array tags, and missing emails without crashing', async () => {
    const employees = [
      null, // entirely malformed
      emp('u-noTags', 'x@org.com', undefined), // no scheduling_tags
      emp('u-emptyTags', 'y@org.com', []), // empty scheduling_tags
      { employeeId: 'u-strTags', email: 'z@org.com', scheduling_tags: 'mentoring' }, // non-array
      emp('u-noEmail', '', ['mentoring']), // tagged but unbookable
      emp('u-ok', 'real@org.com', ['mentoring']),
    ];
    const log = { warn: jest.fn(), info: jest.fn(), error: jest.fn() };
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(employees, policy([]), { log })
    );
    expect(out.map((c) => c.resourceId)).toEqual(['real@org.com']);
    // the tagged-but-emailless employee is logged (UUID only, no email value to leak)
    expect(log.warn).toHaveBeenCalledWith(
      expect.stringContaining('u-noEmail')
    );
  });

  test('tolerates queryEmployees returning null', async () => {
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      depsWith(null, policy([]))
    );
    expect(out).toEqual([]);
  });
});

// ─── appointmentTypeId → routingPolicyId hop ─────────────────────────────────────────

describe('resolveCandidates — appointmentType hop (§A)', () => {
  test('resolves routing_policy_id via the appointment type then queries the pool', async () => {
    const getAppointmentType = jest
      .fn()
      .mockResolvedValue({ routing_policy_id: 'rp-derived' });
    const getRoutingPolicy = jest
      .fn()
      .mockResolvedValue({ routing_policy_id: 'rp-derived', tag_conditions: [] });
    const queryEmployees = jest
      .fn()
      .mockResolvedValue([emp('u1', 'maya@org.com', ['mentoring'])]);

    const out = await resolveCandidates(
      { tenantId: TENANT, appointmentTypeId: 'at-intake' },
      { getAppointmentType, getRoutingPolicy, queryEmployees, log: quietLog }
    );

    expect(getAppointmentType).toHaveBeenCalledWith({
      tenantId: TENANT,
      appointmentTypeId: 'at-intake',
    });
    expect(getRoutingPolicy).toHaveBeenCalledWith({
      tenantId: TENANT,
      routingPolicyId: 'rp-derived',
    });
    expect(out.map((c) => c.resourceId)).toEqual(['maya@org.com']);
  });

  test('throws when the appointment type is missing or has no routing_policy_id', async () => {
    await expect(
      resolveCandidates(
        { tenantId: TENANT, appointmentTypeId: 'at-missing' },
        {
          getAppointmentType: jest.fn().mockResolvedValue(null),
          getRoutingPolicy: jest.fn(),
          queryEmployees: jest.fn(),
          log: quietLog,
        }
      )
    ).rejects.toThrow('at-missing not found or has no routing_policy_id');
  });

  test('throws when the appointment type is found but has no routing_policy_id', async () => {
    await expect(
      resolveCandidates(
        { tenantId: TENANT, appointmentTypeId: 'at-nopolicy' },
        {
          getAppointmentType: jest.fn().mockResolvedValue({ appointment_type_id: 'at-nopolicy' }),
          getRoutingPolicy: jest.fn(),
          queryEmployees: jest.fn(),
          log: quietLog,
        }
      )
    ).rejects.toThrow('at-nopolicy not found or has no routing_policy_id');
  });

  test('prefers an explicit routingPolicyId over the appointmentType hop', async () => {
    const getAppointmentType = jest.fn();
    await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-explicit', appointmentTypeId: 'at-x' },
      depsWith([], policy([]), { getAppointmentType })
    );
    expect(getAppointmentType).not.toHaveBeenCalled();
  });
});

// ─── isEligible (unit) ───────────────────────────────────────────────────────────────

describe('isEligible', () => {
  test('empty conditions → eligible', () => {
    expect(isEligible(['a'], [])).toBe(true);
  });
  test('equals requires all values', () => {
    expect(isEligible(['a'], [{ values: ['a', 'b'] }])).toBe(false);
    expect(isEligible(['a', 'b'], [{ values: ['a', 'b'] }])).toBe(true);
  });
  test('handles a condition with no values array', () => {
    expect(isEligible(['a'], [{ tag: 'x' }])).toBe(true); // [].every → true
  });
  test('in_any: empty tag list → false; at least one match → true', () => {
    expect(isEligible([], [{ operator: 'in_any', values: ['a', 'b'] }])).toBe(false);
    expect(isEligible(['b'], [{ operator: 'in_any', values: ['a', 'b'] }])).toBe(true);
  });
});

// ─── default DI implementations (aws-sdk-client-mock) ─────────────────────────────────

describe('default implementations — DynamoDB', () => {
  test('defaultGetRoutingPolicy returns the unmarshalled item (incl. nested tag_conditions)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId: { S: TENANT },
        routing_policy_id: { S: 'rp-1' },
        tie_breaker: { S: 'round_robin' },
        tag_conditions: {
          L: [
            {
              M: {
                tag: { S: 'program' },
                operator: { S: 'in_any' },
                values: { L: [{ S: 'mentoring' }, { S: 'spanish' }] },
              },
            },
          ],
        },
      },
    });
    const out = await defaultGetRoutingPolicy({ tenantId: TENANT, routingPolicyId: 'rp-1' });
    expect(out.tie_breaker).toBe('round_robin');
    expect(out.tag_conditions).toEqual([
      { tag: 'program', operator: 'in_any', values: ['mentoring', 'spanish'] },
    ]);
  });

  test('defaultGetRoutingPolicy returns null on miss', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    const out = await defaultGetRoutingPolicy({ tenantId: TENANT, routingPolicyId: 'x' });
    expect(out).toBeNull();
  });

  test('defaultGetAppointmentType returns the unmarshalled item', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId: { S: TENANT },
        appointment_type_id: { S: 'at-1' },
        routing_policy_id: { S: 'rp-7' },
      },
    });
    const out = await defaultGetAppointmentType({ tenantId: TENANT, appointmentTypeId: 'at-1' });
    expect(out.routing_policy_id).toBe('rp-7');
  });

  test('defaultGetAppointmentType returns null on miss', async () => {
    ddbMock.on(GetItemCommand).resolves({});
    expect(
      await defaultGetAppointmentType({ tenantId: TENANT, appointmentTypeId: 'x' })
    ).toBeNull();
  });

  test('defaultQueryEmployees follows pagination across pages', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({
        Items: [
          {
            tenantId: { S: TENANT },
            employeeId: { S: 'u1' },
            email: { S: 'a@org.com' },
            scheduling_tags: { L: [{ S: 'mentoring' }] },
          },
        ],
        LastEvaluatedKey: { tenantId: { S: TENANT }, employeeId: { S: 'u1' } },
      })
      .resolvesOnce({
        Items: [
          {
            tenantId: { S: TENANT },
            employeeId: { S: 'u2' },
            email: { S: 'b@org.com' },
            scheduling_tags: { SS: ['donations'] },
          },
        ],
      });
    const out = await defaultQueryEmployees({ tenantId: TENANT });
    expect(out).toHaveLength(2);
    expect(out[0].scheduling_tags).toEqual(['mentoring']);
    expect(out[1].scheduling_tags).toEqual(['donations']); // SS round-trips to a list
    const calls = ddbMock.commandCalls(QueryCommand);
    expect(calls).toHaveLength(2);
    // page 1 has no ExclusiveStartKey; page 2 forwards page 1's LastEvaluatedKey.
    expect(calls[0].args[0].input.ExclusiveStartKey).toBeUndefined();
    expect(calls[1].args[0].input.ExclusiveStartKey).toEqual({
      tenantId: { S: TENANT },
      employeeId: { S: 'u1' },
    });
    // projects only the three fields the resolver reads (cost + PII minimisation).
    expect(calls[0].args[0].input.ProjectionExpression).toBe(
      'employeeId, #email, scheduling_tags'
    );
    expect(calls[0].args[0].input.ExpressionAttributeNames).toEqual({ '#email': 'email' });
  });

  test('defaultQueryEmployees tolerates an empty partition', async () => {
    ddbMock.on(QueryCommand).resolves({});
    expect(await defaultQueryEmployees({ tenantId: TENANT })).toEqual([]);
  });

  test('end-to-end through the defaults (no injected seam)', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        tenantId: { S: TENANT },
        routing_policy_id: { S: 'rp-1' },
        tag_conditions: { L: [] },
      },
    });
    ddbMock.on(QueryCommand).resolves({
      Items: [
        {
          tenantId: { S: TENANT },
          employeeId: { S: 'u1' },
          email: { S: 'maya@org.com' },
          scheduling_tags: { L: [{ S: 'mentoring' }] },
          active: { BOOL: true },
        },
      ],
    });
    const out = await resolveCandidates(
      { tenantId: TENANT, routingPolicyId: 'rp-1' },
      { log: quietLog }
    );
    expect(out).toEqual([
      { resourceId: 'maya@org.com', scheduling_tags: ['mentoring'], coordinatorEmail: 'maya@org.com' },
    ]);
  });
});

// ─── unmarshalItem (attribute-value coverage) ────────────────────────────────────────

describe('unmarshalItem', () => {
  test('covers S/N/BOOL/NULL/L/M/SS/NS and unknown', () => {
    const out = unmarshalItem({
      s: { S: 'str' },
      n: { N: '42' },
      b: { BOOL: false },
      nul: { NULL: true },
      list: { L: [{ S: 'a' }, { N: '2' }] },
      map: { M: { inner: { S: 'v' } } },
      ss: { SS: ['x', 'y'] },
      ns: { NS: ['1', '2'] },
      unknown: { B: 'ignored' }, // binary not handled → undefined
    });
    expect(out).toEqual({
      s: 'str',
      n: 42,
      b: false,
      nul: null,
      list: ['a', 2],
      map: { inner: 'v' },
      ss: ['x', 'y'],
      ns: [1, 2],
      unknown: undefined,
    });
  });
});
