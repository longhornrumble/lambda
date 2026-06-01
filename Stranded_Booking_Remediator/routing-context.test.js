'use strict';

const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');

// Mock the frozen shared contracts BEFORE requiring routing-context so the real
// shared modules (and their @googleapis/calendar transitive deps) never load.
const mockGetBusy = jest.fn();
const mockEvaluatePool = jest.fn();
jest.mock('../shared/scheduling/availability', () => ({
  getBusyIntervals: (...a) => mockGetBusy(...a),
}));
jest.mock('../shared/scheduling/routing', () => ({
  evaluatePool: (...a) => mockEvaluatePool(...a),
}));

process.env.APPOINTMENT_TYPE_TABLE = 'picasso-appointment-type-staging';
process.env.ROUTING_POLICY_TABLE = 'picasso-routing-policy-staging';

const {
  buildResolveAlternate,
  defaultLoadCandidates,
  loadAppointmentType,
  loadRoutingPolicy,
  overlapsSlot,
} = require('./routing-context');

const ddbMock = mockClient(DynamoDBClient);

beforeEach(() => {
  ddbMock.reset();
  mockGetBusy.mockReset();
  mockEvaluatePool.mockReset();
});

function booking(over = {}) {
  return {
    tenantId: 'TEN1',
    bookingId: 'booking#abc',
    appointmentTypeId: over.appointmentTypeId ?? 'apt-1',
    resourceId: 'res-maya',
    coordinatorEmail: 'maya@org.com',
    startAt: '2026-06-01T15:00:00Z',
    endAt: '2026-06-01T15:30:00Z',
  };
}

// Wire the two GetItem reads: appointment-type → routing_policy_id, then routing-policy.
function wireTables({ apt = { routing_policy_id: { S: 'rp-1' } }, policy = { tie_breaker: { S: 'first_available' } } } = {}) {
  ddbMock
    .on(GetItemCommand, { TableName: 'picasso-appointment-type-staging' })
    .resolves({ Item: apt ? { appointment_type_id: { S: 'apt-1' }, ...apt } : undefined });
  ddbMock
    .on(GetItemCommand, { TableName: 'picasso-routing-policy-staging' })
    .resolves({ Item: policy ? { routing_policy_id: { S: 'rp-1' }, ...policy } : undefined });
}

describe('buildResolveAlternate — happy path', () => {
  test('returns the first ordered alternate free at the slot, excluding the departed coordinator', async () => {
    wireTables();
    const loadCandidates = jest.fn().mockResolvedValue([
      { resourceId: 'res-maya', coordinatorEmail: 'maya@org.com', scheduling_tags: ['program:a'] }, // departed → filtered
      { resourceId: 'res-diego', coordinatorEmail: 'diego@org.com', scheduling_tags: ['program:a'] },
    ]);
    mockGetBusy.mockResolvedValue({ busy: [], cachedAt: 'x', source: 'google_freebusy' }); // diego free
    mockEvaluatePool.mockResolvedValue({ ordered: ['res-diego'], tieBreaker: 'first_available' });

    const resolve = buildResolveAlternate({ loadCandidates });
    const out = await resolve(booking());

    expect(out).toEqual({ resourceId: 'res-diego', coordinatorEmail: 'diego@org.com' });
    // departed coordinator excluded from the candidates handed to evaluatePool
    const poolArg = mockEvaluatePool.mock.calls[0][0];
    expect(poolArg.candidates.map((c) => c.resourceId)).toEqual(['res-diego']);
    // freeBusy queried for the alternate at the booking's exact window
    expect(mockGetBusy).toHaveBeenCalledWith(expect.objectContaining({
      resourceId: 'res-diego', windowStart: '2026-06-01T15:00:00Z', windowEnd: '2026-06-01T15:30:00Z',
    }));
  });

  test('a candidate busy at the slot is excluded (freeBusy entry nulled) → no eligible → null', async () => {
    wireTables();
    const loadCandidates = jest.fn().mockResolvedValue([
      { resourceId: 'res-diego', coordinatorEmail: 'diego@org.com', scheduling_tags: [] },
    ]);
    mockGetBusy.mockResolvedValue({ busy: [{ start: '2026-06-01T15:10:00Z', end: '2026-06-01T15:20:00Z' }] });
    mockEvaluatePool.mockResolvedValue({ ordered: [] }); // evaluatePool drops the nulled entry

    const resolve = buildResolveAlternate({ loadCandidates });
    expect(await resolve(booking())).toBeNull();
    const poolArg = mockEvaluatePool.mock.calls[0][0];
    expect(poolArg.freeBusyByResource['res-diego']).toBeNull();
  });

  test('a freeBusy query failure excludes that candidate (null entry)', async () => {
    wireTables();
    const loadCandidates = jest.fn().mockResolvedValue([
      { resourceId: 'res-diego', coordinatorEmail: 'diego@org.com', scheduling_tags: [] },
    ]);
    mockGetBusy.mockRejectedValue(new Error('oauth expired'));
    mockEvaluatePool.mockResolvedValue({ ordered: [] });

    const resolve = buildResolveAlternate({ loadCandidates });
    expect(await resolve(booking())).toBeNull();
    expect(mockEvaluatePool.mock.calls[0][0].freeBusyByResource['res-diego']).toBeNull();
  });
});

describe('buildResolveAlternate — short-circuits', () => {
  test('missing appointment type → null', async () => {
    wireTables({ apt: null });
    const resolve = buildResolveAlternate({ loadCandidates: jest.fn() });
    expect(await resolve(booking())).toBeNull();
    expect(mockEvaluatePool).not.toHaveBeenCalled();
  });

  test('appointment type without routing_policy_id → null', async () => {
    wireTables({ apt: {} });
    const resolve = buildResolveAlternate({ loadCandidates: jest.fn() });
    expect(await resolve(booking())).toBeNull();
  });

  test('missing routing policy row → null', async () => {
    wireTables({ policy: null });
    const resolve = buildResolveAlternate({ loadCandidates: jest.fn() });
    expect(await resolve(booking())).toBeNull();
  });

  test('roster yields no candidates after excluding the departed coordinator → null', async () => {
    wireTables();
    const loadCandidates = jest.fn().mockResolvedValue([
      { resourceId: 'res-maya', coordinatorEmail: 'maya@org.com', scheduling_tags: [] },
    ]);
    const resolve = buildResolveAlternate({ loadCandidates });
    expect(await resolve(booking())).toBeNull();
    expect(mockGetBusy).not.toHaveBeenCalled();
  });

  test('evaluatePool returns empty order → null', async () => {
    wireTables();
    const loadCandidates = jest.fn().mockResolvedValue([
      { resourceId: 'res-diego', coordinatorEmail: 'diego@org.com', scheduling_tags: [] },
    ]);
    mockGetBusy.mockResolvedValue({ busy: [] });
    mockEvaluatePool.mockResolvedValue({ ordered: [] });
    const resolve = buildResolveAlternate({ loadCandidates });
    expect(await resolve(booking())).toBeNull();
  });

  test('default loadCandidates returns [] (roster seam not wired) → reassign degrades to cancel', async () => {
    wireTables();
    const resolve = buildResolveAlternate(); // no loadCandidates → defaultLoadCandidates
    expect(await resolve(booking())).toBeNull();
  });

  test('defaultLoadCandidates resolves to an empty array', async () => {
    await expect(defaultLoadCandidates('TEN1', {}, {})).resolves.toEqual([]);
  });
});

describe('table parsers', () => {
  test('loadRoutingPolicy parses tag_conditions from a JSON-string attribute', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        routing_policy_id: { S: 'rp-1' },
        tag_conditions: { S: JSON.stringify([{ tag: 'program', operator: 'in_any', values: ['a'] }]) },
        tie_breaker: { S: 'round_robin' },
        last_assigned_resource_id: { S: 'res-x' },
        last_assigned_at: { N: '123' },
      },
    });
    const p = await loadRoutingPolicy('TEN1', 'rp-1');
    expect(p.tag_conditions).toEqual([{ tag: 'program', operator: 'in_any', values: ['a'] }]);
    expect(p.tie_breaker).toBe('round_robin');
    expect(p.last_assigned_resource_id).toBe('res-x');
    expect(p.last_assigned_at).toBe(123);
  });

  test('loadRoutingPolicy parses tag_conditions from a DynamoDB List attribute', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: {
        routing_policy_id: { S: 'rp-1' },
        tag_conditions: { L: [{ S: JSON.stringify({ tag: 'lang', operator: 'equals', values: ['en'] }) }] },
      },
    });
    const p = await loadRoutingPolicy('TEN1', 'rp-1');
    expect(p.tag_conditions).toEqual([{ tag: 'lang', operator: 'equals', values: ['en'] }]);
    expect(p.tie_breaker).toBe('round_robin'); // default
  });

  test('loadRoutingPolicy tolerates a malformed tag_conditions string (→ [])', async () => {
    ddbMock.on(GetItemCommand).resolves({
      Item: { routing_policy_id: { S: 'rp-1' }, tag_conditions: { S: '{not json' } },
    });
    const p = await loadRoutingPolicy('TEN1', 'rp-1');
    expect(p.tag_conditions).toEqual([]);
  });

  test('loadAppointmentType / loadRoutingPolicy return null on a missing id', async () => {
    expect(await loadAppointmentType('TEN1', '')).toBeNull();
    expect(await loadRoutingPolicy('TEN1', null)).toBeNull();
  });
});

describe('overlapsSlot', () => {
  const startMs = Date.parse('2026-06-01T15:00:00Z');
  const endMs = Date.parse('2026-06-01T15:30:00Z');
  test('detects an overlapping busy interval', () => {
    expect(overlapsSlot([{ start: '2026-06-01T15:10:00Z', end: '2026-06-01T15:20:00Z' }], startMs, endMs)).toBe(true);
  });
  test('adjacent (touching) intervals do not overlap', () => {
    expect(overlapsSlot([{ start: '2026-06-01T15:30:00Z', end: '2026-06-01T16:00:00Z' }], startMs, endMs)).toBe(false);
  });
  test('empty/undefined busy → no overlap', () => {
    expect(overlapsSlot([], startMs, endMs)).toBe(false);
    expect(overlapsSlot(undefined, startMs, endMs)).toBe(false);
  });
});
