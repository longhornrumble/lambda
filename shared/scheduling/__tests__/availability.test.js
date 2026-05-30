'use strict';

/**
 * Tests for shared/scheduling/availability.js (WS-C4, §B1).
 *
 * Two layers (per feedback_testing_rigor — mocks alone are insufficient):
 *  1. Unit: Secrets Manager mocked via aws-sdk-client-mock, Google mocked via jest.mock.
 *  2. Integration (gated by FREEBUSY_INTEGRATION=1): the SAME module against the REAL
 *     Google freeBusy API + real Secrets Manager. The mock factories below no-op into
 *     the real modules when that env flag is set, so one file covers both layers.
 *
 * Run the integration layer:
 *   FREEBUSY_INTEGRATION=1 \
 *   FREEBUSY_INTEGRATION_TENANT_ID=<tenant> \
 *   FREEBUSY_INTEGRATION_COORDINATOR_ID=<coordinator-secret-id> \
 *   AWS_PROFILE=<profile-with-secrets-access> npm test
 */

const INTEGRATION = !!process.env.FREEBUSY_INTEGRATION;

jest.mock('@googleapis/calendar', () => {
  if (process.env.FREEBUSY_INTEGRATION) {
    return jest.requireActual('@googleapis/calendar');
  }
  const query = jest.fn();
  const client = { freebusy: { query } };
  return { calendar: jest.fn(() => client) };
});

jest.mock('google-auth-library', () => {
  if (process.env.FREEBUSY_INTEGRATION) {
    return jest.requireActual('google-auth-library');
  }
  return {
    OAuth2Client: jest.fn().mockImplementation(() => ({
      setCredentials: jest.fn(),
    })),
  };
});

const { mockClient } = require('aws-sdk-client-mock');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');
const calendarApi = require('@googleapis/calendar');

const availability = require('../availability');

const describeUnit = INTEGRATION ? describe.skip : describe;
const describeIntegration = INTEGRATION ? describe : describe.skip;

const SECRET = {
  client_id: 'cid',
  client_secret: 'csecret',
  refresh_token: 'rtok',
  coordinator_email: 'maya@tenant-a.org',
};

const WINDOW = {
  windowStart: '2026-06-03T00:00:00Z',
  windowEnd: '2026-06-04T00:00:00Z',
};

function freeBusyResponse(calendarId, busy) {
  return { data: { calendars: { [calendarId]: { busy } } } };
}

describeUnit('availability.getBusyIntervals (mocked Google + Secrets)', () => {
  let smMock;
  let freebusyQuery;

  beforeEach(() => {
    availability._resetCacheForTests();
    smMock = mockClient(SecretsManagerClient);
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify(SECRET) });
    freebusyQuery = calendarApi.calendar().freebusy.query;
    freebusyQuery.mockReset();
    freebusyQuery.mockResolvedValue(
      freeBusyResponse('maya@tenant-a.org', [
        { start: '2026-06-03T14:00:00Z', end: '2026-06-03T15:00:00Z' },
      ])
    );
  });

  afterEach(() => {
    smMock.restore();
    jest.restoreAllMocks();
  });

  it('returns the §B1 shape: busy[], ISO cachedAt, source=google_freebusy', async () => {
    const now = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(now);

    const res = await availability.getBusyIntervals({
      tenantId: 'TEN-A',
      resourceId: 'res-1',
      coordinatorId: 'maya@tenant-a.org',
      ...WINDOW,
    });

    expect(res).toEqual({
      busy: [{ start: '2026-06-03T14:00:00Z', end: '2026-06-03T15:00:00Z' }],
      cachedAt: new Date(now).toISOString(),
      source: 'google_freebusy',
    });
    // freeBusy queried the coordinator's calendar over the requested window.
    expect(freebusyQuery).toHaveBeenCalledWith({
      auth: expect.anything(),
      requestBody: {
        timeMin: WINDOW.windowStart,
        timeMax: WINDOW.windowEnd,
        items: [{ id: 'maya@tenant-a.org' }],
      },
    });
  });

  it('caches for 60s (TTL), re-queries only after the window expires', async () => {
    const t0 = 1_700_000_000_000;
    const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(t0);
    const args = { tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW };

    await availability.getBusyIntervals(args);
    expect(freebusyQuery).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(t0 + 59_000); // within 60s → cache hit
    await availability.getBusyIntervals(args);
    expect(freebusyQuery).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(t0 + 61_000); // past 60s → re-query
    await availability.getBusyIntervals(args);
    expect(freebusyQuery).toHaveBeenCalledTimes(2);
  });

  it('SECURITY P2 — isolates cache per tenant: same coordinator, two tenants, no leak', async () => {
    const busyA = [{ start: '2026-06-03T14:00:00Z', end: '2026-06-03T15:00:00Z' }];
    const busyB = [{ start: '2026-06-03T16:00:00Z', end: '2026-06-03T17:00:00Z' }];
    smMock
      .on(GetSecretValueCommand)
      .resolves({ SecretString: JSON.stringify({ ...SECRET, coordinator_email: 'maya@shared.org' }) });
    freebusyQuery
      .mockReset()
      .mockResolvedValueOnce(freeBusyResponse('maya@shared.org', busyA))
      .mockResolvedValueOnce(freeBusyResponse('maya@shared.org', busyB));

    const shared = { resourceId: 'r', coordinatorId: 'maya@shared.org', ...WINDOW };

    const resA = await availability.getBusyIntervals({ tenantId: 'TEN-A', ...shared });
    const resB = await availability.getBusyIntervals({ tenantId: 'TEN-B', ...shared });

    // Tenant B got its OWN freeBusy result — not tenant A's cached entry.
    expect(resA.busy).toEqual(busyA);
    expect(resB.busy).toEqual(busyB);
    expect(freebusyQuery).toHaveBeenCalledTimes(2);

    // Re-reading tenant A still returns A's data from cache (B did not overwrite it).
    const resA2 = await availability.getBusyIntervals({ tenantId: 'TEN-A', ...shared });
    expect(resA2.busy).toEqual(busyA);
    expect(freebusyQuery).toHaveBeenCalledTimes(2);

    // Each tenant fetched its OWN tenant-prefixed secret path.
    const paths = smMock
      .commandCalls(GetSecretValueCommand)
      .map((c) => c.args[0].input.SecretId);
    expect(paths).toContain('picasso/scheduling/oauth/TEN-A/maya@shared.org');
    expect(paths).toContain('picasso/scheduling/oauth/TEN-B/maya@shared.org');
  });

  it('keys distinct windows as distinct cache entries', async () => {
    const base = { tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org' };
    await availability.getBusyIntervals({ ...base, windowStart: '2026-06-03T00:00:00Z', windowEnd: '2026-06-04T00:00:00Z' });
    await availability.getBusyIntervals({ ...base, windowStart: '2026-06-04T00:00:00Z', windowEnd: '2026-06-05T00:00:00Z' });
    expect(freebusyQuery).toHaveBeenCalledTimes(2);
  });

  it('invalidate() drops every cached window for one coordinator, leaving other tenants', async () => {
    const a = { tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org' };
    const b = { tenantId: 'TEN-B', resourceId: 'r', coordinatorId: 'maya@tenant-a.org' };
    await availability.getBusyIntervals({ ...a, windowStart: '2026-06-03T00:00:00Z', windowEnd: '2026-06-04T00:00:00Z' });
    await availability.getBusyIntervals({ ...a, windowStart: '2026-06-04T00:00:00Z', windowEnd: '2026-06-05T00:00:00Z' });
    await availability.getBusyIntervals({ ...b, windowStart: '2026-06-03T00:00:00Z', windowEnd: '2026-06-04T00:00:00Z' });
    expect(freebusyQuery).toHaveBeenCalledTimes(3);

    availability.invalidate('TEN-A', 'maya@tenant-a.org');

    // Tenant A both windows re-query (2 more); tenant B still cached (no new query).
    await availability.getBusyIntervals({ ...a, windowStart: '2026-06-03T00:00:00Z', windowEnd: '2026-06-04T00:00:00Z' });
    await availability.getBusyIntervals({ ...a, windowStart: '2026-06-04T00:00:00Z', windowEnd: '2026-06-05T00:00:00Z' });
    await availability.getBusyIntervals({ ...b, windowStart: '2026-06-03T00:00:00Z', windowEnd: '2026-06-04T00:00:00Z' });
    expect(freebusyQuery).toHaveBeenCalledTimes(5);
  });

  it('invalidate() with missing args is a no-op (no throw)', () => {
    expect(() => availability.invalidate()).not.toThrow();
    expect(() => availability.invalidate('TEN-A')).not.toThrow();
  });

  it('falls back to coordinatorId as the calendar id when secret has no coordinator_email', async () => {
    const { coordinator_email, ...noEmail } = SECRET; // eslint-disable-line no-unused-vars
    smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify(noEmail) });
    freebusyQuery.mockReset().mockResolvedValue(freeBusyResponse('coord-123', []));

    const res = await availability.getBusyIntervals({
      tenantId: 'TEN-A',
      resourceId: 'r',
      coordinatorId: 'coord-123',
      ...WINDOW,
    });

    expect(res.busy).toEqual([]);
    expect(freebusyQuery).toHaveBeenCalledWith(
      expect.objectContaining({ requestBody: expect.objectContaining({ items: [{ id: 'coord-123' }] }) })
    );
  });

  it('returns [] when the calendar has no busy intervals', async () => {
    freebusyQuery.mockReset().mockResolvedValue(freeBusyResponse('maya@tenant-a.org', []));
    const res = await availability.getBusyIntervals({
      tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW,
    });
    expect(res.busy).toEqual([]);
  });

  it('returns [] when the calendar block is entirely absent from the response', async () => {
    freebusyQuery.mockReset().mockResolvedValue({ data: { calendars: {} } });
    const res = await availability.getBusyIntervals({
      tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW,
    });
    expect(res.busy).toEqual([]);
  });

  it('throws when freeBusy returns calendar-level errors (caller DLQs/alarms)', async () => {
    freebusyQuery.mockReset().mockResolvedValue({
      data: { calendars: { 'maya@tenant-a.org': { errors: [{ reason: 'notFound' }] } } },
    });
    await expect(
      availability.getBusyIntervals({ tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW })
    ).rejects.toThrow(/calendar errors: notFound/);
  });

  it('labels a reasonless calendar error as "unknown"', async () => {
    freebusyQuery.mockReset().mockResolvedValue({
      data: { calendars: { 'maya@tenant-a.org': { errors: [{}] } } },
    });
    await expect(
      availability.getBusyIntervals({ tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW })
    ).rejects.toThrow(/calendar errors: unknown/);
  });

  it('returns a frozen result so a cache-hit consumer cannot corrupt the shared entry', async () => {
    const res = await availability.getBusyIntervals({
      tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW,
    });
    expect(Object.isFrozen(res)).toBe(true);
    expect(Object.isFrozen(res.busy)).toBe(true);
  });

  describe('input validation', () => {
    it.each([
      ['tenantId', { resourceId: 'r', coordinatorId: 'c', ...WINDOW }],
      ['coordinatorId', { tenantId: 'TEN-A', resourceId: 'r', ...WINDOW }],
      ['windowStart', { tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'c', windowEnd: WINDOW.windowEnd }],
      ['windowEnd', { tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'c', windowStart: WINDOW.windowStart }],
    ])('throws when %s is missing', async (_field, args) => {
      await expect(availability.getBusyIntervals(args)).rejects.toThrow(/required/);
    });
  });

  describe('secret handling (no path leak in errors)', () => {
    const PATH = 'picasso/scheduling/oauth/TEN-A/maya@tenant-a.org';

    it('throws without leaking the secret path when SecretString is absent', async () => {
      smMock.on(GetSecretValueCommand).resolves({});
      const err = await availability
        .getBusyIntervals({ tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW })
        .catch((e) => e);
      expect(err.message).toMatch(/no SecretString/);
      expect(err.message).not.toContain(PATH);
    });

    it('throws on non-JSON secret without leaking the path', async () => {
      smMock.on(GetSecretValueCommand).resolves({ SecretString: 'not-json{' });
      const err = await availability
        .getBusyIntervals({ tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW })
        .catch((e) => e);
      expect(err.message).toMatch(/not valid JSON/);
      expect(err.message).not.toContain(PATH);
    });

    it('throws when a required secret field is missing/empty', async () => {
      smMock.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ ...SECRET, refresh_token: '' }) });
      await expect(
        availability.getBusyIntervals({ tenantId: 'TEN-A', resourceId: 'r', coordinatorId: 'maya@tenant-a.org', ...WINDOW })
      ).rejects.toThrow(/refresh_token/);
    });
  });
});

describeUnit('availability — env configuration', () => {
  it('honors FREEBUSY_CACHE_TTL_MS and OAUTH_SECRET_PATH_PREFIX overrides', () => {
    jest.isolateModules(() => {
      const prev = {
        ttl: process.env.FREEBUSY_CACHE_TTL_MS,
        prefix: process.env.OAUTH_SECRET_PATH_PREFIX,
      };
      process.env.FREEBUSY_CACHE_TTL_MS = '5000';
      process.env.OAUTH_SECRET_PATH_PREFIX = 'custom/oauth';
      const mod = require('../availability');
      expect(mod._CACHE_TTL_MS).toBe(5000);
      expect(mod.buildSecretPath('T', 'c@x.org')).toBe('custom/oauth/T/c@x.org');
      if (prev.ttl === undefined) delete process.env.FREEBUSY_CACHE_TTL_MS;
      else process.env.FREEBUSY_CACHE_TTL_MS = prev.ttl;
      if (prev.prefix === undefined) delete process.env.OAUTH_SECRET_PATH_PREFIX;
      else process.env.OAUTH_SECRET_PATH_PREFIX = prev.prefix;
    });
  });

  it('builds the canonical default secret path', () => {
    expect(availability.buildSecretPath('TEN-A', 'maya@tenant-a.org')).toBe(
      'picasso/scheduling/oauth/TEN-A/maya@tenant-a.org'
    );
  });
});

describeIntegration('availability — integration (real Google freeBusy)', () => {
  it('returns real busy intervals for a provisioned coordinator', async () => {
    const tenantId = process.env.FREEBUSY_INTEGRATION_TENANT_ID;
    const coordinatorId = process.env.FREEBUSY_INTEGRATION_COORDINATOR_ID;
    if (!tenantId || !coordinatorId) {
      throw new Error(
        'Set FREEBUSY_INTEGRATION_TENANT_ID and FREEBUSY_INTEGRATION_COORDINATOR_ID to run the integration test'
      );
    }
    availability._resetCacheForTests();
    const windowStart = new Date().toISOString();
    const windowEnd = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();

    const res = await availability.getBusyIntervals({
      tenantId,
      resourceId: coordinatorId,
      coordinatorId,
      windowStart,
      windowEnd,
    });

    expect(res.source).toBe('google_freebusy');
    expect(typeof res.cachedAt).toBe('string');
    expect(Array.isArray(res.busy)).toBe(true);
    res.busy.forEach((b) => {
      expect(typeof b.start).toBe('string');
      expect(typeof b.end).toBe('string');
    });
  }, 30000);
});
