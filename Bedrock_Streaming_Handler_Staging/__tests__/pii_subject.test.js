/**
 * Tests for pii_subject.js (BSH port of Master_Function_Staging/pii_subject.py).
 *
 * Goal: behavioral parity with the Python module. The Python module has its own
 * test suite; these tests cover the same surfaces in JS and pin down the
 * race-handling + best-effort fallback semantics M1.G6 / F-DSAR18 depends on.
 */

const { mockClient } = require('aws-sdk-client-mock');
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');

const docClientMock = mockClient(DynamoDBDocumentClient);

// Stand in for the real DynamoDBDocumentClient instance the caller would pass.
// Caller-injection pattern matches form_handler.js's existing pattern.
const fakeDocClient = { send: (...args) => docClientMock.send(...args) };

beforeEach(() => {
  docClientMock.reset();
});

const {
  mintPiiSubjectId,
  normalizeEmail,
  extractEmail,
  getOrCreatePiiSubjectId,
  PII_SUBJECT_INDEX_TABLE,
} = require('../pii_subject');

describe('mintPiiSubjectId', () => {
  test('returns 37-char "psub_" + 32-hex format matching Python', () => {
    const sid = mintPiiSubjectId();
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
  });

  test('returns distinct ids across calls', () => {
    const a = mintPiiSubjectId();
    const b = mintPiiSubjectId();
    expect(a).not.toEqual(b);
  });
});

describe('normalizeEmail', () => {
  test('null/undefined/empty → null', () => {
    expect(normalizeEmail(null)).toBeNull();
    expect(normalizeEmail(undefined)).toBeNull();
    expect(normalizeEmail('')).toBeNull();
    expect(normalizeEmail('   ')).toBeNull();
  });

  test('internal whitespace → null (R1 from Python)', () => {
    expect(normalizeEmail('foo bar@example.com')).toBeNull();
    expect(normalizeEmail('foo@exa mple.com')).toBeNull();
  });

  test('no @-sign → null', () => {
    expect(normalizeEmail('foo')).toBeNull();
  });

  test('multi-@ → null (multi-@ is malformed per Python audit 2026-05-18 #6)', () => {
    expect(normalizeEmail('foo@@bar.com')).toBeNull();
    expect(normalizeEmail('a@b@c.com')).toBeNull();
  });

  test('lowercases domain and local part for non-Gmail', () => {
    expect(normalizeEmail('Foo.BAR@Example.COM')).toBe('foo.bar@example.com');
  });

  test('Gmail dot/plus rules collapse aliases', () => {
    expect(normalizeEmail('foo.bar@gmail.com')).toBe('foobar@gmail.com');
    expect(normalizeEmail('foo+test@gmail.com')).toBe('foo@gmail.com');
    expect(normalizeEmail('foo.bar+test@gmail.com')).toBe('foobar@gmail.com');
  });

  test('googlemail.com is collapsed to gmail.com (same provider)', () => {
    expect(normalizeEmail('foo@googlemail.com')).toBe('foo@gmail.com');
    expect(normalizeEmail('foo.bar+x@googlemail.com')).toBe('foobar@gmail.com');
  });

  test('non-Gmail dots/plus are PRESERVED (audit 2026-05-18 #6 option A)', () => {
    // Plus-tagging is provider-specific; only Gmail is documented to deliver
    // every variant to one inbox. Stripping for other providers would create
    // an imposter-deletion vector. Test guards against regression.
    expect(normalizeEmail('foo.bar@example.com')).toBe('foo.bar@example.com');
    expect(normalizeEmail('foo+tag@example.com')).toBe('foo+tag@example.com');
  });

  test('after stripping dots, empty local Gmail → null', () => {
    expect(normalizeEmail('.@gmail.com')).toBeNull();
  });
});

describe('extractEmail', () => {
  test('non-dict input → null', () => {
    expect(extractEmail(null)).toBeNull();
    expect(extractEmail(undefined)).toBeNull();
    expect(extractEmail('foo@example.com')).toBeNull();
  });

  test('prefers email-named key over any-value scan', () => {
    const r = { email: 'a@example.com', notes: 'b@example.com' };
    expect(extractEmail(r)).toBe('a@example.com');
  });

  test('case-insensitive key matching', () => {
    expect(extractEmail({ Email: 'a@example.com' })).toBe('a@example.com');
    expect(extractEmail({ EMAIL_ADDRESS: 'a@example.com' })).toBe('a@example.com');
  });

  test('falls back to first email-shaped value', () => {
    const r = { notes: 'reach me at hello@example.com', other: 'stuff' };
    // notes value "reach me at hello@example.com" doesn't pass EMAIL_RE
    // (whole-string match). first email-shaped value scan returns nothing
    // since no value is purely an email. Verify.
    expect(extractEmail(r)).toBeNull();
  });

  test('falls back to first standalone email value', () => {
    const r = { notes: 'first note', other: 'me@example.com' };
    expect(extractEmail(r)).toBe('me@example.com');
  });

  test('matched key with non-email value falls through to value scan', () => {
    const r = { email: 'not-an-email', name: 'me@example.com' };
    expect(extractEmail(r)).toBe('me@example.com');
  });
});

describe('getOrCreatePiiSubjectId — index hit (existing subject)', () => {
  test('returns existing sid when index has an entry', async () => {
    docClientMock.on(GetCommand).resolves({
      Item: { pii_subject_id: 'psub_existing0000000000000000000' },
    });
    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toBe('psub_existing0000000000000000000');
    expect(docClientMock.commandCalls(PutCommand).length).toBe(0);
  });
});

describe('getOrCreatePiiSubjectId — index miss (new subject)', () => {
  test('mints + indexes when no existing entry', async () => {
    docClientMock.on(GetCommand).resolves({});  // no Item
    docClientMock.on(PutCommand).resolves({});

    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);

    const puts = docClientMock.commandCalls(PutCommand);
    expect(puts.length).toBe(1);
    const putItem = puts[0].args[0].input.Item;
    expect(putItem.tenant_id).toBe('TEN');
    expect(putItem.normalized_email).toBe('a@example.com');
    expect(putItem.pii_subject_id).toBe(sid);
    expect(typeof putItem.created_at).toBe('string');

    const condExpr = puts[0].args[0].input.ConditionExpression;
    expect(condExpr).toBe('attribute_not_exists(normalized_email)');
  });
});

describe('getOrCreatePiiSubjectId — best-effort fallbacks', () => {
  test('no email in responses → returns minted candidate (UNINDEXED)', async () => {
    const sid = await getOrCreatePiiSubjectId('TEN', { name: 'just name' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
    // Neither GET nor PUT called when no normalized email available
    expect(docClientMock.commandCalls(GetCommand).length).toBe(0);
    expect(docClientMock.commandCalls(PutCommand).length).toBe(0);
  });

  test('DDB GET error → returns minted candidate (UNINDEXED, never throws)', async () => {
    docClientMock.on(GetCommand).rejects(new Error('boom'));
    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
  });

  test('docClient missing → returns minted candidate (UNINDEXED, never throws)', async () => {
    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {});
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
  });
});

describe('getOrCreatePiiSubjectId — race handling', () => {
  test('CCF on first PUT → re-reads + returns winner sid', async () => {
    let getCallCount = 0;
    docClientMock.on(GetCommand).callsFake(() => {
      getCallCount++;
      if (getCallCount === 1) return Promise.resolve({});  // no Item
      // Second GET (after race loss) returns the winner's sid
      return Promise.resolve({
        Item: { pii_subject_id: 'psub_winner00000000000000000000' },
      });
    });
    const ccfErr = new Error('ConditionalCheckFailed');
    ccfErr.name = 'ConditionalCheckFailedException';
    docClientMock.on(PutCommand).rejects(ccfErr);

    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toBe('psub_winner00000000000000000000');
    expect(getCallCount).toBe(2);
    // Second GET must use ConsistentRead=true per the loop pattern
    const secondGet = docClientMock.commandCalls(GetCommand)[1].args[0].input;
    expect(secondGet.ConsistentRead).toBe(true);
  });

  test('unresolved race after 3 attempts → returns minted candidate (UNINDEXED)', async () => {
    docClientMock.on(GetCommand).resolves({});  // never finds existing
    const ccfErr = new Error('ConditionalCheckFailed');
    ccfErr.name = 'ConditionalCheckFailedException';
    docClientMock.on(PutCommand).rejects(ccfErr);

    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
    // Confirms 3 attempts of GET+PUT before giving up
    expect(docClientMock.commandCalls(GetCommand).length).toBe(3);
    expect(docClientMock.commandCalls(PutCommand).length).toBe(3);
  });

  test('non-CCF PUT error → fallback to UNINDEXED candidate, never throws', async () => {
    docClientMock.on(GetCommand).resolves({});
    docClientMock.on(PutCommand).rejects(new Error('throttled'));
    const sid = await getOrCreatePiiSubjectId('TEN', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
  });
});

describe('getOrCreatePiiSubjectId — knownEmail optimization', () => {
  test('uses knownEmail when provided (avoids extractEmail re-walk)', async () => {
    docClientMock.on(GetCommand).resolves({});
    docClientMock.on(PutCommand).resolves({});
    await getOrCreatePiiSubjectId('TEN', { /* no email key */ }, {
      docClient: fakeDocClient,
      knownEmail: 'Pre.Known@Gmail.Com',
    });
    const put = docClientMock.commandCalls(PutCommand)[0].args[0].input;
    // Normalizes via the same pipeline (Gmail dot-strip applies)
    expect(put.Item.normalized_email).toBe('preknown@gmail.com');
  });
});

describe('PII_SUBJECT_INDEX_TABLE default', () => {
  test('defaults to picasso-pii-subject-index-staging when env var unset', () => {
    expect(PII_SUBJECT_INDEX_TABLE).toBe('picasso-pii-subject-index-staging');
  });
});

// Sprint E1 / audit blocker B1 — cross-tenant collision guard.
describe('getOrCreatePiiSubjectId — tenant_id guard (audit blocker B1)', () => {
  test('tenant_id="unknown" → UNINDEXED candidate, no DDB calls', async () => {
    const sid = await getOrCreatePiiSubjectId('unknown', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
    expect(docClientMock.commandCalls(GetCommand).length).toBe(0);
    expect(docClientMock.commandCalls(PutCommand).length).toBe(0);
  });

  test('tenant_id null → UNINDEXED candidate, no DDB calls', async () => {
    const sid = await getOrCreatePiiSubjectId(null, { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
    expect(docClientMock.commandCalls(GetCommand).length).toBe(0);
  });

  test('tenant_id "" → UNINDEXED candidate, no DDB calls', async () => {
    const sid = await getOrCreatePiiSubjectId('', { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
    expect(docClientMock.commandCalls(GetCommand).length).toBe(0);
  });
});

// Sprint F1 / audit-of-audit finding 1 — case/whitespace normalization
describe('getOrCreatePiiSubjectId — tenant_id normalization (audit-of-audit F1)', () => {
  test.each([
    ['Unknown',       'capital U'],
    ['UNKNOWN',       'all caps'],
    ['uNkNoWn',       'mixed case'],
    [' unknown ',     'whitespace padding'],
    ['\nunknown\n',   'newline padding'],
    ['  ',            'whitespace-only'],
  ])('tenant_id %j (%s) → UNINDEXED, no DDB calls', async (bypassAttempt, _label) => {
    const sid = await getOrCreatePiiSubjectId(bypassAttempt, { email: 'a@example.com' }, {
      docClient: fakeDocClient,
    });
    expect(sid).toMatch(/^psub_[0-9a-f]{32}$/);
    expect(docClientMock.commandCalls(GetCommand).length).toBe(0);
    expect(docClientMock.commandCalls(PutCommand).length).toBe(0);
  });
});
