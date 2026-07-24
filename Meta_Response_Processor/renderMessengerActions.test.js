'use strict';

/**
 * Unit tests for the welcome-chip helpers added alongside the M4 CTA renderer:
 *   buildWelcomeChips  — action_chips.default_chips → welcome quick replies
 *   resolveChipPayload — PIC1:chip:{key} tap → the chip's `value` turn text
 *
 * (renderMessengerActions/resolveCtaPayload themselves are exercised via
 * index.test.js's integration coverage; these two are pure and get direct
 * unit coverage here.)
 */

const { buildWelcomeChips, resolveChipPayload } = require('./renderMessengerActions');
const { QUICK_REPLY_TITLE_MAX, QUICK_REPLY_MAX } = require('./capabilities');

const CHIPS = {
  enabled: true,
  max_display: 6,
  show_on_welcome: true,
  default_chips: {
    how_can_i_volunteer: {
      label: '📖 Learn about Mentoring',
      action: 'send_query',
      value: 'Tell me about your mentoring program.',
    },
    donate: { label: '💸 Make a Donation', action: 'send_query', value: 'What are the ways that I can donate?' },
    contact_us: { label: 'Contact Us', action: 'show_info', value: 'Contact us' },
  },
};

describe('buildWelcomeChips', () => {
  test('returns [] when config or action_chips is absent', () => {
    expect(buildWelcomeChips(undefined)).toEqual([]);
    expect(buildWelcomeChips({})).toEqual([]);
    expect(buildWelcomeChips({ action_chips: {} })).toEqual([]);
  });

  test('returns [] when disabled or not flagged for welcome', () => {
    expect(buildWelcomeChips({ action_chips: { ...CHIPS, enabled: false } })).toEqual([]);
    expect(buildWelcomeChips({ action_chips: { ...CHIPS, show_on_welcome: false } })).toEqual([]);
    // both switches must be strictly true (not just truthy)
    expect(buildWelcomeChips({ action_chips: { ...CHIPS, enabled: 1 } })).toEqual([]);
  });

  test('maps default_chips to quick replies with PIC1:chip payloads', () => {
    const chips = buildWelcomeChips({ action_chips: CHIPS });
    expect(chips).toHaveLength(3);
    expect(chips[0]).toEqual({
      content_type: 'text',
      title: '📖 Learn about Mentoring'.length <= QUICK_REPLY_TITLE_MAX
        ? '📖 Learn about Mentoring'
        : expect.any(String),
      payload: 'PIC1:chip:how_can_i_volunteer',
    });
    expect(chips.map((c) => c.payload)).toEqual([
      'PIC1:chip:how_can_i_volunteer',
      'PIC1:chip:donate',
      'PIC1:chip:contact_us',
    ]);
    // every title obeys the C5 quick-reply title cap
    for (const c of chips) expect(c.title.length).toBeLessThanOrEqual(QUICK_REPLY_TITLE_MAX);
  });

  test('truncates an over-long label to the C5 cap with an ellipsis', () => {
    const chips = buildWelcomeChips({
      action_chips: {
        enabled: true,
        show_on_welcome: true,
        default_chips: { k: { label: 'Request Support for Your Family', value: 'help' } },
      },
    });
    expect(chips[0].title.length).toBeLessThanOrEqual(QUICK_REPLY_TITLE_MAX);
    expect(chips[0].title.endsWith('…')).toBe(true);
  });

  test('honours max_display and hard-caps at QUICK_REPLY_MAX', () => {
    const many = {};
    for (let i = 0; i < QUICK_REPLY_MAX + 5; i++) many[`c${i}`] = { label: `Chip ${i}`, value: `v${i}` };
    const capped = buildWelcomeChips({
      action_chips: { enabled: true, show_on_welcome: true, max_display: 2, default_chips: many },
    });
    expect(capped).toHaveLength(2);

    const noMax = buildWelcomeChips({
      action_chips: { enabled: true, show_on_welcome: true, default_chips: many },
    });
    expect(noMax).toHaveLength(QUICK_REPLY_MAX); // default + hard cap
  });

  test('skips chips with no usable label', () => {
    const chips = buildWelcomeChips({
      action_chips: {
        enabled: true,
        show_on_welcome: true,
        default_chips: {
          good: { label: 'Good', value: 'g' },
          blank: { label: '   ', value: 'b' },
          missing: { value: 'm' },
        },
      },
    });
    expect(chips.map((c) => c.payload)).toEqual(['PIC1:chip:good']);
  });
});

describe('resolveChipPayload', () => {
  test('returns null for non-chip payloads', () => {
    expect(resolveChipPayload('PIC1:cta:donate_generic', { action_chips: CHIPS })).toBeNull();
    expect(resolveChipPayload('free text', { action_chips: CHIPS })).toBeNull();
    expect(resolveChipPayload(undefined, { action_chips: CHIPS })).toBeNull();
  });

  test('returns null for an unknown chip key', () => {
    expect(resolveChipPayload('PIC1:chip:nope', { action_chips: CHIPS })).toBeNull();
  });

  test('resolves to the chip value (the precise query, not the truncated title)', () => {
    expect(resolveChipPayload('PIC1:chip:how_can_i_volunteer', { action_chips: CHIPS })).toEqual({
      key: 'how_can_i_volunteer',
      turnText: 'Tell me about your mentoring program.',
    });
  });

  test('falls back to label then key when value is absent', () => {
    const cfg = { action_chips: { default_chips: { a: { label: 'A label' }, b: {} } } };
    expect(resolveChipPayload('PIC1:chip:a', cfg)).toEqual({ key: 'a', turnText: 'A label' });
    expect(resolveChipPayload('PIC1:chip:b', cfg)).toEqual({ key: 'b', turnText: 'b' });
  });
});
