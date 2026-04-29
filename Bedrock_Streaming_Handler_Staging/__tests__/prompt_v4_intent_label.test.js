/**
 * intentLabel unit tests — V4 Action Selector vocabulary mapping.
 *
 * Verifies each CTA action type maps to its expected short label, including
 * the scheduling additions (start_scheduling, resume_scheduling → SCHEDULE)
 * and the default-passthrough behavior for unknown actions.
 *
 * Source: prompt_v4.js — function lifted out of selectActionsV4's closure
 * for testability as part of scheduling sub-phase A1.
 */

const { intentLabel } = require('../prompt_v4');

describe('intentLabel', () => {
  test('maps send_query → LEARN', () => {
    expect(intentLabel('send_query')).toBe('LEARN');
  });

  test('maps start_form → APPLY', () => {
    expect(intentLabel('start_form')).toBe('APPLY');
  });

  test('maps external_link → VISIT', () => {
    expect(intentLabel('external_link')).toBe('VISIT');
  });

  test('maps show_info → INFO', () => {
    expect(intentLabel('show_info')).toBe('INFO');
  });

  test('maps start_scheduling → SCHEDULE (scheduling A1)', () => {
    expect(intentLabel('start_scheduling')).toBe('SCHEDULE');
  });

  test('maps resume_scheduling → SCHEDULE (scheduling A1)', () => {
    expect(intentLabel('resume_scheduling')).toBe('SCHEDULE');
  });

  test('falls back to the raw action for unknown action types', () => {
    expect(intentLabel('totally_unknown_action')).toBe('totally_unknown_action');
    expect(intentLabel('')).toBe('');
  });

  test('SCHEDULE is the same label for both scheduling actions', () => {
    // Documents the deliberate intent: start and resume share the same intent
    // label so the AI vocabulary treats them as one bookable behavior.
    expect(intentLabel('start_scheduling')).toBe(intentLabel('resume_scheduling'));
  });
});
