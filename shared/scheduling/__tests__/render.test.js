'use strict';

const { render } = require('../render');

describe('render (shared {{var}} substitution)', () => {
  test('replaces known vars', () => {
    expect(render('Hi {{firstName}} — {{org}}', { firstName: 'Alex', org: 'Foster Village' }))
      .toBe('Hi Alex — Foster Village');
  });

  test('unknown var renders empty (never a literal {{...}})', () => {
    expect(render('Hi {{firstName}}{{missing}}', { firstName: 'Alex' })).toBe('Hi Alex');
  });

  test('null / undefined value renders empty', () => {
    expect(render('[{{a}}][{{b}}]', { a: null, b: undefined })).toBe('[][]');
  });

  test('falsy-but-present values are coerced, not dropped', () => {
    expect(render('{{n}}/{{flag}}', { n: 0, flag: false })).toBe('0/false');
  });

  test('repeated occurrences all substitute', () => {
    expect(render('{{x}}-{{x}}', { x: 'q' })).toBe('q-q');
  });

  test('no escaping — the caller pre-escapes html-bound vars', () => {
    expect(render('{{v}}', { v: '<b>' })).toBe('<b>');
  });
});
