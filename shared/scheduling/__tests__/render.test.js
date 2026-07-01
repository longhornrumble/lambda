'use strict';

const { render, linkHtml } = require('../render');

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

describe('linkHtml (clickable link-token rendering for html bodies)', () => {
  test('https url → an escaped clickable anchor', () => {
    expect(linkHtml('https://sched.example/r/2')).toBe(
      '<a href="https://sched.example/r/2">https://sched.example/r/2</a>'
    );
  });

  test('non-https / empty / non-string → empty (scheme guard, no dangling anchor)', () => {
    expect(linkHtml('http://x')).toBe('');
    expect(linkHtml('javascript:alert(1)')).toBe('');
    expect(linkHtml('')).toBe('');
    expect(linkHtml(null)).toBe('');
    expect(linkHtml(undefined)).toBe('');
  });

  test('href + visible text are entity-escaped', () => {
    expect(linkHtml('https://x/?a=1&b="2"')).toBe(
      '<a href="https://x/?a=1&amp;b=&quot;2&quot;">https://x/?a=1&amp;b=&quot;2&quot;</a>'
    );
  });
});
