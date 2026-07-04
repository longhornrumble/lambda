/**
 * Tier-2 eval report renderer (chat-experience eval net, sub-phase 1.3).
 *
 * Pure: turns the runner's scored + baseline-compared results into a markdown
 * report string. No I/O, no live calls — run.js owns writing it to disk/stdout.
 */

'use strict';

const STATUS_ICON = {
  ok: '✅',
  fixed: '🟢',
  new: '🆕',
  regression: '❌',
  stale_baseline: '⚠️',
  error: '💥',
};

function truncate(text, max = 240) {
  const s = String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
  return s.length > max ? `${s.slice(0, max)}…` : s;
}

/**
 * @param {Array<Object>} items - combined scenario results (see run.js buildReportItem)
 * @param {Object} meta - { promptVersions, baselineVersions, generatedAt }
 * @returns {string} markdown
 */
function renderReport(items, meta = {}) {
  const counts = items.reduce((acc, it) => {
    acc[it.status] = (acc[it.status] || 0) + 1;
    return acc;
  }, {});
  const order = ['regression', 'stale_baseline', 'error', 'new', 'fixed', 'ok'];
  const failing = items.filter((it) => it.status === 'regression' || it.status === 'stale_baseline' || it.status === 'error');

  const lines = [];
  lines.push('# Tier-2 eval report');
  lines.push('');
  if (meta.generatedAt) lines.push(`- Generated: ${meta.generatedAt}`);
  if (meta.promptVersions) {
    lines.push(`- Prompt versions: conversation \`${meta.promptVersions.conversation}\`, action_selector \`${meta.promptVersions.action_selector}\``);
  }
  if (meta.baselineVersions) {
    lines.push(`- Baseline versions: conversation \`${meta.baselineVersions.conversation}\`, action_selector \`${meta.baselineVersions.action_selector}\``);
  }
  lines.push(`- Scenarios: ${items.length} — ${order.filter((s) => counts[s]).map((s) => `${STATUS_ICON[s]} ${counts[s]} ${s}`).join(', ') || 'none'}`);
  lines.push('');

  lines.push('| Scenario | Status | Assertions | Notes |');
  lines.push('|---|---|---|---|');
  for (const it of items) {
    const passed = (it.assertions || []).filter((a) => a.pass).length;
    const total = (it.assertions || []).length;
    const note = it.error ? `error: ${truncate(it.error, 80)}` : (it.note || '');
    lines.push(`| ${it.id} | ${STATUS_ICON[it.status] || ''} ${it.status} | ${passed}/${total} | ${note} |`);
  }
  lines.push('');

  if (failing.length) {
    lines.push('## Failing scenarios');
    lines.push('');
    for (const it of failing) {
      lines.push(`### ${STATUS_ICON[it.status]} ${it.id} — ${it.status}`);
      if (it.description) lines.push(`_${it.description}_`);
      if (it.error) lines.push(`- **error:** ${truncate(it.error, 400)}`);
      for (const a of it.assertions || []) {
        if (!a.pass) lines.push(`- ❌ \`${a.type}\` — ${truncate(a.detail, 200)}`);
      }
      if (it.responsePreview) lines.push(`- response: \`${truncate(it.responsePreview, 200)}\``);
      if (Array.isArray(it.ctas)) lines.push(`- ctas: [${it.ctas.join(', ')}]`);
      lines.push('');
    }
  }

  return lines.join('\n');
}

module.exports = { renderReport, truncate, STATUS_ICON };
