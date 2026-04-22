/**
 * Marker-based KB markdown mutations.
 *
 * Every operation anchors to HTML comment markers the scanner injects during processing:
 *   <!-- section: events -->          — above each H2, for `afterMarker` append anchors
 *   <!-- source: https://... -->      — above each source-traceable H3, for replace/remove
 *
 * The Applier never searches by heading text or line number. Markers are stable; text drifts.
 */

/**
 * Append markdown content after the line containing `afterMarker`. If the marker isn't found,
 * throw — we do not fall back to end-of-file, which would silently put new content in the
 * wrong section.
 */
export function appendAfterMarker(content, afterMarker, newMarkdown) {
  const idx = content.indexOf(afterMarker);
  if (idx === -1) {
    throw new Error(`Marker not found: ${afterMarker}`);
  }
  const lineEnd = content.indexOf('\n', idx);
  const insertAt = lineEnd === -1 ? content.length : lineEnd + 1;

  // Ensure exactly one blank line between the marker block and the new content.
  const before = content.slice(0, insertAt);
  const after = content.slice(insertAt);
  const separator = before.endsWith('\n\n') ? '' : (before.endsWith('\n') ? '\n' : '\n\n');
  const trailing = newMarkdown.endsWith('\n') ? '' : '\n';
  return before + separator + newMarkdown + trailing + after;
}

/**
 * Find the block owned by `sourceMarker` and return [start, end) character offsets.
 * The block runs from the source marker line through (but not including) the next
 * `<!-- source:` or `<!-- section:` marker — or EOF.
 *
 * Returns null if the marker isn't present.
 */
function findSourceBlock(content, sourceMarker) {
  const start = content.indexOf(sourceMarker);
  if (start === -1) return null;

  const afterStart = start + sourceMarker.length;
  // Search for the next sibling marker that terminates this block.
  const nextSource = content.indexOf('<!-- source:', afterStart);
  const nextSection = content.indexOf('<!-- section:', afterStart);

  const candidates = [nextSource, nextSection].filter(i => i !== -1);
  const end = candidates.length === 0 ? content.length : Math.min(...candidates);

  return { start, end };
}

/**
 * Replace the block owned by `sourceMarker` with `newMarkdown`. The marker line itself is
 * preserved so the block remains source-traceable after the replacement.
 */
export function replaceBySourceMarker(content, sourceMarker, newMarkdown) {
  const block = findSourceBlock(content, sourceMarker);
  if (!block) {
    throw new Error(`Source marker not found: ${sourceMarker}`);
  }
  const before = content.slice(0, block.start);
  const after = content.slice(block.end);
  const body = newMarkdown.endsWith('\n') ? newMarkdown : newMarkdown + '\n';

  // Keep the source marker on its own line above the new body.
  return before + sourceMarker + '\n' + body + after;
}

/**
 * Remove the block owned by `sourceMarker` entirely, including the marker line.
 *
 * Whitespace rule: we only strip a leading newline if doing so would still leave the boundary
 * with a well-formed blank-line separator. Specifically, we strip one `\n` iff the char before
 * is `\n` AND the char after the block end is also `\n` — collapsing `...prev\n\n<block>\n\nnext...`
 * to `...prev\n\nnext...`. If the block sits directly between two markers with no extra
 * spacing, we leave the newlines alone — preventing the bug where consecutive removes collapse
 * `<!-- section: x -->\n\n<block>\n<next-marker>` into `<!-- section: x --><next-marker>`.
 */
export function removeBySourceMarker(content, sourceMarker) {
  const block = findSourceBlock(content, sourceMarker);
  if (!block) {
    throw new Error(`Source marker not found: ${sourceMarker}`);
  }
  let start = block.start;
  const end = block.end;
  if (
    start > 0 &&
    content[start - 1] === '\n' &&
    end < content.length &&
    content[end] === '\n'
  ) {
    start -= 1;
  }
  return content.slice(0, start) + content.slice(end);
}
