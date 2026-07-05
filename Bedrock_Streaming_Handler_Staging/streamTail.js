/**
 * V5 single-pass turn — stream-tail action-block parser (pure module).
 *
 * In the V5 architecture (docs/roadmap/V5_SINGLE_PASS_TURN_PLAN.md, picasso
 * repo) the response model emits the reply prose and then a marked action
 * block at the stream tail:
 *
 *     ...final prose sentence.
 *     <<<ACTIONS ["query_discoverysession","apply_daretodream_volunteer"]>>>
 *
 * SENTINEL SPEC (decided V5.1; single source of truth for the V5.2 prompt
 * text and the V5.5 wiring):
 *   - Open marker: `<<<ACTIONS` (exact, case-sensitive).
 *   - Then optional inline whitespace, a JSON array of strings, optional
 *     inline whitespace, then the close marker `>>>`.
 *   - Single line: a newline between open and close markers disqualifies
 *     the block (it is not a sentinel).
 *   - `<<<ACTIONS []>>>` (empty array) is valid and expected to be common —
 *     it means "no buttons this turn" (restraint), distinct from a missing
 *     or malformed tail (which signals the caller's fallback ladder).
 *   - Constraint on the id vocabulary (holds for all real configs, which use
 *     snake_case ids): ids must not contain `>>>` or newlines.
 *
 * The parser is a chunk-feed state machine that solves the holdback problem:
 * never forward text that could still turn out to be sentinel, never leak
 * sentinel text to the client, and never drop legitimate prose.
 *
 * Invariants (by construction):
 *   1. NO LEAK — text returned by feed() never contains the open marker, and
 *      never ends with a live prefix of it. Once the full open marker is
 *      seen, nothing from the marker onward is released except text that
 *      provably follows a closed or diverged block (which is re-scanned as
 *      prose).
 *   2. NO SWALLOW — for spec-compliant streams (prose never contains the
 *      literal open marker), nothing is dropped: prose is only ever delayed,
 *      by at most SENTINEL_OPEN.length - 1 chars, and end() releases the
 *      remainder in full. Text is dropped ONLY between a literal full open
 *      marker and its close/divergence point — machine-intent text by
 *      definition.
 *   3. CHUNKING-INVARIANCE — every state transition (marker match, newline
 *      divergence, close, end) is determined by the accumulated content, not
 *      by chunk boundaries, so the released text and parse result are
 *      identical for any chunking of the same stream. Outside a block the
 *      held suffix is at most SENTINEL_OPEN.length - 1 chars; inside a block,
 *      newline divergence bounds a spurious marker's swallow to one line
 *      (memory is O(response), same as the existing responseBuffer).
 *
 * Deliberately NOT here: id validation against config, the 4-CTA cap, and
 * fallback selection — those are selection policy and belong to the caller
 * (V5.5 reuses selectActionsV4's validation).
 */

'use strict';

const SENTINEL_OPEN = '<<<ACTIONS';
const SENTINEL_CLOSE = '>>>';

/**
 * Longest suffix of `s` that is a proper prefix of SENTINEL_OPEN — the text
 * that must be held back because the next chunk could complete the marker.
 */
function liveMarkerPrefixLen(s) {
  const max = Math.min(s.length, SENTINEL_OPEN.length - 1);
  for (let k = max; k > 0; k--) {
    if (s.endsWith(SENTINEL_OPEN.slice(0, k))) return k;
  }
  return 0;
}

/**
 * Parse the text between the open and close markers. Returns an array of
 * strings, or null if the content is not a single-line JSON string array.
 */
function parseBlockContent(content) {
  let parsed;
  try {
    parsed = JSON.parse(content.trim());
  } catch (e) {
    return null;
  }
  if (!Array.isArray(parsed)) return null;
  if (!parsed.every((x) => typeof x === 'string')) return null;
  return parsed;
}

/**
 * createTailParser() → { feed, end }
 *
 *   feed(chunk: string): string
 *     Text safe to forward to the client now (possibly '').
 *
 *   end(): { remainingText, actionIds, status, trailingAfterClose }
 *     remainingText — held text that turned out to be prose; forward it.
 *     actionIds     — parsed ids when the LAST marker attempt was valid
 *                     (possibly []); null otherwise.
 *     status        — 'actions'     the last marker attempt parsed cleanly
 *                     'no_sentinel' the open marker never appeared
 *                     'malformed'   the last marker attempt failed to parse
 *                     (drives the V5.5 fail-soft ladder + failure counter)
 *     trailingAfterClose — true when non-whitespace prose followed a valid
 *                     close (out-of-spec model output, forwarded to the user;
 *                     invisible to `status`, surfaced separately so the
 *                     staging health counter can see this defect class).
 *
 * THE LAST MARKER ATTEMPT DECIDES (2026-07-05 retrospective review, blocker
 * #1). If a valid sentinel is followed by a second, malformed attempt (e.g.
 * a self-correction cut off by max_tokens), the earlier capture is discarded
 * and status is 'malformed' — serving the model's abandoned first draft while
 * reporting success would blind the fail-soft counter to exactly the failures
 * it exists to catch. Symmetrically, malformed-then-valid means the model
 * corrected itself: the valid final attempt is served.
 */
function createTailParser() {
  let pending = '';       // text not yet released (prose suffix or block content)
  let inBlock = false;    // saw the full open marker; `pending` is block content
  let actionIds = null;   // decided by the LAST marker attempt (null = it failed)
  let sawMarker = false;  // any open marker seen (distinguishes malformed/no_sentinel)
  let afterValidClose = false;    // a valid close happened; later prose is out-of-spec
  let trailingAfterClose = false; // non-whitespace prose actually followed one

  // Scan `pending`, releasing everything provably prose. Loops because one
  // chunk can close a block AND contain trailing prose or another marker.
  function drain(atEnd) {
    let out = '';
    for (;;) {
      if (!inBlock) {
        const i = pending.indexOf(SENTINEL_OPEN);
        if (i === -1) {
          const hold = atEnd ? 0 : liveMarkerPrefixLen(pending);
          const release = pending.slice(0, pending.length - hold);
          if (afterValidClose && /\S/.test(release)) trailingAfterClose = true;
          out += release;
          pending = pending.slice(pending.length - hold);
          return out;
        }
        const release = pending.slice(0, i);
        if (afterValidClose && /\S/.test(release)) trailingAfterClose = true;
        out += release;
        pending = pending.slice(i + SENTINEL_OPEN.length);
        inBlock = true;
        sawMarker = true;
      } else {
        const close = pending.indexOf(SENTINEL_CLOSE);
        const nl = pending.indexOf('\n');
        if (close !== -1 && (nl === -1 || close < nl)) {
          // Block closed on a single line — parse it. A failed parse NULLS
          // any earlier capture (the last attempt decides).
          actionIds = parseBlockContent(pending.slice(0, close));
          if (actionIds !== null) afterValidClose = true;
          pending = pending.slice(close + SENTINEL_CLOSE.length);
          inBlock = false;
          continue; // trailing text is prose — re-scan
        }
        if (nl !== -1) {
          // Newline before any close: not a sentinel (spec is single-line).
          // Drop the marker + garbage, resume prose from the newline. This
          // was a failed attempt — it invalidates any earlier capture.
          actionIds = null;
          pending = pending.slice(nl);
          inBlock = false;
          continue;
        }
        if (atEnd) {
          // Stream ended inside a block — drop it, never leak it. A block
          // that never closed is a failed attempt (the last attempt decides).
          actionIds = null;
          pending = '';
          inBlock = false;
        }
        return out; // wait for more chunks (or done, at end)
      }
    }
  }

  return {
    feed(chunk) {
      pending += chunk;
      return drain(false);
    },
    end() {
      const remainingText = drain(true);
      const status = actionIds !== null ? 'actions' : sawMarker ? 'malformed' : 'no_sentinel';
      return { remainingText, actionIds, status, trailingAfterClose };
    },
  };
}

module.exports = {
  createTailParser,
  SENTINEL_OPEN,
  SENTINEL_CLOSE,
};
