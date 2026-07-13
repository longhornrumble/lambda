'use strict';

/**
 * Session-boundary computation — contract C8 (docs/messenger/CONTRACTS.md).
 *
 * Messenger threads are endless; V5's TURN-CHECK counts assistant questions
 * over whatever history it is handed. Unscoped, the funnel rules misfire
 * forever after the bot's second-ever question (G4). A *session* is a maximal
 * run of consecutive history rows where the gap between stored
 * `messageTimestamp` values (epoch ms — our write clock, never Meta's event
 * timestamp; that one belongs to the 24h SEND-window guard) is < 24h. A gap
 * >= 24h starts a new session (>= semantics, frozen in C8).
 */

const SESSION_GAP_MS = 24 * 60 * 60 * 1000;

/**
 * @param {Array<{role: string, content: string, messageTimestamp?: number}>} history
 *   Conversation rows, ascending by time (loadConversationContext order).
 * @returns {{ sessionMessages: Array<object>, isSessionFirstTurn: boolean }}
 *   sessionMessages — rows after the most recent boundary (the current session);
 *   isSessionFirstTurn — true when the current session has no prior rows
 *   (empty history, or the newest row is itself >= 24h old — the incoming
 *   message starts a new session).
 */
function computeSessionWindow(history, nowMs = Date.now()) {
  const rows = Array.isArray(history) ? history : [];
  if (rows.length === 0) {
    return { sessionMessages: [], isSessionFirstTurn: true };
  }

  // Walk backwards to the most recent >= 24h gap. Rows without a
  // messageTimestamp cannot prove a gap — treated as same-session (fail-open:
  // never spuriously reset the funnel; the SK is always present in practice).
  let start = 0;
  for (let i = rows.length - 1; i > 0; i--) {
    const cur = rows[i]?.messageTimestamp;
    const prev = rows[i - 1]?.messageTimestamp;
    if (typeof cur === 'number' && typeof prev === 'number' && cur - prev >= SESSION_GAP_MS) {
      start = i;
      break;
    }
  }
  let sessionMessages = rows.slice(start);

  // The incoming message itself may open a NEW session: if the newest stored
  // row is >= 24h old, nothing stored belongs to the current session.
  const newest = rows[rows.length - 1]?.messageTimestamp;
  if (typeof newest === 'number' && nowMs - newest >= SESSION_GAP_MS) {
    sessionMessages = [];
  }

  return { sessionMessages, isSessionFirstTurn: sessionMessages.length === 0 };
}

module.exports = { computeSessionWindow, SESSION_GAP_MS };
