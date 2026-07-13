'use strict';

/**
 * Capability map (contract C5 — docs/messenger/CONTRACTS.md).
 *
 * Per-channel Messenger/Instagram platform limits. Rendering code reads caps
 * from here, never inline literals (C5: "Rendering code reads caps from a
 * single shared constant module ... never inline literals").
 */

/** Max characters per outbound text message, per channel (Meta platform limits). */
const MESSAGE_CHAR_LIMITS = {
  messenger: 2000,
  instagram: 1000,
};

/** Quick replies: max count and title length (C5 — distinct from message caps). */
const QUICK_REPLY_MAX = 13;
const QUICK_REPLY_TITLE_MAX = 20;

/** Button template: max buttons per template (C5). */
const BUTTON_TEMPLATE_MAX = 3;

/** Generic template (carousel): max cards per template (C5 — M8a scheduling slots). */
const CAROUSEL_MAX = 10;

module.exports = {
  MESSAGE_CHAR_LIMITS,
  QUICK_REPLY_MAX,
  QUICK_REPLY_TITLE_MAX,
  BUTTON_TEMPLATE_MAX,
  CAROUSEL_MAX,
};
