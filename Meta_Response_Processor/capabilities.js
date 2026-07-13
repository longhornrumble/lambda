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

module.exports = { MESSAGE_CHAR_LIMITS };
