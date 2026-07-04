/**
 * Scripted Bedrock stream builder — canned InvokeModelWithResponseStream sequences.
 *
 * Extracted verbatim from __tests__/agentEvals.test.js (chat-experience eval net,
 * sub-phase 1.2) so future eval suites can reuse the same scripted-model harness:
 * zero live API calls, deterministic tool_use / text response sequences.
 *
 * Depends on the `jest` global (scriptedBedrock wraps `send` in jest.fn so callers
 * can use jest matchers like toHaveBeenCalledTimes). This is a test-only helper —
 * it is required by jest suites, where the `jest` global is in scope. The Tier-2
 * live runner (evals/run.js) uses a real Bedrock client, not this scripted one.
 */

'use strict';

function enc(obj) {
  return { chunk: { bytes: new TextEncoder().encode(JSON.stringify(obj)) } };
}

/**
 * Build the Bedrock-Anthropic streaming event sequence for one scripted model turn.
 * turn = { text?: string, toolUse?: { id?, name, input }, stopReason?: 'end_turn'|'tool_use' }
 * Mirrors the event shapes the shipped non-agent path consumes in index.js
 * (message_start → content_block_start/delta/stop → message_delta{stop_reason} → message_stop).
 */
function modelTurnEvents({ text, toolUse, stopReason = 'end_turn' }) {
  const events = [
    { type: 'message_start', message: { id: 'msg_scripted', role: 'assistant', usage: { input_tokens: 25 } } },
  ];
  let index = 0;
  if (text) {
    events.push({ type: 'content_block_start', index, content_block: { type: 'text', text: '' } });
    const mid = Math.ceil(text.length / 2); // two deltas — exercises real streaming
    for (const part of [text.slice(0, mid), text.slice(mid)]) {
      if (part) events.push({ type: 'content_block_delta', index, delta: { type: 'text_delta', text: part } });
    }
    events.push({ type: 'content_block_stop', index });
    index += 1;
  }
  if (toolUse) {
    events.push({
      type: 'content_block_start',
      index,
      content_block: { type: 'tool_use', id: toolUse.id || `toolu_${index}`, name: toolUse.name, input: {} },
    });
    events.push({
      type: 'content_block_delta',
      index,
      delta: { type: 'input_json_delta', partial_json: JSON.stringify(toolUse.input || {}) },
    });
    events.push({ type: 'content_block_stop', index });
    index += 1;
  }
  events.push({ type: 'message_delta', delta: { stop_reason: stopReason, stop_sequence: null }, usage: { output_tokens: 12 } });
  events.push({ type: 'message_stop' });
  return events;
}

async function* chunkIterator(events) {
  for (const e of events) yield enc(e);
}

/**
 * Scripted deps.bedrock. `turns` = one entry per expected model call, in order.
 * Records each call's parsed request body (system / messages / tools / model id) in
 * `.calls`. A call beyond the script THROWS — this is load-bearing for the overflow
 * case (a 4th model call is a contract violation, §B17b).
 */
function scriptedBedrock(turns) {
  const calls = [];
  const send = jest.fn(async (command) => {
    const input = command && command.input !== undefined ? command.input : command;
    let body = null;
    if (input && typeof input.body === 'string') {
      try { body = JSON.parse(input.body); } catch (_) { body = null; }
    } else if (input && typeof input.body === 'object' && input.body !== null) {
      body = input.body;
    } else {
      body = input;
    }
    calls.push({ input, body });
    const turn = turns[calls.length - 1];
    if (!turn) {
      throw new Error(`scripted Bedrock exhausted: model call #${calls.length} but only ${turns.length} turn(s) scripted (§B17b MAX_TOOL_ITERATIONS violation?)`);
    }
    return { body: chunkIterator(modelTurnEvents(turn)) };
  });
  return { send, calls };
}

module.exports = { enc, modelTurnEvents, chunkIterator, scriptedBedrock };
