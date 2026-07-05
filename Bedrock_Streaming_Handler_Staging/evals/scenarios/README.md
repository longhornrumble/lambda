# Tier-2 eval scenarios

One JSON file per scenario. The runner (`evals/run.js`) loads every `*.json` here,
builds the **real** Step-2 prompt (`buildV4ConversationPrompt`) from the scenario's
fixture config + recorded KB, invokes **live** Bedrock, optionally runs the real
`selectActionsV4`, then applies the scenario's deterministic assertions.

Packs on disk:
- **Grounding + anti-fabrication (1.4)** — `grounding_*.json`, plus the
  `grounded_in_kb` Haiku judge (see below).
- **CTA quality + safety (1.5)** — `cta_*` / `safety_*` scenarios (deterministic
  assertions only).
- `smoke_grounded.json` — the 1.3 harness placeholder, kept as a minimal example.

## Scenario schema

```jsonc
{
  "id": "unique_scenario_id",              // required; keys the baseline
  "description": "what this checks",
  "run_action_selector": true,             // run selectActionsV4 for CTA assertions
  // Alternatives (mutually exclusive with each other and the above):
  // "run_pool_selection": true,           // V4.1 topic classification + pool filtering
  // "run_single_pass": true,              // V5: merged turn prompt + tail parse (below)
  "config": {                              // fixture tenant config (not a live tenant)
    "tenant_id": "EVAL_SMOKE",
    "chat_title": "Helping Hands",
    "model_id": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "tone_prompt": "…persona…",
    "feature_flags": { "V4_ACTION_SELECTOR": true },
    "cta_definitions": { "learn_x": { "label": "…", "action": "send_query", "ai_available": true } }
  },
  "kb_context": "recorded KB passages, or null for a KB-miss scenario",
  "conversation_history": [ { "role": "user", "content": "…" } ],  // prior turns only
  "user_input": "the current user message",
  "assertions": [ { "type": "response_contains", "value": "Saturday" } ]
}
```

`conversation_history` is the **prior** turns (mirrors production: the current turn
is `user_input`, passed separately). `selectActionsV4` receives `(responseText,
conversation_history, config, client)` exactly as in `index.js`.

## V5 single-pass scenarios (`run_single_pass`)

Sets the runner on the V5 architecture (one call produces prose + actions): the
prompt is `buildV5TurnPrompt` (`prompt_v5.js`) with `V5_TURN_INFERENCE_PARAMS`,
and the full response runs through the `streamTail.js` parser. `ctas` = the
parsed tail ids (`[]` when the tail is missing/malformed — **strict scoring, no
production fail-soft rescue in the harness**), and the `response_*` /
`grounded_in_kb` assertions score the STRIPPED prose (the sentinel never reaches
them). `run_single_pass` is mutually exclusive with `run_action_selector` and
`run_pool_selection`. Baselines for these scenarios go `stale_baseline` when
`V5_TURN_PROMPT_VERSION` bumps (or `V4_CONVERSATION_PROMPT_VERSION` — the V5
prompt splices the V4 conversation prompt).

## Deterministic assertion types

| type | fields | passes when |
|---|---|---|
| `response_contains` | `value` | response contains value (case-insensitive) |
| `response_not_contains` | `value` | response does not contain value |
| `response_matches` | `pattern`, `flags?` | regex matches response |
| `response_not_matches` | `pattern`, `flags?` | regex does not match |
| `response_urls_subset_of_kb` | — | every URL in the response appears in `kb_context` (anti-fabrication) |
| `ctas_include` | `value: []` | all listed CTA ids were selected |
| `ctas_include_any` | `value: []` | at least ONE of the listed CTA ids was selected (OR — e.g. the V5 funnel lock accepts discovery-session OR application) |
| `ctas_exclude` | `value: []` | none of the listed CTA ids were selected |
| `ctas_subset_of` | `value: []` | selected CTAs are all within the allowed set |
| `ctas_equal` | `value: []` | selected CTAs equal the set exactly |
| `ctas_empty` | — | no CTAs selected |
| `ctas_valid` | — | every selected CTA id exists in `config.cta_definitions` |
| `tail_status` | `value` | (`run_single_pass` only) the V5 tail parser's status equals value — `actions` pins format; `ctas_empty` alone can't tell restraint (`<<<ACTIONS []>>>`) from a malformed tail (both yield `ctas: []`) |
| `grounded_in_kb` | — | the Haiku groundedness judge (see below) rules the reply GROUNDED |

An unknown assertion type fails loudly (so typos surface).

## Groundedness judge (`grounded_in_kb`)

Deterministic regex catches fabricated URLs, forbidden phrases, and bad CTA ids;
it cannot catch a fluent invented *fact*. The `grounded_in_kb` assertion routes
the reply to a focused Haiku call (temperature 0, `evals/judge.js`) that decides
whether every factual claim is supported by `kb_context`:

- **GROUNDED** → assertion passes.
- **UNGROUNDED** → assertion fails (a claim isn't in the KB).
- **UNSURE** → **routes to human review**; it does NOT auto-pass or auto-fail.
  The scenario stays non-failing but is flagged (`⚠️ … need human review`) in the
  report and marked `(review)` in the run log.

The judge runs behind the injectable `invokeJudge` seam (mirroring
`invokeResponse`), so the jest suite drives the scoring path with no live call.
It carries its own `GROUNDEDNESS_JUDGE_PROMPT_VERSION`, stamped into results and
baselines — changing the judge wording stales judge-dependent baselines exactly
like a product-prompt bump (see `../baselines/README.md`).

## Running

```bash
# from Bedrock_Streaming_Handler_Staging/ with AWS creds (SSO or OIDC) + Bedrock access
node evals/run.js                      # run all, compare to baseline, print report
node evals/run.js --filter smoke       # subset
node evals/run.js --report out.md      # write report to a file
node evals/run.js --update-baseline    # capture current results as the new baseline
```
