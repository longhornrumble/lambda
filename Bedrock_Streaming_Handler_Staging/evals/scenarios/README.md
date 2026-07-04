# Tier-2 eval scenarios

One JSON file per scenario. The runner (`evals/run.js`) loads every `*.json` here,
builds the **real** Step-2 prompt (`buildV4ConversationPrompt`) from the scenario's
fixture config + recorded KB, invokes **live** Bedrock, optionally runs the real
`selectActionsV4`, then applies the scenario's deterministic assertions.

> **Skeleton note (sub-phase 1.3):** only `smoke_grounded.json` ships here as a
> harness placeholder. The real packs land next — grounding + anti-fabrication (1.4)
> and CTA quality + safety (1.5). The Haiku groundedness *judge* is added in 1.4.

## Scenario schema

```jsonc
{
  "id": "unique_scenario_id",              // required; keys the baseline
  "description": "what this checks",
  "run_action_selector": true,             // run selectActionsV4 for CTA assertions
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

## Deterministic assertion types

| type | fields | passes when |
|---|---|---|
| `response_contains` | `value` | response contains value (case-insensitive) |
| `response_not_contains` | `value` | response does not contain value |
| `response_matches` | `pattern`, `flags?` | regex matches response |
| `response_not_matches` | `pattern`, `flags?` | regex does not match |
| `response_urls_subset_of_kb` | — | every URL in the response appears in `kb_context` (anti-fabrication) |
| `ctas_include` | `value: []` | all listed CTA ids were selected |
| `ctas_exclude` | `value: []` | none of the listed CTA ids were selected |
| `ctas_subset_of` | `value: []` | selected CTAs are all within the allowed set |
| `ctas_equal` | `value: []` | selected CTAs equal the set exactly |
| `ctas_empty` | — | no CTAs selected |
| `ctas_valid` | — | every selected CTA id exists in `config.cta_definitions` |

An unknown assertion type fails loudly (so typos surface).

## Running

```bash
# from Bedrock_Streaming_Handler_Staging/ with AWS creds (SSO or OIDC) + Bedrock access
node evals/run.js                      # run all, compare to baseline, print report
node evals/run.js --filter smoke       # subset
node evals/run.js --report out.md      # write report to a file
node evals/run.js --update-baseline    # capture current results as the new baseline
```
