# Tier-2 eval baselines

`tier2.json` is the committed regression baseline. The runner compares each live
run against it and **fails** (exit 1) on a `regression` or a `stale_baseline`.

## Format

```jsonc
{
  "prompt_versions": {                 // versions at last --update-baseline
    "conversation": "v4-conv.v1",      // from prompt_v4.js (sub-phase 1.1)
    "action_selector": "v4-selector.v1",
    "groundedness_judge": "v1"         // from evals/judge.js (sub-phase 1.4)
  },
  "scenarios": {
    "<scenario id>": {
      "pass": true,                    // did the assertions pass? (judge UNSURE is non-failing)
      "prompt_versions": { "conversation": "v4-conv.v1", "action_selector": "v4-selector.v1", "groundedness_judge": "v1" },
      "assertions": [ { "type": "response_contains", "pass": true } ]
    }
  }
}
```

## Comparison / status

| status | meaning | fails run? |
|---|---|---|
| `ok` | pass matches baseline | no |
| `fixed` | now passing, baseline said failing | no (update to lock in) |
| `new` | scenario has no baseline entry | no (`--strict` makes it fail) |
| `regression` | was passing, now failing | **yes** |
| `stale_baseline` | the governing prompt version changed since the baseline was captured — it no longer applies | **yes** (re-baseline deliberately) |
| `error` | live Bedrock/invoke error | **yes** |

`stale_baseline` is the Phase-2 eval-gate: when a naturalness sub-phase bumps
`V4_CONVERSATION_PROMPT_VERSION` / `ACTION_SELECTOR_PROMPT_VERSION` in `prompt_v4.js`,
the old baseline is invalidated and the PR must commit a fresh one
(`node evals/run.js --update-baseline`) — a deliberate, reviewed act.

The `groundedness_judge` version (from `evals/judge.js`) works the same way, but
only stales scenarios that actually ran the judge (mirrors the action-selector
rule): change the judge wording → bump `GROUNDEDNESS_JUDGE_PROMPT_VERSION` → those
scenarios re-baseline. Judge `UNSURE` verdicts are recorded as `pass: true` (they
route to human review, they don't fail the run).

> **Note:** the 1.4 grounding pack and 1.5 CTA+safety pack commit their baselines
> here. The CI job that enforces them (path-gated + `workflow_dispatch`) lands in 1.6.
