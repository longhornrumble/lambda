# Tier-2 chat-experience eval net

A regression net for the V4 conversation prompt + action selector. It proves that
a prompt change doesn't break **grounding**, **safety**, or **CTA quality** before
it ships — the gate for the Phase-2 naturalness work in
[`docs/roadmap/CHAT_EXPERIENCE_OPTIMIZATION.md`](../../../docs/roadmap/CHAT_EXPERIENCE_OPTIMIZATION.md).

> **Not on the request path.** Everything here is a dev/CI tool. `evals/` is
> unreachable from `index.js` (the esbuild entry), so the deployed Lambda bundle
> is byte-identical. The groundedness judge and the runner never touch live
> customer conversations — they score fixed test scenarios.

## Layout

| Path | What |
|---|---|
| `run.js` | Plain-Node runner: discover scenarios → build the **real** prompt (V4 conversation prompt, or the V5 single-pass turn prompt for `run_single_pass` scenarios) from a fixture config + recorded KB → **live** Bedrock invoke → optional real `selectActionsV4` / pool selection / V5 tail parse → optional groundedness judge → deterministic scoring → baseline compare → markdown report. Exit 1 on regression / stale_baseline / live error. |
| `judge.js` | Haiku groundedness judge (temp 0), own `GROUNDEDNESS_JUDGE_PROMPT_VERSION`. GROUNDED→pass, UNGROUNDED→fail, UNSURE→human review (non-blocking). |
| `report.js` | Pure markdown report renderer. |
| `scenarios/` | One JSON per scenario (grounding + CTA + safety packs). See `scenarios/README.md`. |
| `baselines/tier2.json` | Committed regression baseline. See `baselines/README.md`. |
| `scriptedBedrock.js` | Scripted-stream helper for the jest agent-eval suite (not used by `run.js`). |

Harness logic is unit-tested (no live calls) in `__tests__/evals_runner.test.js` +
`__tests__/evals_judge.test.js` via injectable seams (`invokeResponse`, `invokeJudge`).

## Running locally

Needs AWS creds with `bedrock:InvokeModel` for Haiku 4.5 in `us-east-1`:

```bash
aws sso login --profile myrecruiter-dev
cd Bedrock_Streaming_Handler_Staging
AWS_PROFILE=myrecruiter-dev AWS_REGION=us-east-1 node evals/run.js            # run + compare to baseline
AWS_PROFILE=myrecruiter-dev AWS_REGION=us-east-1 node evals/run.js --filter cta_    # subset
AWS_PROFILE=myrecruiter-dev AWS_REGION=us-east-1 node evals/run.js --update-baseline # re-capture (deliberate)
AWS_PROFILE=myrecruiter-dev AWS_REGION=us-east-1 node evals/run.js --retries 0        # disable per-scenario retry
```

**Per-scenario retry (default 2).** These scenarios assert properties the model
*should* satisfy (grounding, safety, CTA restraint) and it does ~85–99% of the
time; the occasional flip is model stochasticity, not a regression. A failing
scenario is retried up to `--retries N` times and passes if any attempt passes —
a REAL regression fails every attempt and is still caught. Without it, ~15 live
scenarios each flipping a few percent compound to a ~50% full-run flake rate,
which would make the CI gate red half of eval-touching PRs for no real reason.

## CI gate (sub-phase 1.6)

The `chat-eval-net` job in [`.github/workflows/pr-checks.yml`](../../../.github/workflows/pr-checks.yml):

- **Path-gated** — runs only when `prompt_v4.js`, `prompt_v5.js`, `streamTail.js`,
  `scheduling/agentTurn.js`, or `evals/**` change (so unrelated PRs stay
  deterministic and spend no Bedrock).
- **Live Bedrock** via OIDC into the staging `GitHubActionsDeployRole` (Haiku 4.5,
  `us-east-1`).
- **Fails the PR** (through `all-checks-passed`) on any `regression`,
  `stale_baseline`, or live `error`.
- Also runnable on demand via **`workflow_dispatch`**.

**The one sanctioned way to turn a red gate green is a reviewed re-baseline.** When a
prompt-text change bumps the matching version constant
(`V4_CONVERSATION_PROMPT_VERSION` / `ACTION_SELECTOR_PROMPT_VERSION` in `prompt_v4.js`,
`V5_TURN_PROMPT_VERSION` in `prompt_v5.js`, or `GROUNDEDNESS_JUDGE_PROMPT_VERSION`
in `judge.js`), every baseline captured under the old version then reports
`stale_baseline` until the PR commits a fresh `node evals/run.js --update-baseline`.
That coupling **is** the eval gate. Staleness is name-gated: a `single_pass` bump
stales only `run_single_pass` scenarios, exactly as a selector/judge bump stales
only scenarios that ran the selector/judge. (A `conversation` bump stales
everything — including single-pass scenarios, whose prompt splices the V4
conversation prompt.)

**V5 single-pass scenarios score strictly on the tail parser's output**
(`streamTail.js`): `ctas` = the parsed action ids, or `[]` when the tail is
missing/malformed. The production fail-soft ladder (malformed tail → one
`selectActionsV4` rescue call, V5.5) deliberately does NOT run in the harness —
rescuing here would mask exactly the format regressions the V5 scenarios exist
to catch.
