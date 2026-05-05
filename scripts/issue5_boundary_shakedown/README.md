# Issue #5 boundary shakedown

The v7 plan's soak gate validates analytics-writer **code** behavior under load
but doesn't exercise the new staging account's **environment boundaries**.
This script fills that gap.

## Run

```bash
cd scripts/issue5_boundary_shakedown
python3 shakedown.py
# Or with explicit profiles:
python3 shakedown.py --prod-profile chris-admin --staging-profile myrecruiter-staging
```

Exit code 0 = all active tests passed; 1 = at least one failed; 2 = profile/account mismatch.

## What it tests (non-destructive)

| ID | Test | Why |
|---|---|---|
| T1 | S3 cross-account replication freshness | Confirms prod-side tenant config updates reach staging in <90s |
| T2 | `DenyPutsFromStagingAccount` enforcement | Confirms staging principal cannot pollute the replicated bucket |
| T3 | Lambda Function URL reachability | Confirms both Console-saved policy statements (`FunctionURLAllowPublicAccess` + `FunctionURLAllowInvokeAction`) are still in place |
| T4 | `DenyNonUSEast1` SCP enforcement | Confirms the Bedrock carve-out from INT1 didn't widen the SCP for other services |
| T5 | CloudWatch log retention | Confirms both Lambda log groups retain 30 days, not "never expire" |

## What it skips (intentionally)

| ID | Test | Why |
|---|---|---|
| S1 | JWT secret rotation drill | Destructive — invalidates live tokens. Manual checklist required. |
| S2 | Cross-account STS sustained load | Long-running (1h+); run separately, not part of fast smoke. |
| S3 | CloudTrail audit completeness | Blocked pending MontyCloud → owned-trail migration project. |
| S4 | DDB schema parity (staging twins vs prod legacy) | Needs a separate diff script; staging twins audited at create-time. |

## When to run

- **Once after Issue #5 Phase 1 deploy** to baseline environment soundness
- **Once per quarter** as a hygiene check (catches infrastructure drift)
- **Before any account-boundary change** (new SCP, new IAM, new replication rule) to baseline pre-change
- **After any account-boundary change** to confirm intended effect + no collateral damage
- **Before declaring Issue #5 Phase 1 acceptance** as a complement to the code soak

## Interpreting results

- **5/5 PASS**: environment topology is sound; soak gate's code-level testing is the only remaining concern
- **Any FAIL**: investigate immediately; do not promote Phase 1 until resolved
- **SKIP count > 0**: that's expected today (4 skipped); document the skip rationale and address as separate efforts

## Non-goals

- Not a load test; not a benchmark
- Not a substitute for the v7 code soak — they validate orthogonal things
- Not a security audit (use [Security-Reviewer](../../) for that)
- Not a compliance assessment (CloudTrail / SOC 2 work is separate)
