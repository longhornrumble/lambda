# Scheduling_Synthetic_Monitor — infra brief (for the integrator)

The Lambda code lives here (the `lambda` repo); the IaC lives in the **picasso** repo
(`infra/`). The integrator applies the infra below — this note is the brief, not the IaC
(per the work-order: workers propose infra as a PR snippet; the integrator owns `infra/`).
**Staging only.** No prod deploy (the whole scheduling build is staging-gated; the prod-guard
hard-stops a prod run with test-mode on).

## Lambda

- **Runtime:** Node.js 20.x · handler `index.handler` · package via `npm run package`.
- **Dedicated execution role** (CLAUDE.md hard rule — never share IAM roles across Lambdas).
- **Timeout:** ≥ 90s (the `cancel` cycle polls for the async §14.2 status flip — default
  12 × 5s = up to 60s — plus the propose+commit round-trips). Suggest 120s. Memory 256 MB.
- **Env vars:** see [README.md](README.md#configuration). `STAGING_TEST_MODE` MUST NOT be set
  alongside `ENVIRONMENT=production` (the prod-guard refuses init — by design).

## IAM (least-privilege, dedicated role)

| Action | Resource | Why |
|---|---|---|
| `lambda:InvokeFunction` | the staging `Booking_Commit_Handler` ARN | propose / commit / cancel |
| `dynamodb:GetItem`, `UpdateItem`, `DeleteItem`, `Query` | `picasso-booking-staging` (table; Query needs no index — uses the base PK) | read-back, stamp `is_synthetic`, cleanup |
| `cloudwatch:PutMetricData` | `*` (scoped by `cloudwatch:namespace` = `Picasso/SchedulingSynthetic` condition) | cycle success/failure metrics |
| `sns:Publish` | the ops-alerts topic ARN | failure alerts |
| (managed) `AWSLambdaBasicExecutionRole` | — | CloudWatch Logs |

> No Secrets Manager, no Google/Zoom, no SES — the monitor never holds OAuth or the JWT
> signing key. BCH owns all calendar/conference I/O; the redemption endpoint is reached over
> plain HTTPS (outbound, no IAM). The revocation cycle is operator-triggered with a supplied
> token (no minting).

## EventBridge schedules

| Rule | Schedule | Input |
|---|---|---|
| `synthetic-monitor-cancel-staging` | `rate(1 hour)` | `{ "cycle": "cancel" }` |
| `synthetic-monitor-cleanup-staging` | `cron(0 7 * * ? *)` (nightly) | `{ "cycle": "cleanup" }` |

`revocation_observe` is **operator-invoked** (manual `aws lambda invoke` with a real token) —
no EventBridge rule.

## CloudWatch alarm (CI-7 / integrator)

§5.1: ">3 failures in 24h = launch blocker." Alarm on
`Picasso/SchedulingSynthetic` → `CycleFailure` (sum, per `Cycle` dimension) ≥ 4 over 24h →
the existing `picasso-ops-alerts` SNS topic. (CI-7 is a separate pre-launch task; this Lambda
emits the metric it consumes.)

## Contract proposals for FROZEN_CONTRACTS.md (integrator applies — §C, workers don't edit)

This WS consumes a contract that is **described** in `SUBPHASE_E_BUILD_PLAN.md` §3 but **not
yet locked** in `FROZEN_CONTRACTS.md`. Phase-1 only **writes** `is_synthetic` and **reads** it
for cleanup; that minimal surface is stable across §5.1 (locked doc) and the build plan, so
the work proceeded — but the §E section should be added:

> **§E6 — Booking row additions (propose for lock).** Additive non-key attribute
> `is_synthetic: BOOL` on Booking rows. Schema discipline: additive, readers tolerate absence.
> Phase-1 writer = Scheduling_Synthetic_Monitor (stamps its own rows post-commit via
> UpdateItem; does NOT modify the C8 writer). Phase-2 reader = the reminder dispatcher's
> `is_synthetic + STAGING_TEST_MODE` time-compression double-gate (WS-E-REMIND).

> **§E1 — EventBridge Scheduler rule contract (still UNLOCKED; Phase-2 dependency).**
> WS-E-CI6's reminder/attendance/disposition cycles consume it; deferred with WS-E-REMIND.
