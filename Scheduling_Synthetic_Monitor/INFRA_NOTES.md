# Scheduling_Synthetic_Monitor — infra brief (for the integrator)

The Lambda code lives here (the `lambda` repo); the IaC lives in the **picasso** repo
(`infra/`). The integrator applies the infra below — this note is the brief, not the IaC
(per the work-order: workers propose infra as a PR snippet; the integrator owns `infra/`).
**Staging only.** No prod deploy (the whole scheduling build is staging-gated; the prod-guard
hard-stops a prod run with test-mode on).

## Lambda

- **Runtime:** Node.js 20.x · handler `index.handler` · package via `npm run package`.
- **Dedicated execution role** (CLAUDE.md hard rule — never share IAM roles across Lambdas).
- **Timeout:** the `reminder` cycle (Phase-2) polls ~7min for the compressed reminder to
  fire (default 42 × 10s = 420s) — so the function timeout MUST be ≥ ~480s (8min) if the
  same function serves the reminder cycle. The `cancel` cycle alone needs only ≥90s. Set the
  function timeout to **500s** (under the 900s Lambda max) and invoke it ASYNCHRONOUSLY from
  EventBridge. Memory 256 MB. (Tune the poll via `REMINDER_POLL_ATTEMPTS`/`_INTERVAL_MS`.)
- **Env vars:** see [README.md](README.md#configuration). `STAGING_TEST_MODE` MUST NOT be set
  alongside `ENVIRONMENT=production` (the prod-guard refuses init — by design).

## IAM (least-privilege, dedicated role)

| Action | Resource | Why |
|---|---|---|
| `lambda:InvokeFunction` | the staging `Booking_Commit_Handler` ARN | propose / commit / cancel |
| `dynamodb:GetItem`, `UpdateItem`, `DeleteItem`, `Query` | `picasso-booking-staging` (table; Query needs no index — uses the base PK) | read-back, stamp `is_synthetic`, cleanup |
| `dynamodb:Query` | `picasso-scheduled-messages` table **+ its `by-appointment` index ARN** (`.../index/by-appointment`) | reminder cycle reads the booking's reminder rows to observe the `pending→sent` flip (read-only — the monitor never writes/deletes these rows) |
| `cloudwatch:PutMetricData` | `*` (scoped by `cloudwatch:namespace` = `Picasso/SchedulingSynthetic` condition) | cycle success/failure metrics |
| `sns:Publish` | the ops-alerts topic ARN | failure alerts |
| (managed) `AWSLambdaBasicExecutionRole` | — | CloudWatch Logs |

> No Secrets Manager, no Google/Zoom, no SES — the monitor never holds OAuth or the JWT
> signing key. BCH owns all calendar/conference I/O; the redemption endpoint is reached over
> plain HTTPS (outbound, no IAM). The revocation cycle is operator-triggered with a supplied
> token (no minting).
>
> ⚠️ **Do NOT copy an adjacent role's policy wholesale** (e.g. BCH's role): this role MUST
> NOT have `secretsmanager:GetSecretValue` or any Google/SES grant — it has no use for them
> and they would broaden the blast radius. Build the role from the table above only.
>
> 🔒 **REQUIRED (not optional):** scope the DynamoDB **and** `lambda:InvokeFunction` grants
> with a `Condition` on `aws:ResourceAccount = <staging account id>`. This is the only non-code
> backstop for the **test-mode-OFF** case: the `prod-guard` refuses init only when
> `STAGING_TEST_MODE` is ON — a role provisioned in the prod account with the flag OFF would
> let the monitor reach a prod booking table / BCH. **Do not apply this role without this
> condition.**

> **`BOOKING_TABLE` is a required env var, not a safe default.** The code falls back to
> `picasso-booking-${ENV}` only as a staging convenience; under the table-naming-alignment
> program a future bare-name rename would make the fallback mispoint. Always set
> `BOOKING_TABLE` explicitly in the function config.

## EventBridge schedules

| Rule | Schedule | Input |
|---|---|---|
| `synthetic-monitor-cancel-staging` | `rate(1 hour)` | `{ "cycle": "cancel" }` |
| `synthetic-monitor-reminder-staging` | `cron(0 8 * * ? *)` (daily) | `{ "cycle": "reminder" }` |
| `synthetic-monitor-cleanup-staging` | `cron(0 7 * * ? *)` (nightly) | `{ "cycle": "cleanup" }` |

`revocation_observe` is **operator-invoked** (manual `aws lambda invoke` with a real token) —
no EventBridge rule.

## Reminder cycle (Phase-2 dispatch-proof slice) — activation requirements

The `reminder` cycle proves the firing path (EventBridge → `Scheduled_Message_Sender` →
`pending→sent` row flip). It needs, beyond the rule + grant + timeout above:

1. **`STAGING_TEST_MODE=true` on `Booking_Commit_Handler`** (the cross-function dependency).
   The cadence time-compression runs in `scheduleReminders` AT COMMIT, inside BCH — so BCH's
   env (not the monitor's) gates it. Without it the synthetic booking schedules reminders at
   real 24h/1h offsets and the cycle fails cleanly ("no reminder dispatched"). Double-gated by
   `is_synthetic` (the monitor's commit payload sets it) so real bookings are never compressed.
   *(Optional symmetry: also set it on `Scheduled_Message_Sender` + `Reminder_Scheduler`; the
   compression that matters for THIS cycle is BCH's.)*
2. **`SCHEDULED_MESSAGES_TABLE`** env on the monitor (default `picasso-scheduled-messages`).
3. **deploy-staging.yml registration** — the monitor is in `pr-checks.yml` (tests run) but NOT
   in `deploy-staging.yml`. Add it (matrix entry + dispatch option) so the cycle code deploys,
   or accept the full-fleet dispatch redeploys it (the `-f lambda=` input is decorative).

**Still deferred (NOT in this slice):** email/SMS RECEIPT verification (SES inbound or Gmail
polling — the heavy §5.1 part), the missed-event disposition cycle (needs WS-E-ATTEND's
disposition surface), and the §4.3 DST/volume soak.

## CloudWatch alarm (CI-7 / integrator)

§5.1: ">3 failures in 24h = launch blocker." Alarm on
`Picasso/SchedulingSynthetic` → `CycleFailure` (sum, per `Cycle` dimension) ≥ 4 over 24h →
the existing `picasso-ops-alerts` SNS topic. (CI-7 is a separate pre-launch task; this Lambda
emits the metric it consumes.)

## Contract status (FROZEN_CONTRACTS.md — now LOCKED + LIVE)

§E1 (EventBridge Scheduler rule contract) and §E6 (`is_synthetic: BOOL` on Booking rows) are
**locked and shipped** — the Track 1 reminder system (S1–S6) is LIVE on staging. This cycle
consumes them as built:

> **§E6 — `is_synthetic: BOOL`.** Additive, readers tolerate absence. The monitor writes it on
> the COMMIT payload (BCH's `ctx.isSynthetic` → cadence compression double-gate) AND stamps the
> row post-commit (cancel/cleanup discrimination). The compression double-gate
> (`is_synthetic + STAGING_TEST_MODE`) shipped in `Reminder_Scheduler/cadence.js`.

> **§E1 — EventBridge Scheduler rule + `picasso-scheduled-messages` row shape.** Shipped: the
> scheduler writes rows (pk `TENANT#{tenantId}`, sk `SCHEDULED#{startAtIso}#{messageId}`, GSI
> `by-appointment`) + one-time schedules; `Scheduled_Message_Sender` flips status
> `pending→sent`. The reminder cycle reads that flip (read-only).
