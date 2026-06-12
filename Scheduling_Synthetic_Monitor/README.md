# Scheduling_Synthetic_Monitor (CI-6 §5.1)

Synthetic monitoring Lambda for the scheduling system's **Layer 2 staging burn-in**
([`scheduling/docs/scheduling_ci_strategy.md`](../../../scheduling/docs/scheduling_ci_strategy.md) §5.1).
It continuously exercises the **live staging** scheduling surfaces so cross-repo drift and
regressions surface within hours instead of at a customer (§5.2).

> **Scope:** the `cancel` / `revocation_observe` / `cleanup` / `reminder` / **`disposition`**
> cycles all ship live — **all five cycles are now SHIPPED** (CI-6 "all five cycles green"
> gate closed, WS-T3-DISP). The `disposition` cycle exercises the WS-E-ATTEND attendance
> check + §E4 no_show disposition path + idempotency. Still **deferred**: email/SMS RECEIPT
> verification and the §4.3 DST/volume soak. See [Deferred](#deferred-to-phase-2) below.

---

## Cycles

| Cycle (`event.cycle`) | Trigger | Exercises | Producer (live) |
|---|---|---|---|
| `cancel` | EventBridge, hourly | book (propose→commit) → cancel → §14.2 listener flips `status=canceled` | Booking_Commit_Handler + cal-lifecycle consumer |
| `reminder` | EventBridge, daily | book (compressed) → reminder row flips `pending→sent` (firing-path proof) | scheduler + Scheduled_Message_Sender |
| `revocation_observe` | **operator-invoked** | one-time token: first redemption succeeds → replay returns **410 Gone** (§13.7) | Scheduling_Redemption_Handler |
| `cleanup` | EventBridge, nightly | delete synthetic bookings older than 7 days (§5.1 test-data hygiene) | — (this Lambda owns it) |
| `disposition` | EventBridge, daily | book → attend check (`attendance_state=pending_attendance`) → didnt_connect disposition → `coordinator_no_show` + idempotency (`already_resolved`) | Attendance_Disposition_Handler + shared/scheduling/disposition |

### `cancel` cycle
Exercises the **BCH commit/cancel boundary directly** — invokes BCH `scheduling_propose`
(real availability) → default commit (live freeBusy → slot-lock → conference → Google
Calendar insert → Booking write → confirmation email), stamps `is_synthetic=true` on its
own row (§E6), then invokes BCH `scheduling_mutate` cancel. The §14.2 cal-lifecycle listener
flips `Booking.status=canceled` **asynchronously** on the calendar-delete push, so the
monitor **polls** the row (bounded retry) until it flips. A flip that never arrives is a real
finding (listener lag/breakage) — exactly what burn-in should catch.

> **Coverage boundary (honest scope):** this exercises BCH and the cal-lifecycle chain, **not**
> the full public path. The BSH conversation flow, the §B14 action boundary, and the
> widget/session threading are **bypassed** (covered by §5.2 manual exercise until a BSH-level
> cycle lands). The `cancel` cycle does book/cancel **real** staging calendar events.

### `reminder` cycle (Phase-2 dispatch-proof slice)
Books a synthetic appointment with `is_synthetic:true` on the **commit payload** so BCH's
post-commit reminder scheduling compresses the cadence (`STAGING_TEST_MODE && is_synthetic` —
24h→+1m, 1h→+3m). The scheduler writes `picasso-scheduled-messages` rows (`status:pending`) +
one-time EventBridge schedules at the compressed fire times; EventBridge fires
`Scheduled_Message_Sender`, which dispatches and flips the row `status` to `sent`. The cycle
**polls** the booking's reminder rows (via the `by-appointment` GSI) until a cadence-reminder
row reaches `sent` — that flip is the **dispatch proof**. It then cancels the synthetic booking
(best-effort cleanup; never masks the result).

> **Requires** `STAGING_TEST_MODE=true` on **BCH** (the compression runs at commit, inside
> BCH) + a function timeout ≥ ~500s (it polls ~7min for the fire). See
> [`INFRA_NOTES.md`](INFRA_NOTES.md#reminder-cycle-phase-2-dispatch-proof-slice--activation-requirements).
>
> **Coverage boundary (honest scope):** this proves the firing path via the DynamoDB row
> `pending→sent` flip — it does **not** verify the email/SMS was actually RECEIVED (SES inbound
> / Gmail polling is deferred). A row stuck `pending` past the poll window is a real finding
> (EventBridge/Sender lag or breakage). If the scheduler created no cadence-reminder rows
> (e.g. `STAGING_TEST_MODE` off, or a <1h lead), the cycle fails cleanly.

### `revocation_observe` cycle (operator-triggered)
**The monitor never mints or auto-revokes tokens** — it does not hold the JWT signing key and
must not consume jtis on a schedule. The **operator** supplies one real one-time token (e.g.
a cancel link from a synthetic booking's confirmation email) and its slug; the observer
redeems it twice against the redemption endpoint and asserts success → 410. The token is a
one-time credential and is **never logged**.

> It is a **diagnostic spot-check, not a scheduled cycle** — it fires only when an operator
> invokes it, so it does **not** contribute to the §4.3 "24h of green hourly cycles" window
> (that window is satisfied by `cancel` + `cleanup`).

Invoke:
```json
{ "cycle": "revocation_observe", "slug": "/cancel", "token": "<one-time token>" }
```

### `disposition` cycle (CI-6 5th cycle — WS-T3-DISP SHIPPED)
Books a synthetic appointment (`is_synthetic:true`), then directly invokes
`Attendance_Disposition_Handler` with `action:'attendance_check'`. The handler sets the
non-key `attendance_state='pending_attendance'` (idempotent conditional write) and mints
the three §B4 interviewer tokens. The cycle then:

1. Asserts `attendance_state='pending_attendance'` on the row (DDB read-back).
2. Drives ONE disposition via `applyDisposition({purpose:'didnt_connect'})` — the same
   function the `Scheduling_Redemption_Handler` calls after token redemption (§E4 §11.2).
   This sets `Booking.status='coordinator_no_show'` + `attendance_state='resolved'`. The
   `didnt_connect` purpose has NO outbound (no reschedule-token mint, no volunteer email,
   no `lambda:InvokeFunction` on `send_email`) — the only live dependency is the conditional
   DDB UpdateItem. The volunteer-notice path (`no_show` → reoffer + reschedule token) stays
   covered by the ATTEND/redemption unit tests.
3. Polls the row until `status='coordinator_no_show'` + `attendance_state='resolved'` (bounded).
4. Asserts idempotency: a second `applyDisposition` call yields `outcome='already_resolved'`.

> **Design choice — handler-side path, not HTTP redemption endpoint:** the HTTP path would
> require the monitor to extract the real token from the confirmation email (not machine-
> readable) or hold the JWT signing key (state it must not own). The handler-side path
> exercises the same §E4 §11.2 code without touching the token layer. See the disposition-
> cycle.js header for the full justification.
>
> **Why `didnt_connect` (not `no_show`):** `no_show` reaches `tokens.sign` (JWT signing key
> — the monitor must never hold it) and `notify.dispatchVolunteerNotice` → `defaultInvokeEmail`
> (requires `lambda:InvokeFunction` on `send_email` — ungranted). `didnt_connect` has no
> outbound; the only live dep is the conditional DDB UpdateItem on the Booking row.
>
> **Build note:** the runtime `require('../shared/scheduling/disposition')` is resolved at
> build time by the esbuild bundle (`npm run build`), same as other lambdas bundling shared
> modules — no deploy-zip gap.
>
> **Cleanup:** the booking ends in `status='coordinator_no_show'` — a terminal status. The
> nightly `cleanup` cycle deletes `is_synthetic=true` rows by `created_at < cutoff` regardless
> of terminal status. No new cleanup logic is needed.
>
> **Integrator glue required** (see PR + `INFRA_NOTES.md`): EventBridge rule
> `{"cycle":"disposition"}` daily + `ATTEND_FUNCTION_NAME` env var pointing at
> `Attendance_Disposition_Handler` + `lambda:InvokeFunction` grant on
> `Attendance_Disposition_Handler` + `dynamodb:UpdateItem` on the staging Booking table.

### `cleanup` cycle
Deletes `is_synthetic=true` bookings older than `SYNTHETIC_RETENTION_DAYS` (default 7).
Bounded to the synthetic tenant's partition (Query on the PK) — **never a full-table scan,
never cross-tenant** — and filtered to `item_type='booking'` so a real booking can never be
deleted. Refuses to run if `SYNTHETIC_TENANT_ID` is unset.

---

## HARD prod-guard

[`prod-guard.js`](prod-guard.js) `assertSafeMode()` runs at **module load** (an init refusal —
the Lambda cold-start throws, so the function is structurally incapable of running synthetic
logic in prod) **and** at handler entry (defense-in-depth):

> if `STAGING_TEST_MODE` is enabled **and** `ENVIRONMENT` is production-like → **REFUSE**.

This is the environment half of §5.1's `is_synthetic + STAGING_TEST_MODE` double-gate. The
production check matches `production` **and** the legacy `prod` typo (safety bias).

---

## Operational precondition (not code)

The `cancel` cycle books real appointments, so the **synthetic tenant** (`SYNTHETIC_TENANT_ID`)
must be the §5.2 staging burn-in tenant, provisioned with `scheduling_enabled`, a coordinator
with a live OAuth grant, the named appointment type, and a routing policy. Without it, propose
returns no slots and the cycle reports a **clean failure** (no crash).

---

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `ENVIRONMENT` | `staging` | prod-guard + Booking table name suffix |
| `STAGING_TEST_MODE` | (unset) | prod-guard input (Phase-2 time-compression flag) |
| `SYNTHETIC_TENANT_ID` | (required) | the staging burn-in tenant |
| `SYNTHETIC_APPOINTMENT_TYPE_ID` | (required) | appointment type to book |
| `SYNTHETIC_APPOINTMENT_TYPE_NAME` | `Synthetic Monitor Check` | calendar event title |
| `SYNTHETIC_MONITOR_EMAIL` | `scheduling-monitor@myrecruiter.ai` | synthetic attendee (§8 resolved alias) |
| `SYNTHETIC_TIME_ZONE` | `America/Chicago` | user tz for propose/commit |
| `SYNTHETIC_CONFERENCE_TYPE` | `null` | `null` \| `google_meet` \| `zoom` |
| `BOOKING_TABLE` | `picasso-booking-${ENV}` | Booking table (FROZEN §A) |
| `SCHEDULED_MESSAGES_TABLE` | `picasso-scheduled-messages` | reminder rows the `reminder` cycle reads (§E1) |
| `BOOKING_COMMIT_FUNCTION_NAME` | `Booking_Commit_Handler` | BCH invoke target |
| `BCH_INVOKE_TIMEOUT_MS` | `30000` | per-invoke request timeout for the BCH call (heavy I/O) |
| `REDEMPTION_BASE_URL` | `https://schedule.myrecruiter.ai` | revocation endpoint base |
| `OPS_ALERTS_TOPIC_ARN` | (unset → alerts skipped) | SNS ops-alerts topic |
| `MONITOR_METRIC_NAMESPACE` | `Picasso/SchedulingSynthetic` | CloudWatch metric namespace |
| `SYNTHETIC_RETENTION_DAYS` | `7` | cleanup window |
| `CANCEL_POLL_ATTEMPTS` / `CANCEL_POLL_INTERVAL_MS` | `12` / `5000` | cancel status-flip poll |
| `REMINDER_POLL_ATTEMPTS` / `REMINDER_POLL_INTERVAL_MS` | `42` / `10000` | reminder `pending→sent` poll (~7min) |
| `ATTEND_FUNCTION_NAME` | `Attendance_Disposition_Handler` | ATTEND Lambda invoke target (disposition cycle) |
| `DISPOSITION_POLL_ATTEMPTS` / `DISPOSITION_POLL_INTERVAL_MS` | `8` / `3000` | disposition row verification poll |

See [`INFRA_NOTES.md`](INFRA_NOTES.md) for the IAM/EventBridge/IaC integrator brief.

---

## Deferred to Phase 2

The reminder system (S1–S6) is LIVE and the `reminder` cadence cycle above proves its firing
path. Still deferred:

- **Email/SMS RECEIPT verification** — the `reminder` cycle proves dispatch via the DynamoDB
  `pending→sent` flip, not that the message was delivered. End-to-end receipt needs SES inbound
  (MX + S3 receipt rule) or Gmail API polling — the heavy §5.1 part (~1-week effort).
- **Happy-path attendance cycle** — book → reminder windows → mark completed
- **§4.3 DST / volume soak** — ≥3 reminder windows across a DST boundary, ≥50 booked, etc.

---

## Develop

```bash
npm install
npm test            # jest (215 tests, 12 suites)
npm run build       # esbuild → dist/index.js
npm run package     # build + zip → deployment.zip
```
