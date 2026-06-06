# Scheduling_Synthetic_Monitor (CI-6 §5.1) — Phase 1

Synthetic monitoring Lambda for the scheduling system's **Layer 2 staging burn-in**
([`scheduling/docs/scheduling_ci_strategy.md`](../../../scheduling/docs/scheduling_ci_strategy.md) §5.1).
It continuously exercises the **live staging** scheduling surfaces so cross-repo drift and
regressions surface within hours instead of at a customer (§5.2).

> **Scope:** This is **Phase 1** — the three cycles whose producers are already shipped and
> live. The three time-compressed cycles (attendance / reminder-cadence / missed-event
> disposition) are **Phase 2**, deferred until **WS-E-REMIND** + **WS-E-ATTEND** land and
> the `FROZEN_CONTRACTS.md` **§E1 / §E6** contracts are formally locked. See
> [Deferred](#deferred-to-phase-2) below.

---

## Cycles

| Cycle (`event.cycle`) | Trigger | Exercises | Producer (live) |
|---|---|---|---|
| `cancel` | EventBridge, hourly | book (propose→commit) → cancel → §14.2 listener flips `status=canceled` | Booking_Commit_Handler + cal-lifecycle consumer |
| `revocation_observe` | **operator-invoked** | one-time token: first redemption succeeds → replay returns **410 Gone** (§13.7) | Scheduling_Redemption_Handler |
| `cleanup` | EventBridge, nightly | delete synthetic bookings older than 7 days (§5.1 test-data hygiene) | — (this Lambda owns it) |

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
| `BOOKING_COMMIT_FUNCTION_NAME` | `Booking_Commit_Handler` | BCH invoke target |
| `BCH_INVOKE_TIMEOUT_MS` | `30000` | per-invoke request timeout for the BCH call (heavy I/O) |
| `REDEMPTION_BASE_URL` | `https://schedule.myrecruiter.ai` | revocation endpoint base |
| `OPS_ALERTS_TOPIC_ARN` | (unset → alerts skipped) | SNS ops-alerts topic |
| `MONITOR_METRIC_NAMESPACE` | `Picasso/SchedulingSynthetic` | CloudWatch metric namespace |
| `SYNTHETIC_RETENTION_DAYS` | `7` | cleanup window |
| `CANCEL_POLL_ATTEMPTS` / `CANCEL_POLL_INTERVAL_MS` | `12` / `5000` | status-flip poll |

See [`INFRA_NOTES.md`](INFRA_NOTES.md) for the IAM/EventBridge/IaC integrator brief.

---

## Deferred to Phase 2

These require the **STAGING_TEST_MODE time-compression** in the (unbuilt) reminder
dispatcher — that producer is **WS-E-REMIND**'s owned lane — plus **WS-E-ATTEND** for the
disposition wiring, and the **§E1 / §E6** contract lock:

- **Happy-path attendance cycle** — book → reminder windows → mark completed
- **Reminder cadence cycle** — 1h / 30min reminders fire → missed-event prompt at T+30min
- **Missed-event disposition cycle** — Yes / No-show / didn't-connect / no-response escalation

---

## Develop

```bash
npm install
npm test            # jest (132 tests)
npm run build       # esbuild → dist/index.js
npm run package     # build + zip → deployment.zip
```
