# Reminder_Scheduler — deploy note (WS-E-REMIND, §E1/E2/E9)

Everything in this note is **integrator/operator glue** — the worker delivers the code +
tests + this note; the integrator wires the IAM role, env vars, EventBridge trigger, the
call-sites, the orphan-recovery dep, and the CI matrix. Nothing here mutates deployed
infra by itself.

---

## 1. The EventBridge Scheduler IAM **execution role** (§E1 — DEDICATED, never shared)

EventBridge Scheduler assumes a role to invoke the target. Create a **dedicated** role
(do NOT reuse a Lambda's own role — CLAUDE.md never-share-IAM):

- **Trust policy:** principal `scheduler.amazonaws.com` (`sts:AssumeRole`).
- **Permissions:** `lambda:InvokeFunction` on the `Scheduled_Message_Sender` ARN **only**.
- Its ARN is passed as `Target.RoleArn` on **every** `CreateSchedule` (env `SCHEDULER_ROLE_ARN`).

## 2. IAM for the callers of the lifecycle lib (commit / reschedule / cancel) + the reconciler

Whichever Lambda invokes `scheduleReminders` / `rebindReminders` / `deleteReminders`
(Booking_Commit_Handler at commit; the in-chat reschedule flow; the cal-lifecycle
consumer at cancel/move) needs:

- `scheduler:CreateSchedule`, `scheduler:DeleteSchedule` (scoped to the schedule group).
- `iam:PassRole` on the Scheduler execution role above (to set `Target.RoleArn`).
- DynamoDB `PutItem` / `DeleteItem` on `picasso-scheduled-messages`.
- DynamoDB `UpdateItem` on the Booking table (writes the `reminder_schedule_state` E6 bookkeeping).

The **reconciler** Lambda role needs:

- DynamoDB `Query` on the Booking table **`tenantId-start_at-index` GSI** + `UpdateItem` on the base table.
- `scheduler:DeleteSchedule` (terminal-cleanup sweep) on the schedule group.
- (orphan recovery only) Secrets Manager read on `picasso/scheduling/oauth/{tenantId}/{coordinatorId}` + the
  Google Calendar grant — see §6.

## 3. Env vars

| Lambda | Var | Purpose |
|---|---|---|
| caller + reconciler | `SCHEDULER_TARGET_ARN` | the `Scheduled_Message_Sender` ARN (schedule target) |
| caller + reconciler | `SCHEDULER_ROLE_ARN` | the dedicated Scheduler execution role ARN (§1) |
| caller + reconciler | `SCHEDULER_GROUP_NAME` | schedule group (default `default`; recommend a dedicated group) |
| caller + reconciler | `SCHEDULED_MESSAGES_TABLE` | default `picasso-scheduled-messages` |
| caller + reconciler | `BOOKING_TABLE` | default `picasso-booking-${ENVIRONMENT}` |
| reconciler | `BOOKING_START_AT_INDEX` | default `tenantId-start_at-index` |
| reconciler | `SCHEDULING_TENANT_IDS` | comma-separated tenant allow-list (or pass `event.tenant_ids`) |
| reconciler | `RECONCILE_LOOKBACK_DAYS` | default `14` (covers attendance backstop + >7d terminal cleanup) |
| reconciler/scheduler | `STAGING_TEST_MODE`, `ENVIRONMENT` | the §E1 synthetic double-gate + prod guard |
| Scheduled_Message_Sender | `SEND_EMAIL_FUNCTION` | default `send_email` (the §E1 email-as-floor branch) |
| Scheduled_Message_Sender | `SMS_SENDER_FUNCTION`, `SMS_CONSENT_TABLE` | already shipped |

## 4. EventBridge trigger for the reconciler (nightly)

A recurring EventBridge Scheduler (e.g. `cron(0 7 * * ? *)` UTC) → `Reminder_Scheduler.handler`
with input `{ "tenant_ids": ["AUS123957", ...] }` (or rely on `SCHEDULING_TENANT_IDS`).
The handler **refuses to start** if `STAGING_TEST_MODE=true && ENVIRONMENT=production` (§E1 SR-3).

## 5. Call-site wiring (lifecycle lib — integrator owns)

`require('../Reminder_Scheduler')` (or bundle it) and call from the existing flows:

- **At commit (BCH)** — after `bookingStore.writeBooking(...)`:
  `await scheduleReminders({ booking: bookingItemAsObject, tenantPrefs }, deps)` where
  `tenantPrefs = { notificationPrefs: {...}, sms_quiet_hours: {...} }` from the already-loaded tenant config.
- **On token-reschedule** — after `executeReschedule(...)` returns the mutated booking
  (success / pending_calendar_sync): `await rebindReminders({ booking, tenantPrefs }, deps)`.
  ◀ this is the ONLY re-bind trigger (the named exit-criterion test).
- **On any cancel, incl. `booking.calendar_moved`** (cal-lifecycle consumer
  `reconcileMoved`/`reconcileDeleted` cancel path): `await deleteReminders({ booking }, deps)`.
  Move = cancel = DELETE (NOT a re-bind) per §E1.

Deps default to real AWS clients (`buildDefaultDeps()`); pass `{ config }` to override env.

## 6. Orphan recovery wiring (D6-outcome-(ii)) — OPTIONAL integrator glue

The orphan-recovery LOGIC is in `reconciler.js` (tested). The actual Google delete needs the
SHIPPED §B13 facade curried with per-coordinator OAuth, which needs the per-secret IAM grant
(§2) + the **coordinator-identity mapping** (the secret path uses the coordinator id — confirm
it is the coordinator email vs `resource_id`). Wire it by passing `deps.deleteCalendarEvent`:

```js
const { buildCalendarDeleter } = require('../Reminder_Scheduler');
const deleteCalendarEvent = buildCalendarDeleter({
  buildCalendarFacade: require('../shared/scheduling/calendarFacade').buildCalendarFacade,
  getOAuthClient: require('../Booking_Commit_Handler/oauth-client').getOAuthClient,
  calendarEvents: require('../Booking_Commit_Handler/calendar-events'),
});
// then: runReconcile({ tenantIds }, { ...deps, deleteCalendarEvent })
```

Until wired, the reconciler logs `orphan_recovery_skipped_no_dep` and skips (no crash) —
attendance backstop + terminal cleanup still run.

## 7. §E3 `selectChannels` wiring (Scheduled_Message_Sender — integrator glue)

`selectChannels` (§E3) is produced by **WS-E-TCPA** (not yet merged). Once it lands, wire it
into `Scheduled_Message_Sender` `defaultDeps()` (`selectChannels: <the merged fn>`). Until
then SMS is **fail-closed** and only the **email floor** sends for reminder rows — a
TCPA-safe default. ⚠ **`nowLocal` shape seam:** this consumer passes `nowLocal` as a `Date`
whose **UTC** fields carry the booking-timezone wall clock (so `getUTCHours()` = local hour).
Confirm WS-E-TCPA's `inQuietHours` reads it that way at weave, or reconcile the shape.

## 8. CI matrix wiring (`.github/workflows/pr-checks.yml` — integrator glue)

Two dirs need adding so their tests gate in CI (neither is currently in the matrix —
`Scheduled_Message_Sender` had no CI test job before this PR):

- **`Reminder_Scheduler`** — jest. Add to `detect-changes` filter + the `node-tests` matrix +
  `build` matrix + `security` matrix (mirror `Calendar_Lifecycle_Consumer`).
- **`Scheduled_Message_Sender`** — `node:test` (`node --test *.test.mjs`). Add to `detect-changes`
  + the `applier-tests` matrix (mirror `kb_proposal_applier` / `notification_hub`).

Local verification (this PR): `cd Reminder_Scheduler && npm install && npm test` (jest, 55 tests,
coverage gate met) + `cd Scheduled_Message_Sender && npm install && npm test` (node:test, 13 tests).

## 9. Table prerequisites

- `picasso-scheduled-messages` — exists (the shipped consumer reads it). For the additive `ttl`
  self-clean field to take effect, enable DynamoDB **TTL on `ttl`** (harmless when off).
- Booking table `tenantId-start_at-index` GSI — exists (B9/B11/E9 already documented it).
