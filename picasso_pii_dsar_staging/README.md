# picasso-pii-dsar-staging

Operator-invocable DSAR (Data Subject Access Request) fulfillment Lambda for the
MFS-scoped surfaces. Capability-bundle item 1a per
[`CONSUMER_PII_REMEDIATION.md`](https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/CONSUMER_PII_REMEDIATION.md)
§"Path A Re-baseline v3".

## What this Lambda does today

- Cold-start env-guard (refuses to run outside staging account 525)
- Operator input validation (typed payload, `dry_run` default `true`)
- Subject resolution via `picasso-pii-subject-index-staging`
- **Idempotent audit writes** to `picasso-pii-dsar-audit-staging` — `ConditionExpression` refuses replay on identical (`dsar_id`, `event_timestamp`). Per-DSAR events: `request_received` → `surface_walked:<surface>` (one per non-deferred walker) → `closed`. Audit log is the legal trail; idempotency prevents silent overwrite.
- **`form-submissions` walker** — tenant-scoped Query (PK=`tenant_id`) + FilterExpression on `pii_subject_id`:
  - `request_type=access` → matched rows in `exported_rows`
  - `request_type=delete` + `dry_run=true` → count only
  - `request_type=delete` + `dry_run=false` → `DeleteItem` per matched row (corrupted rows missing PK/SK are logged + skipped, batch continues)
- **`notification-sends` walker** — tenant-scoped Query (PK=`TENANT#<tenant_id>`) with case-insensitive Python post-filter on `recipient` to bridge the writer-normalization gap (D5 F-DSAR3 — writers store `recipient` verbatim; walker normalizes via `.strip().lower()` on both sides). Catches direct-to-consumer messages (auto-replies, confirmations). Captures the matched `message_id`s for the chained `notification-events` walk. **Staff-recipient rows** (notifications ABOUT the consumer to staff) are operator/staff PII under a different controller relationship (D5 G-H + F9 + Step 10 v3 §F9) and **NOT auto-deleted** — flagged in `manual_followup` with a copy-pasteable inspection CLI snippet.
- **`notification-events` walker** — chained via the `message_id`s from `notification-sends`. Queries `picasso-notification-events-staging` by the `ByMessageId` GSI per message_id. If `notification-sends` yields no message_ids (the common case today), records `action=no_messages_to_walk` / `rows_touched=0` and does NOT issue GSI queries. **Continue-on-error**: per-message_id GSI failures are recorded in `failed_message_ids` and the walker proceeds to the next; operator sees a complete progress picture. **Bounded fan-out**: `MAX_MESSAGE_IDS_PER_INVOCATION = 200` cap protects the 60s Lambda timeout from high-volume subjects; overflow surfaces in `truncated_message_id_count` with an operator followup.
- **`recent-messages` walker** — chained via the `session_id`s captured from `form-submissions` matched rows. Queries `staging-recent-messages` by `sessionId = :s` for each session_id. The table has **NO subject-linking attribute** (no email, no `pii_subject_id`, no `tenantId`) so direct subject enumeration is structurally impossible — the chained pattern mirrors notification-events. **Article 15 data minimization (advisor 2026-05-21)**: access exports are projected to `{role, content, messageTimestamp}` only via `_project_recent_messages_row` — internal identifiers (`sessionId`, `messageId`, `expires_at`) are intentionally omitted. This diverges from form-submissions/notification-sends walkers (which return full rows) and is justified by `content`'s free-text nature. **F-DSAR4 chat-only gap**: subjects who chatted without ever submitting a form are unreachable via this walker — compensating control = 24h TTL on the row; structural fix tracked in D5 F-DSAR4 (durable: writer emits subject linkage on message row). **Defense-in-depth**: walker requires non-empty `tenant_id` even though it doesn't use it directly. **Bounded fan-out**: `MAX_SESSION_IDS_PER_INVOCATION = 200` (timeout protection) and `MAX_EXPORTED_MESSAGES = 1000` (Lambda 6 MB response cap); overflow surfaces in `truncated_session_id_count` + `exported_messages_truncated_count`. **Continue-on-error** per session_id (`failed_session_ids`). **Logging discipline**: walker NEVER logs `content` — only `sessionId` + `messageTimestamp` on errors.
- Other 2 surfaces (`conversation-summaries`, `audit-read-only`) return honest `manual_followup` and `walker_results[surface].status = "deferred"`

## Status semantics

The response `status` field discriminates between four outcomes (per audit fix-now #5):

| Status | Meaning |
|---|---|
| `completed` | All walkers ran cleanly; no errors; no deferred surfaces. (Will not occur until all 6 walkers ship.) |
| `partial` | At least one walker ran successfully but at least one surface is still deferred. **Today's default outcome** for every DSAR (5 of 6 surfaces are deferred). |
| `partial_error` | At least one walker errored mid-batch — query failure, corrupted-row skip, or surface-audit collision. Operator must inspect logs to determine completeness. |
| `failed` | Env-guard, input validation, or `request_received` audit-collision failure. Lambda never reached the walker dispatch. |

## Coverage gaps

**form-submissions (F-DSAR1, temporal)**: The walker filters by `pii_subject_id`, which only exists on submissions written **after** lambda PR #130 (2026-05-18, Phase 1). Pre-Phase-1 rows lack the attribute and won't match — the response includes an explicit `manual_followup` flagging this. Durable fix = Apply-2 backfill (deferred per Decision A). Interim = manual email-keyed walk for suspected pre-Phase-1 subjects.

**recent-messages (F-DSAR4, structural)**: The walker reaches only sessions linked via form-submissions (chained walk). Subjects who chatted in the widget without ever submitting a form have no durable subject linkage on the message row — the writer never emits subject identifiers. Their messages age out via the 24h TTL on `staging-recent-messages` (best-effort within 48h per DynamoDB TTL semantics). Compensating control = TTL; structural fix tracked in D5 F-DSAR4 (writer emits subject linkage on message row OR rotates `sessionId` on subject-context change). The walker's `manual_followups` block includes an operator-actionable CLI snippet for both direct sessionId queries (when known out-of-band) and a last-resort content-substring scan (case-sensitive — flagged as false-positive-prone).

## What's deferred (follow-up PRs)

- Per-surface walkers for the 2 remaining MFS surfaces (conversation-summaries, audit-read-only). Each waits on schema verification against MFS writer code; conversation-summaries will likely chain off the same form-submissions session_ids as recent-messages.
- Milestone 2 (item 1b): Meta channel-mappings PSID walk, S3 fan-out, ARCHIVE_BUCKET walk
- Capability-bundle item 3: EventBridge SLA alarm Lambda
- Capability-bundle item 6: integration tests against deployed Lambda + staging DDB
- Apply-2 backfill of `pii_subject_id` onto pre-Phase-1 form-submission rows (F-DSAR1 durable fix)
- Writer-side subject-linkage emission on `staging-recent-messages` rows (F-DSAR4 durable fix)
- Staff-recipient notification walk (operator/staff PII under different controller relationship per D5 G-H + F9; current scope is consumer-direct only). Resolution depends on counsel Q1/Q2 + F9 three-part mitigation per Step 10 v3 §F9.

## Contract

```bash
aws lambda invoke \
  --function-name picasso-pii-dsar-staging \
  --profile myrecruiter-staging \
  --payload '{
    "subject_identifier": "subject@example.com",
    "identifier_type":    "email",
    "request_type":       "delete",
    "tenant_id":          "TEN123",
    "operator":           "operator@myrecruiter.ai",
    "dsar_id":            "uuid-from-dsar-log",
    "dry_run":            true
  }' \
  --cli-binary-format raw-in-base64-out \
  response.json
```

### Response shape

```json
{
  "dsar_id":           "<echo>",
  "status":            "completed | partial | partial_error | failed",
  "pii_subject_id":    "<resolved-opaque-id-or-null>",
  "rows_touched":      {"form-submissions": 2, "notification-sends": 0, "...": 0},
  "exported_rows":     {"form-submissions": [<row>, <row>]},
  "manual_followups":  ["form-submissions: dry_run=true; 2 row(s) would be deleted; ...", "..."],
  "audit_row_pks":     ["<dsar_id>|<event_timestamp>", "..."]
}
```

`exported_rows` is populated only on `request_type=access` and only for surfaces with implemented walkers.

`audit_row_pks` lists every audit event written: `request_received` + one `surface_walked:<surface>` per non-deferred walker outcome + `closed`.

## Runtime

- Runtime: `python3.11`
- Memory: 256 MB
- Timeout: 60 s (walk pessimistic case allowance; current scaffold completes in ~1 s)
- Env vars: none (table names + expected account are constants — IaC pins them)
- Execution role: `picasso-pii-dsar-staging-role` (picasso PR #158)

## Deliberate design choices

- **`dry_run` defaults to `true`.** Operator must explicitly set `"dry_run": false` to delete. Typo / missing field can never produce unintended deletion.
- **Cold-start env-guard.** Lambda refuses to run in any account other than `525409062831` (staging). Promotion to prod requires explicit code change — never config-only.
- **Audit writes never read or delete.** Role grants `PutItem` only on `picasso-pii-dsar-audit-staging`. Append-only event log. SLA-alarm Lambda (item 3) reads under a separate role.
- **`status` field duplicated to top-level attribute** so the `StatusIndex` GSI (PR #157) can be queried directly — no nested-attribute scan.
- **Email normalization mirrors Phase-1 writer** (`.strip().lower()`) — required for subject-index `get_item` to hit.

## Tests

```bash
cd Lambdas/lambda/picasso_pii_dsar_staging
python3 -m pytest test_dsar.py -v
```

All unit tests use `unittest.mock` to stub boto3 — no AWS calls during test.
Integration tests against staging DDB land in item 6 (follow-up).

## Why walkers ship one at a time

Per-surface walker implementations require verifying each table's subject-linking attribute against the MFS writer code (D5 F12 is a known gap for the unindexed surfaces). Shipping one walker per PR — `form-submissions` is the first because its `pii_subject_id` attribute is contractually written by Phase 1 (#130) — keeps each surface's behavior reviewable in isolation and lets integration tests verify reachability surface-by-surface.

See [`CONSUMER_PII_REMEDIATION.md`](https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/CONSUMER_PII_REMEDIATION.md) v3 §"The single named next concrete Path A action" for the full bundle plan.
