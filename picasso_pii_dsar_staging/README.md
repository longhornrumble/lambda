# picasso-pii-dsar-staging

Operator-invocable DSAR (Data Subject Access Request) fulfillment Lambda for the
MFS-scoped surfaces. Capability-bundle item 1a per
[`CONSUMER_PII_REMEDIATION.md`](https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/CONSUMER_PII_REMEDIATION.md)
§"Path A Re-baseline v3".

## What this Lambda does today

- Cold-start env-guard (refuses to run outside staging account 525)
- Operator input validation (typed payload, `dry_run` default `true`)
- Subject resolution via `picasso-pii-subject-index-staging`
- Append-only audit writes to `picasso-pii-dsar-audit-staging`
- **`form-submissions` walker** — tenant-scoped Query (PK=`tenant_id`) + FilterExpression on `pii_subject_id`:
  - `request_type=access` → matched rows in `exported_rows`
  - `request_type=delete` + `dry_run=true` → count only
  - `request_type=delete` + `dry_run=false` → `DeleteItem` per matched row
- Other 5 surfaces (`notification-sends/events`, `recent-messages`, `conversation-summaries`, `audit-read-only`) return honest `manual_followup`

## Coverage gap (form-submissions)

The walker filters by `pii_subject_id`, which only exists on submissions written **after** lambda PR #130 (2026-05-18, Phase 1). Pre-Phase-1 rows lack the attribute and won't match — the response includes an explicit `manual_followup` flagging this. Durable fix = Apply-2 backfill (deferred per Decision A). Interim = manual email-keyed walk for suspected pre-Phase-1 subjects.

## What's deferred (follow-up PRs)

- Per-surface walkers for the 5 remaining MFS surfaces (notification-sends/events, recent-messages, conversation-summaries, audit-read-only). Each waits on schema verification against MFS writer code.
- Milestone 2 (item 1b): Meta channel-mappings PSID walk, S3 fan-out, ARCHIVE_BUCKET walk
- Capability-bundle item 3: EventBridge SLA alarm Lambda
- Capability-bundle item 6: integration tests against deployed Lambda + staging DDB
- Apply-2 backfill of `pii_subject_id` onto pre-Phase-1 form-submission rows

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
  "status":            "partial",
  "pii_subject_id":    "<resolved-opaque-id-or-null>",
  "rows_touched":      {"form-submissions": 2, "notification-sends": 0, "...": 0},
  "exported_rows":     {"form-submissions": [<row>, <row>]},
  "manual_followups":  ["form-submissions: dry_run=true; 2 row(s) would be deleted; ...", "..."],
  "audit_row_pks":     ["<dsar_id>|<event_timestamp>", "..."]
}
```

`exported_rows` is populated only on `request_type=access` and only for surfaces with implemented walkers.

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
