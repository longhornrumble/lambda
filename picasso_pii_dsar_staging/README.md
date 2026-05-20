# picasso-pii-dsar-staging

Operator-invocable DSAR (Data Subject Access Request) fulfillment Lambda for the
MFS-scoped surfaces. Capability-bundle item 1a per
[`CONSUMER_PII_REMEDIATION.md`](https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/CONSUMER_PII_REMEDIATION.md)
§"Path A Re-baseline v3".

## What this milestone ships (PR C — milestone 1 scaffold)

- Cold-start env-guard (refuses to run outside staging account 525)
- Operator input validation (typed payload, dry_run default true)
- Subject resolution via `picasso-pii-subject-index-staging`
- Append-only audit writes to `picasso-pii-dsar-audit-staging` (PR #157)
- Per-surface walker scaffolds returning honest `manual_followup` for the
  5 MFS surfaces + audit-read

## What's deferred (follow-up PRs)

- Per-surface walker implementations (`form-submissions`, `notification-sends`,
  `notification-events`, `recent-messages`, `conversation-summaries`,
  `audit-read-only`) — each requires verifying the subject-linking attribute
  against the MFS writer code
- `aws_lambda_function` Terraform resource (lands in picasso `infra/` repo
  after this PR merges so the package path exists)
- Milestone 2 (item 1b): Meta channel-mappings PSID walk, S3 fan-out,
  ARCHIVE_BUCKET walk
- Integration tests (item 6): tested against deployed Lambda + staging DDB

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
  "rows_touched":      {"form-submissions": 0, "...": 0},
  "manual_followups":  ["form-submissions: Walker pending — ...", "..."],
  "audit_row_pks":     ["<dsar_id>|<event_timestamp>", "..."]
}
```

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

## Why scaffold-first

The handler shell + audit-writes + subject-resolution are the parts whose semantics
are well-understood from the existing `picasso-pii-subject-index-staging` and the
PR #157 audit table schema. Per-surface walker implementations require verifying
each table's subject-linking attribute against the MFS writer code (D5 F12 is a
known gap for the unindexed surfaces). Shipping speculation-free scaffolding
avoids walker bugs that would only surface in integration tests later.

See [`CONSUMER_PII_REMEDIATION.md`](https://github.com/longhornrumble/picasso/blob/staging/docs/roadmap/PII-Project/CONSUMER_PII_REMEDIATION.md)
v3 §"The single named next concrete Path A action" for the full bundle plan.
