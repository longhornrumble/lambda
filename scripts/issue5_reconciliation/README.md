# Issue #5 reconciliation script

Phase 1 deliverable per v7 plan §"Reconciliation script spec". Required gate
before Phase 2 (per-tenant production cutover) can start.

Merges session-summary rows from a source table into a destination table for
sessions whose `started_at` falls within the Phase 2 split-write cutover
window. Three-case merge (A: copy, B: field-wise merge, C: leave dest alone)
runs idempotently — re-running with the same inputs produces identical
destination state.

## Run

```bash
AWS_PROFILE=chris-admin python3 reconcile.py \
  --source-table picasso-session-summaries \
  --dest-table picasso-session-summaries-prod \
  --tenant-hash my87674d777bf9 \
  --cutover-start-iso 2026-05-04T20:00:00.000Z \
  --cutover-end-iso 2026-05-04T20:30:00.000Z \
  --region us-east-1 \
  [--dry-run]
```

Always start with `--dry-run` to verify counts before the real pass.

## IAM scope

- `dynamodb:Query` on source table (with `LeadingKeys = TENANT#{hash}`)
- `dynamodb:GetItem` on destination table
- `dynamodb:PutItem` on destination table
- `dynamodb:Query` on destination table (for Case C count)

## Tests

```bash
AWS_DEFAULT_REGION=us-east-1 AWS_ACCESS_KEY_ID=fake AWS_SECRET_ACCESS_KEY=fake \
  python3 -m pytest test_reconcile.py -q
```

7 v7-spec scenarios + 2 invariants pass against moto-mocked DDB tables.

## Operator audit

Every action emits a structured log line:
- `reconcile_query_complete` (window query)
- `reconcile_case_a_copy` (source-only row copied)
- `reconcile_case_b_merge` (both-existed merge)
- `reconcile_case_b_noop` (merge would not change destination)
- `reconcile_complete` (final A/B/C summary)
- `reconcile_failed` (uncaught error — operator investigates)

Grep on `evt` field to audit a run.
