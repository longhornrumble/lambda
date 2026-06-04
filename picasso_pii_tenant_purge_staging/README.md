# picasso-pii-tenant-purge-staging

Operator-invocable **per-tenant offboarding purge** Lambda — P1 (Class A surfaces).

- **Design:** [`docs/roadmap/PII-Project/tenant-offboarding-purge-design.md`](../../../docs/roadmap/PII-Project/tenant-offboarding-purge-design.md) (sign-off complete 2026-06-03, picasso#361).
- **Routes to:** [`data-retention-strategy.md`](../../../docs/roadmap/PII-Project/data-retention-strategy.md) §9 "per-tenant offboarding purge" — the one genuine build.

## Why a new Lambda (not a DSAR `request_type`)

The DSAR Lambda is **subject-scoped** (resolve one `pii_subject_id`, filter every
surface by it). A tenant purge is **partition-scoped** with no subject filter, and
the surfaces split into reachability classes (design §3). Bolting `tenant_purge`
onto the DSAR handler would broaden its blast radius from per-subject DeleteItem
to whole-partition delete and still not solve the hard surfaces. This Lambda is a
dedicated capability with its own execution role and audit table, reusing the
DSAR Lambda's safety patterns as a template.

## What P1 does (Class A only)

Cleanly tenant-partitioned DynamoDB surfaces, plus the notification-events chain:

| Surface | Reached by |
|---|---|
| `picasso-form-submissions-staging` | Query `PK=tenant_id` → delete partition |
| `picasso-notification-sends` | Query `PK=TENANT#{tenant_id}` → delete; yields `message_id`s |
| `picasso-notification-events` | chained via `message_id`s (ByMessageId GSI) |
| `picasso-pii-subject-index-staging` | Query `PK=tenant_id` → delete (re-id key) |
| `picasso-sms-usage` | Query `PK=tenant_id` → delete (also 30d TTL) |

**Not in P1** (design §8 decisions, RESOLVED 2026-06-03):
- **Class B** (recent-messages 24h, session-events 90d, archive): **TTL age-out** — not force-deleted (no sub-TTL erasure promise in v1).
- **Class C** (session-summaries, `tenant_hash`-keyed): **P2** (operator passes `tenant_hash`).
- **Class D** (Glacier log archive, by log group): **365d age-out**.

## Carve-outs that survive the purge (design §5, counsel-gated — never deleted)

- `picasso-sms-consent` (opt-in proof + **STOP/opt-out** rows) — legal floor (4–5yr).
- SES account-level suppression list.
- Audit tables — this Lambda **writes** its own audit; it never deletes audit.

## Invocation contract

```json
{
  "tenant_id":       "<tenant_id>",
  "operator":        "<email of operator>",
  "purge_id":        "<uuid>",
  "grace_confirmed": true,
  "dry_run":         true
}
```

**Dual gate:** deletion runs ONLY when `dry_run=false` **AND** `grace_confirmed=true`.
Either alone → a dry-run count (deletes nothing) + a `manual_followups` note. `dry_run`
defaults `true`. An accidental whole-tenant delete from a single typo is structurally
impossible.

```bash
# Dry-run (default) — see what WOULD be deleted:
aws lambda invoke --function-name picasso-pii-tenant-purge-staging \
  --payload '{"tenant_id":"TEN-X","operator":"chris@myrecruiter.ai","purge_id":"<uuid>","grace_confirmed":true}' \
  --cli-binary-format raw-in-base64-out out.json && cat out.json

# Real delete — BOTH flags required:
aws lambda invoke --function-name picasso-pii-tenant-purge-staging \
  --payload '{"tenant_id":"TEN-X","operator":"chris@myrecruiter.ai","purge_id":"<uuid>","grace_confirmed":true,"dry_run":false}' \
  --cli-binary-format raw-in-base64-out out.json && cat out.json
```

## Safety model (reused from DSAR)

- **Account guard** — refuses to run outside staging account `525409062831`.
- **Dual gate** + **dry-run default** (above).
- **Append-only audit** to `picasso-pii-tenant-purge-audit-staging`: `purge_requested → surface_purged:<surface> → closed`, idempotent on `(purge_id, event_timestamp)`.
- **Corrupted-row skip** + per-surface **delete-failure counts** — one bad row never aborts the cascade.
- **Bounded fan-out** on the events chain; overflow surfaces as a followup (idempotent re-invoke drains it).
- **Redaction in logs** — never logs row PII (email/content); key-field presence only.
- **Idempotent re-run** — re-invoking after a partial failure deletes only what remains.

## Deploy

Operator-invoked Lambda (like DSAR) — **not** in the CI deploy matrix. Terraform owns
function existence + role binding (placeholder zip + `lifecycle.ignore_changes`); real
code deploys via `aws lambda update-function-code` after staging validation. Runs on
stdlib + boto3 (`requirements.txt` intentionally empty).

## Tests

```bash
cd Lambdas/lambda/picasso_pii_tenant_purge_staging
pip install pytest boto3 'moto<5'
python -m pytest -v
```
