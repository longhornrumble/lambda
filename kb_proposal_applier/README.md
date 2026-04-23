# kb_proposal_applier

Applies approved KB-freshness proposals emitted by the `kb-proposal` scanner skill (in `picasso-webscraping/rag-scraper/skills/kb-proposal/`).

Input: one proposal JSON from `s3://myrecruiter-picasso/pending-proposals/{tenantId}/{proposalId}.json` + a list of approved item IDs.

Output: updated KB markdown in `s3://kbragdocs/tenants/{tenantId}/*.md`, mutated tenant config in `s3://myrecruiter-picasso/tenants/{tenantId}/{tenantId}-config.json` (with backup), optional Dub shortlinks, optional Bedrock KB ingestion trigger, and an audit trail at `pending-proposals/{tenantId}/{proposalId}/applied.json`.

## Route

```
POST /proposals/{proposalId}/apply
Authorization: Bearer <clerk-jwt>
Content-Type: application/json

{
  "tenantId": "MYR384719",
  "approvedItemIds": ["item-001", "item-002"]
}
```

Response: the updated proposal envelope with `status: "applied" | "partial_apply_error"` and a populated `applicationResult` block (per-item / per-op results, audit key, optional `bedrockSync` outcome).

Status code is `200` on full success, `207 Multi-Status` when one or more items failed.

## Verb dispatch

| Verb | Target | Behavior |
|---|---|---|
| `kb.append` | KB markdown | Insert `markdown` after the line containing `afterMarker` |
| `kb.replace` | KB markdown | Replace the block owned by `sourceMarker` with `markdown` (marker preserved) |
| `kb.remove` | KB markdown | Remove the block owned by `sourceMarker` entirely |
| `config.add` | Config section at `path` | Array push OR dict set by `op.key` / `value.id` / `value.showcase_id` |
| `config.delete` | Config section at `path` | Array filter-by-`matchBy` OR dict delete-by-match |
| `config.append_to_array` | Config array at `path` | Initialize-if-absent then push (same as `config.add` on arrays) |
| `dub.upsert` | Dub /links/upsert | Upsert by deterministic externalId — tag/folder/image read from `config.monitor.*` |

## Per-item atomicity

Each proposal item is atomic: if ANY op within it fails, the item's earlier successful in-memory mutations are reverted and the whole item is marked `error`. Items that succeeded fully are persisted; items that failed leave S3 untouched. This matches the plan's "no rollback across items" rule while keeping each item's paired ops (KB + config + chip + dub) consistent.

## Bedrock KB sync

Fires `StartIngestionJob` after any successful `kb.*` write iff both are set:
- `config.aws.knowledge_base_id`
- `config.monitor.kbDataSourceId`

If either is missing, sync is skipped and the audit trail records `bedrockSync.skipped` with a reason. KB writes still land — the user can trigger ingestion manually via the Bedrock console.

## Environment

| Var | Purpose |
|---|---|
| `CLERK_JWKS_URL` | Optional override for Clerk JWKS endpoint |
| `CONFIG_BUCKET` | Defaults to `myrecruiter-picasso` |
| `KB_BUCKET` | Defaults to `kbragdocs` |
| `DUB_API_KEY` | Required for `dub.upsert` ops |
| `ENFORCE_AUTH` | `"false"` to allow unauthenticated requests (dev) |

## IAM (when deployed)

- `s3:GetObject` / `s3:PutObject` / `s3:ListBucket` on `myrecruiter-picasso/*` and `kbragdocs/*`
- `bedrock:StartIngestionJob` on the tenant's Knowledge Base ARN (scope by `aws:ResourceTag` or list explicitly)
- `lambda:InvokeFunctionUrl` if ever invoked from another Lambda

## Local testing

Unit tests cover the pure kb/config operation functions:

```sh
npm install
node --test *.test.mjs
```

Integration-style driver (bypasses HTTP handler + Clerk, invokes `applyProposal()` directly):

```sh
AWS_PROFILE=chris-admin AWS_REGION=us-east-1 \
  node ../../../Sandbox/kb_applier_test/run-local.mjs \
    <tenantId> <proposalId> <itemId> [<itemId> ...]
```

The sandbox tenant `MYR384719` is the only safe target for live tests.
