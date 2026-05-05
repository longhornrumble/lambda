# Master_Function: Staging → Production Promotion Brief

**Audience.** A future agent session preparing to promote `Master_Function_Staging` code to the `Master_Function` (production) Lambda.

**Status.** NOT YET EXECUTED. This brief captures findings from a 2026-05-02 P0a JWT security investigation that surfaced unexpected production-vs-staging drift. The promotion is recommended but should run as its own dedicated event after P0a Phase 2 lands on staging.

**Bottom line.** Production is running pre-September 2025 code. Staging has 9 months of accumulated work that has never been promoted. This is the bigger architectural problem; the JWT `iss` claim work that surfaced it (P0a) is just one of many drift items.

---

## 1. The two functions in AWS

| Function | Last Modified | Aliases | Routing |
|---|---|---|---|
| `Master_Function` | 2026-04-13 ($LATEST) | `production` → v14 (Aug 2025), `staging` → v11 (Aug 2025), `STAGING` → v6 | API Gateway integration `vxhbed5` (route `/Master_Function`) routes to production. Was previously also routing `/staging/Master_Function` here via `staging` alias — re-pointed away on 2026-05-02. |
| `Master_Function_Staging` | 2026-05-02 (active) | none | API Gateway integration `859k24v` (route `/staging/Master_Function`) — re-pointed here on 2026-05-02 after the staging alias on `Master_Function` was found to be stale. |

**Note on `Master_Function`'s aliases.** After this promotion, the `staging` and `STAGING` aliases on `Master_Function` should be deleted — they are vestigial from a deprecated routing model. The new model is one Lambda per environment (`Master_Function` for prod, `Master_Function_Staging` for staging) with no aliases.

---

## 2. Code divergence summary

### Files identical between prod (v14, Aug 2025) and staging (current)

```
audit_logger.py
aws_client_manager.py
bedrock_handler_optimized.py
create_audit_table.py
create_blacklist_table.py
session_utils.py
state_clear_handler.py
tenant_inference.py
token_blacklist.py
```

### Files diverged

| File | prod lines | staging lines | Δ |
|---|---|---|---|
| `lambda_function.py` | 1510 | 1990 | **+480** |
| `conversation_handler.py` | 1121 | 1177 | +56 |
| `bedrock_handler.py` | 210 | 325 | +115 |
| `tenant_config_loader.py` | 594 | 638 | +44 |
| `response_formatter.py` | 258 | 274 | +16 |
| `intent_router.py` | 237 | 251 | +14 |

### Modules in staging but NOT in production

These represent entire features that have been developed and validated in staging but never shipped to production:

- `form_handler.py` — conversational forms feature
- `form_cta_enhancer.py` — Phase 1B HTTP fallback parity
- `template_renderer.py` — notification templates
- `contact_extractor.py` — PII extraction logic
- `tenant_registry.py` + `backfill_tenant_registry.py` — tenant registry layer
- `create_conversation_tables.py` — DDB schema scripts
- `create_employee_registry_table.py` + `create_employee_registry_table_v2.py`
- `create_tenant_registry_table.py`
- `notification_templates.json` (data file)

### Bundled Python dependencies in production but NOT in staging

Production's deployment package includes vendored Python packages (~2,000 files) that staging's package does not:

- `boto3` (1.40.11) and `botocore` (1.40.11) — bundled SDK
- `s3transfer`, `jmespath`, `dateutil`, `six` — boto3 dependencies
- `bin/` — pip-installed executables

Staging relies on the AWS Lambda runtime's built-in boto3 (likely older but functional). Production's bundled boto3 may have intentional reasons (e.g., specific bug fix needed) — investigate before discarding.

### PyJWT version drift

- Production: PyJWT 2.8.0
- Staging: PyJWT 2.10.1

Major.minor difference. Review PyJWT's CHANGELOG between 2.8.0 → 2.10.1 for breaking changes before promotion.

---

## 3. Environment variable divergence (CRITICAL)

This is the highest-risk category. The new staging code references env vars that do NOT exist on the production Lambda. Without setting these, the new code will likely crash on import or first call.

### `Master_Function:production` env vars (only 4)

```
S3_BUCKET=myrecruiter-picasso
CONFIG_BUCKET=myrecruiter-picasso
CLOUDFRONT_DOMAIN=chat.myrecruiter.ai
STREAMING_ENDPOINT=https://xqc4wnxwia2nytjkbw6xasjd6q0jckgb.lambda-url.us-east-1.on.aws/
```

### `Master_Function_Staging` env vars (15)

```
S3_BUCKET=myrecruiter-picasso
CONFIG_BUCKET=myrecruiter-picasso
ENVIRONMENT=staging
VERSION=1.1.2
JWT_EXPIRY_MINUTES=30
JWT_SECRET_KEY_NAME=picasso/staging/jwt/signing-key
MESSAGES_TABLE_NAME=staging-recent-messages
SUMMARIES_TABLE_NAME=staging-conversation-summaries
AUDIT_TABLE_NAME=picasso-audit-staging
TENANT_REGISTRY_TABLE=picasso-tenant-registry-staging
USE_REGISTRY_FOR_RESOLUTION=true
SESSION_POOL_SIZE=10
DYNAMODB_POOL_SIZE=5
MONITORING_ENABLED=true
STREAMING_ENDPOINT=https://7pluzq3axftklmb4gbgchfdahu0lcnqd.lambda-url.us-east-1.on.aws/
```

### Pre-promotion env var task

Before the promotion deploy, set production-equivalent values for every staging env var that is **referenced by the new code**. Production-only values (different from staging):

| Env var | Staging value | Required production value |
|---|---|---|
| `ENVIRONMENT` | `staging` | `production` |
| `JWT_SECRET_KEY_NAME` | `picasso/staging/jwt/signing-key` | `picasso/production/jwt/signing-key` (verify this Secrets Manager secret exists) |
| `MESSAGES_TABLE_NAME` | `staging-recent-messages` | `production-recent-messages` (verify table exists) |
| `SUMMARIES_TABLE_NAME` | `staging-conversation-summaries` | `production-conversation-summaries` (verify) |
| `AUDIT_TABLE_NAME` | `picasso-audit-staging` | `picasso-audit-production` (verify) |
| `TENANT_REGISTRY_TABLE` | `picasso-tenant-registry-staging` | `picasso-tenant-registry-production` (does this exist? if not, **create first**) |
| `STREAMING_ENDPOINT` | (staging streaming URL) | (production streaming URL — already set, leave alone) |
| Other vars (`USE_REGISTRY_FOR_RESOLUTION`, pool sizes, etc.) | copy values | likely safe to copy |

**Audit task:** before promotion, grep the staging code for every `os.environ[...]` and `os.environ.get(...)` reference; ensure each has a corresponding production env var (or the code has a sensible default).

---

## 4. DynamoDB table dependencies

Staging code references tables that may not exist in production:

| Table referenced | Likely production equivalent | Verification command |
|---|---|---|
| `picasso-tenant-registry-staging` | `picasso-tenant-registry-production` | `aws dynamodb describe-table --table-name picasso-tenant-registry-production --profile chris-admin` |
| `picasso-employee-registry-v2-staging` | `picasso-employee-registry-v2-production` | (used by future scheduling work; may not exist yet) |
| `staging-recent-messages` | `production-recent-messages` | (used by conversation_handler) |
| `staging-conversation-summaries` | `production-conversation-summaries` | (used by conversation_handler) |
| `picasso-audit-staging` | `picasso-audit-production` | (used by audit_logger) |

**Pre-promotion task:** verify every production-equivalent table exists. For any that don't, either (a) create them first using `create_*_table.py` scripts in the staging codebase with production names, or (b) confirm the new code path that touches each table is dormant in production.

---

## 5. Other divergence categories

### IAM role permissions

Staging Lambda's execution role has DDB permissions for tables that may not be in production's role. Compare:

```
aws iam get-role --role-name <staging-role-name> --profile chris-admin
aws iam get-role --role-name <prod-role-name> --profile chris-admin
```

The new code may need additional `dynamodb:*` permissions on production tables that production's role doesn't currently grant.

### CloudFront / API Gateway routing

Already audited 2026-05-02. The current state:

- Staging widget (`staging.chat.myrecruiter.ai/Master_Function`) → CloudFront `E1CGYA1AJ9OYL0` → API Gateway `kgvc8xnewf` route `/staging/Master_Function` → integration `859k24v` → `Master_Function_Staging`. **Working correctly.**
- Production widget (`chat.myrecruiter.ai/Master_Function`) → production CloudFront → API Gateway `kgvc8xnewf` route `/Master_Function` → integration `vxhbed5` → `Master_Function`. **No changes needed.**

After promotion, no API Gateway changes required — production routing already points at `Master_Function`.

### Tenant config schema

Production tenant configs in S3 (`s3://myrecruiter-picasso/tenants/{id}/{id}-config.json`) may lack fields the new staging code expects (e.g., `cors.allowed_origins`, `notifications.*`, `forms.*`). Newer tenant configs (created via picasso-config-builder) will have these; older configs (created during deploy_tenant_stack era) may not.

**Pre-promotion task:** parse every production tenant config through the new code's schema validation. Flag any tenant whose config will break under the new code.

### Bedrock / Streaming dependencies

`bedrock_handler.py` diverged by +115 lines. The new staging version may call Bedrock APIs differently (different model IDs, different prompt structures). If the production Bedrock Knowledge Base is configured for older API patterns, the new code may fail.

---

## 6. P0a JWT `iss` claim — relationship to this promotion

The 2026-05-02 P0a investigation (Security-Reviewer P0 finding) added an `iss=myrecruiter-chat` claim to all chat-session JWT issuance points. This was Phase 1 of a two-phase rollout:

1. **Phase 1 (deployed to staging 2026-05-02):** issuance side adds `iss`. Decoders unchanged. Backward-compatible.
2. **Phase 2 (planned for 2026-05-03+):** decoders enforce `iss == "myrecruiter-chat"`. New tokens (with `iss`) pass; old tokens (without `iss`) reject. Requires ≥25h soak between Phase 1 and Phase 2 so all live tokens turn over.

**Implication for this promotion:**

- If promotion happens AFTER Phase 2 ships on staging, both phases land on production simultaneously through the normal staging→prod promotion. **This is the cleanest path.**
- If promotion happens DURING the Phase 1→Phase 2 window, the production deploy includes Phase 1 only. Phase 2 then needs its own follow-up production deploy 25h after the production Phase 1 deploy.
- If promotion happens BEFORE Phase 2 on staging — don't. Wait for Phase 2 staging soak completion.

**Current status as of 2026-05-02:** Phase 1 is live on staging with `iss` claim. Phase 2 staging deploy planned for 2026-05-03 ~20:35 UTC. The promotion event should be scheduled for 2-3 days AFTER Phase 2 lands on staging.

---

## 7. Recommended promotion runbook (sketch)

Before the dedicated promotion session, draft a real runbook based on this brief. Skeleton:

### Pre-flight (≥24h before promotion)
- [ ] All production-equivalent env vars identified and prepared (see §3)
- [ ] All production-equivalent DDB tables exist (see §4)
- [ ] Production IAM role audited and updated for new permissions (see §5)
- [ ] Production tenant configs parse cleanly under new code (see §5 — schema validation)
- [ ] Production Bedrock KB compatibility verified (see §5)
- [ ] Phase 2 of P0a has been live on staging ≥25h with no incidents
- [ ] Rollback plan documented: "revert to v14 of Master_Function" — verify v14 is still queryable via `aws lambda get-function --function-name Master_Function --qualifier 14`
- [ ] Maintenance window scheduled (avoid Friday/holiday per `feedback_deploy_timing.md`)
- [ ] Notification sent to any active production tenant admins about the deploy window

### Day-of (in order)
- [ ] Verify pre-flight checklist 100% green
- [ ] Snapshot production tenant configs to S3 backup prefix
- [ ] Download v14 of `Master_Function` deployment artifact for emergency rollback
- [ ] Deploy: `aws lambda update-function-code --function-name Master_Function --zip-file fileb://staging-current.zip`
- [ ] Wait for `LastUpdateStatus: Successful`
- [ ] Publish version with descriptive notes: "Promoted from staging — includes 9 months of accumulated work + P0a Phase 1 + Phase 2"
- [ ] Update `production` alias to the new version
- [ ] Smoke test: open production widget; send 2-3 messages; verify responses
- [ ] Tail CloudWatch on `/aws/lambda/Master_Function` for 30 minutes
- [ ] Watch existing tenant traffic patterns; alert on any error rate spike

### Rollback (if anything looks wrong)
- [ ] `aws lambda update-alias --function-name Master_Function --name production --function-version 14`
- [ ] Verify production widget returns to working state
- [ ] Document the failure mode in a postmortem (per `feedback_blameless_postmortems.md`)

### Cleanup (after 1 week of stable operation)
- [ ] Delete `staging` and `STAGING` aliases on `Master_Function` (vestigial)
- [ ] Delete v11 (Master_Function:staging-aliased version) — old test deploy
- [ ] Update `CLAUDE.md` to document the two-function architecture explicitly

---

## 8. Single biggest risk

**Production env vars are missing for half of what the new code references.** The most likely failure mode is `KeyError: 'TENANT_REGISTRY_TABLE'` (or similar) on the first invocation that touches `tenant_registry.py`. This isn't catchable in staging; it only manifests in production where the env var isn't set.

**Mitigation:** the env var audit in §3 is the highest-leverage pre-flight task. Do it carefully.

---

## 9. References

- Live API Gateway integration map (verified 2026-05-02):
  - Integration `859k24v` (`/staging/Master_Function/*`) → `Master_Function_Staging`
  - Integration `vxhbed5` (`/Master_Function/*`) → `Master_Function`
- Lambda Function URL endpoints:
  - Staging streaming: `https://7pluzq3axftklmb4gbgchfdahu0lcnqd.lambda-url.us-east-1.on.aws/` (Bedrock_Streaming_Handler_Staging)
  - Production streaming: `https://xqc4wnxwia2nytjkbw6xasjd6q0jckgb.lambda-url.us-east-1.on.aws/` (Bedrock_Streaming_Handler)
- Related work in flight:
  - P0a Phase 1: PR #33 (merged 2026-05-02 18:51 UTC)
  - CI infra fix: PR #34 (merged 2026-05-02 19:10 UTC)
  - Test debt tracker: PR #35 (DRAFT, separate session)
  - CORS fix: PR #36 (merged 2026-05-02 19:55 UTC)
  - This brief: this PR

## 10. Change log

| Date | Change | Author |
|---|---|---|
| 2026-05-02 | Initial brief authored after surfacing the 9-month divergence during P0a Phase 1 staging migration. | Chris + Claude |
