# Master_Function: Vestigial Alias Cleanup

**Status:** DRAFT — runs AFTER the staging→prod promotion lands and stabilizes (≥7 days post-promotion).
**Companion:** `PROMOTION_TRACKING.md` (PR #38), `STAGING_TO_PROD_PROMOTION_BRIEF.md` (PR #37).

## Why this PR exists

After the architecture migration on 2026-05-02, the staging widget routes to `Master_Function_Staging` (a separate Lambda function). The previous routing — through `Master_Function:staging` alias — is no longer used. Three vestigial artifacts remain on `Master_Function`:

| Artifact | Why vestigial | Action |
|---|---|---|
| `staging` alias → v11 (Aug 2025 "Testing staging alias") | API Gateway no longer routes here | `aws lambda delete-alias --function-name Master_Function --name staging` |
| `STAGING` alias → v6 | Even older test alias; no traffic | `aws lambda delete-alias --function-name Master_Function --name STAGING` |
| Version 11 itself | Only target of the deleted `staging` alias | `aws lambda delete-function --function-name Master_Function --qualifier 11` |

After cleanup, `Master_Function` has only the `production` alias (the proper two-function deployment model the user prefers).

## Do NOT merge until

- [ ] PR #38 (staging→prod promotion) has been merged AND production has been live ≥7 days with no incidents
- [ ] No CloudFront/API Gateway/external integration still references `Master_Function:staging` or `:STAGING` (grep all repos for those strings before deleting)

## Verification before deletion

```bash
# Confirm no integrations reference the staging alias
aws apigatewayv2 get-integrations --api-id kgvc8xnewf --profile chris-admin \
  --query "Items[?contains(IntegrationUri, 'Master_Function:staging') || contains(IntegrationUri, 'Master_Function:STAGING')]"
# Expected: empty array

# Confirm no Lambda Function URL references
aws lambda get-function-url-config --function-name Master_Function --qualifier staging --profile chris-admin
# Expected: ResourceNotFoundException
```

## Cleanup commands (when ready)

```bash
aws lambda delete-alias --function-name Master_Function --name staging --profile chris-admin
aws lambda delete-alias --function-name Master_Function --name STAGING --profile chris-admin

# v11 deletion is OPTIONAL — Lambda retains version history naturally;
# removing v11 is purely cosmetic and irreversible. Recommend leaving v11 in
# place as historical archive unless storage cost is a real concern.
# aws lambda delete-function --function-name Master_Function --qualifier 11 --profile chris-admin
```

## Verification after cleanup

```bash
aws lambda list-aliases --function-name Master_Function --profile chris-admin \
  --query 'Aliases[*].Name'
# Expected: ["production"]
```

## Why this is a separate PR (not part of the promotion PR)

If something goes wrong with the promotion and we need to roll back, having the vestigial aliases still in place gives us an extra safety net (we could potentially re-route traffic to v11 in an emergency). Once production is stable for a week post-promotion, the aliases are unambiguously dead and safe to remove.

## Links

- [PR #37 — Promotion brief](https://github.com/longhornrumble/lambda/pull/37)
- [PR #38 — Promotion event placeholder](https://github.com/longhornrumble/lambda/pull/38)
