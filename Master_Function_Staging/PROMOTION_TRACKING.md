# Master_Function: Staging → Production Promotion

**Status:** DRAFT — awaiting focused future session.
**Companion:** `STAGING_TO_PROD_PROMOTION_BRIEF.md` (the runbook authored from the 2026-05-02 architecture investigation; merged via PR #37).

## Why this PR exists

Production (`Master_Function`, alias `production` → v14, last published August 2025) is **9 months behind** `Master_Function_Staging`. PR #37's brief enumerates: 10 modules in staging not in prod, +480 lines on `lambda_function.py`, env var divergence (4 prod vars vs 15 staging vars), DynamoDB table dependencies that may not exist in prod, IAM/CloudFront/tenant-config compatibility unknowns.

This PR is the placeholder for the actual promotion event. A future session picks up this branch, executes the promotion runbook, deploys to production, and ships the merged PR.

## Do NOT merge this PR until

- [ ] P0a Phase 2 (decoder hardening) has been live on staging ≥25h with no incidents.
- [ ] Pre-flight env var audit complete per `STAGING_TO_PROD_PROMOTION_BRIEF.md` §3.
- [ ] All production-equivalent DDB tables confirmed to exist (§4).
- [ ] Production IAM role audited and updated (§5).
- [ ] Production tenant configs parse cleanly under new code (§5 — schema validation).
- [ ] Rollback to v14 verified working in dry-run.
- [ ] Maintenance window scheduled (no Friday/holiday per `feedback_deploy_timing.md`).

## When the future session picks this up

1. Read `Master_Function_Staging/STAGING_TO_PROD_PROMOTION_BRIEF.md` end-to-end.
2. Execute the pre-flight checklist (§7 of the brief).
3. Build the deployment artifact from current `Master_Function_Staging/` source.
4. Deploy to `Master_Function`, publish version, update `production` alias.
5. Smoke test production widget; tail CloudWatch.
6. After 7 days of stable operation, run cleanup (§7 of the brief): delete vestigial `staging` and `STAGING` aliases on `Master_Function`; delete v11.

## Links

- [`STAGING_TO_PROD_PROMOTION_BRIEF.md`](./STAGING_TO_PROD_PROMOTION_BRIEF.md) — authored 2026-05-02 via PR #37
- [PR #33 — P0a Phase 1 (iss claim issuance)](https://github.com/longhornrumble/lambda/pull/33) — merged 2026-05-02
- [PR #34 — CI infrastructure repair](https://github.com/longhornrumble/lambda/pull/34) — merged 2026-05-02
- [PR #35 — Master_Function_Staging test debt tracker](https://github.com/longhornrumble/lambda/pull/35) — DRAFT
- [PR #36 — CORS validate_cors_origin restored](https://github.com/longhornrumble/lambda/pull/36) — merged 2026-05-02
- [PR #37 — Promotion brief](https://github.com/longhornrumble/lambda/pull/37) — open
