# Master_Function_Staging Deployment v1.4.1

**Date**: 2025-10-30
**Version**: 10
**Status**: ✅ Deployed to AWS Lambda Staging
**PRD**: Action Chips Explicit Routing with Fallback Navigation Hub

---

## Deployment Summary

Successfully deployed Master_Function_Staging with 3-tier explicit routing logic, eliminating unreliable keyword matching and providing predictable CTA routing.

### Version Information

- **Lambda Version**: 10
- **Code SHA256**: `J+tMDfb63YdnyRf/kRMv8GozkcAr+KopizqyHgkBJhw=`
- **Code Size**: 189,725 bytes
- **Runtime**: Python 3.11
- **Last Modified**: 2025-10-30T21:51:03.000+0000
- **Deployment Status**: Successful

### Version Description
```
v1.4.1 - Action Chips Explicit Routing: 3-tier hierarchy (chips->CTAs->fallback),
deprecated keywords, backward compat. 9/9 tests passing. Deployed: 2025-10-30
```

---

## What Was Deployed

### New Functions

1. **`get_conversation_branch(metadata, tenant_config)`** (lines 626-687)
   - Implements 3-tier routing hierarchy
   - **Tier 1**: Action chip explicit routing via `metadata.action_chip_triggered` + `metadata.target_branch`
   - **Tier 2**: CTA explicit routing via `metadata.cta_triggered` + `metadata.target_branch`
   - **Tier 3**: Fallback navigation hub via `tenant_config.cta_settings.fallback_branch`
   - Returns branch name or None for graceful degradation

2. **`build_ctas_for_branch(branch_name, tenant_config, completed_forms)`** (lines 689-789)
   - Builds CTA array from specific conversation branch
   - Filters completed forms (lovebox, daretodream, etc.)
   - Returns max 3 CTAs
   - Handles primary + secondary CTA logic

### Modified Functions

- **`handle_chat()`** - Updated to use new 3-tier routing instead of keyword detection
- **`lambda_handler()`** - No changes, routing logic isolated to helper functions

### Deprecated (But Kept)

- **Keyword detection** - Marked as deprecated but kept for backward compatibility
- `detection_keywords` field in conversation_branches still supported but ignored by routing logic

---

## Testing & Validation

### Unit Tests: ✅ 9/9 Passing

**Test File**: `test_routing_hierarchy.py`

**Test Coverage**:
1. ✅ Scenario 1: Action chip with valid target_branch → routes to branch
2. ✅ Scenario 2: Action chip with null target_branch → routes to fallback
3. ✅ Scenario 3: CTA with valid target_branch → routes to branch
4. ✅ Scenario 4: CTA with null target_branch → routes to fallback
5. ✅ Scenario 5: Free-form query → routes to fallback
6. ✅ Scenario 6: No fallback configured → returns None (graceful degradation)
7. ✅ Test 7: Invalid target_branch → falls through to fallback
8. ✅ Test 8: buildCtasForBranch with completed forms → filters correctly
9. ✅ Test 9: Empty branch → returns empty array

**Execution Time**: 0.001s

### Integration Tests: ✅ 11/11 Passing

**Test File**: `test_integration_e2e.py`

**Coverage**: End-to-end flow from frontend metadata → Lambda routing → CTA response

---

## Rollback Procedure

If issues occur, rollback to previous version:

```bash
# Rollback to version 9 (previous stable)
aws lambda update-function-configuration \
  --function-name Master_Function_Staging \
  --publish \
  --revision-id <previous-revision-id> \
  --profile ai-developer

# Or use alias to switch versions instantly
aws lambda update-alias \
  --function-name Master_Function_Staging \
  --name staging \
  --function-version 9 \
  --profile ai-developer
```

**Previous Stable Version**: 9
**Previous Description**: "v1.1.1 - Bubble webhook + DynamoDB permissions"

---

## Backward Compatibility

### v1.3 Configs (Array Format)
- ✅ Still supported
- Action chips in array format gracefully degrade to Tier 3 fallback
- No breaking changes

### v1.4 Configs (Dictionary Format)
- ✅ Full explicit routing enabled
- Action chips have `target_branch` field
- CTAs have `target_branch` field
- `cta_settings.fallback_branch` configured

### Keyword Detection
- ⚠️ DEPRECATED but still functional
- Routing logic ignores `detection_keywords` in favor of explicit routing
- Falls back to keyword detection only if explicit routing returns no results
- Will be fully removed in future version (after all tenants migrate to v1.4)

---

## Deployment Steps Taken

```bash
# 1. Create deployment package
cd /Lambdas/lambda/Master_Function_Staging
zip -r deployment.zip . -x "*.pyc" -x "__pycache__/*" -x "test_*.py" -x "*.md"

# 2. Deploy to Lambda
aws lambda update-function-code \
  --function-name Master_Function_Staging \
  --zip-file fileb://deployment.zip \
  --profile ai-developer

# 3. Verify deployment successful
aws lambda get-function \
  --function-name Master_Function_Staging \
  --profile ai-developer

# 4. Publish new version with notes
aws lambda publish-version \
  --function-name Master_Function_Staging \
  --description "v1.4.1 - Action Chips Explicit Routing: 3-tier hierarchy (chips->CTAs->fallback), deprecated keywords, backward compat. 9/9 tests passing. Deployed: 2025-10-30" \
  --profile ai-developer
```

---

## Post-Deployment Validation

### CloudWatch Logs Monitoring

Monitor for these log patterns:

**Tier 1 (Action Chip Routing)**:
```
[Tier 1] Routing via action chip to branch: {branch_name}
[Tier 1] Invalid target_branch: {branch_name}, falling back to next tier
```

**Tier 2 (CTA Routing)**:
```
[Tier 2] Routing via CTA to branch: {branch_name}
[Tier 2] Invalid target_branch: {branch_name}, falling back to next tier
```

**Tier 3 (Fallback)**:
```
[Tier 3] Routing to fallback branch: {branch_name}
[Tier 3] No fallback_branch configured - no CTAs will be shown
[Tier 3] Fallback branch '{branch_name}' not found in conversation_branches
```

### Expected Behavior

- **Action chip clicks**: Should log `[Tier 1]` routing
- **CTA button clicks**: Should log `[Tier 2]` routing
- **Free-form queries**: Should log `[Tier 3]` routing
- **Invalid branches**: Should fall through gracefully to next tier

### Metrics to Track

1. **Routing Tier Distribution**:
   - % requests using Tier 1 (action chips)
   - % requests using Tier 2 (CTAs)
   - % requests using Tier 3 (fallback)
   - % requests using deprecated keyword detection

2. **Error Rates**:
   - Target: <0.1% error rate
   - Monitor Lambda errors in CloudWatch

3. **CTA Display Rates**:
   - Target: >98% of queries show CTAs (vs ~85% with keyword matching)
   - "No CTAs shown" incidents should drop to <2%

---

## Environment Variables (Unchanged)

All environment variables remain the same:

```json
{
  "S3_BUCKET": "myrecruiter-picasso",
  "MESSAGES_TABLE_NAME": "staging-recent-messages",
  "MONITORING_ENABLED": "true",
  "ENVIRONMENT": "staging",
  "JWT_EXPIRY_MINUTES": "30",
  "SUMMARIES_TABLE_NAME": "staging-conversation-summaries",
  "AUDIT_TABLE_NAME": "picasso-audit-staging",
  "SESSION_POOL_SIZE": "10",
  "DYNAMODB_POOL_SIZE": "5",
  "CONFIG_BUCKET": "myrecruiter-picasso",
  "STREAMING_ENDPOINT": "https://7pluzq3axftklmb4gbgchfdahu0lcnqd.lambda-url.us-east-1.on.aws/",
  "JWT_SECRET_KEY_NAME": "picasso/staging/jwt/signing-key",
  "VERSION": "1.1.2",
  "BUBBLE_WEBHOOK_URL": "https://hrfx.bubbleapps.io/version-test/api/1.1/wf/form_submission"
}
```

---

## Lambda Configuration (Unchanged)

- **Timeout**: 300 seconds
- **Memory**: 512 MB
- **Architecture**: x86_64
- **Ephemeral Storage**: 512 MB
- **Handler**: `lambda_function.lambda_handler`

---

## Next Steps

### 1. Deploy Bedrock_Streaming_Handler_Staging (PRIMARY PATH)
Master_Function is only the HTTP fallback (20% traffic). The primary streaming path also needs deployment:

```bash
cd /Lambdas/lambda/Bedrock_Streaming_Handler_Staging
npm ci --production
zip -r deployment.zip . -x "*.md" -x "test_*.js" -x "__tests__/*"
aws lambda update-function-code \
  --function-name Bedrock_Streaming_Handler_Staging \
  --zip-file fileb://deployment.zip \
  --profile ai-developer
```

### 2. Monitor CloudWatch Logs (24-48 hours)
- Watch for routing tier log messages
- Monitor error rates
- Track CTA display rates
- Verify no regression in user experience

### 3. Gradual Tenant Migration
- Update tenant configs to v1.4.1 format
- Add `target_branch` to action chips
- Configure `cta_settings.fallback_branch`
- Test each tenant before moving to next

### 4. Production Deployment (After Staging Validation)
- Deploy to Master_Function_Production
- Deploy to Bedrock_Streaming_Handler_Production
- Monitor production metrics

---

## Related Documentation

- **PRD**: `/Picasso/docs/PRD_ACTION_CHIPS_EXPLICIT_ROUTING_FALLBACK_HUB.md`
- **Implementation Summary**: `/Picasso/docs/ACTION_CHIPS_EXPLICIT_ROUTING_IMPLEMENTATION_SUMMARY.md`
- **Schema Documentation**: `/Picasso/docs/TENANT_CONFIG_SCHEMA.md`
- **Migration Guide**: `/Picasso/docs/MIGRATION_GUIDE_V1.3_TO_V1.4.1.md`
- **Routing Implementation**: `/Lambdas/lambda/Master_Function_Staging/ROUTING_IMPLEMENTATION.md`

---

## Deployment Checklist

- ✅ Code changes implemented and tested locally
- ✅ Unit tests: 9/9 passing
- ✅ Integration tests: 11/11 passing
- ✅ Deployment package created (189,725 bytes)
- ✅ Deployed to Lambda staging
- ✅ Version 10 published with notes
- ✅ Deployment documentation created
- ⏳ Bedrock_Streaming_Handler deployment (next step)
- ⏳ CloudWatch monitoring (24-48 hours)
- ⏳ Production deployment (after staging validation)

---

**Deployed By**: Claude Code (AI Assistant)
**Deployment Status**: ✅ Successful
**Ready for Testing**: Yes
**Production Ready**: After staging validation (24-48 hours)
