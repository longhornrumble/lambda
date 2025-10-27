# Lambda Deployment - v2.1.0

**Deployed:** 2025-10-26
**Version:** 12
**Function:** Bedrock_Streaming_Handler_Staging
**Region:** us-east-1

---

## Overview

This deployment removes inline call-to-action (CTA) language from Bedrock-generated responses to prepare for the systematic CTA button injection system being built in `picasso-config-builder`.

## Strategic Context

### Problem Identified

Austin Angels chatbot was generating formulaic "Get Involved" / "Getting Involved" / "How to Get Started" sections with inline clickable CTAs in every response:

```
Getting Involved:
• Make your first donation
• Start monthly giving
• Email questions to accounting@austinangels.com
```

When the CTA button system (built via `response_enhancer.js`) is deployed, this creates:
1. **Redundancy** - Same action appears as inline link AND button
2. **Inconsistent tracking** - Inline CTAs from Bedrock can't be tracked/measured
3. **Poor UX** - Users confused about which CTAs to click
4. **Lost opportunity** - Context-aware button CTAs are superior to generic inline CTAs

### Solution

**Clean separation of concerns:**
- **Bedrock (index.js prompt):** Generate informational, helpful content ONLY
- **Response Enhancer (response_enhancer.js):** Inject context-aware CTA buttons based on conversation branch detection

---

## Changes Made to index.js

### 1. Removed Contact Info Instruction (Line 238)

**Before:**
```javascript
1. ONLY provide contact information (phone, email, addresses) that appears in the knowledge base results
```

**After:**
```javascript
1. NEVER make up or invent ANY details including program names, services, or contact information
```

**Rationale:** Contact info should only appear if directly answering a question like "How do I contact you?", not appended to every response.

---

### 2. Removed Generic Contact Suggestion (Line 243 - DELETED)

**Before:**
```javascript
6. If no specific contact info is available, suggest visiting the website or contacting the main office
```

**After:** Removed entirely

**Rationale:** This created generic "Contact Us" sections. The CTA button system handles this better.

---

### 3. Added Explicit NO CALLS-TO-ACTION Rule (Line 251)

**Before:**
```javascript
4. **CONTACT INFO STRATEGY**: Include phone numbers and email addresses when found in knowledge base.
   Avoid generic link phrases like "visit our website" or "apply here" - structured action buttons
   will be provided separately based on context
```

**After:**
```javascript
4. **NO CALLS-TO-ACTION**: Do NOT include action phrases like "Apply here", "Sign up today",
   "Contact us to get started", or "Visit our website". Action buttons are provided automatically
   by the system based on conversation context.
```

**Rationale:** Explicit prohibition with examples prevents Bedrock from generating inline CTAs.

---

### 4. Removed Template Contact/Next Steps Section (Lines 273-274)

**Before:**
```javascript
**Contact/Next Steps:**
[Include specific phone numbers and emails if available for direct contact. Action buttons are provided separately.]
```

**After:**
```javascript
End with a warm, supportive closing statement.
```

**Rationale:** The template was triggering formulaic contact sections. New instruction focuses on natural closings.

---

### 5. Updated Example Response Ending (Lines 291-292)

**Before:**
```javascript
**How to Get Started:**
To learn more about our grief counseling services or schedule a session, please contact our
bereavement team at (555) 123-4567 or bereavement@hospice.org.

Our team is here to support you every step of the way. 💙"
```

**After:**
```javascript
We're here to support you every step of the way. 💙"
```

**Rationale:** Removed action-oriented "How to Get Started" section from the example. Contact info removed (unless directly answering contact question). Clean, warm closing only.

---

## Expected Behavior Changes

### Before Deployment

```
🎁 We're excited to share donation options!

Donation Methods:
• Online giving via our website
• Mail checks to [address]
• Wire transfer

Getting Involved:
• Make your first donation
• Start monthly giving
• Email questions to accounting@austinangels.com

We're grateful for your support! 💖
```

### After Deployment

**Bedrock Response:**
```
🎁 We're excited to share donation options!

Donation Methods:
• Online giving
• Mail checks to [address]
• Wire transfer

We're grateful for your support! 💖
```

**Then response_enhancer.js adds:**
```
[Make Your First Donation]  [Start Monthly Giving]
```

---

## What This Enables

### 1. Context-Aware CTAs
`response_enhancer.js` can inject CTAs based on:
- Conversation branch detection (keywords in Bedrock response)
- User engagement level
- Program discussed
- Form eligibility

### 2. Trackable Actions
All CTA clicks flow through `response_enhancer.js`:
- Track which conversation led to which action
- A/B test CTA text and placement
- Measure conversion rates by branch

### 3. Dynamic CTA Management
CTA configuration lives in tenant config (S3):
- Add/edit/remove CTAs without retraining Bedrock
- Different CTAs for different tenants
- Primary/secondary CTA logic

### 4. Clean UX
Users see:
- **Content:** Informational, helpful, natural
- **Actions:** Clear, distinct, trackable buttons

---

## Compatibility

### Backward Compatible
- If `response_enhancer.js` doesn't detect a branch → No CTAs shown (graceful degradation)
- If tenant config has no CTA definitions → No CTAs shown
- Contact info still appears IF user asks "How do I contact you?"

### No Breaking Changes
- Existing conversations continue working
- No changes to Master_Function_Staging required
- No changes to Picasso widget required (until CTA button UI is deployed)

---

## Next Steps

### 1. Deploy picasso-config-builder (MVP)
- Build CTA button system in Picasso widget
- Render `ctaButtons` array from `response_enhancer.js`
- Style buttons consistently

### 2. Configure Tenant CTAs
Use `picasso-config-builder` to:
- Define CTA inventory (Apply for Love Box, Donate, Contact Us, etc.)
- Map CTAs to conversation branches
- Set primary/secondary CTA rules

### 3. Monitor Response Quality
- Check that responses don't feel incomplete without CTAs
- Verify users can still find contact info when needed
- Adjust prompt if too much/too little info

### 4. Measure Impact
Compare before/after:
- CTA click-through rates
- Conversion rates (form submissions, donations)
- User satisfaction
- Support ticket volume

---

## Rollback Plan

If issues arise:

### Quick Rollback to v2.0.0 (Version 11)
```bash
aws lambda update-function-code \
  --function-name Bedrock_Streaming_Handler_Staging \
  --s3-bucket myrecruiter-picasso \
  --s3-key lambda/Bedrock_Streaming_Handler_Staging/v2.0.0.zip \
  --region us-east-1 \
  --profile root
```

### Or Revert Code Changes
Restore previous prompt in `index.js`:
- Add back "CONTACT INFO STRATEGY"
- Add back "Contact/Next Steps" template
- Add back example "How to Get Started" section
- Redeploy

---

## Testing Checklist

- [ ] Verify responses no longer have "Get Involved" sections
- [ ] Verify responses no longer have inline action CTAs
- [ ] Verify contact info still appears when user asks "How do I contact you?"
- [ ] Verify responses end with warm closings
- [ ] Verify informational content quality maintained
- [ ] Verify `response_enhancer.js` still detects branches
- [ ] Verify `ctaButtons` array properly populated in SSE response
- [ ] Test with multiple tenants (Austin Angels, others)

---

## Version History

| Version | Date | Description |
|---------|------|-------------|
| v2.0.0 (11) | 2025-10-02 | Phase 1-3: AWS SDK v3, Advanced Fulfillment, 135 Tests (84% coverage) |
| **v2.1.0 (12)** | **2025-10-26** | **Removed inline CTAs from Bedrock prompt. Action buttons now injected only by response_enhancer.js. Prepares for CTA button system.** |

---

## Code Size & Performance

- **Code Size:** 10.13 MB
- **Runtime:** Node.js 20.x
- **Memory:** 2048 MB
- **Timeout:** 300s (5 min)
- **State:** Active
- **Last Update:** Successful

---

## References

- Sprint Plan: `/picasso-config-builder/docs/SPRINT_PLAN.md`
- CTA Strategy Discussion: This conversation
- Response Enhancer: `/Lambdas/lambda/Bedrock_Streaming_Handler_Staging/response_enhancer.js`
- Config Builder PRD: `/picasso-config-builder/docs/PRD.md`

---

**Deployed by:** Claude Code
**Reviewed by:** Chris Miller
**Status:** ✅ Deployed to Staging
