# Showcase Items as CTA Hubs - Implementation Summary

**Phase 2.3: Lambda Response Enhancer Updates**

## Overview

This implementation adds support for "digital flyer" showcase cards to the Picasso chat widget. When a user clicks an action chip that routes to a branch with a `showcase_item_id`, the Lambda will include the full showcase item with resolved CTAs in the response.

## Files Modified

### `/Users/chrismiller/Desktop/Working_Folder/Lambdas/lambda/Bedrock_Streaming_Handler_Staging/response_enhancer.js`

**Changes:**

1. **Config Loading Enhancement** (Line 76)
   - Added `content_showcase` to the loaded tenant configuration
   - Ensures showcase items are available for lookup

2. **New Helper Functions**

   **`getShowcaseForBranch(branchName, config)`** (Lines 304-338)
   - Looks up a conversation branch by name
   - Returns the associated showcase item if `showcase_item_id` is present
   - Handles edge cases:
     - Branch doesn't exist → returns `null`
     - No `showcase_item_id` → returns `null`
     - Showcase item not found → returns `null`
     - Showcase item disabled → returns `null`

   **`resolveShowcaseCTAs(showcaseItem, config)`** (Lines 350-392)
   - Takes showcase item's `available_ctas.primary` and `available_ctas.secondary`
   - Resolves each CTA ID to full CTA definition from `config.cta_definitions`
   - Returns object with `{ primary: CTA|null, secondary: CTA[] }`
   - Strips legacy `style` field from CTAs
   - Handles missing CTA definitions gracefully

3. **Enhanced Response Generation**

   **Tier 1-3: Explicit Routing** (Lines 614-669)
   - Checks for showcase items when explicit branch routing is triggered
   - Builds showcase card with all fields plus resolved CTAs
   - Includes `showcaseCard` in response if present
   - Adds `has_showcase: true` to metadata

   **Tier 4: AI-Suggested Routing** (Lines 674-792)
   - Same showcase item support for AI-suggested branches
   - Also supports showcase items in fallback branch routing
   - Maintains consistency with explicit routing behavior

4. **Module Exports** (Lines 986-995)
   - Exported `getShowcaseForBranch` for external use
   - Exported `resolveShowcaseCTAs` for external use

## Response Structure

### Enhanced Response with Showcase Card

```javascript
{
  message: "Here are our holiday giving options...",
  ctaButtons: [
    {
      id: "toy_drive_signup",
      label: "Join Toy Drive",
      action: "external_link",
      url: "https://example.com/toy-drive",
      _position: "primary"
    }
  ],
  showcaseCard: {
    id: "holiday_2025",
    type: "campaign",
    name: "Holiday Giving Guide 2025",
    tagline: "Make a child's holiday magical",
    description: "This season, there are many ways...",
    image_url: "https://example.com/images/holiday-2025.jpg",
    highlights: [
      "Multiple ways to give",
      "Direct impact on local families",
      "Tax-deductible donations"
    ],
    ctaButtons: {
      primary: {
        id: "toy_drive_signup",
        label: "Join Toy Drive",
        action: "external_link",
        url: "https://example.com/toy-drive"
      },
      secondary: [
        {
          id: "browse_wishlists",
          label: "Browse Wish Lists",
          action: "external_link",
          url: "https://example.com/wishlists"
        }
      ]
    }
  },
  metadata: {
    enhanced: true,
    branch: "holiday_giving",
    routing_tier: "explicit",
    routing_method: "action_chip",
    has_showcase: true
  }
}
```

## Configuration Schema

### ConversationBranch with Showcase

```typescript
{
  "holiday_giving": {
    "showcase_item_id": "holiday_2025",  // NEW: Links to content_showcase
    "available_ctas": {
      "primary": "toy_drive_signup",
      "secondary": ["browse_wishlists", "donate_online"]
    }
  }
}
```

### ShowcaseItem in content_showcase

```typescript
{
  "id": "holiday_2025",
  "type": "campaign",
  "name": "Holiday Giving Guide 2025",
  "tagline": "Make a child's holiday magical",
  "description": "This season, there are many ways to help...",
  "image_url": "https://example.com/images/holiday-2025.jpg",
  "highlights": [
    "Multiple ways to give",
    "Direct impact on local families"
  ],
  "available_ctas": {           // NEW: CTAs specific to this showcase
    "primary": "toy_drive_signup",
    "secondary": ["browse_wishlists", "donate_online"]
  },
  "enabled": true
}
```

## Backward Compatibility

The implementation is fully backward compatible:

1. **Branches without `showcase_item_id`**: Work exactly as before
2. **Showcase items without `available_ctas`**: Display without CTA buttons
3. **Disabled showcase items**: Automatically filtered out
4. **Missing CTA definitions**: Gracefully handled, only valid CTAs included

## Testing

A comprehensive test suite has been created at:
`/Users/chrismiller/Desktop/Working_Folder/Lambdas/lambda/Bedrock_Streaming_Handler_Staging/test-showcase-items.js`

### Test Coverage

- ✅ Get showcase for branch with showcase item
- ✅ Get showcase for branch without showcase item
- ✅ Get showcase for disabled item
- ✅ Get showcase for non-existent branch
- ✅ Resolve showcase CTAs (primary + secondary)
- ✅ Resolve CTAs with missing definitions
- ✅ Resolve CTAs with no available_ctas

All tests pass successfully.

## Logging

Enhanced logging for debugging:

- `[Showcase]` prefix for showcase item lookups
- `[Showcase CTAs]` prefix for CTA resolution
- Logs showcase card inclusion in responses
- Tracks which routing tier triggered showcase display

## Next Steps

1. **Frontend Integration** (Phase 2.4)
   - Create ShowcaseCard component to render showcase cards
   - Parse `showcaseCard` from Lambda responses
   - Display rich media and CTA buttons

2. **Config Builder Integration** (Phase 2.5)
   - Add showcase item editor UI
   - Link branches to showcase items
   - Configure showcase-specific CTAs

3. **Deployment**
   - Package Lambda with updated response_enhancer.js
   - Deploy to staging environment
   - Test with real tenant configurations

## Implementation Notes

- No breaking changes to existing API
- Follows existing code patterns (similar to `buildCtasFromBranch`)
- Comprehensive error handling and edge case coverage
- Maintains separation of concerns (lookup vs resolution)
- JSDoc comments for all new functions
- Consistent logging style with existing code

## Performance Considerations

- Showcase items are cached as part of tenant config (5-minute TTL)
- Minimal overhead: 2 array lookups per request (if showcase present)
- No additional S3 calls required
- CTA resolution happens in-memory

---

**Status**: Implementation complete, ready for frontend integration
**Author**: Claude Code
**Date**: 2025-12-03
