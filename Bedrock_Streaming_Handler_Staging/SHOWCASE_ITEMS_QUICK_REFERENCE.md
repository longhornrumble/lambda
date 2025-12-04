# Showcase Items - Quick Reference Guide

## For Config Admins

### How to Add a Showcase Item to a Branch

1. **Create the showcase item in `content_showcase`**:

```json
{
  "content_showcase": [
    {
      "id": "holiday_2025",
      "type": "campaign",
      "name": "Holiday Giving Guide 2025",
      "tagline": "Make a child's holiday magical",
      "description": "Multiple ways to support children this holiday season",
      "image_url": "https://cdn.example.com/holiday-2025.jpg",
      "highlights": [
        "Toy drive signup",
        "Wish list fulfillment",
        "Direct donations"
      ],
      "available_ctas": {
        "primary": "toy_drive_signup",
        "secondary": ["browse_wishlists", "donate_online"]
      },
      "enabled": true
    }
  ]
}
```

2. **Link it to a conversation branch**:

```json
{
  "conversation_branches": {
    "holiday_giving": {
      "showcase_item_id": "holiday_2025",
      "available_ctas": {
        "primary": "general_cta",
        "secondary": ["another_cta"]
      }
    }
  }
}
```

3. **Define the CTAs** (if not already defined):

```json
{
  "cta_definitions": {
    "toy_drive_signup": {
      "label": "Join Toy Drive",
      "action": "external_link",
      "url": "https://example.com/toy-drive"
    },
    "browse_wishlists": {
      "label": "Browse Wish Lists",
      "action": "external_link",
      "url": "https://example.com/wishlists"
    }
  }
}
```

### Showcase Item Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `id` | Yes | string | Unique identifier |
| `type` | Yes | string | Type: "campaign", "program", "event" |
| `name` | Yes | string | Display name |
| `tagline` | No | string | Short catchy subtitle |
| `description` | Yes | string | Main description |
| `image_url` | No | string | Image URL (recommended) |
| `highlights` | No | array | Bullet points (2-4 recommended) |
| `available_ctas` | No | object | CTAs for this showcase |
| `enabled` | No | boolean | Enable/disable (default: true) |

### CTA Configuration for Showcases

**Option 1: Use branch CTAs** (showcase has no `available_ctas`)
- Showcase displays with branch's default CTAs

**Option 2: Use showcase-specific CTAs** (recommended)
- Define `available_ctas` in the showcase item
- Lambda will resolve these to full CTA definitions
- Overrides branch CTAs for the showcase card

## For Frontend Developers

### Expected Response Structure

```javascript
{
  message: "AI response text...",
  ctaButtons: [...],  // Branch CTAs (as before)
  showcaseCard: {     // NEW: Optional showcase card
    id: "holiday_2025",
    type: "campaign",
    name: "Holiday Giving Guide 2025",
    tagline: "Make a child's holiday magical",
    description: "...",
    image_url: "https://...",
    highlights: ["...", "..."],
    ctaButtons: {
      primary: { id: "...", label: "...", action: "...", ... },
      secondary: [{ id: "...", label: "...", ... }]
    }
  },
  metadata: {
    enhanced: true,
    has_showcase: true,  // NEW: Indicates showcase present
    branch: "holiday_giving",
    routing_tier: "explicit"
  }
}
```

### How to Detect Showcase Cards

```javascript
// Check for showcase in response
if (response.showcaseCard) {
  // Render showcase card component
  renderShowcaseCard(response.showcaseCard);
}

// Or use metadata flag
if (response.metadata?.has_showcase) {
  // Showcase is present
}
```

### Showcase CTA Structure

```javascript
// Primary CTA (single object or null)
showcaseCard.ctaButtons.primary = {
  id: "toy_drive_signup",
  label: "Join Toy Drive",
  action: "external_link",
  url: "https://example.com/toy-drive",
  description: "Sign up to donate toys"
};

// Secondary CTAs (array)
showcaseCard.ctaButtons.secondary = [
  {
    id: "browse_wishlists",
    label: "Browse Wish Lists",
    action: "external_link",
    url: "https://example.com/wishlists"
  }
];
```

## For Backend Developers

### Using the New Functions

```javascript
const {
  getShowcaseForBranch,
  resolveShowcaseCTAs
} = require('./response_enhancer');

// Get showcase item for a branch
const showcaseItem = getShowcaseForBranch('holiday_giving', config);
// Returns: showcase item object or null

// Resolve CTAs
const resolvedCtas = resolveShowcaseCTAs(showcaseItem, config);
// Returns: { primary: CTA|null, secondary: CTA[] }
```

### Integration Points

The showcase logic is integrated at:
1. **Tier 1-3 (Explicit Routing)**: Lines 614-669
2. **Tier 4 (AI-Suggested)**: Lines 674-792

Both tiers check for showcase items automatically when a branch is determined.

### Logging

Look for these log prefixes:
- `[Showcase]` - Showcase item lookup
- `[Showcase CTAs]` - CTA resolution
- `[Explicit Routing]` - Tier 1-3 routing with showcase
- `[Tier 4]` - AI-suggested routing with showcase

## Testing

### Run the test suite:

```bash
cd /Users/chrismiller/Desktop/Working_Folder/Lambdas/lambda/Bedrock_Streaming_Handler_Staging
node test-showcase-items.js
```

### Manual testing with curl:

```bash
# Test with explicit routing metadata
curl -X POST https://your-api.com/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Tell me about holiday giving",
    "routingMetadata": {
      "action_chip_triggered": true,
      "target_branch": "holiday_giving"
    }
  }'
```

Expected: Response includes `showcaseCard` field

## Common Patterns

### Pattern 1: Seasonal Campaign
```json
{
  "id": "spring_fundraiser_2025",
  "type": "campaign",
  "name": "Spring Fundraiser",
  "image_url": "https://...",
  "highlights": ["Goal: $50,000", "Ends March 31"],
  "available_ctas": {
    "primary": "donate_now",
    "secondary": ["share_campaign", "learn_more"]
  }
}
```

### Pattern 2: Program Spotlight
```json
{
  "id": "mentorship_program",
  "type": "program",
  "name": "Youth Mentorship Program",
  "tagline": "Be the change in a child's life",
  "highlights": [
    "Weekly 1-on-1 sessions",
    "6-month commitment",
    "Full training provided"
  ],
  "available_ctas": {
    "primary": "apply_mentor"
  }
}
```

### Pattern 3: Event Promotion
```json
{
  "id": "annual_gala_2025",
  "type": "event",
  "name": "Annual Charity Gala",
  "tagline": "An evening of impact",
  "description": "Join us for dinner, auctions, and entertainment",
  "highlights": ["June 15, 2025", "Grand Ballroom", "$150 per ticket"],
  "available_ctas": {
    "primary": "buy_tickets",
    "secondary": ["sponsor_event", "volunteer_help"]
  }
}
```

## Troubleshooting

### Showcase not appearing?

1. Check branch has `showcase_item_id`
2. Verify showcase item exists in `content_showcase`
3. Check `enabled` field is `true` (or missing)
4. Verify routing is triggering the correct branch
5. Check CloudWatch logs for `[Showcase]` messages

### CTAs not resolving?

1. Verify CTA IDs in `available_ctas` match `cta_definitions`
2. Check for typos in CTA IDs
3. Review CloudWatch logs for `[Showcase CTAs]` warnings
4. Ensure `cta_definitions` is loaded in config

### Response missing `showcaseCard`?

1. Check `metadata.has_showcase` - should be `true`
2. Verify branch routing is working (check `metadata.branch`)
3. Test with showcase test suite
4. Check if showcase was filtered out (disabled)

---

**Questions?** Check the full implementation guide: `SHOWCASE_ITEMS_IMPLEMENTATION.md`
